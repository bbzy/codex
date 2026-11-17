//! Passive JSON clients for existing local sessions. This module never starts a server or agent.
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::Parser;
use clap::Subcommand;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_client::app_server_control_socket_path;
use codex_app_server_client::verified_local_daemon_socket;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadActiveFlag;
use codex_app_server_protocol::ThreadItemEntry;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadQueueListResponse;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_core::config::find_codex_home;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Parser)]
pub(crate) struct PeerCommand {
    #[command(subcommand)]
    command: Operation,
}

#[derive(Debug, Subcommand)]
enum Operation {
    List {
        /// Only list sessions in the current working directory.
        #[arg(long, conflicts_with = "dir")]
        cur_dir: bool,
        /// Only list sessions in this directory; relative paths use the current working directory.
        #[arg(long, conflicts_with = "cur_dir", value_name = "PATH")]
        dir: Option<PathBuf>,
        #[arg(long)]
        cursor: Option<String>,
    },
    Status {
        session: String,
    },
    Read {
        session: String,
        #[arg(long)]
        cursor: Option<String>,
    },
    Queue {
        session: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        resume: bool,
        #[arg(long)]
        from: String,
    },
    Reply {
        #[arg(long)]
        receipt: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        r#final: bool,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReadCursor {
    operation: String,
    data_context: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "turnID")]
    turn_id: Option<String>,
    native: String,
}

fn read_cursor(
    context: &str,
    session: &str,
    turn: Option<&str>,
    native: Option<String>,
) -> Result<Option<String>> {
    native
        .map(|native| {
            Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&ReadCursor {
                operation: "read".into(),
                data_context: context.into(),
                session_id: session.to_string(),
                turn_id: turn.map(str::to_string),
                native,
            })?))
        })
        .transpose()
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListCursor {
    operation: String,
    data_context: String,
    directory: Option<String>,
    native: String,
}

async fn data_context(client: &RemoteAppServerClient) -> Result<String> {
    let value: Value = rpc(client, "peer/context", json!({})).await?;
    value["dataContext"]
        .as_str()
        .map(str::to_string)
        .context("service_mismatch: Peer data context is unavailable")
}

impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Self::List { .. } => "list",
            Self::Status { .. } => "status",
            Self::Read { .. } => "read",
            Self::Queue { .. } => "queue",
            Self::Reply { .. } => "reply",
        }
    }
}

pub(crate) async fn run(command: PeerCommand) -> Value {
    let operation = command.command.name();
    match execute(command.command).await {
        Ok(mut output) => {
            output["version"] = json!(2);
            output["operation"] = json!(operation);
            output
        }
        Err(error) => json!({"version": 2, "operation": operation, "error": {
            "code": codex_app_server::peer::error_code(&error), "message": format!("{error:#}")
        }}),
    }
}

async fn rpc<T: DeserializeOwned>(
    client: &RemoteAppServerClient,
    method: &str,
    params: Value,
) -> Result<T> {
    let response = tokio::time::timeout(
        Duration::from_secs(15),
        client.request_handle().request_json_rpc(JSONRPCRequest {
            id: RequestId::String(Uuid::new_v4().to_string()),
            method: method.to_string(),
            params: Some(params),
            trace: None,
        }),
    )
    .await
    .context("service_unavailable: Local service request timed out")??;
    let response = response.map_err(|error| {
        anyhow::anyhow!(
            "{}: {method}: {}",
            codex_app_server::peer::native_error_code(&error.message),
            error.message
        )
    })?;
    Ok(serde_json::from_value(response)?)
}

fn validate(operation: &Operation) -> Result<()> {
    match operation {
        Operation::Read {
            cursor: Some(cursor),
            ..
        } => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .context("invalid_cursor: Malformed read cursor")?;
            serde_json::from_slice::<ReadCursor>(&bytes)
                .context("invalid_cursor: Malformed read cursor")?;
        }
        Operation::List {
            cursor: Some(cursor),
            ..
        } => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .context("invalid_cursor: Malformed list cursor")?;
            serde_json::from_slice::<ListCursor>(&bytes)
                .context("invalid_cursor: Malformed list cursor")?;
        }
        _ => {}
    }
    match operation {
        Operation::Status { session } | Operation::Read { session, .. } => {
            if session.trim().is_empty() {
                bail!("invalid_arguments: Session ID must not be empty");
            }
        }
        Operation::Queue {
            session,
            message,
            from,
            ..
        } => {
            if session.trim().is_empty() || message.trim().is_empty() {
                bail!("invalid_arguments: A Session ID and nonempty message are required");
            }
            if !from.split_once(':').is_some_and(|(product, session)| {
                matches!(product, "codex" | "opencode") && !session.trim().is_empty()
            }) {
                bail!("invalid_arguments: --from must be PRODUCT:SESSION_ID");
            }
        }
        Operation::Reply {
            receipt, message, ..
        } => {
            if receipt.trim().is_empty() || message.trim().is_empty() {
                bail!("invalid_arguments: A receipt and nonempty message are required");
            }
        }
        Operation::List { .. } => {}
    }
    Ok(())
}

async fn execute(operation: Operation) -> Result<Value> {
    validate(&operation)?;
    let home = find_codex_home()?;
    let socket = app_server_control_socket_path(&home)?;
    let service = socket.to_string_lossy().to_string();
    let endpoint = verified_local_daemon_socket(socket.as_path())
        .context("service_unavailable: Cannot verify local service")?
        .context("service_unavailable: Local service is not running")?;
    let args = RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::UnixSocket {
            socket_path: endpoint,
        },
        client_name: "peer_cli".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        experimental_api: true,
        required_human_approval: false,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 128,
    };
    #[cfg(windows)]
    let mut client = RemoteAppServerClient::connect_local_daemon(args).await?;
    #[cfg(not(windows))]
    let mut client = RemoteAppServerClient::connect(args)
        .await
        .context("service_unavailable: Local service is unavailable")?;
    let output = dispatch(&mut client, &service, operation).await;
    // Disconnecting a passive client must not cancel the target's execution or decide its approvals.
    let _ = client.shutdown().await;
    output
}

async fn dispatch(
    client: &mut RemoteAppServerClient,
    service: &str,
    operation: Operation,
) -> Result<Value> {
    match operation {
        Operation::List {
            cursor,
            cur_dir,
            dir,
        } => {
            let directory = dir
                .or_else(|| cur_dir.then(|| PathBuf::from(".")))
                .map(AbsolutePathBuf::relative_to_current_dir)
                .transpose()
                .context("Failed to resolve list directory")?;
            let context = data_context(client).await?;
            let directory_scope = directory
                .as_ref()
                .map(|path| path.to_string_lossy().to_string());
            let cursor = cursor.map(|value| -> Result<String> {
                let cursor: ListCursor = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value)?)?;
                if cursor.operation != "list" || cursor.data_context != context || cursor.directory != directory_scope {
                    bail!("Cursor scope changed");
                }
                Ok(cursor.native)
            }).transpose().context("invalid_cursor: Cursor does not belong to this operation, service or directory")?;
            let page: ThreadListResponse = rpc(
                client,
                "thread/list",
                json!({
                    "cursor": cursor, "limit": 50, "sortKey": "updated_at", "archived": false,
                    "sourceKinds": ["cli", "vscode", "exec", "appServer", "unknown"], "modelProviders": [],
                    "cwd": directory,
                }),
            )
            .await?;
            let mut sessions = Vec::new();
            for thread in page.data {
                if thread.ephemeral
                    || thread.parent_thread_id.is_some()
                    || matches!(thread.source, SessionSource::SubAgent(_))
                {
                    continue;
                }
                let snapshot = snapshot(client, &thread, false).await?;
                sessions.push(json!({"sessionID": thread.id, "title": thread.name,
                    "directory": thread.cwd, "updatedAt": thread.updated_at * 1000, "status": snapshot}));
            }
            let next = page
                .next_cursor
                .map(|native| -> Result<String> {
                    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&ListCursor {
                        operation: "list".into(),
                        data_context: context,
                        directory: directory_scope,
                        native,
                    })?))
                })
                .transpose()?;
            Ok(json!({"sessions": sessions, "nextCursor": next}))
        }
        Operation::Status { session } => {
            Ok(json!({"sessionID": session, "status": status(client, &session).await?}))
        }
        Operation::Read { session, cursor } => {
            let context = data_context(client).await?;
            read_thread(client, &session).await?;
            let cursor = cursor
                .map(|value| -> Result<ReadCursor> {
                    let cursor: ReadCursor =
                        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value)?)?;
                    if cursor.session_id != session
                        || cursor.operation != "read"
                        || cursor.data_context != context
                    {
                        bail!("Cursor belongs to a different session");
                    }
                    Ok(cursor)
                })
                .transpose()
                .context(
                    "invalid_cursor: Cursor does not belong to this operation, service or session",
                )?;
            let turn = cursor.as_ref().and_then(|value| value.turn_id.as_deref());
            let page = items(
                client,
                &session,
                turn,
                cursor.as_ref().map(|value| value.native.clone()),
                "asc",
            )
            .await?;
            Ok(
                json!({"sessionID":session,"messages":messages(&page.data),"nextCursor":read_cursor(&context,&session,turn,page.next_cursor)?}),
            )
        }
        Operation::Reply {
            receipt,
            message,
            r#final,
        } => {
            if message.trim().is_empty() {
                bail!("invalid_arguments: Reply message must not be empty");
            }
            rpc(
                client,
                "peer/reply",
                json!({"receipt":receipt,"message":message,"final":r#final}),
            )
            .await
        }
        Operation::Queue {
            session,
            message,
            resume,
            from,
        } => {
            if message.trim().is_empty() {
                bail!("invalid_arguments: Task message must not be empty");
            }
            let (product, source_id) = from
                .split_once(':')
                .context("invalid_arguments: --from must be PRODUCT:SESSION_ID")?;
            if !matches!(product, "codex" | "opencode") || source_id.trim().is_empty() {
                bail!("invalid_arguments: --from must be PRODUCT:SESSION_ID");
            }
            let source_locator = if product == "codex" {
                service.to_string()
            } else {
                let state = if let Some(state) = std::env::var_os("XDG_STATE_HOME") {
                    PathBuf::from(state)
                } else {
                    PathBuf::from(
                        std::env::var_os("HOME")
                            .context("service_unavailable: Native home is unavailable")?,
                    )
                    .join(".local/state")
                };
                state
                    .join("opencode/service.json")
                    .to_string_lossy()
                    .to_string()
            };
            let source =
                codex_app_server::peer::resolve(product, source_id, &source_locator).await?;
            let context: Value = rpc(client, "peer/context", json!({})).await?;
            let target = json!({"product":"codex","sessionID":session,"dataContext":context["dataContext"],"locator":service});
            let mut result: Value = rpc(client, "peer/queue", json!({"sessionID":session,"message":message,"resume":resume,"source":source,"target":target})).await.context("admission_failed: Queue acknowledgement was lost; the task may already exist. Reconcile the target before resending")?;
            if result.get("error").is_none() {
                match status(client, &session).await {
                    Ok(snapshot) => result["status"] = snapshot,
                    Err(_) => {
                        result["observationError"] = json!({"code":"native_error","message":"Task admitted but status observation failed"})
                    }
                }
            }
            Ok(result)
        }
    }
}

async fn read_thread(client: &RemoteAppServerClient, session: &str) -> Result<Thread> {
    let response: ThreadReadResponse = rpc(
        client,
        "thread/read",
        json!({"threadId":session,"includeTurns":false}),
    )
    .await?;
    Ok(response.thread)
}

async fn is_archived(client: &RemoteAppServerClient, session: &str) -> Result<bool> {
    let mut cursor = None;
    loop {
        let page: ThreadListResponse = rpc(
            client,
            "thread/list",
            json!({"archived":true,"limit":100,"cursor":cursor,
            "sourceKinds":["cli","vscode","exec","appServer","unknown"], "modelProviders":[]}),
        )
        .await?;
        if page.data.iter().any(|thread| thread.id == session) {
            return Ok(true);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(false);
        }
    }
}

async fn status(client: &RemoteAppServerClient, session: &str) -> Result<Value> {
    let thread = read_thread(client, session).await?;
    snapshot(client, &thread, is_archived(client, session).await?).await
}

fn blockers(status: &ThreadStatus) -> Vec<Value> {
    let ThreadStatus::Active { active_flags } = status else {
        return Vec::new();
    };
    [
        ThreadActiveFlag::WaitingOnApproval,
        ThreadActiveFlag::WaitingOnUserInput,
    ]
    .into_iter()
    .filter(|flag| active_flags.contains(flag))
    .map(|flag| match flag {
        ThreadActiveFlag::WaitingOnApproval => {
            json!({"reason":"permission","summary":"Waiting for approval","id":null})
        }
        ThreadActiveFlag::WaitingOnUserInput => {
            json!({"reason":"input","summary":"Waiting for user input","id":null})
        }
    })
    .collect()
}

async fn snapshot(
    client: &RemoteAppServerClient,
    thread: &Thread,
    archived: bool,
) -> Result<Value> {
    if thread.status == ThreadStatus::SystemError {
        bail!("Session execution service reports a system error");
    }
    let mut pending = 0;
    let mut cursor = None;
    loop {
        let page: ThreadQueueListResponse = rpc(
            client,
            "thread/queue/list",
            json!({"threadId":thread.id,"cursor":cursor,"limit":100}),
        )
        .await?;
        pending += page.data.len();
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    let mut cursor = None;
    let last = loop {
        let turns: ThreadTurnsListResponse = rpc(
            client,
            "thread/turns/list",
            json!({"threadId":thread.id,"cursor":cursor,"limit":100,"sortDirection":"desc","itemsView":"summary"}),
        ).await?;
        let last = turns.data.iter().find_map(|turn| outcome(&turn.status));
        cursor = turns.next_cursor;
        if last.is_some() || cursor.is_none() {
            break last;
        }
    };
    let reasons = blockers(&thread.status);
    let state = if archived {
        "archived"
    } else if !reasons.is_empty() {
        "blocked"
    } else {
        match thread.status {
            ThreadStatus::NotLoaded => "not_loaded",
            ThreadStatus::Active { .. } => "running",
            _ => "idle",
        }
    };
    Ok(
        json!({"sessionID":thread.id,"state":state,"blocked":reasons.first(),"blockers":reasons,
        "pendingCount":pending,"lastOutcome":last,"observedAt":chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis,true)}),
    )
}

fn outcome(status: &TurnStatus) -> Option<&'static str> {
    match status {
        TurnStatus::Completed => Some("succeeded"),
        TurnStatus::Failed => Some("failed"),
        TurnStatus::Interrupted => Some("interrupted"),
        TurnStatus::InProgress => None,
    }
}

async fn items(
    client: &RemoteAppServerClient,
    session: &str,
    turn: Option<&str>,
    cursor: Option<String>,
    direction: &str,
) -> Result<ThreadItemsListResponse> {
    rpc(client,"thread/items/list",json!({"threadId":session,"turnId":turn,"cursor":cursor,"sortDirection":direction,"limit":200})).await
}

fn messages(items: &[ThreadItemEntry]) -> Vec<Value> {
    items.iter().filter_map(|entry| match &entry.item.payload {
        ThreadItemPayload::UserMessage { id, content, .. } => Some(json!({"id":id,"role":"user", "text":content.iter().filter_map(|input| match input { UserInput::Text { text, .. } => Some(text.as_str()), _ => None }).collect::<Vec<_>>().join("\n")})),
        ThreadItemPayload::AgentMessage { id, text, .. } => Some(json!({"id":id,"role":"assistant","text":text})),
        _ => None,
    }).collect()
}

#[cfg(test)]
#[path = "peer_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
#[path = "peer_admission_tests.rs"]
mod admission_tests;
