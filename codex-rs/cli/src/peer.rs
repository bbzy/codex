//! Passive JSON clients for existing local sessions. This module never starts a server or agent.
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::Parser;
use clap::Subcommand;
use codex_app_server_client::AppServerEvent;
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
use codex_app_server_protocol::ThreadQueueAddResponse;
use codex_app_server_protocol::ThreadQueueListResponse;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_core::config::find_codex_home;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;
use std::time::Duration;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Parser)]
pub(crate) struct PeerCommand {
    #[command(subcommand)]
    command: Operation,
}

#[derive(Debug, Subcommand)]
enum Operation {
    List {
        #[arg(long)]
        cursor: Option<String>,
    },
    Status {
        #[arg(long)]
        session: String,
    },
    Read {
        #[arg(long)]
        session: String,
        #[arg(long)]
        cursor: Option<String>,
    },
    Queue {
        #[arg(long)]
        session: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        resume: bool,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 60)]
        timeout: u32,
    },
    Wait {
        #[arg(long)]
        receipt: String,
        #[arg(long, default_value_t = 60)]
        timeout: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Receipt {
    version: u32,
    service: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReadCursor {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "turnID")]
    turn_id: Option<String>,
    native: String,
}

fn read_cursor(
    session: &str,
    turn: Option<&str>,
    native: Option<String>,
) -> Result<Option<String>> {
    native
        .map(|native| {
            Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&ReadCursor {
                session_id: session.to_string(),
                turn_id: turn.map(str::to_string),
                native,
            })?))
        })
        .transpose()
}

impl Receipt {
    fn encode(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?))
    }

    fn decode(value: &str, service: &str) -> Result<Self> {
        let receipt: Self = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value)?)?;
        if receipt.version != 1 || receipt.service != service {
            bail!("Receipt belongs to a different local service or protocol version");
        }
        Ok(receipt)
    }
}

impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Self::List { .. } => "list",
            Self::Status { .. } => "status",
            Self::Read { .. } => "read",
            Self::Queue { .. } => "queue",
            Self::Wait { .. } => "wait",
        }
    }
}

pub(crate) async fn run(command: PeerCommand) -> Value {
    let operation = command.command.name();
    match execute(command.command).await {
        Ok(mut output) => {
            output["version"] = json!(1);
            output["operation"] = json!(operation);
            output
        }
        Err(error) => json!({"version": 1, "operation": operation, "error": {
            "code": "peer_error", "message": format!("{error:#}")
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
    .context("Local service request timed out")??;
    let response = response.map_err(|error| anyhow::anyhow!("{method}: {}", error.message))?;
    Ok(serde_json::from_value(response)?)
}

async fn execute(operation: Operation) -> Result<Value> {
    let home = find_codex_home()?;
    let socket = app_server_control_socket_path(&home)?;
    let service = socket.to_string_lossy().to_string();
    let endpoint =
        verified_local_daemon_socket(socket.as_path())?.context("Local service is not running")?;
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
        .context("Local service is unavailable")?;
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
        Operation::List { cursor } => {
            let page: ThreadListResponse = rpc(
                client,
                "thread/list",
                json!({
                    "cursor": cursor, "limit": 50, "sortKey": "updated_at", "archived": false,
                    "sourceKinds": ["cli", "vscode", "exec", "appServer", "unknown"], "modelProviders": [],
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
            Ok(json!({"sessions": sessions, "nextCursor": page.next_cursor}))
        }
        Operation::Status { session } => {
            Ok(json!({"sessionID": session, "status": status(client, &session).await?}))
        }
        Operation::Read { session, cursor } => {
            read_thread(client, &session).await?;
            let cursor = cursor
                .map(|value| -> Result<ReadCursor> {
                    let cursor: ReadCursor =
                        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value)?)?;
                    if cursor.session_id != session {
                        bail!("Cursor belongs to a different session");
                    }
                    Ok(cursor)
                })
                .transpose()?;
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
                json!({"sessionID":session,"messages":messages(&page.data),"nextCursor":read_cursor(&session,turn,page.next_cursor)?}),
            )
        }
        Operation::Wait { receipt, timeout } => {
            let decoded = Receipt::decode(&receipt, service)?;
            let mut output = wait(client, &decoded, timeout).await?;
            output["sessionID"] = json!(decoded.session_id);
            output["receipt"] = json!(receipt);
            Ok(output)
        }
        Operation::Queue {
            session,
            message,
            resume,
            wait: should_wait,
            timeout,
        } => {
            if message.trim().is_empty() {
                bail!("Message must not be empty");
            }
            let before = status(client, &session).await?;
            if before["state"] == "archived" {
                bail!("Session is archived");
            }
            let receipt = Receipt {
                version: 1,
                service: service.to_string(),
                session_id: session.clone(),
                message_id: Uuid::now_v7().to_string(),
            };
            let _: ThreadQueueAddResponse = rpc(client, "thread/queue/add", json!({
                "threadId": session, "input": [{"type":"text", "text":message, "text_elements":[]}],
                "clientUserMessageId": receipt.message_id,
            })).await?;
            let token = receipt.encode()?;
            let resume_error = if resume && before["state"] == "not_loaded" {
                rpc::<Value>(
                    client,
                    "thread/resume",
                    json!({"threadId":session, "excludeTurns":true}),
                )
                .await
                .err()
                .map(|error| format!("{error:#}"))
            } else {
                None
            };
            let output = if should_wait && resume_error.is_none() {
                wait(client, &receipt, timeout).await
            } else {
                status(client, &session)
                    .await
                    .map(|snapshot| json!({"result":"queued", "status":snapshot}))
            };
            // Once admission succeeded, always preserve its receipt, including observation failures.
            let mut output = output.unwrap_or_else(
                |error| json!({"result":"queued", "observationError":format!("{error:#}")}),
            );
            output["sessionID"] = json!(session);
            output["receipt"] = json!(token);
            if let Some(error) = resume_error {
                output["resumeError"] = json!(error);
            }
            Ok(output)
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
            json!({"reason":"permission","summary":"Waiting for approval"})
        }
        ThreadActiveFlag::WaitingOnUserInput => {
            json!({"reason":"input","summary":"Waiting for user input"})
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
    let turns: ThreadTurnsListResponse = rpc(
        client,
        "thread/turns/list",
        json!({"threadId":thread.id,"limit":100,"sortDirection":"desc","itemsView":"summary"}),
    )
    .await?;
    let last = turns.data.iter().find_map(|turn| outcome(&turn.status));
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

async fn reply(
    client: &RemoteAppServerClient,
    receipt: &Receipt,
) -> Result<(Option<&'static str>, Value)> {
    let mut cursor = None;
    let turn_id = loop {
        let page = items(client, &receipt.session_id, None, cursor, "desc").await?;
        if let Some(entry) = page.data.iter().find(|entry| {
            matches!(&entry.item.payload,
            ThreadItemPayload::UserMessage { client_id: Some(id), .. } if id == &receipt.message_id)
        }) {
            break entry.turn_id.clone();
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok((None, json!({"messages":[],"nextCursor":null})));
        }
    };
    let mut cursor = None;
    let terminal = loop {
        let page: ThreadTurnsListResponse = rpc(client,"thread/turns/list",json!({"threadId":receipt.session_id,"cursor":cursor,"limit":100,"sortDirection":"desc","itemsView":"summary"})).await?;
        if let Some(turn) = page.data.iter().find(|turn| turn.id == turn_id) {
            break outcome(&turn.status);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break None;
        }
    };
    let mut page = items(client, &receipt.session_id, Some(&turn_id), None, "asc").await?;
    page.data.retain(|entry| {
        !matches!(&entry.item.payload,
        ThreadItemPayload::UserMessage { client_id: Some(id), .. } if id == &receipt.message_id)
    });
    Ok((
        terminal,
        json!({"messages":messages(&page.data),"nextCursor":read_cursor(&receipt.session_id,Some(&turn_id),page.next_cursor)?}),
    ))
}

async fn wait(
    client: &mut RemoteAppServerClient,
    receipt: &Receipt,
    timeout: u32,
) -> Result<Value> {
    // Install the signal handler before polling any observation RPC. Cancelling this
    // wait only drops the passive observation; it never interrupts the session.
    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => Ok(json!({
            "result": "waiting_interrupted", "status": null,
            "messages": [], "nextCursor": null,
        })),
        output = observe_reply(client, receipt, timeout) => output,
    }
}

async fn observe_reply(
    client: &mut RemoteAppServerClient,
    receipt: &Receipt,
    timeout: u32,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(u64::from(timeout));
    loop {
        let snapshot = status(client, &receipt.session_id).await?;
        let (terminal, mut output) = reply(client, receipt).await?;
        let state = terminal
            .map(|value| {
                if value == "succeeded" {
                    "completed"
                } else {
                    value
                }
            })
            .or_else(|| match snapshot["state"].as_str() {
                Some("blocked") => Some("blocked"),
                Some("not_loaded") => Some("not_loaded"),
                Some("archived") => Some("archived"),
                _ => None,
            });
        if let Some(state) = state {
            output["result"] = json!(state);
            output["status"] = snapshot;
            return Ok(output);
        }
        if Instant::now() >= deadline {
            output["result"] = json!("timeout");
            output["status"] = snapshot;
            return Ok(output);
        }
        // A passive client observes requests but never answers or rejects them.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(500).min(deadline.saturating_duration_since(Instant::now())) ) => {},
            event = client.next_event() => {
                if matches!(event, None | Some(AppServerEvent::Disconnected { .. })) { bail!("Local service disconnected"); }
            }
        }
    }
}

#[cfg(test)]
#[path = "peer_tests.rs"]
mod tests;
