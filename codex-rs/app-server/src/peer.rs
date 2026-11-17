//! Durable existing-session peer tasks. No process or session is created here.
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use codex_app_server_protocol::PeerQueueParams;
use codex_app_server_protocol::PeerReplyParams;
use codex_app_server_protocol::PeerResponse;
use codex_state::PeerRoute;
use codex_state::PeerTask;
use codex_state::StateRuntime;
use futures::SinkExt;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct AdmissionRejected(String);

pub struct PeerService {
    db: Option<Arc<StateRuntime>>,
    home: PathBuf,
    delivery: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl PeerService {
    pub fn new(db: Option<Arc<StateRuntime>>, home: PathBuf) -> Self {
        Self {
            db,
            home,
            delivery: Mutex::new(HashMap::new()),
        }
    }

    fn db(&self) -> Result<&StateRuntime> {
        self.db
            .as_deref()
            .context("persistence_failed: Peer tasks require the native state database")
    }

    pub async fn context(&self) -> PeerResponse {
        response(async {
            Ok(json!({"version":2,"product":"codex","dataContext": self.db()?.peer_context().await.context("persistence_failed: Cannot read peer context")?}))
        }.await)
    }

    pub async fn queue(&self, input: PeerQueueParams) -> PeerResponse {
        response(self.admit(input).await)
    }

    async fn admit(&self, input: PeerQueueParams) -> Result<Value> {
        if input.message.trim().is_empty() || input.session_id.trim().is_empty() {
            bail!("invalid_arguments: A target Session ID and nonempty task are required");
        }
        let db = self.db()?;
        let source: PeerRoute = serde_json::from_value(input.source)?;
        let target: PeerRoute = serde_json::from_value(input.target)?;
        if target.locator
            != crate::transport::app_server_control_socket_path(&self.home)?.to_string_lossy()
            || target.product != "codex"
            || target.session_id != input.session_id
            || target.data_context != db.peer_context().await?
        {
            bail!("service_mismatch: Target data context changed");
        }
        verify(&source).await?;
        verify(&target).await?;
        let mut task = PeerTask {
            receipt: URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>()),
            source,
            target,
            message_id: format!("msg_{}", Uuid::now_v7()),
            message: input.message,
            admission: "prepared".into(),
            state: "open".into(),
            replies: Vec::new(),
        };
        while !db
            .peer_prepare(&task)
            .await
            .context("persistence_failed: Cannot save peer task")?
        {
            task.receipt = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>());
        }
        let text = format!(
            "[Peer request]\n{}\nReply explicitly with: codex peer reply --receipt {} --message TEXT [--final]. Ordinary output or idle is not a reply. Do not send automatic acknowledgements.",
            json!({"receipt":task.receipt,"from":{"product":task.source.product,"sessionID":task.source.session_id},"message":task.message}),
            task.receipt
        );
        if let Err(error) = admit_message(&task.target, &task.message_id, &text).await {
            let rejected = error.is::<AdmissionRejected>()
                || matches!(
                    error_code(&error),
                    "session_not_found"
                        | "session_archived"
                        | "service_mismatch"
                        | "unsupported_version"
                        | "invalid_arguments"
                );
            let recorded = db
                .peer_admission(&task.receipt, if rejected { "rejected" } else { "unknown" })
                .await;
            let code = if recorded.is_err() {
                "persistence_failed"
            } else {
                "admission_failed"
            };
            let message = if rejected {
                format!("Native queue explicitly rejected the task: {error}")
            } else {
                "Admission was not confirmed; reconcile the original message ID before resending"
                    .into()
            };
            return Ok(
                json!({"error":{"code":code,"message":message},"receipt":task.receipt,"messageID":task.message_id}),
            );
        }
        if db.peer_admission(&task.receipt, "accepted").await.is_err() {
            return Ok(
                json!({"error":{"code":"persistence_failed","message":"Task admitted but confirmation could not be saved; reconcile before resending"},"receipt":task.receipt,"messageID":task.message_id}),
            );
        }
        let mut result = json!({"sessionID":task.target.session_id,"receipt":task.receipt,"result":"queued","status":null});
        if input.resume
            && wire_request(
                &task.target,
                "thread/resume",
                json!({"threadId":task.target.session_id,"excludeTurns":true}),
            )
            .await
            .is_err()
        {
            result["resumeError"] =
                json!({"code":"native_error","message":"Task admitted but session resume failed"});
        }
        Ok(result)
    }

    pub async fn reply(&self, input: PeerReplyParams) -> PeerResponse {
        response(self.deliver(input).await)
    }

    async fn deliver(&self, input: PeerReplyParams) -> Result<Value> {
        if input.message.trim().is_empty() {
            bail!("invalid_arguments: Reply message must not be empty");
        }
        let lock = self
            .delivery
            .lock()
            .await
            .entry(input.receipt.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        let db = self.db()?;
        let prepared = db.peer_task(&input.receipt).await?;
        if matches!(prepared.admission.as_str(), "prepared" | "unknown") {
            let thread_id = codex_protocol::ThreadId::from_string(&prepared.target.session_id)
                .context("admission_failed: Stored target identity is invalid")?;
            if db
                .thread_queue()
                .contains_admission(thread_id, &prepared.message_id)
                .await
                .context("persistence_failed: Cannot reconcile native admission")?
            {
                db.peer_admission(&input.receipt, "accepted")
                    .await
                    .context("persistence_failed: Cannot save reconciled admission")?;
            }
        }
        let (task, reply) = db
            .peer_append(&input.receipt, &input.message, input.r#final)
            .await
            .map_err(|error| {
                if error_code(&error) == "native_error" {
                    error.context("persistence_failed: Cannot save reply")
                } else {
                    error
                }
            })?;
        let mut result = json!({"receipt":task.receipt,"sequence":reply.sequence,"final":reply.r#final,"result":"queued"});
        for pending in task
            .replies
            .iter()
            .filter(|item| item.delivery == "pending")
        {
            let text = format!(
                "[Peer reply]\n{}\nThis is an explicit reply to your existing task, not a new task. Do not automatically acknowledge it.",
                json!({"receipt":task.receipt,"sequence":pending.sequence,"from":{"product":"codex","sessionID":task.target.session_id},"message":pending.message,"final":pending.r#final})
            );
            if let Err(error) = admit_message(&task.source, &pending.message_id, &text).await {
                result["result"] = json!("pending");
                result["deliveryError"] = json!({"code":"delivery_failed","message":format!("Reply saved; return delivery was not confirmed ({})", error_code(&error))});
                return Ok(result);
            }
            if db
                .peer_delivered(&task.receipt, pending.sequence)
                .await
                .is_err()
            {
                result["result"] = json!("pending");
                result["deliveryError"] = json!({"code":"persistence_failed","message":"Reply admitted but delivery confirmation could not be saved"});
                return Ok(result);
            }
        }
        Ok(result)
    }
}

fn response(result: Result<Value>) -> PeerResponse {
    let value = result.unwrap_or_else(|error| {
        let message = error.to_string();
        let code = error_code(&error);
        json!({"error":{"code":code,"message":message}})
    });
    PeerResponse {
        fields: serde_json::from_value(value).expect("peer response is an object"),
    }
}

pub fn native_error_code(message: &str) -> &'static str {
    if message.starts_with("invalid cursor") || message.starts_with("Invalid cursor") {
        "invalid_cursor"
    } else if message.starts_with("thread not found")
        || message.starts_with("no rollout found")
        || message.starts_with("invalid thread id")
    {
        "session_not_found"
    } else {
        "native_error"
    }
}

pub fn error_code(error: &anyhow::Error) -> &str {
    error
        .chain()
        .find_map(|error| {
            let message = error.to_string();
            let (code, _) = message.split_once(':')?;
            match code {
                "invalid_arguments" => Some("invalid_arguments"),
                "invalid_cursor" => Some("invalid_cursor"),
                "service_unavailable" => Some("service_unavailable"),
                "service_mismatch" => Some("service_mismatch"),
                "unsupported_version" => Some("unsupported_version"),
                "session_not_found" => Some("session_not_found"),
                "session_archived" => Some("session_archived"),
                "receipt_not_found" => Some("receipt_not_found"),
                "task_closed" => Some("task_closed"),
                "admission_failed" => Some("admission_failed"),
                "persistence_failed" => Some("persistence_failed"),
                "delivery_failed" => Some("delivery_failed"),
                _ => None,
            }
        })
        .unwrap_or("native_error")
}

pub async fn verify(route: &PeerRoute) -> Result<()> {
    let context = wire_request(route, "peer/context", json!({})).await?;
    if context["version"] != 2 {
        bail!("unsupported_version: Peer service does not support protocol v2");
    }
    if context["product"] != route.product || context["dataContext"] != route.data_context {
        bail!("service_mismatch: Peer data context changed");
    }
    let method = if route.product == "codex" {
        "thread/read"
    } else {
        "session/get"
    };
    let session = wire_request(
        route,
        method,
        json!({"threadId":route.session_id,"sessionID":route.session_id,"includeTurns":false}),
    )
    .await?;
    if route.product == "opencode" && !session["data"]["time"]["archived"].is_null() {
        bail!("session_archived: Session is archived");
    }
    if route.product == "codex" {
        let mut cursor = Value::Null;
        loop {
            let page = wire_request(
                route,
                "thread/list",
                json!({"archived":true,"limit":100,"cursor":cursor,"modelProviders":[]}),
            )
            .await?;
            if page["data"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["id"] == route.session_id))
            {
                bail!("session_archived: Session is archived");
            }
            cursor = page["nextCursor"].clone();
            if cursor.is_null() {
                break;
            }
        }
    }
    Ok(())
}

pub async fn resolve(product: &str, session_id: &str, locator: &str) -> Result<PeerRoute> {
    let mut route = PeerRoute {
        product: product.into(),
        session_id: session_id.into(),
        locator: locator.into(),
        data_context: String::new(),
    };
    let context = wire_request(&route, "peer/context", json!({})).await?;
    route.data_context = context["dataContext"]
        .as_str()
        .context("unsupported_version: Peer service does not support protocol v2")?
        .to_string();
    verify(&route).await?;
    Ok(route)
}

pub async fn admit_message(route: &PeerRoute, id: &str, text: &str) -> Result<()> {
    verify(route).await?;
    if route.product == "codex" {
        wire_request(route, "thread/queue/add", json!({"threadId":route.session_id,"input":[{"type":"text","text":text,"text_elements":[]}],"clientUserMessageId":id})).await?;
        return Ok(());
    }
    let loaded = wire_request(route, "session/loaded", json!({})).await?;
    let resume = loaded["data"]
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item == &route.session_id));
    wire_request(
        route,
        "session/admit",
        json!({"id":id,"text":text,"delivery":"queue","resume":resume}),
    )
    .await?;
    Ok(())
}

pub async fn wire_request(route: &PeerRoute, method: &str, params: Value) -> Result<Value> {
    if route.product == "codex" {
        return socket_request(Path::new(&route.locator), method, params).await;
    }
    if route.product != "opencode" {
        bail!("invalid_arguments: Unknown peer product");
    }
    let registration: Value = serde_json::from_slice(
        &tokio::fs::read(&route.locator)
            .await
            .context("service_unavailable: Cannot read service registration")?,
    )?;
    let base = registration["url"]
        .as_str()
        .context("service_unavailable: Invalid registration")?;
    let mut url = url::Url::parse(base)?;
    if !matches!(
        url.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
    ) {
        bail!("service_unavailable: Peer requires an existing local service");
    }
    let endpoint = match method {
        "session/get" => format!("/api/session/{}", route.session_id),
        "session/admit" => format!("/api/session/{}/prompt", route.session_id),
        "session/loaded" => "/api/experimental/session/loaded".into(),
        _ => format!(
            "/api/experimental/peer/{}",
            method
                .strip_prefix("peer/")
                .context("Invalid peer method")?
        ),
    };
    url.set_path(&endpoint);
    if matches!(method, "session/get" | "session/admit") {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("service_unavailable: Invalid service URL"))?;
        segments
            .clear()
            .extend(["api", "session", route.session_id.as_str()]);
        if method == "session/admit" {
            segments.push("prompt");
        }
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let request = if matches!(method, "peer/context" | "session/get" | "session/loaded") {
        client.get(url)
    } else {
        client.post(url).json(&params)
    };
    let request = if let Some(password) = registration["password"].as_str() {
        request.basic_auth("opencode", Some(password))
    } else {
        request
    };
    let response = request
        .send()
        .await
        .context("service_unavailable: Local service request failed")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND && method == "session/get" {
        bail!("session_not_found: Session does not exist");
    }
    let response = response.error_for_status()?.json::<Value>().await?;
    if let Some(error) = response.get("error") {
        bail!(
            "{}: {}",
            error["code"].as_str().unwrap_or("native_error"),
            error["message"]
        );
    }
    Ok(response)
}

#[cfg(unix)]
async fn socket_request(socket: &Path, method: &str, params: Value) -> Result<Value> {
    let socket = codex_app_server_transport::verified_local_daemon_socket(socket)?
        .context("service_unavailable: Local Codex service is not running")?;
    tokio::time::timeout(Duration::from_secs(15), async {
        let stream = tokio::net::UnixStream::connect(socket.as_path()).await?;
        let (mut ws, _) = tokio_tungstenite::client_async("ws://localhost/rpc", stream).await?;
        ws.send(Message::Text(json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"peer_cli","version":"2"},"capabilities":{"experimentalApi":true}}}).to_string().into())).await?;
        while let Some(message) = ws.next().await {
            let Message::Text(text) = message? else {
                continue;
            };
            let message: Value = serde_json::from_str(&text)?;
            if message.get("method").is_some() || (message.get("result").is_none() && message.get("error").is_none()) {
                continue;
            }
            if let Some(error) = message.get("error") {
                if method == "thread/queue/add" && matches!(error["code"].as_i64(), Some(-32600 | -32601 | -32602 | -32001)) {
                    return Err(anyhow::Error::new(AdmissionRejected(error["message"].as_str().unwrap_or("Native request rejected").to_string())));
                }
                let code = if method == "peer/context" && error["code"] == -32601 { "unsupported_version" } else { native_error_code(error["message"].as_str().unwrap_or_default()) };
                bail!("{code}: {}", error["message"]);
            }
            if message["id"] == 1 {
                ws.send(Message::Text(json!({"method":"initialized"}).to_string().into())).await?;
                ws.send(Message::Text(json!({"id":2,"method":method,"params":params}).to_string().into())).await?;
            }
            if message["id"] == 2 {
                let result = message["result"].clone();
                ws.close(None).await?;
                if let Some(error) = result.get("error") {
                    bail!("{}: {}", error["code"].as_str().unwrap_or("native_error"), error["message"]);
                }
                return Ok(result);
            }
        }
        bail!("service_unavailable: Local peer RPC disconnected")
    }).await.context("service_unavailable: Local peer RPC timed out")?
}

#[cfg(not(unix))]
async fn socket_request(_: &Path, _: &str, _: Value) -> Result<Value> {
    bail!("service_unavailable: Local peer socket transport is unavailable on this platform")
}
