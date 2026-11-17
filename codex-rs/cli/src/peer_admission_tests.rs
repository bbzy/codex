use anyhow::Result;
use codex_app_server::peer::PeerService;
use codex_app_server_protocol::PeerQueueParams;
use codex_app_server_protocol::PeerReplyParams;
use codex_state::PeerRoute;
use codex_state::StateRuntime;
use codex_utils_absolute_path::AbsolutePathBuf;
use futures::SinkExt;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

async fn failed_admission(disconnect: bool, committed: bool) -> Result<()> {
    let home = tempfile::tempdir()?;
    let db = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(AbsolutePathBuf::from_absolute_path(
            home.path(),
        )?),
        "test".into(),
    )
    .await?;
    let context = db.peer_context().await?;
    let socket = codex_app_server_transport::app_server_control_socket_path(home.path())?;
    let (mut listener, _guard) =
        codex_app_server_transport::bind_private_daemon_socket(socket.as_path()).await?;
    let worker_context = context.clone();
    let worker_db = Arc::clone(&db);
    let worker = tokio::spawn(async move {
        loop {
            let stream = listener.accept().await?;
            let mut ws = tokio_tungstenite::accept_async(stream).await?;
            while let Some(frame) = ws.next().await {
                let Ok(Message::Text(text)) = frame else {
                    break;
                };
                let request: Value = serde_json::from_str(&text)?;
                let Some(id) = request.get("id") else {
                    continue;
                };
                let method = request["method"].as_str().unwrap_or_default();
                if method == "thread/queue/add" {
                    // A server request may share the client's numeric request ID; it is not an acknowledgement.
                    ws.send(Message::Text(
                        json!({"id":2,"method":"item/tool/requestUserInput","params":{}})
                            .to_string()
                            .into(),
                    ))
                    .await?;
                    if disconnect {
                        if committed {
                            let thread = codex_protocol::ThreadId::from_string(
                                request["params"]["threadId"].as_str().expect("thread ID"),
                            )?;
                            let payload = json!({"UserInput":{"content":[],"client_id":request["params"]["clientUserMessageId"]}}).to_string();
                            worker_db.thread_queue().enqueue(thread, &payload).await?;
                        }
                        ws.close(None).await?;
                        break;
                    }
                    ws.send(Message::Text(json!({"id":id,"error":{"code":-32602,"message":"Explicit queue rejection"}}).to_string().into())).await?;
                    continue;
                }
                let result = match method {
                    "initialize" => json!({"userAgent":"peer-admission-test"}),
                    "peer/context" => {
                        json!({"version":2,"product":"codex","dataContext":worker_context})
                    }
                    "thread/read" => json!({"thread":{"id":request["params"]["threadId"]}}),
                    "thread/list" => json!({"data":[],"nextCursor":null}),
                    _ => anyhow::bail!("Unexpected method {method}"),
                };
                ws.send(Message::Text(
                    json!({"id":id,"result":result}).to_string().into(),
                ))
                .await?;
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });
    let target = PeerRoute {
        product: "codex".into(),
        session_id: codex_protocol::ThreadId::new().to_string(),
        data_context: context,
        locator: socket.to_string_lossy().to_string(),
    };
    let source = PeerRoute {
        session_id: codex_protocol::ThreadId::new().to_string(),
        ..target.clone()
    };
    let service = PeerService::new(Some(Arc::clone(&db)), home.path().to_path_buf());
    let response = serde_json::to_value(
        service
            .queue(PeerQueueParams {
                session_id: target.session_id.clone(),
                message: "task".into(),
                resume: false,
                source: serde_json::to_value(source)?,
                target: serde_json::to_value(target)?,
            })
            .await,
    )?;
    worker.abort();
    let _ = worker.await;
    assert_eq!(response["error"]["code"], "admission_failed", "{response}");
    let task = db
        .peer_task(response["receipt"].as_str().expect("receipt retained"))
        .await?;
    assert_eq!(
        task.admission,
        if disconnect { "unknown" } else { "rejected" }
    );
    assert_eq!(response["messageID"], task.message_id);
    assert!(db.peer_append(&task.receipt, "done", true).await.is_err());
    if committed {
        let thread = codex_protocol::ThreadId::from_string(&task.target.session_id)?;
        let queued = db.thread_queue().list_page(thread, 0, 10).await?;
        assert_eq!(queued.len(), 1);
        db.thread_queue().delete(thread, &queued[0].id).await?;
        let reopened = StateRuntime::init(db.sqlite().clone(), "test".into()).await?;
        let service = PeerService::new(Some(reopened), home.path().to_path_buf());
        let response = serde_json::to_value(
            service
                .reply(PeerReplyParams {
                    receipt: task.receipt.clone(),
                    message: "done".into(),
                    r#final: true,
                })
                .await,
        )?;
        assert_eq!(response["result"], "pending", "{response}");
        let recovered = db.peer_task(&task.receipt).await?;
        assert_eq!(recovered.admission, "accepted");
        assert_eq!(recovered.message_id, task.message_id);
        assert_eq!(recovered.replies.len(), 1);
        let thread = codex_protocol::ThreadId::from_string(&task.target.session_id)?;
        assert!(db.thread_queue().list_page(thread, 0, 10).await?.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn explicit_native_rejection_is_durable() -> Result<()> {
    failed_admission(false, false).await
}

#[tokio::test]
async fn lost_native_ack_preserves_unknown_admission_and_original_ids() -> Result<()> {
    failed_admission(true, false).await
}

#[tokio::test]
async fn reply_reconciles_lost_ack_from_native_ledger_without_resubmitting() -> Result<()> {
    failed_admission(true, true).await
}
