//! Runtime timer reads and updates through the public API.

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_sequence;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadTimerGetResponse;
use codex_app_server_protocol::ThreadTimerUpdatedNotification;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_features::Feature;
use core_test_support::responses;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn thread_timer_reads_idle_schedule_and_clears_after_cancel() -> Result<()> {
    let scripts = [
        (
            "set",
            json!({"action":"set", "type":"once", "seconds":3600}),
        ),
        ("cancel", json!({"action":"cancel"})),
    ]
    .into_iter()
    .flat_map(|(id, args)| {
        [
            responses::sse(vec![
                responses::ev_response_created(id),
                responses::ev_function_call(id, "timer", &args.to_string()),
                responses::ev_completed(id),
            ]),
            responses::sse(vec![
                responses::ev_response_created(&format!("{id}-final")),
                responses::ev_completed(&format!("{id}-final")),
            ]),
        ]
    })
    .collect();
    let server = create_mock_responses_server_sequence(scripts).await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::TimerTool)
        .write(home.path())?;
    let mut client = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let id = client
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = client.read_response(id).await?;
    for action in ["set", "cancel"] {
        let id = client
            .send_raw_request(
                "turn/start",
                Some(json!({"threadId":thread.id, "input":[{"type":"text", "text":action}]})),
            )
            .await?;
        let _: codex_app_server_protocol::TurnStartResponse = client.read_response(id).await?;
        let update: ThreadTimerUpdatedNotification = timeout(
            Duration::from_secs(30),
            client.read_notification("thread/timer/updated"),
        )
        .await??;
        assert_eq!(update.thread_id, thread.id);
        assert_eq!(update.timer.is_some(), action == "set");
        let _: TurnCompletedNotification = timeout(
            Duration::from_secs(30),
            client.read_notification("turn/completed"),
        )
        .await??;
        let id = client
            .send_raw_request("thread/timer/get", Some(json!({"threadId":thread.id})))
            .await?;
        let current: ThreadTimerGetResponse = client.read_response(id).await?;
        assert_eq!(current.timer, update.timer);
        let id = client
            .send_raw_request("thread/read", Some(json!({"threadId":thread.id})))
            .await?;
        let read: codex_app_server_protocol::ThreadReadResponse = client.read_response(id).await?;
        assert_eq!(
            read.thread.status,
            codex_app_server_protocol::ThreadStatus::Idle
        );
    }
    // A read must not load a new runtime or schedule a turn.
    let id = client
        .send_raw_request(
            "thread/timer/get",
            Some(json!({"threadId":codex_protocol::ThreadId::new().to_string()})),
        )
        .await?;
    let empty: ThreadTimerGetResponse = client.read_response(id).await?;
    assert!(empty.timer.is_none());
    Ok(())
}
