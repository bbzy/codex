//! Wake timers belong to the current client session and are not restored.

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_sequence;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadTimerUpdatedNotification;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_features::Feature;
use core_test_support::responses;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn thread_timer_clears_when_leaving_and_does_not_resume() -> Result<()> {
    let actions = [
        ("initial", json!({"action":"status"})),
        (
            "set",
            json!({"action":"set", "type":"interval", "seconds":3600}),
        ),
        ("cancel", json!({"action":"cancel"})),
        (
            "set-again",
            json!({"action":"set", "type":"interval", "seconds":3600}),
        ),
        ("warm-resume", json!({"action":"status"})),
        (
            "set-before-cold",
            json!({"action":"set", "type":"interval", "seconds":3600}),
        ),
        ("cold-resume", json!({"action":"status"})),
    ];
    let scripts = actions
        .iter()
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
        .with_root_config("thread_unload_delay_secs = 3600")
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
    for (action, _) in &actions {
        if *action == "warm-resume" {
            let id = client
                .send_raw_request("thread/unsubscribe", Some(json!({"threadId":thread.id})))
                .await?;
            let _: ThreadUnsubscribeResponse = client.read_response(id).await?;
        }
        if *action == "cold-resume" {
            let status = timeout(Duration::from_secs(30), client.shutdown_gracefully()).await??;
            assert!(status.success());
            drop(client);
            client = TestAppServer::builder()
                .with_codex_home(home.path())
                .without_managed_config()
                .build_initialized()
                .await?;
        }
        if matches!(*action, "warm-resume" | "cold-resume") {
            let id = client
                .send_raw_request("thread/resume", Some(json!({"threadId":thread.id})))
                .await?;
            let _: ThreadResumeResponse = client.read_response(id).await?;
        }
        let id = client
            .send_raw_request(
                "turn/start",
                Some(json!({
                    "threadId":thread.id, "input":[{"type":"text", "text":action}]
                })),
            )
            .await?;
        let _: codex_app_server_protocol::TurnStartResponse = client.read_response(id).await?;
        if matches!(*action, "set" | "cancel" | "set-again" | "set-before-cold") {
            let update: ThreadTimerUpdatedNotification = timeout(
                Duration::from_secs(30),
                client.read_notification("thread/timer/updated"),
            )
            .await??;
            assert_eq!(update.thread_id, thread.id);
            assert_eq!(update.timer.is_some(), *action != "cancel");
        }
        let _: TurnCompletedNotification = timeout(
            Duration::from_secs(30),
            client.read_notification("turn/completed"),
        )
        .await??;
    }
    let requests = responses::received_responses_requests(&server).await;
    for action in ["initial", "warm-resume", "cold-resume"] {
        let output = requests
            .iter()
            .find_map(|request| request.function_call_output_text(action))
            .expect("timer status tool output");
        assert_eq!(output, "null", "{action} must have no timer");
    }
    Ok(())
}
