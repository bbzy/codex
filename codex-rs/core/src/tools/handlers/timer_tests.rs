use super::*;
use crate::session::tests::make_session_and_context;
use crate::session::step_context::StepContext;
use crate::tools::context::ToolCallSource;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn invoke(
    session: Arc<crate::session::session::Session>,
    turn: Arc<crate::session::turn_context::TurnContext>,
    arguments: Value,
) -> Result<String, FunctionCallError> {
    let output = TimerHandler
        .handle(ToolInvocation {
            session,
            step_context: StepContext::for_test(Arc::clone(&turn)),
            turn,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
            call_id: "call-1".to_string(),
            tool_name: ToolName::plain(TOOL_NAME),
            source: ToolCallSource::Direct,
            native_call: None,
            environments: Default::default(),
            payload: ToolPayload::Function {
                arguments: arguments.to_string(),
            },
        })
        .await;
    Ok(output?.log_output())
}

#[tokio::test]
async fn set_then_status_and_cancel_round_trip() {
    let (session, turn) = make_session_and_context().await;
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let output = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "type": "once", "seconds": 60, "prompt": "check"}),
    )
    .await
    .expect("set succeeds");
    let status: Value = serde_json::from_str(&output).expect("json output");
    assert_eq!(status["type"], "once");
    assert_eq!(status["seconds"], 60);
    assert_eq!(status["prompt"], "check");
    assert!(status["next_at"].is_string());

    // Setting replaces the previous timer instead of adding one.
    let output = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "type": "interval", "seconds": 30}),
    )
    .await
    .expect("replacement set succeeds");
    let status: Value = serde_json::from_str(&output).expect("json output");
    assert_eq!(status["type"], "interval");
    assert_eq!(status["seconds"], 30);
    assert!(status.get("prompt").is_none());

    let output = invoke(Arc::clone(&session), Arc::clone(&turn), json!({"action": "status"}))
        .await
        .expect("status succeeds");
    assert_eq!(
        serde_json::from_str::<Value>(&output).expect("json output")["seconds"],
        30
    );

    let output = invoke(Arc::clone(&session), Arc::clone(&turn), json!({"action": "cancel"}))
        .await
        .expect("cancel succeeds");
    assert_eq!(
        serde_json::from_str::<Value>(&output).expect("json output")["type"],
        "interval"
    );

    let output = invoke(Arc::clone(&session), Arc::clone(&turn), json!({"action": "status"}))
        .await
        .expect("status succeeds");
    assert_eq!(serde_json::from_str::<Value>(&output).expect("json output"), Value::Null);
    let output = invoke(Arc::clone(&session), Arc::clone(&turn), json!({"action": "cancel"}))
        .await
        .expect("second cancel is a no-op");
    assert_eq!(serde_json::from_str::<Value>(&output).expect("json output"), Value::Null);
}

#[tokio::test]
async fn set_requires_type_and_seconds() {
    let (session, turn) = make_session_and_context().await;
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let err = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "seconds": 60}),
    )
    .await
    .expect_err("missing type must fail");
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("Setting a timer requires type and seconds.".to_string())
    );

    let err = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "type": "once"}),
    )
    .await
    .expect_err("missing seconds must fail");
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("Setting a timer requires type and seconds.".to_string())
    );

    let err = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "type": "once", "seconds": 0}),
    )
    .await
    .expect_err("zero seconds must fail");
    assert!(matches!(err, FunctionCallError::RespondToModel(_)));

    let err = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "type": "once", "seconds": MAX_TIMER_SECONDS + 1}),
    )
    .await
    .expect_err("oversized seconds must fail");
    assert!(matches!(err, FunctionCallError::RespondToModel(_)));
}

#[tokio::test]
async fn rejects_unknown_fields_and_actions() {
    let (session, turn) = make_session_and_context().await;
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let err = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "status", "extra": true}),
    )
    .await
    .expect_err("unknown fields must fail");
    assert!(matches!(err, FunctionCallError::RespondToModel(_)));

    let err = invoke(Arc::clone(&session), Arc::clone(&turn), json!({"action": "fire"}))
        .await
        .expect_err("unknown action must fail");
    assert!(matches!(err, FunctionCallError::RespondToModel(_)));
}

#[tokio::test]
async fn subagents_cannot_set_timers() {
    let (session, mut turn) = make_session_and_context().await;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::Review);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    for arguments in [
        json!({"action": "set", "type": "once", "seconds": 60}),
        json!({"action": "cancel"}),
        json!({"action": "status"}),
    ] {
        let err = invoke(Arc::clone(&session), Arc::clone(&turn), arguments)
            .await
            .expect_err("subagent timer use must fail");
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "Timers are only available in primary sessions.".to_string()
            )
        );
    }
    assert_eq!(session.timer.get(), None);
}

/// A time provider whose reads always fail, matching the sibling clock tools'
/// fatal-error policy for a stalled clock.
struct FailingClock;

impl crate::current_time::TimeProvider for FailingClock {
    fn current_time(&self, _thread_id: ThreadId) -> crate::current_time::TimeFuture<'_> {
        Box::pin(async { Err(anyhow::anyhow!("test clock unavailable")) })
    }

    fn sleep(&self, _thread_id: ThreadId, _duration: std::time::Duration) -> crate::current_time::SleepFuture<'_> {
        Box::pin(async { Err(anyhow::anyhow!("test clock unavailable")) })
    }
}

#[tokio::test]
async fn clock_read_failure_is_fatal_and_schedules_nothing() {
    let (mut session, turn) = make_session_and_context().await;
    session.services.time_provider = Arc::new(FailingClock);
    let turn = Arc::new(turn);
    let session = Arc::new(session);

    let err = invoke(
        Arc::clone(&session),
        Arc::clone(&turn),
        json!({"action": "set", "type": "once", "seconds": 60}),
    )
    .await
    .expect_err("a stalled clock must fail the set");
    assert!(
        matches!(err, FunctionCallError::Fatal(_)),
        "clock failure should be fatal, got: {err:?}"
    );
    assert_eq!(session.timer.get(), None, "nothing may be scheduled");
}
