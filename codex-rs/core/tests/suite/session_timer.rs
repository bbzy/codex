//! Session timer tool: idle wake, busy-skip, and cancel behavior.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use codex_core::SleepFuture;
use codex_core::TimeFuture;
use codex_core::TimeProvider;
use codex_core::TurnInputRequest;
use codex_core::config::CurrentTimeReminderConfig;
use codex_features::CurrentTimeSource;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ResponsesRequest;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::received_responses_requests;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::test_codex::test_codex;
use core_test_support::test_codex::TestCodex;
use core_test_support::wait_for_event;
use core_test_support::wait_for_event_match;
use serde_json::Value;
use serde_json::json;
use tokio::sync::watch;

const BASE_TIME_SECONDS: i64 = 1_781_717_655;

/// A clock the test drives manually; sleep resolves only once the test has
/// advanced the clock past the requested deadline.
struct ManualClock {
    now: watch::Sender<i64>,
}

impl ManualClock {
    fn new() -> Arc<Self> {
        let (now, _) = watch::channel(BASE_TIME_SECONDS);
        Arc::new(Self { now })
    }

    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(*self.now.borrow(), 0).expect("valid test timestamp")
    }

    fn advance_seconds(&self, seconds: i64) {
        self.now.send_modify(|current| *current += seconds);
    }
}

impl TimeProvider for ManualClock {
    fn current_time(&self, _thread_id: ThreadId) -> TimeFuture<'_> {
        let now = self.now();
        Box::pin(async move { Ok(now) })
    }

    fn sleep(&self, _thread_id: ThreadId, duration: Duration) -> SleepFuture<'_> {
        let mut rx = self.now.subscribe();
        let deadline = self.now() + chrono::Duration::from_std(duration).expect("test duration");
        Box::pin(async move {
            // `watch` stores the newest value, so a clock advance that lands
            // before this loop starts still resolves the wait.
            while *rx.borrow_and_update() < deadline.timestamp() {
                rx.changed().await.map_err(|error| anyhow::anyhow!(error))?;
            }
            Ok(())
        })
    }
}

fn timer_wake_turns(requests: &[ResponsesRequest]) -> Vec<&ResponsesRequest> {
    requests
        .iter()
        .filter(|request| {
            request
                .message_input_texts("user")
                .iter()
                .any(|text| text.contains("[Timer wake]"))
        })
        .collect()
}

async fn wait_for_turn_complete(test: &TestCodex) {
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn once_timer_wakes_an_idle_session() -> Result<()> {
    let server = start_mock_server().await;
    let requests = mount_sse_sequence(
        &server,
        vec![
            // Turn 1: the model sets a once timer with a prompt.
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "set-timer",
                    "timer",
                    &json!({"action": "set", "type": "once", "seconds": 60, "prompt": "check the build"})
                        .to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![ev_response_created("resp-1-final"), ev_completed("resp-1-final")]),
            // Turn 2: the timer fires while the session is idle.
            sse(vec![ev_response_created("resp-2"), ev_completed("resp-2")]),
        ],
    )
    .await;

    let clock = ManualClock::new();
    let test = test_codex()
        .with_config(|config| {
            config.include_environment_context = false;
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(Arc::clone(&clock) as Arc<dyn TimeProvider>)
        .build_with_auto_env(&server)
        .await?;

    test.submit_turn("first turn").await?;

    // Advance the clock past the deadline: the timer must wake the session.
    clock.advance_seconds(120);
    wait_for_turn_complete(&test).await;

    let captured = requests.requests();
    let wakes = timer_wake_turns(&captured);
    assert_eq!(wakes.len(), 1, "exactly one timer wake turn expected");
    let texts = wakes[0].message_input_texts("user");
    assert!(
        texts
            .iter()
            .any(|text| text.contains("[Timer wake] check the build")),
        "wake turn should contain the timer prompt, got: {texts:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_session_skips_the_occurrence_and_stays_alive() -> Result<()> {
    // Uses an `interval` timer: the skipped occurrence is observable because
    // the timer must still fire on its next tick after the busy turn ends. A
    // `once` timer leaves no post-hoc trace of being consumed.
    let server = start_mock_server().await;
    let requests = mount_sse_sequence(
        &server,
        vec![
            // Turn 1: set an interval timer.
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "set-timer",
                    "timer",
                    &json!({"action": "set", "type": "interval", "seconds": 60}).to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![ev_response_created("resp-1-final"), ev_completed("resp-1-final")]),
            // Turn 2: the busy turn that covers the first deadline.
            sse(vec![ev_response_created("resp-2"), ev_completed("resp-2")]),
            // Turn 3: the wake after the busy turn ends.
            sse(vec![ev_response_created("resp-3"), ev_completed("resp-3")]),
        ],
    )
    .await;

    let clock = ManualClock::new();
    let test = test_codex()
        .with_config(|config| {
            config.include_environment_context = false;
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(Arc::clone(&clock) as Arc<dyn TimeProvider>)
        .build_with_auto_env(&server)
        .await?;

    test.submit_turn("first turn").await?;

    // Start the busy turn and observe its first model request before advancing
    // the clock, so the wake deterministically lands while the turn is active.
    let busy = tokio::spawn({
        let codex = test.codex.clone();
        async move {
            codex
                .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
                    text: "start long work".to_string(),
                    text_elements: Vec::new(),
                }]))
                .await
        }
    });
    let busy_started = wait_for_event_match(&test.codex, |event| match event {
        EventMsg::ItemStarted(started) => match &started.item {
            TurnItem::UserMessage(message) if message.content.len() == 1 => {
                Some(started.item.clone())
            }
            _ => None,
        },
        _ => None,
    })
    .await;
    let TurnItem::UserMessage(user_message) = busy_started else {
        panic!("expected a user message item");
    };
    assert!(
        matches!(&user_message.content[0], UserInput::Text { text, .. } if text == "start long work"),
        "the busy turn's user message should be observed before the wake"
    );

    // The deadline passes while the turn is active: the occurrence is skipped.
    clock.advance_seconds(60);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let captured_after_busy_start = requests.requests().len();

    // `start_or_steer_turn` only waits for the routing decision, so the busy
    // turn is still running here. Wait for its completion before advancing
    // the clock again, or the second occurrence would also land while busy.
    busy.await.expect("busy turn task")?;
    wait_for_turn_complete(&test).await;

    // The interval timer survives the skipped occurrence and fires on the next
    // tick once the session is idle again.
    clock.advance_seconds(60);
    wait_for_turn_complete(&test).await;

    let captured = requests.requests();
    // No wake may appear while the busy turn was running.
    for request in &captured[..captured_after_busy_start] {
        assert!(
            !request
                .message_input_texts("user")
                .iter()
                .any(|text| text.contains("[Timer wake]")),
            "no wake may fire while the session is busy"
        );
    }
    let wakes = timer_wake_turns(&captured);
    assert_eq!(wakes.len(), 1, "the timer must wake once after the busy turn");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interval_timer_fires_repeatedly_and_cancel_stops_it() -> Result<()> {
    let server = start_mock_server().await;
    let requests = mount_sse_sequence(
        &server,
        vec![
            // Turn 1: set an interval timer.
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "set-timer",
                    "timer",
                    &json!({"action": "set", "type": "interval", "seconds": 60}).to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![ev_response_created("resp-1-final"), ev_completed("resp-1-final")]),
            // Turn 2: first wake.
            sse(vec![ev_response_created("resp-2"), ev_completed("resp-2")]),
            // Turn 3: second wake.
            sse(vec![ev_response_created("resp-3"), ev_completed("resp-3")]),
            // Turn 4: third wake.
            sse(vec![ev_response_created("resp-3b"), ev_completed("resp-3b")]),
            // Turn 5: cancel the timer.
            sse(vec![
                ev_response_created("resp-5"),
                ev_function_call("cancel-timer", "timer", &json!({"action": "cancel"}).to_string()),
                ev_completed("resp-5"),
            ]),
            sse(vec![ev_response_created("resp-5-final"), ev_completed("resp-5-final")]),
        ],
    )
    .await;

    let clock = ManualClock::new();
    let test = test_codex()
        .with_config(|config| {
            config.include_environment_context = false;
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(Arc::clone(&clock) as Arc<dyn TimeProvider>)
        .build_with_auto_env(&server)
        .await?;

    test.submit_turn("set the timer").await?;

    // First wake at +60s.
    clock.advance_seconds(60);
    wait_for_turn_complete(&test).await;

    // Second wake: another full period keeps the cadence anchored.
    clock.advance_seconds(60);
    wait_for_turn_complete(&test).await;

    // Third wake: the interval timer keeps firing until cancelled.
    clock.advance_seconds(60);
    wait_for_turn_complete(&test).await;

    // Cancel stops future wakes.
    let requests_before_cancel = requests.requests().len();
    test.submit_turn("cancel the timer").await?;
    clock.advance_seconds(600);
    // No wake turns for a while: give any erroneous wake a chance to appear.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // History replays earlier wake fragments in later requests, so counting
    // matching requests overcounts. The server-level count includes requests
    // the exhausted mock no longer matches, so an erroneous wake after
    // cancellation is visible here even as a 404.
    let server_requests_after = received_responses_requests(&server).await;
    assert_eq!(
        server_requests_after.len(),
        requests_before_cancel + 2,
        "cancel turn should add exactly its two scripted requests; no wake turns after cancellation"
    );
    // History replays the earlier `set-timer` call in every later request, so
    // find the cancel call by its arguments instead of call id or position.
    let has_cancel_call = server_requests_after.iter().any(|request| {
        request.inputs_of_type("function_call").iter().any(|item| {
            item.get("name").and_then(Value::as_str) == Some("timer")
                && item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|arguments| serde_json::from_str::<Value>(arguments).ok())
                    .and_then(|arguments| {
                        arguments.get("action").and_then(Value::as_str).map(str::to_owned)
                    })
                    .as_deref()
                    == Some("cancel")
        })
    });
    assert!(
        has_cancel_call,
        "cancel turn request should carry the timer cancel call"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_once_timer_never_wakes() -> Result<()> {
    let server = start_mock_server().await;
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "set-timer",
                    "timer",
                    &json!({"action": "set", "type": "once", "seconds": 30}).to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![ev_response_created("resp-1-final"), ev_completed("resp-1-final")]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_function_call("cancel-timer", "timer", &json!({"action": "cancel"}).to_string()),
                ev_completed("resp-2"),
            ]),
            sse(vec![ev_response_created("resp-2-final"), ev_completed("resp-2-final")]),
        ],
    )
    .await;

    let clock = ManualClock::new();
    let test = test_codex()
        .with_config(|config| {
            config.include_environment_context = false;
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(Arc::clone(&clock) as Arc<dyn TimeProvider>)
        .build_with_auto_env(&server)
        .await?;

    test.submit_turn("set the timer").await?;
    let requests_before_cancel = received_responses_requests(&server).await.len();
    test.submit_turn("cancel the timer").await?;

    // Past the original deadline nothing wakes.
    clock.advance_seconds(300);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The server-level count includes requests the exhausted mock no longer
    // matches, so an erroneous wake is visible even as a 404.
    let server_requests = received_responses_requests(&server).await;
    assert_eq!(
        server_requests.len(),
        requests_before_cancel + 2,
        "the cancel turn should add exactly its two scripted requests; no wake after cancellation"
    );

    Ok(())
}
