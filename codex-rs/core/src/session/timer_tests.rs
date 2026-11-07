use super::SessionTimer;
use super::TimerInfo;
use super::TimerKind;
use super::advance_interval;
use super::remaining_until;
use super::run_timer;
use crate::current_time::SleepFuture;
use crate::current_time::TimeFuture;
use crate::current_time::TimeProvider;
use crate::session::tests::make_session_and_context;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use codex_protocol::ThreadId;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio_util::sync::CancellationToken;

/// Clock source the tests can move manually instead of sleeping.
///
/// `current_time` reads the mutex-held value; `sleep` resolves on wall time
/// (this file's tests never wait on a real timer tick).
#[derive(Default)]
struct ManualClock {
    now: std::sync::Mutex<DateTime<Utc>>,
}

impl ManualClock {
    fn at(time: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self {
            now: std::sync::Mutex::new(time),
        })
    }
}

impl TimeProvider for ManualClock {
    fn current_time(&self, _thread_id: ThreadId) -> TimeFuture<'_> {
        let now = *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Box::pin(async move { Ok(now) })
    }

    fn sleep(&self, _thread_id: ThreadId, duration: StdDuration) -> SleepFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }
}

fn epoch(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().expect("valid time")
}

fn info(kind: TimerKind, seconds: u64, next_at: DateTime<Utc>) -> TimerInfo {
    TimerInfo {
        kind,
        seconds,
        prompt: None,
        next_at,
    }
}

#[test]
fn wake_body_uses_prompt_or_default() {
    let mut timed = info(TimerKind::Once, 5, epoch(0));
    timed.prompt = Some("check the build".to_string());
    assert_eq!(timed.wake_body(), "[Timer wake] check the build");

    let unnamed = info(TimerKind::Once, 5, epoch(0));
    assert_eq!(
        unnamed.wake_body(),
        "[Timer wake] Your session timer has fired. Continue any pending work or check whether action is needed."
    );
}

#[test]
fn interval_advance_anchors_to_schedule() {
    // Next wake in the future: cadence stays anchored to next_at.
    let scheduled = info(TimerKind::Interval, 60, epoch(1_000));
    assert_eq!(advance_interval(&scheduled, epoch(1_010)), epoch(1_060));

    // Wakes that land late skip missed periods instead of re-firing immediately.
    let late = info(TimerKind::Interval, 60, epoch(1_000));
    assert_eq!(advance_interval(&late, epoch(1_305)), epoch(1_360));
}

#[test]
fn interval_advance_handles_large_periods() {
    // u64 seconds beyond i64 must not panic or wrap.
    let huge = info(TimerKind::Interval, u64::MAX, epoch(0));
    assert_eq!(advance_interval(&huge, epoch(10)), epoch(0));
}

#[test]
fn interval_advance_clamps_zero_seconds() {
    // Defensive: the tool rejects zero, but the math must not divide by zero.
    // A zero-length period cannot advance, so the schedule stays put.
    let zero = info(TimerKind::Interval, 0, epoch(1_000));
    assert_eq!(advance_interval(&zero, epoch(2_000)), epoch(1_000));
}

#[tokio::test]
async fn remaining_until_clamps_past_deadlines() {
    let clock = ManualClock::at(epoch(1_000));
    let future = remaining_until(clock.as_ref(), ThreadId::default(), &epoch(1_500))
        .await
        .expect("clock available");
    assert_eq!(future, StdDuration::from_secs(500));

    let past = remaining_until(clock.as_ref(), ThreadId::default(), &epoch(500))
        .await
        .expect("clock available");
    assert_eq!(past, StdDuration::ZERO);
}

#[tokio::test]
async fn registry_starts_empty_and_cancel_is_idempotent() {
    let timer = SessionTimer::default();
    assert_eq!(timer.get(), None);
    assert_eq!(timer.cancel(), None);
}

#[tokio::test]
async fn install_replaces_previous_timer() {
    let timer = SessionTimer::default();
    timer.install(
        info(TimerKind::Once, 60, epoch(60)),
        /*generation*/ 0,
        tokio_util::sync::CancellationToken::new(),
    );
    let first = timer.get().expect("first timer installed");
    assert_eq!(first.kind, TimerKind::Once);

    timer.install(
        info(TimerKind::Interval, 30, epoch(30)),
        /*generation*/ 1,
        tokio_util::sync::CancellationToken::new(),
    );
    let second = timer.get().expect("replacement installed");
    assert_eq!(second.kind, TimerKind::Interval);
    assert_eq!(second.seconds, 30);

    let cancelled = timer.cancel().expect("cancel returns the timer");
    assert_eq!(cancelled, second);
    assert_eq!(timer.get(), None);
}

#[tokio::test]
async fn consume_or_advance_only_claims_current_generation() {
    let registry_timer = SessionTimer::default();
    registry_timer.install(
        info(TimerKind::Once, 60, epoch(60)),
        /*generation*/ 7,
        tokio_util::sync::CancellationToken::new(),
    );

    // A stale wake (older generation) must not disturb the current timer.
    let current = super::SessionTimer::consume_or_advance(
        &registry_timer.active,
        /*generation*/ 6,
        epoch(60),
    );
    assert_eq!(current, None);
    assert!(registry_timer.get().is_some());

    // The current generation's wake consumes a `once` timer.
    let current = super::SessionTimer::consume_or_advance(
        &registry_timer.active,
        /*generation*/ 7,
        epoch(60),
    );
    assert_eq!(current, Some(info(TimerKind::Once, 60, epoch(60))));
    assert_eq!(registry_timer.get(), None);
}

#[tokio::test]
async fn consume_or_advance_schedules_next_interval() {
    let registry_timer = SessionTimer::default();
    registry_timer.install(
        info(TimerKind::Interval, 60, epoch(1_000)),
        /*generation*/ 3,
        tokio_util::sync::CancellationToken::new(),
    );

    let current = super::SessionTimer::consume_or_advance(
        &registry_timer.active,
        /*generation*/ 3,
        epoch(1_090),
    );
    // The returned info carries the advanced schedule so the wake task and the
    // registry stay in sync: 90 seconds elapsed, the missed period is skipped,
    // and the next wake is at 1_120.
    assert_eq!(current, Some(info(TimerKind::Interval, 60, epoch(1_120))));
    assert_eq!(
        registry_timer.get(),
        Some(info(TimerKind::Interval, 60, epoch(1_120)))
    );
}

#[test]
fn timer_info_serializes_expected_shape() {
    let timed = TimerInfo {
        kind: TimerKind::Interval,
        seconds: 120,
        prompt: Some("stand up".to_string()),
        next_at: epoch(1_000),
    };
    let value: serde_json::Value = serde_json::to_value(&timed).expect("serializable");
    assert_eq!(value["type"], "interval");
    assert_eq!(value["seconds"], 120);
    assert_eq!(value["prompt"], "stand up");
    assert_eq!(
        value["next_at"],
        serde_json::to_value(&epoch(1_000)).expect("serializable datetime")
    );

    let unnamed = info(TimerKind::Once, 5, epoch(0));
    let value: serde_json::Value = serde_json::to_value(&unnamed).expect("serializable");
    assert!(value.get("prompt").is_none());
}

#[tokio::test]
async fn run_timer_returns_when_the_agent_tree_shuts_down() {
    /// Clock frozen in the future: the timer would sleep for hours without it.
    struct FutureClock;

    impl TimeProvider for FutureClock {
        fn current_time(&self, _thread_id: ThreadId) -> TimeFuture<'_> {
            Box::pin(async { Ok(epoch(1_000_000_000)) })
        }

        fn sleep(&self, _thread_id: ThreadId, _duration: StdDuration) -> SleepFuture<'_> {
            // Sleep long enough that only the shutdown branch can end the test.
            Box::pin(async {
                tokio::time::sleep(StdDuration::from_secs(60)).await;
                Ok(())
            })
        }
    }

    let (session, _turn) = make_session_and_context().await;
    let runtime = session.services.local_agent_runtime.clone();
    runtime.shutdown.cancel();
    let registry_timer = SessionTimer::default();
    registry_timer.install(
        info(TimerKind::Interval, 60, epoch(1_000_000_060)),
        /*generation*/ 0,
        CancellationToken::new(),
    );

    // The timer would sleep for a minute, but the tree shutdown branch must
    // win the select: the loop exits promptly without touching the registry.
    let task = tokio::spawn(run_timer(
        Arc::clone(&registry_timer.active),
        /*generation*/ 0,
        info(TimerKind::Interval, 60, epoch(1_000_000_060)),
        ThreadId::default(),
        runtime,
        Arc::new(FutureClock),
        CancellationToken::new(),
    ));

    tokio::time::timeout(StdDuration::from_secs(5), task)
        .await
        .expect("run_timer must exit promptly on tree shutdown")
        .expect("wake task should not panic");
    // Shutdown exits before touching the registry; ownership belongs to the
    // session teardown's explicit cancel.
    assert!(registry_timer.get().is_some());
}

#[tokio::test]
async fn run_timer_removes_the_timer_after_repeated_wake_failures() {
    let clock = ManualClock::default();
    let (session, _turn) = make_session_and_context().await;
    // Every wake fails at the manager upgrade because this test session's
    // thread is not registered with a live manager.
    let runtime = session.services.local_agent_runtime.clone();
    let registry_timer = SessionTimer::default();
    registry_timer.install(
        info(TimerKind::Interval, 0, epoch(0)),
        /*generation*/ 0,
        CancellationToken::new(),
    );

    // seconds == 0 keeps `next_at` in the past, so the loop performs its three
    // wake attempts back-to-back without sleeping between them.
    let task = tokio::spawn(run_timer(
        Arc::clone(&registry_timer.active),
        /*generation*/ 0,
        info(TimerKind::Interval, 0, epoch(0)),
        session.thread_id,
        runtime,
        Arc::new(clock),
        CancellationToken::new(),
    ));

    tokio::time::timeout(StdDuration::from_secs(5), task)
        .await
        .expect("run_timer must exit after the failure budget is exhausted")
        .expect("wake task should not panic");
    assert_eq!(
        registry_timer.get(),
        None,
        "repeated wake failures must remove the timer"
    );
}

