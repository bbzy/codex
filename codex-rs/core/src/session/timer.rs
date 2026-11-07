//! Per-session wake timers.
//!
//! A session owns at most one timer. The agent sets it with the `timer` tool;
//! when a timer fires, the runtime starts a regular turn with a hidden timer
//! context fragment when the thread is idle and skips the occurrence otherwise.
//! Timers survive turn boundaries but not session shutdown or process exit.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use codex_context_fragments::ContextualUserFragment;
use codex_protocol::ThreadId;
use codex_protocol::models::ResponseItem;
use codex_protocol::turn_input::StartIfIdleSubmission;
use codex_protocol::turn_input::TurnInput;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnStartOptions;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::agent::control::LocalAgentRuntime;
use crate::context::InternalContextSource;
use crate::context::InternalModelContextFragment;
use crate::current_time::TimeProvider;

pub(crate) const TIMER_INTERNAL_CONTEXT_SOURCE: &str = "timer";
const TIMER_WAKE_PREFIX: &str = "[Timer wake]";
const DEFAULT_TIMER_PROMPT: &str =
    "Your session timer has fired. Continue any pending work or check whether action is needed.";

/// How a timer repeats after it fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TimerKind {
    /// Consumed by the first occurrence, whether or not the session was busy.
    Once,
    /// Keeps firing on its cadence without accumulating missed wakes.
    Interval,
}

/// The current timer state reported by the `timer` tool.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct TimerInfo {
    #[serde(rename = "type")]
    pub(crate) kind: TimerKind,
    pub(crate) seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prompt: Option<String>,
    /// Absolute next wake time on the session's time provider.
    pub(crate) next_at: DateTime<Utc>,
}

impl TimerInfo {
    /// The hidden context body delivered to the model when the timer fires.
    pub(crate) fn wake_body(&self) -> String {
        format!(
            "{TIMER_WAKE_PREFIX} {}",
            self.prompt.as_deref().unwrap_or(DEFAULT_TIMER_PROMPT)
        )
    }
}

struct ActiveTimer {
    info: TimerInfo,
    generation: u64,
    cancel: CancellationToken,
}

/// Registry for the session's at-most-one wake timer.
///
/// Clones share the same registry; the wake task only holds weak thread-manager
/// handles and never extends the session's lifetime.
#[derive(Clone, Default)]
pub(crate) struct SessionTimer {
    active: Arc<Mutex<Option<ActiveTimer>>>,
    next_generation: Arc<AtomicU64>,
}

impl SessionTimer {
    /// Returns the current timer, or `None` when no timer is set.
    pub(crate) fn get(&self) -> Option<TimerInfo> {
        Self::lock(&self.active)
            .as_ref()
            .map(|active| active.info.clone())
    }

    /// Cancels the current timer and returns it, or `None` when no timer is set.
    pub(crate) fn cancel(&self) -> Option<TimerInfo> {
        let mut guard = Self::lock(&self.active);
        let active = guard.take();
        if let Some(active) = &active {
            active.cancel.cancel();
        }
        active.map(|active| active.info)
    }

    /// Replaces any existing timer with `info` and spawns its wake task.
    pub(crate) fn set(
        &self,
        info: TimerInfo,
        thread_id: ThreadId,
        runtime: LocalAgentRuntime,
        time_provider: Arc<dyn TimeProvider>,
    ) {
        let cancel = CancellationToken::new();
        let generation = self.next_generation.fetch_add(/*val*/ 1, Ordering::SeqCst);
        self.install(info.clone(), generation, cancel.clone());
        let registry = Arc::clone(&self.active);
        tokio::spawn(run_timer(
            registry,
            generation,
            info,
            thread_id,
            runtime,
            time_provider,
            cancel,
        ));
    }

    /// Registers `info` as the current timer, cancelling any previous one.
    fn install(&self, info: TimerInfo, generation: u64, cancel: CancellationToken) {
        let mut active = Self::lock(&self.active);
        let previous = active.replace(ActiveTimer {
            info,
            generation,
            cancel,
        });
        if let Some(previous) = previous {
            previous.cancel.cancel();
        }
    }

    fn lock(active: &Mutex<Option<ActiveTimer>>) -> MutexGuard<'_, Option<ActiveTimer>> {
        active.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn remove_if_current(registry: &Mutex<Option<ActiveTimer>>, generation: u64) {
        let mut active = Self::lock(registry);
        if active
            .as_ref()
            .is_some_and(|timer| timer.generation == generation)
        {
            *active = None;
        }
    }

    /// Claims the timer for this wake when it is still current: `Once` timers
    /// are removed and `Interval` timers advance to their next occurrence.
    ///
    /// Consuming happens before the wake so an agent that installs a
    /// replacement timer during the wake cannot race the registry state.
    fn consume_or_advance(
        registry: &Mutex<Option<ActiveTimer>>,
        generation: u64,
        now: DateTime<Utc>,
    ) -> Option<TimerInfo> {
        let mut active = Self::lock(registry);
        let timer = active
            .as_ref()
            .filter(|timer| timer.generation == generation)?;
        let mut current = timer.info.clone();
        if current.kind == TimerKind::Once {
            *active = None;
        } else if let Some(timer) = active.as_mut() {
            current.next_at = advance_interval(&current, now);
            timer.info.next_at = current.next_at;
        }
        Some(current)
    }
}

async fn run_timer(
    registry: Arc<Mutex<Option<ActiveTimer>>>,
    generation: u64,
    info: TimerInfo,
    thread_id: ThreadId,
    runtime: LocalAgentRuntime,
    time_provider: Arc<dyn TimeProvider>,
    cancel: CancellationToken,
) {
    const MAX_CONSECUTIVE_WAKE_FAILURES: u32 = 3;
    let mut info = info;
    let mut consecutive_wake_failures: u32 = 0;
    loop {
        let Some(remaining) = remaining_until(&*time_provider, thread_id, &info.next_at).await
        else {
            // A timer that cannot read time must not spin or wake spuriously.
            SessionTimer::remove_if_current(&registry, generation);
            return;
        };
        if !remaining.is_zero() {
            let sleep = time_provider.sleep(thread_id, remaining);
            tokio::pin!(sleep);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                // Stop when the agent tree shuts down even if this session's
                // cancel token is never fired.
                _ = runtime.shutdown.cancelled() => return,
                result = &mut sleep => {
                    if result.is_err() {
                        SessionTimer::remove_if_current(&registry, generation);
                        return;
                    }
                }
            }
            continue;
        }

        let Some(now) = time_provider.current_time(thread_id).await.ok() else {
            SessionTimer::remove_if_current(&registry, generation);
            return;
        };
        let Some(current) = SessionTimer::consume_or_advance(&registry, generation, now) else {
            return;
        };
        if wake_thread(thread_id, &runtime, &current).await.is_err() {
            consecutive_wake_failures += 1;
            if consecutive_wake_failures >= MAX_CONSECUTIVE_WAKE_FAILURES {
                tracing::warn!(thread_id = %thread_id, failures = %consecutive_wake_failures, "timer wake kept failing; removing the timer");
                SessionTimer::remove_if_current(&registry, generation);
                return;
            }
        } else {
            consecutive_wake_failures = 0;
        }
        if current.kind == TimerKind::Once {
            return;
        }
        info = current;
    }
}

/// Computes the next interval occurrence at or after `now`, keeping the cadence
/// anchored to the schedule instead of accumulating missed wakes.
fn advance_interval(info: &TimerInfo, now: DateTime<Utc>) -> DateTime<Utc> {
    let Some(period) = period_duration(info.seconds) else {
        return info.next_at;
    };
    let elapsed = now.signed_duration_since(info.next_at);
    if elapsed <= chrono::Duration::zero() {
        return info.next_at + period;
    }
    // Whole periods missed plus one, so the wake never lands in the past.
    let period_seconds = i64::try_from(info.seconds).unwrap_or(i64::MAX).max(1);
    let periods_behind = elapsed.num_seconds() / period_seconds + 1;
    info.next_at + period * i32::try_from(periods_behind).unwrap_or(i32::MAX)
}

fn period_duration(seconds: u64) -> Option<chrono::Duration> {
    let seconds = i64::try_from(seconds).ok()?;
    chrono::Duration::try_seconds(seconds)
}

/// Returns the wait duration until `next_at`, or `None` when the clock is
/// unavailable. Returns a zero duration once the deadline has passed.
async fn remaining_until(
    time_provider: &dyn TimeProvider,
    thread_id: ThreadId,
    next_at: &DateTime<Utc>,
) -> Option<Duration> {
    let now = time_provider.current_time(thread_id).await.ok()?;
    Some(
        next_at
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::ZERO),
    )
}

/// Starts a regular turn with the timer's context fragment when the thread is
/// idle; busy threads skip the occurrence.
async fn wake_thread(
    thread_id: ThreadId,
    runtime: &LocalAgentRuntime,
    info: &TimerInfo,
) -> anyhow::Result<()> {
    let thread_manager = runtime
        .upgrade_manager()
        .ok_or_else(|| anyhow::anyhow!("thread manager dropped"))?;
    let thread = thread_manager.get_thread(thread_id).await?;
    let start_options = thread
        .thread_extension_data()
        .get::<TurnStartOptions>()
        .map(|options| options.as_ref().clone())
        .unwrap_or_default();
    let item: ResponseItem = ContextualUserFragment::into(InternalModelContextFragment::new(
        InternalContextSource::from_static(TIMER_INTERNAL_CONTEXT_SOURCE),
        info.wake_body(),
    ));
    match thread
        .start_turn_if_idle(
            TurnInputRequest::new(TurnInput::ResponseItem(item)).on_start(TurnStartOptions {
                turn_trigger: Some(TIMER_INTERNAL_CONTEXT_SOURCE.to_string()),
                ..start_options
            }),
        )
        .await?
    {
        StartIfIdleSubmission::Started { turn_id, .. } => {
            tracing::debug!(thread_id = %thread_id, %turn_id, "timer wake started a turn");
        }
        StartIfIdleSubmission::NotSubmitted { reason } => {
            // The occurrence is skipped (and a `once` timer stays consumed),
            // so a dropped wake must stay diagnosable.
            tracing::debug!(thread_id = %thread_id, ?reason, "timer wake skipped because the thread was not idle");
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "timer_tests.rs"]
mod tests;
