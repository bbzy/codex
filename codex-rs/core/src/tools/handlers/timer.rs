use std::collections::BTreeMap;
use std::sync::Arc;

use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolExposure;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde_json::json;

use crate::function_tool::FunctionCallError;
use crate::session::timer::TimerInfo;
use crate::session::timer::TimerKind;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;

#[cfg(test)]
#[path = "timer_tests.rs"]
mod tests;

const TOOL_NAME: &str = "timer";
const MAX_TIMER_SECONDS: u64 = 30 * 24 * 60 * 60;

pub struct TimerHandler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerArgs {
    action: TimerAction,
    #[serde(rename = "type")]
    timer_type: Option<TimerKind>,
    seconds: Option<u64>,
    prompt: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TimerAction {
    Set,
    Cancel,
    Status,
}

fn create_timer_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "action".to_string(),
            JsonSchema::string_enum(
                vec![json!("set"), json!("cancel"), json!("status")],
                Some("The timer operation to perform.".to_string()),
            ),
        ),
        (
            "type".to_string(),
            JsonSchema::string_enum(
                vec![json!("once"), json!("interval")],
                Some(
                    "Required for `set`: `once` fires a single time and `interval` repeats.".to_string(),
                ),
            ),
        ),
        (
            "seconds".to_string(),
            JsonSchema::number(Some(format!(
                "Required for `set`: delay in seconds. Must be an integer between 1 and {MAX_TIMER_SECONDS}."
            ))),
        ),
        (
            "prompt".to_string(),
            JsonSchema::string(Some(
                "Optional reminder delivered to this session when the timer fires.".to_string(),
            )),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: TOOL_NAME.to_string(),
        description:
            "Set, cancel, or inspect this session's wake timer. Set requires type (once or interval) and positive integer seconds; prompt is an optional reminder. Each session owns at most one timer; setting replaces it. Once is consumed before waking. Busy sessions skip the occurrence (once is consumed too). Interval maintains its cadence without accumulating missed wakes. Timers survive turn ends but not session shutdown or process exit. Subagents cannot use timers."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["action".to_string()]),
            /*additional_properties*/ Some(false.into()),
        ),
        output_schema: None,
    })
}

impl ToolExecutor<ToolInvocation> for TimerHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_timer_tool()
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::DirectModelOnly
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let ToolInvocation {
                session,
                payload,
                turn,
                ..
            } = invocation;
            let ToolPayload::Function { arguments } = payload else {
                return Err(FunctionCallError::RespondToModel(format!(
                    "{TOOL_NAME} handler received unsupported payload"
                )));
            };
            if turn.session_source.is_non_root_agent() {
                return Err(FunctionCallError::RespondToModel(
                    "Timers are only available in primary sessions.".to_string(),
                ));
            }
            let args: TimerArgs = parse_arguments(&arguments)?;
            let mut reported = session.timer.get();
            match args.action {
                TimerAction::Set => {
                    let (Some(timer_type), Some(seconds)) = (args.timer_type, args.seconds) else {
                        return Err(FunctionCallError::RespondToModel(
                            "Setting a timer requires type and seconds.".to_string(),
                        ));
                    };
                    if seconds == 0 || seconds > MAX_TIMER_SECONDS {
                        return Err(FunctionCallError::RespondToModel(format!(
                            "seconds must be between 1 and {MAX_TIMER_SECONDS}"
                        )));
                    }
                    let period = chrono::Duration::try_seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
                        .ok_or_else(|| {
                            FunctionCallError::RespondToModel(
                                "seconds is too large to schedule".to_string(),
                            )
                        })?;
                    let now = session
                        .services
                        .time_provider
                        .current_time(session.thread_id)
                        .await
                        .map_err(|error| {
                            FunctionCallError::Fatal(format!(
                                "failed to read the current time for the timer tool: {error:#}"
                            ))
                        })?;
                    let info = TimerInfo {
                        kind: timer_type,
                        seconds,
                        prompt: args.prompt,
                        next_at: now + period,
                    };
                    session.timer.set(
                        info.clone(),
                        session.thread_id,
                        session.services.local_agent_runtime.clone(),
                        Arc::clone(&session.services.time_provider),
                    );
                    reported = Some(info);
                }
                TimerAction::Cancel => {
                    reported = session.timer.cancel();
                }
                TimerAction::Status => {}
            }
            status_output(reported)
        })
    }
}

impl CoreToolRuntime for TimerHandler {
    fn is_builtin_control_tool(&self) -> bool {
        true
    }
}

fn status_output(
    timer: Option<TimerInfo>,
) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let text = serde_json::to_string(&timer).map_err(|error| {
        FunctionCallError::RespondToModel(format!("failed to serialize timer status: {error}"))
    })?;
    Ok(boxed_tool_output(FunctionToolOutput::from_text(
        text,
        /*success*/ Some(true),
    )))
}
