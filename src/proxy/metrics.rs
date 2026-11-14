use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

#[derive(Default)]
struct TokenUsage {
    total: Option<u64>,
    input: Option<u64>,
    output: Option<u64>,
    reasoning: Option<u64>,
    cached: Option<u64>,
}

impl TokenUsage {
    fn from_response(response: &Value) -> Self {
        let count = |path| response.pointer(path).and_then(Value::as_u64);
        Self {
            total: count("/usage/total_tokens"),
            input: count("/usage/input_tokens"),
            output: count("/usage/output_tokens"),
            reasoning: count("/usage/output_tokens_details/reasoning_tokens"),
            cached: count("/usage/input_tokens_details/cached_tokens"),
        }
    }
}

/// One request's upstream metrics. Observe events before forwarding them, and
/// keep this guard alive until its response body finishes or is dropped.
/// Logging happens once on Drop, including failures and client disconnects.
pub(super) struct RequestMetrics {
    logs: broadcast::Sender<String>,
    route: &'static str,
    model: String,
    started: Instant,
    first_output: Option<Duration>,
    finished: Option<Duration>,
    status: &'static str,
    usage: TokenUsage,
}

impl RequestMetrics {
    pub(super) fn new(logs: broadcast::Sender<String>, route: &'static str, model: &str) -> Self {
        Self {
            logs,
            route,
            model: model.chars().filter(|c| !c.is_control()).collect(),
            started: Instant::now(),
            first_output: None,
            finished: None,
            status: "failed",
            usage: TokenUsage::default(),
        }
    }

    pub(super) fn observe(&mut self, event: &Value) {
        self.observe_at(event, self.started.elapsed());
    }

    /// Override an upstream terminal status if response validation failed.
    /// Any token counts reported by that terminal response remain available.
    pub(super) fn fail(&mut self) {
        self.status = "failed";
        if self.finished.is_none() {
            self.finished = Some(self.started.elapsed());
        }
    }

    fn observe_at(&mut self, event: &Value, elapsed: Duration) {
        if self.finished.is_some() {
            return;
        }
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            return;
        };
        let has_output = match kind {
            "response.output_text.delta"
            | "response.reasoning_text.delta"
            | "response.reasoning_summary_text.delta"
            | "response.function_call_arguments.delta" => event
                .get("delta")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty()),
            "response.output_item.added" => {
                event.pointer("/item/type").and_then(Value::as_str) == Some("function_call")
            }
            _ => false,
        };
        if has_output && self.first_output.is_none() {
            self.first_output = Some(elapsed);
        }
        self.status = match kind {
            "response.completed" => "completed",
            "response.incomplete" => "incomplete",
            "response.failed" | "error" => "failed",
            _ => return,
        };
        self.finished = Some(elapsed);
        // Token counts come only from the actual terminal response. A missing
        // field remains unknown, including total_tokens; it is never estimated.
        if let Some(response) = event.get("response") {
            self.usage = TokenUsage::from_response(response);
        }
    }

    fn summary(&self, elapsed: Duration) -> String {
        let duration = self.finished.unwrap_or(elapsed);
        let total_seconds = duration.as_secs_f64();
        let ttft = self
            .first_output
            .map(|first| format!("{}ms", first.as_millis()))
            .unwrap_or_else(|| "unknown".into());
        let decode = self
            .first_output
            .map(|first| duration.saturating_sub(first))
            .unwrap_or(duration);
        // Match the reference's decode-speed calculation. Very short decode
        // intervals fall back to total duration to avoid a misleading spike.
        let effective_seconds = if decode >= Duration::from_millis(200) {
            decode.as_secs_f64()
        } else {
            total_seconds
        };
        let speed = match self.usage.output {
            Some(tokens) if effective_seconds > 0.05 => {
                format!("{:.1} tok/s", tokens as f64 / effective_seconds)
            }
            _ => "unknown".into(),
        };
        let cache_hit = match (self.usage.cached, self.usage.input) {
            (Some(cached), Some(input)) if input > 0 => {
                format!("{:.1}%", cached as f64 / input as f64 * 100.0)
            }
            _ => "unknown".into(),
        };
        let count = |value: Option<u64>| {
            value
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into())
        };
        format!(
            "[METRICS] Model: {} | Status: {} | Total Tokens: {} (Prompt: {}, Output: {} [Thinking: {}], Cache: {}) | Duration: {:.2}s | TTFT: {} | Speed: {} | Cache Hit: {} | Route: {}",
            self.model,
            self.status,
            count(self.usage.total),
            count(self.usage.input),
            count(self.usage.output),
            count(self.usage.reasoning),
            count(self.usage.cached),
            total_seconds,
            ttft,
            speed,
            cache_hit,
            self.route,
        )
    }
}

impl Drop for RequestMetrics {
    fn drop(&mut self) {
        let _ = self.logs.send(self.summary(self.started.elapsed()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metrics() -> RequestMetrics {
        let (logs, _) = broadcast::channel(4);
        RequestMetrics::new(logs, "chat", "test-model")
    }

    #[test]
    fn terminal_usage_and_real_output_define_metrics() {
        let mut metrics = metrics();
        metrics.observe_at(
            &json!({"type":"response.created"}),
            Duration::from_millis(20),
        );
        metrics.observe_at(
            &json!({"type":"response.output_item.added","item":{"type":"message"}}),
            Duration::from_millis(50),
        );
        metrics.observe_at(
            &json!({"type":"response.output_text.delta","delta":""}),
            Duration::from_millis(75),
        );
        metrics.observe_at(
            &json!({"type":"response.reasoning_summary_text.delta","delta":"thinking"}),
            Duration::from_millis(100),
        );
        metrics.observe_at(
            &json!({"type":"response.completed","response":{"usage":{
                "total_tokens":150,"input_tokens":100,"output_tokens":50,
                "input_tokens_details":{"cached_tokens":60},
                "output_tokens_details":{"reasoning_tokens":20}
            }}}),
            Duration::from_millis(1100),
        );
        let log = metrics.summary(Duration::from_secs(3));
        assert!(log.contains("Status: completed"), "{log}");
        assert!(
            log.contains("Total Tokens: 150 (Prompt: 100, Output: 50 [Thinking: 20], Cache: 60)"),
            "{log}"
        );
        assert!(
            log.contains("Duration: 1.10s | TTFT: 100ms | Speed: 50.0 tok/s | Cache Hit: 60.0%"),
            "{log}"
        );
    }

    #[test]
    fn missing_usage_is_unknown_and_disconnect_is_failed() {
        let metrics = metrics();
        let log = metrics.summary(Duration::from_secs(2));
        assert!(log.contains("Status: failed"), "{log}");
        assert!(log.contains("Total Tokens: unknown (Prompt: unknown, Output: unknown [Thinking: unknown], Cache: unknown)"), "{log}");
        assert!(
            log.contains("TTFT: unknown | Speed: unknown | Cache Hit: unknown"),
            "{log}"
        );
    }

    #[test]
    fn terminal_counts_are_not_inferred_and_short_decode_uses_total_duration() {
        let mut metrics = metrics();
        metrics.observe_at(
            &json!({"type":"response.output_item.added","item":{"type":"function_call"}}),
            Duration::from_millis(990),
        );
        metrics.observe_at(
            &json!({"type":"response.incomplete","response":{"usage":{"input_tokens":10,"output_tokens":2}}}),
            Duration::from_secs(1),
        );
        let log = metrics.summary(Duration::from_secs(1));
        assert!(log.contains("Status: incomplete | Total Tokens: unknown (Prompt: 10, Output: 2 [Thinking: unknown], Cache: unknown)"), "{log}");
        assert!(
            log.contains("TTFT: 990ms | Speed: 2.0 tok/s | Cache Hit: unknown"),
            "{log}"
        );
    }

    #[test]
    fn failed_terminal_keeps_actual_usage_and_drop_logs_once_without_sensitive_payload() {
        let (logs, mut receiver) = broadcast::channel(4);
        let mut metrics = RequestMetrics::new(logs, "responses", "test\n\u{1b}[31m-model");
        metrics.observe(
            &json!({"type":"response.function_call_arguments.delta","delta":"secret-tool-input"}),
        );
        metrics.observe(&json!({"type":"response.failed","response":{
            "usage":{"total_tokens":3,"input_tokens":3,"output_tokens":0},
            "error":{"message":"secret-error-payload"}
        }}));
        drop(metrics);
        let log = receiver.try_recv().unwrap();
        assert!(log.contains("Status: failed | Total Tokens: 3"), "{log}");
        assert!(!log.contains("secret-tool-input"));
        assert!(!log.contains("secret-error-payload"));
        assert!(!log.contains('\n'));
        assert!(!log.contains('\u{1b}'));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn protocol_failure_overrides_completed_without_losing_real_usage() {
        let mut metrics = metrics();
        metrics.observe_at(
            &json!({"type":"response.completed","response":{"usage":{"total_tokens":7,"input_tokens":5,"output_tokens":2}}}),
            Duration::from_secs(1),
        );
        metrics.fail();
        let log = metrics.summary(Duration::from_secs(2));
        assert!(
            log.contains("Status: failed | Total Tokens: 7 (Prompt: 5, Output: 2"),
            "{log}"
        );
    }
}
