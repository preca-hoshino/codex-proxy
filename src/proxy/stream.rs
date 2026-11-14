//! Project Codex Responses events into Chat Completions output.
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

#[derive(Default)]
struct ToolCall {
    index: usize,
    id: String,
    name: String,
    arguments: String,
}

/// Incremental Responses-to-Chat converter. Its result is valid only after a
/// completed or incomplete terminal event; transport EOF is not completion.
pub struct ChatConverter {
    id: String,
    model: String,
    created: i64,
    include_usage: bool,
    role_sent: bool,
    terminal: bool,
    error: Option<String>,
    finish_reason: Option<String>,
    usage: Option<Value>,
    // Separate snapshots by output/content indices so multiple parts never
    // overwrite each other or duplicate done/terminal snapshots.
    parts: BTreeMap<(u64, u64, u8), String>,
    tools: BTreeMap<u64, ToolCall>,
}

impl ChatConverter {
    pub fn new(model: String, include_usage: bool) -> Self {
        Self {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            model,
            created: chrono::Utc::now().timestamp(),
            include_usage,
            role_sent: false,
            terminal: false,
            error: None,
            finish_reason: None,
            usage: None,
            parts: BTreeMap::new(),
            tools: BTreeMap::new(),
        }
    }
    pub fn finished(&self) -> bool {
        self.terminal
    }
    pub fn failure(&self) -> Option<String> {
        self.error.clone()
    }
    fn chunk(&self, choices: Value, usage: Option<Value>) -> Value {
        let mut result = json!({"id":self.id,"object":"chat.completion.chunk","created":self.created,"model":self.model,"choices":choices});
        if let Some(usage) = usage {
            result["usage"] = usage;
        }
        result
    }
    fn emit(&self, delta: Value) -> Value {
        self.chunk(
            json!([{"index":0,"delta":delta,"finish_reason":null}]),
            if self.include_usage {
                Some(Value::Null)
            } else {
                None
            },
        )
    }
    fn fail(&mut self, message: impl Into<String>) {
        self.error = Some(message.into());
        self.terminal = true;
    }
    fn part(
        &mut self,
        output: u64,
        part: u64,
        kind: u8,
        text: &str,
        snapshot: bool,
        frames: &mut Vec<Value>,
    ) {
        let existing = self.parts.entry((output, part, kind)).or_default();
        let delta = if snapshot {
            match text.strip_prefix(existing.as_str()) {
                Some(rest) => rest.to_owned(),
                None => {
                    self.fail("upstream content snapshot conflicts with streamed content");
                    return;
                }
            }
        } else {
            text.to_owned()
        };
        existing.push_str(&delta);
        if !delta.is_empty() {
            let field = match kind {
                0 => "content",
                1 => "refusal",
                _ => "reasoning_content",
            };
            let mut obj = Map::new();
            obj.insert(field.into(), delta.into());
            frames.push(self.emit(Value::Object(obj)));
        }
    }
    fn tool_snapshot(&mut self, output: u64, item: &Value, frames: &mut Vec<Value>) {
        let id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
        let name = item.get("name").and_then(Value::as_str).unwrap_or("");
        if !self.tools.contains_key(&output) {
            if id.is_empty() || name.is_empty() {
                self.fail("function call is missing call_id or name");
                return;
            }
            let index = self.tools.len();
            self.tools.insert(
                output,
                ToolCall {
                    index,
                    id: id.into(),
                    name: name.into(),
                    arguments: String::new(),
                },
            );
            frames.push(self.emit(json!({"tool_calls":[{"index":index,"id":id,"type":"function","function":{"name":name,"arguments":""}}]})));
        } else if (!id.is_empty() && self.tools[&output].id != id)
            || (!name.is_empty() && self.tools[&output].name != name)
        {
            self.fail("function call identity changed");
            return;
        }
        if let Some(args) = item.get("arguments").and_then(Value::as_str) {
            self.tool_args(output, args, true, frames);
        }
    }
    fn tool_args(&mut self, output: u64, args: &str, snapshot: bool, frames: &mut Vec<Value>) {
        let Some(tool) = self.tools.get_mut(&output) else {
            self.fail("function arguments received before function call");
            return;
        };
        let delta = if snapshot {
            match args.strip_prefix(tool.arguments.as_str()) {
                Some(rest) => rest.to_owned(),
                None => {
                    self.fail("function argument snapshot conflicts with streamed arguments");
                    return;
                }
            }
        } else {
            args.to_owned()
        };
        tool.arguments.push_str(&delta);
        let index = tool.index;
        if !delta.is_empty() {
            frames.push(
                self.emit(json!({"tool_calls":[{"index":index,"function":{"arguments":delta}}]})),
            );
        }
    }
    fn item_snapshot(&mut self, output: u64, item: &Value, frames: &mut Vec<Value>) {
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => self.tool_snapshot(output, item, frames),
            Some("message") => {
                if let Some(parts) = item.get("content").and_then(Value::as_array) {
                    for (i, p) in parts.iter().enumerate() {
                        match p.get("type").and_then(Value::as_str) {
                            Some("output_text") => {
                                if let Some(text) = p.get("text").and_then(Value::as_str) {
                                    self.part(output, i as u64, 0, text, true, frames);
                                }
                            }
                            Some("refusal") => {
                                if let Some(text) = p.get("refusal").and_then(Value::as_str) {
                                    self.part(output, i as u64, 1, text, true, frames);
                                }
                            }
                            Some(other) => {
                                self.fail(format!("unsupported output content type: {other}"))
                            }
                            None => self.fail("output content type missing"),
                        }
                        if self.terminal {
                            break;
                        }
                    }
                }
            }
            Some("reasoning") => {
                for (field, kind) in [("summary", 2), ("content", 3)] {
                    if let Some(parts) = item.get(field).and_then(Value::as_array) {
                        for (i, p) in parts.iter().enumerate() {
                            if let Some(text) = p.get("text").and_then(Value::as_str) {
                                self.part(output, i as u64, kind, text, true, frames);
                            }
                        }
                    }
                }
            }
            Some(other) => self.fail(format!("unsupported output item type: {other}")),
            None => self.fail("output item type missing"),
        }
    }
    pub fn on_event(&mut self, event: &Value) -> Vec<Value> {
        if self.terminal {
            return Vec::new();
        }
        let mut frames = Vec::new();
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            self.fail("upstream event is missing its type");
            return frames;
        };
        let indexed = kind.starts_with("response.output_text.")
            || kind.starts_with("response.refusal.")
            || kind.starts_with("response.content_part.")
            || kind.starts_with("response.reasoning_summary_text.")
            || kind.starts_with("response.reasoning_text.")
            || kind.starts_with("response.output_item.")
            || kind.starts_with("response.function_call_arguments.");
        if indexed && event.get("output_index").and_then(Value::as_u64).is_none() {
            self.fail("upstream output event is missing output_index");
            return frames;
        }
        if (kind.starts_with("response.output_text.")
            || kind.starts_with("response.refusal.")
            || kind.starts_with("response.content_part.")
            || kind.starts_with("response.reasoning_text."))
            && event.get("content_index").and_then(Value::as_u64).is_none()
        {
            self.fail("upstream content event is missing content_index");
            return frames;
        }
        if kind.starts_with("response.reasoning_summary_text.")
            && event.get("summary_index").and_then(Value::as_u64).is_none()
        {
            self.fail("upstream reasoning event is missing summary_index");
            return frames;
        }
        let output = event
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let part = event
            .get("content_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        match kind {
            "response.created" => {
                if !self.role_sent {
                    frames.push(self.emit(json!({"role":"assistant"})));
                    self.role_sent = true;
                }
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                if let Some(s) = event.get("delta").and_then(Value::as_str) {
                    self.part(
                        output,
                        part,
                        if kind.contains("refusal") { 1 } else { 0 },
                        s,
                        false,
                        &mut frames,
                    );
                } else {
                    self.fail("missing text delta");
                }
            }
            "response.output_text.done" | "response.refusal.done" => {
                let refusal = kind.contains("refusal");
                if let Some(s) = event
                    .get(if refusal { "refusal" } else { "text" })
                    .and_then(Value::as_str)
                {
                    self.part(
                        output,
                        part,
                        if refusal { 1 } else { 0 },
                        s,
                        true,
                        &mut frames,
                    );
                } else {
                    self.fail("missing text snapshot");
                }
            }
            "response.content_part.added" | "response.content_part.done" => {
                if let Some(value) = event.get("part") {
                    match value.get("type").and_then(Value::as_str) {
                        Some("output_text") => {
                            if let Some(text) = value.get("text").and_then(Value::as_str) {
                                self.part(output, part, 0, text, true, &mut frames);
                            }
                        }
                        Some("refusal") => {
                            if let Some(text) = value.get("refusal").and_then(Value::as_str) {
                                self.part(output, part, 1, text, true, &mut frames);
                            }
                        }
                        Some(other) => {
                            self.fail(format!("unsupported output content type: {other}"))
                        }
                        None => self.fail("output content type missing"),
                    }
                } else {
                    self.fail("missing content part");
                }
            }
            "response.reasoning_summary_text.delta"
            | "response.reasoning_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.done" => {
                let summary = kind.contains("summary");
                let snapshot = kind.ends_with(".done");
                let index = event
                    .get(if summary {
                        "summary_index"
                    } else {
                        "content_index"
                    })
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if let Some(s) = event
                    .get(if snapshot { "text" } else { "delta" })
                    .and_then(Value::as_str)
                {
                    self.part(
                        output,
                        index,
                        if summary { 2 } else { 3 },
                        s,
                        snapshot,
                        &mut frames,
                    );
                } else {
                    self.fail("missing reasoning text");
                }
            }
            "response.output_item.added" | "response.output_item.done" => {
                if let Some(item) = event.get("item") {
                    self.item_snapshot(output, item, &mut frames);
                } else {
                    self.fail("missing output item");
                }
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                let snapshot = kind.ends_with(".done");
                if let Some(s) = event
                    .get(if snapshot { "arguments" } else { "delta" })
                    .and_then(Value::as_str)
                {
                    self.tool_args(output, s, snapshot, &mut frames);
                } else {
                    self.fail("missing function arguments");
                }
            }
            "response.completed" | "response.incomplete" => {
                if let Some(response) = event.get("response") {
                    let expected = if kind == "response.completed" {
                        "completed"
                    } else {
                        "incomplete"
                    };
                    if response.get("status").and_then(Value::as_str) != Some(expected) {
                        self.fail("terminal event and response status do not match");
                    }
                    if response.get("output").is_some_and(|v| !v.is_array()) {
                        self.fail("upstream response output is not an array");
                    } else if response.get("output").is_none()
                        && self.parts.is_empty()
                        && self.tools.is_empty()
                    {
                        self.fail("upstream response contains no output or streamed content");
                    }
                    if let Some(items) = response.get("output").and_then(Value::as_array) {
                        for (index, item) in items.iter().enumerate() {
                            self.item_snapshot(index as u64, item, &mut frames);
                            if self.terminal {
                                break;
                            }
                        }
                    }
                    if !self.terminal {
                        self.usage = response
                            .get("usage")
                            .filter(|v| v.is_object())
                            .map(convert_usage);
                        let finish = if kind == "response.incomplete" {
                            match response
                                .pointer("/incomplete_details/reason")
                                .and_then(Value::as_str)
                            {
                                Some("max_output_tokens") => "length",
                                Some("content_filter") => "content_filter",
                                _ => {
                                    self.fail("upstream response incomplete for an unknown reason");
                                    ""
                                }
                            }
                        } else if self.tools.is_empty() {
                            "stop"
                        } else {
                            "tool_calls"
                        };
                        if !self.terminal {
                            self.finish_reason = Some(finish.into());
                            frames.push(self.chunk(
                                json!([{"index":0,"delta":{},"finish_reason":finish}]),
                                if self.include_usage {
                                    Some(Value::Null)
                                } else {
                                    None
                                },
                            ));
                            if self.include_usage {
                                if let Some(usage) = self.usage.clone() {
                                    frames.push(self.chunk(json!([]), Some(usage)));
                                }
                            }
                            self.terminal = true;
                        }
                    }
                } else {
                    self.fail("terminal event is missing response");
                }
            }
            "response.failed" | "error" => {
                let error = event
                    .pointer("/response/error")
                    .or_else(|| event.get("error"))
                    .unwrap_or(event);
                self.fail(
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("upstream response failed"),
                );
            }
            _ => {}
        }
        if !self.role_sent && !frames.is_empty() {
            frames.insert(0, self.emit(json!({"role":"assistant"})));
            self.role_sent = true;
        }
        frames
    }
    pub fn final_response(&self) -> Result<Value, String> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }
        let finish = self
            .finish_reason
            .as_ref()
            .ok_or("upstream response has not completed")?;
        let collect = |kinds: &[u8]| -> Option<String> {
            let values: Vec<&str> = self
                .parts
                .iter()
                .filter(|((_, _, k), _)| kinds.contains(k))
                .map(|(_, s)| s.as_str())
                .collect();
            if values.is_empty() {
                None
            } else {
                Some(values.join(""))
            }
        };
        let mut message =
            json!({"role":"assistant","content":collect(&[0]),"refusal":collect(&[1])});
        if let Some(text) = collect(&[2, 3]) {
            message["reasoning_content"] = text.into();
        }
        if !self.tools.is_empty() {
            let mut tools: Vec<_> = self.tools.values().collect();
            tools.sort_by_key(|t| t.index);
            message["tool_calls"] = tools.into_iter().map(|t| json!({"id":t.id,"type":"function","function":{"name":t.name,"arguments":t.arguments}})).collect::<Vec<_>>().into();
        }
        let mut response = json!({"id":self.id,"object":"chat.completion","created":self.created,"model":self.model,"choices":[{"index":0,"message":message,"finish_reason":finish}]});
        if let Some(usage) = &self.usage {
            response["usage"] = usage.clone();
        }
        Ok(response)
    }
}

fn convert_usage(usage: &Value) -> Value {
    let input = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input + output);
    let mut result = json!({"prompt_tokens":input,"completion_tokens":output,"total_tokens":total});
    if let Some(cached) = usage.pointer("/input_tokens_details/cached_tokens") {
        result["prompt_tokens_details"] = json!({"cached_tokens":cached});
    }
    if let Some(reasoning) = usage.pointer("/output_tokens_details/reasoning_tokens") {
        result["completion_tokens_details"] = json!({"reasoning_tokens":reasoning});
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_tools_stream_distinct_indices_without_snapshot_duplication() {
        let mut c = ChatConverter::new("model".into(), true);
        c.on_event(&json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"a","arguments":""}}));
        c.on_event(&json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"x\":"}));
        let second = c.on_event(&json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"c2","name":"b","arguments":""}}));
        assert_eq!(
            second[0]["choices"][0]["delta"]["tool_calls"][0]["index"],
            1
        );
        c.on_event(
            &json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"1}"}),
        );
        let end = c.on_event(&json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7},"output":[{"type":"function_call","call_id":"c1","name":"a","arguments":"{\"x\":1}"},{"type":"function_call","call_id":"c2","name":"b","arguments":"{}"}]}}));
        assert!(end
            .iter()
            .any(|f| f["choices"].as_array().is_some_and(|c| c.is_empty())
                && f["usage"]["total_tokens"] == 7));
        let result = c.final_response().unwrap();
        assert_eq!(
            result["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            "{\"x\":1}"
        );
        assert_eq!(
            result["choices"][0]["message"]["tool_calls"][1]["function"]["arguments"],
            "{}"
        );
        assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn reasoning_text_incomplete_and_failure_are_distinct() {
        let mut c = ChatConverter::new("model".into(), false);
        c.on_event(&json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"think"}));
        c.on_event(&json!({"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"hello"}));
        c.on_event(&json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"reasoning","summary":[{"type":"summary_text","text":"think"}]},{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}}));
        let result = c.final_response().unwrap();
        assert_eq!(result["choices"][0]["message"]["content"], "hello");
        assert_eq!(
            result["choices"][0]["message"]["reasoning_content"],
            "think"
        );
        assert_eq!(result["choices"][0]["finish_reason"], "length");
        let mut failed = ChatConverter::new("model".into(), false);
        failed.on_event(
            &json!({"type":"response.failed","response":{"error":{"message":"quota exhausted"}}}),
        );
        assert_eq!(failed.failure().as_deref(), Some("quota exhausted"));
        assert!(failed.finished());
        assert!(failed.final_response().is_err());
    }

    #[test]
    fn malformed_events_and_conflicting_snapshots_fail_without_success() {
        let mut missing_index = ChatConverter::new("model".into(), false);
        missing_index
            .on_event(&json!({"type":"response.output_text.delta","content_index":0,"delta":"x"}));
        assert!(missing_index.finished());
        assert!(missing_index.final_response().is_err());
        let mut conflict = ChatConverter::new("model".into(), false);
        conflict.on_event(&json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"abc"}));
        conflict.on_event(&json!({"type":"response.output_text.done","output_index":0,"content_index":0,"text":"different"}));
        assert!(conflict.failure().is_some());
        let mut wrong_status = ChatConverter::new("model".into(), false);
        wrong_status.on_event(
            &json!({"type":"response.completed","response":{"status":"failed","output":[]}}),
        );
        assert!(wrong_status.failure().is_some());
        let mut no_output = ChatConverter::new("model".into(), false);
        no_output.on_event(&json!({"type":"response.completed","response":{"status":"completed"}}));
        assert!(no_output.final_response().is_err());
    }
}
