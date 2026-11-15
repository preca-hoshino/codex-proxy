//! Transform Chat Completions requests into Codex Responses requests.
use serde_json::{json, Map, Value};

/// Strip options rejected by Codex OAuth without touching prompts or tool data.
/// The same normalization runs for both Chat and native Responses requests.
/// Preserve reasoning effort until the selected account's model catalog can
/// resolve public presets at the upstream send boundary.
pub(super) fn clean_codex_options(body: &mut Map<String, Value>) -> Vec<&'static str> {
    let mut removed = Vec::new();
    for field in [
        "max_output_tokens",
        "max_completion_tokens",
        "max_tokens",
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "chat_template_kwargs",
        "user",
        "metadata",
        "safety_identifier",
        "prompt_cache_retention",
    ] {
        if body.remove(field).is_some() {
            removed.push(field);
        }
    }
    // Responses already reports usage. Keep other stream options, including
    // Codex reasoning-summary controls, intact.
    if let Some(options) = body
        .get_mut("stream_options")
        .and_then(Value::as_object_mut)
    {
        if options.remove("include_usage").is_some() {
            removed.push("stream_options.include_usage");
        }
    }
    // Only remove no-op forms; explicit stop/truncation/cache behavior remains.
    if body.get("truncation").and_then(Value::as_str) == Some("disabled") {
        body.remove("truncation");
        removed.push("truncation");
    }
    if body
        .get("stop_sequences")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        body.remove("stop_sequences");
        removed.push("stop_sequences");
    }
    if body
        .get("prompt_cache_options")
        .and_then(Value::as_object)
        .is_some_and(Map::is_empty)
    {
        body.remove("prompt_cache_options");
        removed.push("prompt_cache_options");
    }
    if !removed.is_empty() {
        // Log only fixed field names, never request values or user content.
        tracing::debug!(fields = ?removed, "Removed request options unsupported by Codex");
    }
    removed
}

/// Chat clients can echo display-only reasoning and trace identifiers in
/// assistant history. A cot_id cannot reconstruct encrypted Codex reasoning.
/// Limit cleanup to these exact assistant message fields; do not turn display
/// summaries into prompt content or alter nested tool data/native reasoning.
fn clean_chat_message_metadata(body: &mut Map<String, Value>) -> Vec<&'static str> {
    let mut removed = Vec::new();
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return removed;
    };
    for (field, name) in [
        ("cot_id", "messages[].cot_id"),
        ("cot_summary", "messages[].cot_summary"),
        ("reasoning_content", "messages[].reasoning_content"),
        ("reasoning", "messages[].reasoning"),
    ] {
        let mut found = false;
        for message in messages.iter_mut().filter_map(Value::as_object_mut) {
            if message.get("role").and_then(Value::as_str) == Some("assistant") {
                found |= message.remove(field).is_some();
            }
        }
        if found {
            removed.push(name);
        }
    }
    if !removed.is_empty() {
        tracing::debug!(fields = ?removed, "Removed Chat message display metadata");
    }
    removed
}

fn str_field<'a>(v: &'a Value, name: &str) -> Result<&'a str, String> {
    v.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{name} must be a non-empty string"))
}

fn keys(v: &Value, allowed: &[&str], path: &str) -> Result<(), String> {
    let obj = v
        .as_object()
        .ok_or_else(|| format!("{path} must be an object"))?;
    for (k, value) in obj {
        if !allowed.contains(&k.as_str()) && !value.is_null() {
            return Err(format!("unsupported parameter: {path}.{k}"));
        }
    }
    Ok(())
}

fn convert_image_part(item: &Value) -> Result<Value, String> {
    keys(item, &["type", "image_url"], "image part")?;
    let image = item.get("image_url").ok_or("image_url is required")?;
    let (url, detail) = if let Some(s) = image.as_str() {
        (s, None)
    } else {
        keys(image, &["url", "detail", "media_type"], "image_url")?;
        if image
            .get("media_type")
            .is_some_and(|value| !value.is_null() && !value.as_str().is_some_and(|s| !s.is_empty()))
        {
            return Err("image_url.media_type must be a non-empty string".into());
        }
        (str_field(image, "url")?, image.get("detail"))
    };
    let mut converted = json!({"type":"input_image", "image_url":url});
    if let Some(d) = detail {
        if !matches!(d.as_str(), Some("auto" | "low" | "high")) {
            return Err("invalid image detail".into());
        }
        converted["detail"] = d.clone();
    }
    Ok(converted)
}

/// Convert an OpenAI Chat request to a Responses request. Codex-specific option
/// cleanup leaves structural validation and downstream usage reporting intact.
pub fn chat_to_responses(body: &Value) -> Result<Value, String> {
    let stream = match body.get("stream") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(x)) => *x,
        _ => return Err("stream must be a boolean".into()),
    };
    if let Some(opts) = body.get("stream_options").filter(|v| !v.is_null()) {
        if !stream {
            return Err("stream_options requires stream=true".into());
        }
        keys(opts, &["include_usage"], "stream_options")?;
        if opts.get("include_usage").is_some_and(|v| !v.is_boolean()) {
            return Err("include_usage must be a boolean".into());
        }
    }
    let mut normalized = body.clone();
    clean_codex_options(
        normalized
            .as_object_mut()
            .ok_or("request must be an object")?,
    );
    clean_chat_message_metadata(normalized.as_object_mut().unwrap());
    let body = &normalized;
    keys(
        body,
        &[
            "model",
            "messages",
            "stream",
            "stream_options",
            "n",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "reasoning_effort",
            "reasoning",
            "prompt_cache_key",
            "response_format",
            "store",
        ],
        "request",
    )?;
    let model = str_field(body, "model")?;
    if let Some(n) = body.get("n").filter(|v| !v.is_null()) {
        if n.as_u64() != Some(1) {
            return Err("only n=1 is supported".into());
        }
    }
    if let Some(store) = body.get("store").filter(|v| !v.is_null()) {
        if store != &json!(false) {
            return Err("store must be false".into());
        }
    }
    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .filter(|m| !m.is_empty())
        .ok_or("messages must be a non-empty array")?;
    let mut input = Vec::new();
    for (i, msg) in messages.iter().enumerate() {
        keys(
            msg,
            &["role", "content", "tool_calls", "tool_call_id"],
            &format!("messages[{i}]"),
        )?;
        let role = str_field(msg, "role")?;
        if role == "tool" {
            let call_id = str_field(msg, "tool_call_id")?;
            let output = match msg.get("content") {
                Some(Value::String(text)) => json!(text),
                Some(Value::Array(parts)) => {
                    let mut output = String::new();
                    let mut converted = Vec::new();
                    let mut has_image = false;
                    for part in parts {
                        match part.get("type").and_then(Value::as_str) {
                            Some("text") => {
                                keys(part, &["type", "text"], "tool content part")?;
                                let text = part
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .ok_or("tool content part text must be a string")?;
                                output.push_str(text);
                                converted.push(json!({"type":"input_text","text":text}));
                            }
                            Some("image_url") => {
                                converted.push(convert_image_part(part)?);
                                has_image = true;
                            }
                            _ => return Err("tool content part must be text or image".into()),
                        }
                    }
                    if has_image {
                        json!(converted)
                    } else {
                        json!(output)
                    }
                }
                _ => return Err("tool content must be a string or content array".into()),
            };
            if msg.get("tool_calls").is_some_and(|v| !v.is_null()) {
                return Err("tool message cannot contain tool_calls".into());
            }
            input.push(json!({"type":"function_call_output", "call_id":call_id, "output":output}));
            continue;
        }
        if !matches!(role, "system" | "developer" | "user" | "assistant") {
            return Err(format!("unsupported message role: {role}"));
        }
        if msg.get("tool_call_id").is_some_and(|v| !v.is_null()) {
            return Err("tool_call_id is only valid on tool messages".into());
        }
        if role != "assistant" && msg.get("tool_calls").is_some_and(|v| !v.is_null()) {
            return Err("tool_calls is only valid on assistant messages".into());
        }
        let mut parts = Vec::new();
        let text_kind = if role == "assistant" {
            "output_text"
        } else {
            "input_text"
        };
        match msg.get("content") {
            Some(Value::String(s)) => parts.push(json!({"type":text_kind,"text":s})),
            Some(Value::Array(items)) => {
                for item in items {
                    let kind = str_field(item, "type")?;
                    match kind {
                        "text" => {
                            keys(item, &["type", "text"], "content part")?;
                            let s = item
                                .get("text")
                                .and_then(Value::as_str)
                                .ok_or("text part needs text")?;
                            parts.push(json!({"type":text_kind,"text":s}));
                        }
                        "image_url" if role == "user" => {
                            parts.push(convert_image_part(item)?);
                        }
                        _ => return Err(format!("unsupported content part: {kind}")),
                    }
                }
            }
            None | Some(Value::Null) if role == "assistant" => {}
            _ => return Err(format!("invalid content for {role}")),
        }
        if parts.is_empty()
            && !msg
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|calls| !calls.is_empty())
        {
            return Err(format!("messages[{i}] must contain content or tool calls"));
        }
        if !parts.is_empty() {
            input.push(json!({"type":"message","role":role,"content":parts}));
        }
        if let Some(calls) = msg.get("tool_calls").filter(|v| !v.is_null()) {
            for call in calls.as_array().ok_or("tool_calls must be an array")? {
                keys(call, &["id", "type", "function"], "tool_call")?;
                if call.get("type").and_then(Value::as_str) != Some("function") {
                    return Err("only function tool calls are supported".into());
                }
                let f = call.get("function").ok_or("function is required")?;
                keys(f, &["name", "arguments"], "tool_call.function")?;
                let arguments = f
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or("tool arguments must be a string")?;
                input.push(json!({"type":"function_call", "call_id":str_field(call,"id")?, "name":str_field(f,"name")?, "arguments":arguments}));
            }
        }
    }
    let mut out = json!({"model": model, "input": input, "stream": true, "store": false});
    if let Some(key) = body.get("prompt_cache_key").filter(|key| !key.is_null()) {
        if !key.is_string() {
            return Err("prompt_cache_key must be a string".into());
        }
        out["prompt_cache_key"] = key.clone();
    }
    if let Some(tools) = body.get("tools").filter(|v| !v.is_null()) {
        let mut converted = Vec::new();
        for tool in tools.as_array().ok_or("tools must be an array")? {
            keys(tool, &["type", "function"], "tool")?;
            if tool.get("type").and_then(Value::as_str) != Some("function") {
                return Err("only function tools are supported".into());
            }
            let f = tool.get("function").ok_or("tool.function is required")?;
            keys(
                f,
                &["name", "description", "parameters", "strict"],
                "tool.function",
            )?;
            let mut t = json!({"type":"function","name":str_field(f,"name")?});
            for k in ["description", "parameters", "strict"] {
                if let Some(v) = f.get(k) {
                    t[k] = v.clone();
                }
            }
            converted.push(t);
        }
        out["tools"] = converted.into();
    }
    if let Some(choice) = body.get("tool_choice").filter(|v| !v.is_null()) {
        if let Some(s) = choice.as_str() {
            if !matches!(s, "none" | "auto" | "required") {
                return Err("invalid tool_choice".into());
            }
            out["tool_choice"] = choice.clone();
        } else {
            keys(choice, &["type", "function"], "tool_choice")?;
            if choice.get("type").and_then(Value::as_str) != Some("function") {
                return Err("only function tool_choice is supported".into());
            }
            let f = choice
                .get("function")
                .ok_or("tool_choice.function required")?;
            keys(f, &["name"], "tool_choice.function")?;
            out["tool_choice"] = json!({"type":"function","name":str_field(f,"name")?});
        }
    }
    if let Some(v) = body.get("parallel_tool_calls").filter(|v| !v.is_null()) {
        if !v.is_boolean() {
            return Err("parallel_tool_calls must be a boolean".into());
        }
        out["parallel_tool_calls"] = v.clone();
    }
    let mut reasoning = body
        .get("reasoning")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_object()
                .cloned()
                .ok_or("reasoning must be an object")
        })
        .transpose()?;
    if let Some(v) = body.get("reasoning_effort").filter(|v| !v.is_null()) {
        let s = v
            .as_str()
            .filter(|s| {
                matches!(
                    *s,
                    "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra"
                )
            })
            .ok_or("invalid reasoning_effort")?;
        // Match the Chat compatibility contract: the flat control overrides
        // only effort. Explicit nested summaries and future options survive.
        reasoning
            .get_or_insert_with(|| Map::from_iter([("summary".into(), json!("auto"))]))
            .insert("effort".into(), json!(s));
    }
    if let Some(reasoning) = reasoning {
        for field in ["effort", "summary"] {
            if let Some(value) = reasoning.get(field).filter(|value| !value.is_null()) {
                if !value.as_str().is_some_and(|value| !value.is_empty()) {
                    return Err(format!("reasoning.{field} must be a non-empty string"));
                }
            }
        }
        out["reasoning"] = Value::Object(reasoning);
    }
    if let Some(v) = body.get("response_format").filter(|v| !v.is_null()) {
        keys(v, &["type", "json_schema"], "response_format")?;
        match str_field(v, "type")? {
            "text" => out["text"] = json!({"format":{"type":"text"}}),
            "json_object" => out["text"] = json!({"format":{"type":"json_object"}}),
            "json_schema" => {
                let schema = v.get("json_schema").ok_or("json_schema required")?;
                keys(
                    schema,
                    &["name", "description", "schema", "strict"],
                    "json_schema",
                )?;
                let mut f = schema.as_object().unwrap().clone();
                str_field(schema, "name")?;
                if !f.contains_key("schema") {
                    return Err("json_schema.schema required".into());
                }
                f.insert("type".into(), json!("json_schema"));
                out["text"] = json!({"format":f});
            }
            _ => return Err("unsupported response_format".into()),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_reasoning_preserves_summary_and_unknown_fields() {
        let reasoning = json!({"effort":"high","summary":"detailed","future":{"keep":true},"future_flag":false});
        let request = json!({
            "model":"gpt-6.1-terra", "reasoning":reasoning,
            "messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"previous answer","reasoning":"display-only"}]
        });
        let result = chat_to_responses(&request).unwrap();
        assert_eq!(result["reasoning"], reasoning);
        assert_eq!(result["input"][1]["content"][0]["text"], "previous answer");
        assert!(result["input"][1].get("reasoning").is_none());
        assert_eq!(request["messages"][1]["reasoning"], "display-only");
    }

    #[test]
    fn flat_effort_overrides_nested_effort_without_overwriting_summary() {
        for nested in [
            json!({"effort":"low","summary":"concise","future":[1,2]}),
            json!({"effort":7,"summary":"concise","future":[1,2]}),
        ] {
            let request = json!({"model":"gpt-6.1-terra","messages":[{"role":"user","content":"hello"}],"reasoning":nested,"reasoning_effort":"max"});
            let result = chat_to_responses(&request).unwrap();
            assert_eq!(
                result["reasoning"],
                json!({"effort":"max","summary":"concise","future":[1,2]})
            );
        }
        let request = json!({"model":"gpt-6.1-terra","messages":[{"role":"user","content":"hello"}],"reasoning":{"effort":"low","future":true},"reasoning_effort":"xhigh"});
        assert_eq!(
            chat_to_responses(&request).unwrap()["reasoning"],
            json!({"effort":"xhigh","future":true})
        );
    }

    #[test]
    fn nested_ultra_and_null_reasoning_are_preserved_for_their_expected_paths() {
        let mut request = json!({"model":"gpt-6.1-terra","messages":[{"role":"user","content":"hello"}],"reasoning":{"effort":"ultra","summary":"auto","future":"preserved"}});
        assert_eq!(
            chat_to_responses(&request).unwrap()["reasoning"],
            request["reasoning"]
        );
        request["reasoning"] = Value::Null;
        assert!(chat_to_responses(&request)
            .unwrap()
            .get("reasoning")
            .is_none());
        request["reasoning_effort"] = json!("high");
        assert_eq!(
            chat_to_responses(&request).unwrap()["reasoning"],
            json!({"effort":"high","summary":"auto"})
        );
    }

    #[test]
    fn empty_reasoning_object_is_preserved() {
        let request = json!({"model":"gpt-6.1-terra","messages":[{"role":"user","content":"hello"}],"reasoning":{}});
        assert_eq!(chat_to_responses(&request).unwrap()["reasoning"], json!({}));
    }

    #[test]
    fn nested_reasoning_rejects_invalid_object_and_known_field_types() {
        for reasoning in [
            json!("high"),
            json!(false),
            json!([]),
            json!(3),
            json!({"effort":3}),
            json!({"effort":""}),
            json!({"summary":true}),
            json!({"summary":""}),
        ] {
            let request = json!({"model":"gpt-6.1-terra","messages":[{"role":"user","content":"hello"}],"reasoning":reasoning});
            assert!(chat_to_responses(&request)
                .unwrap_err()
                .starts_with("reasoning"));
        }
    }

    fn chat_history_with_sixth_assistant_message() -> Value {
        json!({
            "model":"gpt-6.1-terra",
            "reasoning_effort":"high",
            "messages":[
                {"role":"system","content":"Help the user."},
                {"role":"user","content":"Find the first result."},
                {"role":"assistant","content":null,"tool_calls":[{"id":"call_first","type":"function","function":{"name":"lookup","arguments":"{\"cot_id\":\"business-id\",\"reasoning_content\":\"business-data\"}"}}]},
                {"role":"tool","tool_call_id":"call_first","content":"{\"cot_id\":\"result-id\",\"reasoning_content\":\"result-data\"}"},
                {"role":"user","content":"Check the next result."},
                {"role":"assistant","content":"The previous result is preserved.","tool_calls":[{"id":"call_second","type":"function","function":{"name":"lookup","arguments":"{\"cot_id\":\"second-id\"}"}}]},
                {"role":"tool","tool_call_id":"call_second","content":"Second tool result."},
                {"role":"user","content":"Summarize both results."}
            ],
            "tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{"cot_id":{"type":"string"},"reasoning_content":{"type":"string"}},"required":["cot_id"]}}}]
        })
    }

    #[test]
    fn sixth_assistant_message_metadata_is_ignored_without_changing_tool_history() {
        let baseline = chat_history_with_sixth_assistant_message();
        let expected = chat_to_responses(&baseline).unwrap();
        for metadata in [
            Value::Null,
            json!("client-display-trace"),
            json!(7),
            json!(true),
            json!({"id":"client-trace"}),
            json!(["client-trace"]),
        ] {
            let mut request = baseline.clone();
            for field in ["cot_id", "cot_summary", "reasoning_content", "reasoning"] {
                request["messages"][5][field] = metadata.clone();
            }
            let result = chat_to_responses(&request).unwrap();
            assert_eq!(result, expected);
            assert_eq!(request["messages"][5]["cot_id"], metadata);
            assert_eq!(result["reasoning"]["effort"], "high");
            assert_eq!(
                result["tools"][0]["parameters"],
                baseline["tools"][0]["function"]["parameters"]
            );
            assert!(result["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["type"] == "function_call"
                    && item["call_id"] == "call_first"
                    && item["arguments"]
                        == baseline["messages"][2]["tool_calls"][0]["function"]["arguments"]));
            assert!(result["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["type"] == "function_call_output"
                    && item["call_id"] == "call_first"
                    && item["output"] == baseline["messages"][3]["content"]));
        }
    }

    #[test]
    fn message_metadata_cleanup_is_shallow_and_reports_only_fixed_field_names() {
        let mut request = chat_history_with_sixth_assistant_message();
        request["messages"][5]["cot_id"] = json!("private-trace-value");
        request["messages"][5]["cot_summary"] = json!("private-summary-value");
        request["messages"][5]["reasoning_content"] = json!("private-display-value");
        request["messages"][5]["reasoning"] = json!("private-reasoning-value");
        let before = request.clone();
        assert_eq!(
            clean_chat_message_metadata(request.as_object_mut().unwrap()),
            [
                "messages[].cot_id",
                "messages[].cot_summary",
                "messages[].reasoning_content",
                "messages[].reasoning"
            ]
        );
        assert!(request["messages"][5].get("cot_id").is_none());
        assert!(request["messages"][5].get("cot_summary").is_none());
        assert!(request["messages"][5].get("reasoning_content").is_none());
        assert!(request["messages"][5].get("reasoning").is_none());
        assert_eq!(
            request["messages"][5]["content"],
            before["messages"][5]["content"]
        );
        assert_eq!(
            request["messages"][5]["tool_calls"],
            before["messages"][5]["tool_calls"]
        );
        assert_eq!(request["messages"][2], before["messages"][2]);
        assert_eq!(request["messages"][3], before["messages"][3]);
        assert_eq!(request["tools"], before["tools"]);
    }

    #[test]
    fn message_metadata_does_not_bypass_protocol_structure_validation() {
        for message in [
            json!({"role":"assistant","content":null}),
            json!({"role":"assistant","content":7}),
            json!({"role":"assistant","content":null,"tool_calls":"invalid"}),
            json!({"role":"assistant","content":null,"tool_calls":[{"type":"function","function":{"name":"lookup","arguments":"{}"}}]}),
            json!({"role":"tool","content":"result"}),
            json!({"role":"user","content":"hello"}),
            json!({"role":"user","content":"hello","unknown_option":true}),
        ] {
            let mut message = message;
            for field in ["cot_id", "cot_summary", "reasoning_content", "reasoning"] {
                message[field] = json!("display-only");
            }
            assert!(
                chat_to_responses(&json!({"model":"gpt-6.1-terra","messages":[message]})).is_err()
            );
        }
    }

    #[test]
    fn reasoning_effort_is_preserved_for_every_supported_level() {
        for effort in [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
        ] {
            let request = json!({
                "model":"gpt-6.1-terra",
                "messages":[{"role":"user","content":"hi"}],
                "temperature":0.7,
                "reasoning_effort":effort
            });
            let result = chat_to_responses(&request).unwrap();
            assert_eq!(result["reasoning"]["effort"], effort);
            assert_eq!(result["reasoning"]["summary"], "auto");
            assert!(result.get("temperature").is_none());
            let mut native = json!({"input":"hi","reasoning":{"effort":effort,"summary":"detailed"},"temperature":0.7});
            clean_codex_options(native.as_object_mut().unwrap());
            assert_eq!(
                native["reasoning"],
                json!({"effort":effort,"summary":"detailed"})
            );
        }
    }

    #[test]
    fn ultra_is_preserved_until_account_catalog_resolution() {
        let request = json!({
            "model":"gpt-6.1-terra",
            "messages":[{"role":"user","content":"Keep the word ultra in this text."}],
            "reasoning_effort":"ultra",
            "tools":[{"type":"function","function":{"name":"ultra","parameters":{"type":"object"}}}]
        });
        let result = chat_to_responses(&request).unwrap();
        assert_eq!(
            result["reasoning"],
            json!({"effort":"ultra","summary":"auto"})
        );
        assert_eq!(
            result["input"][0]["content"][0]["text"],
            "Keep the word ultra in this text."
        );
        assert_eq!(result["tools"][0]["name"], "ultra");
        assert_eq!(request["reasoning_effort"], "ultra");

        let mut native = json!({
            "model":"gpt-6.1-terra", "input":"Keep ultra in this text.",
            "reasoning":{"effort":"ultra","summary":"detailed","extra":"preserved"},
            "tools":[{"type":"function","name":"ultra","parameters":{"type":"object"}}]
        });
        let original_input = native["input"].clone();
        let original_tools = native["tools"].clone();
        assert!(clean_codex_options(native.as_object_mut().unwrap()).is_empty());
        assert_eq!(
            native["reasoning"],
            json!({"effort":"ultra","summary":"detailed","extra":"preserved"})
        );
        assert_eq!(native["input"], original_input);
        assert_eq!(native["tools"], original_tools);
    }

    #[test]
    fn reasoning_effort_rejects_invalid_types_and_unknown_levels() {
        for effort in [
            json!(0),
            json!(true),
            json!([]),
            json!({"effort":"max"}),
            json!("unknown"),
        ] {
            let request = json!({"model":"gpt-6.1-terra","messages":[{"role":"user","content":"hi"}],"reasoning_effort":effort});
            assert_eq!(
                chat_to_responses(&request).unwrap_err(),
                "invalid reasoning_effort"
            );
        }
    }

    #[test]
    fn converts_images_tools_and_tool_results() {
        let request = json!({"model":"gpt-5", "messages":[
            {"role":"user", "content":[{"type":"text","text":"Look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA==","detail":"high"}}]},
            {"role":"assistant","content":null,"tool_calls":[{"id":"call_a","type":"function","function":{"name":"weather","arguments":"{}"}}]},
            {"role":"tool","tool_call_id":"call_a","content":"sunny"}
        ],"tools":[{"type":"function","function":{"name":"weather","parameters":{"type":"object"}}}]});
        let result = chat_to_responses(&request).unwrap();
        assert_eq!(
            result["input"][0]["content"][1],
            json!({"type":"input_image","image_url":"data:image/png;base64,AA==","detail":"high"})
        );
        assert_eq!(result["input"][1]["type"], "function_call");
        assert_eq!(result["input"][2]["call_id"], "call_a");
        assert_eq!(result["tools"][0]["name"], "weather");
        assert_eq!(result["stream"], true);
        assert!(chat_to_responses(
            &json!({"model":"x","messages":[{"role":"user","content":"x"}],"stop":["x"]})
        )
        .is_err());
    }

    #[test]
    fn converts_tool_text_content_arrays_without_losing_text() {
        for (content, expected) in [
            (json!("unchanged"), "unchanged"),
            (
                json!([{"type":"text","text":"first\n"},{"type":"text","text":"second"}]),
                "first\nsecond",
            ),
            (json!([]), ""),
        ] {
            let request = json!({"model":"x","messages":[{"role":"tool","tool_call_id":"call_1","content":content}]});
            let result = chat_to_responses(&request).unwrap();
            assert_eq!(
                result["input"][0],
                json!({"type":"function_call_output","call_id":"call_1","output":expected})
            );
        }
        for content in [
            json!(null),
            json!({"text":"value"}),
            json!([{"type":"audio","audio":"data"}]),
            json!([{"type":"text","text":42}]),
            json!([{"type":"text","text":"ok","extra":"value"}]),
        ] {
            let request = json!({"model":"x","messages":[{"role":"tool","tool_call_id":"call_1","content":content}]});
            assert!(chat_to_responses(&request).is_err());
        }
    }

    #[test]
    fn converts_multimodal_tool_output_in_order_and_validates_images() {
        let request = json!({"model":"x","messages":[{"role":"tool","tool_call_id":"call_1","content":[
            {"type":"text","text":"before"},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,AA==","media_type":"image/png","detail":"high"}},
            {"type":"text","text":"after"},
            {"type":"image_url","image_url":"https://example.com/a.png"}
        ]}]});
        let result = chat_to_responses(&request).unwrap();
        assert_eq!(
            result["input"][0],
            json!({"type":"function_call_output","call_id":"call_1","output":[
                {"type":"input_text","text":"before"},
                {"type":"input_image","image_url":"data:image/png;base64,AA==","detail":"high"},
                {"type":"input_text","text":"after"},
                {"type":"input_image","image_url":"https://example.com/a.png"}
            ]})
        );
        for image in [
            json!({"url":"https://example.com/a.png","media_type":false}),
            json!({"url":"https://example.com/a.png","detail":"invalid"}),
            json!({"url":"https://example.com/a.png","extra":true}),
            json!({}),
        ] {
            let request = json!({"model":"x","messages":[{"role":"tool","tool_call_id":"call_1","content":[{"type":"image_url","image_url":image}]}]});
            assert!(chat_to_responses(&request).is_err());
        }
    }

    #[test]
    fn image_media_type_is_accepted_as_metadata_without_changing_image_url() {
        for url in [
            "data:image/png;base64,AA==",
            "https://example.com/image.png",
        ] {
            let request = json!({"model":"x","messages":[{"role":"user","content":[
                {"type":"image_url","image_url":{"url":url,"detail":"high","media_type":"image/png"}}
            ]}]});
            let result = chat_to_responses(&request).unwrap();
            assert_eq!(
                result["input"][0]["content"][0],
                json!({
                    "type":"input_image", "image_url":url, "detail":"high"
                })
            );
            assert_eq!(
                request["messages"][0]["content"][0]["image_url"]["media_type"],
                "image/png"
            );
        }
        for media_type in [json!(false), json!(42), json!(""), json!({})] {
            let request = json!({"model":"x","messages":[{"role":"user","content":[
                {"type":"image_url","image_url":{"url":"https://example.com/image.png","media_type":media_type}}
            ]}]});
            assert_eq!(
                chat_to_responses(&request).unwrap_err(),
                "image_url.media_type must be a non-empty string"
            );
        }
        let request = json!({"model":"x","messages":[{"role":"user","content":[
            {"type":"image_url","image_url":{"url":"https://example.com/image.png","media_type":"image/png","unknown":true}}
        ]}]});
        assert_eq!(
            chat_to_responses(&request).unwrap_err(),
            "unsupported parameter: image_url.unknown"
        );
    }

    #[test]
    fn unhonored_codex_controls_are_removed_without_changing_prompt_data() {
        for field in [
            "temperature",
            "max_tokens",
            "max_completion_tokens",
            "max_output_tokens",
            "top_p",
            "frequency_penalty",
            "presence_penalty",
            "chat_template_kwargs",
            "user",
            "metadata",
            "safety_identifier",
            "prompt_cache_retention",
        ] {
            let mut request = json!({"model":"x","messages":[{"role":"user","content":"temperature=secret-value"}]});
            request[field] = json!(1);
            let result = chat_to_responses(&request).unwrap();
            assert!(result.get(field).is_none(), "{field}");
            assert_eq!(
                result["input"][0]["content"][0]["text"],
                "temperature=secret-value"
            );
            assert_eq!(request[field], 1);
            request[field] = Value::Null;
            assert!(chat_to_responses(&request).is_ok());
        }
    }

    #[test]
    fn cleanup_preserves_meaningful_controls_and_reports_names_only() {
        let mut request = json!({
            "model":"x", "temperature":"private-value", "metadata":{"private":"value"},
            "stream_options":{"include_usage":true,"include_obfuscation":false},
            "truncation":"disabled", "stop_sequences":[], "prompt_cache_options":{},
            "input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}],
            "tools":[{"type":"function","name":"metadata","parameters":{"type":"object","properties":{"temperature":{"type":"number"}}}}]
        });
        let original_input = request["input"].clone();
        let original_tools = request["tools"].clone();
        let removed = clean_codex_options(request.as_object_mut().unwrap());
        assert_eq!(
            removed,
            [
                "temperature",
                "metadata",
                "stream_options.include_usage",
                "truncation",
                "stop_sequences",
                "prompt_cache_options"
            ]
        );
        assert_eq!(request["input"], original_input);
        assert_eq!(request["tools"], original_tools);
        assert_eq!(
            request["stream_options"],
            json!({"include_obfuscation":false})
        );
        let mut explicit = json!({"truncation":"auto","stop_sequences":["END"],"prompt_cache_options":{"retention":"24h"},"unknown_option":7});
        assert!(clean_codex_options(explicit.as_object_mut().unwrap()).is_empty());
        assert_eq!(explicit["truncation"], "auto");
        assert_eq!(explicit["stop_sequences"], json!(["END"]));
        assert_eq!(explicit["prompt_cache_options"], json!({"retention":"24h"}));
        assert_eq!(explicit["unknown_option"], 7);
    }

    #[test]
    fn cleanup_does_not_bypass_chat_structure_or_usage_validation() {
        for extra in [
            json!({"n":2}),
            json!({"stop":["END"]}),
            json!({"stream":true,"stream_options":{"include_usage":"yes"}}),
            json!({"stream":false,"stream_options":{"include_usage":true}}),
            json!({"messages":[{"role":"unknown","content":"hi"}]}),
        ] {
            let mut request =
                json!({"model":"x","messages":[{"role":"user","content":"hi"}],"temperature":0.7});
            request
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(chat_to_responses(&request).is_err());
        }
        let request = json!({"model":"x","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true},"temperature":0.7});
        let result = chat_to_responses(&request).unwrap();
        assert!(result.get("stream_options").is_none());
        assert_eq!(request["stream_options"]["include_usage"], true);
    }
}
