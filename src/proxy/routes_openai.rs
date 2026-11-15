use crate::{
    proxy::{metrics::RequestMetrics, stream::ChatConverter},
    upstream::transform::chat_to_responses,
    AppState,
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio_tungstenite::tungstenite::Message;

fn error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({"error":{"message":message,"type":"proxy_error"}})),
    )
        .into_response()
}

fn request_validation_reason(message: &str) -> &'static str {
    if message.starts_with("unsupported parameter: request.") {
        "unsupported_request_parameter"
    } else if message.starts_with("unsupported parameter: messages[")
        || message.starts_with("unsupported parameter: content part.")
        || message.starts_with("unsupported parameter: image")
    {
        "unsupported_message_parameter"
    } else if message.starts_with("unsupported parameter: tool") {
        "unsupported_tool_parameter"
    } else if message.starts_with("unsupported parameter: ") {
        "unsupported_nested_parameter"
    } else if message.starts_with("unsupported message role:") {
        "unsupported_message_role"
    } else if message.starts_with("unsupported content part:") {
        "unsupported_content_part"
    } else if message.starts_with("messages[") || message.starts_with("messages must") {
        "invalid_messages"
    } else if message.contains("tool") || message.contains("function") {
        "invalid_tools"
    } else if message.starts_with("reasoning") || message.starts_with("invalid reasoning") {
        "invalid_reasoning"
    } else if message.contains("prompt_cache_key") {
        "invalid_prompt_cache_key"
    } else if message.starts_with("stream") || message.starts_with("include_usage") {
        "invalid_stream_options"
    } else if message.starts_with("response_format") || message.starts_with("json_schema") {
        "invalid_response_format"
    } else {
        "invalid_request"
    }
}

fn validation_detail(message: &str) -> &'static str {
    match message {
        "tool content must be a string or content array" => "tool_content_invalid_type",
        "tool content part must be text or image" => "tool_content_part_unsupported_type",
        "tool content part text must be a string" => "tool_content_part_text_not_string",
        "tool message cannot contain tool_calls" => "tool_message_has_tool_calls",
        "tool_call_id is only valid on tool messages" => "tool_call_id_wrong_role",
        "tool_calls is only valid on assistant messages" => "tool_calls_wrong_role",
        "tool_calls must be an array" => "tool_calls_not_array",
        "only function tool calls are supported" => "unsupported_tool_call_type",
        "function is required" => "tool_call_function_missing",
        "tool arguments must be a string" => "tool_arguments_not_string",
        "tools must be an array" => "tools_not_array",
        "only function tools are supported" => "unsupported_tool_type",
        "tool.function is required" => "tool_function_missing",
        "invalid tool_choice" => "invalid_tool_choice",
        "only function tool_choice is supported" => "unsupported_tool_choice_type",
        "tool_choice.function required" => "tool_choice_function_missing",
        "parallel_tool_calls must be a boolean" => "parallel_tool_calls_not_boolean",
        _ => "unspecified",
    }
}

fn validation_error(state: &AppState, route: &'static str, message: &str) -> Response {
    let _ = state.logs.send(format!(
        "[REJECTED] route={route} reason={} detail={} Request rejected before upstream; no token usage available",
        request_validation_reason(message), validation_detail(message)
    ));
    error(StatusCode::BAD_REQUEST, message)
}
fn authorized(headers: &HeaderMap, state: &AppState) -> bool {
    match &state.config.server.api_key {
        None => true,
        Some(key) => headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|v| v == key),
    }
}

fn add_automatic_cache_key(body: &mut Value, headers: &HeaderMap, enabled: bool) {
    if !enabled
        || body
            .get("prompt_cache_key")
            .is_some_and(|key| !key.is_null())
    {
        return;
    }

    let Some(model) = body
        .get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return;
    };
    let session = [
        "session-id",
        "session_id",
        "x-session-id",
        "conversation-id",
        "x-conversation-id",
        "thread-id",
    ]
    .iter()
    .find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| {
                !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
            })
    });
    let mut hasher = Sha256::new();
    hasher.update(b"codex-proxy:prompt-cache:v1\0");
    hasher.update((model.len() as u64).to_be_bytes());
    hasher.update(model.as_bytes());
    if let Some(session) = session {
        hasher.update(b"session\0");
        hasher.update(session.as_bytes());
    } else {
        let Some(first_user) = body
            .get("input")
            .and_then(Value::as_array)
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item.get("role").and_then(Value::as_str) == Some("user"))
            })
        else {
            return;
        };
        let Some(content) = first_user.get("content") else {
            return;
        };
        hasher.update(b"first-user\0");
        let mut found = false;
        if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
            hasher.update((text.len() as u64).to_be_bytes());
            hasher.update(text.as_bytes());
            found = true;
        } else if let Some(parts) = content.as_array() {
            for text in parts
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("input_text"))
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .filter(|text| !text.is_empty())
            {
                hasher.update((text.len() as u64).to_be_bytes());
                hasher.update(text.as_bytes());
                found = true;
            }
        }
        if !found {
            return;
        }
    }
    body["prompt_cache_key"] = format!("codex-proxy-{:x}", hasher.finalize()).into();
}
pub(super) async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}
pub(super) async fn models(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    extensions: axum::http::Extensions,
) -> Response {
    if !authorized(&headers, &s) {
        return error(StatusCode::UNAUTHORIZED, "invalid API key");
    }
    let wire_id = extensions
        .get::<crate::logger::WireRequestId>()
        .map(|w| w.0)
        .unwrap_or_else(crate::logger::next_request_id);
    match s.accounts.models_with_wire_id(wire_id).await {
        Ok(v) => {
            let data: Vec<Value> = v
                .get("models")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|m| m.get("slug").and_then(Value::as_str))
                .map(|id| json!({"id":id,"object":"model","created":0,"owned_by":"openai"}))
                .collect();
            Json(json!({"object":"list","data":data})).into_response()
        }
        Err(_) => error(
            StatusCode::BAD_GATEWAY,
            "could not obtain upstream model catalog",
        ),
    }
}

/// Only explicit upstream account failures are safe to retry after sending a request.
fn account_failure(event: &Value) -> bool {
    if !matches!(
        event.get("type").and_then(Value::as_str),
        Some("error" | "response.failed")
    ) {
        return false;
    }
    let details = event
        .pointer("/response/error")
        .or_else(|| event.get("error"));
    let status = event
        .get("status_code")
        .or_else(|| event.get("status"))
        .or_else(|| details.and_then(|e| e.get("status_code")))
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|v| v.parse().ok()))
        });
    if matches!(status, Some(401 | 403 | 429)) {
        return true;
    }
    details.is_some_and(|e| {
        ["code", "type"].iter().any(|key| {
            matches!(
                e.get(key).and_then(Value::as_str),
                Some(
                    "rate_limit_exceeded"
                        | "rate_limit_error"
                        | "usage_limit_reached"
                        | "insufficient_quota"
                        | "invalid_api_key"
                        | "invalid_token"
                        | "token_expired"
                        | "account_deactivated"
                )
            )
        })
    })
}

fn client_request_failure(event: &Value) -> Option<&str> {
    if !matches!(
        event.get("type").and_then(Value::as_str),
        Some("error" | "response.failed")
    ) {
        return None;
    }
    let details = event
        .pointer("/response/error")
        .or_else(|| event.get("error"))?;
    let invalid = details.get("type").and_then(Value::as_str) == Some("invalid_request_error")
        || matches!(
            details.get("code").and_then(Value::as_str),
            Some("unsupported_value" | "invalid_request_error" | "invalid_prompt")
        );
    invalid.then(|| {
        details
            .get("message")
            .and_then(Value::as_str)
            .filter(|message| !message.is_empty())
            .unwrap_or("Upstream rejected the request")
    })
}

fn gateway_metadata(event: &Value) -> bool {
    matches!(
        event.get("type").and_then(Value::as_str),
        Some("codex.rate_limits" | "codex.response.metadata")
    )
}

struct UpstreamOpenError {
    status: StatusCode,
    message: String,
}

impl From<String> for UpstreamOpenError {
    fn from(message: String) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message,
        }
    }
}

impl From<&str> for UpstreamOpenError {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

async fn open_upstream(
    s: &AppState,
    body: Value,
    wire_id: u64,
) -> Result<(crate::upstream::http::UpstreamEvents, Vec<String>), UpstreamOpenError> {
    let mut tried = Vec::new();
    let timeout = Duration::from_millis(s.config.retry.timeout_ms);
    for attempt_number in 1..=s.config.retry.max_attempts {
        let a = match s.accounts.select(&tried).await {
            Ok(a) => a,
            Err(_) => break,
        };
        tried.push(a.id.clone());
        let mut account_body = body.clone();
        s.accounts
            .resolve_request_reasoning_with_wire_id(&a, &mut account_body, wire_id)
            .await
            .map_err(|_| "Could not resolve reasoning effort from the upstream model catalog")?;
        let model = account_body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let reasoning_effort = account_body
            .pointer("/reasoning/effort")
            .and_then(Value::as_str)
            .unwrap_or("default");
        let _ = s.logs.send(format!(
            "Sending request to Codex upstream model={model:?} reasoning_effort={reasoning_effort:?} account={:?} transport={} attempt={attempt_number} max_attempts={}",
            a.email, s.config.server.upstream_transport, s.config.retry.max_attempts
        ));
        #[cfg(test)]
        let endpoint = s.test_upstream_url.as_deref();
        #[cfg(not(test))]
        let endpoint: Option<&str> = None;
        let redact = s.config.server.log_redact_secrets;
        let attempt = tokio::time::timeout(timeout, async {
            if s.config.server.upstream_transport == "http" {
                let result = if let Some(url) = endpoint {
                    crate::upstream::http::connect_to(
                        url,
                        &s.upstream_http_client,
                        &a.access_token,
                        Some(&a.id),
                        account_body,
                        wire_id,
                        redact,
                    )
                    .await
                } else {
                    crate::upstream::http::connect(
                        &s.upstream_http_client,
                        &a.access_token,
                        Some(&a.id),
                        account_body,
                        wire_id,
                        redact,
                    )
                    .await
                };
                result.map_err(|e| (e.status, e.message))
            } else {
                let connection = if let Some(url) = endpoint {
                    crate::upstream::connect_to(
                        url,
                        &a.access_token,
                        Some(&a.id),
                        s.config.server.proxy.as_deref(),
                        account_body.get("prompt_cache_key").and_then(Value::as_str),
                        wire_id,
                        redact,
                    )
                    .await
                } else {
                    crate::upstream::connect(
                        &a.access_token,
                        Some(&a.id),
                        s.config.server.proxy.as_deref(),
                        account_body.get("prompt_cache_key").and_then(Value::as_str),
                        wire_id,
                        redact,
                    )
                    .await
                };
                let mut ws = connection.map_err(|e| (e.status, e.to_string()))?;
                // A transport failure after sending is ambiguous: never replay it.
                crate::upstream::start(&mut ws, account_body, wire_id)
                    .await
                    .map_err(|e| (None, e))?;
                let stream: crate::upstream::http::UpstreamEvents =
                    Box::pin(ws.map(move |item| match item {
                        Ok(Message::Text(text)) => {
                            if crate::logger::wire_debug_enabled() {
                                tracing::debug!(
                                    target: "wire",
                                    id = wire_id,
                                    "UPSTREAM_WSS <- RECV {text}"
                                );
                            }
                            Ok(Message::Text(text))
                        }
                        Ok(other) => Ok(other),
                        Err(err) => {
                            if crate::logger::wire_debug_enabled() {
                                tracing::debug!(
                                    target: "wire",
                                    id = wire_id,
                                    "UPSTREAM_WSS <- STREAM_ERROR {err}"
                                );
                            }
                            Err("upstream WebSocket stream failed".to_owned())
                        }
                    }));
                Ok(stream)
            }
        })
        .await;
        let mut ws = match attempt {
            Ok(Ok(ws)) => ws,
            Ok(Err((Some(401 | 403 | 429), _))) => {
                let _ = s
                    .accounts
                    .cooldown(&a.id, s.config.rotation.cooldown_ms.div_ceil(1000))
                    .await;
                let _ = s
                    .logs
                    .send("Upstream rejected an account; trying another".into());
                continue;
            }
            Ok(Err((Some(400), message))) => {
                return Err(UpstreamOpenError {
                    status: StatusCode::BAD_REQUEST,
                    message,
                });
            }
            _ => return Err("Upstream connection failed".into()),
        };
        let first = tokio::time::timeout(timeout, async {
            let mut buffered = Vec::new();
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let event: Value =
                            serde_json::from_str(&text).map_err(|_| "invalid upstream event")?;
                        // Gateway metadata never commits downstream headers or
                        // becomes part of either public response protocol.
                        if gateway_metadata(&event) {
                            continue;
                        }
                        if matches!(
                            event.get("type").and_then(Value::as_str),
                            Some("response.created" | "response.in_progress")
                        ) {
                            if buffered.len() >= 32 {
                                return Err("upstream sent too many bootstrap events");
                            }
                            buffered.push(text.to_string());
                            continue;
                        }
                        // Keep native bootstrap events for ordered replay only
                        // after a non-bootstrap event establishes the result.
                        buffered.push(text.to_string());
                        return Ok::<_, &str>((buffered, event));
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                    _ => return Err("upstream stream interrupted"),
                }
            }
        })
        .await
        .map_err(|_| "upstream response timed out".to_string())?
        .map_err(str::to_string)?;
        if account_failure(&first.1) {
            let _ = s
                .accounts
                .cooldown(&a.id, s.config.rotation.cooldown_ms.div_ceil(1000))
                .await;
            let _ = s
                .logs
                .send("Upstream rejected an account; trying another".into());
            continue;
        }
        if let Some(message) = client_request_failure(&first.1) {
            // Reject before committing HTTP/SSE headers. A request error is
            // terminal and says nothing about this account's health.
            return Err(UpstreamOpenError {
                status: StatusCode::BAD_REQUEST,
                message: message.to_owned(),
            });
        }
        if matches!(
            first.1.get("type").and_then(Value::as_str),
            Some("error" | "response.failed")
        ) {
            return Err("Upstream response failed".into());
        }
        tracing::debug!(target: "proxy", "Connected upstream");
        return Ok((ws, first.0));
    }
    Err("No account could connect to the Codex upstream".into())
}

pub(super) async fn chat(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    extensions: axum::http::Extensions,
    Json(body): Json<Value>,
) -> Response {
    if !authorized(&headers, &s) {
        return error(StatusCode::UNAUTHORIZED, "invalid API key");
    }
    let wire_id = extensions
        .get::<crate::logger::WireRequestId>()
        .map(|w| w.0)
        .unwrap_or_else(crate::logger::next_request_id);
    let mut request_log = RequestMetrics::new(
        s.logs.clone(),
        "chat",
        body.get("model")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    );
    let mut upstream_body = match chat_to_responses(&body) {
        Ok(b) => b,
        Err(e) => return validation_error(&s, "chat", &e),
    };
    add_automatic_cache_key(
        &mut upstream_body,
        &headers,
        s.config.server.auto_prompt_cache,
    );
    if let Err(e) = crate::upstream::prepare_body(upstream_body.clone()) {
        return validation_error(&s, "chat", &e);
    }
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let include_usage = body
        .pointer("/stream_options/include_usage")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let (ws, buffered) = match open_upstream(&s, upstream_body, wire_id).await {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!(target: "proxy", route = "chat", status = %e.status, "Upstream request could not start; token usage may be unavailable");
            return error(e.status, &e.message);
        }
    };
    let ws = futures_util::stream::iter(buffered.into_iter().map(Message::text).map(Ok)).chain(ws);
    let timeout = Duration::from_millis(s.config.retry.timeout_ms);
    if streaming {
        let stream = async_stream::stream! {
            let mut ws=ws; let mut converter=ChatConverter::new(model,include_usage); let mut request_log=request_log;
            loop {
                let next=tokio::time::timeout(timeout,ws.next()).await;
                let text=match next { Ok(Some(Ok(Message::Text(t))))=>t, Ok(Some(Ok(Message::Ping(_))))|Ok(Some(Ok(Message::Pong(_))))=>continue, _=>{let e=json!({"error":{"message":"upstream stream interrupted","type":"upstream_error"}}); yield Ok::<Bytes,Infallible>(Bytes::from(format!("data: {e}\n\n"))); break;} };
                let event:Value=match serde_json::from_str(&text){Ok(v)=>v,Err(_)=>{let e=json!({"error":{"message":"invalid upstream event","type":"upstream_error"}});yield Ok(Bytes::from(format!("data: {e}\n\n")));break;}};
                if gateway_metadata(&event) {continue;}
                request_log.observe(&event);
                for chunk in converter.on_event(&event) { yield Ok(Bytes::from(format!("data: {chunk}\n\n"))); }
                if let Some(e)=converter.failure(){request_log.fail();let e=json!({"error":{"message":e,"type":"upstream_error"}});yield Ok(Bytes::from(format!("data: {e}\n\n")));break;}
                if converter.finished(){yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));break;}
            }
        };
        return (
            [
                (header::CONTENT_TYPE, "text/event-stream"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            Body::from_stream(stream),
        )
            .into_response();
    }
    let mut ws = ws;
    let mut converter = ChatConverter::new(model, include_usage);
    loop {
        match tokio::time::timeout(timeout, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let event: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return error(StatusCode::BAD_GATEWAY, "invalid upstream event"),
                };
                if gateway_metadata(&event) {
                    continue;
                }
                request_log.observe(&event);
                converter.on_event(&event);
                if let Some(e) = converter.failure() {
                    request_log.fail();
                    return error(StatusCode::BAD_GATEWAY, &e);
                }
                if converter.finished() {
                    return match converter.final_response() {
                        Ok(v) => Json(v).into_response(),
                        Err(e) => {
                            request_log.fail();
                            error(StatusCode::BAD_GATEWAY, &e)
                        }
                    };
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => continue,
            _ => return error(StatusCode::BAD_GATEWAY, "upstream stream interrupted"),
        }
    }
}

pub(super) async fn responses(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    extensions: axum::http::Extensions,
    Json(mut body): Json<Value>,
) -> Response {
    if !authorized(&headers, &s) {
        return error(StatusCode::UNAUTHORIZED, "invalid API key");
    }
    let wire_id = extensions
        .get::<crate::logger::WireRequestId>()
        .map(|w| w.0)
        .unwrap_or_else(crate::logger::next_request_id);
    let mut request_log = RequestMetrics::new(
        s.logs.clone(),
        "responses",
        body.get("model")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    );
    let Some(obj) = body.as_object_mut() else {
        return validation_error(&s, "responses", "request must be an object");
    };
    if obj.get("previous_response_id").is_some() {
        return validation_error(
            &s,
            "responses",
            "previous_response_id is not supported without session affinity",
        );
    }
    let streaming = obj.get("stream").and_then(Value::as_bool).unwrap_or(false);
    obj.insert("stream".into(), Value::Bool(true));
    obj.insert("store".into(), Value::Bool(false));
    body = match crate::upstream::prepare_body(body) {
        Ok(body) => body,
        Err(e) => return validation_error(&s, "responses", &e),
    };
    add_automatic_cache_key(&mut body, &headers, s.config.server.auto_prompt_cache);
    let (ws, buffered) = match open_upstream(&s, body, wire_id).await {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!(target: "proxy", route = "responses", status = %e.status, "Upstream request could not start; token usage may be unavailable");
            return error(e.status, &e.message);
        }
    };
    let ws = futures_util::stream::iter(buffered.into_iter().map(Message::text).map(Ok)).chain(ws);
    let timeout = Duration::from_millis(s.config.retry.timeout_ms);
    if streaming {
        let stream = async_stream::stream! {
            let mut ws=ws; let mut request_log=request_log;
            loop { let text=match tokio::time::timeout(timeout,ws.next()).await {Ok(Some(Ok(Message::Text(t))))=>t,Ok(Some(Ok(Message::Ping(_))))|Ok(Some(Ok(Message::Pong(_))))=>continue,_=>{let e=json!({"type":"error","error":{"message":"upstream stream interrupted"}});yield Ok::<Bytes,Infallible>(Bytes::from(format!("event: error\ndata: {e}\n\n")));break;}};
                let event:Value=match serde_json::from_str(&text){Ok(v)=>v,Err(_)=>{let e=json!({"type":"error","error":{"message":"invalid upstream event"}});yield Ok(Bytes::from(format!("event: error\ndata: {e}\n\n")));break;}};
                let kind=event.get("type").and_then(Value::as_str).unwrap_or("message");
                if gateway_metadata(&event) {continue;}
                request_log.observe(&event);
                let terminal = ["response.completed","response.failed","response.incomplete","error"].contains(&kind);
                yield Ok(Bytes::from(format!("event: {kind}\ndata: {event}\n\n")));
                if terminal {break;}
            }
        };
        return (
            [
                (header::CONTENT_TYPE, "text/event-stream"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            Body::from_stream(stream),
        )
            .into_response();
    }
    let mut ws = ws;
    loop {
        match tokio::time::timeout(timeout, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let event: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return error(StatusCode::BAD_GATEWAY, "invalid upstream event"),
                };
                if gateway_metadata(&event) {
                    continue;
                }
                request_log.observe(&event);
                match event.get("type").and_then(Value::as_str) {
                    Some("response.completed") | Some("response.incomplete") => {
                        let Some(response) = event.get("response").filter(|v| v.is_object()) else {
                            request_log.fail();
                            return error(
                                StatusCode::BAD_GATEWAY,
                                "upstream terminal event missing response",
                            );
                        };
                        return Json(response.clone()).into_response();
                    }
                    Some("response.failed") | Some("error") => {
                        return error(StatusCode::BAD_GATEWAY, "upstream response failed")
                    }
                    _ => {}
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => continue,
            _ => return error(StatusCode::BAD_GATEWAY, "upstream stream interrupted"),
        }
    }
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod retry_tests;
