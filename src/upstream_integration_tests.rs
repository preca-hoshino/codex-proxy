#![allow(clippy::result_large_err)] // WebSocket handshake callback has a fixed external error type.
//! End-to-end route tests against a local, simulated WebSocket upstream.
//! No external services or real credentials are used.
use crate::{account::AccountManager, config::AppConfig, server, AppState};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

async fn mock_upstream(
    events: Vec<Value>,
) -> (
    String,
    oneshot::Receiver<Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (send_request, request) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let session = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
        let captured_session = session.clone();
        let mut ws = tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
             response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                assert_eq!(
                    request.headers().get("authorization").unwrap(),
                    "Bearer test-access-token"
                );
                assert_eq!(
                    request.headers().get("chatgpt-account-id").unwrap(),
                    "test-account"
                );
                assert_eq!(
                    request.headers().get("openai-beta").unwrap(),
                    "responses_websockets=2026-02-06"
                );
                *captured_session.lock().unwrap() = request.headers().get("session-id").map(|value| value.to_str().unwrap().to_owned());
                Ok(response)
            },
        )
        .await
        .unwrap();
        let message = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let request: Value = serde_json::from_str(&message).unwrap();
        assert_eq!(
            session.lock().unwrap().as_deref(),
            request.get("prompt_cache_key").and_then(Value::as_str)
        );
        let _ = send_request.send(request);
        for event in events {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
    });
    (
        format!("ws://{addr}/backend-api/codex/responses"),
        request,
        task,
    )
}

async fn call_proxy(path: &str, input: Value, events: Vec<Value>) -> (StatusCode, String, Value) {
    let (upstream, upstream_request, task) = mock_upstream(events).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = AppConfig::default();
    config.server.accounts_file = dir.path().join("accounts.json");
    config.server.api_key = Some("local-secret".into());
    config.retry.timeout_ms = 5000;
    config.retry.max_attempts = 1;
    let client = reqwest::Client::new();
    let accounts = AccountManager::open(
        config.server.accounts_file.clone(),
        client.clone(),
        config.rotation.strategy.clone(),
    )
    .unwrap();
    accounts
        .add_token_response(
            &json!({"access_token":"test-access-token", "account_id":"test-account"}),
        )
        .await
        .unwrap();
    let (logs, _) = broadcast::channel(16);
    let state = Arc::new(AppState {
        accounts,
        config: Arc::new(config),
        upstream_http_client: reqwest::Client::new(),
        logs,
        test_upstream_url: Some(upstream),
    });
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("authorization", "Bearer local-secret")
        .body(Body::from(input.to_string()))
        .unwrap();
    let response = server::router(state).oneshot(request).await.unwrap();
    let status = response.status();
    let body = String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let sent = upstream_request.await.unwrap();
    task.await.unwrap();
    (status, body, sent)
}

fn completed_text() -> Vec<Value> {
    vec![
        json!({"type":"response.created","response":{"id":"resp_test"}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}),
        json!({"type":"response.completed","response":{"id":"resp_test","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}),
    ]
}
fn chat_request(stream: bool) -> Value {
    json!({"model":"test-model","messages":[{"role":"user","content":"Hi"}],"stream":stream})
}

#[tokio::test]
async fn chat_stream_flows_over_websocket_and_finishes() {
    let (status, body, sent) =
        call_proxy("/v1/chat/completions", chat_request(true), completed_text()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sent["type"], "response.create");
    assert_eq!(sent["stream"], true);
    assert_eq!(sent["store"], false);
    assert_eq!(sent["model"], "test-model");
    assert!(body.contains("\"content\":\"hello\""), "{body}");
    assert!(body.contains("\"finish_reason\":\"stop\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn chat_nonstream_aggregates_events() {
    let (status, body, _) = call_proxy(
        "/v1/chat/completions",
        chat_request(false),
        completed_text(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "hello");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["usage"]["total_tokens"], 5);
}

async fn check_multiturn_history_with_cot_metadata(stream: bool) {
    let arguments = r#"{"query":"history lookup","cot_id":"keep-this-tool-argument","reasoning":"keep-this-business-argument"}"#;
    let tool_output = r#"{"result":"found","cot_id":"keep-this-tool-output"}"#;
    let system_text = "Keep the original conversation, including the literal key cot_id.";
    let first_user = "Look up history for cot_id, then tell me what you found.";
    let tool_preface = "I will look up the requested history.";
    let followup_user = "What did the tool return?";
    let historic_answer = r#"It returned {"cot_id":"keep-this-assistant-body"}."#;
    let final_user = "Continue from that result and preserve cot_id in quoted data.";
    let parameters = json!({
        "type":"object",
        "properties":{
            "query":{"type":"string"},
            "cot_id":{"type":"string"},
            "reasoning":{"type":"string"}
        },
        "required":["query","cot_id","reasoning"]
    });
    let request = json!({
        "model":"test-model",
        "stream":stream,
        "messages":[
            {"role":"system","content":system_text},
            {"role":"user","content":first_user},
            {
                "role":"assistant",
                "content":tool_preface,
                "tool_calls":[{
                    "id":"call_history",
                    "type":"function",
                    "function":{"name":"lookup_history","arguments":arguments}
                }]
            },
            {"role":"tool","tool_call_id":"call_history","content":tool_output},
            {"role":"user","content":followup_user},
            {
                "role":"assistant",
                "content":historic_answer,
                "cot_id":"vscode-assistant-turn-metadata",
                "cot_summary":"historical chain-of-thought summary metadata",
                "reasoning_content":"historical thinking metadata",
                "reasoning":{"id":"historical-reasoning-item"}
            },
            {"role":"user","content":final_user}
        ],
        "tools":[{
            "type":"function",
            "function":{"name":"lookup_history","parameters":parameters}
        }]
    });

    let (status, body, sent) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        call_proxy("/v1/chat/completions", request, completed_text()),
    )
    .await
    .expect("multi-turn request should reach the local WebSocket upstream");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(sent["type"], "response.create");
    // Both Chat modes use the streaming Responses WebSocket upstream.
    assert_eq!(sent["stream"], true);
    assert_eq!(
        sent["input"],
        json!([
            {"type":"message","role":"developer","content":[{"type":"input_text","text":system_text}]},
            {"type":"message","role":"user","content":[{"type":"input_text","text":first_user}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":tool_preface}]},
            {"type":"function_call","call_id":"call_history","name":"lookup_history","arguments":arguments},
            {"type":"function_call_output","call_id":"call_history","output":tool_output},
            {"type":"message","role":"user","content":[{"type":"input_text","text":followup_user}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":historic_answer}]},
            {"type":"message","role":"user","content":[{"type":"input_text","text":final_user}]}
        ]),
        "history and function calls must survive; message metadata must not be forwarded"
    );
    assert!(sent["input"].as_array().unwrap().iter().all(|item| [
        "cot_id",
        "cot_summary",
        "reasoning_content",
        "reasoning"
    ]
    .iter()
    .all(|key| item.get(key).is_none())));
    assert_eq!(sent["tools"][0]["parameters"], parameters);
    assert!(sent["tools"][0]["parameters"]["properties"]
        .get("cot_id")
        .is_some());
    assert!(sent["tools"][0]["parameters"]["properties"]
        .get("reasoning")
        .is_some());
    if stream {
        assert!(body.contains("\"content\":\"hello\""), "{body}");
        assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    } else {
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["choices"][0]["message"]["content"], "hello");
        assert_eq!(body["choices"][0]["finish_reason"], "stop");
    }
}

#[tokio::test]
async fn chat_multiturn_stream_preserves_history_and_strips_message_cot_id() {
    check_multiturn_history_with_cot_metadata(true).await;
}

#[tokio::test]
async fn chat_multiturn_nonstream_preserves_history_and_strips_message_cot_id() {
    check_multiturn_history_with_cot_metadata(false).await;
}

#[tokio::test]
async fn chat_stream_translates_multiple_tools() {
    let events = vec![
        json!({"type":"response.created","response":{"id":"resp_test"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_one","name":"first","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":r#"{"x":1}"#}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_two","name":"second","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"delta":"{}"}),
        json!({"type":"response.completed","response":{"status":"completed","output":[]}}),
    ];
    let (status, body, _) = call_proxy("/v1/chat/completions", chat_request(true), events).await;
    assert_eq!(status, StatusCode::OK);
    let chunks: Vec<Value> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let calls: Vec<&Value> = chunks
        .iter()
        .filter_map(|v| v.pointer("/choices/0/delta/tool_calls/0"))
        .collect();
    assert!(calls
        .iter()
        .any(|c| c["index"] == 0 && c["id"] == "call_one"));
    assert!(calls
        .iter()
        .any(|c| c["index"] == 1 && c["id"] == "call_two"));
    assert!(chunks
        .iter()
        .any(|v| v.pointer("/choices/0/finish_reason") == Some(&json!("tool_calls"))));
    assert!(
        calls
            .iter()
            .any(|c| c["index"] == 0
                && c.pointer("/function/arguments") == Some(&json!(r#"{"x":1}"#)))
    );
    assert!(calls
        .iter()
        .any(|c| c["index"] == 1 && c.pointer("/function/arguments") == Some(&json!("{}"))));
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn chat_interrupted_stream_reports_error_without_done() {
    let events = vec![
        json!({"type":"response.created"}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}),
    ];
    let (status, body, _) = call_proxy("/v1/chat/completions", chat_request(true), events).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("partial"), "{body}");
    assert!(body.contains("upstream stream interrupted"), "{body}");
    assert!(!body.contains("[DONE]"), "{body}");
}

#[tokio::test]
async fn chat_failed_stream_reports_error_without_done() {
    let events = vec![
        json!({"type":"response.created"}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}),
        json!({"type":"response.failed","response":{"status":"failed","error":{"message":"quota exhausted"}}}),
    ];
    let (status, body, _) = call_proxy("/v1/chat/completions", chat_request(true), events).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("partial"), "{body}");
    assert!(body.contains("quota exhausted"), "{body}");
    assert!(!body.contains("[DONE]"), "{body}");
}

#[tokio::test]
async fn responses_stream_translates_websocket_events_to_sse() {
    let (status, body, sent) = call_proxy(
        "/v1/responses",
        json!({"model":"test-model","input":"Hi","stream":true}),
        completed_text(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sent["type"], "response.create");
    assert_eq!(sent["input"][0]["content"][0]["text"], "Hi");
    assert!(body.contains("event: response.output_text.delta\n"));
    assert!(body.contains("event: response.completed\n"));
}

#[tokio::test]
async fn responses_nonstream_returns_terminal_response() {
    let (status, body, _) = call_proxy(
        "/v1/responses",
        json!({"model":"test-model","input":"Hi","stream":false}),
        completed_text(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(response["id"], "resp_test");
    assert_eq!(response["output"][0]["content"][0]["text"], "hello");
}

#[tokio::test]
async fn chat_reasoning_override_preserves_summary_over_websocket() {
    let request = json!({
        "model":"test-model",
        "messages":[{"role":"user","content":"keep the body"}],
        "reasoning":{"effort":"low","summary":"detailed"},
        "reasoning_effort":"high",
        "stream":false
    });
    let (status, body, sent) = call_proxy("/v1/chat/completions", request, completed_text()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        sent["reasoning"],
        json!({"effort":"high","summary":"detailed"})
    );
    assert_eq!(sent["model"], "test-model");
    assert_eq!(sent["input"][0]["content"][0]["text"], "keep the body");
    assert_eq!(sent["store"], false);
    assert_eq!(sent["stream"], true);
    assert_eq!(sent["type"], "response.create");
}

#[tokio::test]
async fn websocket_cache_key_and_real_cached_usage_survive_both_routes() {
    for route in ["/v1/chat/completions", "/v1/responses"] {
        let mut payload = if route.contains("chat") {
            chat_request(false)
        } else {
            json!({"model":"test-model","input":"Hi","stream":false})
        };
        payload["prompt_cache_key"] = json!("stable-conversation");
        let mut events = completed_text();
        events.last_mut().unwrap()["response"]["usage"] = json!({
            "input_tokens":2048,"output_tokens":2,"total_tokens":2050,
            "input_tokens_details":{"cached_tokens":1920}
        });
        let (status, body, sent) = call_proxy(route, payload, events).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(sent["prompt_cache_key"], "stable-conversation");
        let response: Value = serde_json::from_str(&body).unwrap();
        let details = if route.contains("chat") {
            "prompt_tokens_details"
        } else {
            "input_tokens_details"
        };
        assert_eq!(response["usage"][details]["cached_tokens"], 1920);
    }
}
