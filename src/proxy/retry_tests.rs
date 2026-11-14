#![allow(clippy::result_large_err)] // WebSocket handshake callback has a fixed external error type.
use super::*;
use crate::{account::AccountManager, config::AppConfig, proxy, AppState};
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::broadcast};
use tower::ServiceExt;

#[test]
fn automatic_cache_key_is_stable_and_preserves_explicit_preferences() {
    fn key(mut body: Value, headers: &HeaderMap, enabled: bool) -> Value {
        add_automatic_cache_key(&mut body, headers, enabled);
        body
    }
    let headers = HeaderMap::new();
    let first = json!({"model":"test","input":[{"role":"user","content":[{"type":"input_text","text":"private first message"}]}]});
    let mut later = first.clone();
    later["input"]
        .as_array_mut()
        .unwrap()
        .push(json!({"role":"user","content":"next turn"}));
    let generated = key(first.clone(), &headers, true)["prompt_cache_key"].clone();
    assert_eq!(generated, key(later, &headers, true)["prompt_cache_key"]);
    assert!(!generated.as_str().unwrap().contains("private"));
    let mut different = first.clone();
    different["input"][0]["content"][0]["text"] = json!("different");
    assert_ne!(
        generated,
        key(different, &headers, true)["prompt_cache_key"]
    );
    let mut different_model = first.clone();
    different_model["model"] = json!("other");
    assert_ne!(
        generated,
        key(different_model, &headers, true)["prompt_cache_key"]
    );
    let mut session_headers = HeaderMap::new();
    session_headers.insert("session-id", "stable-session".parse().unwrap());
    let session_key = key(first.clone(), &session_headers, true)["prompt_cache_key"].clone();
    assert!(session_key
        .as_str()
        .is_some_and(|key| key.starts_with("codex-proxy-")));
    let other = json!({"model":"test","input":[{"role":"user","content":"different"}]});
    assert_eq!(
        session_key,
        key(other, &session_headers, true)["prompt_cache_key"]
    );
    let mut explicit = first.clone();
    explicit["prompt_cache_key"] = json!("client-key");
    assert_eq!(
        key(explicit, &session_headers, true)["prompt_cache_key"],
        "client-key"
    );
    assert!(key(first, &headers, false)
        .get("prompt_cache_key")
        .is_none());
    let image_only = json!({"model":"test","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,abc"}]}]});
    assert!(key(image_only, &headers, true)
        .get("prompt_cache_key")
        .is_none());
}

async fn prepare(endpoint: String) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = AppConfig::default();
    config.server.accounts_file = dir.path().join("accounts.json");
    config.retry.max_attempts = 2;
    config.retry.timeout_ms = 1000;
    let client = reqwest::Client::new();
    let accounts = AccountManager::open(
        config.server.accounts_file.clone(),
        client.clone(),
        "priority".into(),
    )
    .unwrap();
    for n in 1..=2 {
        accounts.add_token_response(&json!({"account_id":format!("account-{n}"), "access_token":format!("test-token-{n}")})).await.unwrap();
    }
    let (logs, _) = broadcast::channel(16);
    let state = Arc::new(AppState {
        accounts,
        config: Arc::new(config),
        upstream_http_client: reqwest::Client::new(),
        logs,
        test_upstream_url: Some(endpoint),
    });
    (dir, state)
}

fn request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":"test", "messages":[{"role":"user","content":"hello"}], "stream":true})
                .to_string(),
        ))
        .unwrap()
}
async fn body(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn preflight_429_retries_and_cools_only_rejected_account() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for n in 1..=2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(stream, move |req: &tokio_tungstenite::tungstenite::handshake::server::Request, res: tokio_tungstenite::tungstenite::handshake::server::Response| {
                assert_eq!(req.headers()["chatgpt-account-id"], format!("account-{n}")); Ok(res)
            }).await.unwrap();
            let sent: Value =
                serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap())
                    .unwrap();
            assert_eq!(sent["reasoning"]["effort"], "high");
            let event = if n == 1 {
                json!({"type":"error","status":429,"error":{"code":"rate_limit_exceeded"}})
            } else {
                json!({"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":3,"output_tokens":2}}})
            };
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
    });
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    let mut logs = state.logs.subscribe();
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":"test", "messages":[{"role":"user","content":"private prompt"}], "reasoning_effort":"high", "stream":true})
                .to_string(),
        ))
        .unwrap();
    let response = proxy::server::router(state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = body(response).await;
    assert!(text.contains("[DONE]"), "{text}");
    server.await.unwrap();
    let accounts = state.accounts.list().await;
    assert_eq!(accounts[0].status, "cooldown");
    assert_eq!(accounts[1].status, "ready");
    let entries: Vec<String> = std::iter::from_fn(|| logs.try_recv().ok()).collect();
    assert!(entries
        .iter()
        .any(|entry| entry == "Upstream rejected an account; trying another"));
    assert!(entries.iter().all(|entry| entry != "Connected upstream"));
    assert!(
        entries.iter().any(|entry| entry.contains("[METRICS]")
            && entry.contains("Status: completed")
            && entry.contains("Prompt: 3")
            && entry.contains("Output: 2")),
        "{entries:?}"
    );
    assert!(
        entries
            .iter()
            .all(|entry| !entry.contains("test-token") && !entry.contains("account-1")),
        "{entries:?}"
    );
    let requests: Vec<_> = entries
        .iter()
        .filter(|line| line.contains("Sending request to Codex upstream"))
        .collect();
    assert_eq!(requests.len(), 2, "{entries:?}");
    for (index, line) in requests.iter().enumerate() {
        for field in [
            "model=\"test\"",
            "reasoning_effort=\"high\"",
            "account=\"unknown\"",
            "transport=wss",
            "max_attempts=2",
        ] {
            assert!(line.contains(field), "{line}");
        }
        assert!(line.contains(&format!("attempt={}", index + 1)), "{line}");
    }
    for private in ["private prompt", "test-token", "account-1"] {
        assert!(
            entries.iter().all(|line| !line.contains(private)),
            "{entries:?}"
        );
    }
}

#[tokio::test]
async fn handshake_unauthorized_retries_and_cools_account() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let result = tokio_tungstenite::accept_hdr_async(
            stream,
            |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
             _: tokio_tungstenite::tungstenite::handshake::server::Response| {
                Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(401)
                    .body(Some("unauthorized".into()))
                    .unwrap())
            },
        )
        .await;
        assert!(result.is_err());
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            json!({"type":"response.completed","response":{"status":"completed","output":[]}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    });
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    let response = proxy::server::router(state.clone())
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body(response).await.contains("[DONE]"));
    server.await.unwrap();
    let accounts = state.accounts.list().await;
    assert_eq!(accounts[0].status, "cooldown");
    assert_eq!(accounts[1].status, "ready");
}

#[tokio::test]
async fn network_failure_does_not_cool_any_account() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    let response = proxy::server::router(state.clone())
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(state
        .accounts
        .list()
        .await
        .iter()
        .all(|a| a.status == "ready"));
}

#[tokio::test]
async fn invalid_cache_key_is_rejected_before_account_selection_or_network_io() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    for route in ["/v1/chat/completions", "/v1/responses"] {
        for key in [json!(42), json!("bad\r\nheader")] {
            let mut payload = if route.contains("chat") {
                json!({"model":"test", "messages":[{"role":"user","content":"hello"}]})
            } else {
                json!({"model":"test", "input":"hello"})
            };
            payload["prompt_cache_key"] = key;
            let request = Request::builder()
                .method("POST")
                .uri(route)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap();
            let response = proxy::server::router(state.clone())
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(body(response).await.contains("prompt_cache_key"));
        }
    }
    assert!(state
        .accounts
        .list()
        .await
        .iter()
        .all(|account| account.last_used == 0));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn locally_rejected_chat_request_reports_safe_reason_and_does_not_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (_dir, state) = prepare(format!("ws://{}/responses", listener.local_addr().unwrap())).await;
    let mut logs = state.logs.subscribe();
    let payload = json!({"model":"test","messages":[{"role":"user","content":"private prompt"}],"secret-field-name":"private value"});
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = proxy::server::router(state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let messages: Vec<_> = std::iter::from_fn(|| logs.try_recv().ok()).collect();
    assert!(
        messages.iter().any(|message| message.contains("[REJECTED]")
            && message.contains("route=chat")
            && message.contains("reason=unsupported_request_parameter")),
        "{messages:?}"
    );
    assert!(
        messages.iter().any(|message| message.contains("[METRICS]")
            && message.contains("Status: failed")
            && message.contains("Total Tokens: unknown")),
        "{messages:?}"
    );
    assert!(
        messages
            .iter()
            .all(|message| !message.contains("private") && !message.contains("secret-field-name")),
        "{messages:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn invalid_request_is_http_400_before_streaming_without_retry_or_account_cooldown() {
    for route in ["/v1/chat/completions", "/v1/responses"] {
        for streaming in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let message = "Unsupported value: 'minimal' is not supported with this model.";
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                ws.next().await.unwrap().unwrap();
                ws.send(Message::Text(
                    json!({"type":"codex.response.metadata"}).to_string().into(),
                ))
                .await
                .unwrap();
                for kind in ["response.created", "response.in_progress"] {
                    ws.send(Message::Text(
                        json!({"type":kind,"response":{"id":"resp_test"}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                }
                ws.send(Message::Text(
                    json!({"type":"codex.rate_limits"}).to_string().into(),
                ))
                .await
                .unwrap();
                ws.send(Message::Text(json!({
                    "type":"error", "status":400,
                    "error":{"code":"unsupported_value","type":"invalid_request_error","param":"reasoning.effort","message":message},
                    "internal_details":"must-not-be-forwarded"
                }).to_string().into())).await.unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_millis(200), listener.accept())
                        .await
                        .is_err(),
                    "invalid requests must use exactly one upstream connection"
                );
            });
            let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
            let payload = if route == "/v1/chat/completions" {
                json!({"model":"test","messages":[{"role":"user","content":"hello"}],"reasoning_effort":"minimal","stream":streaming})
            } else {
                json!({"model":"test","input":"hello","reasoning":{"effort":"minimal"},"stream":streaming})
            };
            let request = Request::builder()
                .method("POST")
                .uri(route)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap();
            let response = proxy::server::router(state.clone())
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{route}, stream={streaming}"
            );
            assert_eq!(response.headers()["content-type"], "application/json");
            let text = body(response).await;
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["error"]["message"], message);
            assert!(!text.contains("must-not-be-forwarded"));
            assert!(!text.contains("data:"));
            server.await.unwrap();
            let accounts = state.accounts.list().await;
            assert!(accounts.iter().all(|account| account.status == "ready"));
            assert!(accounts
                .iter()
                .all(|account| account.cooldown_until.is_none()));
            assert!(accounts[0].last_used > 0);
            assert_eq!(accounts[1].last_used, 0);
        }
    }
}

#[tokio::test]
async fn bootstrap_events_are_replayed_in_order_and_gateway_metadata_is_never_forwarded() {
    for route in ["/v1/chat/completions", "/v1/responses"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.next().await.unwrap().unwrap();
            for event in [
                json!({"type":"codex.response.metadata","internal":"private-preflight-metadata"}),
                json!({"type":"response.created","response":{"id":"resp_test"}}),
                json!({"type":"response.in_progress","response":{"id":"resp_test"}}),
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}),
                json!({"type":"codex.response.metadata","internal":"private-stream-metadata"}),
                json!({"type":"codex.rate_limits","internal":"private-stream-limits"}),
                json!({"type":"response.completed","response":{"id":"resp_test","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}]}}),
            ] {
                ws.send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
        let payload = if route == "/v1/chat/completions" {
            json!({"model":"test","messages":[{"role":"user","content":"hello"}],"stream":true})
        } else {
            json!({"model":"test","input":"hello","stream":true})
        };
        let request = Request::builder()
            .method("POST")
            .uri(route)
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        let response = proxy::server::router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let text = body(response).await;
        assert!(text.contains("hello"));
        assert!(!text.contains("codex.response.metadata"));
        assert!(!text.contains("codex.rate_limits"));
        assert!(!text.contains("private-"));
        if route == "/v1/responses" {
            let created = text.find("event: response.created\n").unwrap();
            let progress = text.find("event: response.in_progress\n").unwrap();
            let delta = text.find("event: response.output_text.delta\n").unwrap();
            let completed = text.find("event: response.completed\n").unwrap();
            assert!(created < progress && progress < delta && delta < completed);
        } else {
            assert!(text.contains("[DONE]"));
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn bootstrap_buffer_limit_returns_bad_gateway_without_retry_or_cooldown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.next().await.unwrap().unwrap();
        for _ in 0..33 {
            ws.send(Message::Text(
                json!({"type":"response.created"}).to_string().into(),
            ))
            .await
            .unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    let response = proxy::server::router(state.clone())
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(body(response).await.contains("too many bootstrap events"));
    server.await.unwrap();
    assert!(state
        .accounts
        .list()
        .await
        .iter()
        .all(|account| account.status == "ready"));
}

#[tokio::test]
async fn unknown_first_error_is_still_bad_gateway_before_streaming() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(json!({"type":"error","status":400,"error":{"code":"unknown_error","message":"internal-detail"}}).to_string().into())).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    let response = proxy::server::router(state.clone())
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let text = body(response).await;
    assert!(!text.contains("internal-detail"));
    assert!(!text.contains("data:"));
    server.await.unwrap();
    assert!(state
        .accounts
        .list()
        .await
        .iter()
        .all(|account| account.status == "ready"));
}

#[tokio::test]
async fn interruption_after_first_event_does_not_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            json!({"type":"response.created"}).to_string().into(),
        ))
        .await
        .unwrap();
        drop(ws);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let (_dir, state) = prepare(format!("ws://{addr}/responses")).await;
    let response = proxy::server::router(state.clone())
        .oneshot(request())
        .await
        .unwrap();
    let text = body(response).await;
    assert!(text.contains("upstream stream interrupted"), "{text}");
    assert!(!text.contains("[DONE]"));
    server.await.unwrap();
    assert!(state
        .accounts
        .list()
        .await
        .iter()
        .all(|a| a.status == "ready"));
}
