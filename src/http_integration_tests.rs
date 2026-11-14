//! End-to-end HTTP transport checks using only a local simulated upstream.
use std::{collections::VecDeque, sync::Arc};

use axum::{
    body::{to_bytes, Body},
    extract::{Json, State},
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use serde_json::{json, Value};
use tokio::{
    net::TcpListener,
    sync::{broadcast, Mutex},
};
use tower::ServiceExt;

use crate::{account::AccountManager, config::AppConfig, server, AppState};

#[derive(Clone)]
struct Mock {
    replies: Arc<Mutex<VecDeque<(StatusCode, String)>>>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn upstream(
    State(mock): State<Mock>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let account = headers
        .get("chatgpt-account-id")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_owned();
    assert_eq!(headers["accept"], "text/event-stream");
    assert_eq!(
        headers
            .get("session-id")
            .map(|value| value.to_str().unwrap()),
        body.get("prompt_cache_key").and_then(Value::as_str)
    );
    mock.requests.lock().await.push((account, body));
    let (status, body) = mock
        .replies
        .lock()
        .await
        .pop_front()
        .expect("unexpected extra upstream request");
    (status, [("content-type", "text/event-stream")], body).into_response()
}

fn sse(events: &[Value], done: bool) -> String {
    let mut result = String::new();
    for event in events {
        result.push_str(&format!("data: {event}\n\n"));
    }
    if done {
        result.push_str("data: [DONE]\n\n");
    }
    result
}

fn complete() -> String {
    sse(
        &[
            json!({"type":"response.created","response":{"id":"resp_local"}}),
            json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}),
            json!({"type":"response.completed","response":{"id":"resp_local","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}),
        ],
        true,
    )
}

struct Fixture {
    _dir: tempfile::TempDir,
    state: Arc<AppState>,
    mock: Mock,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn fixture(replies: Vec<(StatusCode, String)>, accounts: usize) -> Fixture {
    let mock = Mock {
        replies: Arc::new(Mutex::new(replies.into())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/responses", post(upstream))
        .with_state(mock.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let mut config = AppConfig::default();
    config.server.accounts_file = dir.path().join("accounts.json");
    config.server.upstream_transport = "http".into();
    config.retry.timeout_ms = 5000;
    config.retry.max_attempts = accounts;
    let manager = AccountManager::open(
        config.server.accounts_file.clone(),
        reqwest::Client::new(),
        "priority".into(),
    )
    .unwrap();
    for n in 1..=accounts {
        manager.add_token_response(&json!({"account_id":format!("account-{n}"),"access_token":format!("test-token-{n}")})).await.unwrap();
    }
    let (logs, _) = broadcast::channel(16);
    let state = Arc::new(AppState {
        accounts: manager,
        config: Arc::new(config),
        upstream_http_client: reqwest::Client::new(),
        logs,
        test_upstream_url: Some(format!("http://{address}/responses")),
    });
    Fixture {
        _dir: dir,
        state,
        mock,
        server,
    }
}

fn request(route: &str, stream: bool, effort: &str) -> Request<Body> {
    let payload = if route == "/v1/chat/completions" {
        {
            let mut value = json!({"model":"test-model","messages":[{"role":"user","content":"Hi"}],"stream":stream,"reasoning_effort":effort});
            if stream {
                value["stream_options"] = json!({"include_usage":true});
            }
            value
        }
    } else {
        json!({"model":"test-model","input":"Hi","stream":stream,"reasoning":{"effort":effort}})
    };
    Request::builder()
        .method("POST")
        .uri(route)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

async fn call(
    state: Arc<AppState>,
    route: &str,
    stream: bool,
    effort: &str,
) -> (StatusCode, String) {
    let response = server::router(state)
        .oneshot(request(route, stream, effort))
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn chat_and_responses_round_trip_stream_and_nonstream_with_effort_and_usage() {
    for route in ["/v1/chat/completions", "/v1/responses"] {
        for streaming in [false, true] {
            for effort in ["minimal", "low", "medium", "high", "xhigh"] {
                let test = fixture(vec![(StatusCode::OK, complete())], 1).await;
                let (status, body) = call(test.state.clone(), route, streaming, effort).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "{route} stream={streaming} effort={effort}: {body}"
                );
                let recorded = test.mock.requests.lock().await;
                assert_eq!(recorded.len(), 1);
                let request = &recorded[0].1;
                assert!(request.get("type").is_none(), "{request}");
                assert_eq!(request["stream"], true);
                assert_eq!(request["store"], false);
                assert_eq!(request["reasoning"]["effort"], effort);
                assert_eq!(request["input"][0]["content"][0]["text"], "Hi");
                if streaming && route.contains("chat") {
                    assert!(body.contains("\"content\":\"hello\""), "{body}");
                    assert!(body.contains("\"finish_reason\":\"stop\""), "{body}");
                    assert!(body.contains("\"total_tokens\":5"), "{body}");
                    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
                } else if streaming {
                    assert!(body.contains("event: response.completed\n"), "{body}");
                    assert!(body.contains("\"total_tokens\":5"), "{body}");
                } else {
                    let data: Value = serde_json::from_str(&body).unwrap();
                    if route.contains("chat") {
                        assert_eq!(data["choices"][0]["message"]["content"], "hello");
                        assert_eq!(data["choices"][0]["finish_reason"], "stop");
                    } else {
                        assert_eq!(data["status"], "completed");
                        assert_eq!(data["output"][0]["content"][0]["text"], "hello");
                    }
                    assert_eq!(data["usage"]["total_tokens"], 5);
                }
            }
        }
    }
}

#[tokio::test]
async fn http_400_surfaces_safe_public_message_without_retry() {
    let raw = json!({"error":{"message":"Unsupported value: minimal", "secret":"do-not-expose"},"internal":"private-detail"}).to_string();
    let test = fixture(vec![(StatusCode::BAD_REQUEST, raw)], 2).await;
    let (status, body) = call(test.state.clone(), "/v1/chat/completions", true, "minimal").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("Unsupported value: minimal"), "{body}");
    assert!(
        !body.contains("do-not-expose") && !body.contains("private-detail"),
        "{body}"
    );
    assert_eq!(test.mock.requests.lock().await.len(), 1);
    assert!(test
        .state
        .accounts
        .list()
        .await
        .iter()
        .all(|a| a.status == "ready"));
}

#[tokio::test]
async fn http_429_cools_first_account_and_retries_the_next() {
    let test = fixture(
        vec![
            (StatusCode::TOO_MANY_REQUESTS, "rate limited".into()),
            (StatusCode::OK, complete()),
        ],
        2,
    )
    .await;
    let (status, body) = call(test.state.clone(), "/v1/chat/completions", true, "high").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    let requests = test.mock.requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].0, "account-1");
    assert_eq!(requests[1].0, "account-2");
    let accounts = test.state.accounts.list().await;
    assert_eq!(accounts[0].status, "cooldown");
    assert_eq!(accounts[1].status, "ready");
}

#[tokio::test]
async fn partial_http_eof_never_claims_completion_or_replays() {
    let partial = sse(
        &[
            json!({"type":"response.created","response":{"id":"resp_partial"}}),
            json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}),
        ],
        false,
    );
    for route in ["/v1/chat/completions", "/v1/responses"] {
        for streaming in [false, true] {
            let test = fixture(vec![(StatusCode::OK, partial.clone())], 2).await;
            let (status, body) = call(test.state.clone(), route, streaming, "high").await;
            assert_eq!(
                status,
                if streaming {
                    StatusCode::OK
                } else {
                    StatusCode::BAD_GATEWAY
                },
                "{body}"
            );
            assert!(body.contains("upstream stream interrupted"), "{body}");
            if streaming {
                assert!(body.contains("partial"), "{body}");
            }
            assert!(
                !body.contains("[DONE]") && !body.contains("response.completed"),
                "{body}"
            );
            assert_eq!(test.mock.requests.lock().await.len(), 1, "{body}");
        }
    }
}

#[tokio::test]
async fn chat_reasoning_override_preserves_summary_over_http() {
    let test = fixture(vec![(StatusCode::OK, complete())], 1).await;
    let payload = json!({
        "model":"test-model",
        "messages":[{"role":"user","content":"keep the body"}],
        "reasoning":{"effort":"low","summary":"detailed"},
        "reasoning_effort":"high",
        "stream":false
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = server::router(test.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let requests = test.mock.requests.lock().await;
    assert_eq!(requests.len(), 1);
    let sent = &requests[0].1;
    assert_eq!(
        sent["reasoning"],
        json!({"effort":"high","summary":"detailed"})
    );
    assert_eq!(sent["model"], "test-model");
    assert_eq!(sent["input"][0]["content"][0]["text"], "keep the body");
    assert_eq!(sent["store"], false);
    assert_eq!(sent["stream"], true);
    assert!(sent.get("type").is_none());
}

#[tokio::test]
async fn http_cache_key_and_real_cached_usage_survive_both_routes() {
    for route in ["/v1/chat/completions", "/v1/responses"] {
        let events = [json!({"type":"response.completed","response":{
            "id":"resp_cached","status":"completed","output":[],
            "usage":{"input_tokens":2048,"output_tokens":0,"total_tokens":2048,
                "input_tokens_details":{"cached_tokens":1920}}
        }})];
        let test = fixture(vec![(StatusCode::OK, sse(&events, true))], 1).await;
        let mut logs = test.state.logs.subscribe();
        let mut payload = if route.contains("chat") {
            json!({"model":"test-model","messages":[{"role":"user","content":"Hi"}]})
        } else {
            json!({"model":"test-model","input":"Hi"})
        };
        payload["prompt_cache_key"] = json!("stable-conversation");
        let request = Request::builder()
            .method("POST")
            .uri(route)
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        let response = server::router(test.state.clone())
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let response: Value = serde_json::from_slice(&body).unwrap();
        let details = if route.contains("chat") {
            "prompt_tokens_details"
        } else {
            "input_tokens_details"
        };
        assert_eq!(response["usage"][details]["cached_tokens"], 1920);
        let sent = test.mock.requests.lock().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].1["prompt_cache_key"], "stable-conversation");
        let metric = std::iter::from_fn(|| logs.try_recv().ok())
            .find(|message| message.starts_with("[METRICS]"))
            .expect("request must emit metrics");
        assert!(
            metric.contains("Cache: 1920") && metric.contains("Cache Hit: 93.8%"),
            "{metric}"
        );
    }
}

// The body is produced asynchronously, so these exercise real HTTP body streaming,
// not a pre-buffered response split inside the proxy.
async fn timed_fixture(idle_gap: bool) -> (Fixture, Arc<std::sync::atomic::AtomicUsize>) {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    let count = Arc::new(AtomicUsize::new(0));
    let count_for_handler = count.clone();
    let app = Router::new().route("/responses", post(move |Json(_body): Json<Value>| {
        let count = count_for_handler.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            let stream = async_stream::stream! {
                let first = json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"x"});
                yield Ok::<_, std::io::Error>(format!("data: {first}\n\n"));
                if idle_gap {
                    tokio::time::sleep(Duration::from_millis(1600)).await;
                } else {
                    for _ in 0..3 {
                        tokio::time::sleep(Duration::from_millis(350)).await;
                        let delta = json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"x"});
                        yield Ok(format!("data: {delta}\n\n"));
                    }
                    tokio::time::sleep(Duration::from_millis(350)).await;
                }
                let done = json!({"type":"response.completed","response":{"id":"resp_timed","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"xxxx"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}});
                yield Ok(format!("data: {done}\n\n"));
            };
            ([("content-type", "text/event-stream")], Body::from_stream(stream)).into_response()
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let mut config = AppConfig::default();
    config.server.accounts_file = dir.path().join("accounts.json");
    config.server.upstream_transport = "http".into();
    config.retry.timeout_ms = 1000;
    config.retry.max_attempts = 2;
    let limited_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(config.retry.timeout_ms))
        .build()
        .unwrap();
    let manager = AccountManager::open(
        config.server.accounts_file.clone(),
        limited_client,
        "priority".into(),
    )
    .unwrap();
    for n in 1..=2 {
        manager.add_token_response(&json!({"account_id":format!("account-{n}"),"access_token":format!("test-token-{n}")})).await.unwrap();
    }
    let (logs, _) = broadcast::channel(16);
    let state = Arc::new(AppState {
        accounts: manager,
        config: Arc::new(config),
        upstream_http_client: reqwest::Client::builder().build().unwrap(),
        logs,
        test_upstream_url: Some(format!("http://{address}/responses")),
    });
    let mock = Mock {
        replies: Arc::new(Mutex::new(VecDeque::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    (
        Fixture {
            _dir: dir,
            state,
            mock,
            server,
        },
        count,
    )
}

#[tokio::test]
async fn http_timed_active_stream_exceeds_total_timeout_and_completes() {
    use std::{
        sync::atomic::Ordering,
        time::{Duration, Instant},
    };
    for route in ["/v1/chat/completions", "/v1/responses"] {
        for streaming in [false, true] {
            let (test, count) = timed_fixture(false).await;
            let start = Instant::now();
            let (status, body) = call(test.state.clone(), route, streaming, "high").await;
            assert!(start.elapsed() >= Duration::from_millis(1000));
            assert_eq!(status, StatusCode::OK, "{route} stream={streaming}: {body}");
            assert_eq!(count.load(Ordering::SeqCst), 1);
            if streaming && route.contains("chat") {
                assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
            } else if streaming {
                assert!(body.contains("event: response.completed\n"), "{body}");
            } else {
                let value: Value = serde_json::from_str(&body).unwrap();
                if route.contains("chat") {
                    assert_eq!(value["choices"][0]["message"]["content"], "xxxx");
                } else {
                    assert_eq!(value["status"], "completed");
                }
            }
        }
    }
}

#[tokio::test]
async fn http_timed_idle_stream_is_interrupted_without_replay_or_done() {
    use std::sync::atomic::Ordering;
    for route in ["/v1/chat/completions", "/v1/responses"] {
        let (test, count) = timed_fixture(true).await;
        let (status, body) = call(test.state.clone(), route, true, "high").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains("upstream stream interrupted"), "{body}");
        assert!(
            !body.contains("[DONE]") && !body.contains("response.completed"),
            "{body}"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
