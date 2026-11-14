use std::{fmt, pin::Pin};

use futures_util::{Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

const URL: &str = "https://chatgpt.com/backend-api/codex/responses";
const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;

pub type UpstreamEvents = Pin<Box<dyn Stream<Item = Result<Message, String>> + Send>>;

#[derive(Debug)]
pub struct HttpConnectionError {
    pub status: Option<u16>,
    pub message: String,
}

impl fmt::Display for HttpConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for HttpConnectionError {}
impl From<String> for HttpConnectionError {
    fn from(message: String) -> Self {
        Self {
            status: None,
            message,
        }
    }
}

pub async fn connect(
    client: &reqwest::Client,
    token: &str,
    account_id: Option<&str>,
    body: Value,
) -> Result<UpstreamEvents, HttpConnectionError> {
    connect_to(URL, client, token, account_id, body).await
}

// A separate URL is useful for local protocol tests; production calls connect.
pub async fn connect_to(
    url: &str,
    client: &reqwest::Client,
    token: &str,
    account_id: Option<&str>,
    body: Value,
) -> Result<UpstreamEvents, HttpConnectionError> {
    if token.is_empty() {
        return Err("access token is empty".to_owned().into());
    }
    let mut body = super::transport::prepare_body(body)?;
    let mut headers = HeaderMap::new();
    super::headers::apply_websocket_headers(
        &mut headers,
        token,
        account_id,
        body.get("prompt_cache_key").and_then(Value::as_str),
    )?;
    // The WebSocket negotiation flag applies only to WSS, not SSE.
    headers.remove("openai-beta");
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    body.as_object_mut()
        .expect("prepare_body returns an object")
        .remove("type");
    let mut response = client
        .post(url)
        .headers(headers)
        .json(&body)
        .send()
        .await
        .map_err(|_| HttpConnectionError::from("upstream HTTP connection failed".to_owned()))?;
    let status = response.status();
    if !status.is_success() {
        let mut message = format!("upstream HTTP request returned HTTP {}", status.as_u16());
        // A bounded JSON 400 error message is useful for unsupported parameters and context limits.
        if status.as_u16() == 400 {
            let mut data = Vec::new();
            while let Ok(Some(chunk)) = response.chunk().await {
                if data.len().saturating_add(chunk.len()) > MAX_ERROR_BYTES {
                    break;
                }
                data.extend_from_slice(&chunk);
            }
            if let Ok(value) = serde_json::from_slice::<Value>(&data) {
                if let Some(detail) = value.pointer("/error/message").and_then(Value::as_str) {
                    let detail: String = detail
                        .chars()
                        .filter(|c| !c.is_control())
                        .take(512)
                        .collect();
                    let detail = detail.replace(token, "[redacted]");
                    if !detail.is_empty() {
                        message.push_str(": ");
                        message.push_str(&detail);
                    }
                }
            }
        }
        return Err(HttpConnectionError {
            status: Some(status.as_u16()),
            message,
        });
    }
    Ok(parse_sse(response.bytes_stream()))
}

// An SSE line is buffered as bytes so UTF-8 code points split between network chunks survive.
fn parse_sse<S, B, E>(mut input: S) -> UpstreamEvents
where
    S: Stream<Item = Result<B, E>> + Unpin + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: Send + 'static,
{
    Box::pin(async_stream::stream! {
        let mut line = Vec::<u8>::new();
        let mut data = Vec::<u8>::new();
        let mut event = Vec::<u8>::new();
        let mut first_line = true;
        let mut done = false;
        while let Some(chunk) = input.next().await {
            let chunk = match chunk { Ok(chunk) => chunk, Err(_) => { yield Err("upstream HTTP stream failed".to_string()); return; } };
            for &byte in chunk.as_ref() {
                if byte != b'\n' {
                    line.push(byte);
                    if line.len().saturating_add(data.len()).saturating_add(event.len()) > MAX_EVENT_BYTES {
                        yield Err("upstream SSE event exceeds size limit".to_string()); return;
                    }
                    continue;
                }
                if line.last() == Some(&b'\r') { line.pop(); }
                if first_line { first_line = false; if line.starts_with(&[0xef, 0xbb, 0xbf]) { line.drain(..3); } }
                match consume_line(&line, &mut data, &mut event) {
                    Ok(Some(Frame::Message(value))) => yield Ok(Message::Text(value.into())),
                    Ok(Some(Frame::Done)) => { done = true; break; },
                    Ok(None) => {},
                    Err(e) => { yield Err(e); return; }
                }
                line.clear();
            }
            if done { break; }
        }
        if !done {
            if line.last() == Some(&b'\r') { line.pop(); }
            if first_line && line.starts_with(&[0xef, 0xbb, 0xbf]) { line.drain(..3); }
            if !line.is_empty() {
                if let Err(e) = consume_line(&line, &mut data, &mut event) { yield Err(e); return; }
            }
            match dispatch(&mut data, &mut event) {
                Ok(Some(Frame::Message(value))) => yield Ok(Message::Text(value.into())),
                Ok(Some(Frame::Done)) | Ok(None) => {},
                Err(e) => yield Err(e),
            }
        }
    })
}

enum Frame {
    Message(String),
    Done,
}

fn consume_line(
    line: &[u8],
    data: &mut Vec<u8>,
    event: &mut Vec<u8>,
) -> Result<Option<Frame>, String> {
    if line.is_empty() {
        return dispatch(data, event);
    }
    if line[0] == b':' {
        return Ok(None);
    }
    let split = line.iter().position(|&b| b == b':').unwrap_or(line.len());
    let field = &line[..split];
    let mut value = if split < line.len() {
        &line[split + 1..]
    } else {
        &[]
    };
    if value.first() == Some(&b' ') {
        value = &value[1..];
    }
    match field {
        b"data" => {
            if !data.is_empty() {
                data.push(b'\n');
            }
            data.extend_from_slice(value);
        }
        b"event" => {
            event.clear();
            event.extend_from_slice(value);
        }
        _ => {}
    }
    if data.len().saturating_add(event.len()) > MAX_EVENT_BYTES {
        return Err("upstream SSE event exceeds size limit".into());
    }
    Ok(None)
}

fn dispatch(data: &mut Vec<u8>, event: &mut Vec<u8>) -> Result<Option<Frame>, String> {
    let result = if data.is_empty() {
        Ok(None)
    } else {
        let text = std::str::from_utf8(data)
            .map_err(|_| "invalid UTF-8 in upstream SSE event".to_string())?;
        if text == "[DONE]" {
            Ok(Some(Frame::Done))
        } else {
            let event_type = std::str::from_utf8(event)
                .map_err(|_| "invalid UTF-8 in upstream SSE event".to_string())?;
            let mut value: Value = serde_json::from_str(text)
                .map_err(|_| "invalid JSON in upstream SSE event".to_string())?;
            let object = value.as_object_mut().ok_or("invalid upstream SSE event")?;
            if !object.contains_key("type") && !event_type.is_empty() && event_type != "message" {
                object.insert("type".into(), Value::String(event_type.to_owned()));
            }
            Ok(Some(Frame::Message(value.to_string())))
        }
    };
    data.clear();
    event.clear();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::Json,
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::post,
        Router,
    };
    use futures_util::{stream, StreamExt};
    use serde_json::json;

    #[tokio::test]
    async fn sse_handles_every_byte_split_crlf_multiple_data_and_unterminated_final_line() {
        let wire = "\u{feff}: hello\r\nevent: response.output_text.delta\r\ndata: {\"delta\":\r\ndata: \"你\"}\r\n\r\ndata: {\"type\":\"response.completed\"}";
        let chunks = wire
            .as_bytes()
            .iter()
            .map(|byte| Ok::<_, ()>(vec![*byte]))
            .collect::<Vec<_>>();
        let frames = parse_sse(stream::iter(chunks)).collect::<Vec<_>>().await;
        assert_eq!(frames.len(), 2);
        let first: Value =
            serde_json::from_str(frames[0].as_ref().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(
            first,
            json!({"type":"response.output_text.delta","delta":"你"})
        );
        let second: Value =
            serde_json::from_str(frames[1].as_ref().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(second["type"], "response.completed");
    }

    #[tokio::test]
    async fn sse_done_ends_without_synthesizing_completion_or_accepting_later_data() {
        let chunks = vec![Ok::<_, ()>(
            b"data: [DONE]\n\ndata: {\"type\":\"response.completed\"}\n\n".to_vec(),
        )];
        assert!(parse_sse(stream::iter(chunks))
            .collect::<Vec<_>>()
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn sse_errors_are_forwarded_and_bad_data_does_not_look_successful() {
        let chunks = vec![Ok::<_, ()>(
            b"event: error\ndata: {\"message\":\"failure\"}\n\n".to_vec(),
        )];
        let frames = parse_sse(stream::iter(chunks)).collect::<Vec<_>>().await;
        let event: Value =
            serde_json::from_str(frames[0].as_ref().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(event["type"], "error");
        let chunks = vec![Ok::<_, ()>(b"data: \xff\n\n".to_vec())];
        assert!(parse_sse(stream::iter(chunks))
            .next()
            .await
            .unwrap()
            .is_err());
        let chunks = vec![Err::<Vec<u8>, _>(())];
        assert!(parse_sse(stream::iter(chunks))
            .next()
            .await
            .unwrap()
            .is_err());
        let chunks = vec![Ok::<_, ()>(vec![b'x'; MAX_EVENT_BYTES + 1])];
        assert!(parse_sse(stream::iter(chunks))
            .next()
            .await
            .unwrap()
            .is_err());
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/responses")
    }

    #[tokio::test]
    async fn posts_prepared_body_and_codex_identity_to_http_sse_endpoint() {
        let app = Router::new().route(
            "/responses",
            post(|headers: HeaderMap, Json(body): Json<Value>| async move {
                assert_eq!(headers["authorization"], "Bearer secret-token");
                assert_eq!(headers["chatgpt-account-id"], "account-id");
                assert_eq!(headers["accept"], "text/event-stream");
                assert_eq!(headers["originator"], super::super::profile::ORIGINATOR);
                assert_eq!(headers["version"], super::super::profile::version());
                assert_eq!(headers["user-agent"], super::super::profile::user_agent());
                assert!(!headers.contains_key("openai-beta"));
                assert_eq!(body["stream"], true);
                assert_eq!(body["store"], false);
                assert!(body.get("type").is_none());
                assert_eq!(body["input"][0]["content"][0]["text"], "hello");
                let chunks = stream::iter(vec![Ok::<_, std::io::Error>(
                    "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n",
                )]);
                (
                    [("content-type", "text/event-stream")],
                    Body::from_stream(chunks),
                )
                    .into_response()
            }),
        );
        let endpoint = serve(app).await;
        let frames = connect_to(
            &endpoint,
            &reqwest::Client::new(),
            "secret-token",
            Some("account-id"),
            json!({"model":"test","input":"hello"}),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
        assert_eq!(frames.len(), 1);
        assert_eq!(
            serde_json::from_str::<Value>(frames[0].as_ref().unwrap().to_text().unwrap()).unwrap()
                ["type"],
            "response.completed"
        );
    }

    #[tokio::test]
    async fn http_errors_are_bounded_and_redacted() {
        let app = Router::new().route(
            "/responses",
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":{"message":"bad secret-token\nrequest"}})),
                )
            }),
        );
        let endpoint = serve(app).await;
        let error = match connect_to(
            &endpoint,
            &reqwest::Client::new(),
            "secret-token",
            None,
            json!({"input":"hi"}),
        )
        .await
        {
            Ok(_) => panic!("expected error"),
            Err(error) => error,
        };
        assert_eq!(error.status, Some(400));
        assert!(error.message.contains("bad [redacted]request"));
        assert!(!error.message.contains("secret-token"));
    }
}
