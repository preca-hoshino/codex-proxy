use super::routes_openai::{chat, health, models, responses};
use crate::{logger, AppState};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{Request, State},
    http::header,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Router,
};
use futures_util::StreamExt;
use std::sync::Arc;

const MAX_LOGGED_BODY_BYTES: usize = 8 * 1024 * 1024;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat))
        .route("/v1/responses", post(responses))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            log_downstream_traffic,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_LOGGED_BODY_BYTES))
        .with_state(state)
}

async fn log_downstream_traffic(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if !logger::wire_debug_enabled() {
        return next.run(request).await;
    }
    let id = logger::next_request_id();
    let redact = state.config.server.log_redact_secrets;
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MAX_LOGGED_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            tracing::debug!(
                target: "wire",
                id = id,
                "DOWNSTREAM -> {} {} headers={} body=<exceeds limit>",
                parts.method,
                parts.uri,
                logger::format_axum_headers(&parts.headers, redact),
            );
            return axum::http::StatusCode::PAYLOAD_TOO_LARGE.into_response_with_empty();
        }
    };
    let mut request = Request::from_parts(parts, Body::from(bytes.clone()));
    request.extensions_mut().insert(logger::WireRequestId(id));
    if crate::logger::wire_debug_enabled() {
        let header_str = crate::logger::format_axum_headers(request.headers(), redact);
        tracing::debug!(
            target: "wire",
            id = id,
            "DOWNSTREAM -> {} {} headers={} body={}",
            request.method(),
            request.uri(),
            header_str,
            format_body(&bytes)
        );
    }
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    let is_sse = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));
    if is_sse {
        let header_str = logger::format_axum_headers(&parts.headers, redact);
        tracing::debug!(
            target: "wire",
            id = id,
            "DOWNSTREAM <- HTTP {} headers={} stream=start",
            parts.status,
            header_str
        );
        let stream = async_stream::stream! {
            let mut inner = body.into_data_stream();
            while let Some(item) = inner.next().await {
                match item {
                    Ok(chunk) => {
                        tracing::debug!(
                            target: "wire",
                            id = id,
                            "DOWNSTREAM <- CHUNK {}",
                            format_body(&chunk)
                        );
                        yield Ok::<Bytes, axum::Error>(chunk);
                    }
                    Err(err) => {
                        tracing::debug!(
                            target: "wire",
                            id = id,
                            "DOWNSTREAM <- STREAM_ERROR {err}"
                        );
                        yield Err(err);
                        break;
                    }
                }
            }
            tracing::debug!(target: "wire", id = id, "DOWNSTREAM <- stream=end");
        };
        Response::from_parts(parts, Body::from_stream(stream))
    } else {
        let collected = match to_bytes(body, MAX_LOGGED_BODY_BYTES).await {
            Ok(bytes) => bytes,
            Err(_) => {
                tracing::debug!(
                    target: "wire",
                    id = id,
                    "DOWNSTREAM <- HTTP {} headers={} body=<exceeds limit>",
                    parts.status,
                    logger::format_axum_headers(&parts.headers, redact)
                );
                return Response::from_parts(parts, Body::empty());
            }
        };
        let body_text = format_body(&collected);
        let body_text = if parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("json"))
        {
            logger::redact_json_str(&body_text, redact)
        } else {
            body_text
        };
        tracing::debug!(
            target: "wire",
            id = id,
            "DOWNSTREAM <- HTTP {} headers={} body={}",
            parts.status,
            logger::format_axum_headers(&parts.headers, redact),
            body_text
        );
        Response::from_parts(parts, Body::from(collected))
    }
}

trait EmptyStatusExt {
    fn into_response_with_empty(self) -> Response;
}
impl EmptyStatusExt for axum::http::StatusCode {
    fn into_response_with_empty(self) -> Response {
        axum::response::IntoResponse::into_response(self)
    }
}

fn format_body(bytes: &[u8]) -> String {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .unwrap_or_else(|_| format!("<binary {} bytes>", bytes.len()))
}
