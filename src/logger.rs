use axum::http::HeaderMap;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static NEXT_REQ_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug)]
pub struct WireRequestId(pub u64);

pub fn next_request_id() -> u64 {
    NEXT_REQ_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn wire_debug_enabled() -> bool {
    tracing::enabled!(target: "wire", tracing::Level::DEBUG)
}

pub fn build_filter(level: &str) -> EnvFilter {
    let level = level.trim().to_ascii_lowercase();
    if matches!(level.as_str(), "debug" | "trace") {
        EnvFilter::new(format!(
            "info,codex_proxy={level},wire={level},proxy={level},metrics={level},quota={level},profile={level}"
        ))
    } else {
        EnvFilter::new(format!("{level},tower_http=info"))
    }
}

pub fn init_logger(default_level: &str, cli_override: bool) {
    let env_filter = if cli_override {
        build_filter(default_level)
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| build_filter(default_level))
    };
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .compact();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

pub fn format_axum_headers(headers: &HeaderMap, redact_secrets: bool) -> String {
    let mut entries: Vec<String> = headers
        .iter()
        .map(|(name, value)| {
            let key = name.as_str();
            let val = if redact_secrets && is_sensitive_header(key) {
                "[redacted]".to_owned()
            } else {
                String::from_utf8_lossy(value.as_bytes()).into_owned()
            };
            format!("{key}: {val}")
        })
        .collect();
    entries.sort();
    format!("{{{}}}", entries.join(", "))
}

pub fn format_reqwest_headers(
    headers: &reqwest::header::HeaderMap,
    redact_secrets: bool,
) -> String {
    let mut entries: Vec<String> = headers
        .iter()
        .map(|(name, value)| {
            let key = name.as_str();
            let val = if redact_secrets && is_sensitive_header(key) {
                "[redacted]".to_owned()
            } else {
                String::from_utf8_lossy(value.as_bytes()).into_owned()
            };
            format!("{key}: {val}")
        })
        .collect();
    entries.sort();
    format!("{{{}}}", entries.join(", "))
}

pub fn format_tungstenite_headers(
    headers: &tokio_tungstenite::tungstenite::http::HeaderMap,
    redact_secrets: bool,
) -> String {
    let mut entries: Vec<String> = headers
        .iter()
        .map(|(name, value)| {
            let key = name.as_str();
            let val = if redact_secrets && is_sensitive_header(key) {
                "[redacted]".to_owned()
            } else {
                String::from_utf8_lossy(value.as_bytes()).into_owned()
            };
            format!("{key}: {val}")
        })
        .collect();
    entries.sort();
    format!("{{{}}}", entries.join(", "))
}

pub fn redact_proxy_url(proxy: &str, redact_secrets: bool) -> String {
    if !redact_secrets {
        return proxy.to_owned();
    }
    let Ok(mut url) = url::Url::parse(proxy) else {
        return "[configured]".to_owned();
    };
    if !url.username().is_empty() || url.password().is_some() {
        let _ = url.set_username("[redacted]");
        let _ = url.set_password(Some("[redacted]"));
    }
    url.into()
}

pub fn redact_json_str(raw: &str, redact_secrets: bool) -> String {
    if !redact_secrets {
        return raw.to_owned();
    }
    let Ok(mut value) = serde_json::from_str::<Value>(raw) else {
        return raw.to_owned();
    };
    redact_json_value(&mut value);
    value.to_string()
}

pub fn redact_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if is_sensitive_json_key(k) {
                    if !v.is_null() {
                        *v = Value::String("[redacted]".into());
                    }
                } else {
                    redact_json_value(v);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_json_value(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "proxy-authorization" | "x-api-key" | "cookie" | "set-cookie"
    )
}

fn is_sensitive_json_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "access_token" | "refresh_token" | "id_token" | "api_key"
    )
}

pub async fn forward(mut receiver: broadcast::Receiver<String>) {
    loop {
        match receiver.recv().await {
            Ok(message) if message.starts_with("[METRICS]") => {
                tracing::info!(target: "metrics", "{message}");
            }
            Ok(message) if message.starts_with("[QUOTA]") => {
                tracing::info!(target: "quota", "{message}");
            }
            Ok(message) if message.starts_with("Quota refresh:") => {
                tracing::warn!(target: "quota", "{message}");
            }
            Ok(message) if message.starts_with("[REJECTED]") => {
                tracing::warn!(target: "proxy", "{message}");
            }
            Ok(message) if message == "Upstream rejected an account; trying another" => {
                tracing::warn!(target: "proxy", "{message}");
            }
            Ok(message) => tracing::info!(target: "proxy", "{message}"),
            Err(broadcast::error::RecvError::Lagged(count)) => {
                tracing::warn!("Skipped {count} log messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(test)]
static WIRE_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

#[cfg(test)]
pub fn test_lock() -> &'static tokio::sync::Mutex<()> {
    WIRE_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header, HeaderValue};
    use serde_json::json;

    #[test]
    fn logger_build_filter_enables_wire_and_proxy_debug_when_requested() {
        let debug_filter = build_filter("debug").to_string();
        assert!(debug_filter.contains("wire=debug"), "{debug_filter}");
        assert!(debug_filter.contains("proxy=debug"), "{debug_filter}");
        assert!(debug_filter.contains("codex_proxy=debug"), "{debug_filter}");
        let info_filter = build_filter("info").to_string();
        assert!(info_filter.contains("info"), "{info_filter}");
    }

    #[test]
    fn redacts_sensitive_headers_json_and_proxy_when_enabled() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-token"),
        );
        headers.insert("x-custom", HeaderValue::from_static("visible"));
        let redacted = format_axum_headers(&headers, true);
        assert!(!redacted.contains("secret-token"), "{redacted}");
        assert!(redacted.contains("authorization: [redacted]"), "{redacted}");
        assert!(redacted.contains("x-custom: visible"), "{redacted}");
        let unredacted = format_axum_headers(&headers, false);
        assert!(unredacted.contains("secret-token"), "{unredacted}");

        let raw = json!({
            "access_token":"tok-1","refresh_token":"tok-2","id_token":"tok-3","user":"keep"
        })
        .to_string();
        let masked = redact_json_str(&raw, true);
        assert!(
            !masked.contains("tok-1") && !masked.contains("tok-2") && !masked.contains("tok-3")
        );
        assert!(masked.contains("keep"));
        assert_eq!(redact_json_str(&raw, false), raw);

        let proxy = redact_proxy_url("http://alice:pass123@127.0.0.1:7890", true);
        assert!(!proxy.contains("alice") && !proxy.contains("pass123"));
        assert_eq!(
            redact_proxy_url("http://alice:pass123@127.0.0.1:7890", false),
            "http://alice:pass123@127.0.0.1:7890"
        );
    }
}
