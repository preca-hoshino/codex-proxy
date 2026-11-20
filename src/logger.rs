use axum::http::HeaderMap;
use serde_json::Value;
use std::{
    fmt::{self, Write},
    io::IsTerminal,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::broadcast;
use tracing_subscriber::{
    field::RecordFields,
    fmt::{format::Writer, FormatFields},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

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
        EnvFilter::new(level)
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
        .with_ansi(log_ansi_enabled())
        .fmt_fields(LogFields)
        .compact();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

fn log_ansi_enabled() -> bool {
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
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

fn sanitize_message(message: &str) -> String {
    let mut safe = String::new();
    for c in message.chars() {
        match c {
            '\x1b' | '\x07' | '\x08' | '\x0c' | '\x7f' => {
                let _ = write!(safe, "\\x{:02x}", c as u32);
            }
            '\u{80}'..='\u{9f}' => {
                let _ = write!(safe, "\\u{{{:x}}}", c as u32);
            }
            _ => safe.push(c),
        }
    }
    safe
}

fn style_log_message(message: &str, ansi: bool) -> String {
    if ansi && message.starts_with("Sending request to Codex upstream ") {
        let mut styled = String::new();
        let mut quoted = false;
        let mut escaped = false;
        let mut index = 0;
        while index < message.len() {
            let remaining = &message[index..];
            if !quoted && (index == 0 || message.as_bytes()[index - 1] == b' ') {
                if let Some(name) = [
                    "model",
                    "reasoning_effort",
                    "account",
                    "transport",
                    "attempt",
                    "max_attempts",
                ]
                .into_iter()
                .find(|name| {
                    remaining
                        .strip_prefix(name)
                        .is_some_and(|rest| rest.starts_with('='))
                }) {
                    let _ = write!(styled, "\x1b[3m{name}\x1b[0m\x1b[2m=\x1b[0m");
                    index += name.len() + 1;
                    continue;
                }
            }
            let ch = remaining.chars().next().expect("index is within message");
            styled.push(ch);
            if quoted && escaped {
                escaped = false;
            } else if quoted && ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quoted = !quoted;
            }
            index += ch.len_utf8();
        }
        return styled;
    }
    if !ansi
        || !(message.starts_with("[QUOTA]")
            || message.starts_with("[METRICS]")
            || message.starts_with("[REJECTED]")
            || message.starts_with("Quota refresh:"))
    {
        return message.to_owned();
    }
    let mut styled = String::new();
    for (index, part) in message.split(" | ").enumerate() {
        if index > 0 {
            styled.push_str(" | ");
        }
        let (tag, rest) = if part.starts_with('[') {
            part.find(']')
                .map_or(("", part), |end| part.split_at(end + 1))
        } else {
            ("", part)
        };
        if !tag.is_empty() {
            styled.push_str(&format!("\x1b[1;36m{tag}\x1b[0m"));
        }
        if let Some((label, value)) = rest.split_once(": ") {
            styled.push_str(&format!("\x1b[3;36m{label}\x1b[0m: {value}"));
        } else {
            styled.push_str(rest);
        }
    }
    styled
}

struct LogFields;

struct LogVisitor<'a> {
    writer: Writer<'a>,
    result: fmt::Result,
    empty: bool,
}

impl tracing::field::Visit for LogVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }
        self.result = (|| {
            if !self.empty {
                write!(self.writer, " ")?;
            }
            self.empty = false;
            if field.name() == "message" {
                let message = sanitize_message(&format!("{value:?}"));
                write!(
                    self.writer,
                    "{}",
                    style_log_message(&message, self.writer.has_ansi_escapes())
                )
            } else {
                let name = field.name().strip_prefix("r#").unwrap_or(field.name());
                if self.writer.has_ansi_escapes() {
                    write!(self.writer, "\x1b[3m{name}\x1b[0m\x1b[2m=\x1b[0m{value:?}")
                } else {
                    write!(self.writer, "{name}={value:?}")
                }
            }
        })();
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.record_debug(field, &format_args!("{value}"));
        } else {
            self.record_debug(field, &value);
        }
    }
}

impl<'a> FormatFields<'a> for LogFields {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'a>, fields: R) -> fmt::Result {
        let mut visitor = LogVisitor {
            writer,
            result: Ok(()),
            empty: true,
        };
        fields.record(&mut visitor);
        visitor.result
    }
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

    #[derive(Clone)]
    struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn actual_formatter_emits_styles_and_sanitizes_untrusted_escapes() {
        for ansi in [true, false] {
            let output = Captured(Default::default());
            let target = output.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(move || target.clone())
                .with_ansi(ansi)
                .without_time()
                .fmt_fields(LogFields)
                .compact()
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "quota", "[QUOTA] Account: user\x1b[31m | Status: ready");
            });
            let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
            assert!(text.contains("user\\x1b[31m"), "{text:?}");
            assert!(!text.contains("user\x1b[31m"), "{text:?}");
            if ansi {
                assert!(text.contains("\x1b[1;36m[QUOTA]\x1b[0m"), "{text:?}");
                assert!(text.contains("\x1b[3;36m"), "{text:?}");
            } else {
                assert!(!text.contains('\x1b'), "{text:?}");
                assert!(
                    text.contains("[QUOTA] Account: user\\x1b[31m | Status: ready"),
                    "{text:?}"
                );
            }
        }
    }

    #[test]
    fn structured_fields_use_italic_names_and_dim_equals() {
        for ansi in [true, false] {
            let output = Captured(Default::default());
            let target = output.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(move || target.clone())
                .with_ansi(ansi)
                .without_time()
                .fmt_fields(LogFields)
                .compact()
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "proxy", model = %"gpt-test", attempts = 2, "Request sent");
            });
            let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
            if ansi {
                assert!(
                    text.contains("\x1b[3mmodel\x1b[0m\x1b[2m=\x1b[0mgpt-test"),
                    "{text:?}"
                );
                assert!(
                    text.contains("\x1b[3mattempts\x1b[0m\x1b[2m=\x1b[0m2"),
                    "{text:?}"
                );
            } else {
                assert!(text.contains("model=gpt-test attempts=2"), "{text:?}");
                assert!(!text.contains('\x1b'), "{text:?}");
            }
        }
    }

    #[test]
    fn upstream_request_message_styles_only_field_names() {
        let message = "Sending request to Codex upstream model=\"gpt-test\" reasoning_effort=\"xhigh\" account=\"name model=literal\" transport=wss attempt=1 max_attempts=5";
        for ansi in [true, false] {
            let output = Captured(Default::default());
            let target = output.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(move || target.clone())
                .with_ansi(ansi)
                .without_time()
                .fmt_fields(LogFields)
                .compact()
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "proxy", "{message}");
            });
            let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
            if ansi {
                for name in [
                    "model",
                    "reasoning_effort",
                    "account",
                    "transport",
                    "attempt",
                    "max_attempts",
                ] {
                    assert!(
                        text.contains(&format!("\x1b[3m{name}\x1b[0m\x1b[2m=\x1b[0m")),
                        "{text:?}"
                    );
                }
                assert!(text.contains("name model=literal"), "{text:?}");
            } else {
                assert!(text.contains(message), "{text:?}");
                assert!(!text.contains('\x1b'), "{text:?}");
            }
        }
    }
    #[test]
    fn log_styling_preserves_message_and_plain_output() {
        let message = "[QUOTA] Account: user | Status: ready | 5-hour: 99.0% (4h 45m), Weekly: 41.0% (2d 3h 7m)";
        assert_eq!(style_log_message(message, false), message);
        let styled = style_log_message(message, true);
        assert!(styled.contains("\x1b[1;36m[QUOTA]\x1b[0m"));
        assert!(styled.contains("\x1b[3;36m"));
        assert_eq!(
            styled
                .replace("\x1b[1;36m", "")
                .replace("\x1b[3;36m", "")
                .replace("\x1b[0m", ""),
            message
        );
    }

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
