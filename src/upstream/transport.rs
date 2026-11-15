use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::SinkExt;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

const WS_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";
pub type UpstreamSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub struct ConnectionError {
    pub status: Option<u16>,
    message: String,
}
impl From<String> for ConnectionError {
    fn from(message: String) -> Self {
        Self {
            status: None,
            message,
        }
    }
}
impl From<&str> for ConnectionError {
    fn from(message: &str) -> Self {
        message.to_string().into()
    }
}
impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

fn percent_decode(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] == b'%' {
            if pos + 2 >= bytes.len() {
                return Err("invalid proxy credentials".into());
            }
            let hex = std::str::from_utf8(&bytes[pos + 1..pos + 3])
                .map_err(|_| "invalid proxy credentials")?;
            output.push(u8::from_str_radix(hex, 16).map_err(|_| "invalid proxy credentials")?);
            pos += 3;
        } else {
            output.push(bytes[pos]);
            pos += 1;
        }
    }
    String::from_utf8(output).map_err(|_| "invalid proxy credentials".into())
}

async fn proxy_tunnel(proxy: &str, destination: &str) -> Result<TcpStream, String> {
    let url = url::Url::parse(proxy).map_err(|_| "invalid proxy URL".to_string())?;
    if url.scheme() != "http" {
        return Err("WSS proxy must be an http:// CONNECT proxy".into());
    }
    let host = url.host_str().ok_or("proxy URL has no host")?;
    let port = url.port_or_known_default().ok_or("proxy URL has no port")?;
    let mut stream = timeout(Duration::from_secs(30), TcpStream::connect((host, port)))
        .await
        .map_err(|_| "proxy connection timed out".to_string())?
        .map_err(|_| "proxy connection failed".to_string())?;
    let auth = if !url.username().is_empty() || url.password().is_some() {
        let username = percent_decode(url.username())?;
        let password = percent_decode(url.password().unwrap_or(""))?;
        format!(
            "Proxy-Authorization: Basic {}\r\n",
            STANDARD.encode(format!("{username}:{password}"))
        )
    } else {
        String::new()
    };
    let request = format!("CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n{auth}\r\n");
    timeout(Duration::from_secs(30), async {
        stream.write_all(request.as_bytes()).await?;
        let mut header = Vec::new();
        // Read only through the HTTP header, preserving any following TLS bytes.
        while !header.ends_with(b"\r\n\r\n") {
            if header.len() >= 16384 {
                return Err(std::io::Error::other("proxy response header too large"));
            }
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte).await?;
            header.push(byte[0]);
        }
        Ok::<_, std::io::Error>(header)
    })
    .await
    .map_err(|_| "proxy CONNECT timed out".to_string())?
    .map_err(|_| "proxy CONNECT failed".to_string())
    .and_then(|header| {
        let status = std::str::from_utf8(&header)
            .ok()
            .and_then(|s| s.lines().next())
            .and_then(|s| s.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok());
        if status.is_some_and(|n| (200..300).contains(&n)) {
            Ok(stream)
        } else {
            Err(format!(
                "proxy CONNECT rejected (HTTP {})",
                status
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "invalid".into())
            ))
        }
    })
}

async fn socks_tunnel(proxy: &str, host: &str, port: u16) -> Result<TcpStream, String> {
    let url = url::Url::parse(proxy).map_err(|_| "invalid proxy URL".to_string())?;
    let proxy_host = url.host_str().ok_or("proxy URL has no host")?;
    let proxy_port = url.port().unwrap_or(1080);
    let mut stream = timeout(
        Duration::from_secs(30),
        TcpStream::connect((proxy_host, proxy_port)),
    )
    .await
    .map_err(|_| "SOCKS proxy connection timed out".to_string())?
    .map_err(|_| "SOCKS proxy connection failed".to_string())?;
    let username = percent_decode(url.username())?;
    let password = percent_decode(url.password().unwrap_or(""))?;
    let authenticated = !username.is_empty() || url.password().is_some();
    if username.len() > 255 || password.len() > 255 || host.len() > 255 {
        return Err("SOCKS credentials or destination are too long".into());
    }
    timeout(Duration::from_secs(30), async {
        let fail = || std::io::Error::other("SOCKS negotiation failed");
        if authenticated {
            stream.write_all(&[5, 1, 2]).await?;
        } else {
            stream.write_all(&[5, 1, 0]).await?;
        }
        let mut choice = [0_u8; 2];
        stream.read_exact(&mut choice).await?;
        if choice[0] != 5 {
            return Err(fail());
        }
        if authenticated && choice[1] == 2 {
            let mut auth = vec![1, username.len() as u8];
            auth.extend_from_slice(username.as_bytes());
            auth.push(password.len() as u8);
            auth.extend_from_slice(password.as_bytes());
            stream.write_all(&auth).await?;
            let mut result = [0_u8; 2];
            stream.read_exact(&mut result).await?;
            if result != [1, 0] {
                return Err(fail());
            }
        } else if authenticated || choice[1] != 0 {
            return Err(fail());
        }
        let mut request = vec![5, 1, 0, 3, host.len() as u8];
        request.extend_from_slice(host.as_bytes());
        request.extend_from_slice(&port.to_be_bytes());
        stream.write_all(&request).await?;
        let mut reply = [0_u8; 4];
        stream.read_exact(&mut reply).await?;
        if reply[0] != 5 || reply[1] != 0 || reply[2] != 0 {
            return Err(fail());
        }
        let address_len = match reply[3] {
            1 => 4,
            4 => 16,
            3 => {
                let mut len = [0_u8; 1];
                stream.read_exact(&mut len).await?;
                len[0] as usize
            }
            _ => return Err(fail()),
        };
        let mut address = vec![0_u8; address_len + 2];
        stream.read_exact(&mut address).await?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|_| "SOCKS negotiation timed out".to_string())?
    .map_err(|_| "SOCKS negotiation or connection failed".to_string())?;
    Ok(stream)
}

pub async fn connect(
    access_token: &str,
    account_id: Option<&str>,
    proxy: Option<&str>,
    session_id: Option<&str>,
    wire_id: u64,
    redact_secrets: bool,
) -> Result<UpstreamSocket, ConnectionError> {
    connect_to(
        WS_URL,
        access_token,
        account_id,
        proxy,
        session_id,
        wire_id,
        redact_secrets,
    )
    .await
}

/// Injects a WebSocket address for local protocol tests; the application must use `connect` in production.
pub async fn connect_to(
    endpoint: &str,
    access_token: &str,
    account_id: Option<&str>,
    proxy: Option<&str>,
    session_id: Option<&str>,
    wire_id: u64,
    redact_secrets: bool,
) -> Result<UpstreamSocket, ConnectionError> {
    if access_token.is_empty() {
        return Err("access token is empty".into());
    }
    let url = url::Url::parse(endpoint).map_err(|_| "invalid upstream URL".to_string())?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err("invalid upstream WebSocket scheme".into());
    }
    let host = url.host_str().ok_or("upstream URL has no host")?;
    let port = url
        .port_or_known_default()
        .ok_or("upstream URL has no port")?;
    let destination = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut request = endpoint
        .into_client_request()
        .map_err(|_| "invalid upstream URL".to_string())?;
    super::headers::apply_websocket_headers(
        request.headers_mut(),
        access_token,
        account_id,
        session_id,
    )?;
    if crate::logger::wire_debug_enabled() {
        let proxy_note = proxy
            .filter(|p| !p.trim().is_empty())
            .map(|p| crate::logger::redact_proxy_url(p, redact_secrets))
            .unwrap_or_else(|| "direct".into());
        tracing::debug!(
            target: "wire",
            id = wire_id,
            "UPSTREAM_WSS -> HANDSHAKE {endpoint} via={proxy_note} headers={}",
            crate::logger::format_tungstenite_headers(request.headers(), redact_secrets)
        );
    }
    let handshake = async {
        let result = if let Some(proxy) = proxy.filter(|p| !p.trim().is_empty()) {
            let proxy_scheme = url::Url::parse(proxy)
                .map_err(|_| "invalid proxy URL".to_string())?
                .scheme()
                .to_owned();
            let stream = match proxy_scheme.as_str() {
                "http" => proxy_tunnel(proxy, &destination).await?,
                "socks5" | "socks5h" => socks_tunnel(proxy, host, port).await?,
                "https" => {
                    return Err(
                        "HTTPS proxies are not supported for WSS; use HTTP CONNECT or SOCKS5"
                            .into(),
                    )
                }
                _ => return Err("unsupported proxy scheme; use http:// or socks5://".into()),
            };
            client_async_tls_with_config(request, stream, None, None)
                .await
                .map_err(|e| websocket_error(&e, wire_id, redact_secrets))
        } else {
            connect_async(request)
                .await
                .map_err(|e| websocket_error(&e, wire_id, redact_secrets))
        };
        let (socket, response) = result?;
        if crate::logger::wire_debug_enabled() {
            tracing::debug!(
                target: "wire",
                id = wire_id,
                "UPSTREAM_WSS <- HANDSHAKE HTTP {} headers={}",
                response.status(),
                crate::logger::format_tungstenite_headers(response.headers(), redact_secrets)
            );
        }
        Ok(socket)
    };
    timeout(Duration::from_secs(60), handshake)
        .await
        .map_err(|_| "upstream WebSocket connection timed out".to_string())?
}

fn websocket_error(
    error: &tokio_tungstenite::tungstenite::Error,
    wire_id: u64,
    redact_secrets: bool,
) -> ConnectionError {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            if crate::logger::wire_debug_enabled() {
                let body_text = response
                    .body()
                    .as_deref()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                tracing::debug!(
                    target: "wire",
                    id = wire_id,
                    "UPSTREAM_WSS <- HANDSHAKE_ERROR HTTP {} headers={} body={body_text}",
                    response.status(),
                    crate::logger::format_tungstenite_headers(response.headers(), redact_secrets)
                );
            }
            ConnectionError {
                status: Some(response.status().as_u16()),
                message: format!(
                    "upstream WebSocket handshake returned HTTP {}",
                    response.status()
                ),
            }
        }
        other => {
            if crate::logger::wire_debug_enabled() {
                tracing::debug!(
                    target: "wire",
                    id = wire_id,
                    "UPSTREAM_WSS <- CONNECT_ERROR {other}"
                );
            }
            "upstream WebSocket connection failed".into()
        }
    }
}

pub fn prepare_body(mut body: Value) -> Result<Value, String> {
    let object = body
        .as_object_mut()
        .ok_or("Responses request must be a JSON object")?;
    if let Some(key) = object.get("prompt_cache_key").filter(|key| !key.is_null()) {
        let key = key.as_str().ok_or("prompt_cache_key must be a string")?;
        tokio_tungstenite::tungstenite::http::HeaderValue::from_str(key)
            .map_err(|_| "invalid prompt_cache_key")?;
    }
    super::transform::clean_codex_options(object);
    object.insert("type".into(), Value::String("response.create".into()));
    object.insert("stream".into(), Value::Bool(true));
    object.entry("store").or_insert(Value::Bool(false));
    if let Some(Value::String(input)) = object.get("input") {
        object.insert("input".into(), json!([{"type":"message", "role":"user", "content":[{"type":"input_text", "text": input}]}]));
    }
    if let Some(items) = object.get_mut("input").and_then(Value::as_array_mut) {
        for item in items {
            if let Some(item) = item.as_object_mut() {
                if item.get("type").and_then(Value::as_str) == Some("message")
                    && item.get("role").and_then(Value::as_str) == Some("system")
                {
                    item.insert("role".into(), Value::String("developer".into()));
                }
            }
        }
    }
    Ok(body)
}

pub async fn start(ws: &mut UpstreamSocket, body: Value, wire_id: u64) -> Result<(), String> {
    let body = prepare_body(body)?;
    let encoded = body.to_string();
    if crate::logger::wire_debug_enabled() {
        tracing::debug!(
            target: "wire",
            id = wire_id,
            "UPSTREAM_WSS -> SEND {encoded}"
        );
    }
    ws.send(Message::Text(encoded.into()))
        .await
        .map_err(|_| "failed to send upstream response.create".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_is_prepared_for_codex_ws() {
        let request = prepare_body(json!({"model":"test", "input":"hi", "stream":false})).unwrap();
        assert_eq!(request["type"], "response.create");
        assert_eq!(request["input"][0]["content"][0]["text"], "hi");
        assert_eq!(request["stream"], true);
        assert_eq!(request["store"], false);
        let request =
            prepare_body(json!({"model":"test", "input":"hi", "temperature":0.7})).unwrap();
        assert!(request.get("temperature").is_none());
    }

    #[test]
    fn native_responses_uses_the_same_codex_option_cleanup() {
        let mut request = json!({"model":"test","input":"hi","reasoning":{"effort":"high"},"tools":[{"type":"function","name":"test","parameters":{"type":"object"}}],"stream_options":{"include_usage":true,"include_obfuscation":false}});
        for field in [
            "temperature",
            "top_p",
            "max_tokens",
            "max_completion_tokens",
            "max_output_tokens",
            "frequency_penalty",
            "presence_penalty",
            "chat_template_kwargs",
            "user",
            "metadata",
            "safety_identifier",
            "prompt_cache_retention",
        ] {
            request[field] = json!("private-value");
        }
        let result = prepare_body(request).unwrap();
        for field in [
            "temperature",
            "top_p",
            "max_tokens",
            "max_completion_tokens",
            "max_output_tokens",
            "frequency_penalty",
            "presence_penalty",
            "chat_template_kwargs",
            "user",
            "metadata",
            "safety_identifier",
            "prompt_cache_retention",
        ] {
            assert!(result.get(field).is_none(), "{field}");
        }
        assert_eq!(result["reasoning"]["effort"], "high");
        assert_eq!(result["tools"][0]["name"], "test");
        assert_eq!(
            result["stream_options"],
            json!({"include_obfuscation":false})
        );
    }

    #[test]
    fn native_responses_preserves_ultra_for_account_catalog_resolution() {
        let result = prepare_body(json!({
            "model":"gpt-6.1-terra", "input":"ultra is text here",
            "reasoning":{"effort":"ultra","summary":"detailed"}
        }))
        .unwrap();
        assert_eq!(
            result["reasoning"],
            json!({"effort":"ultra","summary":"detailed"})
        );
        assert_eq!(
            result["input"][0]["content"][0]["text"],
            "ultra is text here"
        );
    }
    #[tokio::test]
    async fn http_connect_tunnels_to_requested_destination() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("CONNECT example.com:443 HTTP/1.1\r\n"));
            assert!(request.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
        });
        proxy_tunnel(&format!("http://user:pass@{addr}"), "example.com:443")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn socks5_tunnels_and_authenticates() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [0; 3];
            socket.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 2]);
            socket.write_all(&[5, 2]).await.unwrap();
            let mut auth = [0; 11];
            socket.read_exact(&mut auth).await.unwrap();
            assert_eq!(&auth, b"\x01\x04user\x04pass");
            socket.write_all(&[1, 0]).await.unwrap();
            let mut prefix = [0; 5];
            socket.read_exact(&mut prefix).await.unwrap();
            assert_eq!(&prefix[..4], &[5, 1, 0, 3]);
            let mut domain = vec![0; prefix[4] as usize];
            socket.read_exact(&mut domain).await.unwrap();
            assert_eq!(&domain, b"example.com");
            let mut port = [0; 2];
            socket.read_exact(&mut port).await.unwrap();
            assert_eq!(u16::from_be_bytes(port), 443);
            socket
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 1])
                .await
                .unwrap();
        });
        socks_tunnel(&format!("socks5://user:pass@{addr}"), "example.com", 443)
            .await
            .unwrap();
        server.await.unwrap();
    }
}
