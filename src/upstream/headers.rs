use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue};

use super::profile;

pub(super) fn apply_websocket_headers(
    headers: &mut HeaderMap,
    access_token: &str,
    account_id: Option<&str>,
    session_id: Option<&str>,
) -> Result<(), String> {
    headers.insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {access_token}"))
            .map_err(|_| "invalid access token".to_string())?,
    );
    if let Some(id) = account_id.filter(|id| !id.is_empty()) {
        headers.insert(
            "chatgpt-account-id",
            HeaderValue::from_str(id).map_err(|_| "invalid account ID".to_string())?,
        );
    }
    if let Some(id) = session_id.filter(|id| !id.is_empty()) {
        headers.insert(
            "session-id",
            HeaderValue::from_str(id).map_err(|_| "invalid prompt_cache_key".to_string())?,
        );
    }
    headers.insert(
        "openai-beta",
        HeaderValue::from_static("responses_websockets=2026-02-06"),
    );
    headers.insert("originator", HeaderValue::from_static(profile::ORIGINATOR));
    headers.insert("version", HeaderValue::from_static(profile::version()));
    headers.insert(
        "user-agent",
        HeaderValue::from_str(&profile::user_agent())
            .map_err(|_| "invalid client identity".to_string())?,
    );
    headers.insert(
        "x-client-request-id",
        HeaderValue::from_str(&uuid::Uuid::new_v4().to_string())
            .map_err(|_| "invalid request ID".to_string())?,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_identity_matches_model_discovery_profile() {
        let mut headers = HeaderMap::new();
        apply_websocket_headers(&mut headers, "test-token", Some("test-account"), None).unwrap();
        assert_eq!(headers["version"], profile::version());
        assert_eq!(headers["originator"], profile::ORIGINATOR);
        assert_eq!(headers["user-agent"], profile::user_agent());
    }
}
