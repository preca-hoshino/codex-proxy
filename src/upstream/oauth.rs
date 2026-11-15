use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::{rngs::OsRng, RngCore};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

use super::profile;

const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
pub const REDIRECT_URI: &str = "http://127.0.0.1:1455/auth/callback";

/// JWT claims are decoded for display/routing only; this does not verify a signature.
pub fn jwt_claims(token: &str) -> Value {
    token
        .split('.')
        .nth(1)
        .and_then(|part| URL_SAFE_NO_PAD.decode(part).ok())
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(Value::Null)
}

/// Returns the authorization URL, CSRF state, and PKCE verifier. The caller must
/// compare the callback state before exchanging the code and discard it after use.
pub fn auth_url() -> (String, String, String) {
    let mut verifier_bytes = [0_u8; 32];
    let mut state_bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut verifier_bytes);
    OsRng.fill_bytes(&mut state_bytes);
    let verifier = URL_SAFE_NO_PAD.encode(verifier_bytes);
    let state = URL_SAFE_NO_PAD.encode(state_bytes);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut url =
        url::Url::parse("https://auth.openai.com/oauth/authorize").expect("static OAuth URL");
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", CLIENT_ID)
        .append_pair("redirect_uri", REDIRECT_URI)
        .append_pair(
            "scope",
            "openid profile email offline_access api.connectors.read api.connectors.invoke",
        )
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("id_token_add_organizations", "true")
        .append_pair("codex_cli_simplified_flow", "true")
        .append_pair("originator", profile::ORIGINATOR);
    (url.into(), state, verifier)
}

async fn token_response(
    response: Result<reqwest::Response, reqwest::Error>,
    wire_id: u64,
    redact_secrets: bool,
) -> Result<Value, String> {
    // Do not include response bodies or request URLs in returned errors: they may contain secrets.
    let response = match response {
        Ok(r) => r,
        Err(err) => {
            if crate::logger::wire_debug_enabled() {
                tracing::debug!(
                    target: "wire",
                    id = wire_id,
                    "UPSTREAM_OAUTH <- ERROR {err}"
                );
            }
            return Err("OAuth token request failed".to_string());
        }
    };
    let status = response.status();
    let resp_headers = crate::logger::format_reqwest_headers(response.headers(), redact_secrets);
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            if crate::logger::wire_debug_enabled() {
                tracing::debug!(
                    target: "wire",
                    id = wire_id,
                    "UPSTREAM_OAUTH <- HTTP {status} headers={resp_headers} body=<read error: {err}>"
                );
            }
            return Err("OAuth token response was invalid".to_string());
        }
    };
    if crate::logger::wire_debug_enabled() {
        let body_text = String::from_utf8_lossy(&bytes);
        tracing::debug!(
            target: "wire",
            id = wire_id,
            "UPSTREAM_OAUTH <- HTTP {status} headers={resp_headers} body={}",
            crate::logger::redact_json_str(&body_text, redact_secrets)
        );
    }
    if !status.is_success() {
        return Err(format!("OAuth token endpoint returned HTTP {status}"));
    }
    let data: Value = serde_json::from_slice(&bytes)
        .map_err(|_| "OAuth token response was invalid".to_string())?;
    if !data.is_object() {
        return Err("OAuth token response was not an object".to_string());
    }
    Ok(data)
}

pub async fn refresh(
    client: &reqwest::Client,
    refresh_token: &str,
    redact_secrets: bool,
) -> Result<Value, String> {
    let wire_id = crate::logger::next_request_id();
    let payload = json!({"client_id": CLIENT_ID, "grant_type": "refresh_token", "refresh_token": refresh_token});
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "originator",
        reqwest::header::HeaderValue::from_static(profile::ORIGINATOR),
    );
    if let Ok(value) = reqwest::header::HeaderValue::from_str(&profile::user_agent()) {
        headers.insert(reqwest::header::USER_AGENT, value);
    }
    if crate::logger::wire_debug_enabled() {
        let mut logged_payload = payload.clone();
        if redact_secrets {
            crate::logger::redact_json_value(&mut logged_payload);
        }
        tracing::debug!(
            target: "wire",
            id = wire_id,
            "UPSTREAM_OAUTH -> POST {TOKEN_URL} headers={} body={logged_payload}",
            crate::logger::format_reqwest_headers(&headers, redact_secrets)
        );
    }
    let response = client
        .post(TOKEN_URL)
        .headers(headers)
        .json(&payload)
        .timeout(Duration::from_secs(60))
        .send()
        .await;
    token_response(response, wire_id, redact_secrets).await
}

pub async fn exchange_code(
    client: &reqwest::Client,
    code: &str,
    verifier: &str,
    redact_secrets: bool,
) -> Result<Value, String> {
    let wire_id = crate::logger::next_request_id();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "originator",
        reqwest::header::HeaderValue::from_static(profile::ORIGINATOR),
    );
    if let Ok(value) = reqwest::header::HeaderValue::from_str(&profile::user_agent()) {
        headers.insert(reqwest::header::USER_AGENT, value);
    }
    if crate::logger::wire_debug_enabled() {
        let body = if redact_secrets {
            json!({
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "code": "[redacted]",
                "redirect_uri": REDIRECT_URI,
                "code_verifier": "[redacted]",
            })
        } else {
            json!({
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "code_verifier": verifier,
            })
        };
        tracing::debug!(
            target: "wire",
            id = wire_id,
            "UPSTREAM_OAUTH -> POST {TOKEN_URL} headers={} body={body}",
            crate::logger::format_reqwest_headers(&headers, redact_secrets)
        );
    }
    let response = client
        .post(TOKEN_URL)
        .headers(headers)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", CLIENT_ID),
            ("code", code),
            ("redirect_uri", REDIRECT_URI),
            ("code_verifier", verifier),
        ])
        .timeout(Duration::from_secs(60))
        .send()
        .await;
    token_response(response, wire_id, redact_secrets).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oauth_pkce_and_state_are_generated() {
        let (url, state, verifier) = auth_url();
        let url = url::Url::parse(&url).unwrap();
        let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query["state"], state);
        assert_eq!(
            query["code_challenge"],
            URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
        );
        assert_ne!(auth_url().1, state);
    }
    #[test]
    fn invalid_jwt_does_not_panic() {
        assert_eq!(jwt_claims("invalid"), Value::Null);
    }
}
