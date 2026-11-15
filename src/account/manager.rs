use super::storage::{self, Account, AccountView};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use fs2::FileExt;
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

use crate::upstream::profile;

pub struct AccountManager {
    path: PathBuf,
    _lock: File,
    accounts: Mutex<Vec<Account>>,
    account_locks: StdMutex<HashMap<String, Arc<Mutex<()>>>>,
    model_catalogs: Mutex<HashMap<String, (Instant, Value)>>,
    client: reqwest::Client,
    strategy: String,
    redact_secrets: bool,
}

impl AccountManager {
    pub fn open(path: PathBuf, client: reqwest::Client, strategy: String) -> Result<Arc<Self>> {
        Self::open_with_redaction(path, client, strategy, true)
    }

    pub fn open_with_redaction(
        path: PathBuf,
        client: reqwest::Client,
        strategy: String,
        redact_secrets: bool,
    ) -> Result<Arc<Self>> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let lock_path = path.with_extension("lockfile");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.try_lock_exclusive()
            .context("account file is in use by another codex-proxy process")?;
        let accounts = if path.exists() {
            storage::load_and_migrate(&path)?
        } else {
            Vec::new()
        };
        Ok(Arc::new(Self {
            path,
            _lock: lock,
            accounts: Mutex::new(accounts),
            account_locks: StdMutex::new(HashMap::new()),
            model_catalogs: Mutex::new(HashMap::new()),
            client,
            strategy,
            redact_secrets,
        }))
    }
    fn save(&self, accounts: &[Account]) -> Result<()> {
        storage::save(&self.path, accounts)
    }
    fn account_lock(&self, id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.account_locks.lock().unwrap_or_else(|e| e.into_inner());
        locks
            .entry(id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    // Caller holds this account's lock, but never the global account list lock
    // while waiting for network I/O. Persist a rotated refresh token immediately.
    async fn refresh_if_expired(&self, id: &str) -> Result<Option<Account>> {
        let snapshot = {
            self.accounts
                .lock()
                .await
                .iter()
                .find(|a| a.id == id)
                .cloned()
        };
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        if !snapshot.enabled {
            return Ok(None);
        }
        let now = Utc::now().timestamp();
        if !snapshot.expires_at.is_some_and(|t| t <= now + 60) {
            return Ok(Some(snapshot));
        }
        let refresh = snapshot
            .refresh_token
            .as_deref()
            .ok_or_else(|| anyhow!("expired account has no refresh token"))?;
        let value = crate::upstream::refresh(&self.client, refresh, self.redact_secrets)
            .await
            .map_err(|e| anyhow!("token refresh failed: {e}"))?;
        let access = value
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("token refresh returned no access token"))?;
        let mut accounts = self.accounts.lock().await;
        let Some(acc) = accounts.iter_mut().find(|a| a.id == id) else {
            return Ok(None);
        };
        acc.access_token = access.to_owned();
        if let Some(token) = value.get("refresh_token").and_then(Value::as_str) {
            acc.refresh_token = Some(token.to_owned());
        }
        acc.expires_at = crate::upstream::jwt_claims(access)
            .get("exp")
            .and_then(Value::as_i64)
            .or_else(|| {
                value
                    .get("expires_in")
                    .and_then(Value::as_i64)
                    .map(|t| now + t)
            });
        let updated = acc.clone();
        // If saving fails, keep the new token in memory for a later retry; the
        // old on-disk file remains intact. Never report a durable refresh.
        self.save(&accounts)
            .context("persist refreshed credentials")?;
        Ok(if updated.enabled { Some(updated) } else { None })
    }
    pub async fn list(&self) -> Vec<AccountView> {
        let now = Utc::now().timestamp();
        self.accounts
            .lock()
            .await
            .iter()
            .map(|a| AccountView {
                id: a.id.clone(),
                email: a.email.clone(),
                status: if !a.enabled {
                    "disabled"
                } else if a.cooldown_until.is_some_and(|t| t > now) {
                    "cooldown"
                } else if a.expires_at.is_some_and(|t| t <= now) && a.refresh_token.is_none() {
                    "expired"
                } else {
                    "ready"
                }
                .into(),
                cooldown_until: a.cooldown_until,
                quota: a
                    .quota
                    .as_ref()
                    .and_then(|q| q.pointer("/rate_limit/primary_window/used_percent"))
                    .and_then(Value::as_f64)
                    .map(|used| 100.0 - used),
                quota_secondary: a
                    .quota
                    .as_ref()
                    .and_then(|q| q.pointer("/rate_limit/secondary_window/used_percent"))
                    .and_then(Value::as_f64)
                    .map(|used| 100.0 - used),
                quota_primary_reset: a
                    .quota
                    .as_ref()
                    .and_then(|q| q.pointer("/rate_limit/primary_window/reset_at"))
                    .and_then(Value::as_i64),
                quota_secondary_reset: a
                    .quota
                    .as_ref()
                    .and_then(|q| q.pointer("/rate_limit/secondary_window/reset_at"))
                    .and_then(Value::as_i64),
                last_used: a.last_used,
            })
            .collect()
    }
    pub async fn add_token_response(&self, value: &Value) -> Result<()> {
        let tokens = value.get("tokens").unwrap_or(value);
        let access = tokens
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing access_token"))?;
        let claims = crate::upstream::jwt_claims(access);
        let id_claims = tokens
            .get("id_token")
            .and_then(Value::as_str)
            .map(crate::upstream::jwt_claims)
            .unwrap_or(Value::Null);
        let id = tokens
            .get("account_id")
            .and_then(Value::as_str)
            .or_else(|| {
                claims
                    .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                id_claims
                    .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
                    .and_then(Value::as_str)
            })
            .ok_or_else(|| anyhow!("credential has no ChatGPT account ID"))?
            .to_owned();
        let email = id_claims
            .get("email")
            .or_else(|| claims.get("email"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let expiry = claims.get("exp").and_then(Value::as_i64).or_else(|| {
            tokens
                .get("expires_in")
                .and_then(Value::as_i64)
                .map(|n| Utc::now().timestamp() + n)
        });
        let refresh_token = tokens
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let account_lock = self.account_lock(&id);
        let _guard = account_lock.lock().await;
        let mut accounts = self.accounts.lock().await;
        let account = Account {
            id: id.clone(),
            email,
            access_token: access.to_owned(),
            refresh_token,
            expires_at: expiry,
            enabled: true,
            last_used: 0,
            cooldown_until: None,
            quota: None,
        };
        if let Some(i) = accounts.iter().position(|a| a.id == id) {
            accounts[i] = account;
        } else {
            accounts.push(account);
        }
        self.save(&accounts)
    }
    pub async fn reset(&self, id: Option<&str>) -> Result<()> {
        let mut a = self.accounts.lock().await;
        for account in a
            .iter_mut()
            .filter(|a| id.is_none() || id == Some(&a.id) || id == Some(&a.email))
        {
            account.cooldown_until = None;
        }
        self.save(&a)
    }
    pub async fn remove(&self, id: &str) -> Result<()> {
        let mut a = self.accounts.lock().await;
        let old = a.len();
        a.retain(|a| a.id != id && a.email != id);
        anyhow::ensure!(a.len() != old, "account not found");
        self.save(&a)
    }
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<()> {
        let mut a = self.accounts.lock().await;
        let acc = a
            .iter_mut()
            .find(|a| a.id == id || a.email == id)
            .ok_or_else(|| anyhow!("account not found"))?;
        acc.enabled = enabled;
        self.save(&a)
    }
    pub async fn cooldown(&self, id: &str, seconds: u64) -> Result<()> {
        let mut a = self.accounts.lock().await;
        if let Some(acc) = a.iter_mut().find(|a| a.id == id) {
            acc.cooldown_until = Some(Utc::now().timestamp() + seconds as i64);
        }
        self.save(&a)
    }
    pub async fn select(&self, tried: &[String]) -> Result<Account> {
        let now = Utc::now().timestamp();
        let mut candidates: Vec<Account> = self
            .accounts
            .lock()
            .await
            .iter()
            .filter(|a| {
                a.enabled && !tried.contains(&a.id) && a.cooldown_until.is_none_or(|t| t <= now)
            })
            .cloned()
            .collect();
        if self.strategy != "priority" {
            candidates.sort_by_key(|a| a.last_used);
        }
        for candidate in candidates {
            let account_lock = self.account_lock(&candidate.id);
            let Ok(_guard) = account_lock.try_lock() else {
                continue;
            };
            match self.refresh_if_expired(&candidate.id).await {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(err) => {
                    let mut accounts = self.accounts.lock().await;
                    if let Some(acc) = accounts.iter_mut().find(|a| a.id == candidate.id) {
                        acc.cooldown_until = Some(now + 60);
                    }
                    self.save(&accounts)
                        .context("save account cooldown after refresh failure")?;
                    tracing::warn!("account refresh failed: {err}");
                    continue;
                }
            }
            let mut accounts = self.accounts.lock().await;
            let Some(acc) = accounts.iter_mut().find(|a| {
                a.id == candidate.id && a.enabled && a.cooldown_until.is_none_or(|t| t <= now)
            }) else {
                continue;
            };
            acc.last_used = Utc::now().timestamp_millis();
            let result = acc.clone();
            self.save(&accounts)?;
            return Ok(result);
        }
        Err(anyhow!("no available accounts"))
    }
    pub async fn refresh_all(&self) -> Result<()> {
        self.refresh(None).await
    }
    pub async fn refresh(&self, id: Option<&str>) -> Result<()> {
        let mut failures = 0;
        let ids: Vec<String> = {
            let accounts = self.accounts.lock().await;
            if let Some(id) = id {
                anyhow::ensure!(
                    accounts.iter().any(|a| a.id == id || a.email == id),
                    "account not found"
                );
            }
            accounts
                .iter()
                .filter(|a| a.enabled && id.is_none_or(|id| a.id == id || a.email == id))
                .map(|a| a.id.clone())
                .collect()
        };
        for account_id in ids {
            let account_lock = self.account_lock(&account_id);
            let _guard = account_lock.lock().await;
            let acc = match self.refresh_if_expired(&account_id).await {
                Ok(Some(acc)) => acc,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!("account refresh failed: {err}");
                    failures += 1;
                    continue;
                }
            };
            drop(_guard); // Quota lookup does not require the credential refresh lock.
            let url = "https://chatgpt.com/backend-api/wham/usage";
            let mut req_headers = reqwest::header::HeaderMap::new();
            if let Ok(value) =
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", acc.access_token))
            {
                req_headers.insert(reqwest::header::AUTHORIZATION, value);
            }
            if let Ok(value) = reqwest::header::HeaderValue::from_str(&acc.id) {
                req_headers.insert("chatgpt-account-id", value);
            }
            if let Ok(value) = reqwest::header::HeaderValue::from_str(&profile::user_agent()) {
                req_headers.insert(reqwest::header::USER_AGENT, value);
            }
            let wire_id = crate::logger::next_request_id();
            if crate::logger::wire_debug_enabled() {
                tracing::debug!(
                    target: "wire",
                    id = wire_id,
                    "UPSTREAM_USAGE -> GET {url} headers={}",
                    crate::logger::format_reqwest_headers(&req_headers, self.redact_secrets)
                );
            }
            let response = self.client.get(url).headers(req_headers).send().await;
            let quota = match response {
                Ok(r) => {
                    let status = r.status();
                    let resp_headers =
                        crate::logger::format_reqwest_headers(r.headers(), self.redact_secrets);
                    let bytes = r.bytes().await.ok();
                    if crate::logger::wire_debug_enabled() {
                        let body_text = bytes
                            .as_deref()
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_else(|| "<body read failed>".into());
                        tracing::debug!(
                            target: "wire",
                            id = wire_id,
                            "UPSTREAM_USAGE <- HTTP {status} headers={resp_headers} body={body_text}"
                        );
                    }
                    if status.is_success() {
                        bytes
                            .as_deref()
                            .and_then(|b| serde_json::from_slice::<Value>(b).ok())
                    } else {
                        None
                    }
                }
                Err(err) => {
                    if crate::logger::wire_debug_enabled() {
                        tracing::debug!(
                            target: "wire",
                            id = wire_id,
                            "UPSTREAM_USAGE <- ERROR {err}"
                        );
                    }
                    None
                }
            };
            if let Some(quota) = quota {
                let mut accounts = self.accounts.lock().await;
                if let Some(current) = accounts
                    .iter_mut()
                    .find(|a| a.id == acc.id && a.access_token == acc.access_token)
                {
                    current.quota = Some(quota);
                    self.save(&accounts)?;
                }
            } else {
                failures += 1;
            }
        }
        anyhow::ensure!(
            failures == 0,
            "quota or token refresh failed for {failures} account(s); previous data retained"
        );
        Ok(())
    }
    pub async fn models_with_wire_id(&self, wire_id: u64) -> Result<Value> {
        let acc = self.select(&[]).await?;
        self.fetch_models(&acc, wire_id).await
    }

    /// Fetch the catalog for one account without changing the rotation order.
    pub async fn models_for_account(&self, id: &str) -> Result<Value> {
        let account_lock = self.account_lock(id);
        let guard = account_lock.lock().await;
        let acc = self
            .refresh_if_expired(id)
            .await?
            .ok_or_else(|| anyhow!("account not found or disabled"))?;
        drop(guard);
        self.fetch_models(&acc, crate::logger::next_request_id())
            .await
    }

    /// Ultra is a client mode: derive its wire effort from this account's model metadata.
    pub async fn resolve_request_reasoning_with_wire_id(
        &self,
        account: &Account,
        body: &mut Value,
        wire_id: u64,
    ) -> Result<()> {
        if body.pointer("/reasoning/effort").and_then(Value::as_str) != Some("ultra") {
            return Ok(());
        }
        let cached = {
            let catalogs = self.model_catalogs.lock().await;
            catalogs
                .get(&account.id)
                .filter(|(observed, _)| observed.elapsed() < Duration::from_secs(5 * 60))
                .map(|(_, catalog)| catalog.clone())
        };
        // Never retain the cache lock while performing a network request.
        let catalog = match cached {
            Some(catalog) => catalog,
            None => self
                .fetch_models(account, wire_id)
                .await
                .context("model catalog unavailable for Ultra reasoning resolution")?,
        };
        resolve_catalog_reasoning(&catalog, body)?;
        Ok(())
    }

    async fn fetch_models(&self, acc: &Account, wire_id: u64) -> Result<Value> {
        let url = "https://chatgpt.com/backend-api/codex/models";
        let version = profile::version();
        let mut req_headers = reqwest::header::HeaderMap::new();
        if let Ok(value) =
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", acc.access_token))
        {
            req_headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        if let Ok(value) = reqwest::header::HeaderValue::from_str(&acc.id) {
            req_headers.insert("chatgpt-account-id", value);
        }
        req_headers.insert(
            "originator",
            reqwest::header::HeaderValue::from_static(profile::ORIGINATOR),
        );
        req_headers.insert(
            "version",
            reqwest::header::HeaderValue::from_static(version),
        );
        if let Ok(value) = reqwest::header::HeaderValue::from_str(&profile::user_agent()) {
            req_headers.insert(reqwest::header::USER_AGENT, value);
        }
        if crate::logger::wire_debug_enabled() {
            tracing::debug!(
                target: "wire",
                id = wire_id,
                "UPSTREAM_MODELS -> GET {url}?client_version={version} headers={}",
                crate::logger::format_reqwest_headers(&req_headers, self.redact_secrets)
            );
        }
        let r = match self
            .client
            .get(url)
            .query(&[("client_version", version)])
            .headers(req_headers)
            .send()
            .await
        {
            Ok(r) => r,
            Err(err) => {
                if crate::logger::wire_debug_enabled() {
                    tracing::debug!(
                        target: "wire",
                        id = wire_id,
                        "UPSTREAM_MODELS <- ERROR {err}"
                    );
                }
                return Err(err.into());
            }
        };
        let status = r.status();
        let resp_headers = crate::logger::format_reqwest_headers(r.headers(), self.redact_secrets);
        let bytes = r.bytes().await?;
        if crate::logger::wire_debug_enabled() {
            tracing::debug!(
                target: "wire",
                id = wire_id,
                "UPSTREAM_MODELS <- HTTP {status} headers={resp_headers} body={}",
                String::from_utf8_lossy(&bytes)
            );
        }
        if !status.is_success() {
            anyhow::bail!("HTTP status {status} from upstream model catalog");
        }
        let catalog: Value = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            catalog.get("models").and_then(Value::as_array).is_some(),
            "upstream model catalog is invalid"
        );
        self.model_catalogs
            .lock()
            .await
            .insert(acc.id.clone(), (Instant::now(), catalog.clone()));
        Ok(catalog)
    }
}

fn resolve_catalog_reasoning(
    catalog: &Value,
    body: &mut Value,
) -> Result<Option<(String, String)>> {
    if body.pointer("/reasoning/effort").and_then(Value::as_str) != Some("ultra") {
        return Ok(None);
    }
    let model_id = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Ultra reasoning requires a model ID"))?;
    let model = catalog
        .get("models")
        .and_then(Value::as_array)
        .and_then(|models| {
            models
                .iter()
                .find(|model| model.get("slug").and_then(Value::as_str) == Some(model_id))
        })
        .ok_or_else(|| anyhow!("requested model is not present in account model catalog"))?;
    let supported: Vec<_> = model
        .get("supported_reasoning_levels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|preset| preset.get("effort").and_then(Value::as_str))
        .collect();
    let effort = model
        .get("multi_agent_reasoning_effort")
        .and_then(Value::as_str)
        .filter(|effort| *effort != "ultra" && supported.contains(effort))
        .or_else(|| supported.iter().copied().find(|effort| *effort == "max"))
        .or_else(|| {
            supported
                .iter()
                .rev()
                .copied()
                .find(|effort| *effort != "ultra")
        })
        .unwrap_or("medium")
        .to_owned();
    let model_id = model_id.to_owned();
    body["reasoning"]["effort"] = Value::String(effort.clone());
    Ok(Some((model_id, effort)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ultra_uses_model_override_before_supported_fallbacks() {
        use serde_json::json;
        for (supported, override_effort, expected) in [
            (
                vec!["high", "xhigh", "max", "ultra"],
                Some("xhigh"),
                "xhigh",
            ),
            (vec!["high", "max", "ultra"], None, "max"),
            (vec!["high", "xhigh", "ultra"], None, "xhigh"),
            (vec!["high", "xhigh", "ultra"], Some("ultra"), "xhigh"),
            (vec!["high", "xhigh", "ultra"], Some("low"), "xhigh"),
            (vec![], None, "medium"),
        ] {
            let catalog = json!({"models":[{
                "slug":"test",
                "supported_reasoning_levels":supported.into_iter().map(|effort| json!({"effort":effort})).collect::<Vec<_>>(),
                "multi_agent_reasoning_effort":override_effort,
            }]});
            let mut body = json!({"model":"test","input":"keep ultra in prompt","reasoning":{"effort":"ultra","summary":"auto"},"tools":[{"name":"ultra"}]});
            assert_eq!(
                resolve_catalog_reasoning(&catalog, &mut body).unwrap(),
                Some(("test".into(), expected.into()))
            );
            assert_eq!(body["reasoning"]["effort"], expected);
            assert_eq!(body["reasoning"]["summary"], "auto");
            assert_eq!(body["input"], "keep ultra in prompt");
            assert_eq!(body["tools"][0]["name"], "ultra");
        }
    }

    #[test]
    fn ultra_requires_the_requested_model_and_preserves_other_efforts() {
        use serde_json::json;
        let mut missing = json!({"model":"missing","reasoning":{"effort":"ultra"}});
        let original = missing.clone();
        assert!(resolve_catalog_reasoning(&json!({"models":[]}), &mut missing).is_err());
        assert_eq!(missing, original);
        for effort in ["low", "medium", "high", "xhigh", "max", "future"] {
            let mut body = json!({"model":"test","reasoning":{"effort":effort}});
            let original = body.clone();
            assert!(resolve_catalog_reasoning(&Value::Null, &mut body)
                .unwrap()
                .is_none());
            assert_eq!(body, original);
        }
    }

    #[tokio::test]
    async fn busy_account_does_not_block_listing_or_selecting_another() {
        let dir = tempfile::tempdir().unwrap();
        let manager = AccountManager::open(
            dir.path().join("accounts.json"),
            reqwest::Client::new(),
            "priority".into(),
        )
        .unwrap();
        manager
            .add_token_response(&serde_json::json!({"account_id":"one","access_token":"first"}))
            .await
            .unwrap();
        manager
            .add_token_response(&serde_json::json!({"account_id":"two","access_token":"second"}))
            .await
            .unwrap();
        let lock = manager.account_lock("one");
        let _busy = lock.lock().await;
        let (list, selected) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            (manager.list().await, manager.select(&[]).await.unwrap())
        })
        .await
        .unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(selected.id, "two");
    }

    #[tokio::test]
    async fn per_account_models_rejects_missing_or_disabled_without_changing_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let manager = AccountManager::open(
            dir.path().join("accounts.json"),
            reqwest::Client::new(),
            "priority".into(),
        )
        .unwrap();
        manager
            .add_token_response(&serde_json::json!({"account_id":"one","access_token":"first"}))
            .await
            .unwrap();
        manager.set_enabled("one", false).await.unwrap();
        assert!(manager.models_for_account("missing").await.is_err());
        assert!(manager.models_for_account("one").await.is_err());
        assert_eq!(manager.list().await[0].last_used, 0);
    }
}
