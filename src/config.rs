use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub rotation: RotationConfig,
    pub retry: RetryConfig,
    pub quota: QuotaConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub accounts_file: PathBuf,
    pub proxy: Option<String>,
    pub api_key: Option<String>,
    /// Transport used between this proxy and the ChatGPT Codex backend.
    pub upstream_transport: String,
    pub auto_prompt_cache: bool,
    pub log_level: String,
    pub log_redact_secrets: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RotationConfig {
    pub strategy: String,
    pub cooldown_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub timeout_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct QuotaConfig {
    pub refresh_interval_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 8318,
            accounts_file: "accounts.json".into(),
            proxy: None,
            api_key: None,
            upstream_transport: "wss".into(),
            auto_prompt_cache: true,
            log_level: "info".into(),
            log_redact_secrets: false,
        }
    }
}
impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            strategy: "round_robin".into(),
            cooldown_ms: 60_000,
        }
    }
}
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            timeout_ms: 300_000,
        }
    }
}
impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            refresh_interval_ms: 300_000,
        }
    }
}
impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let config = if path.exists() {
            toml::from_str(&std::fs::read_to_string(path).context("read config")?)
                .context("invalid config")?
        } else {
            Self::default()
        };
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            ["wss", "http"].contains(&self.server.upstream_transport.as_str()),
            "server.upstream_transport must be wss or http"
        );
        let level = self.server.log_level.trim().to_ascii_lowercase();
        anyhow::ensure!(
            ["error", "warn", "info", "debug", "trace"].contains(&level.as_str()),
            "server.log_level must be error, warn, info, debug, or trace"
        );
        anyhow::ensure!(
            ["round_robin", "priority"].contains(&self.rotation.strategy.as_str()),
            "rotation.strategy must be round_robin or priority"
        );
        anyhow::ensure!(
            self.retry.max_attempts > 0
                && self.retry.timeout_ms > 0
                && self.quota.refresh_interval_ms > 0,
            "timeouts, attempts and refresh interval must be positive"
        );
        if let Some(proxy) = &self.server.proxy {
            let parsed = url::Url::parse(proxy).context("invalid server.proxy URL")?;
            anyhow::ensure!(
                matches!(parsed.scheme(), "http" | "socks5" | "socks5h"),
                "server.proxy must use http://, socks5:// or socks5h:// for WSS"
            );
            anyhow::ensure!(parsed.host_str().is_some(), "server.proxy needs a hostname");
        }
        let ip: std::net::IpAddr = self
            .server
            .host
            .parse()
            .context("server.host must be an IP address")?;
        anyhow::ensure!(
            ip.is_loopback() || self.server.api_key.as_ref().is_some_and(|k| !k.is_empty()),
            "non-loopback listening requires server.api_key"
        );
        Ok(())
    }
}

/// Save a supported setting without exposing existing credentials in the UI.
/// Empty proxy/key values clear those optional settings. Changes apply on restart.
pub fn set_value(path: &Path, key: &str, value: &str) -> Result<()> {
    let mut config = AppConfig::load(path)?;
    match key {
        "server.host" => config.server.host = value.to_owned(),
        "server.port" => config.server.port = value.parse().context("invalid port")?,
        "server.proxy" => config.server.proxy = (!value.is_empty()).then(|| value.to_owned()),
        "server.api_key" => config.server.api_key = (!value.is_empty()).then(|| value.to_owned()),
        "server.upstream_transport" => config.server.upstream_transport = value.to_owned(),
        "server.auto_prompt_cache" => {
            config.server.auto_prompt_cache = value.parse().context("invalid auto_prompt_cache")?
        }
        "server.log_level" => config.server.log_level = value.to_ascii_lowercase(),
        "server.log_redact_secrets" => {
            config.server.log_redact_secrets =
                value.parse().context("invalid log_redact_secrets")?
        }
        "rotation.strategy" => config.rotation.strategy = value.to_owned(),
        "rotation.cooldown_ms" => {
            config.rotation.cooldown_ms = value.parse().context("invalid cooldown")?
        }
        "retry.max_attempts" => {
            config.retry.max_attempts = value.parse().context("invalid retry count")?
        }
        "retry.timeout_ms" => config.retry.timeout_ms = value.parse().context("invalid timeout")?,
        "quota.refresh_interval_ms" => {
            config.quota.refresh_interval_ms = value.parse().context("invalid refresh interval")?
        }
        _ => anyhow::bail!("unknown or unsupported configuration key"),
    }
    config.validate()?;
    let temp = path.with_extension("tmp");
    std::fs::write(&temp, toml::to_string_pretty(&config)?)
        .context("write temporary configuration")?;
    std::fs::rename(&temp, path).context("replace configuration")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_defaults_to_wss_and_validates_http_and_unknown_values() {
        let mut config: AppConfig = toml::from_str("[server]\nport = 8318\n").unwrap();
        assert_eq!(config.server.upstream_transport, "wss");
        assert_eq!(config.server.log_level, "info");
        assert!(!config.server.log_redact_secrets);
        assert!(config.validate().is_ok());
        config.server.upstream_transport = "http".into();
        assert!(config.validate().is_ok());
        config.server.upstream_transport = "tcp".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn log_level_validates_supported_levels() {
        let mut config = AppConfig::default();
        for level in ["error", "warn", "info", "debug", "trace", "DEBUG", "Info"] {
            config.server.log_level = level.into();
            assert!(config.validate().is_ok(), "{level}");
        }
        for invalid in ["verbose", "all", ""] {
            config.server.log_level = invalid.into();
            assert!(config.validate().is_err(), "{invalid}");
        }
    }
}
