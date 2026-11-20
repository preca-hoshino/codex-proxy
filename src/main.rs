mod account;
mod cli;
mod config;
mod console;
#[cfg(test)]
mod http_integration_tests;
mod logger;
mod proxy;
mod upstream;
#[cfg(test)]
mod upstream_integration_tests;

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use cli::{AccountCommand, Cli, Command, ConfigCommand};
use futures_util::StreamExt;
use proxy::server;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::broadcast,
};

pub struct AppState {
    pub accounts: Arc<account::AccountManager>,
    pub config: Arc<config::AppConfig>,
    pub upstream_http_client: reqwest::Client,
    #[cfg(test)]
    pub test_upstream_url: Option<String>,
    pub logs: broadcast::Sender<String>,
}

impl AppState {
    pub fn refresh_account_quota_now(&self, account_id: String) {
        let accounts = self.accounts.clone();
        let logs = self.logs.clone();
        tokio::spawn(async move {
            match accounts.refresh(Some(&account_id)).await {
                Ok(()) => {
                    if let Some(view) = accounts.account_view(&account_id).await {
                        let _ = logs.send(console::quota_summary(&view));
                    }
                }
                Err(err) => {
                    let _ = logs.send(format!("Quota refresh: {err}"));
                }
            }
        });
    }
}

fn http_client_builder(config: &config::AppConfig) -> Result<reqwest::ClientBuilder> {
    let mut builder = reqwest::Client::builder().no_proxy();
    if let Some(proxy) = &config.server.proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy)?);
    }
    Ok(builder)
}

fn http_client(config: &config::AppConfig) -> Result<reqwest::Client> {
    Ok(http_client_builder(config)?
        .timeout(Duration::from_millis(config.retry.timeout_ms))
        .build()?)
}

// Generation streams use the route's per-event idle timeout, not reqwest's
// whole-response deadline. They still use the same configured outbound proxy.
fn upstream_http_client(config: &config::AppConfig) -> Result<reqwest::Client> {
    Ok(http_client_builder(config)?.build()?)
}

async fn oauth(client: &reqwest::Client, redact_secrets: bool) -> Result<serde_json::Value> {
    let (url, state, verifier) = upstream::auth_url();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:1455")
        .await
        .context("OAuth callback requires free local port 1455")?;
    println!("Open this authorization URL in your browser:\n{url}");
    let _ = open::that(&url);
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(300), listener.accept())
        .await
        .context("OAuth timed out")??;
    let mut buf = vec![0; 8192];
    let n = tokio::time::timeout(Duration::from_secs(15), stream.read(&mut buf)).await??;
    let req = std::str::from_utf8(&buf[..n])?;
    let path = req
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .context("invalid OAuth callback")?;
    let parsed = url::Url::parse(&format!("http://127.0.0.1:1455{path}"))?;
    anyhow::ensure!(
        parsed.path() == "/auth/callback",
        "unexpected OAuth callback path"
    );
    let query: std::collections::HashMap<String, String> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    anyhow::ensure!(query.get("state") == Some(&state), "OAuth state mismatch");
    let code = query
        .get("code")
        .context("authorization returned no code")?;
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\nContent-Length: 29\r\n\r\nLogin received. Close window.").await?;
    upstream::exchange_code(client, code, &verifier, redact_secrets)
        .await
        .map_err(anyhow::Error::msg)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.command.is_none() {
        logger::init_logger(
            cli.log_level.as_deref().unwrap_or("info"),
            cli.log_level.is_some(),
        );
        Cli::command().print_help()?;
        println!();
        return Ok(());
    }
    let config_path = match &cli.command {
        Some(Command::Config {
            file: Some(path), ..
        }) => path.clone(),
        _ => cli.config.clone(),
    };
    let mut loaded = config::AppConfig::load(&config_path)?;
    let cli_log_override = cli.log_level.is_some();
    if let Some(level) = &cli.log_level {
        loaded.server.log_level = level.clone();
    }
    if let Some(Command::Serve {
        host,
        port,
        accounts,
        upstream_transport,
        log_redact_secrets,
    }) = &cli.command
    {
        if let Some(host) = host {
            loaded.server.host = host.clone();
        }
        if let Some(port) = port {
            loaded.server.port = *port;
        }
        if let Some(path) = accounts {
            loaded.server.accounts_file = path.clone();
        }
        if let Some(transport) = upstream_transport {
            loaded.server.upstream_transport = transport.clone();
        }
        if let Some(redact) = log_redact_secrets {
            loaded.server.log_redact_secrets = *redact;
        }
        loaded.validate()?;
    } else {
        loaded.validate()?;
    }
    logger::init_logger(&loaded.server.log_level, cli_log_override);
    match &cli.command {
        Some(Command::Account {
            file: Some(path), ..
        })
        | Some(Command::Models {
            file: Some(path), ..
        }) => loaded.server.accounts_file = path.clone(),
        _ => {}
    }
    let config = Arc::new(loaded);
    if let Some(Command::Config { action, .. }) = &cli.command {
        match action {
            ConfigCommand::Show => {
                let mut redacted = (*config).clone();
                if redacted.server.api_key.is_some() {
                    redacted.server.api_key = Some("[redacted]".into());
                }
                if redacted.server.proxy.is_some() {
                    redacted.server.proxy = Some("[configured]".into());
                }
                println!("{}", toml::to_string_pretty(&redacted)?);
            }
            ConfigCommand::Check => println!("Configuration is valid"),
            ConfigCommand::Set { key, value } => {
                config::set_value(&config_path, key, value)?;
                println!("Saved {key}; restart the service to apply");
            }
        }
        return Ok(());
    }
    if let Some(Command::Test { model, stream }) = &cli.command {
        let url = format!(
            "http://{}:{}/v1/chat/completions",
            config.server.host, config.server.port
        );
        let client = http_client(&config)?;
        let mut req=client.post(url).json(&serde_json::json!({"model":model,"messages":[{"role":"user","content":"Reply with OK."}],"stream":stream}));
        if let Some(key) = &config.server.api_key {
            req = req.bearer_auth(key);
        }
        let response = req.send().await?.error_for_status()?;
        if *stream {
            use std::io::Write;
            let mut chunks = response.bytes_stream();
            let mut seen = Vec::new();
            while let Some(chunk) = chunks.next().await {
                let chunk = chunk?;
                std::io::stdout().write_all(&chunk)?;
                std::io::stdout().flush()?;
                seen.extend_from_slice(&chunk);
            }
            anyhow::ensure!(
                seen.windows(b"data: [DONE]".len())
                    .any(|w| w == b"data: [DONE]"),
                "stream ended without [DONE]"
            );
        } else {
            let value: serde_json::Value = response.json().await?;
            anyhow::ensure!(
                value
                    .get("choices")
                    .and_then(|v| v.as_array())
                    .is_some_and(|v| !v.is_empty()),
                "response has no choices"
            );
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        return Ok(());
    }
    let client = http_client(&config)?;
    let accounts = account::AccountManager::open_with_redaction(
        config.server.accounts_file.clone(),
        client.clone(),
        config.rotation.strategy.clone(),
        config.server.log_redact_secrets,
    )?;
    match cli.command {
        Some(Command::Account { action, .. }) => {
            match action {
                AccountCommand::List { json } => {
                    let a = accounts.list().await;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&a)?);
                    } else {
                        println!("{}", console::accounts(&a));
                    }
                }
                AccountCommand::Add {
                    oauth: use_oauth,
                    token,
                } => {
                    let value = if let Some(token) = token {
                        eprintln!("Warning: command-line tokens can be visible to other processes; browser OAuth is preferred.");
                        upstream::refresh(&client, &token, config.server.log_redact_secrets)
                            .await
                            .map_err(anyhow::Error::msg)?
                    } else if use_oauth {
                        oauth(&client, config.server.log_redact_secrets).await?
                    } else {
                        anyhow::bail!("specify --oauth or --token")
                    };
                    accounts.add_token_response(&value).await?;
                    println!("Account added");
                }
                AccountCommand::Login => {
                    let value = oauth(&client, config.server.log_redact_secrets).await?;
                    accounts.add_token_response(&value).await?;
                    println!("Account added");
                }
                AccountCommand::Import { file } => {
                    eprintln!("Warning: token rotation may invalidate credentials shared with the source login. Credentials are stored in a local file; protect its permissions.");
                    let value = serde_json::from_slice(&std::fs::read(file)?)?;
                    accounts.add_token_response(&value).await?;
                    println!("Account imported");
                }
                AccountCommand::Remove { id } => {
                    accounts.remove(&id).await?;
                    println!("Removed");
                }
                AccountCommand::Enable { id } => accounts.set_enabled(&id, true).await?,
                AccountCommand::Disable { id } => accounts.set_enabled(&id, false).await?,
                AccountCommand::Refresh { id } => {
                    accounts.refresh(id.as_deref()).await?;
                    println!("Refreshed");
                }
                AccountCommand::Reset { id } => {
                    accounts.reset(id.as_deref()).await?;
                    println!("Reset");
                }
            }
            Ok(())
        }
        Some(Command::Models { json, email, .. }) => {
            upstream::profile::initialize(&client).await;
            let list: Vec<_> = accounts
                .list()
                .await
                .into_iter()
                .filter(|a| {
                    a.status != "disabled"
                        && email.as_ref().is_none_or(|e| &a.email == e || &a.id == e)
                })
                .collect();
            anyhow::ensure!(!list.is_empty(), "no matching enabled accounts");
            let mut results = Vec::new();
            let mut catalogs = Vec::new();
            let mut successes = 0;
            for account in list {
                match accounts.models_for_account(&account.id).await {
                    Ok(models) => {
                        successes += 1;
                        if !json {
                            catalogs.push((account.email.clone(), models.clone()));
                        }
                        results.push(serde_json::json!({"email":account.email,"result":models}));
                    }
                    Err(_) => {
                        if !json {
                            let name: String =
                                account.email.chars().filter(|c| !c.is_control()).collect();
                            eprintln!("无法获取账号 {name} 的上游模型目录，请检查账号和网络");
                        }
                        results.push(serde_json::json!({"email":account.email,"error":"model catalog unavailable"}));
                    }
                }
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&results)?);
            } else if !catalogs.is_empty() {
                println!("{}", console::models_for_accounts(&catalogs));
            }
            anyhow::ensure!(
                successes > 0,
                "failed to fetch model catalog for all matching accounts"
            );
            Ok(())
        }
        Some(Command::Serve { .. }) => {
            let (logs, log_receiver) = broadcast::channel::<String>(256);
            let state = Arc::new(AppState {
                accounts,
                config: config.clone(),
                upstream_http_client: upstream_http_client(&config)?,
                logs,
                #[cfg(test)]
                test_upstream_url: None,
            });
            let listener =
                tokio::net::TcpListener::bind((config.server.host.as_str(), config.server.port))
                    .await
                    .context("bind proxy")?;
            println!(
                "{}",
                console::startup(&config, &cli.config, listener.local_addr()?)
            );
            let log_printer = tokio::spawn(logger::forward(log_receiver));
            let profile = upstream::profile::initialize(&client).await;
            tracing::info!(target: "profile", "Codex CLI profile: {} ({})", profile.version, profile.source);
            let background = state.clone();
            let quota = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(
                    background.config.quota.refresh_interval_ms,
                ));
                let mut initial = true;
                loop {
                    interval.tick().await;
                    if let Err(e) = background.accounts.refresh_all().await {
                        let _ = background.logs.send(format!("Quota refresh: {e}"));
                    }
                    let accounts = background.accounts.list().await;
                    if initial {
                        if accounts.is_empty() {
                            tracing::warn!(
                                "No accounts configured. Use 'account add --oauth' to add one."
                            );
                        }
                        let mut catalogs = Vec::new();
                        for account in accounts.iter().filter(|a| a.status != "disabled") {
                            match background.accounts.models_for_account(&account.id).await {
                                Ok(models) => {
                                    catalogs.push((account.email.clone(), models));
                                }
                                Err(_) => {
                                    let name: String =
                                        account.email.chars().filter(|c| !c.is_control()).collect();
                                    let _ = background.logs.send(format!(
                                        "无法获取账号 {name} 的上游模型目录，请检查账号和网络"
                                    ));
                                }
                            }
                        }
                        if !catalogs.is_empty() {
                            println!("{}", console::models_for_accounts(&catalogs));
                        }
                        initial = false;
                    }
                    for account in &accounts {
                        let _ = background.logs.send(console::quota_summary(account));
                    }
                }
            });
            let result = axum::serve(listener, server::router(state))
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
            quota.abort();
            log_printer.abort();
            result?;
            Ok(())
        }
        _ => unreachable!(),
    }
}
