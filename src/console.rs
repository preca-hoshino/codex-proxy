use crate::{account::AccountView, config::AppConfig};
use colored::Colorize;
use comfy_table::{presets::UTF8_FULL, Cell, Color, Row, Table};
use serde_json::Value;
use std::{fmt::Write, io::IsTerminal, net::SocketAddr, path::Path};

// Account and upstream values must not inject terminal control sequences.
fn safe_text(value: &str) -> String {
    value.chars().filter(|c| !c.is_control()).collect()
}

fn table() -> Table {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    if !std::io::stdout().is_terminal() {
        table.force_no_tty();
    }
    table
}

fn proxy_address(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return "[configured]".into();
    };
    if url.host_str().is_none() {
        return "[configured]".into();
    }
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    safe_text(url.as_str().trim_end_matches('/'))
}

pub fn startup(config: &AppConfig, config_path: &Path, address: SocketAddr) -> String {
    let mut output = String::new();
    let divider = "=========================================================".bright_cyan();
    let _ = writeln!(output, "{divider}");
    let _ = writeln!(
        output,
        "  {}",
        format!(
            "🚀 CODEX PROXY (Rust Core Edition v{})",
            env!("CARGO_PKG_VERSION")
        )
        .bold()
        .bright_green()
    );
    let _ = writeln!(output, "{divider}");
    let _ = writeln!(
        output,
        "  {} http://{address}/v1",
        "• OpenAI API Endpoint: ".bright_yellow()
    );
    let _ = writeln!(
        output,
        "  {} http://{address}/v1/chat/completions",
        "• Chat Completions:    ".bright_yellow()
    );
    let _ = writeln!(
        output,
        "  {} http://{address}/v1/responses",
        "• Responses Endpoint:  ".bright_yellow()
    );
    let _ = writeln!(
        output,
        "  {} {}",
        "• Accounts File:       ".bright_yellow(),
        safe_text(&config.server.accounts_file.display().to_string()).cyan()
    );
    let _ = writeln!(
        output,
        "  {} {}",
        "• Config File:         ".bright_yellow(),
        safe_text(&config_path.display().to_string()).cyan()
    );
    if let Some(proxy) = &config.server.proxy {
        let _ = writeln!(
            output,
            "  {} {}",
            "• Outbound Proxy:      ".bright_yellow(),
            proxy_address(proxy).magenta()
        );
    }
    let _ = writeln!(
        output,
        "  {} {}",
        "• Active Strategy:     ".bright_yellow(),
        safe_text(&config.rotation.strategy).cyan()
    );
    let _ = writeln!(
        output,
        "  {} {}",
        "• Upstream Transport:  ".bright_yellow(),
        safe_text(&config.server.upstream_transport.to_uppercase()).cyan()
    );
    let _ = writeln!(
        output,
        "  {} {}",
        "• Log Level:           ".bright_yellow(),
        safe_text(&config.server.log_level.to_uppercase()).cyan()
    );
    let _ = writeln!(
        output,
        "  {} {}",
        "• Local Authentication:".bright_yellow(),
        if config
            .server
            .api_key
            .as_ref()
            .is_some_and(|key| !key.is_empty())
        {
            "Enabled".green()
        } else {
            "Disabled".yellow()
        }
    );
    let _ = writeln!(output, "{divider}");
    output
}

fn percent(value: Option<f64>) -> String {
    value
        .filter(|n| n.is_finite())
        .map(|n| format!("{n:.1}%"))
        .unwrap_or_else(|| "unknown".into())
}

fn quota_color(value: Option<f64>) -> Color {
    match value.filter(|n| n.is_finite()) {
        Some(n) if n >= 60.0 => Color::Green,
        Some(n) if n >= 20.0 => Color::Yellow,
        Some(_) => Color::Red,
        None => Color::DarkGrey,
    }
}

fn reset_remaining(value: Option<i64>) -> String {
    let Some(seconds) = value else {
        return "unknown".into();
    };
    let remaining = seconds.saturating_sub(chrono::Utc::now().timestamp());
    if remaining <= 0 {
        return "ready".into();
    }
    let days = remaining / 86400;
    let hours = (remaining % 86400) / 3600;
    let minutes = (remaining % 3600) / 60;
    if days > 0 {
        format!("{days}d{hours}h")
    } else if hours > 0 {
        format!("{hours}h{minutes}m")
    } else {
        format!("{minutes}m")
    }
}

fn quota_reset(value: Option<i64>) -> String {
    format!(" (reset {})", reset_remaining(value))
}

pub fn quota_summary(account: &AccountView) -> String {
    let summary = if account.quota.is_none() && account.quota_secondary.is_none() {
        "No quota data (unknown)".into()
    } else {
        format!(
            "Codex (Primary: {}{}, Secondary: {}{})",
            percent(account.quota),
            quota_reset(account.quota_primary_reset),
            percent(account.quota_secondary),
            quota_reset(account.quota_secondary_reset)
        )
    };
    format!(
        "[QUOTA] Account: {} | Status: {} | {summary}",
        safe_text(&account.email),
        safe_text(&account.status)
    )
}

pub fn accounts(accounts: &[AccountView]) -> String {
    if accounts.is_empty() {
        return format!(
            "{}\n",
            "No accounts configured yet. Use 'account add --oauth' to add one.".yellow()
        );
    }
    let mut table = table();
    table.set_header(vec![
        Cell::new("Email").fg(Color::Cyan),
        Cell::new("Status").fg(Color::Green),
        Cell::new("Cool Down").fg(Color::Magenta),
        Cell::new("Primary Left").fg(Color::Blue),
        Cell::new("Secondary Left").fg(Color::Blue),
        Cell::new("Reset (Primary / Secondary)").fg(Color::Yellow),
    ]);
    let now = chrono::Utc::now().timestamp();
    for account in accounts {
        let cooldown = account
            .cooldown_until
            .filter(|until| *until > now)
            .map(|until| format!("{}s", until - now))
            .unwrap_or_else(|| "Ready".into());
        let status_color = match account.status.as_str() {
            "ready" => Color::Green,
            "cooldown" => Color::Yellow,
            _ => Color::Red,
        };
        table.add_row(Row::from(vec![
            Cell::new(safe_text(&account.email)),
            Cell::new(safe_text(&account.status)).fg(status_color),
            Cell::new(cooldown),
            Cell::new(percent(account.quota)).fg(quota_color(account.quota)),
            Cell::new(percent(account.quota_secondary)).fg(quota_color(account.quota_secondary)),
            Cell::new(format!(
                "{} / {}",
                reset_remaining(account.quota_primary_reset),
                reset_remaining(account.quota_secondary_reset)
            )),
        ]));
    }
    format!("{table}\n")
}

pub fn models_for_account(email: &str, value: &Value) -> String {
    let Some(models) = value
        .get("models")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
    else {
        return "No upstream model catalog: response does not contain a model list.\n".into();
    };
    let models: Vec<_> = models
        .iter()
        .filter_map(|model| {
            let slug = safe_text(model.get("slug").or_else(|| model.get("id"))?.as_str()?);
            if slug.is_empty() {
                return None;
            }
            Some((slug, model))
        })
        .collect();
    let mut output = String::new();
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "  {} {} ({} models)",
        "📡 Upstream Available Models for".bright_cyan().bold(),
        safe_text(email).bright_yellow(),
        models.len().to_string().bright_green()
    );
    let mut table = table();
    table.set_header(vec![
        Cell::new("Model ID").fg(Color::Cyan),
        Cell::new("Display Name").fg(Color::White),
        Cell::new("Tiers").fg(Color::Blue),
        Cell::new("Context Window").fg(Color::Yellow),
        Cell::new("Default Tier").fg(Color::Green),
        Cell::new("Visibility").fg(Color::Magenta),
    ]);
    for (slug, model) in models {
        let text = |key| {
            model
                .get(key)
                .and_then(Value::as_str)
                .map(safe_text)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "-".into())
        };
        let reasoning = model
            .get("supported_reasoning_levels")
            .and_then(Value::as_array)
            .map(|levels| {
                levels
                    .iter()
                    .filter_map(|level| {
                        level
                            .get("effort")
                            .and_then(Value::as_str)
                            .or_else(|| level.as_str())
                    })
                    .map(safe_text)
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "-".into());
        let context = model
            .get("context_window")
            .and_then(Value::as_u64)
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".into());
        table.add_row(Row::from(vec![
            Cell::new(slug),
            Cell::new(text("display_name")),
            Cell::new(reasoning),
            Cell::new(context),
            Cell::new(text("default_reasoning_level")),
            Cell::new(text("visibility")),
        ]));
    }
    let _ = writeln!(output, "{table}");
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn startup_never_prints_proxy_credentials_or_api_key() {
        let mut config = AppConfig::default();
        config.server.proxy =
            Some("http://user:proxy-secret@localhost:7890/path?key=hidden".into());
        config.server.api_key = Some("api-secret".into());
        let output = startup(
            &config,
            Path::new("config.toml"),
            "127.0.0.1:8318".parse().unwrap(),
        );
        assert!(output.contains("http://127.0.0.1:8318/v1/chat/completions"));
        assert!(output.contains("http://localhost:7890"));
        assert!(!output.contains("proxy-secret"));
        assert!(!output.contains("api-secret"));
        assert!(!output.contains("hidden"));
    }

    #[test]
    fn accounts_show_unknown_and_remaining_values() {
        let account = AccountView {
            id: "id".into(),
            email: "user@example.test".into(),
            status: "ready".into(),
            cooldown_until: None,
            quota: Some(75.0),
            quota_secondary: None,
            quota_primary_reset: None,
            quota_secondary_reset: None,
            last_used: 0,
        };
        let output = accounts(std::slice::from_ref(&account));
        assert!(output.contains("Primary Left"));
        assert!(output.contains("75.0%"));
        assert!(output.contains('┌'));
        assert!(quota_summary(&account)
            .contains("Primary: 75.0% (reset unknown), Secondary: unknown (reset unknown)"));
        assert!(accounts(&[]).contains("No accounts configured"));
    }

    #[test]
    fn models_handle_missing_and_filter_control_characters() {
        assert!(models_for_account("-", &json!({})).contains("does not contain a model list"));
        let output = models_for_account(
            "user@example.test",
            &json!({"models":[{"slug":"gpt-test\u{1b}[2J"}, {}, {"slug":""}]}),
        );
        assert!(output.contains("user@example.test (1 models)"));
        assert!(output.contains("Model ID"));
        assert!(output.contains('┌'));
        assert_eq!(safe_text("gpt-test\u{1b}[2J"), "gpt-test[2J");
    }
}
