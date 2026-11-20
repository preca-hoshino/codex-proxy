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
    format_remaining(seconds.saturating_sub(chrono::Utc::now().timestamp()))
}

fn format_remaining(remaining: i64) -> String {
    if remaining <= 0 {
        return "ready".into();
    }
    let days = remaining / 86400;
    let hours = (remaining % 86400) / 3600;
    let minutes = (remaining % 3600) / 60;
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

pub fn quota_summary(account: &AccountView) -> String {
    let summary = if account.quota.is_none() && account.quota_secondary.is_none() {
        "No quota data (unknown)".into()
    } else {
        format!(
            "5-hour: {} ({}), Weekly: {} ({})",
            if account.quota_five_hour_not_applicable {
                "-".into()
            } else {
                percent(account.quota)
            },
            if account.quota_five_hour_not_applicable {
                "-".into()
            } else {
                reset_remaining(account.quota_primary_reset)
            },
            percent(account.quota_secondary),
            reset_remaining(account.quota_secondary_reset)
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
        Cell::new("5-hour Left").fg(Color::Blue),
        Cell::new("Weekly Left").fg(Color::Blue),
        Cell::new("Reset (5-hour / Weekly)").fg(Color::Yellow),
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
            Cell::new(if account.quota_five_hour_not_applicable {
                "-".into()
            } else {
                percent(account.quota)
            })
            .fg(quota_color(account.quota)),
            Cell::new(percent(account.quota_secondary)).fg(quota_color(account.quota_secondary)),
            Cell::new(format!(
                "{} / {}",
                if account.quota_five_hour_not_applicable {
                    "-".into()
                } else {
                    reset_remaining(account.quota_primary_reset)
                },
                reset_remaining(account.quota_secondary_reset)
            )),
        ]));
    }
    format!("{table}\n")
}

pub fn models_for_accounts(catalogs: &[(String, Value)]) -> String {
    let mut rows: Vec<([String; 5], Vec<String>)> = Vec::new();
    let mut output = String::new();
    for (email, value) in catalogs {
        let Some(models) = value
            .get("models")
            .or_else(|| value.get("data"))
            .and_then(Value::as_array)
        else {
            let _ = writeln!(
                output,
                "No upstream model catalog for {}: response does not contain a model list.",
                safe_text(email)
            );
            continue;
        };
        for model in models {
            let Some(slug) = model
                .get("slug")
                .or_else(|| model.get("id"))
                .and_then(Value::as_str)
                .map(safe_text)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
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
            let fields = [
                slug,
                text("display_name"),
                reasoning,
                context,
                text("visibility"),
            ];
            let account = safe_text(email);
            if let Some((_, accounts)) = rows.iter_mut().find(|(existing, _)| *existing == fields) {
                if !accounts.contains(&account) {
                    accounts.push(account);
                }
            } else {
                rows.push((fields, vec![account]));
            }
        }
    }
    if catalogs.is_empty() {
        return output;
    }
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "  {}",
        "📡 Upstream Available Models".bright_cyan().bold()
    );
    let mut table = table();
    table.set_header(vec![
        Cell::new("Model ID").fg(Color::Cyan),
        Cell::new("Display Name").fg(Color::White),
        Cell::new("Tiers").fg(Color::Blue),
        Cell::new("Context Window").fg(Color::Yellow),
        Cell::new("Visibility").fg(Color::Magenta),
        Cell::new("Available Accounts").fg(Color::Green),
    ]);
    for (fields, accounts) in rows {
        let mut cells: Vec<Cell> = fields.into_iter().map(Cell::new).collect();
        cells.push(Cell::new(accounts.len()));
        table.add_row(Row::from(cells));
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
            quota_secondary: Some(40.0),
            quota_primary_reset: Some(0),
            quota_secondary_reset: None,
            quota_five_hour_not_applicable: false,
            last_used: 0,
        };
        let output = accounts(std::slice::from_ref(&account));
        assert!(output.contains("5-hour Left"));
        assert!(output.contains("Weekly Left"));
        assert!(output.contains("Reset (5-hour / Weekly)"));
        assert!(output.contains("75.0%"));
        assert!(output.contains("40.0%"));
        assert!(output.contains("ready / unknown"));
        assert!(output.contains('┌'));
        assert!(quota_summary(&account).contains("5-hour: 75.0% (ready), Weekly: 40.0% (unknown)"));
        let mut unknown = account;
        unknown.quota = None;
        unknown.quota_secondary = None;
        assert!(quota_summary(&unknown).contains("No quota data (unknown)"));
        unknown.quota_secondary = Some(12.0);
        unknown.quota_five_hour_not_applicable = true;
        assert!(quota_summary(&unknown).contains("5-hour: - (-), Weekly: 12.0% (unknown)"));
        let output = accounts(&[unknown]);
        assert!(output.contains("12.0%"));
        assert!(output.contains("- / unknown"));
        assert!(accounts(&[]).contains("No accounts configured"));
    }

    #[test]
    fn remaining_time_includes_spaced_minutes() {
        assert_eq!(format_remaining(4 * 3600 + 45 * 60), "4h 45m");
        assert_eq!(format_remaining(2 * 86400 + 3 * 3600 + 7 * 60), "2d 3h 7m");
        assert_eq!(format_remaining(60), "1m");
        assert_eq!(format_remaining(0), "ready");
    }

    #[test]
    fn models_handle_missing_and_filter_control_characters() {
        assert!(models_for_accounts(&[("-".into(), json!({}))])
            .contains("does not contain a model list"));
        let output = models_for_accounts(&[(
            "user@example.test".into(),
            json!({"models":[{"slug":"gpt-test\u{1b}[2J"}, {}, {"slug":""}]}),
        )]);
        assert!(!output.contains("user@example.test"));
        assert!(output.contains("Model ID"));
        assert!(output.contains('┌'));
        assert_eq!(safe_text("gpt-test\u{1b}[2J"), "gpt-test[2J");
    }

    #[test]
    fn models_merge_accounts_without_losing_different_metadata() {
        let first =
            json!({"models":[{"slug":"shared","context_window":100}, {"slug":"first-only"}]});
        let second = json!({"data":[{"id":"shared","context_window":100}, {"slug":"shared","context_window":200}, {"slug":"second-only"}]});
        let output = models_for_accounts(&[
            ("one@example.test".into(), first),
            ("two@example.test".into(), second),
        ]);
        assert!(output.contains("Available Accounts"));
        assert!(!output.contains("Default Tier"));
        assert!(!output.contains("@example.test"));
        let shared: Vec<_> = output
            .lines()
            .filter(|line| line.contains("shared"))
            .collect();
        assert!(shared.iter().any(|line| line.contains("100")
            && line
                .split('┆')
                .last()
                .unwrap()
                .trim()
                .trim_end_matches('│')
                .trim()
                == "2"));
        assert!(shared.iter().any(|line| line.contains("200")
            && line
                .split('┆')
                .last()
                .unwrap()
                .trim()
                .trim_end_matches('│')
                .trim()
                == "1"));
        assert_eq!(output.matches("shared").count(), 2);
        assert!(output.contains("first-only"));
        assert!(output.contains("second-only"));
        assert!(output.contains("100"));
        assert!(output.contains("200"));
    }
}
