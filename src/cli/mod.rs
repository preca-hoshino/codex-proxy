use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    version,
    about = "Personal Codex proxy and command-line management",
    after_help = "Examples:\n  codex-proxy serve\n  codex-proxy serve --host 127.0.0.1 --port 8318\n  codex-proxy account list\n  codex-proxy account add --oauth\n  codex-proxy models\n  codex-proxy config show\n  codex-proxy test --model <MODEL>"
)]
pub struct Cli {
    /// Path to the configuration file
    #[arg(short = 'c', long, default_value = "config.toml", global = true)]
    pub config: PathBuf,
    #[command(subcommand)]
    pub command: Option<Command>,
}
#[derive(Subcommand)]
pub enum Command {
    /// Start the proxy server
    Serve {
        /// Override the configured bind host
        #[arg(long)]
        host: Option<String>,
        /// Override the configured bind port
        #[arg(short, long)]
        port: Option<u16>,
        /// Override the configured accounts file
        #[arg(short, long)]
        accounts: Option<PathBuf>,
        /// Override the upstream transport (wss or http)
        #[arg(long, value_parser = ["wss", "http"])]
        upstream_transport: Option<String>,
    },
    /// Manage local accounts (stop a running instance first)
    Account {
        /// Override the configured accounts file
        #[arg(short, long, global = true)]
        file: Option<PathBuf>,
        #[command(subcommand)]
        action: AccountCommand,
    },
    /// Query the upstream model list
    Models {
        /// Override the configured accounts file
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Filter by account email
        #[arg(short, long)]
        email: Option<String>,
        /// Output raw JSON
        #[arg(long)]
        json: bool,
    },
    /// View, validate, or update configuration
    Config {
        /// Override the configuration file
        #[arg(short, long, global = true)]
        file: Option<PathBuf>,
        #[command(subcommand)]
        action: ConfigCommand,
    },
    /// Test the local proxy with a real request
    Test {
        #[arg(long)]
        model: String,
        #[arg(long)]
        stream: bool,
    },
}
#[derive(Subcommand)]
pub enum AccountCommand {
    /// List accounts and their status
    List {
        /// Output raw JSON
        #[arg(long)]
        json: bool,
    },
    /// Add an account with browser OAuth or an existing refresh token
    Add {
        /// Launch browser OAuth authorization
        #[arg(long, conflicts_with = "token")]
        oauth: bool,
        /// Import an existing refresh token
        #[arg(long)]
        token: Option<String>,
    },
    /// Browser OAuth authorization
    Login,
    /// Import an auth.json file. Refreshing imported credentials may affect the original login.
    Import {
        /// Path to an existing Codex auth.json file
        file: PathBuf,
    },
    /// Remove an account by ID or email
    Remove { id: String },
    /// Enable an account
    Enable { id: String },
    /// Disable an account
    Disable { id: String },
    /// Refresh tokens and quota for one account or all enabled accounts
    Refresh { id: Option<String> },
    /// Clear cooldowns for one account or all accounts
    Reset { id: Option<String> },
}
#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Display the current configuration with secrets redacted
    Show,
    /// Validate the current configuration
    Check,
    /// Update a configuration value
    Set { key: String, value: String },
}
