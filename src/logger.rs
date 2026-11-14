use tokio::sync::broadcast;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_logger() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .compact();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
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
