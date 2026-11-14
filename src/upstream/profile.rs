use std::{process::Stdio, time::Duration};

use serde::Deserialize;
use tokio::{process::Command, sync::OnceCell, time::timeout};

pub const ORIGINATOR: &str = "codex_cli_rs";
const FALLBACK_VERSION: &str = "0.164.0";
const RELEASE_URL: &str = "https://registry.npmjs.org/@openai%2Fcodex/latest";
const MAX_RELEASE_BYTES: usize = 256 * 1024;
const PLATFORMS: [&str; 6] = [
    "darwin-arm64",
    "darwin-x64",
    "linux-arm64",
    "linux-x64",
    "win32-arm64",
    "win32-x64",
];
static PROFILE: OnceCell<ClientProfile> = OnceCell::const_new();

/// One identity for model discovery, OAuth and upstream generation requests.
pub struct ClientProfile {
    pub version: String,
    pub source: &'static str,
}

/// Discover the official stable release once using the configured outbound client.
/// A failed discovery leaves the local CLI and offline version available.
pub async fn initialize(client: &reqwest::Client) -> &'static ClientProfile {
    PROFILE
        .get_or_init(|| async {
            if let Ok(Some(version)) = timeout(Duration::from_secs(5), fetch_release(client)).await
            {
                return ClientProfile {
                    version,
                    source: "official npm stable",
                };
            }
            if let Ok(Some(version)) = timeout(Duration::from_secs(2), local_cli_version()).await {
                return ClientProfile {
                    version,
                    source: "local Codex CLI",
                };
            }
            ClientProfile {
                version: FALLBACK_VERSION.to_owned(),
                source: "offline fallback",
            }
        })
        .await
}

pub fn version() -> &'static str {
    PROFILE
        .get()
        .map_or(FALLBACK_VERSION, |profile| profile.version.as_str())
}

pub fn user_agent() -> String {
    format!("{ORIGINATOR}/{}", version())
}

async fn fetch_release(client: &reqwest::Client) -> Option<String> {
    let mut response = client.get(RELEASE_URL).send().await.ok()?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|length| length > MAX_RELEASE_BYTES as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len().saturating_add(chunk.len()) > MAX_RELEASE_BYTES {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    parse_release(&body)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NpmRelease {
    name: String,
    version: String,
    optional_dependencies: std::collections::HashMap<String, String>,
}

fn parse_release(body: &[u8]) -> Option<String> {
    if body.len() > MAX_RELEASE_BYTES {
        return None;
    }
    let release: NpmRelease = serde_json::from_slice(body).ok()?;
    if release.name != "@openai/codex" || !stable_version(&release.version) {
        return None;
    }
    if PLATFORMS.iter().any(|platform| {
        release
            .optional_dependencies
            .get(&format!("@openai/codex-{platform}"))
            != Some(&format!("npm:@openai/codex@{}-{platform}", release.version))
    }) {
        return None;
    }
    Some(release.version)
}

fn stable_version(version: &str) -> bool {
    let parts: Vec<_> = version.split('.').collect();
    version.len() <= 64
        && parts.len() == 3
        && parts.iter().all(|part| {
            !part.is_empty()
                && (part.len() == 1 || !part.starts_with('0'))
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && part.parse::<u64>().is_ok()
        })
}

async fn local_cli_version() -> Option<String> {
    let output = Command::new("codex")
        .arg("--version")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_cli_output(&String::from_utf8(output.stdout).ok()?)
}

fn parse_cli_output(output: &str) -> Option<String> {
    let version = output.trim().strip_prefix("codex-cli ")?;
    stable_version(version).then(|| version.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn release() -> serde_json::Value {
        let dependencies: serde_json::Map<_, _> = PLATFORMS
            .iter()
            .map(|platform| {
                (
                    format!("@openai/codex-{platform}"),
                    json!(format!("npm:@openai/codex@0.169.0-{platform}")),
                )
            })
            .collect();
        json!({
            "name": "@openai/codex",
            "version": "0.169.0",
            "optionalDependencies": dependencies,
        })
    }

    #[test]
    fn official_release_requires_stable_matching_platforms() {
        let mut release = release();
        assert_eq!(
            parse_release(&serde_json::to_vec(&release).unwrap()).as_deref(),
            Some("0.169.0")
        );
        release["optionalDependencies"]["@openai/codex-win32-x64"] =
            json!("npm:@openai/codex@0.168.0-win32-x64");
        assert!(parse_release(&serde_json::to_vec(&release).unwrap()).is_none());
        release["version"] = json!("0.169.0-alpha.1");
        assert!(parse_release(&serde_json::to_vec(&release).unwrap()).is_none());
    }

    #[test]
    fn invalid_or_unofficial_releases_are_rejected() {
        let mut release = release();
        release["name"] = json!("@other/codex");
        assert!(parse_release(&serde_json::to_vec(&release).unwrap()).is_none());
        assert!(parse_release(br#"{"version":"0.169.0"}"#).is_none());
        assert!(parse_release(&vec![b' '; MAX_RELEASE_BYTES + 1]).is_none());
        for version in [
            "0.169.0+build",
            "0.169.0-alpha.1",
            "0.0169.0",
            "0.169",
            "a.b.c",
        ] {
            assert!(!stable_version(version), "{version}");
        }
    }

    #[test]
    fn local_cli_requires_recognized_stable_version_output() {
        assert_eq!(
            parse_cli_output("codex-cli 0.164.0\r\n").as_deref(),
            Some("0.164.0")
        );
        assert!(parse_cli_output("unrelated 0.164.0").is_none());
        assert!(parse_cli_output("codex-cli 0.164.0-alpha.1").is_none());
        assert!(parse_cli_output("codex-cli 0.164.0\nunexpected").is_none());
    }
}
