use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

// Retained solely to recognize account files written by older releases.
const DPAPI_MAGIC: &[u8] = b"codex-proxy:dpapi:v1\0";

// The caller must hold the account file lock across loading and migration.
pub(super) fn load_and_migrate(path: &Path) -> Result<Vec<Account>> {
    let bytes = fs::read(path).context("read account file")?;
    let encrypted = bytes.starts_with(DPAPI_MAGIC);
    let plain = if encrypted {
        unprotect(&bytes[DPAPI_MAGIC.len()..])
            .context("decrypt account file; existing data preserved")?
    } else {
        bytes
    };
    let accounts: Vec<Account> =
        serde_json::from_slice(&plain).context("invalid accounts file; existing data preserved")?;
    if encrypted {
        write_atomic(path, &plain).context("migrate account file; existing data preserved")?;
    }
    Ok(accounts)
}

pub(super) fn save(path: &Path, accounts: &[Account]) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(accounts)?;
    write_atomic(path, &bytes)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .context("create temporary account file")?;
        file.write_all(bytes)
            .context("write temporary account file")?;
        file.sync_all().context("sync temporary account file")?;
        drop(file);
        fs::rename(&temp, path).context("replace account file; existing data preserved")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

#[cfg(windows)]
fn unprotect(bytes: &[u8]) -> Result<Vec<u8>> {
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CryptUnprotectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
        },
    };
    let len = u32::try_from(bytes.len()).context("account file is too large")?;
    let input = CRYPT_INTEGER_BLOB {
        cbData: len,
        pbData: bytes.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: std::ptr::null_mut(),
    };
    // DPAPI allocates output with LocalAlloc; it must be copied before LocalFree.
    let ok = unsafe {
        CryptUnprotectData(
            &input,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if output.pbData.is_null() {
        anyhow::bail!("DPAPI returned an empty buffer");
    }
    let result = unsafe {
        let result = std::slice::from_raw_parts(output.pbData, output.cbData as usize).to_vec();
        LocalFree(output.pbData.cast());
        result
    };
    Ok(result)
}

#[cfg(not(windows))]
fn unprotect(_: &[u8]) -> Result<Vec<u8>> {
    anyhow::bail!("Windows DPAPI account files require their original Windows user account")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub email: String,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<i64>,
    #[serde(default = "enabled_default")]
    pub enabled: bool,
    #[serde(default)]
    pub last_used: i64,
    #[serde(default)]
    pub cooldown_until: Option<i64>,
    #[serde(default)]
    pub quota: Option<Value>,
}
pub(super) fn enabled_default() -> bool {
    true
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountView {
    pub id: String,
    pub email: String,
    pub status: String,
    pub cooldown_until: Option<i64>,
    pub quota: Option<f64>,
    pub quota_secondary: Option<f64>,
    pub quota_primary_reset: Option<i64>,
    pub quota_secondary_reset: Option<i64>,
    pub last_used: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Account {
        Account {
            id: "account-1".into(),
            email: "user@example.test".into(),
            access_token: "secret-access-token".into(),
            refresh_token: Some("secret-refresh-token".into()),
            expires_at: None,
            enabled: true,
            last_used: 0,
            cooldown_until: None,
            quota: None,
        }
    }

    #[cfg(windows)]
    fn legacy_envelope(plain: &[u8]) -> Vec<u8> {
        use windows_sys::Win32::{
            Foundation::LocalFree,
            Security::Cryptography::{
                CryptProtectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
            },
        };
        let input = CRYPT_INTEGER_BLOB {
            cbData: u32::try_from(plain.len()).unwrap(),
            pbData: plain.as_ptr() as *mut u8,
        };
        let mut output = CRYPT_INTEGER_BLOB {
            cbData: 0,
            pbData: std::ptr::null_mut(),
        };
        let ok = unsafe {
            CryptProtectData(
                &input,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        };
        assert_ne!(ok, 0);
        assert!(!output.pbData.is_null());
        let mut result = DPAPI_MAGIC.to_vec();
        unsafe {
            result.extend_from_slice(std::slice::from_raw_parts(
                output.pbData,
                output.cbData as usize,
            ));
            LocalFree(output.pbData.cast());
        }
        result
    }

    #[cfg(windows)]
    #[test]
    fn encrypted_json_is_migrated_without_losing_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accounts.json");
        let mut value = serde_json::to_value(vec![fixture()]).unwrap();
        value[0]["future_field"] = serde_json::json!({"keep": true});
        let plain = serde_json::to_vec(&value).unwrap();
        fs::write(&path, legacy_envelope(&plain)).unwrap();
        let accounts = load_and_migrate(&path).unwrap();
        assert_eq!(accounts.len(), 1);
        assert_eq!(fs::read(&path).unwrap(), plain);
    }

    #[cfg(windows)]
    #[test]
    fn invalid_decrypted_json_keeps_encrypted_original() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accounts.json");
        let encrypted = legacy_envelope(b"not json");
        fs::write(&path, &encrypted).unwrap();
        assert!(load_and_migrate(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), encrypted);
    }

    #[test]
    fn plain_json_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accounts.json");
        fs::write(&path, serde_json::to_vec(&vec![fixture()]).unwrap()).unwrap();
        let accounts = load_and_migrate(&path).unwrap();
        assert_eq!(
            accounts[0].refresh_token.as_deref(),
            Some("secret-refresh-token")
        );
        save(&path, &accounts).unwrap();
        let bytes = fs::read(&path).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json[0]["access_token"], "secret-access-token");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let loaded = load_and_migrate(&path).unwrap();
        assert_eq!(loaded[0].access_token, "secret-access-token");
        assert_eq!(
            loaded[0].refresh_token.as_deref(),
            Some("secret-refresh-token")
        );
    }

    #[test]
    fn corrupted_envelope_fails_without_replacing_original() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accounts.json");
        let mut corrupted = DPAPI_MAGIC.to_vec();
        corrupted.extend_from_slice(b"corrupt ciphertext");
        fs::write(&path, &corrupted).unwrap();
        assert!(load_and_migrate(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), corrupted);
    }

    #[test]
    fn failed_replace_keeps_existing_destination_and_cleans_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("accounts.json");
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("existing"), b"preserve").unwrap();
        assert!(save(&destination, &[fixture()]).is_err());
        assert_eq!(fs::read(destination.join("existing")).unwrap(), b"preserve");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
