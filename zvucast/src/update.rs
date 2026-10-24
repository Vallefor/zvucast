use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const TARGETS: [&str; 4] = ["x86_64", "aarch64", "armv7-rpi", "armv7-opi"];

pub fn reported_version() -> String {
    let base = env!("CARGO_PKG_VERSION");
    match option_env!("ZVUCAST_RELEASE") {
        Some("1") => base.to_string(),
        _ => format!("{base}+dev"),
    }
}

pub fn baked_target() -> &'static str {
    match option_env!("ZVUCAST_TARGET") {
        Some(t) if TARGETS.contains(&t) => t,
        _ => "unknown",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offer {
    Hidden,
    Ready,
    MissingAsset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asset {
    pub name: String,
    pub url: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub version: String,
    pub assets: BTreeMap<String, Asset>,
}

pub fn asset_url_ok(url: &str, target: &str) -> bool {
    let Some(rest) = url.strip_prefix("https://github.com/Vallefor/zvucast/releases/download/v") else {
        return false;
    };
    let Some((ver, file)) = rest.split_once('/') else {
        return false;
    };
    if file != format!("zvucast-{target}") || rest.matches('/').count() != 1 {
        return false;
    }
    let mut parts = ver.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(a), Some(b), Some(c), None)
            if !a.is_empty()
                && !b.is_empty()
                && !c.is_empty()
                && a.chars().all(|ch| ch.is_ascii_digit())
                && b.chars().all(|ch| ch.is_ascii_digit())
                && c.chars().all(|ch| ch.is_ascii_digit()) =>
        {
            true
        }
        _ => false,
    }
}

pub fn redirect_host_ok(host: &str) -> bool {
    matches!(
        host,
        "release-assets.githubusercontent.com" | "objects.githubusercontent.com"
    )
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let dig = Sha256::digest(bytes);
    dig.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn sum_matches(bytes: &[u8], expect_hex: &str) -> bool {
    sha256_hex(bytes) == expect_hex
}

pub fn update_offer(
    online: bool,
    target: &str,
    node_version: &str,
    release_version: &str,
    asset_present: bool,
) -> Offer {
    if !online || !TARGETS.contains(&target) || node_version == release_version {
        return Offer::Hidden;
    }
    if asset_present {
        Offer::Ready
    } else {
        Offer::MissingAsset
    }
}

pub fn update_order<'a>(ids: &[&'a str], master: Option<&'a str>) -> Vec<&'a str> {
    let mut out: Vec<&str> = ids.iter().copied().filter(|id| Some(*id) != master).collect();
    if let Some(m) = master {
        if ids.contains(&m) {
            out.push(m);
        }
    }
    out
}

pub fn update_body_ok(body: &str) -> bool {
    body.trim().is_empty()
}

pub struct ReleaseCache {
    inner: Mutex<Option<(Instant, Release)>>,
}

impl ReleaseCache {
    pub fn new() -> Self {
        Self { inner: Mutex::new(None) }
    }

    pub fn page(&self, fetch: impl Fn() -> Result<(String, String), String>) -> Result<Release, String> {
        if let Some((at, rel)) = self.inner.lock().map_err(|e| e.to_string())?.as_ref() {
            if at.elapsed() < Duration::from_secs(300) {
                return Ok(rel.clone());
            }
        }
        self.fresh(fetch)
    }

    pub fn fresh(&self, fetch: impl Fn() -> Result<(String, String), String>) -> Result<Release, String> {
        let (latest, sums) = fetch()?;
        let release = parse_release(&latest, &sums)?;
        *self.inner.lock().map_err(|e| e.to_string())? = Some((Instant::now(), release.clone()));
        Ok(release)
    }
}

pub fn sums_url_ok(url: &str) -> bool {
    asset_url_ok(&url.replacen("SHA256SUMS", "zvucast-x86_64", 1), "x86_64") && url.ends_with("/SHA256SUMS")
}

pub async fn fetch_latest() -> Result<(String, String), String> {
    let client = github_client()?;
    let latest = client
        .get("https://api.github.com/repos/Vallefor/zvucast/releases/latest")
        .header("User-Agent", "zvucast")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;
    let parsed: serde_json::Value = serde_json::from_str(&latest).map_err(|e| e.to_string())?;
    let mut sums_url = None;
    if let Some(list) = parsed.get("assets").and_then(|a| a.as_array()) {
        for asset in list {
            if asset.get("name").and_then(|n| n.as_str()) == Some("SHA256SUMS") {
                sums_url = asset
                    .get("browser_download_url")
                    .and_then(|u| u.as_str())
                    .map(str::to_string);
            }
        }
    }
    let sums_url = sums_url.ok_or("нет SHA256SUMS")?;
    if !sums_url_ok(&sums_url) {
        return Err("чужой SHA256SUMS".into());
    }
    let sums = client
        .get(&sums_url)
        .header("User-Agent", "zvucast")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;
    Ok((latest, sums))
}

fn github_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::custom(|try_| {
            let host = try_.url().host_str().unwrap_or("");
            if redirect_host_ok(host) {
                try_.follow()
            } else {
                try_.error("чужой редирект")
            }
        }))
        .build()
        .map_err(|e| e.to_string())
}

pub async fn download_asset(url: &str, target: &str, sha256: &str) -> Result<Vec<u8>, String> {
    if !asset_url_ok(url, target) {
        return Err("адрес".into());
    }
    let client = github_client()?;
    let body = client
        .get(url)
        .header("User-Agent", "zvucast")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;
    if !sum_matches(&body, sha256) {
        return Err("сумма".into());
    }
    Ok(body.to_vec())
}

pub const APPLY_STATUS_PATH: &str = "/run/zvucast-apply-update.json";
const APPLY_UNIT: &str = "zvucast-apply-update";

pub fn write_script(dir: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
    let path = dir.join("apply-update.sh");
    std::fs::write(&path, include_str!("../../deploy/apply-update.sh"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms)?;
    }
    Ok(path)
}

/// Скрипт сам берёт `/var/lock/zvucast-update`. Второй `flock` до `systemd-run`
/// гонялся бы с ним, поэтому блокировку здесь не открываем. `--wait` нет:
/// `systemd-run` возвращается, когда юнит стартовал. Любой ненулевой статус
/// этого запуска — «уже обновляется». Ноль — служба стартовала, дальше
/// [`wait_apply_unit`]. `--wait` на этом пути убил бы рестарт агента и вместе
/// с ним отказ скрипта, который вышел до записи статуса.
pub fn spawn_apply(
    script: &std::path::Path,
    file: &std::path::Path,
    sha256: &str,
    from: &str,
    to: &str,
) -> std::io::Result<()> {
    let status = std::process::Command::new("systemd-run")
        .arg("--collect")
        .arg("--unit=zvucast-apply-update")
        .arg(script)
        .arg(file)
        .arg(sha256)
        .arg(from)
        .arg(to)
        .status()?;
    map_systemd_run_status(status)
}

fn map_systemd_run_status(status: std::process::ExitStatus) -> std::io::Result<()> {
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "уже обновляется"))
    }
}

/// Юнит ещё не кончился. `deactivating` тоже: скрипт ещё дописывает статус.
fn unit_state_running(state: &str) -> bool {
    matches!(state, "active" | "activating" | "reloading" | "deactivating")
}

fn apply_unit_state(unit: &str) -> String {
    let Ok(output) = std::process::Command::new("systemctl")
        .args(["show", "-p", "ActiveState", "--value", unit])
        .output()
    else {
        return String::new();
    };
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// `ok` и `rolled_back` забирает новый процесс агента. Иной текст или пустой
/// файл — этот процесс ещё жив, юнит кончился без итога.
pub fn apply_outcome(text: Option<&str>) -> Result<(), String> {
    if status_settled(text) {
        Ok(())
    } else {
        Err("служба завершилась".into())
    }
}

fn status_settled(text: Option<&str>) -> bool {
    let Some(text) = text else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return false;
    };
    matches!(v.get("result").and_then(|r| r.as_str()), Some("ok" | "rolled_back"))
}

/// После нуля от `systemd-run`. Рестарт агента (успех и откат) может убить
/// этот цикл: новый процесс читает файл статуса. Смерть ожидания сама по себе
/// `UpdateFailed` не шлёт — ошибка только если юнит уже кончился, а в файле
/// нет `ok` и нет `rolled_back`.
pub fn wait_apply_unit() -> Result<(), String> {
    while unit_state_running(&apply_unit_state(APPLY_UNIT)) {
        std::thread::sleep(Duration::from_millis(200));
    }
    let text = std::fs::read_to_string(APPLY_STATUS_PATH).ok();
    apply_outcome(text.as_deref())
}

pub fn parse_release(latest_json: &str, sums: &str) -> Result<Release, String> {
    let v: serde_json::Value = serde_json::from_str(latest_json).map_err(|e| e.to_string())?;
    if v.get("prerelease").and_then(|p| p.as_bool()).unwrap_or(false) {
        return Err("пререлиз".into());
    }
    let tag = v.get("tag_name").and_then(|t| t.as_str()).ok_or("нет тега")?;
    let version = tag.strip_prefix('v').ok_or("тег без v")?.to_string();
    let mut by_name = BTreeMap::new();
    for line in sums.lines() {
        let mut it = line.split_whitespace();
        if let (Some(sum), Some(name)) = (it.next(), it.next()) {
            by_name.insert(name.to_string(), sum.to_string());
        }
    }
    let mut assets = BTreeMap::new();
    let list = v.get("assets").and_then(|a| a.as_array()).cloned().unwrap_or_default();
    for asset in list {
        let name = asset.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let url = asset.get("browser_download_url").and_then(|n| n.as_str()).unwrap_or("");
        let Some(target) = name.strip_prefix("zvucast-") else {
            continue;
        };
        if !TARGETS.contains(&target) {
            continue;
        }
        let sha = by_name.get(name).cloned().unwrap_or_default();
        assets.insert(
            target.to_string(),
            Asset { name: name.to_string(), url: url.to_string(), sha256: sha },
        );
    }
    Ok(Release { version, assets })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_the_nodes_own_asset() {
        let ok = "https://github.com/Vallefor/zvucast/releases/download/v0.1.0/zvucast-aarch64";
        assert!(asset_url_ok(ok, "aarch64"));
        assert!(!asset_url_ok(ok, "armv7-opi"));
        assert!(!asset_url_ok(
            "http://github.com/Vallefor/zvucast/releases/download/v0.1.0/zvucast-aarch64",
            "aarch64"
        ));
        assert!(!asset_url_ok(
            "https://github.com/other/zvucast/releases/download/v0.1.0/zvucast-aarch64",
            "aarch64"
        ));
        assert!(!asset_url_ok(
            "https://github.com/Vallefor/zvucast/releases/download/v0.1.0-rc.1/zvucast-aarch64",
            "aarch64"
        ));
        assert!(sums_url_ok(
            "https://github.com/Vallefor/zvucast/releases/download/v0.1.0/SHA256SUMS"
        ));
        assert!(!sums_url_ok("https://example.com/SHA256SUMS"));
    }

    #[test]
    fn redirects_only_to_github_asset_hosts() {
        assert!(!redirect_host_ok("github.com"));
        assert!(redirect_host_ok("release-assets.githubusercontent.com"));
        assert!(redirect_host_ok("objects.githubusercontent.com"));
        assert!(!redirect_host_ok("example.com"));
        assert!(!redirect_host_ok("objects.githubusercontent.com.evil.com"));
    }

    #[test]
    fn wrong_sum_does_not_match() {
        assert!(sum_matches(b"abc", &sha256_hex(b"abc")));
        assert!(!sum_matches(b"abc", &sha256_hex(b"abd")));
    }

    #[test]
    fn offer_follows_version_and_target() {
        assert_eq!(update_offer(true, "aarch64", "0.1.0+dev", "0.1.0", true), Offer::Ready);
        assert_eq!(update_offer(true, "aarch64", "0.1.0", "0.1.0", true), Offer::Hidden);
        assert_eq!(update_offer(true, "", "0.1.0+dev", "0.1.0", true), Offer::Hidden);
        assert_eq!(update_offer(false, "aarch64", "0.1.0+dev", "0.1.0", true), Offer::Hidden);
        assert_eq!(update_offer(true, "aarch64", "0.1.0+dev", "0.1.0", false), Offer::MissingAsset);
    }

    #[test]
    fn master_is_last() {
        let ids = ["a", "master", "b"];
        assert_eq!(update_order(&ids, Some("master")), vec!["a", "b", "master"]);
    }

    #[test]
    fn update_body_is_empty() {
        assert!(update_body_ok(""));
        assert!(update_body_ok(" \n"));
        assert!(!update_body_ok(r#"{"url":"https://example"}"#));
    }

    #[test]
    fn parses_release_and_sums() {
        let json = r#"{"tag_name":"v0.1.0","prerelease":false,"assets":[
            {"name":"zvucast-aarch64","browser_download_url":"https://github.com/Vallefor/zvucast/releases/download/v0.1.0/zvucast-aarch64"},
            {"name":"SHA256SUMS","browser_download_url":"https://github.com/Vallefor/zvucast/releases/download/v0.1.0/SHA256SUMS"}
        ]}"#;
        let sums = "abc123  zvucast-aarch64\n";
        let rel = parse_release(json, sums).unwrap();
        assert_eq!(rel.version, "0.1.0");
        assert_eq!(rel.assets["aarch64"].sha256, "abc123");
        assert!(parse_release(r#"{"tag_name":"v0.1.0","prerelease":true,"assets":[]}"#, "").is_err());
    }

    #[test]
    fn dev_build_has_dev_suffix_unless_release_flag() {
        let v = reported_version();
        let base = env!("CARGO_PKG_VERSION");
        match option_env!("ZVUCAST_RELEASE") {
            Some("1") => assert_eq!(v, base),
            _ => assert_eq!(v, format!("{base}+dev")),
        }
    }

    #[test]
    fn page_cache_is_five_minutes_fresh_bypasses_it() {
        use std::cell::Cell;
        let cache = ReleaseCache::new();
        let calls = Cell::new(0u32);
        let fetch = || {
            calls.set(calls.get() + 1);
            Ok((
                r#"{"tag_name":"v0.1.0","prerelease":false,"assets":[]}"#.to_string(),
                String::new(),
            ))
        };
        assert_eq!(cache.page(&fetch).unwrap().version, "0.1.0");
        assert_eq!(cache.page(&fetch).unwrap().version, "0.1.0");
        assert_eq!(calls.get(), 1);
        assert_eq!(cache.fresh(&fetch).unwrap().version, "0.1.0");
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn unknown_target_without_env() {
        let t = baked_target();
        if option_env!("ZVUCAST_TARGET").is_none() {
            assert_eq!(t, "unknown");
        } else {
            assert!(TARGETS.contains(&t));
        }
    }

    #[tokio::test]
    async fn refuses_foreign_url_without_a_connection() {
        let err = download_asset("https://example.com/zvucast-aarch64", "aarch64", "abc")
            .await
            .unwrap_err();
        assert!(err.contains("адрес"));
    }

    #[test]
    fn unit_end_without_ok_status_is_a_failure() {
        assert!(unit_state_running("active"));
        assert!(unit_state_running("activating"));
        assert!(unit_state_running("deactivating"));
        assert!(!unit_state_running("inactive"));
        assert!(!unit_state_running("failed"));
        assert!(!unit_state_running(""));

        assert!(apply_outcome(None).is_err());
        assert!(apply_outcome(Some("не json")).is_err());
        assert!(apply_outcome(Some(r#"{"result":"failed","from":"a","to":"b"}"#)).is_err());
        assert!(apply_outcome(Some(r#"{"result":"ok","from":"0.1.0+dev","to":"0.1.0"}"#)).is_ok());
        assert!(apply_outcome(Some(r#"{"result":"rolled_back","from":"0.1.0+dev","to":"0.1.0"}"#)).is_ok());
    }

    #[test]
    fn any_nonzero_systemd_run_status_is_already_updating() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::ExitStatus;

        assert!(map_systemd_run_status(ExitStatus::from_raw(0)).is_ok());
        for code in [1, 2] {
            let err = map_systemd_run_status(ExitStatus::from_raw(code << 8)).unwrap_err();
            assert_eq!(err.to_string(), "уже обновляется");
        }
    }

    #[test]
    fn write_script_in_another_directory_leaves_the_first_file() {
        let root = std::env::temp_dir().join(format!(
            "zvucast-script-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let first_dir = root.join("one");
        let second_dir = root.join("two");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = write_script(&first_dir).unwrap();
        let bytes = std::fs::read(&first).unwrap();
        let second = write_script(&second_dir).unwrap();
        assert_eq!(first, first_dir.join("apply-update.sh"));
        assert_eq!(second, second_dir.join("apply-update.sh"));
        assert_eq!(std::fs::read(&first).unwrap(), bytes);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
