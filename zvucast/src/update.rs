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
}
