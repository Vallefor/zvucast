use std::path::Path;

use serde::Deserialize;
use zproto::Role;

const MCAST_GROUP: &str = "239.0.0.1";
const PORT: u16 = 5004;

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineCfg {
    pub jb_ms: u32,
    pub rx_buffer_us: u32,
    pub rx_latency_us: u32,
    pub tx_buffer_us: u32,
    pub tx_latency_us: u32,
    pub mcast_group: String,
    pub port: u16,
}

impl PipelineCfg {
    pub fn default_rx() -> Self {
        Self {
            jb_ms: 2,
            rx_buffer_us: 5000,
            rx_latency_us: 1000,
            tx_buffer_us: 10000,
            tx_latency_us: 2500,
            mcast_group: MCAST_GROUP.into(),
            port: PORT,
        }
    }

    pub fn default_tx() -> Self {
        Self {
            jb_ms: 2,
            rx_buffer_us: 5000,
            rx_latency_us: 1000,
            tx_buffer_us: 10000,
            tx_latency_us: 2500,
            mcast_group: MCAST_GROUP.into(),
            port: PORT,
        }
    }
}

fn default_ptp_priority() -> u8 {
    128
}

/// Бэкенд вывода для роли rx: alsa = alsasink на playback_device;
/// pipewire = pipewiresink (маршрутизация WirePlumber на дефолтный выход) —
/// для десктопов, где PipeWire эксклюзивно держит ALSA-устройство.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Alsa,
    PipeWire,
}

fn default_backend() -> Backend {
    Backend::Alsa
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub name: String,
    /// Стабильный идентификатор узла (uuid): генерируется при первом старте
    /// и дописывается в этот же конфиг (см. load_or_init_id).
    #[serde(default)]
    pub id: Option<String>,
    /// Явные роли узла; None (поле отсутствует) — авто-детект пробой ALSA
    /// (capture_device открывается → tx, playback_device открывается → rx);
    /// при playback_backend="pipewire" роль rx пробуется pipewiresink'ом.
    #[serde(default)]
    pub roles: Option<Vec<Role>>,
    pub controller: String,
    /// Вход (alsasrc) для роли tx.
    #[serde(default)]
    pub capture_device: Option<String>,
    /// Выход (alsasink) для роли rx.
    #[serde(default)]
    pub playback_device: Option<String>,
    /// Бэкенд вывода rx (см. Backend). При pipewire playback_device не нужен.
    #[serde(default = "default_backend")]
    pub playback_backend: Backend,
    /// Сетевой интерфейс для PTP (zsync). GM выбирается BMCA по ptp_priority.
    pub sync_interface: String,
    /// PTP priority1: меньше = лучше кандидат в Grandmaster (pi4 = 100).
    #[serde(default = "default_ptp_priority")]
    pub ptp_priority: u8,
    pub pipeline: PipelineCfg,
}

pub fn load(path: &Path) -> anyhow::Result<AgentConfig> {
    let text = std::fs::read_to_string(path)?;
    // Понятная ошибка миграции: serde с deny_unknown_fields выдал бы просто
    // "unknown field `role`" — заменяем на инструкцию, как переписать конфиг.
    for line in text.lines() {
        let l = line.trim_start();
        if l.starts_with("role ") || l.starts_with("role=") || l.starts_with("alsa_device") {
            anyhow::bail!(
                "старый формат конфига {path:?}: role = \"tx\" → roles = [\"tx\"], \
                 alsa_device → capture_device (для tx) и/или playback_device (для rx); \
                 ptp_priority опционален (дефолт 128)",
                path = path
            );
        }
    }
    Ok(toml::from_str(&text)?)
}

/// Загружает конфиг; если id отсутствует — генерирует uuid и дописывает в файл.
pub fn load_or_init_id(path: &Path) -> anyhow::Result<(AgentConfig, String)> {
    let mut cfg = load(path)?;
    if let Some(id) = cfg.id.clone() { return Ok((cfg, id)); }
    let id = uuid::Uuid::new_v4().to_string();
    // id пишется в НАЧАЛО файла: append ненадёжен — после секции [pipeline]
    // toml отнёс бы ключ к таблице pipeline, а не к корню конфига.
    let text = std::fs::read_to_string(path)?;
    let mut f = std::fs::File::create(path)?;
    use std::io::Write;
    writeln!(f, "id = \"{id}\"")?;
    f.write_all(text.as_bytes())?;
    cfg.id = Some(id.clone());
    Ok((cfg, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_FULL: &str = r#"
name = "opi-rx2"
roles = ["rx"]
controller = "ws://192.0.2.204:8080/ws"
playback_device = "plughw:CARD=Codec"
sync_interface = "end0"
ptp_priority = 128

[pipeline]
jb_ms = 2
rx_buffer_us = 5000
rx_latency_us = 1000
tx_buffer_us = 10000
tx_latency_us = 2500
mcast_group = "239.0.0.1"
port = 5004
"#;

    // Авто-детект: roles и ptp_priority отсутствуют (конфиг pc).
    const SAMPLE_AUTO: &str = r#"
name = "pc"
controller = "ws://192.0.2.204:8080/ws"
capture_device = "plughw:CARD=Generic_1"
playback_device = "plughw:CARD=Generic_1"
sync_interface = "eth0"

[pipeline]
jb_ms = 2
rx_buffer_us = 5000
rx_latency_us = 1000
tx_buffer_us = 10000
tx_latency_us = 2500
mcast_group = "239.0.0.1"
port = 5004
"#;

    // Формат до mixed-roles: role + alsa_device.
    const SAMPLE_OLD: &str = r#"
name = "opi-rx2"
role = "rx"
controller = "ws://192.0.2.204:8080/ws"
alsa_device = "plughw:CARD=Codec"
sync_interface = "end0"

[pipeline]
jb_ms = 2
rx_buffer_us = 5000
rx_latency_us = 1000
tx_buffer_us = 10000
tx_latency_us = 2500
mcast_group = "239.0.0.1"
port = 5004
"#;

    fn write_tmp(text: &str, tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zvucast-cfg-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("zvucast.toml");
        std::fs::write(&p, text).unwrap();
        p
    }

    #[test]
    fn parses_explicit_roles() {
        let c = load(&write_tmp(SAMPLE_FULL, "full")).unwrap();
        assert_eq!(c.name, "opi-rx2");
        assert_eq!(c.roles, Some(vec![zproto::Role::Rx]));
        assert_eq!(c.playback_device.as_deref(), Some("plughw:CARD=Codec"));
        assert!(c.capture_device.is_none());
        assert_eq!(c.ptp_priority, 128);
        assert_eq!(c.pipeline.jb_ms, 2);
    }

    #[test]
    fn defaults_when_optional_omitted() {
        let c = load(&write_tmp(SAMPLE_AUTO, "auto")).unwrap();
        assert_eq!(c.roles, None, "roles отсутствует — авто-детект");
        assert_eq!(c.ptp_priority, 128, "дефолт priority1");
        assert_eq!(c.capture_device.as_deref(), Some("plughw:CARD=Generic_1"));
        assert_eq!(c.playback_device.as_deref(), Some("plughw:CARD=Generic_1"));
    }

    #[test]
    fn old_format_fails_with_clear_error() {
        let e = load(&write_tmp(SAMPLE_OLD, "old")).unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains("старый формат"), "понятная ошибка миграции: {msg}");
        assert!(msg.contains("roles"), "подсказка про roles: {msg}");
        assert!(msg.contains("capture_device"), "подсказка про capture_device: {msg}");
    }

    #[test]
    fn unknown_field_rejected() {
        let bad = SAMPLE_FULL.replace("playback_device", "playback_devise");
        let e = load(&write_tmp(&bad, "typo")).unwrap_err();
        assert!(format!("{e:#}").contains("unknown field"), "опечатка в поле ловится: {e:#}");
    }

    #[test]
    fn playback_backend_default_and_pipewire() {
        // дефолт — alsa
        let cfg: AgentConfig = toml::from_str(SAMPLE_AUTO).unwrap();
        assert_eq!(cfg.playback_backend, Backend::Alsa);
        // явный pipewire
        let text = SAMPLE_AUTO.replace("sync_interface", "playback_backend = \"pipewire\"\nsync_interface");
        let cfg: AgentConfig = toml::from_str(&text).unwrap();
        assert_eq!(cfg.playback_backend, Backend::PipeWire);
        // неизвестный бэкенд — ошибка парсинга
        let bad = SAMPLE_AUTO.replace("sync_interface", "playback_backend = \"pulse\"\nsync_interface");
        assert!(toml::from_str::<AgentConfig>(&bad).is_err());
    }

    #[test]
    fn node_id_generated_and_persisted() {
        let dir = std::env::temp_dir().join(format!("zvcfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.toml");
        std::fs::write(&path, r#"
name = "t"
controller = "ws://127.0.0.1:8080/ws"
sync_interface = "lo"
[pipeline]
jb_ms = 2
rx_buffer_us = 5000
rx_latency_us = 1000
tx_buffer_us = 10000
tx_latency_us = 2500
mcast_group = "239.0.0.1"
port = 5004
"#).unwrap();
        let (_c1, id1) = super::load_or_init_id(&path).unwrap();
        assert!(!id1.is_empty());
        let (_c2, id2) = super::load_or_init_id(&path).unwrap();
        assert_eq!(id1, id2, "id пережил перезапуск");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn defaults_match_poc() {
        let rx = PipelineCfg::default_rx();
        assert_eq!((rx.jb_ms, rx.rx_buffer_us, rx.rx_latency_us), (2, 5000, 1000));
        let tx = PipelineCfg::default_tx();
        assert_eq!((tx.tx_buffer_us, tx.tx_latency_us), (10000, 2500));
        assert_eq!(tx.mcast_group, "239.0.0.1");
        assert_eq!(tx.port, 5004);
    }
}
