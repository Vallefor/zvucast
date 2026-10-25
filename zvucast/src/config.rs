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

/// Бэкенд вывода для роли rx: alsa = alsasink на цель из УИ;
/// pipewire = pw-play. Пустая цель — запасной sink, не zvucast.
/// Для десктопов, где PipeWire эксклюзивно держит ALSA-устройство.
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
    /// Явные роли узла; None (поле отсутствует) — авто-детект.
    /// pipewire пробует tx и rx без имени устройства; alsa открывать нечего —
    /// roles надо задать явно.
    #[serde(default)]
    pub roles: Option<Vec<Role>>,
    pub controller: String,
    /// Бэкенд вывода rx (см. Backend). Устройство берётся из команды УИ.
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
    // capture_device и playback_device больше не выбирают устройство.
    // Ключи выкидываем до разбора; комментарии и playback_backend остаются.
    // Файл на диске не переписываем.
    let text = strip_retired_device_keys(&text);
    Ok(toml::from_str(&text)?)
}

/// Убирает присваивания capture_device и playback_device. Строка-комментарий
/// и playback_backend не трогаются.
fn strip_retired_device_keys(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let body = line.trim_end_matches(['\r', '\n']);
        let trimmed = body.trim_start();
        if !trimmed.starts_with('#') && retired_device_assignment(trimmed) {
            continue;
        }
        out.push_str(line);
    }
    out
}

fn retired_device_assignment(trimmed: &str) -> bool {
    for key in ["capture_device", "playback_device"] {
        if let Some(rest) = trimmed.strip_prefix(key) {
            if rest.trim_start().starts_with('=') {
                return true;
            }
        }
    }
    false
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
        assert_eq!(c.ptp_priority, 128);
        assert_eq!(c.pipeline.jb_ms, 2);
    }

    #[test]
    fn defaults_when_optional_omitted() {
        let c = load(&write_tmp(SAMPLE_AUTO, "auto")).unwrap();
        assert_eq!(c.roles, None, "roles отсутствует — авто-детект");
        assert_eq!(c.ptp_priority, 128, "дефолт priority1");
    }

    #[test]
    fn load_strips_device_keys_keeps_backend_and_comments() {
        let text = r#"
name = "pc"
controller = "ws://192.0.2.204:8080/ws"
# capture_device = "from-comment"
playback_backend = "pipewire"
capture_device = "from-key"
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
        let path = write_tmp(text, "strip");
        let before = std::fs::read_to_string(&path).unwrap();
        let c = load(&path).unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        assert_eq!(before, after, "файл на диске не переписывается");
        assert!(after.contains("# capture_device = \"from-comment\""));
        assert!(after.contains("capture_device = \"from-key\""));
        assert_eq!(c.name, "pc", "ключи выкинуты до разбора, иначе deny_unknown_fields");
        assert_eq!(c.playback_backend, Backend::PipeWire);
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
        // дефолт — alsa. Ключи устройств выкинуты: в структуре их больше нет.
        let cfg: AgentConfig = toml::from_str(&strip_retired_device_keys(SAMPLE_AUTO)).unwrap();
        assert_eq!(cfg.playback_backend, Backend::Alsa);
        // явный pipewire
        let text = SAMPLE_AUTO.replace("sync_interface", "playback_backend = \"pipewire\"\nsync_interface");
        let cfg: AgentConfig = toml::from_str(&strip_retired_device_keys(&text)).unwrap();
        assert_eq!(cfg.playback_backend, Backend::PipeWire);
        // неизвестный бэкенд — ошибка парсинга
        let bad = SAMPLE_AUTO.replace("sync_interface", "playback_backend = \"pulse\"\nsync_interface");
        assert!(toml::from_str::<AgentConfig>(&strip_retired_device_keys(&bad)).is_err());
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
