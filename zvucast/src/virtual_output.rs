//! Виртуальный стереовыход на каждую зону передатчика: pw-loopback, не null-sink.
//!
//! Sink `zvucast-{zone}` — то, что выбирают в апплете (`Zvucast: {имя}`).
//! Source `zvucast-{zone}-monitor` читает pw-record. Модуль сам забирает
//! буферы, поэтому выбор выхода не останавливает колонки.

use std::collections::{BTreeSet, HashMap};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Context, Result};
use zproto::{CaptureChan, VirtualZone};

use crate::inputs::{is_virtual_device_key, is_virtual_sink_name, VIRTUAL_DEVICE_KEY};

const LEGACY_LOOPBACK: &str = "zvucast-cable";
const NULL_SINK_FACTORY: &str = "support.null-audio-sink";

pub fn sink_name(zone_id: &str) -> String {
    format!("zvucast-{zone_id}")
}

pub fn monitor_name(zone_id: &str) -> String {
    format!("zvucast-{zone_id}-monitor")
}

fn process_name(zone_id: &str) -> String {
    format!("zvucast-zone-{zone_id}")
}

/// Plasma считает устройство виртуальным без флага HARDWARE и прячет его.
/// pipewire-pulse ставит HARDWARE, когда `node.virtual` не true.
/// Пробел и двоеточие в «Zvucast: Зал» без кавычек — синтаксическая ошибка pw-loopback.
fn pw_value(value: &str) -> String {
    let clean = value.replace('"', "");
    if clean.chars().any(|c| c.is_whitespace() || matches!(c, ':' | '=')) {
        format!("\"{clean}\"")
    } else {
        clean
    }
}

fn capture_props(name: &str, description: &str) -> String {
    format!(
        "media.class=Audio/Sink node.name={name} node.description={} audio.rate=48000 node.latency=128/48000 node.virtual=false",
        pw_value(description)
    )
}

/// Источник кабеля в списке микрофонов не нужен.
fn playback_props(name: &str, description: &str) -> String {
    format!(
        "media.class=Audio/Source node.name={name} node.description={} audio.rate=48000 node.latency=128/48000 node.passive=true node.virtual=true",
        pw_value(description)
    )
}

fn applet_label(name: &str) -> String {
    let clean: String = name
        .chars()
        .filter(|c| !matches!(c, '"' | '\n' | '\r' | '='))
        .collect();
    let clean = clean.trim();
    if clean.is_empty() {
        "Zvucast".into()
    } else {
        format!("Zvucast: {clean}")
    }
}

pub fn zone_channels(zone_id: &str, name: &str) -> Vec<CaptureChan> {
    let key = sink_name(zone_id);
    let label = applet_label(name);
    (0..2u16)
        .map(|index| CaptureChan {
            id: format!("{key}:{index}"),
            device_key: key.clone(),
            device_name: label.clone(),
            index,
        })
        .collect()
}

struct Running {
    zone_id: String,
    name: String,
    child: Child,
}

pub struct Cables {
    running: Vec<Running>,
}

impl Default for Cables {
    fn default() -> Self {
        Self { running: Vec::new() }
    }
}

impl Drop for Cables {
    fn drop(&mut self) {
        self.clear();
    }
}

fn prop_str<'a>(item: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    item.pointer(&format!("/info/props/{key}"))?.as_str()
}

fn find_node(dump: &str, name: &str, class: &str) -> Option<serde_json::Value> {
    let items: Vec<serde_json::Value> = serde_json::from_str(dump).ok()?;
    items.into_iter().find(|item| {
        prop_str(item, "node.name") == Some(name) && prop_str(item, "media.class") == Some(class)
    })
}

/// Старый linger null-sink с именем `zvucast`. Его надо снести, иначе имя занято.
fn null_sink_id(dump: &str) -> Option<u32> {
    let item = find_node(dump, VIRTUAL_DEVICE_KEY, "Audio/Sink")?;
    if prop_str(&item, "factory.name") != Some(NULL_SINK_FACTORY) {
        return None;
    }
    item.get("id")?.as_u64().map(|id| id as u32)
}

pub(crate) fn cable_ready(dump: &str, sink: &str, source: &str) -> bool {
    let Some(sink_node) = find_node(dump, sink, "Audio/Sink") else {
        return false;
    };
    if prop_str(&sink_node, "factory.name") == Some(NULL_SINK_FACTORY) {
        return false;
    }
    find_node(dump, source, "Audio/Source").is_some()
}

/// Зона «жива», когда в её sink есть ссылка от клиента, а не от нашего кабеля.
/// Пока ссылки нет, зона остаётся на line-in.
pub fn linked_zone_ids(dump: &str, zone_ids: &[String]) -> BTreeSet<String> {
    let Ok(items) = serde_json::from_str::<Vec<serde_json::Value>>(dump) else {
        return BTreeSet::new();
    };
    let mut name_of: HashMap<u64, String> = HashMap::new();
    let mut sink_of: HashMap<String, u64> = HashMap::new();
    for item in &items {
        if item.get("type").and_then(|t| t.as_str()) != Some("PipeWire:Interface:Node") {
            continue;
        }
        let Some(id) = item.get("id").and_then(|v| v.as_u64()) else { continue };
        let Some(name) = prop_str(item, "node.name") else { continue };
        name_of.insert(id, name.to_string());
        if prop_str(item, "media.class") == Some("Audio/Sink") {
            sink_of.insert(name.to_string(), id);
        }
    }
    let mut live = BTreeSet::new();
    for zone_id in zone_ids {
        let Some(&sid) = sink_of.get(&sink_name(zone_id)) else { continue };
        let client = items.iter().any(|item| {
            if item.get("type").and_then(|t| t.as_str()) != Some("PipeWire:Interface:Link") {
                return false;
            }
            let input = item.pointer("/info/input-node-id").and_then(|v| v.as_u64());
            let output = item.pointer("/info/output-node-id").and_then(|v| v.as_u64());
            if input != Some(sid) {
                return false;
            }
            let Some(out_name) = output.and_then(|id| name_of.get(&id)) else {
                return false;
            };
            !is_virtual_sink_name(out_name)
        });
        if client {
            live.insert(zone_id.clone());
        }
    }
    live
}

/// `hardware` — список со старта. `enabled_channels`: Some — дописать каналы выхода, None — убрать их.
pub fn advertise(hardware: &[CaptureChan], enabled_channels: Option<&[CaptureChan]>) -> Vec<CaptureChan> {
    let mut out: Vec<CaptureChan> = hardware
        .iter()
        .filter(|c| !is_virtual_device_key(&c.device_key))
        .cloned()
        .collect();
    if let Some(channels) = enabled_channels {
        out.extend(channels.iter().cloned());
    }
    out
}

fn socket_ready() -> Result<()> {
    let dir = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/run/user/1000".into());
    let sock = std::path::PathBuf::from(dir).join("pipewire-0");
    if sock.exists() {
        Ok(())
    } else {
        anyhow::bail!("pipewire ещё не слушает {}", sock.display());
    }
}

fn pw(args: &[&str]) -> Result<String> {
    let out = Command::new("pw-cli").args(args).output().context("pw-cli не запустился")?;
    if !out.status.success() {
        anyhow::bail!("pw-cli {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

pub fn dump_text() -> Result<String> {
    let out = Command::new("pw-dump").output().context("pw-dump не запустился")?;
    if !out.status.success() {
        anyhow::bail!("pw-dump: {}", String::from_utf8_lossy(&out.stderr));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

impl Cables {
    /// Поднять по кабелю на зону и вернуть их каналы. Лишние процессы гасятся.
    pub fn sync(&mut self, zones: &[VirtualZone]) -> Result<Vec<CaptureChan>> {
        socket_ready()?;
        self.remove_null_sink()?;
        self.stop_legacy();
        let dumped = dump_text().unwrap_or_default();
        let keep: Vec<bool> = self
            .running
            .iter_mut()
            .map(|slot| slot_ok(slot, zones, &dumped))
            .collect();
        let mut kept = Vec::new();
        for (slot, ok) in self.running.drain(..).zip(keep) {
            if ok {
                kept.push(slot);
            } else {
                stop_slot(slot);
            }
        }
        self.running = kept;
        for zone in zones {
            if self.running.iter().any(|slot| slot.zone_id == zone.zone_id) {
                continue;
            }
            self.spawn(zone)?;
        }
        Ok(zones.iter().flat_map(|z| zone_channels(&z.zone_id, &z.name)).collect())
    }

    pub fn clear(&mut self) {
        for slot in self.running.drain(..) {
            stop_slot(slot);
        }
        self.stop_legacy();
    }

    fn remove_null_sink(&self) -> Result<()> {
        let dumped = dump_text()?;
        if let Some(id) = null_sink_id(&dumped) {
            let id = id.to_string();
            pw(&["destroy", &id])?;
        }
        Ok(())
    }

    fn stop_legacy(&self) {
        let _ = Command::new("pkill").args(["-f", &format!("pw-loopback --name {LEGACY_LOOPBACK}")]).status();
    }

    fn spawn(&mut self, zone: &VirtualZone) -> Result<()> {
        let label = applet_label(&zone.name);
        let sink = sink_name(&zone.zone_id);
        let source = monitor_name(&zone.zone_id);
        let capture = capture_props(&sink, &label);
        let playback = playback_props(&source, &format!("{label}Monitor"));
        let child = Command::new("pw-loopback")
            .arg("--name")
            .arg(process_name(&zone.zone_id))
            .arg("--channels")
            .arg("2")
            .arg("--channel-map")
            .arg("[ FL, FR ]")
            .arg("--capture-props")
            .arg(capture)
            .arg("--playback-props")
            .arg(playback)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .context("pw-loopback не запустился")?;
        let mut slot = Running {
            zone_id: zone.zone_id.clone(),
            name: zone.name.clone(),
            child,
        };
        wait_ready(&mut slot, &sink, &source)?;
        self.running.push(slot);
        Ok(())
    }
}

fn slot_ok(slot: &mut Running, zones: &[VirtualZone], dump: &str) -> bool {
    let Some(zone) = zones.iter().find(|z| z.zone_id == slot.zone_id && z.name == slot.name) else {
        return false;
    };
    if !child_alive(&mut slot.child) {
        return false;
    }
    cable_ready(dump, &sink_name(&zone.zone_id), &monitor_name(&zone.zone_id))
}

fn child_alive(child: &mut Child) -> bool {
    matches!(child.try_wait(), Ok(None))
}

fn wait_ready(slot: &mut Running, sink: &str, source: &str) -> Result<()> {
    for _ in 0..40 {
        if !child_alive(&mut slot.child) {
            anyhow::bail!("pw-loopback завершился, не создав узлы");
        }
        if dump_text().ok().is_some_and(|dumped| cable_ready(&dumped, sink, source)) {
            return Ok(());
        }
        sleep(Duration::from_millis(50));
    }
    stop_slot_in_place(slot);
    anyhow::bail!("pw-loopback не создал {sink} и {source}");
}

fn stop_slot(mut slot: Running) {
    stop_slot_in_place(&mut slot);
}

fn stop_slot_in_place(slot: &mut Running) {
    let _ = slot.child.kill();
    let _ = slot.child.wait();
    let pattern = format!("pw-loopback --name {}", process_name(&slot.zone_id));
    let _ = Command::new("pkill").args(["-f", &pattern]).status();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::virtual_capture_channels;

    #[test]
    fn sink_is_visible_to_the_applet() {
        let sink = capture_props("zvucast-hall", "Zvucast: Зал");
        let source = playback_props("zvucast-hall-monitor", "Zvucast: ЗалMonitor");
        assert!(sink.contains("node.virtual=false"));
        assert!(source.contains("node.virtual=true"));
        assert!(source.contains("node.passive=true"));
        assert!(!sink.contains("node.virtual=true"));
        assert!(sink.contains("node.description=\"Zvucast: Зал\""));
        assert_eq!(applet_label("Зал"), "Zvucast: Зал");
    }

    #[test]
    fn dump_finds_zone_cable_and_ignores_null_sink() {
        let dump = r#"[
            {"id": 7, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "alsa_output.pci", "media.class": "Audio/Sink"}}},
            {"id": 42, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast", "media.class": "Audio/Sink", "factory.name": "support.null-audio-sink"}}},
            {"id": 43, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-hall", "media.class": "Audio/Sink"}}},
            {"id": 44, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-hall-monitor", "media.class": "Audio/Source"}}}
        ]"#;
        assert_eq!(null_sink_id(dump), Some(42));
        assert!(!cable_ready(dump, "zvucast", "zvucast-monitor"));
        assert!(cable_ready(dump, "zvucast-hall", "zvucast-hall-monitor"));
        assert!(!cable_ready("[]", "zvucast-hall", "zvucast-hall-monitor"));
    }

    #[test]
    fn link_from_a_client_marks_only_that_zone() {
        let dump = r#"[
            {"id": 10, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-hall", "media.class": "Audio/Sink"}}},
            {"id": 11, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-hall-monitor", "media.class": "Audio/Source"}}},
            {"id": 12, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-bed", "media.class": "Audio/Sink"}}},
            {"id": 20, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "spotify", "media.class": "Stream/Output/Audio"}}},
            {"id": 30, "type": "PipeWire:Interface:Link", "info": {"output-node-id": 20, "input-node-id": 10}},
            {"id": 31, "type": "PipeWire:Interface:Link", "info": {"output-node-id": 10, "input-node-id": 11}}
        ]"#;
        let ids = vec!["hall".into(), "bed".into()];
        let live = linked_zone_ids(dump, &ids);
        assert!(live.contains("hall"));
        assert!(!live.contains("bed"));
        assert!(linked_zone_ids("[]", &ids).is_empty());
    }

    #[test]
    fn advertise_appends_virtual_channels_and_drops_a_stale_copy() {
        let hw = vec![CaptureChan {
            id: "hw:0".into(),
            device_key: "mic".into(),
            device_name: "Mic".into(),
            index: 0,
        }];
        let mut hardware = hw.clone();
        hardware.extend(virtual_capture_channels());
        hardware.extend(zone_channels("hall", "Зал"));
        let off = advertise(&hardware, None);
        assert_eq!(off, hw);
        let channels = zone_channels("hall", "Зал");
        let on = advertise(&hardware, Some(&channels));
        assert_eq!(on.iter().filter(|c| c.device_key == "zvucast-hall").count(), 2);
        assert_eq!(on.iter().filter(|c| c.device_name == "Zvucast: Зал").count(), 2);
        assert!(on.iter().all(|c| c.device_key != "zvucast"));
    }
}
