//! Виртуальный стереовыход на каждую зону передатчика: свой null-sink.
//!
//! Sink `zvucast-{zone}` — то, что выбирают в апплете (`Zvucast: {имя}`).
//! Это драйвер своей группы: `node.force-quantum` 240 кадров, 5 мс.
//! Часы машины он не запирает и чужую группу не водит. Монитор — порты
//! этого же синка. `pw-record` читает их, отдельного источника нет.
//! Узел жив, пока жив `pw-cli`. `object.linger` нет: после выхода
//! процесса синка не остаётся.

use std::collections::{BTreeSet, HashMap};
use std::io::{self, Write};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
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

/// Имя в фиде. Порты монитора живут на синке, отдельного узла нет,
/// поэтому захват открывает [`feed_capture_target`], а не эту строку.
pub fn monitor_name(zone_id: &str) -> String {
    format!("zvucast-{zone_id}-monitor")
}

fn process_name(zone_id: &str) -> String {
    format!("zvucast-zone-{zone_id}")
}

/// Квант своего синка, кадры. 240 @ 48 кГц = 5 мс, один кадр провода.
/// Через `node.latency` PipeWire округлил бы 240 вниз до 128.
pub const SINK_QUANTUM: &str = "240";

/// Куда `pw-record` целится. `zvucast-…-monitor` — это монитор синка
/// `zvucast-…`, не отдельный источник.
pub fn feed_capture_target(monitor: &str) -> &str {
    match monitor.strip_suffix("-monitor") {
        Some(sink) if sink.starts_with("zvucast-") && !sink.is_empty() => sink,
        _ => monitor,
    }
}

/// `pw-record` монитора своего синка. `stream.capture.sink` читает
/// порты монитора. Квант просит поток, замок и `force-quantum` — нет:
/// квант группы задаёт сам синк.
///
/// `node.dont-move` держит цель из `--target`. После линка WirePlumber
/// пишет её в metadata и следующим проходом подменяет имя синка. Тогда
/// `node.dont-fallback` уничтожает захват: «defined target not found».
pub fn feed_capture_args(monitor: &str) -> Vec<String> {
    let props = format!(
        "{{\"stream.capture.sink\":\"true\",\"node.latency\":\"{q}/48000\",\"node.passive\":\"true\",\"node.dont-fallback\":\"true\",\"node.dont-move\":\"true\"}}",
        q = SINK_QUANTUM,
    );
    vec![
        "--target".into(),
        feed_capture_target(monitor).into(),
        "--rate".into(),
        "48000".into(),
        "--channels".into(),
        "2".into(),
        "--format".into(),
        "s16".into(),
        "--latency".into(),
        SINK_QUANTUM.into(),
        "--properties".into(),
        props,
        "--raw".into(),
        "-".into(),
    ]
}

fn json_str(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            '\n' | '\r' => out.push(' '),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Пропсы `support.null-audio-sink`. `visible` — узел апплета:
/// `node.virtual` не true, pipewire-pulse ставит HARDWARE и Plasma
/// его показывает. Скрытый синк (Sendspin, Spotify) в апплет не лезет.
/// `priority.driver` нет. `object.linger` нет.
pub fn null_sink_props(name: &str, description: &str, visible: bool) -> String {
    let virtual_flag = if visible { "false" } else { "true" };
    format!(
        "{{\"factory.name\":\"{factory}\",\"node.name\":{name},\"node.description\":{desc},\"media.class\":\"Audio/Sink\",\"audio.rate\":48000,\"audio.channels\":2,\"audio.position\":\"[ FL FR ]\",\"node.force-quantum\":{quantum},\"node.virtual\":{virtual_flag},\"session.suspend-timeout-seconds\":0,\"priority.session\":50,\"monitor.channel-volumes\":true,\"monitor.passthrough\":true}}",
        factory = NULL_SINK_FACTORY,
        name = json_str(name),
        desc = json_str(description),
        quantum = SINK_QUANTUM,
    )
}

/// `pw-cli` держит узел, пока открыт stdin. Закрытие или смерть процесса
/// убирает синк.
pub struct HeldSink {
    child: Child,
    stdin: Option<ChildStdin>,
}

impl HeldSink {
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for HeldSink {
    fn drop(&mut self) {
        drop(self.stdin.take());
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

pub fn spawn_null_sink(props: &str) -> Result<HeldSink> {
    let mut child = Command::new("pw-cli")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("pw-cli не запустился")?;
    let mut stdin = child.stdin.take().context("pw-cli без stdin")?;
    writeln!(stdin, "create-node adapter {props}").context("команда синка")?;
    stdin.flush().ok();
    Ok(HeldSink {
        child,
        stdin: Some(stdin),
    })
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
    held: HeldSink,
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

fn prop_force_quantum(item: &serde_json::Value) -> Option<u64> {
    let value = item.pointer("/info/props/node.force-quantum")?;
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    value.as_str()?.parse().ok()
}

/// Синк готов, когда это наш null-sink и квант группы именно 240.
/// Старый pw-loopback и linger-синк без этого кванта — не готов.
pub(crate) fn cable_ready(dump: &str, sink: &str) -> bool {
    let Some(sink_node) = find_node(dump, sink, "Audio/Sink") else {
        return false;
    };
    if prop_str(&sink_node, "factory.name") != Some(NULL_SINK_FACTORY) {
        return false;
    }
    prop_force_quantum(&sink_node) == SINK_QUANTUM.parse().ok()
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
        release_loopback(&process_name(&zone.zone_id));
        let held = spawn_null_sink(&null_sink_props(&sink, &label, true))?;
        let mut slot = Running {
            zone_id: zone.zone_id.clone(),
            name: zone.name.clone(),
            held,
        };
        wait_ready(&mut slot, &sink)?;
        self.running.push(slot);
        Ok(())
    }
}

fn slot_ok(slot: &mut Running, zones: &[VirtualZone], dump: &str) -> bool {
    let Some(zone) = zones.iter().find(|z| z.zone_id == slot.zone_id && z.name == slot.name) else {
        return false;
    };
    if !child_alive(&mut slot.held) {
        return false;
    }
    cable_ready(dump, &sink_name(&zone.zone_id))
}

fn child_alive(held: &mut HeldSink) -> bool {
    matches!(held.try_wait(), Ok(None))
}

fn release_loopback(process: &str) {
    let _ = Command::new("pkill")
        .args(["-f", &format!("pw-loopback --name {process}")])
        .status();
}

fn wait_ready(slot: &mut Running, sink: &str) -> Result<()> {
    for _ in 0..40 {
        if !child_alive(&mut slot.held) {
            anyhow::bail!("синк завершился, не создав узел");
        }
        if dump_text().ok().is_some_and(|dumped| cable_ready(&dumped, sink)) {
            return Ok(());
        }
        sleep(Duration::from_millis(50));
    }
    stop_slot_in_place(slot);
    anyhow::bail!("синк не создал {sink}");
}

fn stop_slot(mut slot: Running) {
    stop_slot_in_place(&mut slot);
}

fn stop_slot_in_place(slot: &mut Running) {
    drop(slot.held.stdin.take());
    if matches!(slot.held.child.try_wait(), Ok(None)) {
        let _ = slot.held.child.kill();
        let _ = slot.held.child.wait();
    }
    release_loopback(&process_name(&slot.zone_id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::virtual_capture_channels;

    #[test]
    fn sink_forces_its_own_quantum_and_does_not_lock_the_graph() {
        let sink = null_sink_props("zvucast-hall", "Zvucast: Зал", true);
        assert!(sink.contains("\"factory.name\":\"support.null-audio-sink\""), "{sink}");
        assert!(sink.contains("\"node.force-quantum\":240"), "{sink}");
        assert!(sink.contains("\"priority.session\":50"), "{sink}");
        assert!(sink.contains("\"session.suspend-timeout-seconds\":0"), "{sink}");
        assert!(!sink.contains("lock-quantum"), "{sink}");
        assert!(!sink.contains("node.latency"), "{sink}");
        assert!(!sink.contains("priority.driver"), "{sink}");
        assert!(!sink.contains("object.linger"), "{sink}");
        assert_eq!(SINK_QUANTUM, "240");
    }

    #[test]
    fn feed_capture_reads_the_sink_monitor() {
        let args = feed_capture_args("zvucast-sendspin-hall-monitor");
        assert_eq!(feed_capture_target("zvucast-hall-monitor"), "zvucast-hall");
        assert_eq!(feed_capture_target("alsa_input.usb"), "alsa_input.usb");
        assert!(
            args.windows(2).any(|pair| pair == ["--target", "zvucast-sendspin-hall"]),
            "{args:?}"
        );
        assert!(args.windows(2).any(|pair| pair == ["--latency", "240"]), "{args:?}");
        let props = args
            .windows(2)
            .find(|pair| pair[0] == "--properties")
            .map(|pair| pair[1].as_str())
            .unwrap_or("");
        assert!(props.contains("\"stream.capture.sink\":\"true\""), "{props}");
        assert!(props.contains("\"node.latency\":\"240/48000\""), "{props}");
        assert!(props.contains("\"node.passive\":\"true\""), "{props}");
        assert!(props.contains("\"node.dont-fallback\":\"true\""), "{props}");
        assert!(props.contains("\"node.dont-move\":\"true\""), "{props}");
        assert!(!props.contains("lock-quantum"), "{props}");
        assert!(!props.contains("force-quantum"), "{props}");
    }

    #[test]
    fn sink_is_visible_to_the_applet() {
        let sink = null_sink_props("zvucast-hall", "Zvucast: Зал", true);
        let hidden = null_sink_props("zvucast-sendspin-hall", "ZvucastSendspin", false);
        assert!(sink.contains("\"node.virtual\":false"), "{sink}");
        assert!(!sink.contains("\"node.virtual\":true"), "{sink}");
        assert!(hidden.contains("\"node.virtual\":true"), "{hidden}");
        assert!(sink.contains("\"node.description\":\"Zvucast: Зал\""), "{sink}");
        assert_eq!(applet_label("Зал"), "Zvucast: Зал");
    }

    #[test]
    fn dump_finds_zone_cable_and_ignores_null_sink() {
        let dump = r#"[
            {"id": 7, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "alsa_output.pci", "media.class": "Audio/Sink"}}},
            {"id": 42, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast", "media.class": "Audio/Sink", "factory.name": "support.null-audio-sink"}}},
            {"id": 43, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-hall", "media.class": "Audio/Sink", "factory.name": "support.null-audio-sink", "node.force-quantum": 240}}},
            {"id": 45, "type": "PipeWire:Interface:Node", "info": {"props": {"node.name": "zvucast-bed", "media.class": "Audio/Sink", "factory.name": "support.null-audio-sink", "node.force-quantum": "240"}}}
        ]"#;
        assert_eq!(null_sink_id(dump), Some(42));
        assert!(!cable_ready(dump, "zvucast"));
        assert!(cable_ready(dump, "zvucast-hall"));
        assert!(cable_ready(dump, "zvucast-bed"));
        assert!(!cable_ready(dump, "alsa_output.pci"));
        assert!(!cable_ready("[]", "zvucast-hall"));
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
