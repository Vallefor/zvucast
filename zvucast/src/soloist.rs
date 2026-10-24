//! Spotify Soloist: один процесс на зону передатчика с PipeWire.
//!
//! Имя в Spotify — «Zvucast: {зона}». Аудио идёт в свой скрытый кабель, не в
//! виртуальный выход апплета. Пока это устройство выбрано в Spotify
//! (`is_active`), зона читает только его монитор: line-in и трансляция с ПК
//! в эту зону не попадают. Пауза устройство не отпускает. Соседние зоны
//! того же передатчика остаются на своём источнике. Один ключ несёт один
//! играющий поток — это ограничение Spotify, второй процесс просто не активен.

use std::io::ErrorKind;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};
use zproto::VirtualZone;

use crate::virtual_output::{self, cable_ready};

const DATA_ROOT: &str = "/var/lib/zvucast/soloist";
const CACHE_ROOT: &str = "/var/cache/zvucast/soloist";

pub fn sink_name(zone_id: &str) -> String {
    format!("zvucast-spotify-{zone_id}")
}

pub fn monitor_name(zone_id: &str) -> String {
    format!("zvucast-spotify-{zone_id}-monitor")
}

pub fn deactivate_frame() -> String {
    r#"{"type":"command","command":"deactivate"}"#.to_string()
}

fn process_name(zone_id: &str) -> String {
    format!("zvucast-spotify-{zone_id}")
}

/// Spotify важнее виртуального выхода, тот важнее line-in.
pub fn zone_source(zone_id: &str, spotify: bool, linked: bool) -> Option<String> {
    if spotify {
        Some(monitor_name(zone_id))
    } else if linked {
        Some(virtual_output::monitor_name(zone_id))
    } else {
        None
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PlayState {
    pub is_active: bool,
}

impl PlayState {
    /// `true`, если после этого события зона принадлежит Spotify.
    pub fn apply_event(&mut self, text: &str) -> bool {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
            return self.is_active;
        };
        match value.get("type").and_then(|t| t.as_str()) {
            Some("device_changed" | "auth_state" | "playback_state") => {
                if let Some(flag) = value.get("is_active").and_then(|v| v.as_bool()) {
                    self.is_active = flag;
                }
            }
            _ => {}
        }
        self.is_active
    }
}

struct Slot {
    zone_id: String,
    name: String,
    /// Интерфейс, с которым слот объявлен. Смена имени гасит слот.
    iface: String,
    stop: Arc<AtomicBool>,
    deactivate: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

/// Слот остаётся, пока интерфейс тот же и в списке есть зона с тем же id и именем.
/// `None` — активного интерфейса нет, объявление гасится.
fn keep_announcement(
    slot_zone: &str,
    slot_name: &str,
    slot_iface: &str,
    zones: &[VirtualZone],
    iface: Option<&str>,
) -> bool {
    let Some(iface) = iface else {
        return false;
    };
    iface == slot_iface
        && zones
            .iter()
            .any(|zone| zone.zone_id == slot_zone && zone.name == slot_name)
}

pub struct Receivers {
    slots: Vec<Slot>,
    active: Arc<Mutex<std::collections::BTreeSet<String>>>,
    deactivate_flags: Arc<Mutex<std::collections::BTreeMap<String, Arc<AtomicBool>>>>,
    key: Option<String>,
    /// Интерфейс, чей адрес Soloist объявляет в mDNS. Пусто — активного нет, слоты гасятся.
    iface: String,
}

impl Default for Receivers {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            active: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
            deactivate_flags: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            key: None,
            iface: String::new(),
        }
    }
}

impl Drop for Receivers {
    fn drop(&mut self) {
        self.stop_all();
    }
}

impl Receivers {
    pub fn active_handle(&self) -> Arc<Mutex<std::collections::BTreeSet<String>>> {
        self.active.clone()
    }

    pub fn has_zones(&self) -> bool {
        !self.slots.is_empty()
    }

    /// Пока флаг поднят, цикл сокета шлёт кадр и после успешной отправки опускает его.
    /// Сокета нет — флаг остаётся, следующая попытка повторит. Зоны без слота — no-op.
    pub fn request_deactivate(&self, zone_id: &str) {
        let Ok(map) = self.deactivate_flags.lock() else {
            return;
        };
        if let Some(flag) = map.get(zone_id) {
            flag.store(true, Ordering::Relaxed);
        }
    }

    pub fn set_iface(&mut self, iface: &str) {
        self.iface = iface.to_string();
    }

    pub fn sync(&mut self, api_key: Option<&str>, zones: &[VirtualZone]) {
        let Some(api_key) = api_key.filter(|k| !k.is_empty()) else {
            self.stop_all();
            return;
        };
        if self.key.as_deref() != Some(api_key) {
            self.stop_all();
            self.key = Some(api_key.to_string());
        }
        let key = self.key.clone().unwrap_or_default();
        // Пустая строка — активного интерфейса нет. Предикат гасит слоты, новые не поднимаются.
        let iface = (!self.iface.is_empty()).then_some(self.iface.as_str());
        let keep: Vec<bool> = self
            .slots
            .iter()
            .map(|slot| keep_announcement(&slot.zone_id, &slot.name, &slot.iface, zones, iface))
            .collect();
        let mut kept = Vec::new();
        for (slot, ok) in self.slots.drain(..).zip(keep) {
            if ok {
                kept.push(slot);
            } else {
                stop_slot(slot, &self.active, &self.deactivate_flags);
            }
        }
        self.slots = kept;
        if iface.is_none() {
            return;
        }
        for zone in zones {
            if self.slots.iter().any(|slot| slot.zone_id == zone.zone_id) {
                continue;
            }
            self.slots.push(start_slot(
                zone,
                key.clone(),
                self.iface.clone(),
                self.active.clone(),
                self.deactivate_flags.clone(),
            ));
        }
    }

    fn stop_all(&mut self) {
        for slot in self.slots.drain(..) {
            stop_slot(slot, &self.active, &self.deactivate_flags);
        }
        self.key = None;
        if let Ok(mut active) = self.active.lock() {
            active.clear();
        }
    }
}

fn start_slot(
    zone: &VirtualZone,
    key: String,
    iface: String,
    active: Arc<Mutex<std::collections::BTreeSet<String>>>,
    deactivate_flags: Arc<Mutex<std::collections::BTreeMap<String, Arc<AtomicBool>>>>,
) -> Slot {
    let stop = Arc::new(AtomicBool::new(false));
    let deactivate = Arc::new(AtomicBool::new(false));
    if let Ok(mut map) = deactivate_flags.lock() {
        map.insert(zone.zone_id.clone(), deactivate.clone());
    }
    let stop_thread = stop.clone();
    let zone_id = zone.zone_id.clone();
    let name = zone.name.clone();
    let deactivate_thread = deactivate.clone();
    let iface_slot = iface.clone();
    let thread = thread::Builder::new()
        .name(format!("soloist-{zone_id}"))
        .spawn(move || run_zone(zone_id, name, key, iface, active, stop_thread, deactivate_thread))
        .ok();
    Slot {
        zone_id: zone.zone_id.clone(),
        name: zone.name.clone(),
        iface: iface_slot,
        stop,
        deactivate,
        thread,
    }
}

fn stop_slot(
    slot: Slot,
    active: &Mutex<std::collections::BTreeSet<String>>,
    deactivate_flags: &Mutex<std::collections::BTreeMap<String, Arc<AtomicBool>>>,
) {
    slot.stop.store(true, Ordering::Relaxed);
    if let Ok(mut map) = deactivate_flags.lock() {
        map.remove(&slot.zone_id);
    }
    if let Ok(mut set) = active.lock() {
        set.remove(&slot.zone_id);
    }
    if let Some(thread) = slot.thread {
        let _ = thread.join();
    }
    let _ = Command::new("pkill")
        .args(["-f", &format!("pw-loopback --name {}", process_name(&slot.zone_id))])
        .status();
    let _ = Command::new("pkill")
        .args(["-f", &format!("--data-dir {DATA_ROOT}/{}", slot.zone_id)])
        .status();
}

fn run_zone(
    zone_id: String,
    name: String,
    key: String,
    iface: String,
    active: Arc<Mutex<std::collections::BTreeSet<String>>>,
    stop: Arc<AtomicBool>,
    deactivate: Arc<AtomicBool>,
) {
    let mut backoff = Duration::from_secs(1);
    let cable = loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match ensure_cable(&zone_id) {
            Ok(child) => break child,
            Err(e) => {
                tracing::warn!("soloist кабель {zone_id}: {e:#}");
                sleep_stop(&stop, backoff);
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    };
    while !stop.load(Ordering::Relaxed) {
        let mut child = match spawn_soloist(&zone_id, &name, &key, &iface) {
            Ok(child) => child,
            Err(e) => {
                tracing::warn!("soloist {zone_id}: {e:#}");
                sleep_stop(&stop, backoff);
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        let watch_stop = Arc::new(AtomicBool::new(false));
        let watch_flag = watch_stop.clone();
        let watch_id = zone_id.clone();
        let watch_active = active.clone();
        let watch_deactivate = deactivate.clone();
        let watcher = thread::Builder::new()
            .name(format!("soloist-ws-{zone_id}"))
            .spawn(move || watch(&watch_id, &watch_active, &watch_flag, &watch_deactivate))
            .ok();
        let status = loop {
            if stop.load(Ordering::Relaxed) {
                break None;
            }
            match child.try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) => thread::sleep(Duration::from_millis(200)),
                Err(_) => break None,
            }
        };
        watch_stop.store(true, Ordering::Relaxed);
        let _ = child.kill();
        let _ = child.wait();
        if let Some(watcher) = watcher {
            let _ = watcher.join();
        }
        if let Ok(mut set) = active.lock() {
            set.remove(&zone_id);
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if status.as_ref().is_some_and(|st| st.code() == Some(10)) {
            tracing::warn!("soloist: сборка протухла (код 10), нужен новый бинарь");
            break;
        }
        sleep_stop(&stop, backoff);
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
    if let Some(mut child) = cable {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn sleep_stop(stop: &AtomicBool, total: Duration) {
    let step = Duration::from_millis(200);
    let mut left = total;
    while left > Duration::ZERO && !stop.load(Ordering::Relaxed) {
        let slice = left.min(step);
        thread::sleep(slice);
        left = left.saturating_sub(slice);
    }
}

fn loopback_args(zone_id: &str) -> Vec<String> {
    let sink = sink_name(zone_id);
    let source = monitor_name(zone_id);
    let mut args = virtual_output::loopback_front(&process_name(zone_id));
    args.push("--capture-props".into());
    args.push(format!(
        "media.class=Audio/Sink node.name={sink} node.description=ZvucastSpotify audio.rate=48000 node.latency=128/48000 node.lock-quantum=true node.virtual=true"
    ));
    args.push("--playback-props".into());
    args.push(format!(
        "media.class=Audio/Source node.name={source} node.description=ZvucastSpotifyMonitor audio.rate=48000 node.latency=128/48000 node.lock-quantum=true node.passive=true node.virtual=true"
    ));
    args
}

fn ensure_cable(zone_id: &str) -> Result<Option<Child>> {
    let sink = sink_name(zone_id);
    let source = monitor_name(zone_id);
    if virtual_output::dump_text().ok().is_some_and(|dump| cable_ready(&dump, &sink, &source)) {
        return Ok(None);
    }
    let _ = Command::new("pkill").args(["-f", &format!("pw-loopback --name {}", process_name(zone_id))]).status();
    let mut child = Command::new("pw-loopback")
        .args(loopback_args(zone_id))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("pw-loopback для soloist не запустился")?;
    for _ in 0..40 {
        if child.try_wait().ok().flatten().is_some() {
            anyhow::bail!("pw-loopback soloist завершился");
        }
        if virtual_output::dump_text().ok().is_some_and(|dump| cable_ready(&dump, &sink, &source)) {
            return Ok(Some(child));
        }
        thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    anyhow::bail!("pw-loopback не создал {sink}")
}

const IFACE_SO: &str = "/usr/local/lib/zvucast-soloist-iface.so";

fn spawn_soloist(zone_id: &str, name: &str, key: &str, iface: &str) -> Result<Child> {
    let bin = std::path::Path::new("/usr/local/bin/soloist");
    if !bin.exists() {
        anyhow::bail!("нет /usr/local/bin/soloist");
    }
    let data = format!("{DATA_ROOT}/{zone_id}");
    let cache = format!("{CACHE_ROOT}/{zone_id}");
    std::fs::create_dir_all(&data)?;
    std::fs::create_dir_all(&cache)?;
    let label = format!("Zvucast: {}", name.replace(['"', '\n', '\r'], ""));
    let mut cmd = Command::new(bin);
    cmd.arg("--device-name")
        .arg(label)
        .arg("--api-key")
        .arg(key)
        .arg("--pipewire-device")
        .arg(sink_name(zone_id))
        .arg("--data-dir")
        .arg(&data)
        .arg("--cache-dir")
        .arg(&cache)
        .arg("--ws")
        .arg("127.0.0.1:0")
        .arg("--initial-volume")
        .arg("80")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let so = std::path::Path::new(IFACE_SO);
    if !iface.is_empty() && so.is_file() {
        cmd.env("ZV_IFACE", iface);
        cmd.env("LD_PRELOAD", IFACE_SO);
    }
    cmd.spawn().context("soloist не запустился")
}

fn watch(
    zone_id: &str,
    active: &Mutex<std::collections::BTreeSet<String>>,
    stop: &AtomicBool,
    deactivate: &AtomicBool,
) {
    let data = format!("{DATA_ROOT}/{zone_id}");
    while !stop.load(Ordering::Relaxed) {
        let Some(port) = wait_port(&data, stop) else { return };
        let url = format!("ws://127.0.0.1:{port}");
        let mut socket = match tungstenite::connect(&url) {
            Ok((socket, _)) => socket,
            Err(_) => {
                thread::sleep(Duration::from_millis(300));
                continue;
            }
        };
        if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
            let _ = tcp.set_read_timeout(Some(Duration::from_millis(400)));
        }
        let mut play = PlayState::default();
        while !stop.load(Ordering::Relaxed) {
            match socket.read() {
                Ok(tungstenite::Message::Text(text)) => {
                    let owns = play.apply_event(&text);
                    set_active(active, zone_id, owns);
                }
                Ok(tungstenite::Message::Ping(payload)) => {
                    let _ = socket.send(tungstenite::Message::Pong(payload));
                }
                Ok(tungstenite::Message::Close(_)) => break,
                Err(tungstenite::Error::Io(err))
                    if err.kind() == ErrorKind::TimedOut || err.kind() == ErrorKind::WouldBlock =>
                {
                    if deactivate.load(Ordering::Relaxed) {
                        match socket.send(tungstenite::Message::Text(deactivate_frame().into())) {
                            Ok(()) => deactivate.store(false, Ordering::Relaxed),
                            Err(_) => break,
                        }
                    }
                    continue;
                }
                Err(_) => break,
                _ => {}
            }
        }
        set_active(active, zone_id, false);
    }
}

fn wait_port(data: &str, stop: &AtomicBool) -> Option<u16> {
    let path = std::path::Path::new(data).join("ws.port");
    for _ in 0..50 {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        if let Ok(text) = std::fs::read_to_string(&path) {
            if let Ok(port) = text.trim().parse() {
                return Some(port);
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    None
}

fn set_active(active: &Mutex<std::collections::BTreeSet<String>>, zone_id: &str, owns: bool) {
    let Ok(mut set) = active.lock() else { return };
    if owns {
        set.insert(zone_id.to_string());
    } else {
        set.remove(zone_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod keep_announcement_tests {
        use super::*;

        #[test]
        fn announcement_restarts_when_the_iface_changes() {
            let zones = [zproto::VirtualZone {
                zone_id: "hall".into(),
                name: "Зал".into(),
            }];
            assert!(keep_announcement("hall", "Зал", "eth0", &zones, Some("eth0")));
            assert!(!keep_announcement(
                "hall",
                "Зал",
                "eth0",
                &zones,
                Some("wlan0")
            ));
            assert!(!keep_announcement("hall", "Зал", "eth0", &zones, None));
        }
    }

    #[test]
    fn spotify_owns_the_zone_while_the_device_is_selected() {
        let mut play = PlayState::default();
        assert!(!play.apply_event(r#"{"type":"auth_state","logged_in":true,"is_active":false}"#));
        assert!(play.apply_event(r#"{"type":"device_changed","is_active":true,"device_name":"Zvucast: Зал"}"#));
        assert!(play.apply_event(r#"{"type":"playback_changed","status":"paused"}"#));
        assert!(play.is_active, "пауза не отдаёт зону line-in");
        assert!(!play.apply_event(r#"{"type":"device_changed","is_active":false}"#));
    }

    #[test]
    fn spotify_source_beats_the_virtual_output() {
        assert_eq!(zone_source("hall", true, true).as_deref(), Some("zvucast-spotify-hall-monitor"));
        assert_eq!(zone_source("hall", false, true).as_deref(), Some("zvucast-hall-monitor"));
        assert_eq!(zone_source("hall", false, false), None);
    }

    #[test]
    fn spotify_cable_is_three_milliseconds_and_locks_quantum() {
        let args = loopback_args("hall");
        assert!(
            args.windows(2).any(|pair| pair == ["--latency", "3"]),
            "{args:?}"
        );
        assert_eq!(args[1], process_name("hall"));
        for flag in ["--capture-props", "--playback-props"] {
            let props = args
                .windows(2)
                .find(|pair| pair[0] == flag)
                .map(|pair| pair[1].as_str())
                .unwrap_or("");
            assert!(props.contains("node.lock-quantum=true"), "{flag}: {props}");
            assert!(props.contains("node.latency=128/48000"), "{flag}: {props}");
        }
    }

    #[test]
    fn deactivate_frame_is_the_soloist_command() {
        assert_eq!(
            deactivate_frame(),
            r#"{"type":"command","command":"deactivate"}"#
        );
    }
}
