use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine;
use cpal::traits::{DeviceTrait, HostTrait};
use mdns_sd::{DaemonEvent, ServiceDaemon, ServiceInfo};
use sendspin::audio::decode::{Decoder, FlacDecoder, PcmDecoder, PcmEndian};
use sendspin::audio::{AudioBuffer, AudioFormat, Codec, SyncedPlayer, SyncedPlayerConfig};
use sendspin::protocol::messages::{
    AudioFormatSpec, GoodbyeReason, Message, PlayerCommandType, PlayerState, PlayerStateCommand,
    PlayerV1Support, ServerCommand, StreamPlayerConfig,
};
use sendspin::{ConnectionManager, ManagedConnection, ProtocolClientBuilder, ProtocolListener};
use tokio::sync::{mpsc as tokio_mpsc, watch, Notify};
use zproto::VirtualZone;

use crate::soloist;
use crate::virtual_output;

const CLIENT_DIR: &str = "/var/lib/zvucast/sendspin";
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const CABLE_WAIT: Duration = Duration::from_secs(2);
const EXTERNAL_RETRY: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Holder {
    #[default]
    None,
    Sendspin,
    Spotify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    StreamStart,
    StreamEnd,
    StreamClear,
    Other,
}

/// Переход внешнего плеера. Отправитель — агент, получатель — рантайм.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum External {
    Enter(String),
    Exit(String),
}

/// Заметка потока от рантайма. Агент применяет её к таблице владельца.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Note {
    Stream(String, Wire),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneEffect {
    pub zone_id: String,
    pub deactivate: bool,
    pub enter_external: bool,
    pub exit_external: bool,
}

struct ZoneEntry {
    name: String,
    holder: Holder,
    spotify_active: bool,
}

pub struct ZoneTable {
    zones: BTreeMap<String, ZoneEntry>,
}

impl ZoneTable {
    pub fn new() -> Self {
        Self {
            zones: BTreeMap::new(),
        }
    }

    pub fn sync(&mut self, zones: &[VirtualZone]) -> bool {
        let incoming: BTreeMap<String, String> = zones
            .iter()
            .map(|z| (z.zone_id.clone(), z.name.clone()))
            .collect();
        let ids_changed =
            self.zones.keys().collect::<BTreeSet<_>>() != incoming.keys().collect::<BTreeSet<_>>();
        self.zones.retain(|id, _| incoming.contains_key(id));
        for (zone_id, name) in incoming {
            match self.zones.get_mut(&zone_id) {
                Some(entry) => entry.name = name,
                None => {
                    self.zones.insert(
                        zone_id,
                        ZoneEntry {
                            name,
                            holder: Holder::None,
                            spotify_active: false,
                        },
                    );
                }
            }
        }
        ids_changed
    }

    pub fn holder(&self, zone_id: &str) -> Holder {
        self.zones
            .get(zone_id)
            .map(|e| e.holder)
            .unwrap_or_default()
    }

    pub fn zone_ids(&self) -> Vec<String> {
        self.zones.keys().cloned().collect()
    }

    pub fn set_spotify(&mut self, active: &BTreeSet<String>) -> Vec<ZoneEffect> {
        let mut effects = Vec::new();
        for (zone_id, entry) in &mut self.zones {
            let now_active = active.contains(zone_id);
            if now_active == entry.spotify_active {
                continue;
            }
            entry.spotify_active = now_active;
            if now_active {
                if entry.holder != Holder::Spotify {
                    entry.holder = Holder::Spotify;
                    effects.push(ZoneEffect {
                        zone_id: zone_id.clone(),
                        deactivate: false,
                        enter_external: true,
                        exit_external: false,
                    });
                }
            } else {
                match entry.holder {
                    Holder::Sendspin => effects.push(ZoneEffect {
                        zone_id: zone_id.clone(),
                        deactivate: false,
                        enter_external: false,
                        exit_external: false,
                    }),
                    Holder::Spotify => {
                        entry.holder = Holder::None;
                        effects.push(ZoneEffect {
                            zone_id: zone_id.clone(),
                            deactivate: false,
                            enter_external: false,
                            exit_external: true,
                        });
                    }
                    Holder::None => {}
                }
            }
        }
        effects
    }

    pub fn on_wire(&mut self, zone_id: &str, wire: Wire) -> ZoneEffect {
        let empty = || ZoneEffect {
            zone_id: zone_id.to_string(),
            deactivate: false,
            enter_external: false,
            exit_external: false,
        };
        let Some(entry) = self.zones.get_mut(zone_id) else {
            return empty();
        };
        match wire {
            Wire::StreamStart => {
                let deactivate = entry.spotify_active;
                entry.holder = Holder::Sendspin;
                ZoneEffect {
                    zone_id: zone_id.to_string(),
                    deactivate,
                    enter_external: false,
                    exit_external: false,
                }
            }
            Wire::StreamEnd | Wire::StreamClear => {
                if entry.holder == Holder::Sendspin {
                    entry.holder = Holder::None;
                }
                empty()
            }
            Wire::Other => empty(),
        }
    }

    pub fn source(&self, zone_id: &str, linked: bool) -> Option<String> {
        // Дополнительный монитор зоны. Line-in пайплайн мешает с ним сам:
        // занятый выход не подменяет микрофон. Зоны нет в таблице, когда
        // Sendspin выключен: контроллер шлёт пустой SetSendspin.
        let holder = self.zones.get(zone_id).map(|entry| entry.holder).unwrap_or(Holder::None);
        match holder {
            Holder::Sendspin => Some(monitor_name(zone_id)),
            Holder::Spotify => Some(soloist::monitor_name(zone_id)),
            Holder::None if linked => Some(virtual_output::monitor_name(zone_id)),
            Holder::None => None,
        }
    }
}

impl Default for ZoneTable {
    fn default() -> Self {
        Self::new()
    }
}

pub fn sink_name(zone_id: &str) -> String {
    format!("zvucast-sendspin-{zone_id}")
}

pub fn monitor_name(zone_id: &str) -> String {
    format!("zvucast-sendspin-{zone_id}-monitor")
}

fn escape_extended_regex(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '.' | '^' | '$' | '*' | '+' | '?' | '{' | '}' | '[' | ']' | '\\' | '|' | '(' | ')' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Extended-regex argument for `pkill -f` when stopping a zone's `pw-loopback`.
fn loopback_pkill_pattern(zone_id: &str) -> String {
    let id = escape_extended_regex(zone_id);
    format!("pw-loopback --name zvucast-sendspin-{id}( |$)")
}

pub fn friendly_name(zone_name: &str) -> String {
    let cleaned: String = zone_name
        .chars()
        .filter(|c| *c != '"' && *c != '\n')
        .collect();
    format!("Zvucast: {cleaned}")
}

pub fn should_advertise(dump: &str, zone_id: &str) -> bool {
    virtual_output::cable_ready(dump, &sink_name(zone_id), &monitor_name(zone_id))
}

/// Первая IPv4 из вывода `ip -4 -o addr show dev <iface>`. Нет строки `inet` — None.
pub fn parse_iface_ipv4(text: &str) -> Option<String> {
    let tokens: Vec<&str> = text.split_whitespace().collect();
    for (i, token) in tokens.iter().enumerate() {
        if *token == "inet" {
            let next = tokens.get(i + 1)?;
            let ip = next.split('/').next()?;
            return Some(ip.to_string());
        }
    }
    None
}

/// Читает UUID из файла. Файла нет — генерирует и пишет. Запись не удалась — возвращает UUID этой попытки.
pub fn load_client_id(path: &std::path::Path) -> String {
    if let Ok(content) = std::fs::read_to_string(path) {
        let line = content.lines().next().unwrap_or("").trim();
        if uuid::Uuid::parse_str(line).is_ok() {
            return line.to_string();
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(%e, ?path, "не удалось создать каталог идентификатора клиента");
            return id;
        }
    }
    if let Err(e) = std::fs::write(path, format!("{id}\n")) {
        tracing::warn!(%e, ?path, "не удалось записать файл идентификатора клиента");
    }
    id
}

/// Объявление колонки в mDNS. Адрес берётся только из `parse_iface_ipv4`.
pub struct Announcement {
    pub service_type: String,
    pub instance: String,
    pub port: u16,
    pub addresses: Vec<String>,
    pub txt: BTreeMap<String, String>,
}

pub fn announcement(zone_id: &str, friendly: &str, port: u16, address: &str) -> Announcement {
    let mut txt = BTreeMap::new();
    txt.insert("path".to_string(), "/sendspin".to_string());
    txt.insert("name".to_string(), friendly.to_string());
    Announcement {
        service_type: "_sendspin._tcp.local.".to_string(),
        instance: format!("zvucast-{zone_id}"),
        port,
        addresses: vec![address.to_string()],
        txt,
    }
}

struct Slot {
    zone_id: String,
    name: String,
    /// Интерфейс, с которым слот объявлен. Смена имени гасит слот.
    iface: String,
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
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

struct ZoneCtx {
    zone_id: String,
    zone_name: String,
    iface: String,
    notes: std::sync::mpsc::Sender<Note>,
    external: tokio_mpsc::Receiver<External>,
    /// Последняя команда владельца. Новое соединение получает её снова.
    latest_external: Option<External>,
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    /// Общий демон. None — `ServiceDaemon::new` не удался, зона повторяет сессию.
    mdns: Option<ServiceDaemon>,
    /// Поколение ошибок mDNS. Сессия жива, пока значение не сменилось.
    mdns_fault: watch::Receiver<u64>,
}

enum SessionEnd {
    Stopped,
    Retry(String),
}

struct Playback {
    decoder: Option<Box<dyn Decoder + Send>>,
    format: Option<AudioFormat>,
    player: Option<SyncedPlayer>,
    volume: u8,
    muted: bool,
    format_warned: bool,
}

struct Announcer {
    daemon: ServiceDaemon,
    fullname: String,
    published: bool,
}

impl Announcer {
    fn unpublish(&mut self) {
        if !self.published {
            return;
        }
        self.published = false;
        if let Err(e) = self.daemon.unregister(&self.fullname) {
            tracing::warn!(%e, name = %self.fullname, "sendspin: снятие mDNS");
        }
    }
}

impl Drop for Announcer {
    fn drop(&mut self) {
        self.unpublish();
    }
}

/// Колонки Sendspin: один tokio-рантайм и слот на зону.
pub struct Players {
    runtime: tokio::runtime::Runtime,
    slots: Vec<Slot>,
    routes: Arc<Mutex<HashMap<String, tokio_mpsc::Sender<External>>>>,
    notes: std::sync::mpsc::Sender<Note>,
    stop_dispatcher: Arc<AtomicBool>,
    dispatcher: Option<JoinHandle<()>>,
    mdns: Option<ServiceDaemon>,
    /// Отправитель живёт вместе с Players. Иначе выход потока наблюдения
    /// закрывает канал, и зона принимает это за ошибку mDNS.
    #[allow(dead_code)]
    mdns_fault: watch::Sender<u64>,
    mdns_fault_rx: watch::Receiver<u64>,
    mdns_monitor: Option<JoinHandle<()>>,
}

impl Players {
    pub fn new(
        notes: std::sync::mpsc::Sender<Note>,
        external: std::sync::mpsc::Receiver<External>,
    ) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime для Sendspin");
        let routes = Arc::new(Mutex::new(HashMap::new()));
        let stop_dispatcher = Arc::new(AtomicBool::new(false));
        let routes_dispatch = Arc::clone(&routes);
        let stop_dispatch = Arc::clone(&stop_dispatcher);
        let dispatcher = thread::Builder::new()
            .name("sendspin-ext".into())
            .spawn(move || dispatch_external(external, routes_dispatch, stop_dispatch))
            .expect("поток команд External");
        let (mdns_fault, mdns_fault_rx) = watch::channel(0);
        let (mdns, mdns_monitor) = match ServiceDaemon::new() {
            Ok(daemon) => {
                let watched = daemon.clone();
                let fault = mdns_fault.clone();
                let monitor = thread::Builder::new()
                    .name("sendspin-mdns".into())
                    .spawn(move || watch_mdns(watched, fault))
                    .expect("поток наблюдения mDNS");
                (Some(daemon), Some(monitor))
            }
            Err(err) => {
                tracing::warn!(
                    %err,
                    "sendspin: mDNS-демон не запустился, зоны будут повторять попытку"
                );
                (None, None)
            }
        };
        Self {
            runtime,
            slots: Vec::new(),
            routes,
            notes,
            stop_dispatcher,
            dispatcher: Some(dispatcher),
            mdns,
            mdns_fault,
            mdns_fault_rx,
            mdns_monitor,
        }
    }

    pub fn sync(&mut self, zones: &[VirtualZone], iface: Option<&str>) {
        // Нет активного интерфейса — старые объявления гасятся, новые не поднимаются.
        let Some(iface) = iface else {
            for slot in std::mem::take(&mut self.slots) {
                self.stop_slot(slot);
            }
            return;
        };
        let keep: Vec<bool> = self
            .slots
            .iter()
            .map(|slot| {
                keep_announcement(&slot.zone_id, &slot.name, &slot.iface, zones, Some(iface))
            })
            .collect();
        let mut kept = Vec::new();
        let mut stopped = Vec::new();
        for (slot, ok) in self.slots.drain(..).zip(keep) {
            if ok {
                kept.push(slot);
            } else {
                stopped.push(slot);
            }
        }
        self.slots = kept;
        for slot in stopped {
            self.stop_slot(slot);
        }
        for zone in zones {
            if self.slots.iter().any(|slot| slot.zone_id == zone.zone_id) {
                continue;
            }
            self.slots.push(self.start_slot(zone, iface));
        }
    }

    fn start_slot(&self, zone: &VirtualZone, iface: &str) -> Slot {
        let (tx, rx) = tokio_mpsc::channel(32);
        if let Ok(mut routes) = self.routes.lock() {
            routes.insert(zone.zone_id.clone(), tx);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let zone_id = zone.zone_id.clone();
        let zone_name = zone.name.clone();
        let iface = iface.to_string();
        let iface_zone = iface.clone();
        let notes = self.notes.clone();
        let stop_task = Arc::clone(&stop);
        let wake_task = Arc::clone(&wake);
        let routes = Arc::clone(&self.routes);
        let mdns = self.mdns.clone();
        let mdns_fault = self.mdns_fault_rx.clone();
        let task = self.runtime.spawn(async move {
            let id = zone_id.clone();
            run_zone(ZoneCtx {
                zone_id,
                zone_name,
                iface: iface_zone,
                notes,
                external: rx,
                latest_external: None,
                stop: stop_task,
                wake: wake_task,
                mdns,
                mdns_fault,
            })
            .await;
            if let Ok(mut guard) = routes.lock() {
                guard.remove(&id);
            }
        });
        Slot {
            zone_id: zone.zone_id.clone(),
            name: zone.name.clone(),
            iface,
            stop,
            wake,
            task,
        }
    }

    fn stop_slot(&self, slot: Slot) {
        slot.stop.store(true, Ordering::Relaxed);
        slot.wake.notify_one();
        if let Ok(mut routes) = self.routes.lock() {
            routes.remove(&slot.zone_id);
        }
        let _ = self.runtime.block_on(slot.task);
    }
}

impl Drop for Players {
    fn drop(&mut self) {
        self.stop_dispatcher.store(true, Ordering::Relaxed);
        if let Some(thread) = self.dispatcher.take() {
            let _ = thread.join();
        }
        for slot in std::mem::take(&mut self.slots) {
            self.stop_slot(slot);
        }
        if let Some(daemon) = self.mdns.take() {
            if let Err(e) = daemon.shutdown() {
                tracing::warn!(%e, "sendspin: остановка mDNS");
            }
        }
        if let Some(thread) = self.mdns_monitor.take() {
            let _ = thread.join();
        }
    }
}

fn dispatch_external(
    external: std::sync::mpsc::Receiver<External>,
    routes: Arc<Mutex<HashMap<String, tokio_mpsc::Sender<External>>>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        match external.recv_timeout(Duration::from_millis(200)) {
            Ok(cmd) => {
                let zone_id = match &cmd {
                    External::Enter(id) | External::Exit(id) => id.clone(),
                };
                let sender = routes
                    .lock()
                    .ok()
                    .and_then(|guard| guard.get(&zone_id).cloned());
                let Some(sender) = sender else {
                    continue;
                };
                if sender.try_send(cmd).is_err() {
                    tracing::warn!("sendspin {zone_id}: команда External не попала в слот");
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

async fn run_zone(mut ctx: ZoneCtx) {
    let mut backoff = RETRY_MIN;
    let mut cable: Option<Child> = None;
    loop {
        if ctx.stop.load(Ordering::Relaxed) {
            kill_loopback(&ctx.zone_id, &mut cable);
            return;
        }
        match run_session(&mut ctx, &mut cable).await {
            SessionEnd::Stopped => return,
            SessionEnd::Retry(err) => {
                tracing::warn!("sendspin {}: {err}", ctx.zone_id);
                if sleep_stop(&ctx.stop, &ctx.wake, backoff).await {
                    kill_loopback(&ctx.zone_id, &mut cable);
                    return;
                }
                backoff = (backoff * 2).min(RETRY_MAX);
            }
        }
    }
}

async fn run_session(ctx: &mut ZoneCtx, cable: &mut Option<Child>) -> SessionEnd {
    if let Err(err) = ensure_cable(&ctx.zone_id, cable, &ctx.stop, &ctx.wake).await {
        if ctx.stop.load(Ordering::Relaxed) {
            kill_loopback(&ctx.zone_id, cable);
            return SessionEnd::Stopped;
        }
        return SessionEnd::Retry(err);
    }
    if ctx.stop.load(Ordering::Relaxed) {
        kill_loopback(&ctx.zone_id, cable);
        return SessionEnd::Stopped;
    }
    if ctx.mdns.is_none() {
        return SessionEnd::Retry("mDNS-демон не запущен".into());
    }

    let client_id = load_client_id(&Path::new(CLIENT_DIR).join(&ctx.zone_id));
    let friendly = friendly_name(&ctx.zone_name);
    let listener = match open_listener(&client_id, &friendly).await {
        Ok(listener) => listener,
        Err(err) => return SessionEnd::Retry(format!("слушатель: {err}")),
    };
    let port = match listener.local_addr() {
        Ok(addr) => addr.port(),
        Err(err) => return SessionEnd::Retry(format!("порт слушателя: {err}")),
    };
    let Some(address) = iface_ipv4(&ctx.iface) else {
        return SessionEnd::Retry(format!("нет IPv4 на {}, объявления нет", ctx.iface));
    };
    let info = announcement(&ctx.zone_id, &friendly, port, &address);
    let Some(daemon) = ctx.mdns.clone() else {
        return SessionEnd::Retry("mDNS-демон не запущен".into());
    };
    let baseline = *ctx.mdns_fault.borrow();
    let mut announcer = match register_announcement(&info, &daemon) {
        Ok(announcer) => announcer,
        Err(err) => return SessionEnd::Retry(err),
    };
    let mut manager = ConnectionManager::new(listener);
    connection_loop(ctx, cable, &mut manager, &mut announcer, baseline).await
}

async fn connection_loop(
    ctx: &mut ZoneCtx,
    cable: &mut Option<Child>,
    manager: &mut ConnectionManager,
    announcer: &mut Announcer,
    baseline: u64,
) -> SessionEnd {
    let zone_id = ctx.zone_id.clone();
    loop {
        let stop = Arc::clone(&ctx.stop);
        let wake = Arc::clone(&ctx.wake);
        tokio::select! {
            biased;
            _ = wait_flag(&stop, &wake) => {
                shutdown_slot(manager, announcer, &zone_id, cable).await;
                return SessionEnd::Stopped;
            }
            _ = mdns_session_failed(&mut ctx.mdns_fault, baseline) => {
                say_goodbye(manager, announcer, &zone_id).await;
                return SessionEnd::Retry("ошибка mDNS".into());
            }
            ended = cable_stopped(cable, &zone_id) => {
                say_goodbye(manager, announcer, &zone_id).await;
                return SessionEnd::Retry(ended);
            }
            incoming = manager.next_connection() => {
                let Some(conn) = incoming else {
                    return SessionEnd::Retry("менеджер соединений остановился".into());
                };
                tokio::select! {
                    biased;
                    ended = cable_stopped(cable, &zone_id) => {
                        say_goodbye(manager, announcer, &zone_id).await;
                        return SessionEnd::Retry(ended);
                    }
                    _ = serve(manager, conn, ctx, baseline) => {}
                }
                if ctx.stop.load(Ordering::Relaxed) {
                    shutdown_slot(manager, announcer, &zone_id, cable).await;
                    return SessionEnd::Stopped;
                }
                if *ctx.mdns_fault.borrow() != baseline {
                    say_goodbye(manager, announcer, &zone_id).await;
                    return SessionEnd::Retry("ошибка mDNS".into());
                }
            }
        }
    }
}

async fn shutdown_slot(
    manager: &ConnectionManager,
    announcer: &mut Announcer,
    zone_id: &str,
    cable: &mut Option<Child>,
) {
    say_goodbye(manager, announcer, zone_id).await;
    kill_loopback(zone_id, cable);
}

async fn say_goodbye(manager: &ConnectionManager, announcer: &mut Announcer, zone_id: &str) {
    if let Err(err) = manager.disconnect(GoodbyeReason::Shutdown).await {
        tracing::warn!("sendspin {zone_id}: goodbye: {err}");
    }
    announcer.unpublish();
}

/// Ребёнок pw-loopback завершился. Если ребёнка нет, принятый кабель
/// проверяется примерно каждые 200 мс. Дамп идёт в `spawn_blocking`.
/// Ошибка `pw-dump` кабель не гасит; сессия рвётся только когда успешный
/// дамп больше не проходит `should_advertise`.
async fn cable_stopped(cable: &mut Option<Child>, zone_id: &str) -> String {
    loop {
        let status = cable.as_mut().map(|child| child.try_wait());
        match status {
            Some(Ok(Some(code))) => {
                cable.take();
                return format!("pw-loopback завершился: {code}");
            }
            Some(Ok(None)) => tokio::time::sleep(Duration::from_millis(200)).await,
            Some(Err(err)) => {
                cable.take();
                return format!("pw-loopback: {err}");
            }
            None => {
                let still_up = match tokio::task::spawn_blocking(virtual_output::dump_text).await {
                    Ok(dump) => adopted_cable_still_up(dump.as_deref(), zone_id),
                    Err(_) => true,
                };
                if !still_up {
                    return "принятый кабель пропал".into();
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn mdns_session_failed(fault: &mut watch::Receiver<u64>, baseline: u64) {
    let _ = fault.wait_for(|generation| *generation != baseline).await;
}

async fn serve(
    manager: &ConnectionManager,
    mut conn: ManagedConnection,
    ctx: &mut ZoneCtx,
    baseline: u64,
) {
    let mut playback = Playback {
        decoder: None,
        format: None,
        player: None,
        volume: 100,
        muted: false,
        format_warned: false,
    };
    let mut applied: Option<External> = None;
    let mut next_retry = tokio::time::Instant::now() + EXTERNAL_RETRY;
    loop {
        match ctx.external.try_recv() {
            Ok(cmd) => ctx.latest_external = Some(cmd),
            Err(tokio_mpsc::error::TryRecvError::Empty) => break,
            Err(tokio_mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    apply_latest(ctx, &conn, &mut playback.player, &mut applied).await;
    let mut messages_open = true;
    let mut audio_open = true;
    loop {
        let retry = match &ctx.latest_external {
            Some(cmd) => applied.as_ref() != Some(cmd),
            None => false,
        };
        let wait_retry = next_retry.saturating_duration_since(tokio::time::Instant::now());
        tokio::select! {
            biased;
            _ = wait_flag(&ctx.stop, &ctx.wake) => break,
            _ = mdns_session_failed(&mut ctx.mdns_fault, baseline) => break,
            cmd = ctx.external.recv() => {
                let Some(cmd) = cmd else { break };
                ctx.latest_external = Some(cmd);
                next_retry = tokio::time::Instant::now() + EXTERNAL_RETRY;
                apply_latest(ctx, &conn, &mut playback.player, &mut applied).await;
            }
            _ = tokio::time::sleep(wait_retry), if retry => {
                next_retry = tokio::time::Instant::now() + EXTERNAL_RETRY;
                apply_latest(ctx, &conn, &mut playback.player, &mut applied).await;
            }
            msg = conn.messages.recv(), if messages_open => {
                match msg {
                    Some(msg) => handle_message(manager, &conn, &ctx.zone_id, &ctx.notes, &mut playback, msg),
                    None => messages_open = false,
                }
            }
            chunk = conn.audio.recv(), if audio_open => {
                match chunk {
                    Some(chunk) => handle_chunk(&ctx.zone_id, &conn, &mut playback, chunk.timestamp, &chunk.data),
                    None => audio_open = false,
                }
            }
        }
        if !messages_open && !audio_open {
            break;
        }
    }
}

async fn apply_latest(
    ctx: &ZoneCtx,
    conn: &ManagedConnection,
    player: &mut Option<SyncedPlayer>,
    applied: &mut Option<External>,
) {
    let Some(cmd) = ctx.latest_external.clone() else {
        return;
    };
    if deliver_external(&ctx.zone_id, &cmd, conn, player).await {
        *applied = Some(cmd);
    } else {
        *applied = None;
    }
}

async fn deliver_external(
    zone_id: &str,
    cmd: &External,
    conn: &ManagedConnection,
    player: &mut Option<SyncedPlayer>,
) -> bool {
    let result = match cmd {
        External::Enter(_) => conn.enter_external_source().await,
        External::Exit(_) => conn.exit_external_source(None).await,
    };
    match result {
        Ok(()) => {
            if matches!(cmd, External::Enter(_)) {
                if let Some(active) = player.take() {
                    drop(active.release_audio_device());
                }
            }
            true
        }
        Err(err) => {
            tracing::warn!("sendspin {zone_id}: внешний источник: {err}");
            false
        }
    }
}

fn handle_message(
    manager: &ConnectionManager,
    conn: &ManagedConnection,
    zone_id: &str,
    notes: &std::sync::mpsc::Sender<Note>,
    playback: &mut Playback,
    msg: Message,
) {
    match msg {
        Message::StreamStart(start) => {
            emit_note(notes, zone_id, Wire::StreamStart);
            playback.decoder = None;
            playback.format = None;
            if let Some(player) = playback.player.as_ref() {
                player.clear();
            }
            manager.set_last_played(Some(conn.server_hello.server_id.clone()));
            if let Some(config) = start.player.as_ref() {
                if !playable_format(config) {
                    warn_rejected_format(zone_id, playback, config);
                } else if let Some((decoder, format)) = decoder_for(zone_id, config) {
                    playback.decoder = Some(decoder);
                    playback.format = Some(format);
                }
            }
        }
        Message::StreamEnd(_) => {
            if let Some(player) = playback.player.as_ref() {
                player.clear();
            }
            emit_note(notes, zone_id, Wire::StreamEnd);
        }
        Message::StreamClear(_) => {
            if let Some(player) = playback.player.as_ref() {
                player.clear();
            }
            emit_note(notes, zone_id, Wire::StreamClear);
        }
        Message::ServerCommand(command) => apply_server_command(playback, &command),
        _ => {}
    }
}

fn apply_server_command(playback: &mut Playback, command: &ServerCommand) {
    let Some(player_cmd) = command.player.as_ref() else {
        return;
    };
    match player_cmd.command {
        PlayerCommandType::Volume => {
            if let Some(volume) = player_cmd.volume {
                playback.volume = volume;
                if let Some(player) = playback.player.as_ref() {
                    player.set_volume(volume);
                }
            }
        }
        PlayerCommandType::Mute => {
            if let Some(muted) = player_cmd.mute {
                playback.muted = muted;
                if let Some(player) = playback.player.as_ref() {
                    player.set_mute(muted);
                }
            }
        }
        _ => {}
    }
}

fn handle_chunk(
    zone_id: &str,
    conn: &ManagedConnection,
    playback: &mut Playback,
    timestamp: i64,
    data: &[u8],
) {
    let (Some(decoder), Some(format)) = (playback.decoder.as_ref(), playback.format.clone()) else {
        if !playback.format_warned {
            playback.format_warned = true;
            tracing::warn!("sendspin {zone_id}: кадр без поддерживаемого потока, пропуск");
        }
        return;
    };
    let samples = match decoder.decode(data) {
        Ok(samples) => samples,
        Err(err) => {
            tracing::warn!("sendspin {zone_id}: декодер: {err}");
            return;
        }
    };
    if playback.player.is_none() {
        let Some(device) = find_output(&sink_name(zone_id)) else {
            tracing::warn!("sendspin {zone_id}: нет устройства {}", sink_name(zone_id));
            return;
        };
        match SyncedPlayer::new(
            format.clone(),
            Arc::clone(&conn.clock_sync),
            SyncedPlayerConfig {
                device: Some(device),
                volume: playback.volume,
                muted: playback.muted,
                buffer_size: None,
            },
        ) {
            Ok(player) => playback.player = Some(player),
            Err(err) => {
                tracing::warn!("sendspin {zone_id}: плеер: {err}");
                return;
            }
        }
    }
    if let Some(player) = playback.player.as_ref() {
        player.enqueue(AudioBuffer {
            timestamp,
            samples,
            format,
        });
    }
}

fn playable_format(config: &StreamPlayerConfig) -> bool {
    matches!(config.codec.as_str(), "flac" | "pcm")
        && config.bit_depth == 16
        && config.channels == 2
        && config.sample_rate == 48_000
}

fn warn_rejected_format(zone_id: &str, playback: &mut Playback, config: &StreamPlayerConfig) {
    if playback.format_warned {
        return;
    }
    playback.format_warned = true;
    tracing::warn!(
        "sendspin {zone_id}: формат {} {} бит, {} кан., {} Гц отклонён, нужен FLAC или PCM 16 бит стерео 48000 Гц, кадр пропущен",
        config.codec,
        config.bit_depth,
        config.channels,
        config.sample_rate
    );
}

fn decoder_for(
    zone_id: &str,
    config: &StreamPlayerConfig,
) -> Option<(Box<dyn Decoder + Send>, AudioFormat)> {
    if !playable_format(config) {
        return None;
    }
    let codec = match config.codec.as_str() {
        "flac" => Codec::Flac,
        "pcm" => Codec::Pcm,
        _ => return None,
    };
    let header = match config.codec_header.as_deref() {
        Some(encoded) => match base64::engine::general_purpose::STANDARD.decode(encoded) {
            Ok(bytes) => Some(bytes),
            Err(err) => {
                tracing::warn!("sendspin {zone_id}: заголовок кодека не base64: {err}");
                return None;
            }
        },
        None => None,
    };
    let decoder: Box<dyn Decoder + Send> = match codec {
        Codec::Flac => {
            let decoder = match header.as_deref() {
                Some(bytes) => match FlacDecoder::with_header(bytes) {
                    Ok(decoder) => decoder,
                    Err(err) => {
                        tracing::warn!("sendspin {zone_id}: заголовок FLAC: {err}");
                        return None;
                    }
                },
                None => FlacDecoder::new(),
            };
            Box::new(decoder)
        }
        Codec::Pcm => Box::new(PcmDecoder::with_endian(16, PcmEndian::Little)),
        _ => return None,
    };
    Some((
        decoder,
        AudioFormat {
            codec,
            sample_rate: config.sample_rate,
            channels: config.channels,
            bit_depth: config.bit_depth,
            codec_header: header,
        },
    ))
}

/// На хосте PipeWire `DeviceId::id()` — имя узла. Совпадает только оно целиком.
fn output_is_sink(node_id: &str, sink: &str) -> bool {
    node_id == sink
}

fn find_output(sink: &str) -> Option<cpal::Device> {
    let mut devices = cpal::default_host().output_devices().ok()?;
    devices.find(|device| {
        device
            .id()
            .ok()
            .is_some_and(|id| output_is_sink(id.id(), sink))
    })
}

fn emit_note(notes: &std::sync::mpsc::Sender<Note>, zone_id: &str, wire: Wire) {
    if notes.send(Note::Stream(zone_id.to_string(), wire)).is_err() {
        tracing::warn!("sendspin {zone_id}: заметка потока не доставлена");
    }
}

async fn open_listener(
    client_id: &str,
    friendly: &str,
) -> Result<ProtocolListener, sendspin::error::Error> {
    ProtocolClientBuilder::builder()
        .client_id(client_id.to_string())
        .name(friendly.to_string())
        .player_v1_support(PlayerV1Support {
            supported_formats: vec![
                AudioFormatSpec {
                    codec: "flac".to_string(),
                    channels: 2,
                    sample_rate: 48_000,
                    bit_depth: 16,
                },
                AudioFormatSpec {
                    codec: "pcm".to_string(),
                    channels: 2,
                    sample_rate: 48_000,
                    bit_depth: 16,
                },
            ],
            buffer_capacity: 1_000_000,
            supported_commands: vec!["volume".into(), "mute".into()],
        })
        .initial_player_state(PlayerState {
            volume: Some(100),
            muted: Some(false),
            static_delay_ms: Some(0),
            required_lead_time_ms: Some(500),
            min_buffer_ms: Some(500),
            supported_commands: Some(vec![PlayerStateCommand::SetStaticDelay]),
        })
        .build()
        .listen("0.0.0.0:0")
        .await
        .map(|listener| listener.path("/sendspin"))
}

fn register_announcement(info: &Announcement, daemon: &ServiceDaemon) -> Result<Announcer, String> {
    let address = info
        .addresses
        .first()
        .ok_or_else(|| "нет адреса, объявления нет".to_string())?;
    let txt_pairs: Vec<(String, String)> = info
        .txt
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let service = ServiceInfo::new(
        info.service_type.as_str(),
        info.instance.as_str(),
        &format!("{}.local.", info.instance),
        address.as_str(),
        info.port,
        &txt_pairs[..],
    )
    .map_err(|err| err.to_string())?;
    let fullname = service.get_fullname().to_string();
    daemon.register(service).map_err(|err| err.to_string())?;
    Ok(Announcer {
        daemon: daemon.clone(),
        fullname,
        published: true,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MdnsNotice {
    EndSessions,
    LogName,
    Quiet,
}

fn mdns_notice(event: &DaemonEvent) -> MdnsNotice {
    match event {
        DaemonEvent::Error(_) => MdnsNotice::EndSessions,
        DaemonEvent::NameChange(_) => MdnsNotice::LogName,
        DaemonEvent::Announce(_, _)
        | DaemonEvent::Respond(_)
        | DaemonEvent::IpAdd(_)
        | DaemonEvent::IpDel(_) => MdnsNotice::Quiet,
        _ => MdnsNotice::Quiet,
    }
}

fn watch_mdns(daemon: ServiceDaemon, fault: watch::Sender<u64>) {
    let events = match daemon.monitor() {
        Ok(events) => events,
        Err(err) => {
            tracing::warn!(%err, "sendspin: канал событий mDNS не открылся");
            return;
        }
    };
    while let Ok(event) = events.recv() {
        match mdns_notice(&event) {
            MdnsNotice::EndSessions => {
                let err = match &event {
                    DaemonEvent::Error(err) => err.to_string(),
                    _ => String::new(),
                };
                tracing::warn!(%err, "sendspin: ошибка mDNS, зоны завершают сессию");
                let next = fault.borrow().wrapping_add(1);
                fault.send_replace(next);
            }
            MdnsNotice::LogName => {
                if let DaemonEvent::NameChange(change) = &event {
                    tracing::warn!(
                        original = %change.original,
                        new_name = %change.new_name,
                        "sendspin: mDNS сменил имя, колонку не переименовываем"
                    );
                }
            }
            MdnsNotice::Quiet => {}
        }
    }
}

fn iface_ipv4(iface: &str) -> Option<String> {
    let output = Command::new("ip")
        .args(["-4", "-o", "addr", "show", "dev", iface])
        .output()
        .ok()?;
    parse_iface_ipv4(&String::from_utf8_lossy(&output.stdout))
}

fn cable_is_up(zone_id: &str) -> bool {
    virtual_output::dump_text()
        .ok()
        .is_some_and(|dump| should_advertise(&dump, zone_id))
}

/// Принятый кабель. Ошибка дампа оставляет его на месте.
/// Нет кабеля только после успешного дампа, где `should_advertise` ложно.
fn adopted_cable_still_up<E>(dump: Result<&str, E>, zone_id: &str) -> bool {
    match dump {
        Ok(text) => should_advertise(text, zone_id),
        Err(_) => true,
    }
}

async fn ensure_cable(
    zone_id: &str,
    cable: &mut Option<Child>,
    stop: &AtomicBool,
    wake: &Notify,
) -> Result<(), String> {
    if cable_is_up(zone_id) {
        return Ok(());
    }
    if let Some(old) = cable.take() {
        kill_child(old);
    }
    let sink = sink_name(zone_id);
    let source = monitor_name(zone_id);
    let _ = Command::new("pkill")
        .args(["-f", &loopback_pkill_pattern(zone_id)])
        .status();
    let mut child = Command::new("pw-loopback")
        .arg("--name")
        .arg(&sink)
        .arg("--channels")
        .arg("2")
        .arg("--channel-map")
        .arg("[ FL, FR ]")
        .arg("--capture-props")
        .arg(format!(
            "media.class=Audio/Sink node.name={sink} node.description=ZvucastSendspin audio.rate=48000 node.latency=128/48000 node.virtual=true"
        ))
        .arg("--playback-props")
        .arg(format!(
            "media.class=Audio/Source node.name={source} node.description=ZvucastSendspinMonitor audio.rate=48000 node.latency=128/48000 node.passive=true node.virtual=true"
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| format!("pw-loopback не запустился: {err}"))?;
    let deadline = tokio::time::Instant::now() + CABLE_WAIT;
    loop {
        if stop.load(Ordering::Relaxed) {
            kill_child(child);
            return Err("остановка".into());
        }
        match child.try_wait() {
            Ok(Some(status)) => return Err(format!("pw-loopback завершился: {status}")),
            Ok(None) => {}
            Err(err) => return Err(format!("pw-loopback: {err}")),
        }
        if cable_is_up(zone_id) {
            *cable = Some(child);
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            kill_child(child);
            return Err(format!("кабель {sink} не появился за 2 секунды"));
        }
        tokio::select! {
            _ = wake.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

fn kill_loopback(zone_id: &str, cable: &mut Option<Child>) {
    if let Some(child) = cable.take() {
        kill_child(child);
    }
    let _ = Command::new("pkill")
        .args(["-f", &loopback_pkill_pattern(zone_id)])
        .status();
}

fn kill_child(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

async fn wait_flag(stop: &AtomicBool, wake: &Notify) {
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        tokio::select! {
            _ = wake.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
}

async fn sleep_stop(stop: &AtomicBool, wake: &Notify, total: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + total;
    loop {
        if stop.load(Ordering::Relaxed) {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        tokio::select! {
            _ = wake.notified() => {}
            _ = tokio::time::sleep(deadline.saturating_duration_since(now)) => {}
        }
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

    fn zone(id: &str, name: &str) -> VirtualZone {
        VirtualZone {
            zone_id: id.into(),
            name: name.into(),
        }
    }

    fn effect(id: &str, deactivate: bool, enter_external: bool, exit_external: bool) -> ZoneEffect {
        ZoneEffect {
            zone_id: id.into(),
            deactivate,
            enter_external,
            exit_external,
        }
    }

    #[test]
    fn names_follow_the_zone_id() {
        assert_eq!(sink_name("hall"), "zvucast-sendspin-hall");
        assert_eq!(monitor_name("hall"), "zvucast-sendspin-hall-monitor");
        assert_eq!(friendly_name("Зал \"ночь\"\n"), "Zvucast: Зал ночь");
    }

    #[test]
    fn stream_start_while_spotify_is_active_takes_the_zone_and_asks_to_deactivate() {
        let mut table = ZoneTable::new();
        table.sync(&[zone("hall", "Зал"), zone("bed", "Спальня")]);
        let active = BTreeSet::from(["hall".to_string()]);
        let steps = table.set_spotify(&active);
        assert_eq!(steps, vec![effect("hall", false, true, false)]);
        assert_eq!(table.holder("hall"), Holder::Spotify);

        let step = table.on_wire("hall", Wire::StreamStart);
        assert_eq!(step, effect("hall", true, false, false));
        assert_eq!(table.holder("hall"), Holder::Sendspin);
        assert_eq!(
            table.source("hall", true).as_deref(),
            Some("zvucast-sendspin-hall-monitor")
        );
        assert_eq!(table.holder("bed"), Holder::None);
        assert_eq!(
            table.source("bed", true).as_deref(),
            Some("zvucast-bed-monitor")
        );
    }

    #[test]
    fn spotify_takes_the_zone_immediately_and_a_later_stream_end_does_not_release_it() {
        let mut table = ZoneTable::new();
        table.sync(&[zone("hall", "Зал")]);
        assert_eq!(
            table.on_wire("hall", Wire::StreamStart),
            effect("hall", false, false, false)
        );
        let steps = table.set_spotify(&BTreeSet::from(["hall".to_string()]));
        assert_eq!(steps, vec![effect("hall", false, true, false)]);
        assert_eq!(table.holder("hall"), Holder::Spotify);
        assert_eq!(
            table.on_wire("hall", Wire::StreamEnd),
            effect("hall", false, false, false)
        );
        assert_eq!(
            table.on_wire("hall", Wire::StreamClear),
            effect("hall", false, false, false)
        );
        assert_eq!(table.holder("hall"), Holder::Spotify);
        assert_eq!(
            table.source("hall", false).as_deref(),
            Some("zvucast-spotify-hall-monitor")
        );
    }

    #[test]
    fn stream_end_returns_the_zone_to_the_virtual_output_or_line_in() {
        let mut table = ZoneTable::new();
        table.sync(&[zone("hall", "Зал")]);
        table.on_wire("hall", Wire::StreamStart);
        assert_eq!(
            table.on_wire("hall", Wire::StreamEnd),
            effect("hall", false, false, false)
        );
        assert_eq!(table.holder("hall"), Holder::None);
        assert_eq!(
            table.source("hall", true).as_deref(),
            Some("zvucast-hall-monitor")
        );
        assert_eq!(table.source("hall", false), None);
        table.on_wire("hall", Wire::StreamStart);
        assert_eq!(table.on_wire("hall", Wire::StreamClear).zone_id, "hall");
        assert_eq!(table.holder("hall"), Holder::None);
    }

    #[test]
    fn spotify_inactive_while_sendspin_owns_the_zone_is_only_the_deactivate_reply() {
        let mut table = ZoneTable::new();
        table.sync(&[zone("hall", "Зал")]);
        table.set_spotify(&BTreeSet::from(["hall".to_string()]));
        table.on_wire("hall", Wire::StreamStart);
        let steps = table.set_spotify(&BTreeSet::new());
        assert_eq!(steps, vec![effect("hall", false, false, false)]);
        assert_eq!(table.holder("hall"), Holder::Sendspin);
        assert!(!steps[0].exit_external);
    }

    #[test]
    fn spotify_inactive_without_a_sendspin_stream_frees_the_zone() {
        let mut table = ZoneTable::new();
        table.sync(&[zone("hall", "Зал")]);
        table.set_spotify(&BTreeSet::from(["hall".to_string()]));
        let steps = table.set_spotify(&BTreeSet::new());
        assert_eq!(steps, vec![effect("hall", false, false, true)]);
        assert_eq!(table.holder("hall"), Holder::None);
        assert_eq!(
            table.source("hall", true).as_deref(),
            Some("zvucast-hall-monitor")
        );
        assert_eq!(table.source("hall", false), None);
        assert!(table.set_spotify(&BTreeSet::new()).is_empty());
    }

    /// Виртуальный выход живёт без Sendspin: таблица плееров пустая,
    /// но занятый кабель всё равно надо читать.
    #[test]
    fn a_linked_output_is_captured_when_sendspin_is_off() {
        let table = ZoneTable::new();
        assert_eq!(
            table.source("hall", true).as_deref(),
            Some("zvucast-hall-monitor")
        );
        assert_eq!(table.source("hall", false), None);
    }

    #[test]
    fn the_player_is_advertised_only_when_its_cable_exists() {
        let dump = r#"[
        {"id": 1, "type": "PipeWire:Interface:Node", "info": {"props": {
            "node.name": "zvucast-sendspin-hall", "media.class": "Audio/Sink"
        }}},
        {"id": 2, "type": "PipeWire:Interface:Node", "info": {"props": {
            "node.name": "zvucast-sendspin-hall-monitor", "media.class": "Audio/Source"
        }}}
    ]"#;
        assert!(should_advertise(dump, "hall"));
        assert!(!should_advertise(dump, "bed"));
        assert!(!should_advertise("[]", "hall"));
    }

    #[test]
    fn a_failed_dump_keeps_an_adopted_cable() {
        let present = r#"[
        {"id": 1, "type": "PipeWire:Interface:Node", "info": {"props": {
            "node.name": "zvucast-sendspin-hall", "media.class": "Audio/Sink"
        }}},
        {"id": 2, "type": "PipeWire:Interface:Node", "info": {"props": {
            "node.name": "zvucast-sendspin-hall-monitor", "media.class": "Audio/Source"
        }}}
    ]"#;
        let failed: Result<&str, ()> = Err(());
        let missing: Result<&str, ()> = Ok("[]");
        let here: Result<&str, ()> = Ok(present);
        let other: Result<&str, ()> = Ok(present);
        assert!(adopted_cable_still_up(failed, "hall"));
        assert!(!adopted_cable_still_up(missing, "hall"));
        assert!(adopted_cable_still_up(here, "hall"));
        assert!(!adopted_cable_still_up(other, "bed"));
    }

    #[test]
    fn iface_address_is_the_first_ipv4() {
        let text = "2: end0    inet 192.0.2.204/24 brd 192.0.2.255 scope global end0\n";
        assert_eq!(parse_iface_ipv4(text).as_deref(), Some("192.0.2.204"));
        assert_eq!(
            parse_iface_ipv4("2: end0    inet6 fe80::1/64 scope link\n"),
            None
        );
    }

    #[test]
    fn client_id_survives_and_a_failed_write_still_returns_one() {
        let dir = std::env::temp_dir().join(format!("zv-spin-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("hall");
        let first = load_client_id(&path);
        assert_eq!(load_client_id(&path), first);
        let blocked = dir.join("not-a-dir").join("hall");
        std::fs::write(dir.join("not-a-dir"), b"x").unwrap();
        let fresh = load_client_id(&blocked);
        assert_eq!(fresh.len(), 36);
        assert_ne!(fresh, first);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn announcement_uses_the_sync_address_and_the_sendspin_path() {
        let info = announcement("hall", "Zvucast: Зал", 41234, "192.0.2.204");
        assert_eq!(info.service_type, "_sendspin._tcp.local.");
        assert_eq!(info.port, 41234);
        assert_eq!(info.addresses, vec!["192.0.2.204".to_string()]);
        assert_eq!(info.txt.get("path").map(String::as_str), Some("/sendspin"));
        assert_eq!(
            info.txt.get("name").map(String::as_str),
            Some("Zvucast: Зал")
        );
        assert_eq!(info.instance, "zvucast-hall");
    }

    #[test]
    fn an_unrelated_message_and_a_rename_do_not_move_the_owner() {
        let mut table = ZoneTable::new();
        table.sync(&[zone("hall", "Зал")]);
        table.on_wire("hall", Wire::StreamStart);
        assert_eq!(
            table.on_wire("hall", Wire::Other),
            effect("hall", false, false, false)
        );
        assert_eq!(table.holder("hall"), Holder::Sendspin);
        assert!(!table.sync(&[zone("hall", "Гостиная")]));
        assert_eq!(table.holder("hall"), Holder::Sendspin);
        assert!(table.sync(&[]));
        assert_eq!(table.holder("hall"), Holder::None);
        assert_eq!(
            table.on_wire("нет", Wire::StreamStart),
            effect("нет", false, false, false)
        );
    }

    fn stream(codec: &str, sample_rate: u32, channels: u8, bit_depth: u8) -> StreamPlayerConfig {
        StreamPlayerConfig {
            codec: codec.to_string(),
            sample_rate,
            channels,
            bit_depth,
            codec_header: None,
        }
    }

    #[test]
    fn output_matches_the_whole_node_name() {
        let sink = "zvucast-sendspin-z1";
        assert!(output_is_sink(sink, sink));
        assert!(!output_is_sink("zvucast-sendspin-z10", sink));
    }

    #[test]
    fn loopback_pkill_pattern_does_not_match_a_longer_zone_id_prefix() {
        let pattern = loopback_pkill_pattern("z1");
        let own = "pw-loopback --name zvucast-sendspin-z1 ";
        let other = "pw-loopback --name zvucast-sendspin-z10 --channels 2";
        assert!(extended_regex_matches(&pattern, own));
        assert!(!extended_regex_matches(&pattern, other));
    }

    fn extended_regex_matches(pattern: &str, haystack: &str) -> bool {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut child = Command::new("grep")
            .args(["-E", "-q", pattern])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("grep для проверки ERE");
        child
            .stdin
            .take()
            .expect("stdin grep")
            .write_all(haystack.as_bytes())
            .expect("запись в grep");
        child.wait().expect("grep").success()
    }

    #[test]
    fn only_16_bit_stereo_48k_flac_and_pcm_are_accepted() {
        assert!(decoder_for("hall", &stream("flac", 48_000, 2, 24)).is_none());
        assert!(decoder_for("hall", &stream("pcm", 44_100, 2, 16)).is_none());

        let (flac_decoder, flac) =
            decoder_for("hall", &stream("flac", 48_000, 2, 16)).expect("16-bit stereo 48000 FLAC");
        assert!(matches!(flac.codec, Codec::Flac));
        assert_eq!(flac.sample_rate, 48_000);
        assert_eq!(flac.channels, 2);
        assert_eq!(flac.bit_depth, 16);
        drop(flac_decoder);

        let (pcm_decoder, pcm) =
            decoder_for("hall", &stream("pcm", 48_000, 2, 16)).expect("16-bit stereo 48000 PCM");
        assert!(matches!(pcm.codec, Codec::Pcm));
        assert_eq!(pcm.sample_rate, 48_000);
        assert_eq!(pcm.channels, 2);
        assert_eq!(pcm.bit_depth, 16);
        drop(pcm_decoder);
    }

    #[test]
    fn only_an_mdns_error_ends_the_session() {
        use std::net::Ipv4Addr;

        use mdns_sd::{DnsNameChange, Error, RRType};

        assert_eq!(
            mdns_notice(&DaemonEvent::Error(Error::Again)),
            MdnsNotice::EndSessions
        );
        assert_eq!(
            mdns_notice(&DaemonEvent::NameChange(DnsNameChange {
                original: "zvucast-hall".into(),
                new_name: "zvucast-hall (2)".into(),
                rr_type: RRType::SRV,
                intf_name: "end0".into(),
            })),
            MdnsNotice::LogName
        );
        assert_eq!(
            mdns_notice(&DaemonEvent::Announce("a".into(), "b".into())),
            MdnsNotice::Quiet
        );
        assert_eq!(
            mdns_notice(&DaemonEvent::Respond("end0".into())),
            MdnsNotice::Quiet
        );
        assert_eq!(
            mdns_notice(&DaemonEvent::IpAdd(Ipv4Addr::LOCALHOST.into())),
            MdnsNotice::Quiet
        );
        assert_eq!(
            mdns_notice(&DaemonEvent::IpDel(Ipv4Addr::LOCALHOST.into())),
            MdnsNotice::Quiet
        );
    }
}
