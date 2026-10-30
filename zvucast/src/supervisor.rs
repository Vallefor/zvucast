use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use zproto::{ChannelMap, PipelineState, ZoneEndpoints};

const RATE_HZ: i64 = 48_000;
const SLOW_RESTART_DELAY: Duration = Duration::from_secs(10);
/// Период пробуждения bus-watch-потока для проверки stop-флага.
///
/// gst_bus_set_flushing НЕ будит gst_poll_wait в gst_bus_timed_pop_filtered
/// (GstPoll-реализация с gst 1.26+, проверено на 1.26.0 и 1.28.7): NONE-таймаут
/// без своего флага остановки — вечный pop.
const BUS_WATCH_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stats {
    pub dropped: u64,
    pub rendered: u64,
    pub queue_ms: i64,
    pub rms_l: f64,
    pub rms_r: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupEvent {
    Failed(String),
    Underrun,
}

pub struct Runner {
    pipeline: gst::Pipeline,
    /// level name="lev" (теперь и в tx тоже)
    pub level: Option<gst::Element>,
    /// alsasink name="asink" (только rx)
    pub sink: Option<gst::Element>,
    /// карта ALSA из playback-устройства (только rx) — для queue_ms из /proc/asound
    alsa_card: Option<String>,
    /// момент последнего успешного start(); None до первого старта
    started_at: Mutex<Option<Instant>>,
    /// Underrun’ы шины. Вместе с num-late и num-lost у jb* это dropped:
    /// stats.dropped у sink при skew остаётся 0.
    underruns: Arc<AtomicU64>,
    rms: Arc<Mutex<RmsBook>>,
    stop_flag: Arc<AtomicBool>,
    /// pw-record общего line-in.
    captures: Vec<MonitorCapture>,
    /// pw-record монитора зоны. Снимается отдельно, чтобы не ронять RTP.
    monitors: Vec<ZoneMonitor>,
    /// pw-record хвостов видов. Не пишет в микшер микрофона.
    feed_captures: Vec<FeedCapture>,
    /// pw-play колонок. Нет, пока rx не через PipeWire.
    playback: Option<DesktopPlay>,
}

struct ZoneMonitor {
    zone_id: String,
    node: String,
    capture: MonitorCapture,
}

struct FeedCapture {
    capture: MonitorCapture,
}

/// Читает source кабеля отдельным процессом. pipewiresrc в агенте на этом
/// узле не забирает буферы: его цикл блокируется в set_state.
struct MonitorCapture {
    stop: Arc<AtomicBool>,
    child: Mutex<Option<Child>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl MonitorCapture {
    fn start(appsrc: gst::Element, node: &str) -> Result<Self> {
        Self::spawn(appsrc, source_capture_args(node))
    }

    /// Монитор своего синка. Цель — сам синк, `stream.capture.sink`.
    fn start_feed(appsrc: gst::Element, monitor: &str) -> Result<Self> {
        Self::spawn(appsrc, crate::virtual_output::feed_capture_args(monitor))
    }

    fn spawn(appsrc: gst::Element, args: Vec<String>) -> Result<Self> {
        let mut child = Command::new("pw-record")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("pw-record не запустился")?;
        let stdout = child.stdout.take().context("pw-record без stdout")?;
        let appsrc = appsrc
            .dynamic_cast::<gst_app::AppSrc>()
            .map_err(|_| anyhow::anyhow!("cap не appsrc"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let thread = std::thread::Builder::new()
            .name("zvucast-capture".into())
            .spawn(move || {
                feed_capture(
                    CaptureSource {
                        inner: stdout,
                        pending: child_stdout_queued,
                    },
                    appsrc,
                    stop_thread,
                )
            })
            .context("поток захвата")?;
        Ok(Self {
            stop,
            child: Mutex::new(Some(child)),
            thread: Mutex::new(Some(thread)),
        })
    }

    fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Ok(mut slot) = self.child.lock() {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Ok(mut slot) = self.thread.lock() {
            if let Some(thread) = slot.take() {
                let _ = thread.join();
            }
        }
    }
}

impl Drop for MonitorCapture {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Воспроизведение отдельными pw-play: запасной sink или по процессу на узел.
/// pipewiresink в агенте этого не делает: в PAUSED он гасит поток, а ссылки
/// на выход остаются active, и часы колонок встают, пока жив процесс.
struct DesktopPlay {
    stop: Arc<AtomicBool>,
    players: Vec<Arc<Mutex<Option<Child>>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl DesktopPlay {
    fn start(appsink: gst::Element, route: crate::inputs::PlaybackRoute) -> Result<Self> {
        let appsink = appsink
            .dynamic_cast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("asink не appsink"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let players = playback_slots(&route);
        let stop_thread = stop.clone();
        let players_thread = players.clone();
        let thread = std::thread::Builder::new()
            .name("zvucast-play".into())
            .spawn(move || play_desktop(appsink, players_thread, stop_thread, route))
            .context("поток воспроизведения")?;
        Ok(Self {
            stop,
            players,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        for slot in &self.players {
            close_player(slot);
        }
        if let Ok(mut slot) = self.thread.lock() {
            if let Some(thread) = slot.take() {
                let _ = thread.join();
            }
        }
    }
}

impl Drop for DesktopPlay {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn playback_slots(route: &crate::inputs::PlaybackRoute) -> Vec<Arc<Mutex<Option<Child>>>> {
    let count = match route {
        crate::inputs::PlaybackRoute::Fallback => 1,
        crate::inputs::PlaybackRoute::Nodes(names) => names.len(),
    };
    (0..count).map(|_| Arc::new(Mutex::new(None))).collect()
}

fn play_desktop(
    appsink: gst_app::AppSink,
    players: Vec<Arc<Mutex<Option<Child>>>>,
    stop: Arc<AtomicBool>,
    route: crate::inputs::PlaybackRoute,
) {
    if let crate::inputs::PlaybackRoute::Nodes(names) = &route {
        if names.is_empty() {
            tracing::warn!("pw-play: выбранные выходы не совпали с живыми sink");
        }
    }
    // Соседний журнал: пауза между записями и нули в байтах. На звук не влияет.
    let mut tap = PlayTap::start();
    while !stop.load(Ordering::Relaxed) {
        let Some(sample) = appsink.try_pull_sample(gst::ClockTime::from_mseconds(200)) else {
            // Нет RTP — процесс не должен висеть suspended со ссылками.
            // В Null try_pull возвращается сразу, без паузы это был бы пустой цикл.
            tap.mark(b'E', &[], None);
            for slot in &players {
                close_player(slot);
            }
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let Some(buffer) = sample.buffer() else { continue };
        let Ok(map) = buffer.map_readable() else { continue };
        if map.is_empty() {
            continue;
        }
        let bytes = map.as_slice();
        match &route {
            crate::inputs::PlaybackRoute::Fallback => {
                if let Some(slot) = players.first() {
                    let fate = write_player(slot, bytes, None);
                    let alive = fate.alive();
                    tap.mark(fate.kind(), bytes, fate.queued());
                    if !alive {
                        close_player(slot);
                    }
                }
            }
            crate::inputs::PlaybackRoute::Nodes(names) => {
                for (index, (slot, name)) in players.iter().zip(names.iter()).enumerate() {
                    let fate = write_player(slot, bytes, Some(name.as_str()));
                    let alive = fate.alive();
                    // Один и тот же буфер уходит во все слоты. Журнал ведём по первому.
                    if index == 0 {
                        tap.mark(fate.kind(), bytes, fate.queued());
                    }
                    if !alive {
                        close_player(slot);
                    }
                }
            }
        }
    }
    for slot in &players {
        close_player(slot);
    }
}

/// Пишет буфер в stdin pw-play.
///
/// Мьютекс слота держится только на spawn и на дублирование дескриптора.
/// Запись идёт в клон, уже без guard: процесс, который перестал читать,
/// не блокирует остальные слоты и не мешает `DesktopPlay::shutdown` убить
/// его. `false` — процесс умер, слот надо закрыть. Полный пайп — не смерть:
/// лишний буфер сбрасывается, `pw-play` остаётся.
fn write_player(slot: &Mutex<Option<Child>>, bytes: &[u8], sink_node: Option<&str>) -> PushFate {
    let owned = {
        let Ok(mut guard) = slot.lock() else {
            return PushFate::Dead;
        };
        if guard.is_none() {
            match spawn_player(sink_node) {
                Ok(child) => *guard = Some(child),
                Err(e) => {
                    tracing::warn!("pw-play: {e:#}");
                    return PushFate::Dead;
                }
            }
        }
        let Some(child) = guard.as_mut() else {
            return PushFate::Dead;
        };
        let Some(stdin) = child.stdin.as_ref() else {
            return PushFate::Dead;
        };
        match stdin.as_fd().try_clone_to_owned() {
            Ok(owned) => owned,
            Err(e) => {
                tracing::warn!("pw-play: stdin: {e}");
                return PushFate::Dead;
            }
        }
    };
    if let Err(e) = set_fd_nonblocking(&owned, true) {
        tracing::warn!("pw-play: stdin: {e}");
        return PushFate::Dead;
    }
    let pipe = std::fs::File::from(owned);
    probe_stdin_unlocked(slot);
    push_playback(pipe, bytes)
}

/// Хватает ли в пайпе ядра места на весь буфер.
///
/// Сколько звука уже лежит в очереди, значения не имеет: потолок в 40 мс
/// вырезал щелчок, хотя ядро запись ещё принимало. Короткий хвост тоже
/// отказ — кусок кадра разъедет каналы.
fn playback_chunk_fits(free: usize, len: usize) -> bool {
    len > 0 && free >= len
}

/// Слот жив во всех ветках, кроме смерти процесса. Буфер мог не лечь,
/// только если пайп ядра не вмещает его целиком.
fn push_playback(mut pipe: std::fs::File, bytes: &[u8]) -> PushFate {
    if bytes.is_empty() {
        return PushFate::Written { queued_before: None };
    }
    let room = pipe_room(&pipe);
    if let Some((_, free)) = room {
        if !playback_chunk_fits(free, bytes.len()) {
            return PushFate::Dropped {
                queued_before: room.map(|(queued, _)| queued),
            };
        }
    }
    let queued_before = room.map(|(queued, _)| queued);
    match pipe.write_all(bytes) {
        Ok(()) => PushFate::Written { queued_before },
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => PushFate::Blocked { queued_before },
        Err(_) => PushFate::Dead,
    }
}

enum PushFate {
    Written { queued_before: Option<usize> },
    Dropped { queued_before: Option<usize> },
    Blocked { queued_before: Option<usize> },
    Dead,
}

impl PushFate {
    fn alive(&self) -> bool {
        !matches!(self, PushFate::Dead)
    }

    fn kind(&self) -> u8 {
        match self {
            PushFate::Written { .. } => b'W',
            PushFate::Dropped { .. } => b'D',
            PushFate::Blocked { .. } => b'B',
            PushFate::Dead => b'X',
        }
    }

    fn queued(&self) -> Option<usize> {
        match self {
            PushFate::Written { queued_before }
            | PushFate::Dropped { queued_before }
            | PushFate::Blocked { queued_before } => *queued_before,
            PushFate::Dead => None,
        }
    }
}

/// Журнал байтов, которые поток отдаёт в pw-play.
///
/// Строка пишется в другом потоке: файловый ввод здесь сам стал бы паузой,
/// которую измеряем. Канал короткий, при переполнении звук не ждёт.
struct PlayTap {
    tx: Option<mpsc::SyncSender<TapRec>>,
    logger: Option<JoinHandle<()>>,
    last: Option<Instant>,
    at_frame: u64,
}

struct TapRec {
    kind: u8,
    unix_us: u64,
    gap_us: u32,
    frames: u32,
    at_frame: u64,
    queued: u32,
    zero_frames: u32,
    longest_zero: u32,
    lead: u32,
    tail: u32,
}

impl PlayTap {
    fn start() -> Self {
        let (tx, rx) = mpsc::sync_channel(1024);
        let logger = std::thread::Builder::new()
            .name("play-tap".to_string())
            .spawn(move || play_tap_logger(rx))
            .ok();
        Self {
            tx: logger.as_ref().map(|_| tx),
            logger,
            last: None,
            at_frame: 0,
        }
    }

    fn mark(&mut self, kind: u8, bytes: &[u8], queued: Option<usize>) {
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        let now = Instant::now();
        let gap_us = self
            .last
            .map(|prev| {
                now.duration_since(prev)
                    .as_micros()
                    .min(u128::from(u32::MAX)) as u32
            })
            .unwrap_or(0);
        self.last = Some(now);
        let unix_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_micros() as u64)
            .unwrap_or(0);
        let frames = (bytes.len() / 4) as u32;
        let (zero_frames, longest_zero, lead, tail) = silence_in_s16le(bytes);
        let queued = queued.map(|n| n.min(u32::MAX as usize) as u32).unwrap_or(u32::MAX);
        let rec = TapRec {
            kind,
            unix_us,
            gap_us,
            frames,
            at_frame: self.at_frame,
            queued,
            zero_frames,
            longest_zero,
            lead,
            tail,
        };
        if kind == b'W' {
            self.at_frame = self.at_frame.saturating_add(u64::from(frames));
        }
        let _ = tx.try_send(rec);
    }
}

impl Drop for PlayTap {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(logger) = self.logger.take() {
            let _ = logger.join();
        }
    }
}

fn play_tap_logger(rx: mpsc::Receiver<TapRec>) {
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o644)
        .open("/tmp/zvucast-play-tap.log")
    {
        Ok(file) => file,
        Err(e) => {
            tracing::warn!("журнал проигрывателя не открылся: {e}");
            return;
        }
    };
    let _ = writeln!(
        file,
        "# kind unix_us gap_us frames at_frame queued zero_frames longest_zero lead tail"
    );
    let mut lines = 0u32;
    while let Ok(rec) = rx.recv() {
        if lines >= 200_000 {
            continue;
        }
        let queued = if rec.queued == u32::MAX {
            -1
        } else {
            rec.queued as i64
        };
        if writeln!(
            file,
            "{} {} {} {} {} {} {} {} {} {}",
            rec.kind as char,
            rec.unix_us,
            rec.gap_us,
            rec.frames,
            rec.at_frame,
            queued,
            rec.zero_frames,
            rec.longest_zero,
            rec.lead,
            rec.tail
        )
        .is_err()
        {
            return;
        }
        lines += 1;
        if lines % 64 == 0 {
            let _ = file.flush();
        }
    }
    let _ = file.flush();
}

/// Кадры, где оба канала тише порога, и самая длинная такая серия в буфере.
///
/// Порог 200 — тот же, что у разбора монитора. Тон в этот пайп приходит
/// с пиком около тысячи, так что серия длиннее кванта — уже дыра в байтах,
/// а не нуль на пересечении волны.
fn silence_in_s16le(bytes: &[u8]) -> (u32, u32, u32, u32) {
    const THRESH: u16 = 200;
    let mut zeros = 0u32;
    let mut run = 0u32;
    let mut longest = 0u32;
    let mut lead = 0u32;
    let mut tail = 0u32;
    let mut seen_loud = false;
    let mut index = 0;
    while index + 4 <= bytes.len() {
        let left = i16::from_le_bytes([bytes[index], bytes[index + 1]]);
        let right = i16::from_le_bytes([bytes[index + 2], bytes[index + 3]]);
        if left.unsigned_abs() < THRESH && right.unsigned_abs() < THRESH {
            zeros += 1;
            run += 1;
            if run > longest {
                longest = run;
            }
            if !seen_loud {
                lead = run;
            }
            tail = run;
        } else {
            seen_loud = true;
            run = 0;
            tail = 0;
        }
        index += 4;
    }
    (zeros, longest, lead, tail)
}

fn pipe_room(fd: &impl AsFd) -> Option<(usize, usize)> {
    let mut queued = 0;
    let raw = fd.as_fd().as_raw_fd();
    let read = unsafe { libc::ioctl(raw, libc::FIONREAD, &mut queued) };
    if read == -1 || queued < 0 {
        return None;
    }
    let size = unsafe { libc::fcntl(raw, libc::F_GETPIPE_SZ) };
    if size <= 0 {
        return None;
    }
    Some((queued as usize, (size as usize).saturating_sub(queued as usize)))
}

fn set_fd_nonblocking(fd: &impl std::os::fd::AsFd, nonblocking: bool) -> std::io::Result<()> {
    socket2::SockRef::from(fd).set_nonblocking(nonblocking)
}

/// В тестах проверяет, что слот уже отпущен, и может поставить запись на паузу.
fn probe_stdin_unlocked(_slot: &Mutex<Option<Child>>) {
    #[cfg(test)]
    {
        if STDIN_PROBE.with(|probe| probe.get()) {
            let _guard = _slot
                .try_lock()
                .expect("мьютекс слота занят на время записи в stdin");
            drop(_guard);
        }
        if let Some(pause) = STDIN_PAUSE.with(|pause| pause.take()) {
            let _ = pause.entered.send(());
            let _ = pause.release.recv();
        }
    }
}

#[cfg(test)]
struct StdinPause {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

#[cfg(test)]
thread_local! {
    static STDIN_PROBE: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static STDIN_PAUSE: std::cell::Cell<Option<StdinPause>> = std::cell::Cell::new(None);
}

fn spawn_player(sink_node: Option<&str>) -> Result<Child> {
    let target = match sink_node {
        Some(name) => {
            if crate::inputs::is_virtual_sink_name(name) {
                anyhow::bail!("виртуальный sink не цель pw-play");
            }
            name.to_string()
        }
        None => crate::pipeline::desktop_playback_target().context("нет sink кроме zvucast")?,
    };
    let ms = configured_play_ms();
    let stock = play_stock_frames(ms);
    apply_device_stock(&target, stock);
    tracing::info!(ms, stock, quantum = PLAY_QUANTUM, target = %target, "pw-play: запас колонки");
    let args = player_args(&target);
    Command::new("pw-play")
        .args(args.iter().map(String::as_str))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("pw-play не запустился")
}

fn close_player(slot: &Mutex<Option<Child>>) {
    let Ok(mut guard) = slot.lock() else { return };
    let Some(mut child) = guard.take() else { return };
    drop(child.stdin.take());
    let _ = child.kill();
    let _ = child.wait();
}

/// Просьба line-in, кадры. Граф этим не запирается. Захват фида
/// просит 240 и читает монитор своего синка. Чтение остаётся по 128 кадров:
/// кадр провода 5 мс собирается из этих кусков.
const CAPTURE_LATENCY: &str = "256";
/// Один read pw-record. Квант захвата больше, пайсер режет по 512 байт.
const CAPTURE_BYTES: usize = 128 * 2 * 2;

fn source_capture_args(node: &str) -> Vec<String> {
    [
        "--target",
        node,
        "--rate",
        "48000",
        "--channels",
        "2",
        "--format",
        "s16",
        "--latency",
        CAPTURE_LATENCY,
        "--raw",
        "-",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// Запас проигрывателя по умолчанию, миллисекунды.
/// `ZVUCAST_PLAY_MS`: 40, 20 или 5, затем перезапуск агента.
/// Это глубина кольца карты, не цикл всего графа. Граф не заперт.
const PLAY_MS_DEFAULT: u32 = 40;
const PLAY_MS_MAX: u32 = 200;
/// Период HiFiBerry. Кольцо карты считается этими квантами.
/// `pw-play` просит тот же размер у группы карты и не ставит замок.
const PLAY_QUANTUM: u32 = 256;
const PLAY_LATENCY: &str = "256";

fn play_ms(raw: Option<&str>) -> u32 {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return PLAY_MS_DEFAULT;
    };
    match raw.parse::<u32>() {
        Ok(ms) if ms >= 1 => ms.min(PLAY_MS_MAX),
        _ => PLAY_MS_DEFAULT,
    }
}

/// Миллисекунды в кадры при 48 кГц. 1 мс = 48 кадров.
fn play_latency_frames(ms: u32) -> u32 {
    ms.saturating_mul(48)
}

/// Кадры в кольце карты: целое число квантов по 256.
/// 40 мс → 2048, это 8 квантов. Середина кванта округляется вверх.
fn play_stock_frames(ms: u32) -> u32 {
    let frames = play_latency_frames(ms);
    let quanta = frames.saturating_add(PLAY_QUANTUM / 2) / PLAY_QUANTUM;
    quanta.max(1) * PLAY_QUANTUM
}

fn player_args(target: &str) -> Vec<String> {
    [
        "--raw",
        "--rate",
        "48000",
        "--channels",
        "2",
        "--format",
        "s16",
        "--latency",
        PLAY_LATENCY,
        "--target",
        target,
        "-",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// `pw-cli ls Node`: строка `id N, type ...`, ниже `node.name = "..."`.
fn node_id_from_list(text: &str, name: &str) -> Option<String> {
    let mut id = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("id ") {
            id = rest.split_once(',').map(|(num, _)| num.trim().to_string());
            continue;
        }
        if let Some(rest) = line.strip_prefix("node.name = ") {
            if rest.trim().trim_matches('"') == name {
                return id;
            }
        }
    }
    None
}

/// Ставит запас в кольцо уже открытой карты. Файл WirePlumber задаёт то же
/// значение по умолчанию; ручка может его сменить без перезапуска PipeWire.
/// Ошибка не роняет плеер: без команды остаётся запас из файла.
fn apply_device_stock(node_name: &str, frames: u32) {
    let listed = Command::new("timeout")
        .args(["3", "pw-cli", "ls", "Node"])
        .output();
    let Ok(listed) = listed else {
        tracing::warn!(node = node_name, "запас колонки: pw-cli не запустился");
        return;
    };
    if !listed.status.success() {
        tracing::warn!(node = node_name, "запас колонки: список узлов пуст");
        return;
    }
    let text = String::from_utf8_lossy(&listed.stdout);
    let Some(id) = node_id_from_list(&text, node_name) else {
        tracing::warn!(node = node_name, "запас колонки: узел не найден");
        return;
    };
    let param = format!("{{ params = [ \"api.alsa.headroom\" {frames} ] }}");
    let set = Command::new("timeout")
        .args(["3", "pw-cli", "set-param", &id, "Props", &param])
        .output();
    match set {
        Ok(out) if out.status.success() => {
            tracing::info!(node = node_name, id, frames, "запас колонки: кольцо");
        }
        Ok(out) => {
            let err = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(node = node_name, frames, %err, "запас колонки: кольцо не встало");
        }
        Err(e) => {
            tracing::warn!(node = node_name, "запас колонки: {e}");
        }
    }
}

fn configured_play_ms() -> u32 {
    play_ms(std::env::var("ZVUCAST_PLAY_MS").ok().as_deref())
}

/// Когда слать уже готовый кусок.
///
/// Срок в прошлом не догоняем пачкой: иначе квант PipeWire (пачка кусков,
/// прочитанная сразу после ожидания) уходит в сокет одним залпом. Следующий
/// кусок ждёт полный период. Живой захват этим больше не пользуется:
/// слоты считает [`FeedPacer`].
#[cfg(test)]
fn pace_deadline(previous: Option<Instant>, now: Instant, period: Duration) -> (Option<Duration>, Instant) {
    match previous {
        Some(at) if at > now => (Some(at - now), at + period),
        _ => (None, now + period),
    }
}

/// Кадр на проводе. 5 мс, 240 отсчётов на канал, 960 байт. С RTP-заголовком
/// это меньше MTU 1400. Квант захвата остаётся 128: его копим, а не шлём.
const FRAME_BYTES: usize = 240 * 2 * 2;
const FRAME_NS: u64 = 5_000_000;
/// Дольше этого один слот не ждём. Труба захвата конечная.
const PACE_MAX_SLEEP: Duration = Duration::from_millis(10);
/// Долг старше этого не отдаём пачкой. Срок слота начинается заново.
const PACE_REANCHOR: Duration = Duration::from_millis(100);

/// Что слот отдаёт на провод. Тишину в микшер не кладём: нули там и есть хрип.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaceSlot {
    Wait,
    Frame,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PaceRelease {
    slot: PaceSlot,
    /// Срок был старше [`PACE_REANCHOR`] и начался заново.
    reanchored: bool,
}

/// Слоты по часам стены. Один кадр за слот. Второй кадр в том же слоте — только
/// если срок уже был и после этого кадра осталось больше последнего куска
/// захвата плюс ещё один кадр. Третьего нет. Первый кадр сессии бонус не берёт:
/// срок ещё не стоял, сразу ставится «сейчас плюс интервал».
struct FeedPacer {
    due: Option<Instant>,
    interval: Duration,
    frame_len: usize,
    /// Последний увиденный кусок захвата, байт. Порог бонуса — он плюс кадр.
    max_chunk: usize,
    bonus_taken: bool,
}

impl FeedPacer {
    fn new(frame_len: usize, interval: Duration) -> FeedPacer {
        FeedPacer {
            due: None,
            interval,
            frame_len,
            max_chunk: 0,
            bonus_taken: false,
        }
    }

    fn on_chunk(&mut self, len: usize) {
        self.max_chunk = self.max_chunk.max(len);
    }

    fn wake(&mut self) {
        self.bonus_taken = false;
    }

    fn due(&self) -> Option<Instant> {
        self.due
    }

    fn release(&mut self, now: Instant, queued: usize) -> PaceRelease {
        let mut reanchored = false;
        match self.due {
            Some(due) if due > now => {
                return PaceRelease {
                    slot: PaceSlot::Wait,
                    reanchored: false,
                };
            }
            Some(due) if now.duration_since(due) > PACE_REANCHOR => {
                reanchored = true;
                self.due = None;
            }
            Some(_) | None => {}
        }
        if queued < self.frame_len {
            return PaceRelease {
                slot: PaceSlot::Wait,
                reanchored,
            };
        }
        let left = queued - self.frame_len;
        // Бонус держит срок на месте. Пока срока не было, даже толстый хвост
        // уходит одним кадром: второй вызов в то же мгновение уже ждёт.
        let bonus = self.due.is_some()
            && !self.bonus_taken
            && left >= self.max_chunk.saturating_add(self.frame_len);
        self.due = match self.due {
            Some(due) if bonus => Some(due),
            other => Some(other.unwrap_or(now) + self.interval),
        };
        self.bonus_taken = bonus;
        PaceRelease {
            slot: PaceSlot::Frame,
            reanchored,
        }
    }
}

/// Метка кадра — якорь плюс число отсчётов. Якорь двигается только вперёд,
/// иначе уже отданный кадр получил бы ту же метку ещё раз.
struct PtsClock {
    base_ns: u64,
    samples: usize,
}

impl PtsClock {
    fn pts_ns(&self) -> u64 {
        const PER_SEC: usize = 48_000 * 2;
        self.base_ns + (self.samples as u64) * 1_000_000_000 / PER_SEC as u64
    }

    fn advance(&mut self, interleaved: usize) {
        const PER_SEC: usize = 48_000 * 2;
        self.samples += interleaved;
        if self.samples >= PER_SEC {
            let secs = self.samples / PER_SEC;
            self.base_ns += secs as u64 * 1_000_000_000;
            self.samples -= secs * PER_SEC;
        }
    }

    fn reanchor(&mut self, anchor_ns: u64) {
        if anchor_ns > self.pts_ns() {
            self.base_ns = anchor_ns;
            self.samples = 0;
        }
    }
}

struct CaptureSource<R> {
    inner: R,
    /// Глубина трубы. Чтение её не смотрит: ноль не значит, что квант ещё не пришёл.
    #[allow(dead_code)]
    pending: fn(&R) -> usize,
}

impl<R: Read> Read for CaptureSource<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

fn child_stdout_queued(stdout: &std::process::ChildStdout) -> usize {
    pipe_room(stdout).map(|(queued, _)| queued).unwrap_or(0)
}

fn push_capture_chunk(appsrc: &gst_app::AppSrc, raw: &[u8], pts: u64, dur_ns: u64) -> bool {
    let mut gstbuf = match gst::Buffer::with_size(raw.len()) {
        Ok(buf) => buf,
        Err(_) => return false,
    };
    {
        let Some(gstbuf) = gstbuf.get_mut() else {
            return false;
        };
        {
            let Ok(mut map) = gstbuf.map_writable() else {
                return false;
            };
            map.as_mut().copy_from_slice(raw);
        }
        gstbuf.set_pts(gst::ClockTime::from_nseconds(pts));
        gstbuf.set_duration(gst::ClockTime::from_nseconds(dur_ns));
    }
    !matches!(
        appsrc.push_buffer(gstbuf),
        Err(gst::FlowError::Flushing) | Err(gst::FlowError::Eos)
    )
}

fn feed_capture(
    mut stdout: CaptureSource<impl Read>,
    appsrc: gst_app::AppSrc,
    stop: Arc<AtomicBool>,
) {
    // Квант PipeWire — не кадр. Читаем по 128 кадров и копим. На провод уходит
    // кадр 5 мс. Пачку не размечаем одной меткой и не вычитываем в копилку
    // целиком: иначе каждый слот берёт бонус. За пробуждение — один квант и
    // не больше двух кадров. Срок впереди — ждём его, но не дольше 10 мс.
    // Срок позади дополнительно не спим: чтение кванта уже ждало звук, второй
    // сон уводит слот за 100 мс и шкала начинается заново на каждом кадре.
    // Старше 100 мс долг прощается. Метка — время прихода минус то, что ещё
    // лежит в копилке, и только вперёд. Дальше 32 мс впереди часов пайплайна
    // кадр не ставим, ждём. Тишину в микшер не пишем.
    appsrc.set_property("do-timestamp", false);
    appsrc.set_property("max-buffers", 32u64);
    let mut raw = [0u8; CAPTURE_BYTES];
    let mut acc = Vec::<u8>::with_capacity(FRAME_BYTES * 4);
    let mut pacer = FeedPacer::new(FRAME_BYTES, Duration::from_nanos(FRAME_NS));
    let mut clock = PtsClock {
        base_ns: 0,
        samples: 0,
    };
    /// 256 кадров @ 48 кГц ≈ 5.3 мс. Запас сверху — дрожание чтения.
    const MAX_LEAD_NS: u64 = 32_000_000;
    while !stop.load(Ordering::Relaxed) {
        if stdout.read_exact(&mut raw).is_err() {
            break;
        }
        acc.extend_from_slice(&raw);
        pacer.on_chunk(raw.len());
        let running = appsrc.current_running_time().map(|t| t.nseconds()).unwrap_or(0);
        let queued_frames = (acc.len() / 4) as u64;
        let anchor = running.saturating_sub(queued_frames * 1_000_000_000 / 48_000);
        clock.reanchor(anchor);
        pacer.wake();
        let mut sent_frames = 0u32;
        loop {
            let rel = pacer.release(Instant::now(), acc.len());
            if rel.reanchored {
                tracing::info!("захват фида: долг слота больше 100 мс, шкала заново");
            }
            if rel.slot != PaceSlot::Frame || acc.len() < FRAME_BYTES {
                break;
            }
            let mut frame = [0u8; FRAME_BYTES];
            frame.copy_from_slice(&acc[..FRAME_BYTES]);
            acc.drain(..FRAME_BYTES);
            let pts = clock.pts_ns();
            let now_run = appsrc.current_running_time().map(|t| t.nseconds()).unwrap_or(0);
            if pts > now_run.saturating_add(MAX_LEAD_NS) {
                let wait = Duration::from_nanos(pts - now_run - MAX_LEAD_NS);
                if wait.as_nanos() >= 5_000_000 {
                    tracing::info!(
                        wait_ns = wait.as_nanos() as u64,
                        "захват фида: кусок ждал потолок метки"
                    );
                }
                std::thread::sleep(wait);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            if !push_capture_chunk(&appsrc, &frame, pts, FRAME_NS) {
                return;
            }
            clock.advance(FRAME_BYTES / 2);
            sent_frames += 1;
            if sent_frames >= 2 {
                break;
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now = Instant::now();
        let wait = match pacer.due() {
            Some(due) if due > now => (due - now).min(PACE_MAX_SLEEP),
            _ => Duration::ZERO,
        };
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }
}

impl Runner {
    /// tx-половина: alsasrc на capture-устройстве; всегда unicast-сборка
    /// (build_tx_unicast) — пустой набор эндпоинтов = tx молчит (tee без
    /// веток), multicast-фолбэка нет. alsasink'а нет — карта для queue_ms
    /// не извлекается.
    pub fn new_tx(
        pipeline_cfg: &crate::config::PipelineCfg,
        device: &str,
        zones: &[ZoneEndpoints],
        dscp: u8,
        bind: &mut dyn crate::pipeline::LocalUdp,
    ) -> Result<Runner> {
        let pipeline = crate::pipeline::build_tx_unicast(pipeline_cfg, device, zones, dscp, bind)?;
        Ok(Runner::from_pipeline_with_card(pipeline, None))
    }

    /// tx через PipeWire (build_tx_unicast_pw). capture_device не открывается:
    /// на десктопе ALSA занят сессией PipeWire.
    pub fn new_tx_pw(
        pipeline_cfg: &crate::config::PipelineCfg,
        zones: &[ZoneEndpoints],
        dscp: u8,
        target: Option<&str>,
        bind: &mut dyn crate::pipeline::LocalUdp,
    ) -> Result<Runner> {
        let pipeline = crate::pipeline::build_tx_unicast_pw(pipeline_cfg, zones, dscp, target, bind)?;
        let mut runner = Runner::from_pipeline_with_card(pipeline, None);
        if let Some(target) = target {
            let src = runner.pipeline.by_name("cap").context("tx pw: нет appsrc cap")?;
            let node = crate::pipeline::capture_node_name(target);
            runner.captures.push(MonitorCapture::start(src, &node)?);
        }
        for zone in zones {
            let Some(node) = zone.source.as_deref() else { continue };
            let name = crate::pipeline::capture_appsrc_name(&zone.zone_id);
            let src = runner
                .pipeline
                .by_name(&name)
                .with_context(|| format!("tx pw: нет appsrc {name}"))?;
            runner.monitors.push(ZoneMonitor {
                zone_id: zone.zone_id.clone(),
                node: node.to_string(),
                capture: MonitorCapture::start(src, node)?,
            });
        }
        Ok(runner)
    }

    /// Монитор входит в микс зоны или выходит из него. Пейлоадер и line-in
    /// остаются: новый RTP сбросил бы метки и колонка потеряла бы другие источники.
    pub fn reconcile_monitors(&mut self, zones: &[ZoneEndpoints]) -> Result<()> {
        crate::pipeline::reconcile_pw_monitors(&self.pipeline, zones)?;
        let wanted: Vec<(String, String)> = zones
            .iter()
            .filter_map(|z| z.source.clone().map(|node| (z.zone_id.clone(), node)))
            .collect();
        self.monitors.retain(|monitor| {
            let keep = wanted
                .iter()
                .any(|(id, node)| id == &monitor.zone_id && node == &monitor.node);
            if !keep {
                monitor.capture.shutdown();
            }
            keep
        });
        for (zone_id, node) in wanted {
            if self.monitors.iter().any(|m| m.zone_id == zone_id) {
                continue;
            }
            let src = self
                .pipeline
                .by_name(&crate::pipeline::capture_appsrc_name(&zone_id))
                .with_context(|| format!("tx pw: нет appsrc зоны {zone_id}"))?;
            self.monitors.push(ZoneMonitor {
                zone_id,
                node: node.clone(),
                capture: MonitorCapture::start(src, &node)?,
            });
        }
        Ok(())
    }

    /// Хвосты видов в том же пайплайне. pw-record пишет в appsrc хвоста,
    /// не в микшер микрофона. Ошибка pw-record не снимает хвост: сигнала нет.
    pub fn reconcile_feeds(
        &mut self,
        feeds: &[zproto::FeedSend],
        dscp: u8,
        bind: &mut dyn crate::pipeline::LocalUdp,
    ) -> Result<()> {
        for cap in self.feed_captures.drain(..) {
            cap.capture.shutdown();
        }
        crate::pipeline::reconcile_feed_tails(&self.pipeline, feeds, dscp, bind)?;
        // Тот же вид в той же зоне — один appsrc. Второй pw-record в него не стартует.
        let mut seen = std::collections::BTreeSet::new();
        for feed in feeds {
            let name = crate::pipeline::feed_appsrc_name(feed.kind.as_str(), &feed.zone_id);
            if !seen.insert(name.clone()) {
                continue;
            }
            let Some(src) = self.pipeline.by_name(&name) else {
                continue;
            };
            match MonitorCapture::start_feed(src, &feed.monitor) {
                Ok(capture) => self.feed_captures.push(FeedCapture { capture }),
                Err(e) => tracing::warn!(monitor = %feed.monitor, "pw-record вида: {e:#}"),
            }
        }
        Ok(())
    }

    /// Уровень каждого хвоста, который реально собран. Нет элемента — записи нет.
    /// Играющий хвост без свежего level — −90/−90, не 0: иначе тишина не копится.
    /// С `lev` микрофона эта пара не смешивается.
    pub fn feed_levels(&self, feeds: &[zproto::FeedSend]) -> Vec<zproto::FeedLevel> {
        let mut out = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for feed in feeds {
            let level_name = crate::pipeline::feed_level_name(feed.kind.as_str(), &feed.zone_id);
            if !seen.insert(level_name.clone()) {
                continue;
            }
            let Some(el) = self.pipeline.by_name(&level_name) else {
                continue;
            };
            let state = match el.current_state() {
                gst::State::Playing | gst::State::Paused => zproto::PipelineState::Playing,
                _ => zproto::PipelineState::Stopped,
            };
            let measured = self
                .rms
                .lock()
                .ok()
                .map(|book| book.recent(&level_name))
                .unwrap_or((0.0, 0.0));
            // 0/0 — level ещё не прислал буфер. Для играющего хвоста это тишина,
            // не «нет данных»: микрофонный `lev` по-прежнему остаётся 0.
            let (rms_l, rms_r) = if state == zproto::PipelineState::Playing && measured == (0.0, 0.0)
            {
                (-90.0, -90.0)
            } else {
                measured
            };
            out.push(zproto::FeedLevel {
                zone_id: feed.zone_id.clone(),
                kind: feed.kind,
                state,
                rms_l,
                rms_r,
            });
        }
        out
    }

    /// rx-половина: udpsrc → … → alsasink(slave-method=skew); карта ALSA
    /// из device — для queue_ms из /proc/asound. cm — матрица микширования
    /// зон (channel-matrix → mix-matrix в mconv).
    pub fn new_rx(
        pipeline_cfg: &crate::config::PipelineCfg,
        device: &str,
        cm: &ChannelMap,
        ports: &[u16],
        branches: &[(u16, ChannelMap)],
        bind: &mut dyn crate::pipeline::LocalUdp,
    ) -> Result<Runner> {
        let pipeline = crate::pipeline::build_rx_ports(pipeline_cfg, device, cm, ports, branches, bind)?;
        let alsa_card = alsa_card_from_device(device);
        Ok(Runner::from_pipeline_with_card(pipeline, alsa_card))
    }

    /// rx-половина через PipeWire (build_rx_pw): карты ALSA нет →
    /// queue_ms в телеметрии будет -1 (alsasink-only метрика /proc/asound).
    /// cm — матрица микширования зон (как в new_rx).
    pub fn new_rx_pw(
        pipeline_cfg: &crate::config::PipelineCfg,
        cm: &ChannelMap,
        route: crate::inputs::PlaybackRoute,
        ports: &[u16],
        branches: &[(u16, ChannelMap)],
        bind: &mut dyn crate::pipeline::LocalUdp,
    ) -> Result<Runner> {
        let pipeline = crate::pipeline::build_rx_pw_ports(pipeline_cfg, cm, ports, branches, bind)?;
        let mut runner = Runner::from_pipeline_with_card(pipeline, None);
        let sink = runner.pipeline.by_name("asink").context("appsink asink")?;
        runner.playback = Some(DesktopPlay::start(sink, route)?);
        Ok(runner)
    }

    pub fn reconcile_mix(
        &self,
        ports: &[u16],
        jb_ms: u32,
        branches: &[(u16, ChannelMap)],
        bind: &mut dyn crate::pipeline::LocalUdp,
    ) -> Result<()> {
        crate::pipeline::reconcile_mix(&self.pipeline, ports, jb_ms, branches, bind)
    }

    /// Запас существующих веток, без перешивки портов и без дропа runner.
    pub fn set_jitter_latency(&self, ports: &[u16], jb_ms: u32) {
        crate::pipeline::set_jitter_latency(&self.pipeline, ports, jb_ms);
    }

    pub fn from_pipeline_for_test(pipeline: gst::Pipeline) -> Runner {
        Runner::from_pipeline_with_card(pipeline, None)
    }

    /// Доступ к пайплайну в тестах (проверка структуры собранных элементов).
    #[cfg(test)]
    pub(crate) fn pipeline(&self) -> &gst::Pipeline {
        &self.pipeline
    }

    fn from_pipeline_with_card(pipeline: gst::Pipeline, alsa_card: Option<String>) -> Runner {
        Runner {
            level: pipeline.by_name("lev"),
            sink: pipeline.by_name("asink"),
            pipeline,
            alsa_card,
            started_at: Mutex::new(None),
            underruns: Arc::new(AtomicU64::new(0)),
            rms: Arc::new(Mutex::new(RmsBook::default())),
            stop_flag: Arc::new(AtomicBool::new(false)),
            captures: Vec::new(),
            monitors: Vec::new(),
            feed_captures: Vec::new(),
            playback: None,
        }
    }

    pub fn start(&self) -> Result<()> {
        // в gst-rs 0.23 set_state возвращает Err при StateChangeReturn::Failure
        self.stop_flag.store(false, Ordering::Relaxed);
        self.pipeline.set_state(gst::State::Playing)?;
        tracing::info!(
            current = ?self.pipeline.current_state(),
            pending = ?self.pipeline.pending_state(),
            "pipeline set_state(Playing)"
        );
        if let Ok(mut t) = self.started_at.lock() {
            *t = Some(Instant::now());
        }
        Ok(())
    }

    pub fn stop(&self) {
        // pw-play не гасим. start() его не создаёт заново: после Stop/Start
        // пайплайн снова Playing, а колонка остаётся без потока. Пока
        // пайплайн в Null, тот же поток сам закрывает pw-play — сэмплов нет.
        // Поток присоединяется в Drop.
        for capture in &self.captures {
            capture.shutdown();
        }
        for monitor in &self.monitors {
            monitor.capture.shutdown();
        }
        for cap in &self.feed_captures {
            cap.capture.shutdown();
        }
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            tracing::warn!("set_state(Null) не удался: {e}");
        }
        // bus-watch-поток выйдет из цикла при ближайшем пробуждении (≤BUS_WATCH_POLL)
        self.stop_flag.store(true, Ordering::Relaxed);
    }

    pub fn state(&self) -> PipelineState {
        match self.pipeline.current_state() {
            gst::State::Playing | gst::State::Paused => PipelineState::Playing,
            _ => PipelineState::Stopped,
        }
    }

    pub fn stats(&self) -> Stats {
        let mut s = Stats::default();
        if let Some(sink) = &self.sink {
            if sink.has_property("stats", None) {
                let st = sink.property::<gst::Structure>("stats");
                s.rendered = st.get::<u64>("rendered").unwrap_or_default();
            }
        }
        // dropped — заикания, не stats.dropped sink’а: при skew тот счётчик стоит на нуле.
        let mut late = 0u64;
        let mut lost = 0u64;
        for child in self.pipeline.children() {
            if !child.name().starts_with("jb") || !child.has_property("stats", None) {
                continue;
            }
            let st = child.property::<gst::Structure>("stats");
            late = late.saturating_add(st.get::<u64>("num-late").unwrap_or(0));
            lost = lost.saturating_add(st.get::<u64>("num-lost").unwrap_or(0));
        }
        s.dropped = stutter_count(self.underruns.load(Ordering::Relaxed), late, lost);
        s.queue_ms = self.alsa_card.as_deref().map_or(-1, read_alsa_queue_ms);
        if let Ok(rms) = self.rms.lock() {
            // Линия `lev` — только голова захвата. Хвосты видов сюда не входят.
            let (l, r) = rms.recent("lev");
            s.rms_l = l;
            s.rms_r = r;
        }
        s
    }

    pub fn spawn_bus_watch(&self, tx: mpsc::Sender<SupEvent>) -> JoinHandle<()> {
        let bus = self
            .pipeline
            .bus()
            .expect("у gst::Pipeline всегда есть шина");
        let rms = self.rms.clone();
        let stop = self.stop_flag.clone();
        let underruns = self.underruns.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let Some(msg) = bus.timed_pop_filtered(
                    gst::ClockTime::from_mseconds(BUS_WATCH_POLL.as_millis() as u64),
                    &[
                        gst::MessageType::Error,
                        gst::MessageType::Warning,
                        gst::MessageType::Element,
                    ],
                ) else {
                    continue;
                };
                if let Some(ev) = handle_message(&msg, &rms) {
                    if matches!(ev, SupEvent::Underrun) {
                        underruns.fetch_add(1, Ordering::Relaxed);
                    }
                    let failed = matches!(ev, SupEvent::Failed(_));
                    if let Err(e) = tx.send(ev) {
                        tracing::debug!("SupEvent {:?} не доставлен: {e}", e.0);
                    }
                    if failed {
                        break;
                    }
                }
            }
        })
    }

    /// Время с последнего успешного start(); Duration::ZERO до первого старта.
    /// При рестарте того же Runner (повторный start) отсчёт начинается заново.
    pub fn uptime(&self) -> Duration {
        self.started_at
            .lock()
            .ok()
            .and_then(|t| *t)
            .map_or(Duration::ZERO, |t| t.elapsed())
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        // pipewiresrc, брошенный в READY, роняет процесс в libgstpipewire
        // (SIGSEGV при dispose). В Null — до того, как пайплайн уничтожится.
        self.stop();
    }
}

/// "plughw:CARD=Codec,DEV=0" → "Codec"; без CARD= — None
fn alsa_card_from_device(device: &str) -> Option<String> {
    device
        .split(',')
        .find_map(|part| part.split_once("CARD=").map(|(_, c)| c.trim().to_string()))
        .filter(|c| !c.is_empty())
}

/// Задержка playback-очереди (мс) из /proc/asound/<card>/pcm0p/sub0/status:
/// delay (сэмплы) при state: RUNNING, пересчёт сэмплы→мс. Логика и формат —
/// как в measurement/sinkmon.c. Любая ошибка → -1 (метрика недоступна).
fn read_alsa_queue_ms(card: &str) -> i64 {
    let path = format!("/proc/asound/{card}/pcm0p/sub0/status");
    let Ok(text) = std::fs::read_to_string(path) else {
        return -1;
    };
    match parse_alsa_status(&text) {
        Some(samples) => samples * 1000 / RATE_HZ,
        None => -1,
    }
}

/// Заикания: underrun’ы шины плюс опоздавшие и потерянные пакеты jitterbuffer.
/// Насыщение, чтобы переполнение не обнулило счётчик.
fn stutter_count(underruns: u64, late: u64, lost: u64) -> u64 {
    underruns.saturating_add(late).saturating_add(lost)
}

/// Парсинг status-файла ALSA: delay (сэмплы) только при state: RUNNING.
fn parse_alsa_status(text: &str) -> Option<i64> {
    let mut running = false;
    let mut delay: Option<i64> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("state") {
            if rest.split(':').nth(1).is_some_and(|v| v.contains("RUNNING")) {
                running = true;
            }
        } else if let Some(rest) = line.strip_prefix("delay") {
            if let Some(v) = rest
                .split(':')
                .nth(1)
                .and_then(|v| v.trim().parse::<i64>().ok())
            {
                delay = Some(v);
            }
        }
    }
    if running { delay.filter(|&d| d >= 0) } else { None }
}

#[derive(Default)]
struct RmsBook {
    slots: HashMap<String, (f64, f64, Instant)>,
}

impl RmsBook {
    fn note(&mut self, key: &str, l: f64, r: f64) {
        self.slots.insert(key.to_string(), (l, r, Instant::now()));
    }

    /// Свежий замер одного level за последние 500 мс. Нет сигнала — 0.
    fn recent(&self, key: &str) -> (f64, f64) {
        let cutoff = Instant::now() - Duration::from_millis(500);
        match self.slots.get(key) {
            Some((l, r, at)) if *at >= cutoff => (*l, *r),
            _ => (0.0, 0.0),
        }
    }
}

fn handle_message(msg: &gst::MessageRef, rms: &Mutex<RmsBook>) -> Option<SupEvent> {
    use gst::MessageView;
    match msg.view() {
        MessageView::Element(e) => {
            let s = e.structure()?;
            if s.name() == "level" {
                if let Ok(arr) = s.get::<gst::glib::ValueArray>("rms") {
                    let l = arr
                        .first()
                        .and_then(|v| v.get::<f64>().ok())
                        .unwrap_or(f64::NEG_INFINITY);
                    let r = arr
                        .get(1)
                        .and_then(|v| v.get::<f64>().ok())
                        .unwrap_or(l);
                    let key = msg.src().map(|o| o.name().to_string()).unwrap_or_else(|| "lev".into());
                    if let Ok(mut g) = rms.lock() {
                        g.note(&key, l, r);
                    }
                }
                None
            } else if s.name() == "GstAudioSink"
                || s.to_string().to_lowercase().contains("underrun")
            {
                Some(SupEvent::Underrun)
            } else {
                None
            }
        }
        MessageView::Warning(w) => {
            let text = w.debug().map(|d| d.to_lowercase()).unwrap_or_default();
            if text.contains("underrun") {
                Some(SupEvent::Underrun)
            } else {
                None
            }
        }
        MessageView::Error(e) => Some(SupEvent::Failed(e.error().to_string())),
        _ => None,
    }
}

pub struct RestartPolicy {
    attempts: u32,
    max_fast: u32,
}

impl RestartPolicy {
    pub fn new(max_fast: u32) -> RestartPolicy {
        RestartPolicy { attempts: 0, max_fast }
    }

    pub fn next_delay(&mut self) -> Duration {
        self.attempts += 1;
        if self.attempts <= self.max_fast {
            Duration::ZERO
        } else {
            SLOW_RESTART_DELAY
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine_pipeline() -> gst::Pipeline {
        gst::parse::launch(
            "audiotestsrc volume=0.5 ! audio/x-raw,rate=48000,channels=2 ! \
             level name=lev ! fakesink",
        )
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap()
    }

    const ALSA_STATUS_RUNNING: &str = "state: RUNNING\n\
owner_pid   : 892\n\
trigger_time: 7569.410828042\n\
tstamp      : 7569.410828042\n\
delay       : 4800\n\
avail       : 960\n\
avail_max   : 1936\n\
hw_ptr      : 313632\n\
appl_ptr    : 313632\n";

    #[test]
    fn alsa_status_parsing() {
        assert_eq!(parse_alsa_status(ALSA_STATUS_RUNNING), Some(4800));
        // не RUNNING → метрика недоступна
        let prepared = ALSA_STATUS_RUNNING.replace("state: RUNNING", "state: PREPARED");
        assert_eq!(parse_alsa_status(&prepared), None);
        // нет поля delay → None
        let no_delay = ALSA_STATUS_RUNNING
            .lines()
            .filter(|l| !l.starts_with("delay"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(parse_alsa_status(&no_delay), None);
        // отрицательный delay недопустим (как d >= 0 в sinkmon.c)
        let negative = ALSA_STATUS_RUNNING.replacen("delay       : 4800", "delay       : -5", 1);
        assert_eq!(parse_alsa_status(&negative), None);
        assert_eq!(parse_alsa_status(""), None);
    }

    #[test]
    fn alsa_card_from_device_parsing() {
        assert_eq!(alsa_card_from_device("plughw:CARD=Codec").as_deref(), Some("Codec"));
        assert_eq!(
            alsa_card_from_device("plughw:CARD=CODEC,DEV=0").as_deref(),
            Some("CODEC")
        );
        assert_eq!(alsa_card_from_device("hw:CARD=PCH,DEV=1").as_deref(), Some("PCH"));
        assert_eq!(alsa_card_from_device("default"), None);
        assert_eq!(alsa_card_from_device("hw:0,0"), None);
        assert_eq!(alsa_card_from_device("plughw:CARD=").as_deref(), None);
    }

    #[test]
    fn runner_new_rx_pw_builds() {
        gst::init().unwrap();
        let r = Runner::new_rx_pw(&crate::config::PipelineCfg::default_rx(), &ChannelMap::default(), crate::inputs::PlaybackRoute::Fallback, &[5004], &[], &mut crate::pipeline::LoopbackUdp).unwrap();
        assert!(r.pipeline().by_name("asink").is_some());
    }

    #[test]
    fn stop_leaves_the_playback_thread_alive() {
        gst::init().unwrap();
        let r = Runner::new_rx_pw(&crate::config::PipelineCfg::default_rx(), &ChannelMap::default(), crate::inputs::PlaybackRoute::Fallback, &[5004], &[], &mut crate::pipeline::LoopbackUdp).unwrap();
        r.stop();
        let alive = std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| std::fs::read_to_string(e.path().join("comm")).ok().is_some_and(|n| n.trim() == "zvucast-play"));
        assert!(alive, "Stop не должен убивать поток pw-play: Start его не поднимает");
        drop(r);
    }

    #[test]
    fn pace_deadline_does_not_catch_up_in_a_burst() {
        let period = Duration::from_millis(3);
        let now = Instant::now();
        let (wait, next) = pace_deadline(None, now, period);
        assert!(wait.is_none());
        assert_eq!(next, now + period);

        let future = now + Duration::from_millis(2);
        let (wait, next) = pace_deadline(Some(future), now, period);
        assert_eq!(wait, Some(Duration::from_millis(2)));
        assert_eq!(next, future + period);

        let past = now - Duration::from_millis(10);
        let (wait, next) = pace_deadline(Some(past), now, period);
        assert!(wait.is_none(), "опоздавший срок не выплёвывает пачку");
        assert_eq!(next, now + period);
    }

    #[test]
    fn pacer_releases_one_frame_a_slot_and_one_bonus_when_the_tail_is_thick() {
        let t0 = Instant::now();
        let frame = FRAME_BYTES;
        let mut pacer = FeedPacer::new(frame, Duration::from_millis(5));
        assert_eq!(pacer.release(t0, 0).slot, PaceSlot::Wait);
        pacer.on_chunk(frame);
        pacer.wake();
        assert_eq!(pacer.release(t0, frame).slot, PaceSlot::Frame);
        assert_eq!(
            pacer.release(t0, frame).slot,
            PaceSlot::Wait,
            "следующий слот через 5 мс"
        );

        let t1 = t0 + Duration::from_millis(5);
        pacer.wake();
        assert_eq!(pacer.release(t1, 10 * frame).slot, PaceSlot::Frame);
        assert_eq!(
            pacer.release(t1, 9 * frame).slot,
            PaceSlot::Frame,
            "второй кадр того же слота"
        );
        assert_eq!(
            pacer.release(t1, 8 * frame).slot,
            PaceSlot::Wait,
            "третьего кадра в слоте нет"
        );
    }

    #[test]
    fn pacer_does_not_bonus_the_first_frame() {
        let t0 = Instant::now();
        let frame = FRAME_BYTES;
        let mut pacer = FeedPacer::new(frame, Duration::from_millis(5));
        pacer.on_chunk(frame);
        pacer.wake();
        assert_eq!(pacer.release(t0, 10 * frame).slot, PaceSlot::Frame);
        assert_eq!(
            pacer.release(t0, 9 * frame).slot,
            PaceSlot::Wait,
            "первый кадр сессии не берёт бонус"
        );
    }

    #[test]
    fn pacer_forgives_a_stall_past_100ms_without_a_burst() {
        let t0 = Instant::now();
        let frame = FRAME_BYTES;
        let mut pacer = FeedPacer::new(frame, Duration::from_millis(5));
        pacer.on_chunk(frame);
        pacer.wake();
        assert_eq!(pacer.release(t0, frame).slot, PaceSlot::Frame);
        let stalled = t0 + Duration::from_millis(185);
        pacer.wake();
        let rel = pacer.release(stalled, 10 * frame);
        assert!(rel.reanchored, "долг старше 100 мс прощается");
        assert_eq!(rel.slot, PaceSlot::Frame);
        assert_eq!(
            pacer.release(stalled, 9 * frame).slot,
            PaceSlot::Wait,
            "после прощения пачки нет"
        );
    }

    #[test]
    fn pts_clock_reanchors_only_forward() {
        let mut clock = PtsClock {
            base_ns: 0,
            samples: 0,
        };
        clock.reanchor(1_000_000_000);
        assert_eq!(clock.pts_ns(), 1_000_000_000);
        clock.advance(480);
        let after = clock.pts_ns();
        assert_eq!(after, 1_005_000_000);
        clock.reanchor(1_000_000_000);
        assert_eq!(clock.pts_ns(), after, "якорь позади шкалу не тянет");
        clock.reanchor(after + 1_000_000);
        assert_eq!(clock.pts_ns(), after + 1_000_000);
        clock.advance(480);
        assert_eq!(clock.pts_ns(), after + 6_000_000);
    }

    #[test]
    fn queue_ms_is_minus_one_for_unknown_card() {
        assert_eq!(read_alsa_queue_ms("NoSuchCardZvucast"), -1);
    }

    #[test]
    fn restart_policy_backoff() {
        let mut p = RestartPolicy::new(3);
        assert_eq!(p.next_delay(), Duration::ZERO);
        assert_eq!(p.next_delay(), Duration::ZERO);
        assert_eq!(p.next_delay(), Duration::ZERO);
        assert_eq!(p.next_delay(), Duration::from_secs(10));
        assert_eq!(p.next_delay(), Duration::from_secs(10));
    }

    #[test]
    fn runner_new_builds_pipeline_for_both_roles() {
        gstreamer::init().unwrap();
        let rx = Runner::new_rx(&crate::config::PipelineCfg::default_rx(), "plughw:CARD=Whatever", &ChannelMap::default(), &[5004], &[], &mut crate::pipeline::LoopbackUdp).unwrap();
        assert_eq!(rx.state(), PipelineState::Stopped);
        assert!(rx.level.is_some(), "level name=lev есть в rx");
        assert!(rx.sink.is_some(), "alsasink name=asink есть в rx");

        let tx = Runner::new_tx(&crate::config::PipelineCfg::default_tx(), "plughw:CARD=Whatever", &[], 0, &mut crate::pipeline::LoopbackUdp).unwrap();
        assert_eq!(tx.state(), PipelineState::Stopped);
        assert!(tx.level.is_some(), "level есть и в tx (rms в телеметрии tx)");
        assert!(tx.sink.is_none(), "в tx нет alsasink");
    }

    #[test]
    fn new_tx_with_empty_endpoints_builds_branchless_tee() {
        gstreamer::init().unwrap();
        let r = Runner::new_tx(&crate::config::PipelineCfg::default_tx(), "plughw:CARD=Whatever", &[], 0, &mut crate::pipeline::LoopbackUdp).unwrap();
        let children = r.pipeline().children();
        assert!(children.iter().any(|e| e.factory().unwrap().name() == "tee"),
                "tee на месте");
        assert!(children.iter().any(|e| e.name() == "usilence"),
                "пустой набор держит fakesink, иначе level не течёт");
        assert!(!children.iter().any(|e| e.factory().unwrap().name() == "udpsink"),
                "udpsink нет: пустой набор = тишина, не multicast");
    }

    /// Пачка кванта не схлопывается в один PTS и не выбрасывается
    /// leaky-очередью, пока udpsink ждёт метку.
    #[test]
    fn quantum_burst_keeps_its_samples_on_the_feed() {
        gst::init().unwrap();
        let zone_id = "hall";
        let tx = crate::pipeline::build_tx_unicast_pw(
            &crate::config::PipelineCfg::default_tx(),
            &[],
            0,
            None,
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        tx.set_state(gst::State::Playing).unwrap();
        let _ = tx.state(gst::ClockTime::from_seconds(2));
        let feeds = vec![zproto::FeedSend {
            zone_id: zone_id.into(),
            kind: zproto::FeedKind::Virtual,
            monitor: format!("zvucast-{zone_id}-monitor"),
            endpoints: vec![zproto::Endpoint {
                ip: "127.0.0.1".into(),
                port: 41987,
            }],
        }];
        crate::pipeline::reconcile_feed_tails(
            &tx,
            &feeds,
            0,
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        let hold = tx.by_name("fhold-virtual-hall").unwrap();
        assert_eq!(hold.property::<u32>("max-size-buffers"), 0);
        assert_eq!(hold.property::<u64>("max-size-time"), 40_000_000);

        let rows: Arc<Mutex<Vec<(Option<gst::ClockTime>, Option<gst::ClockTime>)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let slot = rows.clone();
        tx.by_name("fpay-virtual-hall")
            .unwrap()
            .static_pad("sink")
            .unwrap()
            .add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
                if let Some(buf) = info.buffer() {
                    if let Ok(mut guard) = slot.lock() {
                        guard.push((buf.pts(), buf.duration()));
                    }
                }
                gst::PadProbeReturn::Ok
            })
            .unwrap();

        let src = tx
            .by_name("feed-virtual-hall")
            .unwrap()
            .dynamic_cast::<gst_app::AppSrc>()
            .unwrap();
        let quanta = 8usize;
        let per = 8usize;
        struct Bursts {
            left: usize,
            in_burst: usize,
        }
        impl std::io::Read for Bursts {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.left == 0 {
                    return Ok(0);
                }
                if self.in_burst == 8 {
                    self.in_burst = 0;
                    std::thread::sleep(Duration::from_millis(21));
                }
                let n = buf.len().min(self.left);
                buf[..n].fill(0);
                self.left -= n;
                self.in_burst += 1;
                Ok(n)
            }
        }
        fn no_tail(_: &Bursts) -> usize {
            0
        }
        feed_capture(
            CaptureSource {
                inner: Bursts {
                    left: CAPTURE_BYTES * quanta * per,
                    in_burst: 0,
                },
                pending: no_tail,
            },
            src,
            Arc::new(AtomicBool::new(false)),
        );
        let total = CAPTURE_BYTES * quanta * per;
        let want = total / FRAME_BYTES;
        assert!(total % FRAME_BYTES < FRAME_BYTES);
        assert_eq!(want, 34, "32768 байт — это 34 кадра по 5 мс и остаток");
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline && rows.lock().unwrap().len() < want {
            std::thread::sleep(Duration::from_millis(20));
        }
        let got = rows.lock().unwrap().clone();
        let _ = tx.set_state(gst::State::Null);
        assert_eq!(got.len(), want, "кадры пачки потерялись по дороге к payloader");
        for pair in got.windows(2) {
            let (p0, d0) = pair[0];
            let (p1, _) = pair[1];
            let p0 = p0.expect("pts");
            let d0 = d0.expect("duration");
            let p1 = p1.expect("pts");
            assert_eq!(d0.nseconds(), FRAME_NS, "на провод уходит кадр 5 мс");
            let step = p1.nseconds() - p0.nseconds();
            assert!(
                step >= FRAME_NS,
                "метки налегли: шаг {step}"
            );
        }
    }

    /// Затор в трубе не размечается вперёд одним окном и не выбрасывается.
    /// Куски выходят по периоду, метки остаются встык.
    #[test]
    fn backlog_is_paced_instead_of_stamped_ahead() {
        gst::init().unwrap();
        let zone_id = "hall";
        let tx = crate::pipeline::build_tx_unicast_pw(
            &crate::config::PipelineCfg::default_tx(),
            &[],
            0,
            None,
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        tx.set_state(gst::State::Playing).unwrap();
        let _ = tx.state(gst::ClockTime::from_seconds(2));
        let feeds = vec![zproto::FeedSend {
            zone_id: zone_id.into(),
            kind: zproto::FeedKind::Virtual,
            monitor: format!("zvucast-{zone_id}-monitor"),
            endpoints: vec![zproto::Endpoint {
                ip: "127.0.0.1".into(),
                port: 41988,
            }],
        }];
        crate::pipeline::reconcile_feed_tails(
            &tx,
            &feeds,
            0,
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        let rows: Arc<Mutex<Vec<(Instant, gst::ClockTime)>>> = Arc::new(Mutex::new(Vec::new()));
        let slot = rows.clone();
        tx.by_name("fpay-virtual-hall")
            .unwrap()
            .static_pad("sink")
            .unwrap()
            .add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
                if let Some(buf) = info.buffer() {
                    if let Some(pts) = buf.pts() {
                        if let Ok(mut guard) = slot.lock() {
                            guard.push((Instant::now(), pts));
                        }
                    }
                }
                gst::PadProbeReturn::Ok
            })
            .unwrap();
        struct InstantBacklog {
            left: usize,
        }
        impl std::io::Read for InstantBacklog {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.left == 0 {
                    return Ok(0);
                }
                let n = buf.len().min(self.left);
                buf[..n].fill(0);
                self.left -= n;
                Ok(n)
            }
        }
        fn no_tail(_: &InstantBacklog) -> usize {
            0
        }
        // 200 кусков по 128 кадров = 102400 байт = 106 кадров по 5 мс и остаток.
        let started = Instant::now();
        feed_capture(
            CaptureSource {
                inner: InstantBacklog {
                    left: CAPTURE_BYTES * 200,
                },
                pending: no_tail,
            },
            tx.by_name("feed-virtual-hall")
                .unwrap()
                .dynamic_cast::<gst_app::AppSrc>()
                .unwrap(),
            Arc::new(AtomicBool::new(false)),
        );
        let got = rows.lock().unwrap().clone();
        let elapsed = started.elapsed();
        let _ = tx.set_state(gst::State::Null);
        let total = CAPTURE_BYTES * 200;
        let want = total / FRAME_BYTES;
        assert_eq!(want, 106);
        assert!(
            got.len() == want,
            "кадры затора потерялись: {} из {want}",
            got.len()
        );
        for pair in got.windows(2) {
            let step = pair[1].1.nseconds() - pair[0].1.nseconds();
            assert!(
                (FRAME_NS..FRAME_NS + 1_500_000).contains(&step),
                "шаг метки затора {step}"
            );
        }
        let mut zero_run = 0usize;
        let mut longest_zero_run = 0usize;
        for pair in got.windows(2) {
            let gap = pair[1].0.saturating_duration_since(pair[0].0);
            if gap < Duration::from_millis(1) {
                zero_run += 1;
                longest_zero_run = longest_zero_run.max(zero_run);
            } else {
                zero_run = 0;
            }
        }
        assert!(
            longest_zero_run <= 1,
            "в один слот ушло больше двух кадров"
        );
        assert!(
            elapsed > Duration::from_millis(200),
            "затор ушёл залпом за {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(900),
            "затор читался медленнее периода: {elapsed:?}"
        );
    }

    /// Пауза 40 мс короче прощения в 100 мс. Хвост не сливается залпом:
    /// кадр 5 мс, в один слот не больше двух.
    #[test]
    fn late_quantum_drains_two_chunks_per_slot() {
        gst::init().unwrap();
        let zone_id = "hall";
        let tx = crate::pipeline::build_tx_unicast_pw(
            &crate::config::PipelineCfg::default_tx(),
            &[],
            0,
            None,
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        tx.set_state(gst::State::Playing).unwrap();
        let _ = tx.state(gst::ClockTime::from_seconds(2));
        let feeds = vec![zproto::FeedSend {
            zone_id: zone_id.into(),
            kind: zproto::FeedKind::Virtual,
            monitor: format!("zvucast-{zone_id}-monitor"),
            endpoints: vec![zproto::Endpoint {
                ip: "127.0.0.1".into(),
                port: 41989,
            }],
        }];
        crate::pipeline::reconcile_feed_tails(
            &tx,
            &feeds,
            0,
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        let rows: Arc<Mutex<Vec<(Instant, gst::ClockTime)>>> = Arc::new(Mutex::new(Vec::new()));
        let slot = rows.clone();
        tx.by_name("fpay-virtual-hall")
            .unwrap()
            .static_pad("sink")
            .unwrap()
            .add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
                if let Some(buf) = info.buffer() {
                    if let Some(pts) = buf.pts() {
                        if let Ok(mut guard) = slot.lock() {
                            guard.push((Instant::now(), pts));
                        }
                    }
                }
                gst::PadProbeReturn::Ok
            })
            .unwrap();
        struct TwoQuanta {
            left: usize,
            stalled: bool,
        }
        impl std::io::Read for TwoQuanta {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.left == 0 {
                    return Ok(0);
                }
                if self.left == CAPTURE_BYTES * 8 && !self.stalled {
                    self.stalled = true;
                    std::thread::sleep(Duration::from_millis(40));
                }
                let n = buf.len().min(self.left);
                buf[..n].fill(0);
                self.left -= n;
                Ok(n)
            }
        }
        fn tail(src: &TwoQuanta) -> usize {
            src.left
        }
        feed_capture(
            CaptureSource {
                inner: TwoQuanta {
                    left: CAPTURE_BYTES * 16,
                    stalled: false,
                },
                pending: tail,
            },
            tx.by_name("feed-virtual-hall")
                .unwrap()
                .dynamic_cast::<gst_app::AppSrc>()
                .unwrap(),
            Arc::new(AtomicBool::new(false)),
        );
        let want = (CAPTURE_BYTES * 16) / FRAME_BYTES;
        assert_eq!(want, 8);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && rows.lock().unwrap().len() < want {
            std::thread::sleep(Duration::from_millis(20));
        }
        let got = rows.lock().unwrap().clone();
        let _ = tx.set_state(gst::State::Null);
        assert_eq!(got.len(), want, "опоздавший квант потерял кадры");
        for pair in got.windows(2) {
            let step = pair[1].1.nseconds() - pair[0].1.nseconds();
            assert!(step >= FRAME_NS, "метки налегли: шаг {step}");
        }
        // Восемь квантов до паузы — четыре кадра. Пятый выходит после 40 мс.
        let stall_gap = got[4].0.saturating_duration_since(got[3].0);
        assert!(
            stall_gap >= Duration::from_millis(30),
            "пауза чтения не дошла до провода: {stall_gap:?}"
        );
        // Долг 40 мс меньше 100 мс, поэтому хвост, уже лежащий в чтении,
        // догоняется сразу, а не второй паузой.
        let late = &got[4..];
        let span = late.last().unwrap().0.saturating_duration_since(late[0].0);
        assert!(
            span < Duration::from_millis(80),
            "хвост после короткой паузы не догнал слот: {span:?}"
        );
    }

    #[test]
    fn stats_flow_for_test_pipeline() {
        gstreamer::init().unwrap();
        let r = Runner::from_pipeline_for_test(sine_pipeline());
        r.spawn_bus_watch(mpsc::channel().0);
        r.start().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while r.stats().rms_l == 0.0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }

        let st = r.stats();
        assert!(st.rms_l < 0.0 && st.rms_l > -20.0, "rms_l={}", st.rms_l);
        assert!(st.rms_r < 0.0 && st.rms_r > -20.0, "rms_r={}", st.rms_r);
        assert!(
            (st.rms_l - st.rms_r).abs() < 0.01,
            "каналы совпадают: {} vs {}",
            st.rms_l,
            st.rms_r
        );
        // fakesink не публикует stats, alsa_card нет — fallback-значения
        assert_eq!(st.rendered, 0);
        assert_eq!(st.dropped, 0);
        assert_eq!(st.queue_ms, -1);
        assert_eq!(r.state(), PipelineState::Playing);

        r.stop();
        assert_eq!(r.state(), PipelineState::Stopped);
    }

    #[test]
    fn uptime_counts_from_start() {
        gstreamer::init().unwrap();
        let r = Runner::from_pipeline_for_test(sine_pipeline());
        assert_eq!(r.uptime(), Duration::ZERO, "до старта uptime=0");

        r.start().unwrap();
        let u1 = r.uptime();
        assert!(u1 > Duration::ZERO);
        std::thread::sleep(Duration::from_millis(120));
        assert!(r.uptime() > u1, "uptime растёт после start()");
        r.stop();
    }

    #[test]
    fn bus_watch_exits_on_stop() {
        gstreamer::init().unwrap();
        let r = Runner::from_pipeline_for_test(sine_pipeline());
        let watch = r.spawn_bus_watch(mpsc::channel().0);
        r.start().unwrap();
        std::thread::sleep(Duration::from_millis(200));

        r.stop();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !watch.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(watch.is_finished(), "bus-watch-поток не завершился после stop()");
    }

    #[test]
    fn bus_watch_reports_underrun() {
        gstreamer::init().unwrap();
        let r = Runner::from_pipeline_for_test(sine_pipeline());
        let (tx, rx) = mpsc::channel();
        r.spawn_bus_watch(tx);
        r.start().unwrap();

        // синтетическое element-сообщение от GstAudioSink (так постит alsasink)
        let msg = gst::message::Element::new(gst::Structure::builder("GstAudioSink").build());
        r.pipeline.post_message(msg).unwrap();

        let ev = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("ждём Underrun");
        assert_eq!(ev, SupEvent::Underrun);
        // underrun шины должен попасть в dropped: stats.dropped у sink при skew стоит на нуле
        assert!(
            r.stats().dropped >= 1,
            "после Underrun dropped={}, ждали хотя бы 1",
            r.stats().dropped
        );
        r.stop();
    }

    #[test]
    fn play_ms_defaults_to_forty_and_scales_down() {
        assert_eq!(play_ms(None), 40);
        assert_eq!(play_ms(Some("")), 40);
        assert_eq!(play_ms(Some("нет")), 40);
        assert_eq!(play_ms(Some("0")), 40);
        assert_eq!(play_latency_frames(play_ms(None)), 1920);
        assert_eq!(play_stock_frames(40), 2048);
        assert_eq!(play_ms(Some("20")), 20);
        assert_eq!(play_latency_frames(20), 960);
        assert_eq!(play_stock_frames(20), 1024);
        assert_eq!(play_ms(Some(" 5 ")), 5);
        assert_eq!(play_latency_frames(5), 240);
        assert_eq!(play_stock_frames(5), 256);
        assert_eq!(play_stock_frames(1), 256);
        assert_eq!(play_ms(Some("400")), 200);
        let args = player_args("alsa_output.usb-test");
        let latency_at = args.iter().position(|arg| arg == "--latency").expect("latency");
        assert_eq!(args[latency_at + 1], PLAY_LATENCY);
        assert!(!args.iter().any(|arg| arg.contains("lock-quantum")));
        assert!(!args.iter().any(|arg| arg == "2048"));
    }

    #[test]
    fn node_id_from_list_finds_the_named_node() {
        let text = "\
\tid 59, type PipeWire:Interface:Node/3
\t\tnode.name = \"alsa_output.usb-Burr-Brown_from_TI_USB_Audio_CODEC-00.analog-stereo-output\"
\tid 101, type PipeWire:Interface:Node/3
\t\tnode.name = \"pw-play\"
";
        assert_eq!(
            node_id_from_list(text, "alsa_output.usb-Burr-Brown_from_TI_USB_Audio_CODEC-00.analog-stereo-output").as_deref(),
            Some("59")
        );
        assert_eq!(node_id_from_list(text, "нет").as_deref(), None);
    }

    #[test]
    fn stutter_count_adds_underruns_and_late_packets() {
        assert_eq!(stutter_count(0, 0, 0), 0);
        assert_eq!(stutter_count(1, 2, 4), 7);
        assert_eq!(stutter_count(u64::MAX, 1, 0), u64::MAX);
    }

    #[test]
    fn stats_dropped_reads_jitterbuffer_counters() {
        gstreamer::init().unwrap();
        let rx = Runner::new_rx(
            &crate::config::PipelineCfg::default_rx(),
            "plughw:CARD=Whatever",
            &ChannelMap::default(),
            &[5004],
            &[],
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        let jb = rx.pipeline().by_name("jb5004").expect("jb5004");
        let st = jb.property::<gst::Structure>("stats");
        // Тип поля должен совпасть с тем, что читает stats(): иначе unwrap_or(0)
        // тихо обнулит опоздания и потери.
        let late = st.get::<u64>("num-late").expect("num-late это u64");
        let lost = st.get::<u64>("num-lost").expect("num-lost это u64");
        assert_eq!(rx.stats().dropped, stutter_count(0, late, lost));
    }

    /// Потолок, если запись так и не отпустила слот. Успешный путь от паузы
    /// не зависит: канал открывается уже после try_lock внутри записи.
    const STUCK_WRITE_BOUND: Duration = Duration::from_secs(2);

    #[test]
    fn stuck_writer_does_not_keep_the_slot_mutex() {
        let stuck = Arc::new(Mutex::new(Some(sleeper_with_full_stdin())));
        let reap_stuck = Reap(Arc::clone(&stuck));
        let (cat, mut cat_out) = echoing_reader();
        let live = Arc::new(Mutex::new(Some(cat)));
        let reap_live = Reap(Arc::clone(&live));

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let stuck_write = Arc::clone(&stuck);
        let live_write = Arc::clone(&live);
        let worker = std::thread::spawn(move || {
            STDIN_PROBE.with(|probe| probe.set(true));
            STDIN_PAUSE.with(|pause| {
                pause.set(Some(StdinPause {
                    entered: entered_tx,
                    release: release_rx,
                }));
            });
            let wrote_stuck = write_player(&stuck_write, b"pcm!", None).alive();
            let wrote_live = write_player(&live_write, b"pcm!", None).alive();
            let _ = result_tx.send((wrote_stuck, wrote_live));
        });

        match entered_rx.recv_timeout(STUCK_WRITE_BOUND) {
            Ok(()) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => join_writer(worker),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("запись не дошла до stdin без мьютекса слота");
            }
        }
        {
            let guard = stuck
                .try_lock()
                .expect("shutdown не может забрать слот, пока идёт запись");
            assert!(guard.is_some(), "процесс ещё жив, пока слот не закрыли");
        }

        release_tx.send(()).expect("поток записи уже завершился");
        let (wrote_stuck, wrote_live) = match result_rx.recv_timeout(STUCK_WRITE_BOUND) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Disconnected) => join_writer(worker),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                close_player(&stuck);
                let _ = worker.join();
                panic!("запись в полный stdin не вернулась");
            }
        };
        worker.join().expect("поток записи");
        assert!(
            wrote_stuck,
            "полный stdin сбрасывает буфер и оставляет процесс"
        );
        assert!(
            stuck.lock().expect("слот").is_some(),
            "полный пайп не повод закрывать pw-play"
        );
        assert!(wrote_live, "следующий слот того же буфера должен получить байты");

        let mut got = [0u8; 4];
        cat_out
            .read_exact(&mut got)
            .expect("второй слот должен отдать тот же буфер");
        assert_eq!(&got, b"pcm!");

        drop(reap_stuck);
        drop(reap_live);
        assert!(stuck.lock().expect("слот").is_none());
        assert!(live.lock().expect("слот").is_none());
    }

    #[test]
    fn playback_chunk_fits_ignores_a_long_queue() {
        let chunk = 48_000 * 4 * 10 / 1_000;
        assert!(
            playback_chunk_fits(60_000, chunk),
            "40 мс в очереди не повод выбрасывать кусок, если место есть"
        );
        assert!(
            !playback_chunk_fits(chunk - 1, chunk),
            "кусок кадра не пишем"
        );
    }

    fn join_writer(worker: std::thread::JoinHandle<()>) -> ! {
        match worker.join() {
            Err(payload) => std::panic::resume_unwind(payload),
            Ok(()) => panic!("поток записи завершился без результата"),
        }
    }

    fn sleeper_with_full_stdin() -> Child {
        let mut child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("sleep");
        let stdin = child.stdin.as_mut().expect("stdin");
        set_fd_nonblocking(stdin, true).expect("O_NONBLOCK");
        let chunk = [0u8; 8192];
        loop {
            match stdin.write(&chunk) {
                Ok(0) => panic!("stdin закрылся до заполнения"),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("заполнение stdin: {e}"),
            }
        }
        // Дальше дескриптор снова блокирующий: запись обязана сама выставить
        // O_NONBLOCK, иначе write_all встанет на полном пайпе.
        set_fd_nonblocking(stdin, false).expect("блокирующий stdin");
        child
    }

    fn echoing_reader() -> (Child, std::process::ChildStdout) {
        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("cat");
        let stdout = child.stdout.take().expect("stdout");
        (child, stdout)
    }

    struct Reap(Arc<Mutex<Option<Child>>>);

    impl Drop for Reap {
        fn drop(&mut self) {
            if self.0.try_lock().is_ok() {
                close_player(&self.0);
            }
        }
    }

    #[test]
    fn bus_watch_reports_error_and_exits() {
        gstreamer::init().unwrap();
        // identity error-after=1 → детерминированный GST_MESSAGE_ERROR в рантайме
        let pipe = gst::parse::launch(
            "audiotestsrc num-buffers=10 ! audio/x-raw,rate=48000,channels=2 ! \
             identity error-after=1 ! fakesink",
        )
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        let r = Runner::from_pipeline_for_test(pipe);
        let (tx, rx) = mpsc::channel();
        let watch = r.spawn_bus_watch(tx);
        r.start().unwrap();

        let ev = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("ждём Failed");
        match ev {
            SupEvent::Failed(detail) => assert!(!detail.is_empty()),
            other => panic!("ожидали Failed, получили {other:?}"),
        }
        // после Error поток bus-watch завершается
        watch.join().unwrap();
        r.stop();
    }
}
