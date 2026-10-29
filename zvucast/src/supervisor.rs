use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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
        let mut child = Command::new("pw-record")
            .args([
                "--target",
                node,
                "--rate",
                "48000",
                "--channels",
                "2",
                "--format",
                "s16",
                "--latency",
                PW_LATENCY,
                "--raw",
                "-",
            ])
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
            .spawn(move || feed_capture(stdout, appsrc, stop_thread))
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
    while !stop.load(Ordering::Relaxed) {
        let Some(sample) = appsink.try_pull_sample(gst::ClockTime::from_mseconds(200)) else {
            // Нет RTP — процесс не должен висеть suspended со ссылками.
            // В Null try_pull возвращается сразу, без паузы это был бы пустой цикл.
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
                    if !write_player(slot, bytes, None) {
                        close_player(slot);
                    }
                }
            }
            crate::inputs::PlaybackRoute::Nodes(names) => {
                for (slot, name) in players.iter().zip(names.iter()) {
                    if !write_player(slot, bytes, Some(name.as_str())) {
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
/// его. `false` — слот надо закрыть и идти к следующему в том же буфере.
fn write_player(slot: &Mutex<Option<Child>>, bytes: &[u8], sink_node: Option<&str>) -> bool {
    let owned = {
        let Ok(mut guard) = slot.lock() else { return false };
        if guard.is_none() {
            match spawn_player(sink_node) {
                Ok(child) => *guard = Some(child),
                Err(e) => {
                    tracing::warn!("pw-play: {e:#}");
                    return false;
                }
            }
        }
        let Some(child) = guard.as_mut() else { return false };
        let Some(stdin) = child.stdin.as_ref() else { return false };
        match stdin.as_fd().try_clone_to_owned() {
            Ok(owned) => owned,
            Err(e) => {
                tracing::warn!("pw-play: stdin: {e}");
                return false;
            }
        }
    };
    if let Err(e) = set_fd_nonblocking(&owned, true) {
        tracing::warn!("pw-play: stdin: {e}");
        return false;
    }
    let mut pipe = std::fs::File::from(owned);
    probe_stdin_unlocked(slot);
    pipe.write_all(bytes).is_ok()
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
    Command::new("pw-play")
        .args([
            "--raw",
            "--rate",
            "48000",
            "--channels",
            "2",
            "--format",
            "s16",
            "--latency",
            PW_LATENCY,
            "--target",
            &target,
            "-",
        ])
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

/// Кадры для `pw-cat --latency` (число без единицы — это кадры, не миллисекунды).
/// 128 @ 48 кГц = 2.7 мс: период UCA202 и HiFiBerry. 20 мс раньше сидели
/// сверху кванта и не отдавались в звук.
const PW_LATENCY: &str = "128";
const CAPTURE_BYTES: usize = 128 * 2 * 2;

fn feed_capture(mut stdout: impl Read, appsrc: gst_app::AppSrc, stop: Arc<AtomicBool>) {
    // Квант приходит пачкой кусков по 128 кадров. do-timestamp ставит
    // им один PTS: на приёме куски лежат друг на друге. Монотонная метка
    // лечит хрип, но труба pw-record уже держит сотни миллисекунд. Если
    // разметить весь затор вперёд от «сейчас», колонка доигрывает его:
    // задержка около полусекунды. Дальше одного кванта кадр не ставим.
    appsrc.set_property("do-timestamp", false);
    appsrc.set_property("max-buffers", 32u64);
    let mut raw = [0u8; CAPTURE_BYTES];
    let mut next_ns: Option<u64> = None;
    let frames = (CAPTURE_BYTES / 4) as u64;
    let dur_ns = frames * 1_000_000_000 / 48_000;
    /// 1024 кадра @ 48 кГц ≈ 21 мс. Запас сверху — дрожание чтения.
    const MAX_LEAD_NS: u64 = 32_000_000;
    while !stop.load(Ordering::Relaxed) {
        if stdout.read_exact(&mut raw).is_err() {
            break;
        }
        let now = appsrc.current_running_time().map(|t| t.nseconds()).unwrap_or(0);
        let mut pts = next_ns.unwrap_or(now);
        if now > pts && now - pts > 20_000_000 {
            pts = now;
        }
        if pts > now.saturating_add(MAX_LEAD_NS) {
            continue;
        }
        let mut gstbuf = match gst::Buffer::with_size(raw.len()) {
            Ok(buf) => buf,
            Err(_) => break,
        };
        {
            let Some(gstbuf) = gstbuf.get_mut() else { break };
            {
                let Ok(mut map) = gstbuf.map_writable() else { break };
                map.as_mut().copy_from_slice(&raw);
            }
            gstbuf.set_pts(gst::ClockTime::from_nseconds(pts));
            gstbuf.set_duration(gst::ClockTime::from_nseconds(dur_ns));
        }
        next_ns = Some(pts + dur_ns);
        match appsrc.push_buffer(gstbuf) {
            Ok(_) => {}
            Err(gst::FlowError::Flushing) | Err(gst::FlowError::Eos) => break,
            Err(_) => {}
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
            match MonitorCapture::start(src, &feed.monitor) {
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
        feed_capture(
            Bursts {
                left: CAPTURE_BYTES * quanta * per,
                in_burst: 0,
            },
            src,
            Arc::new(AtomicBool::new(false)),
        );
        let want = quanta * per;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && rows.lock().unwrap().len() < want {
            std::thread::sleep(Duration::from_millis(20));
        }
        let got = rows.lock().unwrap().clone();
        let _ = tx.set_state(gst::State::Null);
        assert_eq!(got.len(), want, "пачка кванта потерялась по дороге к payloader");
        for pair in got.windows(2) {
            let (p0, d0) = pair[0];
            let (p1, _) = pair[1];
            let p0 = p0.expect("pts");
            let d0 = d0.expect("duration");
            let p1 = p1.expect("pts");
            assert_eq!(
                p1.nseconds(),
                p0.nseconds() + d0.nseconds(),
                "метки кусков должны идти встык"
            );
        }
    }

    /// Затор в трубе (сотни миллисекунд сразу) не размечается вперёд:
    /// на payloader уходит только окно около кванта.
    #[test]
    fn backlog_stays_inside_one_quantum() {
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
        let rows: Arc<Mutex<Vec<gst::ClockTime>>> = Arc::new(Mutex::new(Vec::new()));
        let slot = rows.clone();
        tx.by_name("fpay-virtual-hall")
            .unwrap()
            .static_pad("sink")
            .unwrap()
            .add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
                if let Some(buf) = info.buffer() {
                    if let Some(pts) = buf.pts() {
                        if let Ok(mut guard) = slot.lock() {
                            guard.push(pts);
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
        // 200 кусков по 128 кадров ≈ 533 мс, если разметить всё.
        feed_capture(
            InstantBacklog {
                left: CAPTURE_BYTES * 200,
            },
            tx.by_name("feed-virtual-hall")
                .unwrap()
                .dynamic_cast::<gst_app::AppSrc>()
                .unwrap(),
            Arc::new(AtomicBool::new(false)),
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && rows.lock().unwrap().len() < 8 {
            std::thread::sleep(Duration::from_millis(20));
        }
        std::thread::sleep(Duration::from_millis(80));
        let got = rows.lock().unwrap().clone();
        let _ = tx.set_state(gst::State::Null);
        assert!(!got.is_empty(), "окно кванта не дошло до payloader");
        assert!(got.len() < 40, "затор ушёл целиком: {} буферов", got.len());
        let span = got.last().unwrap().nseconds() - got.first().unwrap().nseconds();
        assert!(span < 60_000_000, "окно длиннее кванта: {span} нс");
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
            let wrote_stuck = write_player(&stuck_write, b"pcm!", None);
            let wrote_live = write_player(&live_write, b"pcm!", None);
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
        assert!(!wrote_stuck, "полный stdin не принимает буфер");
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
