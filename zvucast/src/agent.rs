//! Сборка агента: gst-домен (до двух пайплайнов + супервизор) + async-домен
//! (ws + zsync).
//!
//! ГРАНИЦА ПОТОКОВ: все gst-объекты (!Send) живут строго в одном выделенном
//! std::thread («gst-домен», fn `gst_domain`). Async-сторона (tokio: ws_client,
//! zsync) с gst-доменом общается только каналами:
//!   async → gst: tokio mpsc<CtlMsg> → bridge-задача → std mpsc<CtlMsg>
//!     (bridge перехватывает CtlMsg::BecomeMaster: пишет state и включает
//!     контроллер — до gst она не доходит);
//!   gst → async: tokio watch<Telemetry> (по-ролевые state/stats/ptp-offset);
//!   внутри gst-домена: по std mpsc<SupEvent> от bus-watch-потока Runner
//!   каждой половины (sup-каналы tx/rx раздельные).
//! Никакой другой код gst-объекты не трогает.
//!
//! Помимо ws_client агент поднимает stub-сервер (:8081, `stub_app`): страница
//! «мастер не найден» и ручной claim мастерства, пока ws_client не подключён
//! к живому мастеру (флаг `ws_connected`).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use zproto::{
    AgentMsg, BranchChannelMap, CaptureChan, ChannelMap, CtlMsg, EventKind, PipelineState, Role,
    RoleRx, RoleTx, Telemetry, VirtualZone, ZoneEndpoints,
};

use crate::config::AgentConfig;
use crate::discovery::{query_bound, DISCOVERY_PORT};
use crate::supervisor::{RestartPolicy, Runner, SupEvent};
use crate::ws_client;

/// Период публикации телеметрии в watch-канал.
const TELEM_PERIOD: Duration = Duration::from_millis(500);
/// Квант poll-цикла gst-домена: задержка доставки команд/sup-событий ≤ TICK.
const TICK: Duration = Duration::from_millis(50);
/// Число мгновенных рестартов пайплайна до перехода на медленный бэкофф
/// (RestartPolicy: N быстрых → 10 с).
const RESTART_MAX_FAST: u32 = 3;
/// State-файл, который читает контроллер при старте (путь из systemd-юнита
/// zvucast-controller). BecomeMaster и claim перезаписывают его; fn'ы
/// параметризованы путём, чтобы тесты не трогали /etc.
pub const STATE_PATH: &str = "/etc/zvucast/state.json";
/// Stub-сервер агента: страница «мастер не найден» и ручной claim, пока агент
/// не подключён к мастеру.
const STUB_LISTEN: &str = "0.0.0.0:8081";
/// Таймаут discovery-предпроверки claim: живой мастер должен ответить ~1 с.
const CLAIM_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(1);

pub async fn run(cfg: AgentConfig, node_id: String, version: String) -> Result<()> {
    // Роли разрешаются ДО запуска gst-домена и ws_client: и Hello, и набор
    // пайплайнов строятся по итоговому списку. Явный roles в конфиге
    // отключает авто-детект (но устройства валидируются).
    let roles = resolve_roles(&cfg, &|dev, role| device_probe(&cfg, dev, role))?;
    tracing::info!(?roles, "роли узла");

    // Один наблюдатель на процесс. spawn не открывает PTP: пока публикация
    // None, состояние Idle и смещение i64::MAX. start здесь не зовём — он
    // сразу сел бы на имя из конфига.
    let uplink = crate::uplink::watch(cfg.sync_interface.clone());
    let sync = zsync::spawn(cfg.ptp_priority);
    let ptp_offset = sync.offset_ns.clone();
    let mut ptp_link = uplink.clone();
    tokio::spawn(async move {
        loop {
            let name = ptp_link.borrow_and_update().clone().map(|link| link.name);
            sync.set_interface(name);
            if ptp_link.changed().await.is_err() {
                break;
            }
        }
    });

    let (telem_tx, telem_rx) = watch::channel(initial_telemetry(&roles));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<CtlMsg>(8);
    // события gst-домена → контроллер (через ws_client); закрытие канала
    // сигнализирует ws_client о смерти gst-домена
    let (events_tx, events_rx) = mpsc::channel::<AgentMsg>(16);
    // Флаг живой ws-сессии с мастером: ws_client ставит/снимает, stub-сервер
    // читает (claim отклоняется, пока мастер жив).
    let ws_connected = Arc::new(AtomicBool::new(false));

    // Stub-сервер (:8081): страница «мастер не найден» + ручной claim. Порт
    // занят — warn, агент продолжает без stub'а. Предпроверка claim шлёт
    // discovery-запрос на broadcast (как ws_client::Discovery::default).
    {
        let stub = stub_app(
            Path::new(STATE_PATH),
            ws_connected.clone(),
            Arc::new(SystemdCtl),
            format!("255.255.255.255:{DISCOVERY_PORT}"),
            uplink.clone(),
        );
        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(STUB_LISTEN).await {
                Ok(listener) => {
                    tracing::info!("stub-сервер: http://{STUB_LISTEN} (мастер не найден)");
                    if let Err(e) = axum::serve(listener, stub).await {
                        tracing::error!("stub-сервер завершился ошибкой: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!("stub-сервер: bind {STUB_LISTEN}: {e} — продолжаю без него")
                }
            }
        });
    }

    // Список входов снимаем один раз, до gst-потока. Повторный DeviceMonitor
    // при живом пайплайне может зависнуть и больше не отпустить реконнект.
    let inputs = if roles.contains(&Role::Tx) {
        let found = crate::inputs::discover_capture_channels();
        tracing::info!(n = found.len(), "устройства захвата");
        if found.is_empty() {
            crate::inputs::fallback_inputs(cfg.capture_device.as_deref())
        } else {
            found
        }
    } else {
        Vec::new()
    };
    let outputs = if roles.contains(&Role::Rx) {
        let found = crate::inputs::discover_playback_channels();
        tracing::info!(n = found.len(), "устройства воспроизведения");
        if found.is_empty() {
            crate::inputs::fallback_outputs(cfg.playback_device.as_deref())
        } else {
            found
        }
    } else {
        Vec::new()
    };

    // gst-домен: единственный владелец gst-объектов.
    let (gst_tx, gst_rx) = std_mpsc::channel::<CtlMsg>();
    let gst_cfg = cfg.clone();
    let gst_roles = roles.clone();
    let gst_ptp = ptp_offset.clone();
    let gst_inputs = inputs.clone();
    let gst_uplink = uplink.clone();
    std::thread::Builder::new()
        .name("zvucast-gst".into())
        .spawn(move || {
            gst_domain(
                gst_cfg, gst_roles, telem_tx, gst_ptp, gst_rx, events_tx, gst_inputs, gst_uplink,
            )
        })?;

    // bridge async → gst. BecomeMaster/DisableController перехватываются
    // здесь (async-домен, до gst): пишем state + включаем/выключаем
    // контроллер — узел становится/перестаёт быть мастером.
    // fs + systemctl блокирующие и systemctl может идти секунды — уходим в
    // spawn_blocking, не стопая worker-thread (review, Minor).
    tokio::spawn(async move {
        while let Some(ctl) = cmd_rx.recv().await {
            if let CtlMsg::BecomeMaster { state } = &ctl {
                let state = state.clone();
                tokio::task::spawn_blocking(move || match become_master(&state, &SystemdCtl) {
                    Ok(()) => tracing::info!("BecomeMaster: state записан, контроллер запущен"),
                    Err(e) => {
                        tracing::error!("BecomeMaster: занять роль мастера не удалось: {e:#}")
                    }
                });
                continue;
            }
            if let CtlMsg::DisableController = &ctl {
                tokio::task::spawn_blocking(move || match disable_master(&SystemdCtl) {
                    Ok(()) => tracing::info!("DisableController: контроллер выключен, маркер снят"),
                    Err(e) => {
                        tracing::error!("DisableController: выключить контроллер не удалось: {e:#}")
                    }
                });
                continue;
            }
            if gst_tx.send(ctl).is_err() {
                break;
            }
        }
    });

    let supports_virtual_output =
        roles.contains(&Role::Tx) && cfg.playback_backend == crate::config::Backend::PipeWire;
    ws_client::run(
        cfg,
        ws_client::Discovery::default(),
        &node_id,
        version,
        roles,
        inputs,
        outputs,
        supports_virtual_output,
        telem_rx,
        ptp_offset,
        ws_connected,
        cmd_tx,
        events_rx,
        uplink,
    )
    .await
}

// --- Трансфер/claim мастера (Task 10). ---

/// Как включить/выключить локальный контроллер. Прод — systemctl; в тестах —
/// заглушка (systemd не трогаем).
pub trait MasterCtl: Send + Sync {
    fn enable_controller(&self) -> anyhow::Result<()>;
    fn disable_controller(&self) -> anyhow::Result<()>;
}

/// Прод-impl: `systemctl enable --now` / `disable --now zvucast-controller`.
/// Агент работает под systemd от root — systemctl доступен.
pub struct SystemdCtl;

impl SystemdCtl {
    fn systemctl(&self, args: [&str; 3]) -> anyhow::Result<()> {
        let out = std::process::Command::new("systemctl")
            .args(args)
            .output()
            .context("запуск systemctl")?;
        anyhow::ensure!(
            out.status.success(),
            "systemctl {} {} {}: {}",
            args[0],
            args[1],
            args[2],
            String::from_utf8_lossy(&out.stderr).trim()
        );
        Ok(())
    }
}

impl MasterCtl for SystemdCtl {
    fn enable_controller(&self) -> anyhow::Result<()> {
        self.systemctl(["enable", "--now", "zvucast-controller"])
    }

    fn disable_controller(&self) -> anyhow::Result<()> {
        self.systemctl(["disable", "--now", "zvucast-controller"])
    }
}

/// Маркер одноразового пропуска discovery-предпроверки контроллера: агент
/// ставит его при BecomeMaster (трансфер стартует контроллер при ещё живом
/// старом мастере — без пропуска тот bail'ится в crash-loop и трансфер всегда
/// даёт 502), контроллер при старте удаляет и пропускает только проверку.
pub fn successor_marker_path(state_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.successor", state_path.display()))
}

fn remove_successor_marker(state_path: &Path) {
    let _ = std::fs::remove_file(successor_marker_path(state_path));
}

/// Записать state, поставить маркер преемника и включить контроллер — узел
/// становится мастером. Путь параметризован ради тестов; прод использует
/// [`STATE_PATH`].
pub fn become_master_to(path: &Path, state_json: &str, ctl: &dyn MasterCtl) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("каталог {}", parent.display()))?;
    }
    let marker = successor_marker_path(path);
    std::fs::write(&marker, b"")
        .with_context(|| format!("запись маркера в {}", marker.display()))?;
    let result = std::fs::write(path, state_json)
        .with_context(|| format!("запись state в {}", path.display()))
        .and_then(|_| ctl.enable_controller());
    if result.is_err() {
        // без стёртого маркера повторный старт контроллера миновал бы
        // discovery-предпроверку (некоординированный мастер)
        let _ = std::fs::remove_file(&marker);
    }
    result
}

/// Прод-обёртка: state пишется в [`STATE_PATH`].
pub fn become_master(state_json: &str, ctl: &dyn MasterCtl) -> Result<()> {
    become_master_to(Path::new(STATE_PATH), state_json, ctl)
}

/// Откат трансфера: снять маркер преемника и выключить локальный контроллер —
/// иначе при поздней смерти старого мастера узел стартанёт сам, со stale-state.
pub fn disable_master_to(path: &Path, ctl: &dyn MasterCtl) -> Result<()> {
    remove_successor_marker(path);
    ctl.disable_controller()
}

/// Прод-обёртка: маркер снимается рядом с [`STATE_PATH`].
pub fn disable_master(ctl: &dyn MasterCtl) -> Result<()> {
    disable_master_to(Path::new(STATE_PATH), ctl)
}

/// Предпроверка claim (review, Important #1): отвечает ли в сети живой
/// мастер. `Ok(Some(addr))` — мастер жив, claim следует отклонить (409).
/// Ошибка самого запроса (нет линка, привязка не удалась, нет route) НЕ
/// блокирует claim — блокирует только ответ мастера. Запрос уходит с
/// опубликованного интерфейса. `discovery_dest` параметризован ради тестов.
async fn claim_precheck(
    discovery_dest: &str,
    timeout: Duration,
    uplink: &watch::Receiver<Option<crate::uplink::ActiveLink>>,
) -> std::io::Result<Option<String>> {
    let Some(link) = uplink.borrow().clone() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "нет активного интерфейса",
        ));
    };
    query_bound(discovery_dest, timeout, &link.name, link.ipv4).await
}

/// Состояние stub-сервера: флаг живой ws-сессии с мастером (ставит ws_client),
/// путь state, способ включить контроллер и куда слать discovery-запрос
/// предпроверки claim.
#[derive(Clone)]
struct StubState {
    ws_connected: Arc<AtomicBool>,
    state_path: Arc<PathBuf>,
    ctl: Arc<dyn MasterCtl>,
    discovery_dest: String,
    uplink: watch::Receiver<Option<crate::uplink::ActiveLink>>,
}

/// Stub-приложение агента (:8081). Отдельная fn от spawn'а: тесты собирают
/// роутер на temp-пути с фейковым ctl и loopback-dest предпроверки.
pub fn stub_app(
    state_path: &Path,
    ws_connected: Arc<AtomicBool>,
    ctl: Arc<dyn MasterCtl>,
    discovery_dest: String,
    uplink: watch::Receiver<Option<crate::uplink::ActiveLink>>,
) -> Router {
    Router::new()
        .route("/", get(stub_index))
        .route("/api/master/status", get(stub_status))
        .route("/api/master/claim", post(stub_claim))
        .with_state(StubState {
            ws_connected,
            state_path: Arc::new(state_path.to_path_buf()),
            ctl,
            discovery_dest,
            uplink,
        })
}

async fn stub_index() -> Html<&'static str> {
    Html(include_str!("../static/stub.html"))
}

async fn stub_status(State(st): State<StubState>) -> Json<Value> {
    Json(json!({ "ws_connected": st.ws_connected.load(Ordering::Relaxed) }))
}

/// Ручной claim: мастер жив — 409; иначе свежий default-state и запуск
/// контроллер — 200. Default-state: новый мастер стартует с чистой
/// конфигурацией.
///
/// «Мастер жив» проверяем дважды (review, Important #1): флагом ws_connected
/// и discovery-запросом — ws_connected=false бывает при backoff-сне до 30 с
/// при живом мастере, и claim в этом окне дал бы split-brain.
///
/// Известное ограничение: два одновременных claim'а на разных узлах в пределах
/// окна опроса (~1 с) оба пройдут — ручная аварийная процедура без
/// автоматического взаимоисключения между узлами (v1).
async fn stub_claim(State(st): State<StubState>) -> Result<Json<Value>, (StatusCode, String)> {
    if st.ws_connected.load(Ordering::Relaxed) {
        return Err((
            StatusCode::CONFLICT,
            "мастер жив — claim отклонён".to_string(),
        ));
    }
    match claim_precheck(&st.discovery_dest, CLAIM_DISCOVERY_TIMEOUT, &st.uplink).await {
        Ok(Some(addr)) => {
            return Err((
                StatusCode::CONFLICT,
                format!("мастер жив: {addr} — claim отклонён"),
            ));
        }
        Ok(None) => {}
        Err(e) => tracing::warn!("claim: discovery-запрос не удался ({e:#}) — продолжаю claim"),
    }
    let state_json = serde_json::to_string(&crate::state::State::default()).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("сериализация state: {e}"),
        )
    })?;
    // fs + systemctl блокирующие — в spawn_blocking, не стопая worker-thread
    // (review, Minor).
    let state_path = st.state_path.clone();
    let ctl = st.ctl.clone();
    tokio::task::spawn_blocking(move || become_master_to(&state_path, &state_json, ctl.as_ref()))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("claim-задача не завершилась: {e}"),
            )
        })?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("claim: {e:#}")))?;
    Ok(Json(json!({ "ok": true })))
}

/// Гейт «старт только по Start» в gst-домене (фикс ревью).
///
/// `want_playing` ставится только явным Start и снимается Stop'ом. Все
/// рестарт-пути (ретрай первичной сборки, рестарт после Failed) приводят к
/// играющему пайплайну ТОЛЬКО при want_playing:
/// - Failed вооружает рестарт только при want_playing — иначе Error на шине
///   после Stop сам возвращал бы пайплайн в Playing вопреки команде юзера;
/// - ретрай первичной сборки при want_playing == false собирает пайплайн
///   без запуска — автостарта нет даже косвенного.
#[derive(Default)]
struct PlayGate {
    want_playing: bool,
    restart_at: Option<Instant>,
}

/// Чем выполнить назревший рестарт.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartKind {
    /// Пайплайн собран, но не играет — ждёт явного Start.
    BuildOnly,
    /// Собрать и сразу запустить.
    BuildAndStart,
}

impl PlayGate {
    fn on_start(&mut self) {
        self.want_playing = true;
    }

    fn on_stop(&mut self) {
        self.want_playing = false;
        // Stop отменяет и запланированный рестарт: без этого пайплайн
        // «сам возвращается» после Failed, несмотря на команду юзера.
        self.restart_at = None;
    }

    /// Failed вооружает рестарт только если юзер ждёт Playing.
    fn on_failed(&mut self, policy: &mut RestartPolicy) {
        if self.want_playing {
            self.arm(policy);
        }
    }

    /// Запланировать (пере)сборку по политике бэкоффа.
    fn arm(&mut self, policy: &mut RestartPolicy) {
        self.restart_at = Some(Instant::now() + policy.next_delay());
    }

    fn due(&self) -> bool {
        self.restart_at.is_some_and(|at| Instant::now() >= at)
    }

    /// Снимает дедлайн и возвращает, как именно рестартовать.
    fn take_kind(&mut self) -> RestartKind {
        self.restart_at = None;
        if self.want_playing {
            RestartKind::BuildAndStart
        } else {
            RestartKind::BuildOnly
        }
    }
}

/// Транспортное состояние tx в gst-домене: группы зон + dscp.
/// Пустые группы и пустой список не шлют udpsink, поэтому разница dscp
/// между двумя такими наборами пересборку не вызывает.
#[derive(Default)]
struct Transport {
    zones: Vec<ZoneEndpoints>,
    dscp: u8,
}

fn overlay_sources(
    zones: &[ZoneEndpoints],
    linked: &BTreeSet<String>,
    table: &crate::sendspin::ZoneTable,
) -> Vec<ZoneEndpoints> {
    zones
        .iter()
        .cloned()
        .map(|mut zone| {
            zone.source = table.source(&zone.zone_id, linked.contains(&zone.zone_id));
            zone
        })
        .collect()
}

fn zone_sources(
    zones: &[ZoneEndpoints],
    linked: &BTreeSet<String>,
    table: &crate::sendspin::ZoneTable,
) -> BTreeMap<String, Option<String>> {
    zones
        .iter()
        .map(|zone| {
            (
                zone.zone_id.clone(),
                table.source(&zone.zone_id, linked.contains(&zone.zone_id)),
            )
        })
        .collect()
}

fn sources_changed(
    previous: &BTreeMap<String, Option<String>>,
    desired: &BTreeMap<String, Option<String>>,
) -> bool {
    previous != desired
}

/// Имя или набор зон Sendspin. Порядок строк не важен.
fn sendspin_zones_changed(prev: &[VirtualZone], next: &[VirtualZone]) -> bool {
    let catalog = |zones: &[VirtualZone]| -> BTreeMap<String, String> {
        zones
            .iter()
            .map(|zone| (zone.zone_id.clone(), zone.name.clone()))
            .collect()
    };
    catalog(prev) != catalog(next)
}

fn refresh_linked(zones: &RefCell<Vec<VirtualZone>>, linked: &RefCell<BTreeSet<String>>) -> bool {
    let ids: Vec<String> = zones.borrow().iter().map(|z| z.zone_id.clone()).collect();
    if ids.is_empty() {
        let mut current = linked.borrow_mut();
        if current.is_empty() {
            return false;
        }
        current.clear();
        return true;
    }
    let Ok(dump) = crate::virtual_output::dump_text() else {
        return false;
    };
    let now = crate::virtual_output::linked_zone_ids(&dump, &ids);
    let mut current = linked.borrow_mut();
    if *current == now {
        return false;
    }
    *current = now;
    true
}

fn zones_have_sinks(zones: &[ZoneEndpoints]) -> bool {
    zones.iter().any(|z| !z.endpoints.is_empty())
}

/// Результат обработки SetEndpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetEndpointsOutcome {
    /// набор и dscp совпали с применёнными — пересборка не нужна.
    Unchanged,
    /// транспорт изменён — Runner остановлен, пересборка вооружена.
    Changed,
}

/// Обработка CtlMsg::SetEndpoints — вызывается ТОЛЬКО для tx-половины
/// (узлы без неё фильтруются в gst_domain). Дедуп по эффективному транспорту
/// как прежде: два пустых набора равны независимо от dscp (см. Transport).
fn handle_set_endpoints(
    transport: &RefCell<Transport>,
    zones: Vec<ZoneEndpoints>,
    dscp: u8,
    half: &mut RoleHalf,
) -> SetEndpointsOutcome {
    {
        let t = transport.borrow();
        let unchanged = if !zones_have_sinks(&t.zones) && !zones_have_sinks(&zones) {
            t.zones == zones
        } else {
            t.zones == zones && t.dscp == dscp
        };
        if unchanged {
            return SetEndpointsOutcome::Unchanged;
        }
    }
    {
        let mut t = transport.borrow_mut();
        t.zones = zones;
        t.dscp = dscp;
    }
    drop_runner(&mut half.runner);
    half.gate.arm(&mut half.policy);
    SetEndpointsOutcome::Changed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetCaptureTargetOutcome {
    Unchanged,
    Changed,
}

fn handle_set_capture_target(
    current: &RefCell<Option<String>>,
    device_key: Option<String>,
    half: &mut RoleHalf,
) -> SetCaptureTargetOutcome {
    if current.borrow().as_deref() == device_key.as_deref() {
        return SetCaptureTargetOutcome::Unchanged;
    }
    *current.borrow_mut() = device_key;
    drop_runner(&mut half.runner);
    half.gate.arm(&mut half.policy);
    SetCaptureTargetOutcome::Changed
}

/// Результат обработки SetChannelMap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetChannelMapOutcome {
    /// карта совпала с применённой — пересборка не нужна.
    Unchanged,
    /// карта изменена — Runner остановлен, пересборка вооружена.
    Changed,
}

/// Обработка CtlMsg::SetChannelMap — вызывается ТОЛЬКО для rx-половины
/// (узлы без неё фильтруются в gst_domain). Дедуп как в handle_set_endpoints:
/// та же карта → no-op; изменённая → Runner дропается, пересборка вооружается
/// по политике этой половины (BuildAndStart при want_playing — решает гейт
/// в цикле через take_kind).
fn branch_pairs(branch_maps: &RefCell<Vec<BranchChannelMap>>) -> Vec<(u16, ChannelMap)> {
    branch_maps
        .borrow()
        .iter()
        .map(|b| (b.port, b.map))
        .collect()
}

fn canon_branches(mut branches: Vec<BranchChannelMap>) -> Vec<BranchChannelMap> {
    branches.sort_by_key(|b| b.port);
    branches.dedup_by_key(|b| b.port);
    branches
}

fn handle_set_channel_map(
    chan_map: &RefCell<ChannelMap>,
    branch_maps: &RefCell<Vec<BranchChannelMap>>,
    map: ChannelMap,
    branches: Vec<BranchChannelMap>,
    half: &mut RoleHalf,
) -> SetChannelMapOutcome {
    let branches = canon_branches(branches);
    if *chan_map.borrow() == map && *branch_maps.borrow() == branches {
        return SetChannelMapOutcome::Unchanged;
    }
    *chan_map.borrow_mut() = map;
    *branch_maps.borrow_mut() = branches;
    drop_runner(&mut half.runner);
    half.gate.arm(&mut half.policy);
    SetChannelMapOutcome::Changed
}

struct MixPorts {
    ports: Vec<u16>,
    jb_ms: u32,
}

impl Default for MixPorts {
    fn default() -> Self {
        Self { ports: Vec::new(), jb_ms: 2 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetMixOutcome {
    Unchanged,
    Reconciled,
    Rebuild,
}

fn canon_ports(mut ports: Vec<u16>) -> Vec<u16> {
    ports.sort();
    ports.dedup();
    ports
}

/// SetMix перешивает ветки приёмника, не роняя выход. Нет runner или
/// перешивка не удалась — как SetChannelMap: дроп и сборка заново.
/// Без активного линка порты только запоминаются: `udpsrc` не открываем.
fn handle_set_mix(
    mix: &RefCell<MixPorts>,
    branch_maps: &RefCell<Vec<BranchChannelMap>>,
    ports: Vec<u16>,
    half: &mut RoleHalf,
    jb_ms: u32,
    link: Option<&crate::uplink::ActiveLink>,
) -> SetMixOutcome {
    let ports = canon_ports(ports);
    let (ports_same, jb_same) = {
        let cur = mix.borrow();
        (cur.ports == ports, cur.jb_ms == jb_ms)
    };
    if ports_same && jb_same {
        return SetMixOutcome::Unchanged;
    }
    {
        let mut cur = mix.borrow_mut();
        cur.ports = ports.clone();
        cur.jb_ms = jb_ms;
    }
    // Только запас: latency на стоящих jb{port}. Runner не роняем —
    // перешивка портов здесь не нужна и не должна уводить в Rebuild.
    if ports_same {
        if let Some(runner) = half.runner.as_ref() {
            runner.set_jitter_latency(&ports, jb_ms);
        }
        return SetMixOutcome::Reconciled;
    }
    if let Some(runner) = half.runner.as_ref() {
        let branches: Vec<(u16, ChannelMap)> = branch_maps
            .borrow()
            .iter()
            .map(|b| (b.port, b.map))
            .collect();
        let live: &[u16] = if link.is_some() { &ports } else { &[] };
        let owned = link.cloned();
        let mut bind = pipeline_bind(&owned);
        match runner.reconcile_mix(live, jb_ms, &branches, bind.as_mut()) {
            Ok(()) => return SetMixOutcome::Reconciled,
            Err(e) => tracing::warn!("SetMix: перешивка не удалась, пересобираем rx: {e:#}"),
        }
    }
    drop_runner(&mut half.runner);
    half.gate.arm(&mut half.policy);
    SetMixOutcome::Rebuild
}

/// Боевой биндер, когда линк опубликован. Без линка в сборку уходит пустой
/// набор: `tx`/`rx` не вызываются, `LoopbackUdp` только занимает аргумент.
fn pipeline_bind(link: &Option<crate::uplink::ActiveLink>) -> Box<dyn crate::pipeline::LocalUdp> {
    match link {
        Some(link) => Box::new(crate::pipeline::IfaceUdp {
            name: link.name.clone(),
            ipv4: link.ipv4,
        }),
        None => Box::new(crate::pipeline::LoopbackUdp),
    }
}

/// Sendspin и Soloist повторяют имя опубликованного интерфейса.
/// `None` гасит слоты Sendspin. Пустое имя и следующий `sync` гасят Soloist.
fn follow_link(
    link: &Option<crate::uplink::ActiveLink>,
    players: &mut crate::sendspin::Players,
    sendspin_zones: &[VirtualZone],
    soloist: &mut crate::soloist::Receivers,
    soloist_key: &Option<String>,
    soloist_zones: &[VirtualZone],
) {
    let name = link.as_ref().map(|item| item.name.as_str());
    players.sync(sendspin_zones, name);
    soloist.set_iface(name.unwrap_or(""));
    soloist.sync(soloist_key.as_deref(), soloist_zones);
}

/// Одна роль-половина gst-домена: свой Runner, PlayGate и RestartPolicy —
/// падение/пересборка одной половины не трогает другую (спека §5).
struct RoleHalf {
    runner: Option<Runner>,
    gate: PlayGate,
    policy: RestartPolicy,
}

impl RoleHalf {
    fn new() -> Self {
        Self {
            runner: None,
            gate: PlayGate::default(),
            policy: RestartPolicy::new(RESTART_MAX_FAST),
        }
    }
}

/// Start одной половины: want_playing → старт собранного пайплайна; если
/// пайплайна нет (первичная сборка упала / Failed во время Stop) — вооружаем
/// сборку по политике, want_playing уже true.
fn start_half(role: Role, h: &mut RoleHalf) {
    h.gate.on_start();
    match &h.runner {
        Some(r) if r.state() == PipelineState::Playing => {}
        Some(r) => {
            if let Err(e) = r.start() {
                tracing::warn!(?role, "start() не удался: {e:#}");
                drop_runner(&mut h.runner);
                h.gate.arm(&mut h.policy);
            }
        }
        None => {
            tracing::warn!(
                ?role,
                "Start без собранного пайплайна — собираем по политике"
            );
            h.gate.arm(&mut h.policy);
        }
    }
}

/// Stop одной половины: want_playing снят (рестарты отменены), пайплайн в Null.
fn stop_half(h: &mut RoleHalf) {
    h.gate.on_stop();
    if let Some(r) = &h.runner {
        r.stop();
    }
}

/// Sup-событие одной половины: Failed → событие контроллеру + дроп Runner +
/// рестарт по гейту (вооружается только при want_playing); Underrun → событие.
fn handle_sup_event(
    half: &mut RoleHalf,
    role: Role,
    ev: SupEvent,
    events: &mpsc::Sender<AgentMsg>,
) {
    match ev {
        SupEvent::Failed(detail) => {
            tracing::warn!(?role, "пайплайн упал: {detail}");
            emit(
                events,
                AgentMsg::Event {
                    kind: EventKind::PipelineError,
                    detail: format!("{role:?}: {detail}"),
                },
            );
            drop_runner(&mut half.runner);
            half.gate.on_failed(&mut half.policy);
        }
        SupEvent::Underrun => {
            tracing::warn!(?role, "underrun");
            emit(
                events,
                AgentMsg::Event {
                    kind: EventKind::Underrun,
                    detail: format!("{role:?}: underrun"),
                },
            );
        }
    }
}

/// Разрешение ролей узла. Явный список из конфига: дедуп, канонический
/// порядок [Tx, Rx], валидация устройств (tx требует capture_device, rx —
/// playback_device, кроме playback_backend=pipewire). None — авто-детект
/// probe'ом: устройство открывается в нужном режиме → роль поднимается
/// (rx с pipewire пробуется без устройства). Пустой итог — Err (агенту
/// нечего делать).
pub(crate) fn resolve_roles(
    cfg: &AgentConfig,
    probe: &dyn Fn(&str, Role) -> bool,
) -> Result<Vec<Role>> {
    let canonical = |mut roles: Vec<Role>| -> Vec<Role> {
        roles.sort_by_key(|r| match r {
            Role::Tx => 0,
            Role::Rx => 1,
        });
        roles.dedup();
        roles
    };
    match &cfg.roles {
        Some(list) => {
            let roles = canonical(list.clone());
            anyhow::ensure!(
                !roles.is_empty(),
                "roles задан, но пуст — укажи хотя бы одну роль или убери поле для авто-детекта"
            );
            for r in &roles {
                match r {
                    Role::Tx => anyhow::ensure!(
                        cfg.capture_device.is_some(),
                        "roles содержит tx, но capture_device не задан"
                    ),
                    Role::Rx => anyhow::ensure!(
                        cfg.playback_backend != crate::config::Backend::Alsa
                            || cfg.playback_device.is_some(),
                        "roles содержит rx, но playback_device не задан"
                    ),
                }
            }
            Ok(roles)
        }
        None => {
            let mut roles = Vec::new();
            if let Some(dev) = &cfg.capture_device {
                if probe(dev, Role::Tx) {
                    roles.push(Role::Tx);
                } else {
                    tracing::warn!("авто-детект: capture {dev} недоступен — роль tx пропущена");
                }
            }
            if cfg.playback_device.is_some()
                || cfg.playback_backend == crate::config::Backend::PipeWire
            {
                let dev = cfg.playback_device.as_deref().unwrap_or("pipewire");
                if probe(dev, Role::Rx) {
                    roles.push(Role::Rx);
                } else {
                    tracing::warn!("авто-детект: playback {dev} недоступен — роль rx пропущена");
                }
            }
            let roles = canonical(roles);
            anyhow::ensure!(
                !roles.is_empty(),
                "авто-детект не нашёл ни одной рабочей роли (capture_device/playback_device; \
                 для rx с playback_backend=\"pipewire\" устройство не нужно — \
                 проверь, что PipeWire запущен и агент видит его сокет, см. deploy/README.md)"
            );
            Ok(roles)
        }
    }
}

/// Проба устройства вывода/захвата через gst. rx с
/// playback_backend=PipeWire пробуется pipewiresink'ом (устройство не
/// задаётся — выход выбирает WirePlumber); иначе ALSA-проба: alsasrc (tx) /
/// alsasink (rx) переводится в Ready — устройство существует и не занято.
/// Ошибка probe'а выключает роль (спека §8); явный список roles в конфиге —
/// способ отключить ошибочно детектируемую роль.
pub(crate) fn device_probe(cfg: &AgentConfig, device: &str, role: Role) -> bool {
    use gstreamer::prelude::*;
    let _ = gstreamer::init();
    if role == Role::Rx && cfg.playback_backend == crate::config::Backend::PipeWire {
        let Ok(el) = gstreamer::ElementFactory::make("pipewiresink").build() else {
            return false;
        };
        let ok = el.set_state(gstreamer::State::Ready).is_ok();
        let _ = el.set_state(gstreamer::State::Null);
        return ok;
    }
    let factory = match role {
        Role::Tx => "alsasrc",
        Role::Rx => "alsasink",
    };
    let Ok(el) = gstreamer::ElementFactory::make(factory)
        .property("device", device)
        .build()
    else {
        return false;
    };
    let ok = el.set_state(gstreamer::State::Ready).is_ok();
    let _ = el.set_state(gstreamer::State::Null);
    ok
}

/// gst-домен: poll-цикл (команды ← recv_timeout(TICK), sup-события try_recv,
/// телеметрия раз в TELEM_PERIOD). До двух половин: tx (alsasrc → сеть) и
/// rx (сеть → alsasink); у каждой свои Runner/PlayGate/RestartPolicy/sup-канал.
/// Все вызовы Runner — только отсюда.
fn gst_domain(
    cfg: AgentConfig,
    roles: Vec<Role>,
    telem_tx: watch::Sender<Telemetry>,
    ptp_offset: watch::Receiver<i64>,
    cmd_rx: std_mpsc::Receiver<CtlMsg>,
    events: mpsc::Sender<AgentMsg>,
    hardware_inputs: Vec<CaptureChan>,
    mut uplink: watch::Receiver<Option<crate::uplink::ActiveLink>>,
) {
    if let Err(e) = gstreamer::init() {
        tracing::warn!("gstreamer::init: {e}");
    }
    // uptime_s в телеметрии — возраст gst-домена (= процесса агента);
    // по-ролевой uptime при двух пайплайнах не определён.
    let started = Instant::now();

    let mut tx_half = roles.contains(&Role::Tx).then(RoleHalf::new);
    let mut rx_half = roles.contains(&Role::Rx).then(RoleHalf::new);

    // Sup-каналы раздельные: Failed одной половины не перезапускает другую.
    let (sup_tx_tx, sup_rx_tx) = std_mpsc::channel::<SupEvent>();
    let (sup_tx_rx, sup_rx_rx) = std_mpsc::channel::<SupEvent>();

    // Транспортное состояние (unicast-эндпоинты + dscp) — только tx-половина.
    let transport = RefCell::new(Transport::default());
    let capture_target: RefCell<Option<String>> = RefCell::new(None);
    let mut cables = crate::virtual_output::Cables::default();
    let virtual_zones: RefCell<Vec<VirtualZone>> = RefCell::new(Vec::new());
    let linked: RefCell<BTreeSet<String>> = RefCell::new(BTreeSet::new());
    let mut soloist = crate::soloist::Receivers::default();
    let soloist_active = soloist.active_handle();
    let table: RefCell<crate::sendspin::ZoneTable> =
        RefCell::new(crate::sendspin::ZoneTable::new());
    // Рантайм забирает отправителя Note и получатель External. Вторая пара не нужна.
    let (external_tx, external_rx) = std_mpsc::sync_channel::<crate::sendspin::External>(32);
    let (notes_tx, notes_rx) = std_mpsc::channel::<crate::sendspin::Note>();
    let mut players = crate::sendspin::Players::new(notes_tx, external_rx);
    let mut sendspin_zones: Vec<VirtualZone> = Vec::new();
    let mut soloist_key: Option<String> = None;
    let mut soloist_zones: Vec<VirtualZone> = Vec::new();
    // Текущая публикация. Стартуем с того, что уже в канале: клон приёмника
    // помечает это значение просмотренным, has_changed ловит только следующее.
    let link_cell: RefCell<Option<crate::uplink::ActiveLink>> =
        RefCell::new(uplink.borrow_and_update().clone());
    follow_link(
        &link_cell.borrow(),
        &mut players,
        &sendspin_zones,
        &mut soloist,
        &soloist_key,
        &soloist_zones,
    );
    // Источники, с которыми tx собран после последней успешной сборки (не предикат пересборки).
    let built_sources: RefCell<BTreeMap<String, Option<String>>> = RefCell::new(BTreeMap::new());
    // Карта источников на прошлом кадре опроса; пересборка — только при отличии от текущего кадра.
    let mut previous_sources: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut next_links = Instant::now();
    // Применённая матрица зон (ChannelMap) — только rx-половина.
    // branches — карта на порт, когда у зоны несколько источников.
    let chan_map = RefCell::new(ChannelMap::default());
    let branch_maps: RefCell<Vec<BranchChannelMap>> = RefCell::new(Vec::new());
    // Порты источников, которые эта колонка складывает. Пусто — тишина.
    let mix_ports = RefCell::new(MixPorts::default());

    // Фабрики Runner'ов — через closure, чтобы рестарт-путь можно было
    // покрыть тестами без ALSA (подставляем fakesink-пайплайн).
    let make_tx = || -> Result<Runner> {
        let link = link_cell.borrow().clone();
        let r = {
            let t = transport.borrow();
            // Нет линка — пустой набор, как пустой SetEndpoints. Живые зоны в transport не стираем.
            let silent = link.is_none();
            match cfg.playback_backend {
                crate::config::Backend::Alsa => {
                    let device = cfg
                        .capture_device
                        .as_deref()
                        .context("tx: capture_device не задан")?;
                    let zones: &[ZoneEndpoints] = if silent { &[] } else { &t.zones };
                    let mut bind = pipeline_bind(&link);
                    Runner::new_tx(&cfg.pipeline, device, zones, t.dscp, bind.as_mut())?
                }
                // Тот же бэкенд, что у rx: PipeWire не отдаёт ALSA захвату от root.
                crate::config::Backend::PipeWire => {
                    pipewire_socket_ready()?;
                    let target = {
                        let chosen = capture_target.borrow().clone();
                        chosen.or_else(|| {
                            cfg.capture_device
                                .as_deref()
                                .and_then(crate::pipeline::configured_pipewire_node)
                                .map(str::to_string)
                        })
                    };
                    let mut bind = pipeline_bind(&link);
                    if silent {
                        *built_sources.borrow_mut() = BTreeMap::new();
                        Runner::new_tx_pw(
                            &cfg.pipeline,
                            &[],
                            t.dscp,
                            target.as_deref(),
                            bind.as_mut(),
                        )?
                    } else {
                        let linked_now = linked.borrow();
                        let table_now = table.borrow();
                        let zones = overlay_sources(&t.zones, &linked_now, &table_now);
                        let sources = zone_sources(&t.zones, &linked_now, &table_now);
                        let runner = Runner::new_tx_pw(
                            &cfg.pipeline,
                            &zones,
                            t.dscp,
                            target.as_deref(),
                            bind.as_mut(),
                        )?;
                        *built_sources.borrow_mut() = sources;
                        runner
                    }
                }
            }
        };
        r.spawn_bus_watch(sup_tx_tx.clone());
        Ok(r)
    };
    let make_rx = || -> Result<Runner> {
        let link = link_cell.borrow().clone();
        let (stored, jb_ms) = {
            let mix = mix_ports.borrow();
            (mix.ports.clone(), mix.jb_ms)
        };
        // Запас зоны, не константа pipeline.jb_ms: пересборка после тишины
        // и после SetChannelMap собирает ветки с последним SetMix.
        let mut pipeline = cfg.pipeline.clone();
        pipeline.jb_ms = jb_ms;
        // Нет линка — без udpsrc. Порты в mix_ports остаются на следующий Some.
        let ports: &[u16] = if link.is_some() { &stored } else { &[] };
        let branches = branch_pairs(&branch_maps);
        let mut bind = pipeline_bind(&link);
        let r = match cfg.playback_backend {
            crate::config::Backend::Alsa => {
                let device = cfg
                    .playback_device
                    .as_deref()
                    .context("rx: playback_device не задан")?;
                Runner::new_rx(
                    &pipeline,
                    device,
                    &chan_map.borrow(),
                    ports,
                    &branches,
                    bind.as_mut(),
                )?
            }
            crate::config::Backend::PipeWire => {
                pipewire_socket_ready()?;
                let sink = cfg
                    .playback_device
                    .as_deref()
                    .and_then(crate::pipeline::configured_pipewire_node);
                Runner::new_rx_pw(
                    &pipeline,
                    &chan_map.borrow(),
                    sink,
                    ports,
                    &branches,
                    bind.as_mut(),
                )?
            }
        };
        r.spawn_bus_watch(sup_tx_rx.clone());
        Ok(r)
    };

    // Первичная сборка каждой половины (без autostart — запуск по Start).
    // При ошибке — ретраи по политике этой половины, вторая живёт (спека §8).
    if let Some(h) = tx_half.as_mut() {
        if let Err(e) = build(&make_tx, &mut h.runner) {
            tracing::error!("tx: сборка пайплайна: {e:#}");
            h.gate.arm(&mut h.policy);
        }
    }
    if let Some(h) = rx_half.as_mut() {
        if let Err(e) = build(&make_rx, &mut h.runner) {
            tracing::error!("rx: сборка пайплайна: {e:#}");
            h.gate.arm(&mut h.policy);
        }
    }

    let mut next_telem = Instant::now();

    loop {
        // Sup-события дрейним ДО команд: Stop в командной фазе того же тика
        // обнуляет restart_at, иначе Failed, лежавший в sup_rx одновременно
        // со Stop, перевооружил бы рестарт уже после команды (гонка ≤ TICK).
        while let Ok(ev) = sup_rx_tx.try_recv() {
            if let Some(h) = tx_half.as_mut() {
                handle_sup_event(h, Role::Tx, ev, &events);
            }
        }
        while let Ok(ev) = sup_rx_rx.try_recv() {
            if let Some(h) = rx_half.as_mut() {
                handle_sup_event(h, Role::Rx, ev, &events);
            }
        }

        match cmd_rx.recv_timeout(TICK) {
            Ok(CtlMsg::Start) => {
                // v1: Start/Stop действуют на обе половины разом (спека §5).
                if let Some(h) = tx_half.as_mut() {
                    start_half(Role::Tx, h);
                }
                if let Some(h) = rx_half.as_mut() {
                    start_half(Role::Rx, h);
                }
            }
            Ok(CtlMsg::Stop) => {
                if let Some(h) = tx_half.as_mut() {
                    stop_half(h);
                }
                if let Some(h) = rx_half.as_mut() {
                    stop_half(h);
                }
            }
            Ok(CtlMsg::SetParam { .. }) => {
                tracing::warn!("SetParam: не поддерживается в v1, игнорируем")
            }
            Ok(CtlMsg::SetEndpoints { zones, dscp }) => match tx_half.as_mut() {
                None => tracing::debug!("SetEndpoints: у узла нет tx-половины — игнорируем"),
                Some(h) => match handle_set_endpoints(&transport, zones, dscp, h) {
                    SetEndpointsOutcome::Unchanged => tracing::debug!(
                        "SetEndpoints: транспорт не изменился — пересборка не нужна"
                    ),
                    SetEndpointsOutcome::Changed => {
                        tracing::info!("SetEndpoints: транспорт изменён — пересборка tx-пайплайна")
                    }
                },
            },
            Ok(CtlMsg::SetMix { ports, jb_ms }) => match rx_half.as_mut() {
                None => tracing::debug!("SetMix: у узла нет rx-половины — игнорируем"),
                Some(h) => {
                    let link = link_cell.borrow().clone();
                    match handle_set_mix(
                        &mix_ports,
                        &branch_maps,
                        ports,
                        h,
                        jb_ms,
                        link.as_ref(),
                    ) {
                        SetMixOutcome::Unchanged => {
                            tracing::debug!("SetMix: порты и запас не изменились")
                        }
                        SetMixOutcome::Reconciled => {
                            tracing::info!("SetMix: ветки перешиты, выход на месте")
                        }
                        SetMixOutcome::Rebuild => {
                            tracing::info!("SetMix: пересборка rx-пайплайна")
                        }
                    }
                }
            },
            Ok(CtlMsg::SetChannelMap { map, branches }) => match rx_half.as_mut() {
                None => tracing::debug!("SetChannelMap: у узла нет rx-половины — игнорируем"),
                Some(h) => {
                    match handle_set_channel_map(&chan_map, &branch_maps, map, branches, h) {
                        SetChannelMapOutcome::Unchanged => tracing::debug!(
                            "SetChannelMap: карта не изменилась — пересборка не нужна"
                        ),
                        SetChannelMapOutcome::Changed => {
                            tracing::info!(
                                "SetChannelMap: карта изменена — пересборка rx-пайплайна"
                            )
                        }
                    }
                }
            },
            Ok(CtlMsg::ControllerMoved { .. }) => {
                // Сюда не доходит: ws_client перехватывает команду у себя.
                // Арм — страховка на случай будущих изменений маршрутизации.
                tracing::warn!(
                    "ControllerMoved в gst-домене: должен перехватываться ws_client — игнорируем"
                )
            }
            Ok(CtlMsg::BecomeMaster { .. }) => {
                // Сюда не доходит: bridge async → gst перехватывает раньше.
                // Арм — страховка на случай будущих изменений маршрутизации.
                tracing::warn!(
                    "BecomeMaster в gst-домене: должен перехватываться bridge — игнорируем"
                )
            }
            Ok(CtlMsg::DisableController) => {
                // Сюда не доходит: bridge async → gst перехватывает раньше.
                tracing::warn!(
                    "DisableController в gst-домене: должен перехватываться bridge — игнорируем"
                )
            }
            Ok(CtlMsg::SetVirtualOutput { enabled }) => {
                if tx_half.is_none() {
                    tracing::debug!("SetVirtualOutput: нет tx — игнорируем");
                } else if !enabled {
                    cables.clear();
                    virtual_zones.borrow_mut().clear();
                    let had = !linked.borrow().is_empty();
                    linked.borrow_mut().clear();
                    let advertised = crate::virtual_output::advertise(&hardware_inputs, None);
                    if events
                        .try_send(AgentMsg::Inputs { inputs: advertised })
                        .is_err()
                    {
                        tracing::warn!("Inputs не отправлены");
                    }
                    if had {
                        if let Some(h) = tx_half.as_mut() {
                            drop_runner(&mut h.runner);
                            h.gate.arm(&mut h.policy);
                        }
                    }
                }
            }
            Ok(CtlMsg::SetVirtualZones { zones }) => {
                if tx_half.is_none() {
                    tracing::debug!("SetVirtualZones: нет tx — игнорируем");
                } else {
                    let advertised = match cables.sync(&zones) {
                        Ok(channels) => {
                            let extra = if channels.is_empty() {
                                None
                            } else {
                                Some(channels)
                            };
                            crate::virtual_output::advertise(&hardware_inputs, extra.as_deref())
                        }
                        Err(e) => {
                            tracing::warn!("виртуальный выход: {e:#}");
                            crate::virtual_output::advertise(&hardware_inputs, None)
                        }
                    };
                    *virtual_zones.borrow_mut() = zones;
                    if let Some(h) = tx_half.as_mut() {
                        if refresh_linked(&virtual_zones, &linked) {
                            tracing::info!("tx: виртуальный выход занял зону, пересборка");
                            drop_runner(&mut h.runner);
                            h.gate.arm(&mut h.policy);
                        }
                    }
                    if events
                        .try_send(AgentMsg::Inputs { inputs: advertised })
                        .is_err()
                    {
                        tracing::warn!("Inputs не отправлены");
                    }
                }
            }
            Ok(CtlMsg::SetSoloist { api_key, zones }) => {
                if tx_half.is_none() {
                    tracing::debug!("SetSoloist: нет tx — игнорируем");
                } else {
                    soloist_key = api_key.as_ref().map(|key| key.0.clone());
                    soloist_zones = zones.clone();
                    let name = link_cell
                        .borrow()
                        .as_ref()
                        .map(|link| link.name.clone())
                        .unwrap_or_default();
                    soloist.set_iface(&name);
                    soloist.sync(soloist_key.as_deref(), &zones);
                }
            }
            Ok(CtlMsg::SetSendspin { zones }) => {
                if tx_half.is_none() {
                    tracing::debug!("SetSendspin: нет tx — игнорируем");
                } else {
                    let changed = sendspin_zones_changed(&sendspin_zones, &zones);
                    table.borrow_mut().sync(&zones);
                    let iface = link_cell.borrow().as_ref().map(|link| link.name.clone());
                    players.sync(&zones, iface.as_deref());
                    sendspin_zones = zones;
                    if changed {
                        if let Some(h) = tx_half.as_mut() {
                            tracing::info!("tx: зоны Sendspin изменились, пересборка");
                            drop_runner(&mut h.runner);
                            h.gate.arm(&mut h.policy);
                        }
                    }
                }
            }
            Ok(CtlMsg::SetCaptureTarget { device_key }) => match tx_half.as_mut() {
                None => tracing::debug!("SetCaptureTarget: нет tx — игнорируем"),
                Some(h) => {
                    if handle_set_capture_target(&capture_target, device_key, h)
                        == SetCaptureTargetOutcome::Changed
                    {
                        tracing::info!("tx: цель захвата изменилась, пересборка");
                    }
                }
            },
            Ok(CtlMsg::ApplyUpdate { .. }) => {
                tracing::warn!("ApplyUpdate в gst-домене — игнорируем")
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }

        while let Ok(crate::sendspin::Note::Stream(zone_id, wire)) = notes_rx.try_recv() {
            let effect = table.borrow_mut().on_wire(&zone_id, wire);
            if effect.deactivate {
                soloist.request_deactivate(&zone_id);
            }
        }

        let poll_links = cfg.playback_backend == crate::config::Backend::PipeWire
            && Instant::now() >= next_links
            && (!virtual_zones.borrow().is_empty()
                || soloist.has_zones()
                || !table.borrow().zone_ids().is_empty());
        if poll_links {
            next_links = Instant::now() + Duration::from_secs(1);
            if let Some(h) = tx_half.as_mut() {
                let links_changed =
                    !virtual_zones.borrow().is_empty() && refresh_linked(&virtual_zones, &linked);

                let active = soloist_active.lock().map(|g| g.clone()).unwrap_or_default();
                let effects = table.borrow_mut().set_spotify(&active);
                for effect in &effects {
                    if effect.enter_external {
                        let _ = external_tx
                            .try_send(crate::sendspin::External::Enter(effect.zone_id.clone()));
                    }
                    if effect.exit_external {
                        let _ = external_tx
                            .try_send(crate::sendspin::External::Exit(effect.zone_id.clone()));
                    }
                }
                let deactivate: Vec<String> = {
                    let t = table.borrow();
                    t.zone_ids()
                        .into_iter()
                        .filter(|zone_id| {
                            t.holder(zone_id) == crate::sendspin::Holder::Sendspin
                                && active.contains(zone_id)
                        })
                        .collect()
                };
                for zone_id in deactivate {
                    soloist.request_deactivate(&zone_id);
                }

                let source_map_changed = {
                    let t = transport.borrow();
                    let linked_now = linked.borrow();
                    let table_now = table.borrow();
                    let desired = zone_sources(&t.zones, &linked_now, &table_now);
                    let changed = sources_changed(&previous_sources, &desired);
                    previous_sources = desired;
                    changed
                };
                if links_changed || source_map_changed {
                    let zones = {
                        let t = transport.borrow();
                        let linked_now = linked.borrow();
                        let table_now = table.borrow();
                        overlay_sources(&t.zones, &linked_now, &table_now)
                    };
                    // Пересборка заново открывает RTP. Колонка тогда теряет
                    // остальные источники, а микрофон выпадает из потока.
                    let mixed = match h.runner.as_mut() {
                        Some(runner) => match runner.reconcile_monitors(&zones) {
                            Ok(()) => true,
                            Err(e) => {
                                tracing::warn!(
                                    "tx: монитор в живой микс не встал, пересобираем: {e:#}"
                                );
                                false
                            }
                        },
                        None => false,
                    };
                    if mixed {
                        tracing::info!("tx: монитор зоны в миксе с line-in, RTP на месте");
                    } else {
                        tracing::info!("tx: источник зоны сменился, пересборка");
                        drop_runner(&mut h.runner);
                        h.gate.arm(&mut h.policy);
                    }
                }
            }
        }

        // Смена публикации: запоминаем линк и пересобираем половины.
        // transport и mix_ports не трогаем — следующий Some соберёт их на новом сокете.
        // borrow/has_changed работают в std-потоке gst-домена, await здесь нет.
        if uplink.has_changed().unwrap_or(false) {
            let next = uplink.borrow_and_update().clone();
            match &next {
                Some(link) => tracing::info!("uplink: звук на {} ({})", link.name, link.ipv4),
                None => {
                    tracing::info!("uplink: активного интерфейса нет — tx молчит, rx без udpsrc")
                }
            }
            follow_link(
                &next,
                &mut players,
                &sendspin_zones,
                &mut soloist,
                &soloist_key,
                &soloist_zones,
            );
            *link_cell.borrow_mut() = next;
            if let Some(h) = tx_half.as_mut() {
                drop_runner(&mut h.runner);
                h.gate.arm(&mut h.policy);
            }
            if let Some(h) = rx_half.as_mut() {
                drop_runner(&mut h.runner);
                h.gate.arm(&mut h.policy);
            }
        }

        if let Some(h) = tx_half.as_mut() {
            if h.gate.due() {
                let kind = h.gate.take_kind();
                match run_restart(&make_tx, &mut h.runner, kind) {
                    Ok(()) => {
                        h.policy = RestartPolicy::new(RESTART_MAX_FAST);
                        if kind == RestartKind::BuildAndStart {
                            tracing::info!("tx: пайплайн перезапущен");
                            emit(
                                &events,
                                AgentMsg::Event {
                                    kind: EventKind::Restarted,
                                    detail: "tx pipeline restarted".into(),
                                },
                            );
                        } else {
                            tracing::info!("tx: пайплайн пересобран (без старта — ждём Start)");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("tx: перезапуск пайплайна не удался: {e:#}");
                        h.gate.arm(&mut h.policy);
                    }
                }
            }
        }
        if let Some(h) = rx_half.as_mut() {
            if h.gate.due() {
                let kind = h.gate.take_kind();
                match run_restart(&make_rx, &mut h.runner, kind) {
                    Ok(()) => {
                        h.policy = RestartPolicy::new(RESTART_MAX_FAST);
                        if kind == RestartKind::BuildAndStart {
                            tracing::info!("rx: пайплайн перезапущен");
                            emit(
                                &events,
                                AgentMsg::Event {
                                    kind: EventKind::Restarted,
                                    detail: "rx pipeline restarted".into(),
                                },
                            );
                        } else {
                            tracing::info!("rx: пайплайн пересобран (без старта — ждём Start)");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("rx: перезапуск пайплайна не удался: {e:#}");
                        h.gate.arm(&mut h.policy);
                    }
                }
            }
        }

        if Instant::now() >= next_telem {
            next_telem = Instant::now() + TELEM_PERIOD;
            let _ = telem_tx.send(Telemetry {
                tx: tx_half.as_ref().map(|h| tx_telemetry(&h.runner)),
                rx: rx_half.as_ref().map(|h| rx_telemetry(&h.runner)),
                ptp_offset_ns: *ptp_offset.borrow(),
                uptime_s: started.elapsed().as_secs(),
            });
        }
    }

    if let Some(h) = &tx_half {
        if let Some(r) = &h.runner {
            r.stop();
        }
    }
    if let Some(h) = &rx_half {
        if let Some(r) = &h.runner {
            r.stop();
        }
    }
    tracing::info!("gst-домен завершён");
}

/// Собирает Runner через фабрику и ставит в слот (без старта).
fn build(make_runner: &dyn Fn() -> Result<Runner>, slot: &mut Option<Runner>) -> Result<()> {
    *slot = Some(make_runner()?);
    Ok(())
}

/// Сокет пользовательской сессии PipeWire. Агент стартует с multi-user.target,
/// раньше входа в сессию: без сокета pipewiresrc уходит в READY и падает при сносе.
fn pipewire_socket_ready() -> Result<()> {
    let dir = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/run/user/1000".into());
    let sock = std::path::PathBuf::from(dir).join("pipewire-0");
    if sock.exists() {
        Ok(())
    } else {
        anyhow::bail!("pipewire ещё не слушает {}", sock.display());
    }
}

/// Собирает Runner заново и запускает (рестарт после Failed при want_playing).
fn build_and_start(
    make_runner: &dyn Fn() -> Result<Runner>,
    slot: &mut Option<Runner>,
) -> Result<()> {
    let r = make_runner()?;
    if let Err(e) = r.start() {
        r.stop();
        return Err(e);
    }
    *slot = Some(r);
    Ok(())
}

/// Рестарт по решению PlayGate: BuildAndStart — собрать и играть (после
/// явного Start), BuildOnly — только собрать (пайплайн ждёт Start).
fn run_restart(
    make_runner: &dyn Fn() -> Result<Runner>,
    slot: &mut Option<Runner>,
    kind: RestartKind,
) -> Result<()> {
    match kind {
        RestartKind::BuildOnly => build(make_runner, slot),
        RestartKind::BuildAndStart => build_and_start(make_runner, slot),
    }
}

fn drop_runner(slot: &mut Option<Runner>) {
    if let Some(r) = slot.take() {
        r.stop();
    }
}

/// Отправка события контроллеру. gst-домен не блокируется: при переполнении
/// канала событие дропается с warn (контроллер не критичен для воспроизведения).
fn emit(events: &mpsc::Sender<AgentMsg>, msg: AgentMsg) {
    use mpsc::error::TrySendError::*;
    if let Err(e) = events.try_send(msg) {
        match e {
            Full(_) => tracing::warn!("событие не отправлено: канал событий переполнен"),
            Closed(_) => tracing::warn!("событие не отправлено: канал событий закрыт"),
        }
    }
}

/// Телеметрия tx-половины. Несобранный пайплайн (None) → state=Error:
/// роль есть в наборе, но пайплайн не жив — это и есть ошибочное состояние.
pub(crate) fn tx_telemetry(slot: &Option<Runner>) -> RoleTx {
    match slot {
        Some(r) => {
            let s = r.stats();
            RoleTx {
                state: r.state(),
                rms_l: s.rms_l,
                rms_r: s.rms_r,
            }
        }
        None => RoleTx {
            state: PipelineState::Error,
            rms_l: 0.0,
            rms_r: 0.0,
        },
    }
}

/// Телеметрия rx-половины. queue_ms=-1 у None — «нет данных», как прежде.
pub(crate) fn rx_telemetry(slot: &Option<Runner>) -> RoleRx {
    match slot {
        Some(r) => {
            let s = r.stats();
            RoleRx {
                state: r.state(),
                dropped: s.dropped,
                rendered: s.rendered,
                queue_ms: s.queue_ms,
            }
        }
        None => RoleRx {
            state: PipelineState::Error,
            dropped: 0,
            rendered: 0,
            queue_ms: -1,
        },
    }
}

/// Стартовое значение watch-канала телеметрии: половины по набору ролей в
/// Stopped, ptp — «нет данных» (i64::MAX) до первого тика zsync.
pub(crate) fn initial_telemetry(roles: &[Role]) -> Telemetry {
    Telemetry {
        tx: roles.contains(&Role::Tx).then_some(RoleTx {
            state: PipelineState::Stopped,
            rms_l: 0.0,
            rms_r: 0.0,
        }),
        rx: roles.contains(&Role::Rx).then_some(RoleRx {
            state: PipelineState::Stopped,
            dropped: 0,
            rendered: 0,
            queue_ms: -1,
        }),
        ptp_offset_ns: i64::MAX,
        uptime_s: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gstreamer::prelude::*;
    use zproto::{ChanSel, Endpoint};

    #[test]
    fn overlay_reads_the_zone_owner() {
        let zones = vec![zproto::ZoneEndpoints {
            zone_id: "hall".into(),
            endpoints: vec![],
            source: None,
        }];
        let mut table = crate::sendspin::ZoneTable::new();
        table.sync(&[zproto::VirtualZone {
            zone_id: "hall".into(),
            name: "Зал".into(),
        }]);
        table.on_wire("hall", crate::sendspin::Wire::StreamStart);
        let linked = BTreeSet::new();
        let out = overlay_sources(&zones, &linked, &table);
        assert_eq!(
            out[0].source.as_deref(),
            Some("zvucast-sendspin-hall-monitor")
        );
    }

    #[test]
    fn sources_changed() {
        let sendspin = |src: &str| {
            let mut m = BTreeMap::new();
            m.insert("hall".into(), Some(src.into()));
            m
        };
        let frame_s = sendspin("zvucast-sendspin-hall-monitor");
        let frame_m = sendspin("spotify-monitor");

        let mut previous = frame_s.clone();
        assert!(super::sources_changed(&previous, &frame_m));
        previous = frame_m.clone();
        assert!(super::sources_changed(&previous, &frame_s));
        previous = frame_s.clone();
        assert!(super::sources_changed(&previous, &frame_m));
    }

    fn cfg_for_mixed() -> AgentConfig {
        AgentConfig {
            name: "test".into(),
            id: None,
            roles: None,
            controller: "ws://127.0.0.1:9/ws".into(),
            capture_device: Some("plughw:CARD=Cap".into()),
            playback_device: Some("plughw:CARD=Play".into()),
            playback_backend: crate::config::Backend::Alsa,
            sync_interface: "end0".into(),
            ptp_priority: 128,
            pipeline: crate::config::PipelineCfg::default_rx(),
        }
    }

    #[test]
    fn resolve_roles_autodetect_uses_probe() {
        let cfg = cfg_for_mixed();
        // probe «видит» только playback — поднимается только rx
        let roles = resolve_roles(&cfg, &|_, role| role == Role::Rx).unwrap();
        assert_eq!(roles, vec![Role::Rx]);
        // probe видит всё — обе роли, порядок канонический [Tx, Rx]
        let roles = resolve_roles(&cfg, &|_, _| true).unwrap();
        assert_eq!(roles, vec![Role::Tx, Role::Rx]);
        // probe не видит ничего — старт запрещён
        assert!(resolve_roles(&cfg, &|_, _| false).is_err());
    }

    #[test]
    fn resolve_roles_explicit_validates_devices() {
        let mut cfg = cfg_for_mixed();
        cfg.roles = Some(vec![Role::Tx]);
        cfg.capture_device = None;
        let e = resolve_roles(&cfg, &|_, _| true).unwrap_err();
        assert!(
            e.to_string().contains("capture_device"),
            "tx без capture_device — ошибка: {e}"
        );

        let mut cfg = cfg_for_mixed();
        cfg.roles = Some(vec![Role::Rx, Role::Rx, Role::Tx]);
        let roles = resolve_roles(&cfg, &|_, _| true).unwrap();
        assert_eq!(
            roles,
            vec![Role::Tx, Role::Rx],
            "дедуп + канонический порядок"
        );

        let mut cfg = cfg_for_mixed();
        cfg.roles = Some(vec![]);
        assert!(
            resolve_roles(&cfg, &|_, _| true).is_err(),
            "пустой список — ошибка"
        );
    }

    #[test]
    fn resolve_roles_pipewire_skips_device_requirement() {
        let mut cfg = cfg_for_mixed();
        cfg.roles = Some(vec![Role::Rx]);
        cfg.playback_device = None;
        cfg.playback_backend = crate::config::Backend::PipeWire;
        let roles = resolve_roles(&cfg, &|_, _| true).unwrap();
        assert_eq!(roles, vec![Role::Rx]);
    }

    #[test]
    fn resolve_roles_pipewire_autodetect_without_device() {
        let mut cfg = cfg_for_mixed();
        cfg.roles = None;
        cfg.capture_device = None;
        cfg.playback_device = None;
        cfg.playback_backend = crate::config::Backend::PipeWire;
        let roles = resolve_roles(&cfg, &|_, role| role == Role::Rx).unwrap();
        assert_eq!(roles, vec![Role::Rx]);
    }

    #[test]
    fn telemetry_split_by_role() {
        let err = rx_telemetry(&None);
        assert_eq!(err.state, PipelineState::Error);
        assert_eq!(err.queue_ms, -1);
        let err = tx_telemetry(&None);
        assert_eq!(err.state, PipelineState::Error);

        let t = initial_telemetry(&[Role::Tx, Role::Rx]);
        assert!(t.tx.is_some() && t.rx.is_some());
        assert_eq!(t.tx.as_ref().unwrap().state, PipelineState::Stopped);
        assert_eq!(t.rx.as_ref().unwrap().state, PipelineState::Stopped);
        assert_eq!(
            t.ptp_offset_ns,
            i64::MAX,
            "нет данных от zsync до первого тика"
        );
        assert_eq!(t.uptime_s, 0);
        let only_tx = initial_telemetry(&[Role::Tx]);
        assert!(only_tx.rx.is_none(), "нет роли rx — половина null");
    }

    /// Тестовая фабрика Runner'ов без ALSA: audiotestsrc → fakesink
    /// (как sine_pipeline в supervisor). gst::init идемпотентен.
    fn fakesink_runner() -> Result<Runner> {
        gstreamer::init().unwrap();
        let pipe =
            gstreamer::parse::launch("audiotestsrc ! audio/x-raw,rate=48000,channels=2 ! fakesink")
                .unwrap()
                .downcast::<gstreamer::Pipeline>()
                .unwrap();
        Ok(Runner::from_pipeline_for_test(pipe))
    }

    /// set_state(Playing) — асинхронный переход (preroll): поллим state,
    /// как stats_flow_for_test_pipeline в supervisor.
    fn wait_state(r: &Runner, want: PipelineState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while r.state() != want && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(r.state(), want);
    }

    /// Ревью: Failed после Stop НЕ должен возвращать пайплайн в Playing —
    /// рестарт не вооружается (want_playing == false после Stop).
    #[test]
    fn stop_then_failed_does_not_restart() {
        let mut gate = PlayGate::default();
        let mut policy = RestartPolicy::new(RESTART_MAX_FAST);
        let mut slot = Some(fakesink_runner().unwrap());

        gate.on_start();
        slot.as_ref().unwrap().start().unwrap();
        wait_state(slot.as_ref().unwrap(), PipelineState::Playing);

        gate.on_stop();
        // Error на шине после Stop (гонка teardown ≤ BUS_WATCH_POLL)
        gate.on_failed(&mut policy);
        assert!(
            gate.restart_at.is_none(),
            "Failed после Stop не вооружает рестарт — иначе пайплайн вернётся в Playing"
        );
        assert!(!gate.due());

        // поведение цикла: Runner дропаем, слот пуст, рестарт не планируется
        drop_runner(&mut slot);
        assert!(slot.is_none());
        assert!(
            !gate.due(),
            "рестарт так и не вооружён — в Playing не вернёмся"
        );
    }

    /// Ревью: Failed при want_playing (юзер ждёт звук) вооружает рестарт,
    /// который собирает пайплайн и возвращает его в Playing.
    #[test]
    fn failed_with_want_playing_restarts_to_playing() {
        let mut gate = PlayGate::default();
        let mut policy = RestartPolicy::new(RESTART_MAX_FAST);
        let mut slot = Some(fakesink_runner().unwrap());

        gate.on_start();
        slot.as_ref().unwrap().start().unwrap();
        gate.on_failed(&mut policy);
        assert!(
            gate.restart_at.is_some(),
            "Failed при want_playing вооружает рестарт"
        );

        drop_runner(&mut slot);
        let deadline = Instant::now() + Duration::from_secs(2);
        while !gate.due() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            gate.due(),
            "рестарт назрел (первые N попыток — без задержки)"
        );

        let kind = gate.take_kind();
        assert_eq!(kind, RestartKind::BuildAndStart);
        run_restart(&fakesink_runner, &mut slot, kind).unwrap();
        wait_state(slot.as_ref().unwrap(), PipelineState::Playing);
        drop_runner(&mut slot);
    }

    /// Ревью (а): ретрай первичной сборки (ALSA занят при старте агента)
    /// не запускает пайплайн сам — сборка без старта, пока нет Start.
    #[test]
    fn build_retry_without_start_stays_stopped() {
        let mut gate = PlayGate::default();
        let mut policy = RestartPolicy::new(RESTART_MAX_FAST);
        let mut slot: Option<Runner> = None;

        // имитация упавшей первичной сборки: ретрай вооружён, Start не было
        gate.arm(&mut policy);
        let kind = gate.take_kind();
        assert_eq!(
            kind,
            RestartKind::BuildOnly,
            "без Start рестарт — только сборка"
        );
        run_restart(&fakesink_runner, &mut slot, kind).unwrap();
        assert_eq!(
            slot.as_ref().unwrap().state(),
            PipelineState::Stopped,
            "автостарта при ретрае сборки нет — ждём Start"
        );
        drop_runner(&mut slot);
    }

    /// Start при несобранном пайплайне (первичная сборка упала / Failed во
    /// время Stop) вооружает сборку, которая запускается сама —
    /// want_playing уже true (мёртвой ветки «ждём рестарт» нет).
    #[test]
    fn start_with_no_runner_builds_and_plays() {
        let mut gate = PlayGate::default();
        let mut policy = RestartPolicy::new(RESTART_MAX_FAST);
        let mut slot: Option<Runner> = None;

        gate.on_start();
        gate.arm(&mut policy); // ветка Start без runner в gst_domain
        let kind = gate.take_kind();
        assert_eq!(kind, RestartKind::BuildAndStart);
        run_restart(&fakesink_runner, &mut slot, kind).unwrap();
        wait_state(slot.as_ref().unwrap(), PipelineState::Playing);
        drop_runner(&mut slot);
    }

    /// Тестовый Runner без ALSA: зеркалит выбор транспорта
    /// Runner::new_tx (всегда tee: пустой набор — веток нет, тишина;
    /// иначе queue→udpsink на каждый эндпоинт), но вместо alsasrc —
    /// audiotestsrc, чтобы ветку можно было довести до Playing.
    fn transport_runner(t: &Transport) -> Runner {
        let pipe = gstreamer::Pipeline::new();
        let src = gstreamer::ElementFactory::make("audiotestsrc")
            .build()
            .unwrap();
        let tee = gstreamer::ElementFactory::make("tee").build().unwrap();
        pipe.add_many([&src, &tee]).unwrap();
        gstreamer::Element::link_many([&src, &tee]).unwrap();
        for ep in t.zones.iter().flat_map(|z| &z.endpoints) {
            let queue = gstreamer::ElementFactory::make("queue").build().unwrap();
            let sink = gstreamer::ElementFactory::make("udpsink")
                .property("host", &ep.ip)
                .property("port", ep.port as i32)
                .property("qos-dscp", t.dscp as i32)
                .build()
                .unwrap();
            pipe.add_many([&queue, &sink]).unwrap();
            tee.link(&queue).unwrap();
            queue.link(&sink).unwrap();
        }
        Runner::from_pipeline_for_test(pipe)
    }

    fn udpsink_hosts(r: &Runner) -> Vec<String> {
        let children = r.pipeline().children();
        let mut hosts: Vec<String> = children
            .iter()
            .filter(|e| e.factory().unwrap().name() == "udpsink")
            .map(|s| s.property::<String>("host"))
            .collect();
        hosts.sort();
        hosts
    }

    /// SetEndpoints пересобирает только tx-половину: у rx-половины свои
    /// runner/gate/policy — ни остановки, ни вооружения пересборки.
    #[test]
    fn set_endpoints_touches_only_tx_half() {
        gstreamer::init().unwrap();
        let transport = RefCell::new(Transport::default());
        let mut tx_half = RoleHalf::new();
        let mut rx_half = RoleHalf::new();

        tx_half.gate.on_start();
        run_restart(
            &fakesink_runner,
            &mut tx_half.runner,
            RestartKind::BuildAndStart,
        )
        .unwrap();
        rx_half.gate.on_start();
        run_restart(
            &fakesink_runner,
            &mut rx_half.runner,
            RestartKind::BuildAndStart,
        )
        .unwrap();
        wait_state(rx_half.runner.as_ref().unwrap(), PipelineState::Playing);

        let out = handle_set_endpoints(
            &transport,
            vec![ZoneEndpoints {
                zone_id: "зал".into(),
                endpoints: vec![Endpoint {
                    ip: "192.0.2.36".into(),
                    port: 5004,
                }],
                source: None,
            }],
            40,
            &mut tx_half,
        );
        assert_eq!(out, SetEndpointsOutcome::Changed);
        assert!(tx_half.gate.due(), "tx пересобирается");
        assert!(!rx_half.gate.due(), "rx не вооружает пересборку");
        assert!(rx_half.runner.is_some(), "rx-runner на месте");
        wait_state(rx_half.runner.as_ref().unwrap(), PipelineState::Playing);

        drop_runner(&mut tx_half.runner);
        drop_runner(&mut rx_half.runner);
    }

    /// Stop от контроллера действует на обе половины разом (v1): want_playing
    /// снят, пайплайны остановлены, запланированные рестарты отменены.
    #[test]
    fn stop_stops_both_halves() {
        gstreamer::init().unwrap();
        let mut tx_half = RoleHalf::new();
        let mut rx_half = RoleHalf::new();
        for h in [&mut tx_half, &mut rx_half] {
            h.gate.on_start();
            run_restart(&fakesink_runner, &mut h.runner, RestartKind::BuildAndStart).unwrap();
            wait_state(h.runner.as_ref().unwrap(), PipelineState::Playing);
        }
        for h in [&mut tx_half, &mut rx_half] {
            stop_half(h);
        }
        for h in [&tx_half, &rx_half] {
            assert!(!h.gate.want_playing);
            assert!(!h.gate.due(), "Stop отменяет запланированный рестарт");
            assert_eq!(h.runner.as_ref().unwrap().state(), PipelineState::Stopped);
        }
        drop_runner(&mut tx_half.runner);
        drop_runner(&mut rx_half.runner);
    }

    /// tx: SetEndpoints меняет пайплайн только при реальном изменении набора
    /// или dscp. Пересборка — играющая (want_playing сохранён), пустой набор
    /// — тишина: tee без веток, udpsink нет.
    #[test]
    fn set_endpoints_rebuilds_only_on_real_change() {
        gstreamer::init().unwrap();
        let transport = RefCell::new(Transport::default());
        let builds = std::cell::Cell::new(0usize);
        let mut half = RoleHalf::new();

        let make_runner = || -> Result<Runner> {
            builds.set(builds.get() + 1);
            Ok(transport_runner(&transport.borrow()))
        };

        half.gate.on_start();
        run_restart(&make_runner, &mut half.runner, RestartKind::BuildAndStart).unwrap();
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);
        assert_eq!(builds.get(), 1);

        let eps = |ips: &[&str]| {
            vec![ZoneEndpoints {
                zone_id: "зал".into(),
                endpoints: ips
                    .iter()
                    .map(|ip| Endpoint {
                        ip: (*ip).into(),
                        port: 5004,
                    })
                    .collect(),
                source: None,
            }]
        };
        let eps1 = eps(&["192.0.2.36", "192.0.2.99"]);

        handle_set_endpoints(&transport, eps1.clone(), 24, &mut half);
        assert!(half.gate.due(), "изменение транспорта вооружает пересборку");
        let kind = half.gate.take_kind();
        assert_eq!(kind, RestartKind::BuildAndStart, "want_playing сохранён");
        run_restart(&make_runner, &mut half.runner, kind).unwrap();
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);
        assert_eq!(builds.get(), 2);
        let mut want: Vec<String> = eps1[0].endpoints.iter().map(|e| e.ip.clone()).collect();
        want.sort();
        assert_eq!(udpsink_hosts(half.runner.as_ref().unwrap()), want);

        // повторный идентичный набор — дедупликация, пересборки нет
        handle_set_endpoints(&transport, eps1.clone(), 24, &mut half);
        assert_eq!(builds.get(), 2, "идентичный набор не пересобирает");
        assert!(!half.gate.due());
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);

        // изменился только dscp — пересборка
        handle_set_endpoints(&transport, eps1.clone(), 40, &mut half);
        assert!(half.gate.due());
        run_restart(&make_runner, &mut half.runner, half.gate.take_kind()).unwrap();
        assert_eq!(builds.get(), 3);

        // пустой набор → тишина: tee без веток, udpsink нет
        handle_set_endpoints(&transport, vec![], 40, &mut half);
        run_restart(&make_runner, &mut half.runner, half.gate.take_kind()).unwrap();
        assert_eq!(builds.get(), 4);
        assert!(udpsink_hosts(half.runner.as_ref().unwrap()).is_empty());

        drop_runner(&mut half.runner);
    }

    /// Свежий агент ({[], 0} по умолчанию) + пуш SetEndpoints{[], 40} —
    /// дедупликация по эффективному транспорту: branchless-сборка dscp не
    /// применяет, пересборки нет. Непустой набор пересобирает как обычно.
    #[test]
    fn set_endpoints_empty_set_ignores_dscp() {
        gstreamer::init().unwrap();
        let transport = RefCell::new(Transport::default());
        let builds = std::cell::Cell::new(0usize);
        let mut half = RoleHalf::new();

        let make_runner = || -> Result<Runner> {
            builds.set(builds.get() + 1);
            Ok(transport_runner(&transport.borrow()))
        };

        half.gate.on_start();
        run_restart(&make_runner, &mut half.runner, RestartKind::BuildAndStart).unwrap();
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);
        assert_eq!(builds.get(), 1);

        let out = handle_set_endpoints(&transport, vec![], 40, &mut half);
        assert_eq!(out, SetEndpointsOutcome::Unchanged);
        assert!(
            !half.gate.due(),
            "dscp при пустом наборе не вооружает пересборку"
        );
        assert_eq!(builds.get(), 1);

        let out = handle_set_endpoints(&transport, vec![], 0, &mut half);
        assert_eq!(out, SetEndpointsOutcome::Unchanged);
        let out = handle_set_endpoints(&transport, vec![], 40, &mut half);
        assert_eq!(out, SetEndpointsOutcome::Unchanged);
        assert!(!half.gate.due());
        assert_eq!(builds.get(), 1, "пересборок так и не было");
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);

        let out = handle_set_endpoints(
            &transport,
            vec![ZoneEndpoints {
                zone_id: "зал".into(),
                endpoints: vec![Endpoint {
                    ip: "192.0.2.36".into(),
                    port: 5004,
                }],
                source: None,
            }],
            40,
            &mut half,
        );
        assert_eq!(out, SetEndpointsOutcome::Changed);
        assert!(half.gate.due(), "появился эндпоинт — пересборка вооружена");

        drop_runner(&mut half.runner);
    }

    /// Тестовый Runner без ALSA: зеркалит чтение карты в make_rx — имя
    /// sink'а кодирует применённую карту, чтобы тест видел, что пересборка
    /// подхватила именно новую карту (как udpsink_hosts для транспорта).
    fn channel_map_runner(cm: &ChannelMap) -> Runner {
        let sink_name = format!("sink-{:?}-{:?}", cm.out_l, cm.out_r);
        let pipe = gstreamer::Pipeline::new();
        let src = gstreamer::ElementFactory::make("audiotestsrc")
            .build()
            .unwrap();
        let sink = gstreamer::ElementFactory::make("fakesink")
            .name(sink_name.as_str())
            .build()
            .unwrap();
        pipe.add_many([&src, &sink]).unwrap();
        gstreamer::Element::link_many([&src, &sink]).unwrap();
        Runner::from_pipeline_for_test(pipe)
    }

    fn fakesink_name(r: &Runner) -> String {
        r.pipeline()
            .children()
            .iter()
            .find(|e| e.factory().unwrap().name() == "fakesink")
            .unwrap()
            .name()
            .to_string()
    }

    /// SetChannelMap с изменённой картой пересобирает rx-половину по паттерну
    /// SetEndpoints: runner дропается, пересборка вооружена; want_playing
    /// сохранён → BuildAndStart; пайплайн собирается уже с новой картой.
    #[test]
    fn set_channel_map_rebuilds_rx() {
        gstreamer::init().unwrap();
        let chan_map = RefCell::new(ChannelMap::default());
        let branch_maps = RefCell::new(Vec::new());
        let builds = std::cell::Cell::new(0usize);
        let mut half = RoleHalf::new();

        let make_runner = || -> Result<Runner> {
            builds.set(builds.get() + 1);
            Ok(channel_map_runner(&chan_map.borrow()))
        };

        half.gate.on_start();
        run_restart(&make_runner, &mut half.runner, RestartKind::BuildAndStart).unwrap();
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);
        assert_eq!(builds.get(), 1);

        // карта «только L» отличается от дефолта {L, R} → пересборка
        let map_l = ChannelMap {
            out_l: Some(ChanSel::L),
            out_r: None,
        };
        let out = handle_set_channel_map(&chan_map, &branch_maps, map_l, Vec::new(), &mut half);
        assert_eq!(out, SetChannelMapOutcome::Changed);
        assert!(
            half.runner.is_none(),
            "runner дропнут — пересборка по гейту"
        );
        assert!(half.gate.due(), "изменение карты вооружает пересборку");
        let kind = half.gate.take_kind();
        assert_eq!(kind, RestartKind::BuildAndStart, "want_playing сохранён");
        run_restart(&make_runner, &mut half.runner, kind).unwrap();
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);
        assert_eq!(builds.get(), 2);
        assert_eq!(
            fakesink_name(half.runner.as_ref().unwrap()),
            "sink-Some(L)-None",
            "пересборка подхватила новую карту"
        );

        drop_runner(&mut half.runner);
    }

    /// Тот же набор портов и другой запас: latency на jb{port}, runner жив.
    #[test]
    fn same_ports_new_jb_ms_keeps_the_runner() {
        gstreamer::init().unwrap();
        let mut cfg = crate::config::PipelineCfg::default_rx();
        cfg.port = 17670;
        let pipe = crate::pipeline::build_rx_ports(
            &cfg,
            "null",
            &ChannelMap::default(),
            &[cfg.port],
            &[],
            &mut crate::pipeline::LoopbackUdp,
        )
        .unwrap();
        let mut half = RoleHalf::new();
        half.runner = Some(Runner::from_pipeline_for_test(pipe));
        let mix = RefCell::new(MixPorts { ports: vec![cfg.port], jb_ms: 2 });
        let branch_maps = RefCell::new(Vec::new());
        let out = handle_set_mix(&mix, &branch_maps, vec![cfg.port], &mut half, 40, None);
        assert_eq!(out, SetMixOutcome::Reconciled);
        assert!(half.runner.is_some(), "смена только запаса не роняет runner");
        assert!(!half.gate.due(), "пересборка не вооружается");
        let jb = half
            .runner
            .as_ref()
            .unwrap()
            .pipeline()
            .by_name(&format!("jb{}", cfg.port))
            .unwrap();
        assert_eq!(jb.property::<u32>("latency"), 40);
        assert_eq!(mix.borrow().jb_ms, 40);
        let again = handle_set_mix(&mix, &branch_maps, vec![cfg.port], &mut half, 40, None);
        assert_eq!(again, SetMixOutcome::Unchanged);
        drop_runner(&mut half.runner);
    }

    /// Та же карта — дедуп, как в handle_set_endpoints: runner не трогается,
    /// пересборка не вооружается. Отличная от применённой карта пересобирает.
    #[test]
    fn same_channel_map_is_noop() {
        gstreamer::init().unwrap();
        let chan_map = RefCell::new(ChannelMap::default());
        let branch_maps = RefCell::new(Vec::new());
        let builds = std::cell::Cell::new(0usize);
        let mut half = RoleHalf::new();

        let make_runner = || -> Result<Runner> {
            builds.set(builds.get() + 1);
            Ok(channel_map_runner(&chan_map.borrow()))
        };

        half.gate.on_start();
        run_restart(&make_runner, &mut half.runner, RestartKind::BuildAndStart).unwrap();
        wait_state(half.runner.as_ref().unwrap(), PipelineState::Playing);
        assert_eq!(builds.get(), 1);

        // дефолтная карта == применённой → no-op
        let out = handle_set_channel_map(
            &chan_map,
            &branch_maps,
            ChannelMap::default(),
            Vec::new(),
            &mut half,
        );
        assert_eq!(out, SetChannelMapOutcome::Unchanged);
        assert!(!half.gate.due(), "пересборка не вооружена");
        assert_eq!(builds.get(), 1, "пересборки не было");
        assert_eq!(
            half.runner.as_ref().unwrap().state(),
            PipelineState::Playing,
            "играющий runner не тронут"
        );

        // карта изменилась — пересборка вооружается (контраст с дедупом)
        let map_l = ChannelMap {
            out_l: Some(ChanSel::L),
            out_r: None,
        };
        let out = handle_set_channel_map(&chan_map, &branch_maps, map_l, Vec::new(), &mut half);
        assert_eq!(out, SetChannelMapOutcome::Changed);
        assert!(half.gate.due());

        drop_runner(&mut half.runner);
    }

    // --- Task 10: трансфер/claim мастера. ---

    #[test]
    fn become_master_writes_state_marker_and_enables_controller() {
        let dir = std::env::temp_dir().join(format!("zvbm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        struct FakeCtl(std::sync::Mutex<bool>);
        impl MasterCtl for FakeCtl {
            fn enable_controller(&self) -> anyhow::Result<()> {
                *self.0.lock().unwrap() = true;
                Ok(())
            }
            fn disable_controller(&self) -> anyhow::Result<()> {
                *self.0.lock().unwrap() = false;
                Ok(())
            }
        }
        let ctl = FakeCtl(false.into());
        let state = dir.join("state.json");
        become_master_to(&state, "{\"version\":1}", &ctl).unwrap();
        assert!(std::fs::read_to_string(&state).unwrap().contains("version"));
        assert!(*ctl.0.lock().unwrap());
        // маркер преемника — контроллер при старте пропустит discovery-проверку
        assert!(
            successor_marker_path(&state).is_file(),
            "маркер преемника записан"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Откал трансфера (502 на старом мастере): маркер снят, контроллер
    /// выключен — иначе при поздней смерти старого мастера узел стартанул бы
    /// сам, некоординированно, со stale-state.
    #[test]
    fn disable_master_removes_marker_and_disables_controller() {
        let dir = std::env::temp_dir().join(format!("zvbm-off-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        struct FakeCtl(std::sync::Mutex<bool>);
        impl MasterCtl for FakeCtl {
            fn enable_controller(&self) -> anyhow::Result<()> {
                *self.0.lock().unwrap() = true;
                Ok(())
            }
            fn disable_controller(&self) -> anyhow::Result<()> {
                *self.0.lock().unwrap() = false;
                Ok(())
            }
        }
        let ctl = FakeCtl(true.into());
        let state = dir.join("state.json");
        std::fs::write(successor_marker_path(&state), b"").unwrap();
        disable_master_to(&state, &ctl).unwrap();
        assert!(!successor_marker_path(&state).exists(), "маркер снят");
        assert!(!*ctl.0.lock().unwrap(), "disable_controller вызван");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Минимальный HTTP/1.1-клиент для stub-тестов: `Connection: close` —
    /// сервер закрывает соединение после ответа, read_to_end завершается.
    async fn http(port: u16, method: &str, path: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        let status: u16 = head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        (status, body.to_string())
    }

    /// Stub-impl MasterCtl: фиксирует факт enable_controller в разделяемом флаге.
    #[derive(Clone)]
    struct StubCtl {
        enabled: Arc<std::sync::Mutex<bool>>,
    }
    impl MasterCtl for StubCtl {
        fn enable_controller(&self) -> anyhow::Result<()> {
            *self.enabled.lock().unwrap() = true;
            Ok(())
        }
        fn disable_controller(&self) -> anyhow::Result<()> {
            *self.enabled.lock().unwrap() = false;
            Ok(())
        }
    }

    async fn serve_stub(
        state_path: &std::path::Path,
        flag: Arc<AtomicBool>,
        ctl: Arc<dyn MasterCtl>,
        discovery_dest: String,
        uplink: watch::Receiver<Option<crate::uplink::ActiveLink>>,
    ) -> u16 {
        let app = stub_app(state_path, flag, ctl, discovery_dest, uplink);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        port
    }

    /// Предпроверка claim (review, Important #1): отвечающий мастер (fake
    /// responder) → Some(addr) — claim следует отклонить; тишина → None.
    #[tokio::test]
    async fn claim_precheck_detects_live_master() {
        let bind = "127.0.0.1:15105";
        let task = tokio::spawn(crate::discovery::responder(bind));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (_uplink_tx, uplink) = watch::channel(Some(crate::uplink::ActiveLink {
            name: "lo".into(),
            ipv4: std::net::Ipv4Addr::LOCALHOST,
            wireless: false,
        }));
        let got = claim_precheck(bind, Duration::from_secs(1), &uplink)
            .await
            .unwrap();
        assert_eq!(got.as_deref(), Some("ws://127.0.0.1:8080/ws"));
        task.abort();

        // тишина (мертвый порт) → None
        let got = claim_precheck("127.0.0.1:15199", Duration::from_millis(150), &uplink)
            .await
            .unwrap();
        assert!(got.is_none());
    }

    /// Stub-сервер агента: страница «мастер не найден», статус (ws_connected)
    /// и claim — при живом мастере нет, claim пишет default-state и включает ctl.
    #[tokio::test]
    async fn stub_page_status_and_claim() {
        let dir = std::env::temp_dir().join(format!("zvstub-a-{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("state.json");
        std::fs::remove_file(&path).ok();
        let flag = Arc::new(AtomicBool::new(false));
        let enabled = Arc::new(std::sync::Mutex::new(false));
        // discovery молчит (мертвый порт) — claim должен пройти
        let port = serve_stub(
            &path,
            flag,
            Arc::new(StubCtl {
                enabled: enabled.clone(),
            }),
            "127.0.0.1:15199".to_string(),
            watch::channel(Some(crate::uplink::ActiveLink {
                name: "lo".into(),
                ipv4: std::net::Ipv4Addr::LOCALHOST,
                wireless: false,
            }))
            .1,
        )
        .await;

        // GET / — страница «мастер не найден»
        let (status, body) = http(port, "GET", "/").await;
        assert_eq!(status, 200);
        assert!(body.contains("Мастер не найден"), "страница: {body}");

        // GET /api/master/status → ws_connected=false
        let (status, body) = http(port, "GET", "/api/master/status").await;
        assert_eq!(status, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["ws_connected"], false);

        // claim → 200: default-state записан, контроллер «включен»
        let (status, _) = http(port, "POST", "/api/master/claim").await;
        assert_eq!(status, 200);
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert!(
            on_disk.contains("\"version\""),
            "state после claim: {on_disk}"
        );
        assert!(*enabled.lock().unwrap(), "enable_controller вызван");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Мастер жив (WS к нему подключён) — claim отклонён: 409, state не
    /// тронут, контроллер не включён.
    #[tokio::test]
    async fn stub_claim_conflicts_when_master_alive() {
        let dir = std::env::temp_dir().join(format!("zvstub-b-{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("state.json");
        std::fs::remove_file(&path).ok();
        let flag = Arc::new(AtomicBool::new(true)); // WS к мастеру жив
        let enabled = Arc::new(std::sync::Mutex::new(false));
        let port = serve_stub(
            &path,
            flag,
            Arc::new(StubCtl {
                enabled: enabled.clone(),
            }),
            "127.0.0.1:15199".to_string(),
            watch::channel(None).1,
        )
        .await;

        let (status, body) = http(port, "POST", "/api/master/claim").await;
        assert_eq!(status, 409);
        assert!(body.contains("мастер"), "текст 409: {body}");
        assert!(!path.exists(), "state не записан");
        assert!(!*enabled.lock().unwrap(), "enable_controller не вызван");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Split-brain окно (review, Important #1): ws_connected=false (например
    /// backoff-сон до 30 с), но мастер в сети жив — discovery-ответ есть.
    /// Claim обязан быть отклонён: 409, state не тронут, ctl не включён.
    #[tokio::test]
    async fn stub_claim_rejected_when_discovery_finds_master() {
        let dir = std::env::temp_dir().join(format!("zvstub-c-{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("state.json");
        std::fs::remove_file(&path).ok();
        // fake-мастер: discovery responder отвечает на предпроверку
        let responder = tokio::spawn(crate::discovery::responder("127.0.0.1:15106"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let flag = Arc::new(AtomicBool::new(false));
        let enabled = Arc::new(std::sync::Mutex::new(false));
        let port = serve_stub(
            &path,
            flag,
            Arc::new(StubCtl {
                enabled: enabled.clone(),
            }),
            "127.0.0.1:15106".to_string(),
            watch::channel(Some(crate::uplink::ActiveLink {
                name: "lo".into(),
                ipv4: std::net::Ipv4Addr::LOCALHOST,
                wireless: false,
            }))
            .1,
        )
        .await;

        let (status, body) = http(port, "POST", "/api/master/claim").await;
        assert_eq!(status, 409);
        assert!(body.contains("мастер жив"), "текст 409: {body}");
        assert!(!path.exists(), "state не записан");
        assert!(!*enabled.lock().unwrap(), "enable_controller не вызван");

        responder.abort();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn capture_target_change_drops_the_runner_once() {
        gstreamer::init().unwrap();
        let target = RefCell::new(None);
        let mut half = RoleHalf::new();
        half.runner = Some(Runner::from_pipeline_for_test(gstreamer::Pipeline::new()));
        assert_eq!(
            handle_set_capture_target(&target, None, &mut half),
            SetCaptureTargetOutcome::Unchanged
        );
        assert!(half.runner.is_some());
        assert_eq!(
            handle_set_capture_target(&target, Some("zvucast".into()), &mut half),
            SetCaptureTargetOutcome::Changed
        );
        assert!(half.runner.is_none());
        assert_eq!(target.borrow().as_deref(), Some("zvucast"));
    }
}
