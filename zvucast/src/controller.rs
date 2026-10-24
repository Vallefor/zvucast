//! Контроллер: HTTP API + WS-сервер для агентов. Держит Registry и StateStore
//! (зоны, мета узлов), принимает телеметрию/события по WS, мультиплексирует
//! команды (Start/Stop) и пушит SetEndpoints/SetMix/SetChannelMap; зонный пушер
//! дополнительно гоняет Start/Stop по diff «играющего множества» и факту
//! re-hello узлов.
//! Task 10: трансфер роли мастера online-узлу (POST /api/master/transfer) и
//! статус мастера (GET /api/master).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path as FsPath;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::rejection::JsonRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Json, Path, Request, State};
use axum::http::{header, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Mutex};
use zproto::{AgentMsg, BranchChannelMap, ChannelMap, CtlMsg, Endpoint, Role, SecretString, VirtualZone, ZoneEndpoints};

use crate::agent::successor_marker_path;
use crate::discovery::{self, CONTROLLER_PORT, DISCOVERY_PORT};
use crate::uplink::ActiveLink;
use crate::registry::{NodeView, Registry, RegistryError};
use crate::silence::{classify, fold_samples, Sample, SilenceBook, ZoneTick};
use crate::state::{
    assignment, default_jb_ms, owner_ids, owner_maps, source_channel_maps, zone_groups,
    State as MasterState, StateStore, Zone,
};

/// Телеметрия старше этого таймаута → узел в offline по sweep.
const SWEEP_TIMEOUT: Duration = Duration::from_secs(5);

/// Дебаунс пуша SetEndpoints: серия изменений реестра (hello/offline) должна
/// схлопнуться в один пуш, а не гнать штурм команд.
const PUSH_DEBOUNCE: Duration = Duration::from_secs(1);

/// DSCP по умолчанию для RTP-потока (CS5 — voice-admitted; см. spec DSCP).
const DEFAULT_DSCP: u8 = 40;

/// Ожидание поднятия контроллера на новом мастере после трансфера
/// (20 × 500 мс — квант опроса wait_controller_up).
const TRANSFER_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Продакшен-конфиг. Контроллер только читает `sync_interface`, файл не переписывает.
pub const CONFIG_PATH: &str = "/etc/zvucast.toml";

/// Пока конфиг не читается, веб и респондер не поднимаются.
const CONFIG_RETRY: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct AppState {
    reg: Arc<Mutex<Registry>>,
    dscp: Arc<AtomicU8>,
    store: Arc<Mutex<StateStore>>,
    /// Счётчик мутаций state — второй триггер pusher'а (первый — версия
    /// реестра). Бампится в `update_state` после каждой записи.
    state_version: watch::Sender<u64>,
    /// Собственный listen-адрес — для GET /api/master. run() подставляет
    /// реальный; конструкторы дают дефолт.
    listen_addr: String,
    /// Локальный стоп контроллера после успешного трансфера. По умолчанию —
    /// `systemctl disable --now` через 500 мс в отдельном потоке (чтобы ответ
    /// REST успел уйти). Тесты подменяют заглушкой: systemd не трогаем.
    post_transfer_stop: Arc<dyn Fn() + Send + Sync>,
    /// Ожидание поднятия контроллера на цели трансфера. По умолчанию
    /// TRANSFER_WAIT_TIMEOUT; тесты уменьшают, чтобы не ждать 10 с.
    transfer_wait: Duration,
    /// Id зон в холде тишины. Пишет тикер, читают пушер и zone_views.
    /// Замок не брать вместе со `store` или `reg`: сначала клон, потом следующий.
    silence_holds: Arc<Mutex<HashSet<String>>>,
}

/// Счётчик для уникальных путей эфемерного state в `AppState::new`.
static EPHEMERAL_SEQ: AtomicU64 = AtomicU64::new(0);

impl AppState {
    /// State в одноразовом temp-файле — для тестов, где state не важен.
    pub fn new() -> Self {
        let n = EPHEMERAL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join(format!("zvucast-ephemeral-{}-{n}.json", std::process::id()));
        Self::with_state(&path)
    }

    /// Загружает state из `path` (нет файла — default) и предзаливает мету
    /// узлов (display_name, channel_map) в реестр: view узла собирается из
    /// реестра, источник правды меты — state.
    pub fn with_state(path: &FsPath) -> Self {
        let store = StateStore::load(path);
        let mut reg = Registry::new();
        for (node_id, meta) in &store.state().nodes {
            reg.set_meta(node_id, meta.display_name.clone(), meta.channel_map);
        }
        let (state_version, _rx) = watch::channel(0u64);
        Self {
            reg: Arc::new(Mutex::new(reg)),
            dscp: Arc::new(AtomicU8::new(DEFAULT_DSCP)),
            store: Arc::new(Mutex::new(store)),
            state_version,
            listen_addr: "0.0.0.0:8080".to_string(),
            post_transfer_stop: default_post_transfer_stop(),
            transfer_wait: TRANSFER_WAIT_TIMEOUT,
            silence_holds: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Подстановка реального listen-адреса (вызывает run()).
    pub fn set_listen_addr(&mut self, addr: impl Into<String>) {
        self.listen_addr = addr.into();
    }

    /// Тестовый hook: подмена локального стопа после трансфера (по умолчанию
    /// — systemctl, тесты ставят заглушку и фиксируют факт вызова).
    pub fn set_post_transfer_stop(&mut self, f: Arc<dyn Fn() + Send + Sync>) {
        self.post_transfer_stop = f;
    }

    /// Тестовый hook: уменьшение таймаута ожидания поднятия контроллера
    /// на цели трансфера (по умолчанию 10 с — в тестах долго).
    pub fn set_transfer_wait(&mut self, d: Duration) {
        self.transfer_wait = d;
    }

    /// Мутация state единым helper'ом: persist на диск + bump триггера
    /// pusher'а. Ошибка записи — 500 вызывающему handler'у.
    async fn update_state(&self, f: impl FnOnce(&mut MasterState)) -> Result<(), (StatusCode, String)> {
        self.store
            .lock()
            .await
            .update(f)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("запись state: {e}")))?;
        self.state_version.send_modify(|v| *v += 1);
        Ok(())
    }
}

/// Дефолтный пост-трансферный стоп: ответ REST уходит, через 500 мс в
/// отдельном потоке гасим свой контроллер — роль мастера передана.
fn default_post_transfer_stop() -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(|| {
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(500));
            match std::process::Command::new("systemctl")
                .args(["disable", "--now", "zvucast-controller"])
                .output()
            {
                Ok(out) if out.status.success() => {
                    tracing::info!("трансфер: локальный контроллер остановлен (systemctl)")
                }
                Ok(out) => tracing::warn!(
                    "трансфер: systemctl disable --now zvucast-controller: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ),
                Err(e) => tracing::warn!("трансфер: systemctl недоступен: {e}"),
            }
        });
    })
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/{*path}", get(assets))
        .route("/api/nodes", get(list_nodes))
        .route("/api/nodes/{node_id}", patch(patch_node))
        .route("/api/nodes/{name}/start", post(node_start))
        .route("/api/nodes/{name}/stop", post(node_stop))
        .route("/api/start-all", post(start_all))
        .route("/api/stop-all", post(stop_all))
        .route("/api/dscp", get(get_dscp).post(set_dscp))
        .route("/api/zones", get(list_zones).post(create_zone))
        .route("/api/zones/{id}", patch(patch_zone).delete(delete_zone))
        .route("/api/zones/{id}/start", post(zone_start))
        .route("/api/zones/{id}/stop", post(zone_stop))
        .route("/api/master", get(master_info))
        .route("/api/master/transfer", post(master_transfer))
        .route("/ws", get(ws_handler))
        .fallback(spa_fallback)
        .with_state(state)
}

/// Одноразовый маркер легитимного преемника (трансфер): агент ставит его
/// перед `systemctl enable --now`, контроллер при старте удаляет и пропускает
/// ТОЛЬКО discovery-предпроверку. Неудалённый маркер пропуск не даёт:
/// одноразовость обязательна — иначе защита от двух мастеров мертва при
/// каждом рестарте.
fn consume_successor_marker(state_path: &FsPath) -> bool {
    let marker = successor_marker_path(state_path);
    if !marker.is_file() {
        return false;
    }
    if let Err(e) = std::fs::remove_file(&marker) {
        tracing::warn!(
            "маркер преемника не удалён ({}): {e} — предпроверка не пропущена",
            marker.display()
        );
        return false;
    }
    true
}

/// Маркер преемника одноразово пропускает предпроверку: трансфер стартует
/// новый контроллер при ещё живом старом. Без пропуска новый процесс
/// падал бы в crash-loop (Restart=always), и трансфер всегда кончался 502.
fn successor_skips_precheck(state_path: &FsPath) -> bool {
    if !consume_successor_marker(state_path) {
        return false;
    }
    tracing::info!("маркер преемника потреблён: discovery-предпроверка пропущена (трансфер)");
    true
}

/// Непривязанный запрос. Тест маркера зовёт эту функцию, не `run`:
/// `run` ждёт конфиг и первую публикацию интерфейса.
#[cfg(test)]
async fn master_precheck(state_path: &FsPath, discovery_dest: &str) -> anyhow::Result<()> {
    if successor_skips_precheck(state_path) {
        return Ok(());
    }
    if let Some(addr) = discovery::query_once(discovery_dest, Duration::from_secs(1)).await? {
        anyhow::bail!("в сети уже есть мастер {addr}, отказываюсь стартовать");
    }
    Ok(())
}

/// Один запрос с уже опубликованного интерфейса. Ответ мастера — в сети
/// уже есть контроллер, второй не стартует. Ошибка отправки тоже не даёт
/// стартовать: убедиться, что мастера нет, не удалось. Тишина — можно
/// поднимать веб и респондер.
async fn precheck_bound(state_path: &FsPath, link: &ActiveLink) -> anyhow::Result<()> {
    if successor_skips_precheck(state_path) {
        return Ok(());
    }
    if let Some(addr) = discovery::query_bound(
        &format!("255.255.255.255:{DISCOVERY_PORT}"),
        Duration::from_secs(1),
        &link.name,
        link.ipv4,
    )
    .await?
    {
        anyhow::bail!("в сети уже есть мастер {addr}, отказываюсь стартовать");
    }
    Ok(())
}

/// Ждёт toml. Пока `load` падает, сокет 5005 и HTTP не открываются.
async fn wait_config(config_path: &FsPath) -> crate::config::AgentConfig {
    loop {
        match crate::config::load(config_path) {
            Ok(cfg) => return cfg,
            Err(e) => {
                tracing::warn!("конфиг {}: {e:#}", config_path.display());
                tokio::time::sleep(CONFIG_RETRY).await;
            }
        }
    }
}

/// Первая публикация `Some`. До неё предпроверка не уходит.
async fn wait_first_link(
    uplink: &mut tokio::sync::watch::Receiver<Option<ActiveLink>>,
) -> anyhow::Result<ActiveLink> {
    loop {
        if let Some(link) = uplink.borrow().clone() {
            return Ok(link);
        }
        uplink
            .changed()
            .await
            .map_err(|_| anyhow::anyhow!("наблюдатель интерфейса закрылся"))?;
    }
}

/// `config_path` в проде — [`CONFIG_PATH`]. Отдельный флаг CLI не обязателен.
pub async fn run(listen: &str, state_path: &FsPath, config_path: &FsPath) -> anyhow::Result<()> {
    let cfg = wait_config(config_path).await;
    tracing::info!(
        "controller: конфиг прочитан, sync_interface {}",
        cfg.sync_interface
    );
    let mut uplink = crate::uplink::watch(cfg.sync_interface);
    let link = wait_first_link(&mut uplink).await?;
    tracing::info!(
        "controller: активный интерфейс {} ({})",
        link.name,
        link.ipv4
    );
    precheck_bound(state_path, &link).await?;
    // Респондер сам перевязывает сокет. Повторной предпроверки нет:
    // свой ответ на 5005 не должен гасить уже поднятый контроллер.
    tokio::spawn(discovery::serve_uplink(uplink));
    let listener = TcpListener::bind(listen).await?;
    let mut state = AppState::with_state(state_path);
    state.set_listen_addr(listen);
    spawn_endpoint_pusher(state.clone());
    tracing::info!(
        "controller слушает http://{listen} (state: {}); api: /api/*, ws: /ws",
        state_path.display()
    );
    axum::serve(
        listener,
        app(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// Веб-UI: собранный React-SPA из web/dist, встроен на этапе компиляции
/// (rust-embed). Плейсхолдер web/dist/index.html коммитится — cargo build
/// работает без npm; `just build-web` перезаписывает dist собранным SPA.
#[derive(rust_embed::Embed)]
#[folder = "../web/dist/"]
struct WebAssets;

/// Дашборд: index.html собранного SPA.
async fn index() -> impl IntoResponse {
    static_asset(FsPath::new("index.html"))
}

/// GET /assets/{*path} — статика сборки (js/css с хешами в именах).
/// В axum 0.8 wildcard захватывает хвост без префикса маршрута —
/// подставляем «assets/» сами.
async fn assets(Path(path): Path<String>) -> Response {
    static_asset(&FsPath::new("assets").join(path.as_str()))
}

/// SPA fallback: прочие не-API GET → index.html (клиентский роутинг),
/// /api/*, /ws и не-GET без fallback'а — честный 404.
async fn spa_fallback(req: Request) -> Response {
    let path = req.uri().path();
    if req.method() == Method::GET && !path.starts_with("/api/") && path != "/ws" {
        static_asset(FsPath::new("index.html"))
    } else {
        (StatusCode::NOT_FOUND, "not found").into_response()
    }
}

/// Файл из встроенного dist: content-type по расширению, 404 для
/// отсутствующих (fallback отдаёт index.html до сюда не дойдёт).
fn static_asset(path: &FsPath) -> Response {
    match WebAssets::get(path.to_string_lossy().as_ref()) {
        Some(c) => (
            [(
                header::CONTENT_TYPE,
                mime_guess::from_path(path).first_or_octet_stream().to_string(),
            )],
            c.data.into_owned(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[derive(Serialize)]
struct NodeApi {
    #[serde(flatten)]
    node: NodeView,
    /// Эффективный DSCP: свой у передатчика или общий.
    dscp: u8,
    inputs: Vec<crate::inputs::InputView>,
    outputs: Vec<crate::inputs::InputView>,
    virtual_output: bool,
    supports_virtual_output: bool,
    /// Ключ Spotify этого передатчика. Нет поля — Soloist на узле не запускается.
    #[serde(skip_serializing_if = "Option::is_none")]
    soloist_key: Option<String>,
    /// Галочка «Включить Spotify».
    spotify: bool,
    /// Галочка «Включить Sendspin».
    sendspin: bool,
}

async fn list_nodes(State(st): State<AppState>) -> Json<Vec<NodeApi>> {
    // sweep вызывается на каждом GET, а не фоновой задачей: так
    // детерминированнее (зафиксировано в брифе). Основной путь offline —
    // mark_offline из ws-хендлера при разрыве; sweep страхует от «мёртвых»
    // соединений, где разрыв не был замечен.
    let mut reg = st.reg.lock().await;
    reg.sweep(Instant::now(), SWEEP_TIMEOUT);
    let views = reg.list();
    let discovered: Vec<_> = views
        .iter()
        .map(|v| {
            (
                v.node_id.clone(),
                reg.capture_inputs(&v.node_id),
                reg.capture_device_of(&v.node_id),
                v.roles.contains(&Role::Tx),
                reg.playback_outputs(&v.node_id),
                reg.playback_device_of(&v.node_id),
                v.roles.contains(&Role::Rx),
                reg.supports_virtual_output(&v.node_id),
            )
        })
        .collect();
    drop(reg);
    let global = st.dscp.load(Ordering::Relaxed);
    let store = st.store.lock().await;
    let state = store.state();
    let apis = views
        .into_iter()
        .zip(discovered)
        .map(|(node, (_, found, cap, is_tx, outs, play, is_rx, supports))| {
            let meta = state.nodes.get(&node.node_id);
            let dscp = meta.and_then(|m| m.dscp).unwrap_or(global);
            let prefs = meta.map(|m| m.inputs.as_slice()).unwrap_or(&[]);
            let inputs = if is_tx {
                let found = if found.is_empty() {
                    crate::inputs::fallback_inputs(cap.as_deref())
                } else {
                    found
                };
                crate::inputs::visible_inputs(crate::inputs::merge_inputs(&found, prefs, cap.as_deref()))
            } else {
                Vec::new()
            };
            let out_prefs = meta.map(|m| m.outputs.as_slice()).unwrap_or(&[]);
            let outputs = if is_rx {
                let outs = if outs.is_empty() {
                    crate::inputs::fallback_outputs(play.as_deref())
                } else {
                    outs
                };
                crate::inputs::merge_inputs(&outs, out_prefs, play.as_deref())
            } else {
                Vec::new()
            };
            let virtual_output = meta.is_some_and(|m| m.virtual_output);
            let soloist_key = meta.and_then(|m| node_soloist_key_raw(m.soloist_key.as_deref()));
            let spotify = meta.is_some_and(|m| m.spotify);
            let sendspin = meta.is_some_and(|m| m.sendspin);
            NodeApi {
                node,
                dscp,
                inputs,
                outputs,
                virtual_output,
                supports_virtual_output: supports,
                soloist_key,
                spotify,
                sendspin,
            }
        })
        .collect();
    Json(apis)
}

async fn get_dscp(State(st): State<AppState>) -> Json<Value> {
    Json(json!({ "dscp": st.dscp.load(Ordering::Relaxed) }))
}

#[derive(Deserialize)]
struct DscpBody {
    dscp: u8,
}

async fn set_dscp(
    State(st): State<AppState>,
    body: Result<Json<DscpBody>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Json(b) = body.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if b.dscp > 63 {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("dscp {} вне диапазона 0..=63", b.dscp),
        ));
    }
    st.dscp.store(b.dscp, Ordering::Relaxed);
    // Триггер пуша: tx-узлы должны получить свежий dscp с ближайшим
    // SetEndpoints (дебаунс pusher'а схлопывает серию bump'ов в один пуш).
    st.reg.lock().await.bump_version();
    Ok(Json(json!({ "dscp": b.dscp })))
}

async fn node_start(Path(name): Path<String>, State(st): State<AppState>) -> CmdResult {
    send_node_cmd(&st, &name, CtlMsg::Start).await
}

async fn node_stop(Path(name): Path<String>, State(st): State<AppState>) -> CmdResult {
    send_node_cmd(&st, &name, CtlMsg::Stop).await
}

type CmdResult = Result<Json<Value>, (StatusCode, String)>;

/// Команда узлу по пути /api/nodes/{name}/...: идентификатор резолвится
/// сначала как node_id, затем как имя (алиас для старых скриптов/доков,
/// спека §3). Ответ и команда — по node_id.
async fn send_node_cmd(st: &AppState, id_or_name: &str, cmd: CtlMsg) -> CmdResult {
    let mut reg = st.reg.lock().await;
    let Some(node_id) = reg.resolve(id_or_name) else {
        return Err((StatusCode::NOT_FOUND, format!("узел {id_or_name} не найден")));
    };
    match reg.send_cmd(&node_id, cmd) {
        Ok(()) => Ok(Json(json!({ "sent": node_id }))),
        Err(e) => {
            let code = match e {
                RegistryError::UnknownNode => StatusCode::NOT_FOUND,
                RegistryError::Offline => StatusCode::CONFLICT,
                // Канал закрыт/переполнен — узел завис или отвалился.
                RegistryError::Closed | RegistryError::Full => StatusCode::SERVICE_UNAVAILABLE,
            };
            if matches!(e, RegistryError::Closed) {
                reg.mark_offline(&node_id);
            }
            Err((code, e.to_string()))
        }
    }
}

async fn start_all(State(st): State<AppState>) -> Json<Value> {
    Json(json!({ "started": broadcast(&st, CtlMsg::Start).await }))
}

async fn stop_all(State(st): State<AppState>) -> Json<Value> {
    Json(json!({ "stopped": broadcast(&st, CtlMsg::Stop).await }))
}

/// Команда всем узлам: Offline-ошибки игнорируются (по спеке), Closed —
/// mark_offline, переполнение — warn; возвращает список node_id, куда дошло.
async fn broadcast(st: &AppState, cmd: CtlMsg) -> Vec<String> {
    let mut reg = st.reg.lock().await;
    let mut sent = Vec::new();
    for name in reg.names() {
        match reg.send_cmd(&name, cmd.clone()) {
            Ok(()) => sent.push(name),
            Err(RegistryError::Offline) => {}
            Err(RegistryError::Closed) => {
                reg.mark_offline(&name);
            }
            Err(RegistryError::Full) => {
                tracing::warn!(node = name, "канал команд переполнен — команда пропущена");
            }
            Err(RegistryError::UnknownNode) => unreachable!("node_id из reg.names()"),
        }
    }
    sent
}

// --- Мастер: статус и трансфер (Task 10). ---

#[derive(Deserialize)]
struct TransferBody {
    node_id: String,
}

/// GET /api/master: адрес своего WS и кто сейчас мастер
/// (`state.master_node_id`; None — не задан / этот контроллер и есть мастер).
async fn master_info(State(st): State<AppState>) -> Json<Value> {
    let node_id = st.store.lock().await.state().master_node_id.clone();
    Json(json!({
        "addr": format!("ws://{}/ws", st.listen_addr),
        "node_id": node_id,
    }))
}

/// Трансфер роли мастера online-узлу:
/// 1. резолв цели (404 неизвестен / 409 offline)
/// 2. BecomeMaster с сериализованным state контроллера
/// 3. ожидание поднятия контроллера на ip:CONTROLLER_PORT (`wait_controller_up`);
///    таймаут → 502 и best-effort откат (DisableController цели: гасим её
///    контроллер и маркер — иначе enabled-юнит стартанёт сам при поздней
///    смерти старого мастера, некоординированно, со stale-state)
/// 4. master_node_id в state
/// 5. ControllerMoved всем узлам — агенты переподключаются на новый адрес
/// 6. ответ REST, затем локальный стоп (hook: в проде systemctl через 500 мс)
///
/// Шаги 3–6 живут в `tokio::spawn`, не в future запроса. Иначе обрыв
/// клиента (curl/UI не дождались connect сквозь DROP) отменяет handler
/// посередине: BecomeMaster уже включил контроллер на цели, а
/// DisableController не уходит — второй мастер остаётся.
async fn master_transfer(
    State(st): State<AppState>,
    body: Result<Json<TransferBody>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Json(b) = body.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // Резолв + online + ip. Раздельные чтение и отправка: замки reg и store
    // не держим одновременно (инвариант модуля).
    let (target, ip) = {
        let reg = st.reg.lock().await;
        let Some(node_id) = reg.resolve(&b.node_id) else {
            return Err((StatusCode::NOT_FOUND, format!("узел {} не найден", b.node_id)));
        };
        let view = reg
            .list()
            .into_iter()
            .find(|v| v.node_id == node_id)
            .expect("resolve вернул node_id из реестра");
        if !view.online {
            return Err((StatusCode::CONFLICT, format!("узел {node_id} offline — трансфер невозможен")));
        }
        (node_id, view.ip)
    };
    let state_json = {
        let store = st.store.lock().await;
        serde_json::to_string(store.state())
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("сериализация state: {e}")))?
    };
    {
        let mut reg = st.reg.lock().await;
        if let Err(e) = reg.send_cmd(&target, CtlMsg::BecomeMaster { state: state_json }) {
            if matches!(e, RegistryError::Closed) {
                reg.mark_offline(&target);
            }
            let code = match e {
                RegistryError::UnknownNode => StatusCode::NOT_FOUND,
                RegistryError::Offline | RegistryError::Closed => StatusCode::CONFLICT,
                RegistryError::Full => StatusCode::SERVICE_UNAVAILABLE,
            };
            return Err((code, format!("BecomeMaster не доставлен: {e}")));
        }
    }
    let job = tokio::spawn(finish_transfer(st, target, ip));
    match job.await {
        Ok(result) => result,
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("трансфер прерван: {e}"),
        )),
    }
}

/// Хвостовая часть трансфера (после доставленного BecomeMaster).
/// Отдельная задача: см. комментарий у `master_transfer`.
async fn finish_transfer(
    st: AppState,
    target: String,
    ip: String,
) -> Result<Json<Value>, (StatusCode, String)> {
    if let Err(msg) = wait_controller_up(&ip, st.transfer_wait).await {
        // 502: цель контроллер не подняла — best-effort откат. Без него
        // enabled-юнит на цели стартанул бы сам при поздней смерти старого
        // мастера (некоординированно, со stale-state), а живой маркер
        // преемника лишил бы её discovery-предпроверки.
        let mut reg = st.reg.lock().await;
        if let Err(e) = reg.send_cmd(&target, CtlMsg::DisableController) {
            if matches!(e, RegistryError::Closed) {
                reg.mark_offline(&target);
            }
            tracing::warn!(node = target, "откат трансфера: DisableController не доставлен: {e}");
        }
        return Err((StatusCode::BAD_GATEWAY, msg));
    }
    st.update_state(|s| s.master_node_id = Some(target.clone()))
        .await?;
    broadcast(
        &st,
        CtlMsg::ControllerMoved {
            addr: format!("ws://{ip}:{CONTROLLER_PORT}/ws"),
        },
    )
    .await;
    (st.post_transfer_stop)();
    Ok(Json(json!({
        "ok": true,
        "url": format!("http://{ip}:{CONTROLLER_PORT}/"),
    })))
}

/// Квант одной попытки TCP-connect в `wait_controller_up`.
///
/// Попытка не длиннее кванта: при DROP фаервола (UFW на цели) голый
/// `TcpStream::connect` сидит в SYN-SENT до системного tcp_syn_retries
/// (~2 мин) и дедлайн трансфера не наступает. Клиент к тому моменту
/// уже оборвал HTTP, и откат не выполнялся.
const CONNECT_ATTEMPT: Duration = Duration::from_millis(500);

/// Ожидание поднятия контроллера на `ip:CONTROLLER_PORT`: TCP-connect
/// квантами по [`CONNECT_ATTEMPT`] до дедлайна. Тестируется против
/// локального TcpListener.
async fn wait_controller_up(ip: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let addr = format!("{ip}:{CONTROLLER_PORT}");
    while Instant::now() < deadline {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        let slice = left.min(CONNECT_ATTEMPT);
        let started = Instant::now();
        let connected = tokio::time::timeout(slice, tokio::net::TcpStream::connect(addr.as_str()))
            .await
            .ok()
            .and_then(Result::ok)
            .is_some();
        if connected {
            return Ok(());
        }
        // Ранний отказ (connection refused) не должен крутить цикл вхолостую:
        // добиваем квант, но не вылезаем за дедлайн.
        let spent = started.elapsed();
        if spent < slice {
            tokio::time::sleep(slice - spent).await;
        }
    }
    Err(format!("контроллер на {addr} не поднялся за {timeout:?}"))
}

// --- Зоны: REST CRUD + пуск/стоп. Источник правды — state.json; любая
// мутация идёт через `update_state` (persist + bump триггера pusher'а). ---

/// Зона как её видит фронт: поля зоны + `stolen` — node_id членов активной
/// зоны, чей владелец в раскладке assignment — другая зона (больший seq).
/// `silenced` — зона сейчас в холде тишины.
#[derive(Debug, Clone, serde::Serialize)]
struct ZoneView {
    id: String,
    name: String,
    tx: String,
    sources: Vec<String>,
    members: Vec<String>,
    active: bool,
    seq: u64,
    silence_timeout_s: u64,
    silence_threshold_db: f64,
    jb_ms: u32,
    silenced: bool,
    stolen: Vec<String>,
    /// Карты каналов членов зоны (node_id → карта). Пустая запись — стерео на UI.
    maps: HashMap<String, ChannelMap>,
    /// Карта колонки по источнику. Пусто — у всех источников общая `maps`.
    source_maps: HashMap<String, HashMap<String, ChannelMap>>,
}

/// ZoneView всех зон state (отсортированы по имени, tiebreak — id).
/// `held` — id зон в холде; замок холдов берётся отдельно от store.
fn zone_views(state: &MasterState, held: &HashSet<String>) -> Vec<ZoneView> {
    let owners = owner_ids(state);
    let mut out: Vec<ZoneView> = state
        .zones
        .iter()
        .map(|z| {
            let stolen = if z.active {
                z.members
                    .iter()
                    .filter(|m| owners.get(m.as_str()).is_some_and(|o| o != &z.id))
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            ZoneView {
                id: z.id.clone(),
                name: z.name.clone(),
                tx: z.tx.clone(),
                sources: z.source_ids(),
                members: z.members.clone(),
                active: z.active,
                seq: z.seq,
                silence_timeout_s: z.silence_timeout_s,
                silence_threshold_db: z.silence_threshold_db,
                jb_ms: z.jb_ms,
                silenced: held.contains(&z.id),
                stolen,
                maps: z.maps.clone(),
                source_maps: z.source_maps.clone(),
            }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
    out
}

fn zone_view_by_id(state: &MasterState, id: &str, held: &HashSet<String>) -> Option<ZoneView> {
    zone_views(state, held).into_iter().find(|v| v.id == id)
}

/// Ответ зоны свежим view. Между мутацией и этим чтением зону могла удалить
/// concurrent DELETE — честный 404 вместо unwrap.
async fn zone_view_response(
    st: &AppState,
    id: &str,
) -> Result<Json<ZoneView>, (StatusCode, String)> {
    let held = st.silence_holds.lock().await.clone();
    let store = st.store.lock().await;
    zone_view_by_id(store.state(), id, &held)
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, format!("зона {id} не найдена")))
}

#[derive(Deserialize)]
struct CreateZone {
    name: String,
    #[serde(default)]
    tx: Option<String>,
    #[serde(default)]
    sources: Option<Vec<String>>,
    members: Vec<String>,
    /// Нет поля — запас 2 мс.
    #[serde(default)]
    jb_ms: Option<u32>,
}

#[derive(Deserialize, Default)]
struct PatchZone {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    tx: Option<String>,
    #[serde(default)]
    sources: Option<Vec<String>>,
    #[serde(default)]
    members: Option<Vec<String>>,
    #[serde(default)]
    silence_timeout_s: Option<u64>,
    #[serde(default)]
    silence_threshold_db: Option<f64>,
    /// Нет поля — запас зоны не меняется. Вне 1..=500 — 422.
    #[serde(default)]
    jb_ms: Option<u32>,
    /// Переданные ключи заменяют карты зоны; остальные остаются.
    /// Ключ, которого нет в members, молча пропускается.
    #[serde(default)]
    maps: Option<HashMap<String, ChannelMap>>,
    /// member → source → карта. Ключи вне members и источников зоны пропускаются.
    #[serde(default)]
    source_maps: Option<HashMap<String, HashMap<String, ChannelMap>>>,
}

/// Валидация источника зоны: узел известен реестру (online не требуется —
/// роли помнятся с последнего hello) и имеет роль Tx.
/// Список источников. `sources` важнее одного `tx`. Пустой список — ошибка.
fn resolve_sources(
    sources: Option<Vec<String>>,
    tx: Option<String>,
) -> Result<Vec<String>, (StatusCode, String)> {
    let raw = match sources {
        Some(list) => list,
        None => match tx {
            Some(id) => vec![id],
            None => {
                return Err((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "нужен источник".into(),
                ))
            }
        },
    };
    let mut out = Vec::new();
    for id in raw {
        let id = id.trim().to_string();
        if !id.is_empty() && !out.contains(&id) {
            out.push(id);
        }
    }
    if out.is_empty() {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "у зоны должен быть источник".into(),
        ));
    }
    Ok(out)
}

fn validate_zone_tx(reg: &Registry, tx: &str) -> Result<(), (StatusCode, String)> {
    let roles = reg.roles_of(tx).ok_or((
        StatusCode::UNPROCESSABLE_ENTITY,
        format!("tx: неизвестный узел {tx}"),
    ))?;
    if !roles.contains(&Role::Tx) {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("узел {tx} не является tx"),
        ));
    }
    Ok(())
}

/// Валидация членов зоны: каждый известен реестру и имеет роль Rx.
fn validate_zone_members(reg: &Registry, members: &[String]) -> Result<(), (StatusCode, String)> {
    for m in members {
        let roles = reg.roles_of(m).ok_or((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("member: неизвестный узел {m}"),
        ))?;
        if !roles.contains(&Role::Rx) {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("узел {m} не является rx"),
            ));
        }
    }
    Ok(())
}

async fn list_zones(State(st): State<AppState>) -> Json<Value> {
    let held = st.silence_holds.lock().await.clone();
    let store = st.store.lock().await;
    Json(json!({ "zones": zone_views(store.state(), &held) }))
}

async fn create_zone(
    State(st): State<AppState>,
    body: Result<Json<CreateZone>, JsonRejection>,
) -> Result<Json<ZoneView>, (StatusCode, String)> {
    let Json(b) = body.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let mut members = b.members;
    members.sort();
    members.dedup();
    let sources = resolve_sources(b.sources, b.tx)?;
    {
        let reg = st.reg.lock().await;
        for tx in &sources {
            validate_zone_tx(&reg, tx)?;
        }
        validate_zone_members(&reg, &members)?;
    }
    if let Some(jb) = b.jb_ms {
        if !(1..=500).contains(&jb) {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("jb_ms {jb} вне диапазона 1..=500"),
            ));
        }
    }
    let mut zone = Zone {
        id: uuid::Uuid::new_v4().to_string(),
        name: b.name,
        tx: String::new(),
        sources: Vec::new(),
        members,
        active: false,
        seq: 0,
        maps: std::collections::HashMap::new(),
        source_maps: std::collections::HashMap::new(),
        silence_timeout_s: 300,
        silence_threshold_db: -60.0,
        jb_ms: b.jb_ms.unwrap_or_else(default_jb_ms),
    };
    zone.set_sources(sources);
    st.update_state(|s| s.zones.push(zone.clone())).await?;
    for tx in zone.source_ids() {
        push_virtual_zones(&st, &tx).await;
    }
    Ok(Json(zone_view_from(&zone)))
}

/// ZoneView из зоны без раскладки (create-ответ: зона неактивна, stolen пуст).
fn zone_view_from(z: &Zone) -> ZoneView {
    ZoneView {
        id: z.id.clone(),
        name: z.name.clone(),
        tx: z.tx.clone(),
        sources: z.source_ids(),
        members: z.members.clone(),
        active: z.active,
        seq: z.seq,
        silence_timeout_s: z.silence_timeout_s,
        silence_threshold_db: z.silence_threshold_db,
        jb_ms: z.jb_ms,
        silenced: false,
        stolen: Vec::new(),
        maps: z.maps.clone(),
        source_maps: z.source_maps.clone(),
    }
}

async fn patch_zone(
    Path(id): Path<String>,
    State(st): State<AppState>,
    body: Result<Json<PatchZone>, JsonRejection>,
) -> Result<Json<ZoneView>, (StatusCode, String)> {
    let Json(b) = body.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let current = {
        let store = st.store.lock().await;
        store
            .state()
            .zones
            .iter()
            .find(|z| z.id == id)
            .cloned()
            .ok_or((StatusCode::NOT_FOUND, format!("зона {id} не найдена")))?
    };
    let sources_touched = b.sources.is_some() || b.tx.is_some();
    let members_touched = b.members.is_some();
    let new_sources = if sources_touched {
        Some(resolve_sources(b.sources, b.tx)?)
    } else {
        None
    };
    let new_members = match b.members {
        Some(mut m) => {
            m.sort();
            m.dedup();
            m
        }
        None => current.members.clone(),
    };
    {
        let reg = st.reg.lock().await;
        // Валидируем только меняемые поля: роли узла могли смениться с
        // момента создания зоны, и PATCH имени не должен от них зависеть.
        if let Some(sources) = &new_sources {
            for tx in sources {
                validate_zone_tx(&reg, tx)?;
            }
        }
        if members_touched {
            validate_zone_members(&reg, &new_members)?;
        }
    }
    if let Some(jb) = b.jb_ms {
        if !(1..=500).contains(&jb) {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("jb_ms {jb} вне диапазона 1..=500"),
            ));
        }
    }
    let pushed: Vec<String> = match &new_sources {
        Some(next) => {
            let mut ids = current.source_ids();
            for id in next {
                if !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
            ids
        }
        None => current.source_ids(),
    };
    st.update_state(|s| {
        if let Some(z) = s.zones.iter_mut().find(|z| z.id == id) {
            if let Some(name) = b.name {
                z.name = name;
            }
            if let Some(sources) = new_sources {
                z.set_sources(sources);
            }
            z.members = new_members;
            if let Some(secs) = b.silence_timeout_s {
                z.silence_timeout_s = secs;
            }
            if let Some(db) = b.silence_threshold_db {
                z.silence_threshold_db = db;
            }
            if let Some(jb) = b.jb_ms {
                z.jb_ms = jb;
            }
            if let Some(incoming) = b.maps.clone() {
                for (member, map) in incoming {
                    if z.members.iter().any(|m| m == &member) {
                        z.maps.insert(member, map);
                    }
                }
            }
            if let Some(incoming) = b.source_maps.clone() {
                for (member, by_source) in incoming {
                    if !z.members.iter().any(|m| m == &member) {
                        continue;
                    }
                    let known = z.source_ids();
                    let slot = z.source_maps.entry(member).or_default();
                    for (source, map) in by_source {
                        if known.iter().any(|id| id == &source) {
                            slot.insert(source, map);
                        }
                    }
                }
            }
        }
    })
    .await?;
    for tx in pushed {
        push_virtual_zones(&st, &tx).await;
    }
    zone_view_response(&st, &id).await
}

async fn delete_zone(Path(id): Path<String>, State(st): State<AppState>) -> CmdResult {
    let tx = {
        let store = st.store.lock().await;
        let Some(zone) = store.state().zones.iter().find(|z| z.id == id) else {
            return Err((StatusCode::NOT_FOUND, format!("зона {id} не найдена")));
        };
        zone.source_ids()
    };
    st.update_state(|s| s.zones.retain(|z| z.id != id)).await?;
    for source in tx {
        push_virtual_zones(&st, &source).await;
    }
    Ok(Json(json!({ "deleted": id })))
}

async fn zone_start(Path(id): Path<String>, State(st): State<AppState>) -> Result<Json<ZoneView>, (StatusCode, String)> {
    zone_set_active(&st, &id, true).await
}

async fn zone_stop(Path(id): Path<String>, State(st): State<AppState>) -> Result<Json<ZoneView>, (StatusCode, String)> {
    zone_set_active(&st, &id, false).await
}

/// Пуск/стоп зоны. Пуск: active=true и seq = next_seq (затем next_seq += 1 —
/// уникальность seq на API-слое, инвариант кражи в assignment). Идемпотентно:
/// повторный пуск активной зоны ничего не меняет. Стоп: active=false.
///
/// Проверка идемпотентности — и в предварительном чтении (быстрый путь без
/// лишней записи), и ВНУТРИ замыкания update_state: иначе два конкурентных
/// start (оба видят active=false) переназначили бы seq дважды (TOCTOU).
async fn zone_set_active(
    st: &AppState,
    id: &str,
    active: bool,
) -> Result<Json<ZoneView>, (StatusCode, String)> {
    let idempotent = {
        let store = st.store.lock().await;
        let zone = store
            .state()
            .zones
            .iter()
            .find(|z| z.id == id)
            .ok_or((StatusCode::NOT_FOUND, format!("зона {id} не найдена")))?;
        zone.active == active
    };
    if !idempotent {
        st.update_state(|s| {
            if let Some(z) = s.zones.iter_mut().find(|z| z.id == id) {
                if z.active == active {
                    return; // конкурентный вызов уже переключил — seq не трогаем
                }
                z.active = active;
                if active {
                    z.seq = s.next_seq;
                    s.next_seq += 1;
                }
            }
        })
        .await?;
    }
    zone_view_response(st, id).await
}

#[derive(Deserialize, Default)]
struct PatchNode {
    /// None — поле не тронуто; Some(None) — явный null (сброс); Some(Some(v)) — значение.
    /// `Option<Option<T>>` сам по себе null не отличит от absent (serde
    /// отдаёт null внешнему Option) — нужен deserialize_with.
    #[serde(default, deserialize_with = "de_touched")]
    display_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "de_touched")]
    channel_map: Option<Option<ChannelMap>>,
    dscp: Option<u8>,
    inputs: Option<Vec<InputPatch>>,
    outputs: Option<Vec<InputPatch>>,
    virtual_output: Option<bool>,
    /// Пустая строка снимает ключ. Поле отсутствует — не трогать.
    soloist_key: Option<String>,
    /// None — не трогать.
    spotify: Option<bool>,
    sendspin: Option<bool>,
}

#[derive(Deserialize)]
struct InputPatch {
    id: String,
    name: Option<String>,
    enabled: Option<bool>,
}

/// absent → None (default), null → Some(None), значение → Some(Some(v)).
fn de_touched<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

fn apply_chan_patches(prefs: &mut Vec<crate::inputs::InputPref>, patches: Vec<InputPatch>) {
    for p in patches {
        if let Some(pref) = prefs.iter_mut().find(|i| i.id == p.id) {
            if let Some(name) = p.name { pref.name = name; }
            if let Some(enabled) = p.enabled { pref.enabled = enabled; }
        } else {
            prefs.push(crate::inputs::InputPref {
                id: p.id,
                name: p.name.unwrap_or_default(),
                enabled: p.enabled.unwrap_or(true),
            });
        }
    }
}

/// PATCH /api/nodes/{node_id}: display_name узла.
/// Идентификатор резолвится как node_id, затем как имя (как в start/stop).
/// `channel_map` в теле игнорируется: карта живёт в зоне, в NodeMeta не
/// пишется и SetChannelMap отсюда не уходит.
async fn patch_node(
    Path(id_or_name): Path<String>,
    State(st): State<AppState>,
    body: Result<Json<PatchNode>, JsonRejection>,
) -> Result<Json<NodeView>, (StatusCode, String)> {
    let Json(b) = body.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let node_id = {
        let reg = st.reg.lock().await;
        reg.resolve(&id_or_name).ok_or((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("неизвестный узел {id_or_name}"),
        ))?
    };
    let mut meta = {
        let store = st.store.lock().await;
        store.state().nodes.get(&node_id).cloned().unwrap_or_default()
    };
    // channel_map в теле не применяем: не пишем в NodeMeta и не пушим.
    let _ = b.channel_map;
    let mut changed = false;
    if let Some(display_name) = b.display_name.clone() {
        meta.display_name = display_name.clone();
        st.reg.lock().await.set_meta(&node_id, display_name, meta.channel_map);
        changed = true;
    }
    if let Some(dscp) = b.dscp {
        if dscp > 63 {
            return Err((StatusCode::BAD_REQUEST, format!("dscp {dscp} вне диапазона 0..=63")));
        }
        meta.dscp = Some(dscp);
        changed = true;
    }
    let patched_inputs = b.inputs.is_some();
    if let Some(patches) = b.inputs {
        apply_chan_patches(&mut meta.inputs, patches);
        changed = true;
    }
    if let Some(patches) = b.outputs {
        apply_chan_patches(&mut meta.outputs, patches);
        changed = true;
    }
    if let Some(enabled) = b.virtual_output {
        meta.virtual_output = enabled;
        changed = true;
    }
    if let Some(key) = b.soloist_key.as_deref() {
        meta.soloist_key = node_soloist_key_raw(Some(key));
        changed = true;
    }
    if let Some(enabled) = b.spotify {
        meta.spotify = enabled;
        changed = true;
    }
    if let Some(enabled) = b.sendspin {
        meta.sendspin = enabled;
        changed = true;
    }
    let prefs = meta.inputs.clone();
    if changed {
        st.update_state(|s| {
            s.nodes.insert(node_id.clone(), meta);
        })
        .await?;
    }
    let push_soloist = b.virtual_output.is_some()
        || b.soloist_key.is_some()
        || b.spotify.is_some()
        || b.sendspin.is_some();
    let zones_cmd = if push_soloist {
        let store = st.store.lock().await;
        let state = store.state();
        Some((
            b.virtual_output.is_some().then(|| virtual_zones_cmd(state, &node_id)),
            soloist_cmd(state, &node_id),
            sendspin_cmd(state, &node_id),
        ))
    } else {
        None
    };
    {
        let mut reg = st.reg.lock().await;
        if let Some(enabled) = b.virtual_output {
            if reg.supports_virtual_output(&node_id) {
                let _ = reg.send_cmd(&node_id, CtlMsg::SetVirtualOutput { enabled });
            }
        }
        if let Some((zones, soloist, sendspin)) = zones_cmd {
            if reg.supports_virtual_output(&node_id) {
                if let Some(zones) = zones {
                    let _ = reg.send_cmd(&node_id, zones);
                }
                let _ = reg.send_cmd(&node_id, soloist);
                let _ = reg.send_cmd(&node_id, sendspin);
            }
        }
        if patched_inputs {
            let capture_device = reg.capture_device_of(&node_id);
            push_capture_target(&mut reg, &node_id, &prefs, capture_device.as_deref());
        }
    }
    let reg = st.reg.lock().await;
    let view = reg
        .list()
        .into_iter()
        .find(|v| v.node_id == node_id)
        .ok_or((StatusCode::NOT_FOUND, format!("узел {node_id} не найден")))?;
    Ok(Json(view))
}

/// Фоновая задача: пушит SetEndpoints online tx-узлам по зонной модели и
/// держит «играющее множество» узлов — Start/Stop уходит по diff с прошлым
/// проходом. want_playing у агента ставится только явным Start (agent.rs,
/// PlayGate): без этого дифа пуск зоны давал бы тишину — SetEndpoints сам
/// пайплайн не запускает.
/// Триггеры — версия реестра (hello/offline/dscp) и `state_version`
/// (каждая мутация state: зоны/мета). Дебаунс PUSH_DEBOUNCE схлопывает
/// серию изменений в один проход.
///
/// Играющее множество: tx с НЕпустым assign (до холда тишины) и online-rx
/// из этого assign. Холд оставляет группу зоны с пустыми endpoints и не
/// шлёт Stop: захват жив.
/// Порядок прохода: сначала SetEndpoints всем затронутым tx (дедуп по last_sent —
/// без изменений), затем Start вошедшим в множество и Stop выбывшим. Start
/// уходит и узлам со свежим hello из playing: reconnect внутри окна дебаунса
/// (offline+online схлопнулись в один проход) не даёт diff'а, а новый процесс
/// агента теряет want_playing — без Start молчал бы до смены раскладки.
/// После рестарта контроллера множество пусто → всем текущим играющим
/// уходит Start с первым пересчётом (спека §2: active переживает рестарт).
///
/// Для каждого online tx: SetEndpoints — группы `zone_groups`. В endpoints
/// зоны попадают её онлайн-колонки вне холда, на порту этого tx, дедуп по ip.
/// Колонке уходит SetMix: порты tx, в чьих группах она сейчас есть (включая
/// холд тишины — пакеты и так не идут, порт не дёргаем), и запас зоны-владельца
/// из `owners`. Нет владельца — 2. Колонка, которую держит более поздняя зона,
/// получает запас держателя, не каждой зоны, где она числится.
/// Пуш только если группы или dscp изменились. Пустой `zones` тоже шлётся:
/// это тишина, не multicast.
/// Свежий hello tx (регистрация или re-hello, виден по `hello_seqs` реестра)
/// даёт форс-пуш вне зависимости от изменения набора: новый процесс агента
/// после переподключения сам ничего о зонах не знает — в том числе при
/// reconnect внутри окна дебаунса.
///
/// Ошибки доставки Start/Stop (Offline/Closed/Full) — warn, узел считается
/// обработанным: бесконечного ретрая нет, Closed дополнительно ведёт в
/// mark_offline (как у send_node_cmd). Full — агент не читает канал: промах
/// доезжает с ближайшим изменением раскладки или re-hello узла. Рассинхрон
/// лечится следующим пересчётом: offline выпал из множества, re-hello вернёт
/// его — с Start.
///
/// Ручной Stop/Start по legacy-путях (/api/nodes/{name}/stop|start,
/// /api/stop-all) временен: пушер вернёт узлу играющее состояние при
/// ближайшем изменении раскладки (diff) или re-hello (fresh-hello Start).
pub fn spawn_silence_ticker(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut book = SilenceBook::default();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let now = Instant::now();
            let zones = state.store.lock().await.state().zones.clone();
            let mut ticks = Vec::new();
            {
                let reg = state.reg.lock().await;
                for z in &zones {
                    let samples: Vec<Sample> = z
                        .source_ids()
                        .iter()
                        .map(|id| match reg.tx_sample(id, now) {
                            Some((age, playing, l, r)) => {
                                classify(age, playing, l, r, z.silence_threshold_db)
                            }
                            None => Sample::NoData,
                        })
                        .collect();
                    let sample = fold_samples(&samples);
                    ticks.push(ZoneTick {
                        id: z.id.clone(),
                        active: z.active,
                        timeout: Duration::from_secs(z.silence_timeout_s),
                        sample,
                    });
                }
            }
            let held = book.recompute(now, &ticks);
            let mut slot = state.silence_holds.lock().await;
            if *slot != held {
                *slot = held;
                drop(slot);
                state.state_version.send_modify(|v| *v += 1);
            }
        }
    })
}

pub fn spawn_endpoint_pusher(state: AppState) -> tokio::task::JoinHandle<()> {
    let _ticker = spawn_silence_ticker(state.clone());
    tokio::spawn(async move {
        let mut reg_rx = state.reg.lock().await.subscribe();
        let mut st_rx = state.state_version.subscribe();
        // Последний отправленный каждому tx набор: node_id → (dscp, endpoints).
        let mut last_sent: HashMap<String, (u8, Vec<ZoneEndpoints>)> = HashMap::new();
        // Последняя отправленная rx карта зоны-владельца. Выпавший из
        // owner_maps отсюда удаляется — следующий захват снова пушит.
        let mut last_maps: HashMap<String, (ChannelMap, Vec<BranchChannelMap>)> = HashMap::new();
        // Последний SetMix колонке: порты и запас владельца. Пустой набор
        // не шлётся, пока колонка ни разу не слушала: агент и так стартует с тишины.
        let mut last_mix: HashMap<String, (Vec<u16>, u32)> = HashMap::new();
        // Снапшот счётчиков hello прошлого прохода (node_id → hello_seq) —
        // база детекции re-hello: форс-пуш SetEndpoints tx и Start играющим.
        let mut prev_hello: HashMap<String, u64> = HashMap::new();
        // Играющее множество прошлого прохода (node_id) — база diff'а Start/Stop.
        let mut prev_playing: HashSet<String> = HashSet::new();
        loop {
            // Ждём первое изменение после прошлого прохода (любого триггера).
            tokio::select! {
                changed = reg_rx.changed() => if changed.is_err() { return; },
                changed = st_rx.changed() => if changed.is_err() { return; },
            }
            // Дебаунс: пока триггеры продолжают стрелять — продлеваем ожидание.
            loop {
                tokio::select! {
                    changed = reg_rx.changed() => if changed.is_err() { return; },
                    changed = st_rx.changed() => if changed.is_err() { return; },
                    _ = tokio::time::sleep(PUSH_DEBOUNCE) => break,
                }
            }
            // Снапшот state → раскладка и владельцы. Замки по одному:
            // store, затем холды, затем reg — никогда вместе.
            let (assign, groups_of, maps, owners, zones, dscp_of, output_prefs) = {
                let store = state.store.lock().await;
                let st = store.state();
                let dscp_of: HashMap<String, u8> = st
                    .nodes
                    .iter()
                    .filter_map(|(id, n)| n.dscp.map(|d| (id.clone(), d)))
                    .collect();
                let output_prefs: HashMap<String, Vec<crate::inputs::InputPref>> = st
                    .nodes
                    .iter()
                    .map(|(id, n)| (id.clone(), n.outputs.clone()))
                    .collect();
                (
                    assignment(st),
                    zone_groups(st),
                    owner_maps(st),
                    owner_ids(st),
                    st.zones.clone(),
                    dscp_of,
                    output_prefs,
                )
            };
            let held = state.silence_holds.lock().await.clone();
            let dscp_global = state.dscp.load(Ordering::Relaxed);
            let mut reg = state.reg.lock().await;
            let online_tx = reg.online_tx();
            let hello_now = reg.hello_seqs();
            let tx_port = mix_ports_for(&reg, &online_tx);
            // Ушедшие в offline забываем: re-hello должен получить форс-пуш.
            last_sent.retain(|id, _| online_tx.iter().any(|t| t == id));
            // Играющее множество — по assign до холда: пустой endpoints_of
            // при живых слушателях это тишина, Stop не шлём.
            let mut playing: HashSet<String> = HashSet::new();
            for tx_id in &online_tx {
                let mut listeners: Vec<String> = Vec::new();
                for m in assign.get(tx_id).into_iter().flatten() {
                    if reg.rx_endpoint(m).is_some() {
                        listeners.push(m.clone());
                    }
                }
                if !listeners.is_empty() {
                    playing.insert(tx_id.clone());
                    playing.extend(listeners);
                }
                let mut zones: Vec<ZoneEndpoints> = Vec::new();
                for group in groups_of.get(tx_id).into_iter().flatten() {
                    let mut endpoints: Vec<Endpoint> = Vec::new();
                    if !held.contains(&group.zone_id) {
                        if let Some(&src_port) = tx_port.get(tx_id) {
                            for member in &group.members {
                                if let Some(mut ep) = reg.rx_endpoint(member) {
                                    ep.port = src_port;
                                    endpoints.push(ep);
                                }
                            }
                        }
                        endpoints.sort_by(|a, b| a.ip.cmp(&b.ip).then_with(|| a.port.cmp(&b.port)));
                        endpoints.dedup_by(|a, b| a.ip == b.ip && a.port == b.port);
                    }
                    zones.push(ZoneEndpoints {
                        zone_id: group.zone_id.clone(),
                        endpoints,
                        source: None,
                    });
                }
                let dscp = dscp_of.get(tx_id).copied().unwrap_or(dscp_global);
                let changed = last_sent.get(tx_id) != Some(&(dscp, zones.clone()));
                // Форс-пуш по факту свежего hello (re-hello внутри окна дебаунса:
                // was_online ещё true, а новый процесс агента набора не знает).
                let fresh_hello = prev_hello.get(tx_id) != hello_now.get(tx_id);
                if !changed && !fresh_hello {
                    continue;
                }
                last_sent.insert(tx_id.clone(), (dscp, zones.clone()));
                match reg.send_cmd(tx_id, CtlMsg::SetEndpoints { zones, dscp }) {
                    Ok(()) => tracing::info!(tx = tx_id, dscp, "SetEndpoints отправлен tx-узлу"),
                    Err(RegistryError::Closed) => {
                        reg.mark_offline(tx_id);
                    }
                    Err(e) => {
                        tracing::warn!(tx = tx_id, "SetEndpoints не доставлен: {e}");
                    }
                }
            }
            // Start/Stop по diff играющего множества — строго после SetEndpoints:
            // агент сначала получает актуальный набор, потом Start.
            // Start также уходит играющим со свежим hello: reconnect внутри
            // окна дебаунса (offline+online в одном проходе) не даёт diff'а,
            // а новый процесс агента потерял want_playing — без Start молчал
            // бы до изменения раскладки. Union с diff'ом, дедуп — HashSet.
            let mut to_start: HashSet<String> =
                playing.difference(&prev_playing).cloned().collect();
            for id in &playing {
                if prev_hello.get(id) != hello_now.get(id) {
                    to_start.insert(id.clone());
                }
            }
            let mut to_start: Vec<String> = to_start.into_iter().collect();
            to_start.sort();
            for id in &to_start {
                match reg.send_cmd(id, CtlMsg::Start) {
                    Ok(()) => tracing::info!(node = id, "Start отправлен (играющее множество)"),
                    Err(RegistryError::Closed) => {
                        reg.mark_offline(id);
                    }
                    Err(e) => tracing::warn!(node = id, "Start не доставлен: {e}"),
                }
            }
            let mut to_stop: Vec<String> = prev_playing.difference(&playing).cloned().collect();
            to_stop.sort();
            for id in &to_stop {
                match reg.send_cmd(id, CtlMsg::Stop) {
                    Ok(()) => tracing::info!(node = id, "Stop отправлен (играющее множество)"),
                    Err(RegistryError::Closed) => {
                        reg.mark_offline(id);
                    }
                    Err(e) => tracing::warn!(node = id, "Stop не доставлен: {e}"),
                }
            }
            // Карты — после блока Start/Stop, пока держим reg. Холд тишины
            // карту не меняет и этот проход не форсит: пуш, только если карта
            // отличается от last_maps или hello rx свежий. На пуске зоны rx
            // получает Start, затем SetChannelMap.
            last_maps.retain(|id, _| maps.contains_key(id));
            let mut map_ids: Vec<String> = maps.keys().cloned().collect();
            map_ids.sort();
            for id in &map_ids {
                if reg.rx_endpoint(id).is_none() {
                    continue;
                }
                let found = reg.playback_outputs(id);
                let dev = reg.playback_device_of(id);
                let found = if found.is_empty() {
                    crate::inputs::fallback_outputs(dev.as_deref())
                } else {
                    found
                };
                let views = crate::inputs::merge_inputs(
                    &found,
                    output_prefs.get(id).map(|p| p.as_slice()).unwrap_or(&[]),
                    dev.as_deref(),
                );
                let Some(zone) = owners.get(id).and_then(|zid| zones.iter().find(|z| &z.id == zid))
                else {
                    continue;
                };
                let CtlMsg::SetChannelMap { map, branches } =
                    zone_channel_cmd(zone, id, &tx_port, &views)
                else {
                    continue;
                };
                let fresh_hello = prev_hello.get(id) != hello_now.get(id);
                let sent = (map, branches.clone());
                if last_maps.get(id) == Some(&sent) && !fresh_hello {
                    continue;
                }
                last_maps.insert(id.clone(), sent);
                match reg.send_cmd(id, CtlMsg::SetChannelMap { map, branches }) {
                    Ok(()) => tracing::info!(node = id, "SetChannelMap отправлен владельцу"),
                    Err(RegistryError::Closed) => {
                        reg.mark_offline(id);
                    }
                    Err(e) => tracing::warn!(node = id, "SetChannelMap не доставлен: {e}"),
                }
            }
            // SetMix после Start и карты: тесты и агент сначала видят пуск.
            // Холд тишины членов не выкидывает — порт колонки тот же, пакетов нет.
            let mut heard: HashMap<String, Vec<u16>> = HashMap::new();
            for tx_id in &online_tx {
                let Some(&port) = tx_port.get(tx_id) else { continue };
                for group in groups_of.get(tx_id).into_iter().flatten() {
                    for member in &group.members {
                        if reg.rx_endpoint(member).is_none() {
                            continue;
                        }
                        let slots = heard.entry(member.clone()).or_default();
                        if !slots.contains(&port) {
                            slots.push(port);
                        }
                    }
                }
            }
            for slots in heard.values_mut() {
                slots.sort();
            }
            last_mix.retain(|id, _| reg.rx_endpoint(id).is_some());
            for id in reg.online_rx() {
                let ports = heard.get(&id).cloned().unwrap_or_default();
                let jb_ms = speaker_jb_ms(&owners, &zones, &id);
                let fresh_hello = prev_hello.get(&id) != hello_now.get(&id);
                let changed = match last_mix.get(&id) {
                    Some((old_ports, old_jb)) => old_ports != &ports || *old_jb != jb_ms,
                    None => !ports.is_empty(),
                };
                if !changed && !(fresh_hello && !ports.is_empty()) {
                    continue;
                }
                last_mix.insert(id.clone(), (ports.clone(), jb_ms));
                match reg.send_cmd(&id, CtlMsg::SetMix { ports, jb_ms }) {
                    Ok(()) => tracing::info!(node = id, "SetMix отправлен"),
                    Err(RegistryError::Closed) => {
                        reg.mark_offline(&id);
                    }
                    Err(e) => tracing::warn!(node = id, "SetMix не доставлен: {e}"),
                }
            }
            prev_playing = playing;
            prev_hello = hello_now;
        }
    })
}


/// SetChannelMap колонки. Один источник — карта после микшера.
/// Несколько — карта на порт каждого источника, выход микшера сквозной.
fn zone_channel_cmd(
    zone: &Zone,
    member: &str,
    port_of: &HashMap<String, u16>,
    views: &[crate::inputs::InputView],
) -> CtlMsg {
    let per_source = source_channel_maps(zone, member);
    if per_source.is_empty() {
        let map = zone.maps.get(member).copied().unwrap_or_default();
        return CtlMsg::SetChannelMap {
            map: crate::inputs::mask_disabled_outputs(map, views),
            branches: vec![],
        };
    }
    let mut branches = Vec::new();
    for (src, map) in per_source {
        let Some(&port) = port_of.get(&src) else { continue };
        branches.push(BranchChannelMap {
            port,
            map: crate::inputs::mask_disabled_outputs(map, views),
        });
    }
    branches.sort_by_key(|b| b.port);
    CtlMsg::SetChannelMap {
        map: crate::inputs::mask_disabled_outputs(ChannelMap::default(), views),
        branches,
    }
}

/// Запас колонки — `jb_ms` зоны, которая её сейчас держит. Нет владельца — 2.
fn speaker_jb_ms(owners: &HashMap<String, String>, zones: &[Zone], id: &str) -> u32 {
    owners
        .get(id)
        .and_then(|zid| zones.iter().find(|z| z.id == *zid))
        .map(|z| z.jb_ms)
        .unwrap_or_else(default_jb_ms)
}

fn mix_ports_for(reg: &Registry, online_tx: &[String]) -> HashMap<String, u16> {
    let names: Vec<String> = online_tx.iter().filter_map(|id| reg.name_of(id)).collect();
    let assigned = crate::mix::assign_mix_ports(&names);
    let mut out = HashMap::new();
    for id in online_tx {
        let Some(name) = reg.name_of(id) else { continue };
        if let Some((_, port)) = assigned.iter().find(|(n, _)| n == &name) {
            out.insert(id.clone(), *port);
        }
    }
    if out.len() != online_tx.len() {
        tracing::warn!(
            have = out.len(),
            want = online_tx.len(),
            "слоты микса кончились — часть источников не получит порт"
        );
    }
    out
}

async fn ws_handler(
    State(st): State<AppState>,
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, st, addr.ip().to_string()))
}

fn tx_zones(state: &MasterState, node_id: &str) -> Vec<VirtualZone> {
    let mut zones = Vec::new();
    for zone in &state.zones {
        if zone.source_ids().iter().any(|id| id == node_id) {
            zones.push(VirtualZone {
                zone_id: zone.id.clone(),
                name: zone.name.clone(),
            });
        }
    }
    zones.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
    zones
}

fn node_soloist_key_raw(raw: Option<&str>) -> Option<String> {
    let key = raw?.trim();
    if key.is_empty() { None } else { Some(key.to_string()) }
}

fn soloist_cmd(state: &MasterState, node_id: &str) -> CtlMsg {
    let meta = state.nodes.get(node_id);
    let key = meta
        .filter(|n| n.spotify)
        .and_then(|n| node_soloist_key_raw(n.soloist_key.as_deref()));
    CtlMsg::SetSoloist {
        api_key: key.map(SecretString),
        zones: tx_zones(state, node_id),
    }
}

fn sendspin_cmd(state: &MasterState, node_id: &str) -> CtlMsg {
    let zones = if state.nodes.get(node_id).is_some_and(|n| n.sendspin) {
        tx_zones(state, node_id)
    } else {
        Vec::new()
    };
    CtlMsg::SetSendspin { zones }
}

fn virtual_zones_cmd(state: &MasterState, node_id: &str) -> CtlMsg {
    let zones = if state.nodes.get(node_id).is_some_and(|n| n.virtual_output) {
        tx_zones(state, node_id)
    } else {
        Vec::new()
    };
    CtlMsg::SetVirtualZones { zones }
}

async fn push_virtual_zones(st: &AppState, node_id: &str) {
    let (zones, soloist, sendspin) = {
        let store = st.store.lock().await;
        let state = store.state();
        (
            virtual_zones_cmd(state, node_id),
            soloist_cmd(state, node_id),
            sendspin_cmd(state, node_id),
        )
    };
    let reg = st.reg.lock().await;
    if reg.supports_virtual_output(node_id) {
        let _ = reg.send_cmd(node_id, zones);
        let _ = reg.send_cmd(node_id, soloist);
        let _ = reg.send_cmd(node_id, sendspin);
    }
}

fn push_capture_target(reg: &mut Registry, node_id: &str, prefs: &[crate::inputs::InputPref], capture_device: Option<&str>) {
    if !reg.supports_virtual_output(node_id) {
        return;
    }
    let found = reg.capture_inputs(node_id);
    let views = crate::inputs::merge_inputs(&found, prefs, capture_device);
    let device_key = crate::inputs::capture_target(&views);
    let _ = reg.send_cmd(node_id, CtlMsg::SetCaptureTarget { device_key });
}

async fn handle_socket(socket: WebSocket, st: AppState, ip: String) {
    let (mut sink, mut stream) = socket.split();

    // Первый кадр обязан быть Hello — иначе закрываем соединение.
    let (node_id, name, roles, version, capture_device, playback_device, inputs, outputs, supports_virtual_output, link) = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => match serde_json::from_str::<AgentMsg>(&text) {
                Ok(AgentMsg::Hello {
                    node_id,
                    name,
                    roles,
                    version,
                    capture_device,
                    playback_device,
                    inputs,
                    outputs,
                    supports_virtual_output,
                    link,
                }) => break (node_id, name, roles, version, capture_device, playback_device, inputs, outputs, supports_virtual_output, link),
                _ => {
                    tracing::warn!("ws: первое сообщение не Hello — закрываем");
                    return;
                }
            },
            Some(Ok(Message::Close(_))) | None => return,
            // ping/pong tungstenite обрабатывает сам
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                tracing::warn!("ws: ошибка соединения до Hello: {e}");
                return;
            }
        }
    };

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<CtlMsg>(16);
    // Проба для финального mark_offline: после re-hello канал в реестре
    // заменён, и stale-хендлер не должен гасить уже живой узел.
    let my_tx = cmd_tx.clone();
    st.reg
        .lock()
        .await
        .hello(&node_id, &name, roles.clone(), version, capture_device.clone(), playback_device.clone(), ip, cmd_tx);
    let reported = if inputs.is_empty() && roles.contains(&Role::Tx) {
        crate::inputs::fallback_inputs(capture_device.as_deref())
    } else {
        inputs
    };
    let reported_out = if outputs.is_empty() && roles.contains(&Role::Rx) {
        crate::inputs::fallback_outputs(playback_device.as_deref())
    } else {
        outputs
    };
    {
        let mut reg = st.reg.lock().await;
        reg.set_capture_inputs(&node_id, reported);
        reg.set_playback_outputs(&node_id, reported_out.clone());
        reg.set_supports_virtual_output(&node_id, supports_virtual_output);
        reg.set_link(&node_id, link);
    }
    tracing::info!(node = name, %node_id, "узел зарегистрирован");

    // Карта зоны-владельца, если узел — член активной зоны. Выключенный
    // выход из настроек узла в эту карту не входит. Замки по одному.
    let owned = {
        let store = st.store.lock().await;
        let stt = store.state();
        let zone = owner_ids(stt).get(&node_id).and_then(|zid| {
            stt.zones.iter().find(|z| &z.id == zid).cloned()
        });
        let prefs = stt.nodes.get(&node_id).map(|n| n.outputs.clone()).unwrap_or_default();
        (zone, prefs)
    };
    if let Some(zone) = owned.0 {
        let views = crate::inputs::merge_inputs(&reported_out, &owned.1, playback_device.as_deref());
        let reg = st.reg.lock().await;
        let ports = mix_ports_for(&reg, &reg.online_tx());
        let cmd = zone_channel_cmd(&zone, &node_id, &ports, &views);
        if let Err(e) = reg.send_cmd(&node_id, cmd) {
            tracing::info!(node = name, %node_id, "SetChannelMap зоны на hello не доставлен: {e}");
        }
    }

    if supports_virtual_output {
        let (enabled, prefs, zones_cmd, soloist, sendspin) = {
            let store = st.store.lock().await;
            let state = store.state();
            let meta = state.nodes.get(&node_id);
            (
                meta.is_some_and(|n| n.virtual_output),
                meta.map(|n| n.inputs.clone()).unwrap_or_default(),
                virtual_zones_cmd(state, &node_id),
                soloist_cmd(state, &node_id),
                sendspin_cmd(state, &node_id),
            )
        };
        let mut reg = st.reg.lock().await;
        let _ = reg.send_cmd(&node_id, CtlMsg::SetVirtualOutput { enabled });
        let capture_device = reg.capture_device_of(&node_id);
        push_capture_target(&mut reg, &node_id, &prefs, capture_device.as_deref());
        let _ = reg.send_cmd(&node_id, zones_cmd);
        let _ = reg.send_cmd(&node_id, soloist);
        let _ = reg.send_cmd(&node_id, sendspin);
    }

    loop {
        tokio::select! {
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<AgentMsg>(&text) {
                    Ok(AgentMsg::Telemetry(t)) => st.reg.lock().await.telemetry(&node_id, t),
                    Ok(AgentMsg::Event { kind, detail }) => {
                        st.reg.lock().await.event(&node_id, kind, detail)
                    }
                    Ok(AgentMsg::Hello { .. }) => {
                        tracing::warn!(node = name, %node_id, "повторный Hello по живому сокету — игнорируем")
                    }
                    Ok(AgentMsg::Inputs { inputs }) => {
                        let prefs = {
                            let store = st.store.lock().await;
                            store.state().nodes.get(&node_id).map(|n| n.inputs.clone()).unwrap_or_default()
                        };
                        let mut reg = st.reg.lock().await;
                        reg.set_capture_inputs(&node_id, inputs);
                        let capture_device = reg.capture_device_of(&node_id);
                        push_capture_target(&mut reg, &node_id, &prefs, capture_device.as_deref());
                    }
                    Err(e) => tracing::warn!(node = name, "ws: неразобранное сообщение: {e}: {text}"),
                },
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    tracing::warn!(node = name, "ws: ошибка соединения: {e}");
                    break;
                }
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(ctl) => {
                    let text = serde_json::to_string(&ctl).unwrap_or_default();
                    if sink.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                // Все sender'ы дропнуты (канал заменён re-hello) — выходим.
                None => break,
            },
        }
    }

    let mut reg = st.reg.lock().await;
    if reg.is_current_tx(&node_id, &my_tx) {
        reg.mark_offline(&node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_builds_router() {
        // Маршруты собираются в валидный Router (синтаксис путей и т.п.).
        let _ = app(AppState::new());
    }

    /// wait_controller_up: против мёртвого порта — Err не раньше дедлайна;
    /// против поднявшегося TCP-listener (имитация контроллера) — Ok.
    #[tokio::test]
    async fn wait_controller_up_polls_tcp_connect() {
        // 127.0.0.2:8080 никто не слушает → таймаут
        let t0 = Instant::now();
        let err = wait_controller_up("127.0.0.2", Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.contains("127.0.0.2:8080"), "{err}");
        assert!(
            t0.elapsed() >= Duration::from_secs(1),
            "таймаут не раньше дедлайна: {:?}",
            t0.elapsed()
        );

        // «контроллер поднялся» — listener на ip:CONTROLLER_PORT
        let listener = tokio::net::TcpListener::bind(("127.0.0.2", CONTROLLER_PORT))
            .await
            .unwrap();
        let _held = listener.local_addr().unwrap(); // listener жив до конца теста
        wait_controller_up("127.0.0.2", Duration::from_secs(3))
            .await
            .expect("контроллер на 127.0.0.2:8080 поднялся");
    }

    /// C1: маркер преемника одноразово пропускает discovery-предпроверку —
    /// контроллер легитимно стартует при живом старом мастере (трансфер),
    /// маркер при этом потреблён; повторный старт без маркера снова падает
    /// с «уже есть мастер», как и любой посторонний контроллер.
    #[tokio::test]
    async fn successor_marker_skips_precheck_once() {
        // живой мастер в сети — fake discovery responder
        let dest = "127.0.0.1:15321";
        let responder = tokio::spawn(crate::discovery::responder(dest));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let dir = std::env::temp_dir().join(format!("zvsm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let state_path = dir.join("state.json");
        std::fs::remove_file(&state_path).ok();
        std::fs::remove_file(successor_marker_path(&state_path)).ok();

        // без маркера — отказ, как раньше (защита от второго мастера)
        let err = master_precheck(&state_path, dest).await.unwrap_err();
        assert!(
            err.to_string().contains("уже есть мастер"),
            "без маркера при живом мастере — bail: {err:#}"
        );

        // с маркером — старт разрешён и маркер удалён
        std::fs::write(successor_marker_path(&state_path), b"").unwrap();
        master_precheck(&state_path, dest)
            .await
            .expect("маркер преемника пропускает предпроверку");
        assert!(
            !successor_marker_path(&state_path).exists(),
            "маркер одноразовый — потреблён при старте"
        );

        // повторный старт без маркера — снова отказ
        let err = master_precheck(&state_path, dest).await.unwrap_err();
        assert!(
            err.to_string().contains("уже есть мастер"),
            "повторный старт без маркера — снова bail: {err:#}"
        );

        responder.abort();
        std::fs::remove_dir_all(&dir).ok();
    }
}
