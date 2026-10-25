use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use zproto::{AgentMsg, CtlMsg, EventKind, LinkKind, Role, SoloistState, Telemetry};

use crate::config::AgentConfig;
use crate::discovery::{query_bound, DISCOVERY_PORT};
use crate::uplink::ActiveLink;

const TELEM_PERIOD: Duration = Duration::from_secs(2);
const BACKOFF_INITIAL: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Таймаут discovery-запроса перед dial'ом.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(1);

/// Параметры discovery перед каждым подключением: куда слать WHO_IS_MASTER
/// и сколько ждать ответа. Найденный адрес внутри сессии имеет приоритет над
/// `cfg.controller`. Дефолт — broadcast на порт discovery; в тестах —
/// loopback-юникаст с коротким таймаутом.
pub struct Discovery {
    pub dest: String,
    pub timeout: Duration,
}

impl Default for Discovery {
    fn default() -> Self {
        Self {
            dest: format!("255.255.255.255:{DISCOVERY_PORT}"),
            timeout: DISCOVERY_TIMEOUT,
        }
    }
}

/// Бесконечный цикл подключения к контроллеру: перед каждым dial'ом —
/// discovery-запрос (найденный мастер запоминается в `current_addr` на
/// сессию), реконнект с экспоненциальным бэкоффом (500 мс → 30 с).
/// Возвращает Err немедленно при фатальной ошибке — закрытии cmds-канала
/// (потребитель команд исчез) или events-канала (gst-домен агента
/// завершился): вызывающий код обязан завершить процесс, systemd его
/// перезапустит. Ошибки самого соединения — warn + ретрай.
///
/// `ws_connected` — флаг stub-сервера агента: true, пока сессия с мастером
/// жива (ставится при connect, снимается при завершении сессии).
///
/// `uplink` — тот же приёмник, что у часов и звука. Нет активного интерфейса —
/// discovery и набор не вызываются, цикл ждёт публикацию. Смена имени рвёт сессию.
///
/// `pending_finished` — detail `UpdateFinished` из файла apply-скрипта. Агент
/// уже удалил файл. Событие уходит один раз, первым после Hello.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    cfg: AgentConfig,
    discovery: Discovery,
    node_id: &str,
    version: String,
    roles: Vec<Role>,
    inputs: Vec<zproto::CaptureChan>,
    outputs: Vec<zproto::CaptureChan>,
    supports_virtual_output: bool,
    telem: watch::Receiver<Telemetry>,
    ptp_offset: watch::Receiver<i64>,
    ws_connected: Arc<AtomicBool>,
    cmds: mpsc::Sender<CtlMsg>,
    mut events: mpsc::Receiver<AgentMsg>,
    mut uplink: watch::Receiver<Option<ActiveLink>>,
    mut pending_finished: Option<String>,
) -> Result<()> {
    let mut backoff = BACKOFF_INITIAL;
    // Окно между ApplyUpdate и systemd-run. Второй команде в этом окне
    // отвечаем отказом, не начиная второе скачивание.
    let updating = Arc::new(AtomicBool::new(false));
    let (update_tx, mut update_rx) = mpsc::channel::<AgentMsg>(8);
    // Адрес текущей сессии: стартуем с cfg.controller, discovery может
    // переключить на найденного мастера.
    let mut current_addr = cfg.controller.clone();
    // ControllerMoved задаёт адрес авторитетно (приказ живого контроллера):
    // молчащий discovery не должен перезаписывать его cfg.controller на
    // ближайшем реконнекте.
    let mut moved_by_controller = false;
    loop {
        // Пока линка нет, query_bound и dial не вызываем. Сон прерывается
        // публикацией, чтобы не ждать полный бэкофф после появления интерфейса.
        if uplink.borrow().is_none() {
            tracing::warn!("ws: активного интерфейса нет — discovery-запрос не уходит");
            let appeared = tokio::select! {
                _ = tokio::time::sleep(backoff) => false,
                changed = uplink.changed() => {
                    changed.context("наблюдатель интерфейса закрылся")?;
                    true
                }
            };
            if !appeared {
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
            if cmds.is_closed() || events.is_closed() {
                anyhow::bail!("канал команд или событий закрыт");
            }
            continue;
        }
        match connect_once(
            &cfg,
            &discovery,
            &mut current_addr,
            &mut moved_by_controller,
            node_id,
            &version,
            &roles,
            &inputs,
            &outputs,
            supports_virtual_output,
            &telem,
            &ptp_offset,
            &ws_connected,
            &cmds,
            &mut events,
            &mut uplink,
            &updating,
            &update_tx,
            &mut update_rx,
            &mut pending_finished,
        )
        .await
        {
            Ok(()) => backoff = BACKOFF_INITIAL, // чистый разрыв — сброс бэкоффа
            Err(e) => {
                // Фатально: потребитель команд или gst-домен умер. Bail из
                // connect_once глотать ретраем нельзя — events.recv() после
                // смерти sender'а мгновенно даёт None, получился бы вечный
                // зомби-цикл с замёрзшей телеметрией. Прокидываем Err выше.
                if cmds.is_closed() || events.is_closed() {
                    return Err(e);
                }
                tracing::warn!("ws: {e:#}");
            }
        }
        // Сессия завершилась (любым исходом) — флаг ws-подключения снят.
        ws_connected.store(false, Ordering::Relaxed);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Куда dial'ить после одного ответа discovery.
///
/// Пока `moved` поднят (ControllerMoved, сессия на новом адресе ещё не
/// жила), чужой ответ и тишина не переписывают приказ. Иначе умирающий
/// старый мастер, который ещё секунду отвечает на WHO_IS_MASTER, утаскивает
/// агента обратно и сбрасывает флаг — следующий молчащий discovery уводит
/// его в `cfg_controller`.
fn reconcile_addr(
    current: &str,
    cfg_controller: &str,
    moved: &mut bool,
    discovered: Option<&str>,
) -> String {
    match discovered {
        Some(addr) if *moved && addr != current => current.to_string(),
        Some(addr) => {
            *moved = false;
            addr.to_string()
        }
        None if *moved => current.to_string(),
        None if current != cfg_controller => cfg_controller.to_string(),
        None => current.to_string(),
    }
}

/// Имена интерфейсов различаются, включая переход через отсутствие линка.
/// Сравниваются имена, не адреса: тот же интерфейс с другим IPv4 сессию не рвёт.
fn should_redial(prev: Option<&str>, next: Option<&str>) -> bool {
    prev != next
}

#[allow(clippy::too_many_arguments)]
async fn connect_once(
    cfg: &AgentConfig,
    discovery: &Discovery,
    current_addr: &mut String,
    moved_by_controller: &mut bool,
    node_id: &str,
    version: &str,
    roles: &[Role],
    inputs: &[zproto::CaptureChan],
    outputs: &[zproto::CaptureChan],
    supports_virtual_output: bool,
    telem: &watch::Receiver<Telemetry>,
    ptp_offset: &watch::Receiver<i64>,
    ws_connected: &AtomicBool,
    cmds: &mpsc::Sender<CtlMsg>,
    events: &mut mpsc::Receiver<AgentMsg>,
    uplink: &mut watch::Receiver<Option<ActiveLink>>,
    updating: &Arc<AtomicBool>,
    update_tx: &mpsc::Sender<AgentMsg>,
    update_rx: &mut mpsc::Receiver<AgentMsg>,
    pending_finished: &mut Option<String>,
) -> Result<()> {
    // Discovery и набор только с активного интерфейса. Нет линка — наружу,
    // внешний цикл подождёт публикацию и не вызовет query_bound.
    let Some(link) = uplink.borrow_and_update().clone() else {
        tracing::warn!("ws: активного интерфейса нет — discovery-запрос не уходит");
        return Ok(());
    };
    // Ответивший мастер имеет приоритет над cfg.controller и запоминается на
    // сессию; тишина — возврат к сконфигурированному адресу, но не если адрес
    // задан ControllerMoved. Ошибка привязки не фатальна: набор всё равно
    // уйдёт с активного интерфейса по текущему адресу.
    match query_bound(&discovery.dest, discovery.timeout, &link.name, link.ipv4).await {
        Ok(Some(addr)) => {
            let next = reconcile_addr(
                current_addr,
                &cfg.controller,
                moved_by_controller,
                Some(&addr),
            );
            if next != *current_addr {
                tracing::info!("ws: discovery — мастер {next} (было {current_addr})");
                *current_addr = next;
            } else if *moved_by_controller && addr != *current_addr {
                tracing::info!(
                    "ws: discovery говорит {addr}, держу ControllerMoved {current_addr}"
                );
            }
        }
        Ok(None) => {
            let next = reconcile_addr(current_addr, &cfg.controller, moved_by_controller, None);
            if next != *current_addr {
                tracing::info!("ws: discovery молчит — возвращаюсь к {next}");
                *current_addr = next;
            }
        }
        Err(e) => {
            tracing::warn!("ws: discovery-запрос не удался: {e:#}; продолжаю с {current_addr}")
        }
    }
    let Some(dial_link) = uplink.borrow_and_update().clone() else {
        tracing::warn!("ws: активного интерфейса нет — набор пропущен");
        return Ok(());
    };
    if should_redial(Some(link.name.as_str()), Some(dial_link.name.as_str())) {
        tracing::info!(
            "ws: интерфейс {} → {} до набора, сессия заново",
            link.name,
            dial_link.name
        );
        return Ok(());
    }
    let mut ws = dial_bound(current_addr, &dial_link)
        .await
        .with_context(|| format!("подключение к {current_addr}"))?;
    ws_connected.store(true, Ordering::Relaxed);
    let session_iface = dial_link.name;

    let hello = AgentMsg::Hello {
        node_id: node_id.to_string(),
        name: cfg.name.clone(),
        roles: roles.to_vec(),
        version: version.to_string(),
        target: crate::update::baked_target().to_string(),
        // Пока команда не задала цель, Hello несёт null. Это открытое устройство,
        // не ключ из конфига: capture_device и playback_device оттуда больше не читаются.
        capture_device: None,
        playback_device: None,
        inputs: inputs.to_vec(),
        outputs: outputs.to_vec(),
        supports_virtual_output,
        link: Some(if dial_link.wireless { LinkKind::Wifi } else { LinkKind::Wired }),
        setup: Some(vec![]),
        soloist: SoloistState::Unknown,
        soloist_command: String::new(),
    };
    ws.send(Message::Text(serde_json::to_string(&hello)?))
        .await?;
    tracing::info!("ws: подключено к {} как {}", current_addr, cfg.name);
    if let Some(detail) = pending_finished.clone() {
        let msg = AgentMsg::Event {
            kind: EventKind::UpdateFinished,
            detail,
        };
        ws.send(Message::Text(serde_json::to_string(&msg)?)).await?;
        pending_finished.take();
    }

    let mut ticker = tokio::time::interval(TELEM_PERIOD);
    ticker.tick().await; // мгновенный первый тик — пропускаем
                         // Отправитель наблюдателя живёт весь процесс. Если он всё же закрылся,
                         // ветку снимаем: иначе changed() сразу Err и select крутится без паузы.
    let mut watch_uplink = true;
    let mut watch_updates = true;

    loop {
        tokio::select! {
            changed = uplink.changed(), if watch_uplink => {
                match changed {
                    Err(_) => watch_uplink = false,
                    Ok(()) => {
                        let next = uplink.borrow().as_ref().map(|item| item.name.clone());
                        if should_redial(Some(session_iface.as_str()), next.as_deref()) {
                            tracing::info!(
                                "ws: интерфейс {session_iface} → {}, переподключение",
                                next.as_deref().unwrap_or("нет")
                            );
                            return Ok(());
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                let mut t = telem.borrow().clone();
                t.ptp_offset_ns = *ptp_offset.borrow();
                let msg = AgentMsg::Telemetry(t);
                ws.send(Message::Text(serde_json::to_string(&msg)?)).await?;
            }
            frame = ws.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<CtlMsg>(&text) {
                            Ok(ctl) => {
                                // ControllerMoved — транспортная команда
                                // ws-домена: адрес реконнекта меняем здесь,
                                // сокет закрываем (return Ok), в cmds не
                                // передаём — gst-домен её не видит.
                                if let CtlMsg::ControllerMoved { addr } = &ctl {
                                    tracing::info!("ws: ControllerMoved — мастер теперь на {addr}");
                                    *current_addr = addr.clone();
                                    *moved_by_controller = true;
                                    return Ok(());
                                }
                                if let CtlMsg::ApplyUpdate { version: to, url, sha256 } = &ctl {
                                    let to = to.clone();
                                    let url = url.clone();
                                    let sha256 = sha256.clone();
                                    if updating.swap(true, Ordering::AcqRel) {
                                        let msg = AgentMsg::Event {
                                            kind: EventKind::UpdateFailed,
                                            detail: "уже обновляется".into(),
                                        };
                                        ws.send(Message::Text(serde_json::to_string(&msg)?)).await?;
                                    } else {
                                        let updating = Arc::clone(updating);
                                        let tx = update_tx.clone();
                                        let from = version.to_string();
                                        tokio::spawn(async move {
                                            let outcome = run_apply(url, sha256, from, to).await;
                                            updating.store(false, Ordering::Release);
                                            let msg = match outcome {
                                                // restart не сменил процесс: файл
                                                // rolled_back уже есть, а take_apply_result
                                                // на старте его не перечитает. Новый
                                                // процесс, если поднимется, пришлёт то же.
                                                Ok(Some(detail)) => AgentMsg::Event {
                                                    kind: EventKind::UpdateFinished,
                                                    detail: detail.to_string(),
                                                },
                                                Ok(None) => return,
                                                Err(detail) => {
                                                    tracing::warn!("обновление: {detail}");
                                                    AgentMsg::Event {
                                                        kind: EventKind::UpdateFailed,
                                                        detail,
                                                    }
                                                }
                                            };
                                            let _ = tx.send(msg).await;
                                        });
                                    }
                                } else if cmds.send(ctl).await.is_err() {
                                    anyhow::bail!("канал команд закрыт");
                                }
                            }
                            Err(e) => {
                                let shown = if text.contains("api_key") { "<скрыт>" } else { text.as_str() };
                                tracing::warn!("ws: неразобранный CtlMsg: {e}: {shown}");
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    // ping/pong/binary: tungstenite отвечает на пинги сам
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e).context("ошибка соединения"),
                }
            }
            // события от gst-домена (Failed/Underrun/Restarted) → контроллер;
            // закрытый канал = смерть gst-домена → фатально для агента
            event = events.recv() => {
                match event {
                    Some(msg) => {
                        ws.send(Message::Text(serde_json::to_string(&msg)?)).await?;
                    }
                    None => anyhow::bail!("канал событий закрыт (gst-домен завершился)"),
                }
            }
            update = update_rx.recv(), if watch_updates => {
                match update {
                    Some(msg) => {
                        ws.send(Message::Text(serde_json::to_string(&msg)?)).await?;
                    }
                    None => watch_updates = false,
                }
            }
        }
    }
}

/// TCP с активного интерфейса: socket2 `STREAM`, `SO_BINDTODEVICE`,
/// `bind(ipv4:0)`, nonblocking, `connect`, затем `TcpStream::from_std`
/// и `client_async` на этом потоке.
async fn dial_bound(
    url: &str,
    link: &ActiveLink,
) -> Result<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>> {
    let peer = master_ipv4(url).await?;
    let sock = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    crate::bindiface::bind_device(&sock, &link.name)
        .with_context(|| format!("SO_BINDTODEVICE {}", link.name))?;
    sock.bind(&std::net::SocketAddrV4::new(link.ipv4, 0).into())
        .with_context(|| format!("bind {}:0", link.ipv4))?;
    sock.set_nonblocking(true)?;
    match sock.connect(&socket2::SockAddr::from(peer)) {
        Ok(()) => {}
        Err(err) if connect_pending(&err) => {}
        Err(err) => return Err(err).with_context(|| format!("connect {peer}")),
    }
    let stream = tokio::net::TcpStream::from_std(std::net::TcpStream::from(sock))?;
    stream.writable().await?;
    if let Some(err) = stream.take_error()? {
        return Err(err).with_context(|| format!("connect {peer}"));
    }
    let (ws, _) = tokio_tungstenite::client_async(url, stream)
        .await
        .with_context(|| format!("рукопожатие websocket с {url}"))?;
    Ok(ws)
}

async fn run_apply(url: String, sha256: String, from: String, to: String) -> Result<Option<&'static str>, String> {
    let bytes = crate::update::download_asset(&url, crate::update::baked_target(), &sha256).await?;
    tokio::task::spawn_blocking(move || stage_apply(bytes, sha256, from, to))
        .await
        .map_err(|e| e.to_string())?
}

fn stage_apply(bytes: Vec<u8>, sha256: String, from: String, to: String) -> Result<Option<&'static str>, String> {
    // Свой каталог на попытку: вторая запись не обрезает apply-update.sh,
    // который bash первой службы ещё читает. Имя юнита общее и по-прежнему отказывает.
    let dir = std::path::Path::new("/run").join(format!(
        "zvucast-update-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let file = dir.join("zvucast");
    std::fs::write(&file, &bytes).map_err(|e| e.to_string())?;
    let script = crate::update::write_script(&dir).map_err(|e| e.to_string())?;
    crate::update::spawn_apply(&script, &file, &sha256, &from, &to).map_err(|e| e.to_string())?;
    tracing::info!("обновление: служба zvucast-apply-update запущена");
    // Рестарт из скрипта убивает этот процесс на успехе и на откате. Ожидание
    // без `--wait`: если нас убьют, событие не уходит, новый процесс читает
    // файл. Если юнит кончился, а мы живы: нет `ok`/`rolled_back` —
    // UpdateFailed; `rolled_back` — UpdateFinished, процесс restart не сменил.
    crate::update::wait_apply_unit()
}

/// Неблокирующий `connect` ещё не завершился. `EINPROGRESS` на Linux — 115;
/// `ErrorKind::InProgress` пока нестабилен.
fn connect_pending(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::WouldBlock || err.raw_os_error() == Some(115)
}

/// `ws://host:port/path` → первый IPv4. Набор идёт только по v4 активного линка.
async fn master_ipv4(url: &str) -> Result<std::net::SocketAddr> {
    let rest = url
        .strip_prefix("ws://")
        .with_context(|| format!("адрес мастера не ws://: {url}"))?;
    let hostport = rest.split('/').next().unwrap_or(rest);
    let (host, port_str) = hostport
        .rsplit_once(':')
        .with_context(|| format!("в адресе мастера нет порта: {url}"))?;
    let port: u16 = port_str
        .parse()
        .with_context(|| format!("порт мастера: {port_str}"))?;
    tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("разрешение {host}:{port}"))?
        .find(|addr| addr.is_ipv4())
        .with_context(|| format!("у мастера нет IPv4: {host}"))
}

#[cfg(test)]
mod tests {
    use super::{reconcile_addr, should_redial};

    #[test]
    fn dying_master_does_not_override_controller_moved() {
        let cfg = "ws://192.0.2.204:8080/ws";
        let moved_to = "ws://192.0.2.207:8080/ws";
        let mut moved = true;
        // старый мастер ещё отвечает
        let next = reconcile_addr(moved_to, cfg, &mut moved, Some(cfg));
        assert_eq!(next, moved_to);
        assert!(moved, "приказ держится, пока discovery врёт старым адресом");
        // старый замолчал — тоже не откатываемся в cfg
        let next = reconcile_addr(moved_to, cfg, &mut moved, None);
        assert_eq!(next, moved_to);
        assert!(moved);
        // новый мастер подтвердил себя — флаг можно снять
        let mut moved = true;
        let next = reconcile_addr(moved_to, cfg, &mut moved, Some(moved_to));
        assert_eq!(next, moved_to);
        assert!(!moved);
    }

    #[test]
    fn silent_discovery_returns_to_configured_without_move() {
        let cfg = "ws://192.0.2.204:8080/ws";
        let stale = "ws://192.0.2.207:8080/ws";
        let mut moved = false;
        let next = reconcile_addr(stale, cfg, &mut moved, None);
        assert_eq!(next, cfg);
    }

    #[test]
    fn link_change_is_a_reason_to_redial() {
        assert!(should_redial(Some("eth0"), Some("wlan0")));
        assert!(!should_redial(Some("eth0"), Some("eth0")));
        assert!(should_redial(Some("eth0"), None));
        assert!(should_redial(None, Some("wlan0")));
    }
}
