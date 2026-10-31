//! Встроенная PTP-синхронизация системных часов (zsync).
//!
//! Адаптация демона statime-linux (crates `statime` + `statime-linux`,
// Copyright (c) 2023-2024 Pendulum Project — лицензии MIT и Apache-2.0).
//! В отличие от демона, здесь один порт на инстанс, поэтому event-loop
//! владеет портом напрямую и BMCA выполняется инлайн (stop-the-world),
//! без пересылки порта между задачами. Сессия живёт на одном интерфейсе:
//! смена имени закрывает порт и инстанс, следующая открывает их заново.

use std::{
    future::Future,
    net::SocketAddrV4,
    pin::Pin,
    sync::RwLock,
    task::{Context, Poll},
    time::Duration as StdDuration,
};

use anyhow::{bail, Result};
use rand::{rngs::StdRng, Rng, SeedableRng};
use statime::{
    config::{
        AcceptAnyMaster, ClockIdentity, DelayMechanism, InstanceConfig, PortConfig,
        PtpMinorVersion, SdoId, TimePropertiesDS, TimeSource,
    },
    filters::{KalmanConfiguration, KalmanFilter},
    port::{
        is_message_buffer_compatible, PortAction, PortActionIterator, TimestampContext,
        MAX_DATA_LEN,
    },
    time::{Duration, Interval, Time},
    PtpInstance, PtpInstanceState,
};
use statime_linux::{
    clock::{LinuxClock, PortTimestampToTime},
    socket::{open_ipv4_event_socket, open_ipv4_general_socket, PtpTargetAddress},
    tlvforwarder::TlvForwarder,
};
use timestamped_socket::{
    interface::{interfaces, InterfaceName},
    socket::{InterfaceTimestampMode, Socket},
};
use tokio::sync::watch;

/// Хендл фоновой задачи синхронизации.
///
/// Задача завершается, когда сброшены оба приёмника — `offset_ns` и `state`.
pub struct SyncHandle {
    /// Текущее смещение от мастера, наносекунды (0, когда узел — текущий GM;
    /// до первой оценки фильтра и без порта — i64::MAX как признак «нет данных»).
    pub offset_ns: watch::Receiver<i64>,
    /// Текущее состояние порта PTP (Idle/Listening/Slave/Master/...).
    pub state: watch::Receiver<String>,
    /// Желаемый интерфейс. `None` — порт закрыт.
    iface_tx: watch::Sender<Option<String>>,
}

impl SyncHandle {
    /// Переключить PTP на другой интерфейс.
    ///
    /// `None` закрывает порт и возвращает состояние `Idle` со смещением
    /// `i64::MAX`. `Some` закрывает прежнюю сессию и открывает новую.
    pub fn set_interface(&self, interface: Option<String>) {
        let _ = self.iface_tx.send(interface);
    }
}

type Instance = PtpInstance<KalmanFilter, RwLock<PtpInstanceState>>;

/// Пауза после ошибки recv: персистентная ошибка сокета (recv возвращает Err
/// мгновенно) иначе крутит select! без единого await — busy-loop на 100% CPU.
const RECV_ERROR_BACKOFF: StdDuration = StdDuration::from_millis(100);

/// Пауза перед повтором того же имени, которое не удалось открыть.
const OPEN_RETRY: StdDuration = StdDuration::from_secs(1);

/// Чем кончилась сессия на одном интерфейсе.
enum SessionEnd {
    /// Оба приёмника смещения и состояния сброшены.
    Stopped,
    /// Запрошен другой интерфейс или его отсутствие.
    Switch,
    /// Имя не открылось. То же имя не пробуем чаще раза в [`OPEN_RETRY`].
    OpenFailed,
}

/// InstanceConfig для узла домена 0: priority1 — из конфига агента
/// (ptp_priority; меньше = лучше GM-кандидат). slave_only=false у всех:
/// GM выбирается BMCA, при падении текущего GM мастером становится
/// следующий по приоритету узел (перевыборы автоматические).
fn instance_config(priority1: u8) -> Result<InstanceConfig> {
    Ok(InstanceConfig {
        clock_identity: clock_identity(),
        priority_1: priority1,
        priority_2: 128,
        domain_number: 0,
        slave_only: false,
        sdo_id: SdoId::try_from(0).map_err(|()| anyhow::anyhow!("sdo_id вне диапазона"))?,
        path_trace: false,
    })
}

/// Запускает фоновую PTP-задачу без открытого порта.
///
/// Смещение `i64::MAX`, состояние `Idle`, пока [`SyncHandle::set_interface`]
/// не назовёт интерфейс. Все узлы равноправно участвуют в BMCA
/// (slave_only=false, master_only=false); GM выбирается по priority1
/// (меньше = лучше кандидат; pi4 = 100, остальные 128 по дефолту).
pub fn spawn(priority1: u8) -> SyncHandle {
    // До первой оценки фильтра и без порта — «нет данных».
    let (offset_tx, offset_rx) = watch::channel(i64::MAX);
    let (state_tx, state_rx) = watch::channel("Idle".to_string());
    let (iface_tx, iface_rx) = watch::channel(None);

    launch_background(move || {
        // Собственный однопоточный рантайм: изоляция таймингов PTP от
        // рантайма вызывающего кода и отсутствие требования к его flavor.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("zsync: не удалось создать tokio runtime");
        runtime.block_on(supervise(priority1, iface_rx, offset_tx, state_tx));
    });

    SyncHandle {
        offset_ns: offset_rx,
        state: state_rx,
        iface_tx,
    }
}

/// Запускает фоновую PTP-задачу на указанном интерфейсе.
///
/// Интерфейс должен уже существовать. Дальше порт живёт в задаче [`spawn`]
/// и переключается через [`SyncHandle::set_interface`].
pub fn start(priority1: u8, interface: &str) -> Result<SyncHandle> {
    let iface: InterfaceName = interface
        .parse()
        .map_err(|()| anyhow::anyhow!("некорректное имя интерфейса: {interface:?}"))?;
    if iface.get_index().is_none() {
        bail!("интерфейс не найден: {interface:?}");
    }

    let handle = spawn(priority1);
    handle.set_interface(Some(interface.to_string()));
    Ok(handle)
}

/// Внутри tokio — `spawn_blocking`, чтобы drop рантайма дождался выхода.
/// Юнит-тест зовёт [`spawn`] без рантайма: тогда отдельный поток, тело то же.
fn launch_background(body: impl FnOnce() + Send + 'static) {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::spawn_blocking(body);
    } else {
        std::thread::spawn(body);
    }
}

/// `false`, если сброшены оба приёмника и фоновую задачу пора завершать.
fn publish(
    offset_tx: &watch::Sender<i64>,
    state_tx: &watch::Sender<String>,
    offset: i64,
    state: &str,
) -> bool {
    let offset_alive = offset_tx.send(offset).is_ok();
    let state_alive = state_tx.send(state.to_string()).is_ok();
    offset_alive || state_alive
}

async fn both_receivers_closed(offset_tx: &watch::Sender<i64>, state_tx: &watch::Sender<String>) {
    tokio::join!(offset_tx.closed(), state_tx.closed());
}

/// Ждёт новое имя в простое. `true` — оба приёмника сброшены.
async fn idle_until_change(
    iface_rx: &mut watch::Receiver<Option<String>>,
    offset_tx: &watch::Sender<i64>,
    state_tx: &watch::Sender<String>,
) -> bool {
    if !publish(offset_tx, state_tx, i64::MAX, "Idle") {
        return true;
    }
    tokio::select! {
        result = iface_rx.changed() => {
            if result.is_err() {
                // Хендл разобрали (агент оставляет себе только offset_ns).
                // Новых имён не будет — ждём сброса оставшихся приёмников.
                both_receivers_closed(offset_tx, state_tx).await;
                true
            } else {
                false
            }
        }
        _ = both_receivers_closed(offset_tx, state_tx) => true,
    }
}

/// После неудачного открытия то же имя не трогаем секунду.
/// Смена имени прерывает ожидание сразу. `true` — оба приёмника сброшены.
async fn retry_pause(
    failed_name: &str,
    iface_rx: &mut watch::Receiver<Option<String>>,
    offset_tx: &watch::Sender<i64>,
    state_tx: &watch::Sender<String>,
) -> bool {
    let deadline = tokio::time::Instant::now() + OPEN_RETRY;
    // Отправитель мог уже умереть: changed() тогда сразу Err и крутил бы select.
    let mut iface_closed = false;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return false,
            result = iface_rx.changed(), if !iface_closed => {
                if result.is_err() {
                    iface_closed = true;
                    continue;
                }
                // То же имя — досиживаем секунду, даже если send повторили.
                if iface_rx.borrow().as_deref() != Some(failed_name) {
                    return false;
                }
            }
            _ = both_receivers_closed(offset_tx, state_tx) => return true,
        }
    }
}

async fn supervise(
    priority1: u8,
    mut iface_rx: watch::Receiver<Option<String>>,
    offset_tx: watch::Sender<i64>,
    state_tx: watch::Sender<String>,
) {
    loop {
        // Отдельный оператор: Ref от borrow живёт до конца выражения.
        let desired = iface_rx.borrow_and_update().clone();
        let Some(name) = desired else {
            if idle_until_change(&mut iface_rx, &offset_tx, &state_tx).await {
                return;
            }
            continue;
        };

        // Смена и ошибка открытия не должны оставлять смещение прошлой сессии.
        if !publish(&offset_tx, &state_tx, i64::MAX, "Initializing") {
            return;
        }
        match run_session(priority1, &name, &mut iface_rx, &offset_tx, &state_tx).await {
            SessionEnd::Stopped => return,
            SessionEnd::Switch => {}
            SessionEnd::OpenFailed => {
                if retry_pause(&name, &mut iface_rx, &offset_tx, &state_tx).await {
                    return;
                }
            }
        }
    }
}

/// Логарифм интервала sync и delay. 9 = 512 с, 0 = 1 с, −2 = 4 раза в секунду.
/// `ZVUCAST_PTP_LOG2` переключает тест без правки константы. Announce остаётся 2 с.
fn ptp_log2() -> i8 {
    std::env::var("ZVUCAST_PTP_LOG2")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(9)
}

fn port_config() -> PortConfig<AcceptAnyMaster> {
    let log2 = ptp_log2();
    PortConfig {
        acceptable_master_list: AcceptAnyMaster,
        delay_mechanism: DelayMechanism::E2E {
            interval: Interval::from_log_2(log2),
        },
        announce_interval: Interval::from_log_2(1),
        announce_receipt_timeout: 3,
        sync_interval: Interval::from_log_2(log2),
        master_only: false,
        delay_asymmetry: Duration::ZERO,
        minor_ptp_version: PtpMinorVersion::One,
    }
}

/// Одна сессия: свои сокеты и свой инстанс. Порт создаётся внутри
/// [`event_loop`] и дропается до возврата оттуда — раньше инстанса.
async fn run_session(
    priority1: u8,
    name: &str,
    iface_rx: &mut watch::Receiver<Option<String>>,
    offset_tx: &watch::Sender<i64>,
    state_tx: &watch::Sender<String>,
) -> SessionEnd {
    let iface: InterfaceName = match name.parse() {
        Ok(iface) => iface,
        Err(()) => {
            tracing::warn!("zsync: некорректное имя интерфейса: {name:?}");
            return SessionEnd::OpenFailed;
        }
    };

    let mut event_socket =
        match open_ipv4_event_socket(iface, InterfaceTimestampMode::SoftwareAll, None) {
            Ok(socket) => socket,
            Err(error) => {
                tracing::warn!(
                    "zsync: не удалось открыть UDPv4 event socket (319) на {name}: {error}"
                );
                return SessionEnd::OpenFailed;
            }
        };
    let mut general_socket = match open_ipv4_general_socket(iface) {
        Ok(socket) => socket,
        Err(error) => {
            tracing::warn!(
                "zsync: не удалось открыть UDPv4 general socket (320) на {name}: {error}"
            );
            return SessionEnd::OpenFailed;
        }
    };

    let instance_config = match instance_config(priority1) {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!("zsync: не удалось собрать конфиг PTP: {error:#}");
            return SessionEnd::OpenFailed;
        }
    };
    let time_properties =
        TimePropertiesDS::new_arbitrary_time(false, false, TimeSource::InternalOscillator);
    // Объявлен после сокетов: при выходе из функции инстанс дропается первым,
    // порт к этому моменту уже отпущен event_loop'ом.
    let instance = PtpInstance::new(instance_config, time_properties);

    event_loop(
        &instance,
        &mut event_socket,
        &mut general_socket,
        name,
        iface_rx,
        port_config(),
        offset_tx,
        state_tx,
    )
    .await
}

/// Единственный таймер одного из семейств портовых таймеров.
/// `Pin<Box<Sleep>>` внутри, поэтому тип сам по себе `Unpin` и проецируется
/// без pin-project.
struct Timer {
    sleep: Pin<Box<tokio::time::Sleep>>,
    running: bool,
}

impl Timer {
    fn new() -> Self {
        Timer {
            sleep: Box::pin(tokio::time::sleep(StdDuration::ZERO)),
            running: false,
        }
    }

    fn reset(&mut self, duration: StdDuration) {
        self.sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + duration);
        self.running = true;
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.running {
            let result = self.sleep.as_mut().poll(cx);
            if result != Poll::Pending {
                self.running = false;
            }
            result
        } else {
            Poll::Pending
        }
    }
}

struct Timers {
    port_sync_timer: Timer,
    port_announce_timer: Timer,
    port_announce_timeout_timer: Timer,
    delay_request_timer: Timer,
    filter_update_timer: Timer,
}

impl Timers {
    fn new() -> Self {
        Timers {
            port_sync_timer: Timer::new(),
            port_announce_timer: Timer::new(),
            port_announce_timeout_timer: Timer::new(),
            delay_request_timer: Timer::new(),
            filter_update_timer: Timer::new(),
        }
    }
}

type EventSocket = Socket<SocketAddrV4, timestamped_socket::socket::Open>;

async fn event_loop(
    instance: &Instance,
    event_socket: &mut EventSocket,
    general_socket: &mut EventSocket,
    session_name: &str,
    iface_rx: &mut watch::Receiver<Option<String>>,
    port_config: PortConfig<AcceptAnyMaster>,
    offset_tx: &watch::Sender<i64>,
    state_tx: &watch::Sender<String>,
) -> SessionEnd {
    let clock = LinuxClock::CLOCK_TAI;

    let mut tlv_forwarder = TlvForwarder::new();
    let rng = StdRng::from_entropy();
    let port_in_bmca = instance.add_port(
        port_config,
        KalmanConfiguration::default(),
        clock.clone(),
        rng,
    );

    // Первый запуск без BMCA: демон statime делает то же самое (порт стартует
    // из end_bmca, первая итерация BMCA — через bmca_interval).
    let (mut port, actions) = port_in_bmca.end_bmca();
    let mut timers = Timers::new();
    let mut pending = handle_actions(
        actions,
        event_socket,
        general_socket,
        &mut timers,
        &tlv_forwarder,
        &clock,
    )
    .await;
    while let Some((context, timestamp)) = pending {
        pending = handle_actions(
            port.handle_send_timestamp(context, timestamp),
            event_socket,
            general_socket,
            &mut timers,
            &tlv_forwarder,
            &clock,
        )
        .await;
    }

    let mut event_buffer = [0u8; MAX_DATA_LEN];
    let mut general_buffer = [0u8; 2048];
    let mut bmca_timer = Timer::new();
    bmca_timer.reset(instance.bmca_interval());
    let mut stats_timer = tokio::time::interval(StdDuration::from_secs(1));
    let mut last_offset = i64::MAX;
    // Агент забирает из хендла только offset_ns и сразу дропает остальное.
    // changed() после этого сразу Err; без флага select крутился бы вхолостую.
    let mut iface_closed = false;

    loop {
        let mut bmca_due = false;
        let mut actions = tokio::select! {
            result = event_socket.recv(&mut event_buffer) => match result {
                Ok(packet) => {
                    if !is_message_buffer_compatible(&event_buffer[..packet.bytes_read]) {
                        PortActionIterator::empty()
                    } else if let Some(timestamp) = packet.timestamp {
                        port.handle_event_receive(
                            &event_buffer[..packet.bytes_read],
                            clock.port_timestamp_to_time(timestamp),
                        )
                    } else {
                        tracing::warn!("zsync: event-пакет без timestamp, пропуск");
                        PortActionIterator::empty()
                    }
                }
                Err(error) => {
                    tracing::warn!("zsync: ошибка recv event socket: {error}");
                    tokio::time::sleep(RECV_ERROR_BACKOFF).await;
                    PortActionIterator::empty()
                }
            },
            result = general_socket.recv(&mut general_buffer) => match result {
                Ok(packet) => port.handle_general_receive(&general_buffer[..packet.bytes_read]),
                Err(error) => {
                    tracing::warn!("zsync: ошибка recv general socket: {error}");
                    tokio::time::sleep(RECV_ERROR_BACKOFF).await;
                    PortActionIterator::empty()
                }
            },
            () = &mut timers.port_announce_timer => {
                port.handle_announce_timer(&mut tlv_forwarder)
            }
            () = &mut timers.port_sync_timer => {
                port.handle_sync_timer()
            }
            () = &mut timers.port_announce_timeout_timer => {
                port.handle_announce_receipt_timer()
            }
            () = &mut timers.delay_request_timer => {
                port.handle_delay_request_timer()
            }
            () = &mut timers.filter_update_timer => {
                port.handle_filter_update_timer()
            }
            () = &mut bmca_timer => {
                // Stop-the-world BMCA: порт один, пересылать некуда — swap
                // делаем после select, чтобы не разрушать заимствования
                // портом/буферами внутри макроса.
                bmca_due = true;
                PortActionIterator::empty()
            }
            _instant = stats_timer.tick() => {
                let port_state = port.port_ds().port_state;
                let state = match port_state {
                    statime::observability::port::PortState::Initializing => "Initializing",
                    statime::observability::port::PortState::Faulty => "Faulty",
                    statime::observability::port::PortState::Disabled => "Disabled",
                    statime::observability::port::PortState::Listening => "Listening",
                    statime::observability::port::PortState::PreMaster => "PreMaster",
                    statime::observability::port::PortState::Master => "Master",
                    statime::observability::port::PortState::Passive => "Passive",
                    statime::observability::port::PortState::Uncalibrated => "Uncalibrated",
                    statime::observability::port::PortState::Slave => "Slave",
                };
                if matches!(
                    port_state,
                    statime::observability::port::PortState::Master
                        | statime::observability::port::PortState::PreMaster
                ) {
                    // Узел — текущий GM: смещение от мастера по определению 0.
                    // Решение динамическое (BMCA), а не стартовая роль.
                    last_offset = 0;
                } else if let Some(estimate) = port.port_current_ds_contribution() {
                    last_offset = estimate.offset_from_master.nanos_rounded().clamp(
                        i64::MIN as i128,
                        i64::MAX as i128,
                    ) as i64;
                }
                let offset_alive = offset_tx.send(last_offset).is_ok();
                let state_alive = state_tx.send(state.to_string()).is_ok();
                if !offset_alive && !state_alive {
                    // Все SyncHandle дропнуты: завершаем фоновую задачу,
                    // иначе её бесконечное ожидание блокирует shutdown
                    // рантайма вызывающего кода (drop ждёт spawn_blocking).
                    return SessionEnd::Stopped;
                }
                PortActionIterator::empty()
            }
            changed = iface_rx.changed(), if !iface_closed => {
                // Другое имя — выходим и даём порту дропнуться раньше инстанса.
                // Закрытый отправитель сессию не кончает: смещение ещё читают.
                if changed.is_err() {
                    iface_closed = true;
                    PortActionIterator::empty()
                } else if iface_rx.borrow().as_deref() != Some(session_name) {
                    return SessionEnd::Switch;
                } else {
                    PortActionIterator::empty()
                }
            }
        };

        loop {
            let pending = handle_actions(
                actions,
                event_socket,
                general_socket,
                &mut timers,
                &tlv_forwarder,
                &clock,
            )
            .await;

            // Действия могут порождать следующие действия (send timestamp).
            actions = match pending {
                Some((context, timestamp)) => port.handle_send_timestamp(context, timestamp),
                None => break,
            };
        }

        if bmca_due {
            let in_bmca = port.start_bmca();
            let mut in_bmca = in_bmca;
            instance.bmca(&mut [&mut in_bmca]);
            let (running, bmca_actions) = in_bmca.end_bmca();
            port = running;
            bmca_timer.reset(instance.bmca_interval());

            let mut pending = handle_actions(
                bmca_actions,
                event_socket,
                general_socket,
                &mut timers,
                &tlv_forwarder,
                &clock,
            )
            .await;
            while let Some((context, timestamp)) = pending {
                pending = handle_actions(
                    port.handle_send_timestamp(context, timestamp),
                    event_socket,
                    general_socket,
                    &mut timers,
                    &tlv_forwarder,
                    &clock,
                )
                .await;
            }
        }
    }
}

async fn handle_actions(
    actions: PortActionIterator<'_>,
    event_socket: &mut EventSocket,
    general_socket: &mut Socket<SocketAddrV4, timestamped_socket::socket::Open>,
    timers: &mut Timers,
    tlv_forwarder: &TlvForwarder,
    clock: &LinuxClock,
) -> Option<(TimestampContext, Time)> {
    let mut pending_timestamp = None;

    for action in actions {
        match action {
            PortAction::SendEvent {
                context,
                data,
                link_local,
            } => {
                let target = if link_local {
                    SocketAddrV4::PDELAY_EVENT
                } else {
                    SocketAddrV4::PRIMARY_EVENT
                };
                match event_socket.send_to(data, target).await {
                    Ok(Some(time)) => {
                        pending_timestamp = Some((context, clock.port_timestamp_to_time(time)));
                    }
                    Ok(None) => {
                        tracing::warn!("zsync: send event без timestamp");
                    }
                    Err(error) => {
                        tracing::warn!("zsync: ошибка send event: {error}");
                    }
                }
            }
            PortAction::SendGeneral { data, link_local } => {
                let target = if link_local {
                    SocketAddrV4::PDELAY_GENERAL
                } else {
                    SocketAddrV4::PRIMARY_GENERAL
                };
                if let Err(error) = general_socket.send_to(data, target).await {
                    tracing::warn!("zsync: ошибка send general: {error}");
                }
            }
            PortAction::ResetAnnounceTimer { duration } => {
                timers.port_announce_timer.reset(duration)
            }
            PortAction::ResetSyncTimer { duration } => timers.port_sync_timer.reset(duration),
            PortAction::ResetDelayRequestTimer { duration } => {
                timers.delay_request_timer.reset(duration)
            }
            PortAction::ResetAnnounceReceiptTimer { duration } => {
                timers.port_announce_timeout_timer.reset(duration)
            }
            PortAction::ResetFilterUpdateTimer { duration } => {
                timers.filter_update_timer.reset(duration)
            }
            PortAction::ForwardTLV { tlv } => tlv_forwarder.forward(tlv.into_owned()),
        }
    }

    pending_timestamp
}

fn clock_identity() -> ClockIdentity {
    if let Ok(ifs) = interfaces() {
        for (_, data) in ifs {
            if let Some(mac) = data.mac() {
                // Игнорируем multicast- и локально-администрируемые MAC.
                if mac[0] & 0x3 == 0 && mac.iter().any(|&b| b != 0) {
                    let byte = |i| mac.get(i).copied().unwrap_or_default();
                    return ClockIdentity(std::array::from_fn(byte));
                }
            }
        }
    }
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill(&mut bytes[..6]);
    bytes[0] |= 0x2; // locally administered
    ClockIdentity(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_config_uses_priority_and_full_bmca() {
        let c = instance_config(100).unwrap();
        assert_eq!(c.priority_1, 100);
        assert!(
            !c.slave_only,
            "BMCA решает роль — slave_only выключен у всех узлов"
        );
        let d = instance_config(128).unwrap();
        assert_eq!(d.priority_1, 128);
        assert_eq!(d.priority_2, 128);
        assert_eq!(d.domain_number, 0);
    }

    #[test]
    fn missing_iface_does_not_kill_the_task() {
        let handle = spawn(128);
        handle.set_interface(Some("no-such-iface-zz".into()));
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(*handle.offset_ns.borrow(), i64::MAX);
        handle.set_interface(None);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline && handle.state.borrow().as_str() != "Idle" {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(handle.state.borrow().as_str(), "Idle");
        assert_eq!(*handle.offset_ns.borrow(), i64::MAX);
    }
}
