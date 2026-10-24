use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use zproto::{AgentMsg, CtlMsg, EventKind, PipelineState, Role, RoleRx, Telemetry};
use zvucast::config::{AgentConfig, PipelineCfg};

const STEP_TIMEOUT: Duration = Duration::from_secs(5);

fn cfg_for(url: String) -> AgentConfig {
    AgentConfig {
        name: "test-agent".into(),
        id: None,
        roles: Some(vec![Role::Rx]),
        controller: url,
        capture_device: None,
        playback_device: Some("plughw:CARD=Whatever".into()),
        playback_backend: zvucast::config::Backend::Alsa,
        sync_interface: "end0".into(),
        ptp_priority: 128,
        pipeline: PipelineCfg::default_rx(),
    }
}

fn stopped_telemetry() -> Telemetry {
    Telemetry {
        tx: None,
        rx: Some(RoleRx {
            state: PipelineState::Stopped,
            dropped: 0,
            rendered: 0,
            queue_ms: -1,
        }),
        ptp_offset_ns: 0,
        uptime_s: 0,
    }
}

/// Discovery в тестах: loopback-юникаст на порт, где никто не слушает,
/// и короткий таймаут — query мгновенно даёт None, агент дозванивается
/// по адресу тестового сервера из cfg.controller.
fn test_discovery() -> zvucast::ws_client::Discovery {
    zvucast::ws_client::Discovery {
        dest: "127.0.0.1:15099".into(),
        timeout: Duration::from_millis(100),
    }
}

fn lo_link() -> watch::Receiver<Option<zvucast::uplink::ActiveLink>> {
    watch::channel(Some(zvucast::uplink::ActiveLink {
        name: "lo".into(),
        ipv4: std::net::Ipv4Addr::LOCALHOST,
        wireless: false,
    }))
    .1
}

/// Сервер-скрипт: на первом соединении ждёт Hello и Telemetry, шлёт CtlMsg::Start
/// и рвёт соединение; на втором просто ретранслирует всё полученное в `tx`.
async fn server_task(listener: TcpListener, tx: mpsc::Sender<serde_json::Value>) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    let mut got_hello = false;
    let mut got_telemetry = false;
    while let Some(Ok(Message::Text(text))) = ws.next().await {
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        got_hello |= v["type"] == "Hello";
        got_telemetry |= v["type"] == "Telemetry";
        tx.send(v).await.unwrap();
        if got_hello && got_telemetry {
            ws.send(Message::Text(r#"{"type":"Start"}"#.into()))
                .await
                .unwrap();
            break;
        }
    }
    drop(ws);

    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    while let Some(Ok(Message::Text(text))) = ws.next().await {
        tx.send(serde_json::from_str(&text).unwrap()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_telemetry_command_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());

    let (tx, mut rx) = mpsc::channel::<serde_json::Value>(64);
    let server = tokio::spawn(server_task(listener, tx));

    let (_telem_tx, telem_rx) = watch::channel(stopped_telemetry());
    let (_ptp_tx, ptp_rx) = watch::channel(42_i64);
    let (cmds_tx, mut cmds_rx) = mpsc::channel::<CtlMsg>(16);
    let (_events_tx, events_rx) = mpsc::channel::<AgentMsg>(16);

    let client = tokio::spawn(zvucast::ws_client::run(
        cfg_for(url),
        test_discovery(),
        "node-test-1",
        "0.1.0-test".into(),
        vec![Role::Rx],
        Vec::new(),
        Vec::new(),
        false,
        telem_rx,
        ptp_rx,
        Arc::new(AtomicBool::new(false)),
        cmds_tx,
        events_rx,
        lo_link(),
    ));

    // 1. после подключения сервер получает Hello с name/roles/version/устройствами
    let hello = tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hello["type"], "Hello");
    assert_eq!(hello["node_id"], "node-test-1");
    assert_eq!(hello["name"], "test-agent");
    assert_eq!(hello["roles"], serde_json::json!(["rx"]));
    assert_eq!(hello["version"], "0.1.0-test");
    assert_eq!(hello["playback_device"], "plughw:CARD=Whatever");
    assert!(hello["capture_device"].is_null());
    assert_eq!(hello["link"], "wired");

    // 2. телеметрия со state=stopped в rx-половине приходит в течение 3 с
    //    (период отправки 2 с); ptp_offset_ns проброшен из watch-канала ptp
    let telem = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let v = rx.recv().await.unwrap();
            if v["type"] == "Telemetry" {
                break v;
            }
        }
    })
    .await
    .expect("телеметрия не пришла за 3 с");
    assert_eq!(telem["rx"]["state"], "stopped");
    assert_eq!(telem["ptp_offset_ns"], 42);

    // 3. CtlMsg::Start от контроллера доходит до cmds-канала агента
    let ctl = tokio::time::timeout(STEP_TIMEOUT, cmds_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctl, CtlMsg::Start);

    // 4. сервер разорвал соединение → клиент реконнектится → второй Hello
    let hello2 = tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hello2["type"], "Hello");
    assert_eq!(hello2["name"], "test-agent");

    client.abort();
    server.abort();
}

/// События gst-домена (AgentMsg::Event) форвардятся в WS как JSON с тем же
/// телом (kind/detail в snake_case), при живом соединении — без ожидания
/// периода телеметрии.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_forwarded_to_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());

    let (tx, mut rx) = mpsc::channel::<serde_json::Value>(16);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        // ретранслируем всё до первого Event включительно
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            let is_event = v["type"] == "Event";
            tx.send(v).await.unwrap();
            if is_event {
                break;
            }
        }
    });

    let (_telem_tx, telem_rx) = watch::channel(stopped_telemetry());
    let (_ptp_tx, ptp_rx) = watch::channel(0_i64);
    let (cmds_tx, _cmds_rx) = mpsc::channel::<CtlMsg>(16);
    let (events_tx, events_rx) = mpsc::channel::<AgentMsg>(16);

    let client = tokio::spawn(zvucast::ws_client::run(
        cfg_for(url),
        test_discovery(),
        "node-test-1",
        "0.1.0-test".into(),
        vec![Role::Rx],
        Vec::new(),
        Vec::new(),
        false,
        telem_rx,
        ptp_rx,
        Arc::new(AtomicBool::new(false)),
        cmds_tx,
        events_rx,
        lo_link(),
    ));

    // первым всегда идёт Hello — значит, соединение установлено
    let hello = tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hello["type"], "Hello");

    // событие из gst-домена доходит до сервера как {"type":"Event",...}
    events_tx
        .send(AgentMsg::Event {
            kind: EventKind::Underrun,
            detail: "alsasink underrun".into(),
        })
        .await
        .unwrap();
    let ev = tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev["type"], "Event");
    assert_eq!(ev["kind"], "underrun");
    assert_eq!(ev["detail"], "alsasink underrun");

    client.abort();
    server.abort();
}

/// Закрытие events-канала («gst-домен умер») — фатально: run() завершается
/// Err, а не висит в реконнект-цикле (zombie-ретрай с замёрзшей телеметрией).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_exit_when_events_channel_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel::<()>();
    // сервер: принять, прочитать Hello, держать соединение до закрытия
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        connected_tx.send(()).unwrap();
        let _hello = ws.next().await;
        while let Some(Ok(_)) = ws.next().await {}
    });

    let (_telem_tx, telem_rx) = watch::channel(stopped_telemetry());
    let (_ptp_tx, ptp_rx) = watch::channel(0_i64);
    let (cmds_tx, _cmds_rx) = mpsc::channel::<CtlMsg>(16);
    let (events_tx, events_rx) = mpsc::channel::<AgentMsg>(16);

    let client = tokio::spawn(zvucast::ws_client::run(
        cfg_for(url),
        test_discovery(),
        "node-test-1",
        "0.1.0-test".into(),
        vec![Role::Rx],
        Vec::new(),
        Vec::new(),
        false,
        telem_rx,
        ptp_rx,
        Arc::new(AtomicBool::new(false)),
        cmds_tx,
        events_rx,
        lo_link(),
    ));

    connected_rx.await.unwrap(); // соединение установлено, Hello ушёл
    drop(events_tx); // «gst-домен умер»

    let res = tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .expect("run() завис в реконнект-цикле вместо фатального выхода");
    assert!(res.unwrap().is_err(), "run() должен вернуть Err");

    server.abort();
}

/// Закрытие cmds-канала (потребитель команд исчез) — фатально так же:
/// сервер шлёт CtlMsg, клиент не может его доставить → Err из run(),
/// а не реконнект с потерей команд.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_exit_when_cmds_channel_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel::<()>();
    // сервер: принять, прочитать Hello, отправить Start, держать соединение
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        connected_tx.send(()).unwrap();
        let _hello = ws.next().await;
        ws.send(Message::Text(r#"{"type":"Start"}"#.into()))
            .await
            .unwrap();
        while let Some(Ok(_)) = ws.next().await {}
    });

    let (_telem_tx, telem_rx) = watch::channel(stopped_telemetry());
    let (_ptp_tx, ptp_rx) = watch::channel(0_i64);
    let (cmds_tx, cmds_rx) = mpsc::channel::<CtlMsg>(16);
    drop(cmds_rx); // потребитель команд исчез до старта клиента
    let (_events_tx, events_rx) = mpsc::channel::<AgentMsg>(16);

    let client = tokio::spawn(zvucast::ws_client::run(
        cfg_for(url),
        test_discovery(),
        "node-test-1",
        "0.1.0-test".into(),
        vec![Role::Rx],
        Vec::new(),
        Vec::new(),
        false,
        telem_rx,
        ptp_rx,
        Arc::new(AtomicBool::new(false)),
        cmds_tx,
        events_rx,
        lo_link(),
    ));

    connected_rx.await.unwrap();

    let res = tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .expect("run() завис в реконнект-цикле вместо фатального выхода");
    assert!(res.unwrap().is_err(), "run() должен вернуть Err");

    server.abort();
}

/// Task 10: ControllerMoved от контроллера — транспортная команда ws-домена.
/// Клиент сохраняет новый адрес в current_addr и рвёт сокет: реконнект-цикл
/// дозванивается на новый адрес (молчащий discovery его НЕ перезаписывает),
/// в cmds команду не передаём — gst-домен её не видит.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_moved_switches_reconnect_target() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url1 = format!("ws://{}/ws", listener1.local_addr().unwrap());
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url2 = format!("ws://{}/ws", listener2.local_addr().unwrap());

    let (tx, mut rx) = mpsc::channel::<serde_json::Value>(16);
    let server = tokio::spawn(async move {
        // сервер 1: прочитать Hello → прислать ControllerMoved → закрыть
        let (stream, _) = listener1.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _hello = ws.next().await;
        let ctl = CtlMsg::ControllerMoved { addr: url2.clone() };
        ws.send(Message::Text(serde_json::to_string(&ctl).unwrap()))
            .await
            .unwrap();
        drop(ws);
        // сервер 2: принять реконнект (Hello → tx)
        let (stream, _) = listener2.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            tx.send(serde_json::from_str(&text).unwrap()).await.unwrap();
        }
    });

    let (_telem_tx, telem_rx) = watch::channel(stopped_telemetry());
    let (_ptp_tx, ptp_rx) = watch::channel(0_i64);
    let (cmds_tx, mut cmds_rx) = mpsc::channel::<CtlMsg>(16);
    let (_events_tx, events_rx) = mpsc::channel::<AgentMsg>(16);

    let client = tokio::spawn(zvucast::ws_client::run(
        cfg_for(url1),
        test_discovery(),
        "node-test-1",
        "0.1.0-test".into(),
        vec![Role::Rx],
        Vec::new(),
        Vec::new(),
        false,
        telem_rx,
        ptp_rx,
        Arc::new(AtomicBool::new(false)),
        cmds_tx,
        events_rx,
        lo_link(),
    ));

    // второй Hello пришёл на сервер 2 — реконнект ушёл на новый адрес
    let hello2 = tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .expect("реконнекта на новый адрес не случилось")
        .unwrap();
    assert_eq!(hello2["type"], "Hello");
    assert_eq!(hello2["name"], "test-agent");

    // ControllerMoved в cmds не попал — команда перехвачена ws-доменом
    assert!(
        cmds_rx.try_recv().is_err(),
        "ControllerMoved не должен уходить в cmds-канал"
    );

    client.abort();
    server.abort();
}
