//! Интеграционный тест контроллера: WS-сервер + HTTP API.
//! Базовые сценарии (Task 10): hello/телеметрия/start/stop/start-all/дашборд.
//! Зонные сценарии (Task 8): pusher по assignment, кража членов зоны,
//! channel map (PATCH + пуш при hello).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use zproto::{
    AgentMsg, BranchChannelMap, ChanSel, ChannelMap, CtlMsg, Endpoint, LinkKind, PipelineState, Role, RoleRx, RoleTx,
    SecretString, SoloistState, Telemetry, ZoneEndpoints,
};
use zvucast::controller::{app, spawn_endpoint_pusher, AppState};

fn mix_port(tx: &str) -> u16 {
    zvucast::mix::assign_mix_ports(&[tx.to_string()])
        .into_iter()
        .find(|(name, _)| name == tx)
        .unwrap_or_else(|| panic!("нет порта для {tx}"))
        .1
}

/// Порт имени в том же наборе, который пушер отдаёт `assign_mix_ports`.
fn assigned_port(names: &[String], name: &str) -> u16 {
    zvucast::mix::assign_mix_ports(names)
        .into_iter()
        .find(|(got, _)| got == name)
        .unwrap_or_else(|| panic!("нет порта для {name} среди {names:?}"))
        .1
}

fn zone_stream(tx: &str, zone_id: &str, ips: &[&str], dscp: u8) -> CtlMsg {
    let port = mix_port(tx);
    let mut endpoints: Vec<Endpoint> = ips
        .iter()
        .map(|ip| Endpoint { ip: (*ip).into(), port })
        .collect();
    endpoints.sort_by(|a, b| a.ip.cmp(&b.ip));
    CtlMsg::SetEndpoints {
        dscp,
        zones: vec![ZoneEndpoints { zone_id: zone_id.into(), endpoints, source: None }],
    }
}

fn held_stream(zone_id: &str, dscp: u8) -> CtlMsg {
    zone_stream("tx1", zone_id, &[], dscp)
}

fn mix_of(tx: &str) -> CtlMsg {
    CtlMsg::SetMix { ports: vec![mix_port(tx)], jb_ms: 2 }
}

/// Минимальный HTTP/1.1-клиент: с `Connection: close` hyper закрывает
/// соединение после ответа, и read_to_end завершается. Без лишних dev-зависимостей.
async fn http(port: u16, method: &str, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
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

/// Тот же минимальный клиент, но с JSON-телом (POST /api/dscp, зоны, PATCH).
async fn http_body(port: u16, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8(buf).unwrap();
    let (head, out) = text.split_once("\r\n\r\n").unwrap();
    let status: u16 = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, out.to_string())
}

type Ws = tokio_tungstenite::WebSocketStream<TcpStream>;

static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

/// Уникальный путь state-файла в tempdir (тесты бинаря — в одном процессе).
fn temp_state_path(tag: &str) -> PathBuf {
    let n = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("zvucast-test-{tag}-{}-{n}.json", std::process::id()))
}

/// Поднимает приложение на свободном порту. Pusher — отдельным spawn'ом.
async fn serve(state: AppState) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    port
}

async fn open_ws(port: u16, src_ip: &str) -> Ws {
    // Привязка к конкретному loopback-ip: peer-ip узла в реестре = src_ip,
    // мок-агенты различимы в наборах эндпоинтов, как на реальной сети.
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.bind(format!("{src_ip}:0").parse().unwrap()).unwrap();
    let stream = sock
        .connect(format!("127.0.0.1:{port}").parse().unwrap())
        .await
        .unwrap();
    tokio_tungstenite::client_async(format!("ws://127.0.0.1:{port}/ws"), stream)
        .await
        .unwrap()
        .0
}

/// Мок-агент: подключается к /ws, шлёт Hello как первый кадр.
struct MockAgent {
    ws: Ws,
}

impl MockAgent {
    async fn connect(port: u16, name: &str) -> Self {
        Self::connect_as(port, name, Role::Rx).await
    }

    async fn connect_as(port: u16, name: &str, role: Role) -> Self {
        Self::connect_as_from(port, name, role, "127.0.0.1").await
    }

    /// С привязкой к loopback-ip: peer-ip у узла в реестре = src_ip.
    async fn connect_as_from(port: u16, name: &str, role: Role, src_ip: &str) -> Self {
        Self::hello_on(open_ws(port, src_ip).await, name, name, vec![role]).await
    }

    /// node_id отличен от имени — для проверки алиаса по имени в REST-путях.
    async fn connect_named(port: u16, node_id: &str, name: &str, role: Role) -> Self {
        Self::hello_on(open_ws(port, "127.0.0.1").await, node_id, name, vec![role]).await
    }

    async fn hello_on(mut ws: Ws, node_id: &str, name: &str, roles: Vec<Role>) -> Self {
        let hello = AgentMsg::Hello {
            node_id: node_id.into(),
            name: name.into(),
            roles: roles.clone(),
            version: "0.1.0".into(),
            target: String::new(),
            capture_device: roles.contains(&Role::Tx).then(|| "test".to_string()),
            playback_device: roles.contains(&Role::Rx).then(|| "test".to_string()),
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: None,
            setup: Some(vec![]),
            soloist: SoloistState::Unknown,
            soloist_command: String::new(),
        };
        ws.send(Message::Text(serde_json::to_string(&hello).unwrap()))
            .await
            .unwrap();
        Self { ws }
    }

    /// Как `hello_on`, но Soloist не установлен: строка попадает в сводку,
    /// только когда у узла включён Spotify.
    async fn hello_soloist(mut ws: Ws, node_id: &str, name: &str, roles: Vec<Role>) -> Self {
        let hello = AgentMsg::Hello {
            node_id: node_id.into(),
            name: name.into(),
            roles: roles.clone(),
            version: "0.1.0".into(),
            target: String::new(),
            capture_device: roles.contains(&Role::Tx).then(|| "test".to_string()),
            playback_device: roles.contains(&Role::Rx).then(|| "test".to_string()),
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: None,
            setup: Some(vec![]),
            soloist: SoloistState::Missing,
            soloist_command: "curl soloist".into(),
        };
        ws.send(Message::Text(serde_json::to_string(&hello).unwrap()))
            .await
            .unwrap();
        Self { ws }
    }

    async fn connect_virtual(port: u16) -> Self {
        Self::connect_virtual_named(port, "pc").await
    }

    async fn connect_virtual_named(port: u16, name: &str) -> Self {
        let mut ws = open_ws(port, "127.0.0.1").await;
        let hello = AgentMsg::Hello {
            node_id: name.into(),
            name: name.into(),
            roles: vec![Role::Tx],
            version: "0.1.0".into(),
            target: String::new(),
            capture_device: Some("plughw:CARD=Generic_1".into()),
            playback_device: None,
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: true,
            link: None,
            setup: Some(vec![]),
            soloist: SoloistState::Unknown,
            soloist_command: String::new(),
        };
        ws.send(Message::Text(serde_json::to_string(&hello).unwrap())).await.unwrap();
        Self { ws }
    }

    async fn send_telemetry(&mut self) {
        let t = AgentMsg::Telemetry(Telemetry {
            tx: None,
            rx: Some(RoleRx { state: PipelineState::Playing, dropped: 0, rendered: 10, queue_ms: 5 }),
            ptp_offset_ns: 0,
            uptime_s: 3,
            capture_volume: None,
            playback_volume: None,
            device_volumes: Vec::new(),
            feeds: Vec::new(),
        });
        self.ws
            .send(Message::Text(serde_json::to_string(&t).unwrap()))
            .await
            .unwrap();
    }

    async fn send_tx_rms(&mut self, rms_l: f64, rms_r: f64) {
        let t = AgentMsg::Telemetry(Telemetry {
            tx: Some(RoleTx { state: PipelineState::Playing, rms_l, rms_r }),
            rx: None,
            ptp_offset_ns: 0,
            uptime_s: 3,
            capture_volume: None,
            playback_volume: None,
            device_volumes: Vec::new(),
            feeds: Vec::new(),
        });
        self.ws.send(Message::Text(serde_json::to_string(&t).unwrap())).await.unwrap();
    }

    async fn send_telemetry_volumes(&mut self, capture: f64, playback: f64) {
        let t = AgentMsg::Telemetry(Telemetry {
            tx: Some(RoleTx { state: PipelineState::Playing, rms_l: -20.0, rms_r: -20.0 }),
            rx: Some(RoleRx { state: PipelineState::Playing, dropped: 0, rendered: 1, queue_ms: 5 }),
            ptp_offset_ns: 0,
            uptime_s: 3,
            capture_volume: Some(capture),
            playback_volume: Some(playback),
            device_volumes: Vec::new(),
            feeds: Vec::new(),
        });
        self.ws.send(Message::Text(serde_json::to_string(&t).unwrap())).await.unwrap();
    }

    /// Следующий текстовый кадр, включая цели УИ.
    async fn recv_text_ctl(&mut self) -> CtlMsg {
        loop {
            if let Message::Text(t) = self.ws.next().await.unwrap().unwrap() {
                return serde_json::from_str(&t).unwrap();
            }
        }
    }

    /// Следующая команда сценария. Цели УИ уходят на hello сами и здесь
    /// пропускаются: их порядок проверяет `ui_targets_follow_hello_and_prefs`.
    async fn recv_ctl(&mut self) -> CtlMsg {
        loop {
            match self.recv_text_ctl().await {
                CtlMsg::SetPlaybackTarget { .. }
                | CtlMsg::SetMutedZones { .. }
                | CtlMsg::SetCaptureTarget { .. } => continue,
                msg => return msg,
            }
        }
    }

    async fn recv_raw_within(&mut self, secs: u64) -> CtlMsg {
        tokio::time::timeout(Duration::from_secs(secs), self.recv_text_ctl())
            .await
            .expect("не дождались CtlMsg")
    }

    /// recv_ctl с таймаутом: пушер должен успеть за ~2 с дебаунса; 5 с —
    /// запас на CI. Без таймаута сломанный пушер повесил бы тест навсегда.
    async fn recv_ctl_within(&mut self, secs: u64) -> CtlMsg {
        tokio::time::timeout(Duration::from_secs(secs), self.recv_ctl())
            .await
            .expect("не дождались CtlMsg (команда не доставлена)")
    }

    /// Обратное recv_ctl_within: уверенность, что кадр НЕ пришёл
    /// (окно тишины длиннее дебаунса пушера).
    async fn assert_silent_within(&mut self, secs: u64) {
        let res = tokio::time::timeout(Duration::from_secs(secs), self.recv_ctl()).await;
        assert!(res.is_err(), "неожиданный CtlMsg за окном тишины {secs} с");
    }

    /// Узел-комбайн [tx, rx]: Hello с обеими ролями.
    async fn connect_mixed(port: u16, name: &str) -> Self {
        let mut ws = open_ws(port, "127.0.0.1").await;
        let hello = AgentMsg::Hello {
            node_id: name.into(),
            name: name.into(),
            roles: vec![Role::Tx, Role::Rx],
            version: "0.1.0".into(),
            target: String::new(),
            capture_device: Some("cap".into()),
            playback_device: Some("play".into()),
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: None,
            setup: Some(vec![]),
            soloist: SoloistState::Unknown,
            soloist_command: String::new(),
        };
        ws.send(Message::Text(serde_json::to_string(&hello).unwrap()))
            .await
            .unwrap();
        Self { ws }
    }
}

fn node<'a>(nodes: &'a Value, name: &str) -> &'a Value {
    nodes
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == name)
        .unwrap_or_else(|| panic!("узел {name} в ответе: {nodes}"))
}

/// Опрашивает GET /api/nodes, пока предикат не выполнится (до ~5 c).
async fn wait_for<F>(port: u16, f: F) -> Value
where
    F: Fn(&Value) -> bool,
{
    for _ in 0..100 {
        let (_, body) = http(port, "GET", "/api/nodes").await;
        let nodes: Value = serde_json::from_str(&body).unwrap();
        if f(&nodes) {
            return nodes;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("условие по /api/nodes не выполнилось за 5 c");
}

/// Ждёт, пока hello агента будет обработан (узел виден в реестре) — иначе
/// гонка: валидация зоны увидит «неизвестный узел».
async fn wait_node(port: u16, name: &str) {
    wait_for(port, |n| {
        n.as_array()
            .is_some_and(|a| a.iter().any(|v| v["name"] == name))
    })
    .await;
}

/// POST /api/zones с телом; возвращает (статус, ответ).
async fn create_zone(port: u16, body: &str) -> (u16, Value) {
    let (status, resp) = http_body(port, "POST", "/api/zones", body).await;
    (status, serde_json::from_str(&resp).unwrap_or(Value::Null))
}

/// Создаёт зону и жмёт пуск; возвращает id.
async fn create_and_start_zone(port: u16, body: &str) -> String {
    let (status, zone) = create_zone(port, body).await;
    assert_eq!(status, 200, "создание зоны: {zone}");
    let id = zone["id"].as_str().unwrap().to_string();
    let (status, _) = http(port, "POST", &format!("/api/zones/{id}/start")).await;
    assert_eq!(status, 200, "пуск зоны {id}");
    id
}

/// Все зоны как Value (в ответе GET /api/zones).
async fn get_zones(port: u16) -> Value {
    let (status, body) = http(port, "GET", "/api/zones").await;
    assert_eq!(status, 200);
    serde_json::from_str::<Value>(&body).unwrap()["zones"].clone()
}

#[tokio::test]
async fn node_list_reports_wifi_link() {
    let port = serve(AppState::new()).await;
    let mut ws = open_ws(port, "127.0.0.1").await;
    let hello = AgentMsg::Hello {
        node_id: "radio".into(),
        name: "radio".into(),
        roles: vec![Role::Rx],
        version: "0.1.0".into(),
        target: String::new(),
        capture_device: None,
        playback_device: Some("test".into()),
        inputs: vec![],
        outputs: vec![],
        supports_virtual_output: false,
        link: Some(LinkKind::Wifi),
        setup: Some(vec![]),
        soloist: SoloistState::Unknown,
        soloist_command: String::new(),
    };
    ws.send(Message::Text(serde_json::to_string(&hello).unwrap()))
        .await
        .unwrap();
    let nodes = wait_for(port, |n| {
        n.as_array().is_some_and(|a| a.iter().any(|v| v["name"] == "radio"))
    })
    .await;
    assert_eq!(node(&nodes, "radio")["ip"], "127.0.0.1");
    assert_eq!(node(&nodes, "radio")["link"], "wifi");
}

#[tokio::test]
async fn controller_end_to_end() {
    let port = serve(AppState::new()).await;

    // 2. мок-агент m1: Hello + Telemetry
    let mut m1 = MockAgent::connect(port, "m1").await;
    m1.send_telemetry().await;

    // 3. GET /api/nodes → m1 online, телеметрия на месте
    let (status, _) = http(port, "GET", "/api/nodes").await;
    assert_eq!(status, 200);
    // телеметрия приходит асинхронно — ждём, пока сервер её обработает
    let nodes = wait_for(port, |n| {
        n.as_array()
            .is_some_and(|a| a.iter().any(|v| v["name"] == "m1" && !v["telemetry"].is_null()))
    })
    .await;
    assert_eq!(node(&nodes, "m1")["online"], true);
    // IP узла — адрес peer'а WS-соединения (мок-агент с 127.0.0.1)
    assert_eq!(node(&nodes, "m1")["ip"], "127.0.0.1");
    assert!(node(&nodes, "m1")["link"].is_null());

    // 4. POST start → мок-агент получает CtlMsg::Start
    let (status, _) = http(port, "POST", "/api/nodes/m1/start").await;
    assert_eq!(status, 200);
    assert_eq!(m1.recv_ctl().await, CtlMsg::Start);

    // 5. разрыв → mark_offline в WS-хендлере → online=false
    drop(m1);
    let nodes = wait_for(port, |n| {
        n.as_array()
            .is_some_and(|a| a.iter().any(|v| v["name"] == "m1" && v["online"] == false))
    })
    .await;
    assert_eq!(node(&nodes, "m1")["online"], false);

    // 6. start-all при двух подключённых агентах → оба получили Start
    let mut m1 = MockAgent::connect(port, "m1").await; // re-hello возвращает узел online
    let mut m2 = MockAgent::connect(port, "m2").await;
    m1.send_telemetry().await;
    m2.send_telemetry().await;
    let (status, body) = http(port, "POST", "/api/start-all").await;
    assert_eq!(status, 200);
    let started: Vec<String> = serde_json::from_value(
        serde_json::from_str::<Value>(&body).unwrap()["started"].clone(),
    )
    .unwrap();
    assert_eq!(started, vec!["m1".to_string(), "m2".to_string()]);
    assert_eq!(m1.recv_ctl().await, CtlMsg::Start);
    assert_eq!(m2.recv_ctl().await, CtlMsg::Start);

    // stop-all: команда доходит до обоих
    let (status, _) = http(port, "POST", "/api/stop-all").await;
    assert_eq!(status, 200);
    assert_eq!(m1.recv_ctl().await, CtlMsg::Stop);
    assert_eq!(m2.recv_ctl().await, CtlMsg::Stop);

    // 7. GET / → дашборд: 200 и имя продукта в разметке (Task 11: React-SPA
    // из rust-embed — index.html + js-бандл из web/dist)
    let (status, body) = http(port, "GET", "/").await;
    assert_eq!(status, 200);
    assert!(
        body.to_ascii_lowercase().contains("zvucast"),
        "страница должна содержать имя продукта: {body}"
    );
    // SPA fallback: произвальный не-API GET отдаёт тот же index.html
    let (status, spa_body) = http(port, "GET", "/no-such-route").await;
    assert_eq!(status, 200);
    assert_eq!(spa_body, body);

    // Task 5/11: DSCP-контрол и по-ролевые колонки живут в js-бандле —
    // подтянем src из index.html и проверим, что ассет отдаётся с типом js.
    // Если dist — только закоммиченный плейсхолдер (web не собран), бандла
    // нет — проверки ассетов пропускаем (cargo test без npm должен быть зелёным).
    if let Some(asset) = body
        .split("src=\"/assets/")
        .nth(1)
        .and_then(|s| s.split('\"').next())
        .map(|p| format!("/assets/{p}"))
    {
        let (status, js) = http(port, "GET", &asset).await;
        assert_eq!(status, 200);
        assert!(
            js.contains("DSCP") && js.contains("badge") && js.contains("Узлы"),
            "бандл должен содержать DSCP, бейджи и страницу узлов: {asset}"
        );
    }

    // узел, которого нет — 404
    let (status, _) = http(port, "POST", "/api/nodes/ghost/start").await;
    assert_eq!(status, 404);
}

// Task 8: pusher v2 — SetEndpoints по assignment активных зон.
// Зонная семантика: rx вне активных зон tx не меняет; пустой набор пушится
// тоже (последний слушатель отвалился → tx молчит, multicast'а нет).
// 2. мок-tx Hello → пустой набор (нет активных зон)
// 3. зона {tx, [rx]} + пуск → SetEndpoints{[127.0.0.1:5004], dscp:40}
// 4. мок-rx отключается → SetEndpoints{[], dscp:40}
// 5. POST /api/dscp {"dscp":24} → пуш с dscp:24; GET → 24
// 6. POST /api/dscp {"dscp":64} → 400

#[tokio::test]
async fn endpoint_pusher_and_dscp_api() {
    let state_path = temp_state_path("pusher");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    // 2. tx-агент: появился online → пустой набор (активных зон нет)
    let mut tx = MockAgent::connect_as(port, "tx1", Role::Tx).await;
    assert_eq!(
        tx.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );

    // 3. rx вне зон пуша не даёт; зона {tx1, [rx1]} + пуск → набор из rx1
    let rx = MockAgent::connect_as(port, "rx1", Role::Rx).await;
    wait_node(port, "rx1").await;
    let id = create_and_start_zone(port, r#"{"name":"Все","tx":"tx1","members":["rx1"]}"#).await;
    assert_eq!(
        tx.recv_ctl_within(5).await,
        zone_stream("tx1", &id, &["127.0.0.1"], 40)
    );
    // зонный пуск теперь ещё и стартует играющих: сначала набор, затем Start
    assert_eq!(tx.recv_ctl_within(5).await, CtlMsg::Start);

    // 4. rx отвалился → пустой набор (tx замолкает) и Stop играющим
    drop(rx);
    assert_eq!(tx.recv_ctl_within(5).await, held_stream(&id, 40));
    assert_eq!(tx.recv_ctl_within(5).await, CtlMsg::Stop);

    // стоп зоны: набор и так пуст — повторного пуша нет, лишних кадров ждать не будем

    // 5. POST dscp=24 → пуш с новым dscp; GET отдаёт текущее значение
    let (status, body) = http_body(port, "POST", "/api/dscp", r#"{"dscp":24}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["dscp"],
        24,
        "POST отвечает установленным значением: {body}"
    );
    assert_eq!(
        tx.recv_ctl_within(5).await,
        held_stream(&id, 24)
    );
    let (status, body) = http(port, "GET", "/api/dscp").await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["dscp"], 24);

    // 6. dscp вне 0..=63 → 400 (значение не меняется)
    let (status, _) = http_body(port, "POST", "/api/dscp", r#"{"dscp":64}"#).await;
    assert_eq!(status, 400);
    let (status, body) = http(port, "GET", "/api/dscp").await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["dscp"], 24);
}

/// Узел-комбайн [tx, rx]: rx-половина — член собственной зоны (играет себя,
/// спека §0), tx-половина — получатель пуша. /api/nodes отдаёт обе роли.
#[tokio::test]
async fn mixed_node_is_endpoint_and_push_target() {
    let state_path = temp_state_path("mixed");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    // появление pc (как tx) → пустой набор: активных зон ещё нет
    let mut pc = MockAgent::connect_mixed(port, "pc").await;
    assert_eq!(
        pc.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );

    // зона с tx=pc и единственным членом pc → набор из собственного ip
    let id = create_and_start_zone(port, r#"{"name":"pc","tx":"pc","members":["pc"]}"#).await;
    assert_eq!(pc.recv_ctl_within(5).await, zone_stream("pc", &id, &["127.0.0.1"], 40));

    let nodes = wait_for(port, |n| {
        n.as_array().is_some_and(|a| {
            a.iter().any(|v| v["name"] == "pc" && v["roles"] == serde_json::json!(["tx", "rx"]))
        })
    })
    .await;
    assert_eq!(node(&nodes, "pc")["roles"], serde_json::json!(["tx", "rx"]));
    assert_eq!(node(&nodes, "pc")["capture_device"], "cap");
    assert_eq!(node(&nodes, "pc")["playback_device"], "play");
}

// Task 8, бриф: зона пушает SetEndpoints только своим членам.
// Мок-агенты с разных loopback-ip, чтобы членство различалось в наборах.

#[tokio::test]
async fn zone_start_pushes_only_zone_members_to_its_tx() {
    let state_path = temp_state_path("zone-members");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    // tx1 появился → пустой набор (зон нет)
    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );

    // r1..r3 online; в зону войдут только r1 и r2
    let r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    let r2 = MockAgent::connect_as_from(port, "r2", Role::Rx, "127.0.0.4").await;
    let r3 = MockAgent::connect_as_from(port, "r3", Role::Rx, "127.0.0.5").await;
    for n in ["r1", "r2", "r3"] {
        wait_node(port, n).await;
    }

    // создание зоны (неактивна — пуша нет) → пуск
    let (status, zone) = create_zone(
        port,
        r#"{"name":"Зал","tx":"tx1","members":["r1","r2"]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(zone["name"], "Зал");
    assert_eq!(zone["tx"], "tx1");
    assert_eq!(zone["members"], serde_json::json!(["r1", "r2"]));
    assert_eq!(zone["active"], false);
    assert_eq!(zone["stolen"], serde_json::json!([]));
    let id = zone["id"].as_str().unwrap().to_string();

    let (status, zone) = http(port, "POST", &format!("/api/zones/{id}/start")).await;
    assert_eq!(status, 200);
    let zone: Value = serde_json::from_str(&zone).unwrap();
    assert_eq!(zone["active"], true);
    assert_eq!(zone["seq"], 1, "первая активация получает seq = next_seq = 1");

    // tx1 получил набор из r1 и r2 — без r3
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        zone_stream("tx1", &id, &["127.0.0.3", "127.0.0.4"], 40)
    );

    // state.json на диске содержит зону
    let on_disk = std::fs::read_to_string(&state_path).unwrap();
    assert!(on_disk.contains("Зал"), "зона в state.json: {on_disk}");

    // пуск зоны стартовал играющих: Start после SetEndpoints (дренируем)
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);

    // стоп зоны → tx1 получает пустой набор и Stop
    let (status, _) = http(port, "POST", &format!("/api/zones/{id}/stop")).await;
    assert_eq!(status, 200);
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Stop);

    drop((r1, r2, r3));
}

#[tokio::test]
async fn zone_with_two_sources_mixes_both_ports_on_the_speaker() {
    let state_path = temp_state_path("zone-sources");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let mut tx2 = MockAgent::connect_as_from(port, "tx2", Role::Tx, "127.0.0.6").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(
        tx2.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;

    let (status, _) = http_body(
        port,
        "POST",
        "/api/zones",
        r#"{"name":"пусто","sources":[],"members":["r1"]}"#,
    )
    .await;
    assert_eq!(status, 422);

    let (status, zone) = create_zone(
        port,
        r#"{"name":"Зал","sources":["tx1","tx2","tx1"],"members":["r1"]}"#,
    )
    .await;
    assert_eq!(status, 200, "{zone}");
    assert_eq!(zone["tx"], "tx1");
    assert_eq!(zone["sources"], serde_json::json!(["tx1", "tx2"]));
    let id = zone["id"].as_str().unwrap().to_string();

    let (status, _) = http(port, "POST", &format!("/api/zones/{id}/start")).await;
    assert_eq!(status, 200);
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        zone_stream("tx1", &id, &["127.0.0.3"], 40)
    );
    assert_eq!(
        tx2.recv_ctl_within(5).await,
        zone_stream("tx2", &id, &["127.0.0.3"], 40)
    );
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(tx2.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::Start);
    match r1.recv_ctl_within(5).await {
        CtlMsg::SetChannelMap { map, branches } => {
            assert_eq!(map, ChannelMap::default(), "после микшера карта сквозная");
            let mut expect = vec![
                BranchChannelMap { port: mix_port("tx1"), map: ChannelMap::default() },
                BranchChannelMap { port: mix_port("tx2"), map: ChannelMap::default() },
            ];
            expect.sort_by_key(|b| b.port);
            assert_eq!(branches, expect);
        }
        other => panic!("ждали SetChannelMap, пришло {other:?}"),
    }
    let mut ports = vec![mix_port("tx1"), mix_port("tx2")];
    ports.sort();
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::SetMix { ports, jb_ms: 2 });

    let (status, _) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{id}"),
        r#"{"source_maps":{"r1":{"tx1":{"out_l":"r","out_r":null}}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    match r1.recv_ctl_within(5).await {
        CtlMsg::SetChannelMap { branches, .. } => {
            let tx1 = branches.iter().find(|b| b.port == mix_port("tx1")).unwrap();
            assert_eq!(tx1.map, ChannelMap { out_l: Some(ChanSel::R), out_r: None });
            let tx2 = branches.iter().find(|b| b.port == mix_port("tx2")).unwrap();
            assert_eq!(tx2.map, ChannelMap::default(), "у tx2 остаётся общая карта");
        }
        other => panic!("ждали карту tx1, пришло {other:?}"),
    }

    let (status, zone) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{id}"),
        r#"{"sources":["tx2"]}"#,
    )
    .await;
    assert_eq!(status, 200, "{zone}");
    let zone: Value = serde_json::from_str(&zone).unwrap();
    assert_eq!(zone["tx"], "tx2");
    assert_eq!(zone["sources"], serde_json::json!(["tx2"]));
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Stop);
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetMix { ports: vec![mix_port("tx2")], jb_ms: 2 }
    );
}

// Task 8, бриф: поздно активированная зона забирает общего члена; затронутые
// tx получают новые наборы; угнанные члены видны в ZoneView.stolen.

#[tokio::test]
async fn stealing_repushes_endpoints() {
    let state_path = temp_state_path("steal");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    // два tx, два rx; появление tx → пустые наборы
    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let mut tx2 = MockAgent::connect_as_from(port, "tx2", Role::Tx, "127.0.0.6").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(
        tx2.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );

    let r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    let r2 = MockAgent::connect_as_from(port, "r2", Role::Rx, "127.0.0.4").await;
    for n in ["r1", "r2"] {
        wait_node(port, n).await;
    }

    // зона A [r1, r2] на tx1
    let id_a = create_and_start_zone(
        port,
        r#"{"name":"A","tx":"tx1","members":["r1","r2"]}"#,
    )
    .await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        zone_stream("tx1", &id_a, &["127.0.0.3", "127.0.0.4"], 40)
    );
    // пуск зоны стартовал играющих: Start после SetEndpoints (дренируем)
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);

    // зона B [r2] на tx2, активирована позже → забирает r2
    let id_b = create_and_start_zone(port, r#"{"name":"B","tx":"tx2","members":["r2"]}"#).await;

    // tx1 получил второй пуш: без угнанной r2; tx2 — набор со своим членом
    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id_a, &["127.0.0.3"], 40));
    assert_eq!(tx2.recv_ctl_within(5).await, zone_stream("tx2", &id_b, &["127.0.0.4"], 40));

    // stolen: у A угнана r2, у B — никого
    let zones = get_zones(port).await;
    let a = zones.as_array().unwrap().iter().find(|z| z["id"] == id_a).unwrap();
    assert_eq!(a["stolen"], serde_json::json!(["r2"]));
    assert_eq!(a["seq"], 1);
    let b = zones.as_array().unwrap().iter().find(|z| z["name"] == "B").unwrap();
    assert_eq!(b["stolen"], serde_json::json!([]));
    assert_eq!(b["seq"], 2, "вторая активация — seq = 2 (монотонный next_seq)");

    drop((r1, r2));
}

// --- Зонный пуск/стоп гоняет Start/Stop (diff «играющего множества»). ---
// Регрессия E2E: Play зоны пушил только SetEndpoints, want_playing у агента
// ставится лишь явным Start — зоны молчали; рестарт контроллера не
// восстанавливал звук (нарушение спеки §2).

/// Пуск зоны: tx получает SetEndpoints и Start (в этом порядке), rx-член — Start.
#[tokio::test]
async fn zone_start_starts_playing_nodes() {
    let state_path = temp_state_path("zone-play");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;

    let id = create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id, &["127.0.0.3"], 40));
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::Start);

    drop((tx1, r1));
}

#[tokio::test]
async fn zone_silence_hold_drops_member_from_endpoints() {
    let state_path = temp_state_path("zone-silence");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let _ = tx1.recv_ctl_within(5).await; // пустой набор при появлении
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;

    let id = create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    let (status, _) = http_body(
        port, "PATCH", &format!("/api/zones/{id}"),
        r#"{"silence_timeout_s":1,"silence_threshold_db":-60}"#,
    ).await;
    assert_eq!(status, 200);

    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id, &["127.0.0.3"], 40));
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::Start);
    // дефолт зоны уходит после Start; холд тишины карту не перепушивает
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx1"));

    tx1.send_tx_rms(-90.0, -90.0).await;
    assert_eq!(tx1.recv_ctl_within(8).await, held_stream(&id, 40));
    tx1.assert_silent_within(1).await; // Stop не уходит: захват жив
    r1.assert_silent_within(1).await;

    let zones = get_zones(port).await;
    let z = zones.as_array().unwrap().iter().find(|z| z["id"] == id).unwrap();
    assert_eq!(z["silenced"], true);
    assert_eq!(z["active"], true);

    tx1.send_tx_rms(-20.0, -90.0).await;
    assert_eq!(tx1.recv_ctl_within(8).await, zone_stream("tx1", &id, &["127.0.0.3"], 40));
    let zones = get_zones(port).await;
    let z = zones.as_array().unwrap().iter().find(|z| z["id"] == id).unwrap();
    assert_eq!(z["silenced"], false);
}

/// Стоп зоны: tx получает пустой SetEndpoints и Stop, rx-член — Stop.
#[tokio::test]
async fn zone_stop_stops_playing_nodes() {
    let state_path = temp_state_path("zone-stop");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let _ = tx1.recv_ctl_within(5).await; // пустой набор при появлении
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;

    let id = create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    let _ = tx1.recv_ctl_within(5).await; // SetEndpoints
    let _ = tx1.recv_ctl_within(5).await; // Start
    let _ = r1.recv_ctl_within(5).await; // Start
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx1"));

    let (status, _) = http(port, "POST", &format!("/api/zones/{id}/stop")).await;
    assert_eq!(status, 200);
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Stop);
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::Stop);

    drop((tx1, r1));
}

/// Кража члена: угнанный rx продолжает играть — Stop/Start ему не уходят;
/// tx-донор с уменьшенным набором не трогается; доведённый до пустого
/// набора получает SetEndpoints{[]} и Stop.
#[tokio::test]
async fn stealing_keeps_stolen_rx_playing() {
    let state_path = temp_state_path("steal-play");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let mut tx2 = MockAgent::connect_as_from(port, "tx2", Role::Tx, "127.0.0.6").await;
    let mut tx3 = MockAgent::connect_as_from(port, "tx3", Role::Tx, "127.0.0.7").await;
    for tx in [&mut tx1, &mut tx2, &mut tx3] {
        let _ = tx.recv_ctl_within(5).await; // пустой набор при появлении
    }
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    let mut r2 = MockAgent::connect_as_from(port, "r2", Role::Rx, "127.0.0.4").await;
    for n in ["r1", "r2"] {
        wait_node(port, n).await;
    }

    // A [r1, r2] → tx1: все трое в играющем множестве
    let id_a = create_and_start_zone(port, r#"{"name":"A","tx":"tx1","members":["r1","r2"]}"#).await;
    let _ = tx1.recv_ctl_within(5).await; // SetEndpoints {[r1, r2]}
    let _ = tx1.recv_ctl_within(5).await; // Start
    let _ = r1.recv_ctl_within(5).await; // Start
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx1"));
    let _ = r2.recv_ctl_within(5).await; // Start
    assert_eq!(
        r2.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r2.recv_ctl_within(5).await, mix_of("tx1"));

    // B [r2] → tx2, активирована позже: забирает r2. r2 играет дальше —
    // Stop/Start ему не нужны; tx1 лишь получает набор без r2.
    let id_b = create_and_start_zone(port, r#"{"name":"B","tx":"tx2","members":["r2"]}"#).await;
    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id_a, &["127.0.0.3"], 40));
    assert_eq!(tx2.recv_ctl_within(5).await, zone_stream("tx2", &id_b, &["127.0.0.4"], 40));
    assert_eq!(tx2.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(r2.recv_ctl_within(5).await, mix_of("tx2"));
    r1.assert_silent_within(3).await;

    // C [r1] → tx3, самый поздний seq: забирает r1 — у tx1 набор пустой:
    // SetEndpoints{[]} и Stop; r1 играет дальше (теперь слушает tx3).
    let id_c = create_and_start_zone(port, r#"{"name":"C","tx":"tx3","members":["r1"]}"#).await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Stop);
    assert_eq!(
        tx3.recv_ctl_within(5).await,
        zone_stream("tx3", &id_c, &["127.0.0.3"], 40)
    );
    assert_eq!(tx3.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx3"));

    drop((tx1, tx2, tx3, r1, r2));
}

/// Триггер пушера без изменения раскладки (тот же dscp): SetEndpoints не
/// перепушивается (last_sent), Start/Stop не дублируются (diff пуст).
#[tokio::test]
async fn pusher_retrigger_without_changes_is_silent() {
    let state_path = temp_state_path("retrigger");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let _ = tx1.recv_ctl_within(5).await;
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;
    create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    let _ = tx1.recv_ctl_within(5).await; // SetEndpoints
    let _ = tx1.recv_ctl_within(5).await; // Start
    let _ = r1.recv_ctl_within(5).await; // Start
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx1"));

    // тот же dscp — триггер есть, изменения нет
    let (status, _) = http_body(port, "POST", "/api/dscp", r#"{"dscp":40}"#).await;
    assert_eq!(status, 200);
    tx1.assert_silent_within(3).await;
    r1.assert_silent_within(3).await;

    drop((tx1, r1));
}

/// Оффлайн-узел в играющем множестве: его Stop падает с Offline/Closed —
/// warn без паники, узел считается обработанным и бесконечно не ретраится
/// (следующий проход молчит). Живой tx при этом получает SetEndpoints{[]}
/// и собственный Stop.
#[tokio::test]
async fn offline_playing_node_stop_fails_soft() {
    let state_path = temp_state_path("offline-play");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let _ = tx1.recv_ctl_within(5).await;
    let r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;
    let id = create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    let _ = tx1.recv_ctl_within(5).await; // SetEndpoints
    let _ = tx1.recv_ctl_within(5).await; // Start

    drop(r1); // WS разорван → mark_offline → пересчёт
    assert_eq!(tx1.recv_ctl_within(5).await, held_stream(&id, 40));
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Stop);

    // повторный триггер: неудавшийся Stop к r1 не ретраится бесконечно
    let (status, _) = http_body(port, "POST", "/api/dscp", r#"{"dscp":40}"#).await;
    assert_eq!(status, 200);
    tx1.assert_silent_within(3).await;

    drop(tx1);
}

/// Спека §2: active переживает рестарт контроллера. Свежий контроллер по
/// сохранённому state: первый пересчёт пушера шлёт Start всем играющим.
#[tokio::test]
async fn controller_restart_restarts_playing_zone() {
    let state_path = temp_state_path("restart-play");
    // state «после рестарта»: активная зона сохранена на диске
    std::fs::write(
        &state_path,
        r#"{"version":1,"zones":[
            {"id":"z1","name":"Зал","tx":"tx1","members":["r1"],"active":true,"seq":1}
        ],"next_seq":2}"#,
    )
    .unwrap();
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    // r1 подключается первым: зона уже активна, hello сразу шлёт карту
    // владельца. Start — когда tx тоже online.
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;
    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;

    // первый пересчёт по hello tx: набор + Start обоим играющим
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        zone_stream("tx1", "z1", &["127.0.0.3"], 40)
    );
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);
    // Карта владельца приходит на hello и может повториться пушером,
    // если проход случился ещё без tx. Start обязателен.
    let mut saw_start = false;
    for _ in 0..3 {
        match r1.recv_ctl_within(5).await {
            CtlMsg::Start => {
                saw_start = true;
                break;
            }
            CtlMsg::SetChannelMap { map, .. } => assert_eq!(map, ChannelMap::default()),
            CtlMsg::SetMix { .. } => {}
            other => panic!("неожиданный кадр r1: {other:?}"),
        }
    }
    assert!(saw_start, "r1 не получил Start после рестарта");

    drop((tx1, r1));
}

// Fix round 1, Important #1: reconnect tx быстрее дебаунса не должен терять
// SetEndpoints — форс-пуш по факту re-hello (hello_seq реестра), а не по
// «was online в прошлом проходе пушера». Набор при reconnect не меняется.

#[tokio::test]
async fn rehello_tx_receives_endpoints_again() {
    let state_path = temp_state_path("rehello");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    let r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;
    let id = create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    // первый набор — от пуска зоны
    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id, &["127.0.0.3"], 40));

    // разрыв + re-hello в одном окне дебаунса; набор тот же — старый пушер
    // (was_online) здесь молчал бы, новый процесс агента остался бы глухим
    drop(tx1);
    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        zone_stream("tx1", &id, &["127.0.0.3"], 40),
        "re-hello tx: повторный SetEndpoints с тем же набором"
    );
    // и Start по свежему hello: новый процесс агента потерял want_playing,
    // diff играющего множества пуст — Start идёт по факту re-hello
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::Start,
        "re-hello играющего tx: Start по свежему hello"
    );

    drop(r1);
}

// Fix round 2, Important: re-hello ИГРАЮЩЕГО узла внутри окна дебаунса —
// offline+online схлопнулись в один проход, diff пуст. Без Start по факту
// свежего hello новый процесс агента молчал бы (want_playing=false) до
// смены раскладки. Покрыт rx-only узел: fresh-hello Start не только для tx.

#[tokio::test]
async fn rehello_playing_rx_receives_start_again() {
    let state_path = temp_state_path("rehello-rx");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let _ = tx1.recv_ctl_within(5).await; // пустой набор при появлении
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;
    create_and_start_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    let _ = tx1.recv_ctl_within(5).await; // SetEndpoints
    let _ = tx1.recv_ctl_within(5).await; // Start
    let _ = r1.recv_ctl_within(5).await; // Start
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );

    // рестарт агента r1 внутри окна дебаунса: drop + reconnect без паузы.
    // Hello активной зоны сразу шлёт карту, затем Start по свежему hello,
    // и пушер повторяет карту после блока Start/Stop.
    drop(r1);
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] },
        "hello активной зоны шлёт карту владельца"
    );
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::Start,
        "re-hello играющего rx: Start по свежему hello"
    );
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] },
        "пушер повторяет карту по свежему hello"
    );

    drop((tx1, r1));
}

// Fix round 1, Important #2: идемпотентность start внутри update_state —
// конкурентные пуски активной зоны не переназначают seq и не растят next_seq.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zone_double_start_keeps_seq() {
    let state_path = temp_state_path("double-start");
    let state = AppState::with_state(&state_path);
    let port = serve(state).await;

    let tx1 = MockAgent::connect_as(port, "tx1", Role::Tx).await;
    let r1 = MockAgent::connect_as(port, "r1", Role::Rx).await;
    wait_node(port, "tx1").await;
    wait_node(port, "r1").await;

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"tx1","members":["r1"]}"#).await;
    assert_eq!(status, 200);
    let id = zone["id"].as_str().unwrap().to_string();
    let path = format!("/api/zones/{id}/start");

    // два ОДНОВРЕМЕННЫХ пуска: на многопоточном рантайме оба handler'а могут
    // пройти предпроверку active=false — идемпотентность ВНУТРИ update_state
    // должна дать ровно одну активацию (seq = 1, next_seq = 2).
    let ((s1, b1), (s2, b2)) =
        tokio::join!(http(port, "POST", &path), http(port, "POST", &path));
    assert_eq!((s1, s2), (200, 200));
    for b in [&b1, &b2] {
        let z: Value = serde_json::from_str(b).unwrap();
        assert_eq!(z["active"], true);
        assert_eq!(z["seq"], 1, "оба пуска видят одну и ту же активацию: {b}");
    }

    // и последовательный третий пуск — тоже без эффекта
    let (status, body) = http(port, "POST", &path).await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["seq"], 1);

    let on_disk: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
    assert_eq!(
        on_disk["next_seq"], 2,
        "три пуска — next_seq вырос ровно один раз"
    );

    drop((tx1, r1));
}

// Карта каналов живёт в зоне. PATCH channel_map узла больше не пушит
// SetChannelMap и не пишет карту в nodes. display_name по-прежнему сохраняется.

#[tokio::test]
async fn patch_channel_map_pushes_set_channel_map() {
    let state_path = temp_state_path("chmap");
    let state = AppState::with_state(&state_path);
    let port = serve(state).await;

    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;

    // PATCH channel_map узла: HTTP 200, карта в теле как была (null), пуша нет.
    let (status, body) = http_body(
        port,
        "PATCH",
        "/api/nodes/r1",
        r#"{"channel_map":{"out_l":"l","out_r":null}}"#,
    )
    .await;
    assert_eq!(status, 200);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert!(body["channel_map"].is_null(), "карта узла не меняется: {body}");
    r1.assert_silent_within(2).await;

    let on_disk = std::fs::read_to_string(&state_path).unwrap_or_default();
    let parsed: Value = serde_json::from_str(&on_disk).unwrap_or(Value::Null);
    let nodes_txt = parsed.get("nodes").map(|n| n.to_string()).unwrap_or_default();
    assert!(
        !nodes_txt.contains("channel_map"),
        "в nodes нет channel_map: {on_disk}"
    );

    // PATCH display_name → мета узла обновилась (в state и в view)
    let (status, _) = http_body(port, "PATCH", "/api/nodes/r1", r#"{"display_name":"Кухня"}"#).await;
    assert_eq!(status, 200);
    let nodes = wait_for(port, |n| {
        n.as_array()
            .is_some_and(|a| a.iter().any(|v| v["name"] == "r1" && v["display_name"] == "Кухня"))
    })
    .await;
    assert_eq!(node(&nodes, "r1")["display_name"], "Кухня");
    assert!(node(&nodes, "r1")["channel_map"].is_null());
}

// Карта только на узле и без активной зоны на воспроизведение не влияет:
// hello SetChannelMap не получает. Мета узла в GET /api/nodes остаётся.

#[tokio::test]
async fn hello_receives_stored_channel_map() {
    let state_path = temp_state_path("hello-chmap");
    // state «с прошлой жизни»: карта и display_name для r1 записаны заранее,
    // активных зон нет.
    std::fs::write(
        &state_path,
        r#"{"version":1,"nodes":{"r1":{"display_name":"Кухня","channel_map":{"out_l":"l","out_r":null}}},"zones":[],"next_seq":1}"#,
    )
    .unwrap();
    let state = AppState::with_state(&state_path);
    let port = serve(state).await;

    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    r1.assert_silent_within(2).await;

    let nodes = wait_for(port, |n| n.as_array().is_some_and(|a| a.iter().any(|v| v["name"] == "r1")))
        .await;
    assert_eq!(node(&nodes, "r1")["display_name"], "Кухня");
    assert_eq!(node(&nodes, "r1")["channel_map"]["out_l"], "l");
}

/// Карта зоны уходит rx-владельцу: на пуске — дефолт (стерео) после Start,
/// PATCH maps пушит новую карту, поздняя зона забирает члена со своей картой.
#[tokio::test]
async fn zone_map_pushed_to_owning_rx() {
    let state_path = temp_state_path("zone-map");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let _ = tx1.recv_ctl_within(5).await;
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "r1").await;

    let id_a = create_and_start_zone(port, r#"{"name":"A","tx":"tx1","members":["r1"]}"#).await;
    let _ = tx1.recv_ctl_within(5).await; // SetEndpoints
    let _ = tx1.recv_ctl_within(5).await; // Start
    // дефолт зоны — стерео, пушер шлёт его владельцу
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx1"));

    let (status, _) = http_body(
        port, "PATCH", &format!("/api/zones/{id_a}"),
        r#"{"maps":{"r1":{"out_l":"l","out_r":null}}}"#,
    ).await;
    assert_eq!(status, 200);
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap { out_l: Some(ChanSel::L), out_r: None }, branches: vec![] }
    );

    let id_b = create_and_start_zone(port, r#"{"name":"B","tx":"tx1","members":["r1"]}"#).await;
    // B забрала r1, её карта ещё стерео (дефолт миграции/создания) — пуш до PATCH.
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    let (status, _) = http_body(
        port, "PATCH", &format!("/api/zones/{id_b}"),
        r#"{"maps":{"r1":{"out_l":"r","out_r":"r"}}}"#,
    ).await;
    assert_eq!(status, 200);
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap { out_l: Some(ChanSel::R), out_r: Some(ChanSel::R) }, branches: vec![] }
    );
}

/// Смена только запаса зоны шлёт SetMix владельцу: тот же порт, новый jb_ms.
/// Колонка из списка другой зоны получает запас держателя, не той зоны.
#[tokio::test]
async fn zone_jb_ms_follows_the_holder() {
    let state_path = temp_state_path("zone-jb");
    let state = AppState::with_state(&state_path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut tx1 = MockAgent::connect_as_from(port, "tx1", Role::Tx, "127.0.0.2").await;
    let mut tx2 = MockAgent::connect_as_from(port, "tx2", Role::Tx, "127.0.0.6").await;
    assert_eq!(
        tx1.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    assert_eq!(
        tx2.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );
    let mut r1 = MockAgent::connect_as_from(port, "r1", Role::Rx, "127.0.0.3").await;
    let mut r2 = MockAgent::connect_as_from(port, "r2", Role::Rx, "127.0.0.4").await;
    wait_node(port, "r1").await;
    wait_node(port, "r2").await;

    let (status, zone) = create_zone(port, r#"{"name":"A","tx":"tx1","members":["r1","r2"]}"#).await;
    assert_eq!(status, 200, "{zone}");
    assert_eq!(zone["jb_ms"], 2, "создание без поля — запас 2");
    let id_a = zone["id"].as_str().unwrap().to_string();
    let (status, _) = http(port, "POST", &format!("/api/zones/{id_a}/start")).await;
    assert_eq!(status, 200);

    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id_a, &["127.0.0.3", "127.0.0.4"], 40));
    assert_eq!(tx1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(r1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r1.recv_ctl_within(5).await, mix_of("tx1"));
    assert_eq!(r2.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        r2.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(r2.recv_ctl_within(5).await, mix_of("tx1"));

    let (status, _) = http_body(port, "PATCH", &format!("/api/zones/{id_a}"), r#"{"jb_ms":0}"#).await;
    assert_eq!(status, 422);
    let (status, _) = http_body(port, "PATCH", &format!("/api/zones/{id_a}"), r#"{"jb_ms":501}"#).await;
    assert_eq!(status, 422);

    let (status, patched) = http_body(port, "PATCH", &format!("/api/zones/{id_a}"), r#"{"jb_ms":40}"#).await;
    assert_eq!(status, 200, "{patched}");
    let patched: Value = serde_json::from_str(&patched).unwrap();
    assert_eq!(patched["jb_ms"], 40);
    let held = CtlMsg::SetMix { ports: vec![mix_port("tx1")], jb_ms: 40 };
    assert_eq!(r1.recv_ctl_within(5).await, held);
    assert_eq!(r2.recv_ctl_within(5).await, held.clone());

    let (status, zone_b) = create_zone(
        port,
        r#"{"name":"B","tx":"tx2","members":["r2"],"jb_ms":15}"#,
    )
    .await;
    assert_eq!(status, 200, "{zone_b}");
    assert_eq!(zone_b["jb_ms"], 15, "создание сохраняет запас");
    let id_b = zone_b["id"].as_str().unwrap().to_string();
    let (status, _) = http(port, "POST", &format!("/api/zones/{id_b}/start")).await;
    assert_eq!(status, 200);

    assert_eq!(tx1.recv_ctl_within(5).await, zone_stream("tx1", &id_a, &["127.0.0.3"], 40));
    assert_eq!(tx2.recv_ctl_within(5).await, zone_stream("tx2", &id_b, &["127.0.0.4"], 40));
    assert_eq!(tx2.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        r2.recv_ctl_within(5).await,
        CtlMsg::SetMix { ports: vec![mix_port("tx2")], jb_ms: 15 },
        "r2 держит B — запас B, не 40 зоны A, где r2 лишь числится"
    );

    let (status, _) = http_body(port, "PATCH", &format!("/api/zones/{id_a}"), r#"{"jb_ms":80}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        r1.recv_ctl_within(5).await,
        CtlMsg::SetMix { ports: vec![mix_port("tx1")], jb_ms: 80 }
    );
    r2.assert_silent_within(2).await;

    let zones = get_zones(port).await;
    let a = zones.as_array().unwrap().iter().find(|z| z["id"] == id_a).unwrap();
    let b = zones.as_array().unwrap().iter().find(|z| z["id"] == id_b).unwrap();
    assert_eq!(a["jb_ms"], 80);
    assert_eq!(b["jb_ms"], 15);
    let on_disk = std::fs::read_to_string(&state_path).unwrap();
    assert!(on_disk.contains("\"jb_ms\": 80"), "запас A в state.json: {on_disk}");
    assert!(on_disk.contains("\"jb_ms\": 15"), "запас B в state.json: {on_disk}");

    drop((tx1, tx2, r1, r2));
}

/// Создание с запасом вне 1..=500 — 422, зона не пишется. Без поля остаётся 2
/// (это уже проверяет `zone_jb_ms_follows_the_holder`).
#[tokio::test]
async fn create_zone_rejects_jb_ms_outside_range() {
    let state_path = temp_state_path("zone-jb-create");
    let state = AppState::with_state(&state_path);
    let port = serve(state).await;

    let _tx1 = MockAgent::connect_as(port, "tx1", Role::Tx).await;
    let _r1 = MockAgent::connect_as(port, "r1", Role::Rx).await;
    wait_node(port, "tx1").await;
    wait_node(port, "r1").await;

    for bad in [0, 501] {
        let (status, _) = http_body(
            port,
            "POST",
            "/api/zones",
            &format!(r#"{{"name":"плохо-{bad}","tx":"tx1","members":["r1"],"jb_ms":{bad}}}"#),
        )
        .await;
        assert_eq!(status, 422, "jb_ms {bad}");
    }
    assert_eq!(get_zones(port).await, serde_json::json!([]));
    let on_disk = std::fs::read_to_string(&state_path).unwrap_or_default();
    assert!(
        !on_disk.contains("плохо-"),
        "отказ не остаётся в state.json: {on_disk}"
    );
}

/// Валидации зон (спека §3): неизвестные узлы и несоответствие ролей — 422,
/// зона с несуществующим id — 404.
#[tokio::test]
async fn zone_api_validations() {
    let state_path = temp_state_path("zone-valid");
    let state = AppState::with_state(&state_path);
    let port = serve(state).await;

    let _tx1 = MockAgent::connect_as(port, "tx1", Role::Tx).await;
    let _r1 = MockAgent::connect_as(port, "r1", Role::Rx).await;
    wait_node(port, "tx1").await;
    wait_node(port, "r1").await;

    // неизвестный tx
    let (status, _) = http_body(port, "POST", "/api/zones", r#"{"name":"z","tx":"ghost","members":["r1"]}"#).await;
    assert_eq!(status, 422);
    // tx без роли Tx
    let (status, _) = http_body(port, "POST", "/api/zones", r#"{"name":"z","tx":"r1","members":["r1"]}"#).await;
    assert_eq!(status, 422);
    // неизвестный member
    let (status, _) = http_body(port, "POST", "/api/zones", r#"{"name":"z","tx":"tx1","members":["ghost"]}"#).await;
    assert_eq!(status, 422);
    // member без роли Rx
    let (status, _) = http_body(port, "POST", "/api/zones", r#"{"name":"z","tx":"tx1","members":["tx1"]}"#).await;
    assert_eq!(status, 422);

    // ни одна зона не создана
    let zones = get_zones(port).await;
    assert_eq!(zones, serde_json::json!([]));

    // неизвестная зона: PATCH/DELETE/пуск — 404
    let (status, _) = http_body(port, "PATCH", "/api/zones/nope", r#"{"name":"x"}"#).await;
    assert_eq!(status, 404);
    let (status, _) = http(port, "DELETE", "/api/zones/nope").await;
    assert_eq!(status, 404);
    let (status, _) = http(port, "POST", "/api/zones/nope/start").await;
    assert_eq!(status, 404);

    // PATCH узла: неизвестный узел — 422
    let (status, _) = http_body(port, "PATCH", "/api/nodes/ghost", r#"{"display_name":"x"}"#).await;
    assert_eq!(status, 422);
}

/// Обратная совместимость (спека §3): старые пути /api/nodes/{name}/start|stop
/// резолвят узел по node_id, затем по имени-алиасу.
#[tokio::test]
async fn node_paths_resolve_by_node_id_then_name() {
    let state_path = temp_state_path("alias");
    let state = AppState::with_state(&state_path);
    let port = serve(state).await;

    // node_id отличен от имени
    let mut a = MockAgent::connect_named(port, "id-x", "pretty", Role::Rx).await;
    wait_node(port, "pretty").await;

    // пуск по имени-алиасу → ответ и команда по node_id
    let (status, body) = http(port, "POST", "/api/nodes/pretty/start").await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["sent"],
        "id-x",
        "ответ по node_id: {body}"
    );
    assert_eq!(a.recv_ctl().await, CtlMsg::Start);

    // стоп по node_id
    let (status, body) = http(port, "POST", "/api/nodes/id-x/stop").await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["sent"], "id-x");
    assert_eq!(a.recv_ctl().await, CtlMsg::Stop);

    // PATCH по имени-алиасу резолвится тоже
    let (status, body) = http_body(port, "PATCH", "/api/nodes/pretty", r#"{"display_name":"Зал"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["node_id"], "id-x");
}


// --- Task 10: трансфер мастера и статус мастера. ---

/// POST /api/master/transfer: неизвестный узел — 404; hello-нутый, но
/// ушедший в offline — 409. Успешный путь здесь не идёт — он тянет за собой
/// BecomeMaster/ControllerMoved (см. transfer_moves_master_with_broadcast).
#[tokio::test]
async fn transfer_rejects_unknown_and_offline() {
    let state_path = temp_state_path("transfer-rej");
    let mut state = AppState::with_state(&state_path);
    state.set_post_transfer_stop(Arc::new(|| {})); // systemd не трогаем
    let port = serve(state).await;

    // неизвестный узел → 404
    let (status, _) = http_body(port, "POST", "/api/master/transfer", r#"{"node_id":"нет-такого"}"#).await;
    assert_eq!(status, 404);

    // узел hello-нут → online; разрыв WS → offline → 409
    let a = MockAgent::connect(port, "m1").await;
    wait_node(port, "m1").await;
    drop(a);
    wait_for(port, |n| {
        n.as_array()
            .is_some_and(|arr| arr.iter().any(|v| v["name"] == "m1" && v["online"] == false))
    })
    .await;
    let (status, _) = http_body(port, "POST", "/api/master/transfer", r#"{"node_id":"m1"}"#).await;
    assert_eq!(status, 409);
}

/// Трансфер мастера цели (online, ip 127.0.0.2):
/// 1. цель получает BecomeMaster с сериализованным state контроллера
/// 2. контроллер ждёт TCP на ip:8080 (имитация — локальный listener)
/// 3. master_node_id в state и в GET /api/master
/// 4. второй узел получает ControllerMoved на ws-адрес цели
/// 5. локальный стоп вызван через hook (в проде — systemctl через 500 мс)
#[tokio::test]
async fn transfer_moves_master_with_broadcast() {
    let state_path = temp_state_path("transfer-ok");
    let mut state = AppState::with_state(&state_path);
    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_flag = stopped.clone();
    state.set_post_transfer_stop(Arc::new(move || {
        stopped_flag.store(true, Ordering::SeqCst);
    }));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    state.set_listen_addr(format!("127.0.0.1:{port}"));
    tokio::spawn(async move {
        axum::serve(listener, app(state).into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap()
    });

    // GET /api/master до трансфера: addr — наш listen, node_id ещё null
    let (status, body) = http(port, "GET", "/api/master").await;
    assert_eq!(status, 200);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["addr"], format!("ws://127.0.0.1:{port}/ws"));
    assert!(body["node_id"].is_null());

    // «поднявшийся контроллер» цели: TCP-listener на её ip:CONTROLLER_PORT
    let _up = TcpListener::bind(("127.0.0.2", zvucast::discovery::CONTROLLER_PORT))
        .await
        .unwrap();

    // мишень с ip 127.0.0.2 и второй узел — получатель ControllerMoved
    let mut target = MockAgent::connect_as_from(port, "m1", Role::Rx, "127.0.0.2").await;
    let mut other = MockAgent::connect_as(port, "m2", Role::Rx).await;
    wait_node(port, "m1").await;
    wait_node(port, "m2").await;

    let (status, body) = http_body(port, "POST", "/api/master/transfer", r#"{"node_id":"m1"}"#).await;
    assert_eq!(status, 200);
    let posted: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(posted["ok"], true);
    assert_eq!(posted["url"], "http://127.0.0.2:8080/");

    // цель получила BecomeMaster с сериализованным state контроллера
    match target.recv_ctl_within(5).await {
        CtlMsg::BecomeMaster { state } => {
            assert!(state.contains("\"version\""), "state в BecomeMaster: {state}");
            assert!(state.contains("master_node_id"), "state в BecomeMaster: {state}");
        }
        other_msg => panic!("ожидали BecomeMaster, получили {other_msg:?}"),
    }

    // второй узел получил ControllerMoved на ws-адрес цели
    assert_eq!(
        other.recv_ctl_within(5).await,
        CtlMsg::ControllerMoved { addr: "ws://127.0.0.2:8080/ws".into() }
    );

    // master_node_id записан в state.json и отдаётся GET /api/master
    let on_disk = std::fs::read_to_string(&state_path).unwrap();
    assert!(
        on_disk.contains("\"master_node_id\": \"m1\""),
        "state: {on_disk}"
    );
    let (status, body) = http(port, "GET", "/api/master").await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["node_id"], "m1");

    // локальный стоп вызван (hook вместо systemctl)
    assert!(stopped.load(Ordering::SeqCst));

    drop(_up);
}

/// C1, «мина» 502-пути: цель трансфера контроллер не подняла → 502, и цель
/// получила DisableController — гасит свой контроллер и маркер преемника,
/// иначе enabled-юнит стартанул бы сам при поздней смерти старого мастера,
/// некоординированно, со stale-state. state при этом не тронут.
#[tokio::test]
async fn transfer_failure_disables_controller_on_target() {
    let state_path = temp_state_path("transfer-502");
    let mut state = AppState::with_state(&state_path);
    state.set_post_transfer_stop(Arc::new(|| {})); // systemd не трогаем
    state.set_transfer_wait(Duration::from_secs(1)); // hook: не ждать 10 с
    let port = serve(state).await;

    // цель с ip 127.0.0.3; на 127.0.0.3:8080 никто не слушает
    let mut target = MockAgent::connect_as_from(port, "m1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "m1").await;

    let (status, body) = http_body(port, "POST", "/api/master/transfer", r#"{"node_id":"m1"}"#).await;
    assert_eq!(status, 502);
    assert!(body.contains("не поднялся"), "тело 502: {body}");

    // цель сначала получила BecomeMaster, затем — откат DisableController
    match target.recv_ctl_within(5).await {
        CtlMsg::BecomeMaster { state } => {
            assert!(state.contains("\"version\""), "state в BecomeMaster: {state}");
        }
        other_msg => panic!("ожидали BecomeMaster, получили {other_msg:?}"),
    }
    assert_eq!(
        target.recv_ctl_within(5).await,
        CtlMsg::DisableController,
        "на 502 цель получает откат: DisableController"
    );

    // state не тронут: master_node_id не записан
    let (status, body) = http(port, "GET", "/api/master").await;
    assert_eq!(status, 200);
    assert!(
        serde_json::from_str::<Value>(&body).unwrap()["node_id"].is_null(),
        "трансфер не состоялся — master_node_id не записан: {body}"
    );
}

/// Обрыв HTTP-клиента посередине ожидания не отменяет откат.
/// BecomeMaster уже ушёл; без detached-задачи drop соединения съедал
/// handler и DisableController не доезжал — на цели оставался второй мастер.
#[tokio::test]
async fn transfer_client_disconnect_still_rolls_back() {
    let state_path = temp_state_path("transfer-drop");
    let mut state = AppState::with_state(&state_path);
    state.set_post_transfer_stop(Arc::new(|| {}));
    state.set_transfer_wait(Duration::from_millis(800));
    let port = serve(state).await;

    // 127.0.0.4:8080 никто не слушает — wait дойдёт до дедлайна и откатится.
    let mut target = MockAgent::connect_as_from(port, "m1", Role::Rx, "127.0.0.4").await;
    wait_node(port, "m1").await;

    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let body = r#"{"node_id":"m1"}"#;
    let req = format!(
        "POST /api/master/transfer HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();

    match target.recv_ctl_within(5).await {
        CtlMsg::BecomeMaster { .. } => {}
        other_msg => panic!("ожидали BecomeMaster, получили {other_msg:?}"),
    }
    drop(s);

    assert_eq!(
        target.recv_ctl_within(5).await,
        CtlMsg::DisableController,
        "обрыв клиента не отменяет откат"
    );
}

#[tokio::test]
async fn virtual_output_is_stored_and_pushed_only_to_a_capable_node() {
    let path = temp_state_path("virtual-output");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::connect_virtual(port).await;

    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(
        pc.recv_raw_within(2).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"virtual_output":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: true });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"display_name":"Комната"}"#).await;
    assert_eq!(status, 200);
    let (code, nodes) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200);
    let nodes: Value = serde_json::from_str(&nodes).unwrap();
    let pc_json = nodes.as_array().unwrap().iter().find(|n| n["name"] == "pc").unwrap();
    assert_eq!(pc_json["virtual_output"], true);
    assert_eq!(pc_json["supports_virtual_output"], true);
    assert_eq!(pc_json["display_name"], "Комната");

    let mut reported = zvucast::inputs::virtual_capture_channels();
    reported.push(zproto::CaptureChan {
        id: "hw:0".into(),
        device_key: "plughw:CARD=Generic_1".into(),
        device_name: "HD-Audio Generic".into(),
        index: 0,
    });
    reported.push(zproto::CaptureChan {
        id: "zvucast-hall:0".into(),
        device_key: "zvucast-hall".into(),
        device_name: "Zvucast: Зал".into(),
        index: 0,
    });
    let inputs = AgentMsg::Inputs { inputs: reported };
    pc.ws.send(Message::Text(serde_json::to_string(&inputs).unwrap())).await.unwrap();
    assert_eq!(
        pc.recv_raw_within(2).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    let (code, nodes) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200);
    let nodes: Value = serde_json::from_str(&nodes).unwrap();
    let names: Vec<&str> = nodes
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "pc")
        .unwrap()["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["device_name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["HD-Audio Generic", "Zvucast: Зал"]);

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":[]}"#).await;
    assert_eq!(status, 200, "{zone}");
    let hall = zone["id"].as_str().unwrap().to_string();
    // Микрофон без связи вида: устройства и потоки пустые.
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
    let (status, zone) = create_zone(port, r#"{"name":"Спальня","tx":"pc","members":[]}"#).await;
    assert_eq!(status, 200, "{zone}");
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{hall}"),
        r#"{"sources":["pc",{"kind":"virtual","node_id":"pc"}]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetVirtualZones {
            zones: vec![zproto::VirtualZone { zone_id: hall.clone(), name: "Зал".into() }],
        }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetFeeds {
            feeds: vec![zproto::FeedSend {
                zone_id: hall.clone(),
                kind: zproto::FeedKind::Virtual,
                monitor: zvucast::virtual_output::monitor_name(&hall),
                endpoints: vec![],
            }],
        }
    );
}

/// Микрофон и virtual в одной зоне: SetEndpoints остаётся одной группой
/// с `source: None`. Колонка смешивает порт микрофона и порт вида.
#[tokio::test]
async fn virtual_feed_does_not_duplicate_endpoints() {
    let path = temp_state_path("virtual-feed");
    let state = AppState::with_state(&path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(
        pc.recv_raw_within(2).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
    assert_eq!(
        pc.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"virtual_output":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: true });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let mut s1 = MockAgent::connect_as_from(port, "s1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "s1").await;

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":["s1"]}"#).await;
    assert_eq!(status, 200, "{zone}");
    let z1 = zone["id"].as_str().unwrap().to_string();
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http(port, "POST", &format!("/api/zones/{z1}/start")).await;
    assert_eq!(status, 200);
    let endpoints = pc.recv_ctl_within(5).await;
    assert_eq!(endpoints, zone_stream("pc", &z1, &["127.0.0.3"], 40));
    match &endpoints {
        CtlMsg::SetEndpoints { zones, .. } => {
            assert_eq!(zones.len(), 1);
            assert_eq!(zones[0].source, None);
            assert_eq!(zones.iter().filter(|z| z.zone_id == z1).count(), 1);
        }
        other => panic!("ждали одну группу микрофона, пришло {other:?}"),
    }
    assert_eq!(pc.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(s1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        s1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(s1.recv_ctl_within(5).await, CtlMsg::SetMix { ports: vec![mix_port("pc")], jb_ms: 2 });

    let (status, body) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{z1}"),
        r#"{"sources":["pc",{"kind":"virtual","node_id":"pc"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let view: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(view["sources"], serde_json::json!(["pc"]));
    assert_eq!(view["source_maps"], serde_json::json!({}), "новый хозяин не получает карту по умолчанию");

    let feed_name = zvucast::mix::feed_port_name(zproto::FeedKind::Virtual, "pc", &z1);
    let names = vec!["pc".to_string(), feed_name.clone()];
    let mic = assigned_port(&names, "pc");
    let feed_port = assigned_port(&names, &feed_name);
    assert_ne!(mic, feed_port);
    let feed = zproto::FeedSend {
        zone_id: z1.clone(),
        kind: zproto::FeedKind::Virtual,
        monitor: zvucast::virtual_output::monitor_name(&z1),
        endpoints: vec![Endpoint { ip: "127.0.0.3".into(), port: feed_port }],
    };
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetVirtualZones {
            zones: vec![zproto::VirtualZone { zone_id: z1.clone(), name: "Зал".into() }],
        }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![feed.clone()] });
    // Пушер повторяет SetFeeds: немедленная отправка last_feeds не пишет.
    assert_eq!(pc.recv_ctl_within(5).await, CtlMsg::SetFeeds { feeds: vec![feed] });

    let mut branches = vec![
        BranchChannelMap { port: mic, map: ChannelMap::default() },
        BranchChannelMap { port: feed_port, map: ChannelMap::default() },
    ];
    branches.sort_by_key(|b| b.port);
    assert_eq!(
        s1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches }
    );
    let mut ports = vec![mic, feed_port];
    ports.sort();
    assert_eq!(s1.recv_ctl_within(5).await, CtlMsg::SetMix { ports, jb_ms: 2 });

    let (status, _) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{z1}"),
        r#"{"sources":["pc"]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
    assert_eq!(pc.recv_ctl_within(5).await, CtlMsg::SetFeeds { feeds: vec![] });
    assert_eq!(
        s1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(s1.recv_ctl_within(5).await, CtlMsg::SetMix { ports: vec![mix_port("pc")], jb_ms: 2 });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"dscp":41}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(5).await, zone_stream("pc", &z1, &["127.0.0.3"], 41));
}

/// Один узел ведёт Spotify в двух зонах. У каждой свой монитор и свои колонки.
/// Зона без микрофона стартует хозяина и шлёт SetFeeds.
#[tokio::test]
async fn spotify_feeds_stay_in_their_zones() {
    let path = temp_state_path("spotify-feeds");
    let state = AppState::with_state(&path);
    spawn_endpoint_pusher(state.clone());
    let port = serve(state).await;

    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(
        pc.recv_raw_within(2).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
    assert_eq!(
        pc.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 40 }
    );

    let (status, _) = http_body(
        port,
        "PATCH",
        "/api/nodes/pc",
        r#"{"soloist_key":"spot-key","spotify":true}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: Some(SecretString("spot-key".into())), zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let mut s1 = MockAgent::connect_as_from(port, "s1", Role::Rx, "127.0.0.3").await;
    let mut s2 = MockAgent::connect_as_from(port, "s2", Role::Rx, "127.0.0.4").await;
    wait_node(port, "s1").await;
    wait_node(port, "s2").await;

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":["s1"]}"#).await;
    assert_eq!(status, 200, "{zone}");
    let z1 = zone["id"].as_str().unwrap().to_string();
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: Some(SecretString("spot-key".into())), zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, body) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{z1}"),
        r#"{"sources":[{"kind":"spotify","node_id":"pc"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let view: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(view["sources"], serde_json::json!([]));
    assert_eq!(view["source_maps"], serde_json::json!({}));
    let n1 = zvucast::mix::feed_port_name(zproto::FeedKind::Spotify, "pc", &z1);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist {
            api_key: Some(SecretString("spot-key".into())),
            zones: vec![zproto::VirtualZone { zone_id: z1.clone(), name: "Зал".into() }],
        }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetFeeds {
            feeds: vec![zproto::FeedSend {
                zone_id: z1.clone(),
                kind: zproto::FeedKind::Spotify,
                monitor: zvucast::soloist::monitor_name(&z1),
                endpoints: vec![],
            }],
        }
    );

    let (status, _) = http(port, "POST", &format!("/api/zones/{z1}/start")).await;
    assert_eq!(status, 200);
    let p1_alone = assigned_port(&["pc".to_string(), n1.clone()], &n1);
    assert_eq!(
        pc.recv_ctl_within(5).await,
        CtlMsg::SetFeeds {
            feeds: vec![zproto::FeedSend {
                zone_id: z1.clone(),
                kind: zproto::FeedKind::Spotify,
                monitor: zvucast::soloist::monitor_name(&z1),
                endpoints: vec![Endpoint { ip: "127.0.0.3".into(), port: p1_alone }],
            }],
        }
    );
    assert_eq!(pc.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(s1.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        s1.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(
        s1.recv_ctl_within(5).await,
        CtlMsg::SetMix { ports: vec![p1_alone], jb_ms: 2 }
    );

    let (status, zone) = create_zone(port, r#"{"name":"Спальня","tx":"pc","members":["s2"]}"#).await;
    assert_eq!(status, 200, "{zone}");
    let z2 = zone["id"].as_str().unwrap().to_string();
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist {
            api_key: Some(SecretString("spot-key".into())),
            zones: vec![zproto::VirtualZone { zone_id: z1.clone(), name: "Зал".into() }],
        }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetFeeds {
            feeds: vec![zproto::FeedSend {
                zone_id: z1.clone(),
                kind: zproto::FeedKind::Spotify,
                monitor: zvucast::soloist::monitor_name(&z1),
                endpoints: vec![Endpoint { ip: "127.0.0.3".into(), port: p1_alone }],
            }],
        }
    );

    let (status, body) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{z2}"),
        r#"{"sources":[{"kind":"spotify","node_id":"pc"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let view: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(view["source_maps"], serde_json::json!({}));
    let mut soloist_zones = vec![
        zproto::VirtualZone { zone_id: z1.clone(), name: "Зал".into() },
        zproto::VirtualZone { zone_id: z2.clone(), name: "Спальня".into() },
    ];
    soloist_zones.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist {
            api_key: Some(SecretString("spot-key".into())),
            zones: soloist_zones,
        }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    let mut inactive = vec![
        zproto::FeedSend {
            zone_id: z1.clone(),
            kind: zproto::FeedKind::Spotify,
            monitor: zvucast::soloist::monitor_name(&z1),
            endpoints: vec![Endpoint { ip: "127.0.0.3".into(), port: p1_alone }],
        },
        zproto::FeedSend {
            zone_id: z2.clone(),
            kind: zproto::FeedKind::Spotify,
            monitor: zvucast::soloist::monitor_name(&z2),
            endpoints: vec![],
        },
    ];
    inactive.sort_by(|a, b| a.zone_id.cmp(&b.zone_id).then(a.kind.as_str().cmp(b.kind.as_str())));
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: inactive });

    let (status, _) = http(port, "POST", &format!("/api/zones/{z2}/start")).await;
    assert_eq!(status, 200);
    let n2 = zvucast::mix::feed_port_name(zproto::FeedKind::Spotify, "pc", &z2);
    let names = vec!["pc".to_string(), n1.clone(), n2.clone()];
    let p1 = assigned_port(&names, &n1);
    let p2 = assigned_port(&names, &n2);
    let mic = assigned_port(&names, "pc");
    assert_ne!(p1, p2);
    assert_ne!(p1, mic);
    assert_ne!(p2, mic);
    let mut live = vec![
        zproto::FeedSend {
            zone_id: z1.clone(),
            kind: zproto::FeedKind::Spotify,
            monitor: zvucast::soloist::monitor_name(&z1),
            endpoints: vec![Endpoint { ip: "127.0.0.3".into(), port: p1 }],
        },
        zproto::FeedSend {
            zone_id: z2.clone(),
            kind: zproto::FeedKind::Spotify,
            monitor: zvucast::soloist::monitor_name(&z2),
            endpoints: vec![Endpoint { ip: "127.0.0.4".into(), port: p2 }],
        },
    ];
    live.sort_by(|a, b| a.zone_id.cmp(&b.zone_id).then(a.kind.as_str().cmp(b.kind.as_str())));
    assert_eq!(pc.recv_ctl_within(5).await, CtlMsg::SetFeeds { feeds: live });
    assert_eq!(s2.recv_ctl_within(5).await, CtlMsg::Start);
    assert_eq!(
        s2.recv_ctl_within(5).await,
        CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] }
    );
    assert_eq!(s2.recv_ctl_within(5).await, CtlMsg::SetMix { ports: vec![p2], jb_ms: 2 });
    if p1 != p1_alone {
        assert_eq!(s1.recv_ctl_within(5).await, CtlMsg::SetMix { ports: vec![p1], jb_ms: 2 });
    }

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"dscp":41}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(5).await,
        CtlMsg::SetEndpoints { zones: vec![], dscp: 41 }
    );
}

/// PATCH `source_maps` хранит ключ вида `spotify:{узел}` и ключ микрофона.
/// Ключ вида, которого нет среди источников зоны, не остаётся.
#[tokio::test]
async fn feed_source_map_is_stored_under_the_source_key() {
    let path = temp_state_path("feed-source-map");
    let port = serve(AppState::with_state(&path)).await;
    let pc = MockAgent::connect_as_from(port, "pc", Role::Tx, "127.0.0.2").await;
    let s1 = MockAgent::connect_as_from(port, "s1", Role::Rx, "127.0.0.3").await;
    wait_node(port, "pc").await;
    wait_node(port, "s1").await;

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"spotify":true}"#).await;
    assert_eq!(status, 200);
    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":["s1"]}"#).await;
    assert_eq!(status, 200, "{zone}");
    let id = zone["id"].as_str().unwrap();

    let (status, body) = http_body(
        port,
        "PATCH",
        &format!("/api/zones/{id}"),
        r#"{"sources":["pc",{"kind":"spotify","node_id":"pc"}],"source_maps":{"s1":{"pc":{"out_l":"r","out_r":null},"spotify:pc":{"out_l":"l","out_r":"l"},"sendspin:pc":{"out_l":"r","out_r":"r"}}}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let view: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(view["sources"], serde_json::json!(["pc"]));
    assert_eq!(
        view["source_maps"]["s1"],
        serde_json::json!({
            "pc": {"out_l": "r", "out_r": null},
            "spotify:pc": {"out_l": "l", "out_r": "l"}
        })
    );
    assert!(view["source_maps"]["s1"].get("sendspin:pc").is_none());

    drop((pc, s1));
}

#[tokio::test]
async fn soloist_key_stays_on_the_transmitter_that_has_it() {
    let path = temp_state_path("soloist-key");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(pc.recv_raw_within(2).await, CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"soloist_key":"  spot-key  ","spotify":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: Some(SecretString("spot-key".into())), zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let mut other = MockAgent::connect_virtual_named(port, "room").await;
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(other.recv_raw_within(2).await, CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (code, nodes) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200);
    let nodes: Value = serde_json::from_str(&nodes).unwrap();
    let pc_json = nodes.as_array().unwrap().iter().find(|n| n["name"] == "pc").unwrap();
    let room_json = nodes.as_array().unwrap().iter().find(|n| n["name"] == "room").unwrap();
    assert_eq!(pc_json["soloist_key"], "spot-key");
    assert!(room_json.get("soloist_key").is_none());

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":[]}"#).await;
    assert_eq!(status, 200, "{zone}");
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: Some(SecretString("spot-key".into())), zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"soloist_key":""}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: None, zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
}

#[tokio::test]
async fn spotify_checkbox_off_does_not_start_soloist_even_with_a_key() {
    let path = temp_state_path("spotify-off");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(pc.recv_raw_within(2).await, CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"soloist_key":"spot-key"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: None, zones: vec![] },
        "ключ без галочки Spotify Soloist не поднимает"
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"spotify":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: Some(SecretString("spot-key".into())), zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"spotify":false}"#).await;
    assert_eq!(status, 200);
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSoloist { api_key: None, zones: vec![] }
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (code, nodes) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200);
    let nodes: Value = serde_json::from_str(&nodes).unwrap();
    let pc_json = nodes.as_array().unwrap().iter().find(|n| n["name"] == "pc").unwrap();
    assert_eq!(pc_json["soloist_key"], "spot-key");
    assert_eq!(pc_json["spotify"], false);
}

/// Hello без ключей setup, soloist и soloist_command: в сводке узла setup — JSON null.
#[tokio::test]
async fn old_hello_has_null_setup() {
    let path = temp_state_path("old-hello-setup");
    let port = serve(AppState::with_state(&path)).await;
    let hello = AgentMsg::Hello {
        node_id: "pc".into(),
        name: "pc".into(),
        roles: vec![Role::Rx],
        version: "0.1.0".into(),
        target: String::new(),
        capture_device: None,
        playback_device: Some("test".into()),
        inputs: vec![],
        outputs: vec![],
        supports_virtual_output: false,
        link: None,
        setup: Some(vec![]),
        soloist: SoloistState::Unknown,
        soloist_command: String::new(),
    };
    let mut raw = serde_json::to_value(&hello).unwrap();
    let obj = raw.as_object_mut().unwrap();
    obj.remove("setup");
    obj.remove("soloist");
    obj.remove("soloist_command");
    let mut ws = open_ws(port, "127.0.0.1").await;
    ws.send(Message::Text(raw.to_string())).await.unwrap();
    let mut pc = MockAgent { ws };
    assert_eq!(
        pc.recv_raw_within(2).await,
        CtlMsg::SetPlaybackTarget { device_keys: vec!["test".into()] }
    );

    let (code, body) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200, "{body}");
    let nodes: Value = serde_json::from_str(&body).unwrap();
    let pc_json = node(&nodes, "pc");
    assert_eq!(pc_json.get("setup"), Some(&Value::Null), "{pc_json}");
}

#[tokio::test]
async fn spotify_appends_soloist_issue_on_the_node_json() {
    let path = temp_state_path("spotify-setup");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::hello_soloist(open_ws(port, "127.0.0.1").await, "pc", "pc", vec![Role::Rx]).await;
    assert_eq!(
        pc.recv_raw_within(2).await,
        CtlMsg::SetPlaybackTarget { device_keys: vec!["test".into()] }
    );

    let (status, patch_body) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"spotify":true}"#).await;
    assert_eq!(status, 200, "{patch_body}");

    let (code, body) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200, "{body}");
    let nodes: Value = serde_json::from_str(&body).unwrap();
    let setup = node(&nodes, "pc")["setup"].as_array().expect("setup — массив");
    let soloist = setup.iter().find(|i| i["id"] == "soloist").expect("нет строки soloist");
    assert_eq!(soloist["detail"], "бинарника нет");
    assert_eq!(soloist["command"], "curl soloist");
}

#[tokio::test]
async fn sendspin_checkbox_off_does_not_start_clients() {
    let path = temp_state_path("sendspin-off");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(pc.recv_raw_within(2).await, CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":[]}"#).await;
    assert_eq!(status, 200, "{zone}");
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(
        pc.recv_ctl_within(2).await,
        CtlMsg::SetSendspin { zones: vec![] },
        "без галочки Sendspin зоны не поднимают клиентов"
    );
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"sendspin":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"sendspin":false}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (code, nodes) = http(port, "GET", "/api/nodes").await;
    assert_eq!(code, 200);
    let nodes: Value = serde_json::from_str(&nodes).unwrap();
    let pc_json = nodes.as_array().unwrap().iter().find(|n| n["name"] == "pc").unwrap();
    assert_eq!(pc_json["sendspin"], false);
}

#[tokio::test]
async fn sendspin_follows_zones_when_enabled() {
    let path = temp_state_path("sendspin-zones");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(pc.recv_raw_within(2).await, CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/pc", r#"{"sendspin":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, zone) = create_zone(port, r#"{"name":"Зал","tx":"pc","members":[]}"#).await;
    assert_eq!(status, 200, "{zone}");
    let hall = zone["id"].as_str().unwrap().to_string();
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", &format!("/api/zones/{hall}"), r#"{"name":"Гостиная"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let mut other = MockAgent::connect_virtual_named(port, "room").await;
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(other.recv_raw_within(2).await, CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", "/api/nodes/room", r#"{"sendspin":true}"#).await;
    assert_eq!(status, 200);
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "PATCH", &format!("/api/zones/{hall}"), r#"{"tx":"room"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(pc.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });

    let (status, _) = http_body(port, "DELETE", &format!("/api/zones/{hall}"), "").await;
    assert_eq!(status, 200);
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(other.recv_ctl_within(2).await, CtlMsg::SetFeeds { feeds: vec![] });
}

#[tokio::test]
async fn a_node_without_pipewire_is_not_offered_sendspin() {
    let path = temp_state_path("sendspin-alsa");
    let port = serve(AppState::with_state(&path)).await;
    let mut pi = MockAgent::connect_named(port, "pi", "pi", Role::Tx).await;
    let (status, _) = create_zone(port, r#"{"name":"Зал","tx":"pi","members":[]}"#).await;
    assert_eq!(status, 200);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(left, pi.recv_ctl()).await {
            Ok(CtlMsg::SetSendspin { .. }) => panic!("узлу без PipeWire ушёл SetSendspin"),
            Ok(_) => {}
            Err(_) => break,
        }
    }
}

async fn recv_raw(agent: &mut MockAgent) -> CtlMsg {
    tokio::time::timeout(Duration::from_secs(2), agent.recv_text_ctl())
        .await
        .expect("не дождались CtlMsg")
}

/// Hello и смена prefs шлют цель захвата, заглушённые зоны и цель выхода.
#[tokio::test]
async fn ui_targets_follow_hello_and_prefs() {
    let path = temp_state_path("ui-targets");
    let port = serve(AppState::with_state(&path)).await;

    let mut spk = MockAgent::connect(port, "spk").await;
    assert_eq!(
        recv_raw(&mut spk).await,
        CtlMsg::SetPlaybackTarget { device_keys: vec!["test".into()] }
    );

    let mut ws = open_ws(port, "127.0.0.1").await;
    let outs = ["aaa", "bbb"].into_iter().flat_map(|key| {
        (0..2u16).map(move |index| zproto::CaptureChan {
            id: format!("{key}:{index}"),
            device_key: key.into(),
            device_name: key.into(),
            index,
        })
    });
    let hello = AgentMsg::Hello {
        node_id: "col".into(),
        name: "col".into(),
        roles: vec![Role::Rx],
        version: "0.1.0".into(),
        target: String::new(),
        capture_device: None,
        playback_device: None,
        inputs: vec![],
        outputs: outs.collect(),
        supports_virtual_output: false,
        link: None,
        setup: Some(vec![]),
        soloist: SoloistState::Unknown,
        soloist_command: String::new(),
    };
    ws.send(Message::Text(serde_json::to_string(&hello).unwrap())).await.unwrap();
    let mut col = MockAgent { ws };
    assert_eq!(
        recv_raw(&mut col).await,
        CtlMsg::SetPlaybackTarget { device_keys: vec!["aaa".into(), "bbb".into()] }
    );
    let (status, _) = http_body(
        port,
        "PATCH",
        "/api/nodes/col",
        r#"{"outputs":[{"id":"aaa:0","enabled":false},{"id":"aaa:1","enabled":false}]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        recv_raw(&mut col).await,
        CtlMsg::SetPlaybackTarget { device_keys: vec!["bbb".into()] }
    );

    let mut pc = MockAgent::connect_virtual(port).await;
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetVirtualOutput { enabled: false });
    assert_eq!(
        recv_raw(&mut pc).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetMutedZones { zones: vec![] });
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetVirtualZones { zones: vec![] });
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetSoloist { api_key: None, zones: vec![] });
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetSendspin { zones: vec![] });
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetFeeds { feeds: vec![] });

    let mut reported = vec![zproto::CaptureChan {
        id: "hw:0".into(),
        device_key: "plughw:CARD=Generic_1".into(),
        device_name: "HD-Audio Generic".into(),
        index: 0,
    }];
    for index in 0..2u16 {
        reported.push(zproto::CaptureChan {
            id: format!("zvucast-hall:{index}"),
            device_key: "zvucast-hall".into(),
            device_name: "Zvucast: Зал".into(),
            index,
        });
    }
    let inputs = AgentMsg::Inputs { inputs: reported };
    pc.ws.send(Message::Text(serde_json::to_string(&inputs).unwrap())).await.unwrap();
    assert_eq!(
        recv_raw(&mut pc).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    assert_eq!(recv_raw(&mut pc).await, CtlMsg::SetMutedZones { zones: vec![] });

    let (status, _) = http_body(
        port,
        "PATCH",
        "/api/nodes/pc",
        r#"{"inputs":[{"id":"zvucast-hall:0","enabled":false},{"id":"zvucast-hall:1","enabled":false}]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        recv_raw(&mut pc).await,
        CtlMsg::SetCaptureTarget { device_key: Some("plughw:CARD=Generic_1".into()) }
    );
    assert_eq!(
        recv_raw(&mut pc).await,
        CtlMsg::SetMutedZones { zones: vec!["hall".into()] }
    );
}

#[tokio::test]
async fn node_volume_patch_reaches_the_agent() {
    let path = temp_state_path("node-volume");
    let port = serve(AppState::with_state(&path)).await;
    let mut pc = MockAgent::connect_mixed(port, "pi3").await;
    for _ in 0..8 {
        if tokio::time::timeout(Duration::from_millis(200), pc.recv_ctl()).await.is_err() {
            break;
        }
    }

    let (status, body) = http_body(
        port,
        "PATCH",
        "/api/nodes/pi3",
        r#"{"capture_volume":0.8,"playback_volume":1.2}"#,
    ).await;
    assert_eq!(status, 200, "{body}");
    let mut got = None;
    for _ in 0..8 {
        let msg = pc.recv_ctl_within(2).await;
        if let CtlMsg::SetVolume { capture, playback, devices } = msg {
            got = Some((capture, playback, devices));
            break;
        }
    }
    assert_eq!(got, Some((Some(0.8), Some(1.2), vec![])));

    let (status, body) = http_body(
        port,
        "PATCH",
        "/api/nodes/pi3",
        r#"{"device_volumes":[{"device":"snd_rpi_hifiberry_dac","volume":0.5}]}"#,
    ).await;
    assert_eq!(status, 200, "{body}");
    let mut named = None;
    for _ in 0..8 {
        let msg = pc.recv_ctl_within(2).await;
        if let CtlMsg::SetVolume { devices, .. } = msg {
            if !devices.is_empty() {
                named = Some(devices);
                break;
            }
        }
    }
    assert_eq!(
        named,
        Some(vec![zproto::DeviceVolume {
            device: "snd_rpi_hifiberry_dac".into(),
            volume: 0.5,
            card: String::new(),
            class: String::new(),
        }])
    );

    let (status, _) = http_body(
        port,
        "PATCH",
        "/api/nodes/pi3",
        r#"{"device_volumes":[{"device":"snd_rpi_hifiberry_dac","volume":2}]}"#,
    ).await;
    assert_eq!(status, 400);

    let (status, body) = http_body(port, "PATCH", "/api/nodes/pi3", r#"{"capture_volume":1.6}"#).await;
    assert_eq!(status, 400, "{body}");

    pc.send_telemetry_volumes(0.4, 1.0).await;
    let nodes = wait_for(port, |nodes| {
        node(nodes, "pi3")["telemetry"]["capture_volume"].as_f64() == Some(0.4)
    }).await;
    assert_eq!(node(&nodes, "pi3")["telemetry"]["playback_volume"].as_f64(), Some(1.0));
}
