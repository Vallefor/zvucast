use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{mpsc, watch};
use zproto::{CaptureChan, ChannelMap, CtlMsg, Endpoint, EventKind, PipelineState, Role, Telemetry};

/// Узел реестра, как его видит контроллер (в API/дашборде).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NodeView {
    pub node_id: String,
    pub name: String,
    pub display_name: Option<String>,
    pub channel_map: Option<ChannelMap>,
    pub roles: Vec<Role>,
    pub capture_device: Option<String>,
    pub playback_device: Option<String>,
    pub ip: String,
    pub online: bool,
    /// Канал сессии. None — агент его не сообщил.
    pub link: Option<zproto::LinkKind>,
    /// Два агента с одним node_id одновременно online с разных ip.
    pub dup: bool,
    pub telemetry: Option<Telemetry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    UnknownNode,
    Offline,
    /// Канал команд узла закрыт — узел отвалился. Вызывающий должен
    /// `mark_offline(node_id)`.
    Closed,
    /// Канал команд переполнен — узел не читает команды.
    Full,
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::UnknownNode => write!(f, "узел неизвестен"),
            RegistryError::Offline => write!(f, "узел offline"),
            RegistryError::Closed => write!(f, "канал команд узла закрыт"),
            RegistryError::Full => write!(f, "канал команд узла переполнен"),
        }
    }
}

impl std::error::Error for RegistryError {}

struct NodeRec {
    name: String,
    roles: Vec<Role>,
    capture_device: Option<String>,
    playback_device: Option<String>,
    ip: String,
    link: Option<zproto::LinkKind>,
    #[allow(dead_code)]
    version: String,
    dup: bool,
    last_telem: Option<(Instant, Telemetry)>,
    last_event: Option<(EventKind, String)>,
    tx: Option<mpsc::Sender<CtlMsg>>,
    online: bool,
    inputs: Vec<CaptureChan>,
    outputs: Vec<CaptureChan>,
    supports_virtual_output: bool,
    /// Счётчик hello: инкремент на каждом hello (первое подключение = 1).
    /// Пушер сравнивает снапшоты и видит факт re-hello даже внутри окна
    /// дебаунса — переподключившийся tx получает SetEndpoints повторно.
    hello_seq: u64,
}

/// Мета узла (display_name, канальная матрица), заданная пользователем.
/// Хранится отдельно от NodeRec: переживает re-hello (агент её не шлёт и
/// затирать не должен) и ложится на узел, пока тот offline.
#[derive(Debug, Clone, Default, PartialEq)]
struct NodeMeta {
    display_name: Option<String>,
    channel_map: Option<ChannelMap>,
}

/// Реестр узлов контроллера: чистая логика, без сети. Online/offline по
/// таймауту телеметрии (sweep), команды — через try_send (не async).
///
/// `version_tx` — счётчик изменений состава/состояния узлов (hello,
/// mark_offline). Подписчик (pusher в controller) ждёт изменений через
/// `subscribe()` и пересчитывает то, что зависит от реестра.
pub struct Registry {
    /// Ключ — node_id агента (стабильный uuid), не имя: имена не уникальны,
    /// node_id — да (спека §1).
    nodes: HashMap<String, NodeRec>,
    /// Мета (display_name, channel_map) по node_id, независимо от NodeRec.
    meta: HashMap<String, NodeMeta>,
    version_tx: watch::Sender<u64>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        let (version_tx, _rx) = watch::channel(0u64);
        Self {
            nodes: HashMap::new(),
            meta: HashMap::new(),
            version_tx,
        }
    }

    /// Инкремент версии реестра — триггер пересчёта для подписчиков (pusher).
    /// Вызывается из hello/mark_offline; POST /api/dscp бампит вручную, чтобы
    /// tx-узлы получили свежий dscp с ближайшим пушем. `send_modify` в отличие
    /// от `send` не зависит от наличия подписчиков — не падает, когда pusher
    /// ещё не подписался или уже завершился.
    pub fn bump_version(&self) {
        self.version_tx.send_modify(|v| *v += 1);
    }

    /// Подписка на версию реестра (для pusher'а и т.п.).
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.version_tx.subscribe()
    }

    /// Регистрация узла (Hello по WS), ключ — `node_id`. Повторный hello от
    /// того же node_id заменяет канал команд, имя, набор ролей и устройства,
    /// переводит узел online; телеметрия сбрасывается; счётчик hello_seq
    /// инкрементится (факт re-hello для пушера — см. `hello_seqs`). Мета (display_name,
    /// channel_map) не трогается — она живёт отдельно (`set_meta`).
    /// `ip` — адрес peer'а WS-соединения (подставляет controller).
    /// `dup` выставляется, если узел с этим node_id уже online с другого ip
    /// (два агента с одним id одновременно); совпадение ip — обычный re-hello.
    #[allow(clippy::too_many_arguments)]
    pub fn hello(
        &mut self,
        node_id: &str,
        name: &str,
        roles: Vec<Role>,
        version: String,
        capture_device: Option<String>,
        playback_device: Option<String>,
        ip: String,
        tx: mpsc::Sender<CtlMsg>,
    ) {
        let dup = matches!(self.nodes.get(node_id), Some(rec) if rec.online && rec.ip != ip);
        match self.nodes.get_mut(node_id) {
            Some(rec) => {
                rec.name = name.to_string();
                rec.roles = roles;
                rec.version = version;
                rec.capture_device = capture_device;
                rec.playback_device = playback_device;
                rec.ip = ip;
                rec.dup = dup;
                rec.last_telem = None;
                rec.tx = Some(tx);
                rec.online = true;
                rec.hello_seq += 1;
            }
            None => {
                self.nodes.insert(
                    node_id.to_string(),
                    NodeRec {
                        name: name.to_string(),
                        roles,
                        capture_device,
                        playback_device,
                        ip,
                        link: None,
                        version,
                        dup,
                        last_telem: None,
                        last_event: None,
                        tx: Some(tx),
                        online: true,
                        inputs: Vec::new(),
                        outputs: Vec::new(),
                        supports_virtual_output: false,
                        hello_seq: 1,
                    },
                );
            }
        }
        self.bump_version();
    }

    /// Задать мету узла (display_name и/или канальную матрицу). Полное
    /// замещение: None в аргументе стирает поле. Узел может быть offline или
    /// вообще ещё не подключёнся — мета лежит в отдельной мапе и мёржится в
    /// NodeView при `list()`. Версию реестра не бампит: на состав эндпоинтов
    /// и пуши tx-узлам мета не влияет, дашборд получит её с ближайшим поллом.
    pub fn set_meta(
        &mut self,
        node_id: &str,
        display_name: Option<String>,
        channel_map: Option<ChannelMap>,
    ) {
        self.meta.insert(node_id.to_string(), NodeMeta { display_name, channel_map });
    }

    /// Каналы захвата с последнего hello. Пусто, пока агент их не прислал.
    pub fn set_capture_inputs(&mut self, node_id: &str, inputs: Vec<CaptureChan>) {
        if let Some(rec) = self.nodes.get_mut(node_id) {
            rec.inputs = inputs;
        }
    }

    pub fn capture_inputs(&self, node_id: &str) -> Vec<CaptureChan> {
        self.nodes.get(node_id).map(|r| r.inputs.clone()).unwrap_or_default()
    }

    pub fn capture_device_of(&self, node_id: &str) -> Option<String> {
        self.nodes.get(node_id).and_then(|r| r.capture_device.clone())
    }

    pub fn set_playback_outputs(&mut self, node_id: &str, outputs: Vec<CaptureChan>) {
        if let Some(rec) = self.nodes.get_mut(node_id) {
            rec.outputs = outputs;
        }
    }

    pub fn playback_outputs(&self, node_id: &str) -> Vec<CaptureChan> {
        self.nodes.get(node_id).map(|r| r.outputs.clone()).unwrap_or_default()
    }

    pub fn playback_device_of(&self, node_id: &str) -> Option<String> {
        self.nodes.get(node_id).and_then(|r| r.playback_device.clone())
    }

    /// Канал, с которого узел держит сессию. None — агент поле не прислал.
    pub fn set_link(&mut self, node_id: &str, link: Option<zproto::LinkKind>) {
        if let Some(rec) = self.nodes.get_mut(node_id) {
            rec.link = link;
        }
    }

    pub fn set_supports_virtual_output(&mut self, node_id: &str, supported: bool) {
        if let Some(rec) = self.nodes.get_mut(node_id) {
            rec.supports_virtual_output = supported;
        }
    }

    pub fn supports_virtual_output(&self, node_id: &str) -> bool {
        self.nodes.get(node_id).is_some_and(|r| r.supports_virtual_output)
    }

    /// Свежая телеметрия от узла (по node_id). Неизвестный узел — warn, не паника.
    pub fn telemetry(&mut self, node_id: &str, t: Telemetry) {
        match self.nodes.get_mut(node_id) {
            Some(rec) => rec.last_telem = Some((Instant::now(), t)),
            None => tracing::warn!(node = node_id, "телеметрия от неизвестного узла"),
        }
    }

    /// Последняя tx-телеметрия: возраст от `now`, играет ли пайплайн, rms_l, rms_r.
    /// Нет узла, кадра или tx-половины → None.
    pub fn tx_sample(&self, node_id: &str, now: Instant) -> Option<(Duration, bool, f64, f64)> {
        let (at, telem) = self.nodes.get(node_id)?.last_telem.as_ref()?;
        let tx = telem.tx.as_ref()?;
        Some((
            now.saturating_duration_since(*at),
            tx.state == PipelineState::Playing,
            tx.rms_l,
            tx.rms_r,
        ))
    }

    /// Событие от узла (AgentMsg::Event, по node_id). Неизвестный узел — warn,
    /// не паника.
    pub fn event(&mut self, node_id: &str, kind: EventKind, detail: String) {
        match self.nodes.get_mut(node_id) {
            Some(rec) => rec.last_event = Some((kind, detail)),
            None => tracing::warn!(node = node_id, "событие от неизвестного узла"),
        }
    }

    /// Последнее событие узла (None — узел неизвестен или событий не было).
    pub fn last_event(&self, node_id: &str) -> Option<&(EventKind, String)> {
        self.nodes.get(node_id)?.last_event.as_ref()
    }

    /// Принудительный offline (например, при разрыве WS, по node_id). Бампит
    /// версию — состав online-узлов мог измениться.
    pub fn mark_offline(&mut self, node_id: &str) {
        match self.nodes.get_mut(node_id) {
            Some(rec) => {
                rec.online = false;
                self.bump_version();
            }
            None => tracing::warn!(node = node_id, "mark_offline неизвестного узла"),
        }
    }

    /// Этот ли канал сейчас зарегистрирован за узлом (по node_id). WS-хендлер
    /// проверяет это перед mark_offline: после re-hello канал заменён, и
    /// завершившийся старый хендлер не должен гасить уже живое соединение.
    pub fn is_current_tx(&self, node_id: &str, tx: &mpsc::Sender<CtlMsg>) -> bool {
        self.nodes
            .get(node_id)
            .and_then(|rec| rec.tx.as_ref())
            .is_some_and(|cur| cur.same_channel(tx))
    }

    /// Online-узлы с телеметрии старше timeout переводятся в offline.
    pub fn sweep(&mut self, now: Instant, timeout: Duration) {
        for rec in self.nodes.values_mut() {
            if let Some((t, _)) = rec.last_telem {
                if rec.online && now.saturating_duration_since(t) > timeout {
                    rec.online = false;
                }
            }
        }
    }

    /// Снимок реестра, отсортированный по имени (при равных именах — по
    /// node_id, порядок детерминирован). Мета (display_name, channel_map)
    /// мёржится из отдельной мапы — узел может быть offline.
    pub fn list(&self) -> Vec<NodeView> {
        let mut out: Vec<NodeView> = self
            .nodes
            .iter()
            .map(|(node_id, rec)| {
                let meta = self.meta.get(node_id);
                NodeView {
                    node_id: node_id.clone(),
                    name: rec.name.clone(),
                    display_name: meta.and_then(|m| m.display_name.clone()),
                    channel_map: meta.and_then(|m| m.channel_map),
                    roles: rec.roles.clone(),
                    capture_device: rec.capture_device.clone(),
                    playback_device: rec.playback_device.clone(),
                    ip: rec.ip.clone(),
                    online: rec.online,
                    link: rec.link,
                    dup: rec.dup,
                    telemetry: rec.last_telem.as_ref().map(|(_, t)| t.clone()),
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.node_id.cmp(&b.node_id)));
        out
    }

    /// Эндпоинты (ip:5004) online-приёмников — узлов, у которых roles
    /// содержит Rx, — то, что контроллер рассылает передатчикам через
    /// CtlMsg::SetEndpoints. Два rx на одном IP
    /// (например, rx на хосте контроллера, 127.0.0.1) дают один Endpoint —
    /// иначе tx собрал бы двойные ветки queue→udpsink. Сортировка по
    /// (ip, port) даёт детерминированный порядок для сравнения наборов:
    /// перестановка равных ключей не меняет результат, спурных пересборок нет.
    pub fn rx_endpoints(&self) -> Vec<Endpoint> {
        let mut out: Vec<Endpoint> = self
            .nodes
            .values()
            .filter(|rec| rec.online && rec.roles.contains(&Role::Rx))
            .map(|rec| Endpoint {
                ip: rec.ip.clone(),
                port: 5004,
            })
            .collect();
        out.sort_by(|a, b| a.ip.cmp(&b.ip).then_with(|| a.port.cmp(&b.port)));
        out.dedup_by(|a, b| a.ip == b.ip && a.port == b.port);
        out
    }

    /// Отправить команду узлу (по node_id). Не async: try_send в канал агента.
    pub fn send_cmd(&self, node_id: &str, cmd: CtlMsg) -> Result<(), RegistryError> {
        let rec = self.nodes.get(node_id).ok_or(RegistryError::UnknownNode)?;
        if !rec.online {
            return Err(RegistryError::Offline);
        }
        let tx = rec.tx.as_ref().ok_or(RegistryError::Closed)?;
        tx.try_send(cmd).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => RegistryError::Full,
            mpsc::error::TrySendError::Closed(_) => RegistryError::Closed,
        })
    }

    /// Рассылка всем online-узлам, чьи roles содержат `role` (try_send, не
    /// async). Offline/Closed/Full игнорируются — узел просто пропускается
    /// (пушер не «падает» из-за одного мёртвого канала; offline-переход на
    /// Closed делает ws-хендлер). Возвращает имена узлов, куда команда дошла
    /// (отсортированы).
    pub fn send_to_role(&self, role: Role, msg: CtlMsg) -> Vec<String> {
        let mut sent: Vec<String> = self
            .nodes
            .values()
            .filter(|rec| rec.online && rec.roles.contains(&role))
            .filter_map(|rec| rec.tx.as_ref().map(|tx| (rec, tx)))
            .filter_map(|(rec, tx)| tx.try_send(msg.clone()).ok().map(|_| rec.name.clone()))
            .collect();
        sent.sort();
        sent
    }

    /// node_id узлов, отсортированные (ключи реестра; `broadcast` шлёт по ним).
    pub fn names(&self) -> Vec<String> {
        let mut out: Vec<String> = self.nodes.keys().cloned().collect();
        out.sort();
        out
    }

    /// Резолв идентификатора из пути: сначала как node_id, затем как имя
    /// (алиас для обратной совместимости — старые пути /api/nodes/{name}/...).
    pub fn resolve(&self, id_or_name: &str) -> Option<String> {
        if self.nodes.contains_key(id_or_name) {
            return Some(id_or_name.to_string());
        }
        self.nodes
            .iter()
            .find(|(_, rec)| rec.name == id_or_name)
            .map(|(id, _)| id.clone())
    }

    /// Роли узла из последнего hello (None — узел неизвестен реестру).
    /// Валидация зон смотрит роли, а не online: offline-узел остаётся
    /// валидным членом зоны — роли помнятся с последнего подключения.
    pub fn roles_of(&self, node_id: &str) -> Option<Vec<Role>> {
        self.nodes.get(node_id).map(|rec| rec.roles.clone())
    }

    /// Online-узлы с ролью Tx (node_id, отсортированы) — получатели пуша
    /// SetEndpoints из zone-pusher'а.
    pub fn online_tx(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, rec)| rec.online && rec.roles.contains(&Role::Tx))
            .map(|(id, _)| id.clone())
            .collect();
        out.sort();
        out
    }

    /// Имя, с которым агент представился. Порт микса считается от него.
    pub fn name_of(&self, node_id: &str) -> Option<String> {
        self.nodes.get(node_id).map(|r| r.name.clone())
    }

    pub fn online_rx(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, rec)| rec.online && rec.roles.contains(&Role::Rx))
            .map(|(id, _)| id.clone())
            .collect();
        out.sort();
        out
    }

    /// Эндпоинт приёма (ip:5004) узла — если он online и имеет роль Rx.
    /// Узлы без роли Rx и offline-узлы в набор эндпоинтов не попадают.
    pub fn rx_endpoint(&self, node_id: &str) -> Option<Endpoint> {
        let rec = self.nodes.get(node_id)?;
        (rec.online && rec.roles.contains(&Role::Rx))
            .then(|| Endpoint { ip: rec.ip.clone(), port: 5004 })
    }

    /// Снапшот счётчиков hello (node_id → сколько hello узел сделал с момента
    /// старта реестра). Пушер сравнивает с прошлым снапшотом: изменение
    /// счётчика = узел переподключился и получает SetEndpoints принудительно.
    pub fn hello_seqs(&self) -> HashMap<String, u64> {
        self.nodes
            .iter()
            .map(|(id, rec)| (id.clone(), rec.hello_seq))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zproto::{ChanSel, PipelineState, RoleRx};

    fn telem() -> Telemetry {
        Telemetry {
            tx: None,
            rx: Some(RoleRx { state: PipelineState::Playing, dropped: 0, rendered: 42, queue_ms: 5 }),
            ptp_offset_ns: 100,
            uptime_s: 7,
        }
    }

    fn now_plus(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    fn hello(r: &mut Registry, name: &str) {
        let (tx, _rx) = mpsc::channel(1);
        r.hello(&format!("id-{name}"), name, vec![Role::Rx], "0.1".into(), None, None, "192.0.2.1".into(), tx);
    }

    #[test]
    fn list_carries_the_session_link() {
        let mut r = Registry::new();
        hello(&mut r, "opi");
        assert_eq!(r.list()[0].link, None);
        r.set_link("id-opi", Some(zproto::LinkKind::Wifi));
        assert_eq!(r.list()[0].link, Some(zproto::LinkKind::Wifi));
        r.set_link("id-opi", Some(zproto::LinkKind::Wired));
        assert_eq!(r.list()[0].link, Some(zproto::LinkKind::Wired));
    }

    #[test]
    fn lifecycle() {
        let (tx, _rx) = mpsc::channel(1);
        let mut r = Registry::new();
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx);
        assert_eq!(r.list()[0].name, "opi");
        assert!(r.list()[0].online);
        r.telemetry("id-opi", telem());
        assert!(r.list()[0].telemetry.is_some());
        r.sweep(now_plus(6), Duration::from_secs(5));
        assert!(!r.list()[0].online, "должен стать offline");
        assert!(matches!(r.send_cmd("id-opi", CtlMsg::Start), Err(RegistryError::Offline)));
        assert!(matches!(r.send_cmd("ghost", CtlMsg::Stop), Err(RegistryError::UnknownNode)));
    }

    #[test]
    fn rehello_replaces_channel() {
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, mut rx2) = mpsc::channel(1);
        let mut r = Registry::new();
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx1);
        r.hello("id-opi", "opi", vec![Role::Rx], "0.2".into(), None, None, "192.0.2.36".into(), tx2);
        assert!(r.list()[0].online);
        r.send_cmd("id-opi", CtlMsg::Start).unwrap();
        assert!(matches!(rx2.try_recv(), Ok(CtlMsg::Start)));
    }

    #[test]
    fn offline_then_hello_online() {
        let mut r = Registry::new();
        hello(&mut r, "opi");
        r.telemetry("id-opi", telem());
        r.mark_offline("id-opi");
        assert!(!r.list()[0].online);
        hello(&mut r, "opi");
        let v = &r.list()[0];
        assert!(v.online);
        assert!(v.telemetry.is_none(), "телеметрия сбрасывается при hello");
    }

    #[test]
    fn sweep_keeps_fresh_online() {
        let (tx, _rx) = mpsc::channel(1);
        let mut r = Registry::new();
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx);
        r.telemetry("id-opi", telem());
        r.sweep(now_plus(3), Duration::from_secs(5));
        assert!(r.list()[0].online, "свежая телеметрия — online");
        r.send_cmd("id-opi", CtlMsg::Stop).unwrap();
    }

    #[test]
    fn send_to_closed_channel_is_closed_error() {
        let (tx, rx) = mpsc::channel(1);
        let mut r = Registry::new();
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx);
        drop(rx);
        assert!(matches!(r.send_cmd("id-opi", CtlMsg::Stop), Err(RegistryError::Closed)));
    }

    #[test]
    fn is_current_tx_detects_replacement() {
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let mut r = Registry::new();
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx1.clone());
        assert!(r.is_current_tx("id-opi", &tx1));
        assert!(!r.is_current_tx("id-opi", &tx2));
        r.hello("id-opi", "opi", vec![Role::Rx], "0.2".into(), None, None, "192.0.2.36".into(), tx2.clone());
        assert!(!r.is_current_tx("id-opi", &tx1), "после re-hello старый канал не текущий");
        assert!(r.is_current_tx("id-opi", &tx2));
        assert!(!r.is_current_tx("ghost", &tx1));
    }

    #[test]
    fn event_unknown_node_warns_but_no_panic() {
        let mut r = Registry::new();
        r.event("ghost", EventKind::Underrun, "x".into());
        hello(&mut r, "opi");
        r.event("id-opi", EventKind::Underrun, "u1".into());
        assert_eq!(r.last_event("id-opi").unwrap().0, EventKind::Underrun);
    }

    #[test]
    fn list_and_names_sorted() {
        let mut r = Registry::new();
        hello(&mut r, "c-node");
        hello(&mut r, "a-node");
        hello(&mut r, "b-node");
        // list() сортирует по имени, names() отдаёт ключи (node_id).
        let names: Vec<String> = r.list().into_iter().map(|v| v.name).collect();
        assert_eq!(names, vec!["a-node", "b-node", "c-node"]);
        assert_eq!(r.names(), vec!["id-a-node", "id-b-node", "id-c-node"]);
    }

    #[test]
    fn node_view_carries_ip() {
        let mut r = Registry::new();
        hello(&mut r, "opi");
        assert_eq!(r.list()[0].ip, "192.0.2.1");
    }

    #[test]
    fn endpoints_only_online_rx() {
        let mut r = Registry::new();
        let (tx1, _r1) = mpsc::channel(1);
        let (tx2, _r2) = mpsc::channel(1);
        let (tx3, _r3) = mpsc::channel(1);
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx1);
        r.hello("id-pi4", "pi4", vec![Role::Tx], "0.1".into(), None, None, "192.0.2.204".into(), tx2);
        r.hello("id-rx2", "rx2", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.99".into(), tx3);
        r.mark_offline("id-rx2");
        let eps = r.rx_endpoints();
        assert_eq!(eps, vec![Endpoint { ip: "192.0.2.36".into(), port: 5004 }]);
    }

    #[test]
    fn endpoints_recomputed_on_online_change() {
        let mut r = Registry::new();
        let (tx1, _r1) = mpsc::channel(1);
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx1);
        assert_eq!(
            r.rx_endpoints(),
            vec![Endpoint { ip: "192.0.2.36".into(), port: 5004 }]
        );
        r.mark_offline("id-opi");
        assert!(r.rx_endpoints().is_empty());
        let (tx2, _r2) = mpsc::channel(1);
        r.hello("id-opi", "opi", vec![Role::Rx], "0.1".into(), None, None, "192.0.2.36".into(), tx2);
        assert_eq!(
            r.rx_endpoints(),
            vec![Endpoint { ip: "192.0.2.36".into(), port: 5004 }]
        );
    }

    /// Ревью: два rx на одном IP (план разрешает rx на хосте контроллера,
    /// 127.0.0.1) дают один Endpoint — иначе tx собрал бы двойные ветки
    /// queue→udpsink. Порядок детерминирован (ip, затем port) и не зависит
    /// от порядка регистрации (HashMap).
    #[test]
    fn endpoints_dedup_same_ip_stable_order() {
        let mk = |registrations: &[(&str, &str)]| {
            let mut r = Registry::new();
            for (name, ip) in registrations {
                let (tx, _rx) = mpsc::channel(1);
                r.hello(&format!("id-{name}"), name, vec![Role::Rx], "0.1".into(), None, None, (*ip).into(), tx);
            }
            r.rx_endpoints()
        };
        let want = vec![
            Endpoint { ip: "10.0.0.2".into(), port: 5004 },
            Endpoint { ip: "127.0.0.1".into(), port: 5004 },
        ];
        let a = mk(&[("rx-b", "127.0.0.1"), ("rx-a", "127.0.0.1"), ("rx-c", "10.0.0.2")]);
        let b = mk(&[("rx-c", "10.0.0.2"), ("rx-a", "127.0.0.1"), ("rx-b", "127.0.0.1")]);
        assert_eq!(a, want, "дубли по (ip, port) свёрнуты, порядок — по ip");
        assert_eq!(b, want, "порядок регистрации не влияет на результат");
    }

    #[test]
    fn send_to_role_only_online_matching_role() {
        let mut r = Registry::new();
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, _rx2) = mpsc::channel(8);
        let (tx3, mut rx3) = mpsc::channel(8);
        r.hello("id-t1", "t1", vec![Role::Tx], "0.1".into(), None, None, "10.0.0.1".into(), tx1);
        r.hello("id-t2", "t2", vec![Role::Tx], "0.1".into(), None, None, "10.0.0.2".into(), tx2);
        r.hello("id-r1", "r1", vec![Role::Rx], "0.1".into(), None, None, "10.0.0.3".into(), tx3);
        r.mark_offline("id-t2");
        let sent = r.send_to_role(Role::Tx, CtlMsg::Start);
        assert_eq!(sent, vec!["t1".to_string()]);
        assert!(matches!(rx1.try_recv(), Ok(CtlMsg::Start)));
        assert!(matches!(rx3.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[test]
    fn send_to_role_ignores_closed_channel() {
        let mut r = Registry::new();
        let (tx1, rx1) = mpsc::channel(1);
        r.hello("id-t1", "t1", vec![Role::Tx], "0.1".into(), None, None, "10.0.0.1".into(), tx1);
        drop(rx1);
        // Закрытый канал — узел пропускается, паники/ошибки нет, имена не вернулись.
        assert!(r.send_to_role(Role::Tx, CtlMsg::Start).is_empty());
    }

    /// Узел-комбайн [tx, rx]: и эндпоинт для передатчиков, и получатель пуша.
    #[test]
    fn mixed_node_is_endpoint_and_push_target() {
        let mut r = Registry::new();
        let (tx, mut rx) = mpsc::channel(8);
        r.hello("id-pc", "pc", vec![Role::Tx, Role::Rx], "0.1".into(), None, None, "10.0.0.7".into(), tx);
        assert_eq!(
            r.rx_endpoints(),
            vec![Endpoint { ip: "10.0.0.7".into(), port: 5004 }],
            "rx-половина комбайна — эндпоинт"
        );
        let sent = r.send_to_role(Role::Tx, CtlMsg::Start);
        assert_eq!(sent, vec!["pc".to_string()], "tx-половина комбайна — получатель пуша");
        assert!(matches!(rx.try_recv(), Ok(CtlMsg::Start)));
    }

    /// Re-hello с другим набором ролей обновляет запись (состав ролей узла
    /// мог измениться при смене конфига).
    #[test]
    fn rehello_updates_roles() {
        let mut r = Registry::new();
        let (tx1, _r1) = mpsc::channel(1);
        r.hello("id-pc", "pc", vec![Role::Rx], "0.1".into(), None, None, "10.0.0.7".into(), tx1);
        assert_eq!(r.rx_endpoints().len(), 1);
        assert!(r.send_to_role(Role::Tx, CtlMsg::Start).is_empty(), "пока нет tx — пуш некому");
        let (tx2, mut rx2) = mpsc::channel(8);
        r.hello("id-pc", "pc", vec![Role::Tx, Role::Rx], "0.2".into(), Some("cap".into()), Some("play".into()), "10.0.0.7".into(), tx2);
        assert_eq!(r.list()[0].roles, vec![Role::Tx, Role::Rx]);
        assert_eq!(r.list()[0].capture_device.as_deref(), Some("cap"));
        assert_eq!(r.send_to_role(Role::Tx, CtlMsg::Start), vec!["pc".to_string()]);
        assert!(matches!(rx2.try_recv(), Ok(CtlMsg::Start)));
    }

    #[tokio::test]
    async fn version_bumps_on_hello_and_mark_offline_only() {
        let mut r = Registry::new();
        let mut sub = r.subscribe();
        assert_eq!(*sub.borrow_and_update(), 0);

        hello(&mut r, "opi");
        assert!(sub.has_changed().unwrap(), "hello бампит версию");
        sub.borrow_and_update();

        r.telemetry("id-opi", telem());
        assert!(
            !sub.has_changed().unwrap(),
            "телеметрия состав узлов не меняет — версию не бампит"
        );

        r.mark_offline("id-opi");
        assert!(sub.has_changed().unwrap(), "mark_offline бампит версию");
        sub.borrow_and_update();

        r.mark_offline("ghost");
        assert!(
            !sub.has_changed().unwrap(),
            "mark_offline неизвестного узла ничего не меняет"
        );
    }

    #[test]
    fn rehello_same_node_id_keeps_single_entry() {
        let mut r = Registry::new();
        let (tx1, _rx1) = mpsc::channel(1);
        r.hello("id-1", "old-name", vec![Role::Rx], "v1".into(), None, None, "1.1.1.1".into(), tx1);
        let (tx2, _rx2) = mpsc::channel(1);
        r.hello("id-1", "new-name", vec![Role::Rx], "v1".into(), None, None, "2.2.2.2".into(), tx2);
        assert_eq!(r.list().len(), 1);
        assert_eq!(r.list()[0].name, "new-name");
        assert_eq!(r.list()[0].ip, "2.2.2.2");
    }

    #[test]
    fn meta_is_exposed_in_view() {
        let mut r = Registry::new();
        let (tx, _rx) = mpsc::channel(1);
        r.hello("id-1", "opi", vec![Role::Rx], "v1".into(), None, None, "1.1.1.1".into(), tx);
        r.set_meta("id-1", Some("Кухня".into()),
                   Some(ChannelMap { out_l: Some(ChanSel::L), out_r: None }));
        let v = &r.list()[0];
        assert_eq!(v.display_name.as_deref(), Some("Кухня"));
        assert_eq!(v.channel_map.unwrap().out_r, None);
    }

    #[test]
    fn duplicate_node_id_is_flagged() {
        let mut r = Registry::new();
        let (tx1, _rx1) = mpsc::channel(1);
        r.hello("id-1", "a", vec![Role::Rx], "v".into(), None, None, "1.1.1.1".into(), tx1);
        let (tx2, _rx2) = mpsc::channel(1);
        r.hello("id-1", "b", vec![Role::Rx], "v".into(), None, None, "2.2.2.2".into(), tx2);
        assert!(r.list()[0].dup, "второй online-узел с тем же id и другим ip — дубликат");
    }

    /// Мета лежит в отдельной мапе: set_meta для ещё не подключившегося
    /// (offline) узла не теряется и мёржится в NodeView при hello.
    #[test]
    fn meta_for_offline_node_merged_on_hello() {
        let mut r = Registry::new();
        r.set_meta("id-1", Some("Кухня".into()), None);
        let (tx, _rx) = mpsc::channel(1);
        r.hello("id-1", "opi", vec![Role::Rx], "v1".into(), None, None, "1.1.1.1".into(), tx);
        let v = &r.list()[0];
        assert_eq!(v.display_name.as_deref(), Some("Кухня"));
        assert!(v.channel_map.is_none());
    }

    /// Re-hello агента не затирает мету: агент её не шлёт, она — данные
    /// контроллера (пользовательская настройка зоны).
    #[test]
    fn rehello_preserves_meta() {
        let mut r = Registry::new();
        let (tx1, _rx1) = mpsc::channel(1);
        r.hello("id-1", "opi", vec![Role::Rx], "v1".into(), None, None, "1.1.1.1".into(), tx1);
        r.set_meta("id-1", Some("Кухня".into()), None);
        let (tx2, _rx2) = mpsc::channel(1);
        r.hello("id-1", "opi", vec![Role::Rx], "v1".into(), None, None, "1.1.1.1".into(), tx2);
        assert_eq!(r.list()[0].display_name.as_deref(), Some("Кухня"));
    }

    /// Re-hello с тем ip, что сейчас в записи, — не дубликат: dup сбрасывается.
    #[test]
    fn same_ip_rehello_clears_dup() {
        let mut r = Registry::new();
        let (tx1, _rx1) = mpsc::channel(1);
        r.hello("id-1", "a", vec![Role::Rx], "v".into(), None, None, "1.1.1.1".into(), tx1);
        let (tx2, _rx2) = mpsc::channel(1);
        r.hello("id-1", "b", vec![Role::Rx], "v".into(), None, None, "2.2.2.2".into(), tx2);
        assert!(r.list()[0].dup);
        let (tx3, _rx3) = mpsc::channel(1);
        r.hello("id-1", "b", vec![Role::Rx], "v".into(), None, None, "2.2.2.2".into(), tx3);
        assert!(!r.list()[0].dup, "re-hello с совпадающим ip — обычное переподключение");
    }

    /// resolve: сначала node_id, потом имя; неизвестное — None.
    #[test]
    fn resolve_by_node_id_then_name() {
        let mut r = Registry::new();
        let (tx, _rx) = mpsc::channel(1);
        r.hello("id-x", "pretty", vec![Role::Rx], "v".into(), None, None, "1.1.1.1".into(), tx);
        assert_eq!(r.resolve("id-x").as_deref(), Some("id-x"), "node_id резолвится сразу");
        assert_eq!(r.resolve("pretty").as_deref(), Some("id-x"), "имя — алиас к node_id");
        assert!(r.resolve("ghost").is_none());
    }

    /// roles_of: роли с последнего hello, включая offline-узел; неизвестный — None.
    #[test]
    fn roles_of_known_including_offline() {
        let mut r = Registry::new();
        let (tx, _rx) = mpsc::channel(1);
        r.hello("id-t", "t", vec![Role::Tx], "v".into(), None, None, "1.1.1.1".into(), tx);
        assert_eq!(r.roles_of("id-t"), Some(vec![Role::Tx]));
        r.mark_offline("id-t");
        assert_eq!(r.roles_of("id-t"), Some(vec![Role::Tx]), "offline не стирает роли");
        assert_eq!(r.roles_of("ghost"), None);
    }

    /// online_tx — только online с ролью Tx; rx_endpoint — только online Rx.
    #[test]
    fn online_tx_and_rx_endpoint() {
        let mut r = Registry::new();
        let mk = |r: &mut Registry, id: &str, roles: Vec<Role>, ip: &str| {
            let (tx, _rx) = mpsc::channel(1);
            r.hello(id, id, roles, "v".into(), None, None, ip.into(), tx);
        };
        mk(&mut r, "id-tx-on", vec![Role::Tx], "10.0.0.1");
        mk(&mut r, "id-tx-off", vec![Role::Tx], "10.0.0.2");
        mk(&mut r, "id-rx", vec![Role::Rx], "10.0.0.3");
        mk(&mut r, "id-mix", vec![Role::Tx, Role::Rx], "10.0.0.4");
        r.mark_offline("id-tx-off");
        assert_eq!(
            r.online_tx(),
            vec!["id-mix".to_string(), "id-tx-on".to_string()],
            "online Tx: комбайн — да, чистый Rx — нет, offline — нет"
        );
        assert_eq!(
            r.rx_endpoint("id-mix"),
            Some(Endpoint { ip: "10.0.0.4".into(), port: 5004 })
        );
        assert_eq!(
            r.rx_endpoint("id-rx"),
            Some(Endpoint { ip: "10.0.0.3".into(), port: 5004 })
        );
        assert!(r.rx_endpoint("id-tx-on").is_none(), "чистый Tx не эндпоинт");
        assert!(r.rx_endpoint("id-tx-off").is_none(), "offline не эндпоинт");
        assert!(r.rx_endpoint("ghost").is_none());
    }

    /// hello_seq: первое hello = 1, re-hello инкрементит — пушер по снапшотам
    /// видит факт переподключения даже внутри окна дебаунса.
    #[test]
    fn hello_seq_increments_per_hello() {
        let mut r = Registry::new();
        let (t1, _r1) = mpsc::channel(1);
        r.hello("id-1", "a", vec![Role::Tx], "v".into(), None, None, "1.1.1.1".into(), t1);
        assert_eq!(r.hello_seqs()["id-1"], 1);
        let (t2, _r2) = mpsc::channel(1);
        r.hello("id-1", "a", vec![Role::Tx], "v".into(), None, None, "1.1.1.1".into(), t2);
        assert_eq!(r.hello_seqs()["id-1"], 2, "re-hello инкрементит счётчик");
        assert_eq!(r.hello_seqs().len(), 1);
    }
}
