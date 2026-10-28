use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use zproto::{ChannelMap, FeedKind};

use crate::inputs::InputPref;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct NodeMeta {
    #[serde(default)] pub display_name: Option<String>,
    #[serde(default)] pub channel_map: Option<ChannelMap>,
    /// Свой DSCP передатчика. None — общий, из /api/dscp.
    #[serde(default)] pub dscp: Option<u8>,
    /// Имена и галочки каналов захвата. Нет записи — канал включён с именем по умолчанию.
    #[serde(default)] pub inputs: Vec<InputPref>,
    /// То же для каналов воспроизведения.
    #[serde(default)] pub outputs: Vec<InputPref>,
    /// Виртуальный стереовыход PipeWire. false — устройства нет.
    #[serde(default)] pub virtual_output: bool,
    /// Ключ Spotify этого передатчика. Пусто — Soloist на узле не поднимается.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soloist_key: Option<String>,
    /// Галочка «Включить Spotify». false — Soloist не стартует, даже если ключ задан.
    #[serde(default)]
    pub spotify: bool,
    /// Галочка «Включить Sendspin». false — клиенты зон этого передатчика не стартуют.
    #[serde(default)]
    pub sendspin: bool,
}

/// Микрофон узла или вид на узле-хозяине.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneSource {
    Line(String),
    Feed { kind: FeedKind, node_id: String },
}

#[derive(Serialize)]
struct ZoneSourceJson<'a> {
    kind: &'a str,
    node_id: &'a str,
}

#[derive(Deserialize)]
struct ZoneSourceObj {
    kind: String,
    node_id: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ZoneSourceRaw {
    Id(String),
    Obj(ZoneSourceObj),
}

impl ZoneSource {
    pub fn node_id(&self) -> &str {
        match self {
            ZoneSource::Line(id) => id,
            ZoneSource::Feed { node_id, .. } => node_id,
        }
    }

    /// Микрофон — сам `node_id`. Вид — `spotify:pc` и так далее.
    pub fn key(&self) -> String {
        match self {
            ZoneSource::Line(id) => id.clone(),
            ZoneSource::Feed { kind, node_id } => format!("{}:{node_id}", kind.as_str()),
        }
    }
}

impl From<String> for ZoneSource {
    fn from(id: String) -> Self {
        ZoneSource::Line(id)
    }
}

impl Serialize for ZoneSource {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (kind, node_id) = match self {
            ZoneSource::Line(node_id) => ("line", node_id.as_str()),
            ZoneSource::Feed { kind, node_id } => (kind.as_str(), node_id.as_str()),
        };
        ZoneSourceJson { kind, node_id }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ZoneSource {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match ZoneSourceRaw::deserialize(deserializer)? {
            ZoneSourceRaw::Id(id) => Ok(ZoneSource::Line(id)),
            ZoneSourceRaw::Obj(obj) => match obj.kind.as_str() {
                "line" => Ok(ZoneSource::Line(obj.node_id)),
                "spotify" => Ok(ZoneSource::Feed { kind: FeedKind::Spotify, node_id: obj.node_id }),
                "sendspin" => Ok(ZoneSource::Feed { kind: FeedKind::Sendspin, node_id: obj.node_id }),
                "virtual" => Ok(ZoneSource::Feed { kind: FeedKind::Virtual, node_id: obj.node_id }),
                other => Err(de::Error::unknown_variant(
                    other,
                    &["line", "spotify", "sendspin", "virtual"],
                )),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Zone {
    pub id: String,
    pub name: String,
    /// Узел для старого читателя: первый микрофон, иначе узел первого источника.
    pub tx: String,
    /// Микрофоны и виды. Строка старого файла — микрофон. В файл пишется объект.
    #[serde(default)]
    pub sources: Vec<ZoneSource>,
    #[serde(default)] pub members: Vec<String>,
    #[serde(default)] pub active: bool,
    #[serde(default)] pub seq: u64, // порядок активации; больше = позже = забирает колонки
    #[serde(default)] pub maps: HashMap<String, ChannelMap>,
    /// Карта колонки по источнику. Пусто — у всех источников карта `maps`.
    #[serde(default)] pub source_maps: HashMap<String, HashMap<String, ChannelMap>>,
    #[serde(default = "default_silence_timeout_s")] pub silence_timeout_s: u64,
    #[serde(default = "default_silence_threshold_db")] pub silence_threshold_db: f64,
    /// Запас jitter buffer колонок этой зоны, мс. Нет поля в старом state — 2.
    #[serde(default = "default_jb_ms")]
    pub jb_ms: u32,
}
fn default_silence_timeout_s() -> u64 { 300 }
fn default_silence_threshold_db() -> f64 { -60.0 }
pub(crate) fn default_jb_ms() -> u32 { 2 }

fn tx_from(sources: &[ZoneSource]) -> String {
    if let Some(id) = sources.iter().find_map(|src| match src {
        ZoneSource::Line(id) => Some(id.clone()),
        ZoneSource::Feed { .. } => None,
    }) {
        return id;
    }
    sources.first().map(|src| src.node_id().to_string()).unwrap_or_default()
}

impl Zone {
    /// Порядок хранения, без пустого `node_id` и без повторного ключа.
    /// Первый ключ остаётся.
    pub fn zone_sources(&self) -> Vec<ZoneSource> {
        let mut out = Vec::new();
        let mut seen = Vec::new();
        for src in &self.sources {
            if src.node_id().is_empty() {
                continue;
            }
            let key = src.key();
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);
            out.push(src.clone());
        }
        out
    }

    /// Только `node_id` микрофонов, без пустых и повторов. Пустой `sources`
    /// читается как один `tx`, как раньше.
    pub fn source_ids(&self) -> Vec<String> {
        let ids: Vec<String> = self
            .zone_sources()
            .into_iter()
            .filter_map(|src| match src {
                ZoneSource::Line(id) => Some(id),
                ZoneSource::Feed { .. } => None,
            })
            .collect();
        if ids.is_empty() && self.sources.is_empty() && !self.tx.is_empty() {
            vec![self.tx.clone()]
        } else {
            ids
        }
    }

    /// Пишет список и ещё раз оставляет первый из одинаковых ключей.
    /// `String` — микрофон: контроллер пока отдаёт id узлов.
    pub fn set_sources<S: Into<ZoneSource>>(&mut self, sources: Vec<S>) {
        self.sources = sources.into_iter().map(Into::into).collect();
        self.sources = self.zone_sources();
        self.tx = tx_from(&self.sources);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct State {
    pub version: u32,
    #[serde(default)] pub nodes: HashMap<String, NodeMeta>,
    #[serde(default)] pub zones: Vec<Zone>,
    /// Кто сейчас мастер (node_id). Выставляется трансфером; None — либо
    /// этот контроллер и есть мастер, либо трансфера ещё не было.
    #[serde(default)] pub master_node_id: Option<String>,
    #[serde(default = "one")] pub next_seq: u64,
}
fn one() -> u64 { 1 }
impl Default for State {
    fn default() -> Self {
        Self {
            version: 1,
            nodes: Default::default(),
            zones: Default::default(),
            master_node_id: None,
            next_seq: 1,
        }
    }
}

pub struct StateStore { path: PathBuf, state: State }
impl StateStore {
    pub fn load(path: &Path) -> Self {
        let parsed = std::fs::read_to_string(path).ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        let had_file = parsed.is_some();
        let mut state = parsed.unwrap_or_default();
        let path = path.to_path_buf();
        let maps = had_file && migrate_maps(&mut state);
        let sources = had_file && migrate_sources(&mut state);
        let mut store = Self { path, state };
        if maps || sources {
            let _ = store.update(|_| {});
        }
        store
    }
    pub fn state(&self) -> &State { &self.state }

    pub fn update(&mut self, f: impl FnOnce(&mut State)) -> std::io::Result<()> {
        f(&mut self.state);
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(&self.state)
            .map_err(std::io::Error::other)?)?;
        std::fs::rename(&tmp, &self.path)
    }
}

/// Для каждого tx node_id — кто его слушает. У зоны несколько источников:
/// одни и те же колонки стоят у каждого. Поздно активированная зона
/// (больший seq) забирает общих членов. Неактивные зоны не участвуют.
pub fn assignment(state: &State) -> HashMap<String, Vec<String>> {
    let mut owner: HashMap<&str, u64> = HashMap::new();
    let mut zones: Vec<&Zone> = state.zones.iter().filter(|z| z.active).collect();
    zones.sort_by_key(|z| z.seq);
    for z in &zones {
        for m in &z.members { owner.insert(m, z.seq); }
    }
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for z in zones {
        for m in &z.members {
            if owner.get(m.as_str()) == Some(&z.seq) {
                for src in z.source_ids() {
                    out.entry(src).or_default().push(m.clone());
                }
            }
        }
    }
    for v in out.values_mut() { v.sort(); v.dedup(); }
    out
}

/// Владелец каждого члена среди активных зон. Поздний seq побеждает,
/// значение — id зоны. Та же выборка, что у `assignment`: кража и тишина
/// смотрят на одну раскладку.
pub fn owner_ids(state: &State) -> HashMap<String, String> {
    let mut zones: Vec<&Zone> = state.zones.iter().filter(|z| z.active).collect();
    zones.sort_by_key(|z| z.seq);
    let mut owners = HashMap::new();
    for z in zones {
        for m in &z.members {
            owners.insert(m.clone(), z.id.clone());
        }
    }
    owners
}

/// Слушатели одной зоны одного передатчика. Члены — те, кого зона
/// сейчас держит: украденные поздней зоной сюда не входят. Пустой
/// список членов в результат не кладётся.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneGroup {
    pub zone_id: String,
    pub members: Vec<String>,
}

/// Активные зоны, сгруппированные по передатчику. Внутри — по `zone_id`.
/// Колонка в нескольких зонах попадает только к владельцу (поздний `seq`).
pub fn zone_groups(state: &State) -> HashMap<String, Vec<ZoneGroup>> {
    let owners = owner_ids(state);
    let mut zones: Vec<&Zone> = state.zones.iter().filter(|z| z.active).collect();
    zones.sort_by(|a, b| a.id.cmp(&b.id));
    let mut out: HashMap<String, Vec<ZoneGroup>> = HashMap::new();
    for z in zones {
        let mut members: Vec<String> = z
            .members
            .iter()
            .filter(|m| owners.get(m.as_str()) == Some(&z.id))
            .cloned()
            .collect();
        if members.is_empty() {
            continue;
        }
        members.sort();
        for src in z.source_ids() {
            out.entry(src).or_default().push(ZoneGroup {
                zone_id: z.id.clone(),
                members: members.clone(),
            });
        }
    }
    for groups in out.values_mut() {
        groups.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
    }
    out
}

/// Карта каналов зоны-владельца для каждого члена активных зон.
/// Владелец — тот же, что у `owner_ids` (поздний seq побеждает).
/// Нет записи в `maps` владельца — стерео (`ChannelMap::default()`).
pub fn owner_maps(state: &State) -> HashMap<String, ChannelMap> {
    let owners = owner_ids(state);
    let mut out = HashMap::new();
    for (member, zone_id) in owners {
        let map = state
            .zones
            .iter()
            .find(|z| z.id == zone_id)
            .and_then(|z| z.maps.get(&member).copied())
            .unwrap_or_default();
        out.insert(member, map);
    }
    out
}

/// Карты источников для одной колонки. Один источник — пустой список:
/// колонка по-прежнему кладёт `maps[member]` после микшера.
/// Несколько — своя карта на источник; нет записи — общая карта колонки.
pub fn source_channel_maps(zone: &Zone, member: &str) -> Vec<(String, ChannelMap)> {
    let sources = zone.source_ids();
    if sources.len() < 2 {
        return Vec::new();
    }
    let fallback = zone.maps.get(member).copied().unwrap_or_default();
    sources
        .into_iter()
        .map(|src| {
            let map = zone
                .source_maps
                .get(member)
                .and_then(|by| by.get(&src))
                .copied()
                .unwrap_or(fallback);
            (src, map)
        })
        .collect()
}

/// Для членов зоны без записи в `maps` копирует `nodes[id].channel_map`
/// или стерео. `true` — что-то дописали.
pub fn migrate_maps(state: &mut State) -> bool {
    let mut changed = false;
    for z in &mut state.zones {
        for m in z.members.clone() {
            if z.maps.contains_key(&m) { continue; }
            let map = state.nodes.get(&m).and_then(|n| n.channel_map).unwrap_or_default();
            z.maps.insert(m, map);
            changed = true;
        }
    }
    changed
}

/// Старый state хранит один `tx`. Переписывает его в `sources`, чтобы
/// следующий save уже был списком. `tx` — первый микрофон, без микрофонов
/// узел первого источника.
pub fn migrate_sources(state: &mut State) -> bool {
    let mut changed = false;
    for z in &mut state.zones {
        if z.sources.is_empty() {
            if z.tx.is_empty() {
                continue;
            }
            z.sources = vec![ZoneSource::Line(z.tx.clone())];
            changed = true;
        } else {
            let tx = tx_from(&z.zone_sources());
            if z.tx != tx {
                z.tx = tx;
                changed = true;
            }
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use zproto::{ChanSel, FeedKind};

    fn z(id: &str, tx: &str, members: &[&str], active: bool, seq: u64) -> Zone {
        Zone { id: id.into(), name: id.into(), tx: tx.into(), sources: Vec::new(),
               members: members.iter().map(|s| s.to_string()).collect(), active, seq,
               maps: HashMap::new(), source_maps: HashMap::new(),
               silence_timeout_s: 300, silence_threshold_db: -60.0, jb_ms: 2 }
    }

    #[test]
    fn single_active_zone() {
        let s = State { zones: vec![z("A", "tx1", &["r1", "r2"], true, 1)],
                        ..Default::default() };
        let a = assignment(&s);
        assert_eq!(a["tx1"], vec!["r1".to_string(), "r2".to_string()]);
    }

    #[test]
    fn owner_ids_shared_member_goes_to_later_zone() {
        let s = State { zones: vec![z("A", "tx1", &["r1"], true, 1),
                                    z("B", "tx2", &["r1"], true, 2)],
                        ..Default::default() };
        let o = owner_ids(&s);
        assert_eq!(o["r1"], "B", "больший seq забирает общего члена");
    }

    #[test]
    fn owner_maps_follow_the_later_zone() {
        let mut a = z("A", "tx1", &["r1"], true, 1);
        a.maps.insert("r1".into(), ChannelMap { out_l: Some(ChanSel::L), out_r: None });
        let mut b = z("B", "tx1", &["r1"], true, 2);
        b.maps.insert("r1".into(), ChannelMap { out_l: Some(ChanSel::R), out_r: Some(ChanSel::R) });
        let s = State { zones: vec![a, b], ..Default::default() };
        let maps = owner_maps(&s);
        assert_eq!(maps["r1"].out_l, Some(ChanSel::R));
    }

    #[test]
    fn later_zone_steals_shared_member() {
        let s = State { zones: vec![z("A", "tx1", &["r1", "r2"], true, 1),
                                    z("B", "tx1", &["r2"], true, 2)], // B активирована позже
                        ..Default::default() };
        let a = assignment(&s);
        assert_eq!(a["tx1"], vec!["r1".to_string(), "r2".to_string()], "один tx — оба слушателя");
    }

    #[test]
    fn different_tx_steal() {
        let s = State { zones: vec![z("A", "tx1", &["r1", "r2"], true, 1),
                                    z("B", "tx2", &["r2"], true, 2)],
                        ..Default::default() };
        let a = assignment(&s);
        assert_eq!(a["tx1"], vec!["r1".to_string()]);
        assert_eq!(a["tx2"], vec!["r2".to_string()]);
    }

    #[test]
    fn two_sources_share_the_zones_speakers() {
        let mut zone = z("A", "tx1", &["r1", "r2"], true, 1);
        zone.set_sources(vec![
            ZoneSource::Line("tx1".into()),
            ZoneSource::Line("tx2".into()),
            ZoneSource::Line("tx1".into()),
        ]);
        let s = State { zones: vec![zone], ..Default::default() };
        let a = assignment(&s);
        assert_eq!(a["tx1"], vec!["r1".to_string(), "r2".to_string()]);
        assert_eq!(a["tx2"], vec!["r1".to_string(), "r2".to_string()]);
        let g = zone_groups(&s);
        assert_eq!(g["tx1"][0].members, vec!["r1".to_string(), "r2".to_string()]);
        assert_eq!(g["tx2"][0].members, vec!["r1".to_string(), "r2".to_string()]);
    }

    #[test]
    fn each_source_keeps_its_own_channel_map() {
        let mut zone = z("A", "tx1", &["r1"], true, 1);
        zone.set_sources(vec![
            ZoneSource::Line("tx1".into()),
            ZoneSource::Line("tx2".into()),
        ]);
        zone.maps.insert("r1".into(), ChannelMap { out_l: Some(ChanSel::L), out_r: Some(ChanSel::L) });
        zone.source_maps.entry("r1".into()).or_default().insert(
            "tx2".into(),
            ChannelMap { out_l: Some(ChanSel::R), out_r: None },
        );
        let maps = source_channel_maps(&zone, "r1");
        assert_eq!(maps[0].0, "tx1");
        assert_eq!(maps[0].1, zone.maps["r1"], "нет своей карты — общая колонки");
        assert_eq!(maps[1].1.out_l, Some(ChanSel::R));
        assert!(source_channel_maps(&z("B", "tx1", &["r1"], true, 1), "r1").is_empty());
    }

    #[test]
    fn old_state_file_keeps_its_single_tx_as_a_source() {
        let raw = r#"{
        "version": 1,
        "zones": [{ "id": "A", "name": "A", "tx": "tx1", "members": ["rx1"], "active": true, "seq": 1 }]
    }"#;
        let mut st: State = serde_json::from_str(raw).unwrap();
        assert!(st.zones[0].sources.is_empty());
        assert!(migrate_sources(&mut st));
        assert_eq!(st.zones[0].source_ids(), vec!["tx1".to_string()]);
        assert_eq!(st.zones[0].tx, "tx1");
        assert!(!migrate_sources(&mut st));
    }

    #[test]
    fn stopped_zone_releases_and_does_not_return() {
        let s = State { zones: vec![z("A", "tx1", &["r1"], true, 1),
                                    z("B", "tx2", &["r1"], false, 2)],
                        ..Default::default() };
        let a = assignment(&s);
        assert_eq!(a["tx1"], vec!["r1".to_string()]);
        assert!(!a.contains_key("tx2"), "неактивная зона не создаёт запись tx");
    }

    #[test]
    fn zone_defaults_and_map_migration() {
        let raw = r#"{
        "version": 1,
        "nodes": { "rx1": { "channel_map": { "out_l": "l", "out_r": null } } },
        "zones": [{ "id": "A", "name": "A", "tx": "tx1", "members": ["rx1"], "active": true, "seq": 1 }]
    }"#;
        let mut st: State = serde_json::from_str(raw).unwrap();
        assert_eq!(st.zones[0].silence_timeout_s, 300);
        assert_eq!(st.zones[0].silence_threshold_db, -60.0);
        assert_eq!(st.zones[0].jb_ms, 2);
        assert!(st.zones[0].maps.is_empty());
        assert!(migrate_maps(&mut st));
        assert_eq!(st.zones[0].maps["rx1"].out_r, None);
        assert!(!migrate_maps(&mut st), "повторно копировать нечего");
    }

    #[test]
    fn zone_json_without_jb_ms_defaults_to_2() {
        let raw = r#"{"id":"A","name":"A","tx":"tx1","members":["rx1"],"active":true,"seq":1}"#;
        let zone: Zone = serde_json::from_str(raw).unwrap();
        assert_eq!(zone.jb_ms, 2);
    }

    #[test]
    fn missing_file_gives_default() {
        let dir = std::env::temp_dir().join(format!("zvst-{}", std::process::id()));
        let s = StateStore::load(&dir.join("none.json"));
        assert_eq!(s.state().version, 1);
        assert!(s.state().zones.is_empty());
    }

    #[test]
    fn update_persists_atomically() {
        let dir = std::env::temp_dir().join(format!("zvst-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");
        let mut st = StateStore::load(&path);
        st.update(|s| {
            s.zones.push(Zone { id: "z1".into(), name: "Зал".into(), tx: "tx1".into(),
                                sources: Vec::new(),
                                members: vec!["rx1".into()], active: false, seq: 0,
                                maps: HashMap::new(), source_maps: HashMap::new(),
                                silence_timeout_s: 300, silence_threshold_db: -60.0, jb_ms: 2 });
            s.nodes.insert("rx1".into(), NodeMeta {
                display_name: Some("Кухня".into()),
                channel_map: Some(ChannelMap { out_l: Some(ChanSel::L), out_r: None }),
                ..Default::default()
            });
        }).unwrap();
        assert!(!dir.join("state.json.tmp").exists(), "tmp не остался");
        let st2 = StateStore::load(&path);
        assert_eq!(st2.state().zones[0].name, "Зал");
        assert_eq!(st2.state().nodes["rx1"].channel_map.unwrap().out_r, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn shared_speaker_stays_with_the_later_zone_of_one_tx() {
        let s = State {
            zones: vec![
                z("all", "pi4", &["opi", "pc"], true, 1),
                z("kitchen", "pi4", &["opi"], true, 2),
            ],
            ..Default::default()
        };
        let g = zone_groups(&s);
        assert_eq!(g["pi4"][0].zone_id, "all");
        assert_eq!(g["pi4"][0].members, vec!["pc".to_string()]);
        assert_eq!(g["pi4"][1].zone_id, "kitchen");
        assert_eq!(g["pi4"][1].members, vec!["opi".to_string()]);
    }

    #[test]
    fn shared_speaker_moves_to_the_other_transmitters_zone() {
        let s = State {
            zones: vec![
                z("all", "pi4", &["opi", "pc"], true, 1),
                z("kitchen", "pc-tx", &["opi"], true, 2),
            ],
            ..Default::default()
        };
        let g = zone_groups(&s);
        assert_eq!(g["pi4"].len(), 1);
        assert_eq!(g["pi4"][0].members, vec!["pc".to_string()]);
        assert_eq!(g["pc-tx"][0].zone_id, "kitchen");
        assert_eq!(g["pc-tx"][0].members, vec!["opi".to_string()]);
    }

    #[test]
    fn inactive_zone_and_zone_with_no_speakers_are_absent() {
        let s = State {
            zones: vec![
                z("all", "pi4", &["opi"], true, 1),
                z("kitchen", "pi4", &["opi"], true, 2),
                z("off", "pi4", &["pc"], false, 3),
            ],
            ..Default::default()
        };
        let g = zone_groups(&s);
        let ids: Vec<&str> = g["pi4"].iter().map(|z| z.zone_id.as_str()).collect();
        assert_eq!(ids, vec!["kitchen"]);
    }

    #[test]
    fn old_source_strings_are_line_mics() {
        let zone: Zone = serde_json::from_str(
            r#"{"id":"z","name":"Зона","tx":"n1","sources":["n1"]}"#,
        ).unwrap();
        assert_eq!(zone.zone_sources(), vec![ZoneSource::Line("n1".into())]);
        assert_eq!(zone.source_ids(), vec!["n1".to_string()]);
    }

    #[test]
    fn feed_only_zone_roundtrips_and_sets_tx() {
        let mut zone: Zone = serde_json::from_str(
            r#"{"id":"z","name":"Зона","tx":"","sources":[]}"#,
        ).unwrap();
        zone.set_sources(vec![ZoneSource::Feed {
            kind: FeedKind::Spotify,
            node_id: "pc".into(),
        }]);
        assert_eq!(zone.tx, "pc");
        let back: Zone = serde_json::from_str(&serde_json::to_string(&zone).unwrap()).unwrap();
        assert_eq!(back.zone_sources()[0].key(), "spotify:pc");
        assert!(back.source_ids().is_empty());
    }

    #[test]
    fn zone_sources_drop_blanks_and_repeat_keys() {
        let zone: Zone = serde_json::from_str(
            r#"{"id":"z","name":"Зона","tx":"","sources":[
                "",
                {"kind":"line","node_id":"n1"},
                "n1",
                {"kind":"spotify","node_id":"pc"},
                {"kind":"spotify","node_id":"pc"},
                {"kind":"sendspin","node_id":"pc"}
            ]}"#,
        ).unwrap();
        assert_eq!(zone.zone_sources(), vec![
            ZoneSource::Line("n1".into()),
            ZoneSource::Feed { kind: FeedKind::Spotify, node_id: "pc".into() },
            ZoneSource::Feed { kind: FeedKind::Sendspin, node_id: "pc".into() },
        ]);
        let mut zone = zone;
        zone.set_sources(vec![
            ZoneSource::Feed { kind: FeedKind::Virtual, node_id: "pc".into() },
            ZoneSource::Line(String::new()),
            ZoneSource::Line("n1".into()),
            ZoneSource::Line("n1".into()),
        ]);
        assert_eq!(zone.tx, "n1");
        assert_eq!(zone.sources, vec![
            ZoneSource::Feed { kind: FeedKind::Virtual, node_id: "pc".into() },
            ZoneSource::Line("n1".into()),
        ]);
        assert_eq!(
            serde_json::to_string(&ZoneSource::Line("n1".into())).unwrap(),
            r#"{"kind":"line","node_id":"n1"}"#,
        );
    }
}
