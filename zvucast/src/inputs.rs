//! Каналы захвата узла: что видит агент и что пользователь включил и назвал.
//! В стереопоток попадают только index 0 и 1 устройства, с которого реально
//! идёт захват. Остальные каналы видны в настройках, но в карту зоны не входят.

use std::collections::BTreeSet;

use gstreamer as gst;
use gstreamer::prelude::*;
use serde::{Deserialize, Serialize};
use zproto::CaptureChan;

pub const VIRTUAL_DEVICE_KEY: &str = "zvucast";
/// Источник кабеля pw-loopback. pipewiresrc целится сюда, не в монитор sink.
pub const VIRTUAL_SOURCE_KEY: &str = "zvucast-monitor";

/// Старый общий выход и выходы зон (`zvucast-{id}`) — наше пространство имён.
pub fn is_virtual_device_key(key: &str) -> bool {
    key == VIRTUAL_DEVICE_KEY || key.starts_with("zvucast-")
}

/// Sink, в который нельзя отправлять локальное воспроизведение.
pub fn is_virtual_sink_name(name: &str) -> bool {
    name == VIRTUAL_DEVICE_KEY || name.starts_with("zvucast-")
}

/// Кабельный sink зоны: `zvucast-…`, не общий `zvucast` и не монитор `*-monitor`.
pub fn is_zone_sink_key(key: &str) -> bool {
    key.starts_with("zvucast-") && !key.ends_with("-monitor")
}

/// Два канала виртуального выхода. Имена по умолчанию ставит merge_inputs.
pub fn virtual_capture_channels() -> Vec<CaptureChan> {
    (0..2u16)
        .map(|index| CaptureChan {
            id: format!("{VIRTUAL_DEVICE_KEY}:{index}"),
            device_key: VIRTUAL_DEVICE_KEY.into(),
            device_name: "Zvucast".into(),
            index,
        })
        .collect()
}

/// Имя источника, которое уже можно открыть.
/// `zvucast` → `zvucast-monitor`. `zvucast-{id}` без суффикса → `zvucast-{id}-monitor`.
/// Готовый монитор второй раз не дописываем.
pub fn open_capture_name(key: &str) -> String {
    if key == VIRTUAL_DEVICE_KEY {
        VIRTUAL_SOURCE_KEY.to_string()
    } else if is_zone_sink_key(key) {
        format!("{key}-monitor")
    } else {
        key.to_string()
    }
}

/// Имя слота захвата, которое уже можно открыть. Обычный ключ возвращаем
/// как есть, в том числе строку ALSA: на PipeWire её отсеет агент.
/// Заглушки `capture` и `playback` устройством не являются.
pub fn capture_target(views: &[InputView]) -> Option<String> {
    let key = views.iter().find(|v| v.stream.is_some())?.device_key.as_str();
    if key == "capture" || key == "playback" {
        return None;
    }
    Some(open_capture_name(key))
}

/// Отсортированные уникальные ключи, у которых `stream` задан.
/// Виртуальные ключи и заглушки `capture` / `playback` не входят.
pub fn playback_targets(views: &[InputView]) -> Vec<String> {
    let mut keys: Vec<String> = views
        .iter()
        .filter(|v| {
            v.stream.is_some()
                && !is_virtual_device_key(&v.device_key)
                && v.device_key != "capture"
                && v.device_key != "playback"
        })
        .map(|v| v.device_key.clone())
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// Первый ключ из [`playback_targets`].
pub fn playback_target(views: &[InputView]) -> Option<String> {
    playback_targets(views).into_iter().next()
}

/// Зона кабеля `zvucast-{id}`, у которого все каналы index <= 1 выключены.
/// Кабеля в списке нет — зону не глушим.
pub fn muted_zone_ids(views: &[InputView]) -> BTreeSet<String> {
    let mut keys: Vec<&str> = views
        .iter()
        .map(|v| v.device_key.as_str())
        .filter(|key| is_zone_sink_key(key))
        .collect();
    keys.sort();
    keys.dedup();
    let mut muted = BTreeSet::new();
    for key in keys {
        let stereo: Vec<&InputView> = views
            .iter()
            .filter(|v| v.device_key == key && v.index <= 1)
            .collect();
        if stereo.is_empty() || stereo.iter().any(|v| v.enabled) {
            continue;
        }
        if let Some(id) = key.strip_prefix("zvucast-").filter(|id| !id.is_empty()) {
            muted.insert(id.to_string());
        }
    }
    muted
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputPref {
    pub id: String,
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InputView {
    pub id: String,
    pub device_key: String,
    pub device_name: String,
    pub index: u16,
    pub name: String,
    pub enabled: bool,
    /// `l` / `r`, если канал включён и это левый или правый слот стереопотока.
    pub stream: Option<String>,
}

pub fn default_chan_name(index: u16) -> String {
    match index {
        0 => "Левый".into(),
        1 => "Правый".into(),
        n => format!("Канал {}", n + 1),
    }
}

/// Кабельный sink зоны виден в настройках. Скрыты старый ключ `zvucast`,
/// мониторы `*-monitor` и чужой ключ с подписью «Zvucast» или «Zvucast: …».
pub fn visible_inputs(views: Vec<InputView>) -> Vec<InputView> {
    views.into_iter().filter(|v| !hidden_from_settings(v)).collect()
}

fn hidden_from_settings(v: &InputView) -> bool {
    if is_zone_sink_key(&v.device_key) {
        return false;
    }
    v.device_key == VIRTUAL_DEVICE_KEY
        || v.device_key.ends_with("-monitor")
        || v.device_name == "Zvucast"
        || v.device_name.starts_with("Zvucast:")
}

/// Новый канал включён. Уже сохранённые имя и галочка не затираются.
/// Каналов, которых агент больше не видит, в списке нет.
/// Слот стерео берётся из включённых каналов, не из устройства в конфиге.
pub fn merge_inputs(discovered: &[CaptureChan], prefs: &[InputPref]) -> Vec<InputView> {
    let mut views: Vec<InputView> = discovered
        .iter()
        .map(|ch| {
            let pref = prefs.iter().find(|p| p.id == ch.id);
            let name = pref
                .map(|p| p.name.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| default_chan_name(ch.index));
            InputView {
                id: ch.id.clone(),
                device_key: ch.device_key.clone(),
                device_name: ch.device_name.clone(),
                index: ch.index,
                name,
                enabled: pref.map(|p| p.enabled).unwrap_or(true),
                stream: None,
            }
        })
        .collect();
    assign_stream(&mut views);
    views
}

/// Сборка как у [`merge_inputs`], но стерео размечается на каждом включённом
/// выходе, а не на одном слоте захвата.
pub fn merge_outputs(discovered: &[CaptureChan], prefs: &[InputPref]) -> Vec<InputView> {
    let mut views: Vec<InputView> = discovered
        .iter()
        .map(|ch| {
            let pref = prefs.iter().find(|p| p.id == ch.id);
            let name = pref
                .map(|p| p.name.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| default_chan_name(ch.index));
            InputView {
                id: ch.id.clone(),
                device_key: ch.device_key.clone(),
                device_name: ch.device_name.clone(),
                index: ch.index,
                name,
                enabled: pref.map(|p| p.enabled).unwrap_or(true),
                stream: None,
            }
        })
        .collect();
    assign_playback_streams(&mut views);
    views
}

/// Первое по алфавиту невиртуальное устройство с включённым каналом
/// index <= 1. Такого нет и `allow_virtual` — первое виртуальное.
fn stereo_slot_key(views: &[InputView], allow_virtual: bool) -> Option<String> {
    let mut hardware: Vec<&str> = views
        .iter()
        .filter(|ch| ch.enabled && ch.index <= 1 && !is_virtual_device_key(&ch.device_key))
        .map(|ch| ch.device_key.as_str())
        .collect();
    hardware.sort();
    hardware.dedup();
    if let Some(key) = hardware.first() {
        return Some((*key).to_string());
    }
    if !allow_virtual {
        return None;
    }
    let mut virtual_keys: Vec<&str> = views
        .iter()
        .filter(|ch| ch.enabled && ch.index <= 1 && is_virtual_device_key(&ch.device_key))
        .map(|ch| ch.device_key.as_str())
        .collect();
    virtual_keys.sort();
    virtual_keys.dedup();
    virtual_keys.first().map(|key| (*key).to_string())
}

/// Канал 0 включённого слота захвата → `l`, канал 1 → `r`.
/// Выключенный левый не становится правым.
fn assign_stream(views: &mut [InputView]) {
    let Some(key) = stereo_slot_key(views, true) else {
        return;
    };
    for ch in views.iter_mut() {
        if ch.device_key != key || !ch.enabled {
            continue;
        }
        ch.stream = match ch.index {
            0 => Some("l".into()),
            1 => Some("r".into()),
            _ => None,
        };
    }
}

/// Для каждого невиртуального ключа, кроме заглушек `capture` и `playback`,
/// если есть включённый канал с index <= 1: включённый 0 → `l`, включённый 1 → `r`.
/// Виртуальные ключи не размечаются.
fn assign_playback_streams(views: &mut [InputView]) {
    let mut keys: Vec<String> = views
        .iter()
        .filter(|ch| {
            ch.enabled
                && ch.index <= 1
                && !is_virtual_device_key(&ch.device_key)
                && ch.device_key != "capture"
                && ch.device_key != "playback"
        })
        .map(|ch| ch.device_key.clone())
        .collect();
    keys.sort();
    keys.dedup();
    for ch in views.iter_mut() {
        if !ch.enabled || !keys.iter().any(|key| key == &ch.device_key) {
            continue;
        }
        ch.stream = match ch.index {
            0 => Some("l".into()),
            1 => Some("r".into()),
            _ => None,
        };
    }
}

/// Сторона остаётся, если хотя бы у одного невиртуального включённого выхода
/// есть этот index: 0 → `l`, 1 → `r`. Виртуальный ключ сторону не держит.
/// Пустой список видов — агент ещё не прислал выходы, карту не трогаем.
pub fn mask_disabled_outputs(map: zproto::ChannelMap, views: &[InputView]) -> zproto::ChannelMap {
    if views.is_empty() {
        return map;
    }
    let has = |index: u16| {
        views.iter().any(|v| {
            !is_virtual_device_key(&v.device_key) && v.enabled && v.index == index
        })
    };
    zproto::ChannelMap {
        out_l: if has(0) { map.out_l } else { None },
        out_r: if has(1) { map.out_r } else { None },
    }
}

fn fallback_pair(device: Option<&str>, fallback_key: &str, fallback_name: &str) -> Vec<CaptureChan> {
    let key = device.unwrap_or(fallback_key).to_string();
    let name = device.unwrap_or(fallback_name).to_string();
    (0..2)
        .map(|index| CaptureChan {
            id: format!("{key}:{index}"),
            device_key: key.clone(),
            device_name: name.clone(),
            index,
        })
        .collect()
}

/// Если монитор устройств пуст, у tx всё равно два канала настроенного входа.
pub fn fallback_inputs(capture_device: Option<&str>) -> Vec<CaptureChan> {
    fallback_pair(capture_device, "capture", "Захват")
}

/// То же для выхода: два канала настроенного устройства воспроизведения.
pub fn fallback_outputs(playback_device: Option<&str>) -> Vec<CaptureChan> {
    fallback_pair(playback_device, "playback", "Воспроизведение")
}

/// Устройства захвата через gst DeviceMonitor. Пустой список — вызывающий
/// подставит `fallback_inputs`.
pub fn discover_capture_channels() -> Vec<CaptureChan> {
    discover_channels("Audio/Source", false)
}

/// Устройства воспроизведения. Пустой список — `fallback_outputs`.
pub fn discover_playback_channels() -> Vec<CaptureChan> {
    let mut out = discover_channels("Audio/Sink", true);
    if let Some(sinks) = live_sink_nodes() {
        for chan in &mut out {
            let device_key = resolve_playback_key(&chan.device_key, &chan.device_name, &sinks);
            chan.id = format!("{device_key}:{}", chan.index);
            chan.device_key = device_key;
        }
        sort_discovered(&mut out);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkNode {
    pub node_name: String,
    pub card_name: String,
}

pub fn sink_nodes_from_dump(json: &str) -> Vec<SinkNode> {
    let Ok(items) = serde_json::from_str::<Vec<serde_json::Value>>(json) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let Some(props) = item.pointer("/info/props") else { continue };
        if props.get("media.class").and_then(|v| v.as_str()) != Some("Audio/Sink") {
            continue;
        }
        let Some(node_name) = props.get("node.name").and_then(|v| v.as_str()) else { continue };
        if node_name.is_empty() {
            continue;
        }
        let card_name = props
            .get("alsa.card_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        out.push(SinkNode { node_name: node_name.to_string(), card_name });
    }
    out
}

pub fn resolve_playback_key(key: &str, device_name: &str, sinks: &[SinkNode]) -> String {
    if sinks.iter().any(|s| s.node_name == key) {
        return key.to_string();
    }
    let mut matches: Vec<&SinkNode> = sinks
        .iter()
        .filter(|s| !s.card_name.is_empty() && (s.card_name == key || s.card_name == device_name))
        .collect();
    matches.sort_by(|a, b| {
        let rank = |n: &str| if n.contains("stereo-fallback") { 0 } else { 1 };
        rank(&a.node_name).cmp(&rank(&b.node_name)).then(a.node_name.cmp(&b.node_name))
    });
    matches.first().map(|s| s.node_name.clone()).unwrap_or_else(|| key.to_string())
}

pub fn live_sink_nodes() -> Option<Vec<SinkNode>> {
    let out = std::process::Command::new("timeout").args(["2", "pw-dump"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?;
    if serde_json::from_str::<Vec<serde_json::Value>>(&text).is_err() {
        return None;
    }
    Some(sink_nodes_from_dump(&text))
}

fn discover_channels(kind: &str, playback: bool) -> Vec<CaptureChan> {
    if gst::init().is_err() {
        return Vec::new();
    }
    let mon = gst::DeviceMonitor::new();
    if mon.add_filter(Some(kind), None).is_none() || mon.start().is_err() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for dev in mon.devices() {
        let device_name = dev.display_name().to_string();
        let props = dev.properties();
        let device_key = device_key_from_props(props.as_ref(), playback)
            .unwrap_or_else(|| device_name.clone());
        let n = dev.caps().as_ref().map(channel_count).unwrap_or(2);
        for index in 0..n {
            out.push(CaptureChan {
                id: format!("{device_key}:{index}"),
                device_key: device_key.clone(),
                device_name: device_name.clone(),
                index,
            });
        }
    }
    mon.stop();
    sort_discovered(&mut out);
    out
}

/// Воспроизведение: `node.name`, затем `device.string`, затем `object.path`.
/// Захват: прежний порядок. Пустое свойство пропускается.
fn device_key_from_props(props: Option<&gst::Structure>, playback: bool) -> Option<String> {
    let props = props?;
    let fields: &[&str] = if playback {
        &["node.name", "device.string", "object.path"]
    } else {
        &["device.string", "node.name", "object.path"]
    };
    fields.iter().find_map(|field| {
        props.get::<String>(*field).ok().filter(|value| !value.is_empty())
    })
}

fn sort_discovered(out: &mut Vec<CaptureChan>) {
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out.dedup_by(|a, b| a.id == b.id);
    out.sort_by(|a, b| a.device_name.cmp(&b.device_name).then(a.index.cmp(&b.index)));
}

fn channel_count(caps: &gst::Caps) -> u16 {
    let mut n = 0u16;
    for s in caps.iter() {
        if let Ok(c) = s.get::<i32>("channels") {
            if c > 0 {
                n = n.max(c as u16);
            }
        }
    }
    if n == 0 || n > 8 { 2 } else { n }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(id: &str, key: &str, index: u16) -> CaptureChan {
        CaptureChan { id: id.into(), device_key: key.into(), device_name: key.into(), index }
    }

    #[test]
    fn new_channels_start_enabled_and_named() {
        let views = merge_inputs(&[ch("card:0", "card", 0), ch("card:1", "card", 1)], &[]);
        assert!(views.iter().all(|v| v.enabled));
        assert_eq!(views[0].name, "Левый");
        assert_eq!(views[0].stream.as_deref(), Some("l"));
        assert_eq!(views[1].stream.as_deref(), Some("r"));
    }

    #[test]
    fn prefs_stick_and_disabling_left_does_not_promote_right() {
        let prefs = vec![InputPref { id: "card:0".into(), name: "ТВ: левый".into(), enabled: false }];
        let views = merge_inputs(&[ch("card:0", "card", 0), ch("card:1", "card", 1)], &prefs);
        assert!(!views[0].enabled);
        assert_eq!(views[0].name, "ТВ: левый");
        assert!(views[0].stream.is_none());
        assert_eq!(views[1].stream.as_deref(), Some("r"));
    }

    #[test]
    fn disabled_output_drops_out_of_the_map() {
        use zproto::{ChanSel, ChannelMap};
        let views = merge_inputs(&[ch("card:0", "card", 0), ch("card:1", "card", 1)], &[
            InputPref { id: "card:0".into(), name: "ТВ: левый".into(), enabled: false },
        ]);
        let masked = mask_disabled_outputs(
            ChannelMap { out_l: Some(ChanSel::L), out_r: Some(ChanSel::R) },
            &views,
        );
        assert!(masked.out_l.is_none());
        assert_eq!(masked.out_r, Some(ChanSel::R));
    }

    #[test]
    fn enabled_device_takes_the_slot_when_configured_one_is_off() {
        let prefs = vec![
            InputPref { id: "card:0".into(), name: "карта л".into(), enabled: false },
            InputPref { id: "card:1".into(), name: "карта п".into(), enabled: false },
            InputPref { id: "spk:0".into(), name: "Ноутбук: левый".into(), enabled: true },
            InputPref { id: "spk:1".into(), name: "Ноутбук: правый".into(), enabled: true },
        ];
        let views = merge_inputs(
            &[ch("card:0", "card", 0), ch("card:1", "card", 1), ch("spk:0", "spk", 0), ch("spk:1", "spk", 1)],
            &prefs,
        );
        let spk_l = views.iter().find(|v| v.id == "spk:0").unwrap();
        let spk_r = views.iter().find(|v| v.id == "spk:1").unwrap();
        assert_eq!(spk_l.stream.as_deref(), Some("l"));
        assert_eq!(spk_r.stream.as_deref(), Some("r"));
        assert!(views.iter().filter(|v| v.device_key == "card").all(|v| v.stream.is_none()));
    }

    #[test]
    fn gone_device_drops_its_pref() {
        let prefs = vec![InputPref { id: "old:0".into(), name: "старый".into(), enabled: true }];
        let views = merge_inputs(&[ch("card:0", "card", 0)], &prefs);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].id, "card:0");
    }

    #[test]
    fn virtual_channels_leave_the_line_in_slot_alone() {
        let r#virtual = virtual_capture_channels();
        assert_eq!(r#virtual.len(), 2);
        assert_eq!(r#virtual[0].id, "zvucast:0");
        assert_eq!(r#virtual[1].id, "zvucast:1");
        assert_eq!(r#virtual[0].device_key, VIRTUAL_DEVICE_KEY);
        assert_eq!(r#virtual[0].device_name, "Zvucast");

        let hw = CaptureChan {
            id: "hw:0".into(),
            device_key: "plughw:CARD=Generic_1".into(),
            device_name: "HD-Audio Generic".into(),
            index: 0,
        };
        let mut discovered = vec![hw];
        discovered.extend(virtual_capture_channels());
        discovered.push(CaptureChan {
            id: "zvucast-z1:0".into(),
            device_key: "zvucast-z1".into(),
            device_name: "Zvucast: Зал".into(),
            index: 0,
        });

        let both_on = merge_inputs(&discovered, &[]);
        assert_eq!(capture_target(&both_on).as_deref(), Some("plughw:CARD=Generic_1"));
        assert!(both_on.iter().filter(|v| is_virtual_device_key(&v.device_key)).all(|v| v.stream.is_none()));
        assert!(both_on.iter().any(|v| v.device_key == "plughw:CARD=Generic_1" && v.stream.as_deref() == Some("l")));

        let mic = [
            ch("mic:0", "alsa_input.usb-BY-V20.analog-stereo", 0),
            ch("mic:1", "alsa_input.usb-BY-V20.analog-stereo", 1),
        ];
        let aimed = merge_inputs(&mic, &[]);
        assert_eq!(
            capture_target(&aimed).as_deref(),
            Some("alsa_input.usb-BY-V20.analog-stereo"),
            "имя узла PipeWire остаётся ключом слота"
        );

        // Железо выключено — слот захвата у первого виртуального ключа.
        let prefs = vec![InputPref { id: "hw:0".into(), name: "Левый".into(), enabled: false }];
        let hw_off = merge_inputs(&discovered, &prefs);
        assert_eq!(capture_target(&hw_off).as_deref(), Some("zvucast-monitor"));
        assert_eq!(hw_off.iter().find(|v| v.id == "zvucast:0").unwrap().stream.as_deref(), Some("l"));
        assert_eq!(hw_off.iter().find(|v| v.id == "zvucast:1").unwrap().stream.as_deref(), Some("r"));
        assert!(hw_off.iter().find(|v| v.id == "zvucast-z1:0").unwrap().stream.is_none());
        assert!(hw_off.iter().find(|v| v.id == "hw:0").unwrap().stream.is_none());
    }

    #[test]
    fn virtual_speaker_cables_stay_out_of_the_settings_list() {
        let mic = CaptureChan {
            id: "hw:0".into(),
            device_key: "plughw:CARD=Generic_1".into(),
            device_name: "HD-Audio Generic".into(),
            index: 0,
        };
        let cable = CaptureChan {
            id: "zvucast-z1:0".into(),
            device_key: "zvucast-z1".into(),
            device_name: "Zvucast: Зал".into(),
            index: 0,
        };
        let legacy = virtual_capture_channels();
        let mut discovered = vec![mic, cable];
        discovered.extend(legacy);
        // Монитор кабеля иногда приходит с чужим ключом, но с нашей подписью.
        discovered.push(CaptureChan {
            id: "monitor:0".into(),
            device_key: "alsa_input.pci-0000_00_1f.3".into(),
            device_name: "Zvucast: Спальня".into(),
            index: 0,
        });
        discovered.push(CaptureChan {
            id: "src:0".into(),
            device_key: "zvucast-hall-monitor".into(),
            device_name: "monitor".into(),
            index: 0,
        });
        let shown = visible_inputs(merge_inputs(&discovered, &[]));
        assert!(shown.iter().any(|v| v.device_key == "plughw:CARD=Generic_1"));
        assert!(shown.iter().any(|v| v.device_key == "zvucast-z1" && v.device_name == "Zvucast: Зал"));
        assert!(shown.iter().all(|v| v.device_key != VIRTUAL_DEVICE_KEY));
        assert!(shown.iter().all(|v| v.device_name != "Zvucast: Спальня"));
        assert!(shown.iter().all(|v| !v.device_key.ends_with("-monitor")));
    }

    #[test]
    fn hardware_slot_beats_the_virtual_cable() {
        let discovered = vec![
            ch("hw:0", "plughw:CARD=Generic_1", 0),
            ch("hw:1", "plughw:CARD=Generic_1", 1),
            ch("z:0", "zvucast-hall", 0),
            ch("z:1", "zvucast-hall", 1),
        ];
        let views = merge_inputs(&discovered, &[]);
        assert_eq!(capture_target(&views).as_deref(), Some("plughw:CARD=Generic_1"));
        assert!(views.iter().filter(|v| v.device_key == "zvucast-hall").all(|v| v.stream.is_none()));
        assert_eq!(playback_target(&views).as_deref(), Some("plughw:CARD=Generic_1"));
    }

    #[test]
    fn virtual_cable_becomes_the_slot_when_hardware_is_off() {
        let prefs = vec![
            InputPref { id: "hw:0".into(), name: "л".into(), enabled: false },
            InputPref { id: "hw:1".into(), name: "п".into(), enabled: false },
        ];
        let discovered = vec![
            ch("hw:0", "mic", 0),
            ch("hw:1", "mic", 1),
            ch("z:0", "zvucast-hall", 0),
            ch("z:1", "zvucast-hall", 1),
        ];
        let views = merge_inputs(&discovered, &prefs);
        assert_eq!(capture_target(&views).as_deref(), Some("zvucast-hall-monitor"));
        assert_eq!(views.iter().find(|v| v.id == "z:0").unwrap().stream.as_deref(), Some("l"));
        assert_eq!(playback_target(&views), None);
    }

    #[test]
    fn both_virtual_channels_off_mutes_that_zone_only() {
        let prefs = vec![
            InputPref { id: "z:0".into(), name: "л".into(), enabled: false },
            InputPref { id: "z:1".into(), name: "п".into(), enabled: false },
        ];
        let views = merge_inputs(&[
            ch("z:0", "zvucast-hall", 0),
            ch("z:1", "zvucast-hall", 1),
            ch("b:0", "zvucast-bed", 0),
        ], &prefs);
        assert!(muted_zone_ids(&views).contains("hall"));
        assert!(!muted_zone_ids(&views).contains("bed"));
    }

    #[test]
    fn pipewire_node_stays_the_capture_name() {
        let views = merge_inputs(&[
            ch("mic:0", "alsa_input.usb-BY-V20.analog-stereo", 0),
            ch("mic:1", "alsa_input.usb-BY-V20.analog-stereo", 1),
        ], &[]);
        assert_eq!(capture_target(&views).as_deref(), Some("alsa_input.usb-BY-V20.analog-stereo"));
    }

    #[test]
    fn playback_target_ignores_the_placeholder_device() {
        let placeholder = merge_inputs(&[
            ch("playback:0", "playback", 0),
            ch("playback:1", "playback", 1),
        ], &[]);
        assert!(placeholder.iter().all(|v| v.enabled));
        assert_eq!(playback_target(&placeholder), None);

        let real = merge_inputs(&[
            ch("out:0", "alsa_output.pci.analog-stereo", 0),
            ch("out:1", "alsa_output.pci.analog-stereo", 1),
        ], &[]);
        assert!(real.iter().all(|v| v.enabled));
        assert_eq!(
            playback_target(&real).as_deref(),
            Some("alsa_output.pci.analog-stereo"),
        );
    }

    #[test]
    fn saved_virtual_name_returns_when_the_device_comes_back() {
        let prefs = vec![InputPref { id: "zvucast:0".into(), name: "Браузер левый".into(), enabled: false }];
        let views = merge_inputs(&virtual_capture_channels(), &prefs);
        assert_eq!(views[0].name, "Браузер левый");
        assert!(!views[0].enabled);
        let gone = merge_inputs(&[], &prefs);
        assert!(gone.is_empty());
    }

    #[test]
    fn playback_key_resolves_card_name_to_node_name() {
        let dump = r#"[
      {"info":{"props":{
        "media.class":"Audio/Sink",
        "node.name":"alsa_output.platform-soc_sound.stereo-fallback",
        "alsa.card_name":"snd_rpi_hifiberry_dac"
      }}},
      {"info":{"props":{
        "media.class":"Audio/Sink",
        "node.name":"alsa_output.platform-fe00b840.mailbox.stereo-fallback",
        "alsa.card_name":"bcm2835 Headphones"
      }}},
      {"info":{"props":{
        "media.class":"Audio/Source",
        "node.name":"alsa_input.ignore",
        "alsa.card_name":"snd_rpi_hifiberry_dac"
      }}}
    ]"#;
        let sinks = sink_nodes_from_dump(dump);
        assert_eq!(sinks.len(), 2);
        assert_eq!(
            resolve_playback_key("snd_rpi_hifiberry_dac", "snd_rpi_hifiberry_dac", &sinks),
            "alsa_output.platform-soc_sound.stereo-fallback"
        );
        assert_eq!(
            resolve_playback_key("bcm2835 Headphones", "bcm2835 Headphones", &sinks),
            "alsa_output.platform-fe00b840.mailbox.stereo-fallback"
        );
        let node = "alsa_output.platform-soc_sound.stereo-fallback";
        assert_eq!(resolve_playback_key(node, "snd_rpi_hifiberry_dac", &sinks), node);
        assert_eq!(
            resolve_playback_key("нет такой карты", "нет такой карты", &sinks),
            "нет такой карты"
        );
        assert!(sink_nodes_from_dump("не json").is_empty());
    }

    #[test]
    fn playback_key_prefers_stereo_fallback_when_a_card_has_two_sinks() {
        let dump = r#"[
      {"info":{"props":{
        "media.class":"Audio/Sink",
        "node.name":"alsa_output.platform-soc_sound.pro-output-0",
        "alsa.card_name":"snd_rpi_hifiberry_dac"
      }}},
      {"info":{"props":{
        "media.class":"Audio/Sink",
        "node.name":"alsa_output.platform-soc_sound.stereo-fallback",
        "alsa.card_name":"snd_rpi_hifiberry_dac"
      }}}
    ]"#;
        let sinks = sink_nodes_from_dump(dump);
        assert_eq!(
            resolve_playback_key("snd_rpi_hifiberry_dac", "snd_rpi_hifiberry_dac", &sinks),
            "alsa_output.platform-soc_sound.stereo-fallback"
        );
    }

    #[test]
    fn every_enabled_output_is_in_the_stereo_map() {
        let prefs = vec![
            InputPref { id: "jack:0".into(), name: "jack л".into(), enabled: true },
            InputPref { id: "jack:1".into(), name: "jack п".into(), enabled: true },
            InputPref { id: "dac:0".into(), name: "цап л".into(), enabled: true },
            InputPref { id: "dac:1".into(), name: "цап п".into(), enabled: true },
        ];
        let views = merge_outputs(
            &[
                ch("jack:0", "alsa_output.platform-fe00b840.mailbox.stereo-fallback", 0),
                ch("jack:1", "alsa_output.platform-fe00b840.mailbox.stereo-fallback", 1),
                ch("dac:0", "alsa_output.platform-soc_sound.stereo-fallback", 0),
                ch("dac:1", "alsa_output.platform-soc_sound.stereo-fallback", 1),
                ch("z:0", "zvucast-hall", 0),
                ch("z:1", "zvucast-hall", 1),
            ],
            &prefs,
        );
        for id in ["jack:0", "dac:0"] {
            assert_eq!(views.iter().find(|v| v.id == id).unwrap().stream.as_deref(), Some("l"), "{id}");
        }
        for id in ["jack:1", "dac:1"] {
            assert_eq!(views.iter().find(|v| v.id == id).unwrap().stream.as_deref(), Some("r"), "{id}");
        }
        assert!(views.iter().filter(|v| v.device_key == "zvucast-hall").all(|v| v.stream.is_none()));
        assert_eq!(
            playback_targets(&views),
            vec![
                "alsa_output.platform-fe00b840.mailbox.stereo-fallback".to_string(),
                "alsa_output.platform-soc_sound.stereo-fallback".to_string(),
            ]
        );
        assert_eq!(
            playback_target(&views).as_deref(),
            Some("alsa_output.platform-fe00b840.mailbox.stereo-fallback")
        );
    }

    #[test]
    fn disabling_one_output_keeps_the_other_in_the_map() {
        use zproto::{ChanSel, ChannelMap};
        let prefs = vec![
            InputPref { id: "jack:0".into(), name: "jack л".into(), enabled: false },
            InputPref { id: "jack:1".into(), name: "jack п".into(), enabled: false },
            InputPref { id: "dac:0".into(), name: "цап л".into(), enabled: true },
            InputPref { id: "dac:1".into(), name: "цап п".into(), enabled: false },
        ];
        let views = merge_outputs(
            &[
                ch("jack:0", "jack-node", 0),
                ch("jack:1", "jack-node", 1),
                ch("dac:0", "dac-node", 0),
                ch("dac:1", "dac-node", 1),
            ],
            &prefs,
        );
        assert!(views.iter().filter(|v| v.device_key == "jack-node").all(|v| v.stream.is_none()));
        assert_eq!(views.iter().find(|v| v.id == "dac:0").unwrap().stream.as_deref(), Some("l"));
        assert!(views.iter().find(|v| v.id == "dac:1").unwrap().stream.is_none());
        assert_eq!(playback_targets(&views), vec!["dac-node".to_string()]);
        let masked = mask_disabled_outputs(
            ChannelMap { out_l: Some(ChanSel::L), out_r: Some(ChanSel::R) },
            &views,
        );
        assert_eq!(masked.out_l, Some(ChanSel::L));
        assert!(masked.out_r.is_none());
    }
}
