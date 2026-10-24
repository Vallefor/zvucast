//! Каналы захвата узла: что видит агент и что пользователь включил и назвал.
//! В стереопоток попадают только index 0 и 1 устройства, с которого реально
//! идёт захват. Остальные каналы видны в настройках, но в карту зоны не входят.

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

/// Узел PipeWire общей головы: устройство, которому `assign_stream` отдал
/// слот стерео. Виртуальные кабели зон сюда не входят. Строка ALSA
/// (`plughw:`) — не узел, её открывает alsasrc; на PipeWire такая строка
/// оставила бы голову тишиной, поэтому её не возвращаем.
pub fn capture_target(views: &[InputView]) -> Option<String> {
    let key = views
        .iter()
        .find(|v| v.stream.as_deref() == Some("l") && !is_virtual_device_key(&v.device_key))
        .or_else(|| {
            views
                .iter()
                .find(|v| v.stream.is_some() && !is_virtual_device_key(&v.device_key))
        })
        .map(|v| v.device_key.as_str())?;
    crate::pipeline::configured_pipewire_node(key).map(str::to_string)
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

/// Кабели переброса с виртуальных динамиков не настраиваются.
/// В списке входов их нет: ни по нашему ключу, ни по подписи «Zvucast: {зона}».
pub fn visible_inputs(views: Vec<InputView>) -> Vec<InputView> {
    views.into_iter().filter(|v| !hidden_from_settings(v)).collect()
}

fn hidden_from_settings(v: &InputView) -> bool {
    is_virtual_device_key(&v.device_key) || v.device_name == "Zvucast" || v.device_name.starts_with("Zvucast:")
}

/// Новый канал включён. Уже сохранённые имя и галочка не затираются.
/// Каналов, которых агент больше не видит, в списке нет.
pub fn merge_inputs(discovered: &[CaptureChan], prefs: &[InputPref], capture_device: Option<&str>) -> Vec<InputView> {
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
    assign_stream(&mut views, capture_device);
    views
}

fn card_token(capture_device: &str) -> Option<&str> {
    capture_device.split("CARD=").nth(1).filter(|s| !s.is_empty())
}

fn belongs_to_capture(ch: &InputView, capture_device: &str) -> bool {
    if ch.device_key == capture_device || capture_device.contains(&ch.device_key) || ch.device_key.contains(capture_device) {
        return true;
    }
    match card_token(capture_device) {
        Some(card) => ch.device_key.contains(card) || ch.device_name.contains(card),
        None => false,
    }
}

fn enabled_stereo(views: &[InputView], pool: &[usize]) -> bool {
    pool.iter().any(|&i| views[i].enabled && views[i].index <= 1)
}

/// Слот стерео — это аппаратный index 0/1 одного устройства, а не «первый
/// включённый канал». Выключенный левый не превращает правый в левый.
/// Виртуальные выходы зон в этот слот не входят: каждый слышен только своей
/// зоной, line-in остаётся общим. Слот у настроенного устройства, а если у
/// него стерео выключено — у первого по алфавиту, где канал 0 или 1 включён.
fn assign_stream(views: &mut [InputView], capture_device: Option<&str>) {
    let matched: Vec<usize> = views
        .iter()
        .enumerate()
        .filter(|(_, ch)| {
            !is_virtual_device_key(&ch.device_key) && capture_device.is_some_and(|dev| belongs_to_capture(ch, dev))
        })
        .map(|(i, _)| i)
        .collect();
    let pool: Vec<usize> = if enabled_stereo(views, &matched) {
        matched
    } else {
        let mut keys: Vec<&str> = views
            .iter()
            .filter(|ch| ch.enabled && ch.index <= 1 && !is_virtual_device_key(&ch.device_key))
            .map(|ch| ch.device_key.as_str())
            .collect();
        keys.sort();
        keys.dedup();
        match keys.first() {
            Some(key) => views
                .iter()
                .enumerate()
                .filter(|(_, ch)| ch.device_key == *key)
                .map(|(i, _)| i)
                .collect(),
            None => Vec::new(),
        }
    };
    for i in pool {
        if !views[i].enabled {
            continue;
        }
        views[i].stream = match views[i].index {
            0 => Some("l".into()),
            1 => Some("r".into()),
            _ => None,
        };
    }
}

/// Выключенный слот стерео в карту не попадает: этот выход молчит.
/// Пустой список видов — агент ещё не прислал выходы, карту не трогаем.
pub fn mask_disabled_outputs(map: zproto::ChannelMap, views: &[InputView]) -> zproto::ChannelMap {
    if views.is_empty() {
        return map;
    }
    let has = |slot: &str| views.iter().any(|v| v.enabled && v.stream.as_deref() == Some(slot));
    zproto::ChannelMap {
        out_l: if has("l") { map.out_l } else { None },
        out_r: if has("r") { map.out_r } else { None },
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
    discover_channels("Audio/Source")
}

/// Устройства воспроизведения. Пустой список — `fallback_outputs`.
pub fn discover_playback_channels() -> Vec<CaptureChan> {
    discover_channels("Audio/Sink")
}

fn discover_channels(kind: &str) -> Vec<CaptureChan> {
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
        let device_key = props
            .as_ref()
            .and_then(|p| {
                ["device.string", "node.name", "object.path"]
                    .into_iter()
                    .find_map(|field| p.get::<String>(field).ok())
            })
            .filter(|s| !s.is_empty())
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
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out.dedup_by(|a, b| a.id == b.id);
    out.sort_by(|a, b| a.device_name.cmp(&b.device_name).then(a.index.cmp(&b.index)));
    out
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
        let views = merge_inputs(&[ch("card:0", "card", 0), ch("card:1", "card", 1)], &[], Some("plughw:CARD=card"));
        assert!(views.iter().all(|v| v.enabled));
        assert_eq!(views[0].name, "Левый");
        assert_eq!(views[0].stream.as_deref(), Some("l"));
        assert_eq!(views[1].stream.as_deref(), Some("r"));
    }

    #[test]
    fn prefs_stick_and_disabling_left_does_not_promote_right() {
        let prefs = vec![InputPref { id: "card:0".into(), name: "ТВ: левый".into(), enabled: false }];
        let views = merge_inputs(&[ch("card:0", "card", 0), ch("card:1", "card", 1)], &prefs, Some("hw:CARD=card"));
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
        ], Some("plughw:CARD=card"));
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
            Some("plughw:CARD=card"),
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
        let views = merge_inputs(&[ch("card:0", "card", 0)], &prefs, None);
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

        let both_on = merge_inputs(&discovered, &[], Some("plughw:CARD=Generic_1"));
        assert_eq!(capture_target(&both_on), None);
        assert!(both_on.iter().filter(|v| is_virtual_device_key(&v.device_key)).all(|v| v.stream.is_none()));
        assert!(both_on.iter().any(|v| v.device_key == "plughw:CARD=Generic_1" && v.stream.as_deref() == Some("l")));

        let mic = [
            ch("mic:0", "alsa_input.usb-BY-V20.analog-stereo", 0),
            ch("mic:1", "alsa_input.usb-BY-V20.analog-stereo", 1),
        ];
        let aimed = merge_inputs(&mic, &[], Some("plughw:CARD=Generic_1"));
        assert_eq!(
            capture_target(&aimed).as_deref(),
            Some("alsa_input.usb-BY-V20.analog-stereo"),
            "слот стерео на узле PipeWire — это голова захвата, строка plughw её не открывает"
        );

        let prefs = vec![InputPref { id: "hw:0".into(), name: "Левый".into(), enabled: false }];
        let hw_off = merge_inputs(&discovered, &prefs, Some("plughw:CARD=Generic_1"));
        assert_eq!(capture_target(&hw_off), None);
        assert!(hw_off.iter().all(|v| v.stream.is_none()));
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
        let shown = visible_inputs(merge_inputs(&discovered, &[], Some("plughw:CARD=Generic_1")));
        assert_eq!(shown.len(), 1);
        assert_eq!(shown[0].device_name, "HD-Audio Generic");
        assert!(shown.iter().all(|v| !v.device_name.starts_with("Zvucast")));
    }

    #[test]
    fn saved_virtual_name_returns_when_the_device_comes_back() {
        let prefs = vec![InputPref { id: "zvucast:0".into(), name: "Браузер левый".into(), enabled: false }];
        let views = merge_inputs(&virtual_capture_channels(), &prefs, Some("other"));
        assert_eq!(views[0].name, "Браузер левый");
        assert!(!views[0].enabled);
        let gone = merge_inputs(&[], &prefs, Some("other"));
        assert!(gone.is_empty());
    }
}
