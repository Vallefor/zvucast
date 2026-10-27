//! Кубическая громкость узла PipeWire: среднее конечных неотрицательных
//! `channelVolumes`, затем кубический корень. Поле `volume` в Props не читается.

use anyhow::bail;

pub const MIN: f64 = 0.0;
pub const MAX: f64 = 1.5;

/// Конечное число в `MIN..=MAX`.
pub fn accepted(cubic: f64) -> bool {
    cubic.is_finite() && (MIN..=MAX).contains(&cubic)
}

#[derive(Clone)]
pub struct VolumeSample {
    pub capture: Option<f64>,
    pub playback: Option<f64>,
    pub devices: Vec<zproto::DeviceVolume>,
}

/// Имена воспроизведения для чтения громкости. Пустой выбор — запасной sink:
/// туда же `pw-play` играет, когда включённых выходов нет.
pub fn playback_sample_names(selected: &[String], fallback: Option<String>) -> Vec<String> {
    let selected: Vec<String> = selected.iter().filter(|name| !name.is_empty()).cloned().collect();
    if !selected.is_empty() {
        return selected;
    }
    fallback.into_iter().filter(|name| !name.is_empty()).collect()
}

/// Захват — по имени. Пустое имя и `None` дают `capture: None`.
/// Воспроизведение — кубическая громкость первого имени, чей узел есть в дампе.
pub fn from_dump(
    dump: &str,
    capture_name: Option<&str>,
    playback_names: &[String],
) -> VolumeSample {
    let items = parse_items(dump);
    let capture = capture_name
        .filter(|name| !name.is_empty())
        .and_then(|name| named_item(&items, name))
        .and_then(cubic_of);
    let mut playback = None;
    for name in playback_names {
        if let Some(item) = named_item(&items, name) {
            playback = cubic_of(item);
            break;
        }
    }
    VolumeSample { capture, playback, devices: Vec::new() }
}

/// Громкость каждого источника и выхода. Кроме `node.name` добавляет карту
/// и описание, если ими называется ровно один узел: в настройках ключ
/// устройства бывает любым из этих трёх.
pub fn device_volumes(dump: &str) -> Vec<zproto::DeviceVolume> {
    let mut heard = Vec::new();
    for item in parse_items(dump) {
        let Some(props) = item.pointer("/info/props") else { continue };
        let class = props.get("media.class").and_then(|v| v.as_str());
        if class != Some("Audio/Sink") && class != Some("Audio/Source") {
            continue;
        }
        let Some(name) = props.get("node.name").and_then(|v| v.as_str()).filter(|n| !n.is_empty()) else {
            continue;
        };
        let Some(volume) = cubic_of(&item) else { continue };
        let class = if class == Some("Audio/Source") { "source" } else { "sink" };
        heard.push(Heard {
            name: name.to_string(),
            card: text_prop(props, "alsa.card_name"),
            description: text_prop(props, "node.description"),
            class: class.to_string(),
            volume,
        });
    }
    let mut out = Vec::new();
    let mut used = std::collections::BTreeSet::new();
    for node in &heard {
        if used.insert(node.name.clone()) {
            out.push(zproto::DeviceVolume {
                device: node.name.clone(),
                volume: node.volume,
                card: node.card.clone(),
                class: node.class.clone(),
            });
        }
    }
    push_unique_alias(&heard, &mut out, &mut used, |node| &node.card);
    push_unique_alias(&heard, &mut out, &mut used, |node| &node.description);
    out
}

struct Heard {
    name: String,
    card: String,
    description: String,
    class: String,
    volume: f64,
}

fn text_prop(props: &serde_json::Value, key: &str) -> String {
    props.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn push_unique_alias(
    heard: &[Heard],
    out: &mut Vec<zproto::DeviceVolume>,
    used: &mut std::collections::BTreeSet<String>,
    label: impl Fn(&Heard) -> &str,
) {
    let mut labels: Vec<&str> = heard.iter().map(|node| label(node)).filter(|name| !name.is_empty()).collect();
    labels.sort_unstable();
    labels.dedup();
    for alias in labels {
        if used.contains(alias) {
            continue;
        }
        let mut hits = heard.iter().filter(|node| label(node) == alias);
        let Some(first) = hits.next() else { continue };
        if hits.next().is_some() {
            continue;
        }
        used.insert(alias.to_string());
        out.push(zproto::DeviceVolume {
            device: alias.to_string(),
            volume: first.volume,
            card: String::new(),
            class: String::new(),
        });
    }
}

/// `node.name`, либо единственный узел с такой картой или описанием.
pub fn node_name_for(dump: &str, key: &str) -> Option<String> {
    if key.is_empty() {
        return None;
    }
    if object_id(dump, key).is_some() {
        return Some(key.to_string());
    }
    let mut hits = Vec::new();
    for item in parse_items(dump) {
        let Some(props) = item.pointer("/info/props") else { continue };
        let class = props.get("media.class").and_then(|v| v.as_str());
        if class != Some("Audio/Sink") && class != Some("Audio/Source") {
            continue;
        }
        let Some(name) = props.get("node.name").and_then(|v| v.as_str()).filter(|n| !n.is_empty()) else {
            continue;
        };
        let card = props.get("alsa.card_name").and_then(|v| v.as_str());
        let description = props.get("node.description").and_then(|v| v.as_str());
        if card == Some(key) || description == Some(key) {
            hits.push(name.to_string());
        }
    }
    (hits.len() == 1).then(|| hits.remove(0))
}

pub fn object_id(dump: &str, node_name: &str) -> Option<u32> {
    let items = parse_items(dump);
    let id = named_item(&items, node_name)?.get("id")?.as_u64()?;
    u32::try_from(id).ok()
}

pub fn read_dump() -> Option<String> {
    let out = std::process::Command::new("timeout")
        .args(["1", "pw-dump"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?;
    text.starts_with('[').then_some(text)
}

pub fn set_cubic(node_name: &str, cubic: f64) -> anyhow::Result<()> {
    let dump = read_dump().ok_or_else(|| anyhow::anyhow!("pw-dump не дал дамп"))?;
    let Some(resolved) = node_name_for(&dump, node_name) else {
        bail!("узел {node_name} не найден");
    };
    let Some(id) = object_id(&dump, &resolved) else {
        bail!("узел {node_name} не найден");
    };
    let value = format!("{cubic:.4}");
    let status = std::process::Command::new("timeout")
        .args(["2", "wpctl", "set-volume", &id.to_string(), &value])
        .status()?;
    if status.success() {
        Ok(())
    } else {
        bail!("wpctl set-volume {id} завершился с {status}")
    }
}

fn parse_items(dump: &str) -> Vec<serde_json::Value> {
    serde_json::from_str(dump).unwrap_or_default()
}

fn named_item<'a>(items: &'a [serde_json::Value], name: &str) -> Option<&'a serde_json::Value> {
    if name.is_empty() {
        return None;
    }
    items.iter().find(|item| {
        item.pointer("/info/props/node.name")
            .and_then(|v| v.as_str())
            == Some(name)
    })
}

fn cubic_of(item: &serde_json::Value) -> Option<f64> {
    let props = item.pointer("/info/params/Props")?;
    let props = if props.is_array() {
        props.get(0)?
    } else {
        props
    };
    let volumes = props.get("channelVolumes")?.as_array()?;
    let mut sum = 0.0;
    let mut count = 0u32;
    for value in volumes {
        let Some(sample) = value.as_f64() else {
            continue;
        };
        if sample.is_finite() && sample >= 0.0 {
            sum += sample;
            count += 1;
        }
    }
    if count == 0 {
        None
    } else {
        Some((sum / f64::from(count)).cbrt())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dump() -> String {
        r#"[
      {"id":68,"type":"PipeWire:Interface:Node","info":{"props":{"node.name":"alsa_input.usb","media.class":"Audio/Source"},"params":{"Props":[{"volume":1.0,"channelVolumes":[1.0,1.0]}]}}},
      {"id":58,"type":"PipeWire:Interface:Node","info":{"props":{"node.name":"alsa_output.hifi","media.class":"Audio/Sink"},"params":{"Props":[{"volume":1.0,"channelVolumes":[0.064,0.064]}]}}},
      {"id":35,"type":"PipeWire:Interface:Node","info":{"props":{"node.name":"alsa_output.hdmi","media.class":"Audio/Sink"},"params":{"Props":[{"volume":0.5}]}}}
    ]"#
        .into()
    }

    #[test]
    fn cubic_comes_from_channel_volumes_not_the_volume_field() {
        let names = vec![
            "alsa_output.hifi".to_string(),
            "alsa_output.hdmi".to_string(),
        ];
        let sample = from_dump(&dump(), Some("alsa_input.usb"), &names);
        assert_eq!(sample.capture, Some(1.0));
        let playback = sample.playback.unwrap();
        assert!((playback - 0.4).abs() < 0.001, "{playback}");
        assert_eq!(object_id(&dump(), "alsa_output.hifi"), Some(58));
        assert_eq!(from_dump(&dump(), None, &[]).capture, None);
        assert_eq!(
            from_dump(&dump(), Some("alsa_output.hdmi"), &[]).capture,
            None
        );
        assert!(accepted(0.0) && accepted(1.5) && !accepted(1.5001) && !accepted(f64::NAN));
    }

    #[test]
    fn each_device_reports_its_name_and_a_unique_card() {
        let dump = r#"[
          {"info":{"props":{"node.name":"alsa_input.usb","media.class":"Audio/Source","alsa.card_name":"USB Audio CODEC","node.description":"PCM2902"},"params":{"Props":[{"channelVolumes":[1.0,1.0]}]}}},
          {"info":{"props":{"node.name":"alsa_output.hifi","media.class":"Audio/Sink","alsa.card_name":"snd_rpi_hifiberry_dac","node.description":"Built-in Audio Stereo"},"params":{"Props":[{"channelVolumes":[0.064,0.064]}]}}},
          {"info":{"props":{"node.name":"alsa_output.headphones","media.class":"Audio/Sink","alsa.card_name":"bcm2835 Headphones","node.description":"Built-in Audio Stereo"},"params":{"Props":[{"channelVolumes":[0.125,0.125]}]}}}
        ]"#;
        let vols = device_volumes(dump);
        let of = |name: &str| vols.iter().find(|v| v.device == name).map(|v| v.volume);
        assert_eq!(of("alsa_input.usb"), Some(1.0));
        assert_eq!(of("USB Audio CODEC"), Some(1.0));
        assert_eq!(of("PCM2902"), Some(1.0));
        let hifi = of("alsa_output.hifi").unwrap();
        assert!((hifi - 0.4).abs() < 0.001, "{hifi}");
        assert_eq!(of("snd_rpi_hifiberry_dac").unwrap(), hifi);
        assert!(of("Built-in Audio Stereo").is_none(), "общее описание не чьё-то одно");
        assert_eq!(node_name_for(dump, "snd_rpi_hifiberry_dac").as_deref(), Some("alsa_output.hifi"));
        assert_eq!(node_name_for(dump, "Built-in Audio Stereo"), None);
    }

    #[test]
    fn empty_playback_selection_samples_the_fallback_sink() {
        let fallback = Some("alsa_output.hifi".to_string());
        assert_eq!(
            playback_sample_names(&[], fallback.clone()),
            vec!["alsa_output.hifi".to_string()]
        );
        assert_eq!(
            playback_sample_names(&[String::new()], fallback.clone()),
            vec!["alsa_output.hifi".to_string()]
        );
        let selected = vec!["alsa_output.hdmi".to_string()];
        assert_eq!(playback_sample_names(&selected, fallback), selected);
        assert!(playback_sample_names(&[], None).is_empty());
    }
}
