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
    VolumeSample { capture, playback }
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
    let Some(id) = object_id(&dump, node_name) else {
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
}
