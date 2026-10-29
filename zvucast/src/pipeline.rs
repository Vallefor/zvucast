use gstreamer as gst;
use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::os::fd::OwnedFd;
use std::str::FromStr;

use anyhow::{Context, Result};
use gstreamer::prelude::*;
use zproto::proto::{ChanSel, ChannelMap};

/// Сокет, который собирает вызывающий. Пайплайн сам UDP не открывает:
/// `tx` — исходящий unicast, `rx` — приём на порту ветки.
pub trait LocalUdp {
    fn tx(&mut self) -> anyhow::Result<UdpSocket>;
    fn rx(&mut self, port: u16) -> anyhow::Result<UdpSocket>;
}

/// Тестовый и временный биндер агента: `127.0.0.1`, без `SO_BINDTODEVICE`.
/// `SO_REUSEADDR` нужен, потому что параллельные тесты садятся на один порт.
pub struct LoopbackUdp;

impl LocalUdp for LoopbackUdp {
    fn tx(&mut self) -> anyhow::Result<UdpSocket> {
        bind_loopback(0)
    }

    fn rx(&mut self, port: u16) -> anyhow::Result<UdpSocket> {
        bind_loopback(port)
    }
}

/// Боевой биндер: `udp_bound` с `SO_BINDTODEVICE`. `tx` — `(ipv4, 0)`, `rx` — `(ipv4, port)`.
pub struct IfaceUdp {
    pub name: String,
    pub ipv4: Ipv4Addr,
}

impl LocalUdp for IfaceUdp {
    fn tx(&mut self) -> anyhow::Result<UdpSocket> {
        crate::bindiface::udp_bound(&self.name, self.ipv4, 0).context("tx: udp_bound")
    }

    fn rx(&mut self, port: u16) -> anyhow::Result<UdpSocket> {
        crate::bindiface::udp_bound(&self.name, self.ipv4, port)
            .with_context(|| format!("rx: udp_bound :{port}"))
    }
}

fn bind_loopback(port: u16) -> anyhow::Result<UdpSocket> {
    let sock = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    sock.set_reuse_address(true)?;
    sock.bind(&SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into())?;
    sock.set_nonblocking(true)?;
    Ok(sock.into())
}

/// `OwnedFd` уходит в `gio::Socket::from_fd`. Ошибка — `Err`, fd закрываем сами:
/// GLib при неудаче владение не забирает. Элемент без сокета не создаём.
fn gio_from_udp(sock: UdpSocket) -> Result<gio::Socket> {
    use std::os::fd::{FromRawFd, IntoRawFd};
    let raw = OwnedFd::from(sock).into_raw_fd();
    // SAFETY: raw — живой UDP-сокет. Успех: GSocket владеет fd. Ошибка: закрываем ниже.
    let adopted = unsafe { OwnedFd::from_raw_fd(raw) };
    match unsafe { gio::Socket::from_fd(adopted) } {
        Ok(socket) => Ok(socket),
        Err(err) => {
            drop(unsafe { OwnedFd::from_raw_fd(raw) });
            Err(err).context("gio::Socket::from_fd")
        }
    }
}

const RX_CAPS: &str = "application/x-rtp,media=audio,encoding-name=L16,clock-rate=48000,channels=2,payload=96";
const MIX_CAPS: &str = "audio/x-raw,format=F32LE,rate=48000,channels=2";

/// m[in][out]: in L=0,R=1; out L=0,R=1. None → выход молчит.
pub fn channel_matrix(cm: &ChannelMap) -> [[f64; 2]; 2] {
    let mut m = [[0.0; 2]; 2];
    if let Some(sel) = cm.out_l { m[match sel { ChanSel::L => 0, ChanSel::R => 1 }][0] = 1.0; }
    if let Some(sel) = cm.out_r { m[match sel { ChanSel::L => 0, ChanSel::R => 1 }][1] = 1.0; }
    m
}

/// mix-matrix audioconvert — по строкам выходам ([out][in]), поэтому m[in][out]
/// транспонируем. Возвращает сериализованную матрицу для set_property_from_str.
/// Касты `(float)` обязательны: ячейки pspec — gfloat, а парсер строк без каста
/// сделал бы double и сеттер отверг бы значение (invalid or out of range).
fn matrix_value(m: [[f64; 2]; 2]) -> String {
    let t = |i: usize, o: usize| m[o][i];
    format!("<<(float){},(float){}>,<(float){},(float){}>>",
            t(0, 0), t(0, 1), t(1, 0), t(1, 1))
}

/// Матричный audioconvert (name="mconv"). mix-matrix у audioconvert есть с
/// GStreamer 1.20, но тип значения менялся: ≥1.26 — GstValueArray из gfloat
/// (ставим строкой, см. matrix_value), 1.20–1.24 — GValueArray из gfloat
/// (десериализатора строк для него нет — собираем ValueArray нативно).
/// Строки матрицы во всех версиях — выходы [out][in].
fn apply_mix_matrix(el: &gst::Element, cm: &ChannelMap) -> Result<()> {
    let pspec = el.find_property("mix-matrix")
        .ok_or_else(|| anyhow::anyhow!("audioconvert без mix-matrix — нужен GStreamer ≥ 1.20"))?;
    if pspec.value_type() == gst::Array::static_type() {
        el.set_property_from_str("mix-matrix", &matrix_value(channel_matrix(cm)));
    } else {
        // 1.20–1.24: GValueArray из GValueArray из gfloat
        let m = channel_matrix(cm);
        let row = |a: f64, b: f64| {
            gst::glib::ValueArray::from_values([
                gst::glib::Value::from(a as f32),
                gst::glib::Value::from(b as f32),
            ])
            .to_value()
        };
        let arr = gst::glib::ValueArray::from_values([row(m[0][0], m[1][0]), row(m[0][1], m[1][1])]);
        el.set_property("mix-matrix", arr);
    }
    Ok(())
}

fn build_mconv(cm: &ChannelMap) -> Result<gst::Element> {
    let e = gst::ElementFactory::make("audioconvert").property("name", "mconv").build()?;
    apply_mix_matrix(&e, cm)?;
    Ok(e)
}

/// Голова rx: ветки портов (или одна ветка тишины) → audiomixer →
/// audioconvert (mix-matrix из cm) → audioconvert → audioresample(quality=4) →
/// level(name="lev"). Пустой список держит sink тишиной, чтобы он не ловил
/// underrun. `branches` — карта на порт до микшера; тогда mconv сквозной,
/// чтобы не применить матрицу второй раз. Возвращает пайплайн и lev.
fn rx_head(
    cfg: &crate::config::PipelineCfg,
    cm: &ChannelMap,
    ports: &[u16],
    branches: &[(u16, ChannelMap)],
    bind: &mut dyn LocalUdp,
) -> Result<(gst::Pipeline, gst::Element)> {
    let pipe = gst::Pipeline::new();
    let mix = gst::ElementFactory::make("audiomixer")
        .property("name", "mix")
        .property("ignore-inactive-pads", true)
        .property("latency", gst::ClockTime::from_mseconds(2))
        .property_from_str("start-time-selection", "first")
        .build()?;
    let identity = ChannelMap::default();
    let tail = if branches.is_empty() { cm } else { &identity };
    let mconv = build_mconv(tail)?;
    let conv = gst::ElementFactory::make("audioconvert").build()?;
    let res = gst::ElementFactory::make("audioresample").property("quality", 4i32).build()?;
    let lev = gst::ElementFactory::make("level").property("name", "lev").build()?;
    pipe.add_many([&mix, &mconv, &conv, &res, &lev])?;
    gst::Element::link_many([&mix, &mconv, &conv, &res, &lev])?;
    if ports.is_empty() {
        add_silence_branch(&pipe, &mix)?;
    } else {
        let volume = 1.0 / ports.len() as f64;
        for &port in ports {
            let branch = branches.iter().find(|(p, _)| *p == port).map(|(_, map)| map);
            add_rtp_branch(&pipe, &mix, port, cfg.jb_ms, volume, branch, bind)?;
        }
    }
    Ok((pipe, lev))
}

pub fn build_rx(
    cfg: &crate::config::PipelineCfg,
    device: &str,
    cm: &ChannelMap,
    bind: &mut dyn LocalUdp,
) -> Result<gst::Pipeline> {
    build_rx_ports(cfg, device, cm, &[cfg.port], &[], bind)
}

pub fn build_rx_ports(
    cfg: &crate::config::PipelineCfg,
    device: &str,
    cm: &ChannelMap,
    ports: &[u16],
    branches: &[(u16, ChannelMap)],
    bind: &mut dyn LocalUdp,
) -> Result<gst::Pipeline> {
    let (pipe, lev) = rx_head(cfg, cm, ports, branches, bind)?;
    let sink = gst::ElementFactory::make("alsasink")
        .property("name", "asink")
        .property("device", device)
        .property("buffer-time", cfg.rx_buffer_us as i64)
        .property("latency-time", cfg.rx_latency_us as i64)
        .property_from_str("slave-method", "skew")
        .build()?;
    pipe.add(&sink)?;
    gst::Element::link_many([&lev, &sink])?;
    Ok(pipe)
}

/// Имя, которое pw-record открывает.
/// `zvucast` → `zvucast-monitor`. `zvucast-{id}` → `zvucast-{id}-monitor`.
/// Имя, которое уже кончается на `-monitor`, второй раз не дописываем.
pub(crate) fn capture_node_name(device_key: &str) -> String {
    crate::inputs::open_capture_name(device_key)
}

/// Имя узла PipeWire из конфига. Строка ALSA (`plughw:`, `hw:`) — не узел:
/// на таком бэкенде её не открываем, общая голова остаётся тишиной, а
/// воспроизведение идёт в sink по умолчанию.
pub(crate) fn configured_pipewire_node(device: &str) -> Option<&str> {
    let alsa = device.is_empty()
        || device == "default"
        || device == "pipewire"
        || device.starts_with("plughw:")
        || device.starts_with("hw:")
        || device.starts_with("sysdefault:")
        || device.starts_with("front:")
        || device.starts_with("dmix:")
        || device.starts_with("dsnoop:");
    if alsa { None } else { Some(device) }
}

/// Колонки или любой другой sink, только не zvucast. Цель читается в момент
/// запуска pw-play, а не при сборке пайплайна.
pub(crate) fn desktop_playback_target() -> Option<String> {
    let from_meta = std::process::Command::new("pw-metadata")
        .args(["-n", "default"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| playback_target_from_metadata(&text));
    from_meta.or_else(other_playback_sink)
}

/// Колонки или любой другой sink, только не zvucast.
fn other_playback_sink() -> Option<String> {
    let out = std::process::Command::new("timeout").args(["2", "pw-dump"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let items: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).ok()?;
    let mut speaker = None;
    let mut any = None;
    for item in items {
        let Some(props) = item.pointer("/info/props") else { continue };
        if props.get("media.class").and_then(|v| v.as_str()) != Some("Audio/Sink") {
            continue;
        }
        let Some(name) = props.get("node.name").and_then(|v| v.as_str()) else { continue };
        if crate::inputs::is_virtual_sink_name(name) {
            continue;
        }
        if speaker.is_none() && name.contains("Speaker") {
            speaker = Some(name.to_string());
        }
        if any.is_none() {
            any = Some(name.to_string());
        }
    }
    speaker.or(any)
}

pub(crate) fn playback_target_from_metadata(text: &str) -> Option<String> {
    let current = metadata_sink_name(text, "default.audio.sink");
    let configured = metadata_sink_name(text, "default.configured.audio.sink");
    let pick = match current.as_deref() {
        Some(name) if crate::inputs::is_virtual_sink_name(name) => configured,
        None => configured,
        Some(_) => current,
    };
    pick.filter(|name| !crate::inputs::is_virtual_sink_name(name) && !name.is_empty())
}

fn metadata_sink_name(text: &str, key: &str) -> Option<String> {
    let marker = format!("key:'{key}'");
    for line in text.lines() {
        let Some((_, rest)) = line.split_once(&marker) else {
            continue;
        };
        let Some(value) = rest.split("value:'").nth(1).and_then(|v| v.split('\'').next()) else {
            continue;
        };
        let Some(name) = value.split("\"name\":").nth(1) else {
            continue;
        };
        let name = name.trim_start_matches([' ', '"']);
        let name = name.split(['"', '}']).next().unwrap_or("").trim();
        if !name.is_empty() {
            return Some(name.to_string());
        }
    }
    None
}

/// rx через PipeWire: та же голова, но без pipewiresink.
///
/// pipewiresink в PAUSED вызывает pw_stream_set_active(false), уже создав
/// active-ссылки. Пока RTP нет, узел так и остаётся suspended, и часы выхода
/// не тикают. Сэмплы забирает appsink, наружу их пишет отдельный pw-play,
/// и только пока они есть.
pub fn build_rx_pw(
    cfg: &crate::config::PipelineCfg,
    cm: &ChannelMap,
    bind: &mut dyn LocalUdp,
) -> Result<gst::Pipeline> {
    build_rx_pw_ports(cfg, cm, &[cfg.port], &[], bind)
}

pub fn build_rx_pw_ports(
    cfg: &crate::config::PipelineCfg,
    cm: &ChannelMap,
    ports: &[u16],
    branches: &[(u16, ChannelMap)],
    bind: &mut dyn LocalUdp,
) -> Result<gst::Pipeline> {
    let (pipe, lev) = rx_head(cfg, cm, ports, branches, bind)?;
    let caps = gst::Caps::from_str(
        "audio/x-raw,format=S16LE,rate=48000,channels=2,layout=interleaved",
    )?;
    let sink = gst::ElementFactory::make("appsink")
        .property("name", "asink")
        .property("caps", &caps)
        .property("sync", false)
        .property("async", false)
        .property("enable-last-sample", false)
        .property("drop", true)
        .property("max-buffers", 8u32)
        .build()?;
    pipe.add(&sink)?;
    gst::Element::link_many([&lev, &sink])?;
    Ok(pipe)
}


/// Приводит уже собранный пайплайн к новому списку портов. `mix`, `mconv` и
/// `asink` не трогает: пересоздание sink замолкает колонку. Нет `mix` — ошибка,
/// вызывающий пересоберёт пайплайн целиком.
pub fn reconcile_mix(
    pipeline: &gst::Pipeline,
    ports: &[u16],
    jb_ms: u32,
    branches: &[(u16, ChannelMap)],
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let mixer = pipeline.by_name("mix").context("нет элемента mix")?;
    let existing = queue_ports(pipeline);
    if ports.is_empty() {
        // Тишина — нули, уровня не добавляет. Сначала она, потом снятие RTP:
        // иначе короткий буфер выхода ловит underrun без единого входа.
        if pipeline.by_name("uqsilence").is_none() {
            add_silence_branch(pipeline, &mixer)?;
        }
        if let Some(queue) = pipeline.by_name("uqsilence") {
            set_queue_volume(&queue, 1.0);
        }
        for port in existing {
            remove_queue_branch(pipeline, &mixer, &format!("uq{port}"))?;
        }
    } else {
        // Пока уходящая ветка ещё в микшере, сумма не выше 1: все живые
        // входы (и новые) стоят на 1/|объединение|. До 1/|новый список|
        // выжившие поднимаются только после отключения уходящих.
        let mut union_ports = existing.clone();
        for &port in ports {
            if !union_ports.contains(&port) {
                union_ports.push(port);
            }
        }
        let overlap = 1.0 / union_ports.len() as f64;
        let mut new_ports = Vec::new();
        for &port in ports {
            if !new_ports.contains(&port) {
                new_ports.push(port);
            }
        }
        let final_volume = 1.0 / new_ports.len() as f64;
        // Снятие uqsilence у уже играющего audiomixer (GStreamer 1.26)
        // останавливает src-задачу микшера, и она не стартует снова:
        // appsink остаётся на одном сэмпле, RTP дальше не слышен.
        // Ошибка заставляет агента собрать приём заново, уже с портами.
        if pipeline.by_name("uqsilence").is_some() {
            anyhow::bail!("ветка тишины уже в микшере — нужна пересборка приёма");
        }
        for &port in &existing {
            if let Some(queue) = pipeline.by_name(&format!("uq{port}")) {
                set_queue_volume(&queue, overlap);
            }
        }
        for &port in &new_ports {
            if pipeline.by_name(&format!("uq{port}")).is_none() {
                let branch = branches.iter().find(|(p, _)| *p == port).map(|(_, map)| map);
                add_rtp_branch(pipeline, &mixer, port, jb_ms, overlap, branch, bind)?;
            }
        }
        for port in existing {
            if !new_ports.contains(&port) {
                remove_queue_branch(pipeline, &mixer, &format!("uq{port}"))?;
            }
        }
        for &port in &new_ports {
            if let Some(queue) = pipeline.by_name(&format!("uq{port}")) {
                set_queue_volume(&queue, final_volume);
            }
        }
        // Новые ветки уже собраны с jb_ms. Оставшиеся тоже получают этот запас,
        // если он сменился вместе с портами.
        set_jitter_latency(pipeline, &new_ports, jb_ms);
    }
    Ok(())
}

/// Запас уже стоящих `rtpjitterbuffer` (`jb{port}`). Нет элемента — порт пропускаем.
pub(crate) fn set_jitter_latency(pipeline: &gst::Pipeline, ports: &[u16], jb_ms: u32) {
    for &port in ports {
        if let Some(jb) = pipeline.by_name(&format!("jb{port}")) {
            jb.set_property("latency", jb_ms);
        }
    }
}

fn add_rtp_branch(
    pipe: &gst::Pipeline,
    mixer: &gst::Element,
    port: u16,
    jb_ms: u32,
    volume: f64,
    map: Option<&ChannelMap>,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    // Сокет сначала. Ошибка rx/from_fd — ветки нет, udpsrc свой сокет не открывает.
    let sock = bind.rx(port).with_context(|| format!("udpsrc :{port}"))?;
    let gio_socket = gio_from_udp(sock)?;
    let src = gst::ElementFactory::make("udpsrc")
        .property("name", format!("usrc{port}"))
        .property("address", "0.0.0.0")
        .property("port", port as i32)
        .property("auto-multicast", false)
        .property("buffer-size", 2_097_152i32)
        .property("caps", gst::Caps::from_str(RX_CAPS)?)
        .property("socket", &gio_socket)
        .build()?;
    let jb = gst::ElementFactory::make("rtpjitterbuffer")
        .property("name", format!("jb{port}"))
        .property("latency", jb_ms)
        .property("do-lost", true)
        .build()?;
    let depay = gst::ElementFactory::make("rtpL16depay")
        .property("name", format!("depay{port}"))
        .build()?;
    crate::conceal_element::register();
    let conceal = gst::ElementFactory::make("conceal")
        .property("name", format!("conceal{port}"))
        .build()?;
    let conv = gst::ElementFactory::make("audioconvert")
        .property("name", format!("conv{port}"))
        .build()?;
    if let Some(map) = map {
        apply_mix_matrix(&conv, map)?;
    }
    let res = gst::ElementFactory::make("audioresample")
        .property("name", format!("res{port}"))
        .property("quality", 4i32)
        .build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property("name", format!("caps{port}"))
        .property("caps", gst::Caps::from_str(MIX_CAPS)?)
        .build()?;
    let queue = gst::ElementFactory::make("queue")
        .property("name", format!("uq{port}"))
        .build()?;
    let chain = [&src, &jb, &depay, &conceal, &conv, &res, &caps, &queue];
    pipe.add_many(chain)?;
    gst::Element::link_many(chain)?;
    link_queue_to_mixer(&queue, mixer, volume)?;
    for el in chain.iter().rev() {
        el.sync_state_with_parent()?;
    }
    Ok(())
}

fn add_silence_branch(pipe: &gst::Pipeline, mixer: &gst::Element) -> Result<()> {
    let src = gst::ElementFactory::make("audiotestsrc")
        .property("name", "silence")
        .property("is-live", true)
        .property_from_str("wave", "silence")
        .build()?;
    let conv = gst::ElementFactory::make("audioconvert")
        .property("name", "sconv")
        .build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property("name", "scaps")
        .property("caps", gst::Caps::from_str(MIX_CAPS)?)
        .build()?;
    let queue = gst::ElementFactory::make("queue")
        .property("name", "uqsilence")
        .build()?;
    let chain = [&src, &conv, &caps, &queue];
    pipe.add_many(chain)?;
    gst::Element::link_many(chain)?;
    link_queue_to_mixer(&queue, mixer, 1.0)?;
    for el in chain.iter().rev() {
        el.sync_state_with_parent()?;
    }
    Ok(())
}

fn link_queue_to_mixer(queue: &gst::Element, mixer: &gst::Element, volume: f64) -> Result<()> {
    let pad = mixer
        .request_pad_simple("sink_%u")
        .context("audiomixer не отдал sink_%u")?;
    let src = queue.static_pad("src").context("нет src у queue")?;
    src.link(&pad)?;
    set_queue_volume(queue, volume);
    Ok(())
}

fn set_queue_volume(queue: &gst::Element, volume: f64) {
    if let Some(peer) = queue.static_pad("src").and_then(|p| p.peer()) {
        peer.set_property("volume", volume);
    }
}

fn remove_queue_branch(pipe: &gst::Pipeline, mixer: &gst::Element, queue_name: &str) -> Result<()> {
    let queue = pipe
        .by_name(queue_name)
        .with_context(|| format!("нет {queue_name}"))?;
    let mut chain = Vec::new();
    let mut current = queue.clone();
    loop {
        let upstream = current
            .static_pad("sink")
            .and_then(|pad| pad.peer())
            .and_then(|pad| pad.parent_element());
        chain.push(current);
        match upstream {
            Some(el) => current = el,
            None => break,
        }
    }
    if let Some(src) = queue.static_pad("src") {
        if let Some(peer) = src.peer() {
            src.unlink(&peer)?;
            mixer.release_request_pad(&peer);
        }
    }
    for el in &chain {
        el.set_state(gst::State::Null)?;
        pipe.remove(el)?;
    }
    Ok(())
}

fn queue_ports(pipeline: &gst::Pipeline) -> Vec<u16> {
    let mut ports = Vec::new();
    let children = pipeline.children();
    for child in children.iter() {
        let name = child.name();
        if let Some(rest) = name.strip_prefix("uq") {
            if let Ok(port) = rest.parse::<u16>() {
                ports.push(port);
            }
        }
    }
    ports
}

/// Legacy multicast-tx: та же голова + udpsink на mcast-группу
/// (auto-multicast). В проде не используется (пустой набор = тишина,
/// см. build_tx_unicast) — оставлен для тестов головы tx (level, pt, mtu).
pub fn build_tx(cfg: &crate::config::PipelineCfg, device: &str) -> Result<gst::Pipeline> {
    let (pipe, pay) = make_tx_head(cfg, device)?;
    let sink = gst::ElementFactory::make("udpsink")
        .property("host", &cfg.mcast_group)
        .property("port", cfg.port as i32)
        .property("auto-multicast", true)
        .property("ttl", 2i32)
        .build()?;
    pipe.add(&sink)?;
    gst::Element::link_many([&pay, &sink])?;
    Ok(pipe)
}

/// Unicast-tx: та же голова, что у build_tx, но rtpL16pay → tee →
/// на каждый endpoint своя ветка queue→udpsink(host, port, qos-dscp).
/// Пустой набор = в сеть ничего не уходит (не multicast). Вместо udpsink
/// стоит fakesink: иначе у tee нет ветки, alsasrc не течёт и level
/// не шлёт RMS — холд тишины не снимется, когда звук вернётся.
pub fn build_tx_unicast(
    cfg: &crate::config::PipelineCfg,
    device: &str,
    zones: &[zproto::ZoneEndpoints],
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<gst::Pipeline> {
    let (pipe, from) = make_tx_capture(cfg, device)?;
    attach_zone_tails(&pipe, &from, zones, dscp, bind)?;
    Ok(pipe)
}

/// Tx через PipeWire: тот же unicast-хвост, что у build_tx_unicast, но источник
/// pipewiresrc. На десктопе PipeWire держит ALSA эксклюзивно, и alsasrc от
/// root падает сразу после Start.
pub fn build_tx_unicast_pw(
    _cfg: &crate::config::PipelineCfg,
    zones: &[zproto::ZoneEndpoints],
    dscp: u8,
    target: Option<&str>,
    bind: &mut dyn LocalUdp,
) -> Result<gst::Pipeline> {
    let pipe = gst::Pipeline::new();
    // Сэмплы пишет отдельный pw-record в appsrc (name=cap). pipewiresrc здесь
    // нельзя: он делит цикл с pipewiresink и зависает в set_state, ERR растёт
    // каждый квант. Пока слота нет — тишина, источник по умолчанию не открываем.
    let src = if target.is_some() {
        live_appsrc("cap")?
    } else {
        gst::ElementFactory::make("audiotestsrc")
            .property("is-live", true)
            .property_from_str("wave", "silence")
            .build()?
    };
    // Сеть не должна останавливать чтение кабеля.
    let hold = gst::ElementFactory::make("queue")
        .property_from_str("leaky", "downstream")
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()?;
    let conv = gst::ElementFactory::make("audioconvert").build()?;
    let res = gst::ElementFactory::make("audioresample").property("quality", 4i32).build()?;
    // level понимает только native-endian. S16BE — уже после него.
    let caps_rate = gst::ElementFactory::make("capsfilter")
        .property("caps", gst::Caps::from_str("audio/x-raw,format=F32LE,rate=48000,channels=2")?)
        .build()?;
    let lev = gst::ElementFactory::make("level").property("name", "lev").build()?;
    let chain = [&src, &hold, &conv, &res, &caps_rate, &lev];
    pipe.add_many(chain)?;
    gst::Element::link_many(chain)?;
    // Хвост зоны — только голова захвата. Вид идёт своим хвостом, не в этот микшер.
    attach_pw_zone_tails(&pipe, &lev, zones, dscp, bind)?;
    Ok(pipe)
}

fn live_appsrc(name: &str) -> Result<gst::Element> {
    gst::ElementFactory::make("appsrc")
        .property("name", name)
        .property("is-live", true)
        .property("format", gst::Format::Time)
        .property("do-timestamp", true)
        .property("block", false)
        .property("max-buffers", 8u64)
        .property(
            "caps",
            gst::Caps::from_str("audio/x-raw,format=S16LE,rate=48000,channels=2,layout=interleaved")?,
        )
        .build()
        .map_err(Into::into)
}

pub(crate) fn capture_appsrc_name(zone_id: &str) -> String {
    format!("cap-{}", element_token(zone_id))
}

fn element_token(zone_id: &str) -> String {
    zone_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' })
        .collect()
}

/// Каждая зона PipeWire: line-in → микшер → один `rtpL16pay`.
/// Монитор вида в этот микшер не входит.
fn attach_pw_zone_tails(
    pipe: &gst::Pipeline,
    from: &gst::Element,
    zones: &[zproto::ZoneEndpoints],
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let tee = gst::ElementFactory::make("tee").property("name", "utee").build()?;
    pipe.add(&tee)?;
    from.link(&tee)?;
    if zones.is_empty() {
        let queue = gst::ElementFactory::make("queue").build()?;
        let sink = gst::ElementFactory::make("fakesink")
            .property("name", "usilence")
            .property("sync", false)
            .property("async", false)
            .build()?;
        pipe.add_many([&queue, &sink])?;
        tee.link(&queue)?;
        queue.link(&sink)?;
        return Ok(());
    }
    for zone in zones {
        attach_pw_zone(pipe, &tee, zone, dscp, bind)?;
    }
    Ok(())
}

fn attach_pw_zone(
    pipe: &gst::Pipeline,
    tee: &gst::Element,
    zone: &zproto::ZoneEndpoints,
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let token = element_token(&zone.zone_id);
    let mix = gst::ElementFactory::make("audiomixer")
        .property("name", format!("zmix-{token}"))
        .property("ignore-inactive-pads", true)
        .property("latency", gst::ClockTime::from_mseconds(2))
        .property_from_str("start-time-selection", "first")
        .build()?;
    let lin = gst::ElementFactory::make("queue")
        .property("name", format!("lin-{token}"))
        .build()?;
    pipe.add_many([&mix, &lin])?;
    tee.link(&lin)?;
    link_queue_to_mixer(&lin, &mix, 1.0)?;

    let pay_conv = gst::ElementFactory::make("audioconvert").build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::from_str("audio/x-raw,format=S16BE,rate=48000,channels=2")?,
        )
        .build()?;
    let pay = gst::ElementFactory::make("rtpL16pay")
        .property("name", format!("pay-{token}"))
        .property("pt", 96u32)
        .property("mtu", 1400u32)
        .build()?;
    pipe.add_many([&pay_conv, &caps, &pay])?;
    gst::Element::link_many([&mix, &pay_conv, &caps, &pay])?;
    attach_pay_tail(pipe, &pay, &zone.endpoints, dscp, bind)?;
    Ok(())
}

/// Два буфера и сброс вниз: монитор не копит очередь дольше кванта.
fn monitor_queue(name: String) -> Result<gst::Element> {
    Ok(gst::ElementFactory::make("queue")
        .property("name", name)
        .property_from_str("leaky", "downstream")
        .property("max-size-buffers", 2u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()?)
}

fn remove_monitor_branch(pipe: &gst::Pipeline, mix: &gst::Element, token: &str) -> Result<()> {
    let mq_name = format!("mq-{token}");
    if let Some(queue) = pipe.by_name(&mq_name) {
        if let Some(src) = queue.static_pad("src") {
            if let Some(peer) = src.peer() {
                src.unlink(&peer)?;
                mix.release_request_pad(&peer);
            }
        }
    }
    for name in [
        format!("mlsink-{token}"),
        format!("lev-{token}"),
        format!("mlq-{token}"),
        format!("mq-{token}"),
        format!("vtee-{token}"),
        format!("mcaps-{token}"),
        format!("mres-{token}"),
        format!("mconv-{token}"),
        format!("mhold-{token}"),
        format!("cap-{token}"),
    ] {
        let Some(el) = pipe.by_name(&name) else { continue };
        for pad in el.pads() {
            let Some(peer) = pad.peer() else { continue };
            if pad.direction() == gst::PadDirection::Src {
                let _ = pad.unlink(&peer);
            } else {
                let _ = peer.unlink(&pad);
            }
        }
        el.set_state(gst::State::Null)?;
        pipe.remove(&el)?;
    }
    Ok(())
}

/// Снимает ветку монитора из микшера микрофона, если она ещё стоит.
/// Новую не добавляет: вид играет своим хвостом. Другой набор зон —
/// ошибка, это уже смена маршрута.
pub fn reconcile_pw_monitors(pipe: &gst::Pipeline, zones: &[zproto::ZoneEndpoints]) -> Result<()> {
    let mut have = std::collections::BTreeSet::new();
    for child in pipe.children() {
        if let Some(token) = child.name().strip_prefix("zmix-") {
            have.insert(token.to_string());
        }
    }
    let want: std::collections::BTreeSet<String> = zones.iter().map(|z| element_token(&z.zone_id)).collect();
    if have != want {
        anyhow::bail!("набор зон микса не совпал — нужна пересборка");
    }
    for zone in zones {
        let token = element_token(&zone.zone_id);
        let mix = pipe
            .by_name(&format!("zmix-{token}"))
            .with_context(|| format!("нет микшера зоны {token}"))?;
        if pipe.by_name(&format!("cap-{token}")).is_some() {
            remove_monitor_branch(pipe, &mix, &token)?;
            if let Some(lin) = pipe.by_name(&format!("lin-{token}")) {
                set_queue_volume(&lin, 1.0);
            }
        }
    }
    Ok(())
}

pub(crate) fn feed_appsrc_name(kind: &str, zone_id: &str) -> String {
    format!("feed-{kind}-{}", element_token(zone_id))
}

pub(crate) fn feed_level_name(kind: &str, zone_id: &str) -> String {
    format!("lev-feed-{kind}-{}", element_token(zone_id))
}

fn feed_tag(kind: &str, zone_id: &str) -> String {
    format!("{kind}-{}", element_token(zone_id))
}

/// Хвост каждого вида: appsrc монитора, свой level, свой rtpL16pay, свои udpsink.
/// Голова захвата в этот хвост не входит. Пустой список снимает хвосты.
/// Повторный вызов собирает набор заново.
pub fn reconcile_feed_tails(
    pipe: &gst::Pipeline,
    feeds: &[zproto::FeedSend],
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    for name in feed_root_names(pipe) {
        remove_downstream(pipe, &name)?;
    }
    let mut seen = BTreeSet::new();
    for feed in feeds {
        let name = feed_appsrc_name(feed.kind.as_str(), &feed.zone_id);
        if !seen.insert(name) {
            continue;
        }
        attach_feed_tail(pipe, feed, dscp, bind)?;
    }
    Ok(())
}

fn feed_root_names(pipe: &gst::Pipeline) -> Vec<String> {
    let mut names = Vec::new();
    for child in pipe.children() {
        let name = child.name().to_string();
        let appsrc = child
            .factory()
            .is_some_and(|factory| factory.name() == "appsrc");
        if appsrc && name.starts_with("feed-") {
            names.push(name);
        }
    }
    names
}

fn attach_feed_tail(
    pipe: &gst::Pipeline,
    feed: &zproto::FeedSend,
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let kind = feed.kind.as_str();
    let tag = feed_tag(kind, &feed.zone_id);
    let src = live_appsrc(&feed_appsrc_name(kind, &feed.zone_id))?;
    let hold = monitor_queue(format!("fhold-{tag}"))?;
    let conv = gst::ElementFactory::make("audioconvert")
        .property("name", format!("fconv-{tag}"))
        .build()?;
    let res = gst::ElementFactory::make("audioresample")
        .property("name", format!("fres-{tag}"))
        .property("quality", 4i32)
        .build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property("name", format!("fcaps-{tag}"))
        .property(
            "caps",
            gst::Caps::from_str("audio/x-raw,format=F32LE,rate=48000,channels=2")?,
        )
        .build()?;
    let tee = gst::ElementFactory::make("tee")
        .property("name", format!("fmtee-{tag}"))
        .build()?;
    let chain = [&src, &hold, &conv, &res, &caps, &tee];
    pipe.add_many(chain)?;
    gst::Element::link_many(chain)?;

    // Level в стороне: сеть не должна глушить замер, и замер не должен держать RTP.
    let mlq = monitor_queue(format!("fmlq-{tag}"))?;
    let lev = gst::ElementFactory::make("level")
        .property("name", feed_level_name(kind, &feed.zone_id))
        .build()?;
    let meter_sink = gst::ElementFactory::make("fakesink")
        .property("name", format!("fmsink-{tag}"))
        .property("sync", false)
        .property("async", false)
        .build()?;
    pipe.add_many([&mlq, &lev, &meter_sink])?;
    tee.link(&mlq)?;
    gst::Element::link_many([&mlq, &lev, &meter_sink])?;

    let pay_conv = gst::ElementFactory::make("audioconvert")
        .property("name", format!("fpayconv-{tag}"))
        .build()?;
    let pay_caps = gst::ElementFactory::make("capsfilter")
        .property("name", format!("fpaycaps-{tag}"))
        .property(
            "caps",
            gst::Caps::from_str("audio/x-raw,format=S16BE,rate=48000,channels=2")?,
        )
        .build()?;
    let pay = gst::ElementFactory::make("rtpL16pay")
        .property("name", format!("fpay-{tag}"))
        .property("pt", 96u32)
        .property("mtu", 1400u32)
        .build()?;
    pipe.add_many([&pay_conv, &pay_caps, &pay])?;
    tee.link(&pay_conv)?;
    gst::Element::link_many([&pay_conv, &pay_caps, &pay])?;
    attach_named_pay_tail(pipe, &pay, &feed.endpoints, dscp, bind, &tag)?;
    sync_added_tail(pipe, &src)?;
    Ok(())
}

fn attach_named_pay_tail(
    pipe: &gst::Pipeline,
    pay: &gst::Element,
    endpoints: &[zproto::Endpoint],
    dscp: u8,
    bind: &mut dyn LocalUdp,
    tag: &str,
) -> Result<()> {
    if endpoints.is_empty() {
        let sink = gst::ElementFactory::make("fakesink")
            .property("name", format!("fsink-{tag}"))
            .property("sync", false)
            .property("async", false)
            .build()?;
        pipe.add(&sink)?;
        pay.link(&sink)?;
        return Ok(());
    }
    let tee = gst::ElementFactory::make("tee")
        .property("name", format!("ftee-{tag}"))
        .build()?;
    pipe.add(&tee)?;
    pay.link(&tee)?;
    for (index, ep) in endpoints.iter().enumerate() {
        let sock = bind
            .tx()
            .with_context(|| format!("udpsink {}:{}", ep.ip, ep.port))?;
        let gio_socket = gio_from_udp(sock)?;
        let queue = gst::ElementFactory::make("queue")
            .property("name", format!("fsinkq-{tag}-{index}"))
            .build()?;
        // async=false: живой пайплайн иначе ждёт preroll этого сокета и
        // оставляет appsrc в Paused. Кадры не выходят, level молчит.
        let sink = gst::ElementFactory::make("udpsink")
            .property("name", format!("fsink-{tag}-{index}"))
            .property("host", &ep.ip)
            .property("port", ep.port as i32)
            .property("qos-dscp", dscp as i32)
            .property("sync", true)
            .property("async", false)
            .property("socket", &gio_socket)
            .build()?;
        pipe.add_many([&queue, &sink])?;
        tee.link(&queue)?;
        queue.link(&sink)?;
    }
    Ok(())
}

/// Сначала стоки, потом источник: живой пайплайн иначе не переводит хвост в Playing.
fn sync_added_tail(pipe: &gst::Pipeline, root: &gst::Element) -> Result<()> {
    if !matches!(pipe.current_state(), gst::State::Playing | gst::State::Paused) {
        return Ok(());
    }
    for el in downstream_postorder(root) {
        el.sync_state_with_parent()?;
    }
    Ok(())
}

fn downstream_postorder(root: &gst::Element) -> Vec<gst::Element> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    fn walk(el: &gst::Element, seen: &mut BTreeSet<String>, out: &mut Vec<gst::Element>) {
        if !seen.insert(el.name().to_string()) {
            return;
        }
        let kids: Vec<gst::Element> = el
            .pads()
            .into_iter()
            .filter(|pad| pad.direction() == gst::PadDirection::Src)
            .filter_map(|pad| pad.peer().and_then(|peer| peer.parent_element()))
            .collect();
        for kid in kids {
            walk(&kid, seen, out);
        }
        out.push(el.clone());
    }
    walk(root, &mut seen, &mut out);
    out
}

fn remove_downstream(pipe: &gst::Pipeline, root_name: &str) -> Result<()> {
    let Some(root) = pipe.by_name(root_name) else {
        return Ok(());
    };
    let order = downstream_postorder(&root);
    for el in &order {
        for pad in el.pads() {
            let Some(peer) = pad.peer() else { continue };
            if pad.direction() == gst::PadDirection::Src {
                let _ = pad.unlink(&peer);
            }
        }
        if el.factory().is_some_and(|factory| factory.name() == "tee") {
            for pad in el.src_pads() {
                el.release_request_pad(&pad);
            }
        }
    }
    for el in &order {
        el.set_state(gst::State::Null)?;
        // set_state(Null) часто Async. remove до фактического Null
        // уничтожает элемент в Playing и роняет процесс.
        let (_res, cur, _) = el.state(gst::ClockTime::from_seconds(2));
        if cur != gst::State::Null {
            anyhow::bail!("хвост {} не остановился: {cur:?}", el.name());
        }
        pipe.remove(el)?;
    }
    Ok(())
}

/// ALSA-хвост: общий line-in копируется в зоны без своего монитора.
/// Зона с `source` здесь по-прежнему отдельная ветка — у ALSA монитора нет.
/// Пустой общий набор — `fakesink`, чтобы `level` продолжал течь.
fn attach_zone_tails(
    pipe: &gst::Pipeline,
    from: &gst::Element,
    zones: &[zproto::ZoneEndpoints],
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let tee = gst::ElementFactory::make("tee").property("name", "utee").build()?;
    pipe.add(&tee)?;
    from.link(&tee)?;
    let shared: Vec<_> = zones.iter().filter(|z| z.source.is_none()).collect();
    if shared.is_empty() {
        let queue = gst::ElementFactory::make("queue").build()?;
        let sink = gst::ElementFactory::make("fakesink")
            .property("name", "usilence")
            .property("sync", false)
            .property("async", false)
            .build()?;
        pipe.add_many([&queue, &sink])?;
        tee.link(&queue)?;
        queue.link(&sink)?;
    } else {
        for zone in shared {
            attach_shared_branch(pipe, &tee, zone, dscp, bind)?;
        }
    }
    for zone in zones.iter().filter(|z| z.source.is_some()) {
        attach_virtual_branch(pipe, zone, dscp, bind)?;
    }
    Ok(())
}

fn attach_shared_branch(
    pipe: &gst::Pipeline,
    tee: &gst::Element,
    zone: &zproto::ZoneEndpoints,
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let conv = gst::ElementFactory::make("audioconvert").build()?;
    let res = gst::ElementFactory::make("audioresample").property("quality", 4i32).build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::from_str("audio/x-raw,format=S16BE,rate=48000,channels=2")?,
        )
        .build()?;
    let pay = gst::ElementFactory::make("rtpL16pay")
        .property("pt", 96u32)
        .property("mtu", 1400u32)
        .build()?;
    pipe.add_many([&queue, &conv, &res, &caps, &pay])?;
    tee.link(&queue)?;
    gst::Element::link_many([&queue, &conv, &res, &caps, &pay])?;
    attach_pay_tail(pipe, &pay, &zone.endpoints, dscp, bind)
}

/// Свой pw-record этой зоны. Level на ветке, чтобы тишина line-in не
/// затирала RMS играющего выхода.
fn attach_virtual_branch(
    pipe: &gst::Pipeline,
    zone: &zproto::ZoneEndpoints,
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    let token = element_token(&zone.zone_id);
    let src = live_appsrc(&capture_appsrc_name(&zone.zone_id))?;
    let hold = gst::ElementFactory::make("queue")
        .property_from_str("leaky", "downstream")
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()?;
    let conv = gst::ElementFactory::make("audioconvert").build()?;
    let res = gst::ElementFactory::make("audioresample").property("quality", 4i32).build()?;
    let caps_rate = gst::ElementFactory::make("capsfilter")
        .property("caps", gst::Caps::from_str("audio/x-raw,format=F32LE,rate=48000,channels=2")?)
        .build()?;
    let tee = gst::ElementFactory::make("tee").property("name", format!("vtee-{token}")).build()?;
    let chain = [&src, &hold, &conv, &res, &caps_rate, &tee];
    pipe.add_many(chain)?;
    gst::Element::link_many(chain)?;

    let queue = gst::ElementFactory::make("queue").build()?;
    let pay_conv = gst::ElementFactory::make("audioconvert").build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::from_str("audio/x-raw,format=S16BE,rate=48000,channels=2")?,
        )
        .build()?;
    let pay = gst::ElementFactory::make("rtpL16pay")
        .property("pt", 96u32)
        .property("mtu", 1400u32)
        .build()?;
    pipe.add_many([&queue, &pay_conv, &caps, &pay])?;
    tee.link(&queue)?;
    gst::Element::link_many([&queue, &pay_conv, &caps, &pay])?;
    attach_pay_tail(pipe, &pay, &zone.endpoints, dscp, bind)?;

    let lq = gst::ElementFactory::make("queue").build()?;
    let lev = gst::ElementFactory::make("level").property("name", format!("lev-{token}")).build()?;
    let sink = gst::ElementFactory::make("fakesink")
        .property("sync", false)
        .property("async", false)
        .build()?;
    pipe.add_many([&lq, &lev, &sink])?;
    tee.link(&lq)?;
    gst::Element::link_many([&lq, &lev, &sink])?;
    Ok(())
}

fn attach_pay_tail(
    pipe: &gst::Pipeline,
    pay: &gst::Element,
    endpoints: &[zproto::Endpoint],
    dscp: u8,
    bind: &mut dyn LocalUdp,
) -> Result<()> {
    // Пустой список — fakesink, tx() не зовём и udpsink не ставим.
    if endpoints.is_empty() {
        let sink = gst::ElementFactory::make("fakesink")
            .property("sync", false)
            .property("async", false)
            .build()?;
        pipe.add(&sink)?;
        pay.link(&sink)?;
        return Ok(());
    }
    let tee = gst::ElementFactory::make("tee").build()?;
    pipe.add(&tee)?;
    pay.link(&tee)?;
    for ep in endpoints {
        let sock = bind
            .tx()
            .with_context(|| format!("udpsink {}:{}", ep.ip, ep.port))?;
        let gio_socket = gio_from_udp(sock)?;
        let queue = gst::ElementFactory::make("queue").build()?;
        let sink = gst::ElementFactory::make("udpsink")
            .property("host", &ep.ip)
            .property("port", ep.port as i32)
            .property("qos-dscp", dscp as i32)
            .property("sync", true)
            .property("socket", &gio_socket)
            .build()?;
        pipe.add_many([&queue, &sink])?;
        tee.link(&queue)?;
        queue.link(&sink)?;
    }
    Ok(())
}

/// Общая голова tx-цепочки: alsasrc→level→caps(48k/2ch)→audioconvert→
/// audioresample→caps(S16BE)→rtpL16pay. Возвращает пайплайн и pay — последний
/// элемент головы, к которому подключается хвост (udpsink multicast или tee
/// unicast).
/// Захват до развилки: alsasrc → level → 48 кГц стерео. `level` до S16BE.
fn make_tx_capture(
    cfg: &crate::config::PipelineCfg,
    device: &str,
) -> Result<(gst::Pipeline, gst::Element)> {
    let pipe = gst::Pipeline::new();
    let src = gst::ElementFactory::make("alsasrc")
        .property("device", device)
        .property("buffer-time", cfg.tx_buffer_us as i64)
        .property("latency-time", cfg.tx_latency_us as i64)
        .build()?;
    let lev = gst::ElementFactory::make("level").property("name", "lev").build()?;
    let caps_rate = gst::ElementFactory::make("capsfilter")
        .property("caps", gst::Caps::from_str("audio/x-raw,rate=48000,channels=2")?)
        .build()?;
    pipe.add_many([&src, &lev, &caps_rate])?;
    gst::Element::link_many([&src, &lev, &caps_rate])?;
    Ok((pipe, caps_rate))
}

fn make_tx_head(
    cfg: &crate::config::PipelineCfg,
    device: &str,
) -> Result<(gst::Pipeline, gst::Element)> {
    let (pipe, caps_rate) = make_tx_capture(cfg, device)?;
    let conv = gst::ElementFactory::make("audioconvert").build()?;
    let res = gst::ElementFactory::make("audioresample").property("quality", 4i32).build()?;
    let caps_be = gst::ElementFactory::make("capsfilter")
        .property("caps", gst::Caps::from_str("audio/x-raw,format=S16BE,rate=48000,channels=2")?)
        .build()?;
    let pay = gst::ElementFactory::make("rtpL16pay")
        .property("pt", 96u32)
        .property("mtu", 1400u32)
        .build()?;
    pipe.add_many([&conv, &res, &caps_be, &pay])?;
    gst::Element::link_many([&caps_rate, &conv, &res, &caps_be, &pay])?;
    Ok((pipe, pay))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gio::prelude::*;

    fn cfg() -> crate::config::PipelineCfg { crate::config::PipelineCfg::default_rx() }

    #[test]
    fn rtp_branch_conceals_after_depay() {
        gstreamer::init().unwrap();
        let port = cfg().port;
        let pipe = build_rx(&cfg(), "null", &ChannelMap::default(), &mut LoopbackUdp).unwrap();
        let depay = pipe.by_name(&format!("depay{port}")).expect("depay");
        let peer = depay.static_pad("src").unwrap().peer().unwrap();
        assert_eq!(peer.parent_element().unwrap().name().as_str(), format!("conceal{port}"));
        let silent = build_rx_ports(&cfg(), "null", &ChannelMap::default(), &[], &[], &mut LoopbackUdp).unwrap();
        assert!(silent.children().iter().all(|e| !e.name().starts_with("conceal")));
    }

    #[test]
    fn rx_has_required_elements() {
        gstreamer::init().unwrap();
        let p = build_rx(&cfg(), "plughw:CARD=Codec", &ChannelMap::default(), &mut LoopbackUdp).unwrap();
        for factory in ["udpsrc", "rtpjitterbuffer", "rtpL16depay", "audioconvert",
                        "audioresample", "level", "alsasink"] {
            assert!(p.children().iter().any(|e| e.factory().unwrap().name() == factory),
                "нет элемента {factory}");
        }
        // audioconvert три: ветка порта, матричный (mconv) и общий хвост.
        let children = p.children();
        let convs: Vec<_> = children.iter()
            .filter(|e| e.factory().unwrap().name() == "audioconvert").collect();
        assert_eq!(convs.len(), 3, "audioconvert: ветка + матричный + общий");
        let src = p.by_name(&format!("usrc{}", cfg().port)).expect("udpsrc порта");
        assert_eq!(src.property::<String>("address"), "0.0.0.0");
        assert_eq!(src.property::<i32>("port"), cfg().port as i32);
        assert!(!src.property::<bool>("auto-multicast"));
        let bound = src.property::<gio::Socket>("socket");
        let addr = bound.local_address().expect("локальный адрес udpsrc");
        let inet = addr.dynamic_cast::<gio::InetSocketAddress>().expect("inet-адрес");
        assert!(inet.address().is_loopback(), "udpsrc сидит на loopback-сокете");
        assert_eq!(inet.port(), cfg().port);
        let mconv = p.by_name("mconv").expect("матричный audioconvert name=mconv");
        // свойство матрицы версионно-зависимо (mix-matrix ≥1.20; на старых
        // GStreamer не исключаем channel-matrix) — ищем адаптивно и сверяем
        // сериализацию: два выхода = две строки.
        let prop = ["mix-matrix", "channel-matrix"].iter()
            .find(|n| mconv.find_property(n).is_some())
            .expect("у mconv есть mix-matrix или channel-matrix");
        let s = mconv.property_value(prop).serialize()
            .expect("матрица сериализуется").to_string();
        assert_eq!(s.matches('<').count(), 3, "матрица 2x2 (2 строки-выхода): {s}");
        let sink = p.by_name("asink").unwrap();
        let sm = sink.property_value("slave-method");
        assert!(sm.serialize().unwrap().as_str().contains("skew"));
        assert_eq!(sink.property::<i64>("buffer-time"), 5000);
        let children = p.children();
        let jb = children.iter().find(|e| e.factory().unwrap().name() == "rtpjitterbuffer").unwrap();
        assert_eq!(jb.property::<u32>("latency"), 2);
    }

    #[test]
    fn branch_map_is_applied_before_the_mixer() {
        gstreamer::init().unwrap();
        let port = cfg().port;
        let swapped = ChannelMap { out_l: Some(ChanSel::R), out_r: Some(ChanSel::L) };
        let p = build_rx_ports(&cfg(), "null", &ChannelMap::default(), &[port], &[(port, swapped)], &mut LoopbackUdp).unwrap();
        let conv = p.by_name(&format!("conv{port}")).unwrap();
        let mconv = p.by_name("mconv").unwrap();
        let prop = ["mix-matrix", "channel-matrix"].iter()
            .find(|n| conv.find_property(n).is_some())
            .expect("матрица на ветке");
        let branch = conv.property_value(prop).serialize().unwrap().to_string();
        let tail = mconv.property_value(prop).serialize().unwrap().to_string();
        assert_ne!(branch, tail, "ветка источника не совпадает со сквозным выходом микшера");
    }

    #[test]
    fn matrix_mapping() {
        // только L в левый выход, правый молчит
        let cm = ChannelMap { out_l: Some(ChanSel::L), out_r: None };
        assert_eq!(channel_matrix(&cm), [[1.0, 0.0], [0.0, 0.0]]);
        // кросс: левый выход ← R, правый ← L
        let cm = ChannelMap { out_l: Some(ChanSel::R), out_r: Some(ChanSel::L) };
        assert_eq!(channel_matrix(&cm), [[0.0, 1.0], [1.0, 0.0]]);
    }

    /// Сквозной тест ориентации матрицы: реальные сэмплы через audioconvert.
    /// Требует dev-dep `gstreamer-app` в zvucast/Cargo.toml:
    /// [dev-dependencies] gstreamer-app = "0.23"
    #[test]
    fn matrix_functional_appsrc() {
        use gstreamer::prelude::*;
        use gstreamer_app as gst_app;
        gst::init().unwrap();
        // out_l ← R, out_r — тишина; conv — продакшн build_mconv (тот же путь,
        // что в rx_head: строка с (float)-кастами на ≥1.26)
        let cm = ChannelMap { out_l: Some(ChanSel::R), out_r: None };
        let pipe = gst::Pipeline::new();
        let src = gst::ElementFactory::make("appsrc")
            .property("caps", gst::Caps::from_str(
                "audio/x-raw,format=S16LE,rate=48000,channels=2,layout=interleaved").unwrap())
            .property("format", gst::Format::Time)
            .build().unwrap();
        let conv = build_mconv(&cm).unwrap();
        let sink = gst::ElementFactory::make("appsink").build().unwrap();
        pipe.add_many([&src, &conv, &sink]).unwrap();
        gst::Element::link_many([&src, &conv, &sink]).unwrap();
        pipe.set_state(gst::State::Playing).unwrap();

        // 4 фрейма: L=1000, R=-2000 (интерлив)
        let samples: [i16; 8] = [1000, -2000, 1000, -2000, 1000, -2000, 1000, -2000];
        let mut buf = gst::Buffer::with_size(16).unwrap();
        {
            let b = buf.get_mut().unwrap();
            b.set_pts(gst::ClockTime::ZERO);
            let mut map = b.map_writable().unwrap();
            for (i, s) in samples.iter().enumerate() {
                map.as_mut()[i * 2..i * 2 + 2].copy_from_slice(&s.to_le_bytes());
            }
        }
        let appsrc = src.dynamic_cast::<gst_app::AppSrc>().unwrap();
        appsrc.push_buffer(buf).unwrap();
        appsrc.end_of_stream().unwrap();

        let appsink = sink.dynamic_cast::<gst_app::AppSink>().unwrap();
        let sample = appsink.pull_preroll().or_else(|_| appsink.pull_sample()).unwrap();
        let map = sample.buffer().unwrap().map_readable().unwrap();
        let out: Vec<i16> = map.as_slice().as_chunks::<2>().0.iter()
            .map(|c| i16::from_le_bytes([c[0], c[1]])).collect();
        assert_eq!(out[0], -2000, "левый выход = входной R");
        assert_eq!(out[1], 0, "правый выход молчит");
        pipe.set_state(gst::State::Null).unwrap();
    }

    #[test]
    fn rx_pw_does_not_open_pipewiresink() {
        gst::init().unwrap();
        let pipe = build_rx_pw(&crate::config::PipelineCfg::default_rx(), &ChannelMap::default(), &mut LoopbackUdp).unwrap();
        let sink = pipe.by_name("asink").expect("sink name=asink");
        // pipewiresink в PAUSED деактивирует поток, но ссылки на колонки
        // остаются active. Такой узел останавливает часы выхода.
        assert_ne!(sink.factory().unwrap().name(), "pipewiresink");
        assert_eq!(sink.factory().unwrap().name(), "appsink");
        assert!(!sink.property::<bool>("sync"));
        assert!(!sink.property::<bool>("async"));
        let caps = sink.property::<gst::Caps>("caps");
        assert!(caps.to_string().contains("S16LE"), "{caps}");
        assert!(pipe.by_name("lev").is_some(), "level на месте (rms в телеметрию)");
        assert!(pipe.children().iter().all(|e| e.factory().unwrap().name() != "pipewiresink"));
    }

    /// Живой L16 на порт колонки должен дойти до appsink. Зависший микшер
    /// приёма оставляет rendered=0, хотя пакеты на сокете есть.
    #[test]
    fn rx_pw_renders_a_live_rtp_tone() {
        use gstreamer_app as gst_app;
        gst::init().unwrap();
        let mut cfg = cfg();
        cfg.port = 17642;
        let pipe = build_rx_pw(&cfg, &ChannelMap::default(), &mut LoopbackUdp).unwrap();
        let appsink = pipe
            .by_name("asink")
            .unwrap()
            .dynamic_cast::<gst_app::AppSink>()
            .unwrap();
        pipe.set_state(gst::State::Playing).unwrap();

        let tx = gst::Pipeline::new();
        let src = gst::ElementFactory::make("audiotestsrc")
            .property("is-live", true)
            .property_from_str("wave", "sine")
            .property("freq", 440.0f64)
            .build()
            .unwrap();
        let conv = gst::ElementFactory::make("audioconvert").build().unwrap();
        let caps = gst::ElementFactory::make("capsfilter")
            .property(
                "caps",
                gst::Caps::from_str("audio/x-raw,format=S16BE,rate=48000,channels=2").unwrap(),
            )
            .build()
            .unwrap();
        let pay = gst::ElementFactory::make("rtpL16pay")
            .property("pt", 96u32)
            .property("mtu", 1400u32)
            .build()
            .unwrap();
        let sink = gst::ElementFactory::make("udpsink")
            .property("host", "127.0.0.1")
            .property("port", cfg.port as i32)
            .property("sync", true)
            .property("async", false)
            .build()
            .unwrap();
        tx.add_many([&src, &conv, &caps, &pay, &sink]).unwrap();
        gst::Element::link_many([&src, &conv, &caps, &pay, &sink]).unwrap();
        tx.set_state(gst::State::Playing).unwrap();

        let sample = appsink.try_pull_sample(gst::ClockTime::from_seconds(3));
        let err = pipe
            .bus()
            .and_then(|b| b.pop_filtered(&[gst::MessageType::Error]))
            .map(|m| format!("{m:?}"));
        let _ = pipe.set_state(gst::State::Null);
        let _ = tx.set_state(gst::State::Null);
        assert!(sample.is_some(), "rx appsink не получил живой RTP, bus={err:?}");
    }

    /// Уже стоящая ветка берёт новый запас, новая ветка собирается с ним же.
    #[test]
    fn remaining_branch_latency_follows_jb_ms() {
        gst::init().unwrap();
        let mut cfg = cfg();
        cfg.port = 17660;
        let port_a = cfg.port;
        let port_b = cfg.port + 1;
        let pipe = build_rx_ports(
            &cfg,
            "null",
            &ChannelMap::default(),
            &[port_a],
            &[],
            &mut LoopbackUdp,
        )
        .unwrap();
        let latency = |port: u16| {
            pipe.by_name(&format!("jb{port}"))
                .unwrap_or_else(|| panic!("нет jb{port}"))
                .property::<u32>("latency")
        };
        assert_eq!(latency(port_a), 2);
        set_jitter_latency(&pipe, &[port_a], 40);
        assert_eq!(latency(port_a), 40, "тот же порт — latency на месте, ветка не пересобрана");
        reconcile_mix(&pipe, &[port_a, port_b], 80, &[], &mut LoopbackUdp).unwrap();
        assert_eq!(latency(port_a), 80, "оставшаяся ветка получает новый запас вместе с портами");
        assert_eq!(latency(port_b), 80, "новая ветка собрана с переданным jb_ms");
    }

    /// Колонка стартует с тишиной и получает порт уже в Playing.
    /// Подмена ветки на живом микшере его задачу не поднимает — приём
    /// надо собрать заново, тишину при этом не трогаем.
    #[test]
    fn silence_branch_is_rebuilt_not_hot_swapped() {
        gst::init().unwrap();
        let mut cfg = cfg();
        cfg.port = 17644;
        let pipe = build_rx_pw_ports(&cfg, &ChannelMap::default(), &[], &[], &mut LoopbackUdp).unwrap();
        let err = reconcile_mix(&pipe, &[cfg.port], cfg.jb_ms, &[], &mut LoopbackUdp)
            .expect_err("снятие тишины не должно проходить на живом микшере");
        assert!(
            err.to_string().contains("пересборка"),
            "{err}"
        );
        assert!(pipe.by_name("uqsilence").is_some(), "тишина остаётся до пересборки");
        assert!(pipe.by_name(&format!("uq{}", cfg.port)).is_none(), "RTP-ветка не добавляется");
        let _ = pipe.set_state(gst::State::Null);
    }

    #[test]
    fn tx_head_has_level() {
        gstreamer::init().unwrap();
        let c = crate::config::PipelineCfg::default_tx();
        let p = build_tx(&c, "plughw:CARD=CODEC").unwrap();
        assert!(p.by_name("lev").is_some(), "level name=lev есть в tx-голове (rms для телеметрии tx)");
    }

    #[test]
    fn tx_caps_are_l16_compatible() {
        gstreamer::init().unwrap();
        let c = crate::config::PipelineCfg::default_tx();
        let p = build_tx(&c, "plughw:CARD=CODEC").unwrap();
        let children = p.children();
        let pay = children.iter().find(|e| e.factory().unwrap().name() == "rtpL16pay").unwrap();
        assert_eq!(pay.property::<u32>("pt"), 96);
        assert_eq!(pay.property::<u32>("mtu"), 1400);
    }

    #[test]
    fn unicast_has_sink_per_endpoint_with_dscp() {
        gstreamer::init().unwrap();
        let cfg = crate::config::PipelineCfg::default_tx();
        let eps = vec![zproto::ZoneEndpoints {
            zone_id: "зал".into(),
            endpoints: vec![
                zproto::Endpoint { ip: "192.0.2.36".into(), port: 5004 },
                zproto::Endpoint { ip: "192.0.2.99".into(), port: 5004 },
            ],
            source: None,
        }];
        let mut bind = LoopbackUdp;
        let p = build_tx_unicast(&cfg, "plughw:CARD=CODEC", &eps, 24, &mut bind).unwrap();
        let children = p.children();
        let sinks: Vec<_> = children.iter()
            .filter(|e| e.factory().unwrap().name() == "udpsink").collect();
        assert_eq!(sinks.len(), 2);
        assert_eq!(children.iter().filter(|e| e.factory().unwrap().name() == "rtpL16pay").count(), 1);
        let mut hosts: Vec<String> = sinks.iter()
            .map(|s| s.property::<String>("host")).collect();
        hosts.sort();
        assert_eq!(hosts, vec!["192.0.2.36", "192.0.2.99"]);
        for s in sinks {
            assert_eq!(s.property::<i32>("qos-dscp"), 24);
            let sock = s.property::<gio::Socket>("socket");
            let addr = sock.local_address().expect("локальный адрес udpsink");
            let inet = addr.dynamic_cast::<gio::InetSocketAddress>().expect("inet-адрес");
            assert!(inet.address().is_loopback(), "udpsink сидит на loopback-сокете");
        }
        assert!(p.children().iter().any(|e| e.factory().unwrap().name() == "tee"));
    }

    #[test]
    fn unicast_sink_uses_the_provided_socket() {
        let mut bind = LoopbackUdp;
        let sock = bind.tx().unwrap();
        let local = sock.local_addr().unwrap();
        assert!(local.ip().is_loopback());
    }

    #[test]
    fn unicast_gives_each_zone_its_own_payloader() {
        gstreamer::init().unwrap();
        let cfg = crate::config::PipelineCfg::default_tx();
        let zones = vec![
            zproto::ZoneEndpoints {
                zone_id: "зал".into(),
                endpoints: vec![zproto::Endpoint { ip: "192.0.2.36".into(), port: 5004 }],
                source: None,
            },
            zproto::ZoneEndpoints {
                zone_id: "спальня".into(),
                endpoints: vec![],
                source: None,
            },
        ];
        let p = build_tx_unicast(&cfg, "plughw:CARD=CODEC", &zones, 40, &mut LoopbackUdp).unwrap();
        let children = p.children();
        assert_eq!(
            children.iter().filter(|e| e.factory().unwrap().name() == "rtpL16pay").count(),
            2
        );
        let sinks: Vec<_> = children.iter()
            .filter(|e| e.factory().unwrap().name() == "udpsink").collect();
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].property::<String>("host"), "192.0.2.36");
        assert!(children.iter().any(|e| e.factory().unwrap().name() == "fakesink"));
        assert!(p.by_name("lev").is_some());
    }

    #[test]
    fn level_emits_rms() {
        gstreamer::init().unwrap();
        let pipe = gstreamer::parse::launch(
            "audiotestsrc wave=sine volume=0.5 num-buffers=200 samplesperbuffer=480 ! \
             audio/x-raw,rate=48000,channels=2 ! level name=lev ! fakesink").unwrap();
        pipe.set_state(gst::State::Playing).unwrap();
        let bus = pipe.bus().unwrap();
        let mut got = false;
        for msg in bus.iter_timed(gst::ClockTime::from_seconds(5)) {
            if let gst::MessageView::Element(e) = msg.view() {
                if let Some(s) = e.structure() {
                    if s.name() == "level" {
                        let rms = s.get::<gst::glib::ValueArray>("rms").unwrap();
                        let db: f64 = rms.first().unwrap().get().unwrap();
                        assert!(db > -20.0 && db < 0.0, "rms {db}");
                        got = true;
                        break;
                    }
                }
            }
        }
        pipe.set_state(gst::State::Null).unwrap();
        assert!(got, "level-сообщений не было");
    }

    #[test]
    fn unicast_pw_targets_zvucast_only_when_asked() {
        gstreamer::init().unwrap();
        let cfg = crate::config::PipelineCfg::default_tx();
        let plain = build_tx_unicast_pw(&cfg, &[], 0, None, &mut LoopbackUdp).unwrap();
        let src = plain
            .children()
            .into_iter()
            .find(|e| e.factory().unwrap().name() == "audiotestsrc")
            .unwrap();
        assert!(src.property::<bool>("is-live"));

        let aimed = build_tx_unicast_pw(&cfg, &[], 0, Some("zvucast"), &mut LoopbackUdp).unwrap();
        let src = aimed.by_name("cap").unwrap();
        assert_eq!(src.factory().unwrap().name(), "appsrc");
        assert!(src.property::<bool>("is-live"));
        assert!(src.property::<bool>("do-timestamp"));
        assert!(!src.property::<bool>("block"));
        assert_eq!(capture_node_name("zvucast"), "zvucast-monitor");
        assert_eq!(capture_node_name("zvucast-hall"), "zvucast-hall-monitor");
        assert_eq!(capture_node_name("zvucast-hall-monitor"), "zvucast-hall-monitor");
        assert_eq!(capture_node_name("mic"), "mic");
        assert_eq!(configured_pipewire_node("plughw:CARD=Generic_1"), None);
        assert_eq!(configured_pipewire_node("hw:CARD=CODEC"), None);
        assert_eq!(
            configured_pipewire_node("alsa_input.usb-Burr-Brown_from_TI_USB_Audio_CODEC-00.analog-stereo-input"),
            Some("alsa_input.usb-Burr-Brown_from_TI_USB_Audio_CODEC-00.analog-stereo-input"),
        );

        let zones = vec![
            zproto::ZoneEndpoints {
                zone_id: "hall".into(),
                endpoints: vec![zproto::Endpoint { ip: "192.0.2.36".into(), port: 5004 }],
                source: Some("zvucast-hall-monitor".into()),
            },
            zproto::ZoneEndpoints {
                zone_id: "bed".into(),
                endpoints: vec![zproto::Endpoint { ip: "192.0.2.99".into(), port: 5004 }],
                source: None,
            },
        ];
        let split = build_tx_unicast_pw(&cfg, &zones, 0, None, &mut LoopbackUdp).unwrap();
        assert!(split.by_name("cap-hall").is_none(), "монитор не подмешивается в микрофон");
        assert!(split.by_name("cap-bed").is_none(), "line-in остаётся на общей голове");
        assert!(split.by_name("lev-hall").is_none());
        let hall_mix = split.by_name("zmix-hall").expect("микшер зоны");
        assert_eq!(
            hall_mix.sink_pads().len(),
            1,
            "в микшер зоны входит только line-in"
        );
        assert_eq!(
            split.children().iter().filter(|e| e.factory().unwrap().name() == "rtpL16pay").count(),
            2
        );
    }

    /// Монитор не входит в RTP микрофона. `source: None` не создаёт
    /// `cap-{token}` / `lev-{token}`. Заданный source тоже не подмешивается:
    /// у вида свой хвост, не ветка `zmix`.
    #[test]
    fn monitor_is_not_in_the_microphone_tail() {
        gstreamer::init().unwrap();
        let cfg = crate::config::PipelineCfg::default_tx();
        let line = vec![zproto::ZoneEndpoints {
            zone_id: "hall".into(),
            endpoints: vec![zproto::Endpoint {
                ip: "192.0.2.36".into(),
                port: 5004,
            }],
            source: None,
        }];
        let pipe = build_tx_unicast_pw(&cfg, &line, 0, Some("line"), &mut LoopbackUdp).unwrap();
        assert!(pipe.by_name("cap-hall").is_none(), "source: None не создаёт cap-hall");
        assert!(pipe.by_name("lev-hall").is_none(), "source: None не создаёт lev-hall");
        assert!(pipe.by_name("lev").is_some(), "lev головы захвата на месте");
        assert_eq!(pipe.by_name("zmix-hall").unwrap().sink_pads().len(), 1);
        let head = pipe
            .by_name("cap")
            .unwrap()
            .static_pad("src")
            .unwrap()
            .peer()
            .unwrap()
            .parent_element()
            .unwrap();
        assert_eq!(head.property::<u32>("max-size-buffers"), 8, "голова line-in на восьми буферах");

        let sourced = vec![zproto::ZoneEndpoints {
            zone_id: "hall".into(),
            endpoints: vec![zproto::Endpoint {
                ip: "192.0.2.36".into(),
                port: 5004,
            }],
            source: Some("zvucast-hall-monitor".into()),
        }];
        let mixed = build_tx_unicast_pw(&cfg, &sourced, 0, Some("line"), &mut LoopbackUdp).unwrap();
        assert!(
            mixed.by_name("cap-hall").is_none(),
            "монитор не в хвосте микрофона"
        );
        assert!(mixed.by_name("lev-hall").is_none(), "lev монитора не в хвосте микрофона");
        assert_eq!(
            mixed.by_name("zmix-hall").unwrap().sink_pads().len(),
            1,
            "в микшер зоны входит только line-in"
        );
    }

    /// Микрофон и Spotify одной зоны — разные `level`. Голова захвата в хвост вида не входит.
    #[test]
    fn microphone_and_spotify_feed_use_different_levels() {
        gstreamer::init().unwrap();
        let cfg = crate::config::PipelineCfg::default_tx();
        let zones = vec![zproto::ZoneEndpoints {
            zone_id: "hall".into(),
            endpoints: vec![zproto::Endpoint {
                ip: "192.0.2.36".into(),
                port: 5004,
            }],
            source: None,
        }];
        let pipe = build_tx_unicast_pw(&cfg, &zones, 0, Some("mic"), &mut LoopbackUdp).unwrap();
        let feeds = vec![zproto::FeedSend {
            zone_id: "hall".into(),
            kind: zproto::FeedKind::Spotify,
            monitor: "zvucast-spotify-hall-monitor".into(),
            endpoints: vec![zproto::Endpoint {
                ip: "192.0.2.36".into(),
                port: 5010,
            }],
        }];
        reconcile_feed_tails(&pipe, &feeds, 0, &mut LoopbackUdp).unwrap();
        let mic = pipe.by_name("lev").expect("lev микрофона");
        let feed = pipe.by_name("lev-feed-spotify-hall").expect("lev фида");
        assert_ne!(mic.name(), feed.name(), "микрофонный lev не является lev фида");
        assert!(pipe.by_name("feed-spotify-hall").is_some(), "appsrc хвоста вида");
        assert!(pipe.by_name("cap-hall").is_none());
        assert_eq!(pipe.by_name("zmix-hall").unwrap().sink_pads().len(), 1);
        let sinks: Vec<_> = pipe
            .children()
            .into_iter()
            .filter(|e| e.factory().unwrap().name() == "udpsink")
            .collect();
        assert_eq!(sinks.len(), 2, "у микрофона и фида свои udpsink");
        let mut ports: Vec<i32> = sinks.iter().map(|s| s.property::<i32>("port")).collect();
        ports.sort();
        assert_eq!(ports, vec![5004, 5010]);
        let hold = pipe.by_name("fhold-spotify-hall").expect("очередь хвоста");
        assert_eq!(hold.property::<u32>("max-size-buffers"), 2);
        let leaky = hold.property_value("leaky").serialize().expect("leaky");
        assert_eq!(leaky.as_str(), "downstream");

        reconcile_feed_tails(&pipe, &[], 0, &mut LoopbackUdp).unwrap();
        assert!(pipe.by_name("feed-spotify-hall").is_none(), "пустой SetFeeds снимает хвост");
        assert!(pipe.by_name("lev-feed-spotify-hall").is_none());
        assert!(pipe.by_name("lev").is_some(), "голова захвата остаётся");
        assert!(pipe.by_name("pay-hall").is_some(), "пейлоадер микрофона остаётся");
    }

    /// Живой пейлоадер микрофона не получает ветку монитора, даже если source задан.
    #[test]
    fn monitor_does_not_join_the_microphone_payloader() {
        gstreamer::init().unwrap();
        let cfg = crate::config::PipelineCfg::default_tx();
        let mut zone = zproto::ZoneEndpoints {
            zone_id: "hall".into(),
            endpoints: vec![zproto::Endpoint { ip: "192.0.2.36".into(), port: 5004 }],
            source: None,
        };
        let pipe = build_tx_unicast_pw(&cfg, &[zone.clone()], 0, Some("mic"), &mut LoopbackUdp).unwrap();
        let pay = pipe.by_name("pay-hall").expect("пейлоадер зоны");
        let pay_ptr = pay.as_ptr();
        assert_eq!(pipe.by_name("zmix-hall").unwrap().sink_pads().len(), 1);

        zone.source = Some("zvucast-hall-monitor".into());
        reconcile_pw_monitors(&pipe, &[zone.clone()]).unwrap();
        assert_eq!(pipe.by_name("pay-hall").unwrap().as_ptr(), pay_ptr, "RTP-пейлоадер тот же");
        assert!(pipe.by_name("cap-hall").is_none(), "монитор не в хвосте микрофона");
        assert!(pipe.by_name("lev-hall").is_none());
        assert_eq!(pipe.by_name("zmix-hall").unwrap().sink_pads().len(), 1);

        zone.source = None;
        reconcile_pw_monitors(&pipe, &[zone]).unwrap();
        assert_eq!(pipe.by_name("pay-hall").unwrap().as_ptr(), pay_ptr);
        assert!(pipe.by_name("cap-hall").is_none());
        assert_eq!(pipe.by_name("zmix-hall").unwrap().sink_pads().len(), 1);
    }

    #[test]
    fn playback_target_skips_the_virtual_sink() {
        let text = "\
update: id:0 key:'default.configured.audio.sink' value:'{ \"name\": \"alsa_output.speaker\" }' type:''
update: id:0 key:'default.audio.sink' value:'{\"name\":\"zvucast\"}' type:''
";
        assert_eq!(playback_target_from_metadata(text).as_deref(), Some("alsa_output.speaker"));
        let zone_sink = "\
update: id:0 key:'default.configured.audio.sink' value:'{ \"name\": \"alsa_output.speaker\" }' type:''
update: id:0 key:'default.audio.sink' value:'{\"name\":\"zvucast-hall\"}' type:''
";
        assert_eq!(playback_target_from_metadata(zone_sink).as_deref(), Some("alsa_output.speaker"));
        let easy = "update: id:0 key:'default.audio.sink' value:'{\"name\":\"easyeffects_sink\"}' type:''\n";
        assert_eq!(playback_target_from_metadata(easy).as_deref(), Some("easyeffects_sink"));
    }
}
