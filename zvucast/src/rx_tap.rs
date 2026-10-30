//! Наблюдение за приёмом. На звук не влияет.
//!
//! Строки пишет отдельный поток. Поток пайплайна только считает нули и
//! кладёт запись в канал. Канал короткий: если он полон, кадр не ждёт.
//!
//! Файл `/tmp/zvucast-rx-tap.log`, столбцы:
//! kind unix_us gap_us a b frames zero_frames longest lead tail pts_ns dur_ns hole_ns run_ns seg_base seg_offset seg_start src_pos phase_l phase_r
//!
//! - `I` пакет на входе rtpjitterbuffer. `a` — шаг метки RTP в кадрах,
//!   `b` — шаг номера пакета.
//! - `L` событие GstRTPPacketLost на выходе буфера. `a` — длительность в кадрах.
//! - `G` GAP после депакетайзера. `a` — длительность в кадрах.
//! - `P` PCM сразу после депакетайзера, до conceal.
//! - `C` PCM после conceal.
//! - `S` вход микшера, уже после очереди ветки RTP.
//! - `Z` вход микшера от ветки тишины. Строк нет — ветки в графе нет.
//! - `Q` очередь входа в момент, когда микшер уже выбрал сэмплы на следующий
//!   выход. `a` равно 1, если буфер попадает в этот срез. `a` равно 2, если
//!   буфер в очереди есть, но в этот срез микшер его не кладёт. `a` равно 0,
//!   если очередь пуста. `pts_ns` и `dur_ns` — метка выхода. `hole_ns` —
//!   метка входного буфера, `run_ns` — его длительность. `seg_base` — с какого
//!   сэмпла выход сейчас заполняется, `seg_offset` — куда микшер положил бы
//!   этот вход. У `a` = 2 позиции нет: смотрелка среза такой буфер не отдаёт.
//!   `phase_l` и `phase_r` — фаза левого 440 Гц и правого 1000 Гц на первом
//!   отсчёте буфера, в миллионных долях цикла. У остальных строк нули.
//! - `M` выход микшера.
//! У `P`/`C`/`S`/`Z`/`M` поле `a` равно 1, если буфер с DISCONT.
//! `hole_ns` — насколько метка этого буфера ушла вперёд от конца предыдущего.
//! Ноль значит, что шкала непрерывна. `-1` — метки не было.
//! `run_ns` — эта же метка в шкале сегмента. `src_pos` — позиция src микшера
//! в момент прихода буфера. У `S`/`Z`/`M` ещё `seg_base`, `seg_offset`, `seg_start`.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use gstreamer as gst;
use gst::prelude::*;

const THRESH: i16 = 200;
// Минута с очередью Q — около 120 тысяч строк, и до неё ещё прогрев.
// 250 тысяч кончались за две минуты, окно замера оставалось без журнала.
const LINE_CAP: u32 = 1_000_000;

struct Rec {
    kind: u8,
    unix_us: u64,
    gap_us: u32,
    a: i64,
    b: i32,
    frames: u32,
    zero_frames: u32,
    longest: u32,
    lead: u32,
    tail: u32,
    pts_ns: i64,
    dur_ns: i64,
    hole_ns: i64,
    run_ns: i64,
    seg_base: i64,
    seg_offset: i64,
    seg_start: i64,
    src_pos: i64,
    /// Фаза левого 440 Гц и правого 1000 Гц, миллионные доли цикла.
    /// Считается только для `Q`. Ноль — буфера не было или он тихий.
    phase_l: i32,
    phase_r: i32,
}

struct Scan {
    frames: u32,
    zero_frames: u32,
    longest: u32,
    lead: u32,
    tail: u32,
}

struct RtpMemory {
    ts: Option<u32>,
    seq: Option<u16>,
}

struct Clocks {
    rtp: Option<Instant>,
    lost: Option<Instant>,
    gap: Option<Instant>,
    pcm: Option<Instant>,
    conceal: Option<Instant>,
    sink: Option<Instant>,
    silence: Option<Instant>,
    queue: Option<Instant>,
    mix: Option<Instant>,
    pcm_end: Option<(i64, i64)>,
    conceal_end: Option<(i64, i64)>,
    sink_end: Option<(i64, i64)>,
    silence_end: Option<(i64, i64)>,
    mix_end: Option<(i64, i64)>,
}

fn sender() -> &'static SyncSender<Rec> {
    static TX: OnceLock<SyncSender<Rec>> = OnceLock::new();
    TX.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel(2048);
        let _ = std::thread::Builder::new()
            .name("rx-tap".to_string())
            .spawn(move || logger(rx));
        tx
    })
}

fn clocks() -> &'static Mutex<Clocks> {
    static CLOCKS: OnceLock<Mutex<Clocks>> = OnceLock::new();
    CLOCKS.get_or_init(|| {
        Mutex::new(Clocks {
            rtp: None,
            lost: None,
            gap: None,
            pcm: None,
            conceal: None,
            sink: None,
            silence: None,
            queue: None,
            mix: None,
            pcm_end: None,
            conceal_end: None,
            sink_end: None,
            silence_end: None,
            mix_end: None,
        })
    })
}

fn rtp_memory() -> &'static Mutex<RtpMemory> {
    static RTP: OnceLock<Mutex<RtpMemory>> = OnceLock::new();
    RTP.get_or_init(|| Mutex::new(RtpMemory { ts: None, seq: None }))
}

fn stamp(slot: &mut Option<Instant>) -> (u64, u32) {
    let now = Instant::now();
    let gap = slot
        .map(|prev| {
            now.duration_since(prev)
                .as_micros()
                .min(u128::from(u32::MAX)) as u32
        })
        .unwrap_or(0);
    *slot = Some(now);
    let unix_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0);
    (unix_us, gap)
}

fn send(rec: Rec) {
    let _ = sender().try_send(rec);
}

pub fn watch_rtp(pad: &gst::Pad) {
    let _ = pad.add_probe(gst::PadProbeType::BUFFER, |_pad, info| {
        if let Some(buffer) = info.buffer() {
            if let Ok(map) = buffer.map_readable() {
                note_rtp(map.as_slice());
            }
        }
        gst::PadProbeReturn::Ok
    });
}

pub fn watch_lost(pad: &gst::Pad) {
    let _ = pad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, |_pad, info| {
        if let Some(event) = info.event() {
            note_lost(event.as_ref());
        }
        gst::PadProbeReturn::Ok
    });
}

pub fn watch_gap(pad: &gst::Pad) {
    let _ = pad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, |_pad, info| {
        if let Some(event) = info.event() {
            note_gap(event.as_ref());
        }
        gst::PadProbeReturn::Ok
    });
}

pub fn watch_pcm(pad: &gst::Pad, kind: u8) {
    let _ = pad.add_probe(gst::PadProbeType::BUFFER, move |pad, info| {
        if let Some(buffer) = info.buffer() {
            if let Ok(map) = buffer.map_readable() {
                let discont = buffer.flags().contains(gst::BufferFlags::DISCONT);
                let pts = buffer.pts().map(|time| time.nseconds() as i64).unwrap_or(-1);
                let dur = buffer
                    .duration()
                    .map(|time| time.nseconds() as i64)
                    .unwrap_or(-1);
                note_pcm(kind, pad, map.as_slice(), discont, pts, dur);
            }
        }
        gst::PadProbeReturn::Ok
    });
}

/// Очередь входа в тот момент, когда микшер выбирает сэмплы на следующий выход.
///
/// Сигнал только наблюдает. `emit-signals` больше ничего в микшере не меняет.
pub fn watch_mixer(element: &gst::Element) {
    use gstreamer_base::prelude::{AggregatorExt, AggregatorExtManual, AggregatorPadExt};

    let Ok(agg) = element.clone().dynamic_cast::<gstreamer_base::Aggregator>() else {
        return;
    };
    agg.set_emit_signals(true);
    agg.connect_samples_selected(move |agg, _segment, pts, _dts, duration, info| {
        let out_pts = pts.map(|time| time.nseconds() as i64).unwrap_or(-1);
        let out_dur = duration.map(|time| time.nseconds() as i64).unwrap_or(-1);
        let fill = struct_u64(info, "offset");
        let pads: Vec<_> = agg
            .sink_pads()
            .into_iter()
            .filter_map(|pad| pad.dynamic_cast::<gstreamer_base::AggregatorPad>().ok())
            .collect();
        if pads.is_empty() {
            note_queue(None, out_pts, out_dur, fill, None, None);
            return;
        }
        for pad in &pads {
            let sample = agg.peek_next_sample(pad);
            if sample.is_some() {
                note_queue(
                    Some(pad.upcast_ref()),
                    out_pts,
                    out_dur,
                    fill,
                    sample.as_ref(),
                    None,
                );
            } else {
                // Буфер дальше среза peek_next_sample прячет. Голова очереди
                // при этом ещё может лежать на пэде.
                let held = pad.peek_buffer();
                note_queue(
                    Some(pad.upcast_ref()),
                    out_pts,
                    out_dur,
                    fill,
                    None,
                    held.as_ref(),
                );
            }
        }
    });
}

fn note_queue(
    pad: Option<&gst::Pad>,
    out_pts: i64,
    out_dur: i64,
    fill: i64,
    sample: Option<&gst::Sample>,
    held: Option<&gst::Buffer>,
) {
    let selected = sample.and_then(|item| item.buffer());
    let buffer = selected.or_else(|| held.map(|item| item.as_ref()));
    let info = sample.and_then(|item| item.info());
    let queued = if selected.is_some() {
        1
    } else if held.is_some() {
        2
    } else {
        0
    };
    let in_pts = buffer
        .and_then(|item| item.pts())
        .map(|time| time.nseconds() as i64)
        .unwrap_or(-1);
    let in_dur = buffer
        .and_then(|item| item.duration())
        .map(|time| time.nseconds() as i64)
        .unwrap_or(-1);
    // У convert-пада caps входа F32, а в очереди микшера буфер уже может быть S16.
    // Формат берём по размеру, не по caps.
    let format = match buffer {
        Some(buffer) if in_dur > 0 => {
            let frames = (i128::from(in_dur) * 48_000 / 1_000_000_000) as usize;
            let width = if frames == 0 { 0 } else { buffer.size() / frames };
            match width {
                8 => b'F',
                4 => b'L',
                _ => pad.map(pad_format).unwrap_or(b'?'),
            }
        }
        _ => pad.map(pad_format).unwrap_or(b'?'),
    };
    let scan = match (buffer, format) {
        (Some(buffer), b'B') => buffer
            .map_readable()
            .map(|map| scan_s16(map.as_slice(), true))
            .unwrap_or(empty_scan()),
        (Some(buffer), b'L') => buffer
            .map_readable()
            .map(|map| scan_s16(map.as_slice(), false))
            .unwrap_or(empty_scan()),
        (Some(buffer), b'F') => buffer
            .map_readable()
            .map(|map| scan_f32(map.as_slice()))
            .unwrap_or(empty_scan()),
        (Some(buffer), _) => Scan {
            frames: (buffer.size() / 4) as u32,
            ..empty_scan()
        },
        _ => empty_scan(),
    };
    let (phase_l, phase_r) = buffer.map(|item| buffer_phase(item, format)).unwrap_or((0, 0));
    let (unix_us, gap_us) = {
        let Ok(mut clock) = clocks().lock() else {
            return;
        };
        stamp(&mut clock.queue)
    };
    send(Rec {
        kind: b'Q',
        unix_us,
        gap_us,
        a: queued,
        b: i32::from(format),
        frames: scan.frames,
        zero_frames: scan.zero_frames,
        longest: scan.longest,
        lead: scan.lead,
        tail: scan.tail,
        pts_ns: out_pts,
        dur_ns: out_dur,
        hole_ns: in_pts,
        run_ns: in_dur,
        seg_base: fill,
        seg_offset: struct_u64(info, "output-offset"),
        seg_start: struct_u64(info, "position"),
        src_pos: struct_u64(info, "size"),
        phase_l,
        phase_r,
    });
}

fn buffer_phase(buffer: &gst::BufferRef, format: u8) -> (i32, i32) {
    let Ok(map) = buffer.map_readable() else {
        return (0, 0);
    };
    match format {
        b'B' => phase_s16(map.as_slice(), true),
        b'L' => phase_s16(map.as_slice(), false),
        b'F' => phase_f32(map.as_slice()),
        _ => (0, 0),
    }
}

fn milli_cycle(sin_part: f64, cos_part: f64) -> i32 {
    if sin_part.hypot(cos_part) < 1.0 {
        return 0;
    }
    let turns = cos_part.atan2(sin_part) / std::f64::consts::TAU;
    (turns * 1_000_000.0).round() as i32
}

fn phase_s16(bytes: &[u8], big_endian: bool) -> (i32, i32) {
    let frames = bytes.len() / 4;
    if frames < 32 {
        return (0, 0);
    }
    let mut ls = 0.0;
    let mut lc = 0.0;
    let mut rs = 0.0;
    let mut rc = 0.0;
    let wl = std::f64::consts::TAU * 440.0 / 48_000.0;
    let wr = std::f64::consts::TAU * 1000.0 / 48_000.0;
    for i in 0..frames {
        let at = i * 4;
        let (left, right) = if big_endian {
            (
                f64::from(i16::from_be_bytes([bytes[at], bytes[at + 1]])),
                f64::from(i16::from_be_bytes([bytes[at + 2], bytes[at + 3]])),
            )
        } else {
            (
                f64::from(i16::from_le_bytes([bytes[at], bytes[at + 1]])),
                f64::from(i16::from_le_bytes([bytes[at + 2], bytes[at + 3]])),
            )
        };
        let t = i as f64;
        ls += left * (wl * t).sin();
        lc += left * (wl * t).cos();
        rs += right * (wr * t).sin();
        rc += right * (wr * t).cos();
    }
    (milli_cycle(ls, lc), milli_cycle(rs, rc))
}

fn phase_f32(bytes: &[u8]) -> (i32, i32) {
    let frames = bytes.len() / 8;
    if frames < 32 {
        return (0, 0);
    }
    let mut ls = 0.0;
    let mut lc = 0.0;
    let mut rs = 0.0;
    let mut rc = 0.0;
    let wl = std::f64::consts::TAU * 440.0 / 48_000.0;
    let wr = std::f64::consts::TAU * 1000.0 / 48_000.0;
    for i in 0..frames {
        let at = i * 8;
        let left = f64::from(f32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())) * 32768.0;
        let right =
            f64::from(f32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap())) * 32768.0;
        let t = i as f64;
        ls += left * (wl * t).sin();
        lc += left * (wl * t).cos();
        rs += right * (wr * t).sin();
        rc += right * (wr * t).cos();
    }
    (milli_cycle(ls, lc), milli_cycle(rs, rc))
}

fn empty_scan() -> Scan {
    Scan {
        frames: 0,
        zero_frames: 0,
        longest: 0,
        lead: 0,
        tail: 0,
    }
}

fn struct_u64(info: Option<&gst::StructureRef>, name: &str) -> i64 {
    let Some(structure) = info else {
        return -1;
    };
    if let Ok(value) = structure.get::<u64>(name) {
        return value as i64;
    }
    if let Ok(value) = structure.get::<u32>(name) {
        return i64::from(value);
    }
    -1
}

fn note_rtp(bytes: &[u8]) {
    let Some((seq, ts, payload)) = rtp_payload(bytes) else {
        return;
    };
    let scan = scan_s16(payload, true);
    let (rtp_delta, seq_delta) = {
        let Ok(mut mem) = rtp_memory().lock() else {
            return;
        };
        let rtp_delta = mem
            .ts
            .map(|prev| ts.wrapping_sub(prev) as i64)
            .unwrap_or(0);
        let seq_delta = mem
            .seq
            .map(|prev| seq.wrapping_sub(prev) as i32)
            .unwrap_or(0);
        mem.ts = Some(ts);
        mem.seq = Some(seq);
        (rtp_delta, seq_delta)
    };
    let (unix_us, gap_us) = {
        let Ok(mut clock) = clocks().lock() else {
            return;
        };
        stamp(&mut clock.rtp)
    };
    send(Rec {
        kind: b'I',
        unix_us,
        gap_us,
        a: rtp_delta,
        b: seq_delta,
        frames: scan.frames,
        zero_frames: scan.zero_frames,
        longest: scan.longest,
        lead: scan.lead,
        tail: scan.tail,
        pts_ns: -1,
        dur_ns: -1,
        hole_ns: -1,
        run_ns: -1,
        seg_base: -1,
        seg_offset: -1,
        seg_start: -1,
        src_pos: -1,
        phase_l: 0,
        phase_r: 0,
    });
}

fn note_lost(event: &gst::EventRef) {
    let gst::EventView::CustomDownstream(_) = event.view() else {
        return;
    };
    let Some(structure) = event.structure() else {
        return;
    };
    if structure.name() != "GstRTPPacketLost" {
        return;
    }
    let frames = clock_frames(structure);
    let seq = structure
        .get::<u32>("seqnum")
        .map(|value| value as i32)
        .or_else(|_| structure.get::<i32>("seqnum"))
        .unwrap_or(-1);
    let (unix_us, gap_us) = {
        let Ok(mut clock) = clocks().lock() else {
            return;
        };
        stamp(&mut clock.lost)
    };
    send(Rec {
        kind: b'L',
        unix_us,
        gap_us,
        a: i64::from(frames),
        b: seq,
        frames: 0,
        zero_frames: 0,
        longest: 0,
        lead: 0,
        tail: 0,
        pts_ns: -1,
        dur_ns: -1,
        hole_ns: -1,
        run_ns: -1,
        seg_base: -1,
        seg_offset: -1,
        seg_start: -1,
        src_pos: -1,
        phase_l: 0,
        phase_r: 0,
    });
}

fn note_gap(event: &gst::EventRef) {
    let gst::EventView::Gap(gap) = event.view() else {
        return;
    };
    let (_ts, duration) = gap.get();
    let frames = duration.map(|time| frames_of_ns(time.nseconds())).unwrap_or(0);
    let (unix_us, gap_us) = {
        let Ok(mut clock) = clocks().lock() else {
            return;
        };
        stamp(&mut clock.gap)
    };
    send(Rec {
        kind: b'G',
        unix_us,
        gap_us,
        a: i64::from(frames),
        b: 0,
        frames: 0,
        zero_frames: 0,
        longest: 0,
        lead: 0,
        tail: 0,
        pts_ns: -1,
        dur_ns: -1,
        hole_ns: -1,
        run_ns: -1,
        seg_base: -1,
        seg_offset: -1,
        seg_start: -1,
        src_pos: -1,
        phase_l: 0,
        phase_r: 0,
    });
}

fn note_pcm(kind: u8, pad: &gst::Pad, bytes: &[u8], discont: bool, pts: i64, dur: i64) {
    let format = pad_format(pad);
    remember_format(kind, format);
    let scan = match format {
        b'B' => scan_s16(bytes, true),
        b'L' => scan_s16(bytes, false),
        b'F' => scan_f32(bytes),
        _ => Scan {
            frames: (bytes.len() / 4) as u32,
            zero_frames: 0,
            longest: 0,
            lead: 0,
            tail: 0,
        },
    };
    let (run_ns, seg_base, seg_offset, seg_start, src_pos) = if matches!(kind, b'S' | b'Z' | b'M') {
        segment_mark(pad, pts)
    } else {
        (-1, -1, -1, -1, -1)
    };
    let (unix_us, gap_us, hole_ns) = {
        let Ok(mut clock) = clocks().lock() else {
            return;
        };
        match kind {
            b'C' => {
                let stamped = stamp(&mut clock.conceal);
                let hole = timeline_hole(&mut clock.conceal_end, pts, dur);
                (stamped.0, stamped.1, hole)
            }
            b'S' => {
                let stamped = stamp(&mut clock.sink);
                let hole = timeline_hole(&mut clock.sink_end, pts, dur);
                (stamped.0, stamped.1, hole)
            }
            b'Z' => {
                let stamped = stamp(&mut clock.silence);
                let hole = timeline_hole(&mut clock.silence_end, pts, dur);
                (stamped.0, stamped.1, hole)
            }
            b'M' => {
                let stamped = stamp(&mut clock.mix);
                let hole = timeline_hole(&mut clock.mix_end, pts, dur);
                (stamped.0, stamped.1, hole)
            }
            _ => {
                let stamped = stamp(&mut clock.pcm);
                let hole = timeline_hole(&mut clock.pcm_end, pts, dur);
                (stamped.0, stamped.1, hole)
            }
        }
    };
    send(Rec {
        kind,
        unix_us,
        gap_us,
        a: i64::from(discont),
        b: i32::from(format),
        frames: scan.frames,
        zero_frames: scan.zero_frames,
        longest: scan.longest,
        lead: scan.lead,
        tail: scan.tail,
        pts_ns: pts,
        dur_ns: dur,
        hole_ns,
        run_ns,
        seg_base,
        seg_offset,
        seg_start,
        src_pos,
        phase_l: 0,
        phase_r: 0,
    });
}

/// Метка буфера в шкале сегмента и позиция src микшера.
///
/// audiomixer режет вход по running time, не по сырой метке. `-1` — сегмента нет.
fn segment_mark(pad: &gst::Pad, pts: i64) -> (i64, i64, i64, i64, i64) {
    let src_pos = pad
        .parent_element()
        .and_then(|element| element.static_pad("src"))
        .and_then(|src| src.query_position::<gst::ClockTime>())
        .map(|time| time.nseconds() as i64)
        .unwrap_or(-1);
    let Some(event) = pad.sticky_event::<gst::event::Segment>(0) else {
        return (-1, -1, -1, -1, src_pos);
    };
    let Some(segment) = event.segment().downcast_ref::<gst::ClockTime>() else {
        return (-1, -1, -1, -1, src_pos);
    };
    let run = if pts >= 0 {
        clock_ns(segment.to_running_time(gst::ClockTime::from_nseconds(pts as u64)))
    } else {
        -1
    };
    (
        run,
        clock_ns(segment.base()),
        clock_ns(segment.offset()),
        clock_ns(segment.start()),
        src_pos,
    )
}

fn clock_ns(value: Option<gst::ClockTime>) -> i64 {
    value.map(|time| time.nseconds() as i64).unwrap_or(-1)
}

/// Насколько `pts` ушёл вперёд от конца предыдущего буфера.
///
/// Конец предыдущего — его метка плюс его длительность. Ноль значит, что
/// шкала непрерывна. Отрицательное значение на первом буфере не считается.
fn timeline_hole(end: &mut Option<(i64, i64)>, pts: i64, dur: i64) -> i64 {
    let hole = match (*end, pts) {
        (Some((prev_pts, prev_dur)), pts) if prev_pts >= 0 && prev_dur >= 0 && pts >= 0 => {
            pts - (prev_pts + prev_dur)
        }
        _ => -1,
    };
    if pts >= 0 && dur >= 0 {
        *end = Some((pts, dur));
    } else if pts >= 0 {
        *end = None;
    }
    hole
}

fn remember_format(kind: u8, format: u8) {
    static SEEN: Mutex<[bool; 5]> = Mutex::new([false; 5]);
    let index = match kind {
        b'P' => 0,
        b'C' => 1,
        b'M' => 2,
        b'S' => 3,
        b'Z' => 4,
        _ => return,
    };
    let Ok(mut seen) = SEEN.lock() else {
        return;
    };
    if seen[index] {
        return;
    }
    seen[index] = true;
    tracing::info!(tap_kind = kind, sample_format = format, "формат наблюдения приёма");
}

fn pad_format(pad: &gst::Pad) -> u8 {
    let Some(caps) = pad.current_caps() else {
        return b'?';
    };
    let Some(structure) = caps.structure(0) else {
        return b'?';
    };
    match structure.get::<&str>("format").unwrap_or("") {
        "S16BE" => b'B',
        "S16LE" => b'L',
        "F32LE" => b'F',
        _ => b'?',
    }
}

fn clock_frames(structure: &gst::StructureRef) -> u32 {
    if let Ok(time) = structure.get::<gst::ClockTime>("duration") {
        return frames_of_ns(time.nseconds());
    }
    if let Ok(ns) = structure.get::<u64>("duration") {
        return frames_of_ns(ns);
    }
    0
}

fn frames_of_ns(ns: u64) -> u32 {
    ((u128::from(ns) * 48_000) / 1_000_000_000) as u32
}

fn rtp_payload(bytes: &[u8]) -> Option<(u16, u32, &[u8])> {
    if bytes.len() < 12 || bytes[0] >> 6 != 2 {
        return None;
    }
    let cc = (bytes[0] & 0x0f) as usize;
    let mut offset = 12 + cc * 4;
    if bytes[0] & 0x10 != 0 {
        if bytes.len() < offset + 4 {
            return None;
        }
        let words = u16::from_be_bytes([bytes[offset + 2], bytes[offset + 3]]) as usize;
        offset += 4 + words * 4;
    }
    if offset > bytes.len() {
        return None;
    }
    let mut end = bytes.len();
    if bytes[0] & 0x20 != 0 {
        let pad = *bytes.last()? as usize;
        if pad == 0 || pad > end - offset {
            return None;
        }
        end -= pad;
    }
    let seq = u16::from_be_bytes([bytes[2], bytes[3]]);
    let ts = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    Some((seq, ts, &bytes[offset..end]))
}

fn scan_s16(bytes: &[u8], big_endian: bool) -> Scan {
    let mut scan = Scan {
        frames: 0,
        zero_frames: 0,
        longest: 0,
        lead: 0,
        tail: 0,
    };
    let mut run = 0u32;
    let mut index = 0;
    let mut seen_loud = false;
    while index + 4 <= bytes.len() {
        let (left, right) = if big_endian {
            (
                i16::from_be_bytes([bytes[index], bytes[index + 1]]),
                i16::from_be_bytes([bytes[index + 2], bytes[index + 3]]),
            )
        } else {
            (
                i16::from_le_bytes([bytes[index], bytes[index + 1]]),
                i16::from_le_bytes([bytes[index + 2], bytes[index + 3]]),
            )
        };
        scan.frames += 1;
        if left.unsigned_abs() < THRESH as u16 && right.unsigned_abs() < THRESH as u16 {
            scan.zero_frames += 1;
            run += 1;
            if run > scan.longest {
                scan.longest = run;
            }
            if !seen_loud {
                scan.lead = run;
            }
            scan.tail = run;
        } else {
            seen_loud = true;
            run = 0;
            scan.tail = 0;
        }
        index += 4;
    }
    scan
}

fn scan_f32(bytes: &[u8]) -> Scan {
    let thresh = f32::from(THRESH) / 32768.0;
    let mut scan = Scan {
        frames: 0,
        zero_frames: 0,
        longest: 0,
        lead: 0,
        tail: 0,
    };
    let mut run = 0u32;
    let mut index = 0;
    let mut seen_loud = false;
    while index + 8 <= bytes.len() {
        let left = f32::from_le_bytes(bytes[index..index + 4].try_into().unwrap());
        let right = f32::from_le_bytes(bytes[index + 4..index + 8].try_into().unwrap());
        scan.frames += 1;
        if left.abs() < thresh && right.abs() < thresh {
            scan.zero_frames += 1;
            run += 1;
            if run > scan.longest {
                scan.longest = run;
            }
            if !seen_loud {
                scan.lead = run;
            }
            scan.tail = run;
        } else {
            seen_loud = true;
            run = 0;
            scan.tail = 0;
        }
        index += 8;
    }
    scan
}

fn logger(rx: mpsc::Receiver<Rec>) {
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o644)
        .open("/tmp/zvucast-rx-tap.log")
    {
        Ok(file) => file,
        Err(err) => {
            tracing::warn!("журнал приёма не открылся: {err}");
            return;
        }
    };
    let _ = writeln!(
        file,
        "# kind unix_us gap_us a b frames zero_frames longest lead tail pts_ns dur_ns hole_ns run_ns seg_base seg_offset seg_start src_pos phase_l phase_r"
    );
    let mut lines = 0u32;
    while let Ok(rec) = rx.recv() {
        if lines >= LINE_CAP {
            continue;
        }
        if writeln!(
            file,
            "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
            rec.kind as char,
            rec.unix_us,
            rec.gap_us,
            rec.a,
            rec.b,
            rec.frames,
            rec.zero_frames,
            rec.longest,
            rec.lead,
            rec.tail,
            rec.pts_ns,
            rec.dur_ns,
            rec.hole_ns,
            rec.run_ns,
            rec.seg_base,
            rec.seg_offset,
            rec.seg_start,
            rec.src_pos,
            rec.phase_l,
            rec.phase_r
        )
        .is_err()
        {
            return;
        }
        lines += 1;
        if lines % 64 == 0 {
            let _ = file.flush();
        }
    }
    let _ = file.flush();
}
