//! Диагностика передачи. По умолчанию выключена; включается `ZVUCAST_TX_TAP=1`.

use gstreamer as gst;
use gstreamer::prelude::*;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::mpsc::{self, SyncSender};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub struct CaptureTimes {
    reads: VecDeque<(u64, usize)>,
}

impl CaptureTimes {
    pub fn note(&mut self, at_us: u64, len: usize) {
        self.reads.push_back((at_us, len));
    }

    pub fn first_time(&self) -> Option<u64> {
        self.reads.front().map(|(at_us, _)| *at_us)
    }

    pub fn consume(&mut self, mut len: usize) {
        while len > 0 {
            let Some((_, left)) = self.reads.front_mut() else {
                break;
            };
            let used = len.min(*left);
            *left -= used;
            len -= used;
            if *left == 0 {
                self.reads.pop_front();
            }
        }
    }

    pub fn clear(&mut self) {
        self.reads.clear();
    }
}

pub fn normalized_hash(bytes: &[u8], big_endian: bool) -> u64 {
    let mut hash = 14_695_981_039_346_655_537u64;
    for chunk in bytes.chunks_exact(2) {
        let sample = if big_endian {
            i16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            i16::from_le_bytes([chunk[0], chunk[1]])
        };
        hash ^= u64::from(sample as u16);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

pub fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

struct Rec {
    kind: u8,
    at_us: u64,
    capture_us: u64,
    hash: u64,
    level: u32,
    pts_ns: u64,
    dur_ns: u64,
    tag: String,
}

fn sender() -> &'static SyncSender<Rec> {
    static TX: OnceLock<SyncSender<Rec>> = OnceLock::new();
    TX.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel(2048);
        let _ = std::thread::Builder::new()
            .name("tx-tap".into())
            .spawn(move || logger(rx));
        tx
    })
}

fn send(rec: Rec) {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    if !*ENABLED.get_or_init(|| std::env::var("ZVUCAST_TX_TAP").ok().is_some_and(|v| v == "1")) {
        return;
    }
    let _ = sender().try_send(rec);
}

pub fn note_capture(tag: String, capture_us: u64, pts_ns: u64, dur_ns: u64, bytes: &[u8]) {
    send(Rec {
        kind: b'F',
        at_us: unix_us(),
        capture_us,
        hash: normalized_hash(bytes, false),
        level: normalized_level(bytes, false),
        pts_ns,
        dur_ns,
        tag,
    });
}

pub fn watch_rtp(pad: &gst::Pad, kind: u8) {
    let _ = pad.add_probe(gst::PadProbeType::BUFFER, move |pad, info| {
        if let Some(buffer) = info.buffer() {
            if let Ok(map) = buffer.map_readable() {
                if let Some((_seq, ts, payload)) = crate::rx_tap::rtp_payload(map.as_slice()) {
                    let tag = pad
                        .parent_element()
                        .map(|element| element.name().to_string())
                        .unwrap_or_default();
                    send(Rec {
                        kind,
                        at_us: unix_us(),
                        capture_us: 0,
                        hash: normalized_hash(payload, true),
                        level: normalized_level(payload, true),
                        pts_ns: u64::from(ts),
                        dur_ns: 0,
                        tag,
                    });
                }
            }
        }
        gst::PadProbeReturn::Ok
    });
}

fn logger(rx: mpsc::Receiver<Rec>) {
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("/tmp/zvucast-tx-tap.log")
    {
        Ok(file) => file,
        Err(err) => {
            tracing::warn!("журнал передачи не открылся: {err}");
            return;
        }
    };
    let _ = writeln!(file, "# kind unix_us capture_us hash level pts_ns dur_ns tag");
    let mut lines = 0u32;
    while let Ok(rec) = rx.recv() {
        if lines >= 200_000 {
            continue;
        }
        if writeln!(
            file,
            "{} {} {} {:016x} {} {} {} {}",
            rec.kind as char, rec.at_us, rec.capture_us, rec.hash, rec.level, rec.pts_ns, rec.dur_ns, rec.tag
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

pub fn normalized_level(bytes: &[u8], big_endian: bool) -> u32 {
    let mut total = 0u64;
    let mut count = 0u64;
    for chunk in bytes.chunks_exact(2) {
        let sample = if big_endian {
            i16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            i16::from_le_bytes([chunk[0], chunk[1]])
        };
        total += u64::from(sample.unsigned_abs() as u32);
        count += 1;
    }
    if count == 0 { 0 } else { (total / count) as u32 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_times_track_the_first_read_of_a_frame() {
        let mut times = CaptureTimes::default();
        times.note(100, 512);
        times.note(200, 512);
        assert_eq!(times.first_time(), Some(100));
        times.consume(960);
        assert_eq!(times.first_time(), Some(200));
        times.consume(64);
        assert_eq!(times.first_time(), None);
    }

    #[test]
    fn pcm_hash_does_not_depend_on_endianness() {
        let samples = [1i16, -2, 300];
        let little: Vec<u8> = samples.iter().flat_map(|v| v.to_le_bytes()).collect();
        let big: Vec<u8> = samples.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(normalized_hash(&little, false), normalized_hash(&big, true));
        assert_ne!(normalized_hash(&little, false), normalized_hash(&big[..4], true));
    }

    #[test]
    fn pcm_level_does_not_depend_on_endianness() {
        let samples = [1i16, -2, 300];
        let little: Vec<u8> = samples.iter().flat_map(|v| v.to_le_bytes()).collect();
        let big: Vec<u8> = samples.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(normalized_level(&little, false), 101);
        assert_eq!(normalized_level(&little, false), normalized_level(&big, true));
    }
}
