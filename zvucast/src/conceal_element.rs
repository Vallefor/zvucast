use std::sync::{Mutex, Once};

use gstreamer as gst;
use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;

use crate::conceal::{frames_of, Frame, Conceal};

const CHUNK: usize = 960;

glib::wrapper! {
    pub struct ConcealElement(ObjectSubclass<imp::ConcealElement>) @extends gst::Element, gst::Object;
}

pub fn register() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        gst::Element::register(None, "conceal", gst::Rank::NONE, ConcealElement::static_type())
            .expect("регистрация conceal");
    });
}

mod imp {
    use super::*;

    pub struct ConcealElement {
        sinkpad: gst::Pad,
        srcpad: gst::Pad,
        state: Mutex<State>,
    }

    struct State {
        rule: Conceal,
        pass: bool,
    }

    impl ConcealElement {
        fn sink_event(pad: &gst::Pad, parent: Option<&gst::Object>, event: gst::Event) -> bool {
            let Some(element) = parent.and_then(|p| p.downcast_ref::<super::ConcealElement>()) else {
                return false;
            };
            let imp = element.imp();
            let is_gap = matches!(event.view(), gst::EventView::Gap(_));
            if is_gap {
                if imp.state.lock().expect("conceal").pass {
                    return imp.srcpad.push_event(event);
                }
                let Some((ts, n)) = accepted_gap(&event) else {
                    return imp.srcpad.push_event(event);
                };
                let buffers = {
                    let mut st = imp.state.lock().expect("conceal");
                    let mut left = n;
                    let mut pts = ts;
                    let mut bufs = Vec::new();
                    while left > 0 {
                        let take = left.min(CHUNK);
                        let frames = st.rule.push_gap(take);
                        bufs.push(pack(&frames, Some(pts), Some(gst::ClockTime::from_nseconds(ns_of(take)))));
                        pts += gst::ClockTime::from_nseconds(ns_of(take));
                        left -= take;
                    }
                    bufs
                };
                for buf in buffers {
                    if imp.srcpad.push(buf).is_err() {
                        return false;
                    }
                }
                let _ = pad;
                return true;
            }
            let ty = event.type_();
            if ty == gst::EventType::Caps {
                let pass = {
                    let gst::EventView::Caps(caps) = event.view() else {
                        return false;
                    };
                    !caps_ok(caps.caps())
                };
                let mut st = imp.state.lock().expect("conceal");
                st.rule.clear();
                st.pass = pass;
                drop(st);
                return imp.srcpad.push_event(event);
            }
            if matches!(ty, gst::EventType::FlushStart | gst::EventType::FlushStop | gst::EventType::Segment) {
                imp.state.lock().expect("conceal").rule.clear();
            } else if ty == gst::EventType::Eos {
                imp.state.lock().expect("conceal").rule.close_hole();
            }
            imp.srcpad.push_event(event)
        }

        fn chain(
            _pad: &gst::Pad,
            parent: Option<&gst::Object>,
            buffer: gst::Buffer,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            let element = parent
                .and_then(|p| p.downcast_ref::<super::ConcealElement>())
                .ok_or(gst::FlowError::Error)?;
            let imp = element.imp();
            let readable = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
            let decoded = decode(&readable);
            let discont = buffer.flags().contains(gst::BufferFlags::DISCONT);
            let pts = buffer.pts();
            let dur = buffer.duration();
            drop(readable);
            let pass = imp.state.lock().expect("conceal").pass;
            if pass || decoded.is_none() {
                imp.state.lock().expect("conceal").rule.clear();
                return imp.srcpad.push(buffer);
            }
            let frames = decoded.unwrap();
            let out = imp.state.lock().expect("conceal").rule.push_live(&frames, discont);
            if out == frames {
                return imp.srcpad.push(buffer);
            }
            imp.srcpad.push(pack(&out, pts, dur))
        }
    }

    #[glib::object_subclass]
    impl ObjectSubclass for ConcealElement {
        const NAME: &'static str = "ZvucastConceal";
        type Type = super::ConcealElement;
        type ParentType = gst::Element;

        fn with_class(klass: &Self::Class) -> Self {
            let sinkpad = gst::Pad::builder_from_template(&klass.pad_template("sink").unwrap())
                .chain_function(ConcealElement::chain)
                .event_function(ConcealElement::sink_event)
                .build();
            let srcpad = gst::Pad::builder_from_template(&klass.pad_template("src").unwrap()).build();
            Self {
                sinkpad,
                srcpad,
                state: Mutex::new(State { rule: Conceal::new(), pass: false }),
            }
        }
    }

    impl ObjectImpl for ConcealElement {
        fn constructed(&self) {
            self.parent_constructed();
            let obj = self.obj();
            obj.add_pad(&self.sinkpad).unwrap();
            obj.add_pad(&self.srcpad).unwrap();
        }
    }

    impl GstObjectImpl for ConcealElement {}

    impl ElementImpl for ConcealElement {
        fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
            static META: std::sync::OnceLock<gst::subclass::ElementMetadata> = std::sync::OnceLock::new();
            Some(META.get_or_init(|| {
                gst::subclass::ElementMetadata::new(
                    "Zvucast conceal",
                    "Audio/Effect",
                    "Заполняет дырку RTP хвостом",
                    "zvucast",
                )
            }))
        }

        fn pad_templates() -> &'static [gst::PadTemplate] {
            static TEMPL: std::sync::OnceLock<[gst::PadTemplate; 2]> = std::sync::OnceLock::new();
            TEMPL.get_or_init(|| {
                let caps = gst::Caps::builder("audio/x-raw")
                    .field("format", "S16BE")
                    .field("rate", 48000i32)
                    .field("channels", 2i32)
                    .field("layout", "interleaved")
                    .build();
                let sink = gst::PadTemplate::new(
                    "sink",
                    gst::PadDirection::Sink,
                    gst::PadPresence::Always,
                    &caps,
                )
                .unwrap();
                let src = gst::PadTemplate::new(
                    "src",
                    gst::PadDirection::Src,
                    gst::PadPresence::Always,
                    &caps,
                )
                .unwrap();
                [sink, src]
            })
        }
    }

    fn caps_ok(caps: &gst::CapsRef) -> bool {
        let Some(s) = caps.structure(0) else {
            return false;
        };
        s.name() == "audio/x-raw"
            && s.get::<&str>("format").ok() == Some("S16BE")
            && s.get::<i32>("rate").ok() == Some(48_000)
            && s.get::<i32>("channels").ok() == Some(2)
    }

    fn accepted_gap(event: &gst::Event) -> Option<(gst::ClockTime, usize)> {
        let gst::EventView::Gap(gap) = event.view() else {
            return None;
        };
        let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| gap.get()));
        let (ts, dur) = parsed.ok()?;
        let dur = dur?;
        let n = frames_of(dur.nseconds());
        if n == 0 { None } else { Some((ts, n)) }
    }

    fn decode(data: &[u8]) -> Option<Vec<Frame>> {
        if data.len() % 4 != 0 {
            return None;
        }
        Some(
            data.chunks_exact(4)
                .map(|c| Frame {
                    l: i16::from_be_bytes([c[0], c[1]]),
                    r: i16::from_be_bytes([c[2], c[3]]),
                })
                .collect(),
        )
    }

    fn pack(frames: &[Frame], pts: Option<gst::ClockTime>, dur: Option<gst::ClockTime>) -> gst::Buffer {
        let mut buf = gst::Buffer::with_size(frames.len() * 4).unwrap();
        {
            let b = buf.get_mut().unwrap();
            b.set_pts(pts);
            b.set_duration(dur);
            let mut map = b.map_writable().unwrap();
            for (i, f) in frames.iter().enumerate() {
                let o = i * 4;
                map[o..o + 2].copy_from_slice(&f.l.to_be_bytes());
                map[o + 2..o + 4].copy_from_slice(&f.r.to_be_bytes());
            }
        }
        buf
    }

    fn ns_of(frames: usize) -> u64 {
        frames as u64 * 1_000_000_000 / 48_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conceal::{Frame, TAIL_MAX};
    use gstreamer_app as gst_app;

    fn raw_caps() -> gst::Caps {
        gst::Caps::builder("audio/x-raw")
            .field("format", "S16BE")
            .field("rate", 48000i32)
            .field("channels", 2i32)
            .field("layout", "interleaved")
            .build()
    }

    fn encode(frames: &[Frame]) -> Vec<u8> {
        let mut out = Vec::with_capacity(frames.len() * 4);
        for f in frames {
            out.extend_from_slice(&f.l.to_be_bytes());
            out.extend_from_slice(&f.r.to_be_bytes());
        }
        out
    }

    fn decode(data: &[u8]) -> Vec<Frame> {
        data.chunks_exact(4)
            .map(|c| Frame {
                l: i16::from_be_bytes([c[0], c[1]]),
                r: i16::from_be_bytes([c[2], c[3]]),
            })
            .collect()
    }

    fn tone(n: usize) -> Vec<Frame> {
        (0..n).map(|i| Frame { l: 1000 + i as i16, r: -(1000 + i as i16) }).collect()
    }

    fn pipe() -> (gst::Pipeline, gst_app::AppSrc, gst_app::AppSink) {
        gst::init().unwrap();
        register();
        let pipeline = gst::parse::launch(
            "appsrc name=src ! conceal name=c ! appsink name=sink sync=false async=false",
        )
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        let src = pipeline.by_name("src").unwrap().downcast::<gst_app::AppSrc>().unwrap();
        src.set_caps(Some(&raw_caps()));
        src.set_format(gst::Format::Time);
        let sink = pipeline.by_name("sink").unwrap().downcast::<gst_app::AppSink>().unwrap();
        pipeline.set_state(gst::State::Playing).unwrap();
        (pipeline, src, sink)
    }

    fn push_frames(src: &gst_app::AppSrc, frames: &[Frame], pts: gst::ClockTime, discont: bool) {
        // appsrc отдаёт буфер своим потоком. Ждём, пока он возьмёт буфер:
        // следующий push_event дырки тогда встанет за ним на stream-lock.
        let passed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = passed.clone();
        let pad = src.static_pad("src").unwrap();
        let id = pad.add_probe(gst::PadProbeType::BUFFER, move |_, _| {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            gst::PadProbeReturn::Ok
        }).expect("probe");
        let mut buf = gst::Buffer::from_mut_slice(encode(frames));
        {
            let b = buf.get_mut().unwrap();
            b.set_pts(pts);
            b.set_duration(gst::ClockTime::from_nseconds(frames.len() as u64 * 1_000_000_000 / 48_000));
            if discont {
                b.set_flags(gst::BufferFlags::DISCONT);
            }
        }
        src.push_buffer(buf).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !passed.load(std::sync::atomic::Ordering::SeqCst) {
            if std::time::Instant::now() > deadline {
                panic!("буфер не ушёл из appsrc");
            }
            std::thread::yield_now();
        }
        pad.remove_probe(id);
    }

    fn pull_all(sink: &gst_app::AppSink) -> Vec<(Vec<Frame>, Option<gst::ClockTime>)> {
        let mut out = Vec::new();
        while let Some(sample) = sink.try_pull_sample(gst::ClockTime::from_mseconds(200)) {
            let buf = sample.buffer().unwrap();
            let map = buf.map_readable().unwrap();
            out.push((decode(&map), buf.pts()));
        }
        out
    }

    #[test]
    fn long_gap_is_sliced_and_timestamped() {
        let (pipeline, src, sink) = pipe();
        push_frames(&src, &tone(TAIL_MAX), gst::ClockTime::ZERO, false);
        let gap_pts = gst::ClockTime::from_nseconds(TAIL_MAX as u64 * 1_000_000_000 / 48_000);
        let dur = gst::ClockTime::from_mseconds(100);
        src.static_pad("src").unwrap().push_event(gst::event::Gap::new(gap_pts, dur));
        let buffers = pull_all(&sink);
        pipeline.set_state(gst::State::Null).unwrap();
        let gaps: Vec<_> = buffers.into_iter().skip(1).collect();
        assert!(gaps.len() >= 5, "100 мс режутся по 20 мс, кусков {n}", n = gaps.len());
        assert!(gaps.iter().all(|(f, _)| f.len() <= 960));
        assert_eq!(gaps[0].1, Some(gap_pts));
        let step = gst::ClockTime::from_nseconds(960 * 1_000_000_000 / 48_000);
        assert_eq!(gaps[1].1, Some(gap_pts + step));
        assert!(gaps[0].0.iter().any(|f| f.l != 0));
    }

    #[test]
    fn flush_drops_the_tail_and_eos_does_not_extend_the_hole() {
        let (pipeline, src, sink) = pipe();
        push_frames(&src, &tone(TAIL_MAX), gst::ClockTime::ZERO, false);
        let pad = src.static_pad("src").unwrap();
        pad.push_event(gst::event::Gap::new(
            gst::ClockTime::from_mseconds(40),
            gst::ClockTime::from_mseconds(10),
        ));
        pad.push_event(gst::event::FlushStart::new());
        pad.push_event(gst::event::FlushStop::new(true));
        let mark = vec![Frame { l: 7, r: 9 }; 16];
        push_frames(&src, &mark, gst::ClockTime::from_mseconds(80), false);
        let buffers = pull_all(&sink);
        pipeline.set_state(gst::State::Null).unwrap();
        let last = buffers.last().unwrap();
        assert_eq!(last.0, mark);

        let (pipeline, src, sink) = pipe();
        push_frames(&src, &tone(TAIL_MAX), gst::ClockTime::ZERO, false);
        src.static_pad("src").unwrap().push_event(gst::event::Gap::new(
            gst::ClockTime::from_mseconds(40),
            gst::ClockTime::from_mseconds(10),
        ));
        src.end_of_stream().unwrap();
        let buffers = pull_all(&sink);
        pipeline.set_state(gst::State::Null).unwrap();
        let frames: usize = buffers.iter().map(|(f, _)| f.len()).sum();
        assert_eq!(frames, TAIL_MAX + 480);
    }

    #[test]
    fn discont_buffer_is_not_crossfaded() {
        let (pipeline, src, sink) = pipe();
        push_frames(&src, &tone(TAIL_MAX), gst::ClockTime::ZERO, false);
        src.static_pad("src").unwrap().push_event(gst::event::Gap::new(
            gst::ClockTime::from_mseconds(40),
            gst::ClockTime::from_mseconds(10),
        ));
        let mark = vec![Frame { l: 11, r: 13 }; 32];
        push_frames(&src, &mark, gst::ClockTime::from_mseconds(60), true);
        let buffers = pull_all(&sink);
        pipeline.set_state(gst::State::Null).unwrap();
        assert_eq!(buffers.last().unwrap().0, mark);
    }

    #[test]
    fn dropped_rtp_keeps_energy_and_a_long_hole_ends_in_silence() {
        gst::init().unwrap();
        register();
        let pipeline = gst::parse::launch(
            "audiotestsrc is-live=true wave=sine freq=440 volume=0.8 num-buffers=80 \
             ! audio/x-raw,format=S16BE,rate=48000,channels=2,layout=interleaved \
             ! rtpL16pay mtu=1400 \
             ! identity name=drop \
             ! rtpjitterbuffer latency=40 do-lost=true \
             ! rtpL16depay \
             ! conceal name=c \
             ! appsink name=sink sync=true async=false",
        )
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        let n = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let n2 = n.clone();
        pipeline.by_name("drop").unwrap().static_pad("src").unwrap().add_probe(
            gst::PadProbeType::BUFFER,
            move |_, _| {
                let i = n2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i == 15 || (40..=70).contains(&i) {
                    gst::PadProbeReturn::Drop
                } else {
                    gst::PadProbeReturn::Ok
                }
            },
        );
        let sink = pipeline.by_name("sink").unwrap().downcast::<gst_app::AppSink>().unwrap();
        pipeline.set_state(gst::State::Playing).unwrap();
        let bus = pipeline.bus().unwrap();
        let mut pcm = Vec::<Frame>::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if std::time::Instant::now() > deadline {
                panic!("нет EOS за 15 с");
            }
            while let Some(sample) = sink.try_pull_sample(gst::ClockTime::ZERO) {
                let buf = sample.buffer().unwrap();
                let map = buf.map_readable().unwrap();
                pcm.extend(decode(&map));
            }
            match bus.timed_pop_filtered(
                gst::ClockTime::from_mseconds(100),
                &[gst::MessageType::Eos, gst::MessageType::Error],
            ) {
                Some(msg) if msg.type_() == gst::MessageType::Error => {
                    panic!("pipeline error: {msg:?}");
                }
                Some(_) => break,
                None => {}
            }
        }
        while let Some(sample) = sink.try_pull_sample(gst::ClockTime::ZERO) {
            let buf = sample.buffer().unwrap();
            let map = buf.map_readable().unwrap();
            pcm.extend(decode(&map));
        }
        pipeline.set_state(gst::State::Null).unwrap();
        assert!(pcm.len() > 48_000 / 10, "звук доехал");
        let mut runs = Vec::new();
        let mut run = 0usize;
        for frame in &pcm {
            if frame.l == 0 && frame.r == 0 {
                run += 1;
            } else if run > 0 {
                runs.push(run);
                run = 0;
            }
        }
        if run > 0 {
            runs.push(run);
        }
        assert!(runs.iter().any(|n| *n >= 2_400), "после длинной дырки есть ≥50 мс нулей: {runs:?}");
        assert!(
            runs.iter().all(|n| *n < 300 || *n > 2_000),
            "один потерянный пакет не стал полосой нулей: {runs:?}"
        );
    }
}
