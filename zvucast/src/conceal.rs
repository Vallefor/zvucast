#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    pub l: i16,
    pub r: i16,
}

pub const TAIL_MAX: usize = 1_920;
pub const HOLD: usize = 4_800;
pub const FADE: usize = 960;
pub const JOIN: usize = 96;
pub const RETURN_MAX: usize = 240;
pub const MIN_TAIL: usize = 192;
pub const SEAM_TAIL: usize = 384;

pub fn frames_of(duration_ns: u64) -> usize {
    ((duration_ns as u128 * 48_000 + 500_000_000) / 1_000_000_000) as usize
}

pub struct Conceal {
    ring: Vec<Frame>,
    hole_at: Option<usize>,
}

impl Conceal {
    pub fn new() -> Self {
        Self { ring: Vec::new(), hole_at: None }
    }

    pub fn clear(&mut self) {
        self.ring.clear();
        self.hole_at = None;
    }

    pub fn close_hole(&mut self) {
        self.hole_at = None;
    }

    pub fn push_gap(&mut self, frames: usize) -> Vec<Frame> {
        if frames == 0 {
            return Vec::new();
        }
        let start = self.hole_at.unwrap_or(0);
        let mut out = Vec::with_capacity(frames);
        for k in 0..frames {
            let h = start + k;
            out.push(scale(self.loop_frame(h), gain_at(h)));
        }
        self.hole_at = Some(start + frames);
        out
    }

    pub fn push_live(&mut self, frames: &[Frame], discont: bool) -> Vec<Frame> {
        if frames.is_empty() {
            return Vec::new();
        }
        if discont {
            self.clear();
            self.push_ring(frames);
            return frames.to_vec();
        }
        let out = if let Some(at) = self.hole_at {
            let m = frames.len().min(RETURN_MAX);
            let g = gain_at(at);
            let mut out = Vec::with_capacity(frames.len());
            if m == 1 {
                out.push(frames[0]);
            } else {
                for j in 0..m {
                    let live = j as f32 / (m - 1) as f32;
                    let concealed = scale(self.loop_frame(at + j), g);
                    out.push(mix(concealed, frames[j], live));
                }
            }
            if frames.len() > m {
                out.extend_from_slice(&frames[m..]);
            }
            self.hole_at = None;
            out
        } else {
            frames.to_vec()
        };
        self.push_ring(frames);
        out
    }

    fn push_ring(&mut self, frames: &[Frame]) {
        self.ring.extend_from_slice(frames);
        if self.ring.len() > TAIL_MAX {
            let drop_n = self.ring.len() - TAIL_MAX;
            self.ring.drain(0..drop_n);
        }
    }

    fn loop_frame(&self, k: usize) -> Frame {
        let n = self.ring.len();
        if n < MIN_TAIL {
            return Frame { l: 0, r: 0 };
        }
        if n < SEAM_TAIL {
            return self.ring[k % n];
        }
        let p = k % n;
        if p < JOIN {
            let g = p as f32 / (JOIN - 1) as f32;
            mix(self.ring[n - 1], self.ring[p], g)
        } else {
            self.ring[p]
        }
    }
}

fn gain_at(h: usize) -> f32 {
    if h < HOLD {
        1.0
    } else if h < HOLD + FADE {
        let i = h - HOLD;
        (FADE - 1 - i) as f32 / (FADE - 1) as f32
    } else {
        0.0
    }
}

fn scale(frame: Frame, g: f32) -> Frame {
    Frame { l: scale_s(frame.l, g), r: scale_s(frame.r, g) }
}

fn scale_s(sample: i16, g: f32) -> i16 {
    (sample as f32 * g).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

fn mix(a: Frame, b: Frame, live: f32) -> Frame {
    Frame { l: mix_s(a.l, b.l, live), r: mix_s(a.r, b.r, live) }
}

fn mix_s(a: i16, b: i16, live: f32) -> i16 {
    let v = a as f32 * (1.0 - live) + b as f32 * live;
    v.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wave(n: usize) -> Vec<Frame> {
        (0..n)
            .map(|i| {
                let l = (i as i32 - 960) as i16;
                Frame { l, r: l.wrapping_add(3) }
            })
            .collect()
    }

    fn near(a: Frame, b: Frame) {
        assert!((a.l as i32 - b.l as i32).abs() <= 1, "{a:?} vs {b:?}");
        assert!((a.r as i32 - b.r as i32).abs() <= 1, "{a:?} vs {b:?}");
    }

    #[test]
    fn one_packet_replays_the_old_edge_not_a_hundred_ms() {
        let mut c = Conceal::new();
        let src = wave(TAIL_MAX);
        assert_eq!(c.push_live(&src, false), src);
        let hole = c.push_gap(347);
        assert_eq!(hole.len(), 347);
        assert_eq!(hole[0], src[TAIL_MAX - 1]);
        assert_eq!(hole[95], src[95]);
        assert_eq!(hole[96], src[96]);
        let live = vec![Frame { l: 1000, r: 1003 }; 480];
        let out = c.push_live(&live, false);
        assert_eq!(out.len(), 480);
        assert_eq!(out[0], src[347]);
        assert_eq!(out[239], live[239]);
        assert_eq!(&out[240..], &live[240..]);
        let again = c.push_gap(10);
        assert_eq!(again[0], live[live.len() - 1]);
    }

    #[test]
    fn gaps_share_one_budget_then_fade() {
        let mut c = Conceal::new();
        let src = wave(TAIL_MAX);
        c.push_live(&src, false);
        let hold = c.push_gap(HOLD);
        assert_eq!(hold.len(), HOLD);
        assert_eq!(hold[0], src[TAIL_MAX - 1]);
        assert_ne!(hold[100], Frame { l: 0, r: 0 });
        let fade = c.push_gap(FADE);
        assert_eq!(fade[0], src[960]);
        assert_eq!(fade[FADE - 1], Frame { l: 0, r: 0 });
        let rest = c.push_gap(10);
        assert!(rest.iter().all(|f| *f == Frame { l: 0, r: 0 }));
    }

    #[test]
    fn live_joins_at_the_fade_gain() {
        for i in [0usize, 480, 959] {
            let mut c = Conceal::new();
            let src = wave(TAIL_MAX);
            c.push_live(&src, false);
            c.push_gap(HOLD + i);
            let g = (959 - i) as f32 / 959.0;
            let k = (HOLD + i) % TAIL_MAX;
            let base = if k < JOIN {
                let sg = k as f32 / 95.0;
                let last = src[TAIL_MAX - 1];
                let head = src[k];
                Frame {
                    l: (last.l as f32 * (1.0 - sg) + head.l as f32 * sg).round() as i16,
                    r: (last.r as f32 * (1.0 - sg) + head.r as f32 * sg).round() as i16,
                }
            } else {
                src[k]
            };
            let concealed = Frame {
                l: (base.l as f32 * g).round() as i16,
                r: (base.r as f32 * g).round() as i16,
            };
            let live = vec![Frame { l: 500, r: 503 }; 10];
            let out = c.push_live(&live, false);
            near(out[0], concealed);
            assert_eq!(out[9], live[9]);
        }
    }

    #[test]
    fn short_tail_is_silence_and_return_fades_in() {
        let mut c = Conceal::new();
        c.push_live(&wave(100), false);
        let hole = c.push_gap(50);
        assert!(hole.iter().all(|f| *f == Frame { l: 0, r: 0 }));
        let live = vec![Frame { l: 400, r: 403 }; RETURN_MAX];
        let out = c.push_live(&live, false);
        assert_eq!(out[0], Frame { l: 0, r: 0 });
        assert_eq!(out[RETURN_MAX - 1], live[RETURN_MAX - 1]);
    }

    #[test]
    fn mid_tail_loops_from_the_oldest_without_a_seam() {
        let mut c = Conceal::new();
        let src = wave(200);
        c.push_live(&src, false);
        let hole = c.push_gap(10);
        assert_eq!(hole[0], src[0]);
        assert_eq!(hole[1], src[1]);
    }

    #[test]
    fn seam_tail_matches_the_ends() {
        let mut c = Conceal::new();
        let src = wave(SEAM_TAIL);
        c.push_live(&src, false);
        let hole = c.push_gap(JOIN + 1);
        assert_eq!(hole[0], src[SEAM_TAIL - 1]);
        assert_eq!(hole[JOIN - 1], src[JOIN - 1]);
        assert_eq!(hole[JOIN], src[JOIN]);
    }

    #[test]
    fn zeros_do_not_open_a_hole_and_discont_forgets_the_tail() {
        let mut c = Conceal::new();
        let zeros = vec![Frame { l: 0, r: 0 }; TAIL_MAX];
        assert_eq!(c.push_live(&zeros, false), zeros);
        let src = wave(TAIL_MAX);
        c.push_live(&src, false);
        c.push_gap(10);
        let fresh = wave(4);
        assert_eq!(c.push_live(&fresh, true), fresh);
        let hole = c.push_gap(3);
        assert!(hole.iter().all(|f| *f == Frame { l: 0, r: 0 }));
    }

    #[test]
    fn clear_and_close_and_empty_gap() {
        let mut c = Conceal::new();
        c.push_live(&wave(TAIL_MAX), false);
        c.push_gap(10);
        c.clear();
        let live = vec![Frame { l: 7, r: 9 }; 8];
        assert_eq!(c.push_live(&live, false), live);

        let mut c = Conceal::new();
        c.push_live(&wave(TAIL_MAX), false);
        c.push_gap(10);
        c.close_hole();
        let live = vec![Frame { l: 7, r: 9 }; 8];
        assert_eq!(c.push_live(&live, false), live);

        let mut c = Conceal::new();
        c.push_live(&wave(TAIL_MAX), false);
        assert!(c.push_gap(0).is_empty());
        let live = vec![Frame { l: 7, r: 9 }; 8];
        assert_eq!(c.push_live(&live, false), live);
    }

    #[test]
    fn one_live_frame_is_not_mixed() {
        let mut c = Conceal::new();
        c.push_live(&wave(TAIL_MAX), false);
        c.push_gap(10);
        let live = vec![Frame { l: 42, r: 43 }];
        assert_eq!(c.push_live(&live, false), live);
    }

    #[test]
    fn frames_of_rounds_half_up() {
        assert_eq!(frames_of(7_229_167), 347);
        assert_eq!(frames_of(10_416), 0);
        assert_eq!(frames_of(10_417), 1);
        assert_eq!(frames_of(0), 0);
    }
}
