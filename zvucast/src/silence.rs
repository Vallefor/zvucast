use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Сколько громкости подряд нужно, чтобы снять холд.
pub const RESUME_AFTER: Duration = Duration::from_secs(2);
/// Телеметрия старше этого — как будто её нет.
pub const TELEM_STALE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sample {
    Loud,
    Quiet,
    NoData,
}

/// Loud, если любой конечный канал не тише порога; иначе Quiet.
/// Нет плеера, просрок телеметрии или оба канала ровно 0.0
/// (level ещё не измерил сигнал) → NoData. Таймер при этом стоит.
pub fn classify(age: Duration, playing: bool, rms_l: f64, rms_r: f64, threshold_db: f64) -> Sample {
    if !playing || age > TELEM_STALE || (rms_l == 0.0 && rms_r == 0.0) {
        return Sample::NoData;
    }
    let loud = |v: f64| v.is_finite() && v >= threshold_db;
    if loud(rms_l) || loud(rms_r) {
        Sample::Loud
    } else {
        Sample::Quiet
    }
}

/// Несколько источников одной зоны. Громкий любой — зона жива.
/// Тихий без громких копит холд. Данных нет, только если молчат все.
pub fn fold_samples(samples: &[Sample]) -> Sample {
    if samples.iter().any(|s| *s == Sample::Loud) {
        Sample::Loud
    } else if samples.iter().any(|s| *s == Sample::Quiet) {
        Sample::Quiet
    } else {
        Sample::NoData
    }
}

/// Часы одной зоны: тишина копит холд, громкость его снимает.
#[derive(Debug, Clone, Default)]
pub struct ZoneClock {
    pub held: bool,
    silence_since: Option<Instant>,
    loud_since: Option<Instant>,
}

impl ZoneClock {
    /// `true`, если `held` изменился.
    pub fn step(&mut self, now: Instant, sample: Sample, timeout: Duration) -> bool {
        if timeout.is_zero() {
            let was = self.held;
            *self = Self::default();
            return was;
        }
        match sample {
            Sample::NoData => false,
            Sample::Loud => {
                self.silence_since = None;
                if !self.held {
                    self.loud_since = None;
                    false
                } else {
                    let since = *self.loud_since.get_or_insert(now);
                    if now.duration_since(since) >= RESUME_AFTER {
                        self.held = false;
                        self.loud_since = None;
                        true
                    } else {
                        false
                    }
                }
            }
            Sample::Quiet => {
                self.loud_since = None;
                if self.held {
                    false
                } else {
                    let since = *self.silence_since.get_or_insert(now);
                    if now.duration_since(since) >= timeout {
                        self.held = true;
                        true
                    } else {
                        false
                    }
                }
            }
        }
    }
}

pub struct ZoneTick {
    pub id: String,
    pub active: bool,
    pub timeout: Duration,
    pub sample: Sample,
}

/// Часы всех зон. Id, которых нет во входе `recompute`, выкидываются.
#[derive(Debug, Default)]
pub struct SilenceBook {
    clocks: HashMap<String, ZoneClock>,
}

impl SilenceBook {
    /// Id зон, которые сейчас в холде.
    pub fn recompute(&mut self, now: Instant, zones: &[ZoneTick]) -> HashSet<String> {
        let mut seen = HashSet::new();
        let mut held = HashSet::new();
        for z in zones {
            seen.insert(z.id.clone());
            if !z.active {
                self.clocks.insert(z.id.clone(), ZoneClock::default());
                continue;
            }
            let clock = self.clocks.entry(z.id.clone()).or_default();
            clock.step(now, z.sample, z.timeout);
            if clock.held {
                held.insert(z.id.clone());
            }
        }
        self.clocks.retain(|id, _| seen.contains(id));
        held
    }
}

/// Член выкидывается, если его владелец в `held`.
/// Пустой список по tx в результат не кладётся.
pub fn apply_silence(
    assigned: HashMap<String, Vec<String>>,
    owners: &HashMap<String, String>,
    held: &HashSet<String>,
) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    for (tx, members) in assigned {
        let kept: Vec<String> = members
            .into_iter()
            .filter(|m| owners.get(m).is_none_or(|z| !held.contains(z)))
            .collect();
        if !kept.is_empty() {
            out.insert(tx, kept);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, Instant};

    #[test]
    fn classify_loud_quiet_and_nodata() {
        let fresh = Duration::from_secs(1);
        let stale = Duration::from_secs(11);
        assert_eq!(classify(fresh, true, -40.0, -90.0, -60.0), Sample::Loud);
        assert_eq!(classify(fresh, true, -90.0, f64::NEG_INFINITY, -60.0), Sample::Quiet);
        assert_eq!(classify(fresh, false, -10.0, -10.0, -60.0), Sample::NoData);
        assert_eq!(classify(stale, true, -90.0, -90.0, -60.0), Sample::NoData);
        assert_eq!(classify(fresh, true, 0.0, -90.0, -60.0), Sample::Loud);
        assert_eq!(classify(fresh, true, 0.0, 0.0, -60.0), Sample::NoData);
    }

    #[test]
    fn one_loud_source_keeps_the_zone_awake() {
        assert_eq!(
            fold_samples(&[Sample::Quiet, Sample::Loud, Sample::NoData]),
            Sample::Loud
        );
        assert_eq!(fold_samples(&[Sample::Quiet, Sample::NoData]), Sample::Quiet);
        assert_eq!(fold_samples(&[Sample::NoData, Sample::NoData]), Sample::NoData);
        assert_eq!(fold_samples(&[]), Sample::NoData);
    }

    #[test]
    fn hold_after_timeout_and_resume_after_two_seconds() {
        let mut c = ZoneClock::default();
        let t0 = Instant::now();
        assert!(!c.step(t0, Sample::Quiet, Duration::from_secs(300)));
        assert!(c.step(t0 + Duration::from_secs(300), Sample::Quiet, Duration::from_secs(300)));
        assert!(c.held);
        assert!(!c.step(t0 + Duration::from_secs(301), Sample::Loud, Duration::from_secs(300)));
        assert!(c.held, "один громкий сэмпл ещё не 2 секунды");
        assert!(c.step(t0 + Duration::from_secs(303), Sample::Loud, Duration::from_secs(300)));
        assert!(!c.held);
    }

    #[test]
    fn quiet_blip_resets_resume() {
        let mut c = ZoneClock { held: true, ..ZoneClock::default() };
        let t0 = Instant::now();
        c.step(t0, Sample::Loud, Duration::from_secs(300));
        c.step(t0 + Duration::from_secs(1), Sample::Quiet, Duration::from_secs(300));
        assert!(c.held);
        assert!(!c.step(t0 + Duration::from_secs(2), Sample::Loud, Duration::from_secs(300)));
    }

    #[test]
    fn zero_timeout_never_holds_and_clears() {
        let mut c = ZoneClock { held: true, ..ZoneClock::default() };
        let t0 = Instant::now();
        assert!(c.step(t0, Sample::Quiet, Duration::ZERO));
        assert!(!c.held);
    }

    #[test]
    fn nodata_does_not_move_the_clock() {
        let mut c = ZoneClock::default();
        let t0 = Instant::now();
        c.step(t0, Sample::Quiet, Duration::from_secs(10));
        assert!(!c.step(t0 + Duration::from_secs(100), Sample::NoData, Duration::from_secs(10)));
        assert!(!c.held);
    }

    #[test]
    fn book_two_zones_one_tx_and_inactive_clears() {
        let mut book = SilenceBook::default();
        let t0 = Instant::now();
        let tick = |id: &str, active: bool, secs: u64, sample: Sample| ZoneTick {
            id: id.into(), active, timeout: Duration::from_secs(secs), sample,
        };
        let held = book.recompute(t0, &[
            tick("A", true, 60, Sample::Quiet),
            tick("B", true, 300, Sample::Quiet),
        ]);
        assert!(held.is_empty());
        let held = book.recompute(t0 + Duration::from_secs(60), &[
            tick("A", true, 60, Sample::Quiet),
            tick("B", true, 300, Sample::Quiet),
        ]);
        assert_eq!(held, HashSet::from(["A".into()]));
        let held = book.recompute(t0 + Duration::from_secs(61), &[
            tick("A", false, 60, Sample::Quiet),
            tick("B", true, 300, Sample::Quiet),
        ]);
        assert!(held.is_empty(), "Stop снимает холд");
    }

    #[test]
    fn apply_silence_drops_only_held_owner() {
        let assigned = HashMap::from([
            ("tx1".into(), vec!["r1".into(), "r2".into()]),
        ]);
        let owners = HashMap::from([
            ("r1".into(), "A".into()),
            ("r2".into(), "B".into()),
        ]);
        let held = HashSet::from(["A".into()]);
        let out = apply_silence(assigned, &owners, &held);
        assert_eq!(out["tx1"], vec!["r2".to_string()]);
    }
}
