//! Выход усилителя. Пока приём играет, выбранная линия gpiochip0 высокая.
//! Номер в настройках — смещение линии. На Raspberry Pi 4 это номер BCM.

use std::fs::OpenOptions;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// Линии 40-контактной гребёнки Pi, которые можно отдать усилителю.
pub const LINE_MIN: u32 = 2;
pub const LINE_MAX: u32 = 27;

const CHIP: &str = "/dev/gpiochip0";
const OUTPUT: u64 = 1 << 3;
const ATTR_OUTPUT_VALUES: u32 = 2;
const GET_LINE: u64 = 0xC250_B407;
const SET_VALUES: u64 = 0xC010_B40F;

pub fn line_ok(line: u32) -> bool {
    (LINE_MIN..=LINE_MAX).contains(&line)
}

/// Высокий уровень только пока узел играет.
pub fn level_high(playing: bool) -> bool {
    playing
}

trait LineDrive {
    fn request(&mut self, line: u32) -> Result<(), String>;
    fn set(&mut self, high: bool) -> Result<(), String>;
    fn release(&mut self);
    fn is_open(&self) -> bool;
}

struct AmpDrive<L: LineDrive> {
    line: L,
    chosen: Option<u32>,
    high: bool,
    warned: bool,
}

impl<L: LineDrive> AmpDrive<L> {
    fn new(line: L) -> Self {
        Self { line, chosen: None, high: false, warned: false }
    }

    fn set_line(&mut self, next: Option<u32>) {
        if self.chosen == next && (next.is_none() || self.line.is_open()) {
            return;
        }
        self.line.release();
        self.high = false;
        self.warned = false;
        self.chosen = next;
        if let Some(n) = next {
            if let Err(e) = self.line.request(n) {
                tracing::warn!("gpio {n}: {e}");
                self.warned = true;
            }
        }
    }

    fn apply(&mut self, playing: bool) {
        let high = level_high(playing);
        if self.chosen.is_none() || !self.line.is_open() || self.high == high {
            return;
        }
        match self.line.set(high) {
            Ok(()) => {
                self.high = high;
                self.warned = false;
            }
            Err(e) => {
                if !self.warned {
                    tracing::warn!("gpio: {e}");
                    self.warned = true;
                }
            }
        }
    }

    fn clear(&mut self) {
        self.set_line(None);
    }
}

pub struct Amp {
    drive: AmpDrive<LinuxLine>,
}

impl Amp {
    pub fn new() -> Self {
        Self { drive: AmpDrive::new(LinuxLine { fd: None }) }
    }

    pub fn set_line(&mut self, line: Option<u32>) {
        self.drive.set_line(line);
    }

    pub fn apply(&mut self, playing: bool) {
        self.drive.apply(playing);
    }

    pub fn clear(&mut self) {
        self.drive.clear();
    }
}

impl Drop for Amp {
    fn drop(&mut self) {
        self.clear();
    }
}

struct LinuxLine {
    fd: Option<OwnedFd>,
}

impl LineDrive for LinuxLine {
    fn request(&mut self, line: u32) -> Result<(), String> {
        self.release();
        let fd = request_output(line).map_err(|e| e.to_string())?;
        self.fd = Some(fd);
        Ok(())
    }

    fn set(&mut self, high: bool) -> Result<(), String> {
        let Some(fd) = self.fd.as_ref() else {
            return Err("линия не открыта".into());
        };
        set_level(fd.as_raw_fd(), high).map_err(|e| e.to_string())
    }

    fn release(&mut self) {
        if self.fd.is_some() {
            let _ = self.set(false);
            self.fd = None;
        }
    }

    fn is_open(&self) -> bool {
        self.fd.is_some()
    }
}

impl Drop for LinuxLine {
    fn drop(&mut self) {
        self.release();
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct LineAttr {
    id: u32,
    padding: u32,
    values: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ConfigAttr {
    attr: LineAttr,
    mask: u64,
}

#[repr(C)]
struct LineConfig {
    flags: u64,
    num_attrs: u32,
    padding: [u32; 5],
    attrs: [ConfigAttr; 10],
}

#[repr(C)]
struct LineRequest {
    offsets: [u32; 64],
    consumer: [u8; 32],
    config: LineConfig,
    num_lines: u32,
    event_buffer_size: u32,
    padding: [u32; 5],
    fd: i32,
}

#[repr(C)]
struct LineValues {
    bits: u64,
    mask: u64,
}

fn request_output(line: u32) -> io::Result<OwnedFd> {
    let chip = OpenOptions::new().read(true).write(true).open(CHIP)?;
    let mut req = LineRequest {
        offsets: [0; 64],
        consumer: [0; 32],
        config: LineConfig {
            flags: OUTPUT,
            num_attrs: 1,
            padding: [0; 5],
            attrs: [ConfigAttr {
                attr: LineAttr { id: 0, padding: 0, values: 0 },
                mask: 0,
            }; 10],
        },
        num_lines: 1,
        event_buffer_size: 0,
        padding: [0; 5],
        fd: -1,
    };
    req.offsets[0] = line;
    let name = b"zvucast-amp";
    req.consumer[..name.len()].copy_from_slice(name);
    req.config.attrs[0].attr.id = ATTR_OUTPUT_VALUES;
    req.config.attrs[0].attr.values = 0;
    req.config.attrs[0].mask = 1;
    let rc = unsafe { libc::ioctl(chip.as_raw_fd(), GET_LINE as libc::c_ulong, &mut req) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(req.fd) })
}

fn set_level(fd: i32, high: bool) -> io::Result<()> {
    let mut values = LineValues { bits: u64::from(high), mask: 1 };
    let rc = unsafe { libc::ioctl(fd, SET_VALUES as libc::c_ulong, &mut values) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Fake {
        open: Option<u32>,
        level: Option<bool>,
        events: Vec<String>,
    }

    impl LineDrive for Fake {
        fn request(&mut self, line: u32) -> Result<(), String> {
            self.open = Some(line);
            self.level = Some(false);
            self.events.push(format!("request {line}"));
            Ok(())
        }

        fn set(&mut self, high: bool) -> Result<(), String> {
            self.level = Some(high);
            self.events.push(if high { "high".into() } else { "low".into() });
            Ok(())
        }

        fn release(&mut self) {
            if self.open.take().is_some() {
                let _ = self.set(false);
                self.events.push("release".into());
            }
        }

        fn is_open(&self) -> bool {
            self.open.is_some()
        }
    }

    #[test]
    fn header_lines_are_2_through_27() {
        assert!(!line_ok(0));
        assert!(!line_ok(1));
        assert!(line_ok(2));
        assert!(line_ok(27));
        assert!(!line_ok(28));
    }

    #[test]
    fn playing_drives_the_pin_high_and_silence_drives_it_low() {
        assert!(level_high(true));
        assert!(!level_high(false));
        let mut amp = AmpDrive::new(Fake::default());
        amp.apply(true);
        assert!(amp.line.events.is_empty(), "без линии уровень не трогаем");

        amp.set_line(Some(17));
        assert_eq!(amp.line.events, vec!["request 17".to_string()]);
        assert_eq!(amp.line.level, Some(false));

        amp.apply(false);
        amp.apply(true);
        amp.apply(true);
        amp.apply(false);
        assert_eq!(
            amp.line.events,
            vec!["request 17".to_string(), "high".to_string(), "low".to_string()]
        );

        amp.set_line(Some(18));
        amp.apply(true);
        assert_eq!(amp.line.open, Some(18));
        assert_eq!(amp.line.level, Some(true));
        assert!(amp.line.events.iter().any(|e| e == "release"));

        amp.clear();
        assert!(amp.line.open.is_none());
        assert_eq!(amp.line.level, Some(false));
    }

    #[test]
    fn gpio_v2_request_matches_the_kernel_abi() {
        assert_eq!(std::mem::size_of::<LineRequest>(), 592);
        assert_eq!(std::mem::size_of::<LineValues>(), 16);
        assert_eq!(std::mem::size_of::<LineConfig>(), 272);
        assert_eq!(std::mem::align_of::<LineRequest>(), 8);
        assert_eq!(GET_LINE, 0xC250_B407);
        assert_eq!(SET_VALUES, 0xC010_B40F);
    }
}
