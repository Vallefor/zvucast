use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};
use zproto::{SetupIssue, SoloistState};

pub const PKG_UNSUPPORTED: &str =
    "эта система не из семейства Debian или Arch — пакеты этой командой не ставятся";

const OS_RELEASE: &str = "/etc/os-release";
const AGENT_UNIT: &str = "/etc/systemd/system/zvucast-agent.service";
const LIMITS: &str = "/etc/security/limits.conf";
const SOLOIST_BIN: &str = "/usr/local/bin/soloist";
const PW_RECORD: &str = "/usr/bin/pw-record";
const PW_PLAY: &str = "/usr/bin/pw-play";
const QUANTUM_CONF: &str = "/etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf";
const LATENCY_CONF: &str = "/etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf";

const QUANTUM_MARKS: &[&str] = &[
    "Часы машины не запираем",
    "default.clock.rate = 48000",
    "default.clock.allowed-rates = [ 48000 ]",
];
/// Общий период 128, период 256 у HiFiBerry, запас наушников bcm2835.
const LATENCY_MARKS: &[&str] = &[
    "api.alsa.period-size = 128",
    "api.alsa.headroom = 0",
    "api.alsa.disable-batch = true",
    "alsa.card_name = \"bcm2835 Headphones\"",
    "api.alsa.period-size = 1024",
    "api.alsa.headroom = 2048",
    "api.alsa.disable-batch = false",
    "alsa.card_name = \"snd_rpi_hifiberry_dac\"",
    "HiFiBerry ест период 256",
    "api.alsa.period-num = 20",
    "api.alsa.headroom = 1920",
    "alsa.card_name = \"USB Audio CODEC\"",
    "api.alsa.period-size = 960",
    "api.alsa.period-num = 16",
    "api.alsa.headroom = 1920",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Apt,
    Pacman,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cmd {
    pub code: Option<i32>,
    pub stdout: String,
}

pub trait Host: Sync {
    fn read_to_string(&self, path: &str) -> Option<String>;
    fn executable(&self, path: &str) -> bool;
    fn run(&self, secs: u64, program: &str, args: &[String]) -> Cmd;
    fn uname_m(&self) -> String;
    fn wireless_ifaces(&self) -> Vec<String>;
    /// `limits.conf` и `limits.d/*.conf` одним текстом. Каталога может не быть.
    fn limits_text(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupSnapshot {
    pub setup: Vec<SetupIssue>,
    pub soloist: SoloistState,
    pub soloist_command: String,
    pub ready: bool,
}

impl SetupSnapshot {
    pub fn pending() -> Self {
        Self {
            setup: Vec::new(),
            soloist: SoloistState::Unknown,
            soloist_command: String::new(),
            ready: false,
        }
    }
}

pub struct SystemHost;

impl Host for SystemHost {
    fn read_to_string(&self, path: &str) -> Option<String> {
        match std::fs::read_to_string(path) {
            Ok(text) => Some(text),
            // Сокет PipeWire существует, но текстом не читается.
            Err(_) if std::path::Path::new(path).exists() => Some(String::new()),
            Err(_) => None,
        }
    }

    fn executable(&self, path: &str) -> bool {
        std::fs::metadata(path)
            .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }

    fn run(&self, secs: u64, program: &str, args: &[String]) -> Cmd {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let argv = timeout_argv(secs, program, &refs);
        let mut command = Command::new(&argv[0]);
        command.args(&argv[1..]);
        match command.output() {
            Ok(output) => cmd_from_output(output),
            Err(_) => Cmd {
                code: None,
                stdout: String::new(),
            },
        }
    }

    fn uname_m(&self) -> String {
        self.run(2, "uname", &["-m".to_string()])
            .stdout
            .trim()
            .to_string()
    }

    fn wireless_ifaces(&self) -> Vec<String> {
        let mut names = Vec::new();
        let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
            return names;
        };
        for entry in entries.flatten() {
            if entry.path().join("wireless").is_dir() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        names
    }

    fn limits_text(&self) -> String {
        let mut chunks = Vec::new();
        if let Some(text) = self.read_to_string(LIMITS) {
            chunks.push(text);
        }
        let Ok(entries) = std::fs::read_dir("/etc/security/limits.d") else {
            return chunks.join("\n");
        };
        let mut paths: Vec<_> = entries.flatten().map(|entry| entry.path()).collect();
        paths.sort();
        for path in paths {
            if path.extension().and_then(|ext| ext.to_str()) != Some("conf") {
                continue;
            }
            let Some(path) = path.to_str() else {
                continue;
            };
            if let Some(text) = self.read_to_string(path) {
                chunks.push(text);
            }
        }
        chunks.join("\n")
    }
}

fn cmd_from_output(output: Output) -> Cmd {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    // timeout --signal=KILL: 124, либо 137 если процесс убит SIGKILL.
    let code = match output.status.code() {
        Some(124) | Some(137) | None => None,
        Some(code) => Some(code),
    };
    Cmd { code, stdout }
}

pub fn family_of(os_release: &str) -> Family {
    let id = os_field(os_release, "ID");
    let like = os_field(os_release, "ID_LIKE");
    let words: Vec<&str> = like.split_whitespace().collect();
    let apt = matches!(id.as_str(), "debian" | "ubuntu" | "raspbian")
        || words
            .iter()
            .any(|word| *word == "debian" || *word == "ubuntu");
    let pacman =
        matches!(id.as_str(), "arch" | "cachyos") || words.iter().any(|word| *word == "arch");
    match (apt, pacman) {
        (true, false) => Family::Apt,
        (false, true) => Family::Pacman,
        _ => Family::Other,
    }
}

fn os_field(text: &str, key: &str) -> String {
    for line in text.lines() {
        let Some((name, value)) = line.trim().split_once('=') else {
            continue;
        };
        if name == key {
            return unquote(value.trim());
        }
    }
    String::new()
}

fn unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() >= 2 {
        let open = bytes[0];
        let close = bytes[bytes.len() - 1];
        if (open == b'"' && close == b'"') || (open == b'\'' && close == b'\'') {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

pub fn audio_uid(unit_text: &str) -> u32 {
    const MARK: &str = "XDG_RUNTIME_DIR=/run/user/";
    let Some(at) = unit_text.find(MARK) else {
        return 1000;
    };
    let rest = &unit_text[at + MARK.len()..];
    let token_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let token = rest[..token_len].trim_matches(|ch: char| ch == '"' || ch == '\'');
    token.parse().unwrap_or(1000)
}

pub fn soloist_arch(uname_m: &str) -> Option<&'static str> {
    match uname_m.trim() {
        "x86_64" => Some("x86_64"),
        "aarch64" => Some("arm64"),
        "armv7l" => Some("arm32"),
        _ => None,
    }
}

pub fn soloist_command(arch: &str) -> String {
    format!(
        "tmp=$(mktemp -d) && curl --fail --location --max-time 90 -o \"$tmp/soloist.tar.gz\" \"https://soloist-builds.spotifycdn.com/soloist_release_{arch}.tar.gz\" && tar -xzf \"$tmp/soloist.tar.gz\" -C \"$tmp\" && sudo install -m 755 \"$tmp/soloist\" /usr/local/bin/soloist && soloist --version"
    )
}

pub fn pipewire_command(family: Family, user: &str, uid: u32) -> String {
    let tail = format!(
        "sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service && sudo -u {user} XDG_RUNTIME_DIR=/run/user/{uid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus systemctl --user enable --now pipewire.service pipewire-pulse.service wireplumber.service"
    );
    match family {
        Family::Apt => format!(
            "sudo apt-get install -y pipewire pipewire-bin pipewire-pulse wireplumber && {tail}"
        ),
        Family::Pacman => format!(
            "sudo pacman -S --needed --noconfirm pipewire pipewire-pulse wireplumber && {tail}"
        ),
        Family::Other => PKG_UNSUPPORTED.to_string(),
    }
}

pub fn gstreamer_command(family: Family) -> String {
    match family {
        Family::Apt => "sudo apt-get install -y gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad".to_string(),
        Family::Pacman => "sudo pacman -S --needed --noconfirm gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad".to_string(),
        Family::Other => PKG_UNSUPPORTED.to_string(),
    }
}

pub fn linger_command(user: &str, uid: u32) -> String {
    format!("sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service")
}

pub fn limits_command(user: &str) -> String {
    format!(
        "grep -q '@audio - rtprio 95' /etc/security/limits.conf || printf '%s\\n' '@audio - rtprio 95' '@audio - memlock unlimited' | sudo tee -a /etc/security/limits.conf; sudo usermod -aG audio {user}"
    )
}

pub fn wifi_command(family: Family, iface: &str, iw_missing: bool) -> String {
    if !iw_missing {
        return format!("sudo iw dev {iface} set power_save off");
    }
    match family {
        Family::Apt => {
            format!("sudo apt-get install -y iw && sudo iw dev {iface} set power_save off")
        }
        Family::Pacman => format!(
            "sudo pacman -S --needed --noconfirm iw && sudo iw dev {iface} set power_save off"
        ),
        Family::Other => PKG_UNSUPPORTED.to_string(),
    }
}

pub fn controller_command() -> &'static str {
    "sudo systemctl enable --now zvucast-controller"
}

const QUANTUM_BODY: &str = include_str!("../../deploy/pipewire/99-zvucast-quantum.conf");
const LATENCY_BODY: &str = include_str!("../../deploy/wireplumber/99-zvucast-latency.conf");

/// Пишет оба дропа и перезапускает пользовательский PipeWire.
/// Текст файлов — тот же, что кладёт установщик.
pub fn quantum_command(user: &str, uid: u32) -> String {
    format!(
        "sudo mkdir -p -- /etc/pipewire/pipewire.conf.d /etc/wireplumber/wireplumber.conf.d\n\
         sudo tee -- /etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf >/dev/null <<'ZVUCAST_QUANTUM'\n\
         {quantum}\
         ZVUCAST_QUANTUM\n\
         sudo tee -- /etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf >/dev/null <<'ZVUCAST_LATENCY'\n\
         {latency}\
         ZVUCAST_LATENCY\n\
         sudo -u {user} XDG_RUNTIME_DIR=/run/user/{uid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus systemctl --user restart pipewire.service pipewire-pulse.service wireplumber.service",
        quantum = QUANTUM_BODY,
        latency = LATENCY_BODY,
    )
}

fn marks_ok(text: &str, marks: &[&str]) -> bool {
    marks.iter().all(|mark| text.contains(mark))
}

/// Дропы установщика на месте: часы машины не заперты, частота 48 кГц,
/// период 256 у HiFiBerry, большой буфер у встроенного джека Pi.
/// Старый файл с `clock.quantum` или без правила Headphones — нет.
fn quanta_ok(quantum: Option<&str>, latency: Option<&str>) -> bool {
    match (quantum, latency) {
        (Some(quantum), Some(latency)) => {
            marks_ok(quantum, QUANTUM_MARKS) && marks_ok(latency, LATENCY_MARKS)
        }
        _ => false,
    }
}

pub fn timeout_argv(secs: u64, program: &str, args: &[&str]) -> Vec<String> {
    let mut argv = Vec::with_capacity(4 + args.len());
    argv.push("timeout".to_string());
    argv.push("--signal=KILL".to_string());
    argv.push(secs.to_string());
    argv.push(program.to_string());
    argv.extend(args.iter().map(|arg| (*arg).to_string()));
    argv
}

pub fn inspect(host: &(impl Host + Sync)) -> SetupSnapshot {
    let (os_release, unit_text, uname) = std::thread::scope(|scope| {
        let os = scope.spawn(|| host.read_to_string(OS_RELEASE).unwrap_or_default());
        let unit = scope.spawn(|| host.read_to_string(AGENT_UNIT).unwrap_or_default());
        let uname = scope.spawn(|| host.uname_m());
        (
            os.join().expect("os-release"),
            unit.join().expect("unit"),
            uname.join().expect("uname"),
        )
    });

    let family = family_of(&os_release);
    let uid = audio_uid(&unit_text);
    let mut ifaces = host.wireless_ifaces();
    ifaces.sort();

    let probed = std::thread::scope(|scope| {
        let passwd = scope.spawn(|| host.run(2, "getent", &owned(&["passwd", &uid.to_string()])));
        let pipewire = scope.spawn(|| user_service(host, uid, "pipewire"));
        let wireplumber = scope.spawn(|| user_service(host, uid, "wireplumber"));
        let pulse = scope.spawn(|| user_service(host, uid, "pipewire-pulse"));
        let pw_record = scope.spawn(|| host.executable(PW_RECORD));
        let pw_play = scope.spawn(|| host.executable(PW_PLAY));
        let appsrc = scope.spawn(|| host.run(2, "gst-inspect-1.0", &owned(&["appsrc"])));
        let linger = scope.spawn(|| {
            host.run(
                2,
                "loginctl",
                &owned(&["show-user", &uid.to_string(), "-p", "Linger"]),
            )
        });
        let socket = scope.spawn(|| {
            host.read_to_string(&format!("/run/user/{uid}/pipewire-0"))
                .is_some()
        });
        let quantum = scope.spawn(|| host.read_to_string(QUANTUM_CONF));
        let latency = scope.spawn(|| host.read_to_string(LATENCY_CONF));
        let limits = scope.spawn(|| host.limits_text());
        let enabled = scope.spawn(|| {
            host.run(
                2,
                "systemctl",
                &owned(&["is-enabled", "zvucast-controller"]),
            )
        });
        let active =
            scope.spawn(|| host.run(2, "systemctl", &owned(&["is-active", "zvucast-controller"])));
        let wifi: Vec<_> = ifaces
            .iter()
            .map(|iface| {
                let iface = iface.clone();
                scope.spawn(move || {
                    let cmd = host.run(2, "iw", &owned(&["dev", &iface, "get", "power_save"]));
                    (iface, cmd)
                })
            })
            .collect();
        let soloist = scope.spawn(|| {
            if host.executable(SOLOIST_BIN) {
                Some(host.run(3, "soloist", &owned(&["--version"])))
            } else {
                None
            }
        });

        Probes {
            passwd: passwd.join().expect("getent"),
            pipewire: pipewire.join().expect("pipewire"),
            wireplumber: wireplumber.join().expect("wireplumber"),
            pulse: pulse.join().expect("pipewire-pulse"),
            pw_tools: pw_record.join().expect("pw-record") && pw_play.join().expect("pw-play"),
            appsrc: appsrc.join().expect("appsrc"),
            linger: linger.join().expect("linger"),
            socket: socket.join().expect("socket"),
            quantum: quantum.join().expect("quantum"),
            latency: latency.join().expect("latency"),
            limits: limits.join().expect("limits"),
            enabled: enabled.join().expect("enabled"),
            active: active.join().expect("active"),
            wifi: wifi
                .into_iter()
                .map(|handle| handle.join().expect("wifi"))
                .collect(),
            soloist: soloist.join().expect("soloist"),
        }
    });

    let user = user_name(&probed.passwd, uid);
    let mut setup = Vec::new();
    let packages = pipewire_command(family, &user, uid);
    if !is_running(&probed.pipewire) {
        setup.push(issue("pipewire", packages.clone()));
    }
    if !is_running(&probed.wireplumber) {
        setup.push(issue("wireplumber", packages.clone()));
    }
    if !is_running(&probed.pulse) {
        setup.push(issue("pipewire-pulse", packages.clone()));
    }
    if !probed.pw_tools {
        setup.push(issue("pw-cat", packages.clone()));
    }
    if probed.appsrc.code != Some(0) {
        setup.push(issue("gstreamer", gstreamer_command(family)));
    }
    if !probed.linger.stdout.contains("Linger=yes") {
        setup.push(issue("linger", linger_command(&user, uid)));
    }
    if !probed.socket {
        setup.push(issue("pipewire-socket", packages));
    }
    if !quanta_ok(probed.quantum.as_deref(), probed.latency.as_deref()) {
        setup.push(issue("pipewire-quantum", quantum_command(&user, uid)));
    }
    if !audio_rtprio_ok(&probed.limits) {
        setup.push(issue("rt-limits", limits_command(&user)));
    }
    for (iface, cmd) in probed.wifi {
        let iw_missing = matches!(cmd.code, Some(127) | None);
        if iw_missing || cmd.stdout.contains("on") {
            setup.push(issue(
                "wifi-powersave",
                wifi_command(family, &iface, iw_missing),
            ));
        }
    }
    if probed.enabled.stdout.trim() == "enabled" && !is_running(&probed.active) {
        setup.push(issue("controller", controller_command().to_string()));
    }

    let soloist = match probed.soloist {
        None => SoloistState::Missing,
        Some(cmd) => match cmd.code {
            Some(0) => SoloistState::Ok,
            Some(10) => SoloistState::Expired,
            _ => SoloistState::Failed,
        },
    };
    let soloist_command = soloist_arch(&uname)
        .map(soloist_command)
        .unwrap_or_default();

    SetupSnapshot {
        setup,
        soloist,
        soloist_command,
        ready: true,
    }
}

struct Probes {
    passwd: Cmd,
    pipewire: Cmd,
    wireplumber: Cmd,
    pulse: Cmd,
    pw_tools: bool,
    appsrc: Cmd,
    linger: Cmd,
    socket: bool,
    quantum: Option<String>,
    latency: Option<String>,
    limits: String,
    enabled: Cmd,
    active: Cmd,
    wifi: Vec<(String, Cmd)>,
    soloist: Option<Cmd>,
}

fn owned(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}

/// С 257 systemd не пускает root в чужой user bus через `XDG_RUNTIME_DIR`:
/// «Operation not permitted». Менеджер этого uid открывается как
/// `systemctl --machine=<uid>@ --user`.
fn user_service(host: &impl Host, uid: u32, service: &str) -> Cmd {
    let machine = format!("--machine={uid}@");
    host.run(
        2,
        "systemctl",
        &owned(&[&machine, "--user", "is-active", service]),
    )
}

/// `@audio` с rtprio не ниже 95. Строка может лежать в `limits.d`, значение
/// может быть выше 95: на CachyOS это `@audio - rtprio 99`.
fn audio_rtprio_ok(text: &str) -> bool {
    text.lines().any(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return false;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 || parts[0] != "@audio" || parts[2] != "rtprio" {
            return false;
        }
        if !matches!(parts[1], "-" | "hard" | "soft") {
            return false;
        }
        parts[3].parse::<u32>().ok().is_some_and(|n| n >= 95)
    })
}

fn user_name(listed: &Cmd, uid: u32) -> String {
    let line = listed.stdout.lines().next().map(str::trim).unwrap_or("");
    if line.is_empty() {
        return uid.to_string();
    }
    line.split(':').next().unwrap_or("").to_string()
}

fn is_running(cmd: &Cmd) -> bool {
    cmd.code == Some(0) && cmd.stdout.trim() == "active"
}

fn issue(id: &str, command: String) -> SetupIssue {
    SetupIssue {
        id: id.to_string(),
        command,
        detail: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use std::time::Instant;
    use zproto::SoloistState;

    struct Fake {
        os: String,
        unit: String,
        files: HashMap<String, String>,
        exec: HashSet<String>,
        uname: String,
        wireless: Vec<String>,
        answers: HashMap<(String, Vec<String>), Cmd>,
        starts: Mutex<Vec<(String, Vec<String>, Instant)>>,
    }

    impl Fake {
        fn answer(&mut self, program: &str, args: &[&str], cmd: Cmd) {
            self.answers.insert(
                (
                    program.to_string(),
                    args.iter().map(|s| (*s).to_string()).collect(),
                ),
                cmd,
            );
        }

        fn set_os(&mut self, os: &str) {
            self.os = os.to_string();
        }

        fn set_uname(&mut self, uname: &str) {
            self.uname = uname.to_string();
        }

        fn set_wireless(&mut self, wireless: Vec<String>) {
            self.wireless = wireless;
        }

        fn make_exec(&mut self, path: &str) {
            self.exec.insert(path.to_string());
        }

        fn unset_exec(&mut self, path: &str) {
            self.exec.remove(path);
        }
    }

    impl Host for Fake {
        fn read_to_string(&self, path: &str) -> Option<String> {
            match path {
                "/etc/os-release" => Some(self.os.clone()),
                "/etc/systemd/system/zvucast-agent.service" => Some(self.unit.clone()),
                _ => self.files.get(path).cloned(),
            }
        }

        fn executable(&self, path: &str) -> bool {
            self.exec.contains(path)
        }

        fn run(&self, _secs: u64, program: &str, args: &[String]) -> Cmd {
            self.starts.lock().expect("starts").push((
                program.to_string(),
                args.to_vec(),
                Instant::now(),
            ));
            std::thread::sleep(std::time::Duration::from_millis(150));
            self.answers
                .get(&(program.to_string(), args.to_vec()))
                .cloned()
                .unwrap_or(Cmd {
                    code: Some(1),
                    stdout: String::new(),
                })
        }

        fn uname_m(&self) -> String {
            self.uname.clone()
        }

        fn wireless_ifaces(&self) -> Vec<String> {
            self.wireless.clone()
        }

        fn limits_text(&self) -> String {
            let mut paths: Vec<&String> = self
                .files
                .keys()
                .filter(|path| {
                    path.as_str() == LIMITS || path.starts_with("/etc/security/limits.d/")
                })
                .collect();
            paths.sort();
            paths
                .into_iter()
                .filter_map(|path| self.files.get(path))
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        }
    }

    fn healthy_debian() -> Fake {
        let mut host = Fake {
            os: "ID=debian\nID_LIKE=\n".into(),
            unit: "Environment=XDG_RUNTIME_DIR=/run/user/1000\n".into(),
            files: HashMap::new(),
            exec: HashSet::new(),
            uname: "x86_64".into(),
            wireless: Vec::new(),
            answers: HashMap::new(),
            starts: Mutex::new(Vec::new()),
        };
        host.files.insert(
            "/etc/security/limits.conf".into(),
            "@audio - rtprio 95\n".into(),
        );
        host.files
            .insert(QUANTUM_CONF.into(), QUANTUM_MARKS.join("\n"));
        host.files
            .insert(LATENCY_CONF.into(), LATENCY_MARKS.join("\n"));
        host.files
            .insert("/run/user/1000/pipewire-0".into(), String::new());
        host.answer(
            "getent",
            &["passwd", "1000"],
            Cmd {
                code: Some(0),
                stdout: "audio:x:1000:\n".into(),
            },
        );
        for service in ["pipewire", "wireplumber", "pipewire-pulse"] {
            host.answer(
                "systemctl",
                &["--machine=1000@", "--user", "is-active", service],
                Cmd {
                    code: Some(0),
                    stdout: "active\n".into(),
                },
            );
        }
        host.make_exec(PW_RECORD);
        host.make_exec(PW_PLAY);
        host.answer(
            "gst-inspect-1.0",
            &["appsrc"],
            Cmd {
                code: Some(0),
                stdout: String::new(),
            },
        );
        host.answer(
            "loginctl",
            &["show-user", "1000", "-p", "Linger"],
            Cmd {
                code: Some(0),
                stdout: "Linger=yes\n".into(),
            },
        );
        host.answer(
            "systemctl",
            &["is-enabled", "zvucast-controller"],
            Cmd {
                code: Some(1),
                stdout: "disabled\n".into(),
            },
        );
        host
    }

    #[test]
    fn user_services_are_read_from_that_users_manager() {
        let host = healthy_debian();
        let snap = inspect(&host);
        let starts = host.starts.lock().expect("starts");
        for service in ["pipewire", "wireplumber", "pipewire-pulse"] {
            assert!(
                starts.iter().any(|(name, args, _)| {
                    name == "systemctl"
                        && args.as_slice() == ["--machine=1000@", "--user", "is-active", service]
                }),
                "no --machine query for {service}"
            );
        }
        assert!(snap.setup.iter().all(|issue| {
            !matches!(
                issue.id.as_str(),
                "pipewire" | "wireplumber" | "pipewire-pulse"
            )
        }));
    }

    #[test]
    fn installed_quanta_are_not_a_problem() {
        let host = healthy_debian();
        assert!(inspect(&host)
            .setup
            .iter()
            .all(|issue| issue.id != "pipewire-quantum"));
    }

    #[test]
    fn latency_without_headphone_rule_is_reported() {
        let mut host = healthy_debian();
        host.files.insert(
            LATENCY_CONF.into(),
            "api.alsa.period-size = 128\napi.alsa.headroom = 0\napi.alsa.disable-batch = true\n"
                .into(),
        );
        let issue = inspect(&host)
            .setup
            .into_iter()
            .find(|issue| issue.id == "pipewire-quantum")
            .expect("stale latency drop");
        let command = quantum_command("audio", 1000);
        assert_eq!(issue.command, command);
        assert!(
            command.contains("sudo tee -- /etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf")
        );
        assert!(command
            .contains("sudo tee -- /etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf"));
        assert!(command.contains("alsa.card_name = \"bcm2835 Headphones\""));
        assert!(command.contains("api.alsa.period-size = 1024"));
        assert!(command.contains(
            "sudo -u audio XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus systemctl --user restart pipewire.service pipewire-pulse.service wireplumber.service"
        ));
        assert!(!command.contains("install.sh"));
    }

    #[test]
    fn locked_machine_quantum_is_reported() {
        let mut host = healthy_debian();
        host.files.insert(
            QUANTUM_CONF.into(),
            "default.clock.rate = 48000\n\
             default.clock.allowed-rates = [ 48000 ]\n\
             default.clock.quantum = 256\n\
             default.clock.min-quantum = 256\n\
             default.clock.max-quantum = 256\n"
                .into(),
        );
        assert!(inspect(&host)
            .setup
            .iter()
            .any(|issue| issue.id == "pipewire-quantum"));
    }

    #[test]
    fn missing_quantum_drop_is_reported() {
        let mut host = healthy_debian();
        host.files.remove(QUANTUM_CONF);
        assert!(inspect(&host)
            .setup
            .iter()
            .any(|issue| issue.id == "pipewire-quantum"));
    }

    #[test]
    fn audio_rtprio_in_limits_d_is_enough() {
        let mut host = healthy_debian();
        host.files
            .insert("/etc/security/limits.conf".into(), "# none\n".into());
        host.files.insert(
            "/etc/security/limits.d/20-audio.conf".into(),
            "@audio - rtprio 99\n".into(),
        );
        assert!(!inspect(&host).setup.iter().any(|i| i.id == "rt-limits"));
    }

    #[test]
    fn pipewire_group_rtprio_does_not_cover_audio() {
        let mut host = healthy_debian();
        host.files
            .insert("/etc/security/limits.conf".into(), "# none\n".into());
        host.files.insert(
            "/etc/security/limits.d/25-pw-rlimits.conf".into(),
            "@pipewire   - rtprio  95\n".into(),
        );
        assert!(inspect(&host).setup.iter().any(|i| i.id == "rt-limits"));
    }

    #[test]
    fn debian_inactive_pipewire_uses_apt_command() {
        let mut host = healthy_debian();
        host.answer(
            "systemctl",
            &["--machine=1000@", "--user", "is-active", "pipewire"],
            Cmd {
                code: Some(3),
                stdout: "inactive\n".into(),
            },
        );
        let snap = inspect(&host);
        let issue = snap.setup.iter().find(|i| i.id == "pipewire").unwrap();
        assert_eq!(issue.command, pipewire_command(Family::Apt, "audio", 1000));
        assert!(issue.detail.is_empty());
    }

    #[test]
    fn arch_pipewire_uses_pacman_command() {
        let mut host = healthy_debian();
        host.set_os("ID=cachyos\nID_LIKE=arch\n");
        host.answer(
            "systemctl",
            &["--machine=1000@", "--user", "is-active", "pipewire"],
            Cmd {
                code: Some(3),
                stdout: "inactive\n".into(),
            },
        );
        let snap = inspect(&host);
        let issue = snap.setup.iter().find(|i| i.id == "pipewire").unwrap();
        assert_eq!(
            issue.command,
            pipewire_command(Family::Pacman, "audio", 1000)
        );
    }

    #[test]
    fn both_families_refuse_package_commands() {
        assert_eq!(family_of("ID=debian\nID_LIKE=arch\n"), Family::Other);
        assert_eq!(
            pipewire_command(Family::Other, "audio", 1000),
            PKG_UNSUPPORTED
        );
        assert_eq!(gstreamer_command(Family::Other), PKG_UNSUPPORTED);
    }

    #[test]
    fn soloist_codes() {
        let mut host = healthy_debian();
        host.make_exec("/usr/local/bin/soloist");
        host.answer(
            "soloist",
            &["--version"],
            Cmd {
                code: Some(0),
                stdout: "ok\n".into(),
            },
        );
        assert_eq!(inspect(&host).soloist, SoloistState::Ok);

        host.answer(
            "soloist",
            &["--version"],
            Cmd {
                code: Some(10),
                stdout: String::new(),
            },
        );
        assert_eq!(inspect(&host).soloist, SoloistState::Expired);

        host.answer(
            "soloist",
            &["--version"],
            Cmd {
                code: None,
                stdout: String::new(),
            },
        );
        assert_eq!(inspect(&host).soloist, SoloistState::Failed);

        host.unset_exec("/usr/local/bin/soloist");
        assert_eq!(inspect(&host).soloist, SoloistState::Missing);
        assert!(inspect(&host)
            .soloist_command
            .contains("soloist_release_x86_64"));
    }

    #[test]
    fn unknown_cpu_has_no_soloist_command() {
        let mut host = healthy_debian();
        host.set_uname("mips");
        assert_eq!(inspect(&host).soloist_command, "");
    }

    #[test]
    fn wifi_rows_follow_the_iface() {
        let mut host = healthy_debian();
        host.set_wireless(vec!["wlan1".into(), "wlan0".into()]);
        host.answer(
            "iw",
            &["dev", "wlan0", "get", "power_save"],
            Cmd {
                code: Some(0),
                stdout: "Power save: on\n".into(),
            },
        );
        host.answer(
            "iw",
            &["dev", "wlan1", "get", "power_save"],
            Cmd {
                code: Some(0),
                stdout: "Power save: off\n".into(),
            },
        );
        let rows: Vec<_> = inspect(&host)
            .setup
            .into_iter()
            .filter(|i| i.id == "wifi-powersave")
            .collect();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].command.contains("wlan0"));
        assert!(!rows[0].command.contains("apt-get"));
    }

    #[test]
    fn missing_iw_prefixes_the_package() {
        let mut host = healthy_debian();
        host.set_wireless(vec!["wlan0".into()]);
        host.answer(
            "iw",
            &["dev", "wlan0", "get", "power_save"],
            Cmd {
                code: Some(127),
                stdout: String::new(),
            },
        );
        let row = inspect(&host)
            .setup
            .into_iter()
            .find(|i| i.id == "wifi-powersave")
            .unwrap();
        assert!(row
            .command
            .starts_with("sudo apt-get install -y iw && sudo iw dev wlan0 set power_save off"));
    }

    #[test]
    fn alsa_and_pipewiresrc_are_not_required() {
        let host = healthy_debian();
        let snap = inspect(&host);
        let starts = host.starts.lock().expect("starts");
        assert!(!starts.iter().any(|(name, args, _)| {
            name == "gst-inspect-1.0"
                && matches!(
                    args.first().map(String::as_str),
                    Some("alsasrc" | "pipewiresrc")
                )
        }));
        assert!(!snap.setup.iter().any(|issue| issue.id == "gstreamer"));
    }

    #[test]
    fn missing_pw_tool_uses_the_pipewire_command() {
        let mut host = healthy_debian();
        host.unset_exec(PW_RECORD);
        let snap = inspect(&host);
        let issue = snap.setup.iter().find(|i| i.id == "pw-cat").unwrap();
        assert_eq!(issue.command, pipewire_command(Family::Apt, "audio", 1000));
        host.make_exec(PW_RECORD);
        host.unset_exec(PW_PLAY);
        assert!(inspect(&host).setup.iter().any(|i| i.id == "pw-cat"));
    }

    #[test]
    fn gstreamer_and_linger_commands_match_the_spec() {
        let mut host = healthy_debian();
        host.answer(
            "gst-inspect-1.0",
            &["appsrc"],
            Cmd {
                code: Some(1),
                stdout: String::new(),
            },
        );
        host.answer(
            "loginctl",
            &["show-user", "1000", "-p", "Linger"],
            Cmd {
                code: Some(0),
                stdout: "Linger=no\n".into(),
            },
        );
        let snap = inspect(&host);
        assert_eq!(
            snap.setup
                .iter()
                .find(|i| i.id == "gstreamer")
                .unwrap()
                .command,
            gstreamer_command(Family::Apt)
        );
        assert!(!snap
            .setup
            .iter()
            .find(|i| i.id == "gstreamer")
            .unwrap()
            .command
            .contains("alsa"));
        assert!(!snap
            .setup
            .iter()
            .find(|i| i.id == "gstreamer")
            .unwrap()
            .command
            .contains("pipewire"));
        assert_eq!(
            snap.setup
                .iter()
                .find(|i| i.id == "linger")
                .unwrap()
                .command,
            linger_command("audio", 1000)
        );
    }

    #[test]
    fn controller_only_when_enabled_and_down() {
        let mut host = healthy_debian();
        host.answer(
            "systemctl",
            &["is-enabled", "zvucast-controller"],
            Cmd {
                code: Some(0),
                stdout: "enabled\n".into(),
            },
        );
        host.answer(
            "systemctl",
            &["is-active", "zvucast-controller"],
            Cmd {
                code: Some(3),
                stdout: "inactive\n".into(),
            },
        );
        assert!(inspect(&host).setup.iter().any(|i| i.id == "controller"));

        host.answer(
            "systemctl",
            &["is-enabled", "zvucast-controller"],
            Cmd {
                code: Some(1),
                stdout: "disabled\n".into(),
            },
        );
        assert!(!inspect(&host).setup.iter().any(|i| i.id == "controller"));
    }

    #[test]
    fn getent_starts_with_the_pipewire_probe() {
        let host = healthy_debian();
        let _ = inspect(&host);
        let starts = host.starts.lock().expect("starts");
        let stamp = |program: &str, args: &[&str]| {
            starts
                .iter()
                .find(|(name, got, _)| {
                    name == program && got.iter().map(String::as_str).eq(args.iter().copied())
                })
                .map(|(_, _, at)| *at)
                .unwrap_or_else(|| panic!("no start for {program} {args:?}"))
        };
        let getent = stamp("getent", &["passwd", "1000"]);
        let pipewire = stamp(
            "systemctl",
            &["--machine=1000@", "--user", "is-active", "pipewire"],
        );
        let gap =
            getent.saturating_duration_since(pipewire) + pipewire.saturating_duration_since(getent);
        assert!(
            gap < std::time::Duration::from_millis(80),
            "getent and pipewire started {gap:?} apart"
        );
    }

    #[test]
    fn parallel_probes_finish_together() {
        let host = healthy_debian();
        let started = std::time::Instant::now();
        let _ = inspect(&host);
        assert!(started.elapsed() < std::time::Duration::from_millis(800));
    }

    #[test]
    fn timeout_argv_caps_soloist_at_three_seconds() {
        assert_eq!(
            timeout_argv(2, "systemctl", &["is-active", "pipewire"])[..3],
            ["timeout", "--signal=KILL", "2"]
        );
        assert_eq!(timeout_argv(3, "soloist", &["--version"])[2], "3");
    }
}
