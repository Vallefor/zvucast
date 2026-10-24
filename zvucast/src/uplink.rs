//! Выбор живого канала: предпочтительный интерфейс, иначе другая физическая
//! проводная карта, иначе первый Wi-Fi по имени.
//! Публикация сменяется только после того, как новый выбор продержался `HOLD_FOR`.
//! `watch` в фоне читает `/sys/class/net` и `ip -4 -o addr`.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::Path;
use std::time::{Duration, Instant};

/// Как часто `watch` опрашивает интерфейсы.
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Сколько новый выбор должен продержаться, прежде чем его опубликовать.
pub const HOLD_FOR: Duration = Duration::from_secs(1);

/// Интерфейс, на который можно вешать сокет.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveLink {
    pub name: String,
    pub ipv4: Ipv4Addr,
    /// Радио (`phy80211` / `wireless` в sysfs). Провод и loopback — false.
    pub wireless: bool,
}

/// Один интерфейс в снимке: имя, радио, carrier и IPv4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IfaceSnap {
    pub name: String,
    pub wireless: bool,
    /// Есть `device` в sysfs: карта, а не мост, veth или tun.
    pub physical: bool,
    pub carrier: bool,
    pub addrs: Vec<Ipv4Addr>,
}

/// Не 0.0.0.0, не loopback, не link-local, не multicast и не broadcast.
pub fn usable_v4(addr: Ipv4Addr) -> bool {
    !addr.is_unspecified()
        && !addr.is_loopback()
        && !addr.is_link_local()
        && !addr.is_multicast()
        && !addr.is_broadcast()
}

fn first_usable(addrs: &[Ipv4Addr]) -> Option<Ipv4Addr> {
    addrs.iter().copied().find(|&addr| usable_v4(addr))
}

/// Carrier есть и среди адресов нашёлся пригодный.
fn alive(snap: &IfaceSnap) -> Option<Ipv4Addr> {
    if !snap.carrier {
        return None;
    }
    first_usable(&snap.addrs)
}

/// Имена живых физических проводов по алфавиту. Радио, мост, veth и tun не входят.
pub fn wired_alive(ifaces: &[IfaceSnap]) -> Vec<String> {
    let mut names: Vec<String> = ifaces
        .iter()
        .filter(|snap| snap.physical && !snap.wireless && alive(snap).is_some())
        .map(|snap| snap.name.clone())
        .collect();
    names.sort();
    names
}

/// Имена живых Wi-Fi по алфавиту.
pub fn wireless_alive(ifaces: &[IfaceSnap]) -> Vec<String> {
    let mut names: Vec<String> = ifaces
        .iter()
        .filter(|snap| snap.wireless && alive(snap).is_some())
        .map(|snap| snap.name.clone())
        .collect();
    names.sort();
    names
}

/// Сначала `preferred`, если он жив. Иначе первая живая физическая проводная
/// карта. Иначе первый живой Wi-Fi. Мост, veth, tun и loopback запасными не бывают.
pub fn choose(preferred: &str, ifaces: &[IfaceSnap]) -> Option<ActiveLink> {
    if let Some(pref) = ifaces.iter().find(|snap| snap.name == preferred) {
        if let Some(ipv4) = alive(pref) {
            return Some(ActiveLink {
                name: pref.name.clone(),
                ipv4,
                wireless: pref.wireless,
            });
        }
    }
    if let Some(name) = wired_alive(ifaces).into_iter().next() {
        let snap = ifaces.iter().find(|snap| snap.name == name)?;
        return Some(ActiveLink {
            name,
            ipv4: alive(snap)?,
            wireless: false,
        });
    }
    let name = wireless_alive(ifaces).into_iter().next()?;
    let snap = ifaces.iter().find(|snap| snap.name == name)?;
    Some(ActiveLink {
        name,
        ipv4: alive(snap)?,
        wireless: true,
    })
}

/// Кандидат публикуется только после непрерывного `hold`.
/// Пока таймер не вышел, наружу остаётся прежняя публикация.
#[derive(Debug, Default)]
pub struct Hold {
    candidate: Option<ActiveLink>,
    since: Option<Instant>,
    published: Option<ActiveLink>,
}

impl Hold {
    pub fn new() -> Self {
        Self::default()
    }

    /// Смена выбора сбрасывает таймер и не трогает уже опубликованное.
    pub fn observe(
        &mut self,
        now: Instant,
        choice: Option<ActiveLink>,
        hold: Duration,
    ) -> Option<ActiveLink> {
        if self.candidate != choice {
            self.candidate = choice;
            self.since = Some(now);
            return self.published.clone();
        }
        if self.since.is_some_and(|t| now.duration_since(t) >= hold) {
            self.published = self.candidate.clone();
        }
        self.published.clone()
    }
}

/// Строки `ip -4 -o addr`: имя — второе поле, после `inet` адрес до `/`.
/// Адреса одного интерфейса остаются в порядке строк. Строка без `inet` не попадает в карту.
pub fn parse_ip_addrs(text: &str) -> BTreeMap<String, Vec<Ipv4Addr>> {
    let mut map: BTreeMap<String, Vec<Ipv4Addr>> = BTreeMap::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(name) = fields.get(1) else {
            continue;
        };
        let Some(inet_at) = fields.iter().position(|token| *token == "inet") else {
            continue;
        };
        let Some(cidr) = fields.get(inet_at + 1) else {
            continue;
        };
        let Some(addr) = cidr.split('/').next().and_then(|ip| ip.parse().ok()) else {
            continue;
        };
        map.entry((*name).to_string()).or_default().push(addr);
    }
    map
}

/// Каталоги `sys_net`. Carrier — файл `carrier` равен `1` после trim.
/// Радио — каталог `phy80211` или `wireless`. Карта — ссылка `device` существует.
/// Адреса из карты, иначе пусто. Имена по алфавиту.
pub fn read_snap(sys_net: &Path, addrs: &BTreeMap<String, Vec<Ipv4Addr>>) -> Vec<IfaceSnap> {
    let mut snaps = Vec::new();
    let Ok(entries) = std::fs::read_dir(sys_net) else {
        return snaps;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let carrier =
            std::fs::read_to_string(path.join("carrier")).is_ok_and(|text| text.trim() == "1");
        let wireless = path.join("phy80211").is_dir() || path.join("wireless").is_dir();
        let physical = path.join("device").exists();
        let iface_addrs = addrs.get(&name).cloned().unwrap_or_default();
        snaps.push(IfaceSnap {
            name,
            wireless,
            physical,
            carrier,
            addrs: iface_addrs,
        });
    }
    snaps.sort_by(|a, b| a.name.cmp(&b.name));
    snaps
}

/// stdout `ip -4 -o addr`. Команда не запустилась или завершилась с ошибкой — пустая строка,
/// карта адресов будет пустой, carrier и радио всё равно читаются из sysfs.
fn ip_addr_text() -> String {
    let Ok(out) = std::process::Command::new("ip")
        .args(["-4", "-o", "addr"])
        .output()
    else {
        return String::new();
    };
    if !out.status.success() {
        return String::new();
    }
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Набор живых радио сменился и в нём больше одного имени — одна строка.
/// Имя в журнале — то, что вернул `choose`, и только если оно само в этом наборе.
fn log_wifi_choice(alive: &[String], choice: Option<&ActiveLink>, prev: &mut Vec<String>) {
    let changed = alive != prev.as_slice();
    if changed && alive.len() > 1 {
        if let Some(link) = choice.filter(|link| alive.iter().any(|name| name == &link.name)) {
            let picked = &link.name;
            let list = alive.join(", ");
            tracing::info!("wifi: выбран {picked} из [{list}]");
        }
    }
    if changed {
        *prev = alive.to_vec();
    }
}

/// Наблюдатель на процесс. Приёмник стартует с `None`, пока выбор не продержится `HOLD_FOR`.
/// В канал пишем только когда опубликованное значение изменилось.
pub fn watch(preferred: String) -> tokio::sync::watch::Receiver<Option<ActiveLink>> {
    let (tx, rx) = tokio::sync::watch::channel(None);
    std::thread::Builder::new()
        .name("uplink".into())
        .spawn(move || watch_loop(preferred, tx))
        .expect("поток uplink");
    rx
}

fn watch_loop(preferred: String, tx: tokio::sync::watch::Sender<Option<ActiveLink>>) {
    let mut hold = Hold::new();
    let mut published = None;
    let mut prev_wifi = Vec::new();
    loop {
        let addrs = parse_ip_addrs(&ip_addr_text());
        let snaps = read_snap(Path::new("/sys/class/net"), &addrs);
        let alive = wireless_alive(&snaps);
        let choice = choose(&preferred, &snaps);
        log_wifi_choice(&alive, choice.as_ref(), &mut prev_wifi);
        let next = hold.observe(Instant::now(), choice, HOLD_FOR);
        if next != published {
            published = next.clone();
            let _ = tx.send(next);
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::time::{Duration, Instant};

    fn snap(name: &str, wireless: bool, carrier: bool, addrs: &[&str]) -> IfaceSnap {
        card(name, wireless, true, carrier, addrs)
    }

    fn card(
        name: &str,
        wireless: bool,
        physical: bool,
        carrier: bool,
        addrs: &[&str],
    ) -> IfaceSnap {
        IfaceSnap {
            name: name.into(),
            wireless,
            physical,
            carrier,
            addrs: addrs.iter().map(|s| s.parse().unwrap()).collect(),
        }
    }

    #[test]
    fn ethernet_beats_wifi_when_both_alive() {
        let ifaces = vec![
            snap("wlan0", true, true, &["192.0.2.14"]),
            snap("eth0", false, true, &["192.0.2.204"]),
        ];
        let got = choose("eth0", &ifaces).unwrap();
        assert_eq!(got.name, "eth0");
        assert_eq!(got.ipv4, Ipv4Addr::new(192, 0, 2, 204));
        assert!(!got.wireless);
    }

    #[test]
    fn carrier_without_usable_address_is_dead() {
        let ifaces = vec![snap("eth0", false, true, &["169.254.1.1"])];
        assert!(choose("eth0", &ifaces).is_none());
    }

    #[test]
    fn address_without_carrier_is_dead() {
        let ifaces = vec![snap("eth0", false, false, &["192.0.2.204"])];
        assert!(choose("eth0", &ifaces).is_none());
    }

    #[test]
    fn link_local_and_loopback_are_not_usable() {
        assert!(!usable_v4("169.254.1.1".parse().unwrap()));
        assert!(!usable_v4("127.0.0.1".parse().unwrap()));
        assert!(usable_v4("192.0.2.36".parse().unwrap()));
    }

    #[test]
    fn wifi_when_ethernet_is_dead() {
        let ifaces = vec![
            snap("eth0", false, false, &["192.0.2.204"]),
            snap("wlan0", true, true, &["10.0.0.5"]),
        ];
        let got = choose("eth0", &ifaces).unwrap();
        assert_eq!(got.name, "wlan0");
        assert!(got.wireless);
    }

    #[test]
    fn two_radios_pick_the_first_name() {
        let ifaces = vec![
            snap("wlp2s0", true, true, &["10.0.0.8"]),
            snap("wlan0", true, true, &["10.0.0.5"]),
        ];
        assert_eq!(choose("eth0", &ifaces).unwrap().name, "wlan0");
        assert_eq!(
            wireless_alive(&ifaces),
            vec!["wlan0".to_string(), "wlp2s0".to_string()]
        );
    }

    #[test]
    fn docker_and_loopback_are_not_fallback() {
        let ifaces = vec![
            card("docker0", false, false, true, &["172.17.0.1"]),
            card("lo", false, false, true, &["127.0.0.1"]),
        ];
        assert!(choose("eth0", &ifaces).is_none());
    }

    /// Предпочтительный NIC мёртв. Живой кабель на другой карте важнее Wi-Fi.
    /// Мост, veth и tun с адресом в этот запас не входят.
    #[test]
    fn dead_preferred_keeps_another_physical_nic_ahead_of_wifi_and_bridges() {
        let ifaces = vec![
            card("docker0", false, false, true, &["172.17.0.1"]),
            card("tailscale0", false, false, true, &["100.64.0.1"]),
            card("veth78ff042", false, false, true, &["172.17.0.2"]),
            card("eth1", false, true, false, &["192.0.2.50"]),
            card("eth0", false, true, true, &["192.0.2.207"]),
            card("wlan0", true, true, true, &["192.0.2.109"]),
        ];
        let got = choose("eth1", &ifaces).unwrap();
        assert_eq!(got.name, "eth0");
        assert_eq!(got.ipv4, Ipv4Addr::new(192, 0, 2, 207));
        assert!(!got.wireless);
    }

    #[test]
    fn first_usable_address_keeps_snapshot_order() {
        let ifaces = vec![snap(
            "eth0",
            false,
            true,
            &["169.254.1.1", "192.0.2.36", "192.0.2.37"],
        )];
        assert_eq!(
            choose("eth0", &ifaces).unwrap().ipv4,
            Ipv4Addr::new(192, 0, 2, 36)
        );
    }

    #[test]
    fn hold_publishes_only_after_one_second() {
        let t0 = Instant::now();
        let mut hold = Hold::new();
        let choice = choose("eth0", &[snap("eth0", false, true, &["192.0.2.204"])]);
        assert!(hold.observe(t0, choice.clone(), HOLD_FOR).is_none());
        assert!(hold
            .observe(t0 + Duration::from_millis(999), choice.clone(), HOLD_FOR)
            .is_none());
        assert_eq!(
            hold.observe(t0 + HOLD_FOR, choice.clone(), HOLD_FOR)
                .unwrap()
                .name,
            "eth0"
        );
    }

    #[test]
    fn cancelled_change_keeps_the_previous_publication() {
        let t0 = Instant::now();
        let mut hold = Hold::new();
        let eth = choose("eth0", &[snap("eth0", false, true, &["192.0.2.204"])]);
        let _ = hold.observe(t0, eth.clone(), HOLD_FOR);
        assert!(hold.observe(t0 + HOLD_FOR, eth.clone(), HOLD_FOR).is_some());
        let wifi = choose("eth0", &[snap("wlan0", true, true, &["10.0.0.5"])]);
        assert_eq!(
            hold.observe(
                t0 + HOLD_FOR + Duration::from_millis(100),
                wifi.clone(),
                HOLD_FOR
            )
            .unwrap()
            .name,
            "eth0"
        );
        let back = hold.observe(t0 + HOLD_FOR + Duration::from_millis(200), eth, HOLD_FOR);
        assert_eq!(back.unwrap().name, "eth0");
        assert_eq!(
            hold.observe(t0 + Duration::from_secs(3), wifi.clone(), HOLD_FOR)
                .unwrap()
                .name,
            "eth0"
        );
        assert_eq!(
            hold.observe(t0 + Duration::from_secs(4), wifi, HOLD_FOR)
                .unwrap()
                .name,
            "wlan0"
        );
    }

    #[test]
    fn parse_ip_keeps_order_and_skips_non_inet() {
        let text = "\
2: eth0    inet 169.254.1.1/16 brd 169.254.255.255 scope link eth0\n\
2: eth0    inet 192.0.2.36/24 brd 192.0.2.255 scope global eth0\n\
3: wlan0    inet 10.0.0.5/24 brd 10.0.0.255 scope global dynamic wlan0\n";
        let map = parse_ip_addrs(text);
        assert_eq!(
            map["eth0"],
            vec![
                Ipv4Addr::new(169, 254, 1, 1),
                Ipv4Addr::new(192, 0, 2, 36)
            ]
        );
        assert_eq!(map["wlan0"], vec![Ipv4Addr::new(10, 0, 0, 5)]);
    }

    #[test]
    fn parse_ip_ignores_lines_without_inet() {
        let map = parse_ip_addrs("1: lo\n2: eth0    inet not-an-ip/24 scope global eth0\n");
        assert!(map.is_empty());
    }

    #[test]
    fn read_snap_sees_carrier_and_phy() {
        let root = std::env::temp_dir().join(format!("uplink-snap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("eth0")).unwrap();
        std::fs::write(root.join("eth0/carrier"), "1\n").unwrap();
        std::fs::create_dir_all(root.join("wlan0/phy80211")).unwrap();
        std::fs::write(root.join("wlan0/carrier"), "0\n").unwrap();
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert("eth0".into(), vec![Ipv4Addr::new(192, 0, 2, 204)]);
        addrs.insert("wlan0".into(), vec![Ipv4Addr::new(10, 0, 0, 5)]);
        let snaps = read_snap(&root, &addrs);
        let eth = snaps.iter().find(|s| s.name == "eth0").unwrap();
        let wifi = snaps.iter().find(|s| s.name == "wlan0").unwrap();
        assert!(eth.carrier && !eth.wireless);
        assert!(!wifi.carrier && wifi.wireless);
        assert_eq!(eth.addrs, vec![Ipv4Addr::new(192, 0, 2, 204)]);
        assert_eq!(wifi.addrs, vec![Ipv4Addr::new(10, 0, 0, 5)]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_snap_wireless_dir_sorts_and_leaves_unknown_addrs_empty() {
        let root = std::env::temp_dir().join(format!("uplink-snap-w-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("wlan1/wireless")).unwrap();
        std::fs::create_dir_all(root.join("eth1")).unwrap();
        std::fs::write(root.join("not-an-iface"), b"x").unwrap();
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert("eth1".into(), vec![Ipv4Addr::new(192, 0, 2, 9)]);
        let snaps = read_snap(&root, &addrs);
        assert_eq!(
            snaps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            vec!["eth1", "wlan1"]
        );
        let eth = snaps.iter().find(|s| s.name == "eth1").unwrap();
        let wifi = snaps.iter().find(|s| s.name == "wlan1").unwrap();
        assert!(!eth.carrier && !eth.wireless);
        assert_eq!(eth.addrs, vec![Ipv4Addr::new(192, 0, 2, 9)]);
        assert!(!wifi.carrier && wifi.wireless && wifi.addrs.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_snap_treats_a_device_symlink_as_physical() {
        let root = std::env::temp_dir().join(format!("uplink-snap-dev-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let nic = root.join("bus");
        std::fs::create_dir_all(&nic).unwrap();
        std::fs::create_dir_all(root.join("eth0")).unwrap();
        std::os::unix::fs::symlink(&nic, root.join("eth0/device")).unwrap();
        std::fs::create_dir_all(root.join("docker0")).unwrap();
        let snaps = read_snap(&root, &std::collections::BTreeMap::new());
        let eth = snaps.iter().find(|s| s.name == "eth0").unwrap();
        let docker = snaps.iter().find(|s| s.name == "docker0").unwrap();
        assert!(eth.physical);
        assert!(!docker.physical);
        let _ = std::fs::remove_dir_all(&root);
    }
}
