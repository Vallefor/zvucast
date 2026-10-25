# Установка и сводка готовности Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Агент сообщает, чего не хватает на его машине, страница «Узлы» показывает это по всем онлайн-узлам с командой починки, а `install.sh` ставит плату одной командой `curl | bash`.

**Architecture:** Чистая проверка живёт в `zvucast/src/setup.rs` и не ходит в систему сама: ей передают ответы команд. Агент публикует снимок в `Hello` и дальше раз в минуту сообщением `Setup`, потому что повторный `Hello` по живому сокету контроллер игнорирует. Контроллер хранит сырой отчёт и при `GET /api/nodes` дописывает строку Soloist, только если на узле включён Spotify. `install.sh` — отдельный bash: на чистой плате бинарника ещё нет, поэтому текст команд повторён в скрипте, а не вычитан из бинарника.

**Tech Stack:** Rust (zproto, axum), React (Vite), bash.

## Global Constraints

- Спецификация: `docs/superpowers/specs/2026-10-25-setup-install-design.md`. Идентификаторы, тексты команд, подписи UI, порядок установки и коды выхода — оттуда, байт в байт. Ниже они повторены там, где задача их пишет.
- Имена JSON — snake_case.
- Новый агент в `Hello` всегда шлёт `setup` массивом (`Some`), даже пустым. `None` бывает только у старого JSON без поля.
- `soloist` в вектор локальных проблем агент не кладёт. Его добавляет контроллер.
- Агент ничего не включает и не вызывает `sudo`. Предел одной команды: 2 секунды, `soloist --version` — 3. Проверки параллельны, весь снимок укладывается в 5 секунд.
- Повторный `Hello` по живому сокету по-прежнему игнорируется. Минутный снимок — `AgentMsg::Setup`.
- `install.sh` запускается через `bash`, вопросы читает с `/dev/tty`. `api.github.com` не используется.
- Тесты не ходят в сеть, не ставят пакеты и не трогают платы.
- Комментарии и строки UI по-русски, в том же тоне, что соседний код.
- Не коммитить `target/`, `web/node_modules/`, `web/dist/assets/`.
- Один `cargo` за раз. Команды ниже — из корня репозитория исходника. Если в окружении задан `CARGO_TARGET_DIR`, не запускать второй `cargo`, пока первый держит этот каталог.
- Сообщения коммитов: `feat(zvucast): …` / `test(zvucast): …`, по-русски, про причину.

## File Structure

- `zproto/src/lib.rs` — `SetupIssue`, `SoloistState`, поля `Hello`, вариант `Setup`.
- `zvucast/src/setup.rs` — семья пакетов, тексты команд, снимок по ответам хоста. В систему не смотрит.
- `zvucast/src/lib.rs` — `pub mod setup`.
- `zvucast/src/registry.rs` — сырой отчёт на узле, переживает offline.
- `zvucast/src/controller.rs` — запись отчёта из `Hello` и `Setup`, склейка Spotify в `GET /api/nodes`.
- `zvucast/src/agent.rs` — фоновый снимок раз в 60 секунд.
- `zvucast/src/ws_client.rs` — поля в первом `Hello`, дальше `Setup` при смене снимка.
- `zvucast/tests/controller.rs`, `zvucast/tests/ws_client.rs` — литералы `Hello` и вызовы `ws_client::run`.
- `web/src/api.ts`, `web/src/pages/Nodes.tsx`, `web/src/theme.css` — блок «Системе не хватает».
- `install.sh`, `install.test.sh` — установка и её проверка на поддельном `PATH`.
- `README.md` — команда запуска.

---

### Task 1: Поля протокола

**Files:**
- Modify: `zproto/src/lib.rs`
- Modify: каждый литерал `AgentMsg::Hello {` в `zproto/src/lib.rs`, `zvucast/src/ws_client.rs`, `zvucast/tests/controller.rs`
- Test: `zproto/src/lib.rs` (`mod tests`)

**Interfaces:**
- Consumes: ничего
- Produces:
  - `zproto::SetupIssue { id: String, command: String, detail: String }`
  - `zproto::SoloistState` — `Unknown` (default), `Missing`, `Ok`, `Expired`, `Failed`, в JSON строчными буквами
  - `AgentMsg::Hello` получает `setup: Option<Vec<SetupIssue>>` (`#[serde(default)]`), `soloist: SoloistState` (`#[serde(default)]`), `soloist_command: String` (`#[serde(default)]`)
  - `AgentMsg::Setup { setup: Vec<SetupIssue>, soloist: SoloistState, soloist_command: String }` — у `soloist` и `soloist_command` тоже `#[serde(default)]`

- [ ] **Step 1: Write the failing test**

В `zproto/src/lib.rs` внутри `mod tests` добавить:

```rust
#[test]
fn hello_without_setup_is_an_old_agent() {
    let raw = r#"{"type":"Hello","node_id":"n","name":"pc","roles":["tx"],"version":"0.1.0","capture_device":null,"playback_device":null,"inputs":[],"outputs":[]}"#;
    let AgentMsg::Hello { setup, soloist, soloist_command, .. } = serde_json::from_str(raw).unwrap() else {
        panic!("не hello");
    };
    assert_eq!(setup, None);
    assert_eq!(soloist, SoloistState::Unknown);
    assert_eq!(soloist_command, "");
}

#[test]
fn setup_message_roundtrip() {
    let m = AgentMsg::Setup {
        setup: vec![SetupIssue {
            id: "pipewire".into(),
            command: "sudo true".into(),
            detail: String::new(),
        }],
        soloist: SoloistState::Missing,
        soloist_command: "curl soloist".into(),
    };
    let s = serde_json::to_string(&m).unwrap();
    assert!(s.contains("\"type\":\"Setup\""));
    assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), m);
    let bare = r#"{"type":"Setup","setup":[]}"#;
    let AgentMsg::Setup { soloist, soloist_command, setup } = serde_json::from_str(bare).unwrap() else {
        panic!("не setup");
    };
    assert!(setup.is_empty());
    assert_eq!(soloist, SoloistState::Unknown);
    assert_eq!(soloist_command, "");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --offline -p zproto --lib hello_without_setup_is_an_old_agent setup_message_roundtrip`

Expected: FAIL, компиляция не знает `SoloistState` / `Setup`.

- [ ] **Step 3: Write the types and keep every Hello literal compiling**

В `pub mod proto`, рядом с `LinkKind`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetupIssue {
    pub id: String,
    pub command: String,
    #[serde(default)]
    pub detail: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SoloistState {
    #[default]
    Unknown,
    Missing,
    Ok,
    Expired,
    Failed,
}
```

В вариант `Hello` после `link` добавить три поля из блока Interfaces. После варианта `Hello` добавить вариант `Setup` из блока Interfaces.

Каждый литерал `AgentMsg::Hello { ... }` в этом крейте и в `zvucast` дописать перед закрывающей скобкой. Поиск: `rg -n "AgentMsg::Hello \{" zproto zvucast`. На момент плана это `zproto/src/lib.rs` (четыре литерала), `zvucast/src/ws_client.rs` (один), `zvucast/tests/controller.rs` (пять). Разборы с `..` не трогать.

```rust
setup: Some(vec![]),
soloist: SoloistState::Unknown,
soloist_command: String::new(),
```

В `zvucast/src/controller.rs` разбор первого кадра дописать имена `setup`, `soloist`, `soloist_command` в `Ok(AgentMsg::Hello { ... })` и в кортеж `break`, иначе крейт не соберётся. Пока эти три значения никуда не записывать: их заберёт Task 4. Чтобы не словить предупреждение, сразу после `break` связать их через `let _ = (setup, soloist, soloist_command);` — Task 4 эту строку удалит.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --offline -p zproto --lib`

Expected: PASS, включая прежние тесты `hello_*`.

Run: `cargo test --offline -p zvucast --test controller --no-run && cargo test --offline -p zvucast --test ws_client --no-run`

Expected: оба теста компилируются. `--no-run` не исполняет их.

- [ ] **Step 5: Commit**

```bash
git add zproto/src/lib.rs zvucast/src/ws_client.rs zvucast/src/controller.rs zvucast/tests/controller.rs
git commit -m "$(cat <<'EOF'
feat(zvucast): отчёт о системе едет отдельным полем hello

EOF
)"
```

---

### Task 2: Снимок машины без живой системы

**Files:**
- Create: `zvucast/src/setup.rs`
- Modify: `zvucast/src/lib.rs` (`pub mod setup;`)
- Test: `zvucast/src/setup.rs` (`mod tests`)

**Interfaces:**
- Consumes: `zproto::SetupIssue`, `zproto::SoloistState`
- Produces:
  - `setup::Family` — `Apt`, `Pacman`, `Other`
  - `setup::family_of(os_release: &str) -> Family`
  - `setup::audio_uid(unit_text: &str) -> u32` — из `XDG_RUNTIME_DIR=/run/user/<uid>`, иначе `1000`
  - `setup::soloist_arch(uname_m: &str) -> Option<&'static str>` — `x86_64`→`x86_64`, `aarch64`→`arm64`, `armv7l`→`arm32`, иначе `None`
  - `setup::soloist_command(arch: &str) -> String` — однострочник из спецификации
  - `setup::pipewire_command(family, user, uid) -> String` — общая команда для `pipewire`, `wireplumber`, `pipewire-pulse`, `pipewire-socket`
  - `setup::gstreamer_command(family) -> String`
  - `setup::linger_command(user, uid) -> String`
  - `setup::limits_command(user) -> String`
  - `setup::wifi_command(family, iface, iw_missing: bool) -> String`
  - `setup::controller_command() -> &'static str` = `sudo systemctl enable --now zvucast-controller`
  - `setup::PKG_UNSUPPORTED` = `эта система не из семейства Debian или Arch — пакеты этой командой не ставятся`
  - `setup::Cmd { code: Option<i32>, stdout: String }` — `code: None` значит тайм-аут
  - `setup::Host: Sync` с методами `read_to_string(&self, path: &str) -> Option<String>`, `executable(&self, path: &str) -> bool`, `run(&self, secs: u64, program: &str, args: &[String]) -> Cmd`, `uname_m(&self) -> String`, `wireless_ifaces(&self) -> Vec<String>`
  - `setup::inspect(host: &(impl Host + Sync)) -> SetupSnapshot`
  - `setup::SetupSnapshot { setup: Vec<SetupIssue>, soloist: SoloistState, soloist_command: String, ready: bool }`
  - `setup::timeout_argv(secs: u64, program: &str, args: &[&str]) -> Vec<String>` — `timeout`, `--signal=KILL`, секунды, программа, аргументы
  - `SystemHost` в том же файле, не в тестах: `run` зовёт `std::process::Command` по `timeout_argv`. `inspect` его не требует. Прод-вызов появится в Task 5.

Тексты команд, которые функции обязаны вернуть (`{user}` / `{uid}` / `{iface}` / `{arch}` уже подставлены):

```text
sudo apt-get install -y pipewire pipewire-bin pipewire-pulse wireplumber && sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service && sudo -u {user} XDG_RUNTIME_DIR=/run/user/{uid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus systemctl --user enable --now pipewire.service pipewire-pulse.service wireplumber.service
```

```text
sudo pacman -S --needed --noconfirm pipewire pipewire-pulse wireplumber && sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service && sudo -u {user} XDG_RUNTIME_DIR=/run/user/{uid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus systemctl --user enable --now pipewire.service pipewire-pulse.service wireplumber.service
```

```text
sudo apt-get install -y gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-alsa gstreamer1.0-pipewire
```

```text
sudo pacman -S --needed --noconfirm gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugin-pipewire
```

```text
sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service
```

```text
grep -q '@audio - rtprio 95' /etc/security/limits.conf || printf '%s\n' '@audio - rtprio 95' '@audio - memlock unlimited' | sudo tee -a /etc/security/limits.conf; sudo usermod -aG audio {user}
```

```text
sudo iw dev {iface} set power_save off
```

Нет `iw` на Debian — перед этой строкой `sudo apt-get install -y iw && `. На Arch — `sudo pacman -S --needed --noconfirm iw && `.

```text
tmp=$(mktemp -d) && curl --fail --location --max-time 90 -o "$tmp/soloist.tar.gz" "https://soloist-builds.spotifycdn.com/soloist_release_{arch}.tar.gz" && tar -xzf "$tmp/soloist.tar.gz" -C "$tmp" && sudo install -m 755 "$tmp/soloist" /usr/local/bin/soloist && soloist --version
```

- [ ] **Step 1: Write the failing test**

В `zvucast/src/setup.rs` сразу `mod tests`. Хелпер `Fake` хранит `os`, `unit`, `files: HashMap<String, String>`, `exec: HashSet<String>`, `uname`, `wireless`, `answers: HashMap<(String, Vec<String>), Cmd>`, и на каждый `run` спит 150 мс. Неизвестный ключ → `Cmd { code: Some(1), stdout: String::new() }`.

Тесты:

```rust
#[test]
fn debian_inactive_pipewire_uses_apt_command() {
    let mut host = healthy_debian();
    host.answer("systemctl", &["--user", "is-active", "pipewire"], Cmd { code: Some(3), stdout: "inactive\n".into() });
    let snap = inspect(&host);
    let issue = snap.setup.iter().find(|i| i.id == "pipewire").unwrap();
    assert_eq!(issue.command, pipewire_command(Family::Apt, "audio", 1000));
    assert!(issue.detail.is_empty());
}

#[test]
fn arch_pipewire_uses_pacman_command() {
    let mut host = healthy_debian();
    host.set_os("ID=cachyos\nID_LIKE=arch\n");
    host.answer("systemctl", &["--user", "is-active", "pipewire"], Cmd { code: Some(3), stdout: "inactive\n".into() });
    let snap = inspect(&host);
    let issue = snap.setup.iter().find(|i| i.id == "pipewire").unwrap();
    assert_eq!(issue.command, pipewire_command(Family::Pacman, "audio", 1000));
}

#[test]
fn both_families_refuse_package_commands() {
    assert_eq!(family_of("ID=debian\nID_LIKE=arch\n"), Family::Other);
    assert_eq!(pipewire_command(Family::Other, "audio", 1000), PKG_UNSUPPORTED);
    assert_eq!(gstreamer_command(Family::Other), PKG_UNSUPPORTED);
}

#[test]
fn soloist_codes() {
    let mut host = healthy_debian();
    host.make_exec("/usr/local/bin/soloist");
    host.answer("soloist", &["--version"], Cmd { code: Some(0), stdout: "ok\n".into() });
    assert_eq!(inspect(&host).soloist, SoloistState::Ok);

    host.answer("soloist", &["--version"], Cmd { code: Some(10), stdout: String::new() });
    assert_eq!(inspect(&host).soloist, SoloistState::Expired);

    host.answer("soloist", &["--version"], Cmd { code: None, stdout: String::new() });
    assert_eq!(inspect(&host).soloist, SoloistState::Failed);

    host.unset_exec("/usr/local/bin/soloist");
    assert_eq!(inspect(&host).soloist, SoloistState::Missing);
    assert!(inspect(&host).soloist_command.contains("soloist_release_x86_64"));
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
    host.answer("iw", &["dev", "wlan0", "get", "power_save"], Cmd { code: Some(0), stdout: "Power save: on\n".into() });
    host.answer("iw", &["dev", "wlan1", "get", "power_save"], Cmd { code: Some(0), stdout: "Power save: off\n".into() });
    let rows: Vec<_> = inspect(&host).setup.into_iter().filter(|i| i.id == "wifi-powersave").collect();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].command.contains("wlan0"));
    assert!(!rows[0].command.contains("apt-get"));
}

#[test]
fn missing_iw_prefixes_the_package() {
    let mut host = healthy_debian();
    host.set_wireless(vec!["wlan0".into()]);
    host.answer("iw", &["dev", "wlan0", "get", "power_save"], Cmd { code: Some(127), stdout: String::new() });
    let row = inspect(&host).setup.into_iter().find(|i| i.id == "wifi-powersave").unwrap();
    assert!(row.command.starts_with("sudo apt-get install -y iw && sudo iw dev wlan0 set power_save off"));
}

#[test]
fn gstreamer_and_linger_commands_match_the_spec() {
    let mut host = healthy_debian();
    host.answer("gst-inspect-1.0", &["alsasrc"], Cmd { code: Some(1), stdout: String::new() });
    host.answer("loginctl", &["show-user", "1000", "-p", "Linger"], Cmd { code: Some(0), stdout: "Linger=no\n".into() });
    let snap = inspect(&host);
    assert_eq!(snap.setup.iter().find(|i| i.id == "gstreamer").unwrap().command, gstreamer_command(Family::Apt));
    assert_eq!(snap.setup.iter().find(|i| i.id == "linger").unwrap().command, linger_command("audio", 1000));
}

#[test]
fn controller_only_when_enabled_and_down() {
    let mut host = healthy_debian();
    host.answer("systemctl", &["is-enabled", "zvucast-controller"], Cmd { code: Some(0), stdout: "enabled\n".into() });
    host.answer("systemctl", &["is-active", "zvucast-controller"], Cmd { code: Some(3), stdout: "inactive\n".into() });
    assert!(inspect(&host).setup.iter().any(|i| i.id == "controller"));

    host.answer("systemctl", &["is-enabled", "zvucast-controller"], Cmd { code: Some(1), stdout: "disabled\n".into() });
    assert!(!inspect(&host).setup.iter().any(|i| i.id == "controller"));
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
    assert_eq!(timeout_argv(2, "systemctl", &["is-active", "pipewire"])[..3], ["timeout", "--signal=KILL", "2"]);
    assert_eq!(timeout_argv(3, "soloist", &["--version"])[2], "3");
}
```

`healthy_debian` отвечает так, чтобы снимок был пуст: `os` = `ID=debian\nID_LIKE=\n`, `unit` содержит `Environment=XDG_RUNTIME_DIR=/run/user/1000`, `getent passwd 1000` печатает `audio:x:1000:`, `uname` = `x86_64`, три `systemctl --user is-active` для `pipewire`, `wireplumber`, `pipewire-pulse` печатают `active` с кодом 0, оба `gst-inspect-1.0` (`pipewiresrc` и `alsasrc`) код 0, `loginctl show-user 1000 -p Linger` печатает `Linger=yes`, файл `/etc/security/limits.conf` содержит `@audio - rtprio 95`, `/run/user/1000/pipewire-0` существует, `is-enabled zvucast-controller` печатает `disabled`, Soloist не исполняемый, беспроводных интерфейсов нет. `run` для пользовательского systemctl вызывается как программа `systemctl` с аргументами `--user is-active <служба>`: окружение пользователя проверяет Task 5, здесь важен текст команды, а не то, как прод её запускает.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --offline -p zvucast --lib setup::`

Expected: FAIL, модуля `setup` нет.

- [ ] **Step 3: Implement inspect**

`family_of` читает `ID=` и `ID_LIKE=`, снимает кавычки. `apt`, если `ID` равен `debian`, `ubuntu` или `raspbian`, либо слово в `ID_LIKE` равно `debian` или `ubuntu`. `pacman`, если `ID` равен `arch` или `cachyos`, либо слово в `ID_LIKE` равно `arch`. Оба правила или ни одного — `Other`.

`audio_uid` ищет первое `XDG_RUNTIME_DIR=/run/user/` и парсит число до конца токена. Не нашёл — `1000`.

`inspect` делает две волны `std::thread::scope`. Первая параллельно читает семью, uid и `uname_m`. Имя пользователя — первое поле `getent passwd <uid>`; нет строки — десятичная запись uid. Вторая волна параллельно проверяет три пользовательские службы, два `gst-inspect-1.0`, linger, существование сокета, строку в `limits.conf`, каждый беспроводной интерфейс и пару `is-enabled` / `is-active` контроллера, плюс Soloist.

Служба — проблема, если stdout после `trim` не равен `active` или код не `Some(0)`. GStreamer — проблема, если хотя бы один из двух вызовов не `Some(0)`. Linger — проблема, если stdout не содержит `Linger=yes`. Сокет — путь `/run/user/<uid>/pipewire-0`. Лимиты — в тексте файла нет подстроки `@audio - rtprio 95`. Wi-Fi — имена из `wireless_ifaces` уже можно не сортировать, если хост вернул их по алфавиту; `inspect` сортирует сам. Код `127` или `None` у `iw` — нет `iw`. Иначе проблема только если stdout содержит `on`. Контроллер — проблема только когда stdout `is-enabled` после trim равен `enabled`, а `is-active` не `active`.

Soloist: нет `executable("/usr/local/bin/soloist")` — `Missing`, процесс не запускать. Иначе `run(3, "soloist", &["--version"])`: код 0 → `Ok`, код 10 → `Expired`, иначе `Failed`. `soloist_command` пустая, если `soloist_arch` вернул `None`, иначе однострочник. `ready` у снимка всегда `true`. Порядок строк: `pipewire`, `wireplumber`, `pipewire-pulse`, `gstreamer`, `linger`, `pipewire-socket`, `rt-limits`, затем `wifi-powersave` по имени интерфейса, затем `controller`.

`SystemHost::run` запускает `timeout_argv` и забирает stdout. Ненулевой код `timeout` `124` и убийство по `KILL` (код `137`) — это `Cmd { code: None, .. }`. Остальные коды — `Some`. Ошибку `spawn` тоже считать `code: None`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --offline -p zvucast --lib setup::`

Expected: PASS. Тест параллельности зелёный: восемь вызовов по 150 мс последовательно были бы больше 800 мс.

- [ ] **Step 5: Commit**

```bash
git add zvucast/src/setup.rs zvucast/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(zvucast): снимок недостающих пакетов считается без живой машины

EOF
)"
```

---

### Task 3: Реестр помнит сырой отчёт

**Files:**
- Modify: `zvucast/src/registry.rs`
- Test: `zvucast/src/registry.rs` (`mod tests`)

**Interfaces:**
- Consumes: `zproto::SetupIssue`, `zproto::SoloistState`
- Produces:
  - `Registry::set_setup(&mut self, node_id: &str, reported: bool, issues: Vec<SetupIssue>, soloist: SoloistState, command: String)`
  - `Registry::setup_of(&self, node_id: &str) -> (bool, Vec<SetupIssue>, SoloistState, String)` — неизвестный узел: `(false, vec![], Unknown, "")`
  - `set_setup` на неизвестный узел — no-op, как `set_update_note`

Поле не входит в `NodeView`. `mark_offline` его не стирает. Новый `NodeRec` начинает с `reported = false`, `Unknown`, пустой команды. Повторный `hello()` эти поля не трогает: их перезаписывает только `set_setup`.

- [ ] **Step 1: Write the failing test**

В существующий `mod tests` реестра:

```rust
#[test]
fn setup_survives_offline_and_unknown_node_is_empty() {
    let mut r = Registry::new();
    let (tx, _rx) = mpsc::channel(1);
    r.hello("id", "pc", vec![], "0.1.0".into(), String::new(), None, None, "192.0.2.10".into(), tx);
    r.set_setup(
        "id",
        true,
        vec![SetupIssue { id: "linger".into(), command: "sudo true".into(), detail: String::new() }],
        SoloistState::Missing,
        "curl soloist".into(),
    );
    r.mark_offline("id");
    let (reported, issues, soloist, command) = r.setup_of("id");
    assert!(reported);
    assert_eq!(issues.len(), 1);
    assert_eq!(soloist, SoloistState::Missing);
    assert_eq!(command, "curl soloist");
    assert!(!r.list()[0].online);
    assert_eq!(r.setup_of("missing"), (false, vec![], SoloistState::Unknown, String::new()));
}
```

Импорт `SetupIssue` и `SoloistState` добавить к уже существующему `use zproto::...`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --offline -p zvucast --lib registry::setup_survives_offline_and_unknown_node_is_empty`

Expected: FAIL, метода `set_setup` нет.

- [ ] **Step 3: Store the fields on NodeRec**

Четыре поля на `NodeRec`, оба плеча `hello()` (обновление и вставка) их не меняют у уже существующей записи. У новой записи — значения по умолчанию из Interfaces. `set_setup` и `setup_of` — как в Interfaces.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --offline -p zvucast --lib registry::`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zvucast/src/registry.rs
git commit -m "$(cat <<'EOF'
feat(zvucast): реестр хранит сырой отчёт о системе узла

EOF
)"
```

---

### Task 4: Склейка Spotify в API

**Files:**
- Modify: `zvucast/src/controller.rs`
- Modify: `zvucast/tests/controller.rs`
- Test: `zvucast/src/controller.rs` (`mod tests`) и один HTTP-тест в `zvucast/tests/controller.rs`

**Interfaces:**
- Consumes: `Registry::set_setup`, `Registry::setup_of`, поля `Hello` и `Setup` из Task 1
- Produces: `controller::merge_setup(reported: bool, issues: Vec<SetupIssue>, soloist: SoloistState, soloist_command: &str, spotify: bool) -> Option<Vec<SetupIssue>>`
  - `reported == false` → `None`, даже если Spotify включён
  - иначе копия `issues`, и если `spotify` и состояние `Missing`, `Expired` или `Failed`, в конец добавляется `SetupIssue`:
    - `id` = `soloist`
    - `command` = переданная строка, как есть (может быть пустой)
    - `detail` = `бинарника нет` / `soloist --version завершился с кодом 10` / `soloist --version не ответил`
  - `Ok` и `Unknown` строку не добавляют
- `NodeApi` получает `setup: Option<Vec<SetupIssue>>`. В `list_nodes` оно считается через `merge_setup` из `setup_of` и уже посчитанного `spotify`.

- [ ] **Step 1: Write the failing test**

В `mod tests` файла `zvucast/src/controller.rs`:

```rust
#[test]
fn spotify_appends_soloist_only_when_the_agent_reported() {
    let local = vec![SetupIssue { id: "pipewire".into(), command: "apt".into(), detail: String::new() }];
    let merged = merge_setup(true, local.clone(), SoloistState::Missing, "curl soloist", true).unwrap();
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[1].id, "soloist");
    assert_eq!(merged[1].detail, "бинарника нет");
    assert_eq!(merged[1].command, "curl soloist");

    let expired = merge_setup(true, vec![], SoloistState::Expired, "curl soloist", true).unwrap();
    assert_eq!(expired[0].detail, "soloist --version завершился с кодом 10");
    let failed = merge_setup(true, vec![], SoloistState::Failed, "", true).unwrap();
    assert_eq!(failed[0].detail, "soloist --version не ответил");
    assert_eq!(failed[0].command, "");

    assert!(merge_setup(true, local.clone(), SoloistState::Ok, "curl soloist", true).unwrap().iter().all(|i| i.id != "soloist"));
    assert!(merge_setup(true, vec![], SoloistState::Unknown, "", true).unwrap().is_empty());
    assert!(merge_setup(true, local.clone(), SoloistState::Missing, "curl soloist", false).unwrap().iter().all(|i| i.id != "soloist"));
    assert!(merge_setup(false, local, SoloistState::Missing, "curl soloist", true).is_none());
}
```

В `zvucast/tests/controller.rs` HTTP-тест. Хелперы `hello_on` и `http_body` уже есть. Открыть сокет так же, как `connect_named`, но послать сырой JSON `Hello` без ключей `setup`, `soloist`, `soloist_command` (остальные поля как в `hello_on` для узла `pc`). Затем `GET /api/nodes` и у этого узла `setup` — JSON `null`.

Второй узел шлёт обычный `Hello` из хелпера (там `setup: Some(vec![])`, `soloist: Unknown`) — этого мало для строки Soloist. Добавить хелпер `hello_soloist`, копия `hello_on`, но `soloist: SoloistState::Missing` и `soloist_command: "curl soloist"`. После него `PATCH /api/nodes/pc` телом `{"spotify":true}` (так уже делает тест `spotify_checkbox_off_does_not_start_soloist_even_with_a_key`). `GET /api/nodes`: у узла в `setup` есть элемент `id == "soloist"`, `detail == "бинарника нет"`, `command == "curl soloist"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --offline -p zvucast --lib controller::spotify_appends_soloist_only_when_the_agent_reported`

Expected: FAIL, `merge_setup` не определён.

- [ ] **Step 3: Merge and store**

Функция `merge_setup` — `pub(crate)`, тело ровно из Interfaces.

В `handle_socket` удалить заглушку `let _ = (setup, soloist, soloist_command)`. После `reg.hello(...)` вызвать `set_setup`: `reported` = `setup.is_some()`, `issues` = `setup.unwrap_or_default()`, дальше `soloist` и `soloist_command`.

В цикле сокета рядом с `AgentMsg::Telemetry` добавить:

```rust
Ok(AgentMsg::Setup { setup, soloist, soloist_command }) => {
    st.reg.lock().await.set_setup(&node_id, true, setup, soloist, soloist_command);
}
```

В `NodeApi` поле `setup`. В `list_nodes` внутри `map` взять `setup_of` у реестра до `drop(reg)` — сейчас реестр отпускается раньше цикла. Сохранить четвёрки рядом с `discovered`, затем `merge_setup(..., spotify)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --offline -p zvucast --lib controller::spotify_appends_soloist_only_when_the_agent_reported`

Expected: PASS.

Run: `cargo test --offline -p zvucast --test controller spotify_appends`

Имя HTTP-теста задать `spotify_appends_soloist_issue_on_the_node_json` и `old_hello_has_null_setup`. Запустить оба фильтрами.

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zvucast/src/controller.rs zvucast/tests/controller.rs
git commit -m "$(cat <<'EOF'
feat(zvucast): сводка узла добавляет soloist только при включённом spotify

EOF
)"
```

---

### Task 5: Агент публикует снимок

**Files:**
- Modify: `zvucast/src/agent.rs`
- Modify: `zvucast/src/ws_client.rs`
- Modify: `zvucast/tests/ws_client.rs`
- Test: `zvucast/tests/ws_client.rs`

**Interfaces:**
- Consumes: `setup::inspect`, `setup::SystemHost`, `setup::SetupSnapshot`, `AgentMsg::Setup`
- Produces: `ws_client::run` принимает дополнительный аргумент `setup: tokio::sync::watch::Receiver<SetupSnapshot>` сразу перед `pending_finished`. Пять вызовов в `zvucast/tests/ws_client.rs` и вызов в `agent.rs` передают приёмник. Тестовый приёмник создаётся из `watch::channel` со снимком `ready: true`, пустым `setup`, `SoloistState::Unknown`, пустой командой — прежние проверки Hello не ждут 5 секунд.

Поведение `connect_once` перед сборкой `Hello`:

```rust
let _ = tokio::time::timeout(Duration::from_secs(5), setup.wait_for(|s| s.ready)).await;
let snap = setup.borrow().clone();
```

Поля `Hello`: `setup: Some(snap.setup)`, `soloist: snap.soloist`, `soloist_command: snap.soloist_command`.

После успешной отправки `Hello` запомнить клон снимка. В том же `select`, где уже читается телеметрия, добавить ветку `setup.changed()`. Если новый снимок отличается полями `setup`, `soloist` или `soloist_command`, послать `AgentMsg::Setup` с этими тремя полями и запомнить его. `ready == false` не слать. Ошибка записи в сокет завершает сессию так же, как ошибка телеметрии.

В `agent::run` до `ws_client::run`:

```rust
let (setup_tx, setup_rx) = watch::channel(setup::SetupSnapshot::pending());
tokio::spawn(async move {
    loop {
        let snap = tokio::task::spawn_blocking(|| setup::inspect(&setup::SystemHost)).await;
        if let Ok(snap) = snap {
            let _ = setup_tx.send(snap);
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
});
```

`SetupSnapshot::pending()` — все поля пустые, `soloist: Unknown`, `ready: false`. `SystemHost` реализует `Host`, читая живые пути из спецификации: `/etc/os-release`, `/etc/systemd/system/zvucast-agent.service`, `/etc/security/limits.conf`, `/usr/local/bin/soloist`, `/sys/class/net`. `uname_m` — `std::env::consts::ARCH` не равен `uname -m` для `armv7l` (Rust говорит `arm`). Читать `uname -m` через `run(2, "uname", &["-m"])`. Беспроводные интерфейсы — каталоги `/sys/class/net/*/wireless`, имена по алфавиту. Пользовательский `systemctl` запускать так: `run(2, "systemctl", &["--user", "is-active", service])` с переменными окружения `XDG_RUNTIME_DIR=/run/user/<uid>` и `DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/<uid>/bus`. Процесс агента и так root, `sudo` для проверки не нужен. `gst-inspect-1.0`, `loginctl`, `iw`, `getent`, `soloist` — напрямую.

- [ ] **Step 1: Write the failing test**

В `zvucast/tests/ws_client.rs` тест `second_snapshot_is_a_setup_message`. Поднять сервер так же, как `hello_telemetry_command_reconnect`. В `watch` положить готовый пустой снимок, передать приёмник в `run`. Дождаться первого кадра, проверить `type == "Hello"` и `setup == []`. Затем в отправитель положить снимок с одной проблемой `id: "linger"` и `ready: true`. Следующий текстовый кадр от клиента (телеметрия может прийти раньше — пропускать кадры, пока `type` не `Setup`, с общим пределом того же `STEP_TIMEOUT`) имеет `setup[0].id == "linger"`. Повторная отправка того же снимка нового `Setup` не порождает: подождать `STEP_TIMEOUT / 4` и убедиться, что нового `Setup` нет. Кадры телеметрии за это время допустимы.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --offline -p zvucast --test ws_client second_snapshot_is_a_setup_message`

Expected: FAIL, у `run` нет аргумента снимка либо `Setup` не уходит.

- [ ] **Step 3: Publish the watch**

Сигнатура, ожидание первого снимка, сообщение `Setup` и спавн в `agent::run` — как в Interfaces. Пять старых вызовов `run` в тестах собрать с готовым пустым снимком.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --offline -p zvucast --test ws_client`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zvucast/src/agent.rs zvucast/src/ws_client.rs zvucast/src/setup.rs zvucast/tests/ws_client.rs
git commit -m "$(cat <<'EOF'
feat(zvucast): агент шлёт свежий снимок системы раз в минуту

EOF
)"
```

---

### Task 6: Блок на странице «Узлы»

**Files:**
- Modify: `web/src/api.ts`
- Modify: `web/src/pages/Nodes.tsx`
- Modify: `web/src/theme.css`

**Interfaces:**
- Consumes: поле `setup` в JSON узла (`null` или массив `{ id, command, detail }`), уже посчитанный `spotify`
- Produces: блок над таблицей. Заголовок «Системе не хватает». Блока нет, если строк не набралось.

Подписи по `id`:

| id | текст |
|---|---|
| `pipewire` | Служба PipeWire не запущена |
| `wireplumber` | WirePlumber не запущен |
| `pipewire-pulse` | PipeWire Pulse не запущен |
| `gstreamer` | Нет плагинов GStreamer |
| `linger` | После перезагрузки PipeWire не поднимется |
| `pipewire-socket` | Нет сокета PipeWire |
| `rt-limits` | Нет rtprio для группы audio |
| `wifi-powersave` | На Wi-Fi включено энергосбережение |
| `controller` | Контроллер включён, но не работает |
| `soloist` | Spotify включён, а Soloist недоступен |

Неизвестный `id` показывается как есть. Непустой `detail` — второй строкой. Непустая `command` — моноширинный блок. Нажатие копирует текст через `navigator.clipboard.writeText`. Отказ буфера оставляет текст выделенным (`window.getSelection` + `selectNodeContents`). Пустую команду не рисовать.

Берутся узлы с `online`. Порядок — порядок `nodes`, он уже приходит отсортированным. Имя — `display_name ?? name`. Внутри узла список проблем, имя не повторяется на каждой строке.

- `setup == null` — строка «Агент не прислал проверку, обновите узел». Если `spotify`, под ней «Spotify включён, а агент не сообщил про Soloist» без команды.
- `setup` массив — по элементу. Контроллер уже добавил Soloist, страница второй раз его не вычисляет.
- `online == false` не участвует, даже если `setup` непустой.

- [ ] **Step 1: Write the failing test**

Отдельного раннера у `web/` нет, новый не заводить. Контракт данных уже закрыт Task 4. Проверка этой задачи — сборка типов.

В `web/src/api.ts` к `NodeView` добавить:

```ts
export interface SetupIssue {
  id: string
  command: string
  detail?: string
}

// внутри NodeView
setup?: SetupIssue[] | null
```

- [ ] **Step 2: Run the build to see the type gap**

Пока `Nodes.tsx` поле не читает, сборка и так зелёная. Этот шаг не гоняет браузер. После Step 3 сборка обязана остаться зелёной, а компонент обязан обращаться к `n.setup`.

- [ ] **Step 3: Render the block**

Функция `setupBlock(nodes: NodeView[])` в `Nodes.tsx`. Вставить её результат в `<main className="page">` сразу после `page-head`, до `table-wrap`.

Каркас:

```tsx
function setupBlock(nodes: NodeView[]) {
  const groups = nodes.filter((n) => n.online).flatMap((n) => {
    const rows: { key: string; title: string; detail: string; command: string }[] = []
    if (n.setup == null) {
      rows.push({ key: 'old', title: 'Агент не прислал проверку, обновите узел', detail: '', command: '' })
      if (n.spotify) {
        rows.push({ key: 'soloist-old', title: 'Spotify включён, а агент не сообщил про Soloist', detail: '', command: '' })
      }
    } else {
      n.setup.forEach((issue, i) => {
        rows.push({
          key: `${issue.id}-${i}`,
          title: SETUP_TITLE[issue.id] ?? issue.id,
          detail: issue.detail ?? '',
          command: issue.command,
        })
      })
    }
    if (rows.length === 0) return []
    return [{ id: n.node_id, name: n.display_name ?? n.name, rows }]
  })
  if (groups.length === 0) return null
  return (
    <section className="card setup-gap">
      <h2>Системе не хватает</h2>
      {groups.map((g) => (
        <div key={g.id}>
          <h3 className="device-name">{g.name}</h3>
          {g.rows.map((row) => (
            <div key={row.key}>
              <div>{row.title}</div>
              {row.detail && <p className="hint">{row.detail}</p>}
              {row.command && (
                <pre className="setup-cmd" onClick={(e) => void copyCommand(row.command, e.currentTarget)}>{row.command}</pre>
              )}
            </div>
          ))}
        </div>
      ))}
    </section>
  )
}
```

`SETUP_TITLE` — объект с десятью строками из таблицы. `copyCommand` — как в Interfaces.

В `theme.css`:

```css
.setup-gap { margin: 0 0 1rem; }
.setup-gap h2 { margin: 0 0 0.75rem; font-size: 1.05rem; }
.setup-cmd {
  margin: 0.35rem 0 0.75rem;
  padding: 0.55rem 0.7rem;
  background: var(--bg-sunken);
  border-radius: 8px;
  font-family: var(--mono);
  font-size: 0.78rem;
  white-space: pre-wrap;
  cursor: pointer;
  color: var(--text);
}
```

- [ ] **Step 4: Run the build**

Run: `npm run build` в каталоге `web`.

Expected: PASS, `tsc` не ругается на `setup`.

- [ ] **Step 5: Commit**

```bash
git add web/src/api.ts web/src/pages/Nodes.tsx web/src/theme.css
git commit -m "$(cat <<'EOF'
feat(zvucast): на узлах видно, чего не хватает каждой машине

EOF
)"
```

---

### Task 7: `install.sh`

**Files:**
- Create: `install.sh`
- Create: `install.test.sh`
- Modify: `README.md`
- Test: `install.test.sh`

**Interfaces:**
- Consumes: тексты команд и порядок шагов из спецификации. Дропы байт в байт равны `deploy/pipewire/99-zvucast-quantum.conf` и `deploy/wireplumber/99-zvucast-latency.conf`. Юнит контроллера байт в байт равен `deploy/zvucast-controller.service`. Юнит агента равен `deploy/zvucast-agent.service`, кроме строки `Environment=XDG_RUNTIME_DIR=/run/user/<uid>` с uid пользователя звука.
- Produces: `bash install.sh` с кодами выхода `0` (успех или отказ от подтверждения), `1` (машина не узнана, пакет, сумма, Soloist), `2` (нет терминала и нет пары переменных). При `source install.sh` `main` не выполняется: внизу файла

```bash
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
```

Функции, которые тест вызывает напрямую: `family_of`, `asset_name`, `soloist_arch_name`.

`family_of` читает файл из `ZVUCAST_OS_RELEASE`, иначе `/etc/os-release`. Печатает `apt`, `pacman` или пусто и возвращает `1`, если правила спецификации разошлись или не сошлись.

`asset_name` читает `ZVUCAST_UNAME` (иначе `uname -m`) и модель из `ZVUCAST_MODEL` (иначе `/proc/device-tree/model` без нулевых байтов). Печатает `zvucast-x86_64`, `zvucast-aarch64`, `zvucast-armv7-rpi` или `zvucast-armv7-opi`. Иначе печатает в stderr `uname` и модель и возвращает `1`. Сравнение модели без учёта регистра. Оба имени «Raspberry Pi» и «Orange Pi» — тоже `1`.

`soloist_arch_name` печатает `x86_64`, `arm64` или `arm32` для тех же трёх веток `uname`. Для `armv7l` модель не важна.

Переменные, которые меняют только тест, не меню: `ZVUCAST_ROOT` (пустой значит корень файловой системы), `ZVUCAST_UID`, `ZVUCAST_USER`, `ZVUCAST_TEST_NOTTY=1` (считать, что `/dev/tty` нет). Пользователь звука: если `id -u` равен 0, брать `SUDO_USER`, и если он пуст или `root` — выход `1` с текстом `запусти от обычного пользователя`. Иначе `ZVUCAST_USER` или `id -un`, uid — `ZVUCAST_UID` или `id -u`.

- [ ] **Step 1: Write the failing test**

`install.test.sh` с `set -euo pipefail`. Каталог `work=$(mktemp -d)`, `trap 'rm -rf "$work"' EXIT`. В `PATH` первыми лежат заглушки `curl`, `apt-get`, `pacman`, `sudo`, `systemctl`, `loginctl`, `install`, `usermod`, `tee`, `sha256sum` (настоящий `sha256sum` оставить: заглушка только логирует вызов и зовёт системный, если он нужен; проще вызывать системный `sha256sum` из теста и не подменять его).

`sudo` дописывает аргументы в `$work/sudo.log` и выполняет хвост. Флаг `-v` — выход 0. `-u <user>` — выкинуть два аргумента и выполнить остальное.

`curl` дописывает аргументы в `$work/curl.log`. Если среди аргументов есть `releases/latest`, печатает заголовки с `location: https://github.com/Vallefor/zvucast/releases/tag/v0.1.4` и выходит 0, не следуя редиректу. Если URL кончается на `/SHA256SUMS`, печатает файл сумм, который тест положил в `$work/sums`. Если URL кончается на имени ассета, пишет в stdout содержимое `$work/payload`. Иначе выход 1.

`apt-get` и `pacman` дописывают аргументы в свой лог и выходят 0.

Проверки, каждая с чистыми логами:

1. `source install.sh` при `ZVUCAST_OS_RELEASE` с `ID=debian` печатает `apt`. Файл `ID=cachyos` и `ID_LIKE=arch` печатает `pacman` и не содержит `apt`. `ID=gentoo` возвращает 1.
2. `ZVUCAST_UNAME=x86_64` → `zvucast-x86_64`. `aarch64` → `zvucast-aarch64`, `soloist_arch_name` → `arm64`. `armv7l` и модель `Raspberry Pi 2` → `zvucast-armv7-rpi`. Модель `Orange Pi PC Plus` → `zvucast-armv7-opi`. Модель `Weird Board` → код 1, логи `apt-get` и `pacman` пусты (эти функции пакеты не ставят; отдельно прогнать `main` с этой моделью и убедиться, что логов нет).
3. `main` при `ZVUCAST_UNAME=x86_64`, debian, `ZVUCAST_SPOTIFY=0`, `ZVUCAST_CONTROLLER=0`, `ZVUCAST_ROOT=$work`, `ZVUCAST_USER=audio`, `ZVUCAST_UID=1000`. Полезная нагрузка — байты `binary`, её sha256 лежит в суммах строкой `sha256sum`. После успеха файл `$work/usr/local/bin/zvucast` равен `binary`. В логе apt есть `pipewire`. В логе pacman пусто. В логе systemctl есть `disable` и `zvucast-controller`. `$work/etc/zvucast.toml` содержит `controller = "ws://127.0.0.1:8080/ws"` и `playback_backend = "pipewire"`. Дроп квантов совпадает с файлом репозитория (`diff -q`). Юнит контроллера совпадает с `deploy/zvucast-controller.service`. Юнит агента содержит `XDG_RUNTIME_DIR=/run/user/1000`.
4. Повторный `main` после дописи в toml строки `name = "keep"` не меняет файл (сравнить контрольную сумму до и после).
5. Подменить curl так, чтобы полезная нагрузка не совпала с суммой. Код выхода `main` — 1. Прежний файл бинарника не меняется.
6. `ZVUCAST_SPOTIFY=1` `ZVUCAST_CONTROLLER=0`: в `curl.log` есть `soloist_release_x86_64`. В systemctl есть `disable`.
7. `ZVUCAST_TEST_NOTTY=1` и не задан `ZVUCAST_SPOTIFY`. Код 2. Лог apt пуст.
8. CachyOS: в логе pacman есть `pipewire`, лог apt пуст.

- [ ] **Step 2: Run test to verify it fails**

Run: `bash install.test.sh`

Expected: FAIL, `install.sh` отсутствует.

- [ ] **Step 3: Write install.sh and the README line**

Скрипт `set -euo pipefail`. Шапка до вопросов: слово `Zvucast`, `ID` из os-release, `uname -m`, имя ассета. Цвет только если stdout — терминал.

Меню, если есть `/dev/tty` и не задан `ZVUCAST_TEST_NOTTY`: Soloist по умолчанию нет, контроллер по умолчанию нет, подтверждение по умолчанию да. Пустой ввод берёт скобки. Принимаются `y/yes/д/да` и `n/no/н/нет`. Ответ «нет» на подтверждение — выход 0, файлы не писать. Обе переменные `ZVUCAST_SPOTIFY` и `ZVUCAST_CONTROLLER` равны `0` или `1` — меню нет, перечень напечатать и ставить сразу. Нет терминала и нет одной из них или значение иное — выход 2, на stderr имена обеих переменных.

Не root вызывает `sudo -v` один раз и дальше привилегированные команды через `sudo`. Себя заново не исполняет.

Порядок `main`, пути под `"$ZVUCAST_ROOT"`:

1. Пакеты одной командой. Debian: `pipewire pipewire-bin pipewire-pulse wireplumber gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-alsa gstreamer1.0-pipewire alsa-utils curl ca-certificates iw dbus-user-session rtkit python3`. Arch: `pipewire pipewire-alsa pipewire-pulse wireplumber gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugin-pipewire alsa-utils curl ca-certificates iw rtkit python`.
2. Дропы в `/etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf` и `/etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf`. Содержимое heredoc без лишних пробелов, байт в байт как файлы `deploy/`.
3. Если в limits нет `@audio - rtprio 95`, дописать две строки `@audio - rtprio 95` и `@audio - memlock unlimited`. `usermod -aG audio` пользователя звука.
4. `loginctl enable-linger`, `systemctl start user@<uid>.service`, затем `systemctl --user enable --now` для `pipewire`, `pipewire-pulse`, `wireplumber` с `XDG_RUNTIME_DIR` и `DBUS_SESSION_BUS_ADDRESS` этого uid.
5. Wi-Fi всегда. Файл `/etc/NetworkManager/conf.d/wifi-powersave-off.conf` содержит секцию `[connection]` и строку `wifi.powersave = 2`. Если в `PATH` есть `nmcli`, у соединений типа `802-11-wireless` поставить `802-11-wireless.powersave 2`. Юнит `/etc/systemd/system/zvucast-wifi-powersave.service`: `Type=oneshot`, `After=network-online.target`, `Wants=network-online.target`, `RemainAfterExit=yes`, `WantedBy=multi-user.target`. `ExecStart` обходит `/sys/class/net/wlan*` и вызывает `iw dev <имя> set power_save off`, ненулевой код одного интерфейса не валит юнит. `systemctl enable --now` этого юнита.
6. Релиз. `curl -sSI -H 'User-Agent: zvucast' --max-time 30 https://github.com/Vallefor/zvucast/releases/latest` без `-L` и без `-f`. Из заголовка `location` взять `https://github.com/Vallefor/zvucast/releases/tag/v<version>`, где version — три числа. Иначе выход 1. Скачать `SHA256SUMS` и ассет в временный каталог через `curl -fL`. Строка сумм этого файла совпала (`sha256sum -c`) — `install -m 755` в `/usr/local/bin/zvucast`. Иначе выход 1, старый бинарник не трогать.
7. Юниты. Агент — текст `deploy/zvucast-agent.service` с подставленным uid. `enable --now zvucast-agent`. Контроллер — точная копия `deploy/zvucast-controller.service`. Ответ да — `enable --now`. Ответ нет — `disable --now`.
8. Toml только если файла нет. Содержимое — блок из спецификации. `<iface>`: первый по алфавиту каталог `/sys/class/net/*`, у которого есть `carrier` со значением `1`, имя не `lo`, не начинается с `veth`, нет подкаталога `wireless` и нет `bridge`. Иначе первый по алфавиту каталог с подкаталогом `wireless`. Иначе литерал `eth0`. `name` — `hostname -s`.
9. Soloist только при согласии. URL `https://soloist-builds.spotifycdn.com/soloist_release_<arch>.tar.gz`. Внутри файл `soloist`, `install -m 755` в `/usr/local/bin/soloist`, затем `soloist --version`. Прямой curl не удался — один повтор: python (`python3` на apt, `python` на pacman) шлёт прямой UDP-запрос A на `1.1.1.1`, системный резолвер не используется, берётся первый адрес, `curl --resolve soloist-builds.spotifycdn.com:443:<ip>` качает тот же URL. Повтор не удался — выход 1, на stderr однострочник `soloist_command` для этой архитектуры. Пакеты и бинарник к этому моменту уже стоят.

После успеха `systemctl restart zvucast-agent`. Если контроллер enabled — ещё `systemctl restart zvucast-controller`.

В `README.md` после абзаца про сборку добавить:

```markdown
Установка на плату:

```bash
curl -fsSL https://raw.githubusercontent.com/Vallefor/zvucast/main/install.sh | bash
```
```

Строку «Soloist ставится отдельно и в репозиторий не входит.» оставить: бинарник Soloist по-прежнему не в git.

- [ ] **Step 4: Run test to verify it passes**

Run: `bash install.test.sh`

Expected: PASS, все восемь проверок. Временный каталог удалён.

- [ ] **Step 5: Commit**

```bash
git add install.sh install.test.sh README.md
git commit -m "$(cat <<'EOF'
feat(zvucast): установочный скрипт ставит плату по архитектуре

EOF
)"
```
