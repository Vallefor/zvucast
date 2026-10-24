# Устройства из микшера Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Убрать `capture_device` и `playback_device` из конфига: стереослот включённых каналов в УИ выбирает, что открывать; виртуальный выход виден как вход и идёт в те же выходы колонки; счётчик заиканий растёт; у виртуального выхода нет лишнего буфера.

**Architecture:** Слот стерео — каналы 0 и 1 одного устройства, у которого включён канал 0 или 1. Сначала невиртуальное устройство (по алфавиту), иначе виртуальный кабельный sink. Голова захвата открывает это имя. Выход колонки — такое же правило, но виртуальный sink никогда не цель (иначе петля). Канал карты зоны по-прежнему `l`/`r`: микшер раскладывает стереопоток по выходам выбранного устройства. Виртуальный кабельный sink (`zvucast-{id}`, не `*-monitor`) виден во входах. Пока включено железо, виртуальный звук по-прежнему подмешивается монитором зоны и на колонке проходит ту же карту и тот же выход. Оба канала кабеля выключены — монитор этой зоны не подмешивается.

**Tech Stack:** Rust, GStreamer, PipeWire (`pw-loopback`, `pw-record`, `pw-play`), React (Vite).

## Global Constraints

- Стереопоток остаётся двумя каналами. `ChanSel` остаётся `l`/`r`. Новую матрицу каналов не вводить.
- `capture_device` и `playback_device` не обязательны и не выбирают устройство. Старый toml с этими ключами загружается: ключи выкидываются до `toml::from_str`, файл на диске не переписывается.
- Явный `roles` не требует устройства. Авто-детект без ролей: `playback_backend = pipewire` пробует tx и rx элементом PipeWire без имени устройства; `alsa` без ролей и без того, что открывать, — ошибка с текстом, что роли надо задать явно.
- Слот захвата: первое по алфавиту невиртуальное устройство с включённым каналом `index <= 1`. Нет такого — первое виртуальное по тому же правилу. Канал 0 включённого слота → `stream = l`, канал 1 → `r`. Выключенный левый не становится правым.
- `capture_target` возвращает имя, которое уже можно открыть: обычный ключ как есть; `zvucast` → `zvucast-monitor`; `zvucast-{id}` (не оканчивается на `-monitor`) → `zvucast-{id}-monitor`. Ключи-заглушки `capture` и `playback` → `None`.
- Строка ALSA (`plughw:`, `hw:`, … — то, для чего `configured_pipewire_node` даёт `None`) — цель только бэкенда ALSA. PipeWire такое имя не открывает (голова тишины), как раньше. Это не баг.
- Слот выхода считается так же, но виртуальные ключи в слот выхода не входят. `playback_target` — ключ этого слота или `None`. Виртуальный sink целью воспроизведения не бывает.
- Пока цель выхода `None` и бэкенд PipeWire, `pw-play` берёт прежний запасной sink (`desktop_playback_target`), не zvucast. Как только УИ выбрало устройство, играть только в него.
- Оба канала `index <= 1` кабеля `zvucast-{id}` выключены → зона `id` в `muted_zone_ids`. Кабеля в списке нет — зону не глушить.
- Если голова захвата уже открыла тот же монитор, что `zone.source`, источник зоны в оверлее `None` (не писать один монитор дважды).
- Поле телеметрии `RoleRx.dropped` остаётся. В УИ подпись **Заикания**. Число = underrun’ы шины + `num-late` + `num-lost` у `rtpjitterbuffer` (`jb*`). `stats.dropped` у sink не использовать: при skew оно остаётся 0.
- `pw-loopback` виртуального выхода, Spotify и Sendspin: аргумент `--latency` `3` (миллисекунды) и в props обоих концов `node.lock-quantum=true`. `node.latency=128/48000` оставить. Очереди ветки монитора (`mhold-*`, `mq-*`, `mlq-*`) — `leaky=downstream`, `max-size-buffers=2`, `max-size-time=0`.
- Комментарии и строки УИ по-русски, в том же тоне, что соседний код.
- Не коммитить `target/`, `web/node_modules/`, `web/dist/assets/`. `web/dist/index.html` коммитить, если сборка его изменила.
- Тесты: `CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline …` из корня worktree. Не запускать второй cargo, пока первый держит этот `target`.
- Сообщения коммитов как в истории: `fix(zvucast): …` / `feat(zvucast): …`, по-русски, про причину.

Рабочий каталог: `/home/vallefor/ai-work/zvucast-ui-devices`. Запись и cargo — вне песочницы (`required_permissions: ["all"]`), репозиторий не в workspace Cursor.

---

### Task 1: Короткий виртуальный кабель

**Files:**
- Modify: `zvucast/src/virtual_output.rs`
- Modify: `zvucast/src/soloist.rs` (вызов `pw-loopback`)
- Modify: `zvucast/src/sendspin.rs` (вызов `pw-loopback`)
- Modify: `zvucast/src/pipeline.rs` (`add_monitor_branch`)
- Test: те же файлы, `#[cfg(test)]`

**Interfaces:**
- Consumes: ничего нового
- Produces: `virtual_output::LOOPBACK_LATENCY_MS: &str = "3"`; `virtual_output::loopback_front(name: &str) -> Vec<String>` — аргументы до `--capture-props`: `--name`, имя, `--channels`, `2`, `--channel-map`, `[ FL, FR ]`, `--latency`, `3`. Пропсы кабеля содержат `node.lock-quantum=true` и `node.latency=128/48000`.

- [ ] **Step 1: Падающий тест пропсов и аргументов**

В `virtual_output.rs` тест: `loopback_front("zvucast-zone-hall")` содержит подряд `--latency`, `3`. `capture_props(...)` и `playback_props(...)` содержат `node.lock-quantum=true`. Существующий тест `sink_is_visible_to_the_applet` не ослаблять: `node.virtual=false` у sink, `node.passive=true` и `node.virtual=true` у source.

- [ ] **Step 2: Запустить тест, увидеть FAIL**

`CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline -p zvucast --lib virtual_output:: -- --test-threads=8`

- [ ] **Step 3: Реализация**

`spawn` виртуального кабеля собирает команду из `loopback_front` плюс прежние `--capture-props` / `--playback-props`. В `capture_props` и `playback_props` добавить `node.lock-quantum=true`, `node.latency=128/48000` не убирать.

`soloist.rs` и `sendspin.rs`: тот же `--latency 3` и `node.lock-quantum=true` в их строках props. Не копировать вторую реализацию фронта, если вызов можно собрать через `loopback_front`; имя процесса и props у Spotify/Sendspin свои.

`add_monitor_branch`: очереди `mhold-{token}`, `mq-{token}`, `mlq-{token}` — `leaky=downstream`, `max-size-buffers=2`, `max-size-bytes=0`, `max-size-time=0`. Голову line-in (`hold` у `cap`) не менять.

Тест пайплайна: собрать tx PipeWire с одной зоной, у которой `source: Some("zvucast-hall-monitor")`, и проверить у `mhold-*` и `mq-*` `max-size-buffers == 2` и `leaky == Downstream`. Имена элементов уже есть в `add_monitor_branch`.

- [ ] **Step 4: Тесты**

`CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline -p zvucast --lib virtual_output:: soloist:: sendspin:: pipeline:: -- --test-threads=8`

Ожидание: PASS.

- [ ] **Step 5: Коммит**

`fix(zvucast): у виртуального выхода нет лишнего буфера`

---

### Task 2: Счётчик заиканий

**Files:**
- Modify: `zvucast/src/supervisor.rs`
- Modify: `web/src/pages/Nodes.tsx` (только подпись строки)
- Test: `zvucast/src/supervisor.rs`

**Interfaces:**
- Consumes: `SupEvent::Underrun`, элементы `jb{port}` с property `stats` (`num-late`, `num-lost`, оба `u64`)
- Produces: `supervisor::stutter_count(underruns: u64, late: u64, lost: u64) -> u64` = сумма через `saturating_add`. `Runner::stats().dropped` равен `stutter_count(underruns, jitter_late, jitter_lost)`. Поле JSON не переименовывать.

- [ ] **Step 1: Падающий тест**

```rust
#[test]
fn stutter_count_adds_underruns_and_late_packets() {
    assert_eq!(stutter_count(0, 0, 0), 0);
    assert_eq!(stutter_count(1, 2, 4), 7);
    assert_eq!(stutter_count(u64::MAX, 1, 0), u64::MAX);
}
```

Плюс: обработчик сообщения шины, который уже рождает `SupEvent::Underrun`, увеличивает счётчик, который потом читает `stats`. Если текущий `bus_watch_reports_underrun` строит `Runner`, после события `stats().dropped >= 1`. Если нет — вынести `fn note_underrun(counter: &AtomicU64)` и проверить, что один вызов даёт 1. Не считать `stats.dropped` sink’а.

- [ ] **Step 2: FAIL, затем реализация**

`stats()` обходит детей пайплайна, имя начинается с `jb`, есть property `stats`, достаёт `num-late` и `num-lost` (`unwrap_or(0)`). Underrun’ы — `AtomicU64`, общий с потоком шины. Подпись в `Nodes.tsx`: было `Dropped`, стало `Заикания`. Число по-прежнему `rx.dropped`.

- [ ] **Step 3: Тесты**

`CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline -p zvucast --lib supervisor:: -- --test-threads=8`

УИ: `cd web && npm run build`. Если поменялся только текст в бандле, закоммитить `web/dist/index.html`, не `web/dist/assets/`.

- [ ] **Step 4: Коммит**

`fix(zvucast): заикания видны, а не вечный ноль dropped`

---

### Task 3: Конфиг больше не требует устройств

**Files:**
- Modify: `zvucast/src/config.rs`
- Modify: `zvucast/src/agent.rs` (`resolve_roles` и его тесты)
- Test: те же

**Interfaces:**
- Consumes: текст toml
- Produces: `config::load` молча отбрасывает ключи `capture_device` и `playback_device` (не комментарии, не `playback_backend`). Поля структуры пока остаются `Option`, `#[serde(default)]`: их снимет Task 5, когда агент перестанет их читать. До Task 5 агент ещё может их открывать — это временно, не расширять использование.

- [ ] **Step 1: Тест загрузки**

Старый образец с `capture_device` и `playback_device` парсится. После strip эти поля `None` (ключи не доходят до serde). `playback_backend = "pipewire"` на месте. Строка-комментарий `# capture_device = "…"` не ломает файл и не становится значением. `unknown field` по-прежнему ошибка для опечатки, которая не является этими двумя ключами.

- [ ] **Step 2: `resolve_roles`**

Явный `roles = ["tx"]` при `capture_device: None` — `Ok`. Явный `roles = ["rx"]` при `playback_backend = alsa` и `playback_device: None` — `Ok`. Пустой `roles = []` — по-прежнему ошибка. `roles: None` и оба устройства `None` и бэкенд alsa — ошибка, в тексте есть `roles`. `roles: None`, бэкенд pipewire, probe возвращает true — обе роли, без имени устройства (probe для pipewire вызывается с любым плейсхолдером, устройство не читается). Обновить тесты `agent.rs`, которые ждут ошибку «tx без capture_device».

- [ ] **Step 3: Прогон**

`CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline -p zvucast --lib config:: agent::resolve -- --test-threads=8`

Фильтр подобрать по фактическим именам тестов `resolve_roles`. Ожидание: PASS, крейт компилируется целиком (`cargo test --offline -p zvucast --lib --no-run`).

- [ ] **Step 4: Коммит**

`fix(zvucast): роли не требуют устройств в конфиге`

---

### Task 4: Слот стерео из включённых каналов

**Files:**
- Modify: `zvucast/src/inputs.rs`
- Modify: вызовы `merge_inputs` в `zvucast/src/controller.rs` и тестах — третий аргумент удалить
- Test: `zvucast/src/inputs.rs`

**Interfaces:**
- Consumes: `InputView`, `is_virtual_device_key`
- Produces:
  - `merge_inputs(discovered: &[CaptureChan], prefs: &[InputPref]) -> Vec<InputView>` — без устройства из конфига
  - `capture_target(views: &[InputView]) -> Option<String>`
  - `playback_target(views: &[InputView]) -> Option<String>`
  - `muted_zone_ids(views: &[InputView]) -> BTreeSet<String>`
  - `open_capture_name(key: &str) -> String`
  - `is_zone_sink_key(key: &str) -> bool` — `zvucast-…`, не `zvucast`, не окончание `-monitor`

`visible_inputs`: кабельный sink (`is_zone_sink_key`) виден. Скрыты legacy `device_key == "zvucast"`, ключи `*-monitor`, и чужой ключ, у которого `device_name` равен `Zvucast` или начинается с `Zvucast:`.

- [ ] **Step 1: Заменить тесты под новые правила**

`virtual_speaker_cables_stay_out_of_the_settings_list` переписать:

- микрофон `plughw:CARD=Generic_1` виден;
- кабель `zvucast-z1` / `Zvucast: Зал` виден;
- legacy `zvucast` скрыт;
- ключ `alsa_input.pci-…` с именем `Zvucast: Спальня` скрыт.

Новые тесты:

```rust
#[test]
fn hardware_slot_beats_the_virtual_cable() {
    let discovered = vec![
        ch("hw:0", "plughw:CARD=Generic_1", 0),
        ch("hw:1", "plughw:CARD=Generic_1", 1),
        ch("z:0", "zvucast-hall", 0),
        ch("z:1", "zvucast-hall", 1),
    ];
    let views = merge_inputs(&discovered, &[]);
    assert_eq!(capture_target(&views).as_deref(), Some("plughw:CARD=Generic_1"));
    assert!(views.iter().filter(|v| v.device_key == "zvucast-hall").all(|v| v.stream.is_none()));
    assert_eq!(playback_target(&views).as_deref(), Some("plughw:CARD=Generic_1"));
}

#[test]
fn virtual_cable_becomes_the_slot_when_hardware_is_off() {
    let prefs = vec![
        InputPref { id: "hw:0".into(), name: "л".into(), enabled: false },
        InputPref { id: "hw:1".into(), name: "п".into(), enabled: false },
    ];
    let discovered = vec![
        ch("hw:0", "mic", 0),
        ch("hw:1", "mic", 1),
        ch("z:0", "zvucast-hall", 0),
        ch("z:1", "zvucast-hall", 1),
    ];
    let views = merge_inputs(&discovered, &prefs);
    assert_eq!(capture_target(&views).as_deref(), Some("zvucast-hall-monitor"));
    assert_eq!(views.iter().find(|v| v.id == "z:0").unwrap().stream.as_deref(), Some("l"));
    assert_eq!(playback_target(&views), None);
}

#[test]
fn both_virtual_channels_off_mutes_that_zone_only() {
    let prefs = vec![
        InputPref { id: "z:0".into(), name: "л".into(), enabled: false },
        InputPref { id: "z:1".into(), name: "п".into(), enabled: false },
    ];
    let views = merge_inputs(&[
        ch("z:0", "zvucast-hall", 0),
        ch("z:1", "zvucast-hall", 1),
        ch("b:0", "zvucast-bed", 0),
    ], &prefs);
    assert!(muted_zone_ids(&views).contains("hall"));
    assert!(!muted_zone_ids(&views).contains("bed"));
}

#[test]
fn pipewire_node_stays_the_capture_name() {
    let views = merge_inputs(&[
        ch("mic:0", "alsa_input.usb-BY-V20.analog-stereo", 0),
        ch("mic:1", "alsa_input.usb-BY-V20.analog-stereo", 1),
    ], &[]);
    assert_eq!(capture_target(&views).as_deref(), Some("alsa_input.usb-BY-V20.analog-stereo"));
}
```

`capture_target` больше не вызывает `configured_pipewire_node`. Фильтр ALSA/PipeWire — в агенте (Task 5).

Обновить старые вызовы `merge_inputs(..., Some(...))` на два аргумента. Поведение «единственное устройство получает l/r» сохранить. Тест, который ждал `capture_target == None` для `plughw:`, заменить ожиданием самого `plughw`.

- [ ] **Step 2: Реализация и компиляция контроллера**

Все `merge_inputs` в крейте — два аргумента. `push_capture_target` пока может слать `capture_target` как раньше; `SetPlaybackTarget` добавит Task 5.

- [ ] **Step 3: Тесты**

`CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline -p zvucast --lib inputs:: -- --test-threads=8`

Затем `--no-run` всего пакета `zvucast`, чтобы контроллер собрался.

- [ ] **Step 4: Коммит**

`feat(zvucast): слот стерео берётся из включённых каналов`

---

### Task 5: Агент открывает цель из УИ

**Files:**
- Modify: `zproto/src/lib.rs` — `CtlMsg::SetPlaybackTarget { device_key: Option<String> }`, `CtlMsg::SetMutedZones { zones: Vec<String> }` рядом с `SetCaptureTarget`. Старые сообщения без этих вариантов читаются как раньше (это новые теги, не поля старых).
- Modify: `zvucast/src/agent.rs` — `make_tx` / `make_rx` / обработка команд
- Modify: `zvucast/src/controller.rs` — пуш целей
- Modify: `zvucast/src/pipeline.rs` — `capture_node_name` возвращает `String` и идемпотентен для `*-monitor`
- Modify: `zvucast/src/config.rs` — удалить поля `capture_device` и `playback_device`
- Modify: `web/src/pages/Nodes.tsx` — строка «Устройства» убрать; подсказки входов и выходов
- Modify: тесты, которые конструируют `AgentConfig` или матчат `CtlMsg` целиком
- Test: `zproto`, `zvucast/src/agent.rs`, `zvucast/tests/controller.rs`

**Interfaces:**
- Consumes: `capture_target`, `playback_target`, `muted_zone_ids`, `configured_pipewire_node`, `capture_node_name`, `desktop_playback_target`
- Produces: команды контроллера и открытие устройства агентом по таблице ниже

Таблица открытия (агент):

| Бэкенд | Ключ из цели | Действие |
| --- | --- | --- |
| alsa, tx | `configured_pipewire_node(key).is_none()`, ключ есть | `alsasrc` этого ключа |
| alsa, rx | то же | `alsasink` этого ключа |
| pipewire, tx | `configured_pipewire_node(key)` = `Some` | `pw-record` на `capture_node_name` |
| pipewire, tx | ключ ALSA или `None` | голова тишины, как сейчас без цели |
| pipewire, rx | узел PipeWire | `pw-play --target` этот узел |
| pipewire, rx | `None` | прежний `desktop_playback_target` |
| любой | виртуальный sink как playback | не открывать; считать цель `None` |

`SetPlaybackTarget` — тот же жизненный цикл, что `SetCaptureTarget`: то же значение → не пересобирать; новое → дропнуть runner rx и вооружить гейт.

`SetMutedZones`: множество id. `overlay_sources` ставит `zone.source = None`, если id в множестве. Если `capture_target` уже равен текущему `zone.source`, тоже `None`.

Контроллер шлёт обе команды там же, где сегодня `push_capture_target` (hello, смена prefs входов и выходов). Входы: `capture_target` + `muted_zone_ids` по видам после `merge_inputs` (виртуальные кабели уже не спрятаны). Выходы: `playback_target`.

Подсказки УИ, дословно:

- Входы: `Отмеченные каналы входят в стереопоток. Виртуальный выход — такой же вход: он подмешивается в зону и раскладывается по выходам колонки в карте зоны.`
- Выходы: `Отмеченные каналы — выходы колонки в карте зоны. Звук играет на устройство этих каналов.`

Строку фактов `Устройства` удалить.

Hello по-прежнему может нести `capture_device` / `playback_device` как то, что агент открыл (или `null`). Из конфига они больше не читаются. Тесты hello, которые ждут строку из toml, перевести на `null`, пока цель не задана командой.

- [ ] **Step 1: Тесты протокола**

Roundtrip `SetPlaybackTarget` и `SetMutedZones`, по образцу теста `SetCaptureTarget` в `zproto`.

Юнит без ALSA на чистые функции агента, если они уже вынесены (`handle_set_capture_target`): то же для playback. Оверлей: зона `hall` с источником `zvucast-hall-monitor` и mute `hall` даёт `source: None`. Та же зона без mute и с головой `zvucast-hall-monitor` тоже даёт `None`. Чужой источник при голове line-in сохраняется.

- [ ] **Step 2: Реализация до зелёного `--no-run`**

Удалить поля конфига только когда компилятор больше не читает их для открытия устройства. `resolve_roles` к этому моменту уже не смотрит на них (Task 3); сигнатуру probe не расширять ради удалённых полей.

Каждый новый вариант `CtlMsg` разобрать явно. Ветку `_` не добавлять.

- [ ] **Step 3: Тесты**

`CARGO_TARGET_DIR=/home/vallefor/ai-work/zvucast/target cargo test --offline --workspace -- --test-threads=8`

`cd web && npm run build`. Закоммитить `web/dist/index.html`, если изменился.

- [ ] **Step 4: Коммит**

`feat(zvucast): воспроизведение и захват идут по каналам из УИ`

---

### Task 6: Лабораторные конфиги без устройств

Репозиторий `/home/vallefor/ai-work/real-time-sound`, ветка не `main`: `ui-devices` от текущего `main`. В toml стенда удалить строки `capture_device` и `playback_device` и комментарии, которые велят их заполнять. `roles` и `playback_backend` не трогать.

Файлы:

- `lab/deploy/zvucast.pc.toml`
- `lab/deploy/zvucast.pi3.toml`
- `lab/deploy/zvucast.pi4-tx.toml`
- `lab/deploy/zvucast.rpi2.toml`
- `lab/deploy/zvucast.opi-rx2.toml`
- `lab/deploy/zvucast.rx-node-2.toml`
- `lab/deploy/README.md` — устройства выбираются включёнными каналами в УИ, не ключами toml
- `.cursor/skills/setup-rpi-zvucast/SKILL.md`
- `.cursor/skills/setup-opi-zvucast/SKILL.md`
- `.cursor/skills/setup-gy-pcm5102/SKILL.md`

Скиллы больше не пишут эти ключи в конфиг. Имя узла PipeWire по-прежнему нужно, чтобы понять, какая карта живая, но в toml узла его нет: карта выбирается в микшере каналов.

Коммит в `real-time-sound`: `lab: устройства узла задаёт микшер, не toml`. Не пушить.
