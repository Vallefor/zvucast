# Источники зоны Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spotify, Sendspin и виртуальная карта становятся источниками зоны со своим RTP и своей картой каналов, а не примесью к микрофону.

**Architecture:** В `state.json` один список источников: микрофон узла или один из трёх видов на узле-хозяине. Контроллер шлёт микрофон прежним `SetEndpoints` с `source: None`, а вид — новым `SetFeeds` только хозяину. Колонка складывает порты как второго передатчика. Устройство «Zvucast: {имя зоны}» поднимается только пока связь есть.

**Tech Stack:** Rust (`zproto`, `zvucast`), React/TypeScript (`web/`), PipeWire, GStreamer.

**Spec:** `docs/superpowers/specs/2026-10-28-feed-sources-design.md`

## Global Constraints

- В одной зоне не больше одного Spotify, одного Sendspin и одной виртуальной карты. Новый хозяин занимает место прежнего.
- Один узел может вести один вид в нескольких зонах. У каждой своё устройство «Zvucast: {имя зоны}». Звук этого устройства слышит только эта зона.
- Галочка на узле показывает источник, даже в тишине. В зону он попадает только явной связью. Снятие галочки убирает источник и карты каналов во всех зонах.
- Снять связь в одной зоне не трогает другие.
- Связанные источники смешиваются. Sendspin не гасит Spotify.
- Старый `state.json`, где `sources` — список id узлов, читается как микрофоны, без видов.
- Уже настроенные зоны сами связи не получают.
- Один ключ Spotify — один играющий поток. Второй Soloist с тем же ключом молчит.
- Аудио 48 кГц PCM L16. Один jitter buffer — один поток. Два потока одного узла не делят порт. Слотов по-прежнему 64, лишние имена не входят.
- Тихий источник не глушит громкий. Громкий будит зону при тихом микрофоне.
- Узел без `supports_virtual_output` эти галочки не показывает.
- Офлайн и выключение узла связь не снимают.
- Угон колонок, `jb_ms`, порог и таймаут тишины, DSCP и громкость устройств не меняются.
- Лабораторный репозиторий `real-time-sound` не менять. Версию в `zvucast/Cargo.toml` не менять. Не пушить.

## Review Focus

- Телефон играет в «Zvucast: Зона 1», та же галочка связана и с Зоной 2: в `SetFeeds` Зоны 2 другие колонки и другой монитор. Тест в задаче 3.
- Снятие галочки Spotify стирает карты `spotify:{node}` у всех колонок всех зон, не только id источника. Тест в задаче 2.
- Колонка, которую забрала более поздняя зона, не получает порт вида прежней зоны. Тест в задаче 3.
- Тихий Spotify при громком микрофоне того же узла не копит холд. Тест в задаче 5.
- Старый агент получает `SetEndpoints` только с микрофоном (`source: None`) и не видит виды внутри этого сообщения. Тест в задаче 3.

---

### Task 1: Источник в состоянии

**Files:**
- Modify: `zproto/src/lib.rs`
- Modify: `zvucast/src/state.rs`
- Test: `zproto/src/lib.rs`, `zvucast/src/state.rs`

**Interfaces:**
- Consumes: ничего
- Produces:
  - `zproto::FeedKind` — `Spotify`, `Sendspin`, `Virtual`, serde `spotify` / `sendspin` / `virtual`
  - `FeedKind::as_str(self) -> &'static str` — те же три строки
  - `ZoneSource::Line(String)` и `ZoneSource::Feed { kind: FeedKind, node_id: String }` в `state.rs`
  - `ZoneSource::node_id(&self) -> &str`
  - `ZoneSource::key(&self) -> String` — микрофон: сам `node_id`; вид: `format!("{kind}:{node_id}")`, kind через `as_str`
  - `Zone::zone_sources(&self) -> Vec<ZoneSource>` — порядок хранения, без пустых и без повторов ключа
  - `Zone::source_ids(&self) -> Vec<String>` — только `node_id` микрофонов, как раньше
  - `Zone::set_sources(&mut self, Vec<ZoneSource>)` — пишет список; `tx` = первый микрофон, если микрофонов нет — `node_id` первого источника, иначе `""`
  - строка в `sources` десериализуется как `ZoneSource::Line`; объект `{"kind":"line","node_id"}` и `{"kind":"spotify"|"sendspin"|"virtual","node_id"}` — как названы. Сериализация всегда объект

- [ ] **Step 1: Write the failing test**

В `state.rs` зона только из JSON, без тестового конструктора:

```rust
#[test]
fn old_source_strings_are_line_mics() {
    let zone: Zone = serde_json::from_str(
        r#"{"id":"z","name":"Зона","tx":"n1","sources":["n1"]}"#,
    ).unwrap();
    assert_eq!(zone.zone_sources(), vec![ZoneSource::Line("n1".into())]);
    assert_eq!(zone.source_ids(), vec!["n1".to_string()]);
}

#[test]
fn feed_only_zone_roundtrips_and_sets_tx() {
    let mut zone: Zone = serde_json::from_str(
        r#"{"id":"z","name":"Зона","tx":"","sources":[]}"#,
    ).unwrap();
    zone.set_sources(vec![ZoneSource::Feed {
        kind: FeedKind::Spotify,
        node_id: "pc".into(),
    }]);
    assert_eq!(zone.tx, "pc");
    let back: Zone = serde_json::from_str(&serde_json::to_string(&zone).unwrap()).unwrap();
    assert_eq!(back.zone_sources()[0].key(), "spotify:pc");
    assert!(back.source_ids().is_empty());
}
```

В `zproto/src/lib.rs`: `serde_json::to_string(&FeedKind::Spotify)` равен `"\"spotify\""`.

- [ ] **Step 2: Run test to verify it fails**

Run from `/home/vallefor/ai-work/zvucast`: `cargo test -p zvucast --lib state::old_source_strings_are_line_mics -- --test-threads=1`

Expected: FAIL, `zone_sources` или `ZoneSource` не найдены.

- [ ] **Step 3: Implement the types and serde**

Повторы ключа `zone_sources` выкидывает, первый остаётся. `set_sources` после записи ещё раз прогоняет этот же отбор.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zvucast --lib state::old_source_strings_are_line_mics state::feed_only_zone_roundtrips_and_sets_tx -- --test-threads=1`

и `cargo test -p zproto --lib feed_kind -- --test-threads=1`

Ожидание: PASS. Имя теста `FeedKind` в `zproto` — то, которое добавлено в шаге 1.

- [ ] **Step 5: Commit**

```bash
git add zproto/src/lib.rs zvucast/src/state.rs
git commit -m "Источник зоны отличает микрофон от вида"
```

---

### Task 2: Связь, замена хозяина, снятие галочки

**Files:**
- Modify: `zvucast/src/state.rs`
- Modify: `zvucast/src/controller.rs` (`patch_zone`, `patch_node`)
- Test: `zvucast/src/state.rs`

**Interfaces:**
- Consumes: `ZoneSource`, `ZoneSource::key`, `Zone::set_sources`, `FeedKind`
- Produces:
  - `normalize_sources(sources: Vec<ZoneSource>, flags: &dyn Fn(&str, FeedKind) -> bool) -> Result<Vec<ZoneSource>, &'static str>`
  - `clear_feed(zone: &mut Zone, node_id: &str, kind: FeedKind)`
  - ошибки: `"Выберите хотя бы один источник"`, `"Узел не найден"`, `"На узле выключен Spotify"`, `"На узле выключен Sendspin"`, `"На узле выключена виртуальная колонка"`
  - `flags` истинны, только если узел есть в `state.nodes` и галочка этого вида включена. Микрофон через `flags` не ходит
  - два объекта одного вида во входном списке: остаётся последний, это не ошибка
  - `clear_feed(zone, node_id, kind)` удаляет такие источники и ключи `ZoneSource::key` из `source_maps` каждого члена. Другой вид и другая зона не трогает
  - `drop_missing_feed_keys(zone: &mut Zone)` удаляет из `source_maps` ключи `spotify:` / `sendspin:` / `virtual:`, которых нет среди `zone_sources()`. Ключи микрофонов не трогает. Новый ключ не создаёт
  - `patch_zone` после успешного `set_sources` вызывает `drop_missing_feed_keys`. `normalize_sources` карты не трогает
  - `patch_node`, когда галочка вида становится `false`, вызывает `clear_feed` на каждой зоне. Включение галочки списки зон не меняет
  - `clear_feed` не вызывается из реестра и пушера. Офлайн связь не снимает

- [ ] **Step 1: Write the failing test**

`zone_from_old_line` — локальный помощник: JSON со строковым `sources`.

```rust
#[test]
fn second_spotify_replaces_host() {
    let flags = |id: &str, kind: FeedKind| {
        (id == "pc" || id == "pi") && kind == FeedKind::Spotify
    };
    let next = normalize_sources(vec![
        ZoneSource::Line("n1".into()),
        ZoneSource::Feed { kind: FeedKind::Spotify, node_id: "pc".into() },
        ZoneSource::Feed { kind: FeedKind::Spotify, node_id: "pi".into() },
    ], &flags).unwrap();
    assert_eq!(next, vec![
        ZoneSource::Line("n1".into()),
        ZoneSource::Feed { kind: FeedKind::Spotify, node_id: "pi".into() },
    ]);
}

#[test]
fn dropped_feed_key_leaves_the_maps() {
    let mut zone = zone_from_old_line("n1");
    zone.source_maps.insert("spk".into(), HashMap::from([
        ("spotify:pc".into(), ChannelMap::default()),
        ("n1".into(), ChannelMap::default()),
    ]));
    zone.set_sources(vec![ZoneSource::Line("n1".into())]);
    drop_missing_feed_keys(&mut zone);
    assert!(zone.source_maps["spk"].get("spotify:pc").is_none());
    assert!(zone.source_maps["spk"].contains_key("n1"));
}

#[test]
fn clear_feed_on_one_zone_leaves_the_other() {
    let mut a = zone_from_old_line("n1");
    let mut b = zone_from_old_line("n1");
    let feed = ZoneSource::Feed { kind: FeedKind::Spotify, node_id: "pc".into() };
    a.set_sources(vec![ZoneSource::Line("n1".into()), feed.clone()]);
    b.set_sources(vec![ZoneSource::Line("n1".into()), feed]);
    b.source_maps.insert("spk".into(), HashMap::from([
        ("spotify:pc".into(), ChannelMap::default()),
    ]));
    clear_feed(&mut a, "pc", FeedKind::Spotify);
    assert!(a.zone_sources().iter().all(|s| s.key() != "spotify:pc"));
    assert!(b.zone_sources().iter().any(|s| s.key() == "spotify:pc"));
    assert!(b.source_maps["spk"].contains_key("spotify:pc"));
}
```

Ещё три случая в этих же тестах или рядом: один только `Feed` Spotify проходит `normalize_sources`; пустой список даёт `"Выберите хотя бы один источник"`; `flags` ложны для Spotify — `"На узле выключен Spotify"`. `clear_feed(..., Sendspin)` не удаляет ключ `spotify:pc`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zvucast --lib state::second_spotify_replaces_host_and_uncheck_drops_maps -- --test-threads=1`

Expected: FAIL, функции нет.

- [ ] **Step 3: Implement normalize, clear, and the two patch paths**

`patch_zone` принимает `sources` как тот же JSON, что хранит `Zone` (строка или объект). Неизвестный `node_id` микрофона — `"Узел не найден"`. Ошибка — HTTP 400, состояние не писать.

Прямое стерео нового хозяина — это отсутствие ключа: `source_channel_maps` уже подставляет общую карту колонки. Отдельную запись `ChannelMap::default()` не создавать.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zvucast --lib state::second_spotify_replaces_host -- --test-threads=1`

Ожидание: PASS у новых тестов этой задачи. Имя фильтра можно сузить до общего префикса `state::`.

- [ ] **Step 5: Commit**

```bash
git add zvucast/src/state.rs zvucast/src/controller.rs
git commit -m "Связь вида с зоной снимается вместе с картой"
```

---

### Task 3: Свой порт и отдельные команды

**Files:**
- Modify: `zproto/src/lib.rs`
- Modify: `zvucast/src/mix.rs`
- Modify: `zvucast/src/state.rs` (`zone_groups` только для микрофонов, как сейчас через `source_ids`)
- Modify: `zvucast/src/controller.rs` (пушер, `soloist_cmd`, `sendspin_cmd`, `virtual_zones_cmd`, `zone_channel_cmd`)
- Test: `zvucast/src/mix.rs`, `zvucast/tests/controller.rs`

**Interfaces:**
- Consumes: `ZoneSource::key`, `FeedKind`, `hosted` смысл из задачи 2 (`normalize_sources` уже не пускает вид без галочки)
- Produces:
  - `mix::feed_port_name(kind: FeedKind, node_id: &str, zone_id: &str) -> String` = `format!("{}:{node_id}:{zone_id}", kind.as_str())`
  - `CtlMsg::SetFeeds { feeds: Vec<FeedSend> }`
  - `FeedSend { zone_id: String, kind: FeedKind, monitor: String, endpoints: Vec<Endpoint> }`
  - монитор: `soloist::monitor_name`, `sendspin::monitor_name`, `virtual_output::monitor_name` от `zone_id`
  - `SetEndpoints` по-прежнему только группы микрофонов (`source_ids`), поле `source` всегда `None`
  - `SetFeeds` уходит хозяину. Внутри — по одной записи на зону, где этот узел хозяин этого вида и галочка включена. Порт назначения — `assign_mix_ports` от имени `feed_port_name`. Пустой список тоже слать, когда связи хозяина кончились
  - `SetSoloist` / `SetSendspin` / `SetVirtualZones` получают зоны, где узел хозяин и галочка включена, не зоны, где он только микрофон
  - `SetMix` колонки владельца включает порт микрофона и порты видов этой зоны. Чужая зона порт не добавляет
  - `zone_channel_cmd`: ключ ветки для вида — `ZoneSource::key()`, порт берётся из карты портов этой зоны. Один источник в зоне — по-прежнему карта после микшера и пустые `branches`

- [ ] **Step 1: Write the failing test**

В `mix.rs`:

```rust
#[test]
fn feed_port_name_differs_by_zone_and_kind() {
    let a = feed_port_name(FeedKind::Spotify, "pc", "z1");
    let b = feed_port_name(FeedKind::Spotify, "pc", "z2");
    let c = feed_port_name(FeedKind::Sendspin, "pc", "z1");
    assert_ne!(a, b);
    assert_ne!(a, c);
    let ports = assign_mix_ports(&[a.clone(), "pi4-tx".into()]);
    assert_eq!(ports.len(), 2);
    assert_ne!(ports[0].1, ports[1].1);
}
```

В `zvucast/tests/controller.rs` расширить ближайший тест пуша виртуальных зон:

- Узел-микрофон с `virtual_output: true` и без связи вида получает `SetVirtualZones { zones: vec![] }` и `SetFeeds { feeds: vec![] }`.
- Связь virtual на этот узел даёт одну зону в обоих. `VirtualZone.name` равен имени зоны. `SetEndpoints` этой зоны имеет `source: None` и не содержит второй группы с тем же `zone_id`.
- Один узел — хозяин Spotify и для `z1`, и для `z2`. `SetFeeds` содержит две записи. Монитор первой — `soloist::monitor_name("z1")`, endpoints только колонки `z1`. Монитор второй — `soloist::monitor_name("z2")`, колонки `z2`.
- Колонку поздней зоны не класть в `SetMix` с портом вида ранней зоны.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zvucast --lib mix::feed_port_name_differs_by_zone_and_kind -- --test-threads=1`

Expected: FAIL, `feed_port_name` не найдена.

- [ ] **Step 3: Implement port names, SetFeeds, and the push split**

Имена микрофонов в `assign_mix_ports` остаются системными именами узлов. Имена видов добавляются в тот же вызов, чтобы слоты не столкнулись молча. Кончились 64 слота — прежний warn, источник без порта не попадает в `SetMix`. Подпись устройства в апплете и в Spotify не менять: это по-прежнему «Zvucast: {имя зоны}» из уже существующего `VirtualZone.name`.

Существующие тесты контроллера, которые ждут в `SetVirtualZones` / `SetSoloist` / `SetSendspin` каждую зону, где узел числится микрофоном, переписать на пустой список, пока связи вида нет.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zvucast --lib mix::feed_port_name_differs_by_zone_and_kind -- --test-threads=1` и `cargo test -p zvucast --test controller -- --test-threads=1`

Ожидание: PASS.

- [ ] **Step 5: Commit**

```bash
git add zproto/src/lib.rs zvucast/src/mix.rs zvucast/src/state.rs zvucast/src/controller.rs zvucast/tests/controller.rs
git commit -m "Вид зоны уходит отдельным портом"
```

---

### Task 4: Агент не мешает вид в микрофон

**Files:**
- Modify: `zproto/src/lib.rs` (`Telemetry`)
- Modify: `zvucast/src/pipeline.rs`
- Modify: `zvucast/src/agent.rs`
- Modify: `zvucast/src/sendspin.rs`
- Test: `zvucast/src/pipeline.rs`, `zvucast/src/sendspin.rs`, `zvucast/src/agent.rs`

**Interfaces:**
- Consumes: `CtlMsg::SetFeeds`, `FeedSend`, `FeedKind`, имена мониторов из задачи 3
- Produces:
  - `Telemetry.feeds: Vec<FeedLevel>`, `#[serde(default)]`. Старый JSON без поля даёт пустой вектор
  - `FeedLevel { zone_id: String, kind: FeedKind, state: PipelineState, rms_l: f64, rms_r: f64 }`
  - линия `lev` измеряет только голову захвата. Монитор в `zmix-{zone}` не подмешивается: `attach_pw_zone` не вызывает `add_monitor_branch`, `overlay_sources` не пишет `zone.source`
  - каждый `FeedSend` — свой хвост: appsrc этого монитора, свой `level`, свой `rtpL16pay`, свои `udpsink`. Голова захвата в этот хвост не входит
  - пустой `SetFeeds` снимает эти хвосты
  - `ZoneTable::source` больше не выбирает, что писать в RTP микрофона. `request_deactivate` по старту потока Sendspin не вызывать
  - в `feeds` попадает уровень каждого живого хвоста. Нет сигнала — `rms_l` и `rms_r` остаются `0.0`, `state` хвоста `Playing`, только если ветка собрана

- [ ] **Step 1: Write the failing test**

В `pipeline.rs` рядом с тестом, где монитор подмешивается в микшер: набор `ZoneEndpoints` с `source: None` не создаёт элемент `cap-{token}` / `lev-{token}`.

В `sendspin.rs` переписать тест, который ждёт деактивацию Spotify при `StreamStart`: после старта флаг деактивации Spotify остаётся ложным, держатель может помнить Sendspin, но наружу это не команда Spotify.

В `agent.rs` или `pipeline.rs`: два хвоста одного узла (микрофон и feed Spotify) — разные имена `level`. Микрофонный `lev` не является `lev` фида.

В `zproto`: `Telemetry` без `feeds` десериализуется с пустым вектором.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zvucast --lib pipeline:: -- --test-threads=1` на новом тесте «монитор не в хвосте микрофона».

Expected: FAIL, ветка монитора всё ещё строится, либо теста ещё нет и утверждение ложно на текущем коде. Красным должен быть новый тест, не вся сюита.

- [ ] **Step 3: Implement the split tails and stop the deactivate call**

`pw-record` монитора писать в appsrc хвоста вида, не в микшер зоны. Имя appsrc: `feed-{kind}-{token}`, token тот же `element_token(zone_id)`. Имя level: `lev-feed-{kind}-{token}`.

Неизвестный агенту JSON `SetFeeds` уже пропускается в `ws_client` (`неразобранный CtlMsg`). Новый агент его разбирает.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zvucast --lib pipeline:: sendspin:: agent:: -- --test-threads=1` и `cargo test -p zproto --lib -- --test-threads=1`

Ожидание: PASS. Сломанные тесты «монитор подмешан в line-in» и «Sendspin гасит Spotify» удалить или перевернуть на новое поведение в этом же шаге, не оставлять красными.

- [ ] **Step 5: Commit**

```bash
git add zproto/src/lib.rs zvucast/src/pipeline.rs zvucast/src/agent.rs zvucast/src/sendspin.rs
git commit -m "Вид играет своим хвостом, не внутри микрофона"
```

---

### Task 5: Тишина по каждому источнику

**Files:**
- Modify: `zvucast/src/registry.rs`
- Modify: `zvucast/src/controller.rs` (`spawn_silence_ticker`)
- Test: `zvucast/src/controller.rs` или `zvucast/tests/controller.rs`, если тикер уже покрыт тестом реестра — рядом с `tx_sample`

**Interfaces:**
- Consumes: `Telemetry.feeds`, `Zone::zone_sources`, `classify`, `fold_samples`
- Produces:
  - `Registry::feed_sample(&self, node_id: &str, zone_id: &str, kind: FeedKind, now: Instant) -> Option<(Duration, bool, f64, f64)>` — возраст, играет ли, rms. Нет записи — `None`
  - тикер: микрофон — прежний `tx_sample(node_id)`; вид — `feed_sample` хозяина. `None` → `Sample::NoData`. Дальше `fold_samples`, как сейчас

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn quiet_feed_does_not_hold_a_loud_line() {
    let samples = vec![
        classify(Duration::from_secs(0), true, -10.0, -10.0, -60.0),
        classify(Duration::from_secs(0), true, -90.0, -90.0, -60.0),
    ];
    assert_eq!(fold_samples(&samples), Sample::Loud);
}
```

Плюс тест реестра: телеметрия узла с `tx` RMS −10 и `feeds` Spotify RMS −90 по зоне `z1` даёт `tx_sample` −10 и `feed_sample(..., Spotify)` −90. Чужой `zone_id` даёт `None`.

`fold_samples` уже так делает. Если тест на него зелёный без правок, не считать это провалом шага 2: красным обязан быть тест `feed_sample`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zvucast --lib registry::feed_sample -- --test-threads=1`

Expected: FAIL, метода нет.

- [ ] **Step 3: Implement feed_sample and point the ticker at zone_sources**

Не усреднять RMS фида с RMS микрофона в одну пару.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zvucast --lib registry:: silence:: -- --test-threads=1`

Ожидание: PASS.

- [ ] **Step 5: Commit**

```bash
git add zvucast/src/registry.rs zvucast/src/controller.rs
git commit -m "Тишина зоны смотрит на каждый источник"
```

---

### Task 6: Экран зоны

**Files:**
- Modify: `web/src/api.ts`
- Modify: `web/src/zoneMap.ts`
- Modify: `web/src/pages/Zones.tsx`
- Test: `web/zone-map.test.ts`

**Interfaces:**
- Consumes: JSON источника `{"kind":"line"|"spotify"|"sendspin"|"virtual","node_id"}`. Строку в ответе API новый контроллер не шлёт
- Produces:
  - `feedChannelLabel(kind: 'spotify' | 'sendspin' | 'virtual', side: 'l' | 'r') -> string`
  - подписи ровно: `Spotify (левый)`, `Spotify (правый)`, `Sendspin (левый)`, `Sendspin (правый)`, `Виртуальная колонка (левый)`, `Виртуальная колонка (правый)`
  - `feedChoiceLabel(nodeLabel: string, kind: ...) -> string` = `` `${title} — ${nodeLabel}` ``, title: `Spotify`, `Sendspin`, `Виртуальная колонка`
  - `selectFeed(sources, kind, nodeId)` заменяет прежний источник этого вида и не трогает микрофоны и другие виды
  - в настройках зоны микрофоны остаются галочками узлов. Под ними — ряд на каждый узел с включённой галочкой вида, даже если зона его не выбрала. Выбран ровно один на вид. Узел без галочки в ряд не попадает
  - `sourceMapKey(source) -> string`: у микрофона `node_id`, у вида `` `${kind}:${node_id}` ``. Таким ключом «Каналы» пишут `source_maps`. Это тот же ключ, что `ZoneSource::key`
  - в «Каналах» у источника-вида варианты только эти две подписи, не список входов узла

- [ ] **Step 1: Write the failing test**

В `web/zone-map.test.ts`:

```ts
test('подписи вида короткие, выбор хозяина с именем узла', () => {
  assert.equal(feedChannelLabel('spotify', 'l'), 'Spotify (левый)')
  assert.equal(feedChannelLabel('virtual', 'r'), 'Виртуальная колонка (правый)')
  assert.equal(feedChoiceLabel('pi4-tx', 'sendspin'), 'Sendspin — pi4-tx')
  assert.equal(sourceMapKey({ kind: 'line', node_id: 'pi4-tx' }), 'pi4-tx')
  assert.equal(sourceMapKey({ kind: 'spotify', node_id: 'pc' }), 'spotify:pc')
  const next = selectFeed(
    [{ kind: 'line', node_id: 'pi4-tx' }, { kind: 'spotify', node_id: 'pc' }],
    'spotify',
    'pi',
  )
  assert.deepEqual(next, [
    { kind: 'line', node_id: 'pi4-tx' },
    { kind: 'spotify', node_id: 'pi' },
  ])
})
```

- [ ] **Step 2: Run test to verify it fails**

Run from `web`: `node --experimental-strip-types --test zone-map.test.ts`

Expected: FAIL, `feedChannelLabel` не экспортирован.

- [ ] **Step 3: Implement the helpers and wire Zones.tsx**

`api.ts`: `sources` зоны — массив объектов. Старый код, который считает элемент строкой, перевести на `kind` и `node_id`. Создание зоны по-прежнему может слать микрофон объектом `{kind:'line', node_id}`.

Ряд вида не показывать у узла без `supports_virtual_output`. Это уже прячет галочки на странице узла; в зоне тот же предикат.

- [ ] **Step 4: Run test to verify it passes**

Run from `web`: `node --experimental-strip-types --test zone-map.test.ts` и `npx tsc -b --pretty false`

Ожидание: оба зелёные.

- [ ] **Step 5: Commit**

```bash
git add web/src/api.ts web/src/zoneMap.ts web/src/pages/Zones.tsx web/zone-map.test.ts
git commit -m "В зоне видно, куда связан Spotify"
```
