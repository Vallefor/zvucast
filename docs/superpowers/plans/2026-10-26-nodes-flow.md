# Карточки узлов вместо таблицы Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Страница «Узлы» показывает каждый узел карточкой со схемой «входы слева, плата по центру, выходы справа», а настройки делят сеть и возможности узла и дают включить устройство целиком.

**Architecture:** Чистые функции в `web/src/nodesView.ts` решают, играет ли узел, какую кнопку транспорта показать и какие устройства видны на схеме. `Nodes.tsx` только рисует это и по-прежнему ходит в существующий `api`. Таблица и узкая колонка карточек уходят. Блок «Системе не хватает», полл, обновление и тело PATCH не меняются.

**Tech Stack:** React 18, существующие `Button`/`Badge`/`Popup`, CSS-переменные `web/src/theme.css`, `node:test` для чистых функций, `tsc -b` для страницы.

## Global Constraints

- Рабочий каталог: `/home/vallefor/ai-work/zvucast/.worktrees/nodes-flow-ui`. Писать только туда.
- Не менять Rust, `zproto`, `Cargo.toml`, `web/package.json`, `web/src/api.ts`, `web/src/pages/Zones.tsx`, `web/src/zoneMap.ts`, `web/src/update.ts`.
- Не поднимать версию. Не пушить.
- Не коммитить `target/`, `web/node_modules/`, `web/dist/`.
- Палитру не изобретать. Цвета только из уже заданных переменных: `--bg`, `--bg-elev`, `--bg-sunken`, `--line`, `--text`, `--text-dim`, `--text-faint`, `--accent`, `--good`, `--good-soft`, `--quiet`, `--quiet-soft`, `--bad`.
- Состояние воспроизведения приходит строкой `playing` (`PipelineState`, serde `lowercase`). Узел играет, если `telemetry.tx.state === 'playing'` или `telemetry.rx.state === 'playing'`.
- Офлайн-узел не получает ни «Старт», ни «Стоп». Играющий онлайн получает только «Стоп». Онлайн и не играющий получает только «Старт». Команды те же: `api.nodeCmd(id, 'start' | 'stop')`.
- «Старт всех» видна, только если есть онлайн-узел, который не играет. «Стоп всех» видна, только если какой-то узел играет. «Обновить все» остаётся всегда. Запросы те же: `POST /api/start-all`, `POST /api/stop-all`, `updateAll()`.
- Кнопка «Обновить» на узле по-прежнему через `offer` / `updateWait` и `versionCell`. Текст «обновляется», «не вернулся», «не установилось», «откат», «нет файла в релизе» не менять.
- Блок `setupBlock` («Системе не хватает») остаётся над списком узлов.
- Бейджи «Дубликат» и «Мастер», подтверждение передачи мастера и экран «Мастер переехал» остаются.
- Входы рисуются только у роли `tx`, выходы только у роли `rx`. Узел с обеими ролями показывает обе стороны.
- На схеме карточки устройство всегда во второй колонке сетки из трёх. Пустая сторона не рисуется и не сдвигает плату. Узкая ширина тоже держит три колонки.
- На карточке имя устройства обрезается многоточием, полное имя лежит в атрибуте `title`. Рядом зелёное число включённых каналов. Имён каналов и кнопки «Добавить» на карточке нет.
- В настройках «Добавить» есть только у стороны, где есть устройство со всеми выключенными каналами. Пункт — имя устройства, не «левый/правый». Клик включает все каналы этого `device_key`. Устройство с хотя бы одним включённым каналом показывает все свои каналы: чекбокс и поле имени рядом, не внутри одного `<label>`. Когда не остаётся ни одного включённого канала, устройство снова только в «Добавить».
- Чекбокс «Создать виртуальный звуковой выход», Spotify и Sendspin — только при `supports_virtual_output` и только у `tx`, как сейчас. Поле «Ключ Spotify» видно, только когда Spotify включён. Селект «Система / Стерео» убрать.
- DSCP и пресеты CS0=0, CS3=24, CS5=40, EF=46 только у `tx`. Проверка та же: целое 0…63, иначе тост `DSCP — целое от 0 до 63`. Тело PATCH не расширять.
- «Этот узел ведёт панель» пишется только в группе «Сеть» настроек (Task 3). На карточке этой фразы нет: у мастера остаётся бейдж «Мастер». После Task 3 кнопка «Сделать мастером» тоже только в «Сети», на карточке её нет. В Task 2 кнопка ещё на карточке. Офлайн-кнопка передачи по-прежнему disabled.
- Иконка платы одна на все узлы: в API нет типа платы. Иконки Spotify, Sendspin и виртуальной колонки — только когда соответствующий флаг истинен.
- Комментарии по-русски, в тоне соседнего кода.
- Сообщения коммитов: `feat(zvucast): …`, по-русски, про причину.
- Проверка из каталога `web`: `node --test update.test.ts zone-map.test.ts nodes-view.test.ts` и `npx tsc -b --pretty false`. `vite build` не гонять: postinstall esbuild в этом worktree заблокирован.

---

### Task 1: Кто играет и какие устройства видны

**Files:**
- Create: `web/src/nodesView.ts`
- Test: `web/nodes-view.test.ts`

**Interfaces:**
- Consumes: тип `InputView` из `web/src/api.ts` (только тип, `api` не вызывать).
- Produces:
  - `isPlaying(telemetry: { tx: { state: string } | null; rx: { state: string } | null } | null): boolean`
  - `transport(online: boolean, playing: boolean): 'start' | 'stop' | 'none'`
  - `showStartAll(nodes: { online: boolean; playing: boolean }[]): boolean`
  - `showStopAll(nodes: { playing: boolean }[]): boolean`
  - `chanWord(n: number): 'канал' | 'канала' | 'каналов'`
  - `type DeviceGroup = { key: string; name: string; rows: InputView[] }`
  - `groupDevices(rows: InputView[]): DeviceGroup[]` — порядок первого появления `device_key`
  - `flowDevices(rows: InputView[]): { key: string; name: string; count: number }[]` — только устройства с `count > 0`, `count` равен числу `enabled`
  - `addableDevices(rows: InputView[]): { key: string; name: string }[]` — устройства, где ни один канал не `enabled`
  - `enableDevice(rows: InputView[], key: string): InputView[]` — новый массив, у этого `device_key` все `enabled: true`, остальные строки те же объекты

- [ ] **Step 1: Write the failing test**

Создать `web/nodes-view.test.ts`. Фикстура каналов:

```ts
import assert from 'node:assert/strict'
import test from 'node:test'
import type { InputView } from './src/api.ts'
import {
  addableDevices,
  chanWord,
  enableDevice,
  flowDevices,
  groupDevices,
  isPlaying,
  showStartAll,
  showStopAll,
  transport,
} from './src/nodesView.ts'

function chan(part: Partial<InputView> & Pick<InputView, 'id' | 'device_key' | 'device_name'>): InputView {
  return {
    index: 0,
    name: part.id,
    enabled: false,
    stream: null,
    ...part,
  }
}

const rows = [
  chan({ id: 'a0', device_key: 'uca', device_name: 'USB Audio CODEC UCA202', index: 0, enabled: true }),
  chan({ id: 'a1', device_key: 'uca', device_name: 'USB Audio CODEC UCA202', index: 1, enabled: true }),
  chan({ id: 'h0', device_key: 'hdmi', device_name: 'sun9i HDMI Stereo', index: 0, enabled: false }),
  chan({ id: 'h1', device_key: 'hdmi', device_name: 'sun9i HDMI Stereo', index: 1, enabled: false }),
  chan({ id: 'l0', device_key: 'line', device_name: 'H3 Audio Codec Line Out', index: 0, enabled: true }),
  chan({ id: 'l1', device_key: 'line', device_name: 'H3 Audio Codec Line Out', index: 1, enabled: false }),
]

test('играет, если играет tx или rx', () => {
  assert.equal(isPlaying(null), false)
  assert.equal(isPlaying({ tx: { state: 'stopped' }, rx: null }), false)
  assert.equal(isPlaying({ tx: { state: 'playing' }, rx: { state: 'stopped' } }), true)
  assert.equal(isPlaying({ tx: null, rx: { state: 'playing' } }), true)
  assert.equal(isPlaying({ tx: { state: 'error' }, rx: { state: 'stopped' } }), false)
})

test('транспорт зависит от сети и воспроизведения', () => {
  assert.equal(transport(false, false), 'none')
  assert.equal(transport(false, true), 'none')
  assert.equal(transport(true, false), 'start')
  assert.equal(transport(true, true), 'stop')
})

test('общие кнопки следуют за узлами', () => {
  assert.equal(showStartAll([]), false)
  assert.equal(showStartAll([{ online: true, playing: true }]), false)
  assert.equal(showStartAll([{ online: false, playing: false }]), false)
  assert.equal(showStartAll([{ online: true, playing: false }]), true)
  assert.equal(showStopAll([{ playing: false }]), false)
  assert.equal(showStopAll([{ playing: true }]), true)
})

test('русское число каналов', () => {
  assert.equal(chanWord(1), 'канал')
  assert.equal(chanWord(2), 'канала')
  assert.equal(chanWord(5), 'каналов')
  assert.equal(chanWord(11), 'каналов')
  assert.equal(chanWord(21), 'канал')
  assert.equal(chanWord(22), 'канала')
})

test('схема и добавление режут устройства по включённым каналам', () => {
  assert.deepEqual(groupDevices(rows).map((g) => g.key), ['uca', 'hdmi', 'line'])
  assert.equal(groupDevices(rows)[2].rows.length, 2)
  assert.deepEqual(flowDevices(rows), [
    { key: 'uca', name: 'USB Audio CODEC UCA202', count: 2 },
    { key: 'line', name: 'H3 Audio Codec Line Out', count: 1 },
  ])
  assert.deepEqual(addableDevices(rows), [{ key: 'hdmi', name: 'sun9i HDMI Stereo' }])
  const enabled = enableDevice(rows, 'hdmi')
  assert.equal(enabled.find((r) => r.id === 'h0')?.enabled, true)
  assert.equal(enabled.find((r) => r.id === 'h1')?.enabled, true)
  assert.equal(rows.find((r) => r.id === 'h0')?.enabled, false)
  assert.deepEqual(addableDevices(enabled), [])
  assert.equal(flowDevices(enabled).find((d) => d.key === 'hdmi')?.count, 2)
})
```

- [ ] **Step 2: Run test to verify it fails**

Из `web`: `node --test nodes-view.test.ts`

Ожидание: падение на импорте `./src/nodesView.ts` (файла ещё нет).

- [ ] **Step 3: Write minimal implementation**

`web/src/nodesView.ts` с функциями из блока Interfaces. `groupDevices` повторяет порядок первого `device_key`, имя берёт из первой строки группы. `enableDevice` не мутирует вход. `chanWord`: 1, 21, 31… → `канал`, кроме 11; 2–4, 22–24… → `канала`, кроме 12–14; остальное → `каналов`.

- [ ] **Step 4: Run test to verify it passes**

Из `web`: `node --test update.test.ts zone-map.test.ts nodes-view.test.ts`

Ожидание: все тесты зелёные, в том числе прежние 8.

- [ ] **Step 5: Commit**

```bash
git add web/src/nodesView.ts web/nodes-view.test.ts docs/superpowers/plans/2026-10-26-nodes-flow.md
git commit -m "$(cat <<'EOF'
feat(zvucast): правила карточки узла отдельно от вёрстки

EOF
)"
```

План коммитится здесь же, если его ещё нет в git.

---

### Task 2: Карточка со схемой вместо таблицы

**Files:**
- Modify: `web/src/pages/Nodes.tsx`
- Modify: `web/src/theme.css`
- Consumes: `isPlaying`, `transport`, `showStartAll`, `showStopAll`, `flowDevices`, `chanWord` из `web/src/nodesView.ts`

`NodeSettings`, `ChanEditor`, `setupBlock`, `versionCell`, полл, `transfer`, `updateOne`, `updateAll` не переписывать. «Сделать мастером» на этом шаге остаётся в `actions` карточки. Селект «Система» не трогать — это Task 3.

- [ ] **Step 1: Убрать таблицу и узкие карточки**

Удалить блоки `.table-wrap.only-wide` и `.cards.only-narrow`. Вместо них один список `<article className="node-card">` на узел, на всю ширину, и для широкого экрана, и для узкого. Ключ элемента прежний: `` `${n.node_id}@${n.ip}` ``. Класс `off` на всю карточку не вешать.

- [ ] **Step 2: Шапка и плата**

В карточке:

- Имя: `display_name ?? name`. Если `display_name` задано, системное `name` мельче рядом, как сейчас делает `nodeTitle`.
- Бейджи ролей, «Дубликат», «Мастер».
- IP (`num`) и подпись связи: Wi‑Fi / Провод / «—» через существующий `linkLabel`.
- Одна инлайн-SVG плата у всех. Обводка и фон `--good` / `--good-soft`, когда `n.online`. Когда нет — фон `--bg-sunken`, обводка `--line`, без зелёного.
- Слово состояния: нет сети → «не в сети» цветом `--text-faint`; играет → «играет» цветом `--good`; иначе «стоит» цветом `--quiet`. Играет ли узел — `isPlaying(n.telemetry)`.

- [ ] **Step 3: Схема входов и выходов**

Сетка из трёх колонок: `minmax(0, 1fr) minmax(7.5rem, 11rem) minmax(0, 1fr)`. Плата во второй колонке, `justify-self: center`, текст по центру. Входы — первая колонка, только если `roles` содержит `tx`. Выходы — третья, только если есть `rx`. Сторону без роли не рендерить.

На стороне чипы из `flowDevices(n.inputs ?? [])` или `flowDevices(n.outputs ?? [])`. Сторону не рендерить, если чипов нет: пустая подпись «Входы» или «Выходы» плату не сопровождает. Чип: имя устройства с `title={name}`, обрезка `overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0`, и число `count`. У числа `aria-label={`${count} ${chanWord(count)}`}`, фон `--good-soft`, текст `--good`. Кнопки «Добавить» нет. Над чипами подпись стороны есть: «Входы» или «Выходы».

На `max-width: 420px` средняя колонка `minmax(96px, 120px)`. Три колонки сохраняются.

- [ ] **Step 4: Низ карточки**

Метры только с живым значением, без прочерков:

- RMS, если `tx.state === 'playing'` и хотя бы одно из `rms_l` / `rms_r` конечно и не ноль. Текст как `rmsText`: одна цифра после запятой, через ` / `.
- «Очередь», если `rx.state === 'playing'` и `queue_ms >= 0`.
- «Заикания», если `rx` есть и `dropped > 0`.
- «PTP», если узел онлайн, телеметрия есть и `Math.abs(ptp_offset_ns) <= 1e12`. Число как сейчас: `(ptp_offset_ns / 1000).toFixed(1)`.
- «Uptime», если телеметрия есть, формат `fmtUptime`.

Не показывать колонки TX, RX, Rendered.

Дальше `versionCell`. Иконки 18px, `currentColor`, только по флагам: `virtual_output` (колонка, `--text-dim`), `spotify` (`--accent`), `sendspin` (`--quiet`). Подписи для читалок: `aria-label` «Виртуальная колонка», «Spotify», «Sendspin».

Кнопки: «Настройки»; «Старт» или «Стоп» по `transport(n.online, isPlaying(n.telemetry))` — при `'none'` кнопки транспорта нет; «Сделать мастером», если это не мастер, `disabled={!n.online}`.

- [ ] **Step 5: Панель страницы**

«Старт всех» рендерить только при `showStartAll`. «Стоп всех» только при `showStopAll`. В эти функции передать `{ online, playing: isPlaying(n.telemetry) }`. «Обновить все» без условия. Заголовок «Узлы» и «N из M в сети» не менять.

- [ ] **Step 6: Проверка**

Из `web`: `npx tsc -b --pretty false` и `node --test update.test.ts zone-map.test.ts nodes-view.test.ts`. Оба зелёные.

- [ ] **Step 7: Commit**

```bash
git add web/src/pages/Nodes.tsx web/src/theme.css
git commit -m "$(cat <<'EOF'
feat(zvucast): узлы карточками со схемой входов и выходов

EOF
)"
```

---

### Task 3: Настройки — две группы и устройство целиком

**Files:**
- Modify: `web/src/pages/Nodes.tsx`
- Modify: `web/src/theme.css` (только если не хватает классов Task 2)
- Consumes: `groupDevices`, `addableDevices`, `enableDevice` из `web/src/nodesView.ts`

Карточку Task 2 не ломать. `setupBlock` и тело `save` не расширять.

- [ ] **Step 1: Группы**

Под полем «Имя» две группы, только с тем, что узлу доступно.

«Сеть»: IP текстом, связь через `linkLabel`, у `tx` поле DSCP и пресеты как сейчас. Если узел — мастер, текст «Этот узел ведёт панель». Иначе кнопка «Сделать мастером», `disabled`, когда узел не онлайн, тот же `transfer`. С карточки эту кнопку убрать.

«На узле» рисовать только при `isTx && node.supports_virtual_output`. Внутри: виртуальный выход, Spotify, ключ Spotify только при включённом Spotify, Sendspin. Подсказки Spotify и Sendspin оставить дословно. Селект «Система» удалить.

У `rx` без `tx` группы «На узле» нет, DSCP нет.

- [ ] **Step 2: Каналы под группами**

Та же трёхколоночная сетка, что у карточки. Колонка входов при `tx`, выходов при `rx`.

`ChanEditor` больше не выводит каждое устройство всегда. Для строк стороны:

- `groupDevices` → устройство попадает в колонку, если хотя бы один канал `enabled`. Показать все каналы устройства.
- Строка канала — `<div>`, не `<label>`. Чекбокс и поле имени — соседи. Чекбокс не оборачивает поле, клик по полю не переключает канал. `aria-label` чекбокса и поля сохранить по смыслу текущих: «Использовать …» и «Имя канала …».
- `addableDevices` даёт кнопки «Добавить» внизу этой колонки. Текст кнопки — `device_name`, `title` — то же полное имя, длинное имя обрезается. Клик вызывает `enableDevice` и записывает результат в `rows` или `outRows`.
- Снятие последнего включённого канала прячет устройство из колонки: оно снова в списке «Добавить». Отдельного кода «удалить» нет, это следствие фильтра.
- Пустой список устройств стороны: прежние пустые фразы. «Агент ещё не прислал устройства захвата.» для входов, «Агент ещё не прислал устройства воспроизведения.» для выходов. Подсказки `ChanEditor` про стереопоток и карту зоны оставить.

- [ ] **Step 3: Проверка**

Из `web`: `npx tsc -b --pretty false` и `node --test update.test.ts zone-map.test.ts nodes-view.test.ts`. Оба зелёные.

- [ ] **Step 4: Commit**

```bash
git add web/src/pages/Nodes.tsx web/src/theme.css
git commit -m "$(cat <<'EOF'
feat(zvucast): настройки узла группами, устройство включается целиком

EOF
)"
```
