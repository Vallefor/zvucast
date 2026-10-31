// Правила карточки узла отдельно от вёрстки: кто играет и какие устройства
// видны. Страница только показывает результат.

import type { InputView } from './api.ts'

/// Играет, если играет передача или приём. Нет телеметрии и любой другой
/// статус — тишина.
export function isPlaying(
  telemetry: { tx: { state: string } | null; rx: { state: string } | null } | null,
): boolean {
  if (telemetry === null) return false
  return telemetry.tx?.state === 'playing' || telemetry.rx?.state === 'playing'
}

/// Офлайн не стартует и не стопит. Онлайн без звука — старт, со звуком — стоп.
export function transport(online: boolean, playing: boolean): 'start' | 'stop' | 'none' {
  if (!online) return 'none'
  return playing ? 'stop' : 'start'
}

/// Общий старт нужен, когда хотя бы один узел в сети и молчит.
export function showStartAll(nodes: { online: boolean; playing: boolean }[]): boolean {
  return nodes.some((node) => node.online && !node.playing)
}

/// Общий стоп нужен, когда хотя бы один узел играет.
export function showStopAll(nodes: { playing: boolean }[]): boolean {
  return nodes.some((node) => node.playing)
}

export type PendingCommand = { id: string; cmd: 'start' | 'stop'; at: number }

/// Старт применился, когда узел играет. Стоп — когда молчит.
/// Офлайн кнопку прячет само меню, ждать телеметрию нечего.
export function commandApplied(cmd: 'start' | 'stop', online: boolean, playing: boolean): boolean {
  if (!online) return true
  return cmd === 'start' ? playing : !playing
}

/// Снимает команды, которые карточка уже подтвердила или которые
/// старше limitMs. `late` — живой узел, так и не сменивший состояние.
export function settleCommands(
  pending: Record<string, PendingCommand>,
  nodes: { node_id: string; online: boolean; playing: boolean }[],
  now: number,
  limitMs: number,
): { pending: Record<string, PendingCommand>; late: PendingCommand[] } {
  const late: PendingCommand[] = []
  let changed = false
  const next = { ...pending }
  for (const [id, wait] of Object.entries(pending)) {
    const node = nodes.find((item) => item.node_id === id)
    const applied = node == null || commandApplied(wait.cmd, node.online, node.playing)
    const expired = now - wait.at >= limitMs
    if (!applied && !expired) continue
    delete next[id]
    changed = true
    if (!applied && expired) late.push(wait)
  }
  return { pending: changed ? next : pending, late }
}

/// 1, 21, 31… — «канал», кроме 11. 2–4, 22–24… — «канала», кроме 12–14.
/// Остальное — «каналов».
export function chanWord(n: number): 'канал' | 'канала' | 'каналов' {
  const mod100 = n % 100
  const mod10 = n % 10
  if (mod100 >= 11 && mod100 <= 14) return 'каналов'
  if (mod10 === 1) return 'канал'
  if (mod10 >= 2 && mod10 <= 4) return 'канала'
  return 'каналов'
}

export type DeviceGroup = { key: string; name: string; rows: InputView[] }

/// Группы в порядке первого `device_key`. Имя — из первой строки группы.
export function groupDevices(rows: InputView[]): DeviceGroup[] {
  const groups: DeviceGroup[] = []
  const byKey = new Map<string, DeviceGroup>()
  for (const row of rows) {
    const group = byKey.get(row.device_key)
    if (group) {
      group.rows.push(row)
      continue
    }
    const created = { key: row.device_key, name: row.device_name, rows: [row] }
    byKey.set(row.device_key, created)
    groups.push(created)
  }
  return groups
}

/// На схеме только устройства, у которых включён хотя бы один канал.
/// `count` — число включённых, не всех строк.
export function flowDevices(rows: InputView[]): { key: string; name: string; count: number }[] {
  const devices: { key: string; name: string; count: number }[] = []
  for (const group of groupDevices(rows)) {
    const count = group.rows.filter((row) => row.enabled).length
    if (count > 0) devices.push({ key: group.key, name: group.name, count })
  }
  return devices
}

/// Добавить можно устройство, где ни один канал не включён.
export function addableDevices(rows: InputView[]): { key: string; name: string }[] {
  return groupDevices(rows)
    .filter((group) => group.rows.every((row) => !row.enabled))
    .map((group) => ({ key: group.key, name: group.name }))
}

/// Новый массив: у выбранного устройства все каналы включены. Чужие строки
/// остаются теми же объектами, вход не меняется.
export function enableDevice(rows: InputView[], key: string): InputView[] {
  return rows.map((row) => (row.device_key === key ? { ...row, enabled: true } : row))
}

/// Проценты кубической громкости, уже зажатые в 0..150.
export function volumePercent(cubic: number): number {
  const pct = Math.round(cubic * 100)
  if (pct < 0) return 0
  if (pct > 150) return 150
  return pct
}

/// Целое 0..150 обратно в кубическую шкалу 0..1.5.
export function volumeFromPercent(percent: number): number {
  return volumePercent(percent / 100) / 100
}

/// Ползунок есть только у роли, которая уже прислала конечное число.
export function showVolumeSlider(role: boolean, volume: number | null | undefined): boolean {
  return role && typeof volume === 'number' && Number.isFinite(volume)
}

export interface DeviceVolume {
  device: string
  volume: number
  card?: string
  class?: string
}

function finiteVolume(row: DeviceVolume | undefined): row is DeviceVolume {
  return !!row && typeof row.volume === 'number' && Number.isFinite(row.volume)
}

/// Узел PipeWire для карточки устройства. Сначала точное имя, иначе единственная
/// карта той же стороны, чьё имя входит в подпись.
export function deviceVolume(
  volumes: DeviceVolume[] | null | undefined,
  key: string,
  name: string,
  kind: 'in' | 'out',
): DeviceVolume | null {
  const list = volumes ?? []
  const exact = list.find((row) => row.device === key || row.device === name)
  if (finiteVolume(exact)) return exact
  const want = kind === 'in' ? 'source' : 'sink'
  const hits = list.filter((row) => {
    if (row.class !== want || !row.card) return false
    return key.includes(row.card) || name.includes(row.card)
  })
  if (hits.length === 1 && finiteVolume(hits[0])) return hits[0]
  return null
}
