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
