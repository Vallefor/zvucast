// Чистая правка карты колонки. Селекты шлют карту целиком, поэтому вторая
// сторона берётся из уже выбранного (ещё не догнанного поллом) значения,
// а не из последнего ответа сервера.

export type ChanSel = 'l' | 'r'

export interface ChannelMap {
  out_l: ChanSel | null
  out_r: ChanSel | null
}

export const DEFAULT_ZONE_MAP: ChannelMap = { out_l: 'l', out_r: 'r' }

export function sameChannelMap(a: ChannelMap, b: ChannelMap): boolean {
  return a.out_l === b.out_l && a.out_r === b.out_r
}

/// `pending` — локальная правка, которую полл ещё не подтвердил.
/// Нет её — база это карта с сервера, иначе стерео.
export function applyMapSide(
  pending: ChannelMap | undefined,
  polled: ChannelMap | undefined,
  side: 'out_l' | 'out_r',
  value: ChanSel | null,
): ChannelMap {
  const base = pending ?? polled ?? DEFAULT_ZONE_MAP
  return { ...base, [side]: value }
}

/// Сервер молча пропускает ключ, которого ещё нет в members, и отвечает 200.
/// Слать карту можно только когда колонка уже в серверном списке.
export function canSendZoneMap(serverMembers: readonly string[], nodeId: string): boolean {
  return serverMembers.includes(nodeId)
}

/// Что делать после попытки отправить карту. Флаг «в полёте» к этому моменту
/// уже снят — и после ответа, и если fetch бросил. `resend` — пока запрос
/// летел, колонка выбрала другую карту, её надо отправить. `drop` — не
/// удалось, а новой карты нет: откатываем override. `idle` — сервер принял
/// ровно то, что сейчас ждёт полл.
export type MapAttemptNext = 'resend' | 'drop' | 'idle'

export function afterMapAttempt(
  ok: boolean,
  sent: ChannelMap,
  pending: ChannelMap | undefined,
): MapAttemptNext {
  const newer = pending !== undefined && !sameChannelMap(pending, sent)
  if (!ok) return newer ? 'resend' : 'drop'
  return newer ? 'resend' : 'idle'
}

/// С чем сравнивать pending, чтобы не слать лишний PATCH.
/// Принятая карта новее отстающего полла: пока полл её не повторил, база — она.
/// Полл догнал принятую (или отправки ещё не было) — база это полл.
export function confirmedZoneMap(
  polled: ChannelMap | undefined,
  lastAccepted: ChannelMap | undefined,
): ChannelMap | undefined {
  if (lastAccepted && !(polled && sameChannelMap(polled, lastAccepted))) return lastAccepted
  return polled
}

/// `false` — pending уже совпал с известной серверной картой, слать нечего.
/// Откат к старому поллу после принятой правки сюда не попадает: полл отстаёт,
/// и pending отличается от lastAccepted.
export function shouldSendZoneMap(
  pending: ChannelMap,
  polled: ChannelMap | undefined,
  lastAccepted: ChannelMap | undefined,
): boolean {
  const base = confirmedZoneMap(polled, lastAccepted)
  if (!base) return true
  return !sameChannelMap(pending, base)
}
