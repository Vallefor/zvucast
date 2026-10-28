// Чистая правка карты колонки. Селекты шлют карту целиком, поэтому вторая
// сторона берётся из уже выбранного (ещё не догнанного поллом) значения,
// а не из последнего ответа сервера.

export type ChanSel = 'l' | 'r'

/// Канал входа, как его отдаёт список узлов. Карта зоны выбирает сторону
/// стерео, но кабельный выход зоны — такой же вход: отмеченный канал должен
/// быть в списке, даже когда слот захвата занят линейным входом.
export type MappingInput = {
  id: string
  device_key: string
  device_name: string
  index: number
  name: string
  enabled: boolean
  stream: ChanSel | null
}

export type MappingChoice = { id: string; side: ChanSel; label: string }

/// `zvucast-{зона}`, не монитор `*-monitor` и не старый ключ `zvucast`.
export function isZoneCableKey(key: string): boolean {
  return key.startsWith('zvucast-') && !key.endsWith('-monitor')
}

/// Что можно выбрать источником выхода колонки.
/// Пустой список входов — два имени по умолчанию, пока агент их не прислал.
/// У кабеля подпись с именем устройства: иначе все «Левый» неразличимы.
export function mappingChoices(inputs: readonly MappingInput[]): MappingChoice[] {
  if (inputs.length === 0) {
    return [
      { id: 'default-l', side: 'l', label: 'Левый' },
      { id: 'default-r', side: 'r', label: 'Правый' },
    ]
  }
  const choices: MappingChoice[] = []
  for (const side of ['l', 'r'] as const) {
    const index = side === 'l' ? 0 : 1
    for (const input of inputs) {
      if (!input.enabled || input.index !== index) continue
      const cable = isZoneCableKey(input.device_key)
      if (input.stream !== side && !cable) continue
      const named = input.name.startsWith(input.device_name)
      choices.push({
        id: input.id,
        side,
        label: cable && !named ? `${input.device_name}: ${input.name}` : input.name,
      })
    }
  }
  return choices
}

/// Выход колонки, как его отдаёт список узлов. Для строк карты достаточно
/// id, имени, включения и слота стерео.
export type SpeakerOutput = {
  id: string
  name: string
  enabled: boolean
  stream: ChanSel | null
}

export type SpeakerOutputRow = {
  id: string
  side: 'out_l' | 'out_r'
  label: string
}

/// Включённые выходы со слотом `l` или `r`. Пустой список — две строки
/// по умолчанию, пока агент их не прислал. Id канала остаётся своим, чтобы
/// две строки одной стороны не схлопнулись в одну.
export function speakerOutputRows(outputs: readonly SpeakerOutput[]): SpeakerOutputRow[] {
  if (outputs.length === 0) {
    return [
      { id: 'default-l', side: 'out_l', label: 'Левый выход' },
      { id: 'default-r', side: 'out_r', label: 'Правый выход' },
    ]
  }
  const rows: SpeakerOutputRow[] = []
  for (const output of outputs) {
    if (!output.enabled || (output.stream !== 'l' && output.stream !== 'r')) continue
    rows.push({
      id: output.id,
      side: output.stream === 'l' ? 'out_l' : 'out_r',
      label: output.name,
    })
  }
  return rows
}

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

export type FeedKind = 'spotify' | 'sendspin' | 'virtual'

/// Источник зоны, как его отдаёт контроллер: `{kind, node_id}`.
export type ZoneSource = {
  kind: 'line' | FeedKind
  node_id: string
}

export function feedTitle(kind: FeedKind): string {
  switch (kind) {
    case 'spotify':
      return 'Spotify'
    case 'sendspin':
      return 'Sendspin'
    case 'virtual':
      return 'Виртуальная колонка'
  }
}

export function feedChannelLabel(kind: FeedKind, side: 'l' | 'r'): string {
  const sideName = side === 'l' ? 'левый' : 'правый'
  return `${feedTitle(kind)} (${sideName})`
}

export function feedChoiceLabel(nodeLabel: string, kind: FeedKind): string {
  return `${feedTitle(kind)} — ${nodeLabel}`
}

/// В «Каналах» у вида только две стороны, не входы узла.
export function feedMappingChoices(kind: FeedKind): MappingChoice[] {
  return (['l', 'r'] as const).map((side) => ({
    id: `${kind}-${side}`,
    side,
    label: feedChannelLabel(kind, side),
  }))
}

/// Новый хозяин занимает место прежнего источника этого вида.
/// Микрофоны и другие виды не двигаются.
export function selectFeed(
  sources: readonly ZoneSource[],
  kind: FeedKind,
  nodeId: string,
): ZoneSource[] {
  const next: ZoneSource = { kind, node_id: nodeId }
  let placed = false
  const out: ZoneSource[] = []
  for (const src of sources) {
    if (src.kind !== kind) {
      out.push(src)
      continue
    }
    if (!placed) {
      out.push(next)
      placed = true
    }
  }
  if (!placed) out.push(next)
  return out
}

/// Ключ `source_maps`. Микрофон — `node_id`, вид — `spotify:pc`. Как `ZoneSource::key`.
export function sourceMapKey(source: ZoneSource): string {
  return source.kind === 'line' ? source.node_id : `${source.kind}:${source.node_id}`
}
