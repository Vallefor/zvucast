import { useEffect, useRef, useState } from 'react'
import { api, NodeView, ZoneView } from '../api'
import { Badge, Button, Popup, useRun, useUi } from '../ui'
import {
  afterMapAttempt,
  applyMapSide,
  canSendZoneMap,
  ChannelMap,
  ChanSel,
  DEFAULT_ZONE_MAP,
  sameChannelMap,
  shouldSendZoneMap,
} from '../zoneMap'

const POLL_MS = 2000

function nodeLabel(n: NodeView): string {
  return n.display_name ?? n.name
}

function zoneSourceIds(zone: ZoneView): string[] {
  return zone.sources && zone.sources.length > 0 ? zone.sources : [zone.tx]
}

function mapStorageKey(member: string, sourceId: string | null): string {
  return sourceId ? `${member}\n${sourceId}` : member
}

function splitMapKey(key: string): { member: string; sourceId: string | null } {
  const [member, sourceId] = key.split('\n')
  return { member, sourceId: sourceId ?? null }
}

function storedMap(zone: ZoneView, member: string, sourceId: string | null): ChannelMap | undefined {
  if (sourceId) return zone.source_maps?.[member]?.[sourceId] ?? zone.maps[member]
  return zone.maps[member]
}

type StreamOpt = { value: ChanSel; label: string }

/// Включенные каналы источника, у которых есть слот стереопотока.
/// Пустой список устройств — два имени по умолчанию, пока агент их не прислал.
type OutputRow = { side: 'out_l' | 'out_r'; label: string }

/// Включённые выходы колонки со слотом стерео. Пустой список — два выхода
/// по умолчанию, пока агент их не прислал.
function outputRows(rx: NodeView | undefined): OutputRow[] {
  const outputs = rx?.outputs ?? []
  const slotted = outputs.filter((o) => o.enabled && (o.stream === 'l' || o.stream === 'r'))
  if (outputs.length === 0) {
    return [
      { side: 'out_l', label: 'Левый выход' },
      { side: 'out_r', label: 'Правый выход' },
    ]
  }
  return slotted.map((o) => ({
    side: o.stream === 'l' ? 'out_l' : 'out_r',
    label: o.name,
  }))
}

function streamOptions(tx: NodeView | undefined): StreamOpt[] {
  const inputs = tx?.inputs ?? []
  const slotted = inputs.filter((i) => i.enabled && (i.stream === 'l' || i.stream === 'r'))
  if (inputs.length === 0) {
    return [
      { value: 'l', label: 'Левый' },
      { value: 'r', label: 'Правый' },
    ]
  }
  return slotted.map((i) => ({ value: i.stream as ChanSel, label: i.name }))
}

function ChanSelect({
  value,
  label,
  options,
  onChange,
}: {
  value: ChanSel | null
  label: string
  options: StreamOpt[]
  onChange: (v: ChanSel | null) => void
}) {
  const known = value != null && options.some((o) => o.value === value)
  return (
    <select
      className="select"
      aria-label={label}
      value={known ? value : ''}
      onChange={(e) => onChange((e.target.value || null) as ChanSel | null)}
    >
      {options.map((o) => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
      <option value="">—</option>
    </select>
  )
}

function ZoneCard({
  zone,
  txNodes,
  rxNodes,
  byId,
}: {
  zone: ZoneView
  txNodes: NodeView[]
  rxNodes: NodeView[]
  byId: Map<string, NodeView>
}) {
  const run = useRun()
  const { confirm, toast } = useUi()
  const [popup, setPopup] = useState<null | 'add' | 'settings' | 'channels'>(null)
  // members: локальный override (с PATCH-ответа). Пока он есть, poll его не
  // затирает — иначе полл-интервал может откатить только что отправленный тогл.
  const [members, setMembers] = useState<string[] | null>(null)
  // Один PATCH на зону в полёте: гонки чекбоксов сериализуются на уровне UI.
  const [membersPending, setMembersPending] = useState(false)
  const effective = members ?? zone.members
  const membersRef = useRef(members)
  const membersPendingRef = useRef(membersPending)
  const zoneMembersRef = useRef(zone.members)
  membersRef.current = members
  membersPendingRef.current = membersPending
  zoneMembersRef.current = zone.members

  // Карта, которую пользователь уже выбрал, а полл ещё не подтвердил.
  // Вторая сторона читает её, а не последний ответ сервера.
  const [mapOverrides, setMapOverrides] = useState<Record<string, ChannelMap>>({})
  const mapOverridesRef = useRef(mapOverrides)
  mapOverridesRef.current = mapOverrides
  const mapFlight = useRef<Record<string, boolean>>({})
  // Последняя карта, которую сервер уже принял. Полл отстаёт на ~2 с,
  // и сравнивать откат с ним нельзя.
  const lastAcceptedRef = useRef<Record<string, ChannelMap>>({})

  // Сервер (poll) догнал наш override — снимаем его, дальше живём от пропа.
  useEffect(() => {
    if (members && members.length === zone.members.length && members.every((m) => zone.members.includes(m))) {
      membersRef.current = null
      setMembers(null)
    }
  }, [members, zone.members])

  useEffect(() => {
    setMapOverrides((prev) => {
      let changed = false
      const next: Record<string, ChannelMap> = {}
      for (const [id, map] of Object.entries(prev)) {
        const { member, sourceId } = splitMapKey(id)
        const server = storedMap(zone, member, sourceId)
        if (server && sameChannelMap(server, map)) {
          changed = true
          continue
        }
        next[id] = map
      }
      if (!changed) return prev
      mapOverridesRef.current = next
      return next
    })
  }, [zone.maps, zone.source_maps])

  // Пока PATCH членов в полёте, оптимистичный список — ещё не сервер.
  // После ответа берём его; иначе — последний полл.
  const serverMembers = (): string[] => {
    if (membersPendingRef.current) return zoneMembersRef.current
    return membersRef.current ?? zoneMembersRef.current
  }

  const flushMap = (key: string) => {
    const { member, sourceId } = splitMapKey(key)
    if (mapFlight.current[key]) return
    if (!canSendZoneMap(serverMembers(), member)) return
    const map = mapOverridesRef.current[key]
    if (!map) return
    // Полл может ещё показывать карту до принятого PATCH. Сравниваем с ним,
    // только когда он догнал lastAccepted.
    if (!shouldSendZoneMap(map, storedMap(zone, member, sourceId), lastAcceptedRef.current[key])) return
    mapFlight.current[key] = true
    const body = sourceId
      ? { source_maps: { [member]: { [sourceId]: map } } }
      : { maps: { [member]: map } }
    void (async () => {
      let ok = false
      try {
        ok = await run(api.patchZone(zone.id, body), 'карта каналов')
      } catch (e) {
        // run не ловит отказ fetch наружу: без finally флаг остался бы true
        // и колонка молчала бы до перезагрузки.
        console.error(e)
        toast('карта каналов: нет связи')
      } finally {
        mapFlight.current[key] = false
      }
      if (ok) {
        lastAcceptedRef.current = { ...lastAcceptedRef.current, [key]: map }
      }
      const latest = mapOverridesRef.current[key]
      const next = afterMapAttempt(ok, map, latest)
      if (next === 'drop') {
        const copy = { ...mapOverridesRef.current }
        delete copy[key]
        mapOverridesRef.current = copy
        setMapOverrides(copy)
        return
      }
      if (next === 'resend') flushMap(key)
    })()
  }

  const patchMap = (nodeId: string, sourceId: string | null, side: 'out_l' | 'out_r', v: ChanSel | null) => {
    const key = mapStorageKey(nodeId, sourceId)
    const nextMap = applyMapSide(
      mapOverridesRef.current[key],
      storedMap(zone, nodeId, sourceId),
      side,
      v,
    )
    const overrides = { ...mapOverridesRef.current, [key]: nextMap }
    mapOverridesRef.current = overrides
    setMapOverrides(overrides)
    flushMap(key)
  }

  const toggleMember = (id: string, checked: boolean) => {
    if (membersPending) return
    const next = checked ? [...effective, id].sort() : effective.filter((m) => m !== id)
    membersRef.current = next
    membersPendingRef.current = true
    setMembers(next)
    setMembersPending(true)
    void (async () => {
      try {
        const r = await api.patchZone(zone.id, { members: next })
        if (r.ok) {
          // Ответ — авторитетный ZoneView (persist завершён до ответа).
          const view = (await r.json()) as ZoneView
          membersRef.current = view.members
          setMembers(view.members)
        } else {
          toast(`зона: HTTP ${r.status}`)
          membersRef.current = null
          setMembers(null) // откат к серверному состоянию
        }
      } catch (e) {
        console.error(e)
        membersRef.current = null
        setMembers(null)
      } finally {
        membersPendingRef.current = false
        setMembersPending(false)
        // Колонка могла получить карту, пока членство ещё летело: теперь шлём.
        for (const nodeId of Object.keys(mapOverridesRef.current)) flushMap(nodeId)
      }
    })()
  }

  const sourceIds = zoneSourceIds(zone)
  const stolenTitle = zone.stolen
    .map((id) => byId.get(id)?.display_name ?? byId.get(id)?.name ?? id)
    .join(', ')

  const sourceNames = sourceIds.map((id) => {
    const n = txNodes.find((node) => node.node_id === id)
    return n ? nodeLabel(n) : id
  })
  const manySources = sourceIds.length > 1
  const freeRx = rxNodes.filter((n) => !effective.includes(n.node_id))

  return (
    <article className="card">
      <div className="card-head">
        <div className="card-title">
          <span>{zone.name}</span>
          {zone.active && <Badge tone="play">играет</Badge>}
          {zone.silenced && <Badge tone="quiet">тишина</Badge>}
          {zone.stolen.length > 0 && (
            <Badge tone="steal" title={`Угнаны: ${stolenTitle}`}>
              угнано: {zone.stolen.length}
            </Badge>
          )}
        </div>
        <div className="card-actions">
          {zone.active ? (
            <Button onClick={() => void run(api.zoneCmd(zone.id, 'stop'), 'стоп зоны')}>Стоп</Button>
          ) : (
            <Button variant="primary" onClick={() => void run(api.zoneCmd(zone.id, 'start'), 'пуск зоны')}>Пуск</Button>
          )}
          <Button
            variant="danger"
            onClick={() => {
              void (async () => {
                const ok = await confirm({
                  title: 'Удалить зону',
                  body: `Зона «${zone.name}» исчезнет вместе с её колонками и картой.`,
                  confirmLabel: 'Удалить',
                  danger: true,
                })
                if (ok) void run(api.deleteZone(zone.id), 'удаление зоны')
              })()
            }}
          >
            Удалить
          </Button>
        </div>
      </div>
      <div className="lede">Источники: {sourceNames.join(', ')}</div>
      <div className="lede">запас {zone.jb_ms} мс</div>
      <div className="lede">Колонки</div>
      {effective.length === 0 && <p className="lede">В зоне пока нет колонок.</p>}
      {effective.map((id) => {
        const n = byId.get(id)
        return (
          <div key={id} className="member-row">
            <span>{n ? nodeLabel(n) : id}</span>
            <Button className="btn-compact" disabled={membersPending} onClick={() => toggleMember(id, false)}>
              Убрать
            </Button>
          </div>
        )
      })}
      <div className="card-actions" style={{ marginTop: '0.75rem' }}>
        <Button onClick={() => setPopup('add')}>Добавить колонку</Button>
        <Button onClick={() => setPopup('settings')}>Настройки</Button>
        <Button onClick={() => setPopup('channels')}>Каналы</Button>
      </div>
      {popup === 'add' && (
        <Popup title="Добавить колонку" onClose={() => setPopup(null)}>
          {freeRx.length === 0 ? (
            <p>Свободных приёмников нет.</p>
          ) : (
            freeRx.map((n) => (
              <Button
                key={`${n.node_id}@${n.ip}`}
                className="pick"
                disabled={membersPending}
                onClick={() => {
                  toggleMember(n.node_id, true)
                  setPopup(null)
                }}
              >
                {nodeLabel(n)}
              </Button>
            ))
          )}
        </Popup>
      )}
      {popup === 'settings' && (
        <ZoneSettings
          zone={zone}
          txNodes={txNodes}
          onClose={() => setPopup(null)}
        />
      )}
      {popup === 'channels' && (
        <Popup title="Каналы" wide onClose={() => setPopup(null)}>
          <p>
            {manySources
              ? 'У каждого источника своя карта: выход колонки берёт канал этого источника.'
              : 'Каждый включённый выход колонки берёт канал из включённых входов источника.'}
          </p>
          {effective.length === 0 && <p>Сначала добавьте колонку.</p>}
          <div>
            {effective.map((id) => {
              const n = byId.get(id)
              const label = n ? nodeLabel(n) : id
              const rows = outputRows(n)
              const silentOut = (n?.outputs ?? []).filter((o) => o.enabled && o.stream == null)
              return (
                <div key={id} className="stack" style={{ marginBottom: '0.8rem' }}>
                  <strong>{label}</strong>
                  {rows.length === 0 && <p className="hint">У колонки нет включённых выходов в стереопотоке.</p>}
                  {sourceIds.map((sourceId) => {
                    const src = txNodes.find((node) => node.node_id === sourceId)
                    const srcName = src ? nodeLabel(src) : sourceId
                    const srcOptions = streamOptions(src)
                    const sourceKey = manySources ? sourceId : null
                    const map = mapOverrides[mapStorageKey(id, sourceKey)]
                      ?? storedMap(zone, id, sourceKey)
                      ?? DEFAULT_ZONE_MAP
                    return (
                      <div key={sourceId}>
                        {manySources && <div className="device-name">{srcName}</div>}
                        {rows.map((row) => (
                          <label key={`${sourceId}-${row.side}`} className="inline">
                            {row.label}
                            <ChanSelect
                              label={`${row.label}, ${label}, ${srcName}`}
                              options={srcOptions}
                              value={row.side === 'out_l' ? map.out_l : map.out_r}
                              onChange={(v) => patchMap(id, sourceKey, row.side, v)}
                            />
                          </label>
                        ))}
                      </div>
                    )
                  })}
                  {silentOut.length > 0 && (
                    <p className="hint">В стереопоток не входят: {silentOut.map((o) => o.name).join(', ')}.</p>
                  )}
                </div>
              )
            })}
          </div>
          {sourceIds.map((sourceId) => {
            const src = txNodes.find((node) => node.node_id === sourceId)
            const idle = (src?.inputs ?? []).filter((i) => i.enabled && i.stream == null)
            if (idle.length === 0) return null
            const prefix = manySources ? `${src ? nodeLabel(src) : sourceId}: ` : ''
            return (
              <p key={sourceId} className="hint">
                {prefix}В стереопоток не входят: {idle.map((i) => i.name).join(', ')}.
              </p>
            )
          })}
        </Popup>
      )}
    </article>
  )
}

function ZoneSettings({
  zone,
  txNodes,
  onClose,
}: {
  zone: ZoneView
  txNodes: NodeView[]
  onClose: () => void
}) {
  const run = useRun()
  const { toast } = useUi()
  const [name, setName] = useState(zone.name)
  const [picked, setPicked] = useState(zoneSourceIds(zone))
  const [minutes, setMinutes] = useState(String(Math.round(zone.silence_timeout_s / 60)))
  const [db, setDb] = useState(String(zone.silence_threshold_db))
  const [jbMs, setJbMs] = useState(String(zone.jb_ms))

  const save = () => {
    const trimmed = name.trim()
    const min = Number(minutes)
    const threshold = Number(db)
    if (!trimmed) {
      toast('Название зоны пустое')
      return
    }
    if (!Number.isInteger(min) || min < 0) {
      toast('Таймаут тишины — целое число минут, 0 выключает')
      return
    }
    if (!Number.isFinite(threshold)) {
      toast('Порог — число в дБ')
      return
    }
    const jb = Number(jbMs)
    if (jbMs.trim() === '' || !Number.isInteger(jb) || jb < 1 || jb > 500) {
      toast('Запас сети — целое число от 1 до 500')
      return
    }
    if (picked.length === 0) {
      toast('Выберите хотя бы один источник')
      return
    }
    void run(api.patchZone(zone.id, {
      name: trimmed,
      sources: picked,
      silence_timeout_s: min * 60,
      silence_threshold_db: threshold,
      jb_ms: jb,
    }), 'настройки зоны').then((ok) => {
      if (ok) onClose()
    })
  }

  return (
    <Popup
      title="Настройки зоны"
      onClose={onClose}
      footer={(
        <div className="row">
          <Button onClick={onClose}>Отмена</Button>
          <Button variant="primary" onClick={save}>Сохранить</Button>
        </div>
      )}
    >
      <div className="stack">
        <label className="inline">
          Название
          <input className="field" aria-label="Название зоны" value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        <div>
          <div className="lede">Источники</div>
          {txNodes.map((n) => (
            <label key={`${n.node_id}@${n.ip}`} className="source-row">
              <input
                type="checkbox"
                aria-label={`Источник ${nodeLabel(n)}`}
                checked={picked.includes(n.node_id)}
                onChange={() => {
                  setPicked((prev) => (
                    prev.includes(n.node_id)
                      ? prev.filter((id) => id !== n.node_id)
                      : [...prev, n.node_id]
                  ))
                }}
              />
              <span>{nodeLabel(n)}</span>
            </label>
          ))}
          {picked
            .filter((id) => !txNodes.some((n) => n.node_id === id))
            .map((id) => (
              <label key={id} className="source-row">
                <input
                  type="checkbox"
                  aria-label={`Источник ${id}`}
                  checked
                  onChange={() => setPicked((prev) => prev.filter((x) => x !== id))}
                />
                <span>{id} (нет в реестре)</span>
              </label>
            ))}
        </div>
        <label className="inline">
          Тишина, мин
          <input
            className="field"
            type="number"
            min={0}
            aria-label="Таймаут тишины, минуты"
            value={minutes}
            onChange={(e) => setMinutes(e.target.value)}
          />
        </label>
        <label className="inline">
          Порог, дБ
          <input
            className="field"
            type="number"
            aria-label="Порог тишины, дБ"
            value={db}
            onChange={(e) => setDb(e.target.value)}
          />
        </label>
        <label className="inline">
          Запас сети, мс
          <input
            className="field"
            type="number"
            min={1}
            max={500}
            step={1}
            aria-label="Запас сети, мс"
            value={jbMs}
            onChange={(e) => setJbMs(e.target.value)}
          />
        </label>
      </div>
    </Popup>
  )
}

export default function Zones() {
  const run = useRun()
  const { toast } = useUi()
  const [zones, setZones] = useState<ZoneView[]>([])
  const [nodes, setNodes] = useState<NodeView[]>([])

  useEffect(() => {
    let alive = true
    const load = async () => {
      const [zs, ns] = await Promise.all([api.zones(), api.nodes()])
      if (!alive) return
      setZones(zs.zones)
      setNodes(ns)
    }
    load().catch(console.error)
    const t = setInterval(() => load().catch(console.error), POLL_MS)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [])

  const byId = new Map(nodes.map((n) => [n.node_id, n]))
  const txNodes = nodes.filter((n) => n.roles.includes('tx'))
  const rxNodes = nodes.filter((n) => n.roles.includes('rx'))

  const addZone = () => {
    const firstTx = txNodes[0]
    if (!firstTx) {
      toast('Нет передатчиков в сети — нечего назначить источником')
      return
    }
    void run(
      api.createZone({ name: 'Новая зона', sources: [firstTx.node_id], members: [] }),
      'создание зоны',
    )
  }

  return (
    <main className="page">
      <div className="page-head">
        <div>
          <h1>Зоны</h1>
          <p className="lede">Колонки зоны, источники и карта каналов — в отдельных окнах.</p>
        </div>
        <Button variant="primary" onClick={addZone}>Новая зона</Button>
      </div>
      {zones.length === 0 ? (
        <p className="empty">Создайте зону и добавьте в неё колонки.</p>
      ) : (
        <div className="cards">
          {zones.map((z) => (
            <ZoneCard key={z.id} zone={z} txNodes={txNodes} rxNodes={rxNodes} byId={byId} />
          ))}
        </div>
      )}
    </main>
  )
}
