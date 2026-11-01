import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { api, NodeView, ZoneView } from '../api'
import { type Lang } from '../i18n'
import { useI18n } from '../locale'
import { Badge, Button, Popup, useRun, useUi } from '../ui'
import {
  afterMapAttempt,
  applyMapSide,
  canSendZoneMap,
  channelMapLead,
  ChannelMap,
  ChanSel,
  DEFAULT_ZONE_MAP,
  feedChoiceLabel,
  feedMappingChoices,
  FeedKind,
  mappingChoices,
  MappingChoice,
  sameChannelMap,
  selectFeed,
  shouldSendZoneMap,
  sourceMapKey,
  speakerOutputRows,
  swapMapSides,
  ZoneSource,
} from '../zoneMap'

const POLL_MS = 2000

function nodeLabel(n: NodeView): string {
  return n.display_name ?? n.name
}

const FEED_KINDS: readonly FeedKind[] = ['spotify', 'sendspin', 'virtual']

function zoneSources(zone: ZoneView): ZoneSource[] {
  return zone.sources ?? []
}

function sourceCaption(src: ZoneSource, byId: Map<string, NodeView>, lang: Lang): string {
  const node = byId.get(src.node_id)
  const label = node ? nodeLabel(node) : src.node_id
  return src.kind === 'line' ? label : feedChoiceLabel(label, src.kind, lang)
}

function feedHosts(nodes: readonly NodeView[], kind: FeedKind): NodeView[] {
  return nodes.filter((n) => {
    if (!n.supports_virtual_output) return false
    if (kind === 'spotify') return Boolean(n.spotify)
    if (kind === 'sendspin') return Boolean(n.sendspin)
    return Boolean(n.virtual_output)
  })
}

function mapStorageKey(member: string, sourceId: string | null): string {
  return sourceId ? `${member}\n${sourceId}` : member
}

function splitMapKey(key: string): { member: string; sourceId: string | null } {
  const [member, sourceId] = key.split('\n')
  return { member, sourceId: sourceId ?? null }
}

function isFeedMapKey(key: string): boolean {
  return key.startsWith('spotify:') || key.startsWith('sendspin:') || key.startsWith('virtual:')
}

function storedMap(zone: ZoneView, member: string, sourceId: string | null): ChannelMap | undefined {
  if (!sourceId) return zone.maps[member]
  const own = zone.source_maps?.[member]?.[sourceId]
  if (own) return own
  // Нет ключа у вида — прямое стерео, не общая карта микрофона.
  if (isFeedMapKey(sourceId)) return undefined
  return zone.maps[member]
}

let chanSelectSeq = 0

function shownChoice(value: ChanSel | null, picked: string | null, options: MappingChoice[]): string {
  if (value == null) return ''
  const held = options.find((o) => o.id === picked && o.side === value)
  return held?.id ?? options.find((o) => o.side === value)?.id ?? ''
}

function menuBox(btn: HTMLButtonElement, count: number): { top: number; left: number; width: number } {
  const rect = btn.getBoundingClientRect()
  const est = Math.min(240, count * 40 + 10)
  const spaceBelow = window.innerHeight - rect.bottom - 8
  const top = spaceBelow >= Math.min(est, 120) || spaceBelow >= rect.top
    ? rect.bottom + 4
    : Math.max(8, rect.top - 4 - est)
  return { top, left: rect.left, width: rect.width }
}

function ChanSourceSelect({
  value,
  label,
  options,
  onChange,
}: {
  value: ChanSel | null
  label: string
  options: MappingChoice[]
  onChange: (v: ChanSel | null) => void
}) {
  const { t } = useI18n()
  // Карта хранит только сторону. Имя входа помним, пока несколько входов
  // делят одну сторону: иначе «ноутбук: левая» и «микрофон: левый» схлопнутся.
  const [picked, setPicked] = useState<string | null>(null)
  const [open, setOpen] = useState(false)
  const openRef = useRef(false)
  const rootRef = useRef<HTMLDivElement>(null)
  const btnRef = useRef<HTMLButtonElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const mine = useRef(0)
  const [box, setBox] = useState<{ top: number; left: number; width: number } | null>(null)
  if (mine.current === 0) mine.current = ++chanSelectSeq
  const shown = shownChoice(value, picked, options)
  const off = value == null
  const labelText = off
    ? t('chan.off')
    : (options.find((o) => o.id === shown)?.label ?? (value === 'l' ? t('chan.left') : t('chan.right')))

  const setSolo = (next: boolean) => {
    openRef.current = next
    setOpen(next)
    if (next) window.dispatchEvent(new CustomEvent('zvucast-chan-select', { detail: mine.current }))
  }

  useEffect(() => {
    const onSolo = (event: Event) => {
      if ((event as CustomEvent<number>).detail === mine.current) return
      if (!openRef.current) return
      openRef.current = false
      setOpen(false)
    }
    window.addEventListener('zvucast-chan-select', onSolo)
    return () => window.removeEventListener('zvucast-chan-select', onSolo)
  }, [])

  useLayoutEffect(() => {
    if (!open) return
    const place = () => {
      const btn = btnRef.current
      if (!btn) return
      setBox(menuBox(btn, options.length + 1))
    }
    place()
    const onDoc = (event: MouseEvent) => {
      const target = event.target
      if (!(target instanceof Node)) return
      if (rootRef.current?.contains(target) || menuRef.current?.contains(target)) return
      openRef.current = false
      setOpen(false)
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return
      event.stopPropagation()
      openRef.current = false
      setOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    window.addEventListener('keydown', onKey, true)
    window.addEventListener('resize', place)
    window.addEventListener('scroll', place, true)
    return () => {
      document.removeEventListener('mousedown', onDoc)
      window.removeEventListener('keydown', onKey, true)
      window.removeEventListener('resize', place)
      window.removeEventListener('scroll', place, true)
    }
  }, [open, options.length])

  const choose = (id: string) => {
    setPicked(id || null)
    const choice = options.find((o) => o.id === id)
    onChange(choice ? choice.side : null)
    openRef.current = false
    setOpen(false)
  }

  return (
    <div ref={rootRef} className={`chan-select${off ? ' off' : ''}${open ? ' open' : ''}`}>
      <button
        ref={btnRef}
        type="button"
        className="chan-select-btn"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={label}
        onClick={() => setSolo(!openRef.current)}
      >
        <span className="value">{labelText}</span>
        <i className={open ? 'chan-chev open' : 'chan-chev'} />
      </button>
      {open && box && createPortal(
        <div
          ref={menuRef}
          className="chan-menu"
          role="listbox"
          aria-label={label}
          style={{ top: box.top, left: box.left, width: box.width }}
        >
          {options.map((o) => (
            <button
              key={o.id}
              type="button"
              role="option"
              aria-selected={shown === o.id}
              className={shown === o.id ? 'on' : undefined}
              onClick={() => choose(o.id)}
            >
              {o.label}
            </button>
          ))}
          <button
            type="button"
            role="option"
            aria-selected={off}
            className={off ? 'on' : undefined}
            onClick={() => choose('')}
          >
            {t('chan.off')}
          </button>
        </div>,
        document.body,
      )}
    </div>
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
  const { t, lang } = useI18n()
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
        ok = await run(api.patchZone(zone.id, body), t('zones.mapWhat'))
      } catch (e) {
        // run не ловит отказ fetch наружу: без finally флаг остался бы true
        // и колонка молчала бы до перезагрузки.
        console.error(e)
        toast(t('toast.noLink', { what: t('zones.mapWhat') }))
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

  const swapSides = (nodeId: string, sourceId: string | null) => {
    const key = mapStorageKey(nodeId, sourceId)
    const current = mapOverridesRef.current[key] ?? storedMap(zone, nodeId, sourceId) ?? DEFAULT_ZONE_MAP
    const nextMap = swapMapSides(current)
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
          toast(`${t('zones.zoneWhat')}: HTTP ${r.status}`)
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

  const sources = zoneSources(zone)
  const stolenTitle = zone.stolen
    .map((id) => byId.get(id)?.display_name ?? byId.get(id)?.name ?? id)
    .join(', ')

  const sourceNames = sources.map((src) => sourceCaption(src, byId, lang))
  const manySources = sources.length > 1
  const freeRx = rxNodes.filter((n) => !effective.includes(n.node_id))

  return (
    <article className="card">
      <div className="card-head">
        <div className="card-title">
          <span>{zone.name}</span>
          {zone.active && <Badge tone="play">{t('zones.playing')}</Badge>}
          {zone.silenced && <Badge tone="quiet">{t('zones.silence')}</Badge>}
          {zone.stolen.length > 0 && (
            <Badge tone="steal" title={t('zones.stolenTitle', { names: stolenTitle })}>
              {t('zones.stolen', { n: zone.stolen.length })}
            </Badge>
          )}
        </div>
        <div className="card-actions">
          {zone.active ? (
            <Button onClick={() => void run(api.zoneCmd(zone.id, 'stop'), t('zones.stopWhat'))}>{t('zones.stop')}</Button>
          ) : (
            <Button variant="primary" onClick={() => void run(api.zoneCmd(zone.id, 'start'), t('zones.startWhat'))}>{t('zones.start')}</Button>
          )}
          <Button
            variant="danger"
            onClick={() => {
              void (async () => {
                const ok = await confirm({
                  title: t('zones.deleteTitle'),
                  body: t('zones.deleteBody', { name: zone.name }),
                  confirmLabel: t('zones.delete'),
                  danger: true,
                })
                if (ok) void run(api.deleteZone(zone.id), t('zones.deleteWhat'))
              })()
            }}
          >
            {t('zones.delete')}
          </Button>
        </div>
      </div>
      <div className="lede">{t('zones.sources', { list: sourceNames.join(', ') })}</div>
      <div className="lede">{t('zones.reserve', { ms: zone.jb_ms })}</div>
      <div className="lede">{t('zones.speakers')}</div>
      {effective.length === 0 && <p className="lede">{t('zones.noSpeakers')}</p>}
      {effective.map((id) => {
        const n = byId.get(id)
        return (
          <div key={id} className="member-row">
            <span>{n ? nodeLabel(n) : id}</span>
            <Button className="btn-compact" disabled={membersPending} onClick={() => toggleMember(id, false)}>
              {t('zones.remove')}
            </Button>
          </div>
        )
      })}
      <div className="card-actions" style={{ marginTop: '0.75rem' }}>
        <Button onClick={() => setPopup('add')}>{t('zones.addSpeaker')}</Button>
        <Button onClick={() => setPopup('settings')}>{t('zones.settings')}</Button>
        <Button onClick={() => setPopup('channels')}>{t('zones.channels')}</Button>
      </div>
      {popup === 'add' && (
        <Popup title={t('zones.addSpeaker')} onClose={() => setPopup(null)}>
          {freeRx.length === 0 ? (
            <p>{t('zones.noFreeRx')}</p>
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
        <Popup title={t('zones.channels')} wide onClose={() => setPopup(null)}>
          <p>{channelMapLead(sources, lang)}</p>
          {effective.length === 0 && <p>{t('zones.addSpeakerFirst')}</p>}
          {effective.map((id) => {
            const n = byId.get(id)
            const label = n ? nodeLabel(n) : id
            const rows = speakerOutputRows(n?.outputs ?? [], lang)
            const silentOut = (n?.outputs ?? []).filter((o) => o.enabled && o.stream == null)
            return (
              <section key={id} className="chan-speaker">
                <h3>{label} <span>{t('zones.speakerRole')}</span></h3>
                {rows.length === 0 && <p className="hint">{t('zones.noStereoOut')}</p>}
                {sources.map((source, sourceIndex) => {
                  const src = byId.get(source.node_id)
                  const srcName = sourceCaption(source, byId, lang)
                  const srcOptions = source.kind === 'line'
                    ? mappingChoices(src?.inputs ?? [], lang)
                    : feedMappingChoices(source.kind, lang)
                  const mapKey = sourceMapKey(source)
                  const sourceKey = manySources ? mapKey : null
                  const map = mapOverrides[mapStorageKey(id, sourceKey)]
                    ?? storedMap(zone, id, sourceKey)
                    ?? DEFAULT_ZONE_MAP
                  return (
                    <div key={mapKey} className="chan-source">
                      <div className="chan-source-name">{srcName}</div>
                      {sourceIndex === 0 && rows.length > 0 && (
                        <div className="chan-heads">
                          <span>{t('zones.sourceHead')}</span>
                          <span>{t('zones.playsHead')}</span>
                        </div>
                      )}
                      {rows.map((row) => {
                        const value = row.side === 'out_l' ? map.out_l : map.out_r
                        return (
                          <div key={`${mapKey}-${row.id}`} className={value == null ? 'chan-route off' : 'chan-route'}>
                            <ChanSourceSelect
                              label={t('zones.sourceFor', { output: row.label, speaker: label, source: srcName })}
                              options={srcOptions}
                              value={value}
                              onChange={(v) => patchMap(id, sourceKey, row.side, v)}
                            />
                            <div className="chan-arrow" aria-hidden="true">→</div>
                            <div className="chan-play">
                              <span className="chan-mark">{row.side === 'out_l' ? t('chan.leftMark') : t('chan.rightMark')}</span>
                              <b>{row.label}</b>
                            </div>
                          </div>
                        )
                      })}
                      {rows.length >= 2 && (
                        <button type="button" className="chan-swap" onClick={() => swapSides(id, sourceKey)}>
                          {t('zones.swap')}
                        </button>
                      )}
                    </div>
                  )
                })}
                {silentOut.length > 0 && (
                  <p className="hint">{t('zones.notInStereo', { list: silentOut.map((o) => o.name).join(', ') })}</p>
                )}
              </section>
            )
          })}
          {sources.map((source) => {
            if (source.kind !== 'line') return null
            const src = byId.get(source.node_id)
            const listed = new Set(mappingChoices(src?.inputs ?? [], lang).map((c) => c.id))
            const idle = (src?.inputs ?? []).filter((i) => i.enabled && i.stream == null && !listed.has(i.id))
            if (idle.length === 0) return null
            const prefix = manySources ? `${sourceCaption(source, byId, lang)}: ` : ''
            return (
              <p key={sourceMapKey(source)} className="hint">
                {prefix}{t('zones.notInStereo', { list: idle.map((i) => i.name).join(', ') })}
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
  const { t, lang } = useI18n()
  const [name, setName] = useState(zone.name)
  const [picked, setPicked] = useState(zoneSources(zone))
  const lineOn = (id: string) => picked.some((s) => s.kind === 'line' && s.node_id === id)
  const toggleLine = (id: string) => {
    setPicked((prev) => (
      prev.some((s) => s.kind === 'line' && s.node_id === id)
        ? prev.filter((s) => !(s.kind === 'line' && s.node_id === id))
        : [...prev, { kind: 'line', node_id: id }]
    ))
  }
  const toggleFeed = (kind: FeedKind, nodeId: string) => {
    setPicked((prev) => {
      const current = prev.find((s) => s.kind === kind)
      if (current?.node_id === nodeId) return prev.filter((s) => s.kind !== kind)
      return selectFeed(prev, kind, nodeId)
    })
  }
  const [minutes, setMinutes] = useState(String(Math.round(zone.silence_timeout_s / 60)))
  const [db, setDb] = useState(String(zone.silence_threshold_db))
  const [jbMs, setJbMs] = useState(String(zone.jb_ms))

  const save = () => {
    const trimmed = name.trim()
    const min = Number(minutes)
    const threshold = Number(db)
    if (!trimmed) {
      toast(t('zones.nameEmpty'))
      return
    }
    if (!Number.isInteger(min) || min < 0) {
      toast(t('zones.timeoutBad'))
      return
    }
    if (!Number.isFinite(threshold)) {
      toast(t('zones.thresholdBad'))
      return
    }
    const jb = Number(jbMs)
    if (jbMs.trim() === '' || !Number.isInteger(jb) || jb < 1 || jb > 500) {
      toast(t('zones.reserveBad'))
      return
    }
    if (picked.length === 0) {
      toast(t('zones.needSource'))
      return
    }
    void run(api.patchZone(zone.id, {
      name: trimmed,
      sources: picked,
      silence_timeout_s: min * 60,
      silence_threshold_db: threshold,
      jb_ms: jb,
    }), t('zones.settingsWhat')).then((ok) => {
      if (ok) onClose()
    })
  }

  return (
    <Popup
      title={t('zones.settingsTitle')}
      onClose={onClose}
      footer={(
        <div className="row">
          <Button onClick={onClose}>{t('common.cancel')}</Button>
          <Button variant="primary" onClick={save}>{t('common.save')}</Button>
        </div>
      )}
    >
      <div className="stack">
        <label className="inline">
          {t('zones.name')}
          <input className="field" aria-label={t('zones.nameAria')} value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        <div>
          <div className="lede">{t('zones.sourcesLabel')}</div>
          {txNodes.map((n) => (
            <label key={`${n.node_id}@${n.ip}`} className="source-row">
              <input
                type="checkbox"
                aria-label={t('zones.sourceAria', { name: nodeLabel(n) })}
                checked={lineOn(n.node_id)}
                onChange={() => toggleLine(n.node_id)}
              />
              <span>{nodeLabel(n)}</span>
            </label>
          ))}
          {picked
            .filter((s) => s.kind === 'line' && !txNodes.some((n) => n.node_id === s.node_id))
            .map((s) => (
              <label key={s.node_id} className="source-row">
                <input
                  type="checkbox"
                  aria-label={t('zones.sourceAria', { name: s.node_id })}
                  checked
                  onChange={() => toggleLine(s.node_id)}
                />
                <span>{t('zones.notInRegistry', { name: s.node_id })}</span>
              </label>
            ))}
          {FEED_KINDS.map((kind) => {
            const hosts = feedHosts(txNodes, kind)
            if (hosts.length === 0) return null
            const selected = picked.find((s) => s.kind === kind)?.node_id
            return (
              <div key={kind}>
                {hosts.map((n) => {
                  const label = feedChoiceLabel(nodeLabel(n), kind, lang)
                  return (
                    <label key={`${kind}:${n.node_id}`} className="source-row">
                      <input
                        type="checkbox"
                        aria-label={label}
                        checked={selected === n.node_id}
                        onChange={() => toggleFeed(kind, n.node_id)}
                      />
                      <span>{label}</span>
                    </label>
                  )
                })}
              </div>
            )
          })}
        </div>
        <label className="inline">
          {t('zones.silenceMin')}
          <input
            className="field"
            type="number"
            min={0}
            aria-label={t('zones.silenceAria')}
            value={minutes}
            onChange={(e) => setMinutes(e.target.value)}
          />
        </label>
        <label className="inline">
          {t('zones.threshold')}
          <input
            className="field"
            type="number"
            aria-label={t('zones.thresholdAria')}
            value={db}
            onChange={(e) => setDb(e.target.value)}
          />
        </label>
        <label className="inline">
          {t('zones.reserveLabel')}
          <input
            className="field"
            type="number"
            min={1}
            max={500}
            step={1}
            aria-label={t('zones.reserveAria')}
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
  const { t } = useI18n()
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
      toast(t('zones.noTx'))
      return
    }
    void run(
      api.createZone({
        name: t('zones.new'),
        sources: [{ kind: 'line', node_id: firstTx.node_id }],
        members: [],
      }),
      t('zones.createWhat'),
    )
  }

  return (
    <main className="page">
      <div className="page-head">
        <div>
          <h1>{t('zones.title')}</h1>
          <p className="lede">{t('zones.lead')}</p>
        </div>
        <Button variant="primary" onClick={addZone}>{t('zones.new')}</Button>
      </div>
      {zones.length === 0 ? (
        <p className="empty">{t('zones.empty')}</p>
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
