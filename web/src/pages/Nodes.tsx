import { useEffect, useMemo, useState } from 'react'
import { api, InputView, MasterInfo, NodeView } from '../api'
import { offer, updateWait } from '../update'
import { Badge, Button, Popup, useRun, useUi } from '../ui'

const POLL_MS = 2000
/// Свой POST: «не вернулся», если за это время узел так и остался offline.
/// Живой узел на старой версии к этому сроку — «не установилось».
const UPDATE_WAIT_MS = 90_000

type ReleaseView = Awaited<ReturnType<typeof api.release>>
type PendingUpdate = { at: number; version: string; note: string | null }
const DSCP_PRESETS = [
  { label: 'CS0', value: 0 },
  { label: 'CS3', value: 24 },
  { label: 'CS5', value: 40 },
  { label: 'EF', value: 46 },
]

/// GStreamer level уже отдаёт дБ. (0, 0) — замер ещё не пришёл.
function rmsText(v: number): string {
  if (!Number.isFinite(v) || v === 0) return '—'
  return v.toFixed(1)
}

function fmtUptime(s: number): string {
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = s % 60
  return `${h}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}`
}

function linkLabel(link: NodeView['link']): string {
  if (link === 'wifi') return 'Wi‑Fi'
  if (link === 'wired') return 'Провод'
  return '—'
}

function nodeTitle(n: NodeView) {
  return (
    <span>
      {n.display_name ?? n.name}
      {n.display_name && <small> {n.name}</small>}
    </span>
  )
}

const SETUP_TITLE: Record<string, string> = {
  pipewire: 'Служба PipeWire не запущена',
  wireplumber: 'WirePlumber не запущен',
  'pipewire-pulse': 'PipeWire Pulse не запущен',
  gstreamer: 'Нет плагинов GStreamer',
  linger: 'После перезагрузки PipeWire не поднимется',
  'pipewire-socket': 'Нет сокета PipeWire',
  'rt-limits': 'Нет rtprio для группы audio',
  'wifi-powersave': 'На Wi-Fi включено энергосбережение',
  controller: 'Контроллер включён, но не работает',
  soloist: 'Spotify включён, а Soloist недоступен',
}

async function copyCommand(text: string, el: HTMLElement) {
  try {
    const clip = navigator.clipboard
    if (!clip) throw new Error('clipboard')
    await clip.writeText(text)
  } catch {
    const sel = window.getSelection()
    if (!sel) return
    const range = document.createRange()
    range.selectNodeContents(el)
    sel.removeAllRanges()
    sel.addRange(range)
  }
}

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

function groupChans(rows: InputView[]) {
  const order: string[] = []
  const by = new Map<string, InputView[]>()
  for (const row of rows) {
    if (!by.has(row.device_key)) order.push(row.device_key)
    const list = by.get(row.device_key) ?? []
    list.push(row)
    by.set(row.device_key, list)
  }
  return order.map((key) => ({ key, name: by.get(key)![0].device_name, rows: by.get(key)! }))
}

function ChanEditor({
  title,
  hint,
  empty,
  rows,
  onPatch,
}: {
  title: string
  hint: string
  empty: string
  rows: InputView[]
  onPatch: (id: string, next: Partial<InputView>) => void
}) {
  const groups = useMemo(() => groupChans(rows), [rows])
  return (
    <div>
      <div className="lede">{title}</div>
      <p className="hint">{hint}</p>
      {groups.length === 0 && <p className="lede">{empty}</p>}
      <div>
        {groups.map((g) => (
          <div key={g.key}>
            <h3 className="device-name">{g.name}</h3>
            {g.rows.map((row) => (
              <label key={row.id} className="chan-row">
                <input
                  type="checkbox"
                  checked={row.enabled}
                  aria-label={`Использовать ${g.name} ${row.index + 1}`}
                  onChange={(e) => onPatch(row.id, { enabled: e.target.checked })}
                />
                <input
                  className="field"
                  aria-label={`Имя канала ${g.name} ${row.index + 1}`}
                  value={row.name}
                  onChange={(e) => onPatch(row.id, { name: e.target.value })}
                />
              </label>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

function NodeSettings({ node, onClose }: { node: NodeView; onClose: () => void }) {
  const run = useRun()
  const { toast } = useUi()
  const isTx = node.roles.includes('tx')
  const isRx = node.roles.includes('rx')
  const [name, setName] = useState(node.display_name ?? '')
  const [dscp, setDscp] = useState(String(node.dscp ?? 40))
  const [rows, setRows] = useState<InputView[]>(() => (node.inputs ?? []).map((i) => ({ ...i })))
  const [outRows, setOutRows] = useState<InputView[]>(() => (node.outputs ?? []).map((i) => ({ ...i })))
  const [virtualOn, setVirtualOn] = useState(node.virtual_output ?? false)
  const [spotifyOn, setSpotifyOn] = useState(node.spotify ?? false)
  const [sendspinOn, setSendspinOn] = useState(node.sendspin ?? false)
  const [soloistKey, setSoloistKey] = useState(node.soloist_key ?? '')

  const patchRow = (id: string, next: Partial<InputView>) => {
    setRows((list) => list.map((row) => (row.id === id ? { ...row, ...next } : row)))
  }
  const patchOut = (id: string, next: Partial<InputView>) => {
    setOutRows((list) => list.map((row) => (row.id === id ? { ...row, ...next } : row)))
  }

  const save = () => {
    const body: {
      display_name: string | null
      dscp?: number
      inputs?: { id: string; name: string; enabled: boolean }[]
      outputs?: { id: string; name: string; enabled: boolean }[]
      virtual_output?: boolean
      soloist_key?: string
      spotify?: boolean
      sendspin?: boolean
    } = { display_name: name.trim() || null }
    if (isTx) {
      const n = Number(dscp)
      if (!Number.isInteger(n) || n < 0 || n > 63) {
        toast('DSCP — целое от 0 до 63')
        return
      }
      body.dscp = n
      body.inputs = rows.map((row) => ({ id: row.id, name: row.name.trim(), enabled: row.enabled }))
      if (node.supports_virtual_output) {
        body.virtual_output = virtualOn
        body.spotify = spotifyOn
        body.sendspin = sendspinOn
        body.soloist_key = soloistKey.trim()
      }
    }
    if (isRx) {
      body.outputs = outRows.map((row) => ({ id: row.id, name: row.name.trim(), enabled: row.enabled }))
    }
    void run(api.patchNode(node.node_id, body), 'настройки узла').then((ok) => {
      if (ok) onClose()
    })
  }

  return (
    <Popup
      title="Настройки узла"
      wide
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
          Имя
          <input className="field" aria-label="Имя узла" value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        {isTx && (
          <>
            <label className="inline">
              DSCP
              <input
                className="field"
                type="number"
                min={0}
                max={63}
                aria-label="DSCP"
                value={dscp}
                onChange={(e) => setDscp(e.target.value)}
              />
            </label>
            <div className="seg" role="group" aria-label="Пресеты DSCP">
              {DSCP_PRESETS.map((p) => (
                <button
                  key={p.label}
                  type="button"
                  aria-pressed={Number(dscp) === p.value}
                  onClick={() => setDscp(String(p.value))}
                >
                  {p.label}
                </button>
              ))}
            </div>
            {node.supports_virtual_output && (
              <>
                <label className="inline">
                  <input
                    type="checkbox"
                    aria-label="Создать виртуальный звуковой выход"
                    checked={virtualOn}
                    onChange={(e) => setVirtualOn(e.target.checked)}
                  />
                  Создать виртуальный звуковой выход
                </label>
                <label className="inline">
                  Система
                  <select className="field" aria-label="Система" value="stereo" onChange={() => {}}>
                    <option value="stereo">Стерео</option>
                  </select>
                </label>
                <label className="inline">
                  <input
                    type="checkbox"
                    aria-label="Включить Spotify"
                    checked={spotifyOn}
                    onChange={(e) => setSpotifyOn(e.target.checked)}
                  />
                  Включить Spotify
                </label>
                <label className="inline">
                  Ключ Spotify
                  <input
                    className="field"
                    type="password"
                    autoComplete="off"
                    aria-label="Ключ Spotify"
                    value={soloistKey}
                    onChange={(e) => setSoloistKey(e.target.value)}
                  />
                </label>
                <p className="hint">Приёмник Spotify поднимается только на этом передатчике, когда Spotify включён и ключ задан.</p>
                <label className="inline">
                  <input
                    type="checkbox"
                    aria-label="Включить Sendspin"
                    checked={sendspinOn}
                    onChange={(e) => setSendspinOn(e.target.checked)}
                  />
                  Включить Sendspin
                </label>
                <p className="hint">Колонки Sendspin поднимаются на зоны этого передатчика, только когда Sendspin включён.</p>
              </>
            )}
            <ChanEditor
              title="Входы"
              hint="Отмеченные каналы входят в стереопоток. Виртуальный выход — такой же вход: он подмешивается в зону и раскладывается по выходам колонки в карте зоны."
              empty="Агент ещё не прислал устройства захвата."
              rows={rows}
              onPatch={patchRow}
            />
          </>
        )}
        {isRx && (
          <ChanEditor
            title="Выходы"
            hint="Отмеченные каналы — выходы колонки в карте зоны. Звук играет на устройство этих каналов."
            empty="Агент ещё не прислал устройства воспроизведения."
            rows={outRows}
            onPatch={patchOut}
          />
        )}
      </div>
    </Popup>
  )
}

function rolesOf(n: NodeView) {
  return n.roles.map((r) => (
    <Badge key={r} tone="role">{r.toUpperCase()}</Badge>
  ))
}

function facts(n: NodeView) {
  const tm = n.telemetry
  const tx = tm?.tx ?? null
  const rx = tm?.rx ?? null
  const ptp = tm && Math.abs(tm.ptp_offset_ns) <= 1e12 ? (tm.ptp_offset_ns / 1000).toFixed(1) : '—'
  const queue = rx && rx.queue_ms >= 0 ? String(rx.queue_ms) : '—'
  return [
    ['TX', tx?.state ?? '—'],
    ['RMS L/R', tx ? `${rmsText(tx.rms_l)} / ${rmsText(tx.rms_r)}` : '—'],
    ['RX', rx?.state ?? '—'],
    ['Заикания', rx ? String(rx.dropped) : '—'],
    ['Rendered', rx ? String(rx.rendered) : '—'],
    ['Очередь, мс', queue],
    ['PTP, мкс', ptp],
    ['Uptime', tm ? fmtUptime(tm.uptime_s) : '—'],
  ] as const
}

export default function Nodes() {
  const run = useRun()
  const { confirm, toast } = useUi()
  const [nodes, setNodes] = useState<NodeView[]>([])
  const [master, setMaster] = useState<MasterInfo | null>(null)
  const [release, setRelease] = useState<ReleaseView | null>(null)
  const [pending, setPending] = useState<Record<string, PendingUpdate>>({})
  const [now, setNow] = useState(() => Date.now())
  const [movedTo, setMovedTo] = useState<string | null>(null)
  const [settingsId, setSettingsId] = useState<string | null>(null)

  useEffect(() => {
    let alive = true
    const load = async () => {
      const [ns, m] = await Promise.all([api.nodes(), api.master()])
      if (!alive) return
      setNodes(ns)
      setMaster(m)
      setPending((prev) => {
        const ids = Object.keys(prev)
        if (ids.length === 0) return prev
        let changed = false
        const next = { ...prev }
        for (const n of ns) {
          const wait = next[n.node_id]
          const rolled = n.update_note === 'rolled_back' && wait?.note !== 'rolled_back'
          const failed = n.update_note === 'failed' && wait?.note !== 'failed'
          if (wait && (n.version === wait.version || rolled || failed)) {
            delete next[n.node_id]
            changed = true
          }
        }
        return changed ? next : prev
      })
      try {
        const rel = await api.release()
        if (!alive) return
        setRelease(rel)
      } catch (e) {
        console.error(e)
      }
    }
    load().catch(console.error)
    const t = setInterval(() => load().catch(console.error), POLL_MS)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [])

  useEffect(() => {
    if (Object.keys(pending).length === 0) return
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [pending])

  const masterNodeId =
    master?.node_id ?? nodes.find((n) => n.ip === location.hostname)?.node_id ?? null
  const settingsNode = nodes.find((n) => n.node_id === settingsId) ?? null

  const transfer = async (node: NodeView) => {
    const ok = await confirm({
      title: 'Передать мастера',
      body: `Панель откроется на узле «${node.display_name ?? node.name}». Контроллер на этом узле остановится.`,
      confirmLabel: 'Передать',
    })
    if (!ok) return
    const r = await api.transfer(node.node_id)
    if (!r.ok) {
      toast(`трансфер: HTTP ${r.status}`)
      return
    }
    const body = await r.json() as { url?: string }
    if (body.url) {
      setMovedTo(body.url)
      window.location.assign(body.url)
      return
    }
    toast('Роль мастера передана, но адрес новой панели не пришёл')
  }

  const beginWait = (ids: string[], version: string) => {
    if (ids.length === 0) return
    const at = Date.now()
    setNow(at)
    setPending((prev) => {
      const next = { ...prev }
      for (const id of ids) {
        next[id] = {
          at,
          version,
          note: nodes.find((node) => node.node_id === id)?.update_note ?? null,
        }
      }
      return next
    })
  }

  /// POST уже записал свежий релиз. Ждём hello именно с этой версией.
  /// Сбой GET — тот же путь, что у полла: не подставляем старую версию и не
  /// считаем установку успешной.
  const pinInstalled = async (ids: string[]) => {
    try {
      const rel = await api.release()
      setRelease(rel)
      beginWait(ids, rel.version)
    } catch (e) {
      console.error(e)
    }
  }

  const updateOne = (n: NodeView) => {
    void run(api.updateNode(n.node_id), 'обновление').then((ok) => {
      if (ok) void pinInstalled([n.node_id])
    })
  }

  const updateAll = async () => {
    let response: Response
    try {
      response = await api.updateAll()
    } catch (e) {
      console.error(e)
      toast('обновление: нет связи')
      return
    }
    if (!response.ok) {
      toast(`обновление: HTTP ${response.status}`)
      return
    }
    let ids: string[] = []
    try {
      const body: unknown = await response.json()
      if (Array.isArray(body)) ids = body.filter((id): id is string => typeof id === 'string')
    } catch (e) {
      console.error(e)
    }
    await pinInstalled(ids)
  }

  const versionCell = (n: NodeView, compact = false) => {
    const assetPresent = release != null && Object.prototype.hasOwnProperty.call(release.assets, n.target)
    const kind = release
      ? offer(n.online, n.target, n.version, release.version, assetPresent)
      : 'hidden'
    const wait = pending[n.node_id]
    const phase = updateWait(
      n.online,
      n.version,
      n.update_note,
      wait ? { version: wait.version, note: wait.note } : null,
      wait ? now - wait.at : 0,
      UPDATE_WAIT_MS,
    )
    return (
      <div className="row">
        <span className="num">{n.version || '—'}</span>
        {phase.updating && <span>обновляется</span>}
        {phase.silent && <span>не вернулся</span>}
        {phase.failed && !phase.updating && <span>не установилось</span>}
        {phase.rolledBack && <span>откат</span>}
        {kind === 'missing' && <span>нет файла в релизе</span>}
        {kind === 'ready' && !phase.updating && (
          <Button className={compact ? 'btn-compact' : undefined} onClick={() => updateOne(n)}>Обновить</Button>
        )}
      </div>
    )
  }

  const actions = (n: NodeView, compact = false) => (
    <>
      <Button className={compact ? 'btn-compact' : undefined} onClick={() => setSettingsId(n.node_id)}>Настройки</Button>
      <Button className={compact ? 'btn-compact' : undefined} onClick={() => void run(api.nodeCmd(n.node_id, 'start'), 'старт')}>Старт</Button>
      <Button className={compact ? 'btn-compact' : undefined} onClick={() => void run(api.nodeCmd(n.node_id, 'stop'), 'стоп')}>Стоп</Button>
      {masterNodeId !== n.node_id && (
        <Button className={compact ? 'btn-compact' : undefined} disabled={!n.online} onClick={() => void transfer(n)}>Сделать мастером</Button>
      )}
    </>
  )

  const marks = (n: NodeView) => (
    <>
      {n.dup && <Badge tone="dup">Дубликат</Badge>}
      {masterNodeId === n.node_id && <Badge tone="master">Мастер</Badge>}
    </>
  )

  if (movedTo) {
    return (
      <main className="page">
        <h1>Мастер переехал</h1>
        <p className="lede">Если переход не случился сам, откройте новую панель:</p>
        <p><a href={movedTo}>{movedTo}</a></p>
      </main>
    )
  }

  const online = nodes.filter((n) => n.online).length

  return (
    <main className="page">
      <div className="page-head">
        <div>
          <h1>Узлы</h1>
          <p className="lede">{online} из {nodes.length} в сети</p>
        </div>
        <div className="toolbar">
          <Button variant="primary" onClick={() => void run(fetch('/api/start-all', { method: 'POST' }), 'старт всех')}>
            Старт всех
          </Button>
          <Button onClick={() => void run(fetch('/api/stop-all', { method: 'POST' }), 'стоп всех')}>
            Стоп всех
          </Button>
          <Button onClick={() => void updateAll()}>Обновить все</Button>
        </div>
      </div>

      {setupBlock(nodes)}

      <div className="table-wrap only-wide">
        <table className="data">
          <thead>
            <tr>
              <th>Имя</th>
              <th>IP</th>
              <th>Интерфейс</th>
              <th>Роли</th>
              <th>Статус</th>
              <th>TX</th>
              <th>RMS L/R, дБ</th>
              <th>RX</th>
              <th>Заикания</th>
              <th>Rendered</th>
              <th>Очередь</th>
              <th>PTP, мкс</th>
              <th>Uptime</th>
              <th>Версия</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((n) => {
              const row = facts(n)
              return (
                <tr key={`${n.node_id}@${n.ip}`} className={n.online ? '' : 'off'}>
                  <td>
                    {nodeTitle(n)}
                    {' '}
                    {marks(n)}
                  </td>
                  <td className="num">{n.ip || '—'}</td>
                  <td>{linkLabel(n.link)}</td>
                  <td>{rolesOf(n)}</td>
                  <td>
                    <span className={`dot ${n.online ? 'dot-on' : 'dot-off'}`} /> {n.online ? 'online' : 'offline'}
                  </td>
                  <td>{row[0][1]}</td>
                  <td className="num">{row[1][1]}</td>
                  <td>{row[2][1]}</td>
                  <td className="num">{row[3][1]}</td>
                  <td className="num">{row[4][1]}</td>
                  <td className="num">{row[5][1]}</td>
                  <td className="num">{row[6][1]}</td>
                  <td className="num">{row[7][1]}</td>
                  <td>{versionCell(n, true)}</td>
                  <td className="row">{actions(n, true)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="cards only-narrow">
        {nodes.map((n) => (
          <article key={`${n.node_id}@${n.ip}`} className={`card${n.online ? '' : ' off'}`}>
            <div className="card-head">
              <div className="card-title">
                <span className={`dot ${n.online ? 'dot-on' : 'dot-off'}`} />
                {nodeTitle(n)}
                {marks(n)}
              </div>
              <div className="row">{rolesOf(n)}</div>
            </div>
            <dl className="kv">
              <span style={{ display: 'contents' }}>
                <dt>IP</dt>
                <dd className="num">{n.ip || '—'}</dd>
              </span>
              <span style={{ display: 'contents' }}>
                <dt>Интерфейс</dt>
                <dd>{linkLabel(n.link)}</dd>
              </span>
              <span style={{ display: 'contents' }}>
                <dt>Версия</dt>
                <dd>{versionCell(n)}</dd>
              </span>
              {facts(n).map(([k, v]) => (
                <span key={k} style={{ display: 'contents' }}>
                  <dt>{k}</dt>
                  <dd className="num">{v}</dd>
                </span>
              ))}
            </dl>
            <div className="card-actions" style={{ marginTop: '0.75rem' }}>{actions(n)}</div>
          </article>
        ))}
      </div>
      {settingsNode && (
        <NodeSettings key={settingsNode.node_id} node={settingsNode} onClose={() => setSettingsId(null)} />
      )}
    </main>
  )
}
