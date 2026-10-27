import { useEffect, useMemo, useState } from 'react'
import { api, InputView, MasterInfo, NodeView } from '../api'
import { addableDevices, chanWord, enableDevice, flowDevices, groupDevices, isPlaying, showStartAll, showStopAll, showVolumeSlider, transport, volumeFromPercent, volumePercent } from '../nodesView'
import { offer, updateWait } from '../update'
import { Badge, Button, useRun, useUi } from '../ui'

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

/// Нет сети важнее звука: офлайн не бывает «играет», даже если телеметрия старая.
function presenceWord(n: NodeView): { text: string; tone: 'faint' | 'good' | 'quiet' } {
  if (!n.online) return { text: 'не в сети', tone: 'faint' }
  if (isPlaying(n.telemetry)) return { text: 'играет', tone: 'good' }
  return { text: 'стоит', tone: 'quiet' }
}

/// Живые метры. Пустое значение не занимает место прочерком.
function meterFacts(n: NodeView): { key: string; label: string; value: string }[] {
  const tm = n.telemetry
  const tx = tm?.tx ?? null
  const rx = tm?.rx ?? null
  const items: { key: string; label: string; value: string }[] = []
  const rmsLive = tx?.state === 'playing'
    && [tx.rms_l, tx.rms_r].some((v) => Number.isFinite(v) && v !== 0)
  if (tx && rmsLive) {
    items.push({
      key: 'rms',
      label: 'RMS',
      value: `${rmsText(tx.rms_l)} / ${rmsText(tx.rms_r)}`,
    })
  }
  if (rx?.state === 'playing' && rx.queue_ms >= 0) {
    items.push({ key: 'queue', label: 'Очередь', value: String(rx.queue_ms) })
  }
  if (rx && rx.dropped > 0) {
    items.push({ key: 'drop', label: 'Заикания', value: String(rx.dropped) })
  }
  if (n.online && tm && Math.abs(tm.ptp_offset_ns) <= 1e12) {
    items.push({ key: 'ptp', label: 'PTP', value: (tm.ptp_offset_ns / 1000).toFixed(1) })
  }
  if (tm) items.push({ key: 'up', label: 'Uptime', value: fmtUptime(tm.uptime_s) })
  return items
}

function NodeBoard() {
  const pins = [30, 40, 50, 60]
  return (
    <svg className="node-board" viewBox="0 0 148 92" aria-hidden="true">
      <rect className="node-board-plate" x="8" y="6" width="132" height="80" rx="12" />
      <circle className="node-board-mark" cx="20" cy="18" r="2.3" />
      <circle className="node-board-mark" cx="128" cy="18" r="2.3" />
      <circle className="node-board-mark" cx="20" cy="74" r="2.3" />
      <circle className="node-board-mark" cx="128" cy="74" r="2.3" />
      {pins.map((y) => (
        <rect key={`l${y}`} className="node-board-pin" x="0" y={y} width="12" height="3.5" rx="1" />
      ))}
      {pins.map((y) => (
        <rect key={`r${y}`} className="node-board-pin" x="136" y={y} width="12" height="3.5" rx="1" />
      ))}
      <path className="node-board-trace" d="M12 31.7H46M12 41.7H40M12 51.7H40M12 61.7H46M102 31.7H136M108 41.7H136M108 51.7H136M102 61.7H136" />
      <rect className="node-board-chip" x="50" y="28" width="48" height="36" rx="4" />
    </svg>
  )
}

function NodeFlags({ node }: { node: NodeView }) {
  return (
    <>
      {node.virtual_output && (
        <span className="node-flag node-flag-virtual" role="img" aria-label="Виртуальная колонка">
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <path fill="currentColor" d="M2.6 6.5h2.5L8.6 3.5v11L5.1 11.5H2.6V6.5z" />
            <path fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" d="M10.7 7a2.6 2.6 0 0 1 0 4" />
            <path fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" d="M12.4 5.3a4.9 4.9 0 0 1 0 7.4" />
          </svg>
        </span>
      )}
      {node.spotify && (
        <span className="node-flag node-flag-spotify" role="img" aria-label="Spotify">
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <ellipse cx="6.5" cy="13.1" rx="2.5" ry="1.75" transform="rotate(-28 6.5 13.1)" fill="currentColor" />
            <path fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" d="M8.5 12.2V3.6" />
            <path fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" d="M8.5 3.8c2.5.55 4.3 1.45 4.3 1.45" />
          </svg>
        </span>
      )}
      {node.sendspin && (
        <span className="node-flag node-flag-sendspin" role="img" aria-label="Sendspin">
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <path fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" d="M9.1 3.2a5.1 5.1 0 0 1 4.3 2.5" />
            <path fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" d="M13.5 2.8v3.1h-3.1" />
            <path fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" d="M8.9 14.8a5.1 5.1 0 0 1-4.3-2.5" />
            <path fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" d="M4.5 15.2V12.1h3.1" />
          </svg>
        </span>
      )}
    </>
  )
}

function FlowSide({
  kind,
  label,
  devices,
}: {
  kind: 'in' | 'out'
  label: string
  devices: { key: string; name: string; count: number }[]
}) {
  return (
    <div className={`node-flow-side node-flow-${kind}`}>
      <div className="node-flow-label">{label}</div>
      <div className="node-bus">
        {devices.map((device) => (
          <div key={device.key} className="flow-port">
            {kind === 'out' && <span className="flow-stub" aria-hidden="true" />}
            <div className="flow-chip">
              <span className="flow-chip-name" title={device.name}>{device.name}</span>
              <span className="flow-count" aria-label={`${device.count} ${chanWord(device.count)}`}>{device.count}</span>
            </div>
            {kind === 'in' && <span className="flow-stub" aria-hidden="true" />}
          </div>
        ))}
      </div>
    </div>
  )
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
  'pw-cat': 'Нет pw-record или pw-play',
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
    return [{ id: `${n.node_id}@${n.ip}`, name: n.display_name ?? n.name, rows }]
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

function ChanEditor({
  kind,
  title,
  empty,
  rows,
  onPatch,
  onEnable,
}: {
  kind: 'in' | 'out'
  title: string
  empty: string
  rows: InputView[]
  onPatch: (id: string, next: Partial<InputView>) => void
  onEnable: (key: string) => void
}) {
  const [adding, setAdding] = useState(false)
  const shown = useMemo(
    () => groupDevices(rows).filter((group) => group.rows.some((row) => row.enabled)),
    [rows],
  )
  const addable = useMemo(() => addableDevices(rows), [rows])
  return (
    <div className={`node-flow-side node-flow-${kind}`}>
      <div className="node-flow-label">{title}</div>
      {rows.length === 0 && <p className="lede">{empty}</p>}
      <div className="node-bus">
        {shown.map((group) => (
          <div key={group.key} className="flow-port">
            {kind === 'out' && <span className="flow-stub" aria-hidden="true" />}
            <div className="chan-pick">
              <span className="chan-pick-name" title={group.name}>{group.name}</span>
              {group.rows.map((row) => (
                <div key={row.id} className="chan-row">
                  <input
                    type="checkbox"
                    checked={row.enabled}
                    aria-label={`Использовать ${group.name} ${row.index + 1}`}
                    onChange={(e) => onPatch(row.id, { enabled: e.target.checked })}
                  />
                  <input
                    className="field"
                    aria-label={`Имя канала ${group.name} ${row.index + 1}`}
                    value={row.name}
                    onChange={(e) => onPatch(row.id, { name: e.target.value })}
                  />
                </div>
              ))}
            </div>
            {kind === 'in' && <span className="flow-stub" aria-hidden="true" />}
          </div>
        ))}
      </div>
      {addable.length > 0 && (
        <>
          <button
            type="button"
            className="node-add-open"
            aria-expanded={adding}
            onClick={() => setAdding((open) => !open)}
          >
            Добавить
          </button>
          {adding && (
            <div className="node-adder">
              {addable.map((device) => (
                <button
                  key={device.key}
                  type="button"
                  className="node-choice"
                  title={device.name}
                  onClick={() => {
                    onEnable(device.key)
                    setAdding(false)
                  }}
                >
                  {device.name}
                </button>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}

function NodeSettings({
  node,
  master,
  transfer,
  onClose,
}: {
  node: NodeView
  master: boolean
  transfer: () => void
  onClose: () => void
}) {
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
  const [captureDraft, setCaptureDraft] = useState<number | null>(null)
  const [playbackDraft, setPlaybackDraft] = useState<number | null>(null)
  const captureShown = showVolumeSlider(isTx, node.telemetry?.capture_volume)
  const playbackShown = showVolumeSlider(isRx, node.telemetry?.playback_volume)
  const capturePct = captureDraft ?? (typeof node.telemetry?.capture_volume === 'number' ? volumePercent(node.telemetry.capture_volume) : 0)
  const playbackPct = playbackDraft ?? (typeof node.telemetry?.playback_volume === 'number' ? volumePercent(node.telemetry.playback_volume) : 0)

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
      capture_volume?: number
      playback_volume?: number
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
    if (captureShown) body.capture_volume = volumeFromPercent(capturePct)
    if (playbackShown) body.playback_volume = volumeFromPercent(playbackPct)
    void run(api.patchNode(node.node_id, body), 'настройки узла').then((ok) => {
      if (ok) onClose()
    })
  }

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const state = presenceWord(node)
  const features = isTx && node.supports_virtual_output

  return (
    <article className="node-sheet">
      <button type="button" className="node-back" onClick={onClose}>К узлам</button>
      <label className="node-name">
        Имя
        <input className="field" aria-label="Имя узла" value={name} onChange={(e) => setName(e.target.value)} />
      </label>
      <p className="hint">Системное имя {node.name}</p>
      <div className={`node-groups${features ? '' : ' is-single'}`}>
        <section className="node-group" aria-label="Сеть">
          <h2>Сеть</h2>
          <div className="node-fact"><span>IP</span><b className="num">{node.ip || '—'}</b></div>
          <div className="node-fact"><span>Связь</span><b>{linkLabel(node.link)}</b></div>
          {isTx && (
            <>
              <label className="node-dscp">
                <span>DSCP</span>
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
            </>
          )}
          {master ? (
            <p className="hint">Этот узел ведёт панель</p>
          ) : (
            <Button type="button" className="node-master" disabled={!node.online} onClick={transfer}>Сделать мастером</Button>
          )}
        </section>
        {features && (
          <section className="node-group" aria-label="На узле">
            <h2>На узле</h2>
            <label className="node-check">
              <input
                type="checkbox"
                aria-label="Создать виртуальный звуковой выход"
                checked={virtualOn}
                onChange={(e) => setVirtualOn(e.target.checked)}
              />
              Виртуальная колонка
            </label>
            <label className="node-check">
              <input
                type="checkbox"
                aria-label="Включить Spotify"
                checked={spotifyOn}
                onChange={(e) => setSpotifyOn(e.target.checked)}
              />
              Spotify
            </label>
            {spotifyOn && (
              <label className="node-name">
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
            )}
            <label className="node-check">
              <input
                type="checkbox"
                aria-label="Включить Sendspin"
                checked={sendspinOn}
                onChange={(e) => setSendspinOn(e.target.checked)}
              />
              Sendspin
            </label>
            <p className="hint">Spotify и Sendspin поднимаются на этом передатчике, только пока включены. Ключ нужен для Spotify.</p>
          </section>
        )}
      </div>
      {(captureShown || playbackShown) && (
        <section className="node-group node-volume-group" aria-label="Громкость">
          <h2>Громкость</h2>
          {captureShown && (
            <label className="node-volume">
              <span>Захват</span>
              <input
                type="range"
                min={0}
                max={150}
                step={1}
                aria-label="Громкость захвата"
                value={capturePct}
                onChange={(e) => setCaptureDraft(Number(e.target.value))}
              />
              <b className="num">{capturePct}%</b>
            </label>
          )}
          {playbackShown && (
            <label className="node-volume">
              <span>Воспроизведение</span>
              <input
                type="range"
                min={0}
                max={150}
                step={1}
                aria-label="Громкость воспроизведения"
                value={playbackPct}
                onChange={(e) => setPlaybackDraft(Number(e.target.value))}
              />
              <b className="num">{playbackPct}%</b>
            </label>
          )}
        </section>
      )}
      {(isTx || isRx) && (
        <div className="node-flow node-flow-edit">
          {isTx && (
            <ChanEditor
              kind="in"
              title="Входы"
              empty="Агент ещё не прислал устройства захвата."
              rows={rows}
              onPatch={patchRow}
              onEnable={(key) => setRows((list) => enableDevice(list, key))}
            />
          )}
          <div className={`node-board-wrap${node.online ? ' is-on' : ''}`}>
            <NodeBoard />
            <div className="node-sheet-ident">
              <div className="node-card-title">
                <span>{name.trim() || node.name}</span>
                {rolesOf(node)}
                {master && <Badge tone="master">Мастер</Badge>}
              </div>
              <p className={`node-state is-${state.tone}`}>{state.text}</p>
            </div>
          </div>
          {isRx && (
            <ChanEditor
              kind="out"
              title="Выходы"
              empty="Агент ещё не прислал устройства воспроизведения."
              rows={outRows}
              onPatch={patchOut}
              onEnable={(key) => setOutRows((list) => enableDevice(list, key))}
            />
          )}
        </div>
      )}
      {isTx && (
        <p className="hint">Отмеченные каналы входят в стереопоток. Виртуальный выход — такой же вход: он подмешивается в зону и раскладывается по выходам колонки в карте зоны.</p>
      )}
      {isRx && (
        <p className="hint">Отмеченные каналы — выходы колонки в карте зоны. Звук играет на устройство этих каналов.</p>
      )}
      <div className="node-sheet-foot">
        <Button onClick={onClose}>Отмена</Button>
        <Button variant="primary" onClick={save}>Сохранить</Button>
      </div>
    </article>
  )
}

function rolesOf(n: NodeView) {
  return n.roles.map((r) => (
    <Badge key={r} tone="role">{r.toUpperCase()}</Badge>
  ))
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

  const actions = (n: NodeView) => {
    const mode = transport(n.online, isPlaying(n.telemetry))
    return (
      <>
        <Button onClick={() => setSettingsId(n.node_id)}>Настройки</Button>
        {mode === 'start' && (
          <Button onClick={() => void run(api.nodeCmd(n.node_id, 'start'), 'старт')}>Старт</Button>
        )}
        {mode === 'stop' && (
          <Button onClick={() => void run(api.nodeCmd(n.node_id, 'stop'), 'стоп')}>Стоп</Button>
        )}
      </>
    )
  }

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
  const presence = nodes.map((n) => ({ online: n.online, playing: isPlaying(n.telemetry) }))

  return (
    <main className="page">
      <div className="page-head">
        <div>
          <h1>Узлы</h1>
          <p className="lede">{online} из {nodes.length} в сети</p>
        </div>
        <div className="toolbar">
          {showStartAll(presence) && (
            <Button variant="primary" onClick={() => void run(fetch('/api/start-all', { method: 'POST' }), 'старт всех')}>
              Старт всех
            </Button>
          )}
          {showStopAll(presence) && (
            <Button onClick={() => void run(fetch('/api/stop-all', { method: 'POST' }), 'стоп всех')}>
              Стоп всех
            </Button>
          )}
          <Button onClick={() => void updateAll()}>Обновить все</Button>
        </div>
      </div>

      {setupBlock(nodes)}

      {settingsNode && (
        <NodeSettings
          key={settingsNode.node_id}
          node={settingsNode}
          master={masterNodeId === settingsNode.node_id}
          transfer={() => void transfer(settingsNode)}
          onClose={() => setSettingsId(null)}
        />
      )}

      {!settingsNode && <div className="node-cards">
        {nodes.map((n) => {
          const inputs = n.roles.includes('tx') ? flowDevices(n.inputs ?? []) : []
          const outputs = n.roles.includes('rx') ? flowDevices(n.outputs ?? []) : []
          const state = presenceWord(n)
          const meters = meterFacts(n)
          const flagged = Boolean(n.virtual_output || n.spotify || n.sendspin)
          return (
            <article key={`${n.node_id}@${n.ip}`} className="node-card">
              <header className="node-card-head">
                <div className="node-card-title">
                  {nodeTitle(n)}
                  {rolesOf(n)}
                  {marks(n)}
                </div>
                <div className="node-card-link">
                  <span className="num">{n.ip || '—'}</span>
                  <span>{linkLabel(n.link)}</span>
                </div>
              </header>
              <div className="node-flow">
                {inputs.length > 0 && <FlowSide kind="in" label="Входы" devices={inputs} />}
                <div className={`node-board-wrap${n.online ? ' is-on' : ''}`}>
                  <NodeBoard />
                  <p className={`node-state is-${state.tone}`}>{state.text}</p>
                </div>
                {outputs.length > 0 && <FlowSide kind="out" label="Выходы" devices={outputs} />}
              </div>
              {flagged && (
                <div className="node-flags">
                  <NodeFlags node={n} />
                </div>
              )}
              <footer className="node-card-foot">
                {meters.length > 0 && (
                  <div className="node-meters">
                    {meters.map((item) => (
                      <span key={item.key} className="node-meter">
                        <span className="node-meter-label">{item.label}</span>
                        <span className="num">{item.value}</span>
                      </span>
                    ))}
                  </div>
                )}
                <div className="node-card-bar">
                  <div className="node-card-meta">
                    {versionCell(n)}
                  </div>
                  <div className="card-actions">{actions(n)}</div>
                </div>
              </footer>
            </article>
          )
        })}
      </div>}
    </main>
  )
}
