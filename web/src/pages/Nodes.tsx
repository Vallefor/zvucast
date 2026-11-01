import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { api, InputView, MasterInfo, NodeView } from '../api'
import { channelCount, type MessageKey, type Translate } from '../i18n'
import { useI18n } from '../locale'
import { addableDevices, deviceVolume, enableDevice, flowDevices, groupDevices, isPlaying, settleCommands, showStartAll, showStopAll, showVolumeSlider, transport, volumeFromPercent, volumePercent, type PendingCommand } from '../nodesView'
import { offer, updateWait } from '../update'
import { Badge, Button, useRun, useUi } from '../ui'

const POLL_MS = 2000
/// Свой POST: «не вернулся», если за это время узел так и остался offline.
/// Живой узел на старой версии к этому сроку — «не установилось».
const UPDATE_WAIT_MS = 90_000
/// Старт/стоп: крутилка на кнопке, пока телеметрия не подтвердит команду.
const CMD_WAIT_MS = 20_000
/// Попап настроек. Короче обычного «плавного» появления.
const SETTINGS_POP_MS = 250

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

function linkLabel(link: NodeView['link'], t: Translate): string {
  if (link === 'wifi') return 'Wi‑Fi'
  if (link === 'wired') return t('link.wired')
  return '—'
}

/// Нет сети важнее звука: офлайн не бывает «играет», даже если телеметрия старая.
function presenceWord(n: NodeView, t: Translate): { text: string; tone: 'faint' | 'good' | 'quiet' } {
  if (!n.online) return { text: t('presence.offline'), tone: 'faint' }
  if (isPlaying(n.telemetry)) return { text: t('presence.playing'), tone: 'good' }
  return { text: t('presence.idle'), tone: 'quiet' }
}

/// Живые метры. Пустое значение не занимает место прочерком.
function meterFacts(n: NodeView, t: Translate): { key: string; label: string; value: string }[] {
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
    items.push({ key: 'queue', label: t('meter.queue'), value: String(rx.queue_ms) })
  }
  if (rx && rx.dropped > 0) {
    items.push({ key: 'drop', label: t('meter.drops'), value: String(rx.dropped) })
  }
  if (n.online && tm && Math.abs(tm.ptp_offset_ns) <= 1e12) {
    items.push({ key: 'ptp', label: 'PTP', value: (tm.ptp_offset_ns / 1000).toFixed(1) })
  }
  if (tm) items.push({ key: 'up', label: t('meter.uptime'), value: fmtUptime(tm.uptime_s) })
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
  const { t } = useI18n()
  return (
    <>
      {node.virtual_output && (
        <span className="node-flag node-flag-virtual" role="img" aria-label={t('flag.virtual')}>
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
  const { lang } = useI18n()
  return (
    <div className={`node-flow-side node-flow-${kind}`}>
      <div className="node-flow-label">{label}</div>
      <div className="node-bus">
        {devices.map((device) => (
          <div key={device.key} className="flow-port">
            {kind === 'out' && <span className="flow-stub" aria-hidden="true" />}
            <div className="flow-chip">
              <span className="flow-chip-name" title={device.name}>{device.name}</span>
              <span className="flow-count" aria-label={channelCount(lang, device.count)}>{device.count}</span>
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

const AMP_LINES = Array.from({ length: 26 }, (_, i) => i + 2)

function ampChoice(pin: number | null | undefined): string {
  if (pin == null || pin < AMP_LINES[0] || pin > AMP_LINES[AMP_LINES.length - 1]) return ''
  return String(pin)
}

const SETUP_KEY: Record<string, MessageKey> = {
  pipewire: 'setup.pipewire',
  wireplumber: 'setup.wireplumber',
  'pipewire-pulse': 'setup.pipewirePulse',
  'pw-cat': 'setup.pwCat',
  gstreamer: 'setup.gstreamer',
  linger: 'setup.linger',
  'pipewire-socket': 'setup.socket',
  'pipewire-quantum': 'setup.quantum',
  'rt-limits': 'setup.rt',
  'wifi-powersave': 'setup.wifi',
  controller: 'setup.controller',
  soloist: 'setup.soloist',
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

function setupBlock(nodes: NodeView[], t: Translate) {
  const groups = nodes.filter((n) => n.online).flatMap((n) => {
    const rows: { key: string; title: string; detail: string; command: string }[] = []
    if (n.setup == null) {
      rows.push({ key: 'old', title: t('setup.old'), detail: '', command: '' })
      if (n.spotify) {
        rows.push({ key: 'soloist-old', title: t('setup.soloistOld'), detail: '', command: '' })
      }
    } else {
      n.setup.forEach((issue, i) => {
        const key = SETUP_KEY[issue.id]
        rows.push({
          key: `${issue.id}-${i}`,
          title: key ? t(key) : issue.id,
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
      <h2>{t('setup.missing')}</h2>
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
  volumes,
  drafts,
  onVolume,
  onPatch,
  onEnable,
}: {
  kind: 'in' | 'out'
  title: string
  empty: string
  rows: InputView[]
  volumes: { device: string; volume: number; card?: string; class?: string }[] | null | undefined
  drafts: Record<string, number>
  onVolume: (key: string, percent: number) => void
  onPatch: (id: string, next: Partial<InputView>) => void
  onEnable: (key: string) => void
}) {
  const { t } = useI18n()
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
              {deviceVolume(volumes, group.key, group.name, kind) != null && (
                <label className="chan-volume">
                  <input
                    type="range"
                    min={0}
                    max={150}
                    step={1}
                    aria-label={t('chan.volume', { name: group.name })}
                    value={drafts[`${kind}:${group.key}`] ?? volumePercent(deviceVolume(volumes, group.key, group.name, kind)?.volume ?? 0)}
                    onChange={(e) => onVolume(`${kind}:${group.key}`, Number(e.target.value))}
                  />
                  <b className="num">{drafts[`${kind}:${group.key}`] ?? volumePercent(deviceVolume(volumes, group.key, group.name, kind)?.volume ?? 0)}%</b>
                </label>
              )}
              {group.rows.map((row) => (
                <div key={row.id} className="chan-row">
                  <input
                    type="checkbox"
                    checked={row.enabled}
                    aria-label={t('chan.use', { name: group.name, n: row.index + 1 })}
                    onChange={(e) => onPatch(row.id, { enabled: e.target.checked })}
                  />
                  <input
                    className="field"
                    aria-label={t('chan.name', { name: group.name, n: row.index + 1 })}
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
            {t('chan.add')}
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
  const { t } = useI18n()
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
  const [volumeDraft, setVolumeDraft] = useState<Record<string, number>>({})
  const [captureDraft, setCaptureDraft] = useState<number | null>(null)
  const [playbackDraft, setPlaybackDraft] = useState<number | null>(null)
  const [ampLine, setAmpLine] = useState(() => ampChoice(node.amp_gpio))
  const [saving, setSaving] = useState(false)
  const savingRef = useRef(false)
  const perDevice = (node.telemetry?.device_volumes?.length ?? 0) > 0
  const captureShown = !perDevice && showVolumeSlider(isTx, node.telemetry?.capture_volume)
  const playbackShown = !perDevice && showVolumeSlider(isRx, node.telemetry?.playback_volume)
  const capturePct = captureDraft ?? (typeof node.telemetry?.capture_volume === 'number' ? volumePercent(node.telemetry.capture_volume) : 0)
  const playbackPct = playbackDraft ?? (typeof node.telemetry?.playback_volume === 'number' ? volumePercent(node.telemetry.playback_volume) : 0)

  const patchRow = (id: string, next: Partial<InputView>) => {
    setRows((list) => list.map((row) => (row.id === id ? { ...row, ...next } : row)))
  }
  const patchOut = (id: string, next: Partial<InputView>) => {
    setOutRows((list) => list.map((row) => (row.id === id ? { ...row, ...next } : row)))
  }

  const save = () => {
    if (savingRef.current) return
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
      device_volumes?: { device: string; volume: number; card?: string; class?: string }[]
      amp_gpio?: number | null
    } = { display_name: name.trim() || null }
    if (isTx) {
      const n = Number(dscp)
      if (!Number.isInteger(n) || n < 0 || n > 63) {
        toast(t('nodes.dscpRange'))
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
      body.amp_gpio = ampLine === '' ? null : Number(ampLine)
    }
    if (captureShown) body.capture_volume = volumeFromPercent(capturePct)
    if (playbackShown) body.playback_volume = volumeFromPercent(playbackPct)
    if (perDevice) {
      const device_volumes: { device: string; volume: number; card?: string; class?: string }[] = []
      const take = (kind: 'in' | 'out', list: InputView[]) => {
        for (const group of groupDevices(list)) {
          if (!group.rows.some((row) => row.enabled)) continue
          const found = deviceVolume(node.telemetry?.device_volumes, group.key, group.name, kind)
          if (!found) continue
          const pct = volumeDraft[`${kind}:${group.key}`] ?? volumePercent(found.volume)
          device_volumes.push({ device: found.device, volume: volumeFromPercent(pct) })
        }
      }
      if (isTx) take('in', rows)
      if (isRx) take('out', outRows)
      if (device_volumes.length > 0) body.device_volumes = device_volumes
    }
    savingRef.current = true
    setSaving(true)
    void run(api.patchNode(node.node_id, body), t('nodes.saveWhat')).then((ok) => {
      if (ok) onClose()
      else {
        savingRef.current = false
        setSaving(false)
      }
    })
  }

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const state = presenceWord(node, t)
  const features = isTx && node.supports_virtual_output

  return (
    <article className="node-sheet">
      <button type="button" className="node-back" onClick={onClose}>{t('nodes.back')}</button>
      <label className="node-name">
        {t('nodes.name')}
        <input className="field" aria-label={t('nodes.nameAria')} value={name} onChange={(e) => setName(e.target.value)} />
      </label>
      <p className="hint">{t('nodes.systemName', { name: node.name })}</p>
      <div className={`node-groups${features ? '' : ' is-single'}`}>
        <section className="node-group" aria-label={t('nodes.network')}>
          <h2>{t('nodes.network')}</h2>
          <div className="node-fact"><span>IP</span><b className="num">{node.ip || '—'}</b></div>
          <div className="node-fact"><span>{t('nodes.link')}</span><b>{linkLabel(node.link, t)}</b></div>
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
              <div className="seg" role="group" aria-label={t('nodes.dscpPresets')}>
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
            <p className="hint">{t('nodes.thisMaster')}</p>
          ) : (
            <Button type="button" className="node-master" disabled={!node.online} onClick={transfer}>{t('nodes.makeMaster')}</Button>
          )}
        </section>
        {features && (
          <section className="node-group" aria-label={t('nodes.onNode')}>
            <h2>{t('nodes.onNode')}</h2>
            <label className="node-check">
              <input
                type="checkbox"
                aria-label={t('nodes.virtualOut')}
                checked={virtualOn}
                onChange={(e) => setVirtualOn(e.target.checked)}
              />
              {t('nodes.virtual')}
            </label>
            <label className="node-check">
              <input
                type="checkbox"
                aria-label={t('nodes.spotifyOn')}
                checked={spotifyOn}
                onChange={(e) => setSpotifyOn(e.target.checked)}
              />
              Spotify
            </label>
            {spotifyOn && (
              <label className="node-name">
                {t('nodes.spotifyKey')}
                <input
                  className="field"
                  type="password"
                  autoComplete="off"
                  aria-label={t('nodes.spotifyKey')}
                  value={soloistKey}
                  onChange={(e) => setSoloistKey(e.target.value)}
                />
              </label>
            )}
            <label className="node-check">
              <input
                type="checkbox"
                aria-label={t('nodes.sendspinOn')}
                checked={sendspinOn}
                onChange={(e) => setSendspinOn(e.target.checked)}
              />
              Sendspin
            </label>
            <p className="hint">{t('nodes.feedsHint')}</p>
          </section>
        )}
      </div>
      {isRx && (
        <section className="node-group node-volume-group" aria-label={t('nodes.amp')}>
          <h2>{t('nodes.amp')}</h2>
          <label className="node-dscp">
            <span>{t('nodes.ampGpio')}</span>
            <select
              className="select"
              aria-label={t('nodes.ampGpio')}
              value={ampLine}
              onChange={(e) => setAmpLine(e.target.value)}
            >
              <option value="">{t('nodes.ampOff')}</option>
              {AMP_LINES.map((n) => (
                <option key={n} value={n}>{t('nodes.ampPin', { n })}</option>
              ))}
            </select>
          </label>
          <p className="hint">{t('nodes.ampHint')}</p>
        </section>
      )}
      {(captureShown || playbackShown) && (
        <section className="node-group node-volume-group" aria-label={t('nodes.volume')}>
          <h2>{t('nodes.volume')}</h2>
          {captureShown && (
            <label className="node-volume">
              <span>{t('nodes.capture')}</span>
              <input
                type="range"
                min={0}
                max={150}
                step={1}
                aria-label={t('nodes.captureVolume')}
                value={capturePct}
                onChange={(e) => setCaptureDraft(Number(e.target.value))}
              />
              <b className="num">{capturePct}%</b>
            </label>
          )}
          {playbackShown && (
            <label className="node-volume">
              <span>{t('nodes.playback')}</span>
              <input
                type="range"
                min={0}
                max={150}
                step={1}
                aria-label={t('nodes.playbackVolume')}
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
              title={t('nodes.inputs')}
              empty={t('nodes.inputsEmpty')}
              rows={rows}
              volumes={perDevice ? node.telemetry?.device_volumes : []}
              drafts={volumeDraft}
              onVolume={(key, percent) => setVolumeDraft((prev) => ({ ...prev, [key]: percent }))}
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
                {master && <Badge tone="master">{t('nodes.master')}</Badge>}
              </div>
              <p className={`node-state is-${state.tone}`}>{state.text}</p>
            </div>
          </div>
          {isRx && (
            <ChanEditor
              kind="out"
              title={t('nodes.outputs')}
              empty={t('nodes.outputsEmpty')}
              rows={outRows}
              volumes={perDevice ? node.telemetry?.device_volumes : []}
              drafts={volumeDraft}
              onVolume={(key, percent) => setVolumeDraft((prev) => ({ ...prev, [key]: percent }))}
              onPatch={patchOut}
              onEnable={(key) => setOutRows((list) => enableDevice(list, key))}
            />
          )}
        </div>
      )}
      {isTx && (
        <p className="hint">{t('nodes.inputHint')}</p>
      )}
      {isRx && (
        <p className="hint">{t('nodes.outputHint')}</p>
      )}
      <div className="node-sheet-foot">
        <Button onClick={onClose}>{t('common.cancel')}</Button>
        <Button variant="primary" busy={saving} onClick={save}>{t('common.save')}</Button>
      </div>
    </article>
  )
}

function settingsPopMs(): number {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return 0
  return SETTINGS_POP_MS
}

function SettingsPop({
  onClose,
  children,
}: {
  onClose: () => void
  children: (close: () => void) => ReactNode
}) {
  const { t } = useI18n()
  const [phase, setPhase] = useState<'in' | 'out'>('in')
  const onCloseRef = useRef(onClose)
  onCloseRef.current = onClose
  const close = useCallback(() => {
    setPhase((current) => (current === 'out' ? current : 'out'))
  }, [])

  useEffect(() => {
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.body.style.overflow = prev
    }
  }, [])

  useEffect(() => {
    if (phase !== 'out') return
    const timer = window.setTimeout(() => onCloseRef.current(), settingsPopMs())
    return () => window.clearTimeout(timer)
  }, [phase])

  return (
    <div className={`node-settings-pop is-${phase}`}>
      <button type="button" className="node-settings-pop-dim" aria-label={t('nodes.closeSettings')} onClick={close} />
      <div className="node-settings-pop-panel" role="dialog" aria-modal="true" aria-label={t('nodes.settingsTitle')}>
        {children(close)}
      </div>
    </div>
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
  const { t } = useI18n()
  const tRef = useRef(t)
  tRef.current = t
  const [nodes, setNodes] = useState<NodeView[]>([])
  const [master, setMaster] = useState<MasterInfo | null>(null)
  const [release, setRelease] = useState<ReleaseView | null>(null)
  const [pending, setPending] = useState<Record<string, PendingUpdate>>({})
  const [now, setNow] = useState(() => Date.now())
  const [movedTo, setMovedTo] = useState<string | null>(null)
  const [settingsId, setSettingsId] = useState<string | null>(null)
  const [cmd, setCmd] = useState<Record<string, PendingCommand>>({})
  const [posting, setPosting] = useState<Record<string, true>>({})
  const [checking, setChecking] = useState(false)
  const cmdRef = useRef(cmd)
  const postingRef = useRef(posting)
  const toldLate = useRef(new Set<string>())
  cmdRef.current = cmd
  postingRef.current = posting

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
      setCmd((prev) => {
        const settled = settleCommands(
          prev,
          ns.map((n) => ({ node_id: n.node_id, online: n.online, playing: isPlaying(n.telemetry) })),
          Date.now(),
          CMD_WAIT_MS,
        )
        for (const item of settled.late) {
          const key = `${item.id}:${item.at}`
          if (toldLate.current.has(key)) continue
          toldLate.current.add(key)
          const text = item.cmd === 'start' ? tRef.current('nodes.startMissed') : tRef.current('nodes.stopMissed')
          queueMicrotask(() => toast(text))
        }
        return settled.pending
      })
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
      title: t('nodes.transferTitle'),
      body: t('nodes.transferBody', { name: node.display_name ?? node.name }),
      confirmLabel: t('nodes.transferConfirm'),
    })
    if (!ok) return
    const r = await api.transfer(node.node_id)
    if (!r.ok) {
      toast(`${t('nodes.transferWhat')}: HTTP ${r.status}`)
      return
    }
    const body = await r.json() as { url?: string }
    if (body.url) {
      setMovedTo(body.url)
      window.location.assign(body.url)
      return
    }
    toast(t('nodes.transferNoUrl'))
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
    if (postingRef.current[n.node_id] || pending[n.node_id]) return
    postingRef.current = { ...postingRef.current, [n.node_id]: true }
    setPosting(postingRef.current)
    void run(api.updateNode(n.node_id), t('nodes.updateWhat')).then(async (ok) => {
      if (ok) await pinInstalled([n.node_id])
      const next = { ...postingRef.current }
      delete next[n.node_id]
      postingRef.current = next
      setPosting(next)
    })
  }

  const runTransport = (id: string, command: 'start' | 'stop') => {
    if (cmdRef.current[id]) return
    const wait: PendingCommand = { id, cmd: command, at: Date.now() }
    cmdRef.current = { ...cmdRef.current, [id]: wait }
    setCmd((prev) => (prev[id] ? prev : { ...prev, [id]: wait }))
    void run(api.nodeCmd(id, command), command === 'start' ? t('nodes.startWhat') : t('nodes.stopWhat')).then((ok) => {
      if (ok) return
      const next = { ...cmdRef.current }
      delete next[id]
      cmdRef.current = next
      setCmd((prev) => {
        if (!prev[id]) return prev
        const cleared = { ...prev }
        delete cleared[id]
        return cleared
      })
    })
  }

  const checkRelease = () => {
    if (checking) return
    setChecking(true)
    void (async () => {
      try {
        const rel = await api.releaseFresh()
        setRelease(rel)
      } catch (e) {
        console.error(e)
        toast(t('toast.noLink', { what: t('nodes.checkWhat') }))
      } finally {
        setChecking(false)
      }
    })()
  }

  const updateAll = async () => {
    let response: Response
    try {
      response = await api.updateAll()
    } catch (e) {
      console.error(e)
      toast(t('toast.noLink', { what: t('nodes.updateWhat') }))
      return
    }
    if (!response.ok) {
      toast(`${t('nodes.updateWhat')}: HTTP ${response.status}`)
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
        {phase.updating && <span>{t('nodes.updating')}</span>}
        {phase.silent && <span>{t('nodes.silent')}</span>}
        {phase.failed && !phase.updating && <span>{t('nodes.failed')}</span>}
        {phase.rolledBack && <span>{t('nodes.rolled')}</span>}
        {kind === 'missing' && <span>{t('nodes.noAsset')}</span>}
        {(kind === 'ready' || phase.updating) && (
          <Button
            className={compact ? 'btn-compact' : undefined}
            busy={phase.updating || Boolean(posting[n.node_id])}
            onClick={() => updateOne(n)}
          >
            {t('nodes.update')}
          </Button>
        )}
      </div>
    )
  }

  const actions = (n: NodeView) => {
    const mode = transport(n.online, isPlaying(n.telemetry))
    return (
      <>
        <Button onClick={() => setSettingsId(n.node_id)}>{t('nodes.settings')}</Button>
        {mode === 'start' && (
          <Button busy={cmd[n.node_id]?.cmd === 'start'} onClick={() => runTransport(n.node_id, 'start')}>{t('nodes.start')}</Button>
        )}
        {mode === 'stop' && (
          <Button busy={cmd[n.node_id]?.cmd === 'stop'} onClick={() => runTransport(n.node_id, 'stop')}>{t('nodes.stop')}</Button>
        )}
      </>
    )
  }

  const marks = (n: NodeView) => (
    <>
      {n.dup && <Badge tone="dup">{t('nodes.duplicate')}</Badge>}
      {masterNodeId === n.node_id && <Badge tone="master">{t('nodes.master')}</Badge>}
    </>
  )

  if (movedTo) {
    return (
      <main className="page">
        <h1>{t('nodes.movedTitle')}</h1>
        <p className="lede">{t('nodes.movedLead')}</p>
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
          <h1>{t('nodes.title')}</h1>
          <p className="lede">{t('nodes.online', { online, total: nodes.length })}</p>
        </div>
        <div className="toolbar">
          {showStartAll(presence) && (
            <Button variant="primary" onClick={() => void run(fetch('/api/start-all', { method: 'POST' }), t('nodes.startAllWhat'))}>
              {t('nodes.startAll')}
            </Button>
          )}
          {showStopAll(presence) && (
            <Button onClick={() => void run(fetch('/api/stop-all', { method: 'POST' }), t('nodes.stopAllWhat'))}>
              {t('nodes.stopAll')}
            </Button>
          )}
          <Button busy={checking} onClick={checkRelease}>{t('nodes.checkUpdates')}</Button>
          {release && <span className="num">{t('nodes.releaseFound', { version: release.version })}</span>}
          <Button onClick={() => void updateAll()}>{t('nodes.updateAll')}</Button>
        </div>
      </div>

      {setupBlock(nodes, t)}

      {settingsNode && (
        <SettingsPop onClose={() => setSettingsId(null)}>
          {(close) => (
            <NodeSettings
              key={settingsNode.node_id}
              node={settingsNode}
              master={masterNodeId === settingsNode.node_id}
              transfer={() => void transfer(settingsNode)}
              onClose={close}
            />
          )}
        </SettingsPop>
      )}

      <div className="node-cards">
        {nodes.map((n) => {
          const inputs = n.roles.includes('tx') ? flowDevices(n.inputs ?? []) : []
          const outputs = n.roles.includes('rx') ? flowDevices(n.outputs ?? []) : []
          const state = presenceWord(n, t)
          const meters = meterFacts(n, t)
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
                  <span>{linkLabel(n.link, t)}</span>
                </div>
              </header>
              <div className="node-flow">
                {inputs.length > 0 && <FlowSide kind="in" label={t('nodes.inputs')} devices={inputs} />}
                <div className={`node-board-wrap${n.online ? ' is-on' : ''}`}>
                  <NodeBoard />
                  <p className={`node-state is-${state.tone}`}>{state.text}</p>
                </div>
                {outputs.length > 0 && <FlowSide kind="out" label={t('nodes.outputs')} devices={outputs} />}
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
      </div>
    </main>
  )
}
