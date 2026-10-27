// Типы зеркалят Rust-структуры controller/registry (serde-имена как в Rust,
// camelCase не используем).

export type ChanSel = 'l' | 'r'

export interface ChannelMap {
  out_l: ChanSel | null
  out_r: ChanSel | null
}

export interface RoleTx {
  state: string
  rms_l: number
  rms_r: number
}

export interface RoleRx {
  state: string
  dropped: number
  rendered: number
  queue_ms: number
}

export interface Telemetry {
  tx: RoleTx | null
  rx: RoleRx | null
  ptp_offset_ns: number
  uptime_s: number
  capture_volume?: number | null
  playback_volume?: number | null
  device_volumes?: { device: string; volume: number; card?: string; class?: string }[]
}

export interface InputView {
  id: string
  device_key: string
  device_name: string
  index: number
  name: string
  enabled: boolean
  /** `l` / `r`, если канал включён и входит в стереопоток. */
  stream: 'l' | 'r' | null
}

export interface SetupIssue {
  id: string
  command: string
  detail?: string
}

export interface NodeView {
  node_id: string
  name: string
  display_name: string | null
  channel_map: ChannelMap | null
  dup: boolean
  roles: string[]
  version: string
  target: string
  update_note: string | null
  capture_device: string | null
  playback_device: string | null
  ip: string
  online: boolean
  /** `wifi` / `wired`. Старый агент поле не шлёт. */
  link?: 'wifi' | 'wired' | null
  telemetry: Telemetry | null
  dscp?: number
  inputs?: InputView[]
  outputs?: InputView[]
  virtual_output?: boolean
  supports_virtual_output?: boolean
  soloist_key?: string | null
  spotify?: boolean
  sendspin?: boolean
  setup?: SetupIssue[] | null
}

export interface MasterInfo {
  addr: string
  node_id: string | null
}

/// Зона как её видит фронт. `stolen` — node_id членов активной зоны, чей
/// владелец в раскладке — другая зона (больший seq); у неактивных пуст.
export interface ZoneView {
  id: string
  name: string
  tx: string
  sources?: string[]
  members: string[]
  active: boolean
  seq: number
  silence_timeout_s: number
  silence_threshold_db: number
  jb_ms: number
  silenced: boolean
  stolen: string[]
  maps: Record<string, ChannelMap>
  source_maps?: Record<string, Record<string, ChannelMap>>
}

export const api = {
  nodes: (): Promise<NodeView[]> => fetch('/api/nodes').then((r) => r.json()),
  release: (): Promise<{ version: string; assets: Record<string, { url: string; sha256: string }> }> =>
    fetch('/api/release').then((r) => r.json()),
  updateNode: (id: string) => fetch(`/api/nodes/${id}/update`, { method: 'POST' }),
  updateAll: () => fetch('/api/update-all', { method: 'POST' }),
  nodeCmd: (id: string, cmd: 'start' | 'stop') =>
    fetch(`/api/nodes/${id}/${cmd}`, { method: 'POST' }),
  // display_name: null — сброс к системному имени (серийник из node_id).
  patchNode: (id: string, body: {
    display_name?: string | null
    dscp?: number
    inputs?: { id: string; name?: string; enabled?: boolean }[]
    outputs?: { id: string; name?: string; enabled?: boolean }[]
    virtual_output?: boolean
    soloist_key?: string
    spotify?: boolean
    sendspin?: boolean
    capture_volume?: number
    playback_volume?: number
    device_volumes?: { device: string; volume: number; card?: string; class?: string }[]
  }) =>
    fetch(`/api/nodes/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  dscp: (): Promise<{ dscp: number }> => fetch('/api/dscp').then((r) => r.json()),
  setDscp: (dscp: number) =>
    fetch('/api/dscp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dscp }),
    }),
  master: (): Promise<MasterInfo> => fetch('/api/master').then((r) => r.json()),
  transfer: (node_id: string) =>
    fetch('/api/master/transfer', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ node_id }),
    }),
  zones: (): Promise<{ zones: ZoneView[] }> => fetch('/api/zones').then((r) => r.json()),
  createZone: (body: { name: string; tx?: string; sources?: string[]; members: string[] }) =>
    fetch('/api/zones', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  // Ответы PATCH/пуск/стоп — ZoneView; фронту достаточно статуса, следующий
  // poll всё равно перечитает views.
  patchZone: (id: string, body: {
    name?: string
    tx?: string
    sources?: string[]
    members?: string[]
    silence_timeout_s?: number
    silence_threshold_db?: number
    jb_ms?: number
    maps?: Record<string, ChannelMap>
    source_maps?: Record<string, Record<string, ChannelMap>>
  }) =>
    fetch(`/api/zones/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  deleteZone: (id: string) => fetch(`/api/zones/${id}`, { method: 'DELETE' }),
  zoneCmd: (id: string, cmd: 'start' | 'stop') =>
    fetch(`/api/zones/${id}/${cmd}`, { method: 'POST' }),
}
