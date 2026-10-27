import assert from 'node:assert/strict'
import test from 'node:test'
import type { InputView } from './src/api.ts'
import {
  addableDevices,
  chanWord,
  enableDevice,
  flowDevices,
  groupDevices,
  isPlaying,
  showStartAll,
  showStopAll,
  showVolumeSlider,
  transport,
  volumeFromPercent,
  volumePercent,
} from './src/nodesView.ts'

function chan(part: Partial<InputView> & Pick<InputView, 'id' | 'device_key' | 'device_name'>): InputView {
  return {
    index: 0,
    name: part.id,
    enabled: false,
    stream: null,
    ...part,
  }
}

const rows = [
  chan({ id: 'a0', device_key: 'uca', device_name: 'USB Audio CODEC UCA202', index: 0, enabled: true }),
  chan({ id: 'a1', device_key: 'uca', device_name: 'USB Audio CODEC UCA202', index: 1, enabled: true }),
  chan({ id: 'h0', device_key: 'hdmi', device_name: 'sun9i HDMI Stereo', index: 0, enabled: false }),
  chan({ id: 'h1', device_key: 'hdmi', device_name: 'sun9i HDMI Stereo', index: 1, enabled: false }),
  chan({ id: 'l0', device_key: 'line', device_name: 'H3 Audio Codec Line Out', index: 0, enabled: true }),
  chan({ id: 'l1', device_key: 'line', device_name: 'H3 Audio Codec Line Out', index: 1, enabled: false }),
]

test('играет, если играет tx или rx', () => {
  assert.equal(isPlaying(null), false)
  assert.equal(isPlaying({ tx: { state: 'stopped' }, rx: null }), false)
  assert.equal(isPlaying({ tx: { state: 'playing' }, rx: { state: 'stopped' } }), true)
  assert.equal(isPlaying({ tx: null, rx: { state: 'playing' } }), true)
  assert.equal(isPlaying({ tx: { state: 'error' }, rx: { state: 'stopped' } }), false)
})

test('транспорт зависит от сети и воспроизведения', () => {
  assert.equal(transport(false, false), 'none')
  assert.equal(transport(false, true), 'none')
  assert.equal(transport(true, false), 'start')
  assert.equal(transport(true, true), 'stop')
})

test('общие кнопки следуют за узлами', () => {
  assert.equal(showStartAll([]), false)
  assert.equal(showStartAll([{ online: true, playing: true }]), false)
  assert.equal(showStartAll([{ online: false, playing: false }]), false)
  assert.equal(showStartAll([{ online: true, playing: false }]), true)
  assert.equal(showStopAll([{ playing: false }]), false)
  assert.equal(showStopAll([{ playing: true }]), true)
})

test('русское число каналов', () => {
  assert.equal(chanWord(1), 'канал')
  assert.equal(chanWord(2), 'канала')
  assert.equal(chanWord(5), 'каналов')
  assert.equal(chanWord(11), 'каналов')
  assert.equal(chanWord(21), 'канал')
  assert.equal(chanWord(22), 'канала')
})

test('схема и добавление режут устройства по включённым каналам', () => {
  assert.deepEqual(groupDevices(rows).map((g) => g.key), ['uca', 'hdmi', 'line'])
  assert.equal(groupDevices(rows)[2].rows.length, 2)
  assert.deepEqual(flowDevices(rows), [
    { key: 'uca', name: 'USB Audio CODEC UCA202', count: 2 },
    { key: 'line', name: 'H3 Audio Codec Line Out', count: 1 },
  ])
  assert.deepEqual(addableDevices(rows), [{ key: 'hdmi', name: 'sun9i HDMI Stereo' }])
  const enabled = enableDevice(rows, 'hdmi')
  assert.equal(enabled.find((r) => r.id === 'h0')?.enabled, true)
  assert.equal(enabled.find((r) => r.id === 'h1')?.enabled, true)
  assert.equal(rows.find((r) => r.id === 'h0')?.enabled, false)
  const untouched = rows.find((r) => r.id === 'a0')
  assert.equal(enabled.find((r) => r.id === 'a0'), untouched)
  assert.equal(enabled.find((r) => r.id === 'l1')?.enabled, false)
  assert.deepEqual(addableDevices(enabled), [])
  assert.equal(flowDevices(enabled).find((d) => d.key === 'hdmi')?.count, 2)
})

test('громкость в процентах кубической шкалы', () => {
  assert.equal(volumePercent(0.4), 40)
  assert.equal(volumePercent(1), 100)
  assert.equal(volumePercent(1.5), 150)
  assert.equal(volumePercent(2), 150)
  assert.equal(volumeFromPercent(80), 0.8)
  assert.equal(showVolumeSlider(true, 0.4), true)
  assert.equal(showVolumeSlider(true, 0), true)
  assert.equal(showVolumeSlider(false, 0.4), false)
  assert.equal(showVolumeSlider(true, null), false)
  assert.equal(showVolumeSlider(true, undefined), false)
})

test('имя устройства берётся из первой строки ключа', () => {
  const mixed = [
    chan({ id: 'h0', device_key: 'hdmi', device_name: 'sun9i HDMI Stereo', index: 0, enabled: true }),
    chan({ id: 'h1', device_key: 'hdmi', device_name: 'позже другое', index: 1, enabled: true }),
    chan({ id: 'x0', device_key: 'spare', device_name: 'первое имя', index: 0, enabled: false }),
    chan({ id: 'x1', device_key: 'spare', device_name: 'второе имя', index: 1, enabled: false }),
  ]
  assert.equal(groupDevices(mixed).find((g) => g.key === 'hdmi')?.name, 'sun9i HDMI Stereo')
  assert.equal(groupDevices(mixed).find((g) => g.key === 'spare')?.name, 'первое имя')
  assert.equal(flowDevices(mixed).find((d) => d.key === 'hdmi')?.name, 'sun9i HDMI Stereo')
  assert.deepEqual(addableDevices(mixed), [{ key: 'spare', name: 'первое имя' }])
})
