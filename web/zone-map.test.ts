import assert from 'node:assert/strict'
import test from 'node:test'
import {
  afterMapAttempt,
  applyMapSide,
  canSendZoneMap,
  DEFAULT_ZONE_MAP,
  shouldSendZoneMap,
  speakerOutputRows,
} from './src/zoneMap.ts'

test('вторая сторона не затирает первую, пока полл ещё со стерео', () => {
  const polled = DEFAULT_ZONE_MAP
  const afterLeft = applyMapSide(undefined, polled, 'out_l', null)
  assert.deepEqual(afterLeft, { out_l: null, out_r: 'r' })

  const afterRight = applyMapSide(afterLeft, polled, 'out_r', 'l')
  assert.deepEqual(afterRight, { out_l: null, out_r: 'l' })

  // Так ломалось: вторая правка снова читала полл и теряла левый выход.
  const fromPollAgain = applyMapSide(undefined, polled, 'out_r', 'l')
  assert.deepEqual(fromPollAgain, { out_l: 'l', out_r: 'l' })
  assert.notDeepEqual(afterRight, fromPollAgain)
})

test('карту не шлём, пока колонки нет в серверных members', () => {
  assert.equal(canSendZoneMap(['r1'], 'r1'), true)
  assert.equal(canSendZoneMap([], 'r1'), false)
  assert.equal(canSendZoneMap(['r2'], 'r1'), false)
})

test('сбой отправки не оставляет более новую карту без повтора', () => {
  const sent = { out_l: null, out_r: 'r' as const }
  const newer = { out_l: null, out_r: 'l' as const }
  // HTTP-ошибка и обрыв fetch оба приходят как ok=false, флаг уже снят.
  assert.equal(afterMapAttempt(false, sent, newer), 'resend')
  assert.equal(afterMapAttempt(false, sent, sent), 'drop')
  assert.equal(afterMapAttempt(false, sent, undefined), 'drop')
  assert.equal(afterMapAttempt(true, sent, newer), 'resend')
  assert.equal(afterMapAttempt(true, sent, sent), 'idle')
})

test('откат к старому поллу после принятой правки уходит на сервер', () => {
  const polled = DEFAULT_ZONE_MAP
  const muted = { out_l: null, out_r: 'r' as const }
  // полл стерео, ещё ничего не принято — повтор стерео не шлём
  assert.equal(shouldSendZoneMap(polled, polled, undefined), false)
  // левый заглушили: это ещё не на сервере
  assert.equal(shouldSendZoneMap(muted, polled, undefined), true)
  // PATCH заглушения принят, полл всё ещё стерео. Откат к L совпал со старым
  // поллом, но не с принятой картой — его надо отправить.
  assert.equal(shouldSendZoneMap(polled, polled, muted), true)
  // полл догнал заглушение — стерео снова новая правка
  assert.equal(shouldSendZoneMap(polled, muted, muted), true)
  // полл и принятая карта совпали с pending — слать нечего
  assert.equal(shouldSendZoneMap(muted, muted, muted), false)
})

test('откат к поллу во время сохранения всё равно уходит на сервер', () => {
  const polled = DEFAULT_ZONE_MAP
  const changed = applyMapSide(undefined, polled, 'out_l', null)
  assert.equal(shouldSendZoneMap(changed, polled, undefined), true)

  const reverted = applyMapSide(changed, polled, 'out_l', 'l')
  assert.deepEqual(reverted, polled)
  assert.equal(afterMapAttempt(true, changed, reverted), 'resend')
  // Повторный flush знает, что в пути была другая карта.
  assert.equal(shouldSendZoneMap(reverted, polled, changed), true)
  // Без неподтверждённой другой карты совпадение с поллом — не слать.
  assert.equal(shouldSendZoneMap(reverted, polled, undefined), false)
  assert.equal(shouldSendZoneMap(reverted, polled, reverted), false)
})

test('два левых выхода остаются двумя строками', () => {
  const rows = speakerOutputRows([
    { id: 'jack:0', name: 'jack л', enabled: true, stream: 'l' },
    { id: 'dac:0', name: 'цап л', enabled: true, stream: 'l' },
    { id: 'jack:1', name: 'jack п', enabled: true, stream: 'r' },
    { id: 'dac:1', name: 'цап п', enabled: true, stream: 'r' },
    { id: 'off:0', name: 'выкл', enabled: false, stream: 'l' },
    { id: 'silent:0', name: 'без слота', enabled: true, stream: null },
  ])
  assert.deepEqual(rows.map((r) => r.id), ['jack:0', 'dac:0', 'jack:1', 'dac:1'])
  assert.deepEqual(rows.map((r) => r.side), ['out_l', 'out_l', 'out_r', 'out_r'])
  assert.deepEqual(rows.map((r) => r.label), ['jack л', 'цап л', 'jack п', 'цап п'])
  assert.deepEqual(speakerOutputRows([]).map((r) => r.id), ['default-l', 'default-r'])
})
