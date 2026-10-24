import assert from 'node:assert/strict'
import test from 'node:test'
import { offer, updateWait } from './src/update.ts'

test('кнопка только когда вариант известен и версия не равна релизу', () => {
  assert.equal(offer(true, 'aarch64', '0.1.0+dev', '0.1.0', true), 'ready')
  assert.equal(offer(true, 'aarch64', '0.1.0', '0.1.0', true), 'hidden')
  assert.equal(offer(true, '', '0.1.0+dev', '0.1.0', true), 'hidden')
  assert.equal(offer(true, 'aarch64', '0.1.0+dev', '0.1.0', false), 'missing')
})

test('не вернулся только когда узел замолчал', () => {
  const wait = { version: '0.1.0', note: null }
  const limit = 90_000
  assert.deepEqual(updateWait(true, '0.1.0+dev', null, wait, limit, limit), {
    updating: true,
    silent: false,
    rolledBack: false,
  })
  assert.deepEqual(updateWait(false, '0.1.0+dev', null, wait, limit, limit), {
    updating: false,
    silent: true,
    rolledBack: false,
  })
  assert.deepEqual(updateWait(false, '0.1.0+dev', null, wait, limit - 1, limit), {
    updating: true,
    silent: false,
    rolledBack: false,
  })
  assert.deepEqual(updateWait(true, '0.1.0', null, wait, limit, limit), {
    updating: false,
    silent: false,
    rolledBack: false,
  })
  assert.deepEqual(updateWait(false, '0.1.0+dev', 'rolled_back', wait, limit, limit), {
    updating: false,
    silent: false,
    rolledBack: true,
  })
  assert.deepEqual(
    updateWait(true, '0.1.0+dev', 'rolled_back', { version: '0.1.0', note: 'rolled_back' }, limit, limit),
    { updating: true, silent: false, rolledBack: true },
  )
  assert.deepEqual(updateWait(true, '0.1.0+dev', 'rolled_back', null, limit, limit), {
    updating: false,
    silent: false,
    rolledBack: true,
  })
})
