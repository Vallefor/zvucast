import assert from 'node:assert/strict'
import test from 'node:test'
import { offer } from './src/update.ts'

test('кнопка только когда вариант известен и версия не равна релизу', () => {
  assert.equal(offer(true, 'aarch64', '0.1.0+dev', '0.1.0', true), 'ready')
  assert.equal(offer(true, 'aarch64', '0.1.0', '0.1.0', true), 'hidden')
  assert.equal(offer(true, '', '0.1.0+dev', '0.1.0', true), 'hidden')
  assert.equal(offer(true, 'aarch64', '0.1.0+dev', '0.1.0', false), 'missing')
})
