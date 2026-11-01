import assert from 'node:assert/strict'
import test from 'node:test'
import { catalogs, channelCount, storedLang, translate, type MessageKey } from './src/i18n.ts'

test('нет сохранённого языка — английский', () => {
  assert.equal(storedLang(null), 'en')
  assert.equal(storedLang(''), 'en')
  assert.equal(storedLang('en'), 'en')
  assert.equal(storedLang('fr'), 'en')
  assert.equal(storedLang('ru'), 'ru')
})

test('у английского и русского один и тот же набор фраз', () => {
  const enKeys = Object.keys(catalogs.en).sort()
  const ruKeys = Object.keys(catalogs.ru).sort()
  assert.deepEqual(enKeys, ruKeys)
  for (const key of enKeys) {
    const id = key as MessageKey
    assert.notEqual(catalogs.en[id], '')
    assert.notEqual(catalogs.ru[id], '')
  }
})

test('подстановка числа и имени', () => {
  assert.equal(translate('en', 'nodes.online', { online: 1, total: 3 }), '1 of 3 online')
  assert.equal(translate('ru', 'nodes.online', { online: 1, total: 3 }), '1 из 3 в сети')
  assert.equal(translate('en', 'toast.noLink', { what: 'update' }), 'update: no connection')
  assert.equal(translate('ru', 'toast.noLink', { what: 'обновление' }), 'обновление: нет связи')
  assert.equal(translate('en', 'zones.deleteBody', { name: 'Kitchen' }), 'Zone “Kitchen” will go away, along with its speakers and channel map.')
})

test('число каналов на схеме', () => {
  assert.equal(channelCount('en', 0), '0 channels')
  assert.equal(channelCount('en', 1), '1 channel')
  assert.equal(channelCount('en', 2), '2 channels')
  assert.equal(channelCount('ru', 1), '1 канал')
  assert.equal(channelCount('ru', 2), '2 канала')
  assert.equal(channelCount('ru', 5), '5 каналов')
  assert.equal(channelCount('ru', 11), '11 каналов')
  assert.equal(channelCount('ru', 21), '21 канал')
  assert.equal(channelCount('ru', 22), '22 канала')
})
