const TARGETS = ['x86_64', 'aarch64', 'armv7-rpi', 'armv7-opi']

/// Те же ветки, что `update_offer`: offline, пустой или чужой target и
/// совпадение версии прячут кнопку. Нет файла релиза — отдельный исход.
export function offer(
  online: boolean,
  target: string,
  nodeVersion: string,
  releaseVersion: string,
  assetPresent: boolean,
): 'hidden' | 'ready' | 'missing' {
  if (!online || !TARGETS.includes(target) || nodeVersion === releaseVersion) {
    return 'hidden'
  }
  if (assetPresent) return 'ready'
  return 'missing'
}

/// «не вернулся» — 90 с прошли и узел offline. Пока он online, это
/// «обновляется», пока версия не совпадёт или заметка не станет `rolled_back`.
/// `rolledBack` не гасится: страница показывает «откат», пока заметка такая.
export function updateWait(
  online: boolean,
  nodeVersion: string,
  updateNote: string | null,
  wait: { version: string; note: string | null } | null,
  elapsedMs: number,
  limitMs: number,
): { updating: boolean; silent: boolean; rolledBack: boolean } {
  const rolledBack = updateNote === 'rolled_back'
  const waiting = wait != null && nodeVersion !== wait.version
  const becameRolledBack = rolledBack && wait?.note !== 'rolled_back'
  const silent = waiting && !becameRolledBack && !online && elapsedMs >= limitMs
  const updating = waiting && !becameRolledBack && !silent
  return { updating, silent, rolledBack }
}
