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

/// «не вернулся» — срок вышел и узел offline. Тот же срок на живом узле
/// со старой версией — «не установилось»: агент уже прислал отказ или
/// установка не сменила процесс. Заметка `failed` кончает ожидание сразу.
/// `rolledBack` не гасится: страница показывает «откат», пока заметка такая.
export function updateWait(
  online: boolean,
  nodeVersion: string,
  updateNote: string | null,
  wait: { version: string; note: string | null } | null,
  elapsedMs: number,
  limitMs: number,
): { updating: boolean; silent: boolean; rolledBack: boolean; failed: boolean } {
  const rolledBack = updateNote === 'rolled_back'
  const notedFailed = updateNote === 'failed'
  const waiting = wait != null && nodeVersion !== wait.version
  const becameRolledBack = rolledBack && wait?.note !== 'rolled_back'
  const becameFailed = notedFailed && wait?.note !== 'failed'
  const timedOut = waiting && !becameRolledBack && !becameFailed && elapsedMs >= limitMs
  const silent = timedOut && !online
  const failed = !rolledBack && (becameFailed || (timedOut && online) || (notedFailed && wait == null))
  const updating = waiting && !becameRolledBack && !becameFailed && !timedOut
  return { updating, silent, rolledBack, failed }
}
