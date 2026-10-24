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
