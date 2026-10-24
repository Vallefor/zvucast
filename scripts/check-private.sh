#!/usr/bin/env bash
# Запрещённые подстроки приходят снаружи. В этом файле их нет.
set -euo pipefail
cd "$(dirname "$0")/.."
patterns=()
if [[ -n "${ZVUCAST_PRIVATE_PATTERNS:-}" ]]; then
  IFS=',' read -ra extra <<< "$ZVUCAST_PRIVATE_PATTERNS"
  patterns+=("${extra[@]}")
fi
if [[ -f scripts/private-patterns.local ]]; then
  while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    patterns+=("$line")
  done < scripts/private-patterns.local
fi
if [[ ${#patterns[@]} -eq 0 ]]; then
  echo "check-private: пустой список" >&2
  exit 1
fi
# Заголовки ключей собраны из кусков, чтобы сам скрипт не содержал целую строку.
patterns+=("BEGIN OPENSSH PRIVATE ""KEY" "BEGIN RSA PRIVATE ""KEY")
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  mapfile -d '' files < <(git ls-files -z)
else
  mapfile -d '' files < <(find . -type f -not -path './target/*' -not -path './web/node_modules/*' -print0)
fi
fail=0
for p in "${patterns[@]}"; do
  [[ -z "$p" ]] && continue
  if grep -F -n -e "$p" "${files[@]}" >/dev/null; then
    echo "запрещено: $p" >&2
    grep -F -n -e "$p" "${files[@]}" >&2 || true
    fail=1
  fi
done
exit "$fail"
