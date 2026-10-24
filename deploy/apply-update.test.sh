#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
script="$root/deploy/apply-update.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

printf 'old' > "$work/zvucast"
printf 'new' > "$work/payload"
sum=$(sha256sum "$work/payload" | awk '{print $1}')

cat > "$work/systemctl" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$SYSTEMCTL_LOG"
if [[ "$1" == "restart" && -f "$ZVUCAST_STATUS" ]]; then
  echo "status-before-restart $(cat "$ZVUCAST_STATUS")" >> "$SYSTEMCTL_LOG"
fi
if [[ "$1" == "is-active" && "$3" == "zvucast-agent" && -f "$FAIL_AGENT" ]]; then
  restarts=$(grep -c 'restart zvucast-agent' "$SYSTEMCTL_LOG" || true)
  if [[ "${restarts:-0}" -lt 2 ]]; then
    exit 1
  fi
fi
exit 0
EOF
chmod +x "$work/systemctl"

run() {
  SYSTEMCTL_LOG="$work/sys.log" FAIL_AGENT="${1:-}" \
  PATH="$work:$PATH" SYSTEMCTL="$work/systemctl" \
  ZVUCAST_BIN_DIR="$work" ZVUCAST_LOCK="$work/lock" ZVUCAST_STATUS="$work/status.json" \
  ZVUCAST_STABLE_SECONDS=1 ZVUCAST_OBSERVE_SECONDS=3 ZVUCAST_POLL_SECONDS=0 \
  bash "$script" "$work/payload" "$sum" "0.1.0+dev" "0.1.0"
}

: > "$work/sys.log"
run
grep -q '"result":"ok"' "$work/status.json"
grep -q 'status-before-restart.*"result":"ok"' "$work/sys.log"
test "$(cat "$work/zvucast")" = new
test "$(cat "$work/zvucast.prev")" = old

printf 'old' > "$work/zvucast"
rm -f "$work/zvucast.prev" "$work/status.json"
: > "$work/sys.log"
touch "$work/fail"
set +e
run "$work/fail"
code=$?
set -e
test "$code" -ne 0
grep -q '"result":"rolled_back"' "$work/status.json"
test "$(cat "$work/zvucast")" = old
echo ok
