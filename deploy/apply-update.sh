#!/usr/bin/env bash
# usage: deploy/apply-update.sh <file> <sha256> <from> <to>
set -euo pipefail

file=${1:?file}
sum=${2:?sha256}
from=${3:?from}
to=${4:?to}

bin_dir=${ZVUCAST_BIN_DIR:-/usr/local/bin}
lock=${ZVUCAST_LOCK:-/var/lock/zvucast-update}
status=${ZVUCAST_STATUS:-/run/zvucast-apply-update.json}
systemctl=${SYSTEMCTL:-systemctl}
stable=${ZVUCAST_STABLE_SECONDS:-20}
observe=${ZVUCAST_OBSERVE_SECONDS:-45}
poll=${ZVUCAST_POLL_SECONDS:-1}

exec 9>"$lock"
if ! flock -n 9; then
  echo "уже обновляется" >&2
  exit 2
fi

got=$(sha256sum -- "$file" | awk '{print $1}')
if [[ "$got" != "$sum" ]]; then
  exit 1
fi

install -m755 -- "$file" "$bin_dir/zvucast.new"
if [[ -e "$bin_dir/zvucast" ]]; then
  cp -a "$bin_dir/zvucast" "$bin_dir/zvucast.prev"
fi
mv -f "$bin_dir/zvucast.new" "$bin_dir/zvucast"

write_status() {
  printf '{"result":"%s","from":"%s","to":"%s"}\n' "$1" "$from" "$to" > "$status"
}

restart_controller=0
write_status ok
"$systemctl" restart zvucast-agent
if "$systemctl" is-active --quiet zvucast-controller; then
  "$systemctl" restart zvucast-controller
  restart_controller=1
fi

deadline=$((SECONDS + observe))
streak=""
while (( SECONDS < deadline )); do
  if "$systemctl" is-active --quiet zvucast-agent; then
    if [[ -z "$streak" ]]; then
      streak=$SECONDS
    fi
    if (( SECONDS - streak >= stable )); then
      exit 0
    fi
  else
    streak=""
  fi
  sleep "$poll"
done

write_status rolled_back
if [[ -e "$bin_dir/zvucast.prev" ]]; then
  mv -f "$bin_dir/zvucast.prev" "$bin_dir/zvucast"
fi
"$systemctl" restart zvucast-agent
if [[ "$restart_controller" == 1 ]]; then
  "$systemctl" restart zvucast-controller
fi
exit 1
