#!/usr/bin/env bash
# usage: deploy/deploy.sh <ssh-user@host> <binary> <config.toml> [--with-controller]
set -euo pipefail
DEST="${1:?ssh user@host}"
BIN="${2:?path to zvucast binary}"
CFG="${3:?path to zvucast.toml}"
WITH_CONTROLLER=0
if [[ "${4:-}" == "--with-controller" ]]; then
  WITH_CONTROLLER=1
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scp "$BIN" "$DEST:/tmp/zvucast"
scp "$CFG" "$DEST:/tmp/zvucast.toml"
scp "$DIR/zvucast-agent.service" "$DIR/zvucast-controller.service" "$DEST:/tmp/"
scp -r "$DIR/pipewire" "$DIR/wireplumber" "$DEST:/tmp/zvucast-audio"
ssh -t "$DEST" "sudo install -m755 /tmp/zvucast /usr/local/bin/zvucast \
  && sudo install -m644 /tmp/zvucast.toml /etc/zvucast.toml \
  && sudo install -m644 /tmp/zvucast-agent.service /etc/systemd/system/zvucast-agent.service \
  && sudo install -m644 /tmp/zvucast-controller.service /etc/systemd/system/zvucast-controller.service \
  && sudo mkdir -p /etc/pipewire/pipewire.conf.d /etc/wireplumber/wireplumber.conf.d \
  && sudo install -m644 /tmp/zvucast-audio/pipewire/99-zvucast-quantum.conf /etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf \
  && sudo install -m644 /tmp/zvucast-audio/wireplumber/99-zvucast-latency.conf /etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf \
  && sudo systemctl daemon-reload \
  && sudo systemctl enable --now zvucast-agent \
  && sudo systemctl restart zvucast-agent"
if [[ "$WITH_CONTROLLER" == 1 ]]; then
  ssh -t "$DEST" "sudo systemctl enable --now zvucast-controller && sudo systemctl restart zvucast-controller"
fi
