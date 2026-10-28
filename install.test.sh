#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

bin=$work/bin
real=$work/real
mkdir -p "$bin" "$real" "$work/usr/local/bin"

link_real() {
  local cmd path
  for cmd in "$@"; do
    path=$(command -v "$cmd")
    ln -s "$path" "$real/$cmd"
  done
}

link_real sha256sum diff grep awk sed tar gzip mktemp basename dirname tr id hostname uname \
  cat cp rm mv chmod mkdir ln readlink head tail sort bash sh

real_install=$(command -v install)
real_tee=$(command -v tee)

cat > "$bin/sudo" <<EOF
#!/bin/bash
echo "\$*" >> "$work/sudo.log"
if [[ "\${1:-}" == "-v" ]]; then
  exit 0
fi
if [[ "\${1:-}" == "-u" ]]; then
  shift 2
fi
while [[ \$# -gt 0 && "\$1" == *=* ]]; do
  export "\$1"
  shift
done
if [[ \$# -eq 0 ]]; then
  exit 0
fi
exec "\$@"
EOF

cat > "$bin/curl" <<EOF
#!/bin/bash
echo "\$*" >> "$work/curl.log"
for arg in "\$@"; do
  case "\$arg" in
    *releases/latest*)
      printf '%s\n' 'HTTP/1.1 302 Found' 'location: https://github.com/Vallefor/zvucast/releases/tag/v0.1.4'
      exit 0
      ;;
  esac
done
url=
for arg in "\$@"; do
  case "\$arg" in
    http://*|https://*) url=\$arg ;;
  esac
done
case "\$url" in
  */SHA256SUMS)
    cat "$work/sums"
    exit 0
    ;;
esac
base=\${url##*/}
case "\$base" in
  zvucast-x86_64|zvucast-aarch64|zvucast-armv7-rpi|zvucast-armv7-opi)
    cat "$work/payload"
    exit 0
    ;;
  soloist_release_*.tar.gz)
    td=\$(mktemp -d)
    printf '%s\n' '#!/bin/sh' 'printf %s\\\\n soloist-test' > "\$td/soloist"
    chmod +x "\$td/soloist"
    tar -czf - -C "\$td" soloist
    rm -rf "\$td"
    exit 0
    ;;
esac
exit 1
EOF

cat > "$bin/apt-get" <<EOF
#!/bin/bash
echo "\$*" >> "$work/apt.log"
if [[ -n "\${APT_EXIT:-}" ]]; then
  exit "\$APT_EXIT"
fi
exit 0
EOF

cat > "$bin/pacman" <<EOF
#!/bin/bash
echo "\$*" >> "$work/pacman.log"
exit 0
EOF

cat > "$bin/systemctl" <<EOF
#!/bin/bash
echo "\$*" >> "$work/systemctl.log"
exit 0
EOF

cat > "$bin/loginctl" <<EOF
#!/bin/bash
echo "\$*" >> "$work/loginctl.log"
exit 0
EOF

cat > "$bin/install" <<EOF
#!/bin/bash
echo "\$*" >> "$work/install.log"
exec $real_install "\$@"
EOF

cat > "$bin/usermod" <<EOF
#!/bin/bash
echo "\$*" >> "$work/usermod.log"
exit 0
EOF

cat > "$bin/tee" <<EOF
#!/bin/bash
echo "\$*" >> "$work/tee.log"
exec $real_tee "\$@"
EOF

chmod +x "$bin/sudo" "$bin/curl" "$bin/apt-get" "$bin/pacman" "$bin/systemctl" \
  "$bin/loginctl" "$bin/install" "$bin/usermod" "$bin/tee"

touch "$work/sudo.log" "$work/curl.log" "$work/apt.log" "$work/pacman.log" \
  "$work/systemctl.log" "$work/loginctl.log" "$work/install.log" "$work/usermod.log" \
  "$work/tee.log"

printf 'ID=debian\n' > "$work/os-debian"
printf 'ID=cachyos\nID_LIKE=arch\n' > "$work/os-cachyos"
printf 'ID=gentoo\n' > "$work/os-gentoo"

printf 'binary' > "$work/payload"
sumdir=$(mktemp -d)
printf 'binary' > "$sumdir/zvucast-x86_64"
(
  cd "$sumdir"
  sha256sum zvucast-x86_64
) > "$work/sums"
rm -rf "$sumdir"

export PATH="$bin:$work/usr/local/bin:$real"

# shellcheck disable=SC1091
source "$root/install.sh"

clean_logs() {
  : > "$work/sudo.log"
  : > "$work/curl.log"
  : > "$work/apt.log"
  : > "$work/pacman.log"
  : > "$work/systemctl.log"
  : > "$work/loginctl.log"
  : > "$work/install.log"
  : > "$work/usermod.log"
  : > "$work/tee.log"
}

run_install() {
  local expect=$1
  shift
  clean_logs
  set +e
  (
    set -euo pipefail
    unset ZVUCAST_SPOTIFY ZVUCAST_CONTROLLER ZVUCAST_DAC ZVUCAST_TEST_NOTTY ZVUCAST_MODEL \
      ZVUCAST_UNAME ZVUCAST_OS_RELEASE ZVUCAST_ROOT ZVUCAST_USER ZVUCAST_UID
    while [[ $# -gt 0 ]]; do
      export "$1"
      shift
    done
    main
  ) >"$work/main.out" 2>"$work/main.err"
  local code=$?
  set -e
  if [[ "$code" -ne "$expect" ]]; then
    echo "main exit $code, expected $expect" >&2
    echo '--- stdout ---' >&2
    cat "$work/main.out" >&2 || true
    echo '--- stderr ---' >&2
    cat "$work/main.err" >&2 || true
    exit 1
  fi
}

clean_logs
ZVUCAST_OS_RELEASE=$work/os-debian
out=$(family_of)
[[ "$out" == apt ]]

ZVUCAST_OS_RELEASE=$work/os-cachyos
out=$(family_of)
[[ "$out" == pacman ]]
[[ "$out" != *apt* ]]

ZVUCAST_OS_RELEASE=$work/os-gentoo
set +e
family_of >/dev/null
code=$?
set -e
[[ "$code" -eq 1 ]]
unset ZVUCAST_OS_RELEASE

clean_logs
ZVUCAST_UNAME=x86_64
unset ZVUCAST_MODEL || true
[[ "$(asset_name)" == zvucast-x86_64 ]]
[[ "$(soloist_arch_name)" == x86_64 ]]

ZVUCAST_UNAME=aarch64
[[ "$(asset_name)" == zvucast-aarch64 ]]
[[ "$(soloist_arch_name)" == arm64 ]]

ZVUCAST_UNAME=armv7l
ZVUCAST_MODEL='Raspberry Pi 2'
[[ "$(asset_name)" == zvucast-armv7-rpi ]]
[[ "$(soloist_arch_name)" == arm32 ]]

ZVUCAST_MODEL='Orange Pi PC Plus'
[[ "$(asset_name)" == zvucast-armv7-opi ]]

ZVUCAST_MODEL='Weird Board'
set +e
err=$(asset_name 2>&1 >/dev/null)
code=$?
set -e
[[ "$code" -eq 1 ]]
[[ "$err" == *armv7l* ]]
[[ "$err" == *Weird\ Board* ]]
[[ ! -s "$work/apt.log" ]]
[[ ! -s "$work/pacman.log" ]]

ZVUCAST_MODEL='Raspberry Pi Orange Pi'
set +e
asset_name >/dev/null 2>&1
code=$?
set -e
[[ "$code" -eq 1 ]]

clean_logs
run_install 1 \
  ZVUCAST_UNAME=armv7l \
  ZVUCAST_MODEL='Weird Board' \
  ZVUCAST_OS_RELEASE="$work/os-debian" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_ROOT="$work" \
  ZVUCAST_USER=audio \
  ZVUCAST_UID=1000
[[ ! -s "$work/apt.log" ]]
[[ ! -s "$work/pacman.log" ]]
unset ZVUCAST_UNAME ZVUCAST_MODEL

base_env=(
  ZVUCAST_UNAME=x86_64
  ZVUCAST_OS_RELEASE="$work/os-debian"
  ZVUCAST_ROOT="$work"
  ZVUCAST_USER=audio
  ZVUCAST_UID=1000
)

run_install 0 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0
[[ "$(cat "$work/usr/local/bin/zvucast")" == "$(cat "$work/payload")" ]]
grep -q pipewire "$work/apt.log"
upd=$(awk '/^update$/{print NR; exit}' "$work/apt.log")
inst=$(awk '/^install /{print NR; exit}' "$work/apt.log")
[[ -n "$upd" && -n "$inst" && "$upd" -lt "$inst" ]]
[[ ! -s "$work/pacman.log" ]]
grep -q 'iw dev wlan0 set power_save off' "$work/sudo.log"
[[ ! -e "$work/boot/firmware/config.txt" ]]
[[ ! -e "$work/boot/config.txt" ]]
grep -q 'модуль воспроизведения: никакого' "$work/main.out"
grep -q disable "$work/systemctl.log"
grep -q zvucast-controller "$work/systemctl.log"
grep -q 'controller = "ws://127.0.0.1:8080/ws"' "$work/etc/zvucast.toml"
grep -q 'playback_backend = "pipewire"' "$work/etc/zvucast.toml"
grep -q 'Zvucast' "$work/main.out"
grep -q 'https://github.com/Vallefor/zvucast/releases/latest' "$work/curl.log"
if grep -q 'api.github.com' "$work/curl.log" "$root/install.sh"; then
  echo 'api.github.com was used' >&2
  exit 1
fi
diff -q "$root/deploy/pipewire/99-zvucast-quantum.conf" \
  "$work/etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf"
diff -q "$root/deploy/wireplumber/99-zvucast-latency.conf" \
  "$work/etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf"
diff -q "$root/deploy/zvucast-controller.service" \
  "$work/etc/systemd/system/zvucast-controller.service"
grep -q 'XDG_RUNTIME_DIR=/run/user/1000' "$work/etc/systemd/system/zvucast-agent.service"
diff -q "$root/deploy/zvucast-agent.service" \
  "$work/etc/systemd/system/zvucast-agent.service"

printf '\nname = "keep"\n' >> "$work/etc/zvucast.toml"
before=$(sha256sum "$work/etc/zvucast.toml" | awk '{print $1}')
run_install 0 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0
after=$(sha256sum "$work/etc/zvucast.toml" | awk '{print $1}')
[[ "$before" == "$after" ]]
grep -q 'name = "keep"' "$work/etc/zvucast.toml"

before=$(sha256sum "$work/usr/local/bin/zvucast" | awk '{print $1}')
printf 'tampered' > "$work/payload"
run_install 1 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0
after=$(sha256sum "$work/usr/local/bin/zvucast" | awk '{print $1}')
[[ "$before" == "$after" ]]
printf 'binary' > "$work/payload"

run_install 0 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=1 \
  ZVUCAST_CONTROLLER=0
grep -q soloist_release_x86_64 "$work/curl.log"
grep -q disable "$work/systemctl.log"

run_install 2 \
  ZVUCAST_UNAME=x86_64 \
  ZVUCAST_OS_RELEASE="$work/os-debian" \
  ZVUCAST_TEST_NOTTY=1 \
  ZVUCAST_ROOT="$work" \
  ZVUCAST_USER=audio \
  ZVUCAST_UID=1000
grep -q ZVUCAST_SPOTIFY "$work/main.err"
grep -q ZVUCAST_CONTROLLER "$work/main.err"
[[ ! -s "$work/apt.log" ]]

run_install 0 \
  ZVUCAST_UNAME=x86_64 \
  ZVUCAST_OS_RELEASE="$work/os-cachyos" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_ROOT="$work" \
  ZVUCAST_USER=audio \
  ZVUCAST_UID=1000
grep -q pipewire "$work/pacman.log"
sy=$(awk '/^-Sy /{print NR; exit}' "$work/pacman.log")
inst=$(awk '/^-S /{print NR; exit}' "$work/pacman.log")
[[ -n "$sy" && -n "$inst" && "$sy" -lt "$inst" ]]
[[ ! -s "$work/apt.log" ]]
grep -q 'iw dev wlan0 set power_save off' "$work/sudo.log"

clean_logs
set +e
(
  unset ZVUCAST_SPOTIFY ZVUCAST_CONTROLLER
  export ZVUCAST_TEST_NOTTY=1 \
    ZVUCAST_UNAME=x86_64 \
    ZVUCAST_OS_RELEASE="$work/os-debian" \
    ZVUCAST_ROOT="$work" \
    ZVUCAST_USER=audio \
    ZVUCAST_UID=1000
  cat "$root/install.sh" | bash
) >"$work/main.out" 2>"$work/main.err"
code=$?
set -e
[[ "$code" -eq 2 ]]
grep -q ZVUCAST_SPOTIFY "$work/main.err"
grep -q ZVUCAST_CONTROLLER "$work/main.err"
[[ ! -s "$work/apt.log" ]]

run_install 1 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  APT_EXIT=100

[[ "$(resolve_dac '')" == none ]]
[[ "$(resolve_dac 1)" == none ]]
[[ "$(resolve_dac 2)" == pcm5102 ]]
[[ "$(resolve_dac pcm5102)" == pcm5102 ]]
set +e
resolve_dac usb >/dev/null
code=$?
set -e
[[ "$code" -eq 1 ]]

mkdir -p "$work/boot/firmware"
printf '%s\n' '[pi4]' 'dtoverlay=vc4-kms-v3d' '[all]' 'dtparam=audio=on' \
  > "$work/boot/firmware/config.txt"
run_install 0 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_DAC=pcm5102
grep -q 'Оверлей hifiberry-dac записан' "$work/main.out"
awk '
  $0 == "[all]" { in_all = 1; next }
  in_all && $0 == "dtparam=i2s=on" { got_i2s = 1 }
  in_all && got_i2s && $0 == "dtoverlay=hifiberry-dac" { got_dac = 1 }
  in_all && got_dac && $0 == "dtparam=audio=on" { ok = 1 }
  END { exit ok ? 0 : 1 }
' "$work/boot/firmware/config.txt"
[[ "$(grep -c '^dtoverlay=hifiberry-dac$' "$work/boot/firmware/config.txt")" -eq 1 ]]
[[ "$(grep -c '^dtparam=i2s=on$' "$work/boot/firmware/config.txt")" -eq 1 ]]
run_install 0 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_DAC=2
grep -q 'Оверлей hifiberry-dac уже в config.txt' "$work/main.out"
[[ "$(grep -c '^dtoverlay=hifiberry-dac$' "$work/boot/firmware/config.txt")" -eq 1 ]]

old=$work/oldroot
mkdir -p "$old/boot"
printf '%s\n' 'arm_64bit=1' > "$old/boot/config.txt"
run_install 0 \
  ZVUCAST_UNAME=x86_64 \
  ZVUCAST_OS_RELEASE="$work/os-debian" \
  ZVUCAST_ROOT="$old" \
  ZVUCAST_USER=audio \
  ZVUCAST_UID=1000 \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_DAC=pcm5102
grep -q '^arm_64bit=1$' "$old/boot/config.txt"
grep -q '^\[all\]$' "$old/boot/config.txt"
grep -q '^dtparam=i2s=on$' "$old/boot/config.txt"
grep -q '^dtoverlay=hifiberry-dac$' "$old/boot/config.txt"
[[ ! -e "$old/boot/firmware/config.txt" ]]

empty=$work/emptyroot
mkdir -p "$empty"
run_install 1 \
  ZVUCAST_UNAME=x86_64 \
  ZVUCAST_OS_RELEASE="$work/os-debian" \
  ZVUCAST_ROOT="$empty" \
  ZVUCAST_USER=audio \
  ZVUCAST_UID=1000 \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_DAC=pcm5102
[[ ! -s "$work/apt.log" ]]
grep -q 'config.txt' "$work/main.err"

run_install 2 \
  "${base_env[@]}" \
  ZVUCAST_SPOTIFY=0 \
  ZVUCAST_CONTROLLER=0 \
  ZVUCAST_DAC=usb
[[ ! -s "$work/apt.log" ]]
grep -q ZVUCAST_DAC "$work/main.err"

echo ok
