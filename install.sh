#!/usr/bin/env bash
# Установка Zvucast на плату. Тексты дропов и юнитов вшиты: клон репозитория не нужен.
set -euo pipefail

cleanup_dirs=()

root_path() {
  printf '%s%s' "${ZVUCAST_ROOT:-}" "$1"
}

new_tmp() {
  local d
  d=$(mktemp -d)
  cleanup_dirs+=("$d")
  printf '%s' "$d"
}

as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

write_file() {
  local dest=$1
  as_root mkdir -p -- "$(dirname -- "$dest")"
  as_root tee -- "$dest" >/dev/null
}

os_field() {
  local file=$1 key=$2 line name value open close
  [[ -r "$file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line=${line%$'\r'}
    [[ "$line" == *=* ]] || continue
    name=${line%%=*}
    value=${line#*=}
    name=${name#"${name%%[![:space:]]*}"}
    name=${name%"${name##*[![:space:]]}"}
    [[ "$name" == "$key" ]] || continue
    value=${value#"${value%%[![:space:]]*}"}
    value=${value%"${value##*[![:space:]]}"}
    if [[ ${#value} -ge 2 ]]; then
      open=${value:0:1}
      close=${value: -1}
      if [[ ( "$open" == '"' && "$close" == '"' ) || ( "$open" == "'" && "$close" == "'" ) ]]; then
        value=${value:1:${#value}-2}
      fi
    fi
    printf '%s' "$value"
    return 0
  done < "$file"
}

family_of() {
  local file=${ZVUCAST_OS_RELEASE:-/etc/os-release}
  [[ -r "$file" ]] || return 1
  local id like apt=0 pacman=0 word noglob=0
  id=$(os_field "$file" ID)
  like=$(os_field "$file" ID_LIKE)
  case "$id" in
    debian|ubuntu|raspbian) apt=1 ;;
  esac
  case "$id" in
    arch|cachyos) pacman=1 ;;
  esac
  if [[ $- == *f* ]]; then
    noglob=1
  fi
  set -f
  for word in $like; do
    case "$word" in
      debian|ubuntu) apt=1 ;;
      arch) pacman=1 ;;
    esac
  done
  if [[ "$noglob" -eq 0 ]]; then
    set +f
  fi
  if [[ "$apt" -eq 1 && "$pacman" -eq 0 ]]; then
    printf 'apt\n'
    return 0
  fi
  if [[ "$apt" -eq 0 && "$pacman" -eq 1 ]]; then
    printf 'pacman\n'
    return 0
  fi
  return 1
}

read_model() {
  if [[ -n "${ZVUCAST_MODEL+x}" ]]; then
    printf '%s' "$ZVUCAST_MODEL"
    return 0
  fi
  if [[ -r /proc/device-tree/model ]]; then
    tr -d '\000' < /proc/device-tree/model
  fi
}

asset_name() {
  local machine model lower rpi=0 opi=0
  machine=${ZVUCAST_UNAME:-$(uname -m)}
  model=$(read_model)
  case "$machine" in
    x86_64)
      printf 'zvucast-x86_64\n'
      return 0
      ;;
    aarch64)
      printf 'zvucast-aarch64\n'
      return 0
      ;;
    armv7l)
      lower=$(printf '%s' "$model" | tr '[:upper:]' '[:lower:]')
      if [[ "$lower" == *"raspberry pi"* ]]; then
        rpi=1
      fi
      if [[ "$lower" == *"orange pi"* ]]; then
        opi=1
      fi
      if [[ "$rpi" -eq 1 && "$opi" -eq 0 ]]; then
        printf 'zvucast-armv7-rpi\n'
        return 0
      fi
      if [[ "$opi" -eq 1 && "$rpi" -eq 0 ]]; then
        printf 'zvucast-armv7-opi\n'
        return 0
      fi
      ;;
  esac
  printf '%s %s\n' "$machine" "$model" >&2
  return 1
}

soloist_arch_name() {
  local machine=${ZVUCAST_UNAME:-$(uname -m)}
  case "$machine" in
    x86_64) printf 'x86_64\n' ;;
    aarch64) printf 'arm64\n' ;;
    armv7l) printf 'arm32\n' ;;
    *)
      printf '%s\n' "$machine" >&2
      return 1
      ;;
  esac
}

soloist_command() {
  local arch=$1
  printf '%s\n' "tmp=\$(mktemp -d) && curl --fail --location --max-time 90 -o \"\$tmp/soloist.tar.gz\" \"https://soloist-builds.spotifycdn.com/soloist_release_${arch}.tar.gz\" && tar -xzf \"\$tmp/soloist.tar.gz\" -C \"\$tmp\" && sudo install -m 755 \"\$tmp/soloist\" /usr/local/bin/soloist && soloist --version"
}

resolve_user() {
  local current
  current=$(id -u)
  if [[ "$current" -eq 0 ]]; then
    if [[ -z "${SUDO_USER:-}" || "$SUDO_USER" == root ]]; then
      printf '%s\n' 'запусти от обычного пользователя' >&2
      exit 1
    fi
    AUDIO_USER=$SUDO_USER
    AUDIO_UID=${ZVUCAST_UID:-$(id -u "$SUDO_USER")}
  else
    AUDIO_USER=${ZVUCAST_USER:-$(id -un)}
    AUDIO_UID=${ZVUCAST_UID:-$current}
  fi
}

valid_bit() {
  [[ "$1" == 0 || "$1" == 1 ]]
}

tty_ok() {
  [[ -z "${ZVUCAST_TEST_NOTTY:-}" && -r /dev/tty ]]
}

ask() {
  local _var=$1 prompt=$2 default=$3 reply
  while true; do
    if ! read -r -p "$prompt " reply </dev/tty; then
      printf '%s\n' ZVUCAST_SPOTIFY ZVUCAST_CONTROLLER >&2
      exit 2
    fi
    if [[ -z "$reply" ]]; then
      reply=$default
    fi
    case "$reply" in
      y|Y|yes|YES|Yes|д|Д|да|Да|ДА)
        printf -v "$_var" '%s' yes
        return 0
        ;;
      n|N|no|NO|No|н|Н|нет|Нет|НЕТ)
        printf -v "$_var" '%s' no
        return 0
        ;;
    esac
    printf '%s\n' 'ответь y/yes/д/да или n/no/н/нет' >/dev/tty
  done
}

# Пусто, 0 и 1 — пункт меню «никакого». 2 и pcm5102 — плата.
resolve_dac() {
  case "$1" in
    ''|0|1|none|NONE|None)
      printf 'none\n'
      ;;
    2|pcm5102|PCM5102)
      printf 'pcm5102\n'
      ;;
    *)
      return 1
      ;;
  esac
}

ask_playback() {
  local reply
  while true; do
    printf '%s\n' \
      'Какой модуль для воспроизведения звука подключен?' \
      '  1. Никакого' \
      '  2. Плата PCM5102' >/dev/tty
    if ! read -r -p '[1] ' reply </dev/tty; then
      printf '%s\n' ZVUCAST_SPOTIFY ZVUCAST_CONTROLLER ZVUCAST_DAC >&2
      exit 2
    fi
    if dac=$(resolve_dac "$reply"); then
      return 0
    fi
    printf '%s\n' 'ответь 1 или 2' >/dev/tty
  done
}

print_plan() {
  local ctrl solo board
  if [[ "$controller" == 1 ]]; then
    ctrl=да
  else
    ctrl=нет
  fi
  if [[ "$spotify" == 1 ]]; then
    solo=да
  else
    solo=нет
  fi
  if [[ "$dac" == pcm5102 ]]; then
    board='плата PCM5102'
  else
    board=никакого
  fi
  printf '%s\n' \
    'Будет установлено:' \
    "семейство пакетов: ${family}" \
    'PipeWire, WirePlumber, бинарник zvucast, агент' \
    "модуль воспроизведения: ${board}" \
    "контроллер: ${ctrl}" \
    "Soloist: ${solo}"
}

# ZVUCAST_DAC необязателен. Нет переменной — «никакого».
# Без терминала по-прежнему обязательны только ZVUCAST_SPOTIFY и ZVUCAST_CONTROLLER.
choose_options() {
  local solo_answer ctrl_answer confirm_answer
  if [[ -n "${ZVUCAST_DAC+x}" ]]; then
    if ! dac=$(resolve_dac "$ZVUCAST_DAC"); then
      printf '%s\n' ZVUCAST_DAC >&2
      exit 2
    fi
  fi
  if valid_bit "${ZVUCAST_SPOTIFY:-}" && valid_bit "${ZVUCAST_CONTROLLER:-}"; then
    if [[ -z "${ZVUCAST_DAC+x}" ]]; then
      dac=none
    fi
    spotify=$ZVUCAST_SPOTIFY
    controller=$ZVUCAST_CONTROLLER
    print_plan
    return 0
  fi
  if ! tty_ok; then
    printf '%s\n' ZVUCAST_SPOTIFY ZVUCAST_CONTROLLER >&2
    exit 2
  fi
  if [[ -z "${ZVUCAST_DAC+x}" ]]; then
    ask_playback
  fi
  ask solo_answer 'Ставить Soloist? [n]' n
  ask ctrl_answer 'Эта машина — контроллер? [n]' n
  if [[ "$solo_answer" == yes ]]; then
    spotify=1
  else
    spotify=0
  fi
  if [[ "$ctrl_answer" == yes ]]; then
    controller=1
  else
    controller=0
  fi
  print_plan
  ask confirm_answer 'Продолжить? [y]' y
  if [[ "$confirm_answer" == no ]]; then
    exit 0
  fi
}

pick_iface() {
  local net path name val had_nullglob=0
  local -a wired=() wifi=() entries=()
  net=$(root_path /sys/class/net)
  if shopt -q nullglob; then
    had_nullglob=1
  fi
  shopt -s nullglob
  entries=("$net"/*)
  if [[ "$had_nullglob" -eq 0 ]]; then
    shopt -u nullglob
  fi
  for path in "${entries[@]}"; do
    name=$(basename -- "$path")
    if [[ -d "$path/wireless" ]]; then
      wifi+=("$name")
    fi
    if [[ "$name" == lo || "$name" == veth* || "$name" == tun* ]]; then
      continue
    fi
    if [[ -d "$path/wireless" || -d "$path/bridge" ]]; then
      continue
    fi
    if [[ -f "$path/carrier" ]]; then
      val=$(tr -d '[:space:]' < "$path/carrier")
      if [[ "$val" == 1 ]]; then
        wired+=("$name")
      fi
    fi
  done
  if [[ ${#wired[@]} -gt 0 ]]; then
    printf '%s\n' "${wired[0]}"
  elif [[ ${#wifi[@]} -gt 0 ]]; then
    printf '%s\n' "${wifi[0]}"
  else
    printf '%s\n' eth0
  fi
}

install_packages() {
  if [[ "$family" == apt ]]; then
    as_root apt-get update || exit 1
    as_root apt-get install -y \
      pipewire pipewire-bin pipewire-pulse wireplumber \
      gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad gstreamer1.0-alsa gstreamer1.0-pipewire \
      alsa-utils curl ca-certificates iw dbus-user-session rtkit python3 \
      || exit 1
  else
    as_root pacman -Sy --noconfirm || exit 1
    as_root pacman -S --needed --noconfirm \
      pipewire pipewire-alsa pipewire-pulse wireplumber \
      gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad \
      gst-plugin-pipewire alsa-utils curl ca-certificates iw rtkit python \
      || exit 1
  fi
}

trim_line() {
  local value=$1
  value=${value%$'\r'}
  value=${value#"${value%%[![:space:]]*}"}
  value=${value%"${value##*[![:space:]]}"}
  printf '%s' "$value"
}

# Bookworm: /boot/firmware/config.txt. Старее — /boot/config.txt.
boot_config_path() {
  local fw boot firmware bootdir
  fw=$(root_path /boot/firmware/config.txt)
  boot=$(root_path /boot/config.txt)
  firmware=$(root_path /boot/firmware)
  bootdir=$(root_path /boot)
  if [[ -f "$fw" || -L "$fw" ]]; then
    printf '%s\n' "$fw"
    return 0
  fi
  if [[ -f "$boot" || -L "$boot" ]]; then
    printf '%s\n' "$boot"
    return 0
  fi
  if [[ -d "$firmware" ]]; then
    printf '%s\n' "$fw"
    return 0
  fi
  if [[ -d "$bootdir" ]]; then
    printf '%s\n' "$boot"
    return 0
  fi
  return 1
}

configure_overlay() {
  [[ "$dac" == pcm5102 ]] || return 0
  local path src dest line trimmed need_i2s=1 need_dac=1 found_all=0 inserted=0
  if ! path=$(boot_config_path); then
    printf '%s\n' 'нет config.txt: оверлей PCM5102 некуда записать' >&2
    exit 1
  fi
  src=$(new_tmp)/config.txt
  if [[ -f "$path" ]]; then
    cat "$path" > "$src"
  else
    : > "$src"
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    trimmed=$(trim_line "$line")
    case "$trimmed" in
      'dtparam=i2s=on') need_i2s=0 ;;
      'dtoverlay=hifiberry-dac') need_dac=0 ;;
      '[all]') found_all=1 ;;
    esac
  done < "$src"
  if [[ "$need_i2s" -eq 0 && "$need_dac" -eq 0 ]]; then
    printf '%s\n' 'Оверлей hifiberry-dac уже в config.txt. ЦАП появится после перезагрузки.'
    return 0
  fi
  dest=$(new_tmp)/config.new
  : > "$dest"
  if [[ "$found_all" -eq 1 ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      line=${line%$'\r'}
      printf '%s\n' "$line" >> "$dest"
      trimmed=$(trim_line "$line")
      if [[ "$inserted" -eq 0 && "$trimmed" == '[all]' ]]; then
        if [[ "$need_i2s" -eq 1 ]]; then
          printf '%s\n' 'dtparam=i2s=on' >> "$dest"
        fi
        if [[ "$need_dac" -eq 1 ]]; then
          printf '%s\n' 'dtoverlay=hifiberry-dac' >> "$dest"
        fi
        inserted=1
      fi
    done < "$src"
  else
    while IFS= read -r line || [[ -n "$line" ]]; do
      printf '%s\n' "${line%$'\r'}" >> "$dest"
    done < "$src"
    printf '%s\n' '[all]' >> "$dest"
    if [[ "$need_i2s" -eq 1 ]]; then
      printf '%s\n' 'dtparam=i2s=on' >> "$dest"
    fi
    if [[ "$need_dac" -eq 1 ]]; then
      printf '%s\n' 'dtoverlay=hifiberry-dac' >> "$dest"
    fi
  fi
  write_file "$path" < "$dest"
  printf '%s\n' 'Оверлей hifiberry-dac записан в config.txt. ЦАП появится после перезагрузки.'
}

install_drops() {
  local quantum latency
  quantum=$(root_path /etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf)
  latency=$(root_path /etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf)
  write_file "$quantum" <<'ZVUCAST_QUANTUM'
# Часы машины не запираем. Квант 240 кадров, 5 мс, стоит только на
# своём null-sink (node.force-quantum). Граф карты живёт на периоде
# этой карты. Частота одна: 48 кГц.
context.properties = {
    default.clock.rate = 48000
    default.clock.allowed-rates = [ 48000 ]
}
ZVUCAST_QUANTUM
  write_file "$latency" <<'ZVUCAST_LATENCY'
# Граф машины не заперт в один квант. Период карты — её кольцо, не цикл
# всего PipeWire. Квант захвата, 240 кадров, задаёт свой синк, не этот
# файл. Запас колонки — целое число периодов HiFiBerry в кольце карты.
# HiFiBerry ест период 256. 20 периодов — кольцо 5120 кадров, около
# 107 мс. headroom в файле 1920; агент при старте плеера ставит запас
# сам (40 мс → 2048 кадров, 8 квантов): пропуск пробуждения доигрывает его.
# USB Audio CODEC (PCM2902, full speed) отдаёт звук пакетами по 1 мс,
# 48 кадров. Период 128 — не целое число пакетов, в динамике сплошной
# хрип. Кодеку оставляем batch и период 960 (20 мс, ровно 20 пакетов).
# Batch режет период пополам: в железе период 480 и кольцо
# period-num×480. Четыре периода — это 40 мс на всё кольцо, запас
# около 40 мс упирается в край и плата снова вставляет тишину.
# 16 периодов дают кольцо около 160 мс, этот запас в нём помещается.
# bcm2835 Headphones — ШИМ через mailbox VideoCore. Период 128 и
# disable-batch оставляют в кольце доли кванта, прошивка забирает
# звук пачками: delay уходит в минус, sink копит xrun. В наушниках
# это сплошной хрип. Выход не для боя — буфер как у медленной карты.
monitor.alsa.rules = [
  {
    matches = [
      { node.name = "~alsa_input.*" }
      { node.name = "~alsa_output.*" }
    ]
    actions = {
      update-props = {
        api.alsa.period-size = 128
        api.alsa.period-num = 4
        api.alsa.headroom = 0
        api.alsa.disable-batch = true
      }
    }
  }
  {
    matches = [
      {
        node.name = "~alsa_output.*"
        alsa.card_name = "bcm2835 Headphones"
      }
    ]
    actions = {
      update-props = {
        api.alsa.period-size = 1024
        api.alsa.period-num = 4
        api.alsa.headroom = 2048
        api.alsa.disable-batch = false
      }
    }
  }
  {
    matches = [
      {
        node.name = "~alsa_output.*"
        alsa.card_name = "snd_rpi_hifiberry_dac"
      }
    ]
    actions = {
      update-props = {
        api.alsa.period-size = 256
        api.alsa.period-num = 20
        api.alsa.headroom = 1920
        api.alsa.disable-batch = true
      }
    }
  }
  {
    matches = [
      {
        node.name = "~alsa_output.*"
        alsa.card_name = "USB Audio CODEC"
      }
    ]
    actions = {
      update-props = {
        api.alsa.period-size = 960
        api.alsa.period-num = 16
        api.alsa.headroom = 1920
        api.alsa.disable-batch = false
      }
    }
  }
]
ZVUCAST_LATENCY
}

install_limits() {
  local limits
  limits=$(root_path /etc/security/limits.conf)
  as_root mkdir -p -- "$(dirname -- "$limits")"
  if ! grep -q -F '@audio - rtprio 95' "$limits" 2>/dev/null; then
    printf '%s\n' '@audio - rtprio 95' '@audio - memlock unlimited' | as_root tee -a -- "$limits" >/dev/null
  fi
  as_root usermod -aG audio "$AUDIO_USER"
}

install_session() {
  as_root loginctl enable-linger "$AUDIO_USER"
  as_root systemctl start "user@${AUDIO_UID}.service"
  # enable --now не перечитывает уже запущенный граф. Рестарт нужен,
  # чтобы дропы часов и периода карты вступили сразу.
  sudo -u "$AUDIO_USER" \
    "XDG_RUNTIME_DIR=/run/user/${AUDIO_UID}" \
    "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${AUDIO_UID}/bus" \
    systemctl --user enable --now pipewire.service pipewire-pulse.service wireplumber.service
  sudo -u "$AUDIO_USER" \
    "XDG_RUNTIME_DIR=/run/user/${AUDIO_UID}" \
    "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${AUDIO_UID}/bus" \
    systemctl --user restart pipewire.service pipewire-pulse.service wireplumber.service
}

install_wifi() {
  local conf unit line name type
  # Юнит выключит power save на следующей загрузке. Сейчас wlan0 ещё в нём,
  # и PTP с RTP на этом интерфейсе рвутся до рестарта.
  as_root iw dev wlan0 set power_save off || true
  conf=$(root_path /etc/NetworkManager/conf.d/wifi-powersave-off.conf)
  write_file "$conf" <<'EOF'
[connection]
wifi.powersave = 2
EOF
  if command -v nmcli >/dev/null 2>&1; then
    while IFS= read -r line; do
      [[ -n "$line" ]] || continue
      type=${line##*:}
      name=${line%:*}
      name=${name//\\:/:}
      if [[ "$type" == 802-11-wireless ]]; then
        as_root nmcli connection modify "$name" 802-11-wireless.powersave 2
      fi
    done < <(nmcli -t -f NAME,TYPE connection show || true)
  fi
  unit=$(root_path /etc/systemd/system/zvucast-wifi-powersave.service)
  write_file "$unit" <<'EOF'
[Unit]
Description=Zvucast Wi-Fi power save off
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'for path in /sys/class/net/wlan*; do [ -e "$path" ] || continue; name=${path##*/}; iw dev "$name" set power_save off || true; done'

[Install]
WantedBy=multi-user.target
EOF
  as_root systemctl enable --now zvucast-wifi-powersave.service
}

release_version() {
  local headers line key location prefix version
  if ! headers=$(curl -sSI -H 'User-Agent: zvucast' --max-time 30 https://github.com/Vallefor/zvucast/releases/latest); then
    return 1
  fi
  location=
  while IFS= read -r line; do
    line=${line%$'\r'}
    key=${line%%:*}
    key=${key,,}
    if [[ "$key" == location ]]; then
      location=${line#*:}
      location=${location#"${location%%[![:space:]]*}"}
      location=${location%"${location##*[![:space:]]}"}
      break
    fi
  done <<<"$headers"
  prefix='https://github.com/Vallefor/zvucast/releases/tag/v'
  if [[ "$location" != "$prefix"* ]]; then
    printf '%s\n' 'нет тега' >&2
    return 1
  fi
  version=${location#"$prefix"}
  if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s\n' 'нет тега' >&2
    return 1
  fi
  printf '%s\n' "$version"
}

install_binary() {
  local version tmp sums_url asset_url line dest
  if ! version=$(release_version); then
    exit 1
  fi
  tmp=$(new_tmp)
  sums_url="https://github.com/Vallefor/zvucast/releases/download/v${version}/SHA256SUMS"
  asset_url="https://github.com/Vallefor/zvucast/releases/download/v${version}/${asset}"
  if ! curl -fL --max-time 90 "$sums_url" > "$tmp/SHA256SUMS"; then
    exit 1
  fi
  if ! curl -fL --max-time 90 "$asset_url" > "$tmp/$asset"; then
    exit 1
  fi
  line=$(tr -d '\r' < "$tmp/SHA256SUMS" | grep -E "^[0-9a-fA-F]+ [ *]?${asset}\$" || true)
  if [[ -z "$line" ]] || ! (cd "$tmp" && printf '%s\n' "$line" | sha256sum -c - >/dev/null); then
    printf '%s\n' 'сумма' >&2
    exit 1
  fi
  dest=$(root_path /usr/local/bin/zvucast)
  as_root mkdir -p -- "$(dirname -- "$dest")"
  as_root install -m 755 "$tmp/$asset" "$dest"
}

install_units() {
  local agent_path ctrl_path
  agent_path=$(root_path /etc/systemd/system/zvucast-agent.service)
  sed "s#/run/user/1000#/run/user/${AUDIO_UID}#" <<'ZVUCAST_AGENT' | write_file "$agent_path"
[Unit]
Description=zvucast agent
After=network-online.target sound.target
Wants=network-online.target

[Service]
# v1 работает от root: AmbientCapabilities без User= действуют только от root
# (CAP_SYS_TIME — подстройка PTP-часов, CAP_SYS_NICE — RT-приоритет gst).
# Hardening (выделенный юзер + ограниченный набор capabilities) — Phase 3.
Environment=RUST_LOG=info
# PipeWire слушает в сессии пользователя, не в системной. uid на платах и на PC — 1000.
Environment=XDG_RUNTIME_DIR=/run/user/1000
# Запас проигрывателя, мс. 40, 20 или 5 — правка и перезапуск агента, без пересборки.
Environment=ZVUCAST_PLAY_MS=40
ExecStart=/usr/local/bin/zvucast agent --config /etc/zvucast.toml
Restart=always
RestartSec=2
AmbientCapabilities=CAP_SYS_TIME CAP_SYS_NICE

[Install]
WantedBy=multi-user.target
ZVUCAST_AGENT
  as_root systemctl enable --now zvucast-agent.service
  ctrl_path=$(root_path /etc/systemd/system/zvucast-controller.service)
  write_file "$ctrl_path" <<'ZVUCAST_CONTROLLER'
[Unit]
Description=zvucast controller
After=network-online.target
Wants=network-online.target

[Service]
# v1 работает от root: AmbientCapabilities без User= действуют только от root
# (CAP_SYS_TIME — подстройка PTP-часов, CAP_SYS_NICE — RT-приоритет gst).
# Hardening (выделенный юзер + ограниченный набор capabilities) — Phase 3.
Environment=RUST_LOG=info
# /etc/zvucast может отсутствовать на свежем узле — контроллер его не создаёт
# (только claim-агента делает create_dir_all), поэтому директория — до старта.
ExecStartPre=/usr/bin/mkdir -p /etc/zvucast
ExecStart=/usr/local/bin/zvucast controller --listen 0.0.0.0:8080 --state /etc/zvucast/state.json
Restart=always
RestartSec=2
AmbientCapabilities=CAP_SYS_TIME CAP_SYS_NICE

[Install]
WantedBy=multi-user.target
ZVUCAST_CONTROLLER
  if [[ "$controller" == 1 ]]; then
    as_root systemctl enable --now zvucast-controller.service
  else
    as_root systemctl disable --now zvucast-controller.service
  fi
}

install_toml() {
  local dest iface host
  dest=$(root_path /etc/zvucast.toml)
  if [[ -e "$dest" ]]; then
    return 0
  fi
  iface=$(pick_iface)
  host=$(hostname -s)
  write_file "$dest" <<EOF
name = "${host}"
controller = "ws://127.0.0.1:8080/ws"
playback_backend = "pipewire"
sync_interface = "${iface}"

[pipeline]
jb_ms = 2
rx_buffer_us = 5000
rx_latency_us = 1000
tx_buffer_us = 10000
tx_latency_us = 2500
mcast_group = "239.0.0.1"
port = 5004
EOF
}

lookup_a() {
  local py=$1
  "$py" -c 'import socket, sys
name = "soloist-builds.spotifycdn.com"

def encode(n):
    out = bytearray()
    for label in n.split("."):
        raw = label.encode("ascii")
        if not raw or len(raw) > 63:
            raise SystemExit(1)
        out.append(len(raw))
        out += raw
    out.append(0)
    return bytes(out)

def skip_name(data, i):
    while i < len(data):
        n = data[i]
        if n == 0:
            return i + 1
        if n & 0xC0 == 0xC0:
            return i + 2
        i += 1 + n
    raise SystemExit(1)

try:
    query = bytes([0x3C, 0x4D, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]) + encode(name) + bytes([0x00, 0x01, 0x00, 0x01])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)
    sock.sendto(query, ("1.1.1.1", 53))
    data, _addr = sock.recvfrom(2048)
    if len(data) < 12:
        raise SystemExit(1)
    ancount = int.from_bytes(data[6:8], "big")
    i = skip_name(data, 12)
    i += 4
    for _ in range(ancount):
        i = skip_name(data, i)
        typ = int.from_bytes(data[i:i + 2], "big")
        clas = int.from_bytes(data[i + 2:i + 4], "big")
        rdlen = int.from_bytes(data[i + 8:i + 10], "big")
        rdata = data[i + 10:i + 10 + rdlen]
        if typ == 1 and clas == 1 and rdlen == 4:
            sys.stdout.write(".".join(str(b) for b in rdata))
            raise SystemExit(0)
        i += 10 + rdlen
    raise SystemExit(1)
except Exception:
    raise SystemExit(1)
'
}

install_soloist() {
  local arch url dir archive py ip installed
  [[ "$spotify" == 1 ]] || return 0
  arch=$(soloist_arch_name)
  url="https://soloist-builds.spotifycdn.com/soloist_release_${arch}.tar.gz"
  dir=$(new_tmp)
  archive=$dir/soloist.tar.gz
  if ! curl -fL --max-time 90 "$url" > "$archive"; then
    if [[ "$family" == apt ]]; then
      py=python3
    else
      py=python
    fi
    ip=$(lookup_a "$py" || true)
    ip=${ip//[[:space:]]/}
    if [[ -z "$ip" ]] || ! curl -fL --max-time 90 --resolve "soloist-builds.spotifycdn.com:443:${ip}" "$url" > "$archive"; then
      soloist_command "$arch" >&2
      exit 1
    fi
  fi
  if ! tar -xzf "$archive" -C "$dir" || [[ ! -f "$dir/soloist" ]]; then
    soloist_command "$arch" >&2
    exit 1
  fi
  installed=$(root_path /usr/local/bin/soloist)
  as_root mkdir -p -- "$(dirname -- "$installed")"
  as_root install -m 755 "$dir/soloist" "$installed"
  if ! "$installed" --version; then
    soloist_command "$arch" >&2
    exit 1
  fi
}

restart_units() {
  as_root systemctl restart zvucast-agent.service
  if [[ "$controller" == 1 ]]; then
    as_root systemctl restart zvucast-controller.service
  fi
}

main() {
  local os_file os_id
  trap 'if [[ ${#cleanup_dirs[@]} -gt 0 ]]; then rm -rf "${cleanup_dirs[@]}"; fi' EXIT
  resolve_user
  os_file=${ZVUCAST_OS_RELEASE:-/etc/os-release}
  machine=${ZVUCAST_UNAME:-$(uname -m)}
  if ! family=$(family_of); then
    exit 1
  fi
  if ! asset=$(asset_name); then
    exit 1
  fi
  os_id=$(os_field "$os_file" ID)
  if [[ -t 1 ]]; then
    printf '\033[1mZvucast\033[0m %s %s %s\n' "$os_id" "$machine" "$asset"
  else
    printf 'Zvucast %s %s %s\n' "$os_id" "$machine" "$asset"
  fi
  choose_options
  if [[ "$(id -u)" -ne 0 ]]; then
    sudo -v
  fi
  configure_overlay
  install_packages
  install_drops
  install_limits
  install_session
  install_wifi
  install_binary
  install_toml
  install_units
  install_soloist
  restart_units
}

if ! (return 0 2>/dev/null); then
  main "$@"
fi
