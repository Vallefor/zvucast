# Zvucast

Zvucast plays audio across a local network. A PC and small boards join one system, and one of them serves a web panel. You group speakers into zones and choose what each zone plays.

A zone can play several sources at once:

- **Capture on a board.** A line input, a microphone, or a USB audio input. The board sends those channels to the speakers in the zone.
- **A virtual output on the PC.** PipeWire shows a device named `Zvucast:` plus the zone name. Anything on that computer that can play to a sound device can play into the zone.
- **Spotify.** The installer can download [Spotify Soloist](https://developer.spotify.com/documentation/soloist), Spotify's headless player. Soloist is not part of this repository. Create a key under Spotify Soloist in the [Spotify for Developers](https://developer.spotify.com/dashboard) dashboard and paste it into the node settings. One key plays on one node; a second node that uses the same key stays silent. A Soloist build lasts 90 days. After that, the Nodes page reports it and shows the command that installs a fresh build.
- **Sendspin.** The node announces a speaker for each zone that uses it. A Sendspin server on the LAN, including [Music Assistant](https://www.music-assistant.io/), can play to that speaker.

Each of those is its own stream. The speakers mix the ones you link to the zone. A zone can have several line inputs, and at most one virtual output, one Spotify source, and one Sendspin source. Choosing a new computer for one of those three replaces the previous one for that zone.

Audio is 48 kHz stereo PCM. The machines keep a shared clock, so speakers in one zone stay together.

The panel text is Russian. The names used below are translated next to the buttons you will see.

## Install

Run this on each machine, as the user who should own the sound session. The script calls `sudo` for packages and services. It refuses to run as root unless `SUDO_USER` is a normal account.

```bash
curl -fsSL https://raw.githubusercontent.com/Vallefor/zvucast/main/install.sh | bash
```

The script asks four things. Empty input accepts the default in brackets.

| Prompt | Meaning | Answers |
|---|---|---|
| Какой модуль для воспроизведения звука подключен? | Which playback board is attached? | `1` — none. `2` — a PCM5102 board. |
| Ставить Soloist? [n] | Install Spotify Soloist? | `y` or `n` |
| Эта машина — контроллер? [n] | Is this machine the controller? | `y` on one machine, `n` on the others |
| Продолжить? [y] | Go ahead? | `y` or `n` |

`y`/`yes` and `n`/`no` work, and so do `да` and `нет`.

Pick **one** controller. That machine serves the panel. The others find it on the LAN. To move the panel later, use **Сделать мастером** on the Nodes page. A PCM5102 answer belongs on a Raspberry Pi that has that board. A PC and an Orange Pi stay on `1`.

The same choices can be given without a terminal. Set both flags together. `ZVUCAST_DAC` is optional and defaults to none.

```bash
# The PC that serves the panel and can play Spotify.
curl -fsSL https://raw.githubusercontent.com/Vallefor/zvucast/main/install.sh | \
  ZVUCAST_SPOTIFY=1 ZVUCAST_CONTROLLER=1 ZVUCAST_DAC=none bash

# A speaker board with a PCM5102, and no panel of its own.
curl -fsSL https://raw.githubusercontent.com/Vallefor/zvucast/main/install.sh | \
  ZVUCAST_SPOTIFY=0 ZVUCAST_CONTROLLER=0 ZVUCAST_DAC=pcm5102 bash
```

`ZVUCAST_SPOTIFY` and `ZVUCAST_CONTROLLER` are `0` or `1`. `ZVUCAST_DAC` is `none` or `pcm5102`.

The script installs PipeWire, WirePlumber, and GStreamer, downloads the binary for this machine from [GitHub Releases](https://github.com/Vallefor/zvucast/releases), and starts `zvucast-agent`. It starts `zvucast-controller` only on the machine you marked as the controller. It also turns Wi-Fi power save off and locks PipeWire to 48 kHz.

On a Raspberry Pi, `pcm5102` writes this into the boot config, under `[all]`:

```
dtparam=i2s=on
dtoverlay=hifiberry-dac
```

That overlay is what a GY-PCM5102 and a HiFiBerry DAC both use. The DAC shows up after a reboot. The installer does not reboot the machine.

The first run writes `/etc/zvucast.toml`. A later run leaves that file in place. The binary is `/usr/local/bin/zvucast`. The controller keeps its zones in `/etc/zvucast/state.json`.

## Boards

The installer reads `uname -m` and, on 32-bit ARM, `/proc/device-tree/model`. It stops before changing packages when the machine is not one of these.

| Machine | Release asset | What it is used for |
|---|---|---|
| 64-bit PC (`x86_64`) | `zvucast-x86_64` | The usual home for the panel, the virtual output, Spotify, and Sendspin. It can also play and capture. |
| 64-bit ARM (`aarch64`) | `zvucast-aarch64` | Any 64-bit ARM machine. This is the build used on a Raspberry Pi 4. |
| 32-bit Raspberry Pi (`armv7l`, model contains “Raspberry Pi”) | `zvucast-armv7-rpi` | Raspberry Pi 2, which only has a 32-bit system image. A later Pi on a 32-bit image uses it too. |
| 32-bit Orange Pi (`armv7l`, model contains “Orange Pi”) | `zvucast-armv7-opi` | An armv7 Orange Pi. The Orange Pi PC Plus plays through its onboard analog codec. |

The Raspberry Pi 2 has no onboard Wi-Fi. Use Ethernet, or a USB Wi-Fi adapter.

Speaker output on a Raspberry Pi is an I2S DAC: a GY-PCM5102 or a HiFiBerry DAC, chosen as `pcm5102` at install time. The onboard headphone jack is a PWM output with a long buffer. USB sound cards that PipeWire lists can be turned on in the node settings, on a PC or on a board.

A node is often both a capture device and a speaker. The agent detects that. You then enable the inputs and outputs you want.

The virtual output, Spotify, and Sendspin are turned on per node. They need the PipeWire backend, which is what the installer writes. A desktop is the usual place for the virtual output. A board is the usual place to capture a line input and to play a zone on a DAC.

## Operating systems

The installer supports systemd distributions it can install with apt or pacman:

- **apt:** Debian, Ubuntu, Raspberry Pi OS, and a distribution whose `ID` or `ID_LIKE` is `debian` or `ubuntu`. Raspberry Pi OS Bookworm and Trixie both work. The PCM5102 overlay is written to `/boot/firmware/config.txt` when that file or directory exists, and to `/boot/config.txt` otherwise.
- **pacman:** Arch Linux, CachyOS, and a distribution whose `ID` or `ID_LIKE` is `arch`.

Anything else stops before packages are installed. There is no build for Windows or macOS.

The sound server runs in that user's session. The installer enables lingering for the account, adds it to the `audio` group, and starts PipeWire, PipeWire Pulse, and WirePlumber.

## Web panel

On the controller, open:

```
http://<controller-address>:8080
```

The controller listens on `0.0.0.0:8080`. `/` opens the Nodes page. The two pages are:

| Path | Panel | What it is |
|---|---|---|
| `/nodes` | Узлы | The machines: capture, playback, Spotify, Sendspin, the virtual output, and updates. |
| `/zones` | Зоны | Speaker groups, their sources, and the channel map. |

Other nodes find this address by themselves. You do not point them at it.

### Nodes

The page lists every node the controller knows, and how many are online. **Настройки** opens that machine.

- **Имя** is the name shown in the panel. The hostname stays underneath.
- **Входы** are capture channels. Enable the line input, microphone, or USB input that this board should send.
- **Выходы** are playback channels. Enable the DAC, USB card, or other output that should speak for a zone.
- **Громкость** sets capture level (**Захват**) and playback level (**Воспроизведение**). A device can also have its own level.
- **Виртуальная колонка** creates the virtual output. Add it to a zone before an application can play to `Zvucast: <zone>`.
- **Spotify** starts Soloist on this machine while the box is checked. **Ключ Spotify** is the key. Soloist has to be installed.
- **Sendspin** announces this machine to Sendspin servers while the box is checked.
- **Сделать мастером** moves the panel to this machine and stops the controller on the old one. The page shows the new address if the browser does not follow.
- **Старт** and **Стоп** start and stop this node. **Старт всех** and **Стоп всех** do that for every node.
- **Обновить** installs the latest release on this machine. **Обновить все** does it for every machine. The page checks GitHub Releases while it is open. Nodes do not upgrade themselves in the background.

Clearing the virtual output, Spotify, or Sendspin on a node removes that source from every zone.

**Системе не хватает** appears when an online machine is missing something the installer normally provides, such as a running PipeWire session or Soloist while Spotify is enabled. Each row includes a command.

### Zones

**Новая зона** creates a zone. The first capture node on the LAN is its first source. You can change that.

- **Добавить колонку** adds a speaker. **Убрать** removes one. A speaker is any node with playback.
- **Настройки** names the zone and chooses sources: line inputs, and the virtual output, Spotify, or Sendspin of a node where that feature is turned on. It also sets how long silence must last (**Тишина, мин**; `0` keeps the speakers up), the level that still counts as silence (**Порог, дБ**), and how much audio to keep ahead of the network (**Запас сети, мс**, from 1 to 500).
- **Каналы** maps each source onto the left and right of a speaker. **Поменять стороны** swaps them.
- **Пуск** and **Стоп** start and stop the zone. **Удалить** deletes it, including its speakers and channel map.

**играет** means the zone is playing. **тишина** means it has gone quiet and the speakers have dropped.

The virtual device, the Spotify device, and the Sendspin speaker for a zone exist while that source is linked. Their names use the zone name.

## Network

The machines need to see each other's broadcasts. Ethernet and Wi-Fi both work. The installer disables Wi-Fi power save because a sleeping radio breaks the clock and the audio.

| Port | Use |
|---|---|
| TCP 8080 | The panel, and the connection from each node to the controller |
| UDP 5005 | Discovery. Nodes ask who the controller is, and the controller answers. |
| UDP 5004–5130, even ports | Audio. One port per stream, up to 64 streams. |
| UDP 319 and 320 | Clock sync between the nodes |

## Configuration

`/etc/zvucast.toml` is the agent config. The installer writes it once:

```toml
name = "living-room"
controller = "ws://127.0.0.1:8080/ws"
playback_backend = "pipewire"
sync_interface = "eth0"

[pipeline]
jb_ms = 2
rx_buffer_us = 5000
rx_latency_us = 1000
tx_buffer_us = 10000
tx_latency_us = 2500
mcast_group = "239.0.0.1"
port = 5004
```

`sync_interface` is the interface used for clock sync. The installer picks a connected Ethernet interface when there is one, otherwise Wi-Fi. Nodes discover the real controller on the LAN, so the address in this file can stay on localhost. The agent writes a stable `id` into the file the first time it starts. Capture devices and playback devices are chosen in the panel.

A commented copy is in [`examples/zvucast.example.toml`](examples/zvucast.example.toml).

## Build

Released binaries are what the installer and the panel install. To build the PC binary from this tree:

```bash
just build-pc
```

`just check-all` type-checks the workspace. The panel is the React app in `web/`. A release binary embeds it. Cross builds for the boards need the sysroots used by `just release`.

## License

[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
