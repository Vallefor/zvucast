# Установка и сводка готовности

Дата: 2026-10-25. Статус: одобрено пользователем.

## Зачем

Настройка узла должна сходиться к одной команде, а уже стоящая сеть — показывать, чего на какой машине не хватает и какой командой это чинится. Spotify без Soloist, мёртвый PipeWire и остальные пробелы из списка ниже видны одной сводкой по всем онлайн-узлам.

Работа живёт в публичном репозитории. Лабораторные `deploy/install-node-deps.sh` и поиск мастера по UDP не меняются. Протокол `ZVUCAST_WHO_IS_MASTER?` остаётся как есть.

## Сводка

Агент проверяет свою машину и присылает отчёт в `Hello`. Контроллер помнит последний отчёт онлайн-узла. Выключенный узел в сводку не входит: обрыв и так виден в списке узлов.

Агент ничего не включает и не вызывает `sudo`. Команда в отчёте — текст для человека. Сбой проверки, отсутствие утилиты или тайм-аут становятся строкой сводки. Агент при этом стартует и играет звук, как раньше.

Проверки идут параллельно. У каждой команды свой предел 2 секунды, у `soloist --version` — 3 секунды. Общий предел первого снимка — 5 секунд. Что не успело, считается проблемой с той же командой, что и при обычном провале. Первый `Hello` нового агента уходит уже со снимком (`setup` — массив, возможно пустой) и не пропускает поле. Дальше снимок обновляется раз в 60 секунд. Пока новый снимок считается, `Hello` увозит предыдущий.

Старый `Hello` без поля читается как «отчёта нет». Пустой массив значит «проверился, локальных проблем нет».

### Что проверяет агент

Менеджер пакетов агент выбирает сам, теми же правилами, что `install.sh`. Команда в строке — под него.

| `id` | Когда это проблема |
|---|---|
| `pipewire` | Пользовательская служба `pipewire` не `active` |
| `wireplumber` | Пользовательская служба `wireplumber` не `active` |
| `pipewire-pulse` | Пользовательская служба `pipewire-pulse` не `active` |
| `gstreamer` | `gst-inspect-1.0 pipewiresrc` или `gst-inspect-1.0 alsasrc` завершились не с нулём |
| `linger` | `loginctl show-user <uid> -p Linger` не равен `Linger=yes` |
| `pipewire-socket` | Нет файла `/run/user/<uid>/pipewire-0` |
| `rt-limits` | В `/etc/security/limits.conf` нет строки `@audio - rtprio 95` |
| `wifi-powersave` | Есть беспроводной интерфейс, и `iw dev <iface> get power_save` сообщает `on`, либо `iw` не найден |
| `controller` | `zvucast-controller` включён (`is-enabled` = `enabled`), но не `active` |

Выключенный или отсутствующий `zvucast-controller` на обычной плате проблемой не является. `linuxptp` и пакеты для сборки не проверяются: по сети ходит `zsync`, компилятор на узле не нужен.

`<uid>` берётся из `XDG_RUNTIME_DIR` в `/etc/systemd/system/zvucast-agent.service`. Ожидаемый вид — `/run/user/<uid>`. Имя пользователя — первое поле `getent passwd <uid>`. Нет строки в юните — uid `1000`. Нет записи passwd — `{user}` в командах равен десятичной записи этого uid.

Беспроводные интерфейсы — имена каталогов `/sys/class/net/*/wireless` по алфавиту. Нет ни одного — строки `wifi-powersave` нет. Проблема на нескольких интерфейсах — отдельная строка на каждый, с тем же `id` и своим `{iface}`.

### Soloist

В `Hello` это не строка `setup`, а два отдельных поля. Бинарник — `/usr/local/bin/soloist`.

| Состояние | Условие |
|---|---|
| `missing` | Файла нет или он не исполняемый. Процесс не запускается |
| `ok` | `soloist --version` завершился с кодом 0 |
| `expired` | Код 10 |
| `failed` | Другой ненулевой код или тайм-аут 3 секунды |
| `unknown` | Поля нет (старый агент) |

`soloist_command` агент присылает всегда, когда узнал архитектуру. Правила архитектуры те же, что у скрипта: `x86_64`, `aarch64` → `arm64`, `armv7l` → `arm32`. Чужая архитектура — пустая команда.

Контроллер добавляет проблему `soloist` только при сборке ответа API и только если у узла включён Spotify:

- `missing` — detail `бинарника нет`
- `expired` — detail `soloist --version завершился с кодом 10`
- `failed` — detail `soloist --version не ответил`
- `ok` — строки нет
- `unknown` — в массив `setup` не добавляется. Страница рисует её сама, потому что отчёта целиком нет

Галка Spotify меняет сводку на следующем опросе `GET /api/nodes`. Новый `Hello` для этого не нужен. Ключ Spotify в команду не попадает.

### Контракт `Hello`

Поля с `#[serde(default)]`, чтобы старый JSON читался:

```rust
setup: Option<Vec<SetupIssue>>,
soloist: SoloistState,          // нет поля → Unknown
soloist_command: String,        // нет поля → ""
```

```rust
struct SetupIssue {
    id: String,
    command: String,
    #[serde(default)]
    detail: String,
}
```

`SoloistState`: `unknown`, `missing`, `ok`, `expired`, `failed`, в JSON строчными буквами. Локальные проблемы агент кладёт в `Some(vec)`, даже если вектор пуст. `None` по сети не шлёт новый агент: отсутствие поля бывает только у старого.

Идентификаторы локальных проблем — ровно имена из таблицы выше. `soloist` агент в этот вектор не кладёт.

### Контракт `GET /api/nodes`

У узла появляется поле `setup`. Регистр имён как в остальном API, не camelCase.

- Агент не присылал `setup` — в JSON `null`.
- Присылал — массив его строк плюс, при включённом Spotify и состоянии Soloist не `ok` и не `unknown`, одна строка `id = "soloist"` с его `soloist_command` и detail из таблицы. Пустая `soloist_command` так и остаётся пустой.

Остальные поля узла не меняются.

### Страница «Узлы»

Блок стоит над списком узлов. Заголовок — «Системе не хватает». Блока нет, если после правил ниже не набралось ни одной строки.

Берутся только узлы с `online: true`. Узлы идут в том же порядке, что и список. У каждого имя (`display_name`, иначе `name`) и проблемы внутри, без повторения имени на каждой строке.

- `setup == null` — одна строка «Агент не прислал проверку, обновите узел». Если при этом `spotify`, под ней вторая: «Spotify включён, а агент не сообщил про Soloist», без команды.
- `setup` — массив: строка на элемент. Подпись по `id`:

| `id` | Подпись |
|---|---|
| `pipewire` | Служба PipeWire не запущена |
| `wireplumber` | WirePlumber не запущен |
| `pipewire-pulse` | PipeWire Pulse не запущен |
| `gstreamer` | Нет плагинов GStreamer |
| `linger` | После перезагрузки PipeWire не поднимется |
| `pipewire-socket` | Нет сокета PipeWire |
| `rt-limits` | Нет rtprio для группы audio |
| `wifi-powersave` | На Wi-Fi включено энергосбережение |
| `controller` | Контроллер включён, но не работает |
| `soloist` | Spotify включён, а Soloist недоступен |

Неизвестный `id` показывается как есть. Непустой `detail` — второй строкой под подписью. Непустая `command` — моноширинный блок. Нажатие копирует команду в буфер. Если буфер недоступен, текст остаётся выделенным. Пустую команду блок не рисует.

### Команды

`{user}` и `{uid}` — пользователь звука из юнита. `{iface}` — тот беспроводной интерфейс, на котором проблема. `{soloist_arch}` — `x86_64`, `arm64` или `arm32`.

Одна и та же команда у `pipewire`, `wireplumber`, `pipewire-pulse` и `pipewire-socket`: вставка любой из них поднимает все три службы.

Debian и производные:

```text
sudo apt-get install -y pipewire pipewire-bin pipewire-pulse wireplumber && sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service && sudo -u {user} XDG_RUNTIME_DIR=/run/user/{uid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus systemctl --user enable --now pipewire.service pipewire-pulse.service wireplumber.service
```

Arch и CachyOS:

```text
sudo pacman -S --needed --noconfirm pipewire pipewire-pulse wireplumber && sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service && sudo -u {user} XDG_RUNTIME_DIR=/run/user/{uid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus systemctl --user enable --now pipewire.service pipewire-pulse.service wireplumber.service
```

`gstreamer`, Debian:

```text
sudo apt-get install -y gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-alsa gstreamer1.0-pipewire
```

`gstreamer`, Arch:

```text
sudo pacman -S --needed --noconfirm gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugin-pipewire
```

`linger` на обеих семьях:

```text
sudo loginctl enable-linger {user} && sudo systemctl start user@{uid}.service
```

`rt-limits` на обеих семьях:

```text
grep -q '@audio - rtprio 95' /etc/security/limits.conf || printf '%s\n' '@audio - rtprio 95' '@audio - memlock unlimited' | sudo tee -a /etc/security/limits.conf; sudo usermod -aG audio {user}
```

`wifi-powersave`:

```text
sudo iw dev {iface} set power_save off
```

Если `iw` не установлен, перед этим та же строка, что ставит его: Debian — `sudo apt-get install -y iw && `, Arch — `sudo pacman -S --needed --noconfirm iw && `. Постоянное выключение после перезагрузки делает сам `install.sh` (файлы ниже). Повторная вставка `iw` чинит текущий интерфейс сразу.

`controller` на обеих семьях:

```text
sudo systemctl enable --now zvucast-controller
```

`soloist`:

```text
tmp=$(mktemp -d) && curl --fail --location --max-time 90 -o "$tmp/soloist.tar.gz" "https://soloist-builds.spotifycdn.com/soloist_release_{soloist_arch}.tar.gz" && tar -xzf "$tmp/soloist.tar.gz" -C "$tmp" && sudo install -m 755 "$tmp/soloist" /usr/local/bin/soloist && soloist --version
```

Команда Soloist в сводке — этот однострочник, без запасного DNS. Повтор через `1.1.1.1` есть только у `install.sh`.

Система не из семейства Debian и не Arch — строки, которым нужен `apt` или `pacman` (`pipewire`, `wireplumber`, `pipewire-pulse`, `pipewire-socket`, `gstreamer`, и `wifi-powersave`, если не найден `iw`), всё равно сообщаются. Их `command` равен тексту `эта система не из семейства Debian или Arch — пакеты этой командой не ставятся`. `linger`, `rt-limits`, `controller`, `soloist` и `wifi-powersave` при уже стоящем `iw` остаются командами выше: им менеджер пакетов не нужен.

## `install.sh`

Файл в корне репозитория. Запуск:

```bash
curl -fsSL https://raw.githubusercontent.com/Vallefor/zvucast/main/install.sh | bash
```

Именно `bash`. На Debian `sh` — dash, меню на нём не живут. `whiptail` и `dialog` не используются: на чистой плате их нет. Вопросы читаются с `/dev/tty`, потому что stdin занят самим скриптом. Цвет — ANSI, только если stdout является терминалом.

Без терминала скрипт читает `ZVUCAST_SPOTIFY` и `ZVUCAST_CONTROLLER`. Оба обязательны и равны `0` или `1`. Не хватает переменной, терминала или значение иное — выход `2`, система не изменена, на stderr напечатаны оба имени.

Если скрипт уже root, пользователь звука — `SUDO_USER`. Пустой `SUDO_USER` или root — выход `1` с текстом запустить команду от обычного пользователя. Если скрипт не root, пользователь звука — текущий `id -un`, привилегированные шаги идут через `sudo`. Целиком себя скрипт заново не исполняет: при `curl | bash` так нельзя. В начале — `sudo -v`, дальше пароль спрашивает сам `sudo`.

Повторный запуск дописывает недостающее. Существующий `/etc/zvucast.toml` не переписывается.

### Машина

До первого изменения системы скрипт уже знает архитектуру, плату и менеджер пакетов. Не узнал — выход `1`, пакетов нет.

| `uname -m` | Файл релиза | Архитектура Soloist |
|---|---|---|
| `x86_64` | `zvucast-x86_64` | `x86_64` |
| `aarch64` | `zvucast-aarch64` | `arm64` |
| `armv7l`, в device-tree есть `Raspberry Pi` и нет `Orange Pi` | `zvucast-armv7-rpi` | `arm32` |
| `armv7l`, в device-tree есть `Orange Pi` и нет `Raspberry Pi` | `zvucast-armv7-opi` | `arm32` |

Текст модели — `/proc/device-tree/model` без нулевых байтов, сравнение без учёта регистра. Другой `uname -m`, нет файла модели, или в модели оба имени либо ни одного — выход `1`, в тексте `uname -m` и модель, если она прочиталась.

Менеджер пакетов — из `/etc/os-release`, поля `ID` и `ID_LIKE`:

- `apt`, если `ID` равен `debian`, `ubuntu` или `raspbian`, либо `ID_LIKE` содержит `debian` или `ubuntu`. Сюда попадают Raspberry Pi OS, Armbian и Ubuntu.
- `pacman`, если `ID` равен `arch` или `cachyos`, либо `ID_LIKE` содержит `arch`.
- Совпали оба правила или ни одного — выход `1`.

Шапка до вопросов: слово `Zvucast`, имя системы, `uname -m`, имя файла релиза.

### Меню

Два вопроса и подтверждение. Пустой ввод берёт значение в скобках. Принимаются `y`/`yes`/`д`/`да` и `n`/`no`/`н`/`нет`.

1. Ставить Soloist? По умолчанию нет. Сборка чужая и живёт 90 дней.
2. Эта машина — контроллер? По умолчанию нет.
3. Перечень того, что будет установлено, и вопрос «продолжить?». По умолчанию да.

Адрес контроллера не спрашивается. Оба env заданы — меню и подтверждение пропускаются, перечень печатается, установка идёт сразу.

Ответ «нет» на подтверждение — выход `0`, система не изменена.

### Что устанавливается

Порядок жёсткий. Шаг упал — скрипт останавливается, уже поставленные пакеты не откатываются. Бинарник Zvucast до проверки суммы не заменяется.

1. Пакеты. Компилятор и `-dev` пакеты не ставятся.

   Debian: `pipewire`, `pipewire-bin`, `pipewire-pulse`, `wireplumber`, `gstreamer1.0-tools`, `gstreamer1.0-plugins-base`, `gstreamer1.0-plugins-good`, `gstreamer1.0-plugins-bad`, `gstreamer1.0-alsa`, `gstreamer1.0-pipewire`, `alsa-utils`, `curl`, `ca-certificates`, `iw`, `dbus-user-session`, `rtkit`, `python3`.

   Arch: `pipewire`, `pipewire-alsa`, `pipewire-pulse`, `wireplumber`, `gstreamer`, `gst-plugins-base`, `gst-plugins-good`, `gst-plugins-bad`, `gst-plugin-pipewire`, `alsa-utils`, `curl`, `ca-certificates`, `iw`, `rtkit`, `python`.

2. Дропы PipeWire и WirePlumber. Содержимое байт в байт совпадает с `deploy/pipewire/99-zvucast-quantum.conf` и `deploy/wireplumber/99-zvucast-latency.conf`. Скрипт вшивает текст в себя, клон репозитория на плате не нужен. Куда кладёт: `/etc/pipewire/pipewire.conf.d/99-zvucast-quantum.conf` и `/etc/wireplumber/wireplumber.conf.d/99-zvucast-latency.conf`.

3. Если в `/etc/security/limits.conf` ещё нет `@audio - rtprio 95`, дописать `@audio - rtprio 95` и `@audio - memlock unlimited`. Пользователь звука добавляется в группу `audio`.

4. `loginctl enable-linger` для этого пользователя, старт `user@<uid>.service`, затем `systemctl --user enable --now` для `pipewire`, `pipewire-pulse`, `wireplumber` с его `XDG_RUNTIME_DIR` и `DBUS_SESSION_BUS_ADDRESS`.

5. Энергосбережение Wi-Fi выключается всегда, отдельного вопроса нет. Пишется `/etc/NetworkManager/conf.d/wifi-powersave-off.conf` с секцией `[connection]` и строкой `wifi.powersave = 2`. Если есть `nmcli`, у каждого соединения типа `802-11-wireless` ставится `802-11-wireless.powersave 2`. Юнит `/etc/systemd/system/zvucast-wifi-powersave.service`: `Type=oneshot`, `After=network-online.target`, `Wants=network-online.target`, `RemainAfterExit=yes`, `WantedBy=multi-user.target`. `ExecStart` для каждого существующего `/sys/class/net/wlan*` вызывает `iw dev <имя> set power_save off` и игнорирует ненулевой код одного интерфейса. Юнит включается и запускается. Нет Wi-Fi — файлы всё равно лежат, команда юнита ничего не делает.

6. Бинарник из последнего релиза. Поиск тега повторяет `fetch_latest` в `zvucast/src/update.rs` и не ходит в `api.github.com`: `GET https://github.com/Vallefor/zvucast/releases/latest` без следования редиректу, из `Location` берётся `https://github.com/Vallefor/zvucast/releases/tag/v<version>`, где `<version>` — три десятичных числа через точку. Иначе стоп. Дальше качаются `https://github.com/Vallefor/zvucast/releases/download/v<version>/SHA256SUMS` и файл своей архитектуры. Сумма из строки этого файла совпала — `install -m 755` в `/usr/local/bin/zvucast`. Не совпала, файла нет в суммах, GitHub не ответил — прежний `/usr/local/bin/zvucast` не трогается, выход `1`.

7. Юнит агента — тот же, что `deploy/zvucast-agent.service`, но `XDG_RUNTIME_DIR` равен `/run/user/<uid>` этого пользователя, не зашитому `1000`. Юнит включается и запускается. Юнит контроллера кладётся всегда, содержимое совпадает с `deploy/zvucast-controller.service`. Ответ «да» — `enable --now`. Ответ «нет» — `disable --now`, в том числе если служба уже была включена.

8. `/etc/zvucast.toml` создаётся только если файла нет:

   ```toml
   name = "<hostname>"
   controller = "ws://127.0.0.1:8080/ws"
   playback_backend = "pipewire"
   sync_interface = "<iface>"

   [pipeline]
   jb_ms = 2
   rx_buffer_us = 5000
   rx_latency_us = 1000
   tx_buffer_us = 10000
   tx_latency_us = 2500
   mcast_group = "239.0.0.1"
   port = 5004
   ```

   `<hostname>` — короткое имя машины. `<iface>` — первый по алфавиту проводной интерфейс с carrier, который не мост, не veth, не tun и не loopback. Нет такого — первый по алфавиту Wi-Fi. Нет и его — литерал `eth0`. Роли и `id` не пишутся: роли подберёт агент, `id` он допишет при первом старте. `ptp_priority` не пишется, остаётся умолчание 128.

   Строка `controller` на плате, которая не контроллер, — заглушка. Перед каждым подключением агент и так спрашивает, кто мастер, и переключается на ответ. На машине-контроллере та же строка указывает на локальный контроллер.

9. Soloist — последним и только если на него согласились. Качается `https://soloist-builds.spotifycdn.com/soloist_release_<arch>.tar.gz`, внутри ожидается исполняемый файл `soloist`, он ставится в `/usr/local/bin/soloist`, затем `soloist --version`. Прямой `curl` не удался — одна повторная попытка. A-запись `soloist-builds.spotifycdn.com` берётся прямым UDP-запросом к `1.1.1.1` установленным `python3` (Debian) или `python` (Arch). Системный резолвер для этой попытки не используется. Берётся первый адрес, дальше тот же URL качается через `curl --resolve` на него. Повтор тоже не удался — выход `1`. Пакеты, бинарник и службы к этому моменту уже стоят. На stderr печатается команда `soloist` из раздела «Команды».

После успеха скрипт перезапускает `zvucast-agent`. Если контроллер включён — ещё и `zvucast-controller`.

## Проверка

Тесты не ходят в сеть, не ставят пакеты и не трогают платы.

Агент: подставные ответы команд и содержимое файлов. Отдельно — служба не `active` даёт команду своей семьи (`apt` против `pacman`); `soloist --version` с кодами 0 и 10, отсутствие файла и тайм-аут; чужая архитектура оставляет `soloist_command` пустым; общий предел не даёт одной зависшей команде держать снимок дольше 5 секунд.

`zproto`: `Hello` со `setup` и `soloist` проходит круг сериализации. `Hello` без этих полей читается как `setup = None`, `soloist = unknown`, пустая команда.

Контроллер: Spotify включён и Soloist `missing` — в JSON есть строка `soloist` с detail и командой. Spotify выключен — этой строки нет, локальная проблема PipeWire на месте. Soloist `ok` — строки нет. Старый узел без поля — `setup: null`. Узел с `online: false` может по-прежнему нести последний `setup` в JSON. Блок на странице его не рисует.

`install.test.sh` рядом со скриптом, в духе `deploy/apply-update.test.sh`. В `PATH` поддельные `uname`, `curl`, `apt-get`, `pacman`, `systemctl`, `sudo`. Сценарии:

- `x86_64` и Debian выбирают `zvucast-x86_64` и `apt-get`.
- `aarch64` выбирает `zvucast-aarch64` и Soloist `arm64`.
- `armv7l` с моделью Raspberry Pi выбирает `zvucast-armv7-rpi`.
- `armv7l` с моделью Orange Pi выбирает `zvucast-armv7-opi`.
- `armv7l` без узнаваемой модели завершается с `1`, `apt-get` и `pacman` не вызываются.
- CachyOS вызывает `pacman` и не вызывает `apt-get`.
- Незнакомый `ID` завершается с `1` до пакетов.
- Чужая сумма не заменяет путь бинарника.
- `ZVUCAST_SPOTIFY=1` и `ZVUCAST_CONTROLLER=0` без терминала качают Soloist и вызывают `disable --now` у `zvucast-controller`.
- Уже лежащий toml не меняется.
- Вшитые дропы байт в байт равны файлам в `deploy/pipewire` и `deploy/wireplumber`.
- Нет терминала и нет одной из переменных — выход `2`, ни одной установки.

## Вне этой работы

- Профиль ЦАП и прочие накладки под конкретную плату.
- Автоматическое обновление протухшего Soloist. Сводка показывает команду, человек запускает её сам.
- Вход в UI по паролю.
- Изменение поиска мастера.
- Удаление или вызов лабораторных скриптов установки.
