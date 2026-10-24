#!/usr/bin/env bash
# Sysroot для кросс-сборки armv7 (Pi 2 и прочие 32-bit). Хост — ssh-цель
# с уже установленными -dev пакетами (deploy/install-node-deps.sh).
#
#   bash scripts/make-sysroot.sh user@board "$HOME/sysroots/rpi2"
#
# Без --relative: иначе rsync воспроизводит абсолютный путь (usr/usr/lib).
# /lib на Debian 13 — симлинк на usr/lib (usrmerge); копируем как симлинк,
# чтобы линковщик нашёл ld-linux-armhf.so.3 внутри sysroot.
set -euo pipefail

HOST="${1:?ssh host, например user@board}"
DST="${2:-$HOME/sysroots/rpi2}"

# Весь /usr/lib на десктопном образе — это гигабайты python и прочего.
# Линковщику нужны multiarch-библиотеки, динамический линкер и заголовки.
mkdir -p "$DST/usr/lib" "$DST/usr/share"
rsync -a "$HOST:/usr/lib/arm-linux-gnueabihf" "$DST/usr/lib/"
rsync -a "$HOST:/usr/lib/ld-linux-armhf.so.3" "$DST/usr/lib/"
rsync -a "$HOST:/usr/include" "$DST/usr/"
rsync -a "$HOST:/usr/share/pkgconfig" "$DST/usr/share/"
rsync -a "$HOST:/lib" "$DST/"

# Arm GNU Toolchain ищет startfiles в arm-none-linux-gnueabihf,
# Debian multiarch зовётся arm-linux-gnueabihf.
ln -sfn arm-linux-gnueabihf "$DST/usr/lib/arm-none-linux-gnueabihf"
for f in "$DST/usr/lib/arm-linux-gnueabihf"/*.o; do
  ln -sfn "arm-linux-gnueabihf/$(basename "$f")" "$DST/usr/lib/$(basename "$f")"
done
for f in "$DST/usr/lib/arm-linux-gnueabihf"/*.so; do
  ln -sfn "arm-linux-gnueabihf/$(basename "$f")" "$DST/usr/lib/$(basename "$f")"
done

echo "sysroot: $DST"
