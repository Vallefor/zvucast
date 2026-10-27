#!/usr/bin/env bash
# Sysroot для кросс-сборки. Хост — ssh-цель с уже установленными -dev
# пакетами. Архитектуру скрипт берёт с платы (uname -m).
#
#   bash scripts/make-sysroot.sh user@board "$HOME/sysroots/rpi2"
#   bash scripts/make-sysroot.sh user@board "$HOME/sysroots/pi4"
#
# Без --relative: иначе rsync воспроизводит абсолютный путь (usr/usr/lib).
# /lib на Debian 13 — симлинк на usr/lib (usrmerge); копируем как симлинк,
# чтобы линковщик нашёл динамический линкер внутри sysroot.
set -euo pipefail

HOST="${1:?ssh host, например user@board}"
DST="${2:-$HOME/sysroots/rpi2}"

arch=$(ssh -o BatchMode=yes "$HOST" uname -m)
case "$arch" in
  aarch64)
    multiarch=aarch64-linux-gnu
    loader=ld-linux-aarch64.so.1
    toolchain_alias=
    ;;
  armv7l|armv6l)
    multiarch=arm-linux-gnueabihf
    loader=ld-linux-armhf.so.3
    # Arm GNU Toolchain ищет startfiles в arm-none-linux-gnueabihf,
    # Debian multiarch зовётся arm-linux-gnueabihf.
    toolchain_alias=arm-none-linux-gnueabihf
    ;;
  *)
    echo "неподдерживаемая архитектура: $arch" >&2
    exit 1
    ;;
esac

mkdir -p "$DST/usr/lib" "$DST/usr/share"
rsync -a "$HOST:/usr/lib/$multiarch" "$DST/usr/lib/"
rsync -a "$HOST:/usr/lib/$loader" "$DST/usr/lib/"
rsync -a "$HOST:/usr/include" "$DST/usr/"
rsync -a "$HOST:/usr/share/pkgconfig" "$DST/usr/share/"
rsync -a "$HOST:/lib" "$DST/"
# Debian 13: заголовки linux-libc-dev — симлинки в /usr/lib/linux/uapi.
if ssh -o BatchMode=yes "$HOST" 'test -d /usr/lib/linux'; then
  rsync -a "$HOST:/usr/lib/linux" "$DST/usr/lib/"
fi
# libgcc.a и crtbegin.o лежат вне multiarch. На armv7 их даёт
# кросс-gcc на ПК, на aarch64 линкуем clang'ом и берём с платы.
if [ "$arch" = aarch64 ]; then
  rsync -a "$HOST:/usr/lib/gcc" "$DST/usr/lib/"
fi

if [ -n "$toolchain_alias" ]; then
  ln -sfn "$multiarch" "$DST/usr/lib/$toolchain_alias"
  for f in "$DST/usr/lib/$multiarch"/*.o; do
    [ -e "$f" ] || continue
    ln -sfn "$multiarch/$(basename "$f")" "$DST/usr/lib/$(basename "$f")"
  done
  for f in "$DST/usr/lib/$multiarch"/*.so; do
    [ -e "$f" ] || continue
    ln -sfn "$multiarch/$(basename "$f")" "$DST/usr/lib/$(basename "$f")"
  done
fi

echo "sysroot: $DST ($arch)"
