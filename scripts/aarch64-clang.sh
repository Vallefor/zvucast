#!/usr/bin/env bash
# clang + lld как кросс-компилятор aarch64. Sysroot — копия платы
# ($PI4_SYSROOT или ~/sysroots/pi4), не тулчейн в /usr.
set -euo pipefail

sys="${PI4_SYSROOT:-$HOME/sysroots/pi4}"
if [ ! -d "$sys/usr/lib/aarch64-linux-gnu" ]; then
  echo "нет sysroot $sys" >&2
  exit 1
fi

args=(--target=aarch64-linux-gnu --sysroot="$sys")
if [ -d "$sys/usr/lib/gcc/aarch64-linux-gnu" ]; then
  gcc_lib=$(find "$sys/usr/lib/gcc/aarch64-linux-gnu" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort -V | tail -1)
  if [ -n "$gcc_lib" ]; then
    args+=("-B$sys/usr/lib/gcc/aarch64-linux-gnu/$gcc_lib")
  fi
fi

link=1
for a in "$@"; do
  case "$a" in
    -c|-E|-S|-M|-MM) link=0 ;;
  esac
done
if [ "$link" -eq 1 ]; then
  args+=(-fuse-ld=lld "-Wl,-rpath-link,$sys/usr/lib/aarch64-linux-gnu")
fi

exec clang "${args[@]}" "$@"
