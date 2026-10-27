#!/usr/bin/env bash
# cargo для aarch64 с sysroot Pi 4. ZVUCAST_RELEASE, если нужен, задаёт вызывающий.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
sys="${PI4_SYSROOT:-$HOME/sysroots/pi4}"
if [ ! -d "$sys/usr/lib/aarch64-linux-gnu" ]; then
  echo "нет sysroot $sys — bash scripts/make-sysroot.sh user@pi4 \"$sys\"" >&2
  exit 1
fi

unset PKG_CONFIG_PATH
export ZVUCAST_TARGET=aarch64
export PKG_CONFIG_ALLOW_CROSS=1
export PKG_CONFIG_SYSROOT_DIR="$sys"
export PKG_CONFIG_LIBDIR="$sys/usr/lib/aarch64-linux-gnu/pkgconfig:$sys/usr/share/pkgconfig"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$root/scripts/aarch64-clang.sh"
export CC_aarch64_unknown_linux_gnu="$root/scripts/aarch64-clang.sh"
export CFLAGS_aarch64_unknown_linux_gnu="--target=aarch64-linux-gnu --sysroot=$sys"
export BINDGEN_EXTRA_CLANG_ARGS_aarch64_unknown_linux_gnu="--target=aarch64-linux-gnu --sysroot=$sys"

exec cargo "$@"
