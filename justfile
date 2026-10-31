export PATH := env_var('HOME') + '/.cargo/bin:' + env_var('PATH')

# Агент Cursor задаёт CARGO_TARGET_DIR вне ./target. Копировать надо
# оттуда, куда cargo реально записал бинарник.
cargo_target := env_var_or_default("CARGO_TARGET_DIR", "target")

check-all:
    cargo check --workspace

check-private:
    bash scripts/check-private.sh

build-web:
    cd web && npm ci && npm run build

build-pc: build-web
    ZVUCAST_TARGET=x86_64 cargo build --release --workspace --target x86_64-unknown-linux-gnu
    mkdir -p dist
    cp "{{cargo_target}}/x86_64-unknown-linux-gnu/release/zvucast" dist/zvucast-x86_64

build-pi4: build-web
    bash scripts/cargo-aarch64.sh build --release --workspace --target aarch64-unknown-linux-gnu
    mkdir -p dist
    cp "{{cargo_target}}/aarch64-unknown-linux-gnu/release/zvucast" dist/zvucast-aarch64

build-rpi2: build-web
    ZVUCAST_TARGET=armv7-rpi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/rpi2 -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/rpi2 \
    PKG_CONFIG_PATH=$HOME/sysroots/rpi2/usr/lib/pkgconfig:$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/rpi2/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    mkdir -p dist
    cp "{{cargo_target}}/armv7-unknown-linux-gnueabihf/release/zvucast" dist/zvucast-armv7-rpi

build-opi: build-web
    ZVUCAST_TARGET=armv7-opi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/opi -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/opi \
    PKG_CONFIG_PATH=$HOME/sysroots/opi/usr/lib/pkgconfig:$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/opi/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    mkdir -p dist
    cp "{{cargo_target}}/armv7-unknown-linux-gnueabihf/release/zvucast" dist/zvucast-armv7-opi

release:
    #!/usr/bin/env bash
    set -euo pipefail
    ver=$(sed -n 's/^version = "\(.*\)"/\1/p' zvucast/Cargo.toml | head -1)
    gh release view "v$ver" >/dev/null 2>&1 && { echo "тег v$ver уже есть" >&2; exit 1; }
    test -d "$HOME/sysroots/pi4/usr/lib/aarch64-linux-gnu" \
      && test -d "$HOME/sysroots/rpi2" && test -d "$HOME/sysroots/opi" || {
      echo "нужны sysroot: \$HOME/sysroots/pi4, rpi2 и opi" >&2
      exit 1
    }
    out="{{cargo_target}}"

    ( cd web && npm ci && npm run build )

    ZVUCAST_RELEASE=1 ZVUCAST_TARGET=x86_64 cargo build --release --workspace --target x86_64-unknown-linux-gnu
    mkdir -p dist
    cp "$out/x86_64-unknown-linux-gnu/release/zvucast" dist/zvucast-x86_64

    ZVUCAST_RELEASE=1 bash scripts/cargo-aarch64.sh build --release --workspace --target aarch64-unknown-linux-gnu
    cp "$out/aarch64-unknown-linux-gnu/release/zvucast" dist/zvucast-aarch64

    ZVUCAST_RELEASE=1 \
    ZVUCAST_TARGET=armv7-rpi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/rpi2 -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/rpi2 \
    PKG_CONFIG_PATH=$HOME/sysroots/rpi2/usr/lib/pkgconfig:$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/rpi2/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    cp "$out/armv7-unknown-linux-gnueabihf/release/zvucast" dist/zvucast-armv7-rpi

    ZVUCAST_RELEASE=1 \
    ZVUCAST_TARGET=armv7-opi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/opi -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/opi \
    PKG_CONFIG_PATH=$HOME/sysroots/opi/usr/lib/pkgconfig:$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/opi/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    cp "$out/armv7-unknown-linux-gnueabihf/release/zvucast" dist/zvucast-armv7-opi

    got=$(dist/zvucast-x86_64 --version)
    if [ "$got" != "zvucast $ver" ]; then
      echo "x86_64 сообщает «$got», в манифесте $ver" >&2
      exit 1
    fi
    for bin in dist/zvucast-x86_64 dist/zvucast-aarch64 dist/zvucast-armv7-rpi dist/zvucast-armv7-opi; do
      if grep -a -F -q '+dev' "$bin"; then
        echo "$bin собран без ZVUCAST_RELEASE=1" >&2
        exit 1
      fi
    done

    # Текст релиза: файл RELEASE_NOTES_FILE, иначе коммиты после прошлого тега.
    notes_file="${RELEASE_NOTES_FILE:-}"
    cleanup_notes=0
    if [ -z "$notes_file" ]; then
      notes_file=$(mktemp)
      cleanup_notes=1
      prev=$(git describe --tags --abbrev=0 HEAD^ 2>/dev/null || true)
      if [ -n "$prev" ]; then
        git log --reverse --pretty=format:'%s%n%n%b%n' "$prev"..HEAD > "$notes_file"
      else
        printf '%s\n' 'Нет описания.' > "$notes_file"
      fi
    fi
    (
      cd dist
      sha256sum zvucast-x86_64 zvucast-aarch64 zvucast-armv7-rpi zvucast-armv7-opi > SHA256SUMS
      gh release create "v$ver" \
        zvucast-x86_64 zvucast-aarch64 zvucast-armv7-rpi zvucast-armv7-opi SHA256SUMS \
        --title "Zvucast $ver" --notes-file "$notes_file"
    )
    if [ "$cleanup_notes" = 1 ]; then
      rm -f "$notes_file"
    fi
