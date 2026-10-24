export PATH := env_var('HOME') + '/.cargo/bin:' + env_var('PATH')

check-all:
    cargo check --workspace

check-private:
    bash scripts/check-private.sh

build-web:
    cd web && npm ci && npm run build

build-pc: build-web
    ZVUCAST_TARGET=x86_64 cargo build --release --workspace --target x86_64-unknown-linux-gnu
    mkdir -p dist
    cp target/x86_64-unknown-linux-gnu/release/zvucast dist/zvucast-x86_64

build-pi4: build-web
    #!/usr/bin/env bash
    set -euo pipefail
    test -n "${PI4_SSH:-}" || { echo "нужна переменная PI4_SSH" >&2; exit 1; }
    rsync -a --delete --exclude target --exclude node_modules --exclude /dist ./ "$PI4_SSH:~/zvucast/"
    ssh "$PI4_SSH" 'export PATH="$HOME/.cargo/bin:$PATH" ZVUCAST_TARGET=aarch64 && cd ~/zvucast && cargo build --release'
    mkdir -p dist
    scp "$PI4_SSH:~/zvucast/target/release/zvucast" dist/zvucast-aarch64

build-rpi2: build-web
    ZVUCAST_TARGET=armv7-rpi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/rpi2 -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/rpi2 \
    PKG_CONFIG_PATH=$HOME/sysroots/rpi2/usr/lib/pkgconfig:$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/rpi2/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    mkdir -p dist
    cp target/armv7-unknown-linux-gnueabihf/release/zvucast dist/zvucast-armv7-rpi

build-opi: build-web
    ZVUCAST_TARGET=armv7-opi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/opi -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/opi \
    PKG_CONFIG_PATH=$HOME/sysroots/opi/usr/lib/pkgconfig:$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/opi/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    mkdir -p dist
    cp target/armv7-unknown-linux-gnueabihf/release/zvucast dist/zvucast-armv7-opi

release:
    #!/usr/bin/env bash
    set -euo pipefail
    ver=$(sed -n 's/^version = "\(.*\)"/\1/p' zvucast/Cargo.toml | head -1)
    gh release view "v$ver" >/dev/null 2>&1 && { echo "тег v$ver уже есть" >&2; exit 1; }
    test -n "${PI4_SSH:-}"
    test -d "$HOME/sysroots/rpi2" && test -d "$HOME/sysroots/opi" || exit 1

    ( cd web && npm ci && npm run build )

    ZVUCAST_RELEASE=1 ZVUCAST_TARGET=x86_64 cargo build --release --workspace --target x86_64-unknown-linux-gnu
    mkdir -p dist
    cp target/x86_64-unknown-linux-gnu/release/zvucast dist/zvucast-x86_64

    rsync -a --delete --exclude target --exclude node_modules --exclude /dist ./ "$PI4_SSH:~/zvucast/"
    ssh "$PI4_SSH" 'export PATH="$HOME/.cargo/bin:$PATH" ZVUCAST_RELEASE=1 ZVUCAST_TARGET=aarch64 && cd ~/zvucast && cargo build --release'
    scp "$PI4_SSH:~/zvucast/target/release/zvucast" dist/zvucast-aarch64

    ZVUCAST_RELEASE=1 \
    ZVUCAST_TARGET=armv7-rpi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/rpi2 -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/rpi2/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/rpi2 \
    PKG_CONFIG_PATH=$HOME/sysroots/rpi2/usr/lib/pkgconfig:$HOME/sysroots/rpi2/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/rpi2/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    cp target/armv7-unknown-linux-gnueabihf/release/zvucast dist/zvucast-armv7-rpi

    ZVUCAST_RELEASE=1 \
    ZVUCAST_TARGET=armv7-opi \
    RUSTFLAGS="-C link-args=--sysroot=$HOME/sysroots/opi -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf -C link-args=-Wl,-rpath-link,$HOME/sysroots/opi/lib" \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_SYSROOT_DIR=$HOME/sysroots/opi \
    PKG_CONFIG_PATH=$HOME/sysroots/opi/usr/lib/pkgconfig:$HOME/sysroots/opi/usr/lib/arm-linux-gnueabihf/pkgconfig:$HOME/sysroots/opi/usr/share/pkgconfig \
    cargo build --release --workspace --target armv7-unknown-linux-gnueabihf
    cp target/armv7-unknown-linux-gnueabihf/release/zvucast dist/zvucast-armv7-opi

    (
      cd dist
      sha256sum zvucast-x86_64 zvucast-aarch64 zvucast-armv7-rpi zvucast-armv7-opi > SHA256SUMS
      gh release create "v$ver" \
        zvucast-x86_64 zvucast-aarch64 zvucast-armv7-rpi zvucast-armv7-opi SHA256SUMS \
        --title "v$ver" --notes "Бинарники четырёх плат."
    )
