# Minimal production image: static-musl binary on `scratch` — no libc, no
# shell, no package manager, nothing but the server itself. Compare with
# `Dockerfile.bench`, which deliberately skips this (see its own comment)
# to stay closer to a "normal" distroless image for benchmark parity.
FROM rust:1-bookworm AS builder
# musl-tools provides musl-gcc, needed because mlua's "vendored" feature
# compiles PUC Lua's C sources at build time and those need to target musl
# too, not just the Rust code.
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN arch="$(uname -m)" && \
    case "$arch" in \
        x86_64) target=x86_64-unknown-linux-musl ;; \
        aarch64) target=aarch64-unknown-linux-musl ;; \
        *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac && \
    rustup target add "$target" && \
    cargo build --release --target "$target" --bin reflexdb-server && \
    cp "target/$target/release/reflexdb-server" /reflexdb-server

FROM scratch
COPY --from=builder /reflexdb-server /reflexdb-server
EXPOSE 6379
ENTRYPOINT ["/reflexdb-server", "--port", "6379", "--bind", "0.0.0.0", "--dir", "/data", "--appendonly", "no"]
