# syntax=docker/dockerfile:1.7
#
# Simula in one image: the server (Bot API, Control API, client sessions) and the built
# web client, on port 8081.
#
#   docker build -t simula .
#   docker run --rm -p 8081:8081 -v simula-data:/data simula
#
# Then open http://localhost:8081 for the client; bots use http://localhost:8081 as
# their Bot API server. Pass `--memory` to keep nothing between runs.

# The web client (apps/web), built with Bun + Vite.
FROM oven/bun:1.4.0-slim AS web
WORKDIR /src
# Every workspace's manifest, so the frozen lockfile resolves.
COPY package.json bun.lock ./
COPY apps/web/package.json apps/web/
COPY packages/schema/package.json packages/schema/
COPY tools/codegen/package.json tools/codegen/
RUN bun install --frozen-lockfile
COPY packages/schema packages/schema
COPY apps/web apps/web
RUN bun run --cwd apps/web build

# The server binary.
FROM rust:1.98.1-slim-trixie AS server
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates crates
ENV CARGO_PROFILE_RELEASE_STRIP=symbols
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p simula-server \
    && cp target/release/simula /usr/local/bin/simula

# The runtime: the binary, the client, and a volume for sandboxes.
FROM debian:trixie-slim
RUN useradd --system --uid 10001 --home-dir /data simula \
    && mkdir -p /data \
    && chown simula /data
COPY --from=server /usr/local/bin/simula /usr/local/bin/simula
COPY --from=web /src/apps/web/dist /usr/share/simula/web
USER simula
ENV SIMULA_HOST=0.0.0.0 \
    SIMULA_PORT=8081 \
    SIMULA_DATA_DIR=/data \
    SIMULA_WEB_DIR=/usr/share/simula/web \
    RUST_LOG=info
VOLUME /data
EXPOSE 8081
# No curl in the slim image: ask /_simula/v1/health over bash's /dev/tcp.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD \
    ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/${SIMULA_PORT} && printf 'GET /_simula/v1/health HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"ok\"' <&3"]
ENTRYPOINT ["simula"]
