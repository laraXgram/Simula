# Simula

An offline Telegram simulator for building, testing and debugging Telegram bots — in any
language, with any bot library, without internet access or Telegram itself.

> **v2 is a ground-up rewrite in progress.** The previous version lives on `master`.
> See [ROADMAP.md](ROADMAP.md) for status and [docs/architecture.md](docs/architecture.md)
> for the design.

- **Bot API compatible**: point your bot at Simula instead of `api.telegram.org`;
  polling and webhooks both work.
- **Real layering**: an MTProto (TL) core with a Bot API gateway on top, like Telegram's
  own `telegram-bot-api` server.
- **API-driven**: every action a person can do in the client is also available over the
  Control API, so test suites can be written in any language.
- **Client + dev tools**: a Telegram-like client for web and desktop (Windows, macOS,
  Linux) with request logs, update inspectors, webhook console and more.

## Try it (v2, early)

```sh
cargo run -p simula-server -- --memory          # Bot API + Control API on :8081

# create a bot (keep your bot's real token so its code runs unchanged) and a user
curl -X POST localhost:8081/_simula/v1/bots  -H 'content-type: application/json' \
     -d '{"first_name":"Echo","username":"echo_bot","token":"123456789:AAH…"}'
curl -X POST localhost:8081/_simula/v1/users -H 'content-type: application/json' \
     -d '{"first_name":"Alice","username":"alice"}'

# point your bot at http://localhost:8081 instead of https://api.telegram.org, then
# write to it as Alice:
curl -X POST localhost:8081/_simula/v1/accounts/1000000001/send -H 'content-type: application/json' \
     -d '{"chat_id":"@echo_bot","text":"/start"}'
```

### The client

```sh
bun install
cargo run -p simula-server -- --memory   # in one terminal
bun run web                              # in another: http://localhost:5173
```

Pick (or create) a user, open your bot by its `@username` and talk to it: formatting,
inline and reply keyboards, callback answers, commands, deep links and replies work as in
Telegram, and so do basic groups (✚ creates one; add your bot to see privacy mode, group
commands and membership updates). The client is just another session (`/session`,
JSON-TL over WebSocket), so everything it does is scriptable too.

To serve a built client from the server itself (one port for everything):

```sh
bun run --cwd apps/web build
cargo run -p simula-server -- --memory --web-dir apps/web/dist   # http://localhost:8081
```

### Docker

```sh
docker compose up --build        # http://localhost:8081, sandboxes kept in a volume
# or
docker build -t simula .
docker run --rm -p 8081:8081 -v simula-data:/data simula            # persistent
docker run --rm -p 8081:8081 simula --memory                        # throwaway
```

The image serves the client, the Bot API, the Control API and sessions on port 8081.
Bots running on the host reach it at `http://localhost:8081`; bots in other containers
of the same compose project at `http://simula:8081`. Settings are environment variables
(`SIMULA_PORT`, `SIMULA_DATA_DIR`, `SIMULA_WEB_DIR`, `RUST_LOG`) or flags
(`simula --help`).

## License

MIT — see [LICENSE.md](LICENSE.md).
