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

## License

MIT — see [LICENSE.md](LICENSE.md).
