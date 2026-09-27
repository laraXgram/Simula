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

## License

MIT — see [LICENSE.md](LICENSE.md).
