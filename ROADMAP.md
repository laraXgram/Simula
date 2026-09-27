# Simula v2 Roadmap

Full rewrite of Simula (v1 lives on `master`). Design: [docs/architecture.md](docs/architecture.md).
Every finished task is logged in [docs/worklog.md](docs/worklog.md) and committed on the
`v2` branch with its task ID in the commit message.

**Status legend:** `[x]` done · `[~]` in progress · `[ ]` planned · `[-]` dropped

**Targets:** Bot API **10.3** · MTProto layer from tdlib `telegram_api.tl`

**Current focus:** M1 — first message end to end

---

## Milestones

Phases below are the map; milestones are the order of work. Each milestone is a
vertical slice through several phases, so the architecture is exercised end to end early.

- [~] **M1 — first message end to end**: a user writes to a bot, the bot gets it via
  `getUpdates`, replies with `sendMessage`, the user receives the reply.
  Touches P2.1, P2.2, P2.4, P2.5, P2.7, P2.8, P2.9 (partial), P3.1, P3.2, P4.1, P4.4 (partial),
  then P1.9–P1.10 once handler files exist.
- [ ] **M2 — webhooks, formatting, keyboards, callbacks** (P3.3, P3.4, P3.6, wave 1 complete)
- [ ] **M3 — client shell** talking TL over WebSocket (P5.1–P5.4)
- [ ] **M4 — groups and channels** (P2.3, P2.10, P5.5)

---

## P0 — Foundations

- [x] P0.1 `v2` branch, v1 sources removed (kept on `master`)
- [x] P0.2 Architecture doc, roadmap, worklog
- [x] P0.3 Workspace skeleton: Cargo workspace, Bun workspace, lint/format (rustfmt, clippy, biome)
- [ ] P0.4 CI: build, test, lint, "generated code is fresh" check

## P1 — Specs & code generation

- [x] P1.1 Bot API scraper → `spec/botapi/api.json` (methods, types, unions, return types)
- [x] P1.2 Constraint extraction from descriptions (lengths, ranges, enums, defaults, byte limits)
- [x] P1.3 `overrides.json` for manual corrections, applied on top of the scrape
- [x] P1.4 TL parser → `spec/tl/*.json` (vendored `telegram_api.tl`, `mtproto_api.tl`, CRC-verified)
- [x] P1.5 Spec diff: report what changed between two Bot API versions / TL layers
- [x] P1.6 Rust emitter: TL types + binary codec + JSON-TL codec (`simula-tl`)
- [x] P1.7 Rust emitter: Bot API types, method param structs, validators (`simula-botapi-types`)
- [x] P1.8 TypeScript emitter: TL + Bot API types for the client and SDKs
- [ ] P1.9 Registry emitters: TL dispatch table, Bot API method table, stubs for new methods
- [ ] P1.10 Coverage report: implemented / partial / stub per method, generated from registries

## P2 — Core engine (MTProto semantics)

- [x] P2.1 Storage: SQLite per sandbox, migrations, in-memory mode
- [x] P2.2 Accounts (users, bots, tokens), peers, access hashes
- [ ] P2.3 Chats: private, basic group, supergroup, channel, forum; members, rights, bans
- [~] P2.4 Message boxes (per-account vs per-channel IDs), canonical TL storage, presenters
- [~] P2.5 Update engine: pts/qts/seq, channel pts, `Updates` shapes, difference log
- [ ] P2.6 Files: storage, file references, thumbnails metadata
- [~] P2.7 Virtual clock and scheduler (timers for polls, live locations, bans, scheduled posts)
- [~] P2.8 Event bus (RPC calls, updates, deliveries, logs)
- [~] P2.9 TL handlers, wave 1: users/bots, messages send/edit/delete/forward, callbacks
- [ ] P2.10 TL handlers, wave 2: chats/channels admin, invites, join requests, forums
- [ ] P2.11 TL handlers, wave 3: inline, payments/stars, stickers, games, polls, reactions
- [ ] P2.12 TL handlers, wave 4: business, stories, gifts, checklists, suggested posts, boosts, verification

## P3 — Bot API gateway

- [ ] P3.1 HTTP layer: all body formats, token auth, method routing, error model, file downloads
- [ ] P3.2 Update queue, `getUpdates` long polling (offset/limit/timeout/allowed_updates, 409 conflict)
- [ ] P3.3 Webhooks: secret token, max_connections, retries/backoff, webhook info, reply-in-response
- [ ] P3.4 Text parsers: HTML, Markdown, MarkdownV2 with Telegram's exact errors
- [ ] P3.5 InputFile pipeline: multipart, `attach://`, file_id reuse, URLs, local mode paths
- [ ] P3.6 Send pipeline shared by all `send*` methods; edit pipeline
- [ ] P3.7 Converters TL ⇄ Bot API (declarative where possible)
- [ ] P3.8 Method waves mirroring P2.9–P2.12 until 100% of Bot API 10.3 is covered
- [ ] P3.9 Declarative mappings for simple methods (derived from patterns found in P3.8)
- [ ] P3.10 Conformance suite: golden tests against recorded real-Telegram responses

## P4 — Control API

- [ ] P4.1 Server skeleton under `/_simula/v1`, generated OpenAPI
- [ ] P4.2 Sandboxes: create/destroy/list, in-memory, snapshots, fixtures (YAML seed files)
- [ ] P4.3 Entity management: users, bots (BotFather-like settings), groups, channels, members
- [ ] P4.4 Act-as endpoints over TL user sessions (send, press, vote, pay, join, react, …)
- [ ] P4.5 Inspection: event stream (WS), request logs, update queues, webhook deliveries
- [ ] P4.6 Fault injection: errors, 429 + retry_after, latency, dropped updates, webhook failures
- [ ] P4.7 Clock control: freeze, advance, offset
- [ ] P4.8 Rate-limit emulation (optional strict mode: 30 msg/s, 1 msg/s per chat, 20/min per group)

## P5 — Client (web)

- [ ] P5.1 App shell: routing, state, TL session client, theming (light/dark), RTL support
- [ ] P5.2 Chat list + chat view (virtualized), Telegram-like rendering of all message types
- [ ] P5.3 Composer: text + formatting, media, commands menu, reply keyboards, deep links
- [ ] P5.4 Inline keyboards, callback answers (toasts/alerts), inline mode results UI
- [ ] P5.5 Groups, channels, forums, admin tools, join requests, invite links
- [ ] P5.6 Mini App host: real webview + emulated `Telegram.WebApp` bridge, signed initData
- [ ] P5.7 Payments & Stars checkout simulator (shipping, pre-checkout, success/failure)
- [ ] P5.8 Multi-user: quick act-as switcher and split view (same chat, several users)
- [ ] P5.9 Dev tools: request log, update inspector (TL ⇄ Bot API side by side), webhook console
- [ ] P5.10 Dev tools: method playground (forms generated from the spec, "copy as curl/PHP/Python/TS")
- [ ] P5.11 Dev tools: entity/DB browser, fault & clock controls, bot connection status
- [ ] P5.12 Dev tools: trace export/import, replay/redeliver updates, response-time metrics
- [ ] P5.13 Command palette (Ctrl+K) and keyboard-first navigation

## P6 — Desktop (bunium)

- [ ] P6.1 bunium shell: sidecar server lifecycle, `bunium://` UI, tray with server status
- [ ] P6.2 Native menus, notifications for bot messages, Mini Apps in `<bunium-webview>`
- [ ] P6.3 Packaging: Windows, macOS, Linux (deb/rpm/AppImage)
- [ ] P6.4 Auto-update and release CI
- [ ] P6.5 Upstream fixes to bunium as found (committed in the bunium repo)

## P7 — Testing ecosystem

- [ ] P7.1 Headless mode (`simula serve`), Docker image
- [ ] P7.2 SDK: TypeScript
- [ ] P7.3 SDK: PHP (LaraGram)
- [ ] P7.4 SDK: Python
- [ ] P7.5 YAML scenario runner (language-agnostic tests, JUnit output for CI)
- [ ] P7.6 Session recorder: record UI interactions → export as scenario or SDK test code
- [ ] P7.7 Client automation API (drive the UI remotely, screenshots via bunium)
- [ ] P7.8 Load generator: N simulated users, bot latency metrics

## P8 — MTProto wire protocol

- [ ] P8.1 Transports: TCP abridged/intermediate/padded, WebSocket
- [ ] P8.2 Auth key exchange (DH), server RSA key export for clients
- [ ] P8.3 AES-256-IGE encryption, sessions, salts, acks, containers, gzip
- [ ] P8.4 `updates.getDifference` / `getChannelDifference`
- [ ] P8.5 Verified with Telethon, GramJS, Pyrogram, MadelineProto (custom DC config)

## P9 — Beyond bots

- [ ] P9.1 Broaden TL coverage towards a full client simulation

---

## What's new compared to v1

| Area | v1 | v2 |
|---|---|---|
| Structure | 23k-line React component, 5k-line Rust handler files | one responsibility per file, generated registries |
| Specs | scraper generated types only | types, validation, forms, dispatch, coverage, diffs all generated |
| Layering | Bot API handlers write straight to SQLite | Bot API ⇄ MTProto core, like real Telegram |
| Message IDs | global | per-account boxes for private/basic groups, per-channel otherwise |
| Isolation | one world | sandboxes (in-memory per test), snapshots, fixtures |
| Testing | UI only | Control API + SDKs + YAML scenarios + recorder |
| Mini Apps | not rendered | real webview with emulated `Telegram.WebApp` bridge |
| Debugging | logs, webhook list, diff | + TL ⇄ Bot API inspector, playground, faults, clock, replay, metrics |
| Desktop | planned (Electrobun) | bunium, sidecar server |
