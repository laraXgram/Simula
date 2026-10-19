# Simula v2 Roadmap

Full rewrite of Simula (v1 lives on `master`). Design: [docs/architecture.md](docs/architecture.md).
Every finished task is logged in [docs/worklog.md](docs/worklog.md) and committed on the
`v2` branch with its task ID in the commit message.

**Status legend:** `[x]` done · `[~]` in progress · `[ ]` planned · `[-]` dropped

**Targets:** Bot API **10.3** · MTProto layer from tdlib `telegram_api.tl`

**Current focus:** M5 — media and files. M4 is done: every kind of chat, admin rights,
pins, invite links, group upgrades, replies across chats and forums work end to end
(core, Bot API, client). The client becomes a plugin host later (M7, P10); new UI is
shaped for it now.

## Progress at a glance (2026-10-19, after M4)

| | Done | In progress | Planned | Total |
|---|---:|---:|---:|---:|
| Milestones | 4 | 0 | 3 | 7 |
| Tasks, all phases | 23 | 18 | 45 | 86 |

| Phase | Done | In progress | Planned | |
|---|---:|---:|---:|---|
| P0 Foundations | 4 | 0 | 0 | ██████████ complete |
| P1 Specs & codegen | 10 | 0 | 0 | ██████████ complete |
| P2 Core engine | 4 | 5 | 3 | ███▓▓▓▓░░░ |
| P3 Bot API gateway | 3 | 4 | 3 | ███▓▓▓▓░░░ |
| P4 Control API | 1 | 4 | 3 | █▓▓▓▓▓░░░░ |
| P5 Client (web) | 1 | 4 | 8 | █▓▓▓░░░░░░ |
| P6 Desktop | 0 | 0 | 5 | ░░░░░░░░░░ |
| P7 Testing ecosystem | 0 | 1 | 7 | ▓░░░░░░░░░ |
| P8 MTProto wire | 0 | 0 | 5 | ░░░░░░░░░░ |
| P9 Beyond bots | 0 | 0 | 1 | ░░░░░░░░░░ |
| P10 Plugins | 0 | 0 | 10 | ░░░░░░░░░░ |

█ done · ▓ in progress · ░ planned (bars by share of the phase's tasks)

**API coverage** ([docs/coverage.md](docs/coverage.md)): Bot API 10.3 — 48 of 185 methods
(25.9%; 36 earlier the same day), all of "Getting updates"; MTProto layer 229 — 58 of
813 functions (only what bots and the client need is planned). Most remaining Bot API methods
are waves on top of the pipelines that exist now (send, edit, chats, members), so
coverage keeps growing by method families.

**Works today**: private chats, basic groups, supergroups and channels between users and
bots; long polling and webhooks; HTML/Markdown/MarkdownV2 with Telegram's errors; inline
and reply keyboards, callback queries, replies with quotes, edits, deletes, commands with
scopes, deep links, offline link previews, privacy mode, membership updates; admin
rights, restrictions, bans, default permissions; pinned messages; invite links and join
requests; upgrading groups to supergroups; replies to messages in other chats; forums
with topics and reply threads; Control API with act-as and a virtual clock; the web
client; a Docker image. 164 Rust tests plus client tests in CI.

---

## Milestones

Phases below are the map; milestones are the order of work. Each milestone is a
vertical slice through several phases, so the architecture is exercised end to end early.

- [x] **M1 — first message end to end**: a user writes to a bot, the bot gets it via
  `getUpdates`, replies with `sendMessage`, the user receives the reply.
  Touches P2.1, P2.2, P2.4, P2.5, P2.7, P2.8, P2.9 (partial), P3.1, P3.2, P4.1, P4.4 (partial),
  then P1.9–P1.10 once handler files exist.
- [x] **M2 — webhooks, formatting, keyboards, callbacks** (P3.3, P3.4, P3.6, wave 1 complete):
  formatting, keyboards, replies, edits, deletes, callback queries, webhooks, commands
- [x] **M3 — client shell** talking TL over WebSocket (P5.1–P5.4): session gateway, web client;
  media, request buttons and inline results follow their core phases (P2.6, P2.11)
- [x] **M4 — groups and channels** (P2.3, P2.10, P3.8, P5.5)
  - [x] basic groups: create, members, rename, leave; privacy mode; `my_chat_member` /
    `chat_member`; group commands; client support
  - [x] supergroups and channels: per-channel boxes, channel pts, `updateNewChannelMessage`,
    channel posts, public usernames; client support
  - [x] admin rights, bans and restrictions, default permissions, invite links, join
    requests; client support
  - [x] pinned messages in every kind of chat; client support
  - [x] forums (topics) and reply threads, replies to other chats, upgrading a group to
    a supergroup; client support
- [ ] **M5 — media and files** (P2.6, P3.5): `InputFile` pipeline, photos, documents,
  albums, file downloads; media in the client
- [ ] **M6 — inline mode, polls, reactions, payments** (P2.11, P5.4, P5.7)
- [ ] **M7 — plugin platform** (P10): the client becomes a plugin host (activity bar,
  views, panels, commands, status bar); the dev tools (P5.9–P5.12) are written as
  plugins and installed by default. Comes after the client and server are complete
  enough (M4–M6), but new client UI is shaped for it from now on

---

## P0 — Foundations

- [x] P0.1 `v2` branch, v1 sources removed (kept on `master`)
- [x] P0.2 Architecture doc, roadmap, worklog
- [x] P0.3 Workspace skeleton: Cargo workspace, Bun workspace, lint/format (rustfmt, clippy, biome)
- [x] P0.4 CI: build, test, lint, "generated code is fresh" check

## P1 — Specs & code generation

- [x] P1.1 Bot API scraper → `spec/botapi/api.json` (methods, types, unions, return types)
- [x] P1.2 Constraint extraction from descriptions (lengths, ranges, enums, defaults, byte limits)
- [x] P1.3 `overrides.json` for manual corrections, applied on top of the scrape
- [x] P1.4 TL parser → `spec/tl/*.json` (vendored `telegram_api.tl`, `mtproto_api.tl`, CRC-verified)
- [x] P1.5 Spec diff: report what changed between two Bot API versions / TL layers
- [x] P1.6 Rust emitter: TL types + binary codec + JSON-TL codec (`simula-tl`)
- [x] P1.7 Rust emitter: Bot API types, method param structs, validators (`simula-botapi-types`)
- [x] P1.8 TypeScript emitter: TL + Bot API types for the client and SDKs
- [x] P1.9 Registry emitters: TL dispatch table, Bot API method table, stubs for new methods
- [x] P1.10 Coverage report: implemented / partial / stub per method, generated from registries → [docs/coverage.md](docs/coverage.md)

## P2 — Core engine (MTProto semantics)

- [x] P2.1 Storage: SQLite per sandbox, migrations, in-memory mode
- [x] P2.2 Accounts (users, bots, tokens), peers, access hashes
- [x] P2.3 Chats: private, basic group, supergroup, channel, forum; members, rights, bans
- [x] P2.4 Message boxes (per-account vs per-channel IDs), canonical TL storage, presenters
- [~] P2.5 Update engine: pts/qts/seq, channel pts, `Updates` shapes, difference log
- [ ] P2.6 Files: storage, file references, thumbnails metadata
- [~] P2.7 Virtual clock and scheduler (timers for polls, live locations, bans, scheduled posts); waits for answers from other accounts (callback queries)
- [~] P2.8 Event bus (RPC calls, updates, deliveries, logs)
- [~] P2.9 TL handlers, wave 1: users/bots, messages send/edit/delete/forward, callbacks
- [~] P2.10 TL handlers, wave 2: chats/channels admin, invites, join requests, forums
  (done: groups, supergroups, channels, admins, bans, pins, invites, join requests,
  forums, group upgrades; left: slow mode, banning sender chats, participant filters,
  `getExportedChatInvites`)
- [ ] P2.11 TL handlers, wave 3: inline, payments/stars, stickers, games, polls, reactions
- [ ] P2.12 TL handlers, wave 4: business, stories, gifts, checklists, suggested posts, boosts, verification

## P3 — Bot API gateway

- [~] P3.1 HTTP layer: all body formats, token auth, method routing, error model, file downloads
- [x] P3.2 Update queue, `getUpdates` long polling (offset/limit/timeout/allowed_updates, 409 conflict)
- [x] P3.3 Webhooks: secret token, max_connections, retries/backoff, webhook info, reply-in-response (HTTP delivery; see worklog)
- [x] P3.4 Text parsers: HTML, Markdown, MarkdownV2 with Telegram's exact errors (tdlib ports, wired into sends and quotes)
- [ ] P3.5 InputFile pipeline: multipart, `attach://`, file_id reuse, URLs, local mode paths
- [~] P3.6 Send pipeline shared by all `send*` methods (done: text, keyboards, replies, options); edit pipeline (done: text, keyboards)
- [~] P3.7 Converters TL ⇄ Bot API (declarative where possible)
- [~] P3.8 Method waves mirroring P2.9–P2.12 until 100% of Bot API 10.3 is covered
  (wave 1, groups, channels, members, pins, invites, forums: 48 of 185)
- [ ] P3.9 Declarative mappings for simple methods (derived from patterns found in P3.8)
- [ ] P3.10 Conformance suite: golden tests against recorded real-Telegram responses

## P4 — Control API

- [~] P4.1 Server skeleton under `/_simula/v1`, generated OpenAPI
- [ ] P4.2 Sandboxes: create/destroy/list, in-memory, snapshots, fixtures (YAML seed files)
- [~] P4.3 Entity management: users, bots (BotFather-like settings), groups, channels, members
- [~] P4.4 Act-as endpoints over TL user sessions (send, press, vote, pay, join, react, …)
- [~] P4.5 Inspection: event stream (WS), request logs, update queues, webhook deliveries
- [ ] P4.6 Fault injection: errors, 429 + retry_after, latency, dropped updates, webhook failures
- [x] P4.7 Clock control: freeze, advance, offset
- [ ] P4.8 Rate-limit emulation (optional strict mode: 30 msg/s, 1 msg/s per chat, 20/min per group)

## P5 — Client (web)

- [x] P5.1 App shell: routing, state, TL session client, theming (light/dark), RTL support
- [~] P5.2 Chat list + chat view (paged; virtualized rendering pending), Telegram-like rendering of all message types;
  messages whose link would get a preview on Telegram show that they have one (Simula is
  offline and builds no previews)
- [~] P5.3 Composer: text + formatting, media, commands menu, reply keyboards, deep links (media and
  request buttons pending: P2.6, P5.5)
- [~] P5.4 Inline keyboards, callback answers (toasts/alerts), inline mode results UI (inline results
  with P2.11)
- [~] P5.5 Groups, channels, forums, admin tools, join requests, invite links (done:
  groups, supergroups, channels, forums, upgrades, adding admins, pins, invite links,
  joining; left: member lists and per-member rights, handling join requests)
- [ ] P5.6 Mini App host: real webview + emulated `Telegram.WebApp` bridge, signed initData
- [ ] P5.7 Payments & Stars checkout simulator (shipping, pre-checkout, success/failure)
- [ ] P5.8 Multi-user: quick act-as switcher and split view (same chat, several users)
Dev tools P5.9–P5.12 are built as plugins on the plugin platform (P10), bundled and
installed by default.

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

- [~] P7.1 Headless mode (`simula serve`), Docker image (`Dockerfile`, `compose.yaml`,
  `--web-dir`; every stage checked locally, the image itself is built in CI)
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

## P10 — Plugin platform

Design: [architecture.md § Plugins](docs/architecture.md#plugins).

- [ ] P10.1 Manifest (`simula-plugin.json`) schema, validation, `engines.simula` versioning
- [ ] P10.2 Shell slots: activity bar, sidebar view containers, bottom panel, status bar,
  main-area tabs (the chat list becomes the first built-in view)
- [ ] P10.3 Plugin host: one Web Worker per plugin, typed RPC, activation events,
  disposables, crash isolation
- [ ] P10.4 Webviews: sandboxed iframes with the `postMessage` bridge, theming variables
- [ ] P10.5 `@simula/plugin-api`: Control API, TL calls as the current account, event bus,
  commands, menus, notifications, storage, settings
- [ ] P10.6 Server side: `<data-dir>/plugins`, `/_simula/plugins/<id>/` static serving,
  Control API install / enable / disable / remove
- [ ] P10.7 Extensions view: browse installed, install from file or URL, enable, disable
- [ ] P10.8 Built-in plugins: request log, update inspector, webhook console, playground,
  DB browser, faults and clock, trace export (P5.9–P5.12)
- [ ] P10.9 Plugin dev kit: `create-simula-plugin` scaffold, dev mode with reload, docs
- [ ] P10.10 Desktop: plugins in bunium, native menus and notifications contributed by plugins

## Explicit gaps (answer `501 Not implemented in Simula` today)

Every one of these must end up fully supported; each is listed under the task that
removes it. A 501 is a placeholder, never a final answer.

| Gap | Where it answers 501 | Removed by |
|---|---|---|
| Inline messages (`inline_message_id` in edits), inline mode | edit pipeline | P2.11 + P3.8 (inline wave) |
| Business connections (`business_connection_id`) | send / edit pipelines | P2.12 + P3.8 |
| Private-chat topics (bots with topics), `direct_messages_topic_id`, forums as tabs | send pipeline, `toggleForum` | P2.12 + P3.8 |
| `getForumTopicIconStickers` | Bot API | M5 (stickers) |
| Ephemeral messages (`ephemeral_message_parameters`, replies to them) | send pipeline, replies | P2.12 + P3.8 |
| Suggested posts (`suggested_post_parameters`) | send pipeline | P2.12 + P3.8 |
| Replies to checklist tasks and poll options | core replies | P2.11 / P2.12 |
| `login_url` buttons naming another bot | keyboard parsing | P3.8 |
| Game and password callback buttons | `messages.getBotCallbackAnswer` | P2.11 |
| Editing message media, scheduled and quick-reply messages | `messages.editMessage` | P2.11, P3.5 |
| Banning chats (`banChatSenderChat`), contacts/mentions participant filters | core channels | P2.10 |
| Joining via a link found in a message (`inputChannelFromMessage`) | core peers | P2.10 |
| HTTPS webhook delivery (today: plain HTTP to the same host and port) | webhook sender | P3.3 follow-up (TLS client) |
| Message effects checked against Telegram's catalog | core sends | P2.11 |

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
| Extensibility | none | plugin host like VS Code; the dev tools are plugins |
| Desktop | planned (Electrobun) | bunium, sidecar server |
