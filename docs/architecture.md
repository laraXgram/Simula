# Architecture

Simula v2 is an offline Telegram simulator. Bots talk to it exactly as they talk to
`api.telegram.org`; developers and test suites drive simulated users through APIs; a
Telegram-like client (web + desktop) sits on top of those same APIs.

This document records the load-bearing decisions. The plan and progress live in
[ROADMAP.md](../ROADMAP.md); the history of work lives in [worklog.md](worklog.md).

## Principles

1. **One responsibility per file.** One Bot API method per file, one MTProto method per
   file, one converter per type family. Registries that tie files together are generated.
2. **Specs are the source of truth.** The Bot API HTML docs and tdlib's
   `telegram_api.tl` are parsed into committed JSON specs. Types, validation, dispatch
   tables, forms, docs and coverage reports are generated from them.
3. **Telegram's own layering.** A core engine speaks MTProto (TL) semantics. The Bot API
   is a gateway on top of it that translates, the way `telegram-bot-api` sits on TDLib.
   The client is just another MTProto user session. This is what lets us simulate full
   clients later without redesigning.
4. **Everything is API-driven.** Nothing the UI can do is UI-only. Every action goes
   through the TL session API or the Control API, so any language can script it.
5. **Fidelity over convenience.** Same error codes and descriptions, same update
   semantics, same message-ID boxes, same file-ID behaviour as real Telegram. Where
   Simula can't be faithful it says so explicitly (e.g. `501 Not implemented in Simula`).

## Topology

```
          bots (any language)            test suites / SDKs          Simula client
                 │                               │                (web + bunium desktop)
     HTTPS-like Bot API                  Control API (REST+WS)          │
   /bot<token>/<method>                   /_simula/v1/...        TL session (JSON over WS)
   getUpdates │ webhooks ▲                       │                       │
                 ▼                               ▼                       ▼
        ┌────────────────────┐        ┌──────────────────┐     ┌──────────────────┐
        │  simula-botapi     │        │  simula-control  │     │  session gateway │
        │  translate BotAPI  │        │  sandboxes, act- │     │  (later also the │
        │  ⇄ TL, delivery    │        │  as, inspect,    │     │  MTProto wire    │
        └─────────┬──────────┘        │  faults, clock   │     │  transport)      │
                  │ TL calls as bot   └────────┬─────────┘     └────────┬─────────┘
                  ▼                            ▼                        ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  simula-core: accounts, peers, message boxes, update engine (pts/qts),│
        │  files, TL method handlers, virtual clock, event bus                  │
        └───────────────────────────────┬──────────────────────────────────────┘
                                        ▼
                               SQLite (one DB per sandbox)
```

## Repository layout

```
spec/                     source-of-truth inputs (committed)
  botapi/api.json           parsed Bot API spec (methods, types, constraints)
  botapi/overrides.json     manual corrections applied on top of the scrape
  botapi/names.json         names for the docs' anonymous unions (e.g. ReplyMarkup)
  tl/*.tl                   vendored tdlib schemas (telegram_api, mtproto_api)
  tl/*.json                 parsed TL schemas; tl/source.json records layer + tdlib commit
tools/codegen/            Bun/TS: scrape, parse, emit, diff, coverage (build-time only)
crates/                   Rust workspace (runtime)
  tl/                       simula-tl: generated TL types + binary/JSON codecs
  botapi-types/             simula-botapi-types: generated Bot API types + validators
  core/                     simula-core: domain engine + TL method handlers
  botapi/                   simula-botapi: Bot API gateway
  control/                  simula-control: Control API + OpenAPI
  transport/                simula-transport: MTProto wire protocol (later phase)
  server/                   simula-server: the binary
packages/
  schema/                   @simula/schema: generated TL + Bot API TypeScript types
apps/
  web/                      React + TS client
  desktop/                  bunium shell (spawns simula-server as a sidecar)
sdks/                     generated/handwritten test SDKs (later phase)
docs/                     architecture, worklog, guides
```

## Language choices

- **Runtime is Rust** (tokio + axum + rusqlite). Performance was the deciding criterion.
- **Client is React + TypeScript** (Vite), packaged for desktop with bunium.
- **Codegen is TypeScript on Bun.** It runs only at build time, so it has no runtime cost,
  and one generator can emit Rust, TypeScript, OpenAPI and docs from the same model.
- Generated code is committed (with an `@generated` header) so diffs on each Telegram
  update are reviewable. CI fails if generated output is stale.

## Core engine (simula-core)

- **Accounts** are users and bots; both are MTProto accounts. Bots have tokens.
- **Peers**: users, basic groups (`chat`), channels/supergroups/forums (`channel`).
- **Message boxes** mirror Telegram: private chats and basic groups keep per-account
  message IDs (each participant has their own box and `pts`); channels and supergroups
  share one box with a per-channel `pts`. A bot's `message_id` therefore differs from
  the user's for the same private message, exactly like production.
- **Messages are stored as canonical TL objects** (JSON-encoded TL, keyed by constructor
  name) plus indexed columns. New TL fields need no migration; per-viewer rendering
  (`out`, `peer_id`, access hashes) happens in presenters.
- **Update engine** keeps per-account `pts`/`qts`/`seq` and per-channel `pts`, and
  produces the same `Updates` shapes as Telegram (and later `getDifference`).
- **TL handlers**: one file per TL function, signature
  `fn(&mut Ctx, req) -> Result<Resp, RpcError>`. The dispatch table is generated;
  unimplemented functions return a typed `NOT_IMPLEMENTED` error.
- **Virtual clock** per sandbox (real / frozen / offset) drives every timestamp and timer.
- **Event bus** emits every RPC call, update, delivery and log line for inspection.

## Bot API gateway (simula-botapi)

- Accepts GET/POST with query, urlencoded, JSON and multipart bodies, like the official
  server; case-insensitive method names; `/file/bot<token>/<path>` downloads.
- **Per-method files** hold only the translation to TL calls; parameter structs and
  validation (required/optional, unions, enums, lengths, ranges parsed from the docs)
  are generated.
- **Shared pipelines** capture cross-cutting behaviour once: a send pipeline for all
  `send*` methods (chat resolution, reply parameters, markup, business connection,
  thread/topic, effects, paid broadcast), an edit pipeline, a media/InputFile pipeline.
  When Telegram adds a common parameter, one place changes.
- **Converters** TL ⇄ Bot API types live in `convert/`. Simple field-to-field mappings are
  declarative and generated; the rest are handwritten.
- **Delivery**: per-bot update queue with `update_id`, `allowed_updates`, 24h retention;
  long-polling `getUpdates` with conflict semantics (409); webhooks with `secret_token`,
  `max_connections`, retries/backoff, `pending_update_count`, `last_error_*`, and replies
  carried in the webhook response body.
- **Text**: HTML / Markdown / MarkdownV2 parsers producing Telegram's entities and its
  exact `can't parse entities` errors.

## Control API (simula-control)

REST + WebSocket under `/_simula/v1`, described by a generated OpenAPI document.

- Sandboxes: create/destroy isolated worlds (in-memory for tests), snapshots, fixtures.
- Entities: users, bots, groups, channels, memberships, business connections, balances.
- Act-as: thin helpers over TL user sessions (send, press button, vote, pay, …).
- Inspect: event stream, request logs, update queue, webhook deliveries, DB browser.
- Faults: inject errors/latency/429s per method or globally; webhook failure modes.
- Clock: freeze, advance, set offset.

## Client

- The client authenticates as a simulated user and speaks TL over WebSocket; the dev
  tools use the Control API. It holds no business logic of its own.
- Desktop builds wrap the same web app in bunium; the Bun main process starts
  `simula-server` as a sidecar and loads the UI over `bunium://`.

## Plugins

The client is a plugin host, in the way VS Code and JetBrains IDEs are: anyone can write
a plugin, install it, and get an icon in the activity bar with its own views. The dev
tools (request log, update inspector, webhook console, playground, DB browser, faults
and clock, trace export) are **plugins too**, bundled and installed by default; they use
only the public plugin API, so a third-party plugin can do everything they do.

- **Package**: a directory (or a `.simula-plugin` zip of one) with a
  `simula-plugin.json` manifest — `id` (`publisher.name`), `version`, `displayName`,
  `icon`, `engines.simula`, `main` (optional background script), `activationEvents`, and
  `contributes`, VS Code style.
- **Contribution points**: `activityBar` (an icon in the left bar opening a view
  container), `views` (inside a container, the sidebar or the bottom panel), `panels`,
  `commands` (in the command palette, P5.13, with keybindings), `menus` (chat header,
  message context menu, composer), `statusBar` items, `settings` (a schema; values edited
  in a generated settings page), `messageRenderers` later.
- **Runtime**, mirroring VS Code's extension host and webviews: a plugin's `main` runs in
  its own Web Worker and talks to the host over a typed RPC (`@simula/plugin-api`); views
  are sandboxed iframes (webviews) that get the same API through a `postMessage` bridge.
  Plugins never touch the host's DOM or state directly, so a broken plugin can't break
  the client, and plugins can be written with any web framework.
- **API surface** (`@simula/plugin-api`, versioned, typed from the generated schemas):
  the Control API (accounts, act-as, clock, faults, inspection streams), TL calls as the
  current account, the event bus (RPC calls, updates, deliveries, webhook attempts),
  commands, notifications, per-plugin storage, the current chat/account selection, and
  `Disposable`s for everything registered.
- **Installation**: the server keeps plugins in `<data-dir>/plugins/` and serves them at
  `/_simula/plugins/<id>/`; the Control API lists, installs (upload or URL), enables,
  disables and removes them (`/_simula/v1/plugins`); an Extensions view in the client is
  built on those endpoints. Built-in plugins ship in `plugins/` in this repo, can be
  disabled but not removed. The desktop app uses the same mechanism.
- **Shell layout** the host provides: activity bar · sidebar (chat list is the first
  view) · main area (chats; plugins may open editor-like tabs) · bottom panel · status
  bar. New client UI is placed with these slots in mind.

## Identifiers & encodings

- TL `long` is encoded as a JSON string in JSON-TL (64-bit safe for JS); Bot API keeps
  numbers, as Telegram does.
- File IDs are opaque base64url blobs encoding type, DC, id, access hash and
  file reference, with a stable `file_unique_id`, like production.
