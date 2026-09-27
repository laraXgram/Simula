# Worklog

Newest entries first. Each entry names the roadmap task(s) it closes; the matching commit
carries the same task ID in its message (`git log --grep "P1.1"`).

---

## 2026-09-28 — M2 (part 4) · Webhooks (P3.3)

- **`setWebhook` / `deleteWebhook` / `getWebhookInfo`** in telegram-bot-api's order and
  words (`process_set_webhook_query`, `do_set_webhook`, `JsonWebhookInfo`): one
  `setWebhook` with a URL per second (429, `retry_after` 1); "Webhook is already set"
  when nothing changes (only `allowed_updates` are taken); the old webhook is removed
  before the new URL is checked, so a bad URL leaves none; "invalid webhook URL
  specified", "secret token is too long", "secret token contains illegal characters";
  verification resolves the host ("bad webhook: Failed to resolve host: …") or takes
  `ip_address`; success responses carry telegram-bot-api's `description` ("Webhook was
  set", "Webhook was deleted", "Webhook is already deleted") — responses can now carry
  one. `max_connections` is clamped (default 40, 1–100), not rejected.
- **Delivery** (`webhook/sender.rs`, after `WebhookActor`): updates of one queue (a
  chat; a user's callback queries) go out in order, queues in parallel up to
  `max_connections`; `X-Telegram-Bot-Api-Secret-Token`; a failed update is retried at
  once, then after 2, 4, 8… s capped at a random 60–120 s, `Retry-After` honoured, and
  dropped when its next attempt would pass its 24-hour lifetime. Errors land in
  `last_error_date` / `last_error_message` with telegram-bot-api's texts ("Wrong
  response from the webhook: 500 Internal Server Error", "Connection refused", "Read
  timeout expired", "Webhook connection closed"). A 2xx answer carrying a method call
  runs as the bot (except `getX`, `setWebhook`, `deleteWebhook`, `close`, `logOut`),
  through the same decode and legacy-parameter path as HTTP requests.
- **Conflicts with polling**: `getUpdates` answers "Conflict: can't use getUpdates
  method while webhook is active; use deleteWebhook to delete the webhook first", and a
  pending long poll ends with "Conflict: terminated by setWebhook request".
- **Deliberate deviations**, documented in `webhook/mod.rs`: URLs follow
  telegram-bot-api's local mode (plain HTTP, any port and host, localhost included).
  Simula has no TLS client, so `https://` URLs are delivered over plain HTTP to the
  same host and port, as if TLS were terminated in front of the bot — this is what
  makes Telegraf's webhook mode work locally (it registers `https://` but serves plain
  HTTP). Webhooks live in memory (telegram-bot-api persists them); custom certificates
  are accepted and ignored.
- The HTTP client is hyper's own (already in the tree through axum); the only new
  crates are hyper's `want` and `try-lock`.
- **Test layout**: each crate's integration tests are now one binary
  (`crates/botapi/tests/gateway/`, `crates/core/tests/engine/`). Every test binary
  links the 190 MB TL library; with one binary per file, `target/` had grown to 5.4 GB
  on a nearly full disk. It is 1.5 GB after a clean build.
- **Verification**: gateway tests with a real HTTP receiver on a local port (delivery,
  secret header, method call in the response reaching the user, 500 then retry,
  "Connection refused", long poll ended by `setWebhook`, the rate limit, set/delete
  descriptions, `https://` delivered over HTTP). **Real library**: Telegraf 4.16 in
  webhook mode with a secret token; its in-response `sendMessage` (`webhookReply`)
  reaches the user. Workspace: 131 tests, clippy clean; regeneration writes 0 files.
- Coverage: Bot API 11/185, TL 8/813.

## 2026-09-28 — M2 (part 3) · Callback queries (P2.9, P3.8, P4.4)

- **Waits in the core** (`waits.rs`): some calls are answered later by another account
  (callback queries now; inline and pre-checkout queries later). A handler calls
  `ctx.defer(key, timeout_error)` inside its transaction, so the wait exists before the
  other side can see the query; `Sandbox::invoke` awaits it after commit and
  publication, off the sandbox thread, so the answering call can run; `ctx.answer`
  hands the answer over only once the answering transaction commits. Waits of a call
  whose transaction fails are cancelled. The timeout (default 15 s, an approximation of
  Telegram's) is per sandbox; tests shorten it.
- **Core**: `messages.getBotCallbackAnswer` checks the message and that it carries a
  callback button with that data (`MESSAGE_ID_INVALID`, `DATA_INVALID`), sends the bot
  `updateBotCallbackQuery` on the bot's own copy of the message, with a random query
  id and a per-(bot, chat) `chat_instance` derived like access hashes, and waits
  (`BOT_RESPONSE_TIMEOUT`). `messages.setBotCallbackAnswer` only accepts pending
  queries addressed to that bot (`QUERY_ID_INVALID`), texts up to 200 characters
  (`MESSAGE_TOO_LONG`) and, without games, only the bot's own `t.me` links
  (`URL_INVALID`).
- **Gateway**: `callback_query` updates (telegram-bot-api's `JsonCallbackQuery`: the
  message with its own `reply_to_message`, or an `InaccessibleMessage` with date 0)
  and `answerCallbackQuery` (`cache_time` clamped to 30 days like `get_integer_arg`,
  unparsable ids become 0 like `td::to_integer`). The presenter now fetches any
  messages an update needs, in two rounds (the messages, then what they reply to).
- **tdlib error paths**: `ErrorContext` now names the path a call takes. Only message
  sends go through tdlib's `on_send_message_fail` rewrites; edits, deletes and callback
  answers get the server's errors unchanged, as with real tdlib.
- **Control API**: `POST /accounts/{id}/press` presses a callback button by `data` or
  by button `text` and returns the bot's answer.
- **Verification**: core tests (answer delivered to the waiting press, second answer
  rejected, timeout, wrong data), gateway tests (the whole round trip over HTTP,
  answer errors, `t.me` answer URLs, timeouts). **Real library**: a Telegraf counter
  bot (`bot.action` → `editMessageText` with a new keyboard → `answerCbQuery`) driven
  by presses through the Control API: every press returns the bot's answer and the
  user's message shows the edited text and button. Workspace: 126 tests, clippy clean.
- Coverage: Bot API 10/185, TL 8/813.

## 2026-09-28 — M2 (part 2) · Edits and deletes (P2.9, P3.6)

- **Core**: `messages.editMessage` and `messages.deleteMessages` for private chats.
  - Edits change every copy (found through `global_id`); each owner gets
    `updateEditMessage` with its own pts. Only the author may edit
    (`MESSAGE_AUTHOR_REQUIRED`), users within 48 hours (`MESSAGE_EDIT_TIME_EXPIRED`),
    bots their own messages any time; only inline keyboards (`REPLY_MARKUP_INVALID`);
    an edit that changes nothing is `MESSAGE_NOT_MODIFIED`. Entities are rebuilt like
    on send. Omitting the keyboard removes it, as on Telegram; a keyboard-only edit
    keeps the text and preview placement and is marked `edit_hide`.
  - Deletes remove the caller's copies, and with `revoke` the other side's too; each
    affected box gets `updateDeleteMessages` (pts advanced by the message count).
    Unknown ids are ignored; bots can't delete messages older than 48 hours
    (`MESSAGE_DELETE_FORBIDDEN`).
- **Gateway**: `editMessageText`, `editMessageReplyMarkup`, `deleteMessage`,
  `deleteMessages`, built on an **edit pipeline** (`edit.rs`) and a delete step
  (`delete.rs`), with telegram-bot-api's `check_message(s)` as `lookup.rs`.
  - Same order and texts as the real stack: "message identifier is not specified",
    "message to edit not found", tdlib's "message can't be edited" (not the author, or
    the message carries a reply keyboard), "there is no text in the message to edit",
    "inline keyboard expected", the rewritten "message is not modified: …",
    "message to delete not found", "message can't be deleted for everyone" (bots,
    after 48 hours), "too many message identifiers specified".
  - `deleteMessages` skips missing messages, as telegram-bot-api's `allow_empty` does.
  - Edit results come without `reply_to_message` (telegram-bot-api renders edits with
    `need_reply = false`); bots always see `edit_date` (tdlib ignores `edit_hide` for
    bots).
  - Users' edits reach bots as `edited_message`; deletions in private chats produce
    no Bot API update, as on Telegram.
  - `message_ids` joins the `handler_validates` fields (telegram-bot-api's
    `get_message_ids` reports its own errors).
- Inline messages (`inline_message_id`) and business connections answer 501 until
  inline mode and business accounts exist.
- **Verification**: core tests (`tests/edit_delete.rs`: every copy edited with its own
  pts, not-author and not-modified errors, hidden keyboard-only edits, delete for me vs
  for everyone, the 48-hour bot limit on a virtual clock) and gateway tests over HTTP
  (text and keyboard edits, keyboard removal, error texts, users' edits as
  `edited_message`, deletes on both sides). Workspace: 121 tests, 0 warnings, clippy
  clean; regeneration writes 0 files; `verify:ts` passes.
- Coverage: Bot API 9/185, TL 6/813.

## 2026-09-28 — M2 (part 1) · Send pipeline: formatting, keyboards, replies (P3.4, P3.6)

- **Send pipeline** (`crates/botapi/src/send/`), shared by every future `send*` method,
  in telegram-bot-api's `do_send_message` order: parse the common options, reply
  parameters and reply markup → resolve the chat → check the replied message → "tdlib"
  finishes the markup and text → TL call → the bot's copy of the message, presented.
  `sendMessage` is now a thin handler on top of it.
- **Formatting wired in** (P3.4 done): `text::parse` (telegram-bot-api's
  `get_formatted_text`, before the chat is resolved) and `Formatted::fix` (tdlib's
  `fix_formatted_text`, after), so errors come in the real order. Mentions by id are
  sent the way tdlib does for bots: `inputUser` with access hash 0, which the core now
  accepts from a bot only for users it has met.
- **Keyboards**: a port of telegram-bot-api's `get_reply_markup` (which fields make which
  button, in its priority order; lenient styles; its error texts) and tdlib's
  `get_reply_markup`/`get_input_reply_markup` (empty buttons skipped, 12 per row and
  300 in all, request buttons only in private chats, "personal" only outside them,
  `check_link` for URL / login / Web App buttons with tdlib's messages). The core
  normalizes input button forms into stored ones (`inputInlineButtonTypeUserProfile` →
  `inlineButtonTypeUserProfile`, URL-auth button ids, peer requests) and rejects
  callback data over 64 bytes with `BUTTON_DATA_INVALID`. Bots see inline keyboards on
  messages exactly as telegram-bot-api renders them.
- **Replies**: `reply_parameters` with quotes (parsed, trimmed, only quote-allowed entity
  kinds, position shifted like tdlib's `MessageQuote`), `allow_sending_without_reply`,
  "message to be replied not found". In the core, each copy of a reply points at its
  owner's copy of the target (via `global_id`); quotes are located in UTF-16 units,
  nearest the given position (`QUOTE_TEXT_INVALID`, `REPLY_MESSAGE_ID_INVALID`).
- **`reply_to_message` in updates**: sandbox update listeners are now async and awaited
  before a call returns, so the gateway fetches replied messages with the new
  `messages.getMessages` handler, as telegram-bot-api has tdlib do — and updates are
  still fetchable the moment the causing call returns.
- **Options**: `protect_content` → `noforwards` → `has_protected_content`;
  `disable_notification`; `message_effect_id` → `effect_id`; `link_preview_options`
  (`is_disabled` → `no_webpage`, `show_above_text` → `invert_media`). Received text
  follows tdlib's rule: a link and no preview reads as `is_disabled` — Simula is
  offline and never builds previews, which is what Telegram shows when a preview
  can't be fetched.
- **Legacy parameters** telegram-bot-api still honours are rewritten before decoding
  (`reply_to_message_id` + `allow_sending_without_reply`, `disable_web_page_preview`),
  so older bots keep their replies instead of silently losing them.
- **Spec-level fixes** found on the way:
  - `KeyboardButton` may be a bare string (the docs say so; Telegraf's
    `Markup.keyboard([...])` sends exactly that). New `string_shorthand` type override:
    Rust accepts both forms via a `remote = "Self"` derive plus a small visitor, TS
    types it as `KeyboardButton | string`.
  - New `handler_validates` field override for fields the server parses by hand:
    `parse_mode` ("html", "none" are valid), button `style` (case-insensitive,
    "default"), `callback_data` (the error is `BUTTON_DATA_INVALID`). Generated
    validators skip them; TS keeps the documented literal types.
- **Explicitly not simulated yet** (answer 501): ephemeral messages, suggested posts,
  business connections, topics, replies to other chats / checklist tasks / poll
  options, `login_url` buttons naming another bot. Effects are stored but not checked
  against a catalog.
- **Verification**
  - `tests/send_pipeline.rs` over in-process HTTP: parse modes in any casing,
    `text_mention`, explicit entities, error order (parse errors before "chat not
    found", whitespace after), inline keyboards round trip, `BUTTON_DATA_INVALID`,
    string reply keyboards from a form body, replies both ways with quotes, missing
    reply targets, legacy parameters, protected content / effects / link previews.
    Unit tests for the markup port, `check_link` errors, legacy rewriting, the
    link-preview rule and quote location. Workspace: 112 tests, 0 warnings, clippy
    clean; `verify:ts` accepts all 3242 Rust samples; regeneration writes 0 files.
  - **Real library**: an unmodified Telegraf 4.16 bot against the `simula` binary —
    `Markup.keyboard([["Yes","No"]])` arrives as a reply keyboard, `replyWithHTML` with
    `reply_parameters` and an inline keyboard lands in the user's box with a bold
    entity and a reply header pointing at the user's own copy, and the user's reply to
    it reaches the bot with `reply_to_message` filled in.
- Next in M2: edits and deletes (`editMessageText`, `editMessageReplyMarkup`,
  `deleteMessage(s)`, edit pipeline), callback queries (with a wait mechanism in the
  core), webhooks, bot commands.

## 2026-09-27 — M2 (in progress, paused) · Text formatting (P3.4)

Stopped at the maintainer's request partway through wiring formatting into
`sendMessage`. Committed as work in progress.

- **Done and tested** — new `crates/botapi/src/text/` (tdlib's role in the gateway):
  - line-by-line ports of tdlib's `parse_markdown` (legacy), `parse_markdown_v2` and
    `parse_html` from `MessageEntity.cpp`, with their exact error texts;
  - link normalization ported from `LinkManager::check_link_impl` + `parse_url`
    (`http://` default, lowercase host, `/` path, spaces percent-encoded, `tg://user`,
    `tg://emoji`, `tg://time`), tdlib's entity sort priorities, date formats;
  - `prepare()`: telegram-bot-api's `get_formatted_text` (32 KB limit, `parse_mode`,
    explicit `entities`) plus tdlib's validation and trimming, errors formatted as the
    real Bot API does (`Bad Request: can't parse entities: …`);
  - tests: **all 207 cases of tdlib's own parser suite** (121 MarkdownV2, 86 HTML, 56 of
    them error cases), extracted automatically into `text/tdlib_cases.json`, pass; plus
    the reserved-character loop, legacy Markdown, and `prepare()` errors/trimming.
- **Done, compiles, tests not re-run:**
  - core: `inputMessageEntityMentionName` → `messageEntityMentionName` with an access
    hash check (`ENTITY_MENTION_USER_INVALID`); message length measured in UTF-16 units;
  - Bot API output: `text_mention` (with the user), `custom_emoji`, `date_time`,
    `bank_card_number` entities.
- **Not done yet** (the scripted edit stopped here): `sendMessage` still rejects
  `parse_mode`/`entities`/`reply_markup`; the `prepare()` call, `BotCtx::tl_entities`
  and the `protect_content` / `link_preview_options` mapping still have to be wired in.
  Until then the `text` module builds with dead-code warnings.
- Next in M2 after that: keyboards, replies, edits/deletes, callback queries (with a
  wait mechanism in the core), webhooks, commands.

## 2026-09-27 — P0.4 · CI

- `.github/workflows/ci.yml` on pushes to `v2` and PRs, three jobs:
  - Rust: `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`,
    `cargo test --workspace`.
  - TypeScript: `bun install --frozen-lockfile`, `biome ci`, `bun run typecheck`.
  - Generated code: `bun run codegen generate` must leave the tree unchanged, then
    `bun run codegen verify:ts` (Rust JSON vs TypeScript types).
- Every step was run locally and passes (GitHub Actions itself can't run here; first
  real run happens when the branch is pushed).

## 2026-09-27 — P1.9, P1.10 · Registries, scaffolding, coverage

- **Adding a method = adding one file.** `bun run codegen registries` discovers
  `crates/botapi/src/methods/<method>.rs` and `crates/core/src/rpc/handlers/<ns>/<fn>.rs`
  and generates their `mod.rs` files: module lists, routing tables (`route`), and
  `IMPLEMENTED` lists. File names are checked against the spec, so a typo is a codegen
  error instead of silently dead code. Bot API handlers opt into clamping instead of
  rejecting documented limits with `pub const LIMITS: Limits = Limits::Clamp;`.
- `bun run codegen new:botapi <method>` / `new:tl <function>` scaffold a handler with the
  right parameter and return types from the spec, marked `// @stub` until implemented.
  Verified that scaffolds compile (`editMessageText` → `MessageOrTrue`,
  `messages.getHistory` → `messages.Messages`).
- `bun run codegen coverage` writes [coverage.md](coverage.md): per-section and
  per-method status for Bot API 10.3 (now 5/185) and the handled TL functions (3/813).
- `bun run codegen generate` regenerates everything from the committed specs;
  `all` scrapes the docs first. A full regeneration writes 0 files, so the committed
  output matches the specs (CI will enforce this).
- Mixed directories (handwritten handlers + generated `mod.rs`) are synced by a writer
  that only removes files carrying the `@generated` header.
- Build hygiene: incremental compilation is now off workspace-wide (its caches for crates
  built on the TL types took gigabytes on a small disk); building the whole workspace
  avoids duplicate feature variants of the 190 MB TL library. `target/` went from
  7.2 GB to ~1.5 GB. `cargo fmt` applied to all handwritten code.

## 2026-09-27 — M1 (part 2) · Bot API gateway, Control API, server — M1 done

- **`simula-botapi`** (P3.1, P3.2, P3.7): `/bot<token>/<method>` over GET/POST with query,
  JSON, urlencoded and multipart bodies; string values coerced by the generated parameter
  kinds; gzip/deflate/br/zstd request bodies; case-insensitive method names; 401/404 for
  unknown/malformed tokens; documented-but-unimplemented methods answer 501, unknown
  ones 404. Methods: `getMe`, `getUpdates`, `sendMessage` (plain text), `deleteWebhook`,
  `getWebhookInfo`. Converters TL → Bot API for users, chats, messages, entities, updates.
- **Update delivery**: per-bot queue (random first `update_id`, offsets incl. negative,
  `allowed_updates` with the documented opt-in kinds, 24 h retention), long polling,
  and `409 Conflict: terminated by other getUpdates request…` when a newer poll starts.
  Delivery runs synchronously in the sandbox's publish step (listeners), so when an
  action returns its updates are already fetchable — no flaky tests.
- **Errors reproduce the real chain**: MTProto error → tdlib's message (table from
  `MessagesManager.cpp`) → telegram-bot-api's formatting (`Client::fail_query_with_error`:
  code normalization, rewrites, prefix, lowercase rule). E.g. a bot writing first gets
  `403 Forbidden: bot can't initiate conversation with a user`; missing text gets
  telegram-bot-api's own `Bad Request: message text is empty`, whitespace-only text
  tdlib's `Bad Request: text must be non-empty`.
- **Automatic entities** (core): mentions, bot commands (only in chats with a bot),
  hashtags, cashtags ported from tdlib's matchers with tdlib's own test cases; scheme/
  `www.` URLs and emails (bare-domain detection with TLD list not ported yet); UTF-16
  offsets; explicit entities win on overlap.
- **`simula-control`** (P4.1, P4.3, P4.4, P4.5, P4.7) at `/_simula/v1`: create/list
  accounts, `POST /accounts/{id}/call` (**any TL function as any account**, JSON-TL in
  and out), `POST /accounts/{id}/send`, SSE streams (`/events`, `/accounts/{id}/updates`),
  clock (real / frozen / offset, advance).
- **`simula` binary** (`crates/server`): one port for both APIs, `--memory` or
  `--data-dir`, env overrides, graceful shutdown.
- TL codegen: accessors for fields shared by every constructor of a type
  (`MessageEntity::offset()`, `Message::id()`, 74 types).
- **Verification**
  - Gateway tests over in-process HTTP: `getMe` fields, full conversation (bot box ids
    1/2, lossless `message`/`from`/`chat` JSON), form and GET encodings, Telegram error
    texts, long polling wake-up, 409 conflict. Error translation unit-tested against
    the real descriptions.
  - **Real library**: an unmodified **Telegraf** bot ran against the `simula` binary —
    launch, long polling, `bot.start()`/`bot.command()` routing and echo of Persian text
    with emoji all worked. It first exposed two real gaps, both fixed: gzip-compressed
    request bodies and missing automatic `bot_command` entities.
  - 0 warnings, 0 clippy warnings across the workspace.

## 2026-09-27 — M1 (part 1) · `simula-core`: private chats at the MTProto level

- New crate `simula-core`. One **actor thread per sandbox** owns its SQLite connection
  and runs each job in one transaction (no locks, deterministic order). Async API:
  `Sandbox::invoke(caller, Function)`, typed `Sandbox::call(caller, F) -> F::Return`,
  `admin(...)` for Control-API operations, broadcast channels for updates and events.
- Storage (P2.1): migrations via `user_version`, in-memory or file (WAL), tables for
  accounts, per-account state (pts/qts/seq, next message id), message boxes and the
  pts update log. Uncommon attributes go in JSON `extra` columns.
- Accounts (P2.2): users (ids from 1 000 000 001) and bots (from 7 000 000 001) with
  realistic tokens; a supplied token fixes the bot id (frameworks read it from the
  token). Username rules and `USERNAME_OCCUPIED`. **Access hashes are per
  (viewer, target)** and derived from a per-sandbox secret, so nothing is stored and a
  hash one account learned is useless to another — as in Telegram.
- Message boxes (P2.4, private chats): each side stores its own copy with its own id and
  perspective (`out`, `peer_id`), linked by `global_id`. Updates (P2.5): per-account pts,
  logged; the outbox renders recipients' users with *their* access hashes inside the
  transaction and publishes after commit; the calling session is tagged as origin.
- TL handlers (P2.9, first three): `messages.sendMessage` (private), `users.getUsers`,
  `updates.getState`; `invokeWithLayer`/`initConnection`/`invokeWithoutUpdates` unwrap.
  Everything else answers `501 NOT_IMPLEMENTED_IN_SIMULA: <function>`.
- Errors are authentic MTProto errors. Found in tdlib's `MessagesManager.cpp`: the server
  answers a bot writing first with `400 PEER_ID_INVALID`, which tdlib turns into `403
  Bot can't initiate conversation with a user`; the Bot API gateway will replicate that
  translation chain from tdlib instead of inventing texts.
- TL codegen: structs/enums now derive `Default` where possible (2460 structs, 618 enums;
  unit defaults via `#[default]`), so handlers write `types::User { id, .., ..Default::default() }`.
- **Verification**: integration test of a user↔bot exchange (bot can't write first,
  per-box ids 1/2, per-account pts, bot sees the user's `lang_code` and its *own* access
  hash), forged access hash rejected, unimplemented functions reported, token-derived bot
  ids. 0 warnings, 0 clippy warnings.

## 2026-09-27 — P1.8 · TypeScript types (`@simula/schema`)

- `bun run codegen ts` generates `packages/schema/src/generated/{tl,botapi}.ts`:
  - **TL**: JSON-TL interfaces named exactly as in TL (`messageEntityBold` with
    `_: "messageEntityBold"`, union `MessageEntity`), TL namespaces as TS namespaces,
    `long`/`bytes` as strings, the `Function` union, and `Methods` mapping every function
    to params and result for a typed RPC client. The 20 namespace types that shadow root
    types (e.g. `messages.ChatFull`) reference root types through private aliases;
    `true`/`null` constructors are escaped.
  - **Bot API**: interfaces with literal types for constants and enums, `<Method>Params`
    per method, `Methods` map, `ChatId`, `InputFile`, the named anonymous unions.
- **Cross-language verification** (`bun run codegen verify:ts`): the generated Rust
  round-trip tests dump the JSON they produce (`SIMULA_SAMPLES_DIR`), and each value is
  type-checked as `<json> satisfies <TsType>` (excess-property checks make it strict in
  both directions). **TypeScript accepts all 3242 Rust samples** (2471 TL, 771 Bot
  API). First run caught the Rust samples using free text where the docs define an
  enum; samples now use documented values.
- Open item for the session gateway (P4/P5): `messages.receivedQueue` and
  `photos.deletePhotos` return `Vector<long>`; top-level results must be encoded as
  strings there to match the JSON-TL convention.

## 2026-09-27 — P1.7 · `simula-botapi-types`

- `bun run codegen rust:botapi` generates `crates/botapi-types/src/generated/` (593 files):
  one struct per object type, one enum per union, one parameter struct per method
  (`Method` with `NAME`, `PARAMS`, `MULTIPART`, `Return`), `Validate` for everything,
  `registry::METHODS` (185 methods with parameter kinds for request coercion) and
  `BOT_API_VERSION`.
- Unions are untagged serde enums made exact by **tag types**: every documented constant
  ("always “photo”", "Always 0") is a unit type that serializes to its value and rejects
  others, so a wrong variant fails and the next is tried. Variants are ordered most
  specific first. This handles `InlineQueryResultPhoto` vs `…CachedPhoto` (same tag),
  `MaybeInaccessibleMessage` (date 0) and `InputMessageContent` (no tag) with no
  hand-written code. `spec/botapi/names.json` names the 6 anonymous unions
  (`ChatId`, `InputFile`, `ReplyMarkup`, `MessageOrTrue`, `MediaGroupItem`,
  `RichMessageMedia`); an unnamed new one fails codegen.
- Handwritten runtime: `InputFile` (`attach://` uploads vs file_id/URL references),
  `ChatId` (numbers, numeric strings, usernames), `Method`/`ParamSpec`, structured
  `Violation`s with full paths (the gateway maps them to Telegram's texts), tag macros.
- Layout decisions driven by measurements: optional nested objects and union payloads
  are boxed. Inline, `Update` was **167 KB** and `Message` 22 KB (a stack overflow in
  tests); now 224 B and 1.3 KB. Recursive types are boxed via SCC analysis; `Default`
  is derived wherever possible so large structs are easy to build.
- Lengths "after entities parsing" are left to the gateway (they need the parsed text).
- **Verification**: generated round trip of **771** fully populated samples — every
  object, every method, and every union variant *through its union* (which checks the
  discrimination). Handwritten tests: lossless real Telegram payloads, date-0
  inaccessible messages, cached vs URL photos, keyboards with `force_reply`, `chat_id`
  coercion, `attach://`, nested violation paths
  (`reply_markup.inline_keyboard[0][0].callback_data`), ranges/items/enums, registry.
  0 warnings, 0 clippy warnings.
- Spec fix found on the way: the docs' `<br>- item` pseudo-lists produced Markdown where
  the following line continued the last item; `toMarkdown` now separates them.

## 2026-09-27 — P0.3, P1.6 · Workspaces and the `simula-tl` crate

- Workspaces: Cargo (`resolver = 3`, edition 2024, `unsafe_code = forbid`, clippy
  `all`), Bun (`tools/*`), rustfmt, biome. Rust 1.98.1 installed by the maintainer.
- `bun run codegen rust:tl` generates `crates/tl/src/generated/` (1372 files) from
  `spec/tl/*.json`: one struct per constructor, one enum per boxed type (payloads boxed,
  so recursive types are fine), one struct per function with `RemoteCall::Return`, a
  `Function` enum for server-side dispatch, `From` conversions, and the original TL
  declaration as each item's doc comment. `telegram_api` → `simula_tl::{types, enums,
  functions, Function, LAYER}`; `mtproto_api` → `simula_tl::mtproto::*`.
- Handwritten runtime (`crates/tl/src/codec`, `json.rs`): TL primitives (string/bytes
  padding and the 254 long form, boxed/bare vectors, `Bool`, int128/256, `Box<T>`),
  decoding guards (truncation, absurd vector counts, trailing bytes), and JSON-TL
  adapters: `{"_": "constructorName", …}`, `long` as string (accepted as number too),
  bytes as base64, unset optionals and false bit-flags omitted.
- The writer formats all generated files with one rustfmt run and only rewrites files
  whose content changed, so a regeneration touches just what moved (fast rebuilds).
- **Verification**:
  - `tests/known_vectors.rs`: byte-exact encode/decode of the official
    `req_pq_multi` and `resPQ` samples from core.telegram.org/mtproto/samples-auth_key,
    flags bit computation, generic `invokeWithLayer` nesting, exact JSON-TL text.
  - `tests/roundtrip/main.rs` (generated): a sample value for **every one of the 2512
    combinators** (all fields set, all bit flags on, `long`s past 2^32) round-trips
    through both the binary and the JSON codec.
  - 0 compiler warnings, 0 clippy warnings; clean build of the crate ≈ 1.5 min.
- Disk: incremental caches for the generated crate reached 6.5 GB (several variants:
  lib, test, clippy). `simula-tl` now builds non-incrementally (it rarely changes);
  `target/` dropped from 7.3 GB to 1.4 GB.
- Bugs caught by those tests and fixed in the emitter: `mtproto_api.tl` declares binary
  payloads (`pq`, `g_a`, `encrypted_data`) as `string`, so that schema maps `string` to
  bytes; TL's uppercase SRP params (`A`, `M1`, `srp_B`) become snake_case Rust fields
  with serde keeping the TL names.

## 2026-09-27 — P1.5 · Spec diff

- `bun run codegen diff [--base <ref>]` compares the working-tree specs with a git ref;
  `bun run codegen diff:files <a> <b>` compares any two Bot API specs or TL schemas.
  Reports are Markdown: added/removed/changed methods, types, params, fields, union
  variants, return types, requiredness, upload support; for TL, ids, results and params.
  When the Bot API version changes, Telegram's official changelog is appended, since
  behaviour-only changes are invisible to a structural diff.
- **Verification (Bot API)**: parsed the archived 10.2 docs (Wayback Machine) with the
  same pipeline (0 problems — also a regression test of the parser on a second
  version) and diffed 10.2 → 10.3. Every structural item of the official 10.3
  changelog appears (e.g. the ephemeral-parameter swap in all 13 `send*` methods).
  The diff also surfaced changes the changelog omits: `InputRichBlockMap` fields became
  optional, `InputRichMessageMedia.media` gained `InputMediaDocument`,
  `editEphemeralMessageText.text` became optional.
- **Verification (TL)**: diffed layer 228 (from tdlib history) → 229: 31 constructors
  added, including the new `buttonType*` / `inlineButtonType*` keyboard model that
  replaces `keyboardButton*` — the MTProto side of 10.3's `DisabledButton`.

## 2026-09-27 — P1.4 · TL schema parser

- `bun run codegen tl:import <tdlib-dir>` vendors `telegram_api.tl` and `mtproto_api.tl`
  into `spec/tl/` and records the layer and tdlib commit in `spec/tl/source.json`;
  `bun run codegen tl` parses them into `spec/tl/telegram_api.json` / `mtproto_api.json`.
- Imported **layer 229** from tdlib `bc9c263e2` (2026-08-24):
  1660 constructors + 813 functions (telegram_api), 33 + 8 (mtproto_api).
- Structured types: named (boxed/bare), `Vector<T>`/`vector<T>`, generics (`{X:Type}`,
  `!X`), flags fields (`flags:#`, `flags2.3?Type`).
- **Verification**: every id is recomputed with the TL CRC32 rule. All real API
  combinators match; the three that don't (`accessPointRule`, `help.configSimple`,
  `gzip_packed`) were hashed from Telegram's original `bytes` declarations that tdlib
  rewrites, and are reported as warnings.
- tdlib-only declarations are dropped: `invokeWith*Prefix` helpers (they reuse real
  function ids, which would break a server-side decoder), `test.*`, `*Legacy`.
  Duplicate ids now fail the build.
- Note for P8: tdlib comments out `rpc_result`, `msg_container`, `msg_copy`, `ping`
  and `destroy_session` in `mtproto_api.tl` because it handles them by hand. The
  transport will need them (hand-written or an extra `.tl`).

## 2026-09-27 — P1.1, P1.2, P1.3 · Bot API spec pipeline

- `bun run codegen botapi [--offline]` downloads core.telegram.org/bots/api (cached in
  `.cache/`), parses it, applies overrides, validates, and writes `spec/botapi/api.json`.
- Result for Bot API 10.3: **185 methods, 399 types** (26 unions, 23 of them
  discriminated), each method flagged `multipart` when a parameter can reach an upload.
- Code in `tools/codegen/src/botapi/`: `parse.ts` (entries/tables/unions/release),
  `type-expr.ts` (type grammar), `returns.ts` (return types from prose, token-based),
  `constraints.ts` (lengths, ranges, items, enums, consts, defaults), `overrides.ts`,
  `validate.ts`.
- Unions can include non-object members: `RichText` = `String | Array of RichText | RichText*`.
- `spec/botapi/overrides.json` supports per-method, per-type and name-based `all_fields`
  rules (e.g. every `parse_mode` gets the HTML/Markdown/MarkdownV2 enum). Stale entries fail.
- **Verification**: compared against the independent PaulSonOfLars/telegram-bot-api-spec
  (also 10.3): 0 differences in return types, required flags and field types across all
  methods and types. The only difference is `sendMediaGroup.media`, where our
  "Array of (A | B | …)" is more accurate than their "Array of A | Array of B | …".
- Constraint extraction was reviewed field by field; fixed a false positive
  ("ISO 3166-1" read as an item count) and added the bare "; 0-1500" range form.
  Current yield: 122 length, 52 range, 25 items, 85 enum, 176 const, 29 default.
- Known gaps left for the emitters: `InlineQueryResult` shares `type` values between
  cached/non-cached variants (needs tag + field-presence matching);
  `MaybeInaccessibleMessage` and `InputMessageContent` have no tag (shape-based).

## 2026-09-27 — P0.1, P0.2 · Rewrite kickoff

- Created the `v2` branch from `master` and removed all v1 sources (`api-server`,
  `client`, `scraper`, `scripts`, `docs`, `docker-compose.yml`, release workflows).
  v1 remains intact on `master`.
- Wrote [architecture.md](architecture.md), [ROADMAP.md](../ROADMAP.md) and this worklog.
- Decisions made with the maintainer:
  - Runtime in **Rust** (performance is the deciding criterion).
  - MTProto: **TL core now**, wire protocol in a later phase (P8).
  - Client in **React + TypeScript**; desktop via **bunium**.
  - Work happens on the `v2` branch of this repo, one commit per finished task.
- Decided independently (build-time only, no runtime impact): codegen written in
  TypeScript on Bun, generated code committed.
- Review of v1 that shaped the plan:
  - `client/src/components/Chat/TelegramChatPage.tsx` was 23k lines; handler files up to 5k.
  - Bot API handlers wrote straight to SQLite, with global message IDs and no MTProto layer.
  - Mini Apps were never rendered; no isolation between test runs; UI-only interaction.
  - Worth keeping: debug console ideas (request/response log, webhook viewer, JSON diff,
    trace export/import), runtime power/restart controls, broad feature scope
    (business, gifts, stories, suggested posts, checklists, boosts).
- Targets: Bot API 10.3 (current on core.telegram.org and in `telegram-bot-api`).
