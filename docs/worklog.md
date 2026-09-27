# Worklog

Newest entries first. Each entry names the roadmap task(s) it closes; the matching commit
carries the same task ID in its message (`git log --grep "P1.1"`).

---

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
