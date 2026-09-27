# Worklog

Newest entries first. Each entry names the roadmap task(s) it closes; the matching commit
carries the same task ID in its message (`git log --grep "P1.1"`).

---

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
