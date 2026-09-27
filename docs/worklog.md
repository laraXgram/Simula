# Worklog

Newest entries first. Each entry names the roadmap task(s) it closes; the matching commit
carries the same task ID in its message (`git log --grep "P1.1"`).

---

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
