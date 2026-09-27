/**
 * Wire types shared by the Simula client and SDKs, generated from `spec/`.
 * Regenerate with `bun run codegen ts`.
 *
 * - `tl`: JSON-TL, the MTProto shapes the client speaks to the server
 * - `botapi`: the Bot API, as seen by bots and the dev tools
 */
export * as botapi from "./generated/botapi";
export * as tl from "./generated/tl";
