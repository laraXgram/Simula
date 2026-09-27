import { join } from "node:path";
import { loadBotApiSpec } from "../../botapi";
import { paths, ROOT } from "../../paths";
import { loadTlSchema } from "../../tl";
import type { BotApiNames } from "../rust/botapi";
import { type SyncResult, syncTree } from "../writer";
import { emitTsBotApi } from "./botapi";
import { emitTsTl } from "./tl";

/** Generate packages/schema/src/generated from the specs. */
export async function emitTs(): Promise<SyncResult> {
  const names = (await Bun.file(paths.botapi.names).json()) as BotApiNames;
  const tree = new Map([
    ["tl.ts", emitTsTl(await loadTlSchema("telegram_api"))],
    ["botapi.ts", emitTsBotApi(await loadBotApiSpec(), names)],
  ]);
  return syncTree(join(ROOT, "packages", "schema", "src", "generated"), tree);
}
