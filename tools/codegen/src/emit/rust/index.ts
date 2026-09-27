import { join } from "node:path";
import { loadBotApiSpec } from "../../botapi";
import { paths, ROOT } from "../../paths";
import { loadTlSchema } from "../../tl";
import { type FileTree, GENERATED_HEADER, type SyncResult, syncTree } from "../writer";
import { type BotApiNames, emitBotApi, Model } from "./botapi";
import { emitBotApiRoundtripTests } from "./botapi-tests";
import { emitTlSchema } from "./tl";
import { emitTlRoundtripTests } from "./tl-tests";

/** Generate crates/tl/src/generated from spec/tl/*.json. */
export async function emitRustTl(): Promise<SyncResult> {
  const tree: FileTree = new Map([
    [
      "mod.rs",
      `${GENERATED_HEADER}\n\n//! TL schemas: \`api\` (telegram_api.tl) and \`mtproto\` (mtproto_api.tl).\n\npub mod api;\npub mod mtproto;\n`,
    ],
  ]);
  for (const [file, module, options] of [
    ["telegram_api", "api", {}],
    ["mtproto_api", "mtproto", { binaryStrings: true }],
  ] as const) {
    for (const [path, content] of emitTlSchema(await loadTlSchema(file), module, options)) {
      tree.set(path, content);
    }
  }
  const api = await loadTlSchema("telegram_api");
  const mtproto = await loadTlSchema("mtproto_api");
  const tests = emitTlRoundtripTests([
    { schema: api, root: "simula_tl" },
    { schema: mtproto, root: "simula_tl::mtproto", options: { binaryStrings: true } },
  ]);
  await syncTree(join(ROOT, "crates", "tl", "tests", "roundtrip"), new Map([["main.rs", tests]]));

  return syncTree(join(ROOT, "crates", "tl", "src", "generated"), tree);
}

/** Generate crates/botapi-types/src/generated from spec/botapi/api.json. */
export async function emitRustBotApi(): Promise<SyncResult> {
  const names = (await Bun.file(paths.botapi.names).json()) as BotApiNames;
  const spec = await loadBotApiSpec();
  const tests = emitBotApiRoundtripTests(new Model(spec, names), spec);
  await syncTree(
    join(ROOT, "crates", "botapi-types", "tests", "roundtrip"),
    new Map([["main.rs", tests]]),
  );
  return syncTree(
    join(ROOT, "crates", "botapi-types", "src", "generated"),
    emitBotApi(spec, names),
  );
}
