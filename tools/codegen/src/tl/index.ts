import { join } from "node:path";
import { $ } from "bun";
import { paths } from "../paths";
import type { TlCombinator, TlSchema, TlSource } from "./model";
import { parseTl } from "./parse";

export type TlFile = (typeof paths.tl.files)[number];

/**
 * Copy the schemas from a tdlib checkout into spec/tl and record where they came
 * from. `tdlibDir` is the tdlib repository root (the one containing td/generate).
 */
export async function importTl(tdlibDir: string): Promise<TlSource> {
  const schemeDir = join(tdlibDir, "td", "generate", "scheme");
  for (const file of paths.tl.files) {
    await Bun.write(join(paths.tl.dir, `${file}.tl`), Bun.file(join(schemeDir, `${file}.tl`)));
  }

  const version = await Bun.file(join(tdlibDir, "td", "telegram", "Version.h")).text();
  const layer = /MTPROTO_LAYER\s*=\s*(\d+)/.exec(version)?.[1];
  if (!layer) throw new Error("MTPROTO_LAYER not found in td/telegram/Version.h");

  const log = (await $`git -C ${tdlibDir} log -1 --format=%h%n%cs`.text()).trim().split("\n");
  const source: TlSource = {
    layer: Number(layer),
    tdlib_commit: log[0] ?? "unknown",
    tdlib_date: log[1] ?? "unknown",
  };
  await Bun.write(paths.tl.source, `${JSON.stringify(source, null, 2)}\n`);
  return source;
}

/** Parse the vendored .tl files into spec/tl/<file>.json. */
export async function buildTlSchemas(): Promise<Record<TlFile, TlSchema>> {
  const source = (await Bun.file(paths.tl.source).json()) as TlSource;
  const result = {} as Record<TlFile, TlSchema>;
  for (const file of paths.tl.files) {
    const warnings: string[] = [];
    const schema = parseTl(
      await Bun.file(join(paths.tl.dir, `${file}.tl`)).text(),
      `${file}.tl`,
      warnings,
    );
    for (const warning of warnings.filter((w) => !TDLIB_ONLY.test(w.split(":")[0] ?? ""))) {
      console.warn(`warning: ${file}.tl: ${warning}`);
    }
    schema.constructors = schema.constructors.filter(isRealApi);
    schema.functions = schema.functions.filter(isRealApi);
    assertUniqueIds(schema);
    if (file === "telegram_api") schema.layer = source.layer;
    await Bun.write(join(paths.tl.dir, `${file}.json`), `${JSON.stringify(schema, null, 2)}\n`);
    result[file] = schema;
  }
  return result;
}

/**
 * tdlib ships a few declarations that are not Telegram API: `invokeWith*Prefix` helpers
 * (reusing real ids without the `query` param), `test.*` functions and `*Legacy`
 * file locations. They would collide with real ids in a server-side decoder.
 */
const TDLIB_ONLY = /Prefix$|Legacy$|^test\./;

function isRealApi(combinator: TlCombinator): boolean {
  return !TDLIB_ONLY.test(combinator.name);
}

function assertUniqueIds(schema: TlSchema) {
  const seen = new Map<string, string>();
  for (const combinator of [...schema.constructors, ...schema.functions]) {
    const previous = seen.get(combinator.id);
    if (previous)
      throw new Error(
        `${schema.file}: id ${combinator.id} used by ${previous} and ${combinator.name}`,
      );
    seen.set(combinator.id, combinator.name);
  }
}

export async function loadTlSchema(file: TlFile): Promise<TlSchema> {
  return (await Bun.file(join(paths.tl.dir, `${file}.json`)).json()) as TlSchema;
}
