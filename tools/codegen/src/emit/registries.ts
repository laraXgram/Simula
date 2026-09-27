import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { loadBotApiSpec } from "../botapi";
import type { Method } from "../botapi/model";
import { paths, ROOT } from "../paths";
import { loadTlSchema } from "../tl";
import type { TlCombinator } from "../tl/model";
import { type BotApiNames, Model } from "./rust/botapi";
import { ident, pascal, snake, splitName } from "./rust/naming";
import { TlContext } from "./rust/tl";
import { type FileTree, GENERATED_HEADER, syncGenerated } from "./writer";

export const BOTAPI_METHODS_DIR = join(ROOT, "crates", "botapi", "src", "methods");
export const TL_HANDLERS_DIR = join(ROOT, "crates", "core", "src", "rpc", "handlers");

/** Marker line in scaffolded handlers; coverage reports them as stubs. */
export const STUB_MARKER = "// @stub";

export interface Handler<T> {
  item: T;
  /** Path relative to the handlers directory. */
  file: string;
  stub: boolean;
}

/**
 * Bot API method handlers: `crates/botapi/src/methods/<snake_name>.rs`. A file whose name
 * matches no method is an error, so a typo can't silently leave a method unrouted.
 */
export async function botApiHandlers(methods: Method[]): Promise<Handler<Method>[]> {
  const byFile = new Map(methods.map((m) => [`${snake(m.name)}.rs`, m]));
  const handlers: Handler<Method>[] = [];
  for (const file of (await readdir(BOTAPI_METHODS_DIR)).sort()) {
    if (file === "mod.rs" || !file.endsWith(".rs")) continue;
    const item = byFile.get(file);
    if (!item)
      throw new Error(`crates/botapi/src/methods/${file}: no Bot API method with that name`);
    const text = await Bun.file(join(BOTAPI_METHODS_DIR, file)).text();
    handlers.push({ item, file, stub: text.includes(STUB_MARKER) });
  }
  return handlers;
}

/** TL function handlers: `crates/core/src/rpc/handlers/<namespace or root>/<snake_name>.rs`. */
export async function tlHandlers(functions: TlCombinator[]): Promise<Handler<TlCombinator>[]> {
  const byFile = new Map(functions.map((f) => [tlHandlerPath(f), f]));
  const handlers: Handler<TlCombinator>[] = [];
  for (const dir of (await readdir(TL_HANDLERS_DIR, { withFileTypes: true })).filter((d) =>
    d.isDirectory(),
  )) {
    for (const file of (await readdir(join(TL_HANDLERS_DIR, dir.name))).sort()) {
      if (file === "mod.rs" || !file.endsWith(".rs")) continue;
      const path = `${dir.name}/${file}`;
      const item = byFile.get(path);
      if (!item)
        throw new Error(`crates/core/src/rpc/handlers/${path}: no TL function with that name`);
      const text = await Bun.file(join(TL_HANDLERS_DIR, path)).text();
      handlers.push({ item, file: path, stub: text.includes(STUB_MARKER) });
    }
  }
  return handlers.sort((a, b) => a.file.localeCompare(b.file));
}

export function tlHandlerPath(f: TlCombinator): string {
  const { ns, local } = splitName(f.name);
  return `${ns ?? "root"}/${snake(local)}.rs`;
}

/** Regenerate the module/route files that tie handler files together. */
export async function emitRegistries(): Promise<{ botapi: number; tl: number }> {
  const spec = await loadBotApiSpec();
  const schema = await loadTlSchema("telegram_api");
  const bot = await botApiHandlers(spec.methods);
  const tl = await tlHandlers(schema.functions);

  await syncGenerated(BOTAPI_METHODS_DIR, new Map([["mod.rs", await botApiRoutes(bot)]]));
  await syncGenerated(TL_HANDLERS_DIR, tlRoutes(tl));
  return { botapi: bot.length, tl: tl.length };
}

async function botApiRoutes(handlers: Handler<Method>[]): Promise<string> {
  const modules = handlers.map((h) => `pub mod ${h.file.replace(/\.rs$/, "")};`).join("\n");
  const arms: string[] = [];
  for (const h of handlers) {
    const module = h.file.replace(/\.rs$/, "");
    const text = await Bun.file(join(BOTAPI_METHODS_DIR, h.file)).text();
    const limits = /pub const LIMITS\b/.test(text) ? `${module}::LIMITS` : "Limits::Enforce";
    arms.push(
      `${JSON.stringify(h.item.name.toLowerCase())} => run(ctx, params, ${limits}, ${module}::handle).await,`,
    );
  }
  return `${GENERATED_HEADER}

//! One file per Bot API method. This module list and the routing table are generated
//! from the files present (\`bun run codegen registries\`); add a method by adding its file
//! (\`bun run codegen new:botapi <method>\`).

${modules}

use serde_json::{Map, Value};

use crate::ctx::BotCtx;
use crate::dispatch::{Limits, run};
use crate::error::BotResult;

/// Bot API methods with a handler (stubs included).
pub const IMPLEMENTED: &[&str] = &[${handlers.map((h) => JSON.stringify(h.item.name)).join(", ")}];

/// Run the handler for a lowercase method name; \`None\` when there is none.
pub async fn route(ctx: BotCtx, method: &str, params: Map<String, Value>) -> Option<BotResult<Value>> {
    Some(match method {
${arms.join("\n")}
        _ => return None,
    })
}
`;
}

function tlRoutes(handlers: Handler<TlCombinator>[]): FileTree {
  const tree: FileTree = new Map();
  const byDir = new Map<string, Handler<TlCombinator>[]>();
  for (const h of handlers) {
    const dir = h.file.split("/")[0] as string;
    byDir.set(dir, [...(byDir.get(dir) ?? []), h]);
  }
  for (const [dir, items] of byDir) {
    const modules = items.map(
      (h) => `pub mod ${ident(h.file.split("/")[1]?.replace(/\.rs$/, "") ?? "")};`,
    );
    tree.set(`${dir}/mod.rs`, `${GENERATED_HEADER}\n\n${modules.join("\n")}\n`);
  }
  const arms = handlers.map((h) => {
    const { ns, local } = splitName(h.item.name);
    const variant = `${ns ? pascal(ns) : ""}${pascal(local)}`;
    const module = h.file.replace(/\.rs$/, "").replace("/", "::");
    return `Function::${variant}(call) => ${module}::handle(ctx, *call).map(|reply| Box::new(reply) as Box<dyn Reply>),`;
  });
  tree.set(
    "mod.rs",
    `${GENERATED_HEADER}

//! TL function handlers, one file per function: \`<namespace>/<function>.rs\`. This module
//! list and routing table are generated from the files present
//! (\`bun run codegen registries\`); add one with \`bun run codegen new:tl <function>\`.

${[...byDir.keys()]
  .sort()
  .map((d) => `pub mod ${ident(d)};`)
  .join("\n")}

use simula_tl::Function;

use super::Reply;
use crate::ctx::Ctx;
use crate::error::RpcResult;

/// TL functions with a handler (stubs included).
pub const IMPLEMENTED: &[&str] = &[${handlers.map((h) => JSON.stringify(h.item.name)).join(", ")}];

/// Run the handler for \`call\`, or hand the call back when there is none.
pub fn route(ctx: &mut Ctx<'_>, call: Function) -> Result<RpcResult<Box<dyn Reply>>, Function> {
    Ok(match call {
${arms.join("\n")}
        other => return Err(other),
    })
}
`,
  );
  return tree;
}

/** Create a handler file for a Bot API method from the spec, then refresh the registries. */
export async function scaffoldBotApi(name: string): Promise<string> {
  const spec = await loadBotApiSpec();
  const method = spec.methods.find((m) => m.name.toLowerCase() === name.toLowerCase());
  if (!method) throw new Error(`no Bot API method "${name}"`);
  const file = join(BOTAPI_METHODS_DIR, `${snake(method.name)}.rs`);
  if (await Bun.file(file).exists()) throw new Error(`${file} already exists`);

  const names = (await Bun.file(paths.botapi.names).json()) as BotApiNames;
  const model = new Model(spec, names);
  const returns = model
    .rustType(method.returns, undefined)
    .replaceAll("crate::types::", "bot::")
    .replaceAll("crate::", "simula_botapi_types::");
  await Bun.write(
    file,
    `//! \`${method.name}\` — https://core.telegram.org/bots/api#${method.name.toLowerCase()}

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};

${STUB_MARKER}: scaffolded by \`bun run codegen new:botapi\`; remove this line once implemented.
pub async fn handle(_ctx: BotCtx, _params: methods::${pascal(method.name)}) -> BotResult<${returns}> {
    Err(BotError::not_implemented("${method.name}"))
}
`,
  );
  await emitRegistries();
  return file;
}

/** Create a handler file for a TL function, then refresh the registries. */
export async function scaffoldTl(name: string): Promise<string> {
  const schema = await loadTlSchema("telegram_api");
  const f = schema.functions.find((fn) => fn.name === name);
  if (!f) throw new Error(`no TL function "${name}"`);
  const file = join(TL_HANDLERS_DIR, tlHandlerPath(f));
  if (await Bun.file(file).exists()) throw new Error(`${file} already exists`);

  const ctx = new TlContext(schema, "simula_tl");
  const { ns, local } = splitName(f.name);
  const returns =
    f.result.kind === "generic" ? "Box<dyn super::super::Reply>" : ctx.rustType(f.result);
  await Bun.write(
    file,
    `//! \`${f.name}\`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};

${STUB_MARKER}: scaffolded by \`bun run codegen new:tl\`; remove this line once implemented.
pub fn handle(_ctx: &mut Ctx<'_>, _req: functions::${ns ? `${ns}::` : ""}${pascal(local)}) -> RpcResult<${returns}> {
    Err(RpcError::not_implemented("${f.name}"))
}
`,
  );
  await emitRegistries();
  return file;
}
