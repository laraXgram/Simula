import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { paths } from "../paths";
import type { BotApiSpec } from "./model";
import { applyOverrides, type Overrides } from "./overrides";
import { BOT_API_URL, parseBotApi } from "./parse";
import { validateSpec } from "./validate";

export interface BuildOptions {
  /** Use the cached HTML instead of downloading the docs. */
  offline?: boolean;
}

/** Download (or reuse) the docs, parse, apply overrides, validate, write api.json. */
export async function buildBotApiSpec(options: BuildOptions = {}): Promise<BotApiSpec> {
  const html = await loadDocs(options.offline ?? false);
  const spec = parseBotApi(html);

  const overrides = (await Bun.file(paths.botapi.overrides).json()) as Overrides;
  const problems = [...applyOverrides(spec, overrides), ...validateSpec(spec)];
  if (problems.length > 0) {
    throw new Error(`Bot API spec has ${problems.length} problem(s):\n  ${problems.join("\n  ")}`);
  }

  await Bun.write(paths.botapi.spec, `${JSON.stringify(spec, null, 2)}\n`);
  return spec;
}

export async function loadBotApiSpec(): Promise<BotApiSpec> {
  return (await Bun.file(paths.botapi.spec).json()) as BotApiSpec;
}

async function loadDocs(offline: boolean): Promise<string> {
  const cache = Bun.file(paths.botapi.html);
  if (offline) {
    if (!(await cache.exists()))
      throw new Error(`--offline: no cached docs at ${paths.botapi.html}`);
    return cache.text();
  }
  const response = await fetch(BOT_API_URL);
  if (!response.ok) throw new Error(`GET ${BOT_API_URL} → ${response.status}`);
  const html = await response.text();
  await mkdir(dirname(paths.botapi.html), { recursive: true });
  await Bun.write(paths.botapi.html, html);
  return html;
}
