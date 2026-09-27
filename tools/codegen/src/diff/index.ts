import { relative } from "node:path";
import { $ } from "bun";
import type { BotApiSpec } from "../botapi/model";
import { paths, ROOT } from "../paths";
import type { TlSchema } from "../tl/model";
import { diffBotApi } from "./botapi";
import { diffTl } from "./tl";

/** Diff the working-tree specs against the same files at a git ref. */
export async function diffAgainstRef(ref: string): Promise<string> {
  const reports: string[] = [];
  const specFiles = [
    paths.botapi.spec,
    ...paths.tl.files.map((file) => `${paths.tl.dir}/${file}.json`),
  ];
  for (const file of specFiles) {
    const base = await readAtRef(ref, file);
    if (!base) {
      reports.push(`# ${relative(ROOT, file)}\n\nNot present at ${ref}.\n`);
      continue;
    }
    reports.push(diffSpecs(base, await Bun.file(file).json()));
  }
  return reports.join("\n");
}

/** Diff two spec files of the same kind (Bot API or TL). */
export async function diffFiles(baseFile: string, headFile: string): Promise<string> {
  return diffSpecs(await Bun.file(baseFile).json(), await Bun.file(headFile).json());
}

function diffSpecs(base: unknown, head: unknown): string {
  if (isBotApi(base) && isBotApi(head)) return diffBotApi(base, head);
  if (isTl(base) && isTl(head)) return diffTl(base, head);
  throw new Error("both files must be Bot API specs or TL schemas");
}

async function readAtRef(ref: string, file: string): Promise<unknown | undefined> {
  const result = await $`git -C ${ROOT} show ${ref}:${relative(ROOT, file)}`.quiet().nothrow();
  return result.exitCode === 0 ? JSON.parse(result.stdout.toString()) : undefined;
}

function isBotApi(value: unknown): value is BotApiSpec {
  return typeof value === "object" && value !== null && "methods" in value && "release" in value;
}

function isTl(value: unknown): value is TlSchema {
  return (
    typeof value === "object" && value !== null && "constructors" in value && "functions" in value
  );
}
