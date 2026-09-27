import { mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { $ } from "bun";
import { paths, ROOT } from "../paths";

const PER_FILE = 400;

/**
 * Cross-check the Rust and TypeScript emitters: run the generated Rust round-trip tests
 * with `SIMULA_SAMPLES_DIR` set, then type-check every JSON value they produced as
 * `<json> satisfies <TsType>`. Missing required fields, unknown fields and wrong literal
 * tags all fail `tsc`.
 */
export async function verifyTsSamples(): Promise<{ samples: number; skipped: number }> {
  const dir = join(paths.cache, "samples");
  await rm(dir, { recursive: true, force: true });
  await mkdir(dir, { recursive: true });

  const cargo = Bun.which("cargo") ?? join(process.env.HOME ?? "", ".cargo", "bin", "cargo");
  for (const crate of ["simula-tl", "simula-botapi-types"]) {
    await $`${cargo} test -q -p ${crate} --test roundtrip`
      .env({ ...process.env, SIMULA_SAMPLES_DIR: dir })
      .cwd(ROOT)
      .quiet();
  }

  const lines = [
    ...(await Bun.file(join(dir, "simula-tl.jsonl")).text()).split("\n"),
    ...(await Bun.file(join(dir, "simula-botapi-types.jsonl")).text()).split("\n"),
  ].filter(Boolean);

  const checks: string[] = [];
  let skipped = 0;
  for (const line of lines) {
    const tab = line.indexOf("\t");
    const tsType = toTsType(line.slice(0, tab));
    if (!tsType) {
      skipped++;
      continue;
    }
    checks.push(`(${line.slice(tab + 1)}) satisfies ${tsType};`);
  }

  const schema = join(ROOT, "packages", "schema", "src", "index.ts");
  for (let i = 0; i * PER_FILE < checks.length; i++) {
    const body = checks.slice(i * PER_FILE, (i + 1) * PER_FILE).join("\n");
    await Bun.write(
      join(dir, `check_${i}.ts`),
      `import type { botapi, tl } from ${JSON.stringify(schema)};\n\n${body}\n`,
    );
  }
  await Bun.write(
    join(dir, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        strict: true,
        noEmit: true,
        module: "ESNext",
        moduleResolution: "bundler",
        target: "ESNext",
        skipLibCheck: true,
        allowImportingTsExtensions: true,
      },
      include: ["check_*.ts"],
    }),
  );
  const tsc = await $`bunx tsc -p ${dir}`.cwd(ROOT).nothrow().quiet();
  if (tsc.exitCode !== 0) {
    const errors = tsc.stdout
      .toString()
      .split("\n")
      .filter((l) => l.includes("error TS"));
    const kinds = new Map<string, number>();
    for (const e of errors) {
      const kind = e.replace(/^.*error (TS\d+): /, "$1 ").replace(/'[^']*'/g, "'…'");
      kinds.set(kind, (kinds.get(kind) ?? 0) + 1);
    }
    const summary = [...kinds].sort((a, b) => b[1] - a[1]).map(([k, n]) => `${n}× ${k}`);
    throw new Error(
      `TypeScript rejected Rust samples (${errors.length} errors):\n${summary.join("\n")}\n\nfirst:\n${errors.slice(0, 5).join("\n")}`,
    );
  }
  return { samples: checks.length, skipped };
}

/** Map a Rust `type_name` from the generated crates to the matching TypeScript type. */
function toTsType(rust: string): string | undefined {
  const parts = rust.split("::");
  const last = parts.at(-1) ?? "";
  if (rust.startsWith("simula_tl::generated::mtproto::")) return undefined; // not exposed to TS
  if (rust.startsWith("simula_tl::generated::api::")) {
    if (parts.includes("function")) return "tl.Function";
    const ns = parts[parts.indexOf("enums") + 1];
    return ns === "root" ? `tl.${last}` : `tl.${ns}.${last}`;
  }
  if (rust.startsWith("simula_botapi_types::generated::methods::")) return `botapi.${last}Params`;
  if (rust.startsWith("simula_botapi_types::generated::types::")) return `botapi.${last}`;
  throw new Error(`unmapped sample type ${rust}`);
}
