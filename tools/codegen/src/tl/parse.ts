import type { TlCombinator, TlParam, TlSchema, TlType } from "./model";

export class TlParseError extends Error {}

/**
 * Parse a `.tl` schema. Builtin declarations (`int ? = Int;`, `vector {t:Type} # [ t ]`,
 * `int128 4*[ int ]`) are skipped; everything else becomes a constructor or function.
 * Missing ids are computed; present ids are verified against the TL CRC32 rule, which
 * doubles as a check that the parser read the line the way Telegram does.
 */
export function parseTl(text: string, file: string, warnings: string[] = []): TlSchema {
  const schema: TlSchema = { file, constructors: [], functions: [] };
  let section: "constructors" | "functions" = "constructors";

  const statements = stripComments(text)
    .split(";")
    .map((s) => s.replace(/\s+/g, " ").trim())
    .filter(Boolean);

  for (const statement of statements) {
    // A section marker is glued to the statement that follows it.
    let rest = statement;
    for (let marker = /^---(\w+)---\s*/.exec(rest); marker; marker = /^---(\w+)---\s*/.exec(rest)) {
      section = marker[1] === "functions" ? "functions" : "constructors";
      rest = rest.slice(marker[0].length);
    }
    if (!rest || isBuiltin(rest)) continue;
    schema[section].push(parseCombinator(rest, warnings));
  }
  return schema;
}

function stripComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
}

function isBuiltin(statement: string): boolean {
  return (
    / \? = /.test(statement) ||
    statement.includes("[") ||
    /^(?:bytes|int|long|double|string|int128|int256) = /.test(statement)
  );
}

function parseCombinator(statement: string, warnings: string[]): TlCombinator {
  const eq = statement.lastIndexOf(" = ");
  if (eq < 0) throw new TlParseError(`no result type: ${statement}`);
  const [head = "", ...args] = statement.slice(0, eq).split(" ");
  const resultText = statement.slice(eq + 3).trim();

  const idMatch = /^([\w.]+)(?:#([0-9a-f]{1,8}))?$/.exec(head);
  if (!idMatch?.[1]) throw new TlParseError(`bad combinator name: ${head}`);
  const name = idMatch[1];

  const generics: string[] = [];
  const params: TlParam[] = [];
  for (const arg of args) {
    const generic = /^\{(\w+):Type\}$/.exec(arg);
    if (generic?.[1]) {
      generics.push(generic[1]);
      continue;
    }
    params.push(parseParam(arg, generics, statement));
  }

  const combinator: TlCombinator = {
    name,
    id: "",
    params,
    result: parseType(resultText, generics),
  };
  if (generics.length > 0) combinator.generics = generics;

  const [computed = "", ...alternatives] = candidateIds(statement);
  const declared = idMatch[2]?.padStart(8, "0");
  if (declared && declared !== computed && !alternatives.includes(declared)) {
    warnings.push(`${name}: declared id ${declared}, CRC32 gives ${computed}`);
  }
  combinator.id = declared ?? computed;
  return combinator;
}

function parseParam(arg: string, generics: string[], statement: string): TlParam {
  const colon = arg.indexOf(":");
  if (colon < 0) throw new TlParseError(`bad param "${arg}" in: ${statement}`);
  const name = arg.slice(0, colon);
  let typeText = arg.slice(colon + 1);

  if (typeText === "#") return { name, type: { kind: "flags" } };

  let flag: TlParam["flag"];
  const conditional = /^(\w+)\.(\d+)\?(.+)$/.exec(typeText);
  if (conditional?.[1] && conditional[3]) {
    flag = { field: conditional[1], bit: Number(conditional[2]) };
    typeText = conditional[3];
  }

  const param: TlParam = { name, type: parseType(typeText, generics) };
  if (flag) param.flag = flag;
  return param;
}

function parseType(text: string, generics: string[]): TlType {
  if (text.startsWith("!")) return { kind: "generic", name: text.slice(1), query: true };
  if (generics.includes(text)) return { kind: "generic", name: text, query: false };

  const vector = /^([Vv])ector<(.+)>$/.exec(text);
  if (vector?.[2])
    return { kind: "vector", of: parseType(vector[2], generics), bare: vector[1] === "v" };

  const name = text.replace(/^%/, "");
  const local = name.slice(name.lastIndexOf(".") + 1);
  return { kind: "named", name, bare: text.startsWith("%") || /^[a-z]/.test(local) };
}

/**
 * TL constructor id: CRC32 of the declaration without its id, with angle brackets and
 * braces flattened and `?true` flag params removed. Telegram usually hashes `bytes` as
 * `string`, but some ids were computed with `bytes` as written (e.g. ipPortSecret), so
 * both forms are returned; the first is the canonical one used for missing ids.
 */
export function candidateIds(statement: string): string[] {
  const base = statement
    .replace(/^([\w.]+)#[0-9a-f]+/, "$1")
    .replace(/</g, " ")
    .replace(/>/g, "")
    .replace(/[{}]/g, "")
    .replace(/ \w+:\w+\.\d+\?true\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
  const asString = base.replace(/:bytes\b/g, ":string").replace(/\?bytes\b/g, "?string");
  return [crc(asString), crc(base)];
}

function crc(text: string): string {
  return (Bun.hash.crc32(text) >>> 0).toString(16).padStart(8, "0");
}
