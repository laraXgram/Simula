import { formatCombinator } from "../../tl/format";
import type { TlCombinator, TlParam, TlSchema, TlType } from "../../tl/model";
import { type FileTree, GENERATED_HEADER } from "../writer";
import { ident, pascal, snake, splitName } from "./naming";

/**
 * Rust emitter for a TL schema. Layout under `<module>/`:
 *
 *   types/{root,<ns>}/<boxed_type>.rs   one struct per constructor (bare encoding)
 *   enums/{root,<ns>}/<boxed_type>.rs   one enum per boxed type (id + payload)
 *   functions/{root,<ns>}.rs            one struct per function (+ RemoteCall)
 *   function.rs                         `Function`: every function, for server dispatch
 *
 * Constructors of `Bool` are not emitted: `Bool` maps to Rust `bool`.
 */
export interface TlEmitOptions {
  /**
   * Treat TL `string` as raw bytes. mtproto_api.tl declares binary payloads (`pq`,
   * `g_a`, `encrypted_data`, …) as `string`, which is not valid UTF-8.
   */
  binaryStrings?: boolean;
}

export function emitTlSchema(
  schema: TlSchema,
  module: string,
  options: TlEmitOptions = {},
): FileTree {
  const ctx = new TlContext(schema, `crate::generated::${module}`, options);
  const tree: FileTree = new Map();
  const put = (path: string, body: string) =>
    tree.set(`${module}/${path}`, `${GENERATED_HEADER}\n\n${body}`);

  const constructors = schema.constructors.filter((c) => !SKIPPED_TYPES.has(resultName(c)));

  put("mod.rs", moduleRoot(schema));
  emitGrouped(
    put,
    "types",
    groupBy(constructors, (c) => [splitName(c.name).ns, resultName(c)]),
    (items) => items.map((c) => emitStruct(ctx, c)).join("\n"),
  );
  emitGrouped(
    put,
    "enums",
    groupBy(constructors, (c) => [splitName(resultName(c)).ns, resultName(c)]),
    (items) => emitEnum(ctx, resultName(items[0] as TlCombinator), items),
  );
  emitFunctions(put, ctx, schema.functions);
  put("function.rs", emitFunctionEnum(ctx, schema.functions));
  return tree;
}

export const SKIPPED_TYPES = new Set(["Bool"]);

const PRIMITIVES: Record<string, string> = {
  int: "i32",
  long: "i64",
  double: "f64",
  string: "String",
  bytes: "Vec<u8>",
  int128: "[u8; 16]",
  int256: "[u8; 32]",
  Bool: "bool",
  true: "bool",
};

export class TlContext {
  readonly constructorsByName: Map<string, TlCombinator>;
  readonly boxedTypes: Set<string>;

  constructor(
    readonly schema: TlSchema,
    readonly root: string,
    readonly options: TlEmitOptions = {},
  ) {
    this.constructorsByName = new Map(schema.constructors.map((c) => [c.name, c]));
    this.boxedTypes = new Set(schema.constructors.map(resultName));
    this.computeDefaults();
  }

  /** Combinators whose struct can derive `Default` (every required field has one). */
  readonly defaultable = new Set<string>();
  /** Boxed type → constructor its `Default` builds: first unit variant, else first defaultable. */
  readonly enumDefault = new Map<string, TlCombinator>();

  private computeDefaults() {
    const all = [...this.schema.constructors, ...this.schema.functions];
    let changed = true;
    while (changed) {
      changed = false;
      for (const c of all) {
        if (this.defaultable.has(c.name)) continue;
        const ok = c.params.every(
          (p) => p.type.kind === "flags" || !!p.flag || this.typeDefaultable(p.type),
        );
        if (ok) {
          this.defaultable.add(c.name);
          changed = true;
        }
      }
      for (const type of this.boxedTypes) {
        if (this.enumDefault.has(type)) continue;
        const constructors = this.schema.constructors.filter((c) => resultName(c) === type);
        const pick =
          constructors.find((c) => c.params.length === 0) ??
          constructors.find((c) => this.defaultable.has(c.name));
        if (pick) {
          this.enumDefault.set(type, pick);
          changed = true;
        }
      }
    }
  }

  private typeDefaultable(type: TlType): boolean {
    switch (type.kind) {
      case "named":
        if (type.name in PRIMITIVES || (type.name === "string" && this.options.binaryStrings))
          return true;
        return type.bare ? this.defaultable.has(type.name) : this.enumDefault.has(type.name);
      case "vector":
        return true;
      default:
        return false;
    }
  }

  structPath(name: string): string {
    return this.path("types", name);
  }

  enumPath(name: string): string {
    if (!this.boxedTypes.has(name))
      throw new Error(`${this.schema.file}: unknown boxed type ${name}`);
    return this.path("enums", name);
  }

  functionPath(name: string): string {
    return this.path("functions", name);
  }

  private path(kind: string, name: string): string {
    const { ns, local } = splitName(name);
    return `${this.root}::${kind}${ns ? `::${ident(ns)}` : ""}::${pascal(local)}`;
  }

  rustType(type: TlType): string {
    switch (type.kind) {
      case "named": {
        if (type.name === "string" && this.options.binaryStrings) return "Vec<u8>";
        const primitive = PRIMITIVES[type.name];
        if (primitive) return primitive;
        if (type.bare) {
          if (!this.constructorsByName.has(type.name))
            throw new Error(`unknown bare type ${type.name}`);
          return this.structPath(type.name);
        }
        return this.enumPath(type.name);
      }
      case "vector":
        return `Vec<${this.rustType(type.of)}>`;
      case "generic":
        if (type.query) return `Box<${this.root}::Function>`;
        throw new Error(`generic result type ${type.name} used as a value`);
      case "flags":
        throw new Error("flags field has no value type");
    }
  }
}

export function resultName(c: TlCombinator): string {
  if (c.result.kind !== "named")
    throw new Error(`${c.name}: constructor result is not a named type`);
  return c.result.name;
}

// ---------------------------------------------------------------------------
// Module layout

type Put = (path: string, body: string) => void;

function moduleRoot(schema: TlSchema): string {
  const layer = schema.layer
    ? `/// MTProto layer of this schema.\npub const LAYER: i32 = ${schema.layer};\n\n`
    : "";
  return `//! Generated from \`spec/tl/${schema.file}\`.\n\n${layer}pub mod enums;\npub mod functions;\npub mod types;\n\nmod function;\npub use function::Function;\n`;
}

/** key = [namespace, group name] → list of items; preserves schema order. */
function groupBy<T>(
  items: T[],
  key: (item: T) => [string | undefined, string],
): Map<string, Map<string, T[]>> {
  const byNs = new Map<string, Map<string, T[]>>();
  for (const item of items) {
    const [ns = "", group] = key(item);
    const groups = byNs.get(ns) ?? new Map<string, T[]>();
    byNs.set(ns, groups);
    groups.set(group, [...(groups.get(group) ?? []), item]);
  }
  return byNs;
}

function emitGrouped<T>(
  put: Put,
  kind: string,
  byNs: Map<string, Map<string, T[]>>,
  render: (items: T[]) => string,
) {
  const namespaces = [...byNs.keys()].filter(Boolean).sort();
  put(
    `${kind}/mod.rs`,
    `mod root;\npub use root::*;\n\n${namespaces.map((ns) => `pub mod ${ident(ns)};`).join("\n")}\n`,
  );
  for (const [ns, groups] of byNs) {
    const dir = `${kind}/${ns || "root"}`;
    const file = (group: string) => moduleName(splitName(group).local, ns);
    const files = [...groups.keys()].map(file);
    put(`${dir}/mod.rs`, files.map((f) => `mod ${f};\npub use ${f}::*;`).join("\n"));
    for (const [group, items] of groups) put(`${dir}/${file(group)}.rs`, render(items));
  }
}

/**
 * Private module names for group files. Keywords, and names equal to the enclosing
 * namespace (`messages.Messages` → `messages/messages.rs`), get a trailing underscore.
 */
function moduleName(typeName: string, ns: string): string {
  const name = snake(typeName);
  return ident(name) === name && name !== ns ? name : `${name}_`;
}

// ---------------------------------------------------------------------------
// Fields and codecs

interface FieldInfo {
  param: TlParam;
  name: string;
  rust: string;
  /** `flags.N?true` — a bool that lives only in the flags bitmask. */
  bitOnly: boolean;
}

function fields(ctx: TlContext, c: TlCombinator): FieldInfo[] {
  return c.params
    .filter((p) => p.type.kind !== "flags")
    .map((param) => {
      const bitOnly = !!param.flag && param.type.kind === "named" && param.type.name === "true";
      const base = ctx.rustType(param.type);
      // TL has a few uppercase params (SRP: `A`, `M1`, `srp_B`); serde keeps the TL name.
      const name = ident(param.name.toLowerCase());
      return { param, name, rust: param.flag && !bitOnly ? `Option<${base}>` : base, bitOnly };
    });
}

const JSON_ADAPTERS: Record<string, string> = {
  i64: "long",
  "Option<i64>": "opt_long",
  "Vec<i64>": "vec_long",
  "Option<Vec<i64>>": "opt_vec_long",
  "Vec<u8>": "bytes",
  "Option<Vec<u8>>": "opt_bytes",
  "Vec<Vec<u8>>": "vec_bytes",
  "Option<Vec<Vec<u8>>>": "opt_vec_bytes",
  "[u8; 16]": "fixed",
  "[u8; 32]": "fixed",
  "Option<[u8; 16]>": "opt_fixed",
  "Option<[u8; 32]>": "opt_fixed",
};

function serdeAttrs(field: FieldInfo): string {
  const attrs: string[] = [];
  if (field.name.replace(/^r#/, "") !== field.param.name)
    attrs.push(`rename = "${field.param.name}"`);
  if (field.bitOnly) attrs.push(`default, skip_serializing_if = "std::ops::Not::not"`);
  else if (field.param.flag) attrs.push(`default, skip_serializing_if = "Option::is_none"`);

  const adapter = JSON_ADAPTERS[field.rust];
  if (adapter) attrs.push(`with = "crate::json::${adapter}"`);
  else if (!field.rust.includes("::") && /i64|u8/.test(field.rust)) {
    throw new Error(`no JSON adapter for ${field.rust} (${field.param.name})`);
  }
  return attrs.length > 0 ? `#[serde(${attrs.join(", ")})]\n` : "";
}

function serializeBody(ctx: TlContext, c: TlCombinator, withId: boolean): string {
  const lines: string[] = withId
    ? ["<Self as crate::Identifiable>::CONSTRUCTOR_ID.serialize(buf);"]
    : [];
  const infos = new Map(fields(ctx, c).map((f) => [f.param.name, f]));
  for (const param of c.params) {
    if (param.type.kind === "flags") {
      const bits = c.params
        .filter((p) => p.flag?.field === param.name)
        .map((p) => {
          const f = infos.get(p.name) as FieldInfo;
          const test = f.bitOnly ? `self.${f.name}` : `self.${f.name}.is_some()`;
          return `if ${test} { ${param.name} |= 1 << ${p.flag?.bit}; }`;
        });
      lines.push(
        bits.length > 0 ? `let mut ${param.name} = 0u32;` : `let ${param.name} = 0u32;`,
        ...bits,
      );
      lines.push(`${param.name}.serialize(buf);`);
      continue;
    }
    const f = infos.get(param.name) as FieldInfo;
    if (f.bitOnly) continue;
    if (param.flag)
      lines.push(`if let Some(value) = &self.${f.name} { ${write("value", param.type)} }`);
    else lines.push(write(`self.${f.name}`, param.type));
  }
  return lines.join("\n");
}

function write(expr: string, type: TlType): string {
  if (type.kind === "vector" && type.bare) return `crate::codec::write_bare_vector(buf, &${expr});`;
  return `${expr}.serialize(buf);`;
}

function deserializeBody(ctx: TlContext, c: TlCombinator, expectId: string | undefined): string {
  const lines: string[] = [];
  if (expectId) {
    lines.push(
      `let id = reader.read_u32()?;`,
      `if id != <Self as crate::Identifiable>::CONSTRUCTOR_ID { return Err(crate::DecodeError::UnexpectedConstructor { id, expected: <Self as crate::Identifiable>::NAME }); }`,
    );
  }
  const infos = new Map(fields(ctx, c).map((f) => [f.param.name, f]));
  for (const param of c.params) {
    if (param.type.kind === "flags") {
      const used = c.params.some((p) => p.flag?.field === param.name);
      lines.push(`let ${used ? "" : "_"}${param.name} = u32::deserialize(reader)?;`);
      continue;
    }
    const f = infos.get(param.name) as FieldInfo;
    const read = readExpr(ctx, param.type);
    if (f.bitOnly)
      lines.push(`let ${f.name} = ${param.flag?.field} & (1 << ${param.flag?.bit}) != 0;`);
    else if (param.flag) {
      lines.push(
        `let ${f.name} = if ${param.flag.field} & (1 << ${param.flag.bit}) != 0 { Some(${read}) } else { None };`,
      );
    } else lines.push(`let ${f.name} = ${read};`);
  }
  const names = [...infos.values()].map((f) => f.name);
  lines.push(`Ok(Self { ${names.join(", ")} })`);
  return lines.join("\n");
}

function readExpr(ctx: TlContext, type: TlType): string {
  if (type.kind === "vector" && type.bare) return "crate::codec::read_bare_vector(reader)?";
  return `<${ctx.rustType(type)}>::deserialize(reader)?`;
}

// ---------------------------------------------------------------------------
// Items

const DERIVE = "#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]";

function structItem(ctx: TlContext, c: TlCombinator): string {
  const body = fields(ctx, c)
    .map((f) => `${serdeAttrs(f)}pub ${f.name}: ${f.rust},`)
    .join("\n");
  const derive = ctx.defaultable.has(c.name)
    ? DERIVE.replace("PartialEq, ", "PartialEq, Default, ")
    : DERIVE;
  return `/// \`${formatCombinator(c)}\`\n${derive}\npub struct ${pascal(splitName(c.name).local)} {\n${body}\n}\n`;
}

function identifiable(c: TlCombinator): string {
  const name = pascal(splitName(c.name).local);
  return `impl crate::Identifiable for ${name} {\n    const CONSTRUCTOR_ID: u32 = 0x${c.id};\n    const NAME: &'static str = "${c.name}";\n}\n`;
}

function codecImpls(ctx: TlContext, c: TlCombinator, isFunction: boolean): string {
  const name = pascal(splitName(c.name).local);
  const empty = c.params.length === 0;
  const ser = serializeBody(ctx, c, isFunction);
  const de = deserializeBody(ctx, c, isFunction ? name : undefined);
  const buf = empty && !isFunction ? "_buf" : "buf";
  const reader = empty && !isFunction ? "_reader" : "reader";
  return [
    `impl crate::Serializable for ${name} {\n    fn serialize(&self, ${buf}: &mut Vec<u8>) {\n        ${ser}\n    }\n}\n`,
    `impl crate::Deserializable for ${name} {\n    fn deserialize(${reader}: &mut crate::Reader<'_>) -> Result<Self, crate::DecodeError> {\n        ${de}\n    }\n}\n`,
  ].join("\n");
}

function emitStruct(ctx: TlContext, c: TlCombinator): string {
  return [structItem(ctx, c), identifiable(c), codecImpls(ctx, c, false)].join("\n");
}

interface Variant {
  c: TlCombinator;
  name: string;
  unit: boolean;
}

function variants(typeName: string, items: TlCombinator[]): Variant[] {
  const typeLocal = pascal(splitName(typeName).local);
  const taken = new Set<string>();
  return items.map((c) => {
    const full = pascal(splitName(c.name).local);
    let name =
      full.startsWith(typeLocal) && full.length > typeLocal.length
        ? full.slice(typeLocal.length)
        : full;
    if (!/^[A-Z]/.test(name) || name === "Self" || taken.has(name)) name = full;
    taken.add(name);
    return { c, name, unit: c.params.length === 0 };
  });
}

function emitEnum(ctx: TlContext, typeName: string, items: TlCombinator[]): string {
  const name = pascal(splitName(typeName).local);
  const vs = variants(typeName, items);
  const payload = (v: Variant) => ctx.structPath(v.c.name);

  const isDefault = (v: Variant) => v.unit && ctx.enumDefault.get(typeName) === v.c;
  const decl = vs
    .map(
      (v) =>
        `#[serde(rename = "${v.c.name}")]\n${isDefault(v) ? "#[default]\n" : ""}${v.name}${v.unit ? "" : `(Box<${payload(v)}>)`},`,
    )
    .join("\n");
  const pat = (v: Variant, bind = "_") => `Self::${v.name}${v.unit ? "" : `(${bind})`}`;

  const ids = vs.map((v) => `${pat(v)} => 0x${v.c.id},`).join("\n");
  const names = vs.map((v) => `${pat(v)} => "${v.c.name}",`).join("\n");
  const ser = vs
    .map((v) => (v.unit ? `${pat(v)} => {}` : `${pat(v, "value")} => value.serialize(buf),`))
    .join("\n");
  const de = vs
    .map((v) =>
      v.unit
        ? `0x${v.c.id} => Ok(Self::${v.name}),`
        : `0x${v.c.id} => Ok(Self::${v.name}(Box::new(<${payload(v)}>::deserialize(reader)?))),`,
    )
    .join("\n");
  const froms = vs
    .map(
      (v) =>
        `impl From<${payload(v)}> for ${name} {\n    fn from(${v.unit ? "_" : "value"}: ${payload(v)}) -> Self {\n        ${v.unit ? `Self::${v.name}` : `Self::${v.name}(Box::new(value))`}\n    }\n}\n`,
    )
    .join("\n");

  const fallback = ctx.enumDefault.get(typeName);
  const fallbackVariant = fallback ? vs.find((v) => v.c === fallback) : undefined;
  // A unit default is derived (`#[default]`); a boxed payload needs a manual impl.
  const derivedDefault = fallbackVariant?.unit === true;
  const defaultImpl =
    fallbackVariant && !derivedDefault
      ? `impl Default for ${name} {\n    fn default() -> Self {\n        ${fallbackVariant.unit ? `Self::${fallbackVariant.name}` : `Self::${fallbackVariant.name}(Box::default())`}\n    }\n}\n`
      : "";

  return `/// TL type \`${typeName}\`.
${derivedDefault ? DERIVE.replace("PartialEq, ", "PartialEq, Default, ") : DERIVE}
#[serde(tag = "_")]
pub enum ${name} {
${decl}
}

${defaultImpl}

impl ${name} {
${commonAccessors(ctx, vs)}
    pub fn constructor_id(&self) -> u32 {
        match self {
${ids}
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
${names}
        }
    }
}

impl crate::Serializable for ${name} {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.constructor_id().serialize(buf);
        match self {
${ser}
        }
    }
}

impl crate::Deserializable for ${name} {
    fn deserialize(reader: &mut crate::Reader<'_>) -> Result<Self, crate::DecodeError> {
        match reader.read_u32()? {
${de}
            id => Err(crate::DecodeError::UnexpectedConstructor { id, expected: "${typeName}" }),
        }
    }
}

${froms}`;
}

/** Rust types returned by value from accessors; everything else by reference. */
const COPY_TYPES = new Set([
  "i32",
  "i64",
  "f64",
  "bool",
  "Option<i32>",
  "Option<i64>",
  "Option<f64>",
]);

/**
 * Accessors for fields every constructor of the type has with the same Rust type, e.g.
 * `MessageEntity::offset()`. Names clashing with the enum's own methods are skipped.
 */
function commonAccessors(ctx: TlContext, vs: Variant[]): string {
  if (vs.length < 2 || vs.some((v) => v.unit)) return "";
  const perVariant = vs.map((v) => new Map(fields(ctx, v.c).map((f) => [f.name, f])));
  const [first, ...rest] = perVariant;
  if (!first) return "";
  const common = [...first.values()].filter(
    (f) =>
      !["name", "constructor_id"].includes(f.name) &&
      rest.every((m) => m.get(f.name)?.rust === f.rust),
  );
  return common
    .map((f) => {
      const byValue = COPY_TYPES.has(f.rust);
      const arms = vs
        .map((v) => `Self::${v.name}(x) => ${byValue ? "" : "&"}x.${f.name},`)
        .join("\n");
      return `    pub fn ${f.name}(&self) -> ${byValue ? f.rust : `&${f.rust}`} {\n        match self {\n${arms}\n        }\n    }\n`;
    })
    .join("\n");
}

function emitFunctions(put: Put, ctx: TlContext, functions: TlCombinator[]) {
  const byNs = new Map<string, TlCombinator[]>();
  for (const f of functions) {
    const ns = splitName(f.name).ns ?? "";
    byNs.set(ns, [...(byNs.get(ns) ?? []), f]);
  }
  const namespaces = [...byNs.keys()].filter(Boolean).sort();
  const hasRoot = byNs.has("");
  put(
    "functions/mod.rs",
    `${hasRoot ? "mod root;\npub use root::*;\n\n" : ""}${namespaces.map((ns) => `pub mod ${ident(ns)};`).join("\n")}\n`,
  );
  for (const [ns, items] of byNs) {
    put(`functions/${ns || "root"}.rs`, items.map((f) => emitFunction(ctx, f)).join("\n"));
  }
}

function emitFunction(ctx: TlContext, f: TlCombinator): string {
  const name = pascal(splitName(f.name).local);
  const remote =
    f.result.kind === "generic"
      ? ""
      : `impl crate::RemoteCall for ${name} {\n    type Return = ${ctx.rustType(f.result)};\n}\n`;
  return [structItem(ctx, f), identifiable(f), codecImpls(ctx, f, true), remote].join("\n");
}

function emitFunctionEnum(ctx: TlContext, functions: TlCombinator[]): string {
  const variant = (f: TlCombinator) => {
    const { ns, local } = splitName(f.name);
    return `${ns ? pascal(ns) : ""}${pascal(local)}`;
  };
  const path = (f: TlCombinator) => ctx.functionPath(f.name);

  const decl = functions
    .map((f) => `#[serde(rename = "${f.name}")]\n${variant(f)}(Box<${path(f)}>),`)
    .join("\n");
  const ids = functions.map((f) => `Self::${variant(f)}(_) => 0x${f.id},`).join("\n");
  const names = functions.map((f) => `Self::${variant(f)}(_) => "${f.name}",`).join("\n");
  const ser = functions
    .map((f) => `Self::${variant(f)}(value) => value.serialize(buf),`)
    .join("\n");
  const de = functions
    .map(
      (f) => `0x${f.id} => Ok(Self::${variant(f)}(Box::new(<${path(f)}>::deserialize(reader)?))),`,
    )
    .join("\n");
  const froms = functions
    .map(
      (f) =>
        `impl From<${path(f)}> for Function {\n    fn from(value: ${path(f)}) -> Self {\n        Self::${variant(f)}(Box::new(value))\n    }\n}\n`,
    )
    .join("\n");

  return `/// Every function of \`${ctx.schema.file}\`, for decoding incoming calls.
${DERIVE}
#[serde(tag = "_")]
pub enum Function {
${decl}
}

impl Function {
    pub fn constructor_id(&self) -> u32 {
        match self {
${ids}
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
${names}
        }
    }
}

impl crate::Serializable for Function {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
${ser}
        }
    }
}

impl crate::Deserializable for Function {
    fn deserialize(reader: &mut crate::Reader<'_>) -> Result<Self, crate::DecodeError> {
        match reader.peek_u32()? {
${de}
            id => Err(crate::DecodeError::UnexpectedConstructor { id, expected: "Function" }),
        }
    }
}

${froms}`;
}
