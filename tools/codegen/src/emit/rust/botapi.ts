import type { BotApiSpec, Field, Method, ObjectType, TypeDef, TypeRef } from "../../botapi/model";
import { formatTypeRef } from "../../botapi/type-expr";
import { type FileTree, GENERATED_HEADER } from "../writer";
import { ident, pascal, snake } from "./naming";

/** `spec/botapi/names.json`. */
export interface BotApiNames {
  builtins: string[];
  unions: Record<string, string>;
}

/**
 * Rust emitter for the Bot API. Layout under `generated/`:
 *
 *   types/<type>.rs     one struct per object, one untagged enum per union (named or anonymous)
 *   methods/<method>.rs one parameter struct per method: `Method` + `Validate`
 *   tags.rs             one unit type per documented constant ("always “photo”")
 *   registry.rs         every method's name, parameter kinds and multipart flag
 *
 * Unions deserialize untagged; the tag types make that exact, because a variant whose
 * constant field doesn't match fails and the next one is tried. Variants are ordered
 * most-specific first (constant fields, then required fields).
 */
export function emitBotApi(spec: BotApiSpec, names: BotApiNames): FileTree {
  const model = new Model(spec, names);
  const tree: FileTree = new Map();
  const put = (path: string, body: string) => tree.set(path, `${GENERATED_HEADER}\n\n${body}`);

  put(
    "mod.rs",
    `//! Generated from \`spec/botapi/api.json\` (Bot API ${spec.release.version}).\n\npub mod methods;\npub mod registry;\npub mod tags;\npub mod types;\n`,
  );
  put("tags.rs", emitTags(model));

  const typeFiles: string[] = [];
  for (const type of spec.types) {
    const file = moduleName(type.name);
    typeFiles.push(file);
    put(
      `types/${file}.rs`,
      type.kind === "object" ? emitObject(model, type) : emitNamedUnion(model, type.name),
    );
  }
  for (const name of model.anonymousUnions.keys()) {
    const file = moduleName(name);
    typeFiles.push(file);
    put(`types/${file}.rs`, emitAnonymousUnion(model, name));
  }
  put("types/mod.rs", typeFiles.map((f) => `mod ${f};\npub use ${f}::*;`).join("\n"));

  const methodFiles = spec.methods.map((m) => moduleName(m.name));
  for (const method of spec.methods)
    put(`methods/${moduleName(method.name)}.rs`, emitMethod(model, method));
  put("methods/mod.rs", methodFiles.map((f) => `mod ${f};\npub use ${f}::*;`).join("\n"));

  put("registry.rs", emitRegistry(spec));
  return tree;
}

function moduleName(name: string): string {
  const file = snake(name);
  return ident(file) === file ? file : `${file}_`;
}

// ---------------------------------------------------------------------------
// Model: names, unions, recursion (boxing) and Default-ability

interface UnionInfo {
  name: string;
  members: TypeRef[];
  description: string;
}

export class Model {
  readonly types = new Map<string, TypeDef>();
  /** Anonymous unions by their configured name. */
  readonly anonymousUnions = new Map<string, UnionInfo>();
  readonly tags = new Map<string | number, string>();
  private readonly sccOf = new Map<string, number>();
  private readonly defaultable = new Set<string>();

  constructor(
    readonly spec: BotApiSpec,
    readonly names: BotApiNames,
  ) {
    for (const t of spec.types) this.types.set(t.name, t);
    this.collectAnonymousUnions();
    this.collectTags();
    this.computeSccs();
    this.computeDefaultable();
  }

  private collectAnonymousUnions() {
    const visit = (ref: TypeRef, where: string) => {
      if (ref.kind === "array") return visit(ref.of, where);
      if (ref.kind !== "union") return;
      const key = formatTypeRef(ref);
      const name = this.names.unions[key];
      if (!name)
        throw new Error(`${where}: anonymous union "${key}" has no name in spec/botapi/names.json`);
      if (this.names.builtins.includes(name) || this.anonymousUnions.has(name)) return;
      this.anonymousUnions.set(name, { name, members: ref.of, description: key });
    };
    for (const m of this.spec.methods) {
      visit(m.returns, `${m.name} returns`);
      for (const p of m.params) visit(p.type, `${m.name}.${p.name}`);
    }
    for (const t of this.spec.types)
      if (t.kind === "object") for (const f of t.fields) visit(f.type, `${t.name}.${f.name}`);
  }

  private collectTags() {
    const taken = new Set<string>();
    const fields = [
      ...this.spec.types.flatMap((t) => (t.kind === "object" ? t.fields : [])),
      ...this.spec.methods.flatMap((m) => m.params),
    ];
    for (const f of fields) {
      const value = f.constraints?.const;
      if (value === undefined || this.tags.has(value)) continue;
      let name =
        typeof value === "number"
          ? `Int${value < 0 ? "Minus" : ""}${Math.abs(value)}`
          : pascal(value.replace(/[^A-Za-z0-9_]/g, "_"));
      if (!/^[A-Z]/.test(name)) name = `Tag${name}`;
      while (taken.has(name)) name += "_";
      taken.add(name);
      this.tags.set(value, name);
    }
  }

  /** Types reachable without passing through a Vec (which already adds indirection). */
  private directRefs(name: string): string[] {
    const out: string[] = [];
    const collect = (ref: TypeRef) => {
      if (ref.kind === "ref") out.push(ref.name);
      else if (ref.kind === "union") {
        const union = this.unionName(ref);
        if (union && !this.names.builtins.includes(union)) out.push(union);
      }
    };
    const type = this.types.get(name);
    if (type?.kind === "object") for (const f of type.fields) collect(f.type);
    else if (type?.kind === "union") for (const v of type.variants) collect(v);
    else for (const m of this.anonymousUnions.get(name)?.members ?? []) collect(m);
    return out;
  }

  private computeSccs() {
    const nodes = [...this.types.keys(), ...this.anonymousUnions.keys()];
    const index = new Map<string, number>();
    const low = new Map<string, number>();
    const stack: string[] = [];
    const onStack = new Set<string>();
    let counter = 0;
    let scc = 0;
    const strong = (v: string) => {
      index.set(v, counter);
      low.set(v, counter++);
      stack.push(v);
      onStack.add(v);
      for (const w of this.directRefs(v)) {
        if (!index.has(w)) {
          strong(w);
          low.set(v, Math.min(low.get(v) as number, low.get(w) as number));
        } else if (onStack.has(w))
          low.set(v, Math.min(low.get(v) as number, index.get(w) as number));
      }
      if (low.get(v) === index.get(v)) {
        for (let w = stack.pop(); w !== undefined; w = stack.pop()) {
          onStack.delete(w);
          this.sccOf.set(w, scc);
          if (w === v) break;
        }
        scc++;
      }
    };
    for (const v of nodes) if (!index.has(v)) strong(v);
  }

  /** A direct reference from `owner` to `target` needs a Box when they form a cycle. */
  needsBox(owner: string, target: string): boolean {
    return this.sccOf.has(owner) && this.sccOf.get(owner) === this.sccOf.get(target);
  }

  private computeDefaultable() {
    const all = [...this.types.keys(), ...this.anonymousUnions.keys()];
    for (const name of all) this.defaultable.add(name);
    let changed = true;
    while (changed) {
      changed = false;
      for (const name of all) {
        if (!this.defaultable.has(name)) continue;
        const type = this.types.get(name);
        const ok =
          type?.kind === "object"
            ? type.fields.every((f) => !f.required || this.refDefaultable(f.type))
            : this.refDefaultable(this.orderedVariants(name)[0] as TypeRef);
        if (!ok) {
          this.defaultable.delete(name);
          changed = true;
        }
      }
    }
  }

  private refDefaultable(ref: TypeRef): boolean {
    if (ref.kind === "primitive" || ref.kind === "array") return true;
    if (ref.kind === "union") {
      const name = this.unionName(ref);
      return !name || this.names.builtins.includes(name) || this.defaultable.has(name);
    }
    return this.defaultable.has(ref.name);
  }

  isDefaultable(name: string): boolean {
    return this.defaultable.has(name);
  }

  unionName(ref: TypeRef & { kind: "union" }): string | undefined {
    return this.names.unions[formatTypeRef(ref)];
  }

  /** Union members, most specific first: constant fields, then required fields. */
  orderedVariants(name: string): TypeRef[] {
    const type = this.types.get(name);
    const members =
      type?.kind === "union" ? type.variants : (this.anonymousUnions.get(name)?.members ?? []);
    const score = (ref: TypeRef): [number, number] => {
      const object = ref.kind === "ref" ? this.types.get(ref.name) : undefined;
      if (object?.kind !== "object") return [-1, -1];
      const consts = object.fields.filter((f) => f.constraints?.const !== undefined).length;
      return [consts, object.fields.filter((f) => f.required).length];
    };
    return members
      .map((ref, i) => ({ ref, i, s: score(ref) }))
      .sort((a, b) => b.s[0] - a.s[0] || b.s[1] - a.s[1] || a.i - b.i)
      .map((x) => x.ref);
  }

  /** Whether a ref names a generated object or union (not a primitive or builtin). */
  isComposite(ref: TypeRef): boolean {
    if (ref.kind === "ref") return true;
    if (ref.kind !== "union") return false;
    return !this.names.builtins.includes(this.unionName(ref) as string);
  }

  /**
   * Field type. Optional nested objects are boxed too: inline they made `Message`
   * 22 KB and `Update` 167 KB.
   */
  fieldType(ref: TypeRef, owner: string, optional: boolean): string {
    const rust = this.rustType(ref, owner);
    return optional && this.isComposite(ref) && !rust.startsWith("Box<") ? `Box<${rust}>` : rust;
  }

  /** Union payload type: object variants are boxed so the enum stays pointer-sized. */
  variantType(union: string, ref: TypeRef): string {
    const rust = this.rustType(ref, union);
    return ref.kind === "ref" && !rust.startsWith("Box<") ? `Box<${rust}>` : rust;
  }

  /** Rust type of a value of `ref` appearing directly inside `owner`. */
  rustType(ref: TypeRef, owner: string | undefined): string {
    switch (ref.kind) {
      case "primitive":
        return PRIMITIVES[ref.name];
      case "array":
        return `Vec<${this.rustType(ref.of, undefined)}>`;
      case "ref":
        return owner && this.needsBox(owner, ref.name)
          ? `Box<crate::types::${ref.name}>`
          : `crate::types::${ref.name}`;
      case "union": {
        const name = this.unionName(ref) as string;
        if (this.names.builtins.includes(name)) return `crate::${name}`;
        return owner && this.needsBox(owner, name)
          ? `Box<crate::types::${name}>`
          : `crate::types::${name}`;
      }
    }
  }
}

const PRIMITIVES = {
  Integer: "i64",
  Float: "f64",
  String: "String",
  Boolean: "bool",
  True: "bool",
  InputFile: "crate::InputFile",
} as const;

// ---------------------------------------------------------------------------
// Docs

const DOCS = "https://core.telegram.org/bots/api";

/** Markdown from the spec → rustdoc lines: links reduced to their text. */
function doc(markdown: string, indent = ""): string {
  const text = markdown
    .replace(/\[([^\]]*)\]\((#[^)]*)\)/g, "`$1`")
    .replace(/\[([^\]]*)\]\(([^)]*)\)/g, "[$1]($2)")
    .trim();
  if (!text) return "";
  return `${text
    .split("\n")
    .map((line) => `${indent}///${line ? ` ${line}` : ""}`)
    .join("\n")}\n`;
}

function docLink(name: string): string {
  return `///\n/// <${DOCS}#${name.toLowerCase()}>\n`;
}

// ---------------------------------------------------------------------------
// Fields

export interface FieldInfo {
  field: Field;
  name: string;
  rust: string;
  /** `True`-typed optional flag: a bool omitted when false. */
  trueFlag: boolean;
  tag: string | undefined;
}

export function fieldInfo(model: Model, owner: string, field: Field): FieldInfo {
  const constant = field.constraints?.const;
  const tag = constant === undefined ? undefined : model.tags.get(constant);
  const trueFlag = !field.required && field.type.kind === "primitive" && field.type.name === "True";
  let rust = tag ? `crate::tags::${tag}` : model.fieldType(field.type, owner, !field.required);
  if (!field.required && !trueFlag) rust = `Option<${rust}>`;
  return { field, name: ident(field.name), rust, trueFlag, tag };
}

function serdeAttrs(f: FieldInfo): string {
  const attrs: string[] = [];
  if (f.name.replace(/^r#/, "") !== f.field.name) attrs.push(`rename = "${f.field.name}"`);
  if (f.trueFlag) attrs.push(`default, skip_serializing_if = "std::ops::Not::not"`);
  else if (!f.field.required) attrs.push(`default, skip_serializing_if = "Option::is_none"`);
  return attrs.length > 0 ? `    #[serde(${attrs.join(", ")})]\n` : "";
}

function structBody(infos: FieldInfo[]): string {
  return infos
    .map((f) => `${doc(f.field.description, "    ")}${serdeAttrs(f)}    pub ${f.name}: ${f.rust},`)
    .join("\n");
}

/** Whether a TypeRef can contain generated types that have limits of their own. */
function hasNested(ref: TypeRef): boolean {
  switch (ref.kind) {
    case "primitive":
      return false;
    case "array":
      return hasNested(ref.of);
    case "ref":
      return true;
    case "union":
      return ref.of.some(hasNested);
  }
}

function validateBody(infos: FieldInfo[]): string {
  const lines: string[] = [];
  for (const f of infos) {
    if (f.tag || f.trueFlag) continue;
    const c = f.field.constraints;
    const checks: string[] = [];
    const n = (v: number | undefined) => (v === undefined ? "None" : `Some(${v})`);
    const nf = (v: number | undefined) => (v === undefined ? "None" : `Some(${v}f64)`);

    // Lengths measured after entity parsing are checked by the gateway after parsing.
    if (
      c?.length &&
      !c.length.after_entities &&
      f.rust.includes("String") &&
      !f.rust.includes("Vec")
    ) {
      const fn = c.length.unit === "bytes" ? "bytes" : "chars";
      checks.push(`crate::validate::${fn}(&p, v, ${n(c.length.min)}, ${n(c.length.max)}, out);`);
    }
    if (c?.enum && f.rust.includes("String") && !f.rust.includes("Vec")) {
      checks.push(
        `crate::validate::one_of(&p, v, &[${c.enum.map((e) => JSON.stringify(e)).join(", ")}], out);`,
      );
    }
    if (c?.range && /\b(i64|f64)\b/.test(f.rust) && !f.rust.includes("Vec")) {
      const value = /\bi64\b/.test(f.rust) ? "*v as f64" : "*v";
      checks.push(
        `crate::validate::range(&p, ${value}, ${nf(c.range.min)}, ${nf(c.range.max)}, out);`,
      );
    }
    if (c?.items && f.rust.includes("Vec")) {
      checks.push(`crate::validate::items(&p, v, ${n(c.items.min)}, ${n(c.items.max)}, out);`);
    }
    const nested = hasNested(f.field.type);
    if (checks.length === 0 && !nested) continue;

    const path = `let p = crate::validate::join(path, "${f.field.name}");`;
    const recurse = nested ? `crate::Validate::validate_into(&self.${f.name}, &p, out);` : "";
    const own =
      checks.length === 0
        ? ""
        : f.field.required
          ? `{ let v = &self.${f.name}; ${checks.join(" ")} }`
          : `if let Some(v) = &self.${f.name} { ${checks.join(" ")} }`;
    lines.push(`{ ${path} ${own} ${recurse} }`);
  }
  return lines.join("\n");
}

function validateImpl(name: string, body: string): string {
  const [path, out] = body ? ["path", "out"] : ["_path", "_out"];
  return `impl crate::Validate for ${name} {\n    fn validate_into(&self, ${path}: &str, ${out}: &mut Vec<crate::Violation>) {\n${body}\n    }\n}\n`;
}

// ---------------------------------------------------------------------------
// Items

function derives(defaultable: boolean): string {
  return `#[derive(Debug, Clone, PartialEq, ${defaultable ? "Default, " : ""}serde::Serialize, serde::Deserialize)]`;
}

function emitObject(model: Model, type: ObjectType): string {
  const infos = type.fields.map((f) => fieldInfo(model, type.name, f));
  return `${doc(type.description)}${docLink(type.name)}${derives(model.isDefaultable(type.name))}
pub struct ${type.name} {
${structBody(infos)}
}

${validateImpl(type.name, validateBody(infos))}${type.name === "Update" ? updateKinds(infos) : ""}`;
}

/** `Update` carries exactly one optional payload; its field name is the update kind. */
function updateKinds(infos: FieldInfo[]): string {
  const kinds = infos.filter((f) => !f.field.required).map((f) => f.field.name);
  const checks = kinds
    .map(
      (k) => `        if self.${ident(k)}.is_some() {\n            return Some("${k}");\n        }`,
    )
    .join("\n");
  return `
impl Update {
    /// Every update kind, as used in \`allowed_updates\`.
    pub const KINDS: &'static [&'static str] = &[${kinds.map((k) => JSON.stringify(k)).join(", ")}];

    /// The kind of this update (the name of its payload field).
    pub fn kind(&self) -> Option<&'static str> {
${checks}
        None
    }
}
`;
}

export function variantName(union: string, ref: TypeRef): string {
  if (ref.kind === "array") return "Array";
  if (ref.kind === "primitive") return ref.name;
  const name = ref.kind === "ref" ? ref.name : "Union";
  return name.startsWith(union) &&
    name.length > union.length &&
    /^[A-Z]/.test(name.slice(union.length))
    ? name.slice(union.length)
    : name;
}

function emitUnionEnum(model: Model, name: string, description: string): string {
  const variants = model
    .orderedVariants(name)
    .map((ref) => ({ ref, name: variantName(name, ref), rust: model.variantType(name, ref) }));
  const decl = variants.map((v) => `    ${v.name}(${v.rust}),`).join("\n");
  const validate = variants
    .map((v) =>
      hasNested(v.ref)
        ? `Self::${v.name}(value) => crate::Validate::validate_into(value, path, out),`
        : `Self::${v.name}(_) => {}`,
    )
    .join("\n");
  const anyNested = variants.some((v) => hasNested(v.ref));
  const froms = variants
    .filter((v) => v.ref.kind === "ref")
    .map((v) => {
      const target = `crate::types::${(v.ref as { name: string }).name}`;
      const wrap = v.rust.startsWith("Box<") ? "Box::new(value)" : "value";
      return `impl From<${target}> for ${name} {\n    fn from(value: ${target}) -> Self {\n        Self::${v.name}(${wrap})\n    }\n}\n`;
    })
    .join("\n");
  const first = variants[0];
  const defaultImpl =
    model.isDefaultable(name) && first
      ? `impl Default for ${name} {\n    fn default() -> Self {\n        Self::${first.name}(Default::default())\n    }\n}\n`
      : "";

  return `${description}${derives(false)}
#[serde(untagged)]
pub enum ${name} {
${decl}
}

impl crate::Validate for ${name} {
    fn validate_into(&self, ${anyNested ? "path" : "_path"}: &str, ${anyNested ? "out" : "_out"}: &mut Vec<crate::Violation>) {
        match self {
${validate}
        }
    }
}

${defaultImpl}
${froms}`;
}

function emitNamedUnion(model: Model, name: string): string {
  const type = model.types.get(name);
  return emitUnionEnum(model, name, `${doc(type?.description ?? "")}${docLink(name)}`);
}

function emitAnonymousUnion(model: Model, name: string): string {
  const info = model.anonymousUnions.get(name);
  return emitUnionEnum(model, name, `/// \`${info?.description}\` in the docs.\n`);
}

const PARAM_KINDS: Record<string, string> = {
  Integer: "Integer",
  Float: "Float",
  String: "String",
  Boolean: "Boolean",
  True: "Boolean",
  InputFile: "InputFile",
};

function paramKind(model: Model, ref: TypeRef): string {
  if (ref.kind === "primitive") return PARAM_KINDS[ref.name] ?? "Json";
  if (ref.kind === "union") {
    const name = model.unionName(ref);
    if (name === "ChatId") return "IntegerOrString";
    if (name === "InputFile") return "InputFile";
  }
  return "Json";
}

function emitMethod(model: Model, method: Method): string {
  const name = pascal(method.name);
  const infos = method.params.map((f) => fieldInfo(model, name, f));
  const defaultable = method.params.every((p) => !p.required || modelDefaultable(model, p.type));
  const params = method.params
    .map(
      (p) =>
        `crate::ParamSpec { name: "${p.name}", kind: crate::ParamKind::${paramKind(model, p.type)}, required: ${p.required} },`,
    )
    .join("\n");
  const body = infos.length > 0 ? `{\n${structBody(infos)}\n}` : "{}";

  return `${doc(method.description)}${docLink(method.name)}${derives(defaultable)}
pub struct ${name} ${body}

impl crate::Method for ${name} {
    const NAME: &'static str = "${method.name}";
    const PARAMS: &'static [crate::ParamSpec] = &[
${params}
    ];
    const MULTIPART: bool = ${method.multipart};
    type Return = ${model.rustType(method.returns, undefined)};
}

${validateImpl(name, validateBody(infos))}`;
}

function modelDefaultable(model: Model, ref: TypeRef): boolean {
  if (ref.kind === "primitive" || ref.kind === "array") return true;
  if (ref.kind === "union") {
    const name = model.unionName(ref) as string;
    return model.names.builtins.includes(name) || model.isDefaultable(name);
  }
  return model.isDefaultable(ref.name);
}

function emitTags(model: Model): string {
  const lines = [...model.tags].map(([value, name]) =>
    typeof value === "number"
      ? `crate::int_tag!(${name}, ${value});`
      : `crate::string_tag!(${name}, ${JSON.stringify(value)});`,
  );
  return `//! One type per documented constant. Each serializes to its value and rejects any\n//! other value, which is what lets untagged unions pick the right variant.\n\n${lines.join("\n")}\n`;
}

function emitRegistry(spec: BotApiSpec): string {
  const entries = spec.methods
    .map((m) => {
      const t = `crate::methods::${pascal(m.name)}`;
      return `crate::MethodInfo { name: <${t} as crate::Method>::NAME, params: <${t} as crate::Method>::PARAMS, multipart: <${t} as crate::Method>::MULTIPART },`;
    })
    .join("\n");
  return `/// Bot API version the types were generated from.
pub const BOT_API_VERSION: &str = "${spec.release.version}";

/// Every Bot API method, in docs order.
pub const METHODS: &[crate::MethodInfo] = &[
${entries}
];
`;
}
