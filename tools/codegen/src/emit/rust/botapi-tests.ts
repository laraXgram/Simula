import type { BotApiSpec, Field, TypeRef } from "../../botapi/model";
import { GENERATED_HEADER } from "../writer";
import { fieldInfo, type Model, variantName } from "./botapi";
import { pascal } from "./naming";

const CHECKS_PER_TEST = 50;
const CRATE = "simula_botapi_types";

type Mode = "full" | "min";

/**
 * JSON round trips for every Bot API type, method and union variant. Top-level values
 * set every field; nested values set required fields only. Union variants go through
 * their union, so a variant that deserializes as a different one fails the test —
 * this is what checks the untagged-union ordering and the constant tags.
 */
export function emitBotApiRoundtripTests(model: Model, spec: BotApiSpec): string {
  const sampler = new Sampler(model);
  const checks: string[] = [];

  for (const type of spec.types) {
    if (type.kind === "object") checks.push(`check(${sampler.object(type.name, "full")});`);
    else checks.push(...sampler.unionVariants(type.name));
  }
  for (const name of model.anonymousUnions.keys()) checks.push(...sampler.unionVariants(name));
  for (const method of spec.methods) {
    checks.push(
      `check(${CRATE}::methods::${pascal(method.name)} { ${sampler.fields(pascal(method.name), method.params, "full")} });`,
    );
  }

  const tests: string[] = [];
  for (let i = 0; i < checks.length; i += CHECKS_PER_TEST) {
    const n = String(i / CHECKS_PER_TEST).padStart(2, "0");
    tests.push(
      `#[test]\nfn roundtrip_${n}() {\n${checks.slice(i, i + CHECKS_PER_TEST).join("\n")}\n}`,
    );
  }

  return `${GENERATED_HEADER}

//! JSON round trip of a sample value for every Bot API type, union variant and method.

fn check<T>(value: T)
where
    T: PartialEq + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
{
    let json = serde_json::to_string(&value).expect("encode");
    dump(std::any::type_name::<T>(), &json);
    let decoded: T = serde_json::from_str(&json).unwrap_or_else(|e| panic!("decode {json}: {e}"));
    assert_eq!(decoded, value, "round trip of {json}");
}

/// With \`SIMULA_SAMPLES_DIR\` set, every sample's JSON is appended there so
/// \`bun run codegen verify:ts\` can type-check it against the TypeScript types.
fn dump(type_name: &str, json: &str) {
    use std::io::Write;
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let Some(dir) = std::env::var_os("SIMULA_SAMPLES_DIR") else { return };
    let _guard = LOCK.lock().unwrap();
    let path = std::path::Path::new(&dir).join(format!("{}.jsonl", env!("CARGO_PKG_NAME")));
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
    file.write_all(format!("{type_name}\t{json}\n").as_bytes()).unwrap();
}

${tests.join("\n\n")}
`;
}

class Sampler {
  /** Nesting depth needed to build a minimal value (required fields only). */
  private readonly cost = new Map<string, number>();

  constructor(private readonly model: Model) {
    const names = [...model.types.keys(), ...model.anonymousUnions.keys()];
    let changed = true;
    while (changed) {
      changed = false;
      for (const name of names) {
        const next = this.costOf(name);
        if (next < (this.cost.get(name) ?? Number.POSITIVE_INFINITY)) {
          this.cost.set(name, next);
          changed = true;
        }
      }
    }
  }

  private costOf(name: string): number {
    const type = this.model.types.get(name);
    if (type?.kind === "object") {
      return (
        1 + Math.max(0, ...type.fields.filter((f) => f.required).map((f) => this.refCost(f.type)))
      );
    }
    return Math.min(...this.model.orderedVariants(name).map((v) => this.refCost(v)));
  }

  private refCost(ref: TypeRef): number {
    if (ref.kind === "primitive" || ref.kind === "array") return 0;
    const name = ref.kind === "ref" ? ref.name : (this.model.unionName(ref) as string);
    if (this.model.names.builtins.includes(name)) return 0;
    return this.cost.get(name) ?? Number.POSITIVE_INFINITY;
  }

  object(name: string, mode: Mode): string {
    const type = this.model.types.get(name);
    if (type?.kind !== "object") throw new Error(`${name} is not an object`);
    return `${CRATE}::types::${name} { ${this.fields(name, type.fields, mode)} }`;
  }

  fields(owner: string, fields: Field[], mode: Mode): string {
    return fields
      .map((field) => {
        const info = fieldInfo(this.model, owner, field);
        const name = info.name;
        if (info.tag) {
          const tag = `${CRATE}::tags::${info.tag}`;
          return `${name}: ${field.required ? tag : mode === "full" ? `Some(${tag})` : "None"},`;
        }
        if (info.trueFlag) return `${name}: ${mode === "full"},`;
        // Enum-constrained strings get a documented value, as Telegram would send.
        const allowed = field.constraints?.enum?.[0];
        if (
          allowed !== undefined &&
          field.type.kind === "primitive" &&
          field.type.name === "String"
        ) {
          if (!field.required && mode === "min") return `${name}: None,`;
          const value = `String::from(${JSON.stringify(allowed)})`;
          return field.required ? `${name}: ${value},` : `${name}: Some(${value}),`;
        }
        if (!field.required && mode === "min") return `${name}: None,`;
        const boxed = info.rust.replace(/^Option</, "").startsWith("Box<");
        const inner =
          field.type.kind === "array" && mode === "full"
            ? this.value(field.type, "full")
            : this.value(field.type, "min");
        const value = boxed ? `Box::new(${inner})` : inner;
        return field.required ? `${name}: ${value},` : `${name}: Some(${value}),`;
      })
      .join(" ");
  }

  /** `mode` only decides whether arrays get an element. */
  value(ref: TypeRef, mode: Mode): string {
    switch (ref.kind) {
      case "primitive":
        return PRIMITIVE_SAMPLES[ref.name];
      case "array":
        return mode === "full" ? `vec![${this.value(ref.of, "min")}]` : "vec![]";
      case "ref": {
        const type = this.model.types.get(ref.name);
        return type?.kind === "object" ? this.object(ref.name, "min") : this.unionValue(ref.name);
      }
      case "union": {
        const name = this.model.unionName(ref) as string;
        if (name === "ChatId") return `${CRATE}::ChatId::Username(String::from("@channel"))`;
        if (name === "InputFile") return `${CRATE}::InputFile::Reference(String::from("file_id"))`;
        return this.unionValue(name);
      }
    }
  }

  /** The cheapest variant of a union, minimally filled (keeps recursive unions finite). */
  private unionValue(name: string): string {
    const variants = this.model.orderedVariants(name);
    const best = Math.min(...variants.map((v) => this.refCost(v)));
    const cheapest = variants.find((v) => this.refCost(v) === best) as TypeRef;
    return this.variantValue(name, cheapest, "min");
  }

  private variantValue(union: string, ref: TypeRef, mode: Mode): string {
    const variant = `${CRATE}::types::${union}::${variantName(union, ref)}`;
    const boxed = this.model.variantType(union, ref).startsWith("Box<");
    if (ref.kind === "ref" && this.model.types.get(ref.name)?.kind === "object") {
      const literal = this.object(ref.name, mode);
      return `${variant}(${boxed ? `Box::new(${literal})` : literal})`;
    }
    const inner = this.value(ref, mode);
    return `${variant}(${boxed ? `Box::new(${inner})` : inner})`;
  }

  unionVariants(name: string): string[] {
    return this.model
      .orderedVariants(name)
      .map((ref) => `check(${this.variantValue(name, ref, "full")});`);
  }
}

const PRIMITIVE_SAMPLES = {
  Integer: "7",
  Float: "1.5",
  String: 'String::from("sample ✓")',
  Boolean: "true",
  True: "true",
  InputFile: `${CRATE}::InputFile::Attach(String::from("upload"))`,
} as const;
