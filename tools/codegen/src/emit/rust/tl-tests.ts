import type { TlCombinator, TlSchema, TlType } from "../../tl/model";
import { GENERATED_HEADER } from "../writer";
import { ident } from "./naming";
import { resultName, SKIPPED_TYPES, TlContext, type TlEmitOptions } from "./tl";

const CHECKS_PER_TEST = 100;

/**
 * Round-trip tests for every constructor and function of the given schemas. Each
 * top-level value fills every field (optional ones included, bit flags set) while
 * nested values use the cheapest constructor of their type, so every field of every
 * combinator is exercised through both the binary and the JSON-TL codec.
 */
export function emitTlRoundtripTests(
  schemas: { schema: TlSchema; root: string; options?: TlEmitOptions }[],
): string {
  const checks: string[] = [];
  for (const { schema, root, options } of schemas) {
    const sampler = new Sampler(schema, root, options ?? {});
    for (const c of schema.constructors) {
      if (SKIPPED_TYPES.has(resultName(c))) continue;
      checks.push(
        `check(${sampler.ctx.enumPath(resultName(c))}::from(${sampler.literal(c, "full")}));`,
      );
    }
    for (const f of schema.functions) {
      checks.push(`check(${root}::Function::from(${sampler.literal(f, "full")}));`);
    }
  }

  const tests: string[] = [];
  for (let i = 0; i < checks.length; i += CHECKS_PER_TEST) {
    const n = String(i / CHECKS_PER_TEST).padStart(2, "0");
    tests.push(
      `#[test]\nfn roundtrip_${n}() {\n${checks.slice(i, i + CHECKS_PER_TEST).join("\n")}\n}`,
    );
  }

  return `${GENERATED_HEADER}

//! Binary and JSON-TL round trip of a sample value for every TL combinator.

use simula_tl::{Deserializable, Serializable};

fn check<T>(value: T)
where
    T: Serializable + Deserializable + PartialEq + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
{
    let bytes = value.to_bytes();
    let decoded = T::from_bytes(&bytes).unwrap_or_else(|e| panic!("binary decode of {value:?}: {e}"));
    assert_eq!(decoded, value, "binary round trip");

    let json = serde_json::to_string(&value).expect("json encode");
    let decoded: T = serde_json::from_str(&json).unwrap_or_else(|e| panic!("json decode of {json}: {e}"));
    assert_eq!(decoded, value, "json round trip of {json}");
}

${tests.join("\n\n")}
`;
}

type Mode = "full" | "min";

class Sampler {
  readonly ctx: TlContext;
  private readonly byType = new Map<string, TlCombinator[]>();
  /** Cheapest nesting depth at which each boxed type can be built (required fields only). */
  private readonly depth = new Map<string, number>();
  private readonly functions: TlCombinator[];

  constructor(
    schema: TlSchema,
    private readonly root: string,
    private readonly options: TlEmitOptions,
  ) {
    this.ctx = new TlContext(schema, root, options);
    this.functions = schema.functions;
    for (const c of schema.constructors) {
      const name = resultName(c);
      this.byType.set(name, [...(this.byType.get(name) ?? []), c]);
    }
    this.computeDepths();
  }

  private computeDepths() {
    let changed = true;
    while (changed) {
      changed = false;
      for (const [type, constructors] of this.byType) {
        const best = Math.min(...constructors.map((c) => this.cost(c)));
        if (best < (this.depth.get(type) ?? Number.POSITIVE_INFINITY)) {
          this.depth.set(type, best);
          changed = true;
        }
      }
    }
  }

  private cost(c: TlCombinator): number {
    let worst = 0;
    for (const p of c.params) {
      if (p.flag || p.type.kind !== "named" || p.type.name in PRIMITIVE_SAMPLES) continue;
      const inner = p.type.bare
        ? this.cost(this.bareConstructor(p.type.name))
        : this.depth.get(p.type.name);
      worst = Math.max(worst, inner ?? Number.POSITIVE_INFINITY);
    }
    return 1 + worst;
  }

  private bareConstructor(name: string): TlCombinator {
    const found = [...this.byType.values()].flat().find((c) => c.name === name);
    if (!found) throw new Error(`no constructor ${name}`);
    return found;
  }

  private cheapest(type: string): TlCombinator {
    const constructors = this.byType.get(type);
    const target = this.depth.get(type);
    const found = constructors?.find((c) => this.cost(c) === target);
    if (!found || target === undefined || !Number.isFinite(target))
      throw new Error(`cannot build ${type}`);
    return found;
  }

  literal(c: TlCombinator, mode: Mode): string {
    const path = this.functions.includes(c)
      ? this.ctx.functionPath(c.name)
      : this.ctx.structPath(c.name);
    const fields = c.params
      .filter((p) => p.type.kind !== "flags")
      .map((p) => {
        const name = ident(p.name.toLowerCase());
        const bitOnly = p.flag && p.type.kind === "named" && p.type.name === "true";
        if (bitOnly) return `${name}: ${mode === "full"},`;
        if (p.flag)
          return `${name}: ${mode === "full" ? `Some(${this.value(p.type, "full")})` : "None"},`;
        return `${name}: ${this.value(p.type, mode)},`;
      });
    return `${path} { ${fields.join(" ")} }`;
  }

  /** `mode` here only decides whether vectors get an element. */
  private value(type: TlType, mode: Mode): string {
    switch (type.kind) {
      case "named": {
        if (type.name === "string" && this.options.binaryStrings)
          return PRIMITIVE_SAMPLES.bytes as string;
        const primitive = PRIMITIVE_SAMPLES[type.name];
        if (primitive) return primitive;
        if (type.bare) return this.literal(this.bareConstructor(type.name), "min");
        return `${this.ctx.enumPath(type.name)}::from(${this.literal(this.cheapest(type.name), "min")})`;
      }
      case "vector":
        return mode === "full" ? `vec![${this.value(type.of, "min")}]` : "vec![]";
      case "generic":
        return `Box::new(${this.root}::Function::from(${this.literal(this.simplestFunction(), "min")}))`;
      case "flags":
        throw new Error("flags has no value");
    }
  }

  private simplestFunction(): TlCombinator {
    const found = this.functions.find((f) => f.params.length === 0 && f.result.kind !== "generic");
    if (!found) throw new Error("no parameterless function to use as a generic query");
    return found;
  }
}

const PRIMITIVE_SAMPLES: Record<string, string> = {
  int: "7",
  long: "1_099_511_627_776",
  double: "1.5",
  string: 'String::from("sample ✓")',
  bytes: "vec![1u8, 2, 3]",
  int128: "[7u8; 16]",
  int256: "[9u8; 32]",
  Bool: "true",
};
