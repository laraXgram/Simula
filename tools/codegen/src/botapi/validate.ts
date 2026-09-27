import type { BotApiSpec, TypeRef } from "./model";
import { namesIn } from "./type-expr";

const PRIMITIVES = new Set(["Integer", "Float", "String", "Boolean", "True", "InputFile"]);

/** Structural checks on the final spec. Returns human-readable problems. */
export function validateSpec(spec: BotApiSpec): string[] {
  const problems: string[] = [];
  const known = new Set([...PRIMITIVES, ...spec.types.map((t) => t.name)]);

  const checkRef = (where: string, ref: TypeRef) => {
    for (const name of namesIn(ref)) {
      if (name === "__unresolved__")
        problems.push(`${where}: return type not found in description`);
      else if (!known.has(name)) problems.push(`${where}: unknown type "${name}"`);
    }
  };

  for (const method of spec.methods) {
    checkRef(`${method.name} returns`, method.returns);
    for (const param of method.params) checkRef(`${method.name}.${param.name}`, param.type);
  }

  for (const type of spec.types) {
    if (type.kind === "union") {
      for (const [i, variant] of type.variants.entries())
        checkRef(`${type.name} variant #${i}`, variant);
    } else {
      for (const field of type.fields) checkRef(`${type.name}.${field.name}`, field.type);
    }
  }

  const duplicates = (names: string[]) => names.filter((name, i) => names.indexOf(name) !== i);
  for (const name of duplicates(spec.types.map((t) => t.name)))
    problems.push(`duplicate type "${name}"`);
  for (const name of duplicates(spec.methods.map((m) => m.name)))
    problems.push(`duplicate method "${name}"`);

  return problems;
}
