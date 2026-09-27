import type { Primitive, TypeRef } from "./model";

const PRIMITIVES = new Set<string>(["Integer", "Float", "String", "Boolean", "True", "InputFile"]);

/**
 * Parse a docs type expression into a TypeRef.
 *
 *   "Integer or String"                        → union
 *   "Array of Array of InlineKeyboardButton"   → array(array(ref))
 *   "Array of InputMediaAudio, InputMediaPhoto and InputMediaVideo" → array(union)
 *
 * "Array of" binds to everything after it, matching how the docs use it.
 */
export function parseTypeExpr(raw: string): TypeRef {
  const text = raw.replace(/\s+/g, " ").trim();
  if (!text) throw new Error("empty type expression");

  const array = /^Array of (.+)$/i.exec(text);
  if (array?.[1]) return { kind: "array", of: parseTypeExpr(array[1]) };

  const parts = text
    .split(/\s*,\s*|\s+or\s+|\s+and\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length > 1) return union(parts.map(parseTypeExpr));

  return atom(text);
}

export function atom(name: string): TypeRef {
  if (name === "Int") return { kind: "primitive", name: "Integer" };
  if (name === "Float number") return { kind: "primitive", name: "Float" };
  if (PRIMITIVES.has(name)) return { kind: "primitive", name: name as Primitive };
  if (!/^[A-Z][A-Za-z0-9]*$/.test(name)) throw new Error(`not a type name: "${name}"`);
  return { kind: "ref", name };
}

/** Build a union, flattening nested unions and dropping duplicates. */
export function union(members: TypeRef[]): TypeRef {
  const flat: TypeRef[] = [];
  for (const member of members) {
    for (const item of member.kind === "union" ? member.of : [member]) {
      if (!flat.some((existing) => formatTypeRef(existing) === formatTypeRef(item)))
        flat.push(item);
    }
  }
  return flat.length === 1 && flat[0] ? flat[0] : { kind: "union", of: flat };
}

/** Inverse of parseTypeExpr, in the docs' own notation. */
export function formatTypeRef(ref: TypeRef): string {
  switch (ref.kind) {
    case "primitive":
    case "ref":
      return ref.name;
    case "array":
      return `Array of ${formatTypeRef(ref.of)}`;
    case "union":
      return ref.of.map(formatTypeRef).join(" or ");
  }
}

/** Every named type (primitive or ref) mentioned anywhere inside a TypeRef. */
export function namesIn(ref: TypeRef): string[] {
  switch (ref.kind) {
    case "primitive":
    case "ref":
      return [ref.name];
    case "array":
      return namesIn(ref.of);
    case "union":
      return ref.of.flatMap(namesIn);
  }
}
