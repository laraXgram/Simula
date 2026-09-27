import type { BotApiSpec, Constraints, Field, ObjectType, TypeDef } from "./model";
import { parseTypeExpr } from "./type-expr";

/**
 * Manual corrections, kept in `spec/botapi/overrides.json`. Every entry must
 * still match something in the scraped spec; stale entries are errors so the
 * file never silently rots when Telegram fixes its docs.
 */
export interface Overrides {
  /** Rules keyed by field name, applied to every method param and type field so named. */
  all_fields?: Record<string, FieldOverride>;
  methods?: Record<string, MethodOverride>;
  types?: Record<string, TypeOverride>;
}

interface MethodOverride {
  /** Type expression in docs notation, e.g. "Message or True". */
  returns?: string;
  params?: Record<string, FieldOverride>;
}

interface TypeOverride {
  fields?: Record<string, FieldOverride>;
  discriminator?: string | null;
  /** Name of the String field a bare string fills (see `ObjectType.string_shorthand`). */
  string_shorthand?: string;
}

interface FieldOverride {
  type?: string;
  required?: boolean;
  handler_validates?: boolean;
  /** Merged into scraped constraints; a `null` value removes that key. */
  constraints?: { [K in keyof Constraints]?: Constraints[K] | null };
}

export function applyOverrides(spec: BotApiSpec, overrides: Overrides): string[] {
  const errors: string[] = [];

  const everyField = [
    ...spec.methods.flatMap((m) => m.params),
    ...spec.types.flatMap((t) => (t.kind === "object" ? t.fields : [])),
  ];
  for (const [name, patch] of Object.entries(overrides.all_fields ?? {})) {
    if (name.startsWith("//")) continue;
    const targets = everyField.filter((f) => f.name === name);
    if (targets.length === 0) errors.push(`overrides: no field named "${name}" anywhere`);
    for (const target of targets) patchField(target, patch);
  }

  for (const [name, patch] of Object.entries(overrides.methods ?? {})) {
    if (name.startsWith("//")) continue;
    const method = spec.methods.find((m) => m.name === name);
    if (!method) {
      errors.push(`overrides: unknown method "${name}"`);
      continue;
    }
    if (patch.returns) method.returns = parseTypeExpr(patch.returns);
    for (const [field, fieldPatch] of Object.entries(patch.params ?? {})) {
      const target = method.params.find((p) => p.name === field);
      if (target) patchField(target, fieldPatch);
      else errors.push(`overrides: unknown param "${name}.${field}"`);
    }
  }

  for (const [name, patch] of Object.entries(overrides.types ?? {})) {
    if (name.startsWith("//")) continue;
    const type = spec.types.find((t) => t.name === name);
    if (!type) {
      errors.push(`overrides: unknown type "${name}"`);
      continue;
    }
    if (patch.discriminator !== undefined) patchDiscriminator(type, patch.discriminator, errors);
    if (patch.string_shorthand !== undefined) patchShorthand(type, patch.string_shorthand, errors);
    for (const [field, fieldPatch] of Object.entries(patch.fields ?? {})) {
      const target = type.kind === "object" ? type.fields.find((f) => f.name === field) : undefined;
      if (target) patchField(target, fieldPatch);
      else errors.push(`overrides: unknown field "${name}.${field}"`);
    }
  }

  return errors;
}

function patchField(field: Field, patch: FieldOverride) {
  if (patch.type) field.type = parseTypeExpr(patch.type);
  if (patch.required !== undefined) field.required = patch.required;
  if (patch.handler_validates === true) field.handler_validates = true;
  else if (patch.handler_validates === false) delete field.handler_validates;
  if (patch.constraints) {
    const merged: Record<string, unknown> = { ...field.constraints };
    for (const [key, value] of Object.entries(patch.constraints)) {
      if (value === null) delete merged[key];
      else merged[key] = value;
    }
    if (Object.keys(merged).length > 0) field.constraints = merged as Constraints;
    else delete field.constraints;
  }
}

function patchDiscriminator(type: TypeDef, value: string | null, errors: string[]) {
  if (type.kind !== "union") {
    errors.push(`overrides: "${type.name}" is not a union; cannot set a discriminator`);
    return;
  }
  if (value === null) delete type.discriminator;
  else type.discriminator = value;
}

function patchShorthand(type: TypeDef, field: string, errors: string[]) {
  const target = type.kind === "object" ? type.fields.find((f) => f.name === field) : undefined;
  const isString = target?.type.kind === "primitive" && target.type.name === "String";
  if (!target?.required || !isString) {
    errors.push(`overrides: "${type.name}.${field}" is not a required String field`);
    return;
  }
  if (!/String\*? can be used instead of this object/.test(type.description)) {
    errors.push(`overrides: "${type.name}" docs no longer allow a String instead of the object`);
    return;
  }
  (type as ObjectType).string_shorthand = field;
}
