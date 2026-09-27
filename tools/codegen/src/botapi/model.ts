/**
 * Shape of `spec/botapi/api.json` — the parsed, normalized Bot API specification.
 * Everything downstream (Rust/TS emitters, validators, playground forms, coverage)
 * reads this model, never the HTML.
 */

export type Primitive = "Integer" | "Float" | "String" | "Boolean" | "True" | "InputFile";

export type TypeRef =
  | { kind: "primitive"; name: Primitive }
  | { kind: "ref"; name: string }
  | { kind: "array"; of: TypeRef }
  | { kind: "union"; of: TypeRef[] };

export interface Constraints {
  /** String length limits. `after_entities` = measured after parse_mode is applied. */
  length?: { min?: number; max?: number; unit: "chars" | "bytes"; after_entities?: boolean };
  /** Numeric value limits. */
  range?: { min?: number; max?: number };
  /** Array size limits. */
  items?: { min?: number; max?: number };
  /** Allowed string values. */
  enum?: string[];
  /** Fixed value — the discriminator of a union variant ("always “creator”", "Always 0"). */
  const?: string | number;
  /** Documented default ("Defaults to 100"). Type names are kept as strings. */
  default?: string | number | boolean;
}

export interface Field {
  name: string;
  type: TypeRef;
  required: boolean;
  /** Markdown rendering of the docs description. */
  description: string;
  constraints?: Constraints;
}

export interface Method {
  name: string;
  /** Docs section title, e.g. "Available methods", "Stickers", "Inline mode". */
  section: string;
  description: string;
  params: Field[];
  returns: TypeRef;
  /** True when any parameter can carry an InputFile, directly or nested. */
  multipart: boolean;
}

export interface ObjectType {
  kind: "object";
  name: string;
  section: string;
  description: string;
  fields: Field[];
}

export interface UnionType {
  kind: "union";
  name: string;
  section: string;
  description: string;
  /** Usually object type refs; a few unions also admit primitives or arrays (RichText). */
  variants: TypeRef[];
  /** Field whose `const` value tells the variants apart, when there is one. */
  discriminator?: string;
}

export type TypeDef = ObjectType | UnionType;

export interface Release {
  version: string;
  date: string;
  /** Markdown bullet items of the release's changelog. */
  changes: string[];
}

export interface BotApiSpec {
  source: string;
  release: Release;
  types: TypeDef[];
  methods: Method[];
}
