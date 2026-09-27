const KEYWORDS = new Set([
  "as",
  "async",
  "await",
  "break",
  "const",
  "continue",
  "crate",
  "dyn",
  "else",
  "enum",
  "extern",
  "false",
  "fn",
  "for",
  "gen",
  "if",
  "impl",
  "in",
  "let",
  "loop",
  "match",
  "mod",
  "move",
  "mut",
  "pub",
  "ref",
  "return",
  "static",
  "struct",
  "super",
  "trait",
  "true",
  "type",
  "unsafe",
  "use",
  "where",
  "while",
  "abstract",
  "become",
  "box",
  "do",
  "final",
  "macro",
  "override",
  "priv",
  "try",
  "typeof",
  "unsized",
  "virtual",
  "yield",
]);

/** Keywords that can't be raw identifiers. */
const RESERVED = new Set(["self", "Self", "super", "crate"]);

/** `messageEntityBold` → `MessageEntityBold`, `p_q_inner_data_dc` → `PQInnerDataDc`. */
export function pascal(name: string): string {
  return name
    .split("_")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join("");
}

/** `MessageEntity` → `message_entity`, `ServerDHParams` → `server_dh_params`. */
export function snake(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/__+/g, "_")
    .toLowerCase();
}

/** A field or module name that is safe to use as a Rust identifier. */
export function ident(name: string): string {
  if (RESERVED.has(name)) return `${name}_`;
  return KEYWORDS.has(name) ? `r#${name}` : name;
}

/** Split `messages.sendMessage` into namespace (`messages`) and local name. */
export function splitName(name: string): { ns: string | undefined; local: string } {
  const dot = name.lastIndexOf(".");
  return dot < 0
    ? { ns: undefined, local: name }
    : { ns: name.slice(0, dot), local: name.slice(dot + 1) };
}
