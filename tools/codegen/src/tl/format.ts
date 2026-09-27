import type { TlCombinator, TlParam, TlType } from "./model";

/** TL notation for a type, e.g. `Vector<MessageEntity>`, `!X`, `#`. */
export function formatTlType(type: TlType): string {
  switch (type.kind) {
    case "named":
      return type.name;
    case "vector":
      return `${type.bare ? "vector" : "Vector"}<${formatTlType(type.of)}>`;
    case "generic":
      return `${type.query ? "!" : ""}${type.name}`;
    case "flags":
      return "#";
  }
}

/** `name:flags.3?Type` */
export function formatParam(param: TlParam): string {
  const flag = param.flag ? `${param.flag.field}.${param.flag.bit}?` : "";
  return `${param.name}:${flag}${formatTlType(param.type)}`;
}

/** The full declaration, e.g. `messageEntityBold#bd610bc9 offset:int length:int = MessageEntity`. */
export function formatCombinator(c: TlCombinator): string {
  const generics = (c.generics ?? []).map((g) => `{${g}:Type}`);
  return [
    `${c.name}#${c.id}`,
    ...generics,
    ...c.params.map(formatParam),
    "=",
    formatTlType(c.result),
  ].join(" ");
}
