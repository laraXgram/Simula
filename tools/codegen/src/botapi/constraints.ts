import type { Constraints, TypeRef } from "./model";
import { namesIn } from "./type-expr";

/**
 * Pull machine-checkable limits out of a field's prose description.
 * Patterns were catalogued from the real docs; anything they miss is fixed
 * in `spec/botapi/overrides.json` rather than by widening a regex blindly.
 */
export function extractConstraints(type: TypeRef, markdown: string): Constraints | undefined {
  const text = plain(markdown);
  const names = namesIn(type);
  const isString = names.includes("String");
  const isNumber = names.includes("Integer") || names.includes("Float");
  const isArray = type.kind === "array";

  const c: Constraints = {};

  if (isString && !isArray) {
    const length = lengthOf(text);
    if (length) c.length = length;
    const constant = constOf(text);
    if (constant !== undefined) c.const = constant;
    else {
      const values = enumOf(text);
      if (values) c.enum = values;
    }
  }

  if (isNumber && !isArray) {
    const range = rangeOf(text);
    if (range) c.range = range;
  }

  if (isArray) {
    const items = itemsOf(text);
    if (items) c.items = items;
  }

  const fallback = defaultOf(text, type);
  if (fallback !== undefined) c.default = fallback;

  return Object.keys(c).length > 0 ? c : undefined;
}

/** Markdown → text that keeps *emphasis* and “quotes” but drops links and code ticks. */
function plain(markdown: string): string {
  return markdown
    .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/`/g, "")
    .replace(/\s+/g, " ");
}

function lengthOf(text: string): Constraints["length"] {
  const span =
    /(\d+)\s*[-–]\s*(\d+)\s+(characters|bytes)(\s+after\s+entit(?:y|ies)\s+parsing)?/i.exec(text);
  if (span) {
    return {
      min: Number(span[1]),
      max: Number(span[2]),
      unit: span[3]?.toLowerCase() === "bytes" ? "bytes" : "chars",
      ...(span[4] ? { after_entities: true } : {}),
    };
  }
  const upTo = /\bup to (\d+) (characters|bytes)/i.exec(text);
  if (upTo)
    return { max: Number(upTo[1]), unit: upTo[2]?.toLowerCase() === "bytes" ? "bytes" : "chars" };
  return undefined;
}

function rangeOf(text: string): Constraints["range"] {
  const between = /\bbetween (-?\d+)\s*(?:-|–|and)\s*(-?\d+)/i.exec(text);
  if (between) return { min: Number(between[1]), max: Number(between[2]) };
  const accepted = /(-?\d+)\s*[-–]\s*(-?\d+)\s+are accepted/i.exec(text);
  if (accepted) return { min: Number(accepted[1]), max: Number(accepted[2]) };
  // A bare span closing a clause: "…, measured in meters; 0-1500", "…delivery, 1-100. Defaults…"
  const bare = /[;,]\s*(-?\d+)\s*[-–]\s*(-?\d+)\s*(?=$|[.,;])/.exec(text);
  if (bare) return { min: Number(bare[1]), max: Number(bare[2]) };
  return undefined;
}

function itemsOf(text: string): Constraints["items"] {
  // The lookbehind keeps "ISO 3166-1 alpha-2" from reading as a count.
  const span = /(?<!ISO\s)\b(\d+)\s*[-–]\s*(\d+)\s+(?!characters|bytes)[a-z]/i.exec(text);
  if (span && Number(span[1]) <= Number(span[2]))
    return { min: Number(span[1]), max: Number(span[2]) };
  const upTo = /\b(?:up to|at most) (\d+) (?!characters|bytes|significant)[a-z]/i.exec(text);
  if (upTo) return { max: Number(upTo[1]) };
  return undefined;
}

/** "…, always “creator”" / "…, must be *article*" at the end of the description. */
function constOf(text: string): string | undefined {
  const match = /\b(?:always|must be)\s+(?:“([^”]+)”|\*([^*]+)\*)\s*\.?\s*$/.exec(text.trim());
  return match ? (match[1] ?? match[2]) : undefined;
}

function enumOf(text: string): string[] | undefined {
  // "Choose one, depending on …: *typing* for text messages, *upload_photo* for photos, …"
  if (/\bChoose one\b/.test(text)) {
    const values = [...text.matchAll(/\*([a-z_]+)\*\s+for\b/g)].map((m) => m[1] ?? "");
    if (values.length > 1) return unique(values);
  }
  // "can be either “private”, “group” … or “channel”", "must be one of “🎲”, …"
  const trigger = /\b(?:can be|one of|either)\b/i.exec(text);
  if (!trigger) return undefined;
  const clause = text.slice(trigger.index).split(/\.\s+(?=[A-Z])/)[0] ?? "";
  const values = [...clause.matchAll(/“([^”]+)”/g)].map((m) => m[1] ?? "");
  return values.length > 1 ? unique(values) : undefined;
}

function defaultOf(text: string, type: TypeRef): Constraints["default"] {
  const match = /\bDefaults? to (?:“([^”]*)”|\*?([^\s.,;*]+)\*?)/.exec(text);
  if (!match) return undefined;
  if (match[1] !== undefined) return match[1];
  const raw = match[2] ?? "";
  const names = namesIn(type);
  if ((names.includes("Integer") || names.includes("Float")) && /^-?\d+(\.\d+)?$/.test(raw))
    return Number(raw);
  if (
    names.includes("Boolean") &&
    (raw === "true" || raw === "false" || raw === "True" || raw === "False")
  ) {
    return raw.toLowerCase() === "true";
  }
  // "Defaults to BotCommandScopeDefault" on a BotCommandScope field: a variant name.
  const hasRef =
    type.kind === "ref" || (type.kind === "union" && type.of.some((t) => t.kind === "ref"));
  if (hasRef && /^[A-Z][a-z]+[A-Z][A-Za-z0-9]*$/.test(raw)) return raw;
  return undefined;
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}
