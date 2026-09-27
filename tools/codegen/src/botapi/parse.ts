import { type HTMLElement, parse } from "node-html-parser";
import { extractConstraints } from "./constraints";
import { isElement, tag, toMarkdown } from "./html";
import type {
  BotApiSpec,
  Field,
  Method,
  ObjectType,
  Release,
  TypeDef,
  TypeRef,
  UnionType,
} from "./model";
import { extractReturnType } from "./returns";
import { atom, parseTypeExpr } from "./type-expr";

export const BOT_API_URL = "https://core.telegram.org/bots/api";

/** One h4 entry of the docs: a method or a type, with the elements that follow it. */
interface Entry {
  name: string;
  section: string;
  nodes: HTMLElement[];
}

export class SpecParseError extends Error {}

export function parseBotApi(html: string): BotApiSpec {
  const content = parse(html).querySelector("#dev_page_content");
  if (!content) throw new SpecParseError("missing #dev_page_content — did the docs layout change?");

  const elements = content.childNodes.filter(isElement);
  const release = parseRelease(elements);
  const entries = collectEntries(elements);

  const typeEntries = entries.filter((e) => /^[A-Z]/.test(e.name) && e.name !== "InputFile");
  const methodEntries = entries.filter((e) => /^[a-z]/.test(e.name));

  const anchors = new Map(typeEntries.map((e) => [`#${e.name.toLowerCase()}`, e.name]));
  anchors.set("#inputfile", "InputFile");

  const types = typeEntries.map((entry) => parseType(entry, anchors));
  const methods = methodEntries.map((entry) => parseMethod(entry, anchors));

  assignDiscriminators(types);
  markMultipart(methods, types);

  return { source: BOT_API_URL, release, types, methods };
}

function collectEntries(elements: HTMLElement[]): Entry[] {
  const entries: Entry[] = [];
  let section = "";
  let current: Entry | undefined;

  for (const el of elements) {
    if (tag(el) === "h3") {
      section = el.text.trim();
      current = undefined;
    } else if (tag(el) === "h4") {
      const name = el.text.trim();
      current =
        section !== "Recent changes" && /^[A-Za-z][A-Za-z0-9]*$/.test(name)
          ? { name, section, nodes: [] }
          : undefined;
      if (current) entries.push(current);
    } else {
      current?.nodes.push(el);
    }
  }
  return entries;
}

function parseRelease(elements: HTMLElement[]): Release {
  const start = elements.findIndex((el) => tag(el) === "h3" && el.text.trim() === "Recent changes");
  const heading = elements.slice(start + 1).find((el) => tag(el) === "h4");
  if (start < 0 || !heading)
    throw new SpecParseError("could not find the latest release in Recent changes");

  const changes: string[] = [];
  let version = "";
  for (const el of elements.slice(elements.indexOf(heading) + 1)) {
    if (tag(el) === "h4" || tag(el) === "h3") break;
    const text = el.text.trim();
    const versionMatch = /^Bot API (\d+\.\d+)$/.exec(text);
    if (tag(el) === "p" && versionMatch?.[1]) version = versionMatch[1];
    else if (tag(el) === "ul")
      changes.push(...el.querySelectorAll("li").map((li) => toMarkdown(li)));
    else changes.push(toMarkdown(el));
  }
  if (!version) throw new SpecParseError("latest release has no 'Bot API X.Y' line");
  return { version, date: heading.text.trim(), changes };
}

function parseMethod(entry: Entry, anchors: Map<string, string>): Method {
  const { table, before, after } = splitAtTable(entry.nodes);
  const paragraphs = before.filter((el) => tag(el) === "p");
  const returns = extractReturnType(paragraphs, anchors);

  return {
    name: entry.name,
    section: entry.section,
    description: describe([...before, ...after]),
    params: table ? parseRows(entry.name, table, "method") : [],
    // Unresolved return types are filled by overrides; the validator reports leftovers.
    returns: returns ?? { kind: "ref", name: "__unresolved__" },
    multipart: false,
  };
}

function parseType(entry: Entry, anchors: Map<string, string>): TypeDef {
  const { table, before, after } = splitAtTable(entry.nodes);
  if (table) {
    const object: ObjectType = {
      kind: "object",
      name: entry.name,
      section: entry.section,
      description: describe([...before, ...after]),
      fields: parseRows(entry.name, table, "type"),
    };
    return object;
  }

  const list = before.find((el) => tag(el) === "ul" && isTypeList(el, anchors));
  if (list) {
    const listed = list
      .querySelectorAll("a")
      .map((a) => atom(anchors.get(a.getAttribute("href") ?? "") ?? a.text.trim()));
    const lead = before[before.indexOf(list) - 1];
    const union: UnionType = {
      kind: "union",
      name: entry.name,
      section: entry.section,
      description: describe(before.filter((el) => el !== list)),
      variants: [...(lead ? extraVariants(lead, anchors) : []), ...listed],
    };
    return union;
  }

  // "Currently holds no information" types, e.g. ForumTopicClosed, CallbackGame.
  return {
    kind: "object",
    name: entry.name,
    section: entry.section,
    description: describe(before),
    fields: [],
  };
}

function isTypeList(ul: HTMLElement, anchors: Map<string, string>): boolean {
  const items = ul.querySelectorAll("li");
  return (
    items.length > 0 &&
    items.every((li) => {
      const links = li.querySelectorAll("a");
      return (
        links.length === 1 &&
        anchors.has(links[0]?.getAttribute("href") ?? "") &&
        li.text.trim() === links[0]?.text.trim()
      );
    })
  );
}

/**
 * Non-object members named in the sentence that introduces a union's list:
 * "it can be either a String for plain text, an Array of RichText, or any of the following types:"
 */
function extraVariants(lead: HTMLElement, anchors: Map<string, string>): TypeRef[] {
  const text = lead.text.replace(/\s+/g, " ");
  const cut = text.search(/\bany of the following\b/i);
  if (cut < 0) return [];
  const intro = text.slice(0, cut);
  const found: TypeRef[] = [];
  for (const match of intro.matchAll(/\ba (String|Integer|Float|Boolean)\b/g))
    found.push(atom(match[1] ?? ""));
  for (const match of intro.matchAll(/\ban Array of (\w+)/g)) {
    const name = match[1] ?? "";
    if ([...anchors.values()].includes(name)) found.push({ kind: "array", of: atom(name) });
  }
  return found;
}

function splitAtTable(nodes: HTMLElement[]) {
  const index = nodes.findIndex((el) => tag(el) === "table");
  if (index < 0) return { table: undefined, before: nodes, after: [] as HTMLElement[] };
  return { table: nodes[index], before: nodes.slice(0, index), after: nodes.slice(index + 1) };
}

function describe(nodes: HTMLElement[]): string {
  return nodes
    .filter((el) => ["p", "blockquote", "ul", "ol"].includes(tag(el)))
    .map((el) => toMarkdown(el))
    .join("\n\n");
}

function parseRows(owner: string, table: HTMLElement, kind: "method" | "type"): Field[] {
  const header = table.querySelectorAll("th").map((th) => th.text.trim());
  const expected =
    kind === "method"
      ? ["Parameter", "Type", "Required", "Description"]
      : ["Field", "Type", "Description"];
  if (header.join("|") !== expected.join("|")) {
    throw new SpecParseError(`${owner}: unexpected table header [${header.join(", ")}]`);
  }

  return table.querySelectorAll("tbody tr").map((row) => {
    const cells = row.querySelectorAll("td");
    const name = cells[0]?.text.trim() ?? "";
    const descriptionCell = cells[kind === "method" ? 3 : 2];
    if (!descriptionCell) throw new SpecParseError(`${owner}.${name}: row has too few cells`);
    const type = parseCellType(owner, name, cells[1]?.text ?? "");
    let description = toMarkdown(descriptionCell);
    let required: boolean;

    if (kind === "method") {
      required = cells[2]?.text.trim() === "Yes";
    } else {
      required = !description.startsWith("*Optional*");
      description = description.replace(/^\*Optional\*\.\s*/, "");
    }

    const field: Field = { name, type, required, description };
    const constraints = extractConstraints(type, description);
    if (constraints) field.constraints = constraints;
    return field;
  });
}

function parseCellType(owner: string, field: string, raw: string): TypeRef {
  try {
    return parseTypeExpr(raw);
  } catch (error) {
    throw new SpecParseError(`${owner}.${field}: ${(error as Error).message}`);
  }
}

/** A union is discriminated when every variant pins the same field to a constant. */
function assignDiscriminators(types: TypeDef[]) {
  const byName = new Map(types.map((t) => [t.name, t]));
  for (const type of types) {
    if (type.kind !== "union") continue;
    const variants = type.variants.map((ref) =>
      ref.kind === "ref" ? byName.get(ref.name) : undefined,
    );
    if (variants.some((v) => v?.kind !== "object")) continue;
    const objects = variants as ObjectType[];
    const candidates =
      objects[0]?.fields.filter((f) => f.constraints?.const !== undefined).map((f) => f.name) ?? [];
    const field = candidates.find((name) =>
      objects.every((v) =>
        v.fields.some((f) => f.name === name && f.constraints?.const !== undefined),
      ),
    );
    if (field) type.discriminator = field;
  }
}

/**
 * A method needs multipart support when a parameter can reach an InputFile, or a
 * field documented as accepting "attach://<name>" uploads (InputMedia*.media).
 */
function markMultipart(methods: Method[], types: TypeDef[]) {
  const byName = new Map(types.map((t) => [t.name, t]));
  const memo = new Map<string, boolean>();

  const reaches = (ref: TypeRef): boolean => {
    switch (ref.kind) {
      case "primitive":
        return ref.name === "InputFile";
      case "array":
        return reaches(ref.of);
      case "union":
        return ref.of.some(reaches);
      case "ref":
        return typeUploads(ref.name);
    }
  };
  const typeUploads = (name: string): boolean => {
    const cached = memo.get(name);
    if (cached !== undefined) return cached;
    memo.set(name, false); // cycle guard
    const type = byName.get(name);
    const result =
      type?.kind === "union"
        ? type.variants.some(reaches)
        : (type?.fields.some((f) => reaches(f.type) || f.description.includes("attach://")) ??
          false);
    memo.set(name, result);
    return result;
  };

  for (const method of methods) method.multipart = method.params.some((p) => reaches(p.type));
}
