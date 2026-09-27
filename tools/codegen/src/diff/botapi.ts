import type { BotApiSpec, Field, Method, TypeDef } from "../botapi/model";
import { formatTypeRef } from "../botapi/type-expr";
import { diffNamed, isEmpty, renderSection } from "./named";

type FieldOwner = "param" | "field";

/** Markdown report of what changed between two Bot API specs. */
export function diffBotApi(base: BotApiSpec, head: BotApiSpec): string {
  const methods = diffNamed(base.methods, head.methods, compareMethods);
  const types = diffNamed(base.types, head.types, compareTypes);

  const title = `# Bot API ${base.release.version} → ${head.release.version}`;
  if (isEmpty(methods) && isEmpty(types)) return `${title}\n\nNo changes.\n`;

  const summary = `${methods.added.length} methods added, ${methods.removed.length} removed, ${methods.changed.length} changed; ${types.added.length} types added, ${types.removed.length} removed, ${types.changed.length} changed.`;
  return [
    title,
    summary,
    renderSection("Methods", methods, (m) => `→ ${formatTypeRef(m.returns)}`, String),
    renderSection("Types", types, describeType, String),
    base.release.version !== head.release.version ? changelog(head) : "",
  ]
    .filter(Boolean)
    .join("\n\n");
}

function compareMethods(before: Method, after: Method): string[] {
  const changes: string[] = [];
  if (formatTypeRef(before.returns) !== formatTypeRef(after.returns)) {
    changes.push(`returns ${formatTypeRef(before.returns)} → ${formatTypeRef(after.returns)}`);
  }
  if (before.multipart !== after.multipart) {
    changes.push(after.multipart ? "now accepts file uploads" : "no longer accepts file uploads");
  }
  changes.push(...compareFields(before.params, after.params, "param"));
  return changes;
}

function compareTypes(before: TypeDef, after: TypeDef): string[] {
  if (before.kind !== after.kind) return [`kind ${before.kind} → ${after.kind}`];
  if (before.kind === "union" && after.kind === "union") {
    const was = before.variants.map(formatTypeRef);
    const now = after.variants.map(formatTypeRef);
    return [
      ...now.filter((v) => !was.includes(v)).map((v) => `variant added: \`${v}\``),
      ...was.filter((v) => !now.includes(v)).map((v) => `variant removed: \`${v}\``),
    ];
  }
  if (before.kind === "object" && after.kind === "object") {
    return compareFields(before.fields, after.fields, "field");
  }
  return [];
}

function compareFields(before: Field[], after: Field[], owner: FieldOwner): string[] {
  const diff = diffNamed(before, after, (a, b) => {
    const changes: string[] = [];
    if (formatTypeRef(a.type) !== formatTypeRef(b.type)) {
      changes.push(`type ${formatTypeRef(a.type)} → ${formatTypeRef(b.type)}`);
    }
    if (a.required !== b.required) changes.push(b.required ? "now required" : "now optional");
    return changes;
  });
  return [
    ...diff.added.map(
      (f) =>
        `${owner} added: \`${f.name}\`: ${formatTypeRef(f.type)}${f.required ? " (required)" : ""}`,
    ),
    ...diff.removed.map((f) => `${owner} removed: \`${f.name}\``),
    ...diff.changed.map(({ name, changes }) => `${owner} \`${name}\`: ${changes.join(", ")}`),
  ];
}

/** Prose-only changes (behaviour, limits) are only visible in Telegram's own notes. */
function changelog(spec: BotApiSpec): string {
  return [`## Official changelog (${spec.release.date})`, "", ...spec.release.changes].join("\n");
}

function describeType(type: TypeDef): string {
  return type.kind === "union"
    ? `(union of ${type.variants.length})`
    : `(${type.fields.length} fields)`;
}
