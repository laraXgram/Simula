import { formatParam, formatTlType } from "../tl/format";
import type { TlCombinator, TlSchema } from "../tl/model";
import { diffNamed, isEmpty, renderSection } from "./named";

/** Markdown report of what changed between two TL schemas. */
export function diffTl(base: TlSchema, head: TlSchema): string {
  const constructors = diffNamed(base.constructors, head.constructors, compareCombinators);
  const functions = diffNamed(base.functions, head.functions, compareCombinators);

  const layers =
    base.layer || head.layer ? ` (layer ${base.layer ?? "?"} → ${head.layer ?? "?"})` : "";
  const title = `# ${head.file}${layers}`;
  if (isEmpty(constructors) && isEmpty(functions)) return `${title}\n\nNo changes.\n`;

  const describe = (c: TlCombinator) => `#${c.id} = ${formatTlType(c.result)}`;
  return [
    title,
    renderSection("Constructors", constructors, describe, String),
    renderSection("Functions", functions, describe, String),
  ]
    .filter(Boolean)
    .join("\n\n");
}

function compareCombinators(before: TlCombinator, after: TlCombinator): string[] {
  const changes: string[] = [];
  if (before.id !== after.id) changes.push(`id ${before.id} → ${after.id}`);
  if (formatTlType(before.result) !== formatTlType(after.result)) {
    changes.push(`result ${formatTlType(before.result)} → ${formatTlType(after.result)}`);
  }
  const params = diffNamed(before.params, after.params, (a, b) =>
    formatParam(a) === formatParam(b) ? [] : [`${formatParam(a)} → ${formatParam(b)}`],
  );
  changes.push(
    ...params.added.map((p) => `param added: \`${formatParam(p)}\``),
    ...params.removed.map((p) => `param removed: \`${p.name}\``),
    ...params.changed.flatMap(({ changes }) => changes.map((c) => `param \`${c}\``)),
  );
  return changes;
}
