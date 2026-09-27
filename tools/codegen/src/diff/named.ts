/** Result of comparing two lists of named items. */
export interface NamedDiff<T, C> {
  added: T[];
  removed: T[];
  changed: { name: string; changes: C[] }[];
}

/**
 * Compare two lists by name. `compare` returns the list of changes for items present
 * on both sides (empty when equal). Order follows `head`, then removed items.
 */
export function diffNamed<T extends { name: string }, C>(
  base: T[],
  head: T[],
  compare: (before: T, after: T) => C[],
): NamedDiff<T, C> {
  const before = new Map(base.map((item) => [item.name, item]));
  const after = new Map(head.map((item) => [item.name, item]));
  const diff: NamedDiff<T, C> = { added: [], removed: [], changed: [] };

  for (const item of head) {
    const old = before.get(item.name);
    if (!old) {
      diff.added.push(item);
      continue;
    }
    const changes = compare(old, item);
    if (changes.length > 0) diff.changed.push({ name: item.name, changes });
  }
  for (const item of base) if (!after.has(item.name)) diff.removed.push(item);
  return diff;
}

export function isEmpty(diff: NamedDiff<unknown, unknown>): boolean {
  return diff.added.length === 0 && diff.removed.length === 0 && diff.changed.length === 0;
}

/** Render one NamedDiff as a Markdown section. */
export function renderSection<T extends { name: string }, C>(
  title: string,
  diff: NamedDiff<T, C>,
  describe: (item: T) => string,
  describeChange: (change: C) => string,
): string {
  if (isEmpty(diff)) return "";
  const lines = [`## ${title}`, ""];
  for (const item of diff.added)
    lines.push(`- **added** \`${item.name}\` ${describe(item)}`.trimEnd());
  for (const item of diff.removed) lines.push(`- **removed** \`${item.name}\``);
  for (const { name, changes } of diff.changed) {
    lines.push(`- **changed** \`${name}\``);
    for (const change of changes) lines.push(`  - ${describeChange(change)}`);
  }
  return lines.join("\n");
}
