/**
 * Formatting typed in the composer, the way Telegram Desktop accepts it: `**bold**`,
 * `__italic__`, `~~strike~~`, `||spoiler||`, `` `code` ``, ```` ```pre``` ```` (with an
 * optional language on the first line) and `[text](url)`. A backslash escapes a marker.
 * The result is the plain text and its entities, with UTF-16 offsets as TL wants (which
 * is how JavaScript strings index).
 */
import type { tl } from "@simula/schema";

type Simple =
  | "messageEntityBold"
  | "messageEntityItalic"
  | "messageEntityStrike"
  | "messageEntitySpoiler";

const PAIRS: readonly [string, Simple][] = [
  ["**", "messageEntityBold"],
  ["__", "messageEntityItalic"],
  ["~~", "messageEntityStrike"],
  ["||", "messageEntitySpoiler"],
];

export interface Formatted {
  text: string;
  entities: tl.MessageEntity[];
}

export function parseFormatting(input: string): Formatted {
  const entities: tl.MessageEntity[] = [];
  let text = "";
  const open = new Map<string, number>();
  let i = 0;
  while (i < input.length) {
    const rest = input.slice(i);
    if (rest[0] === "\\" && rest.length > 1 && /[*_~|`[\]()\\]/.test(rest[1])) {
      text += rest[1];
      i += 2;
      continue;
    }
    if (rest.startsWith("```")) {
      const end = input.indexOf("```", i + 3);
      if (end > i + 3) {
        let body = input.slice(i + 3, end);
        let language = "";
        const newline = body.indexOf("\n");
        if (newline > 0 && /^[\w+-]+$/.test(body.slice(0, newline))) {
          language = body.slice(0, newline);
          body = body.slice(newline + 1);
        } else if (newline === 0) {
          body = body.slice(1);
        }
        entities.push({
          _: "messageEntityPre",
          offset: text.length,
          length: body.length,
          language,
        });
        text += body;
        i = end + 3;
        continue;
      }
    }
    if (rest[0] === "`") {
      const end = input.indexOf("`", i + 1);
      if (end > i + 1) {
        const body = input.slice(i + 1, end);
        entities.push({ _: "messageEntityCode", offset: text.length, length: body.length });
        text += body;
        i = end + 1;
        continue;
      }
    }
    const link = /^\[([^\]]+)\]\(([^)\s]+)\)/.exec(rest);
    if (link) {
      entities.push({
        _: "messageEntityTextUrl",
        offset: text.length,
        length: link[1].length,
        url: link[2],
      });
      text += link[1];
      i += link[0].length;
      continue;
    }
    const pair = PAIRS.find(([marker]) => rest.startsWith(marker));
    // A marker opens only if a matching one follows; otherwise it is literal text.
    const opens = pair && !open.has(pair[0]) && input.indexOf(pair[0], i + pair[0].length) > 0;
    if (pair && (opens || open.has(pair[0]))) {
      const [marker, kind] = pair;
      const start = open.get(marker);
      if (start === undefined) {
        open.set(marker, text.length);
      } else {
        open.delete(marker);
        if (text.length > start)
          entities.push({ _: kind, offset: start, length: text.length - start });
      }
      i += marker.length;
      continue;
    }
    text += rest[0];
    i += 1;
  }
  return { text, entities: entities.sort((a, b) => a.offset - b.offset) };
}

const MARKERS: Partial<Record<tl.MessageEntity["_"], string>> = {
  messageEntityBold: "**",
  messageEntityItalic: "__",
  messageEntityStrike: "~~",
  messageEntitySpoiler: "||",
  messageEntityCode: "`",
};

/** The composer form of a message, for editing it: the reverse of [`parseFormatting`]. */
export function toFormatting(text: string, entities: readonly tl.MessageEntity[] = []): string {
  // [position, insertion, closing?]; closings go before openings at the same position.
  const inserts: [number, string, boolean][] = [];
  for (const entity of entities) {
    const end = entity.offset + entity.length;
    const marker = MARKERS[entity._];
    if (marker) {
      inserts.push([entity.offset, marker, false], [end, marker, true]);
    } else if (entity._ === "messageEntityPre") {
      inserts.push(
        [entity.offset, `\`\`\`${entity.language ? `${entity.language}\n` : ""}`, false],
        [end, "```", true],
      );
    } else if (entity._ === "messageEntityTextUrl") {
      inserts.push([entity.offset, "[", false], [end, `](${entity.url})`, true]);
    }
  }
  const escaped = (from: number, to: number) =>
    text.slice(from, to).replace(/([*_~|`[\]\\])/g, "\\$1");
  inserts.sort((a, b) => a[0] - b[0] || Number(b[2]) - Number(a[2]));
  let out = "";
  let at = 0;
  for (const [position, insertion] of inserts) {
    out += escaped(at, position) + insertion;
    at = position;
  }
  return out + escaped(at, text.length);
}
