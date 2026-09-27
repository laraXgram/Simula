import type { Node } from "node-html-parser";
import { decode, isElement, tag } from "./html";
import type { TypeRef } from "./model";
import { atom, union } from "./type-expr";

type Token = { kind: "text"; text: string } | { kind: "type"; ref: TypeRef };

const PRIMITIVE_WORD = /\b(String|Int|Integer|True|Boolean)\b/;
const RETURNS_SENTENCE = /\breturn(?:s|ed)?\b|\bon success\b/i;
const ARRAY_PREFIX = /\barray of\s*$/i;

/**
 * Find a method's return type in its description paragraphs, e.g.
 * "On success, if the message is not an inline message, the edited Message is
 * returned, otherwise True is returned." → Message or True.
 *
 * `anchors` maps docs anchors ("#message") to type names, so plural link texts
 * ("an array of Messages") still resolve. Returns undefined when nothing matches.
 */
export function extractReturnType(
  paragraphs: Node[],
  anchors: Map<string, string>,
): TypeRef | undefined {
  const tokens = paragraphs.flatMap((node) => [
    ...tokenize(node, anchors),
    { kind: "text", text: " " } as Token,
  ]);

  for (const sentence of splitSentences(tokens)) {
    const text = sentence.map((t) => (t.kind === "text" ? t.text : "X")).join("");
    if (!RETURNS_SENTENCE.test(text)) continue;

    const found: TypeRef[] = [];
    let before = "";
    for (const token of sentence) {
      if (token.kind === "text") {
        before += token.text;
        continue;
      }
      found.push(ARRAY_PREFIX.test(before) ? { kind: "array", of: token.ref } : token.ref);
      before = "";
    }
    if (found.length > 0) return union(found);
  }
  return undefined;
}

function tokenize(node: Node, anchors: Map<string, string>): Token[] {
  if (!isElement(node)) return splitPrimitiveWords(decode(node.rawText));

  if (tag(node) === "a") {
    const name = anchors.get(node.getAttribute("href") ?? "");
    if (name) return [{ kind: "type", ref: atom(name) }];
  }
  if (tag(node) === "em" && node.text.trim() === "True") {
    return [{ kind: "type", ref: atom("True") }];
  }
  return node.childNodes.flatMap((child) => tokenize(child, anchors));
}

function splitPrimitiveWords(text: string): Token[] {
  const tokens: Token[] = [];
  let rest = text;
  for (let match = PRIMITIVE_WORD.exec(rest); match; match = PRIMITIVE_WORD.exec(rest)) {
    tokens.push({ kind: "text", text: rest.slice(0, match.index) });
    tokens.push({ kind: "type", ref: atom(match[1] ?? "") });
    rest = rest.slice(match.index + match[0].length);
  }
  tokens.push({ kind: "text", text: rest });
  return tokens;
}

/** Split on ". " followed by an uppercase letter, keeping type tokens in place. */
function splitSentences(tokens: Token[]): Token[][] {
  const sentences: Token[][] = [[]];
  for (const token of tokens) {
    if (token.kind === "type") {
      sentences.at(-1)?.push(token);
      continue;
    }
    const pieces = token.text.split(/(?<=[.!?])\s+(?=[A-Z])/);
    pieces.forEach((piece, index) => {
      if (index > 0) sentences.push([]);
      sentences.at(-1)?.push({ kind: "text", text: piece });
    });
  }
  return sentences;
}
