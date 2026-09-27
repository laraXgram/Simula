import { type HTMLElement, type Node, NodeType } from "node-html-parser";

const DOCS_URL = "https://core.telegram.org";

export function isElement(node: Node): node is HTMLElement {
  return node.nodeType === NodeType.ELEMENT_NODE;
}

export function tag(node: Node): string {
  return isElement(node) ? node.rawTagName.toLowerCase() : "";
}

/** Render a docs fragment as compact Markdown (links, emphasis, code, lists). */
export function toMarkdown(node: Node): string {
  return (
    render(node)
      .replace(/[ \t]+\n/g, "\n")
      // `<br>- item` pseudo-lists: a plain line right after an item would otherwise
      // continue that item in Markdown.
      .replace(/^(- .*)\n(?!- |\n)/gm, "$1\n\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim()
  );
}

function render(node: Node): string {
  if (!isElement(node)) return decode(node.rawText).replace(/\s+/g, " ");

  const inner = () => node.childNodes.map(render).join("");
  switch (tag(node)) {
    case "a": {
      const href = node.getAttribute("href") ?? "";
      const text = inner();
      if (!href || href === text) return text;
      return `[${text}](${href.startsWith("/") ? DOCS_URL + href : href})`;
    }
    case "em":
    case "i":
      return node.classList.contains("anchor-icon") ? "" : `*${inner()}*`;
    case "strong":
    case "b":
      return `**${inner()}**`;
    case "code":
      return `\`${inner()}\``;
    case "br":
      return "\n";
    case "img":
      return node.getAttribute("alt") ?? "";
    case "li":
      return `- ${inner().trim()}\n`;
    case "ul":
    case "ol":
      return `\n${inner()}\n`;
    case "p":
    case "blockquote":
      return `${inner()}\n\n`;
    default:
      return inner();
  }
}

const ENTITIES: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
  nbsp: " ",
  laquo: "«",
  raquo: "»",
  mdash: "—",
  ndash: "–",
  hellip: "…",
};

export function decode(text: string): string {
  return text.replace(/&(#x[0-9a-f]+|#\d+|\w+);/gi, (match, code: string) => {
    if (code[0] === "#") {
      const cp =
        code[1] === "x" || code[1] === "X"
          ? Number.parseInt(code.slice(2), 16)
          : Number(code.slice(1));
      return String.fromCodePoint(cp);
    }
    return ENTITIES[code.toLowerCase()] ?? match;
  });
}
