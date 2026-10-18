import { expect, test } from "bun:test";

import { parseFormatting, toFormatting } from "./markdown";

test("markers become entities with UTF-16 offsets", () => {
  const { text, entities } = parseFormatting("**bold** and __😀 it__ `x`");
  expect(text).toBe("bold and 😀 it x");
  expect(entities).toEqual([
    { _: "messageEntityBold", offset: 0, length: 4 },
    { _: "messageEntityItalic", offset: 9, length: 5 },
    { _: "messageEntityCode", offset: 15, length: 1 },
  ]);
});

test("links, pre blocks, escapes and lone markers", () => {
  expect(parseFormatting("[site](https://example.com)").entities[0]).toEqual({
    _: "messageEntityTextUrl",
    offset: 0,
    length: 4,
    url: "https://example.com",
  });
  const pre = parseFormatting("```rust\nfn main() {}```");
  expect(pre.text).toBe("fn main() {}");
  expect(pre.entities[0]).toMatchObject({ _: "messageEntityPre", language: "rust" });
  expect(parseFormatting("2 \\** 3").text).toBe("2 ** 3");
  expect(parseFormatting("a ** b")).toEqual({ text: "a ** b", entities: [] });
});

test("editing round-trips the formatting", () => {
  for (const input of [
    "**bold** and __it__ `x`",
    "[site](https://example.com) ~~old~~",
    "2 \\** 3",
  ]) {
    const { text, entities } = parseFormatting(input);
    expect(parseFormatting(toFormatting(text, entities))).toEqual({ text, entities });
  }
});
