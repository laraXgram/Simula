import { expect, test } from "bun:test";

import { parseDeepLink } from "./links";

test("deep links to users and bots", () => {
  expect(parseDeepLink("https://t.me/demo_bot?start=ref_1")).toEqual({
    username: "demo_bot",
    start: "ref_1",
  });
  expect(parseDeepLink("t.me/alice")).toEqual({ username: "alice", start: undefined });
  expect(parseDeepLink("tg://resolve?domain=demo_bot&start=x")).toEqual({
    username: "demo_bot",
    start: "x",
  });
  expect(parseDeepLink("https://example.com/demo_bot")).toBeUndefined();
  expect(parseDeepLink("https://t.me/joinchat/abc")).toBeUndefined();
});
