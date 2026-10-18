/**
 * Telegram deep links the client opens itself instead of a browser tab:
 * `t.me/<username>`, `telegram.me/<username>` and `tg://resolve?domain=<username>`,
 * with an optional `start` parameter for bots.
 */
export interface DeepLink {
  username: string;
  start?: string;
}

export function parseDeepLink(href: string): DeepLink | undefined {
  let url: URL;
  try {
    url = new URL(/^[a-z][a-z0-9+.-]*:/i.test(href) ? href : `https://${href}`);
  } catch {
    return undefined;
  }
  const start = url.searchParams.get("start") ?? undefined;
  if (url.protocol === "tg:") {
    const domain = url.searchParams.get("domain");
    return url.hostname === "resolve" && domain ? { username: domain, start } : undefined;
  }
  if (!["t.me", "telegram.me", "www.t.me"].includes(url.hostname.toLowerCase())) return undefined;
  const username = url.pathname.split("/")[1] ?? "";
  // Paths like t.me/+invite or t.me/joinchat/... are invite links (groups come with M4).
  return /^[A-Za-z]\w{3,31}$/.test(username) && username !== "joinchat"
    ? { username, start }
    : undefined;
}
