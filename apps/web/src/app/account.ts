/**
 * Which account this tab acts as. Each tab has its own (in the URL as `?as=<id>`, and in
 * the tab's session storage), so several tabs can be different accounts in the same
 * chats at once; the last account picked is remembered for new tabs.
 */
import { useCallback, useState } from "react";

const TAB_KEY = "simula.tab-account";
const LAST_KEY = "simula.last-account";

function storage(kind: "session" | "local"): Storage | null {
  try {
    return kind === "session" ? sessionStorage : localStorage;
  } catch {
    return null;
  }
}

/** The account id this tab should open with, if any. */
function initial(): number | null {
  const params = new URLSearchParams(location.search);
  if (params.has("pick")) return null;
  const fromUrl = Number(params.get("as"));
  if (fromUrl > 0) return fromUrl;
  const fromTab = Number(storage("session")?.getItem(TAB_KEY));
  if (fromTab > 0) return fromTab;
  const last = Number(storage("local")?.getItem(LAST_KEY));
  return last > 0 ? last : null;
}

function writeUrl(id: number | null) {
  const url = new URL(location.href);
  url.searchParams.delete("pick");
  if (id) url.searchParams.set("as", String(id));
  else url.searchParams.delete("as");
  history.replaceState(history.state, "", url);
}

export function useTabAccount(): [number | null, (id: number | null) => void] {
  const [id, setId] = useState(initial);
  const set = useCallback((next: number | null) => {
    setId(next);
    writeUrl(next);
    if (next) {
      storage("session")?.setItem(TAB_KEY, String(next));
      storage("local")?.setItem(LAST_KEY, String(next));
    } else {
      storage("session")?.removeItem(TAB_KEY);
    }
  }, []);
  return [id, set];
}

/** Open the client as `accountId` in a new tab, optionally on a chat. */
export function openTab(accountId: number, chatKey?: string) {
  const url = new URL(location.origin + location.pathname);
  url.searchParams.set("as", String(accountId));
  if (chatKey) url.hash = `#/${chatKey}`;
  window.open(url, "_blank");
}

/** Open the account picker in a new tab. */
export function openPickerTab() {
  const url = new URL(location.origin + location.pathname);
  url.searchParams.set("pick", "1");
  window.open(url, "_blank");
}
