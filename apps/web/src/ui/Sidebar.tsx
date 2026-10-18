/** The chat list, with a box to open a chat by `@username`. */
import type { tl } from "@simula/schema";
import { useState } from "react";

import type { Account } from "../api/control";
import { displayName } from "../state/peers";
import type { Snapshot } from "../state/store";
import { initials } from "./ChatView";

interface Props {
  snapshot: Snapshot;
  account: Account;
  selected?: string;
  onSelect: (key: string) => void;
  onFind: (username: string) => Promise<void>;
  onSwitchAccount: () => void;
  onToggleTheme: () => void;
  onToggleDirection: () => void;
}

export function Sidebar({
  snapshot,
  account,
  selected,
  onSelect,
  onFind,
  onSwitchAccount,
  onToggleTheme,
  onToggleDirection,
}: Props) {
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");

  return (
    <aside className="sidebar">
      <header className="sidebar-header">
        <button type="button" className="account" onClick={onSwitchAccount} title="Switch account">
          <span className="avatar small">{initials(snapshot.me)}</span>
          <span dir="auto">{account.first_name}</span>
        </button>
        <button
          type="button"
          className="icon-button"
          onClick={onToggleDirection}
          title="Left-to-right / right-to-left"
        >
          ⇄
        </button>
        <button type="button" className="icon-button" onClick={onToggleTheme} title="Light / dark">
          ☾
        </button>
      </header>
      <form
        className="find"
        onSubmit={(event) => {
          event.preventDefault();
          setError("");
          onFind(query)
            .then(() => setQuery(""))
            .catch((e: Error) =>
              setError(
                e.message === "USERNAME_NOT_OCCUPIED" ? "No one has that username" : e.message,
              ),
            );
        }}
      >
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Open @username"
        />
      </form>
      {error && <p className="find-error">{error}</p>}
      <nav className="dialogs">
        {snapshot.dialogs.map((dialog) => {
          const user =
            dialog.peer._ === "peerUser" ? snapshot.users.get(dialog.peer.user_id) : undefined;
          const top = snapshot.messages.get(dialog.key)?.get(dialog.top);
          return (
            <button
              key={dialog.key}
              type="button"
              className={dialog.key === selected ? "dialog selected" : "dialog"}
              onClick={() => onSelect(dialog.key)}
            >
              <span className="avatar">{initials(user)}</span>
              <span className="dialog-body">
                <span className="dialog-title">
                  <span dir="auto">{displayName(user)}</span>
                  {user?.bot && <span className="badge">bot</span>}
                  <time>{top ? day(top.date) : ""}</time>
                </span>
                <span className="dialog-preview">
                  <span dir="auto">{preview(top)}</span>
                  {dialog.unread > 0 && <span className="unread">{dialog.unread}</span>}
                </span>
              </span>
            </button>
          );
        })}
        {snapshot.dialogs.length === 0 && (
          <p className="empty">No chats yet. Open a bot by its @username.</p>
        )}
      </nav>
    </aside>
  );
}

function preview(message: tl.message | undefined): string {
  if (!message) return "";
  return `${message.out ? "You: " : ""}${message.message.replace(/\s+/g, " ")}`;
}

function day(date: number): string {
  const d = new Date(date * 1000);
  return d.toDateString() === new Date().toDateString()
    ? d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
    : d.toLocaleDateString();
}
