/** The chat list, with a box to open a chat by `@username` and a form for new groups. */
import { useState } from "react";

import type { Account } from "../api/control";
import { displayName, senderId } from "../state/peers";
import type { AnyMessage, Snapshot } from "../state/store";
import { initials, initialsOf } from "./ChatView";
import { describeService } from "./ServiceMessage";

interface Props {
  snapshot: Snapshot;
  account: Account;
  selected?: string;
  onSelect: (key: string) => void;
  onFind: (username: string) => Promise<void>;
  onCreateGroup: (title: string, usernames: string[]) => Promise<void>;
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
  onCreateGroup,
  onSwitchAccount,
  onToggleTheme,
  onToggleDirection,
}: Props) {
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");
  const [creating, setCreating] = useState(false);
  const [groupTitle, setGroupTitle] = useState("");
  const [groupMembers, setGroupMembers] = useState("");

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
        <button
          type="button"
          className="icon-button"
          onClick={() => setCreating(!creating)}
          title="New group"
        >
          ✚
        </button>
      </header>
      {creating && (
        <form
          className="new-group"
          onSubmit={(event) => {
            event.preventDefault();
            setError("");
            const usernames = groupMembers
              .split(/[\s,]+/)
              .map((name) => name.trim())
              .filter(Boolean);
            onCreateGroup(groupTitle.trim(), usernames)
              .then(() => {
                setCreating(false);
                setGroupTitle("");
                setGroupMembers("");
              })
              .catch((e: Error) => setError(e.message));
          }}
        >
          <input
            value={groupTitle}
            onChange={(event) => setGroupTitle(event.target.value)}
            placeholder="Group name"
            required
          />
          <input
            value={groupMembers}
            onChange={(event) => setGroupMembers(event.target.value)}
            placeholder="Members: @alice, @demo_bot"
          />
          <button type="submit">Create group</button>
        </form>
      )}
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
          const group =
            dialog.peer._ === "peerChat" ? snapshot.chats.get(dialog.peer.chat_id) : undefined;
          const title = group ? group.title : displayName(user);
          const top = snapshot.messages.get(dialog.key)?.get(dialog.top);
          return (
            <button
              key={dialog.key}
              type="button"
              className={dialog.key === selected ? "dialog selected" : "dialog"}
              onClick={() => onSelect(dialog.key)}
            >
              <span className={group ? "avatar group" : "avatar"}>
                {group ? initialsOf(title) : initials(user)}
              </span>
              <span className="dialog-body">
                <span className="dialog-title">
                  <span dir="auto">{title}</span>
                  {user?.bot && <span className="badge">bot</span>}
                  <time>{top ? day(top.date) : ""}</time>
                </span>
                <span className="dialog-preview">
                  <span dir="auto">{preview(top, snapshot, group !== undefined)}</span>
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

function preview(message: AnyMessage | undefined, snapshot: Snapshot, inGroup: boolean): string {
  if (!message) return "";
  if (message._ === "messageService")
    return describeService(message, snapshot.users, snapshot.me?.id);
  const sender = senderId(message);
  const who = message.out
    ? "You: "
    : inGroup && sender
      ? `${displayName(snapshot.users.get(sender)).split(" ")[0]}: `
      : "";
  return `${who}${message.message.replace(/\s+/g, " ")}`;
}

function day(date: number): string {
  const d = new Date(date * 1000);
  return d.toDateString() === new Date().toDateString()
    ? d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
    : d.toLocaleDateString();
}
