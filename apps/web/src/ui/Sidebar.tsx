/**
 * The chat list, with a box to open a chat by `@username` (or join by invite link), the
 * main menu (profile, accounts, appearance) and a form for new groups and channels.
 */
import { useState } from "react";

import type { Account } from "../api/control";
import { openPickerTab } from "../app/account";
import type { GroupKind } from "../state/client";
import { displayName, senderId } from "../state/peers";
import type { AnyMessage, Snapshot } from "../state/store";
import { Avatar } from "./kit/Avatar";
import { Field, Modal } from "./kit/Dialogs";
import { Icon } from "./kit/Icon";
import { Menu } from "./kit/Menu";
import { describeService } from "./ServiceMessage";

interface Props {
  snapshot: Snapshot;
  account: Account;
  selected?: string;
  onSelect: (key: string) => void;
  onFind: (username: string) => Promise<void>;
  onCreateGroup: (kind: GroupKind, title: string, usernames: string[]) => Promise<void>;
  onEditProfile: () => void;
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
  onEditProfile,
  onSwitchAccount,
  onToggleTheme,
  onToggleDirection,
}: Props) {
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");
  const [creating, setCreating] = useState<GroupKind | null>(null);
  return (
    <aside className="sidebar">
      <header className="sidebar-header">
        <Menu
          label="Menu"
          align="start"
          trigger={<Icon name="menu" />}
          items={[
            { icon: "person", label: "Edit profile", onSelect: onEditProfile },
            {
              icon: "swap",
              label: `Switch account (${displayName(snapshot.me) || account.first_name})`,
              onSelect: onSwitchAccount,
            },
            {
              icon: "openNew",
              label: "Open another account in a new tab",
              onSelect: openPickerTab,
            },
            { icon: "moon", label: "Light / dark theme", onSelect: onToggleTheme },
            {
              icon: "direction",
              label: "Left-to-right / right-to-left",
              onSelect: onToggleDirection,
            },
          ]}
        />
        <form
          className="search grow"
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
          <Icon name="search" size={20} />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search @username or invite link"
            aria-label="Search"
          />
        </form>
      </header>
      {error && <p className="find-error">{error}</p>}
      <nav className="dialogs">
        {snapshot.dialogs
          .filter((dialog) => !upgraded(snapshot, dialog.key))
          .map((dialog) => {
            const user =
              dialog.peer._ === "peerUser" ? snapshot.users.get(dialog.peer.user_id) : undefined;
            const group = dialog.peer._ === "peerUser" ? undefined : snapshot.chats.get(dialog.key);
            const broadcast =
              (group?._ === "channel" || group?._ === "channelForbidden") &&
              group.broadcast === true;
            const title = group ? group.title : displayName(user);
            const top = snapshot.messages.get(dialog.key)?.get(dialog.top);
            const id = group?.id ?? user?.id ?? dialog.key;
            return (
              <button
                key={dialog.key}
                type="button"
                className={dialog.key === selected ? "dialog selected" : "dialog"}
                onClick={() => onSelect(dialog.key)}
              >
                <Avatar id={id} name={title} size={52} />
                <span className="dialog-body">
                  <span className="dialog-title">
                    {group && <Icon name={broadcast ? "chat" : "group"} size={16} />}
                    <span dir="auto">{title}</span>
                    {user?.bot && <span className="badge">bot</span>}
                    <time>{top ? day(top.date) : ""}</time>
                  </span>
                  <span className="dialog-preview">
                    <span dir="auto">
                      {preview(top, snapshot, group !== undefined && !broadcast)}
                    </span>
                    {dialog.unread > 0 && <span className="unread">{dialog.unread}</span>}
                  </span>
                </span>
              </button>
            );
          })}
        {snapshot.dialogs.length === 0 && (
          <p className="empty">No chats yet. Search a bot by its @username.</p>
        )}
      </nav>
      <div className="fab-root">
        <Menu
          label="New chat"
          align="end"
          trigger={<Icon name="edit" />}
          items={[
            { icon: "group", label: "New Group", onSelect: () => setCreating("group") },
            { icon: "chat", label: "New Channel", onSelect: () => setCreating("channel") },
          ]}
        />
      </div>
      {creating && (
        <NewChat kind={creating} onClose={() => setCreating(null)} onCreate={onCreateGroup} />
      )}
    </aside>
  );
}

const KIND_NAMES: Record<GroupKind, string> = {
  group: "Group",
  supergroup: "Supergroup",
  forum: "Forum",
  channel: "Channel",
};

/** Name a new group or channel and pick who joins (by @username). */
function NewChat({
  kind,
  onClose,
  onCreate,
}: {
  kind: GroupKind;
  onClose: () => void;
  onCreate: (kind: GroupKind, title: string, usernames: string[]) => Promise<void>;
}) {
  const [title, setTitle] = useState("");
  const [members, setMembers] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const name = KIND_NAMES[kind];
  return (
    <Modal
      title={`New ${name}`}
      onClose={onClose}
      onSubmit={() => {
        setBusy(true);
        setError("");
        const usernames = members
          .split(/[\s,]+/)
          .map((m) => m.trim())
          .filter(Boolean);
        onCreate(kind, title.trim(), usernames)
          .then(onClose)
          .catch((e: Error) =>
            setError(
              e.message === "USERNAME_NOT_OCCUPIED"
                ? "One of those usernames doesn't exist"
                : e.message,
            ),
          )
          .finally(() => setBusy(false));
      }}
      actions={[
        { label: "Cancel", onClick: onClose },
        { label: "Create", primary: true, submit: true, disabled: busy || !title.trim() },
      ]}
    >
      <Field label={`${name} name`} value={title} onChange={setTitle} maxLength={128} autoFocus />
      <Field
        label={kind === "channel" ? "Subscribers (optional)" : "Members (optional)"}
        value={members}
        onChange={setMembers}
        placeholder="@alice, @demo_bot"
        hint={
          kind === "channel" ? "Bots join channels as admins." : "Separate usernames with commas."
        }
      />
      {error && <p className="find-error">{error}</p>}
    </Modal>
  );
}

/** A basic group that became a supergroup: its dialog is the supergroup's now. */
function upgraded(snapshot: Snapshot, key: string): boolean {
  const chat = snapshot.chats.get(key);
  return chat?._ === "chat" && chat.migrated_to !== undefined;
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
