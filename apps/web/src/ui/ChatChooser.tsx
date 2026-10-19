/** Pick a chat from the chat list (to reply there to a message from another chat). */
import { displayName } from "../state/peers";
import type { Snapshot } from "../state/store";
import { initialsOf } from "./ChatView";

interface Props {
  snapshot: Snapshot;
  title: string;
  /** Chats not offered (the one the message is in). */
  exclude?: string;
  onPick: (key: string) => void;
  onCancel: () => void;
}

export function ChatChooser({ snapshot, title, exclude, onPick, onCancel }: Props) {
  const choices = snapshot.dialogs
    .filter((dialog) => dialog.key !== exclude)
    .map((dialog) => ({ key: dialog.key, name: chatTitle(snapshot, dialog.key) }))
    .filter((choice): choice is { key: string; name: string } => choice.name !== undefined);
  return (
    // biome-ignore lint/a11y/noStaticElementInteractions: the backdrop closes on click, Escape is on the list
    // biome-ignore lint/a11y/useKeyWithClickEvents: as above
    <div className="alert-backdrop" onClick={onCancel}>
      <div
        className="chooser"
        role="dialog"
        aria-label={title}
        onClick={(event) => event.stopPropagation()}
        onKeyDown={(event) => event.key === "Escape" && onCancel()}
      >
        <h2>{title}</h2>
        <div className="chooser-list">
          {choices.map((choice) => (
            <button key={choice.key} type="button" onClick={() => onPick(choice.key)}>
              <span className="avatar small">{initialsOf(choice.name)}</span>
              <span dir="auto">{choice.name}</span>
            </button>
          ))}
          {choices.length === 0 && <p className="empty">No other chats</p>}
        </div>
        <button type="button" className="chooser-cancel" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </div>
  );
}

/** A chat's name as the chat list shows it; upgraded basic groups are left out. */
export function chatTitle(snapshot: Snapshot, key: string): string | undefined {
  if (key.startsWith("u")) return displayName(snapshot.users.get(key.slice(1)));
  const chat = snapshot.chats.get(key);
  if (!chat || (chat._ === "chat" && chat.migrated_to)) return undefined;
  return chat.title;
}
