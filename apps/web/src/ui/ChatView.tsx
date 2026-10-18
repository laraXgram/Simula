/** An open chat: header, messages, the bot's keyboard and the composer. */
import type { tl } from "@simula/schema";
import { useEffect, useMemo, useRef, useState } from "react";

import type { Client } from "../state/client";
import { displayName } from "../state/peers";
import type { Snapshot } from "../state/store";
import { Composer, type Draft } from "./Composer";
import { Message, type MessageActions } from "./Message";
import { activeMarkup, ReplyKeyboard } from "./ReplyKeyboard";

export type Notify = (text: string, options?: { alert?: boolean }) => void;

interface Props {
  client: Client;
  snapshot: Snapshot;
  chatKey: string;
  notify: Notify;
  onOpenChat: (key: string) => void;
  onBack: () => void;
}

export function ChatView({ client, snapshot, chatKey, notify, onOpenChat, onBack }: Props) {
  const user = client.userFor(chatKey);
  const dialog = snapshot.dialogs.find((d) => d.key === chatKey);
  const byId = snapshot.messages.get(chatKey);
  const messages = useMemo(() => [...(byId?.values() ?? [])].sort((a, b) => a.id - b.id), [byId]);
  const [draft, setDraft] = useState<Draft>({});
  const [dismissed, setDismissed] = useState<Set<number>>(new Set());
  const [keyboardShown, setKeyboardShown] = useState(true);
  const list = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setDraft({});
    client.openChat(chatKey).catch((error: Error) => notify(error.message));
  }, [client, chatKey, notify]);

  // Read what arrives while the chat is open.
  useEffect(() => {
    if (dialog && dialog.unread > 0) void client.read(chatKey);
  }, [client, chatKey, dialog]);

  // Follow the conversation: scroll down when a newer message arrives.
  const newest = messages.at(-1)?.id;
  useEffect(() => {
    if (newest !== undefined) list.current?.scrollTo({ top: list.current.scrollHeight });
  }, [newest]);

  const markup = activeMarkup(messages);
  const keyboard =
    markup?.kind === "keyboard" && !dismissed.has(markup.message) ? markup : undefined;
  // A new keyboard from the bot shows up even if the last one was hidden.
  const markupMessage = markup?.message;
  useEffect(() => {
    if (markupMessage !== undefined) setKeyboardShown(true);
  }, [markupMessage]);
  // A forced reply answers the message that asked for it.
  const forced = markup?.kind === "force_reply" ? markup : undefined;
  const replyTo =
    draft.replyTo ?? (forced && !draft.editing ? byId?.get(forced.message) : undefined);
  const placeholder = keyboard?.markup.placeholder ?? forced?.markup.placeholder;

  const send = async (text: string, fromKeyboard = false) => {
    try {
      if (draft.editing) await client.editMessage(chatKey, draft.editing.id, text);
      else await client.send(chatKey, { text, replyTo: replyTo?.id });
      if (fromKeyboard && keyboard?.markup.single_use)
        setDismissed(new Set(dismissed).add(keyboard.message));
      setDraft({});
    } catch (error) {
      notify((error as Error).message);
    }
  };

  const actionsFor = (message: tl.message): MessageActions => ({
    onReply: () => setDraft({ replyTo: message }),
    onEdit: message.out ? () => setDraft({ editing: message }) : undefined,
    onDelete: () => {
      if (!window.confirm("Delete this message for everyone?")) return;
      client.deleteMessage(message.id, true).catch((error: Error) => notify(error.message));
    },
    onCommand: (command) => void send(command),
    onMention: (username) =>
      client
        .resolve(username)
        .then(onOpenChat)
        .catch((error: Error) => notify(error.message)),
    onUser: (id) => onOpenChat(client.keyFor(id)),
    onCallback: (data) =>
      client
        .press(chatKey, message.id, data)
        .then((answer) => {
          if (answer.url) {
            if (window.confirm(`Open ${answer.url}?`))
              window.open(answer.url, "_blank", "noopener");
          } else if (answer.message) {
            notify(answer.message, { alert: answer.alert });
          }
        })
        .catch((error: Error) =>
          notify(
            error.message === "BOT_RESPONSE_TIMEOUT" ? "The bot didn't answer" : error.message,
          ),
        ),
    onNotice: (text) => notify(text),
  });

  const commands = user?.bot ? (snapshot.commands.get(user.id) ?? []) : [];

  return (
    <section className="chat">
      <header className="chat-header">
        <button type="button" className="icon-button back" onClick={onBack} title="Back">
          ←
        </button>
        <div className="avatar">{initials(user)}</div>
        <div className="chat-title">
          <span dir="auto">{displayName(user)}</span>
          <span className="chat-subtitle">
            {user?.bot ? "bot" : user?.username ? `@${user.username}` : ""}
          </span>
        </div>
      </header>
      <div className="messages" ref={list}>
        {messages.map((message) => (
          <Message
            key={message.id}
            message={message}
            replied={repliedTo(message, byId)}
            users={snapshot.users}
            readOutbox={dialog?.readOutbox ?? 0}
            actions={actionsFor(message)}
          />
        ))}
      </div>
      <Composer
        draft={{ ...draft, replyTo }}
        placeholder={placeholder}
        commands={commands}
        keyboardAvailable={keyboard !== undefined}
        keyboardShown={keyboardShown}
        onToggleKeyboard={() => setKeyboardShown(!keyboardShown)}
        onCancel={() => setDraft({})}
        onSend={(text) => void send(text)}
      />
      {keyboard && keyboardShown && (
        <ReplyKeyboard
          markup={keyboard.markup}
          onText={(text) => void send(text, true)}
          onNotice={(text) => notify(text)}
        />
      )}
    </section>
  );
}

function repliedTo(message: tl.message, byId: ReadonlyMap<number, tl.message> | undefined) {
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  return header?.reply_to_msg_id !== undefined ? byId?.get(header.reply_to_msg_id) : undefined;
}

export function initials(user: tl.user | undefined): string {
  const words = displayName(user).split(/\s+/).filter(Boolean);
  return (
    words
      .slice(0, 2)
      .map((word) => [...word][0])
      .join("")
      .toUpperCase() || "?"
  );
}
