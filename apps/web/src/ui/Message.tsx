/** One message bubble: reply header, text, link preview, time and ticks, keyboard. */
import type { tl } from "@simula/schema";

import type { DeepLink } from "../state/links";

import { displayName } from "../state/peers";
import { type ButtonActions, InlineKeyboard } from "./InlineKeyboard";
import { LinkPreview } from "./LinkPreview";
import { RichText, type TextActions } from "./RichText";

export interface MessageActions extends TextActions, ButtonActions {
  onUser: (userId: string) => void;
  onDeepLink: (link: DeepLink) => void;
  onReply: () => void;
  onEdit?: () => void;
  onDelete: () => void;
}

interface Props {
  message: tl.message;
  replied?: tl.message;
  users: ReadonlyMap<string, tl.user>;
  /** Outgoing messages up to this id were read by the other side. */
  readOutbox: number;
  /** In groups, incoming messages are signed with their sender's name. */
  showSender?: boolean;
  actions: MessageActions;
}

export function Message({ message, replied, users, readOutbox, showSender, actions }: Props) {
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  const preview = message.media?._ === "messageMediaWebPage" ? message.media : undefined;
  const keyboard =
    message.reply_markup?._ === "replyInlineMarkup" ? message.reply_markup : undefined;
  const sender = message.from_id?._ === "peerUser" ? users.get(message.from_id.user_id) : undefined;
  const edited = message.edit_date !== undefined && !message.edit_hide;
  const card = preview && <LinkPreview media={preview} />;

  return (
    <div className={message.out ? "message out" : "message in"} data-id={message.id}>
      <div className="bubble">
        {showSender && !message.out && (
          <button
            type="button"
            className="sender"
            dir="auto"
            onClick={() => sender && actions.onUser(sender.id)}
          >
            {displayName(sender)}
          </button>
        )}
        {header && (
          <div className="reply-header">
            <span className="reply-author">
              {replied ? displayName(senderOf(replied, users)) : "Deleted message"}
            </span>
            <span className="reply-text" dir="auto">
              {header.quote_text ?? replied?.message ?? ""}
            </span>
          </div>
        )}
        {message.invert_media && card}
        <div className="text" dir="auto">
          <RichText text={message.message} entities={message.entities} {...actions} />
        </div>
        {!message.invert_media && card}
        <div className="meta">
          {message.noforwards && <span title="Protected content">🔒</span>}
          {edited && <span>edited</span>}
          <time dateTime={new Date(message.date * 1000).toISOString()}>{clock(message.date)}</time>
          {message.out && <span className="ticks">{message.id <= readOutbox ? "✓✓" : "✓"}</span>}
        </div>
        <div className="message-tools">
          <button type="button" onClick={actions.onReply} title="Reply">
            ↩
          </button>
          {actions.onEdit && (
            <button type="button" onClick={actions.onEdit} title="Edit">
              ✎
            </button>
          )}
          <button type="button" onClick={actions.onDelete} title="Delete">
            🗑
          </button>
        </div>
      </div>
      {keyboard && <InlineKeyboard markup={keyboard} actions={actions} />}
      {sender && !message.out && !showSender && (
        <span className="sr-only">{displayName(sender)}</span>
      )}
    </div>
  );
}

function senderOf(message: tl.message, users: ReadonlyMap<string, tl.user>) {
  return message.from_id?._ === "peerUser" ? users.get(message.from_id.user_id) : undefined;
}

function clock(date: number): string {
  return new Date(date * 1000).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
}
