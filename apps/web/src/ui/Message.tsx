/**
 * One message bubble (or an album's): reply header, media, text or caption, link
 * preview, time and ticks, keyboard, and its menu (a button, or a right-click).
 */
import type { tl } from "@simula/schema";
import { useState } from "react";

import type { DeepLink } from "../state/links";
import { contentOf, isVisual, largestPhotoSize, mediaLabel, photoLocation } from "../state/media";
import { displayName } from "../state/peers";
import { type ButtonActions, InlineKeyboard } from "./InlineKeyboard";
import { Icon } from "./kit/Icon";
import { ContextMenu, Menu, type MenuItem } from "./kit/Menu";
import { LinkPreview } from "./LinkPreview";
import { Album } from "./media/Album";
import { saveDocument, saveUrl, useMedia } from "./media/context";
import { MessageMedia } from "./media/MessageMedia";
import { RichText, type TextActions } from "./RichText";

export interface MessageActions extends TextActions, ButtonActions {
  onUser: (userId: string) => void;
  onDeepLink: (link: DeepLink) => void;
  onReply: () => void;
  /** Reply to this message from another chat (absent for protected messages). */
  onReplyElsewhere?: () => void;
  /** Open where a reply to another chat's message came from. */
  onOpenReplied?: () => void;
  onEdit?: () => void;
  onDelete?: () => void;
  /** Pin or unpin (absent when the account may not). */
  onPin?: () => void;
}

interface Props {
  message: tl.message;
  /** The album's messages (in order) when `message` heads one. */
  album?: readonly tl.message[];
  replied?: tl.message;
  users: ReadonlyMap<string, tl.user>;
  /** Outgoing messages up to this id were read by the other side. */
  readOutbox: number;
  /** In groups, incoming messages are signed with their sender's name. */
  showSender?: boolean;
  /** For replies to messages of other chats: where the message came from. */
  externalOrigin?: string;
  /** No reply header (a forum message that only names its topic). */
  hideReply?: boolean;
  actions: MessageActions;
}

export function Message({
  message,
  album,
  replied,
  users,
  readOutbox,
  showSender,
  externalOrigin,
  hideReply,
  actions,
}: Props) {
  const media = useMedia();
  const [menuAt, setMenuAt] = useState<{ x: number; y: number } | null>(null);
  const parts = album ?? [message];
  // An album's caption is the text of whichever of its messages has one.
  const captioned = parts.find((m) => m.message) ?? message;
  const content = contentOf(message);
  const visual = album ? parts.every((m) => isVisual(contentOf(m))) : isVisual(content);
  const bare =
    !album &&
    content?.kind === "document" &&
    (content.info.kind === "sticker" || content.info.kind === "round");
  const mediaOnly = (visual || bare) && !captioned.message;
  const header =
    message.reply_to?._ === "messageReplyHeader" && !hideReply ? message.reply_to : undefined;
  const preview = message.media?._ === "messageMediaWebPage" ? message.media : undefined;
  const keyboard =
    captioned.reply_markup?._ === "replyInlineMarkup" ? captioned.reply_markup : undefined;
  const sender = message.from_id?._ === "peerUser" ? users.get(message.from_id.user_id) : undefined;
  const last = parts.at(-1) ?? message;
  const edited = captioned.edit_date !== undefined && !captioned.edit_hide;
  const card = preview && <LinkPreview media={preview} />;
  const download = () => {
    const c = contentOf(captioned.media ? captioned : message);
    if (c?.kind === "document") void saveDocument(media.files, c.info);
    if (c?.kind === "photo") {
      const size = largestPhotoSize(c.photo);
      if (size)
        void media.files
          .url(photoLocation(c.photo, size.type), "image/jpeg")
          .then((url) => saveUrl(url, `photo_${message.id}.jpg`));
    }
  };
  const items: MenuItem[] = [
    { icon: "reply", label: "Reply", onSelect: actions.onReply },
    actions.onReplyElsewhere && {
      icon: "forward",
      label: "Reply in another chat",
      onSelect: actions.onReplyElsewhere,
    },
    captioned.message && {
      icon: "copy",
      label: "Copy text",
      onSelect: () => void navigator.clipboard?.writeText(captioned.message),
    },
    content &&
      content.kind !== "other" &&
      !message.noforwards && { icon: "download", label: "Download", onSelect: download },
    actions.onEdit && {
      icon: "edit",
      label: content ? "Edit caption" : "Edit",
      onSelect: actions.onEdit,
    },
    actions.onPin && {
      icon: "pin",
      label: captioned.pinned ? "Unpin" : "Pin",
      onSelect: actions.onPin,
    },
    actions.onDelete && {
      icon: "delete",
      label: "Delete",
      danger: true,
      onSelect: actions.onDelete,
    },
  ].filter((item): item is MenuItem => Boolean(item));

  return (
    <div className={message.out ? "message out" : "message in"} data-id={message.id}>
      {/* biome-ignore lint/a11y/noStaticElementInteractions: right-click opens the same menu as its button */}
      <div
        className={[
          "bubble",
          visual ? "has-media" : "",
          mediaOnly ? "media-only" : "",
          bare ? "bare" : "",
        ].join(" ")}
        onContextMenu={(event) => {
          if ((event.target as HTMLElement).closest("a, video[controls]")) return;
          event.preventDefault();
          setMenuAt({ x: event.clientX, y: event.clientY });
        }}
      >
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
        {header && externalOrigin !== undefined ? (
          <button
            type="button"
            className="reply-header external"
            onClick={actions.onOpenReplied}
            disabled={!actions.onOpenReplied}
          >
            <span className="reply-author" dir="auto">
              {externalOrigin}
            </span>
            <span className="reply-text" dir="auto">
              {header.quote_text ?? ""}
            </span>
          </button>
        ) : (
          header && (
            <div className="reply-header">
              <span className="reply-author">
                {replied ? displayName(senderOf(replied, users)) : "Deleted message"}
              </span>
              <span className="reply-text" dir="auto">
                {header.quote_text ?? (replied ? replied.message || mediaLabel(replied) || "" : "")}
              </span>
            </div>
          )
        )}
        {album ? (
          <Album messages={album} />
        ) : (
          content && <MessageMedia message={message} content={content} />
        )}
        {message.invert_media && card}
        {captioned.message && (
          <div className="text" dir="auto">
            <RichText text={captioned.message} entities={captioned.entities} {...actions} />
          </div>
        )}
        {!message.invert_media && card}
        <div className="meta">
          {captioned.pinned && <Icon name="pin" size={14} />}
          {message.noforwards && <Icon name="lock" size={14} />}
          {edited && <span>edited</span>}
          <time dateTime={new Date(last.date * 1000).toISOString()}>{clock(last.date)}</time>
          {message.out && <span className="ticks">{last.id <= readOutbox ? "✓✓" : "✓"}</span>}
        </div>
        <div className="message-menu">
          <Menu label="Message actions" trigger={<Icon name="more" size={18} />} items={items} />
        </div>
      </div>
      {keyboard && <InlineKeyboard markup={keyboard} actions={actions} />}
      {sender && !message.out && !showSender && (
        <span className="sr-only">{displayName(sender)}</span>
      )}
      {menuAt && (
        <ContextMenu items={items} x={menuAt.x} y={menuAt.y} onClose={() => setMenuAt(null)} />
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
