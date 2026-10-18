/**
 * The bot's reply keyboard, shown in place of the system keyboard. As in Telegram, the
 * newest incoming message that carries a reply-keyboard markup decides: a keyboard,
 * its removal, or a forced reply.
 */
import type { tl } from "@simula/schema";

import { positioned } from "./InlineKeyboard";

export type ActiveMarkup =
  | { kind: "keyboard"; message: number; markup: tl.replyKeyboardMarkup }
  | { kind: "force_reply"; message: number; markup: tl.replyKeyboardForceReply }
  | undefined;

export function activeMarkup(messages: readonly tl.message[]): ActiveMarkup {
  for (const message of [...messages].sort((a, b) => b.id - a.id)) {
    if (message.out) continue;
    const markup = message.reply_markup;
    if (!markup || markup._ === "replyInlineMarkup") continue;
    if (markup._ === "replyKeyboardHide") return undefined;
    if (markup._ === "replyKeyboardForceReply")
      return { kind: "force_reply", message: message.id, markup };
    return { kind: "keyboard", message: message.id, markup };
  }
  return undefined;
}

interface Props {
  markup: tl.replyKeyboardMarkup;
  onText: (text: string) => void;
  onNotice: (text: string) => void;
}

export function ReplyKeyboard({ markup, onText, onNotice }: Props) {
  return (
    <div className={markup.resize ? "reply-keyboard resized" : "reply-keyboard"}>
      {positioned(markup.rows.map((row) => row.buttons)).map((row) => (
        <div className="reply-row" key={row.id}>
          {row.cells.map(({ id, item: button }) => (
            <button
              key={id}
              type="button"
              className="reply-button"
              dir="auto"
              onClick={() =>
                button.type._ === "buttonTypeDefault"
                  ? onText(button.text)
                  : onNotice(unsupported(button.type))
              }
            >
              {button.text}
            </button>
          ))}
        </div>
      ))}
    </div>
  );
}

function unsupported(type: tl.ButtonType): string {
  switch (type._) {
    case "buttonTypeRequestPhone":
      return "Sharing a phone number comes with contacts (P5.3).";
    case "buttonTypeRequestGeoLocation":
      return "Sharing a location comes with media messages (P5.3).";
    case "buttonTypeRequestPoll":
      return "Creating polls comes with P2.11.";
    case "buttonTypeRequestPeer":
    case "inputButtonTypeRequestPeer":
      return "Sharing users and chats comes with P5.3.";
    case "buttonTypeSimpleWebView":
      return "Mini Apps come with P5.6.";
    default:
      return "This button isn't simulated yet.";
  }
}
