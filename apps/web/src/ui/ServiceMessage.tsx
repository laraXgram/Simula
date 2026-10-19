/** A service message (someone joined, left, renamed the group…), centered like Telegram's. */
import type { tl } from "@simula/schema";

import { displayName, senderId } from "../state/peers";

interface Props {
  message: tl.messageService;
  users: ReadonlyMap<string, tl.user>;
  me?: string;
  /** The message a "pinned" notice points at, if loaded. */
  replied?: tl.message;
  onOpen?: (id: number) => void;
}

export function ServiceMessage({ message, users, me, replied, onOpen }: Props) {
  const target =
    message.reply_to?._ === "messageReplyHeader" ? message.reply_to.reply_to_msg_id : undefined;
  return (
    <div className="service" data-id={message.id}>
      {target !== undefined && onOpen ? (
        <button type="button" className="service-link" dir="auto" onClick={() => onOpen(target)}>
          {describeService(message, users, me, replied)}
        </button>
      ) : (
        <span dir="auto">{describeService(message, users, me, replied)}</span>
      )}
    </div>
  );
}

/** What a service message says, e.g. "Alice added Bob". */
export function describeService(
  message: tl.messageService,
  users: ReadonlyMap<string, tl.user>,
  me?: string,
  replied?: tl.message,
): string {
  const name = (id: string | undefined) =>
    id === undefined ? "Someone" : id === me ? "You" : displayName(users.get(id));
  const actor = senderId(message);
  const who = name(actor);
  const action = message.action;
  switch (action._) {
    case "messageActionChatCreate":
      return `${who} created the group «${action.title}»`;
    case "messageActionChatAddUser":
      return action.users.length === 1 && action.users[0] === actor
        ? `${who} joined the group`
        : `${who} added ${action.users.map(name).join(", ")}`;
    case "messageActionChatDeleteUser":
      return action.user_id === actor
        ? `${who} left the group`
        : `${who} removed ${name(action.user_id)}`;
    case "messageActionChatJoinedByLink":
      return `${who} joined the group via invite link`;
    case "messageActionChatJoinedByRequest":
      return `${who} was accepted into the group`;
    case "messageActionChatEditTitle":
      return message.post
        ? `Channel name was changed to «${action.title}»`
        : `${who} changed the group name to «${action.title}»`;
    case "messageActionChannelCreate":
      return message.post
        ? `Channel «${action.title}» created`
        : `${who} created the group «${action.title}»`;
    case "messageActionTopicCreate":
      return `${who} created the topic «${action.title}»`;
    case "messageActionTopicEdit":
      if (action.closed !== undefined)
        return action.closed ? `${who} closed the topic` : `${who} reopened the topic`;
      if (action.hidden !== undefined)
        return action.hidden ? `${who} hid the topic` : `${who} unhid the topic`;
      if (action.title !== undefined) return `${who} renamed the topic to «${action.title}»`;
      return `${who} changed the topic icon`;
    case "messageActionChatMigrateTo":
      return "The group was upgraded to a supergroup";
    case "messageActionChannelMigrateFrom":
      return `${who} upgraded «${action.title}» to a supergroup`;
    case "messageActionPinMessage": {
      const text = replied?.message.replace(/\s+/g, " ") ?? "";
      const quoted = text
        ? ` «${text.length > 40 ? `${text.slice(0, 40)}…` : text}»`
        : " a message";
      return message.post ? `Pinned${quoted}` : `${who} pinned${quoted}`;
    }
    default:
      return "Service message";
  }
}
