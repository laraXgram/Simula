/** A service message (someone joined, left, renamed the group…), centered like Telegram's. */
import type { tl } from "@simula/schema";

import { displayName, senderId } from "../state/peers";

interface Props {
  message: tl.messageService;
  users: ReadonlyMap<string, tl.user>;
  me?: string;
}

export function ServiceMessage({ message, users, me }: Props) {
  return (
    <div className="service" data-id={message.id}>
      <span dir="auto">{describeService(message, users, me)}</span>
    </div>
  );
}

/** What a service message says, e.g. "Alice added Bob". */
export function describeService(
  message: tl.messageService,
  users: ReadonlyMap<string, tl.user>,
  me?: string,
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
    case "messageActionChatEditTitle":
      return `${who} changed the group name to «${action.title}»`;
    default:
      return "Service message";
  }
}
