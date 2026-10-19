/**
 * Forum topics on the client: which topic a message is in, and what the account may do
 * with topics. A topic's id is the id of the service message that opened it; General
 * is 1 and holds everything posted outside a topic.
 */
import type { tl } from "@simula/schema";

import type { AnyChat, AnyMessage } from "./store";

export const GENERAL = 1;

export function isForum(chat: AnyChat | undefined): boolean {
  return chat?._ === "channel" && chat.forum === true;
}

/** The topic a forum message is in. */
export function topicOf(message: AnyMessage): number {
  if (message._ === "messageService" && message.action._ === "messageActionTopicCreate")
    return message.id;
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  if (!header?.forum_topic) return GENERAL;
  return header.reply_to_top_id ?? header.reply_to_msg_id ?? GENERAL;
}

/** What the account may do with topics, from its view of the forum. */
export function topicRights(chat: AnyChat | undefined) {
  if (chat?._ !== "channel") return { manage: false, create: false };
  const manage = chat.creator === true || chat.admin_rights?.manage_topics === true;
  const admin = chat.creator === true || chat.admin_rights !== undefined;
  const banned = chat.banned_rights?.manage_topics || chat.default_banned_rights?.manage_topics;
  return { manage, create: manage || (!admin && !banned) };
}

/** A topic's icon color as CSS. */
export function topicColor(topic: tl.forumTopic): string {
  return `#${topic.icon_color.toString(16).padStart(6, "0")}`;
}
