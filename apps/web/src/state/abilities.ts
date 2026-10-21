/**
 * What the account may do in a group or channel, read from the chat object it was given
 * (its status, admin rights and the chat's permissions), as Telegram's apps decide which
 * actions to offer.
 */
import type { tl } from "@simula/schema";

import type { AnyChat } from "./store";

export interface Abilities {
  /** A current member (not left, removed or banned). */
  member: boolean;
  creator: boolean;
  admin: boolean;
  broadcast: boolean;
  /** A basic group (not a supergroup). */
  basic: boolean;
  forum: boolean;
  /** May post (channels: admins with post_messages). */
  posts: boolean;
  editsPosts: boolean;
  /** May delete anyone's messages. */
  deletes: boolean;
  pins: boolean;
  /** May add members. */
  invites: boolean;
  /** May manage invite links. */
  links: boolean;
  bans: boolean;
  promotes: boolean;
  changesInfo: boolean;
  manageTopics: boolean;
  /** May see the member list. */
  seesMembers: boolean;
  /** The account's own admin rights (all for the creator). */
  adminRights?: tl.chatAdminRights;
}

const NONE: Abilities = {
  member: false,
  creator: false,
  admin: false,
  broadcast: false,
  basic: false,
  forum: false,
  posts: false,
  editsPosts: false,
  deletes: false,
  pins: false,
  invites: false,
  links: false,
  bans: false,
  promotes: false,
  changesInfo: false,
  manageTopics: false,
  seesMembers: false,
};

/** Every right, as the creator holds them. */
const ALL: tl.chatAdminRights = {
  _: "chatAdminRights",
  change_info: true,
  post_messages: true,
  edit_messages: true,
  delete_messages: true,
  ban_users: true,
  invite_users: true,
  pin_messages: true,
  add_admins: true,
  anonymous: true,
  manage_call: true,
  other: true,
  manage_topics: true,
};

export function abilities(chat: AnyChat | undefined): Abilities {
  if (chat?._ === "chat") {
    const admin = chat.creator === true || chat.admin_rights !== undefined;
    const defaults = chat.default_banned_rights;
    const member = !chat.left && !chat.deactivated;
    return {
      ...NONE,
      member,
      creator: chat.creator === true,
      admin,
      basic: true,
      posts: member,
      deletes: admin,
      pins: member && (admin || !defaults?.pin_messages),
      invites: member && (admin || !defaults?.invite_users),
      links: member && admin,
      bans: member && admin,
      promotes: member && chat.creator === true,
      changesInfo: member && (admin || !defaults?.change_info),
      seesMembers: member,
      adminRights: chat.creator ? ALL : chat.admin_rights,
    };
  }
  if (chat?._ === "channel") {
    const creator = chat.creator === true;
    const rights = creator ? ALL : chat.admin_rights;
    const has = (right: keyof tl.chatAdminRights) => rights?.[right] === true;
    const defaults = chat.default_banned_rights;
    const banned = chat.banned_rights;
    const broadcast = chat.broadcast === true;
    const memberMay = (right: keyof tl.chatBannedRights) =>
      !broadcast && !defaults?.[right] && !banned?.[right];
    const member = !chat.left;
    const admin = creator || chat.admin_rights !== undefined;
    return {
      member,
      creator,
      admin,
      broadcast,
      basic: false,
      forum: chat.forum === true,
      posts: broadcast ? has("post_messages") : memberMay("send_plain"),
      editsPosts: broadcast && has("edit_messages"),
      deletes: has("delete_messages"),
      pins: has("pin_messages") || (broadcast && has("edit_messages")) || memberMay("pin_messages"),
      invites: member && (has("invite_users") || memberMay("invite_users")),
      links: member && has("invite_users"),
      bans: member && has("ban_users"),
      promotes: member && has("add_admins"),
      changesInfo: member && (has("change_info") || memberMay("change_info")),
      manageTopics: has("manage_topics"),
      seesMembers: member && (!broadcast || admin),
      adminRights: rights,
    };
  }
  if (chat?._ === "channelForbidden") return { ...NONE, broadcast: chat.broadcast === true };
  return NONE;
}
