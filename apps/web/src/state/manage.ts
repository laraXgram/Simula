/**
 * Managing a group or channel, as Telegram's apps do it: members by role, admins and
 * their rights, restrictions and bans, default permissions, the description and public
 * link, invite links and join requests. Each action is a TL call; the store then
 * learns the result like any other update.
 */
import type { tl } from "@simula/schema";

import type { Client } from "./client";
import { inputUser } from "./peers";

export type Role = "creator" | "admin" | "member" | "restricted" | "banned";

/** A member (or a removed user) as the management screens show them. */
export interface Member {
  user: tl.user;
  role: Role;
  /** Custom admin title ("rank"). */
  rank?: string;
  adminRights?: tl.chatAdminRights;
  bannedRights?: tl.chatBannedRights;
  /** The account may change this admin's rights. */
  canEdit: boolean;
  date: number;
  /** Who added, promoted or removed them. */
  byId?: string;
}

export type MemberFilter = "recent" | "admins" | "banned" | "kicked" | "bots";

/** A link and its state, as the links screens show it. */
export type Link = tl.chatInviteExported;

export interface LinkOptions {
  title?: string;
  /** Unix time; 0 for never. */
  expireDate?: number;
  /** 0 for no limit. */
  usageLimit?: number;
  requestNeeded?: boolean;
}

export interface Importer {
  user: tl.user;
  date: number;
  about?: string;
  approvedBy?: string;
}

/** Every right an admin can hold in a supergroup or channel, all granted. */
export function fullAdminRights(broadcast: boolean): tl.chatAdminRights {
  return {
    _: "chatAdminRights",
    change_info: true,
    post_messages: broadcast || undefined,
    edit_messages: broadcast || undefined,
    delete_messages: true,
    ban_users: broadcast ? undefined : true,
    invite_users: true,
    pin_messages: broadcast ? undefined : true,
    manage_topics: broadcast ? undefined : true,
    manage_call: true,
    other: true,
  };
}

export class ChatManager {
  constructor(
    private readonly client: Client,
    readonly key: string,
  ) {}

  private get session() {
    return this.client.session;
  }

  private get chat() {
    return this.client.store.snapshot().chats.get(this.key);
  }

  get isChannel(): boolean {
    return this.key.startsWith("ch");
  }

  private channel(): tl.InputChannel {
    const channel = this.client.inputChannelOf(this.key);
    if (!channel) throw new Error("not a supergroup or channel");
    return channel;
  }

  private peer(): tl.InputPeer {
    const peer = this.client.inputPeerOf(this.key);
    if (!peer) throw new Error("unknown chat");
    return peer;
  }

  private user(id: string): tl.InputUser {
    const user = this.client.store.snapshot().users.get(id);
    if (!user) throw new Error("unknown user");
    return inputUser(user, this.client.me);
  }

  private refresh() {
    return this.client.loadDetails(this.key).catch(() => undefined);
  }

  // Members

  /** Members by role (basic groups know only their current members). */
  async members(filter: MemberFilter, query = ""): Promise<Member[]> {
    if (!this.isChannel) return this.groupMembers(filter, query);
    const filters: Record<MemberFilter, tl.ChannelParticipantsFilter> = {
      recent: query
        ? { _: "channelParticipantsSearch", q: query }
        : { _: "channelParticipantsRecent" },
      admins: { _: "channelParticipantsAdmins" },
      banned: { _: "channelParticipantsBanned", q: query },
      kicked: { _: "channelParticipantsKicked", q: query },
      bots: { _: "channelParticipantsBots" },
    };
    const result = await this.session.call("channels.getParticipants", {
      channel: this.channel(),
      filter: filters[filter],
      offset: 0,
      limit: 200,
      hash: "0",
    });
    if (result._ !== "channels.channelParticipants") return [];
    this.client.store.addUsers(result.users);
    const users = this.client.store.snapshot().users;
    const out: Member[] = [];
    for (const p of result.participants) {
      const id =
        p._ === "channelParticipantBanned" || p._ === "channelParticipantLeft"
          ? p.peer._ === "peerUser"
            ? p.peer.user_id
            : undefined
          : p.user_id;
      const user = id ? users.get(id) : undefined;
      if (!user) continue;
      switch (p._) {
        case "channelParticipantCreator":
          out.push({
            user,
            role: "creator",
            rank: p.rank,
            canEdit: false,
            date: 0,
            adminRights: rights(p.admin_rights),
          });
          break;
        case "channelParticipantAdmin":
          out.push({
            user,
            role: "admin",
            rank: p.rank,
            canEdit: p.can_edit === true,
            date: p.date,
            byId: p.promoted_by,
            adminRights: rights(p.admin_rights),
          });
          break;
        case "channelParticipantBanned":
          out.push({
            user,
            role: p.left ? "banned" : "restricted",
            canEdit: true,
            date: p.date,
            byId: p.kicked_by,
            bannedRights: p.banned_rights._ === "chatBannedRights" ? p.banned_rights : undefined,
          });
          break;
        case "channelParticipant":
        case "channelParticipantSelf":
          out.push({ user, role: "member", rank: p.rank, canEdit: false, date: p.date });
          break;
        default:
          break;
      }
    }
    return query ? out.filter((m) => matches(m.user, query)) : out;
  }

  private async groupMembers(filter: MemberFilter, query: string): Promise<Member[]> {
    if (filter === "banned" || filter === "kicked") return [];
    const full = await this.session.call("messages.getFullChat", { chat_id: this.key.slice(1) });
    this.client.store.addUsers(full.users);
    const users = this.client.store.snapshot().users;
    const info = full.full_chat;
    if (info._ !== "chatFull" || info.participants._ !== "chatParticipants") return [];
    const me = this.client.me;
    const iAmCreator = info.participants.participants.some(
      (p) => p._ === "chatParticipantCreator" && p.user_id === me,
    );
    const out: Member[] = [];
    for (const p of info.participants.participants) {
      const user = users.get(p.user_id);
      if (!user) continue;
      const role: Role =
        p._ === "chatParticipantCreator"
          ? "creator"
          : p._ === "chatParticipantAdmin"
            ? "admin"
            : "member";
      if (filter === "admins" && role === "member") continue;
      if (filter === "bots" && !user.bot) continue;
      out.push({
        user,
        role,
        canEdit: iAmCreator && role !== "creator",
        date: "date" in p ? p.date : 0,
        byId: "inviter_id" in p ? p.inviter_id : undefined,
      });
    }
    return query ? out.filter((m) => matches(m.user, query)) : out;
  }

  /** Add people by user id. Channels take bots only as admins. */
  async add(userIds: readonly string[]) {
    if (!this.isChannel) {
      for (const id of userIds) {
        const result = await this.session.call("messages.addChatUser", {
          chat_id: this.key.slice(1),
          user_id: this.user(id),
          fwd_limit: 100,
        });
        this.client.store.applyUpdates(result.updates);
      }
      return this.refresh();
    }
    const chat = this.chat;
    const broadcast = chat?._ === "channel" && chat.broadcast === true;
    const users = this.client.store.snapshot().users;
    const bots = userIds.filter((id) => users.get(id)?.bot);
    const people = userIds.filter((id) => !users.get(id)?.bot);
    if (people.length > 0 || (!broadcast && bots.length > 0)) {
      const result = await this.session.call("channels.inviteToChannel", {
        channel: this.channel(),
        users: (broadcast ? people : [...people, ...bots]).map((id) => this.user(id)),
      });
      this.client.store.applyUpdates(result.updates);
    }
    if (broadcast) {
      for (const bot of bots) await this.promote(bot, fullAdminRights(true), "");
    }
    return this.refresh();
  }

  /** Make an admin (or change an admin's rights and title). */
  async promote(userId: string, adminRights: tl.chatAdminRights, rank: string) {
    if (!this.isChannel) {
      await this.session.call("messages.editChatAdmin", {
        chat_id: this.key.slice(1),
        user_id: this.user(userId),
        is_admin: true,
      });
      return this.refresh();
    }
    this.client.store.applyUpdates(
      await this.session.call("channels.editAdmin", {
        channel: this.channel(),
        user_id: this.user(userId),
        admin_rights: adminRights,
        rank,
      }),
    );
    return this.refresh();
  }

  /** Take an admin's rights away. */
  async dismiss(userId: string) {
    if (!this.isChannel) {
      await this.session.call("messages.editChatAdmin", {
        chat_id: this.key.slice(1),
        user_id: this.user(userId),
        is_admin: false,
      });
      return this.refresh();
    }
    this.client.store.applyUpdates(
      await this.session.call("channels.editAdmin", {
        channel: this.channel(),
        user_id: this.user(userId),
        admin_rights: { _: "chatAdminRights" },
        rank: "",
      }),
    );
    return this.refresh();
  }

  /** Restrict a member: the rights they lose, until `until` (0: forever). */
  async restrict(userId: string, banned: tl.chatBannedRights) {
    this.client.store.applyUpdates(
      await this.session.call("channels.editBanned", {
        channel: this.channel(),
        participant: this.inputPeerOfUser(userId),
        banned_rights: banned,
      }),
    );
    return this.refresh();
  }

  /** Remove a member and keep them out (until unbanned). */
  async ban(userId: string) {
    if (!this.isChannel) return this.remove(userId);
    return this.restrict(userId, { _: "chatBannedRights", view_messages: true, until_date: 0 });
  }

  /** Lift a ban or all restrictions. */
  async unban(userId: string) {
    return this.restrict(userId, { _: "chatBannedRights", until_date: 0 });
  }

  /** Remove a member who may come back (Telegram bans, then lifts the ban). */
  async remove(userId: string) {
    if (!this.isChannel) {
      this.client.store.applyUpdates(
        await this.session.call("messages.deleteChatUser", {
          chat_id: this.key.slice(1),
          user_id: this.user(userId),
        }),
      );
      return this.refresh();
    }
    await this.ban(userId);
    return this.unban(userId);
  }

  private inputPeerOfUser(userId: string): tl.InputPeer {
    const user = this.user(userId);
    if (user._ === "inputUserSelf") return { _: "inputPeerSelf" };
    if (user._ !== "inputUser") throw new Error("unknown user");
    return { _: "inputPeerUser", user_id: user.user_id, access_hash: user.access_hash };
  }

  // The chat itself

  async setDefaultPermissions(banned: tl.chatBannedRights) {
    this.client.store.applyUpdates(
      await this.session.call("messages.editChatDefaultBannedRights", {
        peer: this.peer(),
        banned_rights: banned,
      }),
    );
    return this.refresh();
  }

  async setAbout(about: string) {
    await this.session.call("messages.editChatAbout", { peer: this.peer(), about });
    return this.refresh();
  }

  async setTitle(title: string) {
    return this.client.rename(this.key, title);
  }

  /** Make the chat public with `username`, or private (""). */
  async setUsername(username: string) {
    await this.session.call("channels.updateUsername", { channel: this.channel(), username });
    const result = await this.session.call("channels.getChannels", { id: [this.channel()] });
    if (result._ === "messages.chats") this.client.store.addChats(result.chats);
  }

  /** Whether `username` is free for this chat. */
  async usernameFree(username: string): Promise<boolean> {
    return this.session.call("channels.checkUsername", { channel: this.channel(), username });
  }

  async delete() {
    this.client.store.applyUpdates(
      await this.session.call("channels.deleteChannel", { channel: this.channel() }),
    );
  }

  // Invite links

  async links(revoked: boolean): Promise<Link[]> {
    const result = await this.session.call("messages.getExportedChatInvites", {
      revoked: revoked || undefined,
      peer: this.peer(),
      admin_id: { _: "inputUserSelf" },
      limit: 100,
    });
    this.client.store.addUsers(result.users);
    return result.invites.filter((i): i is Link => i._ === "chatInviteExported");
  }

  async createLink(options: LinkOptions): Promise<Link> {
    const link = await this.session.call("messages.exportChatInvite", {
      peer: this.peer(),
      title: options.title || undefined,
      expire_date: options.expireDate || undefined,
      usage_limit: options.usageLimit || undefined,
      request_needed: options.requestNeeded || undefined,
    });
    if (link._ !== "chatInviteExported") throw new Error("no link");
    return link;
  }

  async editLink(link: string, options: LinkOptions) {
    await this.session.call("messages.editExportedChatInvite", {
      peer: this.peer(),
      link,
      title: options.title ?? "",
      expire_date: options.expireDate ?? 0,
      usage_limit: options.usageLimit ?? 0,
      request_needed: options.requestNeeded ?? false,
    });
  }

  async revokeLink(link: string) {
    await this.session.call("messages.editExportedChatInvite", {
      peer: this.peer(),
      link,
      revoked: true,
    });
    return this.refresh();
  }

  async deleteLink(link: string) {
    await this.session.call("messages.deleteExportedChatInvite", { peer: this.peer(), link });
  }

  async deleteRevokedLinks() {
    await this.session.call("messages.deleteRevokedExportedChatInvites", {
      peer: this.peer(),
      admin_id: { _: "inputUserSelf" },
    });
  }

  /** Who joined through `link`, or (no link) who asked to join. */
  async importers(link?: string): Promise<Importer[]> {
    const result = await this.session.call("messages.getChatInviteImporters", {
      requested: link === undefined || undefined,
      peer: this.peer(),
      link,
      offset_date: 0,
      offset_user: { _: "inputUserEmpty" },
      limit: 100,
    });
    this.client.store.addUsers(result.users);
    const users = this.client.store.snapshot().users;
    return result.importers.flatMap((i) => {
      const user = users.get(i.user_id);
      return user ? [{ user, date: i.date, about: i.about, approvedBy: i.approved_by }] : [];
    });
  }

  /** Approve or dismiss a join request. */
  async decide(userId: string, approve: boolean) {
    this.client.store.applyUpdates(
      await this.session.call("messages.hideChatJoinRequest", {
        approved: approve || undefined,
        peer: this.peer(),
        user_id: this.user(userId),
      }),
    );
    return this.refresh();
  }
}

function rights(r: tl.ChatAdminRights): tl.chatAdminRights | undefined {
  return r._ === "chatAdminRights" ? r : undefined;
}

function matches(user: tl.user, query: string): boolean {
  const q = query.toLowerCase().replace(/^@/, "");
  return [user.first_name, user.last_name, user.username]
    .filter(Boolean)
    .some((part) => part?.toLowerCase().includes(q));
}
