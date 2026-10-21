/**
 * The client's actions: the chat list, opening chats, sending, buttons, reading, groups,
 * supergroups and channels (members, admins, pins, invite links). Every result that
 * carries updates goes through the store, like pushed updates do.
 */
import type { tl } from "@simula/schema";

import { type Params, Session } from "../api/session";
import type { Attachment } from "./attach";
import { Files, randomId, UploadCancelled } from "./files";
import { ChatManager } from "./manage";
import { parseFormatting } from "./markdown";
import { toBase64 } from "./media";
import { inputChannelFor, inputPeerFor, inputUser, peerKey, userPeer } from "./peers";
import { chatKey, type PendingUpload, Store } from "./store";

export interface Outgoing {
  text: string;
  replyTo?: number;
  /** The forum topic written in (General when absent). */
  topic?: number;
  /** The chat `replyTo` is in, when it isn't the one written to. */
  replyIn?: string;
}

/** What "New group" creates. */
export type GroupKind = "group" | "supergroup" | "forum" | "channel";

/** What editing a topic changes. */
export interface TopicEdit {
  title?: string;
  closed?: boolean;
  hidden?: boolean;
}

/** Every right an admin can be given (what the client grants when promoting). */
const ADMIN_RIGHTS: tl.chatAdminRights = {
  _: "chatAdminRights",
  change_info: true,
  post_messages: true,
  edit_messages: true,
  delete_messages: true,
  ban_users: true,
  invite_users: true,
  pin_messages: true,
  manage_call: true,
  other: true,
};

export class Client {
  readonly store = new Store();
  private unsubscribe: () => void;

  readonly files: Files;

  private constructor(readonly session: Session) {
    this.files = new Files(session);
    this.unsubscribe = session.onUpdates((updates) => this.store.applyUpdates(updates));
  }

  static async connect(account: string): Promise<Client> {
    const client = new Client(await Session.open(account));
    const [me] = await client.session.call("users.getUsers", { id: [{ _: "inputUserSelf" }] });
    if (me?._ === "user") client.store.setMe(me);
    await client.loadDialogs();
    return client;
  }

  close() {
    this.files.close();
    this.unsubscribe();
    this.session.close();
  }

  get me(): string {
    return this.session.account;
  }

  async loadDialogs() {
    const dialogs = await this.session.call("messages.getDialogs", {
      offset_date: 0,
      offset_id: 0,
      offset_peer: { _: "inputPeerEmpty" },
      limit: 100,
      hash: "0",
    });
    this.store.setDialogs(dialogs);
  }

  /** The newest messages of a chat, plus what its header needs (members, commands). */
  async openChat(key: string) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    this.store.addHistory(await this.session.call("messages.getHistory", history(peer, 0)));
    if (key.startsWith("c")) {
      await this.loadDetails(key).catch(() => undefined);
    } else {
      await this.loadCommands(key);
    }
    await this.read(key);
  }

  /**
   * Messages older than `before` (in a forum topic, when given); resolves with how many
   * came (0 = the beginning).
   */
  async loadOlder(key: string, before: number, topic?: number): Promise<number> {
    const peer = this.inputPeerOf(key);
    if (!peer) return 0;
    const result =
      topic === undefined
        ? await this.session.call("messages.getHistory", { ...history(peer, before), limit: 50 })
        : await this.session.call("messages.getReplies", {
            ...history(peer, before),
            msg_id: topic,
            limit: 50,
          });
    this.store.addHistory(result);
    return "messages" in result ? result.messages.length : 0;
  }

  async read(key: string) {
    const dialog = this.store.snapshot().dialogs.find((d) => d.key === key);
    if (!dialog || dialog.unread === 0) return;
    const channel = this.inputChannelOf(key);
    if (channel) {
      await this.session.call("channels.readHistory", { channel, max_id: dialog.top });
    } else {
      const peer = this.inputPeerOf(key);
      if (!peer) return;
      await this.session.call("messages.readHistory", { peer, max_id: dialog.top });
    }
    this.store.markRead(key, dialog.top);
  }

  async send(key: string, outgoing: Outgoing) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    const { text, entities } = parseFormatting(outgoing.text);
    const params: Params<"messages.sendMessage"> = { peer, message: text, random_id: randomId() };
    if (entities.length > 0) params.entities = entities;
    params.reply_to = this.replyTo(outgoing);
    this.store.applyUpdates(await this.session.call("messages.sendMessage", params));
  }

  /** In a forum topic, a message replies to the topic unless it replies to a message. */
  private replyTo(outgoing: Omit<Outgoing, "text">): tl.InputReplyTo | undefined {
    const topic = outgoing.topic !== undefined && outgoing.topic !== 1 ? outgoing.topic : undefined;
    const replyTo = outgoing.replyTo ?? topic;
    if (!replyTo) return undefined;
    return {
      _: "inputReplyToMessage",
      reply_to_msg_id: replyTo,
      top_msg_id: topic,
      reply_to_peer_id: outgoing.replyIn ? this.inputPeerOf(outgoing.replyIn) : undefined,
    };
  }

  /**
   * Send files, each a pending message with its progress until it's sent. Grouped, they
   * go as albums (photos with videos, audio with audio, files with files; ten at most)
   * captioned by `outgoing.text`; otherwise one by one, after the caption as its own
   * message when there are several, as Telegram's apps do.
   */
  async sendFiles(
    key: string,
    attachments: readonly Attachment[],
    outgoing: Outgoing,
    group: boolean,
  ) {
    const peer = this.inputPeerOf(key);
    if (!peer || attachments.length === 0) return;
    const { text, entities } = parseFormatting(outgoing.text);
    const caption = { message: text, entities: entities.length > 0 ? entities : undefined };
    const reply_to = this.replyTo(outgoing);
    const batches = group ? albums(attachments) : attachments.map((a) => [a]);
    if (!group && attachments.length > 1 && text.trim()) {
      await this.send(key, outgoing);
      caption.message = "";
      caption.entities = undefined;
    }
    for (const [index, batch] of batches.entries()) {
      const captioned = index === 0 ? caption : { message: "" };
      const pending = batch.map((a) => this.pending(key, a, outgoing.topic));
      try {
        const media = await Promise.all(batch.map((a, i) => this.inputMedia(key, a, pending[i])));
        if (batch.length === 1) {
          this.store.applyUpdates(
            await this.session.call("messages.sendMedia", {
              peer,
              media: media[0],
              random_id: randomId(),
              reply_to,
              ...captioned,
            }),
          );
        } else {
          const uploaded = await Promise.all(
            media.map((m) => this.session.call("messages.uploadMedia", { peer, media: m })),
          );
          this.store.applyUpdates(
            await this.session.call("messages.sendMultiMedia", {
              peer,
              reply_to,
              multi_media: uploaded.map((m, i) => ({
                _: "inputSingleMedia",
                media: reuse(m),
                random_id: randomId(),
                ...(i === 0 ? captioned : { message: "" }),
              })),
            }),
          );
        }
      } catch (error) {
        if (!(error instanceof UploadCancelled)) throw error;
      } finally {
        for (const p of pending) this.store.removeUpload(key, p.id);
      }
    }
  }

  private nextUpload = 1;

  private pending(key: string, a: Attachment, topic?: number) {
    const controller = new AbortController();
    const upload: PendingUpload = {
      id: this.nextUpload++,
      name: a.name,
      kind: a.as === "gif" ? "video" : a.as,
      size: a.size,
      progress: 0,
      preview: a.preview,
      topic,
      cancel: () => {
        controller.abort();
        this.store.removeUpload(key, upload.id);
      },
    };
    this.store.setUpload(key, upload);
    return { ...upload, signal: controller.signal };
  }

  private async inputMedia(
    key: string,
    a: Attachment,
    pending: PendingUpload & { signal: AbortSignal },
  ): Promise<tl.InputMedia> {
    const progress = (fraction: number) => {
      if (!pending.signal.aborted) this.store.setUpload(key, { ...pending, progress: fraction });
    };
    const file = await this.files.upload(a.file, a.name, progress, pending.signal);
    if (a.as === "photo") return { _: "inputMediaUploadedPhoto", file };
    const thumb = a.thumb ? await this.files.upload(a.thumb, "thumb.jpg") : undefined;
    const attributes: tl.DocumentAttribute[] =
      a.as === "voice" ? [] : [{ _: "documentAttributeFilename", file_name: a.name }];
    if (a.as === "video")
      attributes.push({
        _: "documentAttributeVideo",
        supports_streaming: true,
        duration: a.duration ?? 0,
        w: a.w ?? 0,
        h: a.h ?? 0,
      });
    if (a.as === "gif") {
      attributes.push({ _: "documentAttributeAnimated" });
      if (a.w && a.h) attributes.push({ _: "documentAttributeImageSize", w: a.w, h: a.h });
    }
    if (a.as === "audio" || a.as === "voice")
      attributes.push({
        _: "documentAttributeAudio",
        voice: a.as === "voice" || undefined,
        duration: Math.round(a.duration ?? 0),
        title: a.title,
        performer: a.performer,
        waveform: a.waveform && toBase64(a.waveform),
      });
    return {
      _: "inputMediaUploadedDocument",
      file,
      thumb,
      mime_type: a.mime,
      attributes,
      force_file: a.as === "file" || undefined,
    };
  }

  /** Start a bot, as a deep link's START button does: `/start <param>`. */
  async startBot(key: string, param = "") {
    const user = this.userFor(key);
    if (!user) return;
    this.store.applyUpdates(
      await this.session.call("messages.startBot", {
        bot: inputUser(user, this.me),
        peer: { _: "inputPeerUser", user_id: user.id, access_hash: user.access_hash ?? "0" },
        random_id: randomId(),
        start_param: param,
      }),
    );
  }

  /** Press a callback button; resolves with the bot's answer (toast, alert or URL). */
  press(key: string, messageId: number, data: string) {
    const peer = this.inputPeerOf(key);
    if (!peer) return Promise.reject(new Error("unknown chat"));
    return this.session.call("messages.getBotCallbackAnswer", { peer, msg_id: messageId, data });
  }

  async deleteMessages(key: string, ids: number[], revoke: boolean) {
    const channel = this.inputChannelOf(key);
    const result = channel
      ? await this.session.call("channels.deleteMessages", { channel, id: ids })
      : await this.session.call("messages.deleteMessages", {
          id: ids,
          revoke: revoke || undefined,
        });
    const update: tl.Update = channel
      ? {
          _: "updateDeleteChannelMessages",
          channel_id: key.slice(2),
          messages: ids,
          pts: result.pts,
          pts_count: result.pts_count,
        }
      : {
          _: "updateDeleteMessages",
          messages: ids,
          pts: result.pts,
          pts_count: result.pts_count,
        };
    this.store.applyUpdates({
      _: "updates",
      updates: [update],
      users: [],
      chats: [],
      date: 0,
      seq: 0,
    });
  }

  async editMessage(key: string, id: number, input: string) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    const { text, entities } = parseFormatting(input);
    this.store.applyUpdates(
      await this.session.call("messages.editMessage", {
        peer,
        id,
        message: text,
        entities: entities.length > 0 ? entities : undefined,
      }),
    );
  }

  async pin(key: string, id: number, unpin: boolean) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    this.store.applyUpdates(
      await this.session.call("messages.updatePinnedMessage", {
        peer,
        id,
        unpin: unpin || undefined,
      }),
    );
  }

  /** Find `@username` (a user, bot, public group or channel); returns the dialog key. */
  async resolve(username: string): Promise<string> {
    const resolved = await this.session.call("contacts.resolveUsername", {
      username: username.replace(/^@/, ""),
    });
    this.store.addUsers(resolved.users);
    this.store.addChats(resolved.chats);
    return peerKey(resolved.peer);
  }

  // Groups, supergroups and channels

  /** Create a group, supergroup or channel with the people named by `usernames`. */
  async createGroup(kind: GroupKind, title: string, usernames: readonly string[]): Promise<string> {
    const users: tl.user[] = [];
    for (const username of usernames) {
      const user = this.userFor(await this.resolve(username));
      if (user) users.push(user);
    }
    if (kind === "group") {
      const result = await this.session.call("messages.createChat", {
        users: users.map((u) => inputUser(u, this.me)),
        title,
      });
      this.store.applyUpdates(result.updates);
      return createdKey(result.updates);
    }
    const created = await this.session.call("channels.createChannel", {
      broadcast: kind === "channel" || undefined,
      megagroup: kind === "supergroup" || kind === "forum" || undefined,
      forum: kind === "forum" || undefined,
      title,
      about: "",
    });
    this.store.applyUpdates(created);
    const key = createdKey(created);
    this.store.ensureDialog(key, { _: "peerChannel", channel_id: key.slice(2) });
    for (const user of users) await this.addUser(key, user);
    return key;
  }

  async addMember(key: string, username: string) {
    const user = this.userFor(await this.resolve(username));
    if (!user) throw new Error("that's not a user");
    await this.addUser(key, user);
    await this.loadDetails(key);
  }

  /** Channels take bots only as admins, as Telegram's apps add them. */
  private async addUser(key: string, user: tl.user) {
    const channel = this.inputChannelOf(key);
    if (!channel) {
      const result = await this.session.call("messages.addChatUser", {
        chat_id: key.slice(1),
        user_id: inputUser(user, this.me),
        fwd_limit: 100,
      });
      this.store.applyUpdates(result.updates);
      return;
    }
    const chat = this.store.snapshot().chats.get(key);
    if (user.bot && chat?._ === "channel" && chat.broadcast) {
      await this.promote(key, user);
      return;
    }
    const result = await this.session.call("channels.inviteToChannel", {
      channel,
      users: [inputUser(user, this.me)],
    });
    this.store.applyUpdates(result.updates);
  }

  /** Make `@username` an admin with every right. */
  async promoteByName(key: string, username: string) {
    const user = this.userFor(await this.resolve(username));
    if (!user) throw new Error("that's not a user");
    await this.promote(key, user);
    await this.loadDetails(key);
  }

  private async promote(key: string, user: tl.user) {
    const channel = this.inputChannelOf(key);
    if (!channel) throw new Error("only supergroups and channels have admins with rights");
    this.store.applyUpdates(
      await this.session.call("channels.editAdmin", {
        channel,
        user_id: inputUser(user, this.me),
        admin_rights: ADMIN_RIGHTS,
      }),
    );
  }

  async join(key: string) {
    const channel = this.inputChannelOf(key);
    if (!channel) return;
    const result = await this.session.call("channels.joinChannel", { channel });
    if (result._ === "messages.chatInviteJoinResultOk") this.store.applyUpdates(result.updates);
    this.store.ensureDialog(key, { _: "peerChannel", channel_id: key.slice(2) });
    await this.openChat(key);
  }

  /** Use an invite link; returns the chat's key, or throws INVITE_REQUEST_SENT. */
  async joinByLink(link: string): Promise<string> {
    const hash = link.trim().replace(/^(https?:\/\/)?(t|telegram)\.me\/(\+|joinchat\/)/, "");
    const invite = await this.session.call("messages.checkChatInvite", { hash });
    if (invite._ === "chatInviteAlready") {
      this.store.addChats([invite.chat]);
      return chatKey(invite.chat) ?? "";
    }
    const result = await this.session.call("messages.importChatInvite", { hash });
    if (result._ !== "messages.chatInviteJoinResultOk") throw new Error("can't join here");
    this.store.applyUpdates(result.updates);
    const chats = "chats" in result.updates ? result.updates.chats : [];
    const key = chats.map(chatKey).find(Boolean) ?? "";
    if (key.startsWith("ch")) {
      this.store.ensureDialog(key, { _: "peerChannel", channel_id: key.slice(2) });
    }
    return key;
  }

  /** The account's primary invite link (made on first use). */
  async inviteLink(key: string): Promise<string> {
    const known = this.store.snapshot().details.get(key)?.inviteLink;
    if (known) return known;
    const peer = this.inputPeerOf(key);
    if (!peer) throw new Error("unknown chat");
    const invite = await this.session.call("messages.exportChatInvite", {
      peer,
      legacy_revoke_permanent: true,
    });
    if (invite._ !== "chatInviteExported") throw new Error("no link");
    await this.loadDetails(key);
    return invite.link;
  }

  async leave(key: string) {
    const channel = this.inputChannelOf(key);
    if (channel) {
      this.store.applyUpdates(await this.session.call("channels.leaveChannel", { channel }));
      return;
    }
    this.store.applyUpdates(
      await this.session.call("messages.deleteChatUser", {
        chat_id: key.slice(1),
        user_id: { _: "inputUserSelf" },
      }),
    );
  }

  // Forums

  /** The forum's topics, and the newest message of each. */
  async loadTopics(key: string) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    const found = await this.session.call("messages.getForumTopics", {
      peer,
      offset_date: 0,
      offset_id: 0,
      offset_topic: 0,
      limit: 100,
    });
    this.store.addUsers(found.users);
    this.store.addChats(found.chats);
    this.store.addHistory({
      _: "messages.messages",
      messages: found.messages,
      topics: [],
      chats: [],
      users: [],
    });
    this.store.setTopics(
      key,
      found.topics.filter((t): t is tl.forumTopic => t._ === "forumTopic"),
    );
  }

  /** The newest messages of a topic. */
  async openTopic(key: string, topic: number) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    this.store.addHistory(
      await this.session.call("messages.getReplies", { ...history(peer, 0), msg_id: topic }),
    );
  }

  /** Open a topic; returns its id. */
  async createTopic(key: string, title: string, iconColor?: number): Promise<number> {
    const peer = this.inputPeerOf(key);
    if (!peer) throw new Error("unknown chat");
    const updates = await this.session.call("messages.createForumTopic", {
      peer,
      title,
      icon_color: iconColor,
      random_id: randomId(),
    });
    this.store.applyUpdates(updates);
    await this.loadTopics(key);
    const list = "updates" in updates ? updates.updates : [];
    for (const update of list) {
      if (update._ === "updateNewChannelMessage" && update.message._ === "messageService")
        return update.message.id;
    }
    throw new Error("the topic wasn't created");
  }

  async editTopic(key: string, topic: number, edit: TopicEdit) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    this.store.applyUpdates(
      await this.session.call("messages.editForumTopic", { peer, topic_id: topic, ...edit }),
    );
    await this.loadTopics(key);
  }

  async pinTopic(key: string, topic: number, pinned: boolean) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    this.store.applyUpdates(
      await this.session.call("messages.updatePinnedForumTopic", { peer, topic_id: topic, pinned }),
    );
    await this.loadTopics(key);
  }

  /** Delete a topic with everything in it. */
  async deleteTopic(key: string, topic: number) {
    const peer = this.inputPeerOf(key);
    if (!peer) return;
    await this.session.call("messages.deleteTopicHistory", { peer, top_msg_id: topic });
    await this.loadTopics(key);
  }

  /** Turn a supergroup into a forum or back (its creator only). */
  async toggleForum(key: string, enabled: boolean) {
    const channel = this.inputChannelOf(key);
    if (!channel) return;
    this.store.applyUpdates(
      await this.session.call("channels.toggleForum", { channel, enabled, tabs: false }),
    );
    await this.loadDetails(key);
  }

  /** Upgrade a basic group to a supergroup; returns the supergroup's key. */
  async upgrade(key: string): Promise<string> {
    const updates = await this.session.call("messages.migrateChat", { chat_id: key.slice(1) });
    this.store.applyUpdates(updates);
    const chats = "chats" in updates ? updates.chats : [];
    const channel = chats.find((c) => c._ === "channel");
    if (!channel) throw new Error("the group wasn't upgraded");
    const upgraded = `ch${channel.id}`;
    this.store.ensureDialog(upgraded, { _: "peerChannel", channel_id: channel.id });
    return upgraded;
  }

  // My profile

  async myBio(): Promise<string> {
    const full = await this.session.call("users.getFullUser", { id: { _: "inputUserSelf" } });
    this.store.addUsers(full.users);
    return full.full_user._ === "userFull" ? (full.full_user.about ?? "") : "";
  }

  async updateProfile(first_name: string, last_name: string, about: string) {
    const me = await this.session.call("account.updateProfile", { first_name, last_name, about });
    if (me._ === "user") this.store.setMe(me);
  }

  /** Change my username; "" removes it. */
  async setUsername(username: string) {
    const me = await this.session.call("account.updateUsername", { username });
    if (me._ === "user") this.store.setMe(me);
  }

  async usernameFree(username: string): Promise<boolean> {
    return this.session.call("account.checkUsername", { username });
  }

  async rename(key: string, title: string) {
    const channel = this.inputChannelOf(key);
    this.store.applyUpdates(
      channel
        ? await this.session.call("channels.editTitle", { channel, title })
        : await this.session.call("messages.editChatTitle", { chat_id: key.slice(1), title }),
    );
  }

  /** Members (basic groups), counts, description, the bots and their commands. */
  async loadDetails(key: string) {
    const channel = this.inputChannelOf(key);
    const full = channel
      ? await this.session.call("channels.getFullChannel", { channel })
      : await this.session.call("messages.getFullChat", { chat_id: key.slice(1) });
    this.store.addUsers(full.users);
    this.store.addChats(full.chats);
    const info = full.full_chat;
    const bots: string[] = [];
    if (info._ === "chatFull" || info._ === "channelFull") {
      for (const bot of info.bot_info ?? []) {
        if (bot._ !== "botInfo" || bot.user_id === undefined) continue;
        bots.push(bot.user_id);
        this.store.setCommands(
          bot.user_id,
          (bot.commands ?? []).filter((c): c is tl.botCommand => c._ === "botCommand"),
        );
      }
    }
    const link = (invite: tl.ExportedChatInvite | undefined) =>
      invite?._ === "chatInviteExported" ? invite.link : undefined;
    if (info._ === "chatFull") {
      const members =
        info.participants._ === "chatParticipants" ? info.participants.participants : [];
      this.store.setDetails(key, {
        members,
        count: members.length,
        about: info.about,
        bots,
        inviteLink: link(info.exported_invite),
      });
    } else if (info._ === "channelFull") {
      this.store.setDetails(key, {
        count: info.participants_count ?? 0,
        admins: info.admins_count,
        kicked: info.kicked_count,
        restricted: info.banned_count,
        requests: info.requests_pending,
        about: info.about,
        bots,
        inviteLink: link(info.exported_invite),
        migratedFrom:
          info.migrated_from_chat_id !== undefined ? `c${info.migrated_from_chat_id}` : undefined,
      });
    }
  }

  private async loadCommands(key: string) {
    const user = this.userFor(key);
    if (!user?.bot || this.store.snapshot().commands.has(user.id)) return;
    const full = await this.session.call("users.getFullUser", { id: inputUser(user, this.me) });
    const info = full.full_user._ === "userFull" ? full.full_user.bot_info : undefined;
    const commands = (info?._ === "botInfo" ? (info.commands ?? []) : []).filter(
      (c): c is tl.botCommand => c._ === "botCommand",
    );
    this.store.setCommands(user.id, commands);
  }

  inputPeerOf(key: string): tl.InputPeer | undefined {
    const snapshot = this.store.snapshot();
    return inputPeerFor(key, snapshot.users, snapshot.chats, this.me);
  }

  inputChannelOf(key: string): tl.InputChannel | undefined {
    return key.startsWith("ch") ? inputChannelFor(key, this.store.snapshot().chats) : undefined;
  }

  /** Management of a group or channel (members, admins, permissions, links). */
  manage(key: string): ChatManager {
    return new ChatManager(this, key);
  }

  userFor(key: string): tl.user | undefined {
    const id = key.startsWith("u") ? key.slice(1) : undefined;
    return id ? this.store.snapshot().users.get(id) : undefined;
  }

  keyFor(userId: string): string {
    return peerKey(userPeer(userId));
  }
}

/** The key of the group or channel a creation call returned. */
function createdKey(updates: tl.Updates): string {
  const chats = "chats" in updates ? updates.chats : [];
  const key = chats.map(chatKey).find(Boolean);
  if (!key) throw new Error("the group wasn't created");
  return key;
}

function history(peer: tl.InputPeer, before: number): Params<"messages.getHistory"> {
  return {
    peer,
    offset_id: before,
    offset_date: 0,
    add_offset: 0,
    limit: 100,
    max_id: 0,
    min_id: 0,
    hash: "0",
  };
}

/** Split files into albums Telegram accepts: photos with videos, audio with audio, files
 * with files, ten at most, in the order they were picked. */
function albums(attachments: readonly Attachment[]): Attachment[][] {
  // GIFs and voice messages never join albums.
  const kind = (a: Attachment) =>
    a.as === "photo" || a.as === "video"
      ? "visual"
      : a.as === "audio" || a.as === "file"
        ? a.as
        : undefined;
  const out: Attachment[][] = [];
  for (const a of attachments) {
    const last = out.at(-1);
    const fits = last?.[0] && kind(a) && kind(last[0]) === kind(a) && last.length < 10;
    if (fits) last.push(a);
    else out.push([a]);
  }
  return out;
}

/** An uploaded media, to send in an album. */
function reuse(media: tl.MessageMedia): tl.InputMedia {
  if (media._ === "messageMediaPhoto" && media.photo?._ === "photo") {
    const { id, access_hash, file_reference } = media.photo;
    return { _: "inputMediaPhoto", id: { _: "inputPhoto", id, access_hash, file_reference } };
  }
  if (media._ === "messageMediaDocument" && media.document?._ === "document") {
    const { id, access_hash, file_reference } = media.document;
    return { _: "inputMediaDocument", id: { _: "inputDocument", id, access_hash, file_reference } };
  }
  throw new Error("MEDIA_INVALID");
}
