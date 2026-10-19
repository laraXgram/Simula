/**
 * What the client knows: users, groups and channels, the chat list and the messages
 * loaded per chat (service messages included). It is filled from call results and kept
 * current by the updates the session pushes, the way a Telegram client applies
 * `updateNewMessage` / `updateNewChannelMessage`, edits, deletions, read receipts and
 * pins. Messages of private chats and basic groups live in the account's own box;
 * each supergroup and channel has its own ids, so updates name the channel.
 */
import type { tl } from "@simula/schema";

import { peerKey } from "./peers";

/** A message as stored: regular, or a service message (a member joined, …). */
export type AnyMessage = tl.message | tl.messageService;

/** A basic group, supergroup or channel. */
export type AnyChat = tl.chat | tl.chatForbidden | tl.channel | tl.channelForbidden;

export interface Dialog {
  key: string;
  peer: tl.Peer;
  top: number;
  date: number;
  unread: number;
  readInbox: number;
  readOutbox: number;
}

/** What a group's or channel's full object told the client. */
export interface ChatDetails {
  /** Basic groups: everyone in it. */
  members?: readonly tl.ChatParticipant[];
  count: number;
  about: string;
  /** Bots in the chat (their commands are in `commands`). */
  bots: readonly string[];
  /** The account's primary invite link, if it made one. */
  inviteLink?: string;
  /** Supergroups: the basic group it was upgraded from ("c…"), whose history is older. */
  migratedFrom?: string;
}

export interface Snapshot {
  me?: tl.user;
  users: ReadonlyMap<string, tl.user>;
  /** Groups and channels by dialog key ("c…", "ch…"). */
  chats: ReadonlyMap<string, AnyChat>;
  /** Details of the groups and channels that were opened, by dialog key. */
  details: ReadonlyMap<string, ChatDetails>;
  dialogs: readonly Dialog[];
  /** Per dialog key, messages by id. */
  messages: ReadonlyMap<string, ReadonlyMap<number, AnyMessage>>;
  /** Bot commands per bot user id, for the commands menu. */
  commands: ReadonlyMap<string, readonly tl.botCommand[]>;
}

export class Store {
  private state: Snapshot = {
    users: new Map(),
    chats: new Map(),
    details: new Map(),
    dialogs: [],
    messages: new Map(),
    commands: new Map(),
  };
  private listeners = new Set<() => void>();

  snapshot = () => this.state;

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  setMe(me: tl.user) {
    this.addUsers([me]);
    this.commit({ me });
  }

  addUsers(users: readonly tl.User[]) {
    const next = new Map(this.state.users);
    for (const user of users)
      if (user._ === "user") next.set(user.id, { ...next.get(user.id), ...user });
    this.commit({ users: next });
  }

  addChats(chats: readonly tl.Chat[]) {
    const next = new Map(this.state.chats);
    for (const chat of chats) {
      const key = chatKey(chat);
      if (key) next.set(key, chat as AnyChat);
    }
    this.commit({ chats: next });
  }

  setDetails(key: string, details: ChatDetails) {
    const next = new Map(this.state.details);
    next.set(key, details);
    this.commit({ details: next });
  }

  setCommands(bot: string, commands: readonly tl.botCommand[]) {
    const next = new Map(this.state.commands);
    next.set(bot, commands);
    this.commit({ commands: next });
  }

  /** `messages.getDialogs`. */
  setDialogs(result: tl.messages.Dialogs) {
    if (result._ === "messages.dialogsNotModified") return;
    this.addUsers(result.users);
    this.addChats(result.chats);
    this.addMessages(result.messages);
    const dialogs: Dialog[] = [];
    for (const dialog of result.dialogs) {
      if (dialog._ !== "dialog") continue;
      const key = peerKey(dialog.peer);
      const top = this.state.messages.get(key)?.get(dialog.top_message);
      dialogs.push({
        key,
        peer: dialog.peer,
        top: dialog.top_message,
        date: top?.date ?? 0,
        unread: dialog.unread_count,
        readInbox: dialog.read_inbox_max_id,
        readOutbox: dialog.read_outbox_max_id,
      });
    }
    this.commit({ dialogs: sortDialogs(dialogs) });
  }

  /** `messages.getHistory` and the like. */
  addHistory(result: tl.messages.Messages) {
    if (result._ === "messages.messagesNotModified") return;
    this.addUsers(result.users);
    this.addChats(result.chats);
    this.addMessages(result.messages);
    // A chat listed before any message was known (joined just now) gets its top message.
    for (const dialog of this.state.dialogs) {
      if (dialog.top === 0 && this.state.messages.get(dialog.key)?.size)
        this.refreshTop(dialog.key);
    }
    this.commit({ dialogs: sortDialogs([...this.state.dialogs]) });
  }

  /** Every `Updates` the client gets, pushed or as a call's result. */
  applyUpdates(updates: tl.Updates) {
    if (updates._ !== "updates" && updates._ !== "updatesCombined") return;
    this.addUsers(updates.users);
    this.addChats(updates.chats);
    for (const update of updates.updates) this.applyUpdate(update);
  }

  markRead(key: string, maxId: number) {
    this.updateDialog(key, (d) => ({ ...d, readInbox: Math.max(d.readInbox, maxId), unread: 0 }));
  }

  /** A chat to list even before it has messages (a channel just joined). */
  ensureDialog(key: string, peer: tl.Peer) {
    if (!this.state.dialogs.some((d) => d.key === key)) {
      this.commit({ dialogs: sortDialogs([emptyDialog(key, peer), ...this.state.dialogs]) });
    }
  }

  private applyUpdate(update: tl.Update) {
    switch (update._) {
      case "updateNewMessage":
      case "updateEditMessage":
      case "updateNewChannelMessage":
      case "updateEditChannelMessage": {
        const message = update.message;
        if (message._ === "messageEmpty") return;
        const isNew =
          (update._ === "updateNewMessage" || update._ === "updateNewChannelMessage") &&
          !this.has(message);
        this.addMessages([message]);
        this.touchDialog(message, isNew && !message.out);
        return;
      }
      case "updateDeleteMessages":
        // Ids of the account's own box: every chat but channels.
        for (const key of this.state.messages.keys()) {
          if (!key.startsWith("ch")) this.removeMessages(key, update.messages);
        }
        return;
      case "updateDeleteChannelMessages":
        this.removeMessages(`ch${update.channel_id}`, update.messages);
        return;
      case "updateReadHistoryInbox":
        this.upsertDialog(update.peer, (d) => ({
          ...d,
          readInbox: update.max_id,
          unread: update.still_unread_count,
        }));
        return;
      case "updateReadHistoryOutbox":
        // It can arrive before the result of the send it reads, so the dialog may be new.
        this.upsertDialog(update.peer, (d) => ({
          ...d,
          readOutbox: Math.max(d.readOutbox, update.max_id),
        }));
        return;
      case "updateReadChannelInbox":
        this.upsertDialog({ _: "peerChannel", channel_id: update.channel_id }, (d) => ({
          ...d,
          readInbox: update.max_id,
          unread: update.still_unread_count,
        }));
        return;
      case "updateReadChannelOutbox":
        this.upsertDialog({ _: "peerChannel", channel_id: update.channel_id }, (d) => ({
          ...d,
          readOutbox: Math.max(d.readOutbox, update.max_id),
        }));
        return;
      case "updatePinnedMessages":
        this.setPinned(peerKey(update.peer), update.messages, update.pinned === true);
        return;
      case "updatePinnedChannelMessages":
        this.setPinned(`ch${update.channel_id}`, update.messages, update.pinned === true);
        return;
      case "updateChannel": {
        // Joined (or promoted, restricted…): make sure the channel is listed.
        const key = `ch${update.channel_id}`;
        const chat = this.state.chats.get(key);
        if (chat?._ === "channel" && !chat.left) {
          this.ensureDialog(key, { _: "peerChannel", channel_id: update.channel_id });
        }
        return;
      }
      default:
        return;
    }
  }

  private has(message: AnyMessage) {
    return this.state.messages.get(peerKey(message.peer_id))?.has(message.id) ?? false;
  }

  private addMessages(messages: readonly tl.Message[]) {
    const next = new Map(this.state.messages);
    for (const message of messages) {
      if (message._ === "messageEmpty") continue;
      const key = peerKey(message.peer_id);
      const list = new Map(next.get(key));
      list.set(message.id, message);
      next.set(key, list);
    }
    this.commit({ messages: next });
  }

  private removeMessages(key: string, ids: readonly number[]) {
    const list = this.state.messages.get(key);
    if (!list || !ids.some((id) => list.has(id))) return;
    const gone = new Set(ids);
    const next = new Map(this.state.messages);
    next.set(key, new Map([...list].filter(([id]) => !gone.has(id))));
    this.commit({ messages: next });
    this.refreshTop(key);
  }

  private setPinned(key: string, ids: readonly number[], pinned: boolean) {
    const list = this.state.messages.get(key);
    if (!list) return;
    const next = new Map(list);
    for (const id of ids) {
      const message = next.get(id);
      if (message?._ === "message") next.set(id, { ...message, pinned: pinned || undefined });
    }
    const messages = new Map(this.state.messages);
    messages.set(key, next);
    this.commit({ messages });
  }

  /** A new or edited message moves its dialog to the top (or creates it). */
  private touchDialog(message: AnyMessage, unread: boolean) {
    const key = peerKey(message.peer_id);
    const existing = this.state.dialogs.find((d) => d.key === key);
    const base: Dialog = existing ?? emptyDialog(key, message.peer_id);
    const newer = message.id >= base.top;
    const dialog: Dialog = {
      ...base,
      top: newer ? message.id : base.top,
      date: newer ? message.date : base.date,
      unread: base.unread + (unread ? 1 : 0),
    };
    this.commit({
      dialogs: sortDialogs([dialog, ...this.state.dialogs.filter((d) => d.key !== key)]),
    });
  }

  private refreshTop(key: string) {
    const ids = [...(this.state.messages.get(key)?.keys() ?? [])];
    const top = ids.length ? Math.max(...ids) : 0;
    const message = this.state.messages.get(key)?.get(top);
    this.updateDialog(key, (d) => ({ ...d, top, date: message?.date ?? d.date }));
  }

  private upsertDialog(peer: tl.Peer, change: (dialog: Dialog) => Dialog) {
    const key = peerKey(peer);
    if (!this.state.dialogs.some((d) => d.key === key)) {
      this.commit({ dialogs: [...this.state.dialogs, emptyDialog(key, peer)] });
    }
    this.updateDialog(key, change);
  }

  private updateDialog(key: string, change: (dialog: Dialog) => Dialog) {
    this.commit({ dialogs: this.state.dialogs.map((d) => (d.key === key ? change(d) : d)) });
  }

  private commit(change: Partial<Snapshot>) {
    this.state = { ...this.state, ...change };
    for (const listener of this.listeners) listener();
  }
}

/** The dialog key of a group or channel object. */
export function chatKey(chat: tl.Chat): string | undefined {
  switch (chat._) {
    case "chat":
    case "chatForbidden":
      return `c${chat.id}`;
    case "channel":
    case "channelForbidden":
      return `ch${chat.id}`;
    default:
      return undefined;
  }
}

function emptyDialog(key: string, peer: tl.Peer): Dialog {
  return { key, peer, top: 0, date: 0, unread: 0, readInbox: 0, readOutbox: 0 };
}

function sortDialogs(dialogs: Dialog[]): Dialog[] {
  return [...dialogs].sort((a, b) => b.date - a.date || b.top - a.top);
}
