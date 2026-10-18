/**
 * What the client knows: users, the chat list and the messages loaded per chat. It is
 * filled from call results and kept current by the updates the session pushes, the
 * way a Telegram client applies `updateNewMessage`, `updateEditMessage`,
 * `updateDeleteMessages` and the read-history updates.
 */
import type { tl } from "@simula/schema";

import { peerKey } from "./peers";

export interface Dialog {
  key: string;
  peer: tl.Peer;
  top: number;
  date: number;
  unread: number;
  readInbox: number;
  readOutbox: number;
}

export interface Snapshot {
  me?: tl.user;
  users: ReadonlyMap<string, tl.user>;
  dialogs: readonly Dialog[];
  /** Per dialog key, messages by id. */
  messages: ReadonlyMap<string, ReadonlyMap<number, tl.message>>;
  /** Bot commands per bot user id, for the commands menu. */
  commands: ReadonlyMap<string, readonly tl.botCommand[]>;
}

export class Store {
  private state: Snapshot = {
    users: new Map(),
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

  setCommands(bot: string, commands: readonly tl.botCommand[]) {
    const next = new Map(this.state.commands);
    next.set(bot, commands);
    this.commit({ commands: next });
  }

  /** `messages.getDialogs`. */
  setDialogs(result: tl.messages.Dialogs) {
    if (result._ === "messages.dialogsNotModified") return;
    this.addUsers(result.users);
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
    this.addMessages(result.messages);
  }

  /** Every `Updates` the client gets, pushed or as a call's result. */
  applyUpdates(updates: tl.Updates) {
    if (updates._ !== "updates" && updates._ !== "updatesCombined") return;
    this.addUsers(updates.users);
    for (const update of updates.updates) this.applyUpdate(update);
  }

  markRead(key: string, maxId: number) {
    this.updateDialog(key, (d) => ({ ...d, readInbox: Math.max(d.readInbox, maxId), unread: 0 }));
  }

  private applyUpdate(update: tl.Update) {
    switch (update._) {
      case "updateNewMessage":
      case "updateEditMessage": {
        if (update.message._ !== "message") return;
        const message = update.message;
        const isNew = update._ === "updateNewMessage" && !this.has(message);
        this.addMessages([message]);
        this.touchDialog(message, isNew && !message.out);
        return;
      }
      case "updateDeleteMessages": {
        const gone = new Set(update.messages);
        const next = new Map(this.state.messages);
        const touched: string[] = [];
        for (const [key, list] of this.state.messages) {
          if (![...list.keys()].some((id) => gone.has(id))) continue;
          next.set(key, new Map([...list].filter(([id]) => !gone.has(id))));
          touched.push(key);
        }
        this.commit({ messages: next });
        for (const key of touched) this.refreshTop(key);
        return;
      }
      case "updateReadHistoryInbox":
        this.updateDialog(peerKey(update.peer), (d) => ({
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
      default:
        return;
    }
  }

  private has(message: tl.message) {
    return this.state.messages.get(peerKey(message.peer_id))?.has(message.id) ?? false;
  }

  private addMessages(messages: readonly tl.Message[]) {
    const next = new Map(this.state.messages);
    for (const message of messages) {
      if (message._ !== "message") continue;
      const key = peerKey(message.peer_id);
      const list = new Map(next.get(key));
      list.set(message.id, message);
      next.set(key, list);
    }
    this.commit({ messages: next });
  }

  /** A new or edited message moves its dialog to the top (or creates it). */
  private touchDialog(message: tl.message, unread: boolean) {
    const key = peerKey(message.peer_id);
    const existing = this.state.dialogs.find((d) => d.key === key);
    const base: Dialog = existing ?? {
      key,
      peer: message.peer_id,
      top: 0,
      date: 0,
      unread: 0,
      readInbox: 0,
      readOutbox: 0,
    };
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
      const empty: Dialog = { key, peer, top: 0, date: 0, unread: 0, readInbox: 0, readOutbox: 0 };
      this.commit({ dialogs: [...this.state.dialogs, empty] });
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

function sortDialogs(dialogs: Dialog[]): Dialog[] {
  return [...dialogs].sort((a, b) => b.date - a.date || b.top - a.top);
}
