/**
 * The client's actions: load the chat list, open a chat, send, press buttons, read.
 * Every result that carries updates goes through the store, like pushed updates.
 */
import type { tl } from "@simula/schema";

import { type Params, Session } from "../api/session";
import { inputPeer, inputUser, peerKey, userPeer } from "./peers";
import { Store } from "./store";

export interface Outgoing {
  text: string;
  replyTo?: number;
}

export class Client {
  readonly store = new Store();
  private unsubscribe: () => void;

  private constructor(readonly session: Session) {
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

  /** The newest messages of a chat, and the bot's commands when it is a bot. */
  async openChat(key: string) {
    const user = this.userFor(key);
    if (!user) return;
    const history = await this.session.call("messages.getHistory", {
      peer: inputPeer(user, this.me),
      offset_id: 0,
      offset_date: 0,
      add_offset: 0,
      limit: 100,
      max_id: 0,
      min_id: 0,
      hash: "0",
    });
    this.store.addHistory(history);
    if (user.bot && !this.store.snapshot().commands.has(user.id)) {
      const full = await this.session.call("users.getFullUser", { id: inputUser(user, this.me) });
      const info = full.full_user._ === "userFull" ? full.full_user.bot_info : undefined;
      const commands = (info?._ === "botInfo" ? (info.commands ?? []) : []).filter(
        (c): c is tl.botCommand => c._ === "botCommand",
      );
      this.store.setCommands(user.id, commands);
    }
    await this.read(key);
  }

  async read(key: string) {
    const dialog = this.store.snapshot().dialogs.find((d) => d.key === key);
    const user = this.userFor(key);
    if (!dialog || !user || dialog.unread === 0) return;
    await this.session.call("messages.readHistory", {
      peer: inputPeer(user, this.me),
      max_id: dialog.top,
    });
    this.store.markRead(key, dialog.top);
  }

  async send(key: string, outgoing: Outgoing) {
    const user = this.userFor(key);
    if (!user) return;
    const params: Params<"messages.sendMessage"> = {
      peer: inputPeer(user, this.me),
      message: outgoing.text,
      random_id: randomId(),
    };
    if (outgoing.replyTo)
      params.reply_to = { _: "inputReplyToMessage", reply_to_msg_id: outgoing.replyTo };
    this.store.applyUpdates(await this.session.call("messages.sendMessage", params));
  }

  /** Press a callback button; resolves with the bot's answer (toast, alert or URL). */
  press(key: string, messageId: number, data: string) {
    const user = this.userFor(key);
    if (!user) return Promise.reject(new Error("unknown chat"));
    return this.session.call("messages.getBotCallbackAnswer", {
      peer: inputPeer(user, this.me),
      msg_id: messageId,
      data,
    });
  }

  async deleteMessage(id: number, revoke: boolean) {
    const result = await this.session.call("messages.deleteMessages", {
      id: [id],
      revoke: revoke || undefined,
    });
    this.store.applyUpdates({
      _: "updates",
      updates: [
        { _: "updateDeleteMessages", messages: [id], pts: result.pts, pts_count: result.pts_count },
      ],
      users: [],
      chats: [],
      date: 0,
      seq: 0,
    });
  }

  async editMessage(key: string, id: number, text: string) {
    const user = this.userFor(key);
    if (!user) return;
    this.store.applyUpdates(
      await this.session.call("messages.editMessage", {
        peer: inputPeer(user, this.me),
        id,
        message: text,
      }),
    );
  }

  /** Find `@username` and open (or start) the chat with it. Returns the dialog key. */
  async resolve(username: string): Promise<string> {
    const resolved = await this.session.call("contacts.resolveUsername", {
      username: username.replace(/^@/, ""),
    });
    this.store.addUsers(resolved.users);
    return peerKey(resolved.peer);
  }

  userFor(key: string): tl.user | undefined {
    const id = key.startsWith("u") ? key.slice(1) : undefined;
    return id ? this.store.snapshot().users.get(id) : undefined;
  }

  keyFor(userId: string): string {
    return peerKey(userPeer(userId));
  }
}

/** A random positive 63-bit id, as a decimal string (JSON-TL `long`). */
function randomId(): string {
  const words = crypto.getRandomValues(new Uint32Array(2));
  return ((BigInt(words[0] & 0x7fffffff) << 32n) | BigInt(words[1])).toString();
}
