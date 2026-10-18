/**
 * The client's actions: the chat list, opening chats, sending, buttons, reading, and
 * basic groups. Every result that carries updates goes through the store, like pushed
 * updates do.
 */
import type { tl } from "@simula/schema";

import { type Params, Session } from "../api/session";
import { parseFormatting } from "./markdown";
import { inputPeerFor, inputUser, peerKey, userPeer } from "./peers";
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

  /** The newest messages of a chat, plus what its header needs (members, commands). */
  async openChat(key: string) {
    const peer = this.peer(key);
    if (!peer) return;
    this.store.addHistory(await this.session.call("messages.getHistory", history(peer, 0)));
    if (key.startsWith("c")) {
      await this.loadGroup(key).catch(() => undefined);
    } else {
      await this.loadCommands(key);
    }
    await this.read(key);
  }

  /** Messages older than `before`; resolves with how many came (0 = the beginning). */
  async loadOlder(key: string, before: number): Promise<number> {
    const peer = this.peer(key);
    if (!peer) return 0;
    const result = await this.session.call("messages.getHistory", {
      ...history(peer, before),
      limit: 50,
    });
    this.store.addHistory(result);
    return "messages" in result ? result.messages.length : 0;
  }

  async read(key: string) {
    const dialog = this.store.snapshot().dialogs.find((d) => d.key === key);
    const peer = this.peer(key);
    if (!dialog || !peer || dialog.unread === 0) return;
    await this.session.call("messages.readHistory", { peer, max_id: dialog.top });
    this.store.markRead(key, dialog.top);
  }

  async send(key: string, outgoing: Outgoing) {
    const peer = this.peer(key);
    if (!peer) return;
    const { text, entities } = parseFormatting(outgoing.text);
    const params: Params<"messages.sendMessage"> = { peer, message: text, random_id: randomId() };
    if (entities.length > 0) params.entities = entities;
    if (outgoing.replyTo)
      params.reply_to = { _: "inputReplyToMessage", reply_to_msg_id: outgoing.replyTo };
    this.store.applyUpdates(await this.session.call("messages.sendMessage", params));
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
    const peer = this.peer(key);
    if (!peer) return Promise.reject(new Error("unknown chat"));
    return this.session.call("messages.getBotCallbackAnswer", { peer, msg_id: messageId, data });
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

  async editMessage(key: string, id: number, input: string) {
    const peer = this.peer(key);
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

  /** Find `@username` and open (or start) the chat with it. Returns the dialog key. */
  async resolve(username: string): Promise<string> {
    const resolved = await this.session.call("contacts.resolveUsername", {
      username: username.replace(/^@/, ""),
    });
    this.store.addUsers(resolved.users);
    return peerKey(resolved.peer);
  }

  // Basic groups

  /** Create a group with the people named by `usernames`; returns its key. */
  async createGroup(title: string, usernames: readonly string[]): Promise<string> {
    const users: tl.InputUser[] = [];
    for (const username of usernames) {
      const key = await this.resolve(username);
      const user = this.userFor(key);
      if (user) users.push(inputUser(user, this.me));
    }
    const result = await this.session.call("messages.createChat", { users, title });
    this.store.applyUpdates(result.updates);
    const created =
      "chats" in result.updates ? result.updates.chats.find((c) => c._ === "chat") : undefined;
    if (!created) throw new Error("the group wasn't created");
    return `c${created.id}`;
  }

  async addMember(key: string, username: string) {
    const user = this.userFor(await this.resolve(username));
    if (!user) return;
    const result = await this.session.call("messages.addChatUser", {
      chat_id: key.slice(1),
      user_id: inputUser(user, this.me),
      fwd_limit: 100,
    });
    this.store.applyUpdates(result.updates);
    await this.loadGroup(key);
  }

  async leave(key: string) {
    this.store.applyUpdates(
      await this.session.call("messages.deleteChatUser", {
        chat_id: key.slice(1),
        user_id: { _: "inputUserSelf" },
      }),
    );
  }

  async rename(key: string, title: string) {
    this.store.applyUpdates(
      await this.session.call("messages.editChatTitle", { chat_id: key.slice(1), title }),
    );
  }

  /** Members of a group, and the commands of the bots in it. */
  async loadGroup(key: string) {
    const full = await this.session.call("messages.getFullChat", { chat_id: key.slice(1) });
    this.store.addUsers(full.users);
    this.store.addChats(full.chats);
    if (full.full_chat._ !== "chatFull") return;
    if (full.full_chat.participants._ === "chatParticipants") {
      this.store.setMembers(key.slice(1), full.full_chat.participants.participants);
    }
    for (const info of full.full_chat.bot_info ?? []) {
      if (info._ !== "botInfo" || info.user_id === undefined) continue;
      this.store.setCommands(
        info.user_id,
        (info.commands ?? []).filter((c): c is tl.botCommand => c._ === "botCommand"),
      );
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

  private peer(key: string): tl.InputPeer | undefined {
    return inputPeerFor(key, this.store.snapshot().users, this.me);
  }

  userFor(key: string): tl.user | undefined {
    const id = key.startsWith("u") ? key.slice(1) : undefined;
    return id ? this.store.snapshot().users.get(id) : undefined;
  }

  keyFor(userId: string): string {
    return peerKey(userPeer(userId));
  }
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

/** A random positive 63-bit id, as a decimal string (JSON-TL `long`). */
function randomId(): string {
  const words = crypto.getRandomValues(new Uint32Array(2));
  return ((BigInt(words[0] & 0x7fffffff) << 32n) | BigInt(words[1])).toString();
}
