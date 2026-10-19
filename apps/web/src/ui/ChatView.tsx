/**
 * An open chat: header (with group and channel actions), the pinned message, messages,
 * the bot's keyboard and the composer — or a bar when the account can't write here.
 */
import type { tl } from "@simula/schema";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { Client } from "../state/client";
import type { DeepLink } from "../state/links";
import { displayName } from "../state/peers";
import type { AnyChat, AnyMessage, ChatDetails, Snapshot } from "../state/store";
import { GENERAL, isForum, topicOf, topicRights } from "../state/topics";
import { chatTitle } from "./ChatChooser";
import { Composer, type Draft } from "./Composer";
import { Message, type MessageActions } from "./Message";
import { activeMarkup, ReplyKeyboard } from "./ReplyKeyboard";
import { ServiceMessage } from "./ServiceMessage";
import { TopicList } from "./TopicList";

export type Notify = (text: string, options?: { alert?: boolean }) => void;

interface Props {
  client: Client;
  snapshot: Snapshot;
  chatKey: string;
  notify: Notify;
  onOpenChat: (key: string) => void;
  onDeepLink: (link: DeepLink) => void;
  /** A deep link's `start` parameter waiting for the START button. */
  startParam?: string;
  /** A message of another chat (`from`) to reply to here. */
  externalReply?: { from: string; message: tl.message };
  onReplyElsewhere: (message: tl.message) => void;
  onExternalReplyDone: () => void;
  onStarted: () => void;
  onBack: () => void;
}

export function ChatView({
  client,
  snapshot,
  chatKey,
  notify,
  onOpenChat,
  onDeepLink,
  startParam,
  externalReply,
  onReplyElsewhere,
  onExternalReplyDone,
  onStarted,
  onBack,
}: Props) {
  const user = client.userFor(chatKey);
  const group = chatKey.startsWith("c") ? snapshot.chats.get(chatKey) : undefined;
  const details = snapshot.details.get(chatKey);
  const rights = chatRights(group);
  const dialog = snapshot.dialogs.find((d) => d.key === chatKey);
  const byId = snapshot.messages.get(chatKey);
  // Forums show their topics first; an open topic shows only its messages.
  const forum = isForum(group);
  const [topic, setTopic] = useState<number | undefined>();
  const topics = snapshot.topics.get(chatKey) ?? [];
  const openTopic = forum ? topics.find((t) => t.id === topic) : undefined;
  const topicVersion = snapshot.topicsVersion.get(chatKey) ?? 0;
  const all = useMemo(
    () =>
      [...(byId?.values() ?? [])]
        .filter((m) => !forum || topic === undefined || topicOf(m) === topic)
        .sort((a, b) => a.id - b.id),
    [byId, forum, topic],
  );
  const messages = useMemo(() => all.filter(isRegular), [all]);
  const [draft, setDraft] = useState<Draft>({});
  const [dismissed, setDismissed] = useState<Set<number>>(new Set());
  const [keyboardShown, setKeyboardShown] = useState(true);
  const list = useRef<HTMLDivElement>(null);
  const [loaded, setLoaded] = useState(false);
  const [reachedStart, setReachedStart] = useState(false);
  const loadingOlder = useRef(false);
  /** Scroll position to restore once older messages are rendered above. */
  const anchor = useRef<{ height: number; top: number } | null>(null);

  useEffect(() => {
    setDraft({});
    setLoaded(false);
    setReachedStart(false);
    setTopic(undefined);
    client
      .openChat(chatKey)
      .then(() => setLoaded(true))
      .catch((error: Error) => notify(error.message));
  }, [client, chatKey, notify]);

  // A forum's topics while they are listed, again whenever they or their messages change.
  const listing = forum && topic === undefined;
  const top = dialog?.top;
  useEffect(() => {
    if (listing && topicVersion >= 0 && top !== -1)
      client.loadTopics(chatKey).catch(() => undefined);
  }, [client, chatKey, listing, topicVersion, top]);

  useEffect(() => {
    if (!forum || topic === undefined) return;
    setDraft({});
    setReachedStart(false);
    client.openTopic(chatKey, topic).catch((error: Error) => notify(error.message));
  }, [client, chatKey, forum, topic, notify]);

  // Older messages appear above without moving what the reader is looking at.
  const oldest = all[0]?.id;
  useLayoutEffect(() => {
    const element = list.current;
    if (oldest === undefined || !element || !anchor.current) return;
    element.scrollTop = element.scrollHeight - anchor.current.height + anchor.current.top;
    anchor.current = null;
  }, [oldest]);

  const onScroll = () => {
    const element = list.current;
    if (
      !element ||
      element.scrollTop > 80 ||
      loadingOlder.current ||
      reachedStart ||
      oldest === undefined
    )
      return;
    loadingOlder.current = true;
    anchor.current = { height: element.scrollHeight, top: element.scrollTop };
    client
      .loadOlder(chatKey, oldest, forum ? topic : undefined)
      .then((count) => {
        if (count === 0) setReachedStart(true);
      })
      .catch((error: Error) => notify(error.message))
      .finally(() => {
        loadingOlder.current = false;
      });
  };

  // Read what arrives while the chat is open.
  useEffect(() => {
    if (dialog && dialog.unread > 0) void client.read(chatKey);
  }, [client, chatKey, dialog]);

  // Follow the conversation: scroll down when a newer message arrives, and once the
  // opened chat's history is in (it loads above the message the chat list showed).
  const newest = all.at(-1)?.id;
  useEffect(() => {
    if (loaded && newest !== undefined) list.current?.scrollTo({ top: list.current.scrollHeight });
  }, [loaded, newest]);

  const markup = activeMarkup(messages);
  const keyboard =
    markup?.kind === "keyboard" && !dismissed.has(markup.message) ? markup : undefined;
  // A new keyboard from the bot shows up even if the last one was hidden.
  const markupMessage = markup?.message;
  useEffect(() => {
    if (markupMessage !== undefined) setKeyboardShown(true);
  }, [markupMessage]);
  // A forced reply answers the message that asked for it.
  const forced = markup?.kind === "force_reply" ? markup : undefined;
  const forcedMessage = forced && !draft.editing ? byId?.get(forced.message) : undefined;
  const replyTo =
    draft.replyTo ?? (forcedMessage && isRegular(forcedMessage) ? forcedMessage : undefined);
  const placeholder = keyboard?.markup.placeholder ?? forced?.markup.placeholder;

  const external = draft.editing || draft.replyTo ? undefined : externalReply;
  const send = async (text: string, fromKeyboard = false) => {
    try {
      if (draft.editing) await client.editMessage(chatKey, draft.editing.id, text);
      else if (external) {
        await client.send(chatKey, {
          text,
          replyTo: external.message.id,
          replyIn: external.from,
          topic,
        });
        onExternalReplyDone();
      } else await client.send(chatKey, { text, replyTo: replyTo?.id, topic });
      if (fromKeyboard && keyboard?.markup.single_use)
        setDismissed(new Set(dismissed).add(keyboard.message));
      setDraft({});
    } catch (error) {
      notify((error as Error).message);
    }
  };

  const actionsFor = (message: tl.message): MessageActions => ({
    onReply: () => setDraft({ replyTo: message }),
    onReplyElsewhere: message.noforwards ? undefined : () => onReplyElsewhere(message),
    onOpenReplied: externalSource(message, snapshot)
      ? () => {
          const source = externalSource(message, snapshot);
          if (source) onOpenChat(source);
        }
      : undefined,
    onEdit: message.out || rights.editsPosts ? () => setDraft({ editing: message }) : undefined,
    onDelete:
      !chatKey.startsWith("ch") || message.out || rights.deletes
        ? () => {
            // Supergroups and channels have one box: deleting is always for everyone.
            const forEveryone =
              chatKey.startsWith("ch") ||
              window.confirm("Delete this message for everyone? (Cancel deletes it only for you.)");
            client
              .deleteMessage(chatKey, message.id, forEveryone)
              .catch((error: Error) => notify(error.message));
          }
        : undefined,
    onPin:
      !group || rights.pins
        ? () =>
            client
              .pin(chatKey, message.id, message.pinned === true)
              .catch((error: Error) => notify(error.message))
        : undefined,
    onCommand: (command) => void send(command),
    onMention: (username) =>
      client
        .resolve(username)
        .then(onOpenChat)
        .catch((error: Error) => notify(error.message)),
    onUser: (id) => onOpenChat(client.keyFor(id)),
    onDeepLink,
    onCallback: (data) =>
      client
        .press(chatKey, message.id, data)
        .then((answer) => {
          if (answer.url) {
            if (window.confirm(`Open ${answer.url}?`))
              window.open(answer.url, "_blank", "noopener");
          } else if (answer.message) {
            notify(answer.message, { alert: answer.alert });
          }
        })
        .catch((error: Error) =>
          notify(
            error.message === "BOT_RESPONSE_TIMEOUT" ? "The bot didn't answer" : error.message,
          ),
        ),
    onNotice: (text) => notify(text),
  });

  const commands = user?.bot
    ? (snapshot.commands.get(user.id) ?? [])
    : group
      ? groupCommands(details, snapshot)
      : [];
  // Telegram offers START for a bot chat with no messages yet, or after a deep link.
  const needsStart = user?.bot === true && loaded && (startParam !== undefined || all.length === 0);

  const run = (action: Promise<unknown>) => action.catch((error: Error) => notify(error.message));
  const addMember = () => {
    const username = window.prompt("Add a member by @username")?.trim();
    if (username) void run(client.addMember(chatKey, username));
  };
  const rename = () => {
    const title = window.prompt("Group name", group?.title)?.trim();
    if (title && title !== group?.title) void run(client.rename(chatKey, title));
  };
  const upgrade = () => {
    const confirmed = window.confirm(
      `Upgrade «${group?.title}» to a supergroup? Everyone moves along; this can't be undone.`,
    );
    if (confirmed) void run(client.upgrade(chatKey).then(onOpenChat));
  };
  const leave = () => {
    if (window.confirm(`Leave «${group?.title}»?`)) void run(client.leave(chatKey));
  };
  const promote = () => {
    const username = window.prompt("Make an admin (every right) by @username")?.trim();
    if (username) void run(client.promoteByName(chatKey, username));
  };
  const inviteLink = () =>
    void run(
      client.inviteLink(chatKey).then((link) => {
        void navigator.clipboard?.writeText(link).catch(() => undefined);
        notify(`Invite link (copied): ${link}`, { alert: true });
      }),
    );
  const title = openTopic ? openTopic.title : group ? group.title : displayName(user);
  const subtitle = openTopic
    ? `topic in ${group?.title}`
    : group
      ? groupSubtitle(group, details)
      : user?.bot
        ? "bot"
        : user?.username
          ? `@${user.username}`
          : "";
  const pinned = messages.filter((m) => m.pinned).at(-1);
  const topicManage = topicRights(group);
  const ownTopic = openTopic?.my === true && openTopic.id !== GENERAL;
  const blocked = group
    ? (blockedReason(group, rights) ??
      (openTopic?.closed && !topicManage.manage && !ownTopic ? "This topic is closed" : undefined))
    : undefined;
  const topicActions = openTopic && {
    rename: () => {
      const next = window.prompt("Topic name", openTopic.title)?.trim();
      if (next && next !== openTopic.title)
        void run(client.editTopic(chatKey, openTopic.id, { title: next }));
    },
    close: () => void run(client.editTopic(chatKey, openTopic.id, { closed: !openTopic.closed })),
    hide: () => void run(client.editTopic(chatKey, openTopic.id, { hidden: !openTopic.hidden })),
    pin: () => void run(client.pinTopic(chatKey, openTopic.id, !openTopic.pinned)),
    remove: () => {
      if (window.confirm(`Delete the topic «${openTopic.title}» with all its messages?`))
        void run(client.deleteTopic(chatKey, openTopic.id).then(() => setTopic(undefined)));
    },
  };
  const toggleForum = () => {
    const turnOn = !forum;
    const question = turnOn
      ? "Turn on topics? Existing messages move to General."
      : "Turn off topics?";
    if (window.confirm(question)) void run(client.toggleForum(chatKey, turnOn));
  };
  const upgradedTo =
    group?._ === "chat" && group.migrated_to?._ === "inputChannel"
      ? `ch${group.migrated_to.channel_id}`
      : undefined;

  return (
    <section className="chat">
      <header className="chat-header">
        <button
          type="button"
          className={openTopic ? "icon-button" : "icon-button back"}
          onClick={openTopic ? () => setTopic(undefined) : onBack}
          title={openTopic ? "Topics" : "Back"}
        >
          ←
        </button>
        <div className={group ? `avatar ${rights.broadcast ? "channel" : "group"}` : "avatar"}>
          {initialsOf(title)}
        </div>
        <div className="chat-title">
          <span dir="auto">{title}</span>
          <span className="chat-subtitle">{subtitle}</span>
        </div>
        {topicActions && (
          <div className="chat-actions">
            {(topicManage.manage || ownTopic) && (
              <button
                type="button"
                className="icon-button"
                onClick={topicActions.rename}
                title="Rename topic"
              >
                ✎
              </button>
            )}
            {(topicManage.manage || ownTopic) && (
              <button
                type="button"
                className="icon-button"
                onClick={topicActions.close}
                title={openTopic.closed ? "Reopen topic" : "Close topic"}
              >
                {openTopic.closed ? "🔓" : "🔒"}
              </button>
            )}
            {topicManage.manage && openTopic.id === GENERAL && (
              <button
                type="button"
                className="icon-button"
                onClick={topicActions.hide}
                title={openTopic.hidden ? "Show General" : "Hide General"}
              >
                {openTopic.hidden ? "◉" : "◌"}
              </button>
            )}
            {topicManage.manage && (
              <button
                type="button"
                className="icon-button"
                onClick={topicActions.pin}
                title={openTopic.pinned ? "Unpin topic" : "Pin topic"}
              >
                📌
              </button>
            )}
            {(rights.deletes || ownTopic) && openTopic.id !== GENERAL && (
              <button
                type="button"
                className="icon-button"
                onClick={topicActions.remove}
                title="Delete topic"
              >
                🗑
              </button>
            )}
          </div>
        )}
        {rights.member && !topicActions && (
          <div className="chat-actions">
            {group?._ === "channel" && group.creator && !group.broadcast && (
              <button
                type="button"
                className={forum ? "icon-button active" : "icon-button"}
                onClick={toggleForum}
                title={forum ? "Turn off topics" : "Turn on topics"}
              >
                ☰
              </button>
            )}
            {rights.invites && (
              <button type="button" className="icon-button" onClick={addMember} title="Add member">
                ＋
              </button>
            )}
            {rights.promotes && (
              <button type="button" className="icon-button" onClick={promote} title="Add admin">
                ★
              </button>
            )}
            {rights.links && (
              <button
                type="button"
                className="icon-button"
                onClick={inviteLink}
                title="Invite link"
              >
                🔗
              </button>
            )}
            {rights.changesInfo && (
              <button type="button" className="icon-button" onClick={rename} title="Rename">
                ✎
              </button>
            )}
            {group?._ === "chat" && group.creator && (
              <button
                type="button"
                className="icon-button"
                onClick={upgrade}
                title="Upgrade to supergroup"
              >
                ⇪
              </button>
            )}
            <button type="button" className="icon-button" onClick={leave} title="Leave">
              ⎋
            </button>
          </div>
        )}
      </header>
      {pinned && (
        <div className="pinned-bar">
          <button
            type="button"
            className="pinned-message"
            onClick={() =>
              list.current
                ?.querySelector(`[data-id="${pinned.id}"]`)
                ?.scrollIntoView({ block: "center", behavior: "smooth" })
            }
          >
            <span className="pinned-label">Pinned message</span>
            <span className="pinned-text" dir="auto">
              {pinned.message}
            </span>
          </button>
          {(!group || rights.pins) && (
            <button
              type="button"
              className="icon-button"
              title="Unpin"
              onClick={() => void run(client.pin(chatKey, pinned.id, true))}
            >
              ✕
            </button>
          )}
        </div>
      )}
      {forum && topic === undefined ? (
        <TopicList
          topics={topics}
          messages={byId}
          users={snapshot.users}
          me={snapshot.me?.id}
          canCreate={rights.member && topicManage.create}
          onOpen={setTopic}
          onCreate={(name) => client.createTopic(chatKey, name).then(setTopic)}
        />
      ) : (
        <>
          <div className="messages" ref={list} onScroll={onScroll}>
            {reachedStart && all.length > 0 && !details?.migratedFrom && (
              <p className="history-start">Beginning of the chat</p>
            )}
            {details?.migratedFrom && (reachedStart || oldest === 1) && (
              <button
                type="button"
                className="history-start link"
                onClick={() => details.migratedFrom && onOpenChat(details.migratedFrom)}
              >
                History from before the upgrade
              </button>
            )}
            {all.map((message) =>
              isRegular(message) ? (
                <Message
                  key={message.id}
                  message={message}
                  replied={repliedTo(message, byId)}
                  users={snapshot.users}
                  readOutbox={dialog?.readOutbox ?? 0}
                  showSender={group !== undefined && !rights.broadcast}
                  hideReply={forum && repliesToTopic(message)}
                  externalOrigin={externalOrigin(message, snapshot)}
                  actions={actionsFor(message)}
                />
              ) : (
                <ServiceMessage
                  key={message.id}
                  message={message}
                  users={snapshot.users}
                  me={snapshot.me?.id}
                  replied={repliedTo(message, byId)}
                  onOpen={(id) =>
                    list.current
                      ?.querySelector(`[data-id="${id}"]`)
                      ?.scrollIntoView({ block: "center", behavior: "smooth" })
                  }
                />
              ),
            )}
          </div>
          {blocked ? (
            <div className="start-bar">
              {blocked === "upgraded" ? (
                <button type="button" onClick={() => upgradedTo && onOpenChat(upgradedTo)}>
                  OPEN THE SUPERGROUP
                </button>
              ) : blocked === "join" ? (
                <button type="button" onClick={() => void run(client.join(chatKey))}>
                  {rights.broadcast ? "JOIN CHANNEL" : "JOIN GROUP"}
                </button>
              ) : (
                <span>{blocked}</span>
              )}
            </div>
          ) : needsStart ? (
            <div className="start-bar">
              <button
                type="button"
                onClick={() =>
                  client
                    .startBot(chatKey, startParam)
                    .then(onStarted)
                    .catch((error: Error) => notify(error.message))
                }
              >
                START
              </button>
            </div>
          ) : (
            <Composer
              draft={{
                ...draft,
                replyTo,
                external: external && {
                  chat: chatTitle(snapshot, external.from) ?? "another chat",
                  message: external.message,
                },
              }}
              placeholder={placeholder}
              commands={commands}
              keyboardAvailable={keyboard !== undefined}
              keyboardShown={keyboardShown}
              onToggleKeyboard={() => setKeyboardShown(!keyboardShown)}
              onCancel={() => {
                setDraft({});
                if (external) onExternalReplyDone();
              }}
              onSend={(text) => void send(text)}
            />
          )}
          {keyboard && keyboardShown && (
            <ReplyKeyboard
              markup={keyboard.markup}
              onText={(text) => void send(text, true)}
              onNotice={(text) => notify(text)}
            />
          )}
        </>
      )}
    </section>
  );
}

/** Where a reply to another chat's message came from: the channel, or the author. */
function externalOrigin(message: tl.message, snapshot: Snapshot): string | undefined {
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  const from = header?.reply_from;
  if (!from) return undefined;
  const origin = from.from_id;
  if (origin?._ === "peerUser") return displayName(snapshot.users.get(origin.user_id));
  if (origin?._ === "peerChannel")
    return chatTitle(snapshot, `ch${origin.channel_id}`) ?? "a channel";
  return from.from_name ?? "Hidden sender";
}

/** The supergroup or channel a reply to another chat's message points into, if known. */
function externalSource(message: tl.message, snapshot: Snapshot): string | undefined {
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  const peer = header?.reply_from ? header.reply_to_peer_id : undefined;
  const key = peer?._ === "peerChannel" ? `ch${peer.channel_id}` : undefined;
  const chat = key ? snapshot.chats.get(key) : undefined;
  return chat?._ === "channel" && (!chat.left || chat.username) ? key : undefined;
}

/** A forum message that only names its topic (it replies to the topic's creation). */
function repliesToTopic(message: tl.message): boolean {
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  return header?.forum_topic === true && header.reply_to_top_id === undefined;
}

export function isRegular(message: AnyMessage): message is tl.message {
  return message._ === "message";
}

function repliedTo(message: AnyMessage, byId: ReadonlyMap<number, AnyMessage> | undefined) {
  const header = message.reply_to?._ === "messageReplyHeader" ? message.reply_to : undefined;
  const sameChat = header?.reply_from === undefined && header?.reply_to_peer_id === undefined;
  const replied =
    sameChat && header?.reply_to_msg_id !== undefined
      ? byId?.get(header.reply_to_msg_id)
      : undefined;
  return replied && isRegular(replied) ? replied : undefined;
}

/** The commands of every bot in the group, as `/command@bot` like Telegram shows them. */
function groupCommands(details: ChatDetails | undefined, snapshot: Snapshot): tl.botCommand[] {
  return (details?.bots ?? []).flatMap((id) => {
    const bot = snapshot.users.get(id);
    if (!bot?.bot) return [];
    return (snapshot.commands.get(bot.id) ?? []).map((c) => ({
      ...c,
      command: `${c.command}@${bot.username}`,
    }));
  });
}

/** What the account may do in a group or channel, from the chat object it was given. */
interface Rights {
  member: boolean;
  broadcast: boolean;
  /** May post (channels: admins with post_messages). */
  posts: boolean;
  editsPosts: boolean;
  /** Supergroups and channels: may delete anyone's messages. */
  deletes: boolean;
  pins: boolean;
  invites: boolean;
  links: boolean;
  promotes: boolean;
  changesInfo: boolean;
}

function chatRights(chat: AnyChat | undefined): Rights {
  const none: Rights = {
    member: false,
    broadcast: false,
    posts: false,
    editsPosts: false,
    deletes: false,
    pins: false,
    invites: false,
    links: false,
    promotes: false,
    changesInfo: false,
  };
  if (chat?._ === "chat") {
    const admin = chat.creator === true || chat.admin_rights !== undefined;
    const defaults = chat.default_banned_rights;
    return {
      ...none,
      member: !chat.left && !chat.deactivated,
      posts: !chat.left,
      pins: admin || !defaults?.pin_messages,
      invites: true,
      links: admin,
      changesInfo: admin || !defaults?.change_info,
    };
  }
  if (chat?._ === "channel") {
    const creator = chat.creator === true;
    const rights = chat.admin_rights;
    const has = (right: keyof tl.chatAdminRights) => creator || rights?.[right] === true;
    const defaults = chat.default_banned_rights;
    const banned = chat.banned_rights;
    const memberMay = (right: keyof tl.chatBannedRights) =>
      !chat.broadcast && !defaults?.[right] && !banned?.[right];
    return {
      member: !chat.left,
      broadcast: chat.broadcast === true,
      posts: chat.broadcast ? has("post_messages") : !banned?.send_plain && !defaults?.send_plain,
      editsPosts: chat.broadcast === true && has("edit_messages"),
      deletes: has("delete_messages"),
      pins:
        has("pin_messages") ||
        (chat.broadcast === true && has("edit_messages")) ||
        memberMay("pin_messages"),
      invites: has("invite_users") || memberMay("invite_users"),
      links: has("invite_users"),
      promotes: has("add_admins"),
      changesInfo: has("change_info") || memberMay("change_info"),
    };
  }
  if (chat?._ === "channelForbidden") return { ...none, broadcast: chat.broadcast === true };
  return none;
}

function groupSubtitle(chat: AnyChat, details: ChatDetails | undefined): string {
  switch (chat._) {
    case "chatForbidden":
      return "you were removed";
    case "channelForbidden":
      return "you are banned";
    case "chat":
      return chat.left ? "you left" : `${details?.count ?? chat.participants_count} members`;
    case "channel": {
      const count = details?.count ?? chat.participants_count ?? 0;
      const kind = chat.broadcast ? "subscribers" : "members";
      const counted = `${count} ${kind}`;
      return chat.username ? `${counted} · @${chat.username}` : counted;
    }
  }
}

/**
 * Why the composer is replaced by a bar ("join" offers joining, "upgraded" opens the
 * supergroup), if it is.
 */
function blockedReason(chat: AnyChat, rights: Rights): string | undefined {
  if (chat._ === "chatForbidden") return "You were removed from this group";
  if (chat._ === "channelForbidden") return "You are banned here";
  if (chat._ === "chat") {
    if (chat.migrated_to) return "upgraded";
    if (chat.deactivated) return "This group was deleted";
    return chat.left ? "You left this group" : undefined;
  }
  if (chat.left) return chat.username ? "join" : "You are not a member";
  if (!rights.posts)
    return rights.broadcast ? "Only admins post in this channel" : "You can't write here";
  return undefined;
}

export function initials(user: tl.user | undefined): string {
  return initialsOf(displayName(user));
}

export function initialsOf(name: string): string {
  const words = name.split(/\s+/).filter(Boolean);
  return (
    words
      .slice(0, 2)
      .map((word) => [...word][0])
      .join("")
      .toUpperCase() || "?"
  );
}
