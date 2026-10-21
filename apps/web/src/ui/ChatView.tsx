/**
 * An open chat: header (with group and channel actions), the pinned message, messages,
 * the bot's keyboard and the composer — or a bar when the account can't write here.
 */
import type { tl } from "@simula/schema";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { type Abilities, abilities } from "../state/abilities";
import type { Attachment } from "../state/attach";
import type { Client } from "../state/client";
import type { DeepLink } from "../state/links";
import { contentOf, isVisual, mediaLabel } from "../state/media";
import { displayName } from "../state/peers";
import type { AnyChat, AnyMessage, ChatDetails, Snapshot } from "../state/store";
import { GENERAL, isForum, topicColor, topicOf, topicRights } from "../state/topics";
import { chatTitle } from "./ChatChooser";
import { Composer, type Draft } from "./Composer";
import type { Screen } from "./info/nav";
import { Avatar } from "./kit/Avatar";
import { useDialogs } from "./kit/Dialogs";
import { Icon } from "./kit/Icon";
import { Menu, type MenuItem } from "./kit/Menu";
import { Message, type MessageActions } from "./Message";
import { AttachDialog } from "./media/AttachDialog";
import { MediaContext } from "./media/context";
import { Pending } from "./media/Pending";
import { Viewer } from "./media/Viewer";
import { OpenAs } from "./OpenAs";
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
  /** Open the info panel on a screen. */
  onOpenInfo: (screen: Screen) => void;
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
  onOpenInfo,
}: Props) {
  const dialogs = useDialogs();
  const [openingAs, setOpeningAs] = useState(false);
  const user = client.userFor(chatKey);
  const group = chatKey.startsWith("c") ? snapshot.chats.get(chatKey) : undefined;
  const details = snapshot.details.get(chatKey);
  const rights = abilities(group);
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

  const uploads = (snapshot.uploads.get(chatKey) ?? []).filter(
    (upload) => !forum || upload.topic === topic,
  );
  // Follow the conversation: scroll down when a newer message arrives or a file starts
  // uploading, and once the opened chat's history is in (it loads above the message the
  // chat list showed).
  const newest = all.at(-1)?.id;
  const sending = uploads.length;
  useEffect(() => {
    if (loaded && (newest !== undefined || sending > 0))
      list.current?.scrollTo({ top: list.current.scrollHeight });
  }, [loaded, newest, sending]);

  // Photos and videos open in the viewer, which pages through all of them.
  const visual = useMemo(() => messages.filter((m) => isVisual(contentOf(m))), [messages]);
  const [viewing, setViewing] = useState<number | null>(null);
  const media = useMemo(
    () => ({
      files: client.files,
      openViewer: (message: tl.message) =>
        setViewing(
          Math.max(
            0,
            visual.findIndex((m) => m.id === message.id),
          ),
        ),
    }),
    [client, visual],
  );
  const author = (message: tl.message) =>
    message.post || !message.from_id
      ? title
      : message.from_id._ === "peerUser"
        ? displayName(snapshot.users.get(message.from_id.user_id))
        : (chatTitle(
            snapshot,
            `ch${"channel_id" in message.from_id ? message.from_id.channel_id : ""}`,
          ) ?? title);

  // Files picked, dropped or pasted wait in the send dialog.
  const [attaching, setAttaching] = useState<File[] | null>(null);
  const [dragging, setDragging] = useState(false);
  const sendFiles = (attachments: Attachment[], caption: string, grouped: boolean) => {
    setAttaching(null);
    const reply = replyTo?.id;
    setDraft({});
    client
      .sendFiles(chatKey, attachments, { text: caption, replyTo: reply, topic }, grouped)
      .catch((error: Error) => notify(error.message));
  };

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

  const deleteMessages = async (targets: readonly tl.message[]) => {
    const ids = targets.map((m) => m.id);
    const title = ids.length === 1 ? "Delete message?" : `Delete ${ids.length} messages?`;
    // Supergroups and channels have one box: deleting is always for everyone.
    if (chatKey.startsWith("ch")) {
      const ok = await dialogs.confirm({
        title,
        text: "It will be deleted for everyone.",
        ok: "Delete",
        danger: true,
      });
      if (ok)
        client.deleteMessages(chatKey, ids, true).catch((error: Error) => notify(error.message));
      return;
    }
    const other = user ? displayName(user).split(" ")[0] : "everyone";
    const choice = await dialogs.choose({
      title,
      text: "This can't be undone.",
      choices: [
        { label: `Delete for me and ${other}`, value: "everyone", danger: true },
        { label: "Delete for me", value: "me", danger: true },
      ],
    });
    if (choice)
      client
        .deleteMessages(chatKey, ids, choice === "everyone")
        .catch((error: Error) => notify(error.message));
  };
  /** What a message's menu does; for an album, `message` is the one with the caption. */
  const actionsFor = (message: tl.message, album?: readonly tl.message[]): MessageActions => ({
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
            void deleteMessages(album ?? [message]);
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
            const url = answer.url;
            void dialogs
              .confirm({ title: "Open this link?", text: url, ok: "Open" })
              .then((ok) => ok && window.open(url, "_blank", "noopener"));
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
  const kind = rights.broadcast ? "channel" : "group";
  const inviteLink = () =>
    void run(
      client.inviteLink(chatKey).then((link) => {
        void navigator.clipboard?.writeText(link).catch(() => undefined);
        notify("Invite link copied to clipboard");
      }),
    );
  const leave = async () => {
    const ok = await dialogs.confirm({
      title: `Leave ${kind}?`,
      text: `Are you sure you want to leave «${group && "title" in group ? group.title : ""}»?`,
      ok: "Leave",
      danger: true,
    });
    if (ok) void run(client.leave(chatKey));
  };
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
  const canWrite = !blocked && !needsStart && !listing;
  const renameTopic = async () => {
    if (!openTopic) return;
    const next = (
      await dialogs.prompt({
        title: "Edit topic",
        label: "Topic name",
        value: openTopic.title,
        maxLength: 128,
      })
    )?.trim();
    if (next && next !== openTopic.title)
      void run(client.editTopic(chatKey, openTopic.id, { title: next }));
  };
  const deleteTopic = async () => {
    if (!openTopic) return;
    const ok = await dialogs.confirm({
      title: "Delete topic?",
      text: `«${openTopic.title}» will be deleted with all its messages.`,
      ok: "Delete",
      danger: true,
    });
    if (ok) void run(client.deleteTopic(chatKey, openTopic.id).then(() => setTopic(undefined)));
  };
  const menu: (MenuItem | false | undefined)[] = openTopic
    ? [
        (topicManage.manage || ownTopic) && {
          icon: "edit",
          label: "Edit topic",
          onSelect: renameTopic,
        },
        (topicManage.manage || ownTopic) && {
          icon: openTopic.closed ? "unlock" : "lock",
          label: openTopic.closed ? "Reopen topic" : "Close topic",
          onSelect: () =>
            void run(client.editTopic(chatKey, openTopic.id, { closed: !openTopic.closed })),
        },
        topicManage.manage && {
          icon: "pin",
          label: openTopic.pinned ? "Unpin topic" : "Pin topic",
          onSelect: () => void run(client.pinTopic(chatKey, openTopic.id, !openTopic.pinned)),
        },
        topicManage.manage &&
          openTopic.id === GENERAL && {
            icon: "info",
            label: openTopic.hidden ? "Show General" : "Hide General",
            onSelect: () =>
              void run(client.editTopic(chatKey, openTopic.id, { hidden: !openTopic.hidden })),
          },
        (rights.deletes || ownTopic) &&
          openTopic.id !== GENERAL && {
            icon: "delete",
            label: "Delete topic",
            danger: true,
            onSelect: deleteTopic,
          },
      ]
    : [
        {
          icon: "info",
          label: group ? `View ${kind} info` : user?.bot ? "View bot info" : "View profile",
          onSelect: () => onOpenInfo({ name: "info" }),
        },
        group &&
          rights.member &&
          (rights.changesInfo || rights.admin) && {
            icon: "edit",
            label: `Manage ${kind}`,
            onSelect: () => onOpenInfo({ name: "edit" }),
          },
        group &&
          rights.invites && {
            icon: "personAdd",
            label: rights.broadcast ? "Add subscribers" : "Add members",
            onSelect: () => onOpenInfo({ name: "add" }),
          },
        group && rights.links && { icon: "link", label: "Copy invite link", onSelect: inviteLink },
        { icon: "openNew", label: "Open as another account…", onSelect: () => setOpeningAs(true) },
        group &&
          rights.member && { icon: "leave", label: `Leave ${kind}`, danger: true, onSelect: leave },
      ];
  const upgradedTo =
    group?._ === "chat" && group.migrated_to?._ === "inputChannel"
      ? `ch${group.migrated_to.channel_id}`
      : undefined;

  return (
    <MediaContext.Provider value={media}>
      {/* biome-ignore lint/a11y/noStaticElementInteractions: files can be dropped anywhere on the chat; the attach button does the same */}
      <section
        className={dragging ? "chat dragging" : "chat"}
        onDragOver={(event) => {
          if (!canWrite || !event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          setDragging(true);
        }}
        onDragLeave={(event) => {
          if (!event.currentTarget.contains(event.relatedTarget as Node)) setDragging(false);
        }}
        onDrop={(event) => {
          setDragging(false);
          const files = [...event.dataTransfer.files];
          if (!canWrite || files.length === 0) return;
          event.preventDefault();
          setAttaching(files);
        }}
      >
        <header className="chat-header">
          <button
            type="button"
            className={openTopic ? "icon-button" : "icon-button back"}
            onClick={openTopic ? () => setTopic(undefined) : onBack}
            aria-label={openTopic ? "Topics" : "Back"}
            title={openTopic ? "Topics" : "Back"}
          >
            <Icon name="back" />
          </button>
          <button
            type="button"
            className="chat-header-main"
            onClick={() => onOpenInfo({ name: "info" })}
            title="Info"
          >
            {openTopic ? (
              <span className="topic-icon" style={{ background: topicColor(openTopic) }}>
                {openTopic.id === GENERAL ? "#" : [...openTopic.title][0]?.toUpperCase()}
              </span>
            ) : (
              <Avatar id={group?.id ?? user?.id ?? chatKey} name={title} size={42} />
            )}
            <span className="chat-title">
              <span dir="auto">{title}</span>
              <span className="chat-subtitle">{subtitle}</span>
            </span>
          </button>
          <Menu label="More" trigger={<Icon name="more" />} items={menu} />
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
                {pinned.message || mediaLabel(pinned)}
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
            onCreate={(name, color) => client.createTopic(chatKey, name, color).then(setTopic)}
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
              {withAlbums(all).map(({ message, album }) =>
                isRegular(message) ? (
                  <Message
                    key={message.id}
                    message={message}
                    album={album}
                    replied={repliedTo(message, byId)}
                    users={snapshot.users}
                    readOutbox={dialog?.readOutbox ?? 0}
                    showSender={group !== undefined && !rights.broadcast}
                    hideReply={forum && repliesToTopic(message)}
                    externalOrigin={externalOrigin(message, snapshot)}
                    actions={actionsFor(album?.find((m) => m.message) ?? message, album)}
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
              {uploads.map((upload) => (
                <Pending key={upload.id} upload={upload} />
              ))}
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
                onAttach={setAttaching}
                onRecorded={(recording) => sendFiles([recording], "", false)}
                onError={(text) => notify(text)}
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
        {openingAs && (
          <OpenAs me={client.me} chatKey={chatKey} onClose={() => setOpeningAs(false)} />
        )}
        {attaching && (
          <AttachDialog files={attaching} onClose={() => setAttaching(null)} onSend={sendFiles} />
        )}
        {viewing !== null && (
          <Viewer
            items={visual}
            index={viewing}
            author={author}
            onIndex={setViewing}
            onClose={() => setViewing(null)}
          />
        )}
        {dragging && <div className="drop-zone">Drop files to send them</div>}
      </section>
    </MediaContext.Provider>
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
      const kind = chat.broadcast
        ? count === 1
          ? "subscriber"
          : "subscribers"
        : count === 1
          ? "member"
          : "members";
      const counted = `${count} ${kind}`;
      return chat.username ? `${counted} · @${chat.username}` : counted;
    }
  }
}

/**
 * Why the composer is replaced by a bar ("join" offers joining, "upgraded" opens the
 * supergroup), if it is.
 */
function blockedReason(chat: AnyChat, rights: Abilities): string | undefined {
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

/** Messages in order, each album (consecutive messages with one `grouped_id`) as one. */
function withAlbums(all: readonly AnyMessage[]): { message: AnyMessage; album?: tl.message[] }[] {
  const out: { message: AnyMessage; album?: tl.message[] }[] = [];
  for (const message of all) {
    const last = out.at(-1);
    const grouped = message._ === "message" ? message.grouped_id : undefined;
    const head = last?.message;
    if (last && grouped && head?._ === "message" && head.grouped_id === grouped) {
      last.album = [...(last.album ?? [head]), message as tl.message];
    } else out.push({ message });
  }
  return out;
}
