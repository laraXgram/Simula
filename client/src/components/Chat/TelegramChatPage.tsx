import { FormEvent, MouseEvent, useEffect, useMemo, useRef, useState } from 'react';
import {
  ChevronDown,
  Bot,
  CircleUserRound,
  Copy,
  MoreVertical,
  Reply,
  Pencil,
  Paperclip,
  Plus,
  Search,
  SendHorizonal,
  ShieldCheck,
  Trash2,
  UserPlus,
  X,
} from 'lucide-react';
import {
  clearSimHistory,
  createSimBot,
  deleteBotMessage,
  deleteBotMessages,
  editBotMessageCaption,
  editUserMessageMedia,
  editBotMessageText,
  getBotFile,
  getSimulationBootstrap,
  sendUserMedia,
  sendUserMessage,
  setUserMessageReaction,
  updateSimBot,
  upsertSimUser,
} from '../../services/botApi';
import { API_BASE_URL, DEFAULT_BOT_TOKEN } from '../../services/config';
import { useBotUpdates } from '../../hooks/useBotUpdates';
import { BotUpdate, ChatMessage, MessageEntity, SimBot, SimUser } from '../../types/app';

const DEFAULT_USER: SimUser = {
  id: 10001,
  first_name: 'Test User',
  username: 'test_user',
};

const START_KEY = 'laragram-started-chats';
const BOTS_KEY = 'laragram-sim-bots';
const USERS_KEY = 'laragram-sim-users';
const MESSAGES_KEY = 'laragram-sim-messages';
const SELECTED_BOT_KEY = 'laragram-selected-bot-token';
const SELECTED_USER_KEY = 'laragram-selected-user-id';
type SidebarTab = 'chats' | 'bots' | 'users';
type ChatScopeTab = 'private' | 'group' | 'channel';
type BotModalMode = 'create' | 'edit';
type UserModalMode = 'create' | 'edit';
type ComposerParseMode = 'none' | 'MarkdownV2' | 'Markdown' | 'HTML';

const TELEGRAM_REACTION_EMOJIS = [
  '👍', '👎', '❤', '🔥', '🎉', '😁', '🤔', '😢', '😱', '👏', '🤩', '🙏', '👌', '🤣', '💯', '⚡',
  '💔', '🥰', '🤬', '🤯', '🤮', '🥱', '😈', '😎', '🗿', '🆒', '😘', '👀', '🤝', '🍾',
];

interface TelegramChatPageProps {
  initialTab?: SidebarTab;
}

export default function TelegramChatPage({ initialTab = 'chats' }: TelegramChatPageProps) {
  const [activeTab, setActiveTab] = useState<SidebarTab>(initialTab);
  const [chatScopeTab, setChatScopeTab] = useState<ChatScopeTab>('private');
  const [availableBots, setAvailableBots] = useState<SimBot[]>(() => {
    try {
      const raw = localStorage.getItem(BOTS_KEY);
      return raw ? (JSON.parse(raw) as SimBot[]) : [];
    } catch {
      return [];
    }
  });
  const [selectedBotToken, setSelectedBotToken] = useState(() => localStorage.getItem(SELECTED_BOT_KEY) || DEFAULT_BOT_TOKEN);
  const [availableUsers, setAvailableUsers] = useState<SimUser[]>(() => {
    try {
      const raw = localStorage.getItem(USERS_KEY);
      const parsed = raw ? (JSON.parse(raw) as SimUser[]) : [];
      return parsed.length > 0 ? parsed : [DEFAULT_USER];
    } catch {
      return [DEFAULT_USER];
    }
  });
  const [selectedUserId, setSelectedUserId] = useState<number>(() => {
    const raw = localStorage.getItem(SELECTED_USER_KEY);
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_USER.id;
  });
  const [chatSearch, setChatSearch] = useState('');
  const [composerText, setComposerText] = useState('');
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    try {
      const raw = localStorage.getItem(MESSAGES_KEY);
      return raw ? (JSON.parse(raw) as ChatMessage[]) : [];
    } catch {
      return [];
    }
  });
  const [lastUpdateId, setLastUpdateId] = useState<number>(0);
  const [isSending, setIsSending] = useState(false);
  const [isBootstrapping, setIsBootstrapping] = useState(false);
  const [errorText, setErrorText] = useState('');
  const [showBotModal, setShowBotModal] = useState(false);
  const [showUserModal, setShowUserModal] = useState(false);
  const [botModalMode, setBotModalMode] = useState<BotModalMode>('create');
  const [userModalMode, setUserModalMode] = useState<UserModalMode>('create');
  const [botDraft, setBotDraft] = useState({
    first_name: '',
    username: '',
  });
  const [userDraft, setUserDraft] = useState({
    first_name: '',
    username: '',
    id: '',
  });
  const [composerEditTarget, setComposerEditTarget] = useState<ChatMessage | null>(null);
  const [replyTarget, setReplyTarget] = useState<ChatMessage | null>(null);
  const [messageMenu, setMessageMenu] = useState<{ messageId: number; x: number; y: number } | null>(null);
  const [chatMenuOpen, setChatMenuOpen] = useState(false);
  const [selectionMode, setSelectionMode] = useState(false);
  const [selectedMessageIds, setSelectedMessageIds] = useState<number[]>([]);
  const [copiedToken, setCopiedToken] = useState(false);
  const [selectedUploads, setSelectedUploads] = useState<File[]>([]);
  const [composerParseMode, setComposerParseMode] = useState<ComposerParseMode>('none');
  const [showFormattingTools, setShowFormattingTools] = useState(false);
  const [mediaUrlByFileId, setMediaUrlByFileId] = useState<Record<string, string>>({});
  const [showScrollToBottom, setShowScrollToBottom] = useState(false);
  const [highlightedMessageId, setHighlightedMessageId] = useState<number | null>(null);
  const [startedChats, setStartedChats] = useState<Record<string, boolean>>(() => {
    try {
      const raw = localStorage.getItem(START_KEY);
      return raw ? (JSON.parse(raw) as Record<string, boolean>) : {};
    } catch {
      return {};
    }
  });

  const selectedBot = useMemo(
    () => availableBots.find((bot) => bot.token === selectedBotToken),
    [availableBots, selectedBotToken],
  );

  const selectedUser = useMemo(
    () => availableUsers.find((user) => user.id === selectedUserId) || DEFAULT_USER,
    [availableUsers, selectedUserId],
  );

  const selectedChatId = selectedUser.id;
  const chatKey = `${selectedBotToken}:${selectedChatId}`;
  const hasStarted = Boolean(startedChats[chatKey]);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const messagesContainerRef = useRef<HTMLElement | null>(null);
  const messageRefs = useRef<Record<number, HTMLDivElement | null>>({});
  const isNearBottomRef = useRef(true);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const composerTextareaRef = useRef<HTMLTextAreaElement | null>(null);

  const visibleMessages = useMemo(
    () => messages.filter((message) => message.chatId === selectedChatId && message.botToken === selectedBotToken),
    [messages, selectedBotToken, selectedChatId],
  );

  const renderedMessageBlocks = useMemo(() => {
    const blocks: Array<
      | { kind: 'single'; message: ChatMessage }
      | { kind: 'album'; mediaGroupId: string; messages: ChatMessage[] }
    > = [];

    for (let i = 0; i < visibleMessages.length; i += 1) {
      const current = visibleMessages[i];
      if (!current.mediaGroupId || !current.media) {
        blocks.push({ kind: 'single', message: current });
        continue;
      }

      const group: ChatMessage[] = [current];
      let cursor = i + 1;
      while (cursor < visibleMessages.length) {
        const next = visibleMessages[cursor];
        if (
          next.mediaGroupId === current.mediaGroupId
          && next.media
          && next.isOutgoing === current.isOutgoing
        ) {
          group.push(next);
          cursor += 1;
          continue;
        }
        break;
      }

      if (group.length > 1) {
        blocks.push({ kind: 'album', mediaGroupId: current.mediaGroupId, messages: group });
        i = cursor - 1;
      } else {
        blocks.push({ kind: 'single', message: current });
      }
    }

    return blocks;
  }, [visibleMessages]);

  const filteredUsers = useMemo(() => {
    const keyword = chatSearch.trim().toLowerCase();
    if (!keyword) {
      return availableUsers;
    }

    return availableUsers.filter((user) => {
      const firstName = user.first_name.toLowerCase();
      const username = (user.username || '').toLowerCase();
      const idText = String(user.id);
      return firstName.includes(keyword) || username.includes(keyword) || idText.includes(keyword);
    });
  }, [availableUsers, chatSearch]);

  useEffect(() => {
    localStorage.setItem(BOTS_KEY, JSON.stringify(availableBots));
  }, [availableBots]);

  useEffect(() => {
    localStorage.setItem(USERS_KEY, JSON.stringify(availableUsers));
  }, [availableUsers]);

  useEffect(() => {
    localStorage.setItem(MESSAGES_KEY, JSON.stringify(messages));
  }, [messages]);

  useEffect(() => {
    localStorage.setItem(SELECTED_BOT_KEY, selectedBotToken);
  }, [selectedBotToken]);

  useEffect(() => {
    localStorage.setItem(SELECTED_USER_KEY, String(selectedUserId));
  }, [selectedUserId]);

  useBotUpdates({
    token: selectedBotToken,
    lastUpdateId,
    onUpdate: (update: BotUpdate) => {
      if (update.message_reaction) {
        const actor = update.message_reaction.user;
        const actorKey = actor ? `${actor.id}:${actor.is_bot ? 1 : 0}` : null;
        const newReactionEmojis = update.message_reaction.new_reaction
          .filter((item) => item.type === 'emoji')
          .map((item) => item.emoji);

        if (actorKey) {
          setMessages((prev) => prev.map((message) => {
            if (
              message.botToken !== selectedBotToken
              || message.chatId !== update.message_reaction!.chat.id
              || message.id !== update.message_reaction!.message_id
            ) {
              return message;
            }

            const actorReactions = { ...(message.actorReactions || {}) };
            if (newReactionEmojis.length === 0) {
              delete actorReactions[actorKey];
            } else {
              actorReactions[actorKey] = newReactionEmojis;
            }

            return {
              ...message,
              actorReactions,
            };
          }));
        }
      }

      if (update.message_reaction_count) {
        const counts = update.message_reaction_count.reactions
          .filter((item) => (item.type as { type?: string }).type === 'emoji')
          .map((item) => ({
            emoji: (item.type as { emoji?: string }).emoji || '',
            count: item.total_count,
          }))
          .filter((item) => item.emoji);

        setMessages((prev) => prev.map((message) => {
          if (
            message.botToken !== selectedBotToken
            || message.chatId !== update.message_reaction_count!.chat.id
            || message.id !== update.message_reaction_count!.message_id
          ) {
            return message;
          }

          return {
            ...message,
            reactionCounts: counts,
          };
        }));
      }

      const payload = update.edited_message || update.message;
      if (!payload) {
        setLastUpdateId((current) => Math.max(current, update.update_id));
        return;
      }

      let media: ChatMessage['media'];
      if (payload.photo && payload.photo.length > 0) {
        const bestPhoto = payload.photo[payload.photo.length - 1];
        media = {
          type: 'photo',
          fileId: bestPhoto.file_id,
          mimeType: bestPhoto.mime_type,
        };
      } else if (payload.video) {
        media = {
          type: 'video',
          fileId: payload.video.file_id,
          mimeType: payload.video.mime_type,
          fileName: payload.video.file_name,
        };
      } else if (payload.audio) {
        media = {
          type: 'audio',
          fileId: payload.audio.file_id,
          mimeType: payload.audio.mime_type,
          fileName: payload.audio.file_name,
        };
      } else if (payload.voice) {
        media = {
          type: 'voice',
          fileId: payload.voice.file_id,
          mimeType: payload.voice.mime_type,
        };
      } else if (payload.document) {
        media = {
          type: 'document',
          fileId: payload.document.file_id,
          mimeType: payload.document.mime_type,
          fileName: payload.document.file_name,
        };
      }

      const mapped: ChatMessage = {
        id: payload.message_id,
        botToken: selectedBotToken,
        chatId: payload.chat.id,
        text: payload.text || payload.caption || '',
        date: payload.date,
        isOutgoing: Boolean(payload.from?.is_bot),
        fromName: payload.from?.first_name || 'Bot',
        fromUserId: payload.from?.id || 0,
        media,
        mediaGroupId: payload.media_group_id,
        replyTo: payload.reply_to_message ? {
          messageId: payload.reply_to_message.message_id,
          fromName: payload.reply_to_message.from?.first_name || 'Unknown',
          text: payload.reply_to_message.text || payload.reply_to_message.caption || '',
          hasMedia: Boolean(
            payload.reply_to_message.photo?.length
            || payload.reply_to_message.video
            || payload.reply_to_message.audio
            || payload.reply_to_message.voice
            || payload.reply_to_message.document,
          ),
          mediaType: payload.reply_to_message.photo?.length
            ? 'photo'
            : payload.reply_to_message.video
              ? 'video'
              : payload.reply_to_message.audio
                ? 'audio'
                : payload.reply_to_message.voice
                  ? 'voice'
                  : payload.reply_to_message.document
                    ? 'document'
                    : undefined,
        } : undefined,
        entities: payload.entities,
        captionEntities: payload.caption_entities,
        editDate: payload.edit_date,
      };

      setMessages((prev) => {
        const existingIndex = prev.findIndex(
          (m) => m.id === mapped.id && m.botToken === mapped.botToken && m.chatId === mapped.chatId,
        );

        if (existingIndex >= 0) {
          const next = [...prev];
          const existing = next[existingIndex];
          next[existingIndex] = {
            ...mapped,
            reactionCounts: existing.reactionCounts,
            actorReactions: existing.actorReactions,
          };
          return next;
        }

        return [...prev, mapped];
      });

      setLastUpdateId((current) => Math.max(current, update.update_id));
    },
  });

  useEffect(() => {
    let mounted = true;

    const loadBotProfile = async () => {
      setIsBootstrapping(true);
      setErrorText('');
      try {
        const bootstrap = await getSimulationBootstrap(selectedBotToken);

        if (!mounted) {
          return;
        }

        const bot: SimBot = {
          id: bootstrap.bot.id,
          token: selectedBotToken,
          username: bootstrap.bot.username || `bot_${bootstrap.bot.id}`,
          first_name: bootstrap.bot.first_name,
        };

        setAvailableBots((prev) => {
          if (prev.some((item) => item.token === bot.token)) {
            return prev.map((item) => (item.token === bot.token ? bot : item));
          }
          return [...prev, bot];
        });

        const bootstrapUsers = bootstrap.users.length > 0 ? bootstrap.users : [DEFAULT_USER];
        setAvailableUsers((prev) => {
          const byId = new Map<number, SimUser>();
          [...bootstrapUsers, ...prev].forEach((user) => {
            byId.set(user.id, user);
          });
          return Array.from(byId.values());
        });
      } catch (error) {
        if (mounted) {
          setErrorText(error instanceof Error ? error.message : 'Failed to load bot profile');
        }
      } finally {
        if (mounted) {
          setIsBootstrapping(false);
        }
      }
    };

    loadBotProfile();
    return () => {
      mounted = false;
    };
  }, [selectedBotToken]);

  const persistStarted = (next: Record<string, boolean>) => {
    setStartedChats(next);
    localStorage.setItem(START_KEY, JSON.stringify(next));
  };

  const sendAsUser = async (
    text: string,
    parseMode?: Exclude<ComposerParseMode, 'none'>,
    replyToMessageId?: number,
  ) => {
    if (!text.trim() || isSending) {
      return;
    }

    setIsSending(true);
    setErrorText('');
    try {
      await sendUserMessage(selectedBotToken, {
        chat_id: selectedChatId,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        text,
        parse_mode: parseMode,
        reply_to_message_id: replyToMessageId,
      });
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'User send failed');
    } finally {
      setIsSending(false);
    }
  };

  const submitComposer = async () => {
    const text = composerText.trim();
    if (!text && selectedUploads.length === 0 && !composerEditTarget) {
      return;
    }

    if (composerEditTarget) {
      try {
        if (composerEditTarget.media) {
          if (selectedUploads.length > 0) {
            if (selectedUploads.length !== 1) {
              setErrorText('When editing media, select exactly one file.');
              return;
            }

            const uploadTarget = inferUploadMethod(selectedUploads[0]);
            const mediaKind = uploadTarget.field;

            await editUserMessageMedia(selectedBotToken, {
              chatId: selectedChatId,
              messageId: composerEditTarget.id,
              mediaKind,
              file: selectedUploads[0],
              caption: text,
              parseMode: composerParseMode === 'none' ? undefined : composerParseMode,
            });
          } else {
            await editBotMessageCaption(selectedBotToken, {
              chat_id: selectedChatId,
              message_id: composerEditTarget.id,
              caption: text,
              parse_mode: composerParseMode === 'none' ? undefined : composerParseMode,
            });
          }
        } else {
          await editBotMessageText(selectedBotToken, {
            chat_id: selectedChatId,
            message_id: composerEditTarget.id,
            text,
            parse_mode: composerParseMode === 'none' ? undefined : composerParseMode,
          });
        }
        setComposerEditTarget(null);
        setComposerText('');
        setSelectedUploads([]);
      } catch (error) {
        setErrorText(error instanceof Error ? error.message : 'Message edit failed');
        // Prevent accidental lock-in edit mode after a failed media edit.
        if (selectedUploads.length > 0) {
          setComposerEditTarget(null);
          setSelectedUploads([]);
        }
      }
      return;
    }

    if (selectedUploads.length > 0) {
      try {
        if (selectedUploads.length === 1) {
          await sendUserMedia(selectedBotToken, {
            chatId: selectedChatId,
            userId: selectedUser.id,
            firstName: selectedUser.first_name,
            username: selectedUser.username,
            file: selectedUploads[0],
            caption: text || undefined,
            parseMode: text && composerParseMode !== 'none' ? composerParseMode : undefined,
            replyToMessageId: replyTarget?.id,
          });
        } else {
          for (let index = 0; index < selectedUploads.length; index += 1) {
            const file = selectedUploads[index];
            await sendUserMedia(selectedBotToken, {
              chatId: selectedChatId,
              userId: selectedUser.id,
              firstName: selectedUser.first_name,
              username: selectedUser.username,
              file,
              caption: index === 0 ? (text || undefined) : undefined,
              parseMode: index === 0 && text && composerParseMode !== 'none' ? composerParseMode : undefined,
              replyToMessageId: index === 0 ? replyTarget?.id : undefined,
            });
          }
        }

        setComposerText('');
        setSelectedUploads([]);
        setReplyTarget(null);
        isNearBottomRef.current = true;
        window.setTimeout(() => {
          messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
        }, 0);
      } catch (error) {
        setErrorText(error instanceof Error ? error.message : 'Media upload failed');
      }
      return;
    }

    setComposerText('');
    await sendAsUser(
      text,
      composerParseMode === 'none' ? undefined : composerParseMode,
      replyTarget?.id,
    );
    setReplyTarget(null);
    isNearBottomRef.current = true;
    window.setTimeout(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    }, 0);
  };

  const onSubmitComposer = async (event: FormEvent) => {
    event.preventDefault();
    await submitComposer();
  };

  const onStart = async () => {
    const next = { ...startedChats, [chatKey]: true };
    persistStarted(next);
    await sendAsUser('/start');
  };

  const onCreateBot = () => {
    setBotModalMode('create');
    setBotDraft({
      first_name: `LaraGram Bot ${Math.floor(Math.random() * 9000 + 1000)}`,
      username: `laragram_${Math.random().toString(36).slice(2, 8)}`,
    });
    setShowBotModal(true);
  };

  const randomizeBotDraft = () => {
    setBotDraft({
      first_name: `LaraGram Bot ${Math.floor(Math.random() * 9000 + 1000)}`,
      username: `laragram_${Math.random().toString(36).slice(2, 8)}`,
    });
  };

  const openCreateUserModal = () => {
    const randomId = Math.floor(Math.random() * 900000 + 10000);
    setUserModalMode('create');
    setUserDraft({
      first_name: `Test User ${Math.floor(Math.random() * 900 + 100)}`,
      username: `test_user_${Math.random().toString(36).slice(2, 7)}`,
      id: String(randomId),
    });
    setShowUserModal(true);
  };

  const randomizeUserDraft = () => {
    const randomId = Math.floor(Math.random() * 900000 + 10000);
    setUserDraft({
      first_name: `Test User ${Math.floor(Math.random() * 900 + 100)}`,
      username: `test_user_${Math.random().toString(36).slice(2, 7)}`,
      id: String(randomId),
    });
  };

  const openEditBotModal = (bot: SimBot) => {
    setBotModalMode('edit');
    setBotDraft({
      first_name: bot.first_name,
      username: bot.username,
    });
    setSelectedBotToken(bot.token);
    setShowBotModal(true);
  };

  const openEditUserModal = (user: SimUser) => {
    setUserModalMode('edit');
    setUserDraft({
      first_name: user.first_name,
      username: user.username || '',
      id: String(user.id),
    });
    setSelectedUserId(user.id);
    setShowUserModal(true);
  };

  const commitBotModal = async () => {
    setErrorText('');
    setIsBootstrapping(true);

    try {
      if (botModalMode === 'create') {
        const created = await createSimBot({
          first_name: botDraft.first_name,
          username: botDraft.username,
        });

        const bot: SimBot = {
          id: created.id,
          token: created.token,
          username: created.username,
          first_name: created.first_name,
        };

        setAvailableBots((prev) => [...prev, bot]);
        setSelectedBotToken(bot.token);
      } else {
        const updated = await updateSimBot(selectedBotToken, {
          first_name: botDraft.first_name,
          username: botDraft.username,
        });

        setAvailableBots((prev) => prev.map((bot) => (
          bot.token === selectedBotToken
            ? {
              ...bot,
              first_name: updated.first_name,
              username: updated.username,
            }
            : bot
        )));
      }

      setLastUpdateId(0);
      setShowBotModal(false);
      setBotDraft({ first_name: '', username: '' });
      setActiveTab('bots');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Bot could not be saved');
    } finally {
      setIsBootstrapping(false);
    }
  };

  const commitUserModal = async () => {
    setErrorText('');
    const parsedId = Number(userDraft.id);
    const id = Number.isFinite(parsedId) && parsedId > 0 ? parsedId : undefined;

    if (userModalMode === 'create' && id && availableUsers.some((item) => item.id === id)) {
      setErrorText('User id already exists. Choose another id.');
      return;
    }

    try {
      const saved = await upsertSimUser({
        id,
        first_name: userDraft.first_name,
        username: userDraft.username,
      });

      const normalized: SimUser = {
        id: saved.id,
        first_name: saved.first_name,
        username: saved.username,
      };

      setAvailableUsers((prev) => {
        const existingIndex = prev.findIndex((item) => item.id === normalized.id);
        if (existingIndex >= 0) {
          const next = [...prev];
          next[existingIndex] = normalized;
          return next;
        }
        return [...prev, normalized];
      });

      setSelectedUserId(normalized.id);
      setUserDraft({ first_name: '', username: '', id: '' });
      setShowUserModal(false);
      setActiveTab('users');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'User could not be saved');
    }
  };

  const onEditMessage = (message: ChatMessage) => {
    if (message.isOutgoing) {
      setMessageMenu(null);
      return;
    }

    setComposerEditTarget(message);
    setReplyTarget(null);
    setComposerText(message.text);
    setSelectedUploads([]);
    setMessageMenu(null);
  };

  const onReplyMessage = (message: ChatMessage) => {
    setReplyTarget(message);
    setComposerEditTarget(null);
    setMessageMenu(null);
    composerTextareaRef.current?.focus();
  };

  const cancelEditingMessage = () => {
    setComposerEditTarget(null);
    setComposerText('');
    setSelectedUploads([]);
  };

  const cancelReplyingMessage = () => {
    setReplyTarget(null);
  };

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    setShowScrollToBottom(false);
  };

  const scrollToMessage = (messageId: number) => {
    const target = messageRefs.current[messageId];
    if (!target) {
      return;
    }

    target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    setHighlightedMessageId(messageId);
    window.setTimeout(() => {
      setHighlightedMessageId((current) => (current === messageId ? null : current));
    }, 1600);
  };

  const onMessagesScroll = () => {
    const container = messagesContainerRef.current;
    if (!container) {
      return;
    }

    const distanceFromBottom = container.scrollHeight - container.scrollTop - container.clientHeight;
    isNearBottomRef.current = distanceFromBottom < 120;
    setShowScrollToBottom(distanceFromBottom > 240);
  };

  const onReactToMessage = async (message: ChatMessage, emoji: string) => {
    const actorKey = `${selectedUser.id}:0`;
    const current = message.actorReactions?.[actorKey] || [];
    const nextReaction = current.includes(emoji) ? [] : [emoji];

    try {
      await setUserMessageReaction(selectedBotToken, {
        chat_id: selectedChatId,
        message_id: message.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        reaction: nextReaction.map((item) => ({ type: 'emoji' as const, emoji: item })),
      });
      setMessageMenu(null);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Reaction failed');
    }
  };

  const onDeleteMessage = async (message: ChatMessage) => {
    try {
      await deleteBotMessage(selectedBotToken, {
        chat_id: selectedChatId,
        message_id: message.id,
      });

      setMessages((prev) => prev.filter((item) => !(
        item.botToken === selectedBotToken && item.chatId === selectedChatId && item.id === message.id
      )));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Message delete failed');
    }

    setMessageMenu(null);
  };

  const onDeleteSelectedMessages = async () => {
    if (selectedMessageIds.length === 0) {
      return;
    }

    try {
      await deleteBotMessages(selectedBotToken, {
        chat_id: selectedChatId,
        message_ids: [...selectedMessageIds].sort((a, b) => a - b),
      });

      setMessages((prev) => prev.filter((item) => !(
        item.botToken === selectedBotToken &&
        item.chatId === selectedChatId &&
        selectedMessageIds.includes(item.id)
      )));

      setSelectedMessageIds([]);
      setSelectionMode(false);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Bulk message delete failed');
    }
  };

  const onClearHistory = async () => {
    try {
      await clearSimHistory(selectedBotToken, selectedChatId);
      setMessages((prev) => prev.filter((item) => !(item.botToken === selectedBotToken && item.chatId === selectedChatId)));
      persistStarted({ ...startedChats, [chatKey]: false });
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Clear history failed');
    }
    setChatMenuOpen(false);
  };

  const toggleMessageSelection = (messageId: number) => {
    setSelectedMessageIds((prev) => (
      prev.includes(messageId) ? prev.filter((id) => id !== messageId) : [...prev, messageId]
    ));
  };

  const onOpenMessageMenu = (
    event: MouseEvent<HTMLDivElement>,
    messageId: number,
  ) => {
    event.stopPropagation();

    if (selectionMode) {
      toggleMessageSelection(messageId);
      return;
    }

    event.preventDefault();
    setMessageMenu({
      messageId,
      x: event.clientX,
      y: event.clientY,
    });
  };

  const onMessageClick = (messageId: number) => {
    if (!selectionMode) {
      return;
    }

    toggleMessageSelection(messageId);
  };

  const onMessageDoubleClick = (messageId: number) => {
    if (!selectionMode) {
      setSelectionMode(true);
    }
    toggleMessageSelection(messageId);
    setMessageMenu(null);
  };

  const copyToken = async (token: string) => {
    try {
      await navigator.clipboard.writeText(token);
      setCopiedToken(true);
      window.setTimeout(() => setCopiedToken(false), 1200);
    } catch {
      setErrorText('Token copy failed');
    }
  };

  const removeBot = (token: string) => {
    if (availableBots.length <= 1) {
      setErrorText('At least one bot must remain in simulator.');
      return;
    }

    const next = availableBots.filter((bot) => bot.token !== token);
    setAvailableBots(next);
    if (selectedBotToken === token) {
      setSelectedBotToken(next[0].token);
      setLastUpdateId(0);
    }
  };

  const removeUser = (id: number) => {
    if (availableUsers.length <= 1) {
      setErrorText('At least one user must remain in simulator.');
      return;
    }

    const next = availableUsers.filter((user) => user.id !== id);
    setAvailableUsers(next);
    if (selectedUserId === id) {
      setSelectedUserId(next[0].id);
    }
  };

  const formatMessageTime = (unix: number) => {
    const date = new Date(unix * 1000);
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  const buildMediaUrl = (token: string, filePath: string) => {
    const encodedPath = filePath
      .split('/')
      .filter((segment) => segment.length > 0)
      .map((segment) => encodeURIComponent(segment))
      .join('/');

    return `${API_BASE_URL}/file/bot${encodeURIComponent(token)}/${encodedPath}`;
  };

  const resolveMediaUrl = async (token: string, fileId: string) => {
    const file = await getBotFile(token, fileId);
    if (!file.file_path) {
      throw new Error('file_path is empty');
    }
    return buildMediaUrl(token, file.file_path);
  };

  const inferUploadMethod = (file: File): {
    method: 'sendPhoto' | 'sendVideo' | 'sendAudio' | 'sendVoice' | 'sendDocument';
    field: 'photo' | 'video' | 'audio' | 'voice' | 'document';
  } => {
    const mime = file.type.toLowerCase();
    if (mime.startsWith('image/')) {
      return { method: 'sendPhoto', field: 'photo' };
    }
    if (mime.startsWith('video/')) {
      return { method: 'sendVideo', field: 'video' };
    }
    if (mime.startsWith('audio/ogg') || mime.includes('opus')) {
      return { method: 'sendVoice', field: 'voice' };
    }
    if (mime.startsWith('audio/')) {
      return { method: 'sendAudio', field: 'audio' };
    }
    return { method: 'sendDocument', field: 'document' };
  };

  const toReactKey = (prefix: string, index: number) => `${prefix}-${index}`;

  const highlightCode = (source: string, language?: string) => {
    if (!language) {
      return source;
    }

    const lang = language.toLowerCase();
    const keywordsByLang: Record<string, string[]> = {
      js: ['const', 'let', 'var', 'function', 'return', 'if', 'else', 'for', 'while', 'class', 'new'],
      javascript: ['const', 'let', 'var', 'function', 'return', 'if', 'else', 'for', 'while', 'class', 'new'],
      ts: ['const', 'let', 'var', 'function', 'return', 'if', 'else', 'for', 'while', 'class', 'new', 'interface', 'type'],
      typescript: ['const', 'let', 'var', 'function', 'return', 'if', 'else', 'for', 'while', 'class', 'new', 'interface', 'type'],
      python: ['def', 'class', 'return', 'if', 'elif', 'else', 'for', 'while', 'import', 'from', 'with', 'as'],
      rust: ['fn', 'let', 'mut', 'pub', 'impl', 'struct', 'enum', 'match', 'if', 'else', 'use', 'mod', 'return'],
      php: ['function', 'class', 'public', 'private', 'protected', 'return', 'if', 'else', 'foreach', 'use'],
    };

    const keywordClasses = 'text-[#ffd480]';
    const stringClasses = 'text-[#9adf91]';
    const commentClasses = 'text-[#8aa2b8]';
    const keywords = keywordsByLang[lang];
    if (!keywords) {
      return source;
    }

    const parts = source.split(/(\"[^\"\\]*(?:\\.[^\"\\]*)*\"|'[^'\\]*(?:\\.[^'\\]*)*'|#[^\n]*|\/\/[^\n]*)/g);
    return parts.map((part, idx) => {
      if (!part) {
        return null;
      }
      if (part.startsWith('#') || part.startsWith('//')) {
        return <span key={`c-${idx}`} className={commentClasses}>{part}</span>;
      }
      if ((part.startsWith('"') && part.endsWith('"')) || (part.startsWith("'") && part.endsWith("'"))) {
        return <span key={`s-${idx}`} className={stringClasses}>{part}</span>;
      }

      const tokenRegex = new RegExp(`\\b(${keywords.join('|')})\\b`, 'g');
      const tokenized = part.split(tokenRegex);
      return (
        <span key={`t-${idx}`}>
          {tokenized.map((token, tokenIdx) => (
            keywords.includes(token)
              ? <span key={`k-${idx}-${tokenIdx}`} className={keywordClasses}>{token}</span>
              : <span key={`n-${idx}-${tokenIdx}`}>{token}</span>
          ))}
        </span>
      );
    });
  };

  const styleForEntity = (entityType: string) => {
    switch (entityType) {
      case 'bold':
        return 'font-semibold';
      case 'italic':
        return 'italic';
      case 'underline':
        return 'underline underline-offset-2';
      case 'strikethrough':
        return 'line-through';
      case 'spoiler':
        return 'rounded px-1 text-transparent bg-white/20 hover:text-white transition';
      case 'blockquote':
      case 'expandable_blockquote':
        return 'my-1 block border-l-4 border-[#79b7df] bg-[#1b3348]/50 px-3 py-2 italic';
      case 'code':
      case 'pre':
        return 'rounded bg-black/35 px-1 py-0.5 font-mono text-[13px]';
      case 'hashtag':
      case 'mention':
      case 'url':
      case 'text_link':
        return 'text-[#8bd1ff] underline-offset-2 hover:underline';
      default:
        return '';
    }
  };

  const renderEntityText = (text: string, entities?: MessageEntity[]) => {
    if (!entities || entities.length === 0) {
      return text;
    }

    const validEntities = [...entities]
      .filter((entity) => entity.length > 0)
      .sort((a, b) => a.offset - b.offset);

    const nodes: Array<string | JSX.Element> = [];
    let cursor = 0;

    validEntities.forEach((entity, index) => {
      if (entity.offset > cursor) {
        nodes.push(text.substring(cursor, entity.offset));
      }

      const chunk = text.substring(entity.offset, entity.offset + entity.length);
      if (!chunk) {
        return;
      }

      const key = toReactKey(entity.type, index);
      const classes = styleForEntity(entity.type);

      if (entity.type === 'pre') {
        nodes.push(
          <pre key={key} className="my-2 overflow-x-auto rounded-lg bg-black/40 p-2 font-mono text-[13px] leading-6">
            <code>{highlightCode(chunk, entity.language)}</code>
          </pre>,
        );
      } else if (entity.type === 'code') {
        nodes.push(
          <code key={key} className={classes}>
            {highlightCode(chunk, entity.language)}
          </code>,
        );
      } else if ((entity.type === 'text_link' || entity.type === 'url') && entity.url) {
        nodes.push(
          <a key={key} href={entity.url} target="_blank" rel="noreferrer" className={classes}>
            {chunk}
          </a>,
        );
      } else {
        nodes.push(
          <span key={key} className={classes}>
            {chunk}
          </span>,
        );
      }

      cursor = Math.max(cursor, entity.offset + entity.length);
    });

    if (cursor < text.length) {
      nodes.push(text.substring(cursor));
    }

    return nodes;
  };

  const parseComposerPreview = (input: string, mode: ComposerParseMode): { text: string; entities: MessageEntity[] } => {
    if (!input) {
      return { text: '', entities: [] };
    }

    if (mode === 'none') {
      return { text: input, entities: [] };
    }

    if (mode === 'HTML') {
      const stripped = input
        .replace(/<br\s*\/?\s*>/gi, '\n')
        .replace(/<[^>]+>/g, '');
      return { text: stripped, entities: [] };
    }

    const entities: MessageEntity[] = [];
    let text = input;

    const rules: Array<{ regex: RegExp; type: MessageEntity['type'] }> = mode === 'MarkdownV2'
      ? [
        { regex: /\*([^*]+)\*/g, type: 'bold' },
        { regex: /_([^_]+)_/g, type: 'italic' },
        { regex: /__([^_]+)__/g, type: 'underline' },
        { regex: /~([^~]+)~/g, type: 'strikethrough' },
        { regex: /`([^`]+)`/g, type: 'code' },
      ]
      : [
        { regex: /\*([^*]+)\*/g, type: 'bold' },
        { regex: /_([^_]+)_/g, type: 'italic' },
        { regex: /`([^`]+)`/g, type: 'code' },
      ];

    rules.forEach((rule) => {
      text = text.replace(rule.regex, (_, content: string, offset: number) => {
        entities.push({ type: rule.type, offset, length: content.length });
        return content;
      });
    });

    return { text, entities };
  };

  const composerPreview = useMemo(
    () => parseComposerPreview(composerText, composerParseMode),
    [composerText, composerParseMode],
  );

  const renderMediaContent = (message: ChatMessage, compact = false) => {
    if (!message.media) {
      return null;
    }

    const mediaUrl = mediaUrlByFileId[message.media.fileId];
    if (!mediaUrl) {
      return (
        <button
          type="button"
          onClick={async () => {
            try {
              const url = await resolveMediaUrl(message.botToken, message.media!.fileId);
              setMediaUrlByFileId((prev) => ({ ...prev, [message.media!.fileId]: url }));
            } catch {
              setErrorText('Failed to resolve media URL');
            }
          }}
          className="rounded-lg border border-white/20 bg-black/25 px-3 py-1.5 text-xs text-white hover:bg-white/10"
        >
          Load media
        </button>
      );
    }

    if (message.media.type === 'photo') {
      return <img src={mediaUrl} alt="photo" className={compact ? 'h-40 w-full rounded-xl object-cover' : 'max-h-80 w-auto rounded-xl'} />;
    }

    if (message.media.type === 'video') {
      return <video src={mediaUrl} controls className={compact ? 'h-40 w-full rounded-xl object-cover' : 'max-h-80 w-auto rounded-xl'} />;
    }

    if (message.media.type === 'audio' || message.media.type === 'voice') {
      return <audio src={mediaUrl} controls className="w-72 max-w-full" />;
    }

    if (message.media.type === 'document') {
      return (
        <a
          href={mediaUrl}
          download={message.media.fileName || 'document'}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center rounded-lg border border-white/20 bg-black/25 px-3 py-2 text-xs text-white hover:bg-white/10"
        >
          Download {message.media.fileName || 'file'}
        </a>
      );
    }

    return null;
  };

  const renderReactionChips = (message: ChatMessage) => {
    if (!message.reactionCounts || message.reactionCounts.length === 0) {
      return null;
    }

    const actorKey = `${selectedUser.id}:0`;
    return (
      <div className="mt-2 flex flex-wrap items-center gap-1.5">
        {message.reactionCounts.map((reaction) => {
          const selected = (message.actorReactions?.[actorKey] || []).includes(reaction.emoji);
          return (
            <button
              key={`${message.id}-${reaction.emoji}`}
              type="button"
              onClick={() => void onReactToMessage(message, reaction.emoji)}
              className={[
                'rounded-full border px-2.5 py-1 text-xs font-medium transition',
                selected
                  ? 'border-[#86d3ff] bg-[#5f9ec7]/80 text-white shadow-[0_0_0_1px_rgba(134,211,255,0.35)]'
                  : 'border-white/20 bg-black/25 text-[#dceaf5] hover:bg-white/10',
              ].join(' ')}
            >
              <span className="mr-1">{reaction.emoji}</span>
              <span>{reaction.count}</span>
            </button>
          );
        })}
      </div>
    );
  };

  useEffect(() => {
    if (availableBots.length === 0) {
      return;
    }

    if (!availableBots.some((bot) => bot.token === selectedBotToken)) {
      setSelectedBotToken(availableBots[0].token);
      setLastUpdateId(0);
    }
  }, [availableBots, selectedBotToken]);

  useEffect(() => {
    if (availableUsers.length === 0) {
      return;
    }

    if (!availableUsers.some((user) => user.id === selectedUserId)) {
      setSelectedUserId(availableUsers[0].id);
    }
  }, [availableUsers, selectedUserId]);

  useEffect(() => {
    isNearBottomRef.current = true;
    setShowScrollToBottom(false);
    window.setTimeout(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'auto', block: 'end' });
    }, 0);
  }, [selectedBotToken, selectedChatId]);

  useEffect(() => {
    if (isNearBottomRef.current) {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    }
  }, [visibleMessages.length, selectedChatId, selectedBotToken]);

  useEffect(() => {
    let cancelled = false;

    const loadVisibleMediaUrls = async () => {
      for (const message of visibleMessages) {
        const fileId = message.media?.fileId;
        if (!fileId || mediaUrlByFileId[fileId]) {
          continue;
        }

        try {
          const url = await resolveMediaUrl(message.botToken, fileId);
          if (cancelled) {
            return;
          }
          setMediaUrlByFileId((prev) => ({ ...prev, [fileId]: url }));
        } catch {
          // Keep UI responsive even if a single media file fails to resolve.
        }
      }
    };

    void loadVisibleMediaUrls();

    return () => {
      cancelled = true;
    };
  }, [visibleMessages, mediaUrlByFileId]);

  useEffect(() => {
    const closeMenus = () => {
      setMessageMenu(null);
      setChatMenuOpen(false);
    };

    window.addEventListener('click', closeMenus);
    return () => window.removeEventListener('click', closeMenus);
  }, []);

  useEffect(() => {
    if (!errorText) {
      return;
    }

    const timeout = window.setTimeout(() => {
      setErrorText('');
    }, 4500);

    return () => window.clearTimeout(timeout);
  }, [errorText]);

  useEffect(() => {
    if (selectionMode && selectedMessageIds.length === 0) {
      setSelectionMode(false);
    }
  }, [selectionMode, selectedMessageIds.length]);

  useEffect(() => {
    const textarea = composerTextareaRef.current;
    if (!textarea) {
      return;
    }

    textarea.style.height = 'auto';
    const maxHeight = 180;
    const nextHeight = Math.min(textarea.scrollHeight, maxHeight);
    textarea.style.height = `${nextHeight}px`;
    textarea.style.overflowY = textarea.scrollHeight > maxHeight ? 'auto' : 'hidden';
  }, [composerText, composerEditTarget]);

  return (
    <div className="h-screen bg-app-pattern text-telegram-text">
      <div className="mx-auto flex h-full max-w-[1500px] border-x border-white/10 backdrop-blur-md">
        <aside className="w-[360px] border-r border-white/10 bg-[#152434]/95">
          <div className="border-b border-white/10 px-4 py-3">
            <div className="mb-2 flex items-center justify-between">
              <h1 className="text-xl font-semibold tracking-wide">LaraGram Studio</h1>
              <ShieldCheck className="h-5 w-5 text-[#66c1ff]" />
            </div>
            <p className="text-xs text-telegram-textSecondary">Telegram-like Bot Simulator</p>
          </div>

          <div className="p-3">
            <div className="mb-3 grid grid-cols-3 gap-2 rounded-xl bg-black/20 p-1.5">
              <button
                type="button"
                onClick={() => setActiveTab('chats')}
                className={`rounded-lg px-2 py-2 text-xs font-medium ${activeTab === 'chats' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Chats
              </button>
              <button
                type="button"
                onClick={() => setActiveTab('bots')}
                className={`rounded-lg px-2 py-2 text-xs font-medium ${activeTab === 'bots' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Bots
              </button>
              <button
                type="button"
                onClick={() => setActiveTab('users')}
                className={`rounded-lg px-2 py-2 text-xs font-medium ${activeTab === 'users' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Users
              </button>
            </div>
            <div className="mb-3 grid grid-cols-3 gap-1 rounded-xl border border-white/10 bg-black/20 p-1 text-[11px]">
              <button
                type="button"
                onClick={() => setChatScopeTab('private')}
                className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'private' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Private
              </button>
              <button
                type="button"
                className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'group' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Group
              </button>
              <button
                type="button"
                onClick={() => setChatScopeTab('channel')}
                className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'channel' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Channel
              </button>
            </div>

            {chatScopeTab !== 'private' ? (
              <div className="mb-3 rounded-xl border border-dashed border-white/20 bg-black/20 px-3 py-3 text-xs text-telegram-textSecondary">
                {chatScopeTab === 'group' ? 'Group chat simulator will be enabled in next phase.' : null}
                {chatScopeTab === 'channel' ? 'Channel simulator will be enabled in next phase.' : null}
              </div>
            ) : null}

            <div className="mb-3 flex items-center justify-between rounded-xl bg-white/5 px-3 py-2">
              <div className="min-w-0 text-xs text-telegram-textSecondary">
                <p className="font-medium text-white">Bot: {selectedBot?.first_name || 'Loading'}</p>
                <p className="break-all text-[11px] leading-4">Token: {selectedBotToken}</p>
              </div>
              <div className="ml-3 flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => void copyToken(selectedBotToken)}
                  className="rounded-full bg-white/10 p-2 text-white hover:bg-white/20"
                  title="Copy token"
                >
                  <Copy className="h-4 w-4" />
                </button>
                <button
                  type="button"
                  onClick={() => (activeTab === 'users' ? openCreateUserModal() : onCreateBot())}
                  className="rounded-full bg-[#2f6ea1] p-2 text-white hover:bg-[#3b82bf]"
                >
                  <Plus className="h-4 w-4" />
                </button>
              </div>
            </div>
            {copiedToken ? <p className="mb-2 text-[11px] text-[#9bd1f5]">Token copied.</p> : null}

            {activeTab === 'chats' && chatScopeTab === 'private' ? (
              <>
                <div className="mb-3 flex items-center gap-2 rounded-xl bg-white/5 px-3 py-2 text-sm text-telegram-textSecondary">
                  <Search className="h-4 w-4" />
                  <input
                    value={chatSearch}
                    onChange={(e) => setChatSearch(e.target.value)}
                    className="w-full bg-transparent text-sm text-white outline-none placeholder:text-telegram-textSecondary"
                    placeholder="Search users"
                  />
                </div>

                <div className="space-y-2">
                  {filteredUsers.map((user) => {
                    const isActive = user.id === selectedUserId;
                    const userChatKey = `${selectedBotToken}:${user.id}`;
                    const started = Boolean(startedChats[userChatKey]);
                    return (
                      <button
                        key={user.id}
                        type="button"
                        onClick={() => setSelectedUserId(user.id)}
                        className={`w-full rounded-xl border px-3 py-2 text-left transition ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20 hover:bg-black/30'}`}
                      >
                        <div className="flex items-center justify-between">
                          <p className="font-medium text-white">{user.first_name}</p>
                          <span className="text-[10px] text-[#b5cfdf]">{started ? 'Started' : 'Tap to chat'}</span>
                        </div>
                        <p className="text-xs text-telegram-textSecondary">@{user.username || `user_${user.id}`}</p>
                      </button>
                    );
                  })}
                </div>
              </>
            ) : null}

            {activeTab === 'bots' ? (
              <div className="space-y-2">
                {availableBots.map((bot) => {
                  const isActive = bot.token === selectedBotToken;
                  return (
                    <div
                      key={bot.token}
                      className={`rounded-xl border px-3 py-2 ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20'}`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <button
                          type="button"
                          onClick={() => {
                            setSelectedBotToken(bot.token);
                            setLastUpdateId(0);
                          }}
                          className="min-w-0 flex-1 text-left"
                        >
                          <p className="truncate font-medium text-white">{bot.first_name}</p>
                          <p className="truncate text-xs text-telegram-textSecondary">@{bot.username}</p>
                        </button>
                        <button
                          type="button"
                          onClick={() => openEditBotModal(bot)}
                          className="rounded-full p-1 text-telegram-textSecondary hover:bg-white/10 hover:text-white"
                          title="Edit bot"
                        >
                          <Pencil className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          onClick={() => removeBot(bot.token)}
                          className="rounded-full p-1 text-telegram-textSecondary hover:bg-white/10 hover:text-white"
                          title="Delete bot"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                      <p className="mt-1 truncate text-[11px] text-[#aac4d7]">{bot.token}</p>
                    </div>
                  );
                })}
              </div>
            ) : null}

            {activeTab === 'users' ? (
              <div className="space-y-2">
                {availableUsers.map((user) => {
                  const isActive = user.id === selectedUserId;
                  return (
                    <div
                      key={user.id}
                      className={`rounded-xl border px-3 py-2 ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20'}`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <button
                          type="button"
                          onClick={() => setSelectedUserId(user.id)}
                          className="min-w-0 flex-1 text-left"
                        >
                          <p className="truncate font-medium text-white">{user.first_name}</p>
                          <p className="truncate text-xs text-telegram-textSecondary">@{user.username || `user_${user.id}`}</p>
                        </button>
                        <button
                          type="button"
                          onClick={() => openEditUserModal(user)}
                          className="rounded-full p-1 text-telegram-textSecondary hover:bg-white/10 hover:text-white"
                          title="Edit user"
                        >
                          <Pencil className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          onClick={() => removeUser(user.id)}
                          className="rounded-full p-1 text-telegram-textSecondary hover:bg-white/10 hover:text-white"
                          title="Delete user"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                      <p className="mt-1 text-[11px] text-[#aac4d7]">id: {user.id}</p>
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>
        </aside>

        <section className="flex flex-1 flex-col bg-[#0f1e2d]/70">
          <header className="flex items-center justify-between border-b border-white/10 bg-[#1a2a3b]/70 px-5 py-3">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[#2b5278]">
                <Bot className="h-5 w-5" />
              </div>
              <div>
                <h2 className="font-semibold">{selectedBot?.first_name || 'Bot'}</h2>
                <p className="text-xs text-telegram-textSecondary">
                  @{selectedBot?.username || 'unknown'} | user #{selectedUser.id}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2 rounded-full bg-black/20 px-2 py-1.5 text-xs text-telegram-textSecondary">
              <CircleUserRound className="h-4 w-4" />
              <select
                value={selectedUserId}
                onChange={(event) => setSelectedUserId(Number(event.target.value))}
                className="max-w-[220px] bg-transparent pr-1 text-xs text-white outline-none"
              >
                {availableUsers.map((user) => (
                  <option key={user.id} value={user.id} className="bg-[#152434] text-white">
                    {user.first_name} ({user.id})
                  </option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2">
              {selectionMode ? (
                <button
                  type="button"
                  onClick={() => {
                    setSelectionMode(false);
                    setSelectedMessageIds([]);
                  }}
                  className="rounded-full border border-white/15 bg-black/20 px-3 py-1.5 text-xs text-white hover:bg-white/10"
                >
                  Exit Select ({selectedMessageIds.length})
                </button>
              ) : null}
              <div className="relative">
              <button
                type="button"
                onClick={(event) => {
                  event.stopPropagation();
                  setChatMenuOpen((prev) => !prev);
                }}
                className="rounded-full border border-white/15 bg-black/20 p-2 text-xs text-white hover:bg-white/10"
                title="Chat menu"
              >
                <MoreVertical className="h-4 w-4" />
              </button>
              {chatMenuOpen ? (
                <div
                  className="absolute right-0 top-11 z-20 w-52 rounded-xl border border-white/15 bg-[#132130] p-1 shadow-2xl"
                  onClick={(event) => event.stopPropagation()}
                >
                  <button
                    type="button"
                    onClick={() => void onDeleteSelectedMessages()}
                    disabled={selectedMessageIds.length === 0}
                    className="w-full rounded-lg px-3 py-2 text-left text-sm text-red-200 hover:bg-white/10 disabled:opacity-40"
                  >
                    Delete selected ({selectedMessageIds.length})
                  </button>
                  <button
                    type="button"
                    onClick={() => void onClearHistory()}
                    className="w-full rounded-lg px-3 py-2 text-left text-sm text-red-300 hover:bg-white/10"
                  >
                    Clear history
                  </button>
                </div>
              ) : null}
              </div>
            </div>
          </header>

          <main
            ref={messagesContainerRef}
            onScroll={onMessagesScroll}
            className="relative flex-1 overflow-y-auto bg-[url('/telegram-bg.svg')] bg-cover bg-center px-6 py-5"
          >
            {chatScopeTab !== 'private' ? (
              <div className="mx-auto mt-16 max-w-md rounded-2xl border border-dashed border-white/20 bg-black/20 p-6 text-center shadow-2xl">
                <h3 className="mb-2 text-2xl font-semibold">{chatScopeTab === 'group' ? 'Groups' : 'Channels'} Coming Soon</h3>
                <p className="mb-2 text-sm leading-6 text-telegram-textSecondary">
                  Structure is ready. In next phase this area will show {chatScopeTab} list and dedicated message threads.
                </p>
              </div>
            ) : !hasStarted ? (
              <div className="mx-auto mt-16 max-w-md rounded-2xl border border-white/15 bg-black/20 p-6 text-center shadow-2xl">
                <h3 className="mb-2 text-2xl font-semibold">No messages here yet</h3>
                <p className="mb-2 text-sm leading-6 text-telegram-textSecondary">
                  Tap Start in the bottom bar to begin this conversation exactly like Telegram private bot chat.
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {visibleMessages.length === 0 ? (
                  <p className="text-center text-sm text-telegram-textSecondary">No messages yet.</p>
                ) : null}
                {renderedMessageBlocks.map((block) => {
                  if (block.kind === 'single') {
                    const message = block.message;
                    return (
                      <div
                        key={message.id}
                        ref={(node) => { messageRefs.current[message.id] = node; }}
                        onContextMenu={(event) => onOpenMessageMenu(event, message.id)}
                        onClick={() => onMessageClick(message.id)}
                        onDoubleClick={() => onMessageDoubleClick(message.id)}
                        className={[
                          'relative max-w-[72%] rounded-2xl px-4 py-3 shadow-lg',
                          selectionMode && selectedMessageIds.includes(message.id) ? 'ring-2 ring-[#87cbff]' : '',
                          highlightedMessageId === message.id ? 'ring-2 ring-[#f9e07f] shadow-[0_0_0_2px_rgba(249,224,127,0.35)]' : '',
                          message.isOutgoing ? 'mr-auto rounded-bl-md bg-[#182533]' : 'ml-auto rounded-br-md bg-[#2b5278]',
                        ].join(' ')}
                      >
                        {message.replyTo ? (
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              scrollToMessage(message.replyTo!.messageId);
                            }}
                            className="mb-2 block w-full rounded-lg border-l-2 border-[#8ecbff] bg-black/20 px-2 py-1 text-left text-xs text-[#c7deee] hover:bg-black/30"
                          >
                            <div className="font-medium text-[#9fd8ff]">Reply to {message.replyTo.fromName} #{message.replyTo.messageId}</div>
                            <div className="truncate">
                              {message.replyTo.text || (message.replyTo.hasMedia ? `[{message.replyTo.mediaType || 'media'}]` : 'message')}
                            </div>
                          </button>
                        ) : null}
                        {message.media ? <div className="mb-2">{renderMediaContent(message)}</div> : null}
                        {message.text ? (
                          <div className="text-sm leading-6 break-words whitespace-pre-wrap">{renderEntityText(message.text, message.entities || message.captionEntities)}</div>
                        ) : null}
                        {renderReactionChips(message)}
                        <div className="mt-1 flex items-center justify-end gap-2 text-[10px] text-[#a5bfd3]">
                          <span>#{message.id}</span>
                          {message.editDate ? <span>edited</span> : null}
                          <span>{formatMessageTime(message.date)}</span>
                        </div>
                      </div>
                    );
                  }

                  const lead = block.messages[0];
                  return (
                    <div
                      key={`album-${block.mediaGroupId}-${lead.id}`}
                      ref={(node) => { messageRefs.current[lead.id] = node; }}
                      onContextMenu={(event) => onOpenMessageMenu(event, lead.id)}
                      onClick={() => onMessageClick(lead.id)}
                      onDoubleClick={() => onMessageDoubleClick(lead.id)}
                      className={[
                        'relative max-w-[72%] rounded-2xl px-3 py-3 shadow-lg',
                        selectionMode && selectedMessageIds.includes(lead.id) ? 'ring-2 ring-[#87cbff]' : '',
                        highlightedMessageId === lead.id ? 'ring-2 ring-[#f9e07f] shadow-[0_0_0_2px_rgba(249,224,127,0.35)]' : '',
                        lead.isOutgoing ? 'mr-auto rounded-bl-md bg-[#182533]' : 'ml-auto rounded-br-md bg-[#2b5278]',
                      ].join(' ')}
                    >
                      {lead.replyTo ? (
                        <button
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation();
                            scrollToMessage(lead.replyTo!.messageId);
                          }}
                          className="mb-2 block w-full rounded-lg border-l-2 border-[#8ecbff] bg-black/20 px-2 py-1 text-left text-xs text-[#c7deee] hover:bg-black/30"
                        >
                          <div className="font-medium text-[#9fd8ff]">Reply to {lead.replyTo.fromName} #{lead.replyTo.messageId}</div>
                          <div className="truncate">
                            {lead.replyTo.text || (lead.replyTo.hasMedia ? `[{lead.replyTo.mediaType || 'media'}]` : 'message')}
                          </div>
                        </button>
                      ) : null}
                      <div className="mb-2 grid grid-cols-2 gap-2">
                        {block.messages.map((message) => (
                          <div key={message.id} className="overflow-hidden rounded-xl bg-black/20">
                            {renderMediaContent(message, true)}
                          </div>
                        ))}
                      </div>

                      {lead.text ? (
                        <div className="text-sm leading-6 break-words whitespace-pre-wrap">{renderEntityText(lead.text, lead.captionEntities)}</div>
                      ) : null}
                      {renderReactionChips(lead)}
                      <div className="mt-1 flex items-center justify-end gap-2 text-[10px] text-[#a5bfd3]">
                        <span>Album {block.messages.length} items</span>
                        {lead.editDate ? <span>edited</span> : null}
                        <span>{formatMessageTime(lead.date)}</span>
                      </div>
                    </div>
                  );
                })}
                <div ref={messagesEndRef} />
              </div>
            )}

            {showScrollToBottom ? (
              <button
                type="button"
                onClick={scrollToBottom}
                className="sticky bottom-4 ml-auto flex h-11 w-11 items-center justify-center rounded-full border border-white/20 bg-[#2b5278] text-white shadow-xl hover:bg-[#366892]"
                title="Scroll to bottom"
              >
                <ChevronDown className="h-5 w-5" />
              </button>
            ) : null}
          </main>

          <footer className="border-t border-white/10 px-5 py-4">
            {chatScopeTab !== 'private' ? (
              <div className="rounded-xl border border-dashed border-white/20 bg-black/20 px-4 py-3 text-center text-xs text-telegram-textSecondary">
                Message composer for {chatScopeTab} will be enabled in the next phase.
              </div>
            ) : !hasStarted ? (
              <button
                type="button"
                onClick={onStart}
                className="w-full rounded-xl bg-[#2b5278] px-4 py-3 text-sm font-semibold tracking-wide text-white transition hover:bg-[#366892]"
              >
                START
              </button>
            ) : (
              <div className="space-y-2">
                {replyTarget ? (
                  <div className="flex items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
                    <span className="truncate pr-3">
                      Replying to {replyTarget.fromName} #{replyTarget.id}: {replyTarget.text || (replyTarget.media ? `[${replyTarget.media.type}]` : 'message')}
                    </span>
                    <button
                      type="button"
                      onClick={cancelReplyingMessage}
                      className="rounded-md border border-white/15 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                    >
                      Cancel
                    </button>
                  </div>
                ) : null}
                {composerEditTarget ? (
                  <div className="flex items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
                    <span>
                      Editing {composerEditTarget.media ? 'media message' : 'text message'} #{composerEditTarget.id}
                    </span>
                    <button
                      type="button"
                      onClick={cancelEditingMessage}
                      className="rounded-md border border-white/15 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                    >
                      Cancel
                    </button>
                  </div>
                ) : null}
                {selectedUploads.length > 0 ? (
                  <div className="flex items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
                    <span className="truncate pr-3">
                      Selected: {selectedUploads.length === 1 ? selectedUploads[0].name : `${selectedUploads.length} files`}
                    </span>
                    <button
                      type="button"
                      onClick={() => setSelectedUploads([])}
                      className="rounded-md border border-white/15 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                    >
                      Remove
                    </button>
                  </div>
                ) : null}
                <div className="flex items-center justify-end">
                  <button
                    type="button"
                    onClick={() => setShowFormattingTools((prev) => !prev)}
                    className="rounded-md border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-telegram-textSecondary hover:bg-white/10"
                  >
                    {showFormattingTools ? 'Hide formatting' : 'Show formatting'}
                  </button>
                </div>
                {showFormattingTools ? (
                  <div className="space-y-2">
                    <div className="flex items-center justify-between gap-2 rounded-xl border border-white/10 bg-black/20 px-3 py-2">
                      <label htmlFor="parse-mode" className="text-[11px] text-telegram-textSecondary">Parse mode</label>
                      <select
                        id="parse-mode"
                        value={composerParseMode}
                        onChange={(event) => setComposerParseMode(event.target.value as ComposerParseMode)}
                        className="rounded-md border border-white/10 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                      >
                        <option value="none">None</option>
                        <option value="MarkdownV2">MarkdownV2</option>
                        <option value="Markdown">Markdown</option>
                        <option value="HTML">HTML</option>
                      </select>
                    </div>
                    {composerText.trim() ? (
                      <div className="rounded-xl border border-white/10 bg-black/20 px-3 py-2">
                        <p className="mb-1 text-[11px] text-telegram-textSecondary">Rich preview</p>
                        <div className="text-sm leading-6 break-words whitespace-pre-wrap">{renderEntityText(composerPreview.text, composerPreview.entities)}</div>
                      </div>
                    ) : null}
                  </div>
                ) : null}
                <form onSubmit={onSubmitComposer} className="flex items-center gap-3">
                  <input
                    ref={fileInputRef}
                    type="file"
                    multiple
                    className="hidden"
                    onClick={(event) => {
                      (event.currentTarget as HTMLInputElement).value = '';
                    }}
                    onChange={(event) => {
                      const files = Array.from(event.target.files || []);
                      setSelectedUploads(files);
                      event.currentTarget.value = '';
                    }}
                  />
                  <button
                    type="button"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={!hasStarted || (!!composerEditTarget && !composerEditTarget?.media)}
                    className="rounded-full border border-white/10 bg-black/25 p-3 text-white transition hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                    title="Attach media"
                  >
                    <Paperclip className="h-5 w-5" />
                  </button>
                  <textarea
                    ref={composerTextareaRef}
                    value={composerText}
                    onChange={(e) => setComposerText(e.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' && !event.shiftKey) {
                        event.preventDefault();
                        void submitComposer();
                      }
                    }}
                    disabled={!hasStarted}
                    rows={2}
                    className="min-h-[52px] max-h-[180px] flex-1 resize-none rounded-2xl border border-white/10 bg-black/25 px-4 py-3 text-sm leading-6 outline-none transition focus:border-telegram-lightBlue disabled:cursor-not-allowed disabled:opacity-60"
                    placeholder={composerEditTarget ? 'Edit message...' : 'Write a message...'}
                  />
                  <button
                    type="submit"
                    disabled={!hasStarted || isSending}
                    className="rounded-full bg-telegram-blue p-3 text-white transition hover:bg-telegram-darkBlue disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <SendHorizonal className="h-5 w-5" />
                  </button>
                </form>
              </div>
            )}

            {errorText ? <p className="mt-2 text-xs text-red-300">{errorText}</p> : null}
            <div className="mt-2 text-[11px] text-telegram-textSecondary">
              {isBootstrapping ? 'syncing bot profile...' : 'realtime mode active'}
            </div>
          </footer>
        </section>
      </div>

      {showBotModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void commitBotModal();
            }}
            className="w-full max-w-md rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl"
          >
            <div className="mb-3 flex items-center justify-between">
              <h3 className="flex items-center gap-2 text-lg font-semibold">
                <Bot className="h-5 w-5" />
                {botModalMode === 'create' ? 'Create Bot' : 'Edit Bot'}
              </h3>
              <button type="button" onClick={() => setShowBotModal(false)} className="rounded-full p-1 hover:bg-white/10">
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="space-y-2">
              <input
                value={botDraft.first_name}
                onChange={(e) => setBotDraft((prev) => ({ ...prev, first_name: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Bot first name"
              />
              <input
                value={botDraft.username}
                onChange={(e) => setBotDraft((prev) => ({ ...prev, username: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="bot_username"
              />
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={randomizeBotDraft}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Random
              </button>
              <button
                type="submit"
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892]"
              >
                {botModalMode === 'create' ? 'Create Bot' : 'Save Changes'}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {showUserModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void commitUserModal();
            }}
            className="w-full max-w-md rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl"
          >
            <div className="mb-3 flex items-center justify-between">
              <h3 className="flex items-center gap-2 text-lg font-semibold">
                <UserPlus className="h-5 w-5" />
                {userModalMode === 'create' ? 'Create Test User' : 'Edit User'}
              </h3>
              <button type="button" onClick={() => setShowUserModal(false)} className="rounded-full p-1 hover:bg-white/10">
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="space-y-2">
              <input
                value={userDraft.first_name}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, first_name: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="First name"
              />
              <input
                value={userDraft.username}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, username: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="username (optional)"
              />
              <input
                value={userDraft.id}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, id: e.target.value }))}
                disabled={userModalMode === 'edit'}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="user id (optional)"
              />
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={randomizeUserDraft}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Random
              </button>
              <button
                type="submit"
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892]"
              >
                {userModalMode === 'create' ? 'Create User' : 'Save Changes'}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {messageMenu ? (
        <div
          className="fixed z-50 w-60 rounded-2xl border border-white/15 bg-[#132130] p-2 shadow-2xl"
          style={{ left: messageMenu.x, top: messageMenu.y }}
          onClick={(event) => event.stopPropagation()}
        >
          {(() => {
            const target = visibleMessages.find((message) => message.id === messageMenu.messageId);
            if (!target) {
              return null;
            }

            return (
              <>
                <div className="px-2 pb-1 pt-1 text-[11px] font-medium text-telegram-textSecondary">Quick reactions</div>
                <div className="mb-2 grid grid-cols-8 gap-1 px-1">
                  {TELEGRAM_REACTION_EMOJIS.slice(0, 24).map((emoji) => {
                    const actorKey = `${selectedUser.id}:0`;
                    const selected = (target.actorReactions?.[actorKey] || []).includes(emoji);
                    return (
                    <button
                      key={`${target.id}-${emoji}`}
                      type="button"
                      onClick={() => void onReactToMessage(target, emoji)}
                      className={[
                        'rounded-lg border px-1 py-1 text-sm transition',
                        selected
                          ? 'border-[#86d3ff] bg-[#4f86ad]/80'
                          : 'border-white/15 bg-black/20 hover:bg-white/10',
                      ].join(' ')}
                    >
                      {emoji}
                    </button>
                    );
                  })}
                </div>
                <button
                  type="button"
                  onClick={() => onReplyMessage(target)}
                  className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
                >
                  <Reply className="h-4 w-4" />
                  Reply
                </button>
                {!target.isOutgoing ? (
                  <button
                    type="button"
                    onClick={() => onEditMessage(target)}
                    className="w-full rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
                  >
                    {target.media ? 'Edit caption/media' : 'Edit text'}
                  </button>
                ) : null}
                <button
                  type="button"
                  onClick={() => void onDeleteMessage(target)}
                  className="w-full rounded-lg px-3 py-2 text-left text-sm text-red-200 hover:bg-white/10"
                >
                  Delete
                </button>
              </>
            );
          })()}
        </div>
      ) : null}
    </div>
  );
}
