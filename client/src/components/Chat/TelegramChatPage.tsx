import { FormEvent, MouseEvent, useEffect, useMemo, useRef, useState } from 'react';
import {
  ArrowLeft,
  ChevronLeft,
  ChevronDown,
  ChevronRight,
  Clapperboard,
  Contact,
  Dice5,
  Eye,
  Gamepad2,
  MapPin,
  MapPinned,
  Bot,
  Copy,
  Forward,
  Mic,
  MoreVertical,
  Pause,
  Play,
  Reply,
  Pencil,
  Paperclip,
  Plus,
  Search,
  SendHorizonal,
  ShieldCheck,
  Smile,
  Star,
  Trash2,
  UserPlus,
  Video,
  Wallet,
  Wrench,
  X,
} from 'lucide-react';
import {
  chooseInlineResult,
  clearSimHistory,
  createSimBot,
  createSimulationGroup,
  approveChatJoinRequest,
  banChatMember,
  unbanChatMember,
  restrictChatMember,
  promoteChatMember,
  setChatAdministratorCustomTitle,
  setChatMemberTag,
  banChatSenderChat,
  unbanChatSenderChat,
  setChatTitle,
  setChatDescription,
  setChatPermissions,
  setChatPhoto,
  deleteChatPhoto,
  setChatStickerSet,
  deleteChatStickerSet,
  pinChatMessage,
  unpinChatMessage,
  unpinAllChatMessages,
  leaveChat,
  getChat,
  getChatAdministrators,
  getChatMemberCount,
  getChatMember,
  getForumTopicIconStickers,
  createForumTopic,
  editForumTopic,
  closeForumTopic,
  reopenForumTopic,
  deleteForumTopic,
  unpinAllForumTopicMessages,
  editGeneralForumTopic,
  closeGeneralForumTopic,
  reopenGeneralForumTopic,
  hideGeneralForumTopic,
  unhideGeneralForumTopic,
  unpinAllGeneralForumTopicMessages,
  getChatMenuButton,
  getSimBotPrivacyMode,
  exportChatInviteLink,
  createChatInviteLink,
  editChatInviteLink,
  revokeChatInviteLink,
  createChatSubscriptionInviteLink,
  editChatSubscriptionInviteLink,
  declineChatJoinRequest,
  setSimBotPrivacyMode,
  setChatMenuButton,
  createNewStickerSet,
  addStickerToSet,
  deleteStickerFromSet,
  deleteStickerSet,
  getCustomEmojiStickers,
  getStickerSet,
  deleteBotMessage,
  deleteBotMessages,
  editBotMessageCaption,
  editUserMessageMedia,
  editBotMessageText,
  replaceStickerInSet,
  sendPoll,
  setCustomEmojiStickerSetThumbnail,
  setStickerEmojiList,
  setStickerKeywords,
  setStickerMaskPosition,
  setStickerPositionInSet,
  setStickerSetThumbnail,
  setStickerSetTitle,
  sendInvoice,
  stopPoll,
  votePoll,
  getPollVoters,
  payInvoice,
  PollVoterInfo,
  getCallbackQueryAnswer,
  getBotFile,
  getGameHighScores,
  pressInlineButton,
  sendUserContact,
  sendUserDice,
  sendUserGame,
  sendUserLocation,
  sendUserVenue,
  sendInlineQuery,
  getInlineQueryAnswer,
  setGameScore,
  getSimulationBootstrap,
  openSimulationChannelDirectMessages,
  removeSimulationBusinessConnection,
  setSimulationBusinessConnection,
  deleteSimulationGroup,
  joinSimulationGroup,
  joinSimulationGroupByInviteLink,
  leaveSimulationGroup,
  markSimulationChannelMessageView,
  sendUserMedia,
  sendUserMediaByReference,
  uploadStickerFile,
  sendUserMessage,
  forwardMessage,
  sendBotMediaFile,
  sendBotMessage,
  setUserMessageReaction,
  setSimulationBotGroupMembership,
  updateSimulationGroup,
  deleteSimUser,
  updateSimBot,
  upsertSimUser,
} from '../../services/botApi';
import { API_BASE_URL, DEFAULT_BOT_TOKEN } from '../../services/config';
import { useBotUpdates } from '../../hooks/useBotUpdates';
import type { GetChatMenuButtonRequest, SetChatMenuButtonRequest } from '../../types/generated/methods';
import type {
  BusinessBotRights as GeneratedBusinessBotRights,
  BusinessConnection as GeneratedBusinessConnection,
  ChatShared as GeneratedChatShared,
  ChatMember as GeneratedChatMember,
  MenuButton as GeneratedMenuButton,
  MenuButtonCommands as GeneratedMenuButtonCommands,
  MenuButtonDefault as GeneratedMenuButtonDefault,
  MenuButtonWebApp as GeneratedMenuButtonWebApp,
  UsersShared as GeneratedUsersShared,
  WebAppData as GeneratedWebAppData,
} from '../../types/generated/types';
import {
  BotReplyMarkup,
  BotUpdate,
  ChatMessage,
  InlineKeyboardButton,
  InlineQueryResult,
  SimBootstrapChannelDirectMessages,
  MessageParseMode,
  MessageEntity,
  ReplyKeyboardButton,
  SimBootstrapForumTopic,
  SimRealtimeEvent,
  SimBot,
  SimUser,
} from '../../types/app';

const DEFAULT_USER: SimUser = {
  id: 10001,
  first_name: 'Test User',
  username: 'test_user',
};

const START_KEY = 'laragram-started-chats';
const BOTS_KEY = 'laragram-sim-bots';
const USERS_KEY = 'laragram-sim-users';
const MESSAGES_KEY = 'laragram-sim-messages';
const LAST_UPDATES_KEY = 'laragram-last-update-ids';
const SELECTED_BOT_KEY = 'laragram-selected-bot-token';
const SELECTED_USER_KEY = 'laragram-selected-user-id';
const CHAT_SCOPE_KEY = 'laragram-chat-scope';
const GROUP_CHATS_KEY = 'laragram-group-chats';
const GROUP_MEMBERSHIP_KEY = 'laragram-group-memberships';
const SELECTED_GROUP_BY_BOT_KEY = 'laragram-selected-group-by-bot';
const GROUP_INVITE_LINKS_KEY = 'laragram-group-invite-links';
const GROUP_JOIN_REQUESTS_KEY = 'laragram-group-join-requests';
const GROUP_PINNED_MESSAGES_KEY = 'laragram-group-pinned-messages';
const INVOICE_META_KEY = 'laragram-invoice-meta-by-message';
const FORUM_TOPICS_KEY = 'laragram-forum-topics-by-chat';
const SELECTED_FORUM_TOPIC_KEY = 'laragram-selected-forum-topic-by-chat';
const BUSINESS_CONNECTIONS_KEY = 'laragram-business-connections';
const USER_WALLETS_KEY = 'laragram-user-wallets';
const GENERAL_FORUM_TOPIC_THREAD_ID = 1;
const DEFAULT_FORUM_ICON_COLOR = 0x6FB9F0;
const DEFAULT_WALLET_STATE = {
  fiat: 50000,
  stars: 2500,
};
type SidebarTab = 'chats' | 'bots' | 'users';
type ChatScopeTab = 'private' | 'group' | 'channel';
type BotModalMode = 'create' | 'edit';
type UserModalMode = 'create' | 'edit';
type GroupSettingsPage = 'home' | 'bot-membership' | 'discovery' | 'topics' | 'members' | 'sender-chat' | 'danger-zone';
type ComposerParseMode = 'none' | 'MarkdownV2' | 'Markdown' | 'HTML';
type PaymentMethod = 'wallet' | 'card' | 'stars';
type CheckoutStep = 1 | 2 | 3;
type MediaDrawerTab =
  | 'stickers'
  | 'animations'
  | 'voice'
  | 'video_note'
  | 'dice'
  | 'game'
  | 'contact'
  | 'location'
  | 'venue'
  | 'poll'
  | 'invoice'
  | 'studio';

interface StickerShelfSet {
  name: string;
  title: string;
  stickers: Array<{
    file_id: string;
    file_unique_id: string;
    is_video: boolean;
    is_animated: boolean;
    set_name?: string;
    emoji?: string;
  }>;
}

interface CheckoutFlowState {
  messageId: number;
  step: CheckoutStep;
  method: PaymentMethod;
  outcome: 'success' | 'failed';
  tip: string;
}

interface WalletState {
  fiat: number;
  stars: number;
}

const TELEGRAM_REACTION_EMOJIS = [
  '👍', '👎', '❤', '🔥', '🎉', '😁', '🤔', '😢', '😱', '👏', '🤩', '🙏', '👌', '🤣', '💯', '⚡',
  '💔', '🥰', '🤬', '🤯', '🤮', '🥱', '😈', '😎', '🗿', '🆒', '😘', '👀', '🤝', '🍾',
];

const DICE_EMOJIS = ['🎲', '🎯', '🏀', '⚽', '🎳', '🎰', '🏐'] as const;
const FORUM_ICON_COLOR_PRESETS = [
  0x6FB9F0,
  0xFFD67E,
  0xFF93B2,
  0x90D98B,
  0xB8A9FF,
  0x7ED9D1,
  0xFFA76D,
  0x8EC5FF,
];
const FORUM_TOPIC_EMOJI_PRESETS = ['😀', '😎', '🎯', '📣', '💡', '🚀', '🎮', '🛠️', '🧪', '📌'];

function normalizeWalletState(raw?: Partial<WalletState>): WalletState {
  const fiat = Math.floor(Number(raw?.fiat));
  const stars = Math.floor(Number(raw?.stars));
  return {
    fiat: Number.isFinite(fiat) && fiat >= 0 ? fiat : DEFAULT_WALLET_STATE.fiat,
    stars: Number.isFinite(stars) && stars >= 0 ? stars : DEFAULT_WALLET_STATE.stars,
  };
}

function mapIncomingReplyMarkup(raw?: unknown): BotReplyMarkup | undefined {
  if (!raw || typeof raw !== 'object') {
    return undefined;
  }

  const rawRecord = raw as Record<string, unknown>;

  if (Array.isArray(rawRecord.keyboard)) {
    return {
      kind: 'reply',
      keyboard: rawRecord.keyboard as ReplyKeyboardButton[][],
      is_persistent: typeof rawRecord.is_persistent === 'boolean' ? rawRecord.is_persistent : undefined,
      resize_keyboard: typeof rawRecord.resize_keyboard === 'boolean' ? rawRecord.resize_keyboard : undefined,
      one_time_keyboard: typeof rawRecord.one_time_keyboard === 'boolean' ? rawRecord.one_time_keyboard : undefined,
      input_field_placeholder: typeof rawRecord.input_field_placeholder === 'string' ? rawRecord.input_field_placeholder : undefined,
      selective: typeof rawRecord.selective === 'boolean' ? rawRecord.selective : undefined,
    };
  }

  if (Array.isArray(rawRecord.inline_keyboard)) {
    return {
      kind: 'inline',
      inline_keyboard: rawRecord.inline_keyboard as InlineKeyboardButton[][],
    };
  }

  if (typeof rawRecord.remove_keyboard === 'boolean') {
    return {
      kind: 'remove',
      remove_keyboard: rawRecord.remove_keyboard,
      selective: typeof rawRecord.selective === 'boolean' ? rawRecord.selective : undefined,
    };
  }

  return {
    kind: 'other',
    raw: rawRecord,
  };
}

interface TelegramChatPageProps {
  initialTab?: SidebarTab;
}

interface GroupChatItem {
  id: number;
  type: 'group' | 'supergroup' | 'channel';
  title: string;
  username?: string;
  description?: string;
  isForum?: boolean;
  isDirectMessages?: boolean;
  parentChannelChatId?: number;
  linkedDiscussionChatId?: number;
  settings?: GroupSettingsSnapshot;
}

interface GroupSettingsSnapshot {
  showAuthorSignature: boolean;
  directMessagesEnabled: boolean;
  directMessagesStarCount: number;
  messageHistoryVisible: boolean;
  slowModeDelay: number;
  allowSendMessages: boolean;
  allowSendMedia: boolean;
  allowSendAudios: boolean;
  allowSendDocuments: boolean;
  allowSendPhotos: boolean;
  allowSendVideos: boolean;
  allowSendVideoNotes: boolean;
  allowSendVoiceNotes: boolean;
  allowSendOtherMessages: boolean;
  allowAddWebPagePreviews: boolean;
  allowPolls: boolean;
  allowInviteUsers: boolean;
  allowPinMessages: boolean;
  allowChangeInfo: boolean;
  allowManageTopics: boolean;
}

interface PendingGroupJoinRequest {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  date: number;
  inviteLink?: string;
}

interface GroupMemberRestrictionDraft {
  canSendMessages: boolean;
  canSendAudios: boolean;
  canSendDocuments: boolean;
  canSendPhotos: boolean;
  canSendVideos: boolean;
  canSendVideoNotes: boolean;
  canSendVoiceNotes: boolean;
  canSendPolls: boolean;
  canSendOtherMessages: boolean;
  canAddWebPagePreviews: boolean;
  canInviteUsers: boolean;
  canChangeInfo: boolean;
  canPinMessages: boolean;
  canManageTopics: boolean;
  untilHours: string;
}

interface GroupMemberMeta {
  customTitle?: string;
  tag?: string;
}

interface ChannelAdminRightsDraft {
  canManageChat: boolean;
  canPostMessages: boolean;
  canEditMessages: boolean;
  canDeleteMessages: boolean;
  canInviteUsers: boolean;
  canChangeInfo: boolean;
  canManageDirectMessages: boolean;
}

interface GroupMenuButtonDraft {
  scope: 'default' | 'private-chat';
  targetChatId: string;
  type: 'default' | 'commands' | 'web_app';
  text: string;
  webAppUrl: string;
}

interface BusinessRightsDraft {
  canReply: boolean;
  canReadMessages: boolean;
  canDeleteSentMessages: boolean;
  canDeleteAllMessages: boolean;
  canEditName: boolean;
  canEditBio: boolean;
  canEditProfilePhoto: boolean;
  canEditUsername: boolean;
}

interface KeyboardRequestUsersModalState {
  buttonText: string;
  request: NonNullable<ReplyKeyboardButton['request_users']>;
  candidates: Array<{
    userId: number;
    firstName: string;
    username?: string;
    isBot: boolean;
  }>;
  selectedUserIds: number[];
}

interface KeyboardRequestChatModalState {
  buttonText: string;
  request: NonNullable<ReplyKeyboardButton['request_chat']>;
  candidates: GroupChatItem[];
  selectedChatId: number | null;
}

interface ActiveChatActionState {
  action: string;
  actorName: string;
  expiresAt: number;
}

interface InvoiceMetaState {
  photoUrl?: string;
  maxTipAmount?: number;
  suggestedTipAmounts?: number[];
  needName?: boolean;
  needPhoneNumber?: boolean;
  needEmail?: boolean;
  needShippingAddress?: boolean;
  isFlexible?: boolean;
  sendPhoneNumberToProvider?: boolean;
  sendEmailToProvider?: boolean;
}

interface ForumTopicState {
  messageThreadId: number;
  name: string;
  iconColor: number;
  iconCustomEmojiId?: string;
  isClosed: boolean;
  isHidden: boolean;
  isGeneral: boolean;
  updatedAt?: number;
}

interface ForumTopicContextMenuState {
  x: number;
  y: number;
  topic: ForumTopicState;
}

const isMessageParseMode = (value: unknown): value is MessageParseMode => (
  value === 'Markdown' || value === 'MarkdownV2' || value === 'HTML'
);

function normalizeForumTopics(
  topics: ForumTopicState[],
  options: { includeGeneralFallback?: boolean } = {},
): ForumTopicState[] {
  const includeGeneralFallback = options.includeGeneralFallback !== false;
  const byThreadId = new Map<number, ForumTopicState>();
  topics.forEach((topic) => {
    const threadId = Math.floor(Number(topic.messageThreadId));
    if (!Number.isFinite(threadId) || threadId <= 0) {
      return;
    }

    if (
      !includeGeneralFallback
      && Boolean(topic.isGeneral)
      && threadId === GENERAL_FORUM_TOPIC_THREAD_ID
      && (!topic.updatedAt || topic.name === 'General')
    ) {
      return;
    }

    byThreadId.set(threadId, {
      messageThreadId: threadId,
      name: topic.name || `Topic ${threadId}`,
      iconColor: Number.isFinite(Number(topic.iconColor)) ? Math.floor(Number(topic.iconColor)) : DEFAULT_FORUM_ICON_COLOR,
      iconCustomEmojiId: topic.iconCustomEmojiId || undefined,
      isClosed: Boolean(topic.isClosed),
      isHidden: Boolean(topic.isHidden),
      isGeneral: includeGeneralFallback
        ? (topic.isGeneral || threadId === GENERAL_FORUM_TOPIC_THREAD_ID)
        : false,
      updatedAt: Number.isFinite(Number(topic.updatedAt)) ? Math.floor(Number(topic.updatedAt)) : undefined,
    });
  });

  if (includeGeneralFallback && !byThreadId.has(GENERAL_FORUM_TOPIC_THREAD_ID)) {
    byThreadId.set(GENERAL_FORUM_TOPIC_THREAD_ID, {
      messageThreadId: GENERAL_FORUM_TOPIC_THREAD_ID,
      name: 'General',
      iconColor: DEFAULT_FORUM_ICON_COLOR,
      iconCustomEmojiId: undefined,
      isClosed: false,
      isHidden: false,
      isGeneral: true,
    });
  }

  return Array.from(byThreadId.values()).sort((a, b) => {
    if (includeGeneralFallback) {
      if (a.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID) {
        return -1;
      }
      if (b.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID) {
        return 1;
      }
    }
    const left = a.updatedAt || 0;
    const right = b.updatedAt || 0;
    if (left !== right) {
      return right - left;
    }
    return a.name.localeCompare(b.name);
  });
}

function splitForumTopicNameWithEmoji(rawName: string): { emoji: string; name: string } {
  const normalized = rawName.trim();
  if (!normalized) {
    return { emoji: '', name: '' };
  }

  const matched = FORUM_TOPIC_EMOJI_PRESETS.find((emoji) => (
    normalized === emoji || normalized.startsWith(`${emoji} `)
  ));
  if (!matched) {
    return { emoji: '', name: normalized };
  }

  const name = normalized === matched
    ? ''
    : normalized.slice(matched.length).trim();
  return { emoji: matched, name };
}

function buildForumTopicNameWithEmoji(rawName: string, emoji: string): string {
  const name = rawName.trim();
  const normalizedEmoji = emoji.trim();
  if (!normalizedEmoji) {
    return name;
  }
  if (!name) {
    return normalizedEmoji;
  }
  if (name === normalizedEmoji || name.startsWith(`${normalizedEmoji} `)) {
    return name;
  }
  return `${normalizedEmoji} ${name}`;
}

function buildForumTopicNameForIconMode(rawName: string, normalEmoji: string, iconCustomEmojiId?: string): string {
  const customId = iconCustomEmojiId?.trim() || '';
  if (customId) {
    return rawName.trim();
  }
  return buildForumTopicNameWithEmoji(rawName, normalEmoji);
}

function formatChatActionLabel(action: string): string {
  switch (action) {
    case 'typing':
      return 'typing';
    case 'upload_photo':
      return 'uploading a photo';
    case 'record_video':
      return 'recording a video';
    case 'upload_video':
      return 'uploading a video';
    case 'record_voice':
      return 'recording a voice message';
    case 'upload_voice':
      return 'uploading a voice message';
    case 'upload_document':
      return 'uploading a file';
    case 'choose_sticker':
      return 'choosing a sticker';
    case 'find_location':
      return 'sharing location';
    case 'record_video_note':
      return 'recording a video note';
    case 'upload_video_note':
      return 'uploading a video note';
    default:
      return action;
  }
}

function extractBotCommandTargetUsername(text?: string, entities?: MessageEntity[]): string | null {
  if (!text || !text.trim()) {
    return null;
  }

  const commandEntities = [...(entities || [])]
    .filter((entity) => entity.type === 'bot_command' && entity.length > 0)
    .sort((a, b) => a.offset - b.offset);

  for (let index = 0; index < commandEntities.length; index += 1) {
    const entity = commandEntities[index];
    const chunk = text.slice(entity.offset, entity.offset + entity.length).trim();
    const targetedMatch = chunk.match(/^\/[A-Za-z][A-Za-z0-9_]{0,31}@([A-Za-z0-9_]{5,32})$/);
    if (targetedMatch?.[1]) {
      return targetedMatch[1];
    }
  }

  const fallbackMatch = text.match(/(?:^|\s)\/[A-Za-z][A-Za-z0-9_]{0,31}@([A-Za-z0-9_]{5,32})(?=\s|$)/);
  return fallbackMatch?.[1] || null;
}

function extractChatMemberStatus(member?: Record<string, unknown>): string | undefined {
  const raw = member?.status;
  return typeof raw === 'string' ? raw : undefined;
}

function extractChatMemberUser(member?: Record<string, unknown>): { id: number; firstName?: string } | null {
  const rawUser = member?.user;
  if (!rawUser || typeof rawUser !== 'object') {
    return null;
  }
  const user = rawUser as Record<string, unknown>;
  const id = typeof user.id === 'number' ? user.id : Number(user.id);
  if (!Number.isFinite(id)) {
    return null;
  }
  const firstName = typeof user.first_name === 'string' ? user.first_name : undefined;
  return { id, firstName };
}

function isJoinedMembershipStatus(status?: string): boolean {
  if (!status) {
    return false;
  }
  return ['joined', 'member', 'restricted', 'admin', 'owner', 'administrator', 'creator'].includes(status);
}

function canEditGroupByStatus(status?: string): boolean {
  if (!status) {
    return false;
  }
  return ['owner', 'admin', 'creator', 'administrator'].includes(status);
}

function canDeleteGroupByStatus(status?: string): boolean {
  if (!status) {
    return false;
  }
  return ['owner', 'creator'].includes(status);
}

function normalizeMembershipStatus(status?: string): string {
  if (!status) {
    return 'left';
  }
  if (status === 'creator') {
    return 'owner';
  }
  if (status === 'administrator') {
    return 'admin';
  }
  if (status === 'member') {
    return 'member';
  }
  if (status === 'kicked') {
    return 'banned';
  }
  return status;
}

function defaultGroupSettings(): GroupSettingsSnapshot {
  return {
    showAuthorSignature: false,
    directMessagesEnabled: false,
    directMessagesStarCount: 0,
    messageHistoryVisible: true,
    slowModeDelay: 0,
    allowSendMessages: true,
    allowSendMedia: true,
    allowSendAudios: true,
    allowSendDocuments: true,
    allowSendPhotos: true,
    allowSendVideos: true,
    allowSendVideoNotes: true,
    allowSendVoiceNotes: true,
    allowSendOtherMessages: true,
    allowAddWebPagePreviews: true,
    allowPolls: true,
    allowInviteUsers: true,
    allowPinMessages: false,
    allowChangeInfo: false,
    allowManageTopics: false,
  };
}

function mapServerSettingsToSnapshot(raw?: {
  show_author_signature?: boolean;
  direct_messages_enabled?: boolean;
  direct_messages_star_count?: number;
  message_history_visible?: boolean;
  slow_mode_delay?: number;
  permissions?: unknown;
}): GroupSettingsSnapshot {
  const defaults = defaultGroupSettings();
  const permissions = raw?.permissions && typeof raw.permissions === 'object'
    ? (raw.permissions as Record<string, unknown>)
    : {};

  const allowSendAudios = permissions['can_send_audios'] !== false;
  const allowSendDocuments = permissions['can_send_documents'] !== false;
  const allowSendPhotos = permissions['can_send_photos'] !== false;
  const allowSendVideos = permissions['can_send_videos'] !== false;
  const allowSendVideoNotes = permissions['can_send_video_notes'] !== false;
  const allowSendVoiceNotes = permissions['can_send_voice_notes'] !== false;
  const allowSendOtherMessages = permissions['can_send_other_messages'] !== false;
  const allowAddWebPagePreviews = permissions['can_add_web_page_previews'] !== false;
  const mediaAllowed = allowSendAudios
    && allowSendDocuments
    && allowSendPhotos
    && allowSendVideos
    && allowSendVideoNotes
    && allowSendVoiceNotes
    && allowSendOtherMessages
    && allowAddWebPagePreviews;

  return {
    showAuthorSignature: raw?.show_author_signature ?? defaults.showAuthorSignature,
    directMessagesEnabled: raw?.direct_messages_enabled ?? defaults.directMessagesEnabled,
    directMessagesStarCount: Math.max(0, Math.floor(Number(raw?.direct_messages_star_count ?? defaults.directMessagesStarCount) || 0)),
    messageHistoryVisible: raw?.message_history_visible ?? defaults.messageHistoryVisible,
    slowModeDelay: Math.max(0, Math.floor(Number(raw?.slow_mode_delay ?? defaults.slowModeDelay) || 0)),
    allowSendMessages: permissions['can_send_messages'] !== false,
    allowSendMedia: mediaAllowed,
    allowSendAudios,
    allowSendDocuments,
    allowSendPhotos,
    allowSendVideos,
    allowSendVideoNotes,
    allowSendVoiceNotes,
    allowSendOtherMessages,
    allowAddWebPagePreviews,
    allowPolls: permissions['can_send_polls'] !== false,
    allowInviteUsers: permissions['can_invite_users'] !== false,
    allowPinMessages: permissions['can_pin_messages'] === true,
    allowChangeInfo: permissions['can_change_info'] === true,
    allowManageTopics: permissions['can_manage_topics'] === true,
  };
}

function defaultBusinessRightsDraft(): BusinessRightsDraft {
  return {
    canReply: true,
    canReadMessages: true,
    canDeleteSentMessages: true,
    canDeleteAllMessages: true,
    canEditName: true,
    canEditBio: true,
    canEditProfilePhoto: true,
    canEditUsername: true,
  };
}

function toBusinessBotRights(draft: BusinessRightsDraft): GeneratedBusinessBotRights {
  return {
    can_reply: draft.canReply,
    can_read_messages: draft.canReadMessages,
    can_delete_sent_messages: draft.canDeleteSentMessages,
    can_delete_all_messages: draft.canDeleteAllMessages,
    can_edit_name: draft.canEditName,
    can_edit_bio: draft.canEditBio,
    can_edit_profile_photo: draft.canEditProfilePhoto,
    can_edit_username: draft.canEditUsername,
  };
}

function mapBusinessRightsToDraft(rights?: GeneratedBusinessBotRights): BusinessRightsDraft {
  const defaults = defaultBusinessRightsDraft();
  if (!rights) {
    return defaults;
  }
  return {
    canReply: rights.can_reply ?? defaults.canReply,
    canReadMessages: rights.can_read_messages ?? defaults.canReadMessages,
    canDeleteSentMessages: rights.can_delete_sent_messages ?? defaults.canDeleteSentMessages,
    canDeleteAllMessages: rights.can_delete_all_messages ?? defaults.canDeleteAllMessages,
    canEditName: rights.can_edit_name ?? defaults.canEditName,
    canEditBio: rights.can_edit_bio ?? defaults.canEditBio,
    canEditProfilePhoto: rights.can_edit_profile_photo ?? defaults.canEditProfilePhoto,
    canEditUsername: rights.can_edit_username ?? defaults.canEditUsername,
  };
}

function mapSnapshotToServerPermissions(snapshot: GroupSettingsSnapshot): Record<string, boolean> {
  return {
    can_send_messages: snapshot.allowSendMessages,
    can_send_audios: snapshot.allowSendAudios,
    can_send_documents: snapshot.allowSendDocuments,
    can_send_photos: snapshot.allowSendPhotos,
    can_send_videos: snapshot.allowSendVideos,
    can_send_video_notes: snapshot.allowSendVideoNotes,
    can_send_voice_notes: snapshot.allowSendVoiceNotes,
    can_send_polls: snapshot.allowPolls,
    can_send_other_messages: snapshot.allowSendOtherMessages,
    can_add_web_page_previews: snapshot.allowAddWebPagePreviews,
    can_change_info: snapshot.allowChangeInfo,
    can_invite_users: snapshot.allowInviteUsers,
    can_pin_messages: snapshot.allowPinMessages,
    can_manage_topics: snapshot.allowManageTopics,
    can_edit_tag: false,
  };
}

function fullMemberPermissions(): Record<string, boolean> {
  return {
    can_send_messages: true,
    can_send_audios: true,
    can_send_documents: true,
    can_send_photos: true,
    can_send_videos: true,
    can_send_video_notes: true,
    can_send_voice_notes: true,
    can_send_polls: true,
    can_send_other_messages: true,
    can_add_web_page_previews: true,
    can_change_info: true,
    can_invite_users: true,
    can_pin_messages: true,
    can_manage_topics: true,
    can_edit_tag: true,
  };
}

function defaultGroupMemberRestrictionDraft(): GroupMemberRestrictionDraft {
  return {
    canSendMessages: true,
    canSendAudios: true,
    canSendDocuments: true,
    canSendPhotos: true,
    canSendVideos: true,
    canSendVideoNotes: true,
    canSendVoiceNotes: true,
    canSendPolls: true,
    canSendOtherMessages: true,
    canAddWebPagePreviews: true,
    canInviteUsers: true,
    canChangeInfo: true,
    canPinMessages: true,
    canManageTopics: true,
    untilHours: '168',
  };
}

function mapRestrictionDraftToPermissions(draft: GroupMemberRestrictionDraft): Record<string, boolean> {
  return {
    can_send_messages: draft.canSendMessages,
    can_send_audios: draft.canSendAudios,
    can_send_documents: draft.canSendDocuments,
    can_send_photos: draft.canSendPhotos,
    can_send_videos: draft.canSendVideos,
    can_send_video_notes: draft.canSendVideoNotes,
    can_send_voice_notes: draft.canSendVoiceNotes,
    can_send_polls: draft.canSendPolls,
    can_send_other_messages: draft.canSendOtherMessages,
    can_add_web_page_previews: draft.canAddWebPagePreviews,
    can_change_info: draft.canChangeInfo,
    can_invite_users: draft.canInviteUsers,
    can_pin_messages: draft.canPinMessages,
    can_manage_topics: draft.canManageTopics,
    can_edit_tag: false,
  };
}

function parseGroupMemberMeta(member: GeneratedChatMember): GroupMemberMeta {
  const raw = member as Record<string, unknown>;
  return {
    customTitle: typeof raw.custom_title === 'string' ? raw.custom_title : undefined,
    tag: typeof raw.tag === 'string' ? raw.tag : undefined,
  };
}

function defaultChannelAdminRightsDraft(): ChannelAdminRightsDraft {
  return {
    canManageChat: true,
    canPostMessages: true,
    canEditMessages: true,
    canDeleteMessages: true,
    canInviteUsers: true,
    canChangeInfo: true,
    canManageDirectMessages: true,
  };
}

function parseChannelAdminRightsDraft(member: GeneratedChatMember): ChannelAdminRightsDraft | null {
  const raw = member as Record<string, unknown>;
  const status = typeof raw.status === 'string' ? raw.status : '';
  if (status !== 'administrator' && status !== 'creator') {
    return null;
  }

  const canManageChat = Boolean(raw.can_manage_chat);
  return {
    canManageChat,
    canPostMessages: Boolean(raw.can_post_messages),
    canEditMessages: Boolean(raw.can_edit_messages),
    canDeleteMessages: Boolean(raw.can_delete_messages),
    canInviteUsers: Boolean(raw.can_invite_users),
    canChangeInfo: Boolean(raw.can_change_info),
    canManageDirectMessages: Boolean(raw.can_manage_direct_messages),
  };
}

function parseForwardOriginLabel(rawOrigin: unknown): { label?: string; date?: number } {
  if (!rawOrigin || typeof rawOrigin !== 'object') {
    return {};
  }

  const origin = rawOrigin as Record<string, unknown>;
  const originType = typeof origin.type === 'string' ? origin.type : '';
  const rawDate = Number(origin.date);
  const date = Number.isFinite(rawDate) ? Math.floor(rawDate) : undefined;

  const toName = (value: unknown): string | undefined => {
    if (!value || typeof value !== 'object') {
      return undefined;
    }
    const record = value as Record<string, unknown>;
    if (typeof record.title === 'string' && record.title.trim()) {
      return record.title;
    }
    if (typeof record.first_name === 'string' && record.first_name.trim()) {
      return record.first_name;
    }
    if (typeof record.username === 'string' && record.username.trim()) {
      return `@${record.username}`;
    }
    const id = Math.floor(Number(record.id));
    if (Number.isFinite(id) && id !== 0) {
      return `chat_${id}`;
    }
    return undefined;
  };

  if (originType === 'channel') {
    return {
      label: toName(origin.chat) || 'a channel',
      date,
    };
  }

  if (originType === 'chat') {
    return {
      label: toName(origin.sender_chat) || 'a chat',
      date,
    };
  }

  if (originType === 'user') {
    return {
      label: toName(origin.sender_user) || 'a user',
      date,
    };
  }

  if (originType === 'hidden_user') {
    const hiddenName = typeof origin.sender_user_name === 'string' && origin.sender_user_name.trim()
      ? origin.sender_user_name
      : 'Hidden User';
    return {
      label: hiddenName,
      date,
    };
  }

  return {
    label: toName(origin.chat) || toName(origin.sender_chat) || toName(origin.sender_user),
    date,
  };
}

function normalizeThreadMatchText(value?: string): string {
  if (!value) {
    return '';
  }

  return value.trim().replace(/\s+/g, ' ').toLowerCase();
}

function isLikelySameChannelPost(candidate: ChatMessage, channelPost: ChatMessage): boolean {
  const candidateText = normalizeThreadMatchText(candidate.text);
  const channelPostText = normalizeThreadMatchText(channelPost.text);

  if (candidateText && channelPostText) {
    return candidateText === channelPostText;
  }

  if (channelPost.media?.type && candidate.media?.type) {
    return channelPost.media.type === candidate.media.type;
  }

  return false;
}

function findFallbackDiscussionRootMessage(
  discussionMessages: ChatMessage[],
  channelPost: ChatMessage,
): ChatMessage | undefined {
  const channelChatId = channelPost.chatId;
  const candidates = discussionMessages.filter((message) => (
    !message.service
    && message.senderChatId === channelChatId
  ));

  if (candidates.length === 0) {
    return undefined;
  }

  const ranked = [...candidates].sort((a, b) => {
    const aSamePost = isLikelySameChannelPost(a, channelPost);
    const bSamePost = isLikelySameChannelPost(b, channelPost);
    if (aSamePost !== bSamePost) {
      return aSamePost ? -1 : 1;
    }

    const aDateDelta = Math.abs(a.date - channelPost.date);
    const bDateDelta = Math.abs(b.date - channelPost.date);
    if (aDateDelta !== bDateDelta) {
      return aDateDelta - bDateDelta;
    }

    return a.id - b.id;
  });

  return ranked[0];
}

function collectDiscussionReplyTreeMessages(
  discussionMessages: ChatMessage[],
  rootMessageId: number,
): ChatMessage[] {
  const threadMessageIds = new Set<number>([rootMessageId]);
  let changed = true;

  while (changed) {
    changed = false;
    discussionMessages.forEach((message) => {
      const replyMessageId = message.replyTo?.messageId;
      if (
        typeof replyMessageId === 'number'
        && threadMessageIds.has(replyMessageId)
        && !threadMessageIds.has(message.id)
      ) {
        threadMessageIds.add(message.id);
        changed = true;
      }
    });
  }

  return discussionMessages.filter((message) => threadMessageIds.has(message.id));
}

function resolveRequestUsersMaxQuantity(
  request: NonNullable<ReplyKeyboardButton['request_users']>,
  fallbackMaxQuantity = 10,
): number {
  const rawMaxQuantity = Math.floor(Number(request.max_quantity));
  if (Number.isFinite(rawMaxQuantity) && rawMaxQuantity > 0) {
    return Math.max(1, Math.min(10, rawMaxQuantity));
  }

  // Bot API default for request_users allows selecting multiple users (up to 10).
  // When payload omits max_quantity or sends an invalid value, keep multi-select behavior.
  if (!Number.isFinite(rawMaxQuantity) || rawMaxQuantity <= 0) {
    return Math.max(1, Math.min(10, fallbackMaxQuantity || 10));
  }

  const normalizedFallback = Math.floor(Number(fallbackMaxQuantity));
  if (!Number.isFinite(normalizedFallback) || normalizedFallback <= 0) {
    return 10;
  }

  return Math.max(1, Math.min(10, normalizedFallback));
}

function parseMenuButtonSummary(menuButton: GeneratedMenuButton): { type: string; text?: string; url?: string } {
  const raw = menuButton as Record<string, unknown>;
  const webApp = raw.web_app;
  const webAppUrl = webApp && typeof webApp === 'object' && typeof (webApp as Record<string, unknown>).url === 'string'
    ? ((webApp as Record<string, unknown>).url as string)
    : undefined;

  return {
    type: typeof raw.type === 'string' ? raw.type : 'default',
    text: typeof raw.text === 'string' ? raw.text : undefined,
    url: webAppUrl,
  };
}

function buildMenuButtonFromDraft(draft: GroupMenuButtonDraft): GeneratedMenuButton {
  if (draft.type === 'commands') {
    const menuButton: GeneratedMenuButtonCommands = {
      type: 'commands',
    };
    return menuButton as unknown as GeneratedMenuButton;
  }

  if (draft.type === 'web_app') {
    const normalizedText = draft.text.trim() || 'Menu';
    const normalizedUrl = draft.webAppUrl.trim() || 'https://example.com';
    const menuButton: GeneratedMenuButtonWebApp = {
      type: 'web_app',
      text: normalizedText,
      web_app: {
        url: normalizedUrl,
      },
    };
    return menuButton as unknown as GeneratedMenuButton;
  }

  const menuButton: GeneratedMenuButtonDefault = {
    type: 'default',
  };
  return menuButton as unknown as GeneratedMenuButton;
}

export default function TelegramChatPage({ initialTab = 'chats' }: TelegramChatPageProps) {
  const [activeTab, setActiveTab] = useState<SidebarTab>(initialTab);
  const [chatScopeTab, setChatScopeTab] = useState<ChatScopeTab>(() => {
    const raw = localStorage.getItem(CHAT_SCOPE_KEY);
    if (raw === 'group' || raw === 'channel' || raw === 'private') {
      return raw;
    }
    return 'private';
  });
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
  const [groupChats, setGroupChats] = useState<GroupChatItem[]>(() => {
    try {
      const raw = localStorage.getItem(GROUP_CHATS_KEY);
      return raw ? (JSON.parse(raw) as GroupChatItem[]) : [];
    } catch {
      return [];
    }
  });
  const [selectedGroupByBot, setSelectedGroupByBot] = useState<Record<string, number>>(() => {
    try {
      const raw = localStorage.getItem(SELECTED_GROUP_BY_BOT_KEY);
      return raw ? (JSON.parse(raw) as Record<string, number>) : {};
    } catch {
      return {};
    }
  });
  const [selectedGroupChatId, setSelectedGroupChatId] = useState<number | null>(() => {
    try {
      const token = localStorage.getItem(SELECTED_BOT_KEY) || DEFAULT_BOT_TOKEN;
      const raw = localStorage.getItem(SELECTED_GROUP_BY_BOT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, number>) : {};
      const value = Number(parsed[token]);
      return Number.isFinite(value) ? value : null;
    } catch {
      return null;
    }
  });
  const [showCreateGroupForm, setShowCreateGroupForm] = useState(false);
  const [groupDraft, setGroupDraft] = useState({
    title: '',
    type: 'supergroup' as 'group' | 'supergroup' | 'channel',
    username: '',
    description: '',
    isForum: false,
  });
  const [isCreatingGroup, setIsCreatingGroup] = useState(false);
  const [groupMembershipByUser, setGroupMembershipByUser] = useState<Record<string, string>>(() => {
    try {
      const raw = localStorage.getItem(GROUP_MEMBERSHIP_KEY);
      return raw ? (JSON.parse(raw) as Record<string, string>) : {};
    } catch {
      return {};
    }
  });
  const [groupInviteLinkInput, setGroupInviteLinkInput] = useState('');
  const [groupInviteLinksByChat, setGroupInviteLinksByChat] = useState<Record<string, string>>(() => {
    try {
      const raw = localStorage.getItem(GROUP_INVITE_LINKS_KEY);
      return raw ? (JSON.parse(raw) as Record<string, string>) : {};
    } catch {
      return {};
    }
  });
  const [pendingJoinRequestsByChat, setPendingJoinRequestsByChat] = useState<Record<string, PendingGroupJoinRequest[]>>(() => {
    try {
      const raw = localStorage.getItem(GROUP_JOIN_REQUESTS_KEY);
      return raw ? (JSON.parse(raw) as Record<string, PendingGroupJoinRequest[]>) : {};
    } catch {
      return {};
    }
  });
  const [showGroupActionsModal, setShowGroupActionsModal] = useState(false);
  const [groupSettingsPage, setGroupSettingsPage] = useState<GroupSettingsPage>('home');
  const [showGroupProfileModal, setShowGroupProfileModal] = useState(false);
  const [groupProfileDraft, setGroupProfileDraft] = useState({
    title: '',
    username: '',
    description: '',
    isForum: false,
    showAuthorSignature: false,
    directMessagesEnabled: false,
    directMessagesStarCount: 0,
    messageHistoryVisible: true,
    slowModeDelay: 0,
    allowSendMessages: true,
    allowSendMedia: true,
    allowSendAudios: true,
    allowSendDocuments: true,
    allowSendPhotos: true,
    allowSendVideos: true,
    allowSendVideoNotes: true,
    allowSendVoiceNotes: true,
    allowSendOtherMessages: true,
    allowAddWebPagePreviews: true,
    allowPolls: true,
    allowInviteUsers: true,
    allowPinMessages: false,
    allowChangeInfo: false,
    allowManageTopics: false,
  });
  const [businessConnectionByUserKey, setBusinessConnectionByUserKey] = useState<Record<string, GeneratedBusinessConnection>>(() => {
    try {
      const raw = localStorage.getItem(BUSINESS_CONNECTIONS_KEY);
      return raw ? (JSON.parse(raw) as Record<string, GeneratedBusinessConnection>) : {};
    } catch {
      return {};
    }
  });
  const [businessConnectionDraftId, setBusinessConnectionDraftId] = useState('');
  const [businessConnectionDraftEnabled, setBusinessConnectionDraftEnabled] = useState(true);
  const [businessRightsDraft, setBusinessRightsDraft] = useState<BusinessRightsDraft>(defaultBusinessRightsDraft());
  const [businessDraftBotToken, setBusinessDraftBotToken] = useState<string>(selectedBotToken);
  const [isBusinessActionRunning, setIsBusinessActionRunning] = useState(false);
  const [groupMembersFilter, setGroupMembersFilter] = useState('');
  const [expandedGroupMemberId, setExpandedGroupMemberId] = useState<number | null>(null);
  const [groupMemberRestrictionDraftByChatKey, setGroupMemberRestrictionDraftByChatKey] = useState<Record<string, Record<number, GroupMemberRestrictionDraft>>>({});
  const [groupMemberAdminTitleByChatKey, setGroupMemberAdminTitleByChatKey] = useState<Record<string, Record<number, string>>>({});
  const [groupMemberTagByChatKey, setGroupMemberTagByChatKey] = useState<Record<string, Record<number, string>>>({});
  const [groupMemberMetaByChatKey, setGroupMemberMetaByChatKey] = useState<Record<string, Record<number, GroupMemberMeta>>>({});
  const [channelAdminRightsDraftByChatKey, setChannelAdminRightsDraftByChatKey] = useState<Record<string, Record<number, ChannelAdminRightsDraft>>>({});
  const [groupSenderChatModerationDraft, setGroupSenderChatModerationDraft] = useState('');
  const [groupInspectorOutput, setGroupInspectorOutput] = useState('');
  const [chatActionByChatKey, setChatActionByChatKey] = useState<Record<string, ActiveChatActionState>>({});
  const [invoiceMetaByMessageKey, setInvoiceMetaByMessageKey] = useState<Record<string, InvoiceMetaState>>(() => {
    try {
      const raw = localStorage.getItem(INVOICE_META_KEY);
      return raw ? (JSON.parse(raw) as Record<string, InvoiceMetaState>) : {};
    } catch {
      return {};
    }
  });
  const [forumTopicsByChatKey, setForumTopicsByChatKey] = useState<Record<string, ForumTopicState[]>>(() => {
    try {
      const raw = localStorage.getItem(FORUM_TOPICS_KEY);
      if (!raw) {
        return {};
      }

      const parsed = JSON.parse(raw) as Record<string, ForumTopicState[]>;
      const hydrated: Record<string, ForumTopicState[]> = {};
      Object.entries(parsed).forEach(([key, topics]) => {
        hydrated[key] = Array.isArray(topics) ? topics : [];
      });
      return hydrated;
    } catch {
      return {};
    }
  });
  const [selectedForumTopicByChatKey, setSelectedForumTopicByChatKey] = useState<Record<string, number>>(() => {
    try {
      const raw = localStorage.getItem(SELECTED_FORUM_TOPIC_KEY);
      if (!raw) {
        return {};
      }

      const parsed = JSON.parse(raw) as Record<string, number>;
      const normalized: Record<string, number> = {};
      Object.entries(parsed).forEach(([key, value]) => {
        const threadId = Math.floor(Number(value));
        if (Number.isFinite(threadId) && threadId > 0) {
          normalized[key] = threadId;
        }
      });
      return normalized;
    } catch {
      return {};
    }
  });
  const [forumTopicDraft, setForumTopicDraft] = useState({
    messageThreadId: '',
    name: '',
    normalEmoji: '',
    iconColor: String(DEFAULT_FORUM_ICON_COLOR),
    iconCustomEmojiId: '',
    generalName: 'General',
  });
  const [forumTopicIconStickers, setForumTopicIconStickers] = useState<Array<{
    file_id: string;
    emoji?: string;
    custom_emoji_id?: string;
  }>>([]);
  const [showForumTopicModal, setShowForumTopicModal] = useState(false);
  const [forumTopicModalMode, setForumTopicModalMode] = useState<'create' | 'edit'>('create');
  const [forumTopicModalThreadId, setForumTopicModalThreadId] = useState<number | null>(null);
  const [expandedForumTopicThreadId, setExpandedForumTopicThreadId] = useState<number | null>(null);
  const [forumTopicContextMenu, setForumTopicContextMenu] = useState<ForumTopicContextMenuState | null>(null);
  const [groupMenuButtonDraft, setGroupMenuButtonDraft] = useState<GroupMenuButtonDraft>({
    scope: 'default',
    targetChatId: '',
    type: 'default',
    text: '',
    webAppUrl: '',
  });
  const [groupMenuButtonSummary, setGroupMenuButtonSummary] = useState<{ type: string; text?: string; url?: string } | null>(null);
  const [groupPrivacyModeEnabled, setGroupPrivacyModeEnabled] = useState(true);
  const [isGroupPrivacyModeLoading, setIsGroupPrivacyModeLoading] = useState(false);
  const [groupInviteEditorDraft, setGroupInviteEditorDraft] = useState({
    inviteLink: '',
    name: '',
    expireDate: '',
    memberLimit: '',
    createsJoinRequest: false,
    subscriptionPeriod: '2592000',
    subscriptionPrice: '100',
  });
  const [channelDiscussionLinkDraft, setChannelDiscussionLinkDraft] = useState('');
  const [groupStickerSetDraft, setGroupStickerSetDraft] = useState('');
  const [removeGroupStickerSetDraft, setRemoveGroupStickerSetDraft] = useState(false);
  const [groupPhotoDraftFile, setGroupPhotoDraftFile] = useState<File | null>(null);
  const [removeGroupPhotoDraft, setRemoveGroupPhotoDraft] = useState(false);
  const [isGroupActionRunning, setIsGroupActionRunning] = useState(false);
  const [pinnedMessageByChatKey, setPinnedMessageByChatKey] = useState<Record<string, number[]>>(() => {
    try {
      const raw = localStorage.getItem(GROUP_PINNED_MESSAGES_KEY);
      return raw ? (JSON.parse(raw) as Record<string, number[]>) : {};
    } catch {
      return {};
    }
  });
  const [pinnedPreviewIndex, setPinnedPreviewIndex] = useState(0);
  const [composerText, setComposerText] = useState('');
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    try {
      const raw = localStorage.getItem(MESSAGES_KEY);
      return raw ? (JSON.parse(raw) as ChatMessage[]) : [];
    } catch {
      return [];
    }
  });
  const [lastUpdateByBot, setLastUpdateByBot] = useState<Record<string, number>>(() => {
    try {
      const raw = localStorage.getItem(LAST_UPDATES_KEY);
      return raw ? (JSON.parse(raw) as Record<string, number>) : {};
    } catch {
      return {};
    }
  });
  const [lastUpdateId, setLastUpdateId] = useState<number>(() => {
    try {
      const raw = localStorage.getItem(LAST_UPDATES_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, number>) : {};
      const saved = parsed[selectedBotToken];
      return Number.isFinite(saved) && saved > 0 ? saved : 0;
    } catch {
      return 0;
    }
  });
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
  const [commentSourceByDiscussionChatKey, setCommentSourceByDiscussionChatKey] = useState<
    Record<string, { channelChatId: number; channelMessageId: number; discussionRootMessageId?: number }>
  >({});
  const [messageMenu, setMessageMenu] = useState<{ messageId: number; x: number; y: number } | null>(null);
  const [forwardMessageId, setForwardMessageId] = useState<number | null>(null);
  const [forwardTargetChatId, setForwardTargetChatId] = useState('');
  const [keyboardRequestUsersModal, setKeyboardRequestUsersModal] = useState<KeyboardRequestUsersModalState | null>(null);
  const [keyboardRequestChatModal, setKeyboardRequestChatModal] = useState<KeyboardRequestChatModalState | null>(null);
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
  const [dismissedOneTimeKeyboards, setDismissedOneTimeKeyboards] = useState<Record<string, number>>({});
  const [activeInlineQueryId, setActiveInlineQueryId] = useState<string | null>(null);
  const [inlineResults, setInlineResults] = useState<InlineQueryResult[]>([]);
  const [inlineNextOffset, setInlineNextOffset] = useState<string | null>(null);
  const [inlineModeError, setInlineModeError] = useState('');
  const [isInlineModeSending, setIsInlineModeSending] = useState(false);
  const [callbackToast, setCallbackToast] = useState<string | null>(null);
  const [callbackModalText, setCallbackModalText] = useState<string | null>(null);
  const [pollSelections, setPollSelections] = useState<Record<string, number[]>>({});
  const [pollVotersByPollId, setPollVotersByPollId] = useState<Record<string, PollVoterInfo[]>>({});
  const [pollAnonymousByPollId, setPollAnonymousByPollId] = useState<Record<string, boolean>>({});
  const [expandedPollVoters, setExpandedPollVoters] = useState<Record<string, boolean>>({});
  const [pollVotersLoading, setPollVotersLoading] = useState<Record<string, boolean>>({});
  const [selectedDiceEmoji, setSelectedDiceEmoji] = useState<(typeof DICE_EMOJIS)[number]>('🎲');
  const [gameShortNameDraft, setGameShortNameDraft] = useState('');
  const [showMediaDrawer, setShowMediaDrawer] = useState(false);
  const [mediaDrawerTab, setMediaDrawerTab] = useState<MediaDrawerTab>('stickers');
  const [shareDraft, setShareDraft] = useState({
    phoneNumber: '+10000000000',
    contactFirstName: '',
    contactLastName: '',
    latitude: '35.6892',
    longitude: '51.3890',
    venueTitle: 'Coffee Spot',
    venueAddress: 'Main Street',
  });
  const [pollBuilder, setPollBuilder] = useState({
    type: 'regular' as 'regular' | 'quiz',
    question: '',
    options: ['', ''],
    optionsParseMode: 'none' as ComposerParseMode,
    isAnonymous: false,
    allowsRevoting: true,
    allowsMultipleAnswers: false,
    correctOptionIds: [0],
    explanation: '',
    questionParseMode: 'none' as ComposerParseMode,
    explanationParseMode: 'none' as ComposerParseMode,
    description: '',
    descriptionParseMode: 'none' as ComposerParseMode,
    openPeriod: '',
    closeDate: '',
    isClosed: false,
  });
  const [invoiceBuilder, setInvoiceBuilder] = useState({
    title: '',
    description: '',
    amount: '100',
    currency: 'USD',
    payload: '',
    maxTipAmount: '',
    suggestedTips: '',
    photoUrl: '',
    needShippingAddress: false,
    isFlexible: false,
    needName: false,
    needPhoneNumber: false,
    needEmail: false,
    sendPhoneNumberToProvider: false,
    sendEmailToProvider: false,
  });
  const [stickerStudio, setStickerStudio] = useState({
    userId: String(DEFAULT_USER.id),
    setName: '',
    setTitle: '',
    stickerType: 'regular',
    stickerFormat: 'static',
    needsRepainting: false,
    emojiList: '😀',
    keywords: '',
    oldStickerId: '',
    targetStickerId: '',
    position: '0',
    customEmojiId: '',
    maskPoint: 'forehead',
    maskXShift: '0',
    maskYShift: '0',
    maskScale: '1',
    sendEmoji: '😀',
    sendDuration: '5',
    sendLength: '384',
  });
  const [stickerStudioFile, setStickerStudioFile] = useState<File | null>(null);
  const [stickerStudioThumbnailFile, setStickerStudioThumbnailFile] = useState<File | null>(null);
  const [uploadedStickerFileId, setUploadedStickerFileId] = useState('');
  const [stickerStudioOutput, setStickerStudioOutput] = useState('');
  const [stickerShelf, setStickerShelf] = useState<StickerShelfSet[]>([]);
  const [stickerShelfActiveSet, setStickerShelfActiveSet] = useState('');
  const [stickerPreviewFailedByFileId, setStickerPreviewFailedByFileId] = useState<Record<string, boolean>>({});
  const [stickerShelfNameInput, setStickerShelfNameInput] = useState('');
  const [animationUploadFile, setAnimationUploadFile] = useState<File | null>(null);
  const [voiceUploadFile, setVoiceUploadFile] = useState<File | null>(null);
  const [videoNoteUploadFile, setVideoNoteUploadFile] = useState<File | null>(null);
  const [canUseMicrophone, setCanUseMicrophone] = useState(false);
  const [canUseCamera, setCanUseCamera] = useState(false);
  const [isRecordingVoice, setIsRecordingVoice] = useState(false);
  const [recordedVoiceBlob, setRecordedVoiceBlob] = useState<Blob | null>(null);
  const [voiceRecordError, setVoiceRecordError] = useState('');
  const [playingVideoNoteMessageId, setPlayingVideoNoteMessageId] = useState<number | null>(null);
  const [paymentMethodByInvoice, setPaymentMethodByInvoice] = useState<Record<number, PaymentMethod>>({});
  const [paymentTipByInvoice, setPaymentTipByInvoice] = useState<Record<number, string>>({});
  const [checkoutFlow, setCheckoutFlow] = useState<CheckoutFlowState | null>(null);
  const [walletByUserId, setWalletByUserId] = useState<Record<string, WalletState>>(() => {
    try {
      const raw = localStorage.getItem(USER_WALLETS_KEY);
      if (!raw) {
        return {};
      }

      const parsed = JSON.parse(raw) as Record<string, Partial<WalletState>>;
      const normalized: Record<string, WalletState> = {};
      Object.entries(parsed).forEach(([userId, wallet]) => {
        normalized[userId] = normalizeWalletState(wallet);
      });
      return normalized;
    } catch {
      return {};
    }
  });
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
  const selectedUserWalletKey = String(selectedUser.id);
  const walletState = walletByUserId[selectedUserWalletKey] || DEFAULT_WALLET_STATE;
  const setWalletState = (next: WalletState | ((prev: WalletState) => WalletState)) => {
    setWalletByUserId((prev) => {
      const current = prev[selectedUserWalletKey] || DEFAULT_WALLET_STATE;
      const resolved = typeof next === 'function'
        ? (next as (value: WalletState) => WalletState)(current)
        : next;
      const normalized = normalizeWalletState(resolved);

      if (current.fiat === normalized.fiat && current.stars === normalized.stars) {
        return prev;
      }

      return {
        ...prev,
        [selectedUserWalletKey]: normalized,
      };
    });
  };

  const selectedBusinessConnectionStateKey = `${selectedBotToken}:${selectedUser.id}`;
  const businessDraftStateKey = `${businessDraftBotToken}:${selectedUser.id}`;
  const selectedBusinessConnection = businessConnectionByUserKey[selectedBusinessConnectionStateKey];
  const activeBusinessConnectionId = chatScopeTab === 'private' && selectedBusinessConnection?.is_enabled
    ? selectedBusinessConnection.id
    : undefined;

  const inlineTrigger = useMemo(() => {
    const username = selectedBot?.username?.toLowerCase();
    if (!username) {
      return null;
    }

    const trimmed = composerText.trimStart();
    const match = trimmed.match(/^@([A-Za-z0-9_]{3,64})(?:\s+([\s\S]*))?$/);
    if (!match) {
      return null;
    }

    if (match[1].toLowerCase() !== username) {
      return null;
    }

    return {
      query: (match[2] || '').trim(),
    };
  }, [composerText, selectedBot?.username]);

  const scopedGroupChats = useMemo(() => {
    if (chatScopeTab === 'group') {
      return groupChats.filter((group) => group.type === 'group' || group.type === 'supergroup');
    }
    if (chatScopeTab === 'channel') {
      return groupChats.filter((group) => group.type === 'channel');
    }
    return [] as GroupChatItem[];
  }, [chatScopeTab, groupChats]);

  const filteredGroups = useMemo(() => {
    const keyword = chatSearch.trim().toLowerCase();
    if (!keyword) {
      return scopedGroupChats;
    }

    return scopedGroupChats.filter((group) => {
      const title = group.title.toLowerCase();
      const username = (group.username || '').toLowerCase();
      const idText = String(group.id);
      return title.includes(keyword) || username.includes(keyword) || idText.includes(keyword);
    });
  }, [chatSearch, scopedGroupChats]);

  const selectedGroup = useMemo(
    () => groupChats.find((group) => group.id === selectedGroupChatId) || null,
    [groupChats, selectedGroupChatId],
  );

  const channelDiscussionCandidates = useMemo(() => {
    if (!selectedGroup || selectedGroup.type !== 'channel') {
      return [] as GroupChatItem[];
    }

    return groupChats
      .filter((group) => (
        group.id !== selectedGroup.id
        && (group.type === 'group' || group.type === 'supergroup')
        && !group.isDirectMessages
      ))
      .sort((a, b) => a.title.localeCompare(b.title));
  }, [groupChats, selectedGroup]);

  const groupMembershipKey = selectedGroup
    ? `${selectedBotToken}:${selectedGroup.id}:${selectedUser.id}`
    : null;
  const selectedGroupStateKey = selectedGroup
    ? `${selectedBotToken}:${selectedGroup.id}`
    : null;
  const groupMembershipStatus = groupMembershipKey ? groupMembershipByUser[groupMembershipKey] : undefined;
  const groupMembership = isJoinedMembershipStatus(groupMembershipStatus) ? 'joined' : 'left';
  const isDirectMessagesGroup = Boolean(selectedGroup?.isDirectMessages);
  const canLeaveSelectedGroup = !isDirectMessagesGroup
    && groupMembership === 'joined'
    && normalizeMembershipStatus(groupMembershipStatus) !== 'owner';
  const canEditSelectedGroup = !isDirectMessagesGroup && canEditGroupByStatus(groupMembershipStatus);
  const canDeleteSelectedGroup = !isDirectMessagesGroup && canDeleteGroupByStatus(groupMembershipStatus);
  const selectedDirectMessagesParentStateKey = selectedGroup?.isDirectMessages && selectedGroup.parentChannelChatId
    ? `${selectedBotToken}:${selectedGroup.parentChannelChatId}`
    : null;
  const selectedDirectMessagesParentMembershipStatus = selectedGroup?.isDirectMessages && selectedGroup.parentChannelChatId
    ? normalizeMembershipStatus(groupMembershipByUser[`${selectedBotToken}:${selectedGroup.parentChannelChatId}:${selectedUser.id}`])
    : 'left';
  const normalizedSelectedGroupMembershipStatus = normalizeMembershipStatus(groupMembershipStatus);
  const selectedDirectMessagesAdminRights = selectedDirectMessagesParentStateKey
    ? channelAdminRightsDraftByChatKey[selectedDirectMessagesParentStateKey]?.[selectedUser.id]
    : undefined;
  const isSelectedUserDirectMessagesManager = Boolean(
    selectedGroup?.isDirectMessages
    && (
      selectedDirectMessagesParentMembershipStatus === 'owner'
      || (
        selectedDirectMessagesParentMembershipStatus === 'admin'
        && selectedDirectMessagesAdminRights?.canManageDirectMessages === true
      )
    ),
  );
  const selectedChannelAdminRights = selectedGroupStateKey
    ? channelAdminRightsDraftByChatKey[selectedGroupStateKey]?.[selectedUser.id]
    : undefined;
  const canSelectedUserManageChannelDirectMessages = Boolean(
    selectedGroup?.type === 'channel'
    && !selectedGroup?.isDirectMessages
    && (
      normalizedSelectedGroupMembershipStatus === 'owner'
      || (
        normalizedSelectedGroupMembershipStatus === 'admin'
        && selectedChannelAdminRights?.canManageDirectMessages === true
      )
    ),
  );
  const selectedDirectMessagesParentGroup = selectedGroup?.isDirectMessages && selectedGroup.parentChannelChatId
    ? groupChats.find((chat) => chat.id === selectedGroup.parentChannelChatId)
    : undefined;
  const selectedDirectMessagesStarCost = selectedGroup?.isDirectMessages
    ? Math.max(
      0,
      Math.floor(Number(
        selectedDirectMessagesParentGroup?.settings?.directMessagesStarCount
        ?? selectedGroup.settings?.directMessagesStarCount
        ?? 0,
      ) || 0),
    )
    : 0;
  const isSelectedUserDirectMessagesAdmin = Boolean(
    selectedGroup?.isDirectMessages
    && (
      selectedDirectMessagesParentMembershipStatus === 'owner'
      || selectedDirectMessagesParentMembershipStatus === 'admin'
    ),
  );
  const shouldChargeSelectedUserDirectMessages = Boolean(
    selectedGroup?.isDirectMessages
    && !isSelectedUserDirectMessagesAdmin
    && selectedDirectMessagesStarCost > 0,
  );
  const isChannelScope = chatScopeTab === 'channel';
  const canPostInSelectedChannel = !isChannelScope || canEditSelectedGroup;
  const channelPostingRestrictionReason = isChannelScope && groupMembership === 'joined' && !canEditSelectedGroup
    ? 'Only channel owner/admin can publish posts.'
    : null;
  const groupMemberRestrictionDraftByUserId = selectedGroupStateKey
    ? (groupMemberRestrictionDraftByChatKey[selectedGroupStateKey] || {})
    : {};
  const groupMemberAdminTitleByUserId = selectedGroupStateKey
    ? (groupMemberAdminTitleByChatKey[selectedGroupStateKey] || {})
    : {};
  const groupMemberTagByUserId = selectedGroupStateKey
    ? (groupMemberTagByChatKey[selectedGroupStateKey] || {})
    : {};
  const setGroupMemberAdminTitleByUserId = (updater: (prev: Record<number, string>) => Record<number, string>) => {
    if (!selectedGroupStateKey) {
      return;
    }
    setGroupMemberAdminTitleByChatKey((prev) => {
      const current = prev[selectedGroupStateKey] || {};
      return {
        ...prev,
        [selectedGroupStateKey]: updater(current),
      };
    });
  };
  const setGroupMemberTagByUserId = (updater: (prev: Record<number, string>) => Record<number, string>) => {
    if (!selectedGroupStateKey) {
      return;
    }
    setGroupMemberTagByChatKey((prev) => {
      const current = prev[selectedGroupStateKey] || {};
      return {
        ...prev,
        [selectedGroupStateKey]: updater(current),
      };
    });
  };
  const canManageForumTopics = Boolean(
    selectedGroup
    && selectedGroup.type === 'supergroup'
    && selectedGroup.isForum
    && canEditSelectedGroup,
  );
  const canDeleteDirectMessagesTopicByActiveActor = (topic: ForumTopicState | null | undefined): boolean => {
    if (!selectedGroup?.isDirectMessages || !topic) {
      return false;
    }

    return isSelectedUserDirectMessagesManager;
  };
  const selectedBotMembershipKey = selectedGroup && selectedBot
    ? `${selectedBotToken}:${selectedGroup.id}:${selectedBot.id}`
    : null;
  const selectedBotMembershipStatus = selectedBotMembershipKey
    ? groupMembershipByUser[selectedBotMembershipKey]
    : undefined;
  const normalizedSelectedBotMembershipStatus = normalizeMembershipStatus(selectedBotMembershipStatus);
  const isSelectedBotInGroup = isJoinedMembershipStatus(selectedBotMembershipStatus);
  const canSetSelectedBotAsMember = normalizedSelectedBotMembershipStatus !== 'member';
  const canSetSelectedBotAsAdmin = normalizedSelectedBotMembershipStatus !== 'admin' && normalizedSelectedBotMembershipStatus !== 'owner';
  const selectedGroupInviteLink = selectedGroupStateKey ? groupInviteLinksByChat[selectedGroupStateKey] : undefined;
  const selectedGroupJoinRequests = selectedGroupStateKey ? (pendingJoinRequestsByChat[selectedGroupStateKey] || []) : [];
  const selectedPinnedMessageIds = selectedGroupStateKey ? (pinnedMessageByChatKey[selectedGroupStateKey] || []) : [];
  const selectedForumTopics = useMemo(() => {
    if (!selectedGroupStateKey || !(selectedGroup?.isForum || selectedGroup?.isDirectMessages)) {
      return [] as ForumTopicState[];
    }

    const normalizedTopics = normalizeForumTopics(
      forumTopicsByChatKey[selectedGroupStateKey] || [],
      { includeGeneralFallback: Boolean(selectedGroup?.isForum && !selectedGroup?.isDirectMessages) },
    );

    if (selectedGroup?.isDirectMessages && isSelectedUserDirectMessagesManager) {
      return normalizedTopics.filter((topic) => topic.messageThreadId !== selectedUser.id);
    }

    return normalizedTopics;
  }, [
    forumTopicsByChatKey,
    isSelectedUserDirectMessagesManager,
    selectedGroup?.isDirectMessages,
    selectedGroup?.isForum,
    selectedGroupStateKey,
    selectedUser.id,
  ]);

  const activeForumTopicThreadId = useMemo(() => {
    if (!selectedGroupStateKey || !(selectedGroup?.isForum || selectedGroup?.isDirectMessages)) {
      return undefined;
    }

    const selected = selectedForumTopicByChatKey[selectedGroupStateKey];
    if (selectedGroup?.isDirectMessages) {
      if (!isSelectedUserDirectMessagesManager) {
        return selectedUser.id;
      }

      if (
        Number.isFinite(selected)
        && selected > 0
        && selectedForumTopics.some((topic) => topic.messageThreadId === selected)
      ) {
        return selected;
      }

      const firstDirectTopic = selectedForumTopics.find((topic) => !topic.isGeneral);
      return firstDirectTopic?.messageThreadId;
    }

    if (Number.isFinite(selected) && selected > 0 && selectedForumTopics.some((topic) => topic.messageThreadId === selected)) {
      return selected;
    }

    const firstVisibleTopic = selectedGroup?.isDirectMessages
      ? selectedForumTopics.find((topic) => !topic.isGeneral)
      : selectedForumTopics[0];

    if (firstVisibleTopic) {
      return firstVisibleTopic.messageThreadId;
    }

    if (selectedGroup?.isForum) {
      return GENERAL_FORUM_TOPIC_THREAD_ID;
    }

    return undefined;
  }, [
    isSelectedUserDirectMessagesManager,
    selectedForumTopicByChatKey,
    selectedForumTopics,
    selectedGroup?.isDirectMessages,
    selectedGroup?.isForum,
    selectedGroupStateKey,
    selectedUser.id,
  ]);

  const activeForumTopic = useMemo(() => {
    if (!(selectedGroup?.isForum || selectedGroup?.isDirectMessages) || activeForumTopicThreadId === undefined) {
      return null;
    }
    return selectedForumTopics.find((topic) => topic.messageThreadId === activeForumTopicThreadId) || null;
  }, [activeForumTopicThreadId, selectedForumTopics, selectedGroup?.isDirectMessages, selectedGroup?.isForum]);

  const activeMessageThreadId = (selectedGroup?.isForum || selectedGroup?.isDirectMessages)
    ? activeForumTopicThreadId
    : undefined;
  const activeDirectMessagesTopicId = selectedGroup?.isDirectMessages
    ? (isSelectedUserDirectMessagesManager ? activeMessageThreadId : undefined)
    : undefined;
  const outboundMessageThreadId = selectedGroup?.isForum
    ? activeMessageThreadId
    : undefined;

  const selectForumTopicThread = (threadId: number) => {
    if (!selectedGroupStateKey || !(selectedGroup?.isForum || selectedGroup?.isDirectMessages)) {
      return;
    }

    if (selectedGroup?.isDirectMessages && !isSelectedUserDirectMessagesManager) {
      return;
    }

    const normalizedThreadId = Math.floor(Number(threadId));
    if (!Number.isFinite(normalizedThreadId) || normalizedThreadId <= 0) {
      return;
    }

    setSelectedForumTopicByChatKey((prev) => ({
      ...prev,
      [selectedGroupStateKey]: normalizedThreadId,
    }));
  };

  const ensureActiveForumTopicWritable = (): boolean => {
    if (chatScopeTab !== 'group' || !(selectedGroup?.isForum || selectedGroup?.isDirectMessages)) {
      return true;
    }

    if (selectedGroup?.isDirectMessages) {
      if (isSelectedUserDirectMessagesManager && !activeMessageThreadId) {
        setErrorText('Select a direct messages topic first.');
        return false;
      }
      return true;
    }

    if (!activeForumTopic) {
      return true;
    }

    if (activeForumTopic.isClosed) {
      setErrorText('Selected forum topic is closed. Reopen it to send new messages.');
      return false;
    }

    if (activeForumTopic.isGeneral && activeForumTopic.isHidden) {
      setErrorText('General topic is hidden. Unhide it before sending messages.');
      return false;
    }

    return true;
  };

  const selectedChatId = chatScopeTab === 'private'
    ? selectedUser.id
    : (selectedGroup?.id ?? 0);
  const linkedDiscussionPairs = useMemo(() => {
    const channelToDiscussion = new Map<number, number>();
    const discussionToChannel = new Map<number, number>();

    groupChats.forEach((chat) => {
      const rawLinkedChatId = Math.floor(Number(chat.linkedDiscussionChatId));
      if (!Number.isFinite(rawLinkedChatId) || rawLinkedChatId === 0) {
        return;
      }

      if (chat.type === 'channel') {
        channelToDiscussion.set(chat.id, rawLinkedChatId);
        return;
      }

      if (chat.type === 'group' || chat.type === 'supergroup') {
        discussionToChannel.set(chat.id, rawLinkedChatId);
      }
    });

    channelToDiscussion.forEach((discussionChatId, channelChatId) => {
      if (!discussionToChannel.has(discussionChatId)) {
        discussionToChannel.set(discussionChatId, channelChatId);
      }
    });

    discussionToChannel.forEach((channelChatId, discussionChatId) => {
      if (!channelToDiscussion.has(channelChatId)) {
        channelToDiscussion.set(channelChatId, discussionChatId);
      }
    });

    return {
      channelToDiscussion,
      discussionToChannel,
    };
  }, [groupChats]);
  const activeChannelLinkedDiscussionChatId = useMemo(() => {
    if (chatScopeTab !== 'channel' || !selectedGroup || selectedGroup.type !== 'channel') {
      return undefined;
    }

    return linkedDiscussionPairs.channelToDiscussion.get(selectedGroup.id);
  }, [chatScopeTab, linkedDiscussionPairs, selectedGroup]);
  const activeDiscussionLinkedChannelId = useMemo(() => {
    if (
      chatScopeTab !== 'group'
      || !selectedGroup
      || (selectedGroup.type !== 'group' && selectedGroup.type !== 'supergroup')
    ) {
      return undefined;
    }

    return linkedDiscussionPairs.discussionToChannel.get(selectedGroup.id);
  }, [chatScopeTab, linkedDiscussionPairs, selectedGroup]);
  // Keep discussion comments authored by the active user, not the linked channel identity.
  const activeDiscussionSenderChatId: number | undefined = undefined;
  const chatKey = `${selectedBotToken}:${selectedChatId}`;
  const forwardTargetDirectory = useMemo(() => {
    const privateTargets = availableUsers.map((user) => ({
      chatId: user.id,
      title: user.id === selectedUser.id ? `${user.first_name} (Saved Messages)` : user.first_name,
      username: user.username,
      kind: 'private' as const,
      searchKey: `${user.first_name} ${user.username || ''} ${user.id}`.toLowerCase(),
    }));

    const groupTargets = groupChats.map((chat) => ({
      chatId: chat.id,
      title: chat.title,
      username: chat.username,
      kind: chat.type,
      searchKey: `${chat.title} ${chat.username || ''} ${chat.id}`.toLowerCase(),
    }));

    const merged = [...privateTargets, ...groupTargets];
    const byId = new Map<number, (typeof merged)[number]>();
    merged.forEach((target) => {
      byId.set(target.chatId, target);
    });

    return Array.from(byId.values()).sort((a, b) => a.title.localeCompare(b.title));
  }, [availableUsers, groupChats, selectedUser.id]);

  const filteredForwardTargets = useMemo(() => {
    const keyword = forwardTargetChatId.trim().toLowerCase();
    if (!keyword) {
      return forwardTargetDirectory;
    }
    return forwardTargetDirectory.filter((target) => target.searchKey.includes(keyword));
  }, [forwardTargetChatId, forwardTargetDirectory]);

  const activeDiscussionSource = useMemo(() => {
    if (chatScopeTab !== 'group' || !selectedGroup) {
      return null;
    }

    const context = commentSourceByDiscussionChatKey[chatKey];
    if (!context) {
      return null;
    }

    if (selectedGroup.type !== 'group' && selectedGroup.type !== 'supergroup') {
      return null;
    }

    if (
      activeDiscussionLinkedChannelId
      && activeDiscussionLinkedChannelId !== context.channelChatId
    ) {
      return null;
    }

    return context;
  }, [
    activeDiscussionLinkedChannelId,
    chatKey,
    chatScopeTab,
    commentSourceByDiscussionChatKey,
    selectedGroup,
  ]);

  const hasStarted = chatScopeTab === 'private'
    ? Boolean(startedChats[chatKey])
    : (selectedGroup?.isDirectMessages ? true : groupMembership === 'joined');
  const activeChatAction = chatActionByChatKey[chatKey] && chatActionByChatKey[chatKey].expiresAt > Date.now()
    ? chatActionByChatKey[chatKey]
    : null;
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const messagesContainerRef = useRef<HTMLElement | null>(null);
  const messageRefs = useRef<Record<number, HTMLDivElement | null>>({});
  const isNearBottomRef = useRef(true);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const composerTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const videoNoteRefs = useRef<Record<number, HTMLVideoElement | null>>({});
  const voiceRecorderRef = useRef<MediaRecorder | null>(null);
  const voiceRecorderChunksRef = useRef<BlobPart[]>([]);
  const inlineRequestSeqRef = useRef(0);
  const channelViewAckAtRef = useRef<Record<string, number>>({});

  const visibleMessages = useMemo(() => {
    if (chatScopeTab === 'group' && activeDiscussionSource) {
      const discussionMessagesInChat = messages
        .filter((message) => (
          message.chatId === selectedChatId
          && message.botToken === selectedBotToken
        ))
        .sort((a, b) => {
          if (a.date === b.date) {
            return a.id - b.id;
          }
          return a.date - b.date;
        });

      const relatedDiscussionMessages = discussionMessagesInChat
        .filter((message) => (
          message.linkedChannelChatId === activeDiscussionSource.channelChatId
          && message.linkedChannelMessageId === activeDiscussionSource.channelMessageId
        ))
        .sort((a, b) => (a.id - b.id));

      const explicitDiscussionRootMessageId = relatedDiscussionMessages
        .map((message) => message.linkedDiscussionRootMessageId)
        .find((id): id is number => typeof id === 'number' && id > 0);

      const channelPost = messages.find((message) => (
        message.botToken === selectedBotToken
        && message.chatId === activeDiscussionSource.channelChatId
        && message.id === activeDiscussionSource.channelMessageId
      ));
      const fallbackDiscussionRootMessageId = activeDiscussionSource.discussionRootMessageId
        || (channelPost
          ? findFallbackDiscussionRootMessage(discussionMessagesInChat, channelPost)?.id
          : undefined);
      const discussionRootMessageId = explicitDiscussionRootMessageId || fallbackDiscussionRootMessageId;

      const threadMessages = (() => {
        if (!discussionRootMessageId) {
          return relatedDiscussionMessages;
        }

        const replyTreeMessages = collectDiscussionReplyTreeMessages(
          discussionMessagesInChat,
          discussionRootMessageId,
        );
        const byId = new Map<number, ChatMessage>();
        [...relatedDiscussionMessages, ...replyTreeMessages].forEach((message) => {
          byId.set(message.id, message);
        });
        return [...byId.values()].sort((a, b) => {
          if (a.date === b.date) {
            return a.id - b.id;
          }
          return a.date - b.date;
        });
      })();

      if (!discussionRootMessageId) {
        return threadMessages;
      }

      return threadMessages.filter((message) => message.id !== discussionRootMessageId);

    }

    return messages
      .filter((message) => {
        if (message.chatId !== selectedChatId || message.botToken !== selectedBotToken) {
          return false;
        }

        const isDirectMessagesView = Boolean(selectedGroup?.isDirectMessages || selectedGroup?.parentChannelChatId);
        if (chatScopeTab === 'group' && (selectedGroup?.isForum || isDirectMessagesView)) {
          const targetThreadId = selectedGroup?.isForum
            ? (activeMessageThreadId || GENERAL_FORUM_TOPIC_THREAD_ID)
            : (isSelectedUserDirectMessagesManager ? activeMessageThreadId : selectedUser.id);
          if (!targetThreadId) {
            return false;
          }
          const messageThreadId = selectedGroup?.isForum
            ? (message.messageThreadId || GENERAL_FORUM_TOPIC_THREAD_ID)
            : message.messageThreadId;
          return messageThreadId === targetThreadId;
        }

        return true;
      })
      .sort((a, b) => {
        if (a.date === b.date) {
          return a.id - b.id;
        }
        return a.date - b.date;
      });
  }, [
    activeDiscussionSource,
    activeMessageThreadId,
    chatScopeTab,
    messages,
    selectedBotToken,
    selectedChatId,
    selectedUser.id,
    isSelectedUserDirectMessagesManager,
    selectedGroup?.isDirectMessages,
    selectedGroup?.isForum,
    selectedGroup?.parentChannelChatId,
  ]);

  const linkedDiscussionCommentsByChannelMessageId = useMemo(() => {
    const commentsByChannelPost = new Map<number, {
      discussionRootMessageId?: number;
      discussionMessageThreadId?: number;
      comments: ChatMessage[];
    }>();

    if (
      chatScopeTab !== 'channel'
      || !selectedGroup
      || selectedGroup.type !== 'channel'
      || !activeChannelLinkedDiscussionChatId
    ) {
      return commentsByChannelPost;
    }

    const discussionChatId = activeChannelLinkedDiscussionChatId;
    const discussionMessages = messages
      .filter((message) => (
        message.botToken === selectedBotToken
        && message.chatId === discussionChatId
      ))
      .sort((a, b) => {
        if (a.date === b.date) {
          return a.id - b.id;
        }
        return a.date - b.date;
      });

    const relatedDiscussionMessages = discussionMessages
      .filter((message) => (
        message.linkedChannelChatId === selectedGroup.id
        && typeof message.linkedChannelMessageId === 'number'
        && message.linkedChannelMessageId > 0
      ));

    relatedDiscussionMessages.forEach((message) => {
      const channelMessageId = message.linkedChannelMessageId as number;
      const existing = commentsByChannelPost.get(channelMessageId) || {
        comments: [] as ChatMessage[],
      };

      if (!existing.discussionMessageThreadId && message.messageThreadId) {
        existing.discussionMessageThreadId = message.messageThreadId;
      }

      if (message.linkedDiscussionRootMessageId && message.linkedDiscussionRootMessageId > 0) {
        existing.discussionRootMessageId = existing.discussionRootMessageId || message.linkedDiscussionRootMessageId;
      }

      const isRootMessage = Boolean(
        message.linkedDiscussionRootMessageId
        && message.id === message.linkedDiscussionRootMessageId,
      );

      if (!isRootMessage) {
        existing.comments.push(message);
      }

      commentsByChannelPost.set(channelMessageId, existing);
    });

    const channelPosts = messages
      .filter((message) => (
        message.botToken === selectedBotToken
        && message.chatId === selectedGroup.id
        && !message.service
      ));

    channelPosts.forEach((channelPost) => {
      const existing = commentsByChannelPost.get(channelPost.id);
      if (existing?.discussionRootMessageId) {
        return;
      }

      const fallbackRoot = findFallbackDiscussionRootMessage(discussionMessages, channelPost);
      if (!fallbackRoot) {
        return;
      }

      const fallbackThreadMessages = collectDiscussionReplyTreeMessages(discussionMessages, fallbackRoot.id);
      const fallbackComments = fallbackThreadMessages.filter((message) => message.id !== fallbackRoot.id);

      commentsByChannelPost.set(channelPost.id, {
        discussionRootMessageId: fallbackRoot.id,
        discussionMessageThreadId: fallbackRoot.messageThreadId,
        comments: fallbackComments,
      });
    });

    commentsByChannelPost.forEach((entry, channelMessageId) => {
      if (!entry.discussionRootMessageId || entry.discussionRootMessageId <= 0) {
        return;
      }

      const replyTreeMessages = collectDiscussionReplyTreeMessages(
        discussionMessages,
        entry.discussionRootMessageId,
      );
      if (replyTreeMessages.length === 0) {
        return;
      }

      const mergedCommentsById = new Map<number, ChatMessage>();
      [...entry.comments, ...replyTreeMessages]
        .filter((message) => message.id !== entry.discussionRootMessageId)
        .forEach((message) => {
          mergedCommentsById.set(message.id, message);
        });

      commentsByChannelPost.set(channelMessageId, {
        ...entry,
        comments: [...mergedCommentsById.values()].sort((a, b) => {
          if (a.date === b.date) {
            return a.id - b.id;
          }
          return a.date - b.date;
        }),
      });
    });

    return commentsByChannelPost;
  }, [
    activeChannelLinkedDiscussionChatId,
    chatScopeTab,
    messages,
    selectedBotToken,
    selectedGroup?.id,
    selectedGroup?.type,
  ]);

  const activeDiscussionCommentContext = useMemo(() => {
    if (chatScopeTab !== 'group' || !selectedGroup) {
      return null;
    }

    const context = commentSourceByDiscussionChatKey[chatKey];
    if (!context) {
      return null;
    }

    if (selectedGroup.type !== 'group' && selectedGroup.type !== 'supergroup') {
      return null;
    }

    if (
      activeDiscussionLinkedChannelId
      && activeDiscussionLinkedChannelId !== context.channelChatId
    ) {
      return null;
    }

    const discussionMessagesInChat = messages
      .filter((message) => (
        message.botToken === selectedBotToken
        && message.chatId === selectedGroup.id
      ))
      .sort((a, b) => {
        if (a.date === b.date) {
          return a.id - b.id;
        }
        return a.date - b.date;
      });

    const relatedDiscussionMessages = discussionMessagesInChat
      .filter((message) => (
        message.linkedChannelChatId === context.channelChatId
        && message.linkedChannelMessageId === context.channelMessageId
      ));

    const explicitDiscussionRootMessageId = relatedDiscussionMessages
      .map((message) => message.linkedDiscussionRootMessageId)
      .find((id): id is number => typeof id === 'number' && id > 0);

    const channelPost = messages.find((message) => (
      message.botToken === selectedBotToken
      && message.chatId === context.channelChatId
      && message.id === context.channelMessageId
    ));
    const fallbackDiscussionRootMessageId = context.discussionRootMessageId
      || (channelPost
        ? findFallbackDiscussionRootMessage(discussionMessagesInChat, channelPost)?.id
        : undefined);
    const discussionRootMessageId = explicitDiscussionRootMessageId || fallbackDiscussionRootMessageId;

    const threadMessages = (() => {
      if (!discussionRootMessageId) {
        return relatedDiscussionMessages;
      }

      const replyTreeMessages = collectDiscussionReplyTreeMessages(
        discussionMessagesInChat,
        discussionRootMessageId,
      );
      const byId = new Map<number, ChatMessage>();
      [...relatedDiscussionMessages, ...replyTreeMessages].forEach((message) => {
        byId.set(message.id, message);
      });
      return [...byId.values()].sort((a, b) => {
        if (a.date === b.date) {
          return a.id - b.id;
        }
        return a.date - b.date;
      });
    })();

    const rootMessage = discussionRootMessageId
      ? discussionMessagesInChat.find((message) => message.id === discussionRootMessageId)
      : undefined;

    const discussionMessageThreadId = rootMessage?.messageThreadId
      || threadMessages.find((message) => (
        typeof message.messageThreadId === 'number'
        && message.messageThreadId > 0
      ))?.messageThreadId;

    const commentMessages = threadMessages.filter((message) => {
      if (!discussionRootMessageId || discussionRootMessageId <= 0) {
        return true;
      }
      return message.id !== discussionRootMessageId;
    });

    return {
      ...context,
      discussionRootMessageId,
      discussionMessageThreadId,
      rootMessage,
      commentsCount: commentMessages.length,
      latestCommentsPreview: commentMessages.slice(-5),
    };
  }, [
    activeDiscussionLinkedChannelId,
    chatKey,
    chatScopeTab,
    commentSourceByDiscussionChatKey,
    messages,
    selectedBotToken,
    selectedGroup,
  ]);

  const activeDiscussionChannelPost = useMemo(() => {
    if (!activeDiscussionCommentContext) {
      return null;
    }

    return messages.find((message) => (
      message.botToken === selectedBotToken
      && message.chatId === activeDiscussionCommentContext.channelChatId
      && message.id === activeDiscussionCommentContext.channelMessageId
    )) || null;
  }, [activeDiscussionCommentContext, messages, selectedBotToken]);

  const isDiscussionThreadView = chatScopeTab === 'group' && Boolean(activeDiscussionCommentContext);

  const resolveComposerReplyTargetId = (preferredReplyToMessageId?: number): number | undefined => {
    if (activeDiscussionCommentContext?.discussionRootMessageId
      && activeDiscussionCommentContext.discussionRootMessageId > 0) {
      return activeDiscussionCommentContext.discussionRootMessageId;
    }

    if (activeDiscussionSource?.discussionRootMessageId
      && activeDiscussionSource.discussionRootMessageId > 0) {
      return activeDiscussionSource.discussionRootMessageId;
    }

    const normalizedPreferredReply = Math.floor(Number(preferredReplyToMessageId));
    if (Number.isFinite(normalizedPreferredReply) && normalizedPreferredReply > 0) {
      return normalizedPreferredReply;
    }

    if (replyTarget?.id && replyTarget.id > 0) {
      return replyTarget.id;
    }
    return undefined;
  };

  const selectedPinnedMessages = useMemo(() => {
    if (selectedPinnedMessageIds.length === 0) {
      return [] as ChatMessage[];
    }

    const byId = new Map<number, ChatMessage>();
    visibleMessages.forEach((message) => {
      byId.set(message.id, message);
    });

    return [...selectedPinnedMessageIds]
      .reverse()
      .map((messageId) => byId.get(messageId))
      .filter((message): message is ChatMessage => Boolean(message));
  }, [selectedPinnedMessageIds, visibleMessages]);

  const isMessagePinned = (messageId: number) => selectedPinnedMessageIds.includes(messageId);

  const activePinnedMessage = selectedPinnedMessages[pinnedPreviewIndex] || selectedPinnedMessages[0] || null;

  const selectedGroupMembers = useMemo(() => {
    if (!selectedGroup) {
      return [] as Array<{
        userId: number;
        firstName: string;
        username?: string;
        status: string;
        isBot: boolean;
        customTitle?: string;
        tag?: string;
      }>;
    }

    const membershipPrefix = `${selectedBotToken}:${selectedGroup.id}:`;
    const statusPriority: Record<string, number> = {
      owner: 0,
      admin: 1,
      member: 2,
      restricted: 3,
      left: 4,
      banned: 5,
    };

    const metaByUserId = selectedGroupStateKey ? (groupMemberMetaByChatKey[selectedGroupStateKey] || {}) : {};
    const rows = Object.entries(groupMembershipByUser)
      .filter(([key]) => key.startsWith(membershipPrefix))
      .map(([key, status]) => {
        const userId = Number(key.slice(membershipPrefix.length));
        const simUser = availableUsers.find((user) => user.id === userId);
        const botAsUser = selectedBot && selectedBot.id === userId
          ? { first_name: selectedBot.first_name, username: selectedBot.username }
          : null;
        const memberMeta = metaByUserId[userId] || {};

        return {
          userId,
          firstName: simUser?.first_name || botAsUser?.first_name || `User ${userId}`,
          username: simUser?.username || botAsUser?.username,
          status: normalizeMembershipStatus(status),
          isBot: Boolean(selectedBot && selectedBot.id === userId),
          customTitle: selectedGroup.type === 'channel' ? undefined : memberMeta.customTitle,
          tag: selectedGroup.type === 'channel' ? undefined : memberMeta.tag,
        };
      })
      .filter((member) => Number.isFinite(member.userId) && member.userId > 0)
      .sort((a, b) => {
        const rankDiff = (statusPriority[a.status] ?? 99) - (statusPriority[b.status] ?? 99);
        if (rankDiff !== 0) {
          return rankDiff;
        }
        return a.firstName.localeCompare(b.firstName);
      });

    const keyword = groupMembersFilter.trim().toLowerCase();
    if (!keyword) {
      return rows;
    }

    return rows.filter((member) => (
      member.firstName.toLowerCase().includes(keyword)
      || (member.username || '').toLowerCase().includes(keyword)
      || String(member.userId).includes(keyword)
    ));
  }, [availableUsers, groupMemberMetaByChatKey, groupMembershipByUser, groupMembersFilter, selectedBot, selectedBotToken, selectedGroup, selectedGroupStateKey]);

  const resolveGroupSenderBadges = (fromUserId: number): { customTitle?: string; tag?: string } => {
    if (!selectedGroup) {
      return {};
    }

    const membershipKey = `${selectedBotToken}:${selectedGroup.id}:${fromUserId}`;
    const status = normalizeMembershipStatus(groupMembershipByUser[membershipKey]);
    const meta = selectedGroupStateKey
      ? (groupMemberMetaByChatKey[selectedGroupStateKey]?.[fromUserId] || {})
      : {};

    return {
      customTitle: (status === 'admin' || status === 'owner') ? meta.customTitle : undefined,
      tag: selectedGroup.type === 'channel' ? undefined : meta.tag,
    };
  };

  const canPinInSelectedGroup = useMemo(() => {
    if (!selectedGroup || groupMembership !== 'joined') {
      return false;
    }
    if (canEditSelectedGroup) {
      return true;
    }
    return Boolean(selectedGroup.settings?.allowPinMessages);
  }, [canEditSelectedGroup, groupMembership, selectedGroup]);

  const groupSettingsTitle = isChannelScope
    ? (groupSettingsPage === 'home'
      ? 'Channel controls'
      : groupSettingsPage === 'bot-membership'
        ? 'Channel bot posting access'
        : groupSettingsPage === 'discovery'
          ? 'Channel discovery & invite links'
          : groupSettingsPage === 'members'
            ? 'Channel members management'
          : groupSettingsPage === 'danger-zone'
            ? 'Channel danger zone'
            : 'Channel controls')
    : (groupSettingsPage === 'home'
      ? 'Group settings'
      : groupSettingsPage === 'bot-membership'
        ? 'Bot membership'
        : groupSettingsPage === 'discovery'
          ? 'Discovery & invite links'
          : groupSettingsPage === 'topics'
            ? 'Forum topics'
            : groupSettingsPage === 'members'
              ? 'Members management'
              : groupSettingsPage === 'sender-chat'
                ? 'Sender chat moderation'
                : 'Danger zone');

  useEffect(() => {
    setPinnedPreviewIndex(0);
  }, [selectedGroupStateKey, selectedPinnedMessages.length]);

  useEffect(() => {
    if (!selectedGroup || selectedGroup.type !== 'channel') {
      setChannelDiscussionLinkDraft('');
      return;
    }

    const resolvedLinkedDiscussionChatId = activeChannelLinkedDiscussionChatId
      ?? selectedGroup.linkedDiscussionChatId;
    setChannelDiscussionLinkDraft(
      resolvedLinkedDiscussionChatId ? String(resolvedLinkedDiscussionChatId) : '',
    );
  }, [activeChannelLinkedDiscussionChatId, selectedGroup]);

  useEffect(() => {
    if (!isChannelScope) {
      return;
    }

    if (groupSettingsPage === 'topics' || groupSettingsPage === 'sender-chat') {
      setGroupSettingsPage('home');
    }
  }, [groupSettingsPage, isChannelScope]);

  const isMessageOutgoingForSelected = (message: ChatMessage) => {
    if (chatScopeTab === 'private') {
      return message.fromUserId === selectedUser.id;
    }

    if (chatScopeTab === 'group') {
      if (selectedGroup?.isDirectMessages) {
        if (isSelectedUserDirectMessagesManager) {
          const parentChannelChatId = Number(selectedGroup.parentChannelChatId);
          if (!Number.isFinite(parentChannelChatId) || parentChannelChatId === 0) {
            return false;
          }
          return message.senderChatId === parentChannelChatId;
        }
        return message.fromUserId === selectedUser.id;
      }
      return message.fromUserId === selectedUser.id;
    }

    if (chatScopeTab === 'channel') {
      return false;
    }

    return message.fromUserId === selectedUser.id;
  };

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
          && isMessageOutgoingForSelected(next) === isMessageOutgoingForSelected(current)
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
  }, [
    chatScopeTab,
    groupMembershipByUser,
    selectedBot?.id,
    selectedBotToken,
    selectedGroup?.id,
    selectedUser.id,
    visibleMessages,
  ]);

  const activeReplyKeyboard = useMemo(() => {
    let active: { sourceMessageId: number; markup: BotReplyMarkup } | null = null;

    for (const message of visibleMessages) {
      if (!message.isOutgoing || !message.replyMarkup) {
        continue;
      }

      if (message.replyMarkup.kind === 'remove' && message.replyMarkup.remove_keyboard) {
        active = null;
        continue;
      }

      if (message.replyMarkup.kind === 'reply') {
        active = {
          sourceMessageId: message.id,
          markup: message.replyMarkup,
        };
      }
    }

    if (!active || active.markup.kind !== 'reply') {
      return null;
    }

    if (
      active.markup.one_time_keyboard
      && dismissedOneTimeKeyboards[chatKey] === active.sourceMessageId
    ) {
      return null;
    }

    return active;
  }, [visibleMessages, dismissedOneTimeKeyboards, chatKey]);

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

  const stickerSetNamesFromMessages = useMemo(() => {
    const names = new Set<string>();
    visibleMessages.forEach((message) => {
      const setName = message.media?.type === 'sticker' ? message.media.setName : undefined;
      if (setName) {
        names.add(setName);
      }
    });
    return Array.from(names);
  }, [visibleMessages]);

  const activeStickerSet = useMemo(
    () => stickerShelf.find((set) => set.name === stickerShelfActiveSet) || null,
    [stickerShelf, stickerShelfActiveSet],
  );

  const animationGallery = useMemo(() => {
    const seen = new Set<string>();
    const items: Array<{ fileId: string; from: string }> = [];
    [...visibleMessages].reverse().forEach((message) => {
      if (message.media?.type !== 'animation') {
        return;
      }
      if (seen.has(message.media.fileId)) {
        return;
      }
      seen.add(message.media.fileId);
      items.push({
        fileId: message.media.fileId,
        from: message.fromName,
      });
    });
    return items;
  }, [visibleMessages]);

  useEffect(() => {
    if (chatScopeTab !== 'channel' || !selectedGroup || groupMembership !== 'joined') {
      return;
    }

    const recentChannelMessages = visibleMessages
      .filter((message) => !message.service)
      .slice(-120);
    if (recentChannelMessages.length === 0) {
      return;
    }

    let cancelled = false;
    const now = Math.floor(Date.now() / 1000);
    const viewWindowSeconds = 24 * 60 * 60;

    const syncViews = async () => {
      for (const message of recentChannelMessages) {
        const cacheKey = `${selectedBotToken}:${selectedGroup.id}:${selectedUser.id}:${message.id}`;
        const lastAckAt = channelViewAckAtRef.current[cacheKey] || 0;
        if ((now - lastAckAt) < viewWindowSeconds) {
          continue;
        }

        try {
          const result = await markSimulationChannelMessageView(selectedBotToken, {
            chat_id: selectedGroup.id,
            message_id: message.id,
            user_id: selectedUser.id,
            first_name: selectedUser.first_name,
            username: selectedUser.username,
          });

          if (cancelled) {
            return;
          }

          channelViewAckAtRef.current[cacheKey] = now;
          setMessages((prev) => prev.map((item) => {
            if (
              item.botToken === selectedBotToken
              && item.chatId === result.chat_id
              && item.id === result.message_id
            ) {
              if (item.views === result.views) {
                return item;
              }
              return {
                ...item,
                views: result.views,
              };
            }
            return item;
          }));
        } catch {
          delete channelViewAckAtRef.current[cacheKey];
        }
      }
    };

    void syncViews();

    return () => {
      cancelled = true;
    };
  }, [
    chatScopeTab,
    groupMembership,
    selectedBotToken,
    selectedGroup,
    selectedUser.first_name,
    selectedUser.id,
    selectedUser.username,
    visibleMessages,
  ]);

  useEffect(() => {
    let mounted = true;
    if (!navigator.mediaDevices || !navigator.mediaDevices.enumerateDevices) {
      setCanUseMicrophone(false);
      setCanUseCamera(false);
      return;
    }

    navigator.mediaDevices
      .enumerateDevices()
      .then((devices) => {
        if (!mounted) {
          return;
        }
        setCanUseMicrophone(devices.some((d) => d.kind === 'audioinput'));
        setCanUseCamera(devices.some((d) => d.kind === 'videoinput'));
      })
      .catch(() => {
        if (!mounted) {
          return;
        }
        setCanUseMicrophone(false);
        setCanUseCamera(false);
      });

    return () => {
      mounted = false;
    };
  }, []);

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
    localStorage.setItem(LAST_UPDATES_KEY, JSON.stringify(lastUpdateByBot));
  }, [lastUpdateByBot]);

  useEffect(() => {
    localStorage.setItem(SELECTED_BOT_KEY, selectedBotToken);
  }, [selectedBotToken]);

  useEffect(() => {
    setLastUpdateId(() => {
      const saved = lastUpdateByBot[selectedBotToken] || 0;
      return saved > 0 ? saved : 0;
    });
  }, [selectedBotToken, lastUpdateByBot]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      const now = Date.now();
      setChatActionByChatKey((prev) => {
        const entries = Object.entries(prev).filter(([, value]) => value.expiresAt > now);
        if (entries.length === Object.keys(prev).length) {
          return prev;
        }
        return Object.fromEntries(entries);
      });
    }, 500);

    return () => {
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    const loadPrivacyMode = async () => {
      setIsGroupPrivacyModeLoading(true);
      try {
        const result = await getSimBotPrivacyMode(selectedBotToken);
        if (!cancelled) {
          setGroupPrivacyModeEnabled(result.enabled);
        }
      } catch {
        if (!cancelled) {
          setGroupPrivacyModeEnabled(true);
        }
      } finally {
        if (!cancelled) {
          setIsGroupPrivacyModeLoading(false);
        }
      }
    };

    void loadPrivacyMode();

    return () => {
      cancelled = true;
    };
  }, [selectedBotToken]);

  useEffect(() => {
    setForumTopicDraft((prev) => ({
      ...prev,
      messageThreadId: '',
      name: '',
      normalEmoji: '',
      iconColor: String(DEFAULT_FORUM_ICON_COLOR),
      iconCustomEmojiId: '',
    }));
    setForumTopicIconStickers([]);
    setShowForumTopicModal(false);
    setForumTopicModalThreadId(null);
    setExpandedForumTopicThreadId(null);
    setForumTopicContextMenu(null);
  }, [selectedGroupStateKey]);

  useEffect(() => {
    if (!selectedGroupStateKey || !selectedGroup?.isForum) {
      return;
    }

    const selectedThread = selectedForumTopicByChatKey[selectedGroupStateKey];
    const hasSelected = Number.isFinite(selectedThread)
      && selectedThread > 0
      && selectedForumTopics.some((topic) => topic.messageThreadId === selectedThread);
    if (hasSelected) {
      return;
    }

    const fallbackThreadId = selectedForumTopics[0]?.messageThreadId || GENERAL_FORUM_TOPIC_THREAD_ID;
    setSelectedForumTopicByChatKey((prev) => ({
      ...prev,
      [selectedGroupStateKey]: fallbackThreadId,
    }));
  }, [selectedForumTopicByChatKey, selectedForumTopics, selectedGroup?.isForum, selectedGroupStateKey]);

  useEffect(() => {
    if (!selectedGroup?.isForum) {
      return;
    }

    const generalTopic = selectedForumTopics.find((topic) => topic.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID);
    if (!generalTopic) {
      return;
    }

    setForumTopicDraft((prev) => {
      if ((prev.generalName || '').trim() === generalTopic.name.trim()) {
        return prev;
      }
      return {
        ...prev,
        generalName: generalTopic.name,
      };
    });
  }, [selectedForumTopics, selectedGroup?.isForum]);

  useEffect(() => {
    localStorage.setItem(SELECTED_USER_KEY, String(selectedUserId));
  }, [selectedUserId]);

  useEffect(() => {
    localStorage.setItem(CHAT_SCOPE_KEY, chatScopeTab);
  }, [chatScopeTab]);

  useEffect(() => {
    localStorage.setItem(GROUP_CHATS_KEY, JSON.stringify(groupChats));
  }, [groupChats]);

  useEffect(() => {
    localStorage.setItem(GROUP_MEMBERSHIP_KEY, JSON.stringify(groupMembershipByUser));
  }, [groupMembershipByUser]);

  useEffect(() => {
    localStorage.setItem(GROUP_INVITE_LINKS_KEY, JSON.stringify(groupInviteLinksByChat));
  }, [groupInviteLinksByChat]);

  useEffect(() => {
    localStorage.setItem(GROUP_JOIN_REQUESTS_KEY, JSON.stringify(pendingJoinRequestsByChat));
  }, [pendingJoinRequestsByChat]);

  useEffect(() => {
    localStorage.setItem(GROUP_PINNED_MESSAGES_KEY, JSON.stringify(pinnedMessageByChatKey));
  }, [pinnedMessageByChatKey]);

  useEffect(() => {
    localStorage.setItem(INVOICE_META_KEY, JSON.stringify(invoiceMetaByMessageKey));
  }, [invoiceMetaByMessageKey]);

  useEffect(() => {
    localStorage.setItem(FORUM_TOPICS_KEY, JSON.stringify(forumTopicsByChatKey));
  }, [forumTopicsByChatKey]);

  useEffect(() => {
    localStorage.setItem(SELECTED_FORUM_TOPIC_KEY, JSON.stringify(selectedForumTopicByChatKey));
  }, [selectedForumTopicByChatKey]);

  useEffect(() => {
    localStorage.setItem(BUSINESS_CONNECTIONS_KEY, JSON.stringify(businessConnectionByUserKey));
  }, [businessConnectionByUserKey]);

  useEffect(() => {
    localStorage.setItem(USER_WALLETS_KEY, JSON.stringify(walletByUserId));
  }, [walletByUserId]);

  useEffect(() => {
    localStorage.setItem(SELECTED_GROUP_BY_BOT_KEY, JSON.stringify(selectedGroupByBot));
  }, [selectedGroupByBot]);

  useEffect(() => {
    const current = businessConnectionByUserKey[businessDraftStateKey];
    setBusinessConnectionDraftId(current?.id || '');
    setBusinessConnectionDraftEnabled(current?.is_enabled ?? true);
    setBusinessRightsDraft(mapBusinessRightsToDraft(current?.rights));
  }, [businessConnectionByUserKey, businessDraftStateKey]);

  useEffect(() => {
    if (availableBots.length === 0) {
      return;
    }

    if (availableBots.some((bot) => bot.token === businessDraftBotToken)) {
      return;
    }

    const fallbackBotToken = availableBots.some((bot) => bot.token === selectedBotToken)
      ? selectedBotToken
      : availableBots[0].token;
    setBusinessDraftBotToken(fallbackBotToken);
  }, [availableBots, businessDraftBotToken, selectedBotToken]);

  useEffect(() => {
    if (!selectedGroup || selectedGroup.type !== 'channel' || selectedGroup.isDirectMessages || !selectedGroupStateKey) {
      return;
    }

    if (normalizeMembershipStatus(groupMembershipStatus) !== 'admin') {
      return;
    }

    if (channelAdminRightsDraftByChatKey[selectedGroupStateKey]?.[selectedUser.id]) {
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const member = await getChatMember(selectedBotToken, {
          chat_id: selectedGroup.id,
          user_id: selectedUser.id,
        }, selectedUser.id);
        if (cancelled) {
          return;
        }

        const parsed = parseChannelAdminRightsDraft(member);
        if (!parsed) {
          return;
        }

        setChannelAdminRightsDraftByChatKey((prev) => ({
          ...prev,
          [selectedGroupStateKey]: {
            ...(prev[selectedGroupStateKey] || {}),
            [selectedUser.id]: parsed,
          },
        }));
      } catch {
        // Keep UI responsive; permission checks are also enforced server-side.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [
    channelAdminRightsDraftByChatKey,
    groupMembershipStatus,
    selectedBotToken,
    selectedGroup,
    selectedGroupStateKey,
    selectedUser.id,
  ]);

  useEffect(() => {
    const parentChannelChatId = selectedGroup?.parentChannelChatId;
    if (!selectedGroup?.isDirectMessages || parentChannelChatId === undefined || !selectedDirectMessagesParentStateKey) {
      return;
    }

    if (selectedDirectMessagesParentMembershipStatus !== 'admin') {
      return;
    }

    if (channelAdminRightsDraftByChatKey[selectedDirectMessagesParentStateKey]?.[selectedUser.id]) {
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const member = await getChatMember(selectedBotToken, {
          chat_id: parentChannelChatId,
          user_id: selectedUser.id,
        }, selectedUser.id);
        if (cancelled) {
          return;
        }

        const parsed = parseChannelAdminRightsDraft(member);
        if (!parsed) {
          return;
        }

        setChannelAdminRightsDraftByChatKey((prev) => ({
          ...prev,
          [selectedDirectMessagesParentStateKey]: {
            ...(prev[selectedDirectMessagesParentStateKey] || {}),
            [selectedUser.id]: parsed,
          },
        }));
      } catch {
        // Keep UI responsive; permission checks are also enforced server-side.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [
    channelAdminRightsDraftByChatKey,
    selectedGroup?.parentChannelChatId,
    selectedBotToken,
    selectedDirectMessagesParentMembershipStatus,
    selectedDirectMessagesParentStateKey,
    selectedGroup,
    selectedUser.id,
  ]);

  useEffect(() => {
    const saved = selectedGroupByBot[selectedBotToken];
    const hasSaved = Number.isFinite(saved) && groupChats.some((group) => group.id === saved);
    setSelectedGroupChatId((current) => {
      if (hasSaved) {
        return current === saved ? current : saved;
      }
      if (current !== null && groupChats.some((group) => group.id === current)) {
        return current;
      }
      return null;
    });
  }, [selectedBotToken, selectedGroupByBot, groupChats]);

  useEffect(() => {
    setSelectedGroupByBot((prev) => {
      const current = prev[selectedBotToken];
      const selectedIsValid = selectedGroupChatId !== null
        && groupChats.some((group) => group.id === selectedGroupChatId);
      if (!selectedIsValid) {
        if (current === undefined) {
          return prev;
        }
        const next = { ...prev };
        delete next[selectedBotToken];
        return next;
      }
      const normalizedSelectedChatId = selectedGroupChatId as number;
      if (current === normalizedSelectedChatId) {
        return prev;
      }
      return {
        ...prev,
        [selectedBotToken]: normalizedSelectedChatId,
      };
    });
  }, [selectedBotToken, selectedGroupChatId, groupChats]);

  useBotUpdates({
    token: selectedBotToken,
    lastUpdateId,
    onSimEvent: (event: SimRealtimeEvent) => {
      if (event.sim_event === 'chat_action') {
        const chatId = Number(event.chat_id);
        const normalizedAction = String(event.action || '').trim();
        if (!Number.isFinite(chatId) || !normalizedAction) {
          return;
        }

        const actorName = event.from_name?.trim()
          || (Number.isFinite(event.from_user_id) ? `user_${event.from_user_id}` : '')
          || selectedBot?.first_name
          || 'Bot';

        const actionKey = `${selectedBotToken}:${chatId}`;
        setChatActionByChatKey((prev) => ({
          ...prev,
          [actionKey]: {
            action: normalizedAction,
            actorName,
            expiresAt: Date.now() + 5000,
          },
        }));
        return;
      }

      if (event.sim_event === 'invoice_meta') {
        const chatId = Number(event.chat_id);
        const messageId = Number(event.message_id);
        if (!Number.isFinite(chatId) || !Number.isFinite(messageId)) {
          return;
        }

        const meta = event.invoice_meta || {};
        const suggested = Array.isArray(meta.suggested_tip_amounts)
          ? meta.suggested_tip_amounts
            .map((item) => Number(item))
            .filter((item) => Number.isFinite(item) && item > 0)
          : undefined;

        const key = `${selectedBotToken}:${chatId}:${messageId}`;
        setInvoiceMetaByMessageKey((prev) => ({
          ...prev,
          [key]: {
            photoUrl: typeof meta.photo_url === 'string' ? meta.photo_url : undefined,
            maxTipAmount: Number.isFinite(Number(meta.max_tip_amount)) ? Number(meta.max_tip_amount) : undefined,
            suggestedTipAmounts: suggested,
            needName: typeof meta.need_name === 'boolean' ? meta.need_name : undefined,
            needPhoneNumber: typeof meta.need_phone_number === 'boolean' ? meta.need_phone_number : undefined,
            needEmail: typeof meta.need_email === 'boolean' ? meta.need_email : undefined,
            needShippingAddress: typeof meta.need_shipping_address === 'boolean' ? meta.need_shipping_address : undefined,
            isFlexible: typeof meta.is_flexible === 'boolean' ? meta.is_flexible : undefined,
            sendPhoneNumberToProvider: typeof meta.send_phone_number_to_provider === 'boolean' ? meta.send_phone_number_to_provider : undefined,
            sendEmailToProvider: typeof meta.send_email_to_provider === 'boolean' ? meta.send_email_to_provider : undefined,
          },
        }));
      }
    },
    onUpdate: (update: BotUpdate) => {
      if (update.business_connection?.user?.id) {
        const incomingConnection = update.business_connection;
        const connectionStateKey = `${selectedBotToken}:${incomingConnection.user.id}`;
        setBusinessConnectionByUserKey((prev) => {
          if (!incomingConnection.is_enabled) {
            if (!prev[connectionStateKey] || prev[connectionStateKey].id !== incomingConnection.id) {
              return prev;
            }

            const next = { ...prev };
            delete next[connectionStateKey];
            return next;
          }

          return {
            ...prev,
            [connectionStateKey]: incomingConnection,
          };
        });
      }

      const membershipUpdate = update.chat_member || update.my_chat_member;
      if (membershipUpdate) {
        const oldMember = membershipUpdate.old_chat_member as Record<string, unknown> | undefined;
        const newMember = membershipUpdate.new_chat_member as Record<string, unknown> | undefined;
        const oldStatus = extractChatMemberStatus(oldMember);
        const newStatus = extractChatMemberStatus(newMember);
        const targetUser = extractChatMemberUser(newMember) || extractChatMemberUser(oldMember);

        if (targetUser) {
          const key = `${selectedBotToken}:${membershipUpdate.chat.id}:${targetUser.id}`;
          const normalizedStatus = normalizeMembershipStatus(newStatus || oldStatus);
          setGroupMembershipByUser((prev) => ({
            ...prev,
            [key]: normalizedStatus,
          }));

          if (['member', 'admin', 'owner'].includes(normalizedStatus)) {
            const requestKey = `${selectedBotToken}:${membershipUpdate.chat.id}`;
            setPendingJoinRequestsByChat((prev) => {
              const list = prev[requestKey] || [];
              const nextList = list.filter((item) => item.userId !== targetUser.id);
              if (nextList.length === list.length) {
                return prev;
              }
              return {
                ...prev,
                [requestKey]: nextList,
              };
            });
          }
        }
      }

      if (update.chat_join_request) {
        const request = update.chat_join_request;
        const requestKey = `${selectedBotToken}:${request.chat.id}`;
        const mapped: PendingGroupJoinRequest = {
          chatId: request.chat.id,
          userId: request.from.id,
          firstName: request.from.first_name || `user_${request.from.id}`,
          username: request.from.username || undefined,
          date: request.date,
          inviteLink: request.invite_link?.invite_link,
        };

        setPendingJoinRequestsByChat((prev) => {
          const current = prev[requestKey] || [];
          const withoutUser = current.filter((item) => item.userId !== mapped.userId);
          return {
            ...prev,
            [requestKey]: [...withoutUser, mapped].sort((a, b) => a.date - b.date),
          };
        });
      }

      if (update.message_reaction) {
        const actor = update.message_reaction.user;
        const actorKey = actor ? `${actor.id}:${actor.is_bot ? 1 : 0}` : null;
        const newReactionEmojis = update.message_reaction.new_reaction
          .filter((item) => item.type === 'emoji')
          .map((item) => item.emoji)
          .filter((emoji): emoji is string => typeof emoji === 'string' && emoji.length > 0);

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

      const payload = update.edited_business_message
        || update.edited_channel_post
        || update.edited_message
        || update.business_message
        || update.channel_post
        || update.message;
      if (update.poll) {
        setMessages((prev) => prev.map((message) => {
          if (!message.poll || message.poll.id !== update.poll!.id) {
            return message;
          }

          return {
            ...message,
            poll: update.poll,
          };
        }));
      }

      if (update.poll_answer?.poll_id && update.poll_answer.user?.id) {
        const voter = update.poll_answer.user;
        const selectionKey = `${voter.id}:${update.poll_answer.poll_id}`;
        setPollSelections((prev) => ({
          ...prev,
          [selectionKey]: update.poll_answer!.option_ids,
        }));
      }

      if (!payload) {
        setLastUpdateId((current) => Math.max(current, update.update_id));
        setLastUpdateByBot((prev) => {
          const current = prev[selectedBotToken] || 0;
          const next = Math.max(current, update.update_id);
          if (next === current) {
            return prev;
          }
          return {
            ...prev,
            [selectedBotToken]: next,
          };
        });
        return;
      }

      let media: ChatMessage['media'];
      if (payload.photo && payload.photo.length > 0) {
        const bestPhoto = payload.photo[payload.photo.length - 1];
        media = {
          type: 'photo',
          fileId: bestPhoto.file_id,
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
      } else if (payload.sticker) {
        media = {
          type: 'sticker',
          fileId: payload.sticker.file_id,
          mimeType: (payload.sticker.is_video || payload.sticker.is_animated) ? 'video/webm' : 'image/webp',
          fileName: payload.sticker.set_name ? `${payload.sticker.set_name}.webp` : 'sticker.webp',
          setName: payload.sticker.set_name,
        };
      } else if (payload.animation) {
        media = {
          type: 'animation',
          fileId: payload.animation.file_id,
          mimeType: payload.animation.mime_type,
          fileName: payload.animation.file_name,
        };
      } else if (payload.video_note) {
        media = {
          type: 'video_note',
          fileId: payload.video_note.file_id,
          fileName: 'video_note.mp4',
        };
      }

      const serviceMessage = (() => {
        const displayName = (member?: { id?: number; first_name?: string; username?: string } | null): string => {
          if (!member) {
            return 'Unknown';
          }
          if (member.first_name && member.first_name.trim()) {
            return member.first_name;
          }
          if (member.username && member.username.trim()) {
            return `@${member.username}`;
          }
          return `user_${member.id ?? 0}`;
        };

        const actorName = displayName(payload.from || null);

        if (payload.new_chat_members && payload.new_chat_members.length > 0) {
          const memberNames = payload.new_chat_members.map((member) => displayName(member));
          const selfJoin = Boolean(
            payload.from
            && payload.new_chat_members.length === 1
            && payload.new_chat_members[0].id === payload.from.id,
          );
          return {
            text: payload.text || (selfJoin
              ? `${actorName} joined the group`
              : `${actorName} added ${memberNames.join(', ')}`),
            service: {
              kind: selfJoin ? 'join' as const : 'member_update' as const,
              targetName: memberNames.join(', '),
            },
          };
        }

        if (payload.left_chat_member) {
          const targetName = displayName(payload.left_chat_member);
          const selfLeave = Boolean(payload.from && payload.left_chat_member.id === payload.from.id);
          return {
            text: payload.text || (selfLeave
              ? `${targetName} left the group`
              : `${actorName} removed ${targetName}`),
            service: {
              kind: selfLeave ? 'leave' as const : 'member_update' as const,
              targetName,
            },
          };
        }

        if (payload.pinned_message) {
          const pinnedMessageId = payload.pinned_message.message_id;
          return {
            text: payload.text || `${actorName} pinned a message`,
            service: {
              kind: 'system' as const,
              targetName: Number.isFinite(pinnedMessageId) ? `#${pinnedMessageId}` : undefined,
            },
          };
        }

        if (payload.new_chat_title) {
          return {
            text: payload.text || `${actorName} changed the group name to "${payload.new_chat_title}"`,
            service: {
              kind: 'system' as const,
            },
          };
        }

        if (payload.forum_topic_created) {
          const topicName = payload.forum_topic_created.name || 'topic';
          return {
            text: payload.text || `${actorName} created the topic "${topicName}"`,
            service: {
              kind: 'system' as const,
              targetName: topicName,
            },
          };
        }

        if (payload.forum_topic_edited) {
          const editedName = payload.forum_topic_edited.name?.trim() || undefined;
          return {
            text: payload.text || (editedName
              ? `${actorName} edited topic "${editedName}"`
              : `${actorName} edited a topic`),
            service: {
              kind: 'system' as const,
              targetName: editedName,
            },
          };
        }

        if (payload.forum_topic_closed) {
          return {
            text: payload.text || `${actorName} closed a topic`,
            service: {
              kind: 'system' as const,
            },
          };
        }

        if (payload.forum_topic_reopened) {
          return {
            text: payload.text || `${actorName} reopened a topic`,
            service: {
              kind: 'system' as const,
            },
          };
        }

        if (payload.general_forum_topic_hidden) {
          return {
            text: payload.text || `${actorName} hid the General topic`,
            service: {
              kind: 'system' as const,
              targetName: 'General',
            },
          };
        }

        if (payload.general_forum_topic_unhidden) {
          return {
            text: payload.text || `${actorName} unhid the General topic`,
            service: {
              kind: 'system' as const,
              targetName: 'General',
            },
          };
        }

        if (payload.group_chat_created || payload.supergroup_chat_created || payload.channel_chat_created) {
          const chatType = payload.channel_chat_created
            ? 'channel'
            : payload.supergroup_chat_created
              ? 'supergroup'
              : 'group';
          return {
            text: payload.text || `${actorName} created the ${chatType}`,
            service: {
              kind: 'system' as const,
            },
          };
        }

        return null;
      })();

      const isChannelMemberLifecycleService = payload.chat?.type === 'channel'
        && Boolean((payload.new_chat_members && payload.new_chat_members.length > 0) || payload.left_chat_member);
      if (isChannelMemberLifecycleService) {
        setLastUpdateId((current) => Math.max(current, update.update_id));
        setLastUpdateByBot((prev) => {
          const current = prev[selectedBotToken] || 0;
          const next = Math.max(current, update.update_id);
          if (next === current) {
            return prev;
          }
          return {
            ...prev,
            [selectedBotToken]: next,
          };
        });
        return;
      }

      const payloadRecord = payload as unknown as Record<string, unknown>;
      const senderChatRaw = payloadRecord.sender_chat;
      const senderChat = senderChatRaw && typeof senderChatRaw === 'object'
        ? senderChatRaw as Record<string, unknown>
        : undefined;
      const directTopicRaw = payloadRecord.direct_messages_topic;
      const directTopic = directTopicRaw && typeof directTopicRaw === 'object'
        ? (directTopicRaw as Record<string, unknown>)
        : undefined;
      const isChannelPayload = payload.chat?.type === 'channel';
      const authorSignature = typeof payloadRecord.author_signature === 'string'
        ? payloadRecord.author_signature
        : undefined;
      const senderChatTitle = typeof senderChat?.title === 'string'
        ? senderChat.title
        : undefined;
      const senderChatId = Number.isFinite(Number(senderChat?.id))
        ? Math.floor(Number(senderChat?.id))
        : undefined;
      const forwardOrigin = parseForwardOriginLabel(payloadRecord.forward_origin);
      const normalizedViews = Number(payloadRecord.views);
      const views = Number.isFinite(normalizedViews) && normalizedViews >= 0
        ? Math.floor(normalizedViews)
        : undefined;
      const rawMessageThreadId = Math.floor(Number(payloadRecord.message_thread_id));
      const fallbackDirectTopicId = Number.isFinite(Number(directTopic?.topic_id))
        ? Math.floor(Number(directTopic?.topic_id))
        : undefined;
      const messageThreadId = Number.isFinite(rawMessageThreadId) && rawMessageThreadId > 0
        ? rawMessageThreadId
        : (Number.isFinite(fallbackDirectTopicId) && (fallbackDirectTopicId || 0) > 0
          ? fallbackDirectTopicId
          : undefined);
      const rawParseMode = payloadRecord.sim_parse_mode;
      const parseMode = isMessageParseMode(rawParseMode)
        ? rawParseMode
        : undefined;
      const rawBusinessConnectionId = typeof payloadRecord.business_connection_id === 'string'
        ? payloadRecord.business_connection_id.trim()
        : '';
      const businessConnectionId = rawBusinessConnectionId || undefined;
      const rawPaidMessageStarCount = Math.floor(Number(payloadRecord.paid_message_star_count));
      const paidMessageStarCount = Number.isFinite(rawPaidMessageStarCount) && rawPaidMessageStarCount > 0
        ? rawPaidMessageStarCount
        : undefined;
      const rawLinkedChannelChatId = Math.floor(Number(payloadRecord.linked_channel_chat_id));
      const linkedChannelChatId = Number.isFinite(rawLinkedChannelChatId) && rawLinkedChannelChatId !== 0
        ? rawLinkedChannelChatId
        : undefined;
      const rawLinkedChannelMessageId = Math.floor(Number(payloadRecord.linked_channel_message_id));
      const linkedChannelMessageId = Number.isFinite(rawLinkedChannelMessageId) && rawLinkedChannelMessageId > 0
        ? rawLinkedChannelMessageId
        : undefined;
      const rawLinkedDiscussionRootMessageId = Math.floor(Number(payloadRecord.linked_discussion_root_message_id));
      const linkedDiscussionRootMessageId = Number.isFinite(rawLinkedDiscussionRootMessageId) && rawLinkedDiscussionRootMessageId > 0
        ? rawLinkedDiscussionRootMessageId
        : undefined;
      const isTopicMessage = typeof payloadRecord.is_topic_message === 'boolean'
        ? payloadRecord.is_topic_message
        : (messageThreadId !== undefined ? true : undefined);
      const resolvedFromName = isChannelPayload
        ? (authorSignature || senderChatTitle || payload.from?.first_name || (payload.from?.username ? `@${payload.from.username}` : 'Channel'))
        : (authorSignature || senderChatTitle || payload.from?.first_name || (payload.from?.username ? `@${payload.from.username}` : 'Bot'));
      const resolvedFromUserId = senderChatId
        || payload.from?.id
        || (isChannelPayload && selectedBot ? selectedBot.id : undefined)
        || 0;

      const mapped: ChatMessage = {
        id: payload.message_id,
        botToken: selectedBotToken,
        chatId: payload.chat.id,
        messageThreadId,
        isTopicMessage,
        text: serviceMessage?.text || payload.text || payload.caption || '',
        date: payload.date,
        parseMode,
        isOutgoing: Boolean(payload.from?.is_bot),
        fromName: resolvedFromName,
        fromUserId: resolvedFromUserId,
        senderChatId,
        senderChatTitle,
        businessConnectionId,
        paidMessageStarCount,
        views: serviceMessage ? undefined : views,
        forwardedFrom: serviceMessage ? undefined : forwardOrigin.label,
        forwardedDate: serviceMessage ? undefined : forwardOrigin.date,
        service: serviceMessage?.service,
        isInlineOrigin: Boolean(payload.via_bot?.id),
        viaBotUsername: payload.via_bot?.username,
        contact: payload.contact,
        location: payload.location,
        venue: payload.venue,
        dice: payload.dice,
        game: payload.game,
        poll: payload.poll,
        invoice: payload.invoice,
        successfulPayment: payload.successful_payment,
        media,
        mediaGroupId: payload.media_group_id,
        linkedChannelChatId,
        linkedChannelMessageId,
        linkedDiscussionRootMessageId,
        replyTo: payload.reply_to_message ? {
          messageId: payload.reply_to_message.message_id,
          fromName: payload.reply_to_message.sender_chat?.title
            || payload.reply_to_message.from?.first_name
            || (payload.reply_to_message.from?.username ? `@${payload.reply_to_message.from.username}` : 'Unknown'),
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
        replyMarkup: mapIncomingReplyMarkup(payload.reply_markup),
        editDate: payload.edit_date,
      };

      setMessages((prev) => {
        const existingIndex = prev.findIndex(
          (m) => m.id === mapped.id && m.botToken === mapped.botToken && m.chatId === mapped.chatId,
        );

        if (existingIndex >= 0) {
          const next = [...prev];
          const existing = next[existingIndex];
          const resolvedMessageThreadId = mapped.messageThreadId ?? existing.messageThreadId;
          const resolvedIsTopicMessage = mapped.isTopicMessage ?? existing.isTopicMessage;
          const resolvedSenderChatId = mapped.senderChatId ?? existing.senderChatId;
          const resolvedSenderChatTitle = mapped.senderChatTitle ?? existing.senderChatTitle;
          const resolvedFromUserId = resolvedSenderChatId
            || (mapped.fromUserId !== 0 ? mapped.fromUserId : existing.fromUserId);
          const resolvedFromName = resolvedSenderChatId
            ? (resolvedSenderChatTitle || mapped.fromName || existing.fromName)
            : (mapped.fromName || existing.fromName);
          next[existingIndex] = {
            ...existing,
            ...mapped,
            messageThreadId: resolvedMessageThreadId,
            isTopicMessage: resolvedIsTopicMessage,
            senderChatId: resolvedSenderChatId,
            senderChatTitle: resolvedSenderChatTitle,
            fromUserId: resolvedFromUserId,
            fromName: resolvedFromName,
            isInlineOrigin: Boolean(existing.isInlineOrigin || mapped.isInlineOrigin),
            reactionCounts: existing.reactionCounts,
            actorReactions: existing.actorReactions,
          };
          return next;
        }

        return [...prev, mapped];
      });

      const isDirectMessagesPayload = Boolean(payload.chat?.is_direct_messages);
      if (isDirectMessagesPayload && messageThreadId && messageThreadId > 0) {
        const directTopicUserRaw = directTopic?.user;
        const directTopicUser = directTopicUserRaw && typeof directTopicUserRaw === 'object'
          ? (directTopicUserRaw as Record<string, unknown>)
          : undefined;
        const directTopicUserId = Number.isFinite(Number(directTopicUser?.id))
          ? Math.floor(Number(directTopicUser?.id))
          : undefined;
        const directTopicUserFirstName = typeof directTopicUser?.first_name === 'string'
          ? directTopicUser.first_name.trim()
          : '';
        const directTopicUsername = typeof directTopicUser?.username === 'string'
          ? directTopicUser.username.trim()
          : '';
        const directTopicName = directTopicUserFirstName
          || (directTopicUsername ? `@${directTopicUsername}` : `User ${directTopicUserId || messageThreadId}`);
        const directTopicStateKey = `${selectedBotToken}:${payload.chat.id}`;
        const directTopicUpdatedAt = Number.isFinite(Number(payload.date))
          ? Math.floor(Number(payload.date))
          : undefined;

        setForumTopicsByChatKey((prev) => {
          const current = prev[directTopicStateKey] || [];
          const next = [
            ...current.filter((topic) => topic.messageThreadId !== messageThreadId),
            {
              messageThreadId,
              name: directTopicName,
              iconColor: DEFAULT_FORUM_ICON_COLOR,
              iconCustomEmojiId: undefined,
              isClosed: false,
              isHidden: false,
              isGeneral: false,
              updatedAt: directTopicUpdatedAt,
            } as ForumTopicState,
          ];

          return {
            ...prev,
            [directTopicStateKey]: normalizeForumTopics(next, { includeGeneralFallback: false }),
          };
        });

        setSelectedForumTopicByChatKey((prev) => {
          const selectedThreadId = prev[directTopicStateKey];

          if (isSelectedUserDirectMessagesManager) {
            if (Number.isFinite(selectedThreadId) && selectedThreadId > 0) {
              return prev;
            }
            return {
              ...prev,
              [directTopicStateKey]: messageThreadId,
            };
          }

          if (selectedThreadId === selectedUser.id) {
            return prev;
          }

          return {
            ...prev,
            [directTopicStateKey]: selectedUser.id,
          };
        });
      }

      const pinnedMessageIdFromPayload = payload.pinned_message?.message_id;
      if (Number.isFinite(pinnedMessageIdFromPayload) && payload.chat?.id) {
        const stateKey = `${selectedBotToken}:${payload.chat.id}`;
        const normalizedPinnedId = Number(pinnedMessageIdFromPayload);
        setPinnedMessageByChatKey((prev) => {
          const current = prev[stateKey] || [];
          const next = [...current.filter((id) => id !== normalizedPinnedId), normalizedPinnedId];
          return {
            ...prev,
            [stateKey]: next,
          };
        });
      }

      setLastUpdateId((current) => Math.max(current, update.update_id));
      setLastUpdateByBot((prev) => {
        const current = prev[selectedBotToken] || 0;
        const next = Math.max(current, update.update_id);
        if (next === current) {
          return prev;
        }
        return {
          ...prev,
          [selectedBotToken]: next,
        };
      });
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

        const settingsByChatId = new Map<number, {
          description?: string;
          linkedChatId?: number;
          settings: GroupSettingsSnapshot;
        }>();
        (bootstrap.chat_settings || []).forEach((entry) => {
          const rawLinkedChatId = Math.floor(Number(entry.linked_chat_id));
          const linkedChatId = Number.isFinite(rawLinkedChatId) && rawLinkedChatId !== 0
            ? rawLinkedChatId
            : undefined;
          settingsByChatId.set(entry.chat_id, {
            description: entry.description || undefined,
            linkedChatId,
            settings: mapServerSettingsToSnapshot(entry),
          });
        });

        const directMessagesParentByChatId = new Map<number, number>();
        (bootstrap.channel_direct_messages || []).forEach((entry: SimBootstrapChannelDirectMessages) => {
          const directMessagesChatId = Math.floor(Number(entry.direct_messages_chat_id));
          const channelChatId = Math.floor(Number(entry.channel_chat_id));
          if (!Number.isFinite(directMessagesChatId) || !Number.isFinite(channelChatId)) {
            return;
          }
          directMessagesParentByChatId.set(directMessagesChatId, channelChatId);
        });

        const incomingGroups: GroupChatItem[] = (bootstrap.chats || [])
          .filter((chat) => chat.type === 'group' || chat.type === 'supergroup' || chat.type === 'channel')
          .map((chat) => {
            const isDirectMessages = Boolean(chat.is_direct_messages);
            const parentChannelChatId = directMessagesParentByChatId.get(chat.id);
            return {
              id: chat.id,
              type: chat.type as 'group' | 'supergroup' | 'channel',
              title: chat.title || (isDirectMessages ? `Direct Messages ${Math.abs(chat.id)}` : `Group ${Math.abs(chat.id)}`),
              username: chat.username || undefined,
              description: settingsByChatId.get(chat.id)?.description,
              isForum: Boolean(chat.is_forum) && !isDirectMessages,
              isDirectMessages,
              parentChannelChatId,
              linkedDiscussionChatId: settingsByChatId.get(chat.id)?.linkedChatId,
              settings: settingsByChatId.get(chat.id)?.settings || defaultGroupSettings(),
            };
          });
        const incomingGroupById = new Map<number, GroupChatItem>(incomingGroups.map((group) => [group.id, group]));
        incomingGroups.forEach((group) => {
          if (group.type !== 'channel') {
            return;
          }

          const rawDiscussionChatId = Math.floor(Number(group.linkedDiscussionChatId));
          if (!Number.isFinite(rawDiscussionChatId) || rawDiscussionChatId === 0) {
            return;
          }

          const discussionGroup = incomingGroupById.get(rawDiscussionChatId);
          if (!discussionGroup) {
            return;
          }

          if (
            (discussionGroup.type !== 'group' && discussionGroup.type !== 'supergroup')
            || discussionGroup.isDirectMessages
            || discussionGroup.linkedDiscussionChatId
          ) {
            return;
          }

          discussionGroup.linkedDiscussionChatId = group.id;
        });

        setGroupMembershipByUser((prev) => {
          const prefix = `${selectedBotToken}:`;
          const next: Record<string, string> = {};

          Object.entries(prev).forEach(([key, value]) => {
            if (!key.startsWith(prefix)) {
              next[key] = value;
            }
          });

          (bootstrap.memberships || []).forEach((membership) => {
            const key = `${selectedBotToken}:${membership.chat_id}:${membership.user_id}`;
            next[key] = normalizeMembershipStatus(membership.status);
          });

          return next;
        });

        setGroupMemberMetaByChatKey((prev) => {
          const prefix = `${selectedBotToken}:`;
          const next: Record<string, Record<number, GroupMemberMeta>> = {};

          Object.entries(prev).forEach(([key, value]) => {
            if (!key.startsWith(prefix)) {
              next[key] = value;
            }
          });

          (bootstrap.memberships || []).forEach((membership) => {
            const hasCustomTitle = typeof membership.custom_title === 'string';
            const hasTag = typeof membership.tag === 'string';
            if (!hasCustomTitle && !hasTag) {
              return;
            }

            const userId = membership.user_id;
            const chatKey = `${selectedBotToken}:${membership.chat_id}`;
            const existingByChat = next[chatKey] || {};
            const existing = existingByChat[userId] || {};
            next[chatKey] = {
              ...existingByChat,
              [userId]: {
                customTitle: hasCustomTitle ? (membership.custom_title || undefined) : existing.customTitle,
                tag: hasTag ? (membership.tag || undefined) : existing.tag,
              },
            };
          });

          return next;
        });

        setPendingJoinRequestsByChat((prev) => {
          const prefix = `${selectedBotToken}:`;
          const next: Record<string, PendingGroupJoinRequest[]> = {};

          Object.entries(prev).forEach(([key, value]) => {
            if (!key.startsWith(prefix)) {
              next[key] = value;
            }
          });

          (bootstrap.join_requests || [])
            .filter((request) => request.status === 'pending')
            .forEach((request) => {
              const key = `${selectedBotToken}:${request.chat_id}`;
              const mapped: PendingGroupJoinRequest = {
                chatId: request.chat_id,
                userId: request.user_id,
                firstName: request.first_name || `user_${request.user_id}`,
                username: request.username || undefined,
                date: request.date,
                inviteLink: request.invite_link,
              };
              next[key] = [...(next[key] || []).filter((item) => item.userId !== mapped.userId), mapped]
                .sort((a, b) => a.date - b.date);
            });

          return next;
        });

        const incomingForumTopicsByChat: Record<string, ForumTopicState[]> = {};
        (bootstrap.forum_topics || []).forEach((rawTopic: SimBootstrapForumTopic) => {
          const chatId = Number(rawTopic.chat_id);
          const messageThreadId = Math.floor(Number(rawTopic.message_thread_id));
          if (!Number.isFinite(chatId) || !Number.isFinite(messageThreadId) || messageThreadId <= 0) {
            return;
          }

          const key = `${selectedBotToken}:${chatId}`;
          const topic: ForumTopicState = {
            messageThreadId,
            name: rawTopic.name || `Topic ${messageThreadId}`,
            iconColor: Number.isFinite(Number(rawTopic.icon_color)) ? Math.floor(Number(rawTopic.icon_color)) : DEFAULT_FORUM_ICON_COLOR,
            iconCustomEmojiId: rawTopic.icon_custom_emoji_id || undefined,
            isClosed: Boolean(rawTopic.is_closed),
            isHidden: Boolean(rawTopic.is_hidden),
            isGeneral: Boolean(rawTopic.is_general) || messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID,
            updatedAt: Number.isFinite(Number(rawTopic.updated_at)) ? Math.floor(Number(rawTopic.updated_at)) : undefined,
          };

          incomingForumTopicsByChat[key] = [...(incomingForumTopicsByChat[key] || []), topic];
        });

        const normalizeTopicsForChat = (chatId: number, topics: ForumTopicState[]) => {
          const chat = incomingGroupById.get(chatId);
          return normalizeForumTopics(topics, {
            includeGeneralFallback: Boolean(chat?.isForum && !chat?.isDirectMessages),
          });
        };

        incomingGroups
          .filter((group) => group.type === 'supergroup' && group.isForum)
          .forEach((group) => {
            const key = `${selectedBotToken}:${group.id}`;
            const topics = incomingForumTopicsByChat[key] || [];
            incomingForumTopicsByChat[key] = normalizeTopicsForChat(group.id, topics);
          });

        setForumTopicsByChatKey((prev) => {
          const prefix = `${selectedBotToken}:`;
          const next: Record<string, ForumTopicState[]> = {};

          Object.entries(prev).forEach(([key, value]) => {
            if (!key.startsWith(prefix)) {
              next[key] = value;
            }
          });

          Object.entries(incomingForumTopicsByChat).forEach(([key, value]) => {
            const chatId = Math.floor(Number(key.slice(key.lastIndexOf(':') + 1)));
            if (!Number.isFinite(chatId)) {
              next[key] = normalizeForumTopics(value);
              return;
            }
            next[key] = normalizeTopicsForChat(chatId, value);
          });

          return next;
        });

        setSelectedForumTopicByChatKey((prev) => {
          const prefix = `${selectedBotToken}:`;
          const next: Record<string, number> = {};

          Object.entries(prev).forEach(([key, value]) => {
            if (!key.startsWith(prefix)) {
              next[key] = value;
            }
          });

          Object.entries(incomingForumTopicsByChat).forEach(([key, topics]) => {
            const chatId = Math.floor(Number(key.slice(key.lastIndexOf(':') + 1)));
            if (!Number.isFinite(chatId)) {
              return;
            }

            const normalized = normalizeTopicsForChat(chatId, topics);
            if (normalized.length === 0) {
              return;
            }

            const chat = incomingGroupById.get(chatId);
            const fallbackTopic = chat?.isDirectMessages
              ? normalized.find((topic) => !topic.isGeneral)
              : normalized[0];
            if (!fallbackTopic) {
              return;
            }

            const previousThreadId = prev[key];
            const hasPrevious = Number.isFinite(previousThreadId)
              && previousThreadId > 0
              && normalized.some((topic) => topic.messageThreadId === previousThreadId);
            next[key] = hasPrevious
              ? previousThreadId
              : fallbackTopic.messageThreadId;
          });

          return next;
        });

        const sortedIncomingGroups = [...incomingGroups].sort((a, b) => a.title.localeCompare(b.title));
        if (sortedIncomingGroups.length > 0) {
          setGroupChats(sortedIncomingGroups);
          setSelectedGroupChatId((current) => {
            if (current && sortedIncomingGroups.some((group) => group.id === current)) {
              return current;
            }
            return sortedIncomingGroups[0].id;
          });
          setSelectedGroupByBot((prev) => {
            const savedGroupId = Number(prev[selectedBotToken]);
            const hasSavedGroup = Number.isFinite(savedGroupId)
              && sortedIncomingGroups.some((group) => group.id === savedGroupId);
            if (hasSavedGroup) {
              return prev;
            }

            const fallbackGroupId = sortedIncomingGroups[0].id;
            if (prev[selectedBotToken] === fallbackGroupId) {
              return prev;
            }

            return {
              ...prev,
              [selectedBotToken]: fallbackGroupId,
            };
          });
        } else {
          setGroupChats([]);
          setSelectedGroupChatId(null);
          setSelectedGroupByBot((prev) => {
            if (prev[selectedBotToken] === undefined) {
              return prev;
            }
            const next = { ...prev };
            delete next[selectedBotToken];
            return next;
          });
        }
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

  useEffect(() => {
    if (chatScopeTab !== 'group' && chatScopeTab !== 'channel') {
      return;
    }

    if (scopedGroupChats.length === 0) {
      setSelectedGroupChatId(null);
      return;
    }

    if (selectedGroupChatId && scopedGroupChats.some((group) => group.id === selectedGroupChatId)) {
      return;
    }

    setSelectedGroupChatId(scopedGroupChats[0].id);
  }, [chatScopeTab, scopedGroupChats, selectedGroupChatId]);

  const persistStarted = (next: Record<string, boolean>) => {
    setStartedChats(next);
    localStorage.setItem(START_KEY, JSON.stringify(next));
  };

  const ensureDirectMessagesStarsAvailable = (messageCount = 1): boolean => {
    if (!shouldChargeSelectedUserDirectMessages) {
      return true;
    }

    const requiredStars = selectedDirectMessagesStarCost * Math.max(1, Math.floor(messageCount));
    if (walletState.stars < requiredStars) {
      setErrorText(`Not enough stars. You need ${requiredStars}⭐ for this DM action.`);
      return false;
    }

    return true;
  };

  const consumeDirectMessagesStars = (messageCount = 1) => {
    if (!shouldChargeSelectedUserDirectMessages) {
      return;
    }

    const debit = selectedDirectMessagesStarCost * Math.max(1, Math.floor(messageCount));
    setWalletState((prev) => ({
      ...prev,
      stars: Math.max(prev.stars - debit, 0),
    }));
  };

  const sendAsUser = async (
    text: string,
    parseMode?: Exclude<ComposerParseMode, 'none'>,
    replyToMessageId?: number,
  ) => {
    if (!text.trim() || isSending) {
      return;
    }
    if (!ensureActiveForumTopicWritable()) {
      return;
    }
    if (!ensureDirectMessagesStarsAvailable(1)) {
      return;
    }

    setIsSending(true);
    setErrorText('');
    try {
      const effectiveReplyToMessageId = resolveComposerReplyTargetId(replyToMessageId);
      await sendUserMessage(selectedBotToken, {
        chat_id: selectedChatId,
        message_thread_id: outboundMessageThreadId,
        direct_messages_topic_id: activeDirectMessagesTopicId,
        business_connection_id: activeBusinessConnectionId,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        sender_chat_id: activeDiscussionSenderChatId,
        text,
        parse_mode: parseMode,
        reply_to_message_id: effectiveReplyToMessageId,
      });
      consumeDirectMessagesStars(1);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'User send failed');
    } finally {
      setIsSending(false);
    }
  };

  const submitComposer = async () => {
    const text = composerText.trim();

    if (!composerEditTarget && !ensureActiveForumTopicWritable()) {
      return;
    }

    const pollTrigger = (() => {
      const lower = text.toLowerCase();
      const isPoll = lower.startsWith('/poll ');
      const isQuiz = lower.startsWith('/quiz ');
      if (!isPoll && !isQuiz) {
        return null;
      }

      const payload = text.slice(isQuiz ? 6 : 6).trim();
      const parts = payload.split('|').map((item) => item.trim()).filter(Boolean);
      if (parts.length < 3) {
        return null;
      }

      if (isQuiz) {
        const maybeCorrectIndex = Number(parts[parts.length - 1]);
        const hasCorrectIndex = Number.isInteger(maybeCorrectIndex)
          && maybeCorrectIndex >= 1
          && maybeCorrectIndex <= parts.length - 2;

        const options = hasCorrectIndex ? parts.slice(1, -1) : parts.slice(1);
        if (options.length < 2) {
          return null;
        }

        return {
          type: 'quiz' as const,
          question: parts[0],
          options,
          correctOptionId: hasCorrectIndex ? maybeCorrectIndex - 1 : 0,
        };
      }

      return {
        type: 'regular' as const,
        question: parts[0],
        options: parts.slice(1),
      };
    })();

    const invoiceTrigger = text.toLowerCase().startsWith('/invoice');

    if (pollTrigger && !composerEditTarget && selectedUploads.length === 0) {
      try {
        await sendPoll(selectedBotToken, {
          chat_id: selectedChatId,
          message_thread_id: outboundMessageThreadId,
          question: pollTrigger.question,
          options: pollTrigger.options.map((option) => ({ text: option })),
          is_anonymous: false,
          allows_multiple_answers: false,
          type: pollTrigger.type,
          correct_option_ids: pollTrigger.type === 'quiz' ? [pollTrigger.correctOptionId] : undefined,
        });
        setComposerText('');
      } catch (error) {
        setErrorText(error instanceof Error ? error.message : 'Poll send failed');
      }
      return;
    }

    if (invoiceTrigger && !composerEditTarget && selectedUploads.length === 0) {
      const payload = text.slice('/invoice'.length).trim();
      const parts = payload.split('|').map((item) => item.trim()).filter(Boolean);

      setInvoiceBuilder((prev) => ({
        ...prev,
        title: parts[0] || prev.title || `${selectedUser.first_name} invoice`,
        description: parts[1] || prev.description || 'Simulated payment request',
        amount: parts[2] || prev.amount || '100',
        currency: parts[3] || prev.currency || 'USD',
        payload: parts[4] || prev.payload,
      }));
      setShowMediaDrawer(true);
      setMediaDrawerTab('invoice');
      setShowFormattingTools(false);
      setComposerText('');
      return;
    }

    if (inlineTrigger) {
      if (inlineResults.length === 0) {
        setInlineModeError(isInlineModeSending
          ? 'Inline results are loading...'
          : 'No inline result yet. Wait for bot answer.');
        return;
      }

      await onChooseInlineResult(inlineResults[0]);
      return;
    }

    if (chatScopeTab === 'channel' && !composerEditTarget) {
      if (!canPostInSelectedChannel) {
        setErrorText('Only channel owner/admin can publish posts.');
        return;
      }

      if (!text && selectedUploads.length === 0) {
        return;
      }

      try {
        if (selectedUploads.length > 0) {
          for (let index = 0; index < selectedUploads.length; index += 1) {
            const file = selectedUploads[index];
            const uploadTarget = inferUploadMethod(file);
            await sendBotMediaFile(
              selectedBotToken,
              {
                chatId: selectedChatId,
                method: uploadTarget.method,
                field: uploadTarget.field,
                file,
                caption: index === 0 ? (text || undefined) : undefined,
                parseMode: index === 0 && text && composerParseMode !== 'none' ? composerParseMode : undefined,
                replyToMessageId: index === 0 ? replyTarget?.id : undefined,
              },
              selectedUser.id,
            );
          }
        } else {
          await sendBotMessage(
            selectedBotToken,
            {
              chat_id: selectedChatId,
              text,
              parse_mode: composerParseMode === 'none' ? undefined : composerParseMode,
              reply_parameters: replyTarget
                ? {
                  message_id: replyTarget.id,
                }
                : undefined,
            },
            selectedUser.id,
          );
        }

        setComposerText('');
        setSelectedUploads([]);
        setReplyTarget(null);
        dismissActiveOneTimeKeyboard();
        isNearBottomRef.current = true;
        window.setTimeout(() => {
          messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
        }, 0);
      } catch (error) {
        setErrorText(error instanceof Error ? error.message : 'Channel send failed');
      }
      return;
    }

    if (!text && selectedUploads.length === 0 && !composerEditTarget) {
      return;
    }

    if (composerEditTarget) {
      if (composerEditTarget.isInlineOrigin || composerEditTarget.viaBotUsername) {
        setErrorText('Inline-origin messages cannot be edited from the client simulator.');
        setComposerEditTarget(null);
        setSelectedUploads([]);
        return;
      }

      const ownerIsActiveUser = composerEditTarget.fromUserId === selectedUser.id;

      if (!ownerIsActiveUser) {
        setErrorText('Only message owner can edit this message.');
        setComposerEditTarget(null);
        setSelectedUploads([]);
        return;
      }

      const actorUserId = selectedUser.id;

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
            }, actorUserId);
          } else {
            await editBotMessageCaption(selectedBotToken, {
              chat_id: selectedChatId,
              message_id: composerEditTarget.id,
              caption: text,
              parse_mode: composerParseMode === 'none' ? undefined : composerParseMode,
            }, actorUserId);
          }
        } else {
          await editBotMessageText(selectedBotToken, {
            chat_id: selectedChatId,
            message_id: composerEditTarget.id,
            text,
            parse_mode: composerParseMode === 'none' ? undefined : composerParseMode,
          }, actorUserId);
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
      if (!ensureDirectMessagesStarsAvailable(selectedUploads.length)) {
        return;
      }

      let sentCount = 0;
      try {
        if (selectedUploads.length === 1) {
          await sendUserMedia(selectedBotToken, {
            chatId: selectedChatId,
            messageThreadId: outboundMessageThreadId,
            directMessagesTopicId: activeDirectMessagesTopicId,
            businessConnectionId: activeBusinessConnectionId,
            userId: selectedUser.id,
            firstName: selectedUser.first_name,
            username: selectedUser.username,
            senderChatId: activeDiscussionSenderChatId,
            file: selectedUploads[0],
            caption: text || undefined,
            parseMode: text && composerParseMode !== 'none' ? composerParseMode : undefined,
            replyToMessageId: resolveComposerReplyTargetId(replyTarget?.id),
          });
          sentCount += 1;
        } else {
          for (let index = 0; index < selectedUploads.length; index += 1) {
            const file = selectedUploads[index];
            await sendUserMedia(selectedBotToken, {
              chatId: selectedChatId,
              messageThreadId: outboundMessageThreadId,
              directMessagesTopicId: activeDirectMessagesTopicId,
              businessConnectionId: activeBusinessConnectionId,
              userId: selectedUser.id,
              firstName: selectedUser.first_name,
              username: selectedUser.username,
              senderChatId: activeDiscussionSenderChatId,
              file,
              caption: index === 0 ? (text || undefined) : undefined,
              parseMode: index === 0 && text && composerParseMode !== 'none' ? composerParseMode : undefined,
              replyToMessageId: index === 0 ? resolveComposerReplyTargetId(replyTarget?.id) : undefined,
            });
            sentCount += 1;
          }
        }

        if (sentCount > 0) {
          consumeDirectMessagesStars(sentCount);
        }

        setComposerText('');
        setSelectedUploads([]);
        setReplyTarget(null);
        dismissActiveOneTimeKeyboard();
        isNearBottomRef.current = true;
        window.setTimeout(() => {
          messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
        }, 0);
      } catch (error) {
        if (sentCount > 0) {
          consumeDirectMessagesStars(sentCount);
        }
        setErrorText(error instanceof Error ? error.message : 'Media upload failed');
      }
      return;
    }

    setComposerText('');
    await sendAsUser(
      text,
      composerParseMode === 'none' ? undefined : composerParseMode,
      resolveComposerReplyTargetId(replyTarget?.id),
    );
    setReplyTarget(null);
    dismissActiveOneTimeKeyboard();
    isNearBottomRef.current = true;
    window.setTimeout(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    }, 0);
  };

  const submitPollBuilder = async () => {
    if (!ensureActiveForumTopicWritable()) {
      return;
    }

    const question = pollBuilder.question.trim();
    const options = pollBuilder.options.map((item) => item.trim()).filter(Boolean);

    if (question.length === 0) {
      setErrorText('Poll question is required.');
      return;
    }
    if (options.length < 2) {
      setErrorText('Poll needs at least 2 options.');
      return;
    }

    const parseOrUndefined = (mode: ComposerParseMode): string | undefined => (
      mode === 'none' ? undefined : mode
    );

    const closeDateUnix = pollBuilder.closeDate
      ? Math.floor(new Date(pollBuilder.closeDate).getTime() / 1000)
      : undefined;
    const openPeriodNum = pollBuilder.openPeriod ? Number(pollBuilder.openPeriod) : undefined;
    const normalizedCorrectOptionIds = Array.from(new Set(pollBuilder.correctOptionIds))
      .sort((a, b) => a - b);

    if (openPeriodNum !== undefined && (!Number.isFinite(openPeriodNum) || openPeriodNum < 5 || openPeriodNum > 2_628_000)) {
      setErrorText('open_period must be between 5 and 2628000 seconds.');
      return;
    }

    if (pollBuilder.type === 'quiz') {
      if (normalizedCorrectOptionIds.length === 0) {
        setErrorText('Quiz requires at least one correct option.');
        return;
      }
      if (normalizedCorrectOptionIds.some((id) => id < 0 || id >= options.length)) {
        setErrorText('Quiz correct option is invalid.');
        return;
      }
      if (!pollBuilder.allowsMultipleAnswers && normalizedCorrectOptionIds.length !== 1) {
        setErrorText('Quiz with single-answer mode must have exactly one correct option.');
        return;
      }
    }

    try {
      await sendPoll(selectedBotToken, {
        chat_id: selectedChatId,
        message_thread_id: outboundMessageThreadId,
        question,
        question_parse_mode: parseOrUndefined(pollBuilder.questionParseMode),
        options: options.map((text) => ({
          text,
          text_parse_mode: parseOrUndefined(pollBuilder.optionsParseMode),
        })),
        is_anonymous: pollBuilder.isAnonymous,
        type: pollBuilder.type,
        allows_revoting: pollBuilder.allowsRevoting,
        allows_multiple_answers: pollBuilder.allowsMultipleAnswers,
        correct_option_ids: pollBuilder.type === 'quiz' ? normalizedCorrectOptionIds : undefined,
        explanation: pollBuilder.type === 'quiz' ? (pollBuilder.explanation.trim() || undefined) : undefined,
        explanation_parse_mode: pollBuilder.type === 'quiz' ? parseOrUndefined(pollBuilder.explanationParseMode) : undefined,
        description: pollBuilder.description.trim() || undefined,
        description_parse_mode: parseOrUndefined(pollBuilder.descriptionParseMode),
        open_period: openPeriodNum,
        close_date: closeDateUnix,
        is_closed: pollBuilder.isClosed || undefined,
      });

      setPollBuilder({
        type: 'regular',
        question: '',
        options: ['', ''],
        optionsParseMode: 'none',
        isAnonymous: false,
        allowsRevoting: true,
        allowsMultipleAnswers: false,
        correctOptionIds: [0],
        explanation: '',
        questionParseMode: 'none',
        explanationParseMode: 'none',
        description: '',
        descriptionParseMode: 'none',
        openPeriod: '',
        closeDate: '',
        isClosed: false,
      });
      setErrorText('');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Poll send failed');
    }
  };

  const submitInvoiceBuilder = async () => {
    if (!ensureActiveForumTopicWritable()) {
      return;
    }

    const title = invoiceBuilder.title.trim();
    const description = invoiceBuilder.description.trim();
    const amount = Number(invoiceBuilder.amount);
    const currency = invoiceBuilder.currency.trim().toUpperCase();
    const payload = invoiceBuilder.payload.trim() || `invoice_${Date.now()}`;
    const maxTipAmount = invoiceBuilder.maxTipAmount.trim() ? Number(invoiceBuilder.maxTipAmount) : 0;
    const suggestedTipAmounts = invoiceBuilder.suggestedTips
      .split(',')
      .map((item) => Number(item.trim()))
      .filter((item) => Number.isFinite(item) && item > 0)
      .map((item) => Math.floor(item));
    const photoUrl = invoiceBuilder.photoUrl.trim();

    if (!title) {
      setErrorText('Invoice title is required.');
      return;
    }
    if (!description) {
      setErrorText('Invoice description is required.');
      return;
    }
    if (!Number.isFinite(amount) || amount <= 0) {
      setErrorText('Invoice amount must be greater than 0.');
      return;
    }
    if (!currency) {
      setErrorText('Invoice currency is required.');
      return;
    }
    if (!Number.isFinite(maxTipAmount) || maxTipAmount < 0) {
      setErrorText('Max tip amount must be a non-negative number.');
      return;
    }
    if (suggestedTipAmounts.length > 4) {
      setErrorText('Suggested tips can contain at most 4 values.');
      return;
    }

    const isStarsCurrency = currency === 'XTR';
    if (
      isStarsCurrency
      && (
        invoiceBuilder.needShippingAddress
        || invoiceBuilder.isFlexible
        || invoiceBuilder.needName
        || invoiceBuilder.needPhoneNumber
        || invoiceBuilder.needEmail
        || invoiceBuilder.sendPhoneNumberToProvider
        || invoiceBuilder.sendEmailToProvider
        || maxTipAmount > 0
        || suggestedTipAmounts.length > 0
      )
    ) {
      setErrorText('Shipping, contact collection, and tips are not supported for Telegram Stars invoices.');
      return;
    }

    if (invoiceBuilder.isFlexible && !invoiceBuilder.needShippingAddress) {
      setErrorText('Flexible shipping requires Need shipping.');
      return;
    }

    try {
      await sendInvoice(selectedBotToken, {
        chat_id: selectedChatId,
        message_thread_id: outboundMessageThreadId,
        title,
        description,
        payload,
        provider_token: isStarsCurrency ? '' : 'sim_provider_token',
        currency,
        prices: [
          {
            label: title,
            amount: Math.floor(amount),
          },
        ],
        max_tip_amount: isStarsCurrency ? undefined : (maxTipAmount > 0 ? Math.floor(maxTipAmount) : undefined),
        suggested_tip_amounts: isStarsCurrency ? undefined : (suggestedTipAmounts.length > 0 ? suggestedTipAmounts : undefined),
        photo_url: photoUrl || undefined,
        need_name: isStarsCurrency ? false : invoiceBuilder.needName,
        need_phone_number: isStarsCurrency ? false : invoiceBuilder.needPhoneNumber,
        need_email: isStarsCurrency ? false : invoiceBuilder.needEmail,
        need_shipping_address: isStarsCurrency ? false : invoiceBuilder.needShippingAddress,
        send_phone_number_to_provider: isStarsCurrency ? false : invoiceBuilder.sendPhoneNumberToProvider,
        send_email_to_provider: isStarsCurrency ? false : invoiceBuilder.sendEmailToProvider,
        is_flexible: isStarsCurrency ? false : invoiceBuilder.isFlexible,
      });

      setErrorText('');
      setInvoiceBuilder((prev) => ({
        ...prev,
        payload: '',
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Invoice send failed');
    }
  };

  const stickerEmojiList = useMemo(
    () => stickerStudio.emojiList.split(',').map((item) => item.trim()).filter(Boolean),
    [stickerStudio.emojiList],
  );

  const stickerKeywordList = useMemo(
    () => stickerStudio.keywords.split(',').map((item) => item.trim()).filter(Boolean),
    [stickerStudio.keywords],
  );

  const uploadStickerAsset = async () => {
    if (!stickerStudioFile) {
      setErrorText('Select a sticker file first.');
      return;
    }

    try {
      const uploaded = await uploadStickerFile(selectedBotToken, {
        user_id: Number(stickerStudio.userId) || selectedUser.id,
        sticker_format: stickerStudio.stickerFormat,
        sticker: { extra: stickerStudioFile },
      });
      setUploadedStickerFileId(uploaded.file_id);
      setStickerStudioOutput(`Uploaded sticker file_id: ${uploaded.file_id}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Sticker upload failed');
    }
  };

  const createStickerSetAction = async () => {
    if (!uploadedStickerFileId) {
      setErrorText('Upload sticker file first.');
      return;
    }
    if (!stickerStudio.setName.trim() || !stickerStudio.setTitle.trim()) {
      setErrorText('Sticker set name/title are required.');
      return;
    }
    if (stickerEmojiList.length === 0) {
      setErrorText('Provide at least one emoji in emoji list.');
      return;
    }

    try {
      await createNewStickerSet(selectedBotToken, {
        user_id: Number(stickerStudio.userId) || selectedUser.id,
        name: stickerStudio.setName.trim(),
        title: stickerStudio.setTitle.trim(),
        sticker_type: stickerStudio.stickerType,
        needs_repainting: stickerStudio.stickerType === 'custom_emoji'
          ? stickerStudio.needsRepainting
          : undefined,
        stickers: [
          {
            sticker: uploadedStickerFileId,
            format: stickerStudio.stickerFormat,
            emoji_list: stickerEmojiList,
            keywords: stickerKeywordList.length ? stickerKeywordList : undefined,
            mask_position: stickerStudio.stickerType === 'mask'
              ? {
                point: stickerStudio.maskPoint,
                x_shift: Number(stickerStudio.maskXShift) || 0,
                y_shift: Number(stickerStudio.maskYShift) || 0,
                scale: Number(stickerStudio.maskScale) || 1,
              }
              : undefined,
          },
        ],
      });
      await loadStickerSetIntoShelf(stickerStudio.setName.trim(), { silent: true });
      setStickerStudioOutput(`Sticker set created: ${stickerStudio.setName.trim()}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Create sticker set failed');
    }
  };

  const addStickerToSetAction = async () => {
    if (!uploadedStickerFileId || !stickerStudio.setName.trim()) {
      setErrorText('Upload sticker and provide set name.');
      return;
    }
    if (stickerEmojiList.length === 0) {
      setErrorText('Provide at least one emoji in emoji list.');
      return;
    }

    try {
      await addStickerToSet(selectedBotToken, {
        user_id: Number(stickerStudio.userId) || selectedUser.id,
        name: stickerStudio.setName.trim(),
        sticker: {
          sticker: uploadedStickerFileId,
          format: stickerStudio.stickerFormat,
          emoji_list: stickerEmojiList,
          keywords: stickerKeywordList.length ? stickerKeywordList : undefined,
          mask_position: stickerStudio.stickerType === 'mask'
            ? {
              point: stickerStudio.maskPoint,
              x_shift: Number(stickerStudio.maskXShift) || 0,
              y_shift: Number(stickerStudio.maskYShift) || 0,
              scale: Number(stickerStudio.maskScale) || 1,
            }
            : undefined,
        },
      });
      await loadStickerSetIntoShelf(stickerStudio.setName.trim(), { silent: true });
      setStickerStudioOutput(`Sticker added to: ${stickerStudio.setName.trim()}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Add sticker failed');
    }
  };

  const fetchStickerSetAction = async () => {
    if (!stickerStudio.setName.trim()) {
      setErrorText('Set name is required.');
      return;
    }

    try {
      const result = await getStickerSet(selectedBotToken, { name: stickerStudio.setName.trim() });
      setStickerStudioOutput(JSON.stringify(result, null, 2));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Get sticker set failed');
    }
  };

  const applyStickerSetMetaActions = async () => {
    if (!stickerStudio.setName.trim()) {
      setErrorText('Set name is required.');
      return;
    }

    try {
      await setStickerSetTitle(selectedBotToken, {
        name: stickerStudio.setName.trim(),
        title: stickerStudio.setTitle.trim() || stickerStudio.setName.trim(),
      });

      if (stickerStudioThumbnailFile) {
        await setStickerSetThumbnail(selectedBotToken, {
          name: stickerStudio.setName.trim(),
          user_id: Number(stickerStudio.userId) || selectedUser.id,
          thumbnail: { extra: stickerStudioThumbnailFile } as unknown as Record<string, unknown>,
          format: stickerStudio.stickerFormat,
        });
      }

      if (stickerStudio.customEmojiId.trim()) {
        await setCustomEmojiStickerSetThumbnail(selectedBotToken, {
          name: stickerStudio.setName.trim(),
          custom_emoji_id: stickerStudio.customEmojiId.trim(),
        });
      }

      await loadStickerSetIntoShelf(stickerStudio.setName.trim(), { silent: true });
      setStickerStudioOutput(`Sticker set metadata updated: ${stickerStudio.setName.trim()}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Sticker set metadata update failed');
    }
  };

  const applyStickerItemActions = async () => {
    const stickerRef = stickerStudio.targetStickerId.trim() || uploadedStickerFileId;
    if (!stickerRef) {
      setErrorText('Sticker file_id is required for item actions.');
      return;
    }
    if (stickerEmojiList.length === 0) {
      setErrorText('Provide at least one emoji in emoji list.');
      return;
    }

    try {
      await setStickerEmojiList(selectedBotToken, {
        sticker: stickerRef,
        emoji_list: stickerEmojiList,
      });
      await setStickerKeywords(selectedBotToken, {
        sticker: stickerRef,
        keywords: stickerKeywordList.length ? stickerKeywordList : undefined,
      });
      if (stickerStudio.stickerType === 'mask') {
        await setStickerMaskPosition(selectedBotToken, {
          sticker: stickerRef,
          mask_position: {
            point: stickerStudio.maskPoint,
            x_shift: Number(stickerStudio.maskXShift) || 0,
            y_shift: Number(stickerStudio.maskYShift) || 0,
            scale: Number(stickerStudio.maskScale) || 1,
          },
        });
      }

      if (stickerStudio.setName.trim()) {
        await loadStickerSetIntoShelf(stickerStudio.setName.trim(), { silent: true });
      }
      setStickerStudioOutput(`Sticker item metadata updated: ${stickerRef}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Sticker item actions failed');
    }
  };

  const reorderOrReplaceStickerAction = async () => {
    if (!stickerStudio.targetStickerId.trim()) {
      setErrorText('target sticker id is required.');
      return;
    }

    try {
      await setStickerPositionInSet(selectedBotToken, {
        sticker: stickerStudio.targetStickerId.trim(),
        position: Number(stickerStudio.position) || 0,
      });

      if (stickerStudio.oldStickerId.trim() && uploadedStickerFileId && stickerStudio.setName.trim()) {
        await replaceStickerInSet(selectedBotToken, {
          user_id: Number(stickerStudio.userId) || selectedUser.id,
          name: stickerStudio.setName.trim(),
          old_sticker: stickerStudio.oldStickerId.trim(),
          sticker: {
            sticker: uploadedStickerFileId,
            format: stickerStudio.stickerFormat,
            emoji_list: stickerEmojiList,
            keywords: stickerKeywordList.length ? stickerKeywordList : undefined,
          },
        });
      }

      if (stickerStudio.setName.trim()) {
        await loadStickerSetIntoShelf(stickerStudio.setName.trim(), { silent: true });
      }
      setStickerStudioOutput('Sticker position/replace operation completed.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Sticker reorder/replace failed');
    }
  };

  const deleteStickerActions = async () => {
    try {
      if (stickerStudio.targetStickerId.trim()) {
        await deleteStickerFromSet(selectedBotToken, { sticker: stickerStudio.targetStickerId.trim() });
      }
      if (stickerStudio.setName.trim()) {
        await deleteStickerSet(selectedBotToken, { name: stickerStudio.setName.trim() });
        setStickerShelf((prev) => prev.filter((set) => set.name !== stickerStudio.setName.trim()));
        setStickerShelfActiveSet((prev) => (prev === stickerStudio.setName.trim() ? '' : prev));
      }
      setStickerStudioOutput('Delete action completed (sticker and/or set).');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Delete sticker/set failed');
    }
  };

  const queryCustomEmojiStickersAction = async () => {
    if (!stickerStudio.customEmojiId.trim()) {
      setErrorText('custom emoji id is required.');
      return;
    }

    try {
      const result = await getCustomEmojiStickers(selectedBotToken, {
        custom_emoji_ids: stickerStudio.customEmojiId.split(',').map((item) => item.trim()).filter(Boolean),
      });
      setStickerStudioOutput(JSON.stringify(result, null, 2));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'getCustomEmojiStickers failed');
    }
  };

  const loadStickerSetIntoShelf = async (setNameRaw?: string, options?: { silent?: boolean }) => {
    const setName = (setNameRaw || stickerShelfNameInput).trim();
    if (!setName) {
      if (!options?.silent) {
        setErrorText('Set name is required.');
      }
      return;
    }

    try {
      const result = await getStickerSet(selectedBotToken, { name: setName });
      const normalized: StickerShelfSet = {
        name: result.name,
        title: result.title,
        stickers: (result.stickers || []).map((item) => ({
          file_id: item.file_id,
          file_unique_id: item.file_unique_id,
          is_video: Boolean(item.is_video),
          is_animated: Boolean(item.is_animated),
          set_name: item.set_name,
          emoji: item.emoji,
        })),
      };

      setStickerShelf((prev) => {
        const idx = prev.findIndex((entry) => entry.name === normalized.name);
        if (idx >= 0) {
          const next = [...prev];
          next[idx] = normalized;
          return next;
        }
        return [...prev, normalized];
      });
      setStickerShelfActiveSet(normalized.name);
      setStickerShelfNameInput('');
    } catch (error) {
      if (!options?.silent) {
        setErrorText(error instanceof Error ? error.message : 'Unable to load sticker set');
      }
    }
  };

  const sendUserMediaByFileRef = async (
    mediaKind: 'sticker' | 'animation' | 'video_note' | 'voice',
    mediaRef: string,
  ) => {
    if (!ensureActiveForumTopicWritable()) {
      return;
    }
    if (!ensureDirectMessagesStarsAvailable(1)) {
      return;
    }

    try {
      await sendUserMediaByReference(selectedBotToken, {
        chatId: selectedChatId,
        messageThreadId: outboundMessageThreadId,
        directMessagesTopicId: activeDirectMessagesTopicId,
        businessConnectionId: activeBusinessConnectionId,
        userId: selectedUser.id,
        firstName: selectedUser.first_name,
        username: selectedUser.username,
        senderChatId: activeDiscussionSenderChatId,
        mediaKind,
        media: mediaRef,
        replyToMessageId: resolveComposerReplyTargetId(replyTarget?.id),
      });
      consumeDirectMessagesStars(1);
      setShowMediaDrawer(false);
      setReplyTarget(null);
      isNearBottomRef.current = true;
      window.setTimeout(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
      }, 0);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'User media send failed');
    }
  };

  const sendUserMediaFile = async (
    file: File | null,
    mediaKind: 'animation' | 'voice' | 'video_note',
  ) => {
    if (!ensureActiveForumTopicWritable()) {
      return;
    }

    if (!file) {
      setErrorText('Select a file first.');
      return;
    }
    if (!ensureDirectMessagesStarsAvailable(1)) {
      return;
    }

    try {
      await sendUserMedia(selectedBotToken, {
        chatId: selectedChatId,
        messageThreadId: outboundMessageThreadId,
        directMessagesTopicId: activeDirectMessagesTopicId,
        businessConnectionId: activeBusinessConnectionId,
        userId: selectedUser.id,
        firstName: selectedUser.first_name,
        username: selectedUser.username,
        senderChatId: activeDiscussionSenderChatId,
        file,
        mediaKind,
        replyToMessageId: resolveComposerReplyTargetId(replyTarget?.id),
      });
      consumeDirectMessagesStars(1);
      if (mediaKind === 'animation') {
        setAnimationUploadFile(null);
      }
      if (mediaKind === 'voice') {
        setVoiceUploadFile(null);
      }
      if (mediaKind === 'video_note') {
        setVideoNoteUploadFile(null);
      }
      setShowMediaDrawer(false);
      setReplyTarget(null);
      isNearBottomRef.current = true;
      window.setTimeout(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
      }, 0);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'User media upload failed');
    }
  };

  const startVoiceRecording = async () => {
    if (!canUseMicrophone || isRecordingVoice) {
      return;
    }

    try {
      setVoiceRecordError('');
      setRecordedVoiceBlob(null);
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const recorder = new MediaRecorder(stream);
      voiceRecorderChunksRef.current = [];

      recorder.ondataavailable = (event) => {
        if (event.data && event.data.size > 0) {
          voiceRecorderChunksRef.current.push(event.data);
        }
      };

      recorder.onstop = () => {
        const blob = new Blob(voiceRecorderChunksRef.current, { type: recorder.mimeType || 'audio/ogg' });
        setRecordedVoiceBlob(blob.size > 0 ? blob : null);
        stream.getTracks().forEach((track) => track.stop());
        setIsRecordingVoice(false);
      };

      recorder.onerror = () => {
        setVoiceRecordError('Voice recording failed. You can still upload an audio file.');
        stream.getTracks().forEach((track) => track.stop());
        setIsRecordingVoice(false);
      };

      voiceRecorderRef.current = recorder;
      recorder.start();
      setIsRecordingVoice(true);
    } catch {
      setVoiceRecordError('Microphone is unavailable. Use upload fallback.');
      setIsRecordingVoice(false);
    }
  };

  const stopVoiceRecording = () => {
    const recorder = voiceRecorderRef.current;
    if (!recorder || recorder.state !== 'recording') {
      return;
    }
    recorder.stop();
  };

  const sendRecordedVoice = async () => {
    if (!recordedVoiceBlob) {
      setErrorText('No recorded voice found.');
      return;
    }

    const file = new File([recordedVoiceBlob], `voice_${Date.now()}.ogg`, { type: recordedVoiceBlob.type || 'audio/ogg' });
    await sendUserMediaFile(file, 'voice');
    setRecordedVoiceBlob(null);
  };

  const toggleVideoNotePlayback = (messageId: number) => {
    const node = videoNoteRefs.current[messageId];
    if (!node) {
      return;
    }

    if (node.paused) {
      void node.play().then(() => {
        setPlayingVideoNoteMessageId(messageId);
      }).catch(() => {
        setPlayingVideoNoteMessageId(null);
      });
    } else {
      node.pause();
      setPlayingVideoNoteMessageId(null);
    }
  };

  const resolveInvoiceForPayButton = (message: ChatMessage): ChatMessage | null => {
    if (message.invoice) {
      return message;
    }

    if (message.replyTo?.messageId) {
      const repliedInvoice = messages.find((item) => (
        item.botToken === message.botToken
        && item.chatId === message.chatId
        && item.id === message.replyTo!.messageId
        && Boolean(item.invoice)
      ));
      if (repliedInvoice?.invoice) {
        return repliedInvoice;
      }
    }

    return [...messages]
      .reverse()
      .find((item) => (
        item.botToken === message.botToken
        && item.chatId === message.chatId
        && Boolean(item.invoice)
      )) || null;
  };

  const onPayInvoice = async (
    message: ChatMessage,
    outcome: 'success' | 'failed' = 'success',
    methodOverride?: PaymentMethod,
    tipOverride?: number,
  ) => {
    if (!message.invoice) {
      return;
    }

    const amount = message.invoice.total_amount;
    const isStarsCurrency = message.invoice.currency.toUpperCase() === 'XTR';
    const selectedMethod = methodOverride || paymentMethodByInvoice[message.id] || (isStarsCurrency ? 'stars' : 'wallet');
    const tipAmount = isStarsCurrency
      ? 0
      : Math.max(Math.floor(
        typeof tipOverride === 'number'
          ? tipOverride
          : (Number(paymentTipByInvoice[message.id] || '0') || 0),
      ), 0);
    const totalDebit = amount + tipAmount;

    if (isStarsCurrency && selectedMethod !== 'stars') {
      setErrorText('XTR invoice can only be paid with Telegram Stars.');
      return;
    }

    if (!isStarsCurrency && selectedMethod === 'stars') {
      setErrorText('Telegram Stars payment is only available for XTR invoices.');
      return;
    }

    if (selectedMethod === 'wallet' && walletState.fiat < totalDebit) {
        setErrorText('Not enough wallet balance.');
        return;
    }

    if (selectedMethod === 'stars' && walletState.stars < amount) {
      setErrorText('Not enough stars for this payment.');
      return;
    }

    try {
      const result = await payInvoice(selectedBotToken, {
        chat_id: message.chatId,
        message_id: message.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        payment_method: selectedMethod,
        outcome,
        tip_amount: tipAmount > 0 ? tipAmount : undefined,
      });

      if (result.status === 'success') {
        if (selectedMethod === 'wallet') {
          setWalletState((prev) => ({
            ...prev,
            fiat: Math.max(prev.fiat - totalDebit, 0),
          }));
        }

        if (selectedMethod === 'stars') {
          setWalletState((prev) => ({
            ...prev,
            stars: Math.max(prev.stars - amount, 0),
          }));
        }
      }

      setErrorText(result.status === 'success'
        ? `Payment succeeded via ${result.payment_method}`
        : `Payment failed via ${result.payment_method}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Invoice payment failed');
    }
  };

  const openCheckoutFlow = (message: ChatMessage) => {
    if (!message.invoice) {
      return;
    }

    const isStarsCurrency = message.invoice.currency.toUpperCase() === 'XTR';
    setCheckoutFlow({
      messageId: message.id,
      step: 1,
      method: paymentMethodByInvoice[message.id] || (isStarsCurrency ? 'stars' : 'wallet'),
      outcome: 'success',
      tip: paymentTipByInvoice[message.id] || '',
    });
  };

  const checkoutMessage = useMemo(
    () => (checkoutFlow
      ? visibleMessages.find((message) => message.id === checkoutFlow.messageId && Boolean(message.invoice)) || null
      : null),
    [checkoutFlow, visibleMessages],
  );

  const commitCheckoutFlow = async () => {
    if (!checkoutFlow || !checkoutMessage?.invoice) {
      return;
    }

    const tipValue = checkoutMessage.invoice.currency.toUpperCase() === 'XTR'
      ? 0
      : Math.max(Math.floor(Number(checkoutFlow.tip || '0') || 0), 0);

    setPaymentMethodByInvoice((prev) => ({
      ...prev,
      [checkoutMessage.id]: checkoutFlow.method,
    }));
    setPaymentTipByInvoice((prev) => ({
      ...prev,
      [checkoutMessage.id]: String(tipValue),
    }));

    await onPayInvoice(checkoutMessage, checkoutFlow.outcome, checkoutFlow.method, tipValue);
    setCheckoutFlow(null);
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

  const onCreateGroup = async () => {
    const title = groupDraft.title.trim();
    if (!title || !selectedBotToken || isCreatingGroup) {
      return;
    }

    const scopedType: 'group' | 'supergroup' | 'channel' = chatScopeTab === 'channel'
      ? 'channel'
      : (groupDraft.type === 'channel' ? 'supergroup' : groupDraft.type);

    setIsCreatingGroup(true);
    setErrorText('');
    try {
      const created = await createSimulationGroup(selectedBotToken, {
        title,
        chat_type: scopedType,
        owner_user_id: selectedUser.id,
        owner_first_name: selectedUser.first_name,
        owner_username: selectedUser.username,
        username: groupDraft.username.trim() || undefined,
        description: groupDraft.description.trim() || undefined,
        is_forum: scopedType === 'supergroup' ? groupDraft.isForum : false,
      });

      const chat = created.chat;
      const settings = mapServerSettingsToSnapshot(created.settings);
      const mapped: GroupChatItem = {
        id: chat.id,
        type: chat.type as 'group' | 'supergroup' | 'channel',
        title: chat.title || title,
        username: chat.username || undefined,
        description: groupDraft.description.trim() || undefined,
        isForum: chat.is_forum || false,
        isDirectMessages: Boolean(chat.is_direct_messages),
        parentChannelChatId: undefined,
        settings,
      };

      setGroupChats((prev) => {
        const byId = new Map<number, GroupChatItem>();
        [...prev, mapped].forEach((group) => byId.set(group.id, group));
        return Array.from(byId.values()).sort((a, b) => a.title.localeCompare(b.title));
      });
      setSelectedGroupChatId((current) => current ?? mapped.id);
      setGroupMembershipByUser((prev) => {
        const next = {
          ...prev,
          [`${selectedBotToken}:${mapped.id}:${selectedUser.id}`]: 'owner',
        };
        if (selectedBot?.id) {
          next[`${selectedBotToken}:${mapped.id}:${selectedBot.id}`] = 'admin';
        }
        return next;
      });
      setGroupDraft({
        title: '',
        type: chatScopeTab === 'channel' ? 'channel' : (scopedType === 'group' ? 'group' : 'supergroup'),
        username: '',
        description: '',
        isForum: scopedType === 'supergroup' ? groupDraft.isForum : false,
      });
      setErrorText(`${scopedType === 'channel' ? 'Channel' : 'Group'} created: ${mapped.title}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to create chat');
    } finally {
      setIsCreatingGroup(false);
    }
  };

  const onJoinSelectedGroup = async () => {
    if (!selectedGroup) {
      return;
    }
    setIsBootstrapping(true);
    setErrorText('');
    try {
      const result = await joinSimulationGroup(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
      });
      if (result.pending) {
        setErrorText('Join request sent. Waiting for owner/admin approval.');
      } else if (result.joined) {
        setGroupMembershipByUser((prev) => ({
          ...prev,
          [`${selectedBotToken}:${selectedGroup.id}:${selectedUser.id}`]: 'member',
        }));
        setShowGroupActionsModal(false);
      }
    } catch (error) {
      const fallbackLabel = selectedGroup?.type === 'channel' ? 'channel' : 'group';
      setErrorText(error instanceof Error ? error.message : `Failed to join ${fallbackLabel}`);
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onLeaveSelectedGroup = async () => {
    if (!selectedGroup) {
      return;
    }
    setIsBootstrapping(true);
    setErrorText('');
    try {
      await leaveSimulationGroup(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
      });
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${selectedUser.id}`]: 'left',
      }));
      setShowGroupActionsModal(false);
    } catch (error) {
      const fallbackLabel = selectedGroup?.type === 'channel' ? 'channel' : 'group';
      setErrorText(error instanceof Error ? error.message : `Failed to leave ${fallbackLabel}`);
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onOpenChannelDirectMessages = async () => {
    if (!selectedGroup || selectedGroup.type !== 'channel') {
      return;
    }
    if (!selectedGroup.settings?.directMessagesEnabled) {
      setErrorText('Enable channel direct messages in Channel Info and save first.');
      return;
    }

    setIsGroupActionRunning(true);
    setErrorText('');
    try {
      const result = await openSimulationChannelDirectMessages(selectedBotToken, {
        channel_chat_id: selectedGroup.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
      });

      const mappedChat: GroupChatItem = {
        id: result.chat.id,
        type: result.chat.type as 'group' | 'supergroup' | 'channel',
        title: result.chat.title || `${selectedGroup.title} Direct Messages`,
        username: result.chat.username || undefined,
        description: undefined,
        isForum: false,
        isDirectMessages: Boolean(result.chat.is_direct_messages),
        parentChannelChatId: result.parent_chat.id,
        linkedDiscussionChatId: undefined,
        settings: defaultGroupSettings(),
      };

      setGroupChats((prev) => {
        const byId = new Map<number, GroupChatItem>();
        [...prev, mappedChat].forEach((chat) => {
          byId.set(chat.id, chat.id === mappedChat.id ? mappedChat : chat);
        });
        return Array.from(byId.values()).sort((a, b) => a.title.localeCompare(b.title));
      });

      const dmChatStateKey = `${selectedBotToken}:${mappedChat.id}`;
      const dmTopics = (result.topics || [])
        .map((topic): ForumTopicState | null => {
          const topicId = Math.floor(Number(topic.topic_id));
          if (!Number.isFinite(topicId) || topicId <= 0) {
            return null;
          }

          return {
            messageThreadId: topicId,
            name: topic.name || `User ${topicId}`,
            iconColor: DEFAULT_FORUM_ICON_COLOR,
            iconCustomEmojiId: undefined,
            isClosed: false,
            isHidden: false,
            isGeneral: false,
            updatedAt: Number.isFinite(Number(topic.updated_at)) ? Math.floor(Number(topic.updated_at)) : undefined,
          };
        })
        .filter((topic): topic is ForumTopicState => topic !== null);

      setForumTopicsByChatKey((prev) => ({
        ...prev,
        [dmChatStateKey]: normalizeForumTopics(dmTopics, { includeGeneralFallback: false }),
      }));

      setSelectedForumTopicByChatKey((prev) => {
        const existing = prev[dmChatStateKey];
        if (!canSelectedUserManageChannelDirectMessages) {
          if (existing === selectedUser.id) {
            return prev;
          }
          return {
            ...prev,
            [dmChatStateKey]: selectedUser.id,
          };
        }

        if (Number.isFinite(existing) && existing > 0 && dmTopics.some((topic) => topic.messageThreadId === existing)) {
          return prev;
        }

        const fallbackTopic = dmTopics[0]?.messageThreadId;
        if (!fallbackTopic) {
          return prev;
        }

        return {
          ...prev,
          [dmChatStateKey]: fallbackTopic,
        };
      });

      setShowGroupProfileModal(false);
      setShowGroupActionsModal(false);
      setGroupSettingsPage('home');
      setChatMenuOpen(false);
      const applySelection = () => {
        setChatScopeTab('group');
        setSelectedGroupChatId(mappedChat.id);
        setSelectedGroupByBot((prev) => ({
          ...prev,
          [selectedBotToken]: mappedChat.id,
        }));
      };
      if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
        window.requestAnimationFrame(applySelection);
      } else {
        applySelection();
      }
      setErrorText(`Opened direct messages for ${selectedGroup.title}.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to open channel direct messages');
    } finally {
      setIsGroupActionRunning(false);
    }
  };

  const onSetSelectedBotMembership = async (status: 'admin' | 'member' | 'left') => {
    if (!selectedGroup || !selectedBot) {
      return;
    }
    if (!canEditSelectedGroup) {
      setErrorText('Only owner/admin can manage bot membership.');
      return;
    }

    setIsBootstrapping(true);
    setErrorText('');
    try {
      const result = await setSimulationBotGroupMembership(selectedBotToken, {
        chat_id: selectedGroup.id,
        actor_user_id: selectedUser.id,
        actor_first_name: selectedUser.first_name,
        actor_username: selectedUser.username,
        status,
      });

      const key = `${selectedBotToken}:${selectedGroup.id}:${selectedBot.id}`;
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [key]: normalizeMembershipStatus(result.status),
      }));

      if (status === 'left') {
        setErrorText('Bot removed from group.');
      } else if (status === 'admin') {
        setErrorText('Bot is now group admin.');
      } else {
        setErrorText('Bot added as member.');
      }
      setShowGroupActionsModal(false);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to update bot membership');
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onCreateGroupInviteLink = async (createsJoinRequest: boolean) => {
    if (!selectedGroup) {
      return;
    }
    if (!canEditSelectedGroup) {
      setErrorText('Only owner/admin can create invite links.');
      return;
    }

    setIsBootstrapping(true);
    setErrorText('');
    try {
      const invite = await createChatInviteLink(selectedBotToken, {
        chat_id: selectedGroup.id,
        creates_join_request: createsJoinRequest,
      }, selectedUser.id);

      const key = `${selectedBotToken}:${selectedGroup.id}`;
      setGroupInviteLinksByChat((prev) => ({
        ...prev,
        [key]: invite.invite_link,
      }));
      setGroupInviteLinkInput(invite.invite_link);
      setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink: invite.invite_link }));

      try {
        await navigator.clipboard.writeText(invite.invite_link);
        setErrorText(createsJoinRequest ? 'Join-request invite link created and copied.' : 'Invite link created and copied.');
      } catch {
        setErrorText(createsJoinRequest ? 'Join-request invite link created.' : 'Invite link created.');
      }
      setShowGroupActionsModal(false);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to create invite link');
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onExportPrimaryInviteLink = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      const inviteLink = await exportChatInviteLink(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      const key = `${selectedBotToken}:${selectedGroup.id}`;
      setGroupInviteLinksByChat((prev) => ({
        ...prev,
        [key]: inviteLink,
      }));
      setGroupInviteLinkInput(inviteLink);
      setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink }));
      setErrorText('Primary invite link exported via official API.');
    });
  };

  const onEditInviteLinkByDraft = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    const inviteLink = groupInviteEditorDraft.inviteLink.trim();
    if (!inviteLink) {
      setErrorText('Invite link is required for editing.');
      return;
    }

    await runGroupAction(async () => {
      const edited = await editChatInviteLink(selectedBotToken, {
        chat_id: selectedGroup.id,
        invite_link: inviteLink,
        name: groupInviteEditorDraft.name.trim() || undefined,
        expire_date: groupInviteEditorDraft.expireDate.trim()
          ? Math.max(0, Math.floor(Number(groupInviteEditorDraft.expireDate) || 0))
          : undefined,
        member_limit: groupInviteEditorDraft.memberLimit.trim()
          ? Math.max(0, Math.floor(Number(groupInviteEditorDraft.memberLimit) || 0))
          : undefined,
        creates_join_request: groupInviteEditorDraft.createsJoinRequest,
      }, selectedUser.id);

      const key = `${selectedBotToken}:${selectedGroup.id}`;
      setGroupInviteLinksByChat((prev) => ({
        ...prev,
        [key]: edited.invite_link,
      }));
      setGroupInviteLinkInput(edited.invite_link);
      setErrorText('Invite link updated via official API.');
    });
  };

  const onRevokeInviteLinkByDraft = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    const inviteLink = groupInviteEditorDraft.inviteLink.trim();
    if (!inviteLink) {
      setErrorText('Invite link is required for revoke.');
      return;
    }

    await runGroupAction(async () => {
      const revoked = await revokeChatInviteLink(selectedBotToken, {
        chat_id: selectedGroup.id,
        invite_link: inviteLink,
      }, selectedUser.id);
      setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink: revoked.invite_link }));
      setErrorText('Invite link revoked via official API.');
    });
  };

  const onCreateSubscriptionInviteLinkByDraft = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      const created = await createChatSubscriptionInviteLink(selectedBotToken, {
        chat_id: selectedGroup.id,
        name: groupInviteEditorDraft.name.trim() || undefined,
        subscription_period: Math.max(1, Math.floor(Number(groupInviteEditorDraft.subscriptionPeriod) || 1)),
        subscription_price: Math.max(1, Math.floor(Number(groupInviteEditorDraft.subscriptionPrice) || 1)),
      }, selectedUser.id);
      setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink: created.invite_link }));
      setErrorText('Subscription invite link created via official API.');
    });
  };

  const onEditSubscriptionInviteLinkByDraft = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    const inviteLink = groupInviteEditorDraft.inviteLink.trim();
    if (!inviteLink) {
      setErrorText('Subscription invite link is required for editing.');
      return;
    }

    await runGroupAction(async () => {
      const edited = await editChatSubscriptionInviteLink(selectedBotToken, {
        chat_id: selectedGroup.id,
        invite_link: inviteLink,
        name: groupInviteEditorDraft.name.trim() || undefined,
      }, selectedUser.id);
      setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink: edited.invite_link }));
      setErrorText('Subscription invite link updated via official API.');
    });
  };

  const renderInspector = (label: string, value: unknown) => {
    setGroupInspectorOutput(`${label}\n${JSON.stringify(value, null, 2)}`);
  };

  const applyGroupMemberMetaState = (targetUserId: number, member: GeneratedChatMember) => {
    if (!selectedGroupStateKey) {
      return;
    }

    const parsed = parseGroupMemberMeta(member);
    setGroupMemberMetaByChatKey((prev) => ({
      ...prev,
      [selectedGroupStateKey]: {
        ...(prev[selectedGroupStateKey] || {}),
        [targetUserId]: parsed,
      },
    }));
    if (parsed.customTitle !== undefined) {
      setGroupMemberAdminTitleByChatKey((prev) => ({
        ...prev,
        [selectedGroupStateKey]: {
          ...(prev[selectedGroupStateKey] || {}),
          [targetUserId]: parsed.customTitle || '',
        },
      }));
    }
    if (parsed.tag !== undefined) {
      setGroupMemberTagByChatKey((prev) => ({
        ...prev,
        [selectedGroupStateKey]: {
          ...(prev[selectedGroupStateKey] || {}),
          [targetUserId]: parsed.tag || '',
        },
      }));
    }

    const adminRights = parseChannelAdminRightsDraft(member);
    if (adminRights) {
      setChannelAdminRightsDraftByChatKey((prev) => ({
        ...prev,
        [selectedGroupStateKey]: {
          ...(prev[selectedGroupStateKey] || {}),
          [targetUserId]: adminRights,
        },
      }));
    }
  };

  const onInspectSelectedGroupChat = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      const details = await getChat(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      renderInspector('getChat', details);
      if (details.invite_link) {
        setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink: details.invite_link || prev.inviteLink }));
      }
      setErrorText('getChat completed.');
    });
  };

  const onInspectSelectedGroupAdmins = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      const admins = await getChatAdministrators(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      renderInspector('getChatAdministrators', admins);
      setErrorText('getChatAdministrators completed.');
    });
  };

  const onInspectSelectedGroupMemberCount = async () => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      const count = await getChatMemberCount(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      renderInspector('getChatMemberCount', { count });
      setErrorText('getChatMemberCount completed.');
    });
  };

  const onInspectSelectedGroupMember = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      const member = await getChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
      }, selectedUser.id);
      applyGroupMemberMetaState(targetUserId, member);
      renderInspector(`getChatMember(${targetUserId})`, member);
      setErrorText(`Fetched member ${targetUserId} via official API.`);
    });
  };

  const resolveMenuButtonTargetChatId = () => {
    if (groupMenuButtonDraft.scope === 'default') {
      return undefined;
    }
    const parsed = Math.floor(Number(groupMenuButtonDraft.targetChatId.trim()));
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
    return selectedUser.id;
  };

  const onSetGroupMenuButtonFromDraft = async () => {
    await runGroupAction(async () => {
      const payload: SetChatMenuButtonRequest = {
        chat_id: resolveMenuButtonTargetChatId(),
        menu_button: buildMenuButtonFromDraft(groupMenuButtonDraft),
      };
      await setChatMenuButton(selectedBotToken, payload, selectedUser.id);
      setGroupMenuButtonSummary(parseMenuButtonSummary(payload.menu_button as GeneratedMenuButton));
      setErrorText('setChatMenuButton completed.');
    });
  };

  const onGetGroupMenuButtonFromDraft = async () => {
    await runGroupAction(async () => {
      const payload: GetChatMenuButtonRequest = {
        chat_id: resolveMenuButtonTargetChatId(),
      };
      const menuButton = await getChatMenuButton(selectedBotToken, payload, selectedUser.id);
      setGroupMenuButtonSummary(parseMenuButtonSummary(menuButton));
      setErrorText('getChatMenuButton completed.');
    });
  };

  const onToggleGroupPrivacyMode = async () => {
    setIsGroupPrivacyModeLoading(true);
    setErrorText('');
    try {
      const next = !groupPrivacyModeEnabled;
      const updated = await setSimBotPrivacyMode(selectedBotToken, next);
      setGroupPrivacyModeEnabled(updated.enabled);
      setErrorText(`Bot privacy mode ${updated.enabled ? 'enabled' : 'disabled'}.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Privacy mode update failed');
    } finally {
      setIsGroupPrivacyModeLoading(false);
    }
  };

  const updateForumTopicsForChat = (
    chatId: number,
    updater: (topics: ForumTopicState[]) => ForumTopicState[],
  ) => {
    const chatKeyForTopics = `${selectedBotToken}:${chatId}`;
    setForumTopicsByChatKey((prev) => {
      const current = normalizeForumTopics(prev[chatKeyForTopics] || []);
      const nextTopics = normalizeForumTopics(updater(current));
      return {
        ...prev,
        [chatKeyForTopics]: nextTopics,
      };
    });
  };

  const openCreateForumTopicModal = () => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }

    setForumTopicModalMode('create');
    setForumTopicModalThreadId(null);
    setForumTopicDraft((prev) => ({
      ...prev,
      messageThreadId: '',
      name: '',
      normalEmoji: '',
      iconColor: String(activeForumTopic?.iconColor || DEFAULT_FORUM_ICON_COLOR),
      iconCustomEmojiId: '',
    }));
    setShowForumTopicModal(true);
    setForumTopicContextMenu(null);
  };

  const openEditForumTopicModal = (topic: ForumTopicState) => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }

    setForumTopicModalMode('edit');
    setForumTopicModalThreadId(topic.messageThreadId);
    const hasCustomIcon = Boolean(topic.iconCustomEmojiId?.trim());
    const parsedTopicName = splitForumTopicNameWithEmoji(topic.name);
    setForumTopicDraft((prev) => ({
      ...prev,
      messageThreadId: String(topic.messageThreadId),
      name: hasCustomIcon ? topic.name : parsedTopicName.name,
      normalEmoji: hasCustomIcon ? '' : parsedTopicName.emoji,
      iconColor: String(topic.iconColor),
      iconCustomEmojiId: topic.iconCustomEmojiId || '',
      generalName: topic.isGeneral ? topic.name : prev.generalName,
    }));
    setShowForumTopicModal(true);
    setForumTopicContextMenu(null);
  };

  const onQuickCreateForumTopic = () => {
    openCreateForumTopicModal();
  };

  const resolveForumTopicThreadId = (threadIdOverride?: number | string): number | null => {
    const source = threadIdOverride === undefined
      ? forumTopicDraft.messageThreadId.trim()
      : threadIdOverride;
    const parsed = Math.floor(Number(source));
    if (!Number.isFinite(parsed) || parsed <= 0) {
      setErrorText('Valid message_thread_id is required.');
      return null;
    }
    return parsed;
  };

  const onLoadForumTopicIconStickers = async () => {
    if (!canManageForumTopics) {
      return;
    }

    await runGroupAction(async () => {
      const stickers = await getForumTopicIconStickers(selectedBotToken, {});
      setForumTopicIconStickers(stickers.map((sticker) => ({
        file_id: sticker.file_id,
        emoji: sticker.emoji || undefined,
        custom_emoji_id: sticker.custom_emoji_id || undefined,
      })));
      renderInspector('getForumTopicIconStickers', stickers);
      setErrorText(`Loaded ${stickers.length} forum icon sticker(s).`);
    });
  };

  const onCreateForumTopicFromDraft = async (): Promise<boolean> => {
    if (!selectedGroup || !canManageForumTopics) {
      return false;
    }

    const name = buildForumTopicNameForIconMode(
      forumTopicDraft.name,
      forumTopicDraft.normalEmoji,
      forumTopicDraft.iconCustomEmojiId,
    );
    if (!name) {
      setErrorText('Topic name is required.');
      return false;
    }

    let created = false;

    await runGroupAction(async () => {
      const parsedColor = Math.floor(Number(forumTopicDraft.iconColor.trim()));
      const topic = await createForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
        name,
        icon_color: Number.isFinite(parsedColor) && parsedColor > 0 ? parsedColor : undefined,
        icon_custom_emoji_id: forumTopicDraft.iconCustomEmojiId.trim() || undefined,
      }, selectedUser.id);

      const hasCustomIcon = Boolean(topic.icon_custom_emoji_id?.trim());
      const parsedTopicName = splitForumTopicNameWithEmoji(topic.name);

      setForumTopicDraft((prev) => ({
        ...prev,
        messageThreadId: String(topic.message_thread_id),
        name: hasCustomIcon ? topic.name : parsedTopicName.name,
        normalEmoji: hasCustomIcon ? '' : parsedTopicName.emoji,
        iconColor: String(topic.icon_color),
        iconCustomEmojiId: topic.icon_custom_emoji_id || '',
      }));
      updateForumTopicsForChat(selectedGroup.id, (topics) => ([
        ...topics,
        {
          messageThreadId: topic.message_thread_id,
          name: topic.name,
          iconColor: topic.icon_color,
          iconCustomEmojiId: topic.icon_custom_emoji_id || undefined,
          isClosed: false,
          isHidden: false,
          isGeneral: false,
        },
      ]));
      if (selectedGroupStateKey) {
        setSelectedForumTopicByChatKey((prev) => ({
          ...prev,
          [selectedGroupStateKey]: topic.message_thread_id,
        }));
      }
      renderInspector('createForumTopic', topic);
      setErrorText(`Forum topic created with thread id ${topic.message_thread_id}.`);
      created = true;
    });

    return created;
  };

  const onEditForumTopicFromDraft = async (threadIdOverride?: number): Promise<boolean> => {
    if (!selectedGroup || !canManageForumTopics) {
      return false;
    }

    const messageThreadId = resolveForumTopicThreadId(threadIdOverride);
    if (!messageThreadId) {
      return false;
    }

    let updated = false;

    await runGroupAction(async () => {
      const payload: {
        chat_id: number;
        message_thread_id: number;
        name?: string;
        icon_custom_emoji_id?: string;
      } = {
        chat_id: selectedGroup.id,
        message_thread_id: messageThreadId,
      };

      const nextName = buildForumTopicNameForIconMode(
        forumTopicDraft.name,
        forumTopicDraft.normalEmoji,
        forumTopicDraft.iconCustomEmojiId,
      );
      if (nextName) {
        payload.name = nextName;
      }

      if (forumTopicDraft.iconCustomEmojiId.trim()) {
        payload.icon_custom_emoji_id = forumTopicDraft.iconCustomEmojiId.trim();
      } else {
        payload.icon_custom_emoji_id = '';
      }

      const ok = await editForumTopic(selectedBotToken, payload, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === messageThreadId
            ? {
              ...topic,
              name: payload.name || topic.name,
              iconCustomEmojiId: payload.icon_custom_emoji_id
                ? payload.icon_custom_emoji_id
                : undefined,
            }
            : topic
        )));
      }
      renderInspector('editForumTopic', { ...payload, ok });
      setErrorText(ok ? 'Forum topic updated.' : 'Forum topic update returned false.');
      updated = ok;
    });

    return updated;
  };

  const onCloseForumTopicFromDraft = async (threadIdOverride?: number) => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }

    const messageThreadId = resolveForumTopicThreadId(threadIdOverride);
    if (!messageThreadId) {
      return;
    }

    await runGroupAction(async () => {
      const ok = await closeForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
        message_thread_id: messageThreadId,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === messageThreadId
            ? { ...topic, isClosed: true }
            : topic
        )));
      }
      renderInspector('closeForumTopic', { message_thread_id: messageThreadId, ok });
      setErrorText(ok ? 'Forum topic closed.' : 'closeForumTopic returned false.');
    });
  };

  const onReopenForumTopicFromDraft = async (threadIdOverride?: number) => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }

    const messageThreadId = resolveForumTopicThreadId(threadIdOverride);
    if (!messageThreadId) {
      return;
    }

    await runGroupAction(async () => {
      const ok = await reopenForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
        message_thread_id: messageThreadId,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === messageThreadId
            ? { ...topic, isClosed: false }
            : topic
        )));
      }
      renderInspector('reopenForumTopic', { message_thread_id: messageThreadId, ok });
      setErrorText(ok ? 'Forum topic reopened.' : 'reopenForumTopic returned false.');
    });
  };

  const onDeleteForumTopicFromDraft = async (threadIdOverride?: number) => {
    if (!selectedGroup) {
      return;
    }

    const messageThreadId = resolveForumTopicThreadId(threadIdOverride);
    if (!messageThreadId) {
      return;
    }

    const topic = selectedForumTopics.find((item) => item.messageThreadId === messageThreadId) || null;
    const canDeleteTopic = canManageForumTopics || canDeleteDirectMessagesTopicByActiveActor(topic);
    if (!canDeleteTopic) {
      setErrorText('Not enough rights to delete this topic.');
      return;
    }

    if (messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID) {
      setErrorText('General forum topic cannot be deleted.');
      return;
    }

    await runGroupAction(async () => {
      const ok = await deleteForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
        message_thread_id: messageThreadId,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.filter((topic) => topic.messageThreadId !== messageThreadId));
        if (selectedGroupStateKey) {
          setSelectedForumTopicByChatKey((prev) => {
            if (prev[selectedGroupStateKey] !== messageThreadId) {
              return prev;
            }
            return {
              ...prev,
              [selectedGroupStateKey]: GENERAL_FORUM_TOPIC_THREAD_ID,
            };
          });
        }
      }
      renderInspector('deleteForumTopic', { message_thread_id: messageThreadId, ok });
      setErrorText(ok ? 'Forum topic deleted.' : 'deleteForumTopic returned false.');
    });
  };

  const onUnpinAllForumTopicMessagesFromDraft = async (threadIdOverride?: number) => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }

    const messageThreadId = resolveForumTopicThreadId(threadIdOverride);
    if (!messageThreadId) {
      return;
    }

    await runGroupAction(async () => {
      const ok = await unpinAllForumTopicMessages(selectedBotToken, {
        chat_id: selectedGroup.id,
        message_thread_id: messageThreadId,
      }, selectedUser.id);
      renderInspector('unpinAllForumTopicMessages', { message_thread_id: messageThreadId, ok });
      setErrorText(ok ? 'Unpinned all topic messages.' : 'unpinAllForumTopicMessages returned false.');
    });
  };

  const onEditGeneralForumTopicFromDraft = async (nameOverride?: string): Promise<boolean> => {
    if (!selectedGroup || !canManageForumTopics) {
      return false;
    }

    const name = (nameOverride ?? forumTopicDraft.generalName).trim();
    if (!name) {
      setErrorText('General topic name is required.');
      return false;
    }

    let updated = false;

    await runGroupAction(async () => {
      const ok = await editGeneralForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
        name,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID
            ? { ...topic, name }
            : topic
        )));
        setForumTopicDraft((prev) => ({
          ...prev,
          generalName: name,
        }));
      }
      renderInspector('editGeneralForumTopic', { name, ok });
      setErrorText(ok ? 'General forum topic updated.' : 'editGeneralForumTopic returned false.');
      updated = ok;
    });

    return updated;
  };

  const onSubmitForumTopicModal = async () => {
    if (!canManageForumTopics) {
      return;
    }

    if (forumTopicModalMode === 'create') {
      const created = await onCreateForumTopicFromDraft();
      if (created) {
        setShowForumTopicModal(false);
      }
      return;
    }

    const threadId = resolveForumTopicThreadId(forumTopicModalThreadId ?? forumTopicDraft.messageThreadId);
    if (!threadId) {
      return;
    }

    const isGeneral = threadId === GENERAL_FORUM_TOPIC_THREAD_ID;
    const modalTopicName = buildForumTopicNameForIconMode(
      forumTopicDraft.name,
      forumTopicDraft.normalEmoji,
      forumTopicDraft.iconCustomEmojiId,
    );
    const updated = isGeneral
      ? await onEditGeneralForumTopicFromDraft(modalTopicName)
      : await onEditForumTopicFromDraft(threadId);
    if (updated) {
      setShowForumTopicModal(false);
    }
  };

  const onRunForumTopicContextAction = async (
    action: 'edit' | 'close' | 'reopen' | 'delete' | 'unpin' | 'hide' | 'unhide',
  ) => {
    const topic = forumTopicContextMenu?.topic;
    if (!topic) {
      return;
    }

    const canDeleteTopic = canManageForumTopics || canDeleteDirectMessagesTopicByActiveActor(topic);
    if (action === 'delete' && !canDeleteTopic) {
      setErrorText('Not enough rights to delete this topic.');
      setForumTopicContextMenu(null);
      return;
    }

    if (action !== 'delete' && !canManageForumTopics) {
      setForumTopicContextMenu(null);
      return;
    }

    if (action === 'edit') {
      openEditForumTopicModal(topic);
      return;
    }

    if (action === 'close') {
      if (topic.isGeneral) {
        await onCloseGeneralForumTopic();
      } else {
        await onCloseForumTopicFromDraft(topic.messageThreadId);
      }
      setForumTopicContextMenu(null);
      return;
    }

    if (action === 'reopen') {
      if (topic.isGeneral) {
        await onReopenGeneralForumTopic();
      } else {
        await onReopenForumTopicFromDraft(topic.messageThreadId);
      }
      setForumTopicContextMenu(null);
      return;
    }

    if (action === 'delete') {
      await onDeleteForumTopicFromDraft(topic.messageThreadId);
      setForumTopicContextMenu(null);
      return;
    }

    if (action === 'unpin') {
      if (topic.isGeneral) {
        await onUnpinAllGeneralForumTopicMessages();
      } else {
        await onUnpinAllForumTopicMessagesFromDraft(topic.messageThreadId);
      }
      setForumTopicContextMenu(null);
      return;
    }

    if (action === 'hide' && topic.isGeneral) {
      await onHideGeneralForumTopic();
      setForumTopicContextMenu(null);
      return;
    }

    if (action === 'unhide' && topic.isGeneral) {
      await onUnhideGeneralForumTopic();
      setForumTopicContextMenu(null);
    }
  };

  const onCloseGeneralForumTopic = async () => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }
    await runGroupAction(async () => {
      const ok = await closeGeneralForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID
            ? { ...topic, isClosed: true }
            : topic
        )));
      }
      renderInspector('closeGeneralForumTopic', { ok });
      setErrorText(ok ? 'General forum topic closed.' : 'closeGeneralForumTopic returned false.');
    });
  };

  const onReopenGeneralForumTopic = async () => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }
    await runGroupAction(async () => {
      const ok = await reopenGeneralForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID
            ? { ...topic, isClosed: false }
            : topic
        )));
      }
      renderInspector('reopenGeneralForumTopic', { ok });
      setErrorText(ok ? 'General forum topic reopened.' : 'reopenGeneralForumTopic returned false.');
    });
  };

  const onHideGeneralForumTopic = async () => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }
    await runGroupAction(async () => {
      const ok = await hideGeneralForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID
            ? { ...topic, isHidden: true }
            : topic
        )));
      }
      renderInspector('hideGeneralForumTopic', { ok });
      setErrorText(ok ? 'General forum topic hidden.' : 'hideGeneralForumTopic returned false.');
    });
  };

  const onUnhideGeneralForumTopic = async () => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }
    await runGroupAction(async () => {
      const ok = await unhideGeneralForumTopic(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      if (ok) {
        updateForumTopicsForChat(selectedGroup.id, (topics) => topics.map((topic) => (
          topic.messageThreadId === GENERAL_FORUM_TOPIC_THREAD_ID
            ? { ...topic, isHidden: false }
            : topic
        )));
      }
      renderInspector('unhideGeneralForumTopic', { ok });
      setErrorText(ok ? 'General forum topic unhidden.' : 'unhideGeneralForumTopic returned false.');
    });
  };

  const onUnpinAllGeneralForumTopicMessages = async () => {
    if (!selectedGroup || !canManageForumTopics) {
      return;
    }
    await runGroupAction(async () => {
      const ok = await unpinAllGeneralForumTopicMessages(selectedBotToken, {
        chat_id: selectedGroup.id,
      }, selectedUser.id);
      renderInspector('unpinAllGeneralForumTopicMessages', { ok });
      setErrorText(ok ? 'Unpinned all general-topic messages.' : 'unpinAllGeneralForumTopicMessages returned false.');
    });
  };

  const onJoinGroupByInviteLink = async () => {
    const inviteLink = groupInviteLinkInput.trim();
    if (!inviteLink) {
      setErrorText('Invite link is empty.');
      return;
    }

    setIsBootstrapping(true);
    setErrorText('');
    try {
      const result = await joinSimulationGroupByInviteLink(selectedBotToken, {
        invite_link: inviteLink,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
      });

      const existingChatType = groupChats.find((chat) => chat.id === result.chat_id)?.type;
      const joinedChatType = result.chat_type || existingChatType || 'supergroup';
      const joinedChatLabel = joinedChatType === 'channel' ? 'channel' : 'group';

      setChatScopeTab(joinedChatType === 'channel' ? 'channel' : 'group');
      setGroupChats((prev) => {
        if (prev.some((group) => group.id === result.chat_id)) {
          return prev;
        }
        return [
          ...prev,
          {
            id: result.chat_id,
            type: joinedChatType,
            title: `${joinedChatType === 'channel' ? 'Channel' : 'Group'} ${Math.abs(result.chat_id)}`,
            isDirectMessages: false,
            parentChannelChatId: undefined,
          },
        ].sort((a, b) => a.title.localeCompare(b.title));
      });
      setSelectedGroupChatId(result.chat_id);

      if (result.joined) {
        setGroupMembershipByUser((prev) => ({
          ...prev,
          [`${selectedBotToken}:${result.chat_id}:${selectedUser.id}`]: 'member',
        }));
        setErrorText(`Joined ${joinedChatLabel} by invite link.`);
      } else if (result.pending) {
        setErrorText(`${joinedChatType === 'channel' ? 'Channel' : 'Group'} join request sent. Waiting for owner/admin approval.`);
      } else {
        setErrorText(result.reason || 'Unable to join with invite link.');
      }
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to join by invite link');
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onDeleteSelectedGroup = async () => {
    if (!selectedGroup) {
      return;
    }
    if (!canDeleteSelectedGroup) {
      setErrorText(`Only ${selectedGroup.type === 'channel' ? 'channel' : 'group'} owner can delete this ${selectedGroup.type === 'channel' ? 'channel' : 'group'}.`);
      return;
    }

    setErrorText('');
    try {
      await deleteSimulationGroup(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: selectedUser.id,
        actor_first_name: selectedUser.first_name,
        actor_username: selectedUser.username,
      });

      const groupStateKey = `${selectedBotToken}:${selectedGroup.id}`;
      const membershipPrefix = `${groupStateKey}:`;
      const remainingGroups = groupChats
        .filter((group) => group.id !== selectedGroup.id)
        .sort((a, b) => a.title.localeCompare(b.title));
      const fallbackGroupId = remainingGroups[0]?.id ?? null;

      setGroupChats(remainingGroups);
      setSelectedGroupChatId((current) => (current === selectedGroup.id ? fallbackGroupId : current));
      setSelectedGroupByBot((prev) => {
        const next = { ...prev };
        if (fallbackGroupId === null) {
          delete next[selectedBotToken];
        } else {
          next[selectedBotToken] = fallbackGroupId;
        }
        return next;
      });
      setMessages((prev) => prev.filter((message) => !(message.botToken === selectedBotToken && message.chatId === selectedGroup.id)));
      setGroupMembershipByUser((prev) => {
        const next: Record<string, string> = {};
        Object.entries(prev).forEach(([key, value]) => {
          if (!key.startsWith(membershipPrefix)) {
            next[key] = value;
          }
        });
        return next;
      });
      setGroupInviteLinksByChat((prev) => {
        const next = { ...prev };
        delete next[groupStateKey];
        return next;
      });
      setPendingJoinRequestsByChat((prev) => {
        const next = { ...prev };
        delete next[groupStateKey];
        return next;
      });
      setPinnedMessageByChatKey((prev) => {
        const next = { ...prev };
        delete next[groupStateKey];
        return next;
      });
      setForumTopicsByChatKey((prev) => {
        const next = { ...prev };
        delete next[groupStateKey];
        return next;
      });
      setSelectedForumTopicByChatKey((prev) => {
        const next = { ...prev };
        delete next[groupStateKey];
        return next;
      });
      setInvoiceMetaByMessageKey((prev) => {
        const prefix = `${selectedBotToken}:${selectedGroup.id}:`;
        const next: Record<string, InvoiceMetaState> = {};
        Object.entries(prev).forEach(([key, value]) => {
          if (!key.startsWith(prefix)) {
            next[key] = value;
          }
        });
        return next;
      });
      setShowGroupActionsModal(false);
      setShowGroupProfileModal(false);
      setErrorText('Group deleted.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to delete group');
    }
  };

  const onApproveJoinRequest = async (request: PendingGroupJoinRequest) => {
    if (!selectedGroup) {
      return;
    }

    setIsBootstrapping(true);
    setErrorText('');
    try {
      const approved = await approveChatJoinRequest(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: request.userId,
      }, selectedUser.id);

      const key = `${selectedBotToken}:${selectedGroup.id}`;
      setPendingJoinRequestsByChat((prev) => ({
        ...prev,
        [key]: (prev[key] || []).filter((item) => item.userId !== request.userId),
      }));
      if (approved) {
        setGroupMembershipByUser((prev) => ({
          ...prev,
          [`${selectedBotToken}:${selectedGroup.id}:${request.userId}`]: 'member',
        }));
      }
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to approve join request');
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onDeclineJoinRequest = async (request: PendingGroupJoinRequest) => {
    if (!selectedGroup) {
      return;
    }

    setIsBootstrapping(true);
    setErrorText('');
    try {
      await declineChatJoinRequest(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: request.userId,
      }, selectedUser.id);

      const key = `${selectedBotToken}:${selectedGroup.id}`;
      setPendingJoinRequestsByChat((prev) => ({
        ...prev,
        [key]: (prev[key] || []).filter((item) => item.userId !== request.userId),
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to decline join request');
    } finally {
      setIsBootstrapping(false);
    }
  };

  const onSaveSelectedUserBusinessConnection = async () => {
    setIsBusinessActionRunning(true);
    setErrorText('');
    try {
      const normalizedBusinessConnectionId = businessConnectionDraftId.trim();
      if (!normalizedBusinessConnectionId) {
        const existingConnection = businessConnectionByUserKey[businessDraftStateKey];
        if (existingConnection) {
          await removeSimulationBusinessConnection(businessDraftBotToken, {
            user_id: selectedUser.id,
            business_connection_id: existingConnection.id,
          });

          setBusinessConnectionByUserKey((prev) => {
            const next = { ...prev };
            delete next[businessDraftStateKey];
            return next;
          });
          setBusinessConnectionDraftId('');
          setBusinessConnectionDraftEnabled(true);
          setBusinessRightsDraft(defaultBusinessRightsDraft());
          setErrorText(`Business connection ${existingConnection.id} cleared.`);
        } else {
          setErrorText('Business connection id is empty. Nothing to clear.');
        }
        return;
      }

      const connection = await setSimulationBusinessConnection(businessDraftBotToken, {
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        business_connection_id: normalizedBusinessConnectionId,
        enabled: businessConnectionDraftEnabled,
        rights: toBusinessBotRights(businessRightsDraft),
      });

      const stateKey = `${businessDraftBotToken}:${connection.user.id}`;
      setBusinessConnectionByUserKey((prev) => ({
        ...prev,
        [stateKey]: connection,
      }));
      setBusinessConnectionDraftId(connection.id);
      setBusinessConnectionDraftEnabled(connection.is_enabled);
      setBusinessRightsDraft(mapBusinessRightsToDraft(connection.rights));
      setErrorText(connection.is_enabled
        ? `Business connection ${connection.id} is active for ${connection.user.first_name}.`
        : `Business connection ${connection.id} is disabled.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to configure business connection');
    } finally {
      setIsBusinessActionRunning(false);
    }
  };

  const onRemoveSelectedUserBusinessConnection = async () => {
    const explicitConnectionId = businessConnectionDraftId.trim();
    const current = businessConnectionByUserKey[businessDraftStateKey];
    const targetConnectionId = explicitConnectionId || current?.id;
    if (!targetConnectionId) {
      setErrorText('No business connection to remove for this bot/user pair.');
      return;
    }

    setIsBusinessActionRunning(true);
    setErrorText('');
    try {
      await removeSimulationBusinessConnection(businessDraftBotToken, {
        user_id: selectedUser.id,
        business_connection_id: targetConnectionId,
      });

      setBusinessConnectionByUserKey((prev) => {
        const next: Record<string, GeneratedBusinessConnection> = {};
        Object.entries(prev).forEach(([key, value]) => {
          const isSameBot = key.startsWith(`${businessDraftBotToken}:`);
          const shouldDelete = isSameBot && value.id === targetConnectionId;
          if (!shouldDelete) {
            next[key] = value;
          }
        });
        return next;
      });
      setBusinessConnectionDraftId('');
      setBusinessConnectionDraftEnabled(true);
      setBusinessRightsDraft(defaultBusinessRightsDraft());
      setErrorText(`Business connection ${targetConnectionId} removed.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to remove business connection');
    } finally {
      setIsBusinessActionRunning(false);
    }
  };

  const onOpenGroupProfile = () => {
    if (!selectedGroup) {
      return;
    }
    if (!canEditSelectedGroup) {
      setErrorText(`Only owner/admin can edit ${selectedGroup.type === 'channel' ? 'channel' : 'group'} profile.`);
      setChatMenuOpen(false);
      return;
    }
    const currentSettings = selectedGroup.settings || defaultGroupSettings();
    setGroupProfileDraft({
      title: selectedGroup.title,
      username: selectedGroup.username || '',
      description: selectedGroup.description || '',
      isForum: Boolean(selectedGroup.isForum),
      showAuthorSignature: currentSettings.showAuthorSignature,
      directMessagesEnabled: currentSettings.directMessagesEnabled,
      directMessagesStarCount: currentSettings.directMessagesStarCount,
      messageHistoryVisible: currentSettings.messageHistoryVisible,
      slowModeDelay: currentSettings.slowModeDelay,
      allowSendMessages: currentSettings.allowSendMessages ?? true,
      allowSendMedia: currentSettings.allowSendMedia ?? true,
      allowSendAudios: currentSettings.allowSendAudios ?? true,
      allowSendDocuments: currentSettings.allowSendDocuments ?? true,
      allowSendPhotos: currentSettings.allowSendPhotos ?? true,
      allowSendVideos: currentSettings.allowSendVideos ?? true,
      allowSendVideoNotes: currentSettings.allowSendVideoNotes ?? true,
      allowSendVoiceNotes: currentSettings.allowSendVoiceNotes ?? true,
      allowSendOtherMessages: currentSettings.allowSendOtherMessages ?? true,
      allowAddWebPagePreviews: currentSettings.allowAddWebPagePreviews ?? true,
      allowPolls: currentSettings.allowPolls ?? true,
      allowInviteUsers: currentSettings.allowInviteUsers ?? true,
      allowPinMessages: currentSettings.allowPinMessages ?? false,
      allowChangeInfo: currentSettings.allowChangeInfo ?? false,
      allowManageTopics: currentSettings.allowManageTopics ?? false,
    });
    setGroupPhotoDraftFile(null);
    setRemoveGroupPhotoDraft(false);
    setGroupStickerSetDraft('');
    setRemoveGroupStickerSetDraft(false);
    setShowGroupProfileModal(true);
    setShowGroupActionsModal(false);
    setChatMenuOpen(false);
  };

  const onBackFromGroupProfile = () => {
    setShowGroupProfileModal(false);
    setGroupSettingsPage('home');
    setExpandedGroupMemberId(null);
    setShowGroupActionsModal(true);
  };

  const runGroupAction = async (action: () => Promise<void>) => {
    setIsGroupActionRunning(true);
    setErrorText('');
    try {
      await action();
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Group action failed');
    } finally {
      setIsGroupActionRunning(false);
    }
  };

  const onBanGroupMember = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    await runGroupAction(async () => {
      await banChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
      }, selectedUser.id);
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${targetUserId}`]: 'banned',
      }));
      setErrorText(`User ${targetUserId} banned.`);
    });
  };

  const onUnbanGroupMember = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    await runGroupAction(async () => {
      await unbanChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
      }, selectedUser.id);
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${targetUserId}`]: 'left',
      }));
      setErrorText(`User ${targetUserId} unbanned.`);
    });
  };

  const hydrateGroupMemberMeta = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    if (!selectedGroupStateKey) {
      return;
    }
    if (groupMemberMetaByChatKey[selectedGroupStateKey]?.[targetUserId]) {
      return;
    }

    try {
      const member = await getChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
      }, selectedUser.id);
      applyGroupMemberMetaState(targetUserId, member);
    } catch {
      // Keep UI responsive even if metadata fetch fails.
    }
  };

  const onToggleGroupMemberExpanded = (targetUserId: number) => {
    if (!selectedGroupStateKey) {
      return;
    }
    setExpandedGroupMemberId((current) => {
      const next = current === targetUserId ? null : targetUserId;
      if (next === targetUserId) {
        void hydrateGroupMemberMeta(targetUserId);
      }
      return next;
    });
    setGroupMemberRestrictionDraftByChatKey((prev) => {
      const current = prev[selectedGroupStateKey] || {};
      if (current[targetUserId]) {
        return prev;
      }
      return {
        ...prev,
        [selectedGroupStateKey]: {
          ...current,
          [targetUserId]: defaultGroupMemberRestrictionDraft(),
        },
      };
    });
  };

  const onUpdateGroupMemberRestrictionDraft = (
    targetUserId: number,
    patch: Partial<GroupMemberRestrictionDraft>,
  ) => {
    if (!selectedGroupStateKey) {
      return;
    }
    setGroupMemberRestrictionDraftByChatKey((prev) => {
      const current = prev[selectedGroupStateKey] || {};
      return {
        ...prev,
        [selectedGroupStateKey]: {
          ...current,
          [targetUserId]: {
            ...(current[targetUserId] || defaultGroupMemberRestrictionDraft()),
            ...patch,
          },
        },
      };
    });
  };

  const onApplyGroupMemberRestriction = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    const draft = selectedGroupStateKey
      ? (groupMemberRestrictionDraftByChatKey[selectedGroupStateKey]?.[targetUserId] || defaultGroupMemberRestrictionDraft())
      : defaultGroupMemberRestrictionDraft();
    const untilHours = Math.max(1, Math.floor(Number(draft.untilHours) || 1));
    const untilDate = Math.floor(Date.now() / 1000) + (untilHours * 60 * 60);

    await runGroupAction(async () => {
      await restrictChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
        permissions: mapRestrictionDraftToPermissions(draft),
        until_date: untilDate,
      }, selectedUser.id);

      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${targetUserId}`]: 'restricted',
      }));
      setErrorText(`Restrictions applied to user ${targetUserId}.`);
    });
  };

  const onLiftGroupMemberRestriction = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      await restrictChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
        permissions: fullMemberPermissions(),
        until_date: undefined,
      }, selectedUser.id);

      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${targetUserId}`]: 'member',
      }));
      setErrorText(`Restrictions lifted for user ${targetUserId}.`);
    });
  };

  const onPromoteGroupMember = async (targetUserId: number, promote: boolean) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    await runGroupAction(async () => {
      if (selectedGroup.type === 'channel') {
        const rightsDraft = selectedGroupStateKey
          ? (channelAdminRightsDraftByChatKey[selectedGroupStateKey]?.[targetUserId] || defaultChannelAdminRightsDraft())
          : defaultChannelAdminRightsDraft();
        await promoteChatMember(selectedBotToken, {
          chat_id: selectedGroup.id,
          user_id: targetUserId,
          can_manage_chat: promote && rightsDraft.canManageChat,
          can_post_messages: promote && rightsDraft.canPostMessages,
          can_edit_messages: promote && rightsDraft.canEditMessages,
          can_delete_messages: promote && rightsDraft.canDeleteMessages,
          can_invite_users: promote && rightsDraft.canInviteUsers,
          can_change_info: promote && rightsDraft.canChangeInfo,
          can_manage_direct_messages: promote && rightsDraft.canManageDirectMessages,
          can_pin_messages: promote,
        }, selectedUser.id);
      } else {
        await promoteChatMember(selectedBotToken, {
          chat_id: selectedGroup.id,
          user_id: targetUserId,
          can_manage_chat: promote,
          can_delete_messages: promote,
          can_manage_video_chats: promote,
          can_restrict_members: promote,
          can_promote_members: false,
          can_change_info: promote,
          can_invite_users: promote,
          can_post_stories: promote,
          can_edit_stories: promote,
          can_delete_stories: promote,
          can_pin_messages: promote,
          can_manage_topics: promote,
        }, selectedUser.id);
      }
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${targetUserId}`]: promote ? 'admin' : 'member',
      }));
      setErrorText(promote
        ? `User ${targetUserId} promoted to admin.`
        : `User ${targetUserId} demoted to member.`);
    });
  };

  const onUpdateChannelAdminRightsDraft = (targetUserId: number, patch: Partial<ChannelAdminRightsDraft>) => {
    if (!selectedGroupStateKey) {
      return;
    }
    setChannelAdminRightsDraftByChatKey((prev) => {
      const current = prev[selectedGroupStateKey] || {};
      return {
        ...prev,
        [selectedGroupStateKey]: {
          ...current,
          [targetUserId]: {
            ...(current[targetUserId] || defaultChannelAdminRightsDraft()),
            ...patch,
          },
        },
      };
    });
  };

  const onApplyChannelAdminRights = async (targetUserId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }

    const rightsDraft = selectedGroupStateKey
      ? (channelAdminRightsDraftByChatKey[selectedGroupStateKey]?.[targetUserId] || defaultChannelAdminRightsDraft())
      : defaultChannelAdminRightsDraft();

    await runGroupAction(async () => {
      await promoteChatMember(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
        can_manage_chat: rightsDraft.canManageChat,
        can_post_messages: rightsDraft.canPostMessages,
        can_edit_messages: rightsDraft.canEditMessages,
        can_delete_messages: rightsDraft.canDeleteMessages,
        can_invite_users: rightsDraft.canInviteUsers,
        can_change_info: rightsDraft.canChangeInfo,
        can_manage_direct_messages: rightsDraft.canManageDirectMessages,
        can_pin_messages: true,
      }, selectedUser.id);
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${targetUserId}`]: 'admin',
      }));
      setErrorText(`Channel admin rights updated for user ${targetUserId}.`);
    });
  };

  const onSubmitForwardMessage = async () => {
    if (!selectedBotToken || !selectedChatId || !forwardMessageId) {
      return;
    }

    const sourceMessage = messages.find((message) => (
      message.id === forwardMessageId
      && message.botToken === selectedBotToken
      && message.chatId === selectedChatId
    ));
    if (sourceMessage?.service) {
      setErrorText('Service messages cannot be forwarded.');
      setForwardMessageId(null);
      setForwardTargetChatId('');
      return;
    }

    const normalizedTargetInput = forwardTargetChatId.trim();
    const numericTargetChatId = Math.floor(Number(normalizedTargetInput));
    const matchedTarget = !Number.isFinite(numericTargetChatId) || numericTargetChatId === 0
      ? forwardTargetDirectory.find((target) => {
        const byTitle = target.title.toLowerCase() === normalizedTargetInput.toLowerCase();
        const byUsername = target.username
          ? target.username.toLowerCase() === normalizedTargetInput.replace(/^@/, '').toLowerCase()
          : false;
        return byTitle || byUsername;
      })
      : undefined;
    const singleFilteredTarget = (!Number.isFinite(numericTargetChatId) || numericTargetChatId === 0)
      && !matchedTarget
      && filteredForwardTargets.length === 1
      ? filteredForwardTargets[0]
      : undefined;
    const targetChatId = Number.isFinite(numericTargetChatId) && numericTargetChatId !== 0
      ? numericTargetChatId
      : (matchedTarget?.chatId || singleFilteredTarget?.chatId || 0);

    if (!Number.isFinite(targetChatId) || targetChatId === 0) {
      setErrorText('Forward target chat id is invalid.');
      return;
    }

    setIsSending(true);
    try {
      await forwardMessage(selectedBotToken, {
        chat_id: targetChatId,
        from_chat_id: selectedChatId,
        message_id: forwardMessageId,
      }, selectedUser.id);
      setErrorText(`Forwarded message ${forwardMessageId} to chat ${targetChatId}.`);
      setForwardMessageId(null);
      setForwardTargetChatId('');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to forward message.');
    } finally {
      setIsSending(false);
    }
  };

  const onSetGroupAdminTitle = async (targetUserId: number, customTitle: string) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    if (selectedGroup.type === 'channel') {
      setErrorText('Channel members do not use admin/member tags. Use admin rights controls instead.');
      return;
    }
    const normalizedTitle = customTitle.trim();
    if (!normalizedTitle) {
      setErrorText('Admin custom title is empty.');
      return;
    }
    await runGroupAction(async () => {
      await setChatAdministratorCustomTitle(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
        custom_title: normalizedTitle,
      }, selectedUser.id);
      if (selectedGroupStateKey) {
        setGroupMemberMetaByChatKey((prev) => ({
          ...prev,
          [selectedGroupStateKey]: {
            ...(prev[selectedGroupStateKey] || {}),
            [targetUserId]: {
              ...(prev[selectedGroupStateKey]?.[targetUserId] || {}),
              customTitle: normalizedTitle,
            },
          },
        }));
      }
      setErrorText(`Admin title set for user ${targetUserId}.`);
    });
  };

  const onSetGroupMemberTag = async (targetUserId: number, tag?: string) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    await runGroupAction(async () => {
      const normalizedTag = tag?.trim() || undefined;
      await setChatMemberTag(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: targetUserId,
        tag: normalizedTag,
      }, selectedUser.id);
      if (selectedGroupStateKey) {
        setGroupMemberMetaByChatKey((prev) => ({
          ...prev,
          [selectedGroupStateKey]: {
            ...(prev[selectedGroupStateKey] || {}),
            [targetUserId]: {
              ...(prev[selectedGroupStateKey]?.[targetUserId] || {}),
              tag: normalizedTag,
            },
          },
        }));
      }
      setErrorText(`Tag updated for user ${targetUserId}.`);
    });
  };

  const onBanSenderChat = async (ban: boolean, senderChatId: number) => {
    if (!selectedGroup || !canEditSelectedGroup) {
      return;
    }
    if (!Number.isFinite(senderChatId) || senderChatId <= 0) {
      setErrorText('Sender chat id is invalid.');
      return;
    }
    await runGroupAction(async () => {
      if (ban) {
        await banChatSenderChat(selectedBotToken, {
          chat_id: selectedGroup.id,
          sender_chat_id: senderChatId,
        }, selectedUser.id);
      } else {
        await unbanChatSenderChat(selectedBotToken, {
          chat_id: selectedGroup.id,
          sender_chat_id: senderChatId,
        }, selectedUser.id);
      }
      setErrorText(ban
        ? `Sender chat ${senderChatId} banned.`
        : `Sender chat ${senderChatId} unbanned.`);
    });
  };

  const onPinMessageById = async (messageId: number) => {
    if (!selectedGroup || !selectedGroupStateKey || !canPinInSelectedGroup) {
      return;
    }
    await runGroupAction(async () => {
      await pinChatMessage(selectedBotToken, {
        chat_id: selectedGroup.id,
        message_id: messageId,
        disable_notification: true,
      }, selectedUser.id);
      setPinnedMessageByChatKey((prev) => {
        const current = prev[selectedGroupStateKey] || [];
        const next = [...current.filter((id) => id !== messageId), messageId];
        return {
          ...prev,
          [selectedGroupStateKey]: next,
        };
      });
      setErrorText(`Pinned message ${messageId}.`);
    });
  };

  const onUnpinMessageById = async (messageId?: number) => {
    if (!selectedGroup || !selectedGroupStateKey || !canPinInSelectedGroup) {
      return;
    }

    await runGroupAction(async () => {
      if (messageId) {
        await unpinChatMessage(selectedBotToken, {
          chat_id: selectedGroup.id,
          message_id: messageId,
        }, selectedUser.id);
      } else {
        await unpinAllChatMessages(selectedBotToken, {
          chat_id: selectedGroup.id,
        }, selectedUser.id);
      }

      setPinnedMessageByChatKey((prev) => {
        if (!messageId) {
          const next = { ...prev };
          delete next[selectedGroupStateKey];
          return next;
        }

        const current = prev[selectedGroupStateKey] || [];
        const nextList = current.filter((id) => id !== messageId);
        if (nextList.length === 0) {
          const next = { ...prev };
          delete next[selectedGroupStateKey];
          return next;
        }
        return {
          ...prev,
          [selectedGroupStateKey]: nextList,
        };
      });
      setErrorText(messageId ? 'Pinned message removed.' : 'All pinned messages cleared.');
    });
  };

  const onBotLeaveByApi = async () => {
    if (!selectedGroup || !selectedBot || !canEditSelectedGroup) {
      return;
    }
    await runGroupAction(async () => {
      await leaveChat(selectedBotToken, {
        chat_id: selectedGroup.id,
      });
      setGroupMembershipByUser((prev) => ({
        ...prev,
        [`${selectedBotToken}:${selectedGroup.id}:${selectedBot.id}`]: 'left',
      }));
      setErrorText('Bot left the group via leaveChat.');
    });
  };

  const onSaveGroupProfile = async () => {
    if (!selectedGroup) {
      return;
    }
    if (!canEditSelectedGroup) {
      setErrorText('Only owner/admin can edit group profile.');
      return;
    }

    setIsGroupActionRunning(true);
    setErrorText('');
    try {
      const normalizedSlowModeDelay = Math.max(0, Math.floor(Number(groupProfileDraft.slowModeDelay) || 0));
      const draftSettings: GroupSettingsSnapshot = {
        showAuthorSignature: groupProfileDraft.showAuthorSignature,
        directMessagesEnabled: groupProfileDraft.directMessagesEnabled,
        directMessagesStarCount: Math.max(0, Math.floor(Number(groupProfileDraft.directMessagesStarCount) || 0)),
        messageHistoryVisible: groupProfileDraft.messageHistoryVisible,
        slowModeDelay: normalizedSlowModeDelay,
        allowSendMessages: groupProfileDraft.allowSendMessages,
        allowSendMedia: groupProfileDraft.allowSendMedia,
        allowSendAudios: groupProfileDraft.allowSendAudios,
        allowSendDocuments: groupProfileDraft.allowSendDocuments,
        allowSendPhotos: groupProfileDraft.allowSendPhotos,
        allowSendVideos: groupProfileDraft.allowSendVideos,
        allowSendVideoNotes: groupProfileDraft.allowSendVideoNotes,
        allowSendVoiceNotes: groupProfileDraft.allowSendVoiceNotes,
        allowSendOtherMessages: groupProfileDraft.allowSendOtherMessages,
        allowAddWebPagePreviews: groupProfileDraft.allowAddWebPagePreviews,
        allowPolls: groupProfileDraft.allowPolls,
        allowInviteUsers: groupProfileDraft.allowInviteUsers,
        allowPinMessages: groupProfileDraft.allowPinMessages,
        allowChangeInfo: groupProfileDraft.allowChangeInfo,
        allowManageTopics: groupProfileDraft.allowManageTopics,
      };

      await setChatTitle(selectedBotToken, {
        chat_id: selectedGroup.id,
        title: groupProfileDraft.title.trim(),
      }, selectedUser.id);

      await setChatDescription(selectedBotToken, {
        chat_id: selectedGroup.id,
        description: groupProfileDraft.description.trim() || undefined,
      }, selectedUser.id);

      if (selectedGroup.type !== 'channel') {
        await setChatPermissions(selectedBotToken, {
          chat_id: selectedGroup.id,
          permissions: mapSnapshotToServerPermissions(draftSettings),
        }, selectedUser.id);
      }

      if (groupPhotoDraftFile) {
        await setChatPhoto(selectedBotToken, {
          chat_id: selectedGroup.id,
          photo: groupPhotoDraftFile,
        }, selectedUser.id);
      } else if (removeGroupPhotoDraft) {
        await deleteChatPhoto(selectedBotToken, {
          chat_id: selectedGroup.id,
        }, selectedUser.id);
      }

      const stickerSetName = groupStickerSetDraft.trim();
      if (selectedGroup.type === 'supergroup') {
        if (stickerSetName) {
          await setChatStickerSet(selectedBotToken, {
            chat_id: selectedGroup.id,
            sticker_set_name: stickerSetName,
          }, selectedUser.id);
        } else if (removeGroupStickerSetDraft) {
          await deleteChatStickerSet(selectedBotToken, {
            chat_id: selectedGroup.id,
          }, selectedUser.id);
        }
      }

      const result = await updateSimulationGroup(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: selectedUser.id,
        actor_first_name: selectedUser.first_name,
        actor_username: selectedUser.username,
        username: groupProfileDraft.username.trim() || undefined,
        is_forum: selectedGroup.type === 'supergroup' ? groupProfileDraft.isForum : undefined,
        show_author_signature: selectedGroup.type === 'channel' ? groupProfileDraft.showAuthorSignature : undefined,
        direct_messages_enabled: selectedGroup.type === 'channel' ? groupProfileDraft.directMessagesEnabled : undefined,
        direct_messages_star_count: selectedGroup.type === 'channel'
          ? Math.max(0, Math.floor(Number(groupProfileDraft.directMessagesStarCount) || 0))
          : undefined,
        message_history_visible: selectedGroup.type === 'channel' ? undefined : draftSettings.messageHistoryVisible,
        slow_mode_delay: selectedGroup.type === 'channel' ? undefined : draftSettings.slowModeDelay,
      });

      const updatedSettings = result.settings
        ? mapServerSettingsToSnapshot(result.settings)
        : draftSettings;
      const updatedDescription = result.settings?.description
        ?? (groupProfileDraft.description.trim() || undefined);
      const rawUpdatedLinkedDiscussionId = Math.floor(Number(result.settings?.linked_chat_id));
      const updatedLinkedDiscussionId = Number.isFinite(rawUpdatedLinkedDiscussionId) && rawUpdatedLinkedDiscussionId !== 0
        ? rawUpdatedLinkedDiscussionId
        : undefined;

      setGroupChats((prev) => {
        const updated = prev.map((group) => (
          group.id === selectedGroup.id
            ? {
              ...group,
              title: result.chat.title || group.title,
              username: result.chat.username || undefined,
              description: updatedDescription,
              isForum: result.chat.is_forum || false,
              linkedDiscussionChatId: selectedGroup.type === 'channel'
                ? (updatedLinkedDiscussionId ?? group.linkedDiscussionChatId)
                : group.linkedDiscussionChatId,
              settings: updatedSettings,
            }
            : group
        ));

        if (selectedGroup.type === 'channel' && !updatedSettings.directMessagesEnabled) {
          return updated.filter((group) => !(group.isDirectMessages && group.parentChannelChatId === selectedGroup.id));
        }

        return updated;
      });
      setShowGroupProfileModal(false);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to update group profile');
    } finally {
      setIsGroupActionRunning(false);
    }
  };

  const onApplyChannelDiscussionLink = async () => {
    if (!selectedGroup || selectedGroup.type !== 'channel') {
      return;
    }
    if (!canEditSelectedGroup) {
      setErrorText('Only owner/admin can configure linked discussion group.');
      return;
    }

    const rawLinkValue = channelDiscussionLinkDraft.trim();
    const parsedLinkedChatId = Math.floor(Number(rawLinkValue));
    const hasLink = rawLinkValue.length > 0;
    if (hasLink && (!Number.isFinite(parsedLinkedChatId) || parsedLinkedChatId === 0)) {
      setErrorText('Linked discussion chat id is invalid.');
      return;
    }
    const previousLinkedDiscussionChatId = activeChannelLinkedDiscussionChatId;

    setIsGroupActionRunning(true);
    setErrorText('');
    try {
      const result = await updateSimulationGroup(selectedBotToken, {
        chat_id: selectedGroup.id,
        user_id: selectedUser.id,
        actor_first_name: selectedUser.first_name,
        actor_username: selectedUser.username,
        linked_chat_id: hasLink ? parsedLinkedChatId : 0,
      });

      const rawLinkedChatId = Math.floor(Number(result.settings?.linked_chat_id));
      const linkedChatId = Number.isFinite(rawLinkedChatId) && rawLinkedChatId !== 0
        ? rawLinkedChatId
        : undefined;

      setGroupChats((prev) => prev.map((group) => {
        if (group.id === selectedGroup.id) {
          return { ...group, linkedDiscussionChatId: linkedChatId };
        }

        const isDiscussionGroup = group.type === 'group' || group.type === 'supergroup';
        if (!isDiscussionGroup) {
          return group;
        }

        if (linkedChatId && group.id === linkedChatId) {
          return { ...group, linkedDiscussionChatId: selectedGroup.id };
        }

        if (
          previousLinkedDiscussionChatId
          && group.id === previousLinkedDiscussionChatId
          && linkedChatId !== previousLinkedDiscussionChatId
          && group.linkedDiscussionChatId === selectedGroup.id
        ) {
          return { ...group, linkedDiscussionChatId: undefined };
        }

        return group;
      }));

      setChannelDiscussionLinkDraft(linkedChatId ? String(linkedChatId) : '');
      setErrorText(linkedChatId
        ? `Linked discussion group set to ${linkedChatId}.`
        : 'Linked discussion group removed.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to update linked discussion group');
    } finally {
      setIsGroupActionRunning(false);
    }
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
    setBusinessDraftBotToken(selectedBotToken);
    setBusinessConnectionDraftId('');
    setBusinessConnectionDraftEnabled(true);
    setBusinessRightsDraft(defaultBusinessRightsDraft());
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
    const businessStateKey = `${selectedBotToken}:${user.id}`;
    const connection = businessConnectionByUserKey[businessStateKey];
    setUserModalMode('edit');
    setUserDraft({
      first_name: user.first_name,
      username: user.username || '',
      id: String(user.id),
    });
    setSelectedUserId(user.id);
    setBusinessDraftBotToken(selectedBotToken);
    setBusinessConnectionDraftId(connection?.id || '');
    setBusinessConnectionDraftEnabled(connection?.is_enabled ?? true);
    setBusinessRightsDraft(mapBusinessRightsToDraft(connection?.rights));
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

  const canEditMessageByActiveActor = (message: ChatMessage) => {
    if (!selectedBot) {
      return false;
    }
    if (message.isInlineOrigin || message.viaBotUsername || message.service) {
      return false;
    }

    const ownerIsActiveUser = message.fromUserId === selectedUser.id;
    return ownerIsActiveUser;
  };

  const onEditMessage = (message: ChatMessage) => {
    if (!canEditMessageByActiveActor(message)) {
      setMessageMenu(null);
      return;
    }

    const sourceEntities =
      (message.media ? (message.captionEntities || message.entities) : (message.entities || message.captionEntities)) || [];
    const preferredParseMode: ComposerParseMode = message.parseMode || composerParseMode;
    const rebuilt = rebuildFormattedMessageForEditing(
      message.text,
      sourceEntities,
      preferredParseMode,
      Boolean(message.parseMode),
    );

    setComposerEditTarget(message);
    setReplyTarget(null);
    setComposerText(rebuilt.text);
    setComposerParseMode(message.parseMode || rebuilt.parseMode);
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

  const dismissActiveOneTimeKeyboard = () => {
    if (!activeReplyKeyboard || activeReplyKeyboard.markup.kind !== 'reply') {
      return;
    }

    if (!activeReplyKeyboard.markup.one_time_keyboard) {
      return;
    }

    setDismissedOneTimeKeyboards((prev) => ({
      ...prev,
      [chatKey]: activeReplyKeyboard.sourceMessageId,
    }));
  };

  const sendStructuredReplyKeyboardMessage = async (
    outgoingText: string,
    options: {
      usersSharedPayload?: GeneratedUsersShared;
      chatSharedPayload?: GeneratedChatShared;
      webAppDataPayload?: GeneratedWebAppData;
    },
  ): Promise<boolean> => {
    if (!ensureActiveForumTopicWritable()) {
      return false;
    }

    setIsSending(true);
    setErrorText('');
    try {
      await sendUserMessage(selectedBotToken, {
        chat_id: selectedChatId,
        message_thread_id: outboundMessageThreadId,
        direct_messages_topic_id: activeDirectMessagesTopicId,
        business_connection_id: activeBusinessConnectionId,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        sender_chat_id: activeDiscussionSenderChatId,
        text: outgoingText,
        parse_mode: composerParseMode === 'none' ? undefined : composerParseMode,
        reply_to_message_id: resolveComposerReplyTargetId(replyTarget?.id),
        users_shared: options.usersSharedPayload,
        chat_shared: options.chatSharedPayload,
        web_app_data: options.webAppDataPayload,
      });

      setReplyTarget(null);
      dismissActiveOneTimeKeyboard();
      isNearBottomRef.current = true;
      window.setTimeout(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
      }, 0);

      return true;
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to send keyboard request payload');
      return false;
    } finally {
      setIsSending(false);
    }
  };

  const onSubmitRequestUsersModal = async () => {
    if (!keyboardRequestUsersModal) {
      return;
    }

    const selectedCandidates = keyboardRequestUsersModal.candidates.filter((candidate) => (
      keyboardRequestUsersModal.selectedUserIds.includes(candidate.userId)
    ));
    if (selectedCandidates.length === 0) {
      setErrorText('Select at least one user.');
      return;
    }

    const usersSharedPayload: GeneratedUsersShared = {
      request_id: keyboardRequestUsersModal.request.request_id,
      users: selectedCandidates.map((candidate) => ({
        user_id: candidate.userId,
        first_name: keyboardRequestUsersModal.request.request_name ? candidate.firstName : undefined,
        username: keyboardRequestUsersModal.request.request_username ? (candidate.username || undefined) : undefined,
      })),
    };

    const sharedUsersText = selectedCandidates
      .map((candidate) => {
        const identity = candidate.username ? `@${candidate.username}` : `id ${candidate.userId}`;
        return `- ${candidate.firstName}${candidate.isBot ? ' (bot)' : ''} (${identity})`;
      })
      .join('\n');

    const sent = await sendStructuredReplyKeyboardMessage(
      selectedCandidates.length === 1
        ? `👤 Shared user:\n${sharedUsersText}`
        : `👥 Shared users (${selectedCandidates.length}):\n${sharedUsersText}`,
      { usersSharedPayload },
    );
    if (sent) {
      setKeyboardRequestUsersModal(null);
    }
  };

  const onSubmitRequestChatModal = async () => {
    if (!keyboardRequestChatModal) {
      return;
    }

    const picked = keyboardRequestChatModal.candidates.find((chat) => chat.id === keyboardRequestChatModal.selectedChatId);
    if (!picked) {
      setErrorText('Select a chat first.');
      return;
    }

    const chatSharedPayload: GeneratedChatShared = {
      request_id: keyboardRequestChatModal.request.request_id,
      chat_id: picked.id,
      title: keyboardRequestChatModal.request.request_title ? picked.title : undefined,
      username: keyboardRequestChatModal.request.request_username ? (picked.username || undefined) : undefined,
    };

    const sharedChatIdentity = picked.username ? `@${picked.username}` : `id ${picked.id}`;

    const sent = await sendStructuredReplyKeyboardMessage(
      `💬 Shared ${picked.type === 'channel' ? 'channel' : 'chat'}:\n- ${picked.title} (${sharedChatIdentity}, ${picked.type})`,
      { chatSharedPayload },
    );
    if (sent) {
      setKeyboardRequestChatModal(null);
    }
  };

  const onReplyKeyboardButtonPress = async (button: ReplyKeyboardButton) => {
    const text = button.text.trim();
    if (!text || isSending) {
      return;
    }

    if (!ensureActiveForumTopicWritable()) {
      return;
    }

    let outgoingText = text;
    let usersSharedPayload: GeneratedUsersShared | undefined;
    let chatSharedPayload: GeneratedChatShared | undefined;
    let webAppDataPayload: GeneratedWebAppData | undefined;

    const legacyRequestUser = (() => {
      const rawLegacy = (button as unknown as Record<string, unknown>).request_user;
      if (!rawLegacy || typeof rawLegacy !== 'object') {
        return undefined;
      }

      const legacy = rawLegacy as Record<string, unknown>;
      const requestId = Number(legacy.request_id);
      if (!Number.isFinite(requestId)) {
        return undefined;
      }

      const toOptionalBool = (value: unknown): boolean | undefined => (
        typeof value === 'boolean' ? value : undefined
      );

      return {
        request_id: Math.trunc(requestId),
        user_is_bot: toOptionalBool(legacy.user_is_bot),
        user_is_premium: toOptionalBool(legacy.user_is_premium),
        max_quantity: 10,
        request_name: toOptionalBool(legacy.request_name),
        request_username: toOptionalBool(legacy.request_username),
        request_photo: toOptionalBool(legacy.request_photo),
      } satisfies NonNullable<ReplyKeyboardButton['request_users']>;
    })();

    const requestUsersButton = button.request_users ?? legacyRequestUser;

    if (button.request_contact) {
      setShowMediaDrawer(true);
      setMediaDrawerTab('contact');
      setShowFormattingTools(false);
      setMessageMenu(null);
      setShareDraft((prev) => ({
        ...prev,
        contactFirstName: prev.contactFirstName || selectedUser.first_name,
      }));
      composerTextareaRef.current?.focus();
      return;
    } else if (button.request_location) {
      setShowMediaDrawer(true);
      setMediaDrawerTab('location');
      setShowFormattingTools(false);
      setMessageMenu(null);
      composerTextareaRef.current?.focus();
      return;
    } else if (button.request_poll) {
      const isQuiz = button.request_poll.type === 'quiz';
      setShowMediaDrawer(true);
      setMediaDrawerTab('poll');
      setShowFormattingTools(false);
      setPollBuilder({
        type: isQuiz ? 'quiz' : 'regular',
        question: isQuiz ? `${selectedUser.first_name}'s Quiz` : `${selectedUser.first_name}'s Poll`,
        options: isQuiz ? ['Correct option', 'Wrong option'] : ['Yes', 'No'],
        optionsParseMode: 'none',
        isAnonymous: false,
        allowsRevoting: !isQuiz,
        allowsMultipleAnswers: false,
        correctOptionIds: [0],
        explanation: isQuiz ? 'Choose the correct answer.' : '',
        questionParseMode: 'none',
        explanationParseMode: 'none',
        description: '',
        descriptionParseMode: 'none',
        openPeriod: '',
        closeDate: '',
        isClosed: false,
      });
      setMessageMenu(null);
      composerTextareaRef.current?.focus();
      return;
    } else if (requestUsersButton) {
      const request = requestUsersButton;
      const humanCandidates = availableUsers.map((user) => ({
        userId: user.id,
        firstName: user.first_name,
        username: user.username,
        isBot: false,
      }));
      const botCandidates = selectedBot ? [{
        userId: selectedBot.id,
        firstName: selectedBot.first_name,
        username: selectedBot.username,
        isBot: true,
      }] : [];

      let candidates = [...humanCandidates, ...botCandidates];
      if (request.user_is_bot === true) {
        candidates = candidates.filter((candidate) => candidate.isBot);
      } else if (request.user_is_bot === false) {
        candidates = candidates.filter((candidate) => !candidate.isBot);
      }
      if (request.user_is_premium === true) {
        candidates = [];
      }

      if (candidates.length === 0) {
        setErrorText('No suitable user found for request_users.');
        return;
      }

      const maxQuantity = resolveRequestUsersMaxQuantity(request, Math.min(10, candidates.length));

      setKeyboardRequestUsersModal({
        buttonText: text,
        request,
        candidates,
        selectedUserIds: candidates.slice(0, Math.min(1, maxQuantity)).map((candidate) => candidate.userId),
      });
      setMessageMenu(null);
      return;
    } else if (button.request_chat) {
      const request = button.request_chat;
      let candidates = groupChats.filter((chat) => (
        request.chat_is_channel ? chat.type === 'channel' : chat.type !== 'channel'
      ));

      if (request.chat_has_username) {
        candidates = candidates.filter((chat) => Boolean(chat.username));
      }
      if (request.chat_is_forum) {
        candidates = candidates.filter((chat) => Boolean(chat.isForum));
      }
      if (request.chat_is_created) {
        candidates = candidates.filter((chat) => {
          const statusKey = `${selectedBotToken}:${chat.id}:${selectedUser.id}`;
          return normalizeMembershipStatus(groupMembershipByUser[statusKey]) === 'owner';
        });
      }
      if (request.bot_is_member && selectedBot) {
        candidates = candidates.filter((chat) => {
          const botStatusKey = `${selectedBotToken}:${chat.id}:${selectedBot.id}`;
          return isJoinedMembershipStatus(groupMembershipByUser[botStatusKey]);
        });
      }

      if (request.user_administrator_rights) {
        candidates = candidates.filter((chat) => {
          const actorStatusKey = `${selectedBotToken}:${chat.id}:${selectedUser.id}`;
          const actorStatus = normalizeMembershipStatus(groupMembershipByUser[actorStatusKey]);
          return actorStatus === 'owner' || actorStatus === 'admin';
        });
      }
      if (request.bot_administrator_rights) {
        if (!selectedBot) {
          candidates = [];
        } else {
          candidates = candidates.filter((chat) => {
            const botStatusKey = `${selectedBotToken}:${chat.id}:${selectedBot.id}`;
            const botStatus = normalizeMembershipStatus(groupMembershipByUser[botStatusKey]);
            return botStatus === 'owner' || botStatus === 'admin';
          });
        }
      }

      if (candidates.length === 0) {
        setErrorText('No suitable chat found for request_chat.');
        return;
      }

      setKeyboardRequestChatModal({
        buttonText: text,
        request,
        candidates,
        selectedChatId: candidates[0]?.id ?? null,
      });
      setMessageMenu(null);
      return;
    }

    if (button.web_app?.url) {
      window.open(button.web_app.url, '_blank', 'noopener,noreferrer');
      webAppDataPayload = {
        button_text: text,
        data: JSON.stringify({
          source: 'reply_keyboard_web_app',
          url: button.web_app.url,
          actor_user_id: selectedUser.id,
          actor_username: selectedUser.username || null,
          timestamp: Math.floor(Date.now() / 1000),
        }),
      };
    }

    const hasStructuredPayload = Boolean(usersSharedPayload || chatSharedPayload || webAppDataPayload);
    if (hasStructuredPayload) {
      await sendStructuredReplyKeyboardMessage(outgoingText, {
        usersSharedPayload,
        chatSharedPayload,
        webAppDataPayload,
      });
    } else {
      await sendAsUser(
        outgoingText,
        composerParseMode === 'none' ? undefined : composerParseMode,
        resolveComposerReplyTargetId(replyTarget?.id),
      );
      setReplyTarget(null);
      dismissActiveOneTimeKeyboard();
      isNearBottomRef.current = true;
      window.setTimeout(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
      }, 0);
    }
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

  const openLinkedChannelPost = (channelChatId: number, channelMessageId?: number) => {
    if (!Number.isFinite(channelChatId) || channelChatId === 0) {
      return;
    }

    setChatScopeTab('channel');
    setSelectedGroupChatId(channelChatId);

    if (channelMessageId && channelMessageId > 0) {
      window.setTimeout(() => {
        scrollToMessage(channelMessageId);
      }, 150);
    }
  };

  const clearActiveDiscussionCommentContext = () => {
    setCommentSourceByDiscussionChatKey((prev) => {
      if (!Object.prototype.hasOwnProperty.call(prev, chatKey)) {
        return prev;
      }
      const next = { ...prev };
      delete next[chatKey];
      return next;
    });

    if (
      activeDiscussionCommentContext?.discussionRootMessageId
      && replyTarget?.id === activeDiscussionCommentContext.discussionRootMessageId
    ) {
      setReplyTarget(null);
    }
  };

  const closeDiscussionThreadAndReturnToChannel = () => {
    if (!activeDiscussionCommentContext) {
      return;
    }

    const channelChatId = activeDiscussionCommentContext.channelChatId;
    const channelMessageId = activeDiscussionCommentContext.channelMessageId;
    clearActiveDiscussionCommentContext();
    openLinkedChannelPost(channelChatId, channelMessageId);
  };

  const openLinkedDiscussionForChannelPost = (channelMessageId: number) => {
    const discussionChatId = activeChannelLinkedDiscussionChatId;
    if (
      !selectedGroup
      || selectedGroup.type !== 'channel'
      || typeof discussionChatId !== 'number'
      || !Number.isFinite(discussionChatId)
      || discussionChatId === 0
    ) {
      return;
    }

    const discussionExists = groupChats.some((chat) => (
      chat.id === discussionChatId && (chat.type === 'group' || chat.type === 'supergroup')
    ));
    if (!discussionExists) {
      setErrorText(`Linked discussion chat ${discussionChatId} is not available.`);
      return;
    }

    const discussionSummary = linkedDiscussionCommentsByChannelMessageId.get(channelMessageId);
    const channelPost = messages.find((message) => (
      message.botToken === selectedBotToken
      && message.chatId === selectedGroup.id
      && message.id === channelMessageId
    ));
    const discussionMessages = messages
      .filter((message) => (
        message.botToken === selectedBotToken
        && message.chatId === discussionChatId
      ));
    const fallbackRootMessage = channelPost
      ? findFallbackDiscussionRootMessage(discussionMessages, channelPost)
      : undefined;
    const targetRootMessageId = discussionSummary?.discussionRootMessageId
      || fallbackRootMessage?.id;
    const targetMessageId = targetRootMessageId
      || discussionSummary?.comments[0]?.id;
    const targetThreadId = discussionSummary?.discussionMessageThreadId
      || fallbackRootMessage?.messageThreadId;
    const discussionStateKey = `${selectedBotToken}:${discussionChatId}`;

    setCommentSourceByDiscussionChatKey((prev) => ({
      ...prev,
      [discussionStateKey]: {
        channelChatId: selectedGroup.id,
        channelMessageId,
        discussionRootMessageId: targetRootMessageId,
      },
    }));

    if (targetMessageId && targetMessageId > 0) {
      const targetDiscussionMessage = messages.find((message) => (
        message.botToken === selectedBotToken
        && message.chatId === discussionChatId
        && message.id === targetMessageId
      ));
      setReplyTarget(targetDiscussionMessage || null);
    } else {
      setReplyTarget(null);
    }

    if (targetThreadId && targetThreadId > 0) {
      setSelectedForumTopicByChatKey((prev) => ({
        ...prev,
        [discussionStateKey]: targetThreadId,
      }));
    }

    setChatScopeTab('group');
    setSelectedGroupChatId(discussionChatId);

    const discussionMembershipKey = `${selectedBotToken}:${discussionChatId}:${selectedUser.id}`;
    if (!isJoinedMembershipStatus(groupMembershipByUser[discussionMembershipKey])) {
      setErrorText('Join the linked discussion group to leave comments.');
    }

    if (targetMessageId && targetMessageId > 0) {
      window.setTimeout(() => {
        scrollToMessage(targetMessageId);
      }, 180);
    }
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

  const onInlineButtonClick = async (message: ChatMessage, button: InlineKeyboardButton) => {
    if (button.pay) {
      const targetInvoiceMessage = resolveInvoiceForPayButton(message);

      if (!targetInvoiceMessage?.invoice) {
        setErrorText('No invoice context found for this Pay button.');
        return;
      }

      const forcedMethod: PaymentMethod = targetInvoiceMessage.invoice.currency.toUpperCase() === 'XTR'
        ? 'stars'
        : 'wallet';
      await onPayInvoice(targetInvoiceMessage, 'success', forcedMethod);
      return;
    }

    const url = typeof button.url === 'string' ? button.url : undefined;
    if (url) {
      window.open(url, '_blank', 'noopener,noreferrer');
      return;
    }

    const callbackData = typeof button.callback_data === 'string' ? button.callback_data : undefined;
    if (callbackData) {
      try {
        setCallbackToast(null);
        const pressed = await pressInlineButton(selectedBotToken, {
          chat_id: selectedChatId,
          message_id: message.id,
          user_id: selectedUser.id,
          first_name: selectedUser.first_name,
          username: selectedUser.username,
          callback_data: callbackData,
        });

        if (pressed.callback_query_id) {
          for (let attempt = 0; attempt < 30; attempt += 1) {
            const callbackAnswer = await getCallbackQueryAnswer(
              selectedBotToken,
              pressed.callback_query_id,
            );
            if (callbackAnswer.answered && callbackAnswer.answer) {
              if (callbackAnswer.answer.url) {
                window.open(callbackAnswer.answer.url, '_blank', 'noopener,noreferrer');
              }

              presentCallbackAnswer(
                callbackAnswer.answer.text,
                callbackAnswer.answer.show_alert,
              );
              break;
            }

            await new Promise((resolve) => window.setTimeout(resolve, 350));
            if (attempt === 29) {
              setCallbackToast('No callback response from bot yet.');
            }
          }
        }
      } catch (error) {
        setErrorText(error instanceof Error ? error.message : 'Inline callback failed');
      }
      return;
    }

    if (typeof button.switch_inline_query_current_chat === 'string') {
      const username = selectedBot?.username || 'bot';
      const suffix = button.switch_inline_query_current_chat.trim();
      setComposerText(`@${username}${suffix ? ` ${suffix}` : ''}`);
      composerTextareaRef.current?.focus();
      return;
    }

    if (typeof button.switch_inline_query === 'string') {
      const username = selectedBot?.username || 'bot';
      const suffix = button.switch_inline_query.trim();
      setComposerText(`@${username}${suffix ? ` ${suffix}` : ''}`);
      composerTextareaRef.current?.focus();
      return;
    }

    if (button.switch_inline_query_chosen_chat && typeof button.switch_inline_query_chosen_chat === 'object') {
      const query = button.switch_inline_query_chosen_chat.query;
      if (typeof query === 'string') {
        const username = selectedBot?.username || 'bot';
        const suffix = query.trim();
        setComposerText(`@${username}${suffix ? ` ${suffix}` : ''}`);
        composerTextareaRef.current?.focus();
        return;
      }
    }

    if (button.copy_text && typeof button.copy_text === 'object') {
      const textToCopy = typeof button.copy_text.text === 'string' && button.copy_text.text.length > 0
        ? button.copy_text.text
        : (typeof button.text === 'string' ? button.text : '');
      if (textToCopy) {
        try {
          await navigator.clipboard.writeText(textToCopy);
        } catch {
          setErrorText('Copy to clipboard failed');
        }
      }
      return;
    }

    if (button.login_url?.url) {
      window.open(button.login_url.url, '_blank', 'noopener,noreferrer');
      return;
    }

    if (button.web_app?.url) {
      window.open(button.web_app.url, '_blank', 'noopener,noreferrer');
      return;
    }

    if (button.callback_game) {
      if (!message.game) {
        setErrorText('Game payload not found on message');
        return;
      }

      try {
        const scores = await getGameHighScores(selectedBotToken, {
          user_id: selectedUser.id,
          chat_id: message.chatId,
          message_id: message.id,
        });
        const current = scores.find((item) => item.user.id === selectedUser.id)?.score || 0;
        await setGameScore(selectedBotToken, {
          user_id: selectedUser.id,
          score: current + 1,
          force: false,
          disable_edit_message: false,
          chat_id: message.chatId,
          message_id: message.id,
        });

        setCallbackToast(`Game callback handled: ${message.game.title}`);
      } catch (error) {
        setErrorText(error instanceof Error ? error.message : 'Game callback failed');
      }
      return;
    }

    setErrorText('This inline button type is not implemented yet.');
  };

  const onDeleteMessage = async (message: ChatMessage) => {
    try {
      await deleteBotMessage(selectedBotToken, {
        chat_id: selectedChatId,
        message_id: message.id,
      }, selectedUser.id);

      setMessages((prev) => prev.filter((item) => !(
        item.botToken === selectedBotToken && item.chatId === selectedChatId && item.id === message.id
      )));

      setPinnedMessageByChatKey((prev) => {
        const next: Record<string, number[]> = {};
        Object.entries(prev).forEach(([key, ids]) => {
          const filtered = ids.filter((id) => id !== message.id);
          if (filtered.length > 0) {
            next[key] = filtered;
          }
        });
        return next;
      });
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
      }, selectedUser.id);

      setMessages((prev) => prev.filter((item) => !(
        item.botToken === selectedBotToken &&
        item.chatId === selectedChatId &&
        selectedMessageIds.includes(item.id)
      )));

      setInvoiceMetaByMessageKey((prev) => {
        const selectedSet = new Set(selectedMessageIds);
        const next: Record<string, InvoiceMetaState> = {};
        Object.entries(prev).forEach(([key, value]) => {
          const parts = key.split(':');
          const token = parts[0] || '';
          const chatId = Number(parts[1]);
          const messageId = Number(parts[2]);
          if (token === selectedBotToken && chatId === selectedChatId && selectedSet.has(messageId)) {
            return;
          }
          next[key] = value;
        });
        return next;
      });

      setPinnedMessageByChatKey((prev) => {
        const selectedSet = new Set(selectedMessageIds);
        const next: Record<string, number[]> = {};
        Object.entries(prev).forEach(([key, ids]) => {
          const filtered = ids.filter((id) => !selectedSet.has(id));
          if (filtered.length > 0) {
            next[key] = filtered;
          }
        });
        return next;
      });

      setSelectedMessageIds([]);
      setSelectionMode(false);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Bulk message delete failed');
    }
  };

  const onClearHistory = async () => {
    const normalizedActiveMessageThreadId = typeof activeMessageThreadId === 'number'
      && Number.isFinite(activeMessageThreadId)
      && activeMessageThreadId > 0
      ? Math.floor(activeMessageThreadId)
      : undefined;
    const scopedDirectMessagesThreadId = selectedGroup?.isDirectMessages
      ? normalizedActiveMessageThreadId
      : undefined;
    const removedMessageIdsInScope = scopedDirectMessagesThreadId === undefined
      ? null
      : new Set(
        messages
          .filter((item) => (
            item.botToken === selectedBotToken
            && item.chatId === selectedChatId
            && item.messageThreadId === scopedDirectMessagesThreadId
          ))
          .map((item) => item.id),
      );

    try {
      await clearSimHistory(selectedBotToken, selectedChatId, scopedDirectMessagesThreadId);
      setMessages((prev) => prev.filter((item) => {
        if (item.botToken !== selectedBotToken || item.chatId !== selectedChatId) {
          return true;
        }
        if (scopedDirectMessagesThreadId === undefined) {
          return false;
        }
        return item.messageThreadId !== scopedDirectMessagesThreadId;
      }));
      setInvoiceMetaByMessageKey((prev) => {
        const prefix = `${selectedBotToken}:${selectedChatId}:`;
        const next: Record<string, InvoiceMetaState> = {};
        Object.entries(prev).forEach(([key, value]) => {
          if (!key.startsWith(prefix)) {
            next[key] = value;
            return;
          }
          if (scopedDirectMessagesThreadId === undefined) {
            return;
          }

          const messageIdRaw = Number(key.slice(prefix.length));
          if (!Number.isFinite(messageIdRaw) || !removedMessageIdsInScope?.has(messageIdRaw)) {
            next[key] = value;
          }
        });
        return next;
      });
      if (scopedDirectMessagesThreadId === undefined) {
        persistStarted({ ...startedChats, [chatKey]: false });
      }
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
    }
  };

  const removeUser = async (id: number) => {
    if (availableUsers.length <= 1) {
      setErrorText('At least one user must remain in simulator.');
      return;
    }

    setIsBootstrapping(true);
    setErrorText('');
    try {
      await deleteSimUser({ id });

      const next = availableUsers.filter((user) => user.id !== id);
      setAvailableUsers(next);
      if (selectedUserId === id) {
        setSelectedUserId(next[0].id);
      }

      setBusinessConnectionByUserKey((prev) => Object.fromEntries(
        Object.entries(prev).filter(([key]) => !key.endsWith(`:${id}`)),
      ));
      setGroupMembershipByUser((prev) => Object.fromEntries(
        Object.entries(prev).filter(([key]) => !key.endsWith(`:${id}`)),
      ));
      setPendingJoinRequestsByChat((prev) => Object.fromEntries(
        Object.entries(prev).map(([key, requests]) => [
          key,
          requests.filter((request) => request.userId !== id),
        ]),
      ));
      setErrorText(`User ${id} deleted.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to delete user');
    } finally {
      setIsBootstrapping(false);
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
      case 'cashtag':
        return 'rounded-md bg-[#0c3048]/55 px-1 text-[#8fcfff] underline-offset-2';
      case 'mention':
        return 'rounded-md bg-[#1d3b57]/65 px-1 text-[#a7dcff] font-medium underline-offset-2 hover:underline';
      case 'bot_command':
        return 'rounded-md bg-[#114463]/70 px-1 text-[#8ce1ff] font-semibold underline-offset-2 hover:underline';
      case 'url':
      case 'text_link':
        return 'rounded-md bg-[#0c3048]/55 px-1 text-[#9ad8ff] underline-offset-2 hover:underline';
      case 'custom_emoji':
        return 'inline-flex items-center justify-center align-middle text-[1.08em]';
      default:
        return '';
    }
  };

  const premiumEmojiGlyph = (customEmojiId?: string) => {
    const glyphs = ['✨', '⭐', '💠', '🌟', '🔹'];
    if (!customEmojiId) {
      return glyphs[0];
    }

    let hash = 0;
    for (let i = 0; i < customEmojiId.length; i += 1) {
      hash = (hash * 31 + customEmojiId.charCodeAt(i)) >>> 0;
    }
    return glyphs[hash % glyphs.length];
  };

  const keyboardButtonClass = (style?: string, inline = false) => {
    const normalized = (style || '').toLowerCase();
    if (normalized === 'primary' || normalized === 'filled') {
      return inline
        ? 'border-[#67bcf2]/50 bg-[#2f6ea1]/90 text-white hover:bg-[#3b82bf]'
        : 'border-[#67bcf2]/50 bg-[#2f6ea1]/90 text-white hover:bg-[#3b82bf]';
    }
    if (normalized === 'danger') {
      return inline
        ? 'border-red-300/35 bg-red-600/35 text-red-100 hover:bg-red-600/45'
        : 'border-red-300/35 bg-red-600/35 text-red-100 hover:bg-red-600/45';
    }
    if (normalized === 'bordered' || normalized === 'secondary') {
      return inline
        ? 'border-[#7dbbde]/50 bg-black/20 text-[#d9efff] hover:bg-[#24435a]/45'
        : 'border-[#7dbbde]/50 bg-[#1f3d56]/70 text-[#d9efff] hover:bg-[#2b5278]';
    }
    return inline
      ? 'border-white/20 bg-black/25 text-white hover:bg-white/10'
      : 'border-white/20 bg-[#234666]/75 text-white hover:bg-[#2f5e85]';
  };

  const mergeAutoEntities = (text: string, entities: MessageEntity[]) => {
    const occupied = entities.map((e) => [e.offset, e.offset + e.length] as const);
    const isFree = (start: number, end: number) => occupied.every(([s, e]) => end <= s || start >= e);

    const patterns: Array<{ regex: RegExp; type: MessageEntity['type'] }> = [
      { regex: /\/[A-Za-z][A-Za-z0-9_]{0,31}(?:@[A-Za-z0-9_]{5,32})?/g, type: 'bot_command' },
      { regex: /@[A-Za-z0-9_]{1,32}/g, type: 'mention' },
      { regex: /#[\p{L}\p{N}_]{1,64}/gu, type: 'hashtag' },
      { regex: /\$[A-Za-z]{1,8}(?:_[A-Za-z]{1,8})?/g, type: 'cashtag' },
    ];

    const auto: MessageEntity[] = [];
    patterns.forEach(({ regex, type }) => {
      const local = new RegExp(regex.source, regex.flags);
      let match = local.exec(text);
      while (match) {
        const value = match[0] || '';
        const start = match.index;
        const end = start + value.length;
        if (value.length > 0 && isFree(start, end)) {
          auto.push({ type, offset: start, length: value.length });
          occupied.push([start, end]);
        }
        match = local.exec(text);
      }
    });

    return [...entities, ...auto].sort((a, b) => a.offset - b.offset);
  };

  const parseHtmlPreview = (input: string): { text: string; entities: MessageEntity[] } => {
    const normalized = input.replace(/<br\s*\/?\s*>/gi, '\n');
    const tgEmojiRegex = /<tg-emoji\b([^>]*)>([\s\S]*?)<\/tg-emoji>/gi;
    const entities: MessageEntity[] = [];
    let text = '';
    let cursor = 0;
    let match = tgEmojiRegex.exec(normalized);

    while (match) {
      const index = match.index;
      const attrs = match[1] || '';
      const rawInner = match[2] || '';
      const plainBefore = normalized.slice(cursor, index).replace(/<[^>]+>/g, '');
      text += plainBefore;

      const plainInner = rawInner.replace(/<[^>]+>/g, '');
      const start = text.length;
      text += plainInner;

      const emojiIdMatch = attrs.match(/emoji-id\s*=\s*['\"]([^'\"]+)['\"]/i);
      const emojiId = emojiIdMatch?.[1];
      if (emojiId && plainInner.length > 0) {
        entities.push({
          type: 'custom_emoji',
          offset: start,
          length: plainInner.length,
          custom_emoji_id: emojiId,
        });
      }

      cursor = index + match[0].length;
      match = tgEmojiRegex.exec(normalized);
    }

    text += normalized.slice(cursor).replace(/<[^>]+>/g, '');
    return { text, entities };
  };

  const renderEntityText = (text: string, entities?: MessageEntity[]) => {
    const validEntities = mergeAutoEntities(
      text,
      [...(entities || [])]
      .filter((entity) => entity.length > 0)
      .sort((a, b) => a.offset - b.offset),
    );

    if (validEntities.length === 0) {
      return text;
    }

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
      } else if (entity.type === 'custom_emoji') {
        nodes.push(
          <span key={key} className={`${classes} tg-premium-emoji`} title="Telegram custom emoji">
            {chunk || premiumEmojiGlyph(entity.custom_emoji_id)}
          </span>,
        );
      } else if ((entity.type === 'text_link' || entity.type === 'url') && entity.url) {
        nodes.push(
          <a key={key} href={entity.url} target="_blank" rel="noreferrer" className={classes}>
            {chunk}
          </a>,
        );
      } else if (entity.type === 'url') {
        const href = chunk.startsWith('http://') || chunk.startsWith('https://') ? chunk : `https://${chunk}`;
        nodes.push(
          <a key={key} href={href} target="_blank" rel="noreferrer" className={classes}>
            {chunk}
          </a>,
        );
      } else if (entity.type === 'bot_command') {
        nodes.push(
          <button
            key={key}
            type="button"
            className={`${classes} cursor-pointer`}
            onClick={() => {
              if (!hasStarted || isSending) {
                return;
              }
              void sendAsUser(chunk, undefined, resolveComposerReplyTargetId(replyTarget?.id));
              setReplyTarget(null);
            }}
            title="Send command"
          >
            {chunk}
          </button>,
        );
      } else if (entity.type === 'mention') {
        nodes.push(
          <span key={key} className={classes} title="Mention">
            {chunk}
          </span>,
        );
      } else if (entity.type === 'hashtag' || entity.type === 'cashtag') {
        nodes.push(
          <span key={key} className={classes} title={entity.type === 'cashtag' ? 'Cashtag' : 'Hashtag'}>
            {chunk}
          </span>,
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
      return { text: input, entities: mergeAutoEntities(input, []) };
    }

    if (mode === 'HTML') {
      const parsed = parseHtmlPreview(input);
      return {
        text: parsed.text,
        entities: mergeAutoEntities(parsed.text, parsed.entities),
      };
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

    return { text, entities: mergeAutoEntities(text, entities) };
  };

  const markdownV2SpecialChars = new Set(['\\', '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']);

  const escapeHtmlText = (value: string) => value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  const escapeHtmlAttr = (value: string) => value
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  const escapeMarkdownV2Text = (value: string) => {
    let escaped = '';
    for (let i = 0; i < value.length; i += 1) {
      const char = value[i];
      if (markdownV2SpecialChars.has(char)) {
        escaped += `\\${char}`;
      } else {
        escaped += char;
      }
    }
    return escaped;
  };

  const formattedEntityTypes = new Set([
    'bold',
    'italic',
    'underline',
    'strikethrough',
    'spoiler',
    'code',
    'pre',
    'text_link',
    'custom_emoji',
    'date_time',
    'blockquote',
    'expandable_blockquote',
  ]);

  const resolveEditParseMode = (
    entities: MessageEntity[],
    preferredMode: ComposerParseMode,
    preservePreferredMode = false,
  ): ComposerParseMode => {
    if (preservePreferredMode && preferredMode !== 'none') {
      return preferredMode;
    }

    if (entities.length === 0) {
      return 'none';
    }

    const hasQuote = entities.some((entity) => entity.type === 'blockquote' || entity.type === 'expandable_blockquote');
    const hasMarkdownV2Only = entities.some((entity) => (
      entity.type === 'underline'
      || entity.type === 'strikethrough'
      || entity.type === 'spoiler'
      || entity.type === 'custom_emoji'
      || entity.type === 'date_time'
    ));

    if (preferredMode === 'HTML') {
      return 'HTML';
    }
    if (preferredMode === 'MarkdownV2') {
      return hasQuote ? 'HTML' : 'MarkdownV2';
    }
    if (preferredMode === 'Markdown') {
      if (hasQuote) {
        return 'HTML';
      }
      if (hasMarkdownV2Only) {
        return 'MarkdownV2';
      }
      return 'Markdown';
    }

    if (hasQuote) {
      return 'HTML';
    }
    if (hasMarkdownV2Only) {
      return 'MarkdownV2';
    }
    return 'Markdown';
  };

  const markerForEntity = (
    entity: MessageEntity,
    mode: Exclude<ComposerParseMode, 'none'>,
  ): { open: string; close: string } | null => {
    if (mode === 'HTML') {
      switch (entity.type) {
        case 'bold':
          return { open: '<b>', close: '</b>' };
        case 'italic':
          return { open: '<i>', close: '</i>' };
        case 'underline':
          return { open: '<u>', close: '</u>' };
        case 'strikethrough':
          return { open: '<s>', close: '</s>' };
        case 'spoiler':
          return { open: '<tg-spoiler>', close: '</tg-spoiler>' };
        case 'code':
          return { open: '<code>', close: '</code>' };
        case 'pre': {
          const language = (entity.language || '').trim();
          if (language) {
            return {
              open: `<pre><code class="language-${escapeHtmlAttr(language)}">`,
              close: '</code></pre>',
            };
          }
          return { open: '<pre>', close: '</pre>' };
        }
        case 'text_link': {
          const url = entity.url?.trim();
          if (!url) {
            return null;
          }
          return { open: `<a href="${escapeHtmlAttr(url)}">`, close: '</a>' };
        }
        case 'custom_emoji': {
          const id = entity.custom_emoji_id?.trim();
          if (!id) {
            return null;
          }
          return { open: `<tg-emoji emoji-id="${escapeHtmlAttr(id)}">`, close: '</tg-emoji>' };
        }
        case 'date_time': {
          const unix = Number.isFinite(Number(entity.unix_time)) ? Math.trunc(Number(entity.unix_time)) : 0;
          const formatAttr = entity.date_time_format?.trim()
            ? ` format="${escapeHtmlAttr(entity.date_time_format.trim())}"`
            : '';
          return { open: `<tg-time unix="${unix}"${formatAttr}>`, close: '</tg-time>' };
        }
        case 'blockquote':
          return { open: '<blockquote>', close: '</blockquote>' };
        case 'expandable_blockquote':
          return { open: '<blockquote expandable>', close: '</blockquote>' };
        default:
          return null;
      }
    }

    if (mode === 'Markdown') {
      switch (entity.type) {
        case 'bold':
          return { open: '*', close: '*' };
        case 'italic':
          return { open: '_', close: '_' };
        case 'code':
          return { open: '`', close: '`' };
        case 'pre': {
          const language = (entity.language || '').replace(/[\r\n`]/g, '').trim();
          return {
            open: language ? `\`\`\`${language}\n` : '```\n',
            close: '\n```',
          };
        }
        case 'text_link': {
          const url = entity.url?.trim();
          if (!url) {
            return null;
          }
          return { open: '[', close: `](${url.replace(/\)/g, '\\)')})` };
        }
        default:
          return null;
      }
    }

    switch (entity.type) {
      case 'bold':
        return { open: '*', close: '*' };
      case 'italic':
        return { open: '_', close: '_' };
      case 'underline':
        return { open: '__', close: '__' };
      case 'strikethrough':
        return { open: '~', close: '~' };
      case 'spoiler':
        return { open: '||', close: '||' };
      case 'code':
        return { open: '`', close: '`' };
      case 'pre': {
        const language = (entity.language || '').replace(/[\r\n`]/g, '').trim();
        return {
          open: language ? `\`\`\`${language}\n` : '```\n',
          close: '\n```',
        };
      }
      case 'text_link': {
        const url = entity.url?.trim();
        if (!url) {
          return null;
        }
        return { open: '[', close: `](${url.replace(/\)/g, '\\)')})` };
      }
      case 'custom_emoji': {
        const id = entity.custom_emoji_id?.trim();
        if (!id) {
          return null;
        }
        return { open: '![', close: `](tg://emoji?id=${id})` };
      }
      case 'date_time': {
        const unix = Number.isFinite(Number(entity.unix_time)) ? Math.trunc(Number(entity.unix_time)) : 0;
        const fmt = entity.date_time_format?.trim()
          ? `&format=${encodeURIComponent(entity.date_time_format.trim())}`
          : '';
        return { open: '![', close: `](tg://time?unix=${unix}${fmt})` };
      }
      default:
        return null;
    }
  };

  const rebuildFormattedMessageForEditing = (
    text: string,
    entities: MessageEntity[],
    preferredMode: ComposerParseMode,
    preservePreferredMode = false,
  ): { text: string; parseMode: ComposerParseMode } => {
    if (!text || entities.length === 0) {
      return {
        text,
        parseMode: preservePreferredMode && preferredMode !== 'none' ? preferredMode : 'none',
      };
    }

    const formattingEntities = entities
      .filter((entity) => entity.length > 0 && formattedEntityTypes.has(entity.type))
      .sort((a, b) => a.offset - b.offset);
    if (formattingEntities.length === 0) {
      return {
        text,
        parseMode: preservePreferredMode && preferredMode !== 'none' ? preferredMode : 'none',
      };
    }

    const parseMode = resolveEditParseMode(formattingEntities, preferredMode, preservePreferredMode);
    if (parseMode === 'none') {
      return { text, parseMode };
    }

    type Marker = {
      open: string;
      close: string;
      start: number;
      end: number;
      length: number;
    };

    const openAt = new Map<number, Marker[]>();
    const closeAt = new Map<number, Marker[]>();
    const textLength = text.length;

    formattingEntities.forEach((entity) => {
      const marker = markerForEntity(entity, parseMode);
      if (!marker) {
        return;
      }
      const start = Math.max(0, Math.min(textLength, entity.offset));
      const end = Math.max(start, Math.min(textLength, entity.offset + entity.length));
      if (end <= start) {
        return;
      }

      const item: Marker = {
        open: marker.open,
        close: marker.close,
        start,
        end,
        length: end - start,
      };

      const opens = openAt.get(start) || [];
      opens.push(item);
      openAt.set(start, opens);

      const closes = closeAt.get(end) || [];
      closes.push(item);
      closeAt.set(end, closes);
    });

    if (openAt.size === 0) {
      return {
        text,
        parseMode: preservePreferredMode && preferredMode !== 'none' ? preferredMode : 'none',
      };
    }

    openAt.forEach((items) => {
      items.sort((a, b) => {
        if (b.length !== a.length) {
          return b.length - a.length;
        }
        return b.open.length - a.open.length;
      });
    });

    closeAt.forEach((items) => {
      items.sort((a, b) => {
        if (a.length !== b.length) {
          return a.length - b.length;
        }
        return b.start - a.start;
      });
    });

    let rebuilt = '';
    for (let i = 0; i <= textLength; i += 1) {
      const closes = closeAt.get(i);
      if (closes) {
        closes.forEach((marker) => {
          rebuilt += marker.close;
        });
      }

      if (i === textLength) {
        break;
      }

      const opens = openAt.get(i);
      if (opens) {
        opens.forEach((marker) => {
          rebuilt += marker.open;
        });
      }

      const char = text[i];
      rebuilt += parseMode === 'HTML'
        ? escapeHtmlText(char)
        : parseMode === 'MarkdownV2'
          ? escapeMarkdownV2Text(char)
          : char;
    }

    return { text: rebuilt, parseMode };
  };

  const pollInlineAnswer = async (inlineQueryId: string, requestSeq: number, append = false) => {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const result = await getInlineQueryAnswer(selectedBotToken, inlineQueryId);
      if (requestSeq !== inlineRequestSeqRef.current) {
        return;
      }

      if (result.answered) {
        const nextOffset = result.answer?.next_offset?.trim();
        setInlineNextOffset(nextOffset ? nextOffset : null);
        const incoming = result.answer?.results || [];
        if (append) {
          setInlineResults((prev) => {
            const merged = new Map<string, InlineQueryResult>();
            prev.forEach((item) => merged.set(String(item.id || ''), item));
            incoming.forEach((item) => merged.set(String(item.id || ''), item));
            return Array.from(merged.values());
          });
        } else {
          setInlineResults(incoming);
        }
        setInlineModeError('');
        return;
      }
      await new Promise((resolve) => window.setTimeout(resolve, 350));
    }
    if (requestSeq === inlineRequestSeqRef.current) {
      setInlineResults([]);
      setInlineNextOffset(null);
      setInlineModeError('No inline answer yet. Bot should call answerInlineQuery.');
    }
  };

  const onLoadMoreInlineResults = async () => {
    if (!inlineTrigger || !inlineNextOffset) {
      return;
    }

    const requestSeq = inlineRequestSeqRef.current + 1;
    inlineRequestSeqRef.current = requestSeq;
    setIsInlineModeSending(true);
    setInlineModeError('');

    try {
      const created = await sendInlineQuery(selectedBotToken, {
        chat_id: selectedChatId,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        query: inlineTrigger.query,
        offset: inlineNextOffset,
      });

      if (requestSeq !== inlineRequestSeqRef.current) {
        return;
      }

      setActiveInlineQueryId(created.inline_query_id);
      await pollInlineAnswer(created.inline_query_id, requestSeq, true);
    } catch (error) {
      if (requestSeq === inlineRequestSeqRef.current) {
        setInlineModeError(error instanceof Error ? error.message : 'Loading more inline results failed');
      }
    } finally {
      if (requestSeq === inlineRequestSeqRef.current) {
        setIsInlineModeSending(false);
      }
    }
  };

  const onChooseInlineResult = async (result: InlineQueryResult) => {
    if (!activeInlineQueryId) {
      return;
    }

    const resultId = String(result.id || '').trim();
    if (!resultId) {
      setInlineModeError('Inline result id is missing.');
      return;
    }

    try {
      await chooseInlineResult(selectedBotToken, {
        inline_query_id: activeInlineQueryId,
        result_id: resultId,
      });
      setComposerText('');
      setInlineResults([]);
      setInlineNextOffset(null);
      setInlineModeError('');
      setActiveInlineQueryId(null);
    } catch (error) {
      setInlineModeError(error instanceof Error ? error.message : 'Choosing inline result failed');
    }
  };

  const composerPreview = useMemo(
    () => parseComposerPreview(composerText, composerParseMode),
    [composerText, composerParseMode],
  );

  const presentCallbackAnswer = (text?: string, showAlert?: boolean) => {
    const normalized = (text || '').trim();
    if (!normalized) {
      return;
    }

    if (showAlert) {
      setCallbackModalText(normalized);
      return;
    }

    setCallbackToast(normalized);
  };

  const onVotePoll = async (message: ChatMessage, optionIndex: number) => {
    if (!message.poll || message.poll.is_closed) {
      return;
    }

    const selectionKey = `${selectedUser.id}:${message.poll.id}`;
    const currentSelection = pollSelections[selectionKey] || [];
    const voteLocked = currentSelection.length > 0 && !message.poll.allows_revoting;
    if (voteLocked) {
      return;
    }

    let nextSelection: number[] = [optionIndex];
    if (message.poll.type === 'quiz') {
      if (message.poll.allows_multiple_answers) {
        if (currentSelection.includes(optionIndex)) {
          nextSelection = currentSelection.length > 1
            ? currentSelection.filter((id) => id !== optionIndex)
            : currentSelection;
        } else {
          nextSelection = [...currentSelection, optionIndex].sort((a, b) => a - b);
        }
      } else {
        nextSelection = [optionIndex];
      }
    } else if (message.poll.allows_multiple_answers) {
      if (currentSelection.includes(optionIndex)) {
        nextSelection = currentSelection.filter((id) => id !== optionIndex);
      } else {
        nextSelection = [...currentSelection, optionIndex].sort((a, b) => a - b);
      }
    } else if (currentSelection.includes(optionIndex)) {
      nextSelection = [];
    }

    try {
      await votePoll(selectedBotToken, {
        chat_id: selectedChatId,
        message_id: message.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        option_ids: nextSelection,
      });
      setPollSelections((prev) => ({
        ...prev,
        [selectionKey]: nextSelection,
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Poll vote failed');
    }
  };

  const togglePollVoters = async (message: ChatMessage) => {
    if (!message.poll) {
      return;
    }

    const pollId = message.poll.id;
    const isExpanded = Boolean(expandedPollVoters[pollId]);
    if (isExpanded) {
      setExpandedPollVoters((prev) => ({ ...prev, [pollId]: false }));
      return;
    }

    setExpandedPollVoters((prev) => ({ ...prev, [pollId]: true }));
    if (pollVotersByPollId[pollId] || pollAnonymousByPollId[pollId]) {
      return;
    }

    setPollVotersLoading((prev) => ({ ...prev, [pollId]: true }));
    try {
      const result = await getPollVoters(selectedBotToken, selectedChatId, message.id);
      setPollAnonymousByPollId((prev) => ({
        ...prev,
        [pollId]: result.anonymous,
      }));
      setPollVotersByPollId((prev) => ({
        ...prev,
        [pollId]: result.voters,
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Poll voters load failed');
    } finally {
      setPollVotersLoading((prev) => ({ ...prev, [pollId]: false }));
    }
  };

  const onRetractPollVote = async (message: ChatMessage) => {
    if (!message.poll || message.poll.is_closed || message.poll.type === 'quiz' || !message.poll.allows_revoting) {
      return;
    }

    try {
      await votePoll(selectedBotToken, {
        chat_id: selectedChatId,
        message_id: message.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        option_ids: [],
      });
      const selectionKey = `${selectedUser.id}:${message.poll.id}`;
      setPollSelections((prev) => ({
        ...prev,
        [selectionKey]: [],
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Poll vote retraction failed');
    }
  };

  const onStopPoll = async (message: ChatMessage) => {
    if (!message.poll || message.poll.is_closed) {
      return;
    }

    try {
      await stopPoll(selectedBotToken, {
        chat_id: selectedChatId,
        message_id: message.id,
      });
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Stop poll failed');
    }
  };

  const renderPollCard = (message: ChatMessage) => {
    if (!message.poll) {
      return null;
    }

    const totalVotes = Math.max(message.poll.total_voter_count, 1);
    const selectionKey = `${selectedUser.id}:${message.poll.id}`;
    const currentSelection = pollSelections[selectionKey] || [];
    const hasVoted = currentSelection.length > 0;
    const voteLocked = hasVoted && !message.poll.allows_revoting;
    const canRetract = !message.poll.is_closed && message.poll.type !== 'quiz' && hasVoted && message.poll.allows_revoting;
    const votersExpanded = Boolean(expandedPollVoters[message.poll.id]);
    const votersLoading = Boolean(pollVotersLoading[message.poll.id]);
    const isAnonymous = pollAnonymousByPollId[message.poll.id] ?? message.poll.is_anonymous;
    const voters = pollVotersByPollId[message.poll.id] || [];

    return (
      <div className="mb-2 rounded-xl border border-white/20 bg-black/20 p-3">
        <div className="mb-1 flex items-center justify-between gap-2">
          <div className="text-sm font-semibold text-white">{message.poll.question}</div>
          <span className="rounded-md border border-white/20 bg-white/10 px-1.5 py-0.5 text-[10px] text-[#d8ecfb]">
            {isAnonymous ? 'anonymous' : 'public'}
          </span>
        </div>
        {message.poll.explanation ? (
          <div className="mb-2 rounded-md border border-[#6caad4]/30 bg-[#17354b]/55 px-2 py-1 text-[11px] text-[#d6eeff]">
            {message.poll.explanation}
          </div>
        ) : null}
        {message.poll.description ? (
          <div className="mb-2 text-[11px] text-[#c5dff2]">{message.poll.description}</div>
        ) : null}
        <div className="space-y-1.5">
          {message.poll.options.map((option, index) => {
            const ratio = Math.round((option.voter_count / totalVotes) * 100);
            const isSelected = currentSelection.includes(index);
            const isQuiz = message.poll?.type === 'quiz';
            const showQuizResult = isQuiz && hasVoted;
            const isCorrect = Array.isArray(message.poll?.correct_option_ids)
              && message.poll!.correct_option_ids!.includes(index);
            const isWrongSelected = showQuizResult && isSelected && !isCorrect;
            return (
              <button
                key={`${message.id}-poll-${index}`}
                type="button"
                disabled={message.poll?.is_closed || voteLocked}
                onClick={() => void onVotePoll(message, index)}
                className={`relative w-full overflow-hidden rounded-lg border px-2 py-2 text-left text-xs text-[#dcefff] disabled:cursor-not-allowed disabled:opacity-70 ${
                  isWrongSelected
                    ? 'border-red-400/50 bg-[#3a1f28]'
                    : showQuizResult && isCorrect
                      ? 'border-emerald-400/55 bg-[#153828]'
                      : isSelected
                        ? 'border-cyan-300/60 bg-[#1a3f56]'
                        : 'border-white/15 bg-[#123148]'
                }`}
              >
                <span
                  className="absolute inset-y-0 left-0 bg-[#2b5278]/55"
                  style={{ width: `${ratio}%` }}
                />
                <span className="relative z-10 flex items-center justify-between gap-2">
                  <span className="flex items-center gap-1.5">
                    <span>{option.text}</span>
                    {showQuizResult && isCorrect ? <span className="text-emerald-200">✓</span> : null}
                    {isWrongSelected ? <span className="text-red-200">✗</span> : null}
                  </span>
                  <span>{option.voter_count}</span>
                </span>
              </button>
            );
          })}
        </div>
        <div className="mt-2 flex items-center justify-between text-[11px] text-telegram-textSecondary">
          <span>{message.poll.total_voter_count} votes</span>
          <span>
            {message.poll.is_closed ? 'closed' : (!message.poll.allows_revoting && hasVoted ? 'final vote' : '')}
          </span>
        </div>
        {!isAnonymous ? (
          <button
            type="button"
            onClick={() => void togglePollVoters(message)}
            className="mt-2 rounded-md border border-white/20 bg-white/10 px-2 py-1 text-[11px] text-white hover:bg-white/15"
          >
            {votersExpanded ? 'Hide voters' : 'Show voters'}
          </button>
        ) : null}
        {votersExpanded ? (
          <div className="mt-2 rounded-md border border-white/15 bg-black/25 px-2 py-2 text-[11px] text-[#d6eaff]">
            {votersLoading ? (
              <div>Loading voters...</div>
            ) : isAnonymous ? (
              <div>This poll is anonymous. Voter identities are hidden.</div>
            ) : voters.length === 0 ? (
              <div>No voters yet.</div>
            ) : (
              <div className="space-y-1">
                {voters.map((voter) => (
                  <div key={`${message.poll!.id}-${voter.user_id}`} className="flex items-center justify-between gap-2">
                    <span>{voter.first_name}{voter.username ? ` (@${voter.username})` : ''}</span>
                    <span className="text-[#9cc8e3]">
                      {voter.option_ids.map((id) => message.poll!.options[id]?.text).filter(Boolean).join(', ')}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : null}
        {!message.poll.is_closed && message.isOutgoing ? (
          <button
            type="button"
            onClick={() => void onStopPoll(message)}
            className="mt-2 rounded-md border border-white/20 bg-white/10 px-2 py-1 text-[11px] text-white hover:bg-white/15"
          >
            Stop poll
          </button>
        ) : null}
        {canRetract ? (
          <button
            type="button"
            onClick={() => void onRetractPollVote(message)}
            className="mt-2 ml-2 rounded-md border border-white/20 bg-white/10 px-2 py-1 text-[11px] text-white hover:bg-white/15"
          >
            Retract vote
          </button>
        ) : null}
      </div>
    );
  };

  const renderInvoiceCard = (message: ChatMessage) => {
    if (!message.invoice) {
      return null;
    }

    const isStars = message.invoice.currency.toUpperCase() === 'XTR';
    const invoiceMetaKey = `${selectedBotToken}:${message.chatId}:${message.id}`;
    const invoiceMeta = invoiceMetaByMessageKey[invoiceMetaKey];
    const invoiceImage = invoiceMeta?.photoUrl;
    const suggestedTips = invoiceMeta?.suggestedTipAmounts || [];

    return (
      <div className="mb-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 p-3">
        {invoiceImage ? (
          <img
            src={invoiceImage}
            alt="invoice"
            className="mb-2 max-h-48 w-full rounded-lg object-cover"
            onError={(event) => {
              event.currentTarget.style.display = 'none';
            }}
          />
        ) : null}
        <div className="text-sm font-semibold text-white">{message.invoice.title}</div>
        <div className="mt-1 text-xs text-[#d1e7f7]">{message.invoice.description}</div>
        <div className="mt-2 text-xs text-[#9fc6df]">
          {message.invoice.total_amount} {message.invoice.currency}
        </div>
        {suggestedTips.length > 0 ? (
          <div className="mt-2 flex flex-wrap gap-1">
            {suggestedTips.map((tip) => (
              <span key={`invoice-tip-${message.id}-${tip}`} className="rounded border border-white/20 bg-white/10 px-1.5 py-0.5 text-[10px] text-[#d7ecfb]">
                Tip {tip}
              </span>
            ))}
          </div>
        ) : null}
        {message.isOutgoing ? (
          <div className="mt-2 space-y-2">
            <div className="flex items-center justify-between gap-2">
              <span className="text-[10px] text-[#8fb8d4]">
                {isStars ? 'Stars invoice' : 'Fiat invoice'}
              </span>
              {isStars ? (
                <span className="inline-flex items-center gap-1 rounded border border-[#7ec8fb]/45 bg-[#1d3f56]/75 px-2 py-0.5 text-[10px] text-[#d6eeff]">
                  <Star className="h-3 w-3" />
                  Only Stars
                </span>
              ) : null}
            </div>

            <button
              type="button"
              onClick={() => openCheckoutFlow(message)}
              className="w-full rounded-md border border-[#6ab8ef]/50 bg-[#1f5379] px-2 py-1 text-[11px] text-white transition hover:bg-[#2b6a98]"
            >
              Open Checkout
            </button>

            <div className="grid grid-cols-2 gap-1.5">
              <button
                type="button"
                onClick={() => void onPayInvoice(message, 'success', isStars ? 'stars' : 'wallet', 0)}
                className="rounded-md border border-emerald-300/50 bg-emerald-700/35 px-2 py-1 text-[11px] text-emerald-100 transition hover:bg-emerald-700/45"
              >
                Quick Success
              </button>
              <button
                type="button"
                onClick={() => void onPayInvoice(message, 'failed')}
                className="rounded-md border border-red-300/40 bg-red-700/30 px-2 py-1 text-[11px] text-red-100 transition hover:bg-red-700/40"
              >
                Quick Fail
              </button>
            </div>
          </div>
        ) : null}
      </div>
    );
  };

  const renderSuccessfulPaymentCard = (message: ChatMessage) => {
    if (!message.successfulPayment) {
      return null;
    }

    return (
      <div className="mb-2 rounded-xl border border-emerald-400/40 bg-emerald-900/25 p-3">
        <div className="text-sm font-semibold text-emerald-100">Payment successful</div>
        <div className="mt-1 text-xs text-emerald-200/90">
          {message.successfulPayment.total_amount} {message.successfulPayment.currency}
        </div>
        <div className="mt-1 text-[11px] text-emerald-300/85">
          charge: {message.successfulPayment.telegram_payment_charge_id}
        </div>
      </div>
    );
  };

  const renderContactCard = (message: ChatMessage) => {
    if (!message.contact) {
      return null;
    }

    return (
      <div className="mb-2 rounded-xl border border-[#4f7ea0]/45 bg-[#17374d]/75 p-3 text-[#dcf0ff]">
        <div className="text-sm font-semibold">Contact</div>
        <div className="mt-1 text-xs">
          {message.contact.first_name}{message.contact.last_name ? ` ${message.contact.last_name}` : ''}
        </div>
        <div className="text-xs text-[#b7d7ed]">{message.contact.phone_number}</div>
      </div>
    );
  };

  const renderLocationCard = (message: ChatMessage) => {
    if (!message.location) {
      return null;
    }

    const lat = message.location.latitude.toFixed(6);
    const lon = message.location.longitude.toFixed(6);
    return (
      <div className="mb-2 rounded-xl border border-[#4f7ea0]/45 bg-[#17374d]/75 p-3 text-[#dcf0ff]">
        <div className="text-sm font-semibold">Location</div>
        <div className="mt-1 text-xs">{lat}, {lon}</div>
      </div>
    );
  };

  const renderVenueCard = (message: ChatMessage) => {
    if (!message.venue) {
      return null;
    }

    const lat = message.venue.location.latitude.toFixed(6);
    const lon = message.venue.location.longitude.toFixed(6);
    return (
      <div className="mb-2 rounded-xl border border-[#4f7ea0]/45 bg-[#17374d]/75 p-3 text-[#dcf0ff]">
        <div className="text-sm font-semibold">{message.venue.title}</div>
        <div className="mt-1 text-xs text-[#b7d7ed]">{message.venue.address}</div>
        <div className="mt-1 text-[11px] text-[#a7cadf]">{lat}, {lon}</div>
      </div>
    );
  };

  const renderDiceCard = (message: ChatMessage) => {
    if (!message.dice) {
      return null;
    }

    return (
      <div className="mb-2 inline-flex items-center gap-2 rounded-xl border border-white/20 bg-black/20 px-3 py-2 text-sm text-white">
        <span className="text-xl leading-none">{message.dice.emoji}</span>
        <span className="font-semibold">{message.dice.value}</span>
      </div>
    );
  };

  const renderGameCard = (message: ChatMessage) => {
    if (!message.game) {
      return null;
    }

    return (
      <div className="mb-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 p-3">
        <div className="text-sm font-semibold text-white">{message.game.title}</div>
        <div className="mt-1 text-xs text-[#d1e7f7]">{message.game.description}</div>
        <div className="mt-2 flex flex-wrap gap-1.5">
          <button
            type="button"
            onClick={() => {
              void (async () => {
                try {
                  const scores = await getGameHighScores(selectedBotToken, {
                    user_id: selectedUser.id,
                    chat_id: message.chatId,
                    message_id: message.id,
                  });
                  const current = scores.find((item) => item.user.id === selectedUser.id)?.score || 0;
                  const nextScore = current + (Math.floor(Math.random() * 12) + 1);
                  await setGameScore(selectedBotToken, {
                    user_id: selectedUser.id,
                    score: nextScore,
                    force: false,
                    disable_edit_message: false,
                    chat_id: message.chatId,
                    message_id: message.id,
                  });
                } catch (error) {
                  setErrorText(error instanceof Error ? error.message : 'Set game score failed');
                }
              })();
            }}
            className="rounded-md border border-[#6ab8ef]/50 bg-[#1f5379] px-2 py-1 text-[11px] text-white transition hover:bg-[#2b6a98]"
          >
            Play (+score)
          </button>
          <button
            type="button"
            onClick={() => {
              void (async () => {
                try {
                  const scores = await getGameHighScores(selectedBotToken, {
                    user_id: selectedUser.id,
                    chat_id: message.chatId,
                    message_id: message.id,
                  });
                  const top = scores.slice(0, 5).map((s) => `${s.position}. ${s.user.first_name}: ${s.score}`).join('\n');
                  setCallbackModalText(top || 'No scores yet');
                } catch (error) {
                  setErrorText(error instanceof Error ? error.message : 'Get game scores failed');
                }
              })();
            }}
            className="rounded-md border border-white/20 bg-white/10 px-2 py-1 text-[11px] text-white transition hover:bg-white/15"
          >
            High scores
          </button>
        </div>
      </div>
    );
  };

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
      return <img src={mediaUrl} alt="photo" className={compact ? 'h-40 w-full rounded-xl object-cover' : 'max-h-80 w-full rounded-xl object-contain sm:w-auto'} />;
    }

    if (message.media.type === 'video') {
      return <video src={mediaUrl} controls className={compact ? 'h-40 w-full rounded-xl object-cover' : 'max-h-80 w-full rounded-xl object-contain sm:w-auto'} />;
    }

    if (message.media.type === 'animation') {
      return (
        <div className={compact ? 'h-40 w-full overflow-hidden rounded-xl bg-black/25' : 'max-w-[260px] overflow-hidden rounded-xl bg-black/25 sm:max-w-[300px]'}>
          <video
            src={mediaUrl}
            autoPlay
            muted
            loop
            playsInline
            controls={false}
            className="h-full w-full object-cover"
          />
        </div>
      );
    }

    if (message.media.type === 'video_note') {
      const isPlaying = playingVideoNoteMessageId === message.id;
      return (
        <button
          type="button"
          onClick={() => toggleVideoNotePlayback(message.id)}
          className={compact
            ? 'relative h-32 w-32 overflow-hidden rounded-full border border-white/20 bg-black/35'
            : 'relative h-40 w-40 overflow-hidden rounded-full border border-white/20 bg-black/35'}
        >
          <video
            ref={(node) => {
              videoNoteRefs.current[message.id] = node;
            }}
            src={mediaUrl}
            playsInline
            controls={false}
            className="h-full w-full object-cover"
            onPause={() => {
              if (playingVideoNoteMessageId === message.id) {
                setPlayingVideoNoteMessageId(null);
              }
            }}
            onEnded={() => {
              if (playingVideoNoteMessageId === message.id) {
                setPlayingVideoNoteMessageId(null);
              }
            }}
          />
          <span className="absolute inset-0 flex items-center justify-center bg-black/30">
            {isPlaying ? <Pause className="h-8 w-8 text-white" /> : <Play className="h-8 w-8 text-white" />}
          </span>
        </button>
      );
    }

    if (message.media.type === 'sticker') {
      const isVideoSticker = (message.media.mimeType || '').toLowerCase().includes('video');
      if (isVideoSticker) {
        return (
          <video
            src={mediaUrl}
            autoPlay
            muted
            loop
            playsInline
            controls={false}
            className={compact ? 'h-28 w-28 rounded-xl object-contain' : 'h-36 w-36 rounded-xl object-contain'}
          />
        );
      }
      return <img src={mediaUrl} alt="sticker" className={compact ? 'h-28 w-28 rounded-xl object-contain' : 'h-36 w-36 rounded-xl object-contain'} />;
    }

    if (message.media.type === 'audio' || message.media.type === 'voice') {
      return (
        <div className="max-w-full rounded-xl border border-white/15 bg-black/20 px-2 py-2">
          <audio src={mediaUrl} controls className="w-64 max-w-full" controlsList="nodownload noplaybackrate" />
        </div>
      );
    }

    if (message.media.type === 'document') {
      return (
        <a
          href={mediaUrl}
          download={message.media.fileName || 'document'}
          target="_blank"
          rel="noreferrer"
          className="inline-flex max-w-full items-center gap-2 rounded-lg border border-white/20 bg-black/25 px-3 py-2 text-xs text-white hover:bg-white/10"
        >
          <span className="shrink-0">Download</span>
          <span className="min-w-0 break-all">{message.media.fileName || message.media.fileId || 'file'}</span>
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

  const renderInlineKeyboard = (message: ChatMessage) => {
    if (!message.replyMarkup || message.replyMarkup.kind !== 'inline') {
      return null;
    }

    if (!message.replyMarkup.inline_keyboard || message.replyMarkup.inline_keyboard.length === 0) {
      return null;
    }

    const buttonIndicator = (button: InlineKeyboardButton): { icon: string; hint: string } => {
      if (button.callback_data) {
        return { icon: '⏺', hint: 'Callback data' };
      }
      if (button.url) {
        return { icon: '↗', hint: 'Open link' };
      }
      if (button.copy_text) {
        return { icon: '⧉', hint: 'Copy text' };
      }
      if (button.switch_inline_query || button.switch_inline_query_current_chat || button.switch_inline_query_chosen_chat) {
        return { icon: '⌕', hint: 'Switch inline query' };
      }
      if (button.login_url) {
        return { icon: '🔐', hint: 'Login URL' };
      }
      if (button.web_app) {
        return { icon: '🗔', hint: 'Web App' };
      }
      if (button.callback_game) {
        return { icon: '🎮', hint: 'Game callback' };
      }
      if (button.pay) {
        return { icon: '★', hint: 'Payment' };
      }
      return { icon: '•', hint: 'Inline button' };
    };

    const keyboardRows = message.replyMarkup.inline_keyboard.filter((row) => row.length > 0);

    if (keyboardRows.length === 0) {
      return null;
    }

    return (
      <div className="mt-2 space-y-1.5">
        {keyboardRows.map((row, rowIndex) => (
          <div
            key={`ik-row-${message.id}-${rowIndex}`}
            className="grid gap-1.5"
            style={{ gridTemplateColumns: `repeat(${Math.max(row.length, 1)}, minmax(0, 1fr))` }}
          >
            {row.map((button, buttonIndex) => {
              const payInvoiceContext = button.pay ? resolveInvoiceForPayButton(message) : null;
              const label = button.pay && payInvoiceContext?.invoice
                ? `${button.text || 'Pay'} ${payInvoiceContext.invoice.total_amount} ${payInvoiceContext.invoice.currency}`
                : (typeof button.text === 'string' ? button.text : 'Button');
              const indicator = buttonIndicator(button);
              return (
                <button
                  key={`ik-btn-${message.id}-${rowIndex}-${buttonIndex}`}
                  type="button"
                  onClick={() => void onInlineButtonClick(message, button)}
                  className={`rounded-lg border px-3 py-1.5 text-xs transition ${keyboardButtonClass(button.style, true)}`}
                  title={`${indicator.hint}: ${label}`}
                >
                  <span className="inline-flex items-center gap-1.5">
                    {button.icon_custom_emoji_id ? (
                      <span className="tg-premium-emoji text-[13px] leading-none" title="Premium custom emoji icon">
                        {premiumEmojiGlyph(button.icon_custom_emoji_id)}
                      </span>
                    ) : null}
                    <span className="text-[11px] leading-none opacity-90">{indicator.icon}</span>
                    <span className="line-clamp-1">{label}</span>
                  </span>
                </button>
              );
            })}
          </div>
        ))}
      </div>
    );
  };

  useEffect(() => {
    if (availableBots.length === 0) {
      return;
    }

    if (!availableBots.some((bot) => bot.token === selectedBotToken)) {
      setSelectedBotToken(availableBots[0].token);
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
      setForumTopicContextMenu(null);
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
    if (!stickerShelfActiveSet && stickerShelf.length > 0) {
      setStickerShelfActiveSet(stickerShelf[0].name);
    }
  }, [stickerShelf, stickerShelfActiveSet]);

  const stickerRealtimeKey = useMemo(
    () => visibleMessages
      .filter((message) => message.media?.type === 'sticker' && message.media.setName)
      .slice(-16)
      .map((message) => `${message.id}:${message.media?.setName || ''}`)
      .join('|'),
    [visibleMessages],
  );

  useEffect(() => {
    const discovered = new Set<string>(stickerSetNamesFromMessages.filter(Boolean));
    if (stickerStudio.setName.trim()) {
      discovered.add(stickerStudio.setName.trim());
    }

    const toLoad = Array.from(discovered);
    if (toLoad.length === 0) {
      return;
    }

    let cancelled = false;
    void (async () => {
      for (const setName of toLoad) {
        if (cancelled) {
          return;
        }
        await loadStickerSetIntoShelf(setName, { silent: true });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [stickerSetNamesFromMessages, stickerStudio.setName, stickerRealtimeKey]);

  useEffect(() => {
    const targets = [
      ...(activeStickerSet?.stickers || []).map((item) => item.file_id),
      ...animationGallery.slice(0, 18).map((item) => item.fileId),
    ].filter((fileId) => !mediaUrlByFileId[fileId]);

    if (targets.length === 0) {
      return;
    }

    let cancelled = false;
    void (async () => {
      const updates: Record<string, string> = {};
      for (const fileId of targets) {
        try {
          const url = await resolveMediaUrl(selectedBotToken, fileId);
          if (cancelled) {
            return;
          }
          updates[fileId] = url;
        } catch {
          // Ignore preview fetch failures to keep drawer responsive.
        }
      }
      if (!cancelled && Object.keys(updates).length > 0) {
        setMediaUrlByFileId((prev) => ({ ...prev, ...updates }));
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [activeStickerSet, animationGallery, mediaUrlByFileId, selectedBotToken]);

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

  useEffect(() => {
    if (!hasStarted || !inlineTrigger) {
      inlineRequestSeqRef.current += 1;
      setIsInlineModeSending(false);
      setActiveInlineQueryId(null);
      setInlineResults([]);
      setInlineModeError('');
      return;
    }

    const requestSeq = inlineRequestSeqRef.current + 1;
    inlineRequestSeqRef.current = requestSeq;
    const timeout = window.setTimeout(() => {
      void (async () => {
        setIsInlineModeSending(true);
        setInlineModeError('');
        try {
          const created = await sendInlineQuery(selectedBotToken, {
            chat_id: selectedChatId,
            user_id: selectedUser.id,
            first_name: selectedUser.first_name,
            username: selectedUser.username,
            query: inlineTrigger.query,
          });

          if (requestSeq !== inlineRequestSeqRef.current) {
            return;
          }

          setActiveInlineQueryId(created.inline_query_id);
          await pollInlineAnswer(created.inline_query_id, requestSeq);
        } catch (error) {
          if (requestSeq === inlineRequestSeqRef.current) {
            setInlineResults([]);
            setInlineNextOffset(null);
            setInlineModeError(error instanceof Error ? error.message : 'Inline query failed');
          }
        } finally {
          if (requestSeq === inlineRequestSeqRef.current) {
            setIsInlineModeSending(false);
          }
        }
      })();
    }, 280);

    return () => {
      window.clearTimeout(timeout);
    };
  }, [
    hasStarted,
    inlineTrigger,
    selectedBotToken,
    selectedChatId,
    selectedUser.id,
    selectedUser.first_name,
    selectedUser.username,
  ]);

  useEffect(() => {
    if (!callbackToast) {
      return;
    }

    const timeout = window.setTimeout(() => {
      setCallbackToast(null);
    }, 4200);

    return () => window.clearTimeout(timeout);
  }, [callbackToast]);

  return (
    <div className="h-screen overflow-hidden bg-app-pattern text-telegram-text">
      <div className="mx-auto flex h-full w-full min-w-0 max-w-[1500px] border-x border-white/10 backdrop-blur-md">
        <aside className="w-[260px] shrink-0 overflow-y-auto border-r border-white/10 bg-[#152434]/95 sm:w-[280px] lg:w-[300px]">
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
                onClick={() => {
                  setChatScopeTab('group');
                  setGroupDraft((prev) => ({
                    ...prev,
                    type: prev.type === 'channel' ? 'supergroup' : prev.type,
                  }));
                }}
                className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'group' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Group
              </button>
              <button
                type="button"
                onClick={() => {
                  setChatScopeTab('channel');
                  setGroupDraft((prev) => ({
                    ...prev,
                    type: 'channel',
                    isForum: false,
                  }));
                }}
                className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'channel' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary'}`}
              >
                Channel
              </button>
            </div>

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

            {activeTab === 'chats' && (chatScopeTab === 'group' || chatScopeTab === 'channel') ? (
              <>
                <div className="mb-3 flex items-center gap-2 rounded-xl bg-white/5 px-3 py-2 text-sm text-telegram-textSecondary">
                  <Search className="h-4 w-4" />
                  <input
                    value={chatSearch}
                    onChange={(e) => setChatSearch(e.target.value)}
                    className="w-full bg-transparent text-sm text-white outline-none placeholder:text-telegram-textSecondary"
                    placeholder={chatScopeTab === 'channel' ? 'Search channels' : 'Search groups'}
                  />
                </div>

                <button
                  type="button"
                  onClick={() => setShowCreateGroupForm((prev) => !prev)}
                  className="mb-2 w-full rounded-xl border border-white/15 bg-black/20 px-3 py-2 text-left text-xs text-white hover:bg-black/30"
                >
                  {showCreateGroupForm
                    ? `Close ${chatScopeTab === 'channel' ? 'channel' : 'group'} creator`
                    : `Create new ${chatScopeTab === 'channel' ? 'channel' : 'group'}`}
                </button>

                {showCreateGroupForm ? (
                  <div className="mb-3 space-y-2 rounded-xl border border-white/10 bg-black/20 p-3 text-xs">
                    <input
                      value={groupDraft.title}
                      onChange={(e) => setGroupDraft((prev) => ({ ...prev, title: e.target.value }))}
                      className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                      placeholder={chatScopeTab === 'channel' ? 'Channel title' : 'Group title'}
                    />
                    {chatScopeTab === 'group' ? (
                      <div className="grid grid-cols-2 gap-2">
                        <select
                          value={groupDraft.type === 'channel' ? 'supergroup' : groupDraft.type}
                          onChange={(e) => setGroupDraft((prev) => ({ ...prev, type: e.target.value as 'group' | 'supergroup' }))}
                          className="rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                        >
                          <option value="group">group</option>
                          <option value="supergroup">supergroup</option>
                        </select>
                        <input
                          value={groupDraft.username}
                          onChange={(e) => setGroupDraft((prev) => ({ ...prev, username: e.target.value }))}
                          className="rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                          placeholder="public username"
                        />
                      </div>
                    ) : (
                      <input
                        value={groupDraft.username}
                        onChange={(e) => setGroupDraft((prev) => ({ ...prev, username: e.target.value }))}
                        className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                        placeholder="public username"
                      />
                    )}
                    <input
                      value={groupDraft.description}
                      onChange={(e) => setGroupDraft((prev) => ({ ...prev, description: e.target.value }))}
                      className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                      placeholder="description"
                    />
                    {chatScopeTab === 'group' && groupDraft.type === 'supergroup' ? (
                      <label className="flex items-center gap-2 text-telegram-textSecondary">
                        <input
                          type="checkbox"
                          checked={groupDraft.isForum}
                          onChange={(e) => setGroupDraft((prev) => ({ ...prev, isForum: e.target.checked }))}
                        />
                        Enable forum topics
                      </label>
                    ) : null}
                    <button
                      type="button"
                      onClick={() => void onCreateGroup()}
                      disabled={isCreatingGroup || !groupDraft.title.trim()}
                      className="w-full rounded-lg bg-[#2b5278] px-3 py-2 text-white disabled:opacity-50"
                    >
                      {isCreatingGroup
                        ? 'Creating...'
                        : (chatScopeTab === 'channel'
                          ? 'Create Channel'
                          : (groupDraft.type === 'supergroup' ? 'Create Supergroup' : 'Create Group'))}
                    </button>
                  </div>
                ) : null}

                <div className="mb-3 space-y-2 rounded-xl border border-white/10 bg-black/20 p-3 text-xs">
                  <p className="text-[11px] text-telegram-textSecondary">
                    {chatScopeTab === 'channel' ? 'Join channel by invite link' : 'Join group by invite link'}
                  </p>
                  <div className="flex items-center gap-2">
                    <input
                      value={groupInviteLinkInput}
                      onChange={(e) => setGroupInviteLinkInput(e.target.value)}
                      className="min-w-0 flex-1 rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                      placeholder="https://t.me/+..."
                    />
                    <button
                      type="button"
                      onClick={() => void onJoinGroupByInviteLink()}
                      disabled={isBootstrapping || !groupInviteLinkInput.trim()}
                      className="rounded-lg bg-[#2b5278] px-3 py-1.5 text-white disabled:opacity-50"
                    >
                      Join
                    </button>
                  </div>
                  {selectedGroupInviteLink ? (
                    <button
                      type="button"
                      onClick={async () => {
                        try {
                          await navigator.clipboard.writeText(selectedGroupInviteLink);
                          setErrorText('Invite link copied.');
                        } catch {
                          setErrorText('Invite link copy failed.');
                        }
                      }}
                      className="w-full truncate rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-left text-[11px] text-[#bfe4ff] hover:bg-[#14283a]"
                      title={selectedGroupInviteLink}
                    >
                      Latest invite: {selectedGroupInviteLink}
                    </button>
                  ) : null}
                </div>

                <div className="space-y-2">
                  {filteredGroups.map((group) => {
                    const isActive = group.id === selectedGroupChatId;
                    const memberState = groupMembershipByUser[`${selectedBotToken}:${group.id}:${selectedUser.id}`] || 'unknown';
                    return (
                      <button
                        key={group.id}
                        type="button"
                        onClick={() => setSelectedGroupChatId(group.id)}
                        className={`w-full rounded-xl border px-3 py-2 text-left transition ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20 hover:bg-black/30'}`}
                      >
                        <div className="flex items-center justify-between">
                          <p className="font-medium text-white truncate">{group.title}</p>
                          <span className="text-[10px] text-[#b5cfdf]">{group.isDirectMessages ? 'direct-messages' : group.type}</span>
                        </div>
                        <p className="text-xs text-telegram-textSecondary truncate">
                          {group.username ? `@${group.username}` : `id ${group.id}`}
                        </p>
                        {group.isDirectMessages && group.parentChannelChatId ? (
                          <p className="text-[10px] text-[#9ec6df]">channel #{group.parentChannelChatId}</p>
                        ) : null}
                        <p className="text-[10px] text-telegram-textSecondary">membership: {memberState}</p>
                      </button>
                    );
                  })}
                  {filteredGroups.length === 0 ? (
                    <p className="rounded-xl border border-white/10 bg-black/20 px-3 py-3 text-xs text-telegram-textSecondary">
                      {chatScopeTab === 'channel'
                        ? 'No channels yet. Create your first channel.'
                        : 'No groups yet. Create your first group.'}
                    </p>
                  ) : null}
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
                  const userBusinessConnection = businessConnectionByUserKey[`${selectedBotToken}:${user.id}`];
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
                          onClick={() => void removeUser(user.id)}
                          className="rounded-full p-1 text-telegram-textSecondary hover:bg-white/10 hover:text-white"
                          title="Delete user"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                      <p className="mt-1 text-[11px] text-[#aac4d7]">id: {user.id}</p>
                      {userBusinessConnection ? (
                        <p className="mt-1 text-[11px] text-[#9ec3dc]">
                          business: {userBusinessConnection.id} · {userBusinessConnection.is_enabled ? 'enabled' : 'disabled'}
                        </p>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>
        </aside>

        <section className="flex min-w-0 flex-1 flex-col bg-[#0f1e2d]/70">
          <header className="flex flex-wrap items-center justify-between gap-2 border-b border-white/10 bg-[#1a2a3b]/70 px-3 py-3 sm:px-4 lg:px-5">
            <div className="flex min-w-0 items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[#2b5278]">
                <Bot className="h-5 w-5" />
              </div>
              <div className="min-w-0">
                <h2 className="truncate font-semibold">{selectedBot?.first_name || 'Bot'}</h2>
                <p className="truncate text-xs text-telegram-textSecondary">
                  @{selectedBot?.username || 'unknown'} | {chatScopeTab === 'private'
                    ? 'Private'
                    : (isDiscussionThreadView
                      ? `Discussion · ${activeDiscussionCommentContext?.commentsCount || 0} comments`
                      : (selectedGroup?.title || (chatScopeTab === 'channel' ? 'Channel' : 'Group')))}
                  {chatScopeTab === 'group' && !isDiscussionThreadView && (selectedGroup?.isForum || selectedGroup?.isDirectMessages) && activeForumTopic
                    ? ` · ${selectedGroup?.isDirectMessages ? 'DM Topic' : 'Topic'}: ${activeForumTopic.name}`
                    : ''}
                </p>
              </div>
            </div>
            <div className="flex flex-wrap items-center justify-end gap-2">
              <select
                value={selectedUserId}
                onChange={(e) => setSelectedUserId(Number(e.target.value))}
                className="max-w-[180px] rounded-lg border border-white/15 bg-black/20 px-2 py-1.5 text-xs text-white outline-none"
                title="Quick user switch"
              >
                {availableUsers.map((user) => (
                  <option key={user.id} value={user.id}>
                    {user.first_name} ({user.id})
                  </option>
                ))}
              </select>
              {chatScopeTab === 'channel'
                && selectedGroup
                && selectedGroup.type === 'channel'
                && !selectedGroup.isDirectMessages
                && selectedGroup.settings?.directMessagesEnabled
                && groupMembership === 'joined' ? (
                <button
                  type="button"
                  onClick={() => void onOpenChannelDirectMessages()}
                  disabled={isGroupActionRunning}
                  className="rounded-full border border-white/15 bg-black/20 px-3 py-1.5 text-xs text-white hover:bg-white/10 disabled:opacity-40"
                  title={canSelectedUserManageChannelDirectMessages
                    ? 'Open inbox topics.'
                    : 'Open your direct message thread.'}
                >
                  {canSelectedUserManageChannelDirectMessages ? 'Inbox' : 'Open DM'}
                </button>
              ) : null}
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
                  className="absolute right-0 top-11 z-20 w-72 max-w-[85vw] rounded-xl border border-white/15 bg-[#132130] p-1 shadow-2xl"
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
                  {(chatScopeTab === 'group' || chatScopeTab === 'channel') && !selectedGroup?.isDirectMessages ? (
                    <button
                      type="button"
                      onClick={() => {
                        setGroupInviteEditorDraft((prev) => ({
                          ...prev,
                          inviteLink: selectedGroupInviteLink || prev.inviteLink,
                        }));
                        setGroupSettingsPage('home');
                        setExpandedGroupMemberId(null);
                        setShowGroupActionsModal(true);
                        setChatMenuOpen(false);
                      }}
                      disabled={!selectedGroup}
                      className="w-full rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      Open {chatScopeTab === 'channel' ? 'channel' : 'group'} controls
                    </button>
                  ) : null}
                  {(chatScopeTab === 'group' || chatScopeTab === 'channel') && !selectedGroup?.isDirectMessages ? (
                    <button
                      type="button"
                      onClick={() => {
                        void onLeaveSelectedGroup();
                        setChatMenuOpen(false);
                      }}
                      disabled={!selectedGroup || !canLeaveSelectedGroup}
                      className="w-full rounded-lg px-3 py-2 text-left text-sm text-orange-200 hover:bg-white/10 disabled:opacity-40"
                    >
                      Leave {chatScopeTab === 'channel' ? 'channel' : 'group'}
                    </button>
                  ) : null}
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

          {chatScopeTab === 'group' && activeDiscussionCommentContext ? (
            <div className="shrink-0 border-b border-white/10 bg-[#102235]/95 px-3 py-2 backdrop-blur sm:px-4 lg:px-6">
              <div className="mx-auto w-full max-w-3xl rounded-2xl border border-[#77b9e1]/45 bg-[#11314a]/85 px-3 py-2.5 shadow-lg">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="min-w-0">
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-[#9dd8ff]">Channel discussion</p>
                    <p className="truncate text-sm text-[#def2ff]">
                      Post #{activeDiscussionCommentContext.channelMessageId} · {activeDiscussionCommentContext.commentsCount} comments
                    </p>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <button
                      type="button"
                      onClick={closeDiscussionThreadAndReturnToChannel}
                      className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-[#d6ecff] hover:bg-black/30"
                    >
                      Go to channel
                    </button>
                  </div>
                </div>

                <div className="mt-2 rounded-xl border border-white/15 bg-black/25 px-3 py-2.5">
                  <p className="text-[10px] uppercase tracking-wide text-[#a5d5f0]">Parent message</p>
                  <p className="mt-1 truncate text-sm text-[#e4f3ff]">
                    {(activeDiscussionChannelPost?.text
                      || activeDiscussionCommentContext.rootMessage?.text
                      || (activeDiscussionChannelPost?.media
                        ? `[${activeDiscussionChannelPost.media.type}]`
                        : (activeDiscussionCommentContext.rootMessage?.media
                          ? `[${activeDiscussionCommentContext.rootMessage.media.type}]`
                          : 'Channel post')))}
                  </p>
                </div>
              </div>
            </div>
          ) : null}

          {chatScopeTab === 'group' && (selectedGroup?.isForum || selectedGroup?.isDirectMessages) ? (
            <div className="shrink-0 border-b border-white/10 bg-[#0f2234]/90 px-3 py-2 backdrop-blur sm:px-4 lg:px-6">
              <div className="mx-auto w-full max-w-3xl">
                <div className="flex items-center gap-2 overflow-x-auto pb-1">
                  {selectedForumTopics
                    .filter((topic) => !topic.isHidden || topic.messageThreadId === activeMessageThreadId)
                    .filter((topic) => (
                      !selectedGroup?.isDirectMessages
                      || isSelectedUserDirectMessagesManager
                      || topic.messageThreadId === selectedUser.id
                    ))
                    .map((topic) => {
                      const isActive = topic.messageThreadId === activeMessageThreadId;
                      const badgeColor = topic.iconColor.toString(16).padStart(6, '0');
                      const isPremiumIcon = Boolean(topic.iconCustomEmojiId);
                      return (
                        <button
                          key={`forum-topic-tab-${topic.messageThreadId}`}
                          type="button"
                          onClick={() => selectForumTopicThread(topic.messageThreadId)}
                          onContextMenu={(event) => {
                            const canOpenTopicContextMenu = canManageForumTopics || Boolean(selectedGroup?.isDirectMessages);
                            if (!canOpenTopicContextMenu) {
                              return;
                            }
                            event.preventDefault();
                            event.stopPropagation();
                            setForumTopicContextMenu({
                              x: event.clientX,
                              y: event.clientY,
                              topic,
                            });
                          }}
                          className={`inline-flex shrink-0 items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs transition ${isActive ? 'border-[#8ad1ff]/70 bg-[#214865]/80 text-white' : 'border-white/15 bg-black/20 text-[#c8e4f6] hover:bg-white/10'}`}
                          title={`thread #${topic.messageThreadId}${canManageForumTopics || selectedGroup?.isDirectMessages ? ' (right-click for actions)' : ''}`}
                        >
                          <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: `#${badgeColor}` }} />
                          <span className="max-w-[180px] truncate">{topic.name}</span>
                          {isPremiumIcon ? <Star className="h-3 w-3 text-amber-200" /> : null}
                          {topic.isClosed ? <span className="text-[10px] text-amber-200">closed</span> : null}
                        </button>
                      );
                    })}
                  {canManageForumTopics ? (
                    <button
                      type="button"
                      onClick={onQuickCreateForumTopic}
                      className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-[#76b8e4]/50 bg-[#1a3f5a]/75 text-[#e2f3ff] hover:bg-[#225276]"
                      title="Create topic"
                    >
                      <Plus className="h-3.5 w-3.5" />
                    </button>
                  ) : null}
                </div>
                <div className="mt-1 flex flex-wrap items-center justify-between gap-2 text-[11px] text-[#9ac4df]">
                  <span>
                    active thread #{activeMessageThreadId || (selectedGroup?.isForum ? GENERAL_FORUM_TOPIC_THREAD_ID : '-')}
                  </span>
                  {selectedGroup?.isDirectMessages
                    ? (
                      isSelectedUserDirectMessagesManager
                        ? <span>Select a user topic to reply as the channel</span>
                        : <span>Your topic is created automatically when you send a message</span>
                    )
                    : (canManageForumTopics ? <span>right-click on a topic chip for quick actions</span> : null)}
                  {selectedGroup?.isDirectMessages && selectedDirectMessagesStarCost > 0 ? (
                    <span className="inline-flex items-center gap-1 text-amber-200">
                      <Star className="h-3 w-3" />
                      {isSelectedUserDirectMessagesManager
                        ? `Inbound DM cost: ${selectedDirectMessagesStarCost}⭐`
                        : `Cost: ${selectedDirectMessagesStarCost}⭐ per message · Wallet: ${walletState.stars}⭐`}
                    </span>
                  ) : null}
                </div>
              </div>
            </div>
          ) : null}

          {(chatScopeTab === 'group' || chatScopeTab === 'channel') && selectedGroup && selectedGroupJoinRequests.length > 0 ? (
            <div className="shrink-0 border-b border-white/10 bg-[#112738]/90 px-3 py-2 backdrop-blur sm:px-4 lg:px-6">
              <div className="mx-auto w-full max-w-3xl rounded-2xl border border-[#4d6f89]/45 bg-[#112738]/85 p-3 shadow-lg">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <p className="text-xs font-semibold uppercase tracking-wide text-[#9fd8ff]">
                    Pending join requests ({selectedGroupJoinRequests.length})
                  </p>
                  {!canEditSelectedGroup ? (
                    <span className="text-[11px] text-telegram-textSecondary">Visible to owner/admin for moderation</span>
                  ) : null}
                </div>
                <div className="space-y-2">
                  {selectedGroupJoinRequests.map((request) => (
                    <div
                      key={`join-request-inline-${request.userId}`}
                      className="rounded-xl border border-white/10 bg-black/25 px-3 py-2"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="min-w-0">
                          <p className="truncate text-sm text-white">
                            {request.firstName}{request.username ? ` (@${request.username})` : ''}
                          </p>
                          <p className="text-[11px] text-telegram-textSecondary">
                            user id: {request.userId}
                            {request.inviteLink ? ` | via ${request.inviteLink}` : ''}
                          </p>
                        </div>
                        {canEditSelectedGroup ? (
                          <div className="flex items-center gap-1.5">
                            <button
                              type="button"
                              onClick={() => void onApproveJoinRequest(request)}
                              className="rounded border border-emerald-300/45 bg-emerald-700/35 px-2.5 py-1 text-[11px] text-emerald-100 hover:bg-emerald-700/45"
                            >
                              Approve
                            </button>
                            <button
                              type="button"
                              onClick={() => void onDeclineJoinRequest(request)}
                              className="rounded border border-red-300/45 bg-red-700/30 px-2.5 py-1 text-[11px] text-red-100 hover:bg-red-700/40"
                            >
                              Decline
                            </button>
                          </div>
                        ) : null}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : null}

          {(chatScopeTab === 'group' || chatScopeTab === 'channel') && selectedGroup && selectedPinnedMessages.length > 0 ? (
            <div className="shrink-0 border-b border-white/10 bg-[#0f2231]/90 px-3 py-2 backdrop-blur sm:px-4 lg:px-6">
              <div className="mx-auto w-full max-w-3xl rounded-2xl border border-[#4d7390]/45 bg-[#112a3e]/90 p-2 shadow-lg">
                <div className="flex flex-wrap items-center gap-2 sm:flex-nowrap">
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-[#7db9de]/45 bg-[#143a55]/70">
                    <MapPinned className="h-4 w-4 text-[#9ed8ff]" />
                  </div>

                  <button
                    type="button"
                    onClick={() => {
                      if (activePinnedMessage) {
                        scrollToMessage(activePinnedMessage.id);
                      }
                    }}
                    className="min-w-0 flex-1 rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-left hover:bg-black/30"
                  >
                    <p className="truncate text-[10px] font-semibold uppercase tracking-wide text-[#9fd8ff]">
                      Pinned {Math.min(pinnedPreviewIndex + 1, selectedPinnedMessages.length)} / {selectedPinnedMessages.length}
                      {activePinnedMessage ? ` · #${activePinnedMessage.id}` : ''}
                    </p>
                    <p className="truncate text-sm text-[#def1ff]">
                      {activePinnedMessage
                        ? (activePinnedMessage.text || (activePinnedMessage.media ? `[${activePinnedMessage.media.type}]` : 'Pinned message'))
                        : 'Pinned message'}
                    </p>
                  </button>

                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      onClick={() => {
                        setPinnedPreviewIndex((prev) => {
                          if (selectedPinnedMessages.length === 0) {
                            return 0;
                          }
                          return (prev - 1 + selectedPinnedMessages.length) % selectedPinnedMessages.length;
                        });
                      }}
                      disabled={selectedPinnedMessages.length <= 1}
                      className="rounded-lg border border-white/20 bg-black/20 p-1.5 text-[#cbe7fa] hover:bg-white/10 disabled:opacity-40"
                      title="Previous pinned"
                    >
                      <ChevronLeft className="h-3.5 w-3.5" />
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        setPinnedPreviewIndex((prev) => {
                          if (selectedPinnedMessages.length === 0) {
                            return 0;
                          }
                          return (prev + 1) % selectedPinnedMessages.length;
                        });
                      }}
                      disabled={selectedPinnedMessages.length <= 1}
                      className="rounded-lg border border-white/20 bg-black/20 p-1.5 text-[#cbe7fa] hover:bg-white/10 disabled:opacity-40"
                      title="Next pinned"
                    >
                      <ChevronRight className="h-3.5 w-3.5" />
                    </button>
                  </div>

                  {canPinInSelectedGroup ? (
                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        onClick={() => void onUnpinMessageById(activePinnedMessage?.id)}
                        disabled={isGroupActionRunning || !activePinnedMessage}
                        className="rounded-lg border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        Unpin
                      </button>
                      <button
                        type="button"
                        onClick={() => void onUnpinMessageById()}
                        disabled={isGroupActionRunning}
                        className="rounded-lg border border-red-300/35 bg-red-900/20 px-2 py-1 text-[11px] text-red-100 hover:bg-red-900/30 disabled:opacity-40"
                      >
                        Unpin all
                      </button>
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          ) : null}

          <main
            ref={messagesContainerRef}
            onScroll={onMessagesScroll}
            className="relative min-w-0 flex-1 overflow-y-auto overflow-x-hidden bg-[url('/telegram-bg.svg')] bg-cover bg-center px-3 py-4 sm:px-4 sm:py-5 lg:px-6"
          >
            {!hasStarted ? (
              <div className="mx-auto mt-16 max-w-md rounded-2xl border border-white/15 bg-black/20 p-6 text-center shadow-2xl">
                <h3 className="mb-2 text-2xl font-semibold">No messages here yet</h3>
                <p className="mb-2 text-sm leading-6 text-telegram-textSecondary">
                  {chatScopeTab === 'private'
                    ? 'Tap Start in the bottom bar to begin this conversation exactly like Telegram private bot chat.'
                    : (chatScopeTab === 'channel'
                      ? 'Join this channel as current user to view posts. Publishing is limited to channel owner/admin.'
                      : 'Join this group as current user to send and receive messages.')}
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
                    const isOutgoingForSelected = isMessageOutgoingForSelected(message);
                    const senderBadges = chatScopeTab === 'group'
                      ? resolveGroupSenderBadges(message.fromUserId)
                      : {};
                    const commandTargetBotUsername = (chatScopeTab === 'group' || chatScopeTab === 'channel')
                      ? extractBotCommandTargetUsername(message.text, message.entities || message.captionEntities)
                      : null;
                    const isMediaOnly = Boolean(
                      message.media
                      && !message.text
                      && !message.poll
                      && !message.invoice
                      && !message.successfulPayment,
                    );
                    const linkedDiscussionSummary = chatScopeTab === 'channel'
                      ? linkedDiscussionCommentsByChannelMessageId.get(message.id)
                      : undefined;
                    const linkedDiscussionCommentsCount = linkedDiscussionSummary?.comments.length || 0;
                    return (
                      <div
                        key={message.id}
                        ref={(node) => { messageRefs.current[message.id] = node; }}
                        onContextMenu={(event) => onOpenMessageMenu(event, message.id)}
                        onClick={() => onMessageClick(message.id)}
                        onDoubleClick={() => onMessageDoubleClick(message.id)}
                        className={[
                          'relative min-w-0 overflow-hidden rounded-2xl px-3 py-3 shadow-lg sm:px-4',
                          message.service ? 'mx-auto max-w-[95%] sm:max-w-[70%] rounded-xl bg-black/30 text-center' : '',
                          isMediaOnly ? 'w-fit max-w-[90vw] sm:max-w-[340px]' : 'w-full max-w-[92%] sm:max-w-[84%] lg:max-w-[72%]',
                          selectionMode && selectedMessageIds.includes(message.id) ? 'ring-2 ring-[#87cbff]' : '',
                          highlightedMessageId === message.id ? 'ring-2 ring-[#f9e07f] shadow-[0_0_0_2px_rgba(249,224,127,0.35)]' : '',
                          message.service ? '' : (isOutgoingForSelected ? 'ml-auto rounded-br-md bg-[#2b5278]' : 'mr-auto rounded-bl-md bg-[#182533]'),
                        ].join(' ')}
                      >
                        {chatScopeTab === 'group' && !message.service && (!isOutgoingForSelected || commandTargetBotUsername) ? (
                          <div className={[
                            'mb-2 flex flex-wrap items-center gap-1.5 text-[11px] font-medium',
                            isOutgoingForSelected ? 'justify-end text-[#c6e5fb]' : 'text-[#9dd4ff]',
                          ].join(' ')}>
                            {!isOutgoingForSelected ? <span>{message.fromName}</span> : null}
                            {!isOutgoingForSelected && senderBadges.customTitle ? (
                              <span className="rounded border border-amber-300/35 bg-amber-900/25 px-1.5 py-0.5 text-[10px] text-amber-100">
                                {senderBadges.customTitle}
                              </span>
                            ) : null}
                            {!isOutgoingForSelected && senderBadges.tag ? (
                              <span className="rounded border border-sky-300/35 bg-sky-900/25 px-1.5 py-0.5 text-[10px] text-sky-100">
                                {senderBadges.tag}
                              </span>
                            ) : null}
                            {commandTargetBotUsername ? (
                              <span className="rounded border border-emerald-300/35 bg-emerald-900/25 px-1.5 py-0.5 text-[10px] text-emerald-100">
                                to @{commandTargetBotUsername}
                              </span>
                            ) : null}
                          </div>
                        ) : null}
                        {chatScopeTab === 'channel' && !message.service && selectedGroup?.settings?.showAuthorSignature ? (
                          <div className="mb-2 text-[11px] font-medium text-[#9dd4ff]">
                            {message.fromName}
                          </div>
                        ) : null}
                        {chatScopeTab === 'channel' && !message.service && commandTargetBotUsername ? (
                          <div className="mb-2 text-[11px] text-[#9dd4ff]">
                            to @{commandTargetBotUsername}
                          </div>
                        ) : null}
                        {!message.service && message.forwardedFrom ? (
                          <div className="mb-2 text-[11px] text-[#9dd4ff]">
                            Forwarded from {message.forwardedFrom}
                          </div>
                        ) : null}
                        {!message.service && chatScopeTab === 'channel' && activeChannelLinkedDiscussionChatId ? (
                          <div className="mb-2 rounded-lg border border-[#7fc6ff]/30 bg-[#102a3a]/55 px-2 py-1.5">
                            <div className="flex items-center gap-2">
                              <button
                                type="button"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  openLinkedDiscussionForChannelPost(message.id);
                                }}
                                className="min-w-0 flex-1 text-left"
                              >
                                <span className="inline-flex items-center gap-1 text-[11px] font-medium text-[#bde5ff] hover:text-[#d6efff]">
                                  <Reply className="h-3 w-3" />
                                  Comments
                                </span>
                                <span className="block truncate text-[10px] text-[#9ecae7]">
                                  {`${linkedDiscussionCommentsCount} comment${linkedDiscussionCommentsCount === 1 ? '' : 's'}`}
                                </span>
                              </button>
                            </div>
                          </div>
                        ) : null}
                        {!message.service && chatScopeTab === 'group' && message.linkedChannelMessageId && message.linkedChannelChatId ? (
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              openLinkedChannelPost(message.linkedChannelChatId!, message.linkedChannelMessageId);
                            }}
                            className="mb-2 rounded border border-[#7fc6ff]/35 bg-[#133145]/60 px-2 py-1 text-[11px] text-[#bde5ff] hover:bg-[#17405b]"
                          >
                            {`Discussion for channel post #${message.linkedChannelMessageId}`}
                          </button>
                        ) : null}
                        {message.replyTo ? (
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              scrollToMessage(message.replyTo!.messageId);
                            }}
                            className="mb-2 block w-full rounded-lg border-l-2 border-[#8ecbff] bg-black/20 px-2 py-1 text-left text-xs text-[#c7deee] hover:bg-black/30"
                          >
                            <div className="break-all font-medium text-[#9fd8ff]">Reply to {message.replyTo.fromName} #{message.replyTo.messageId}</div>
                            <div className="truncate">
                              {message.replyTo.text || (message.replyTo.hasMedia ? `[{message.replyTo.mediaType || 'media'}]` : 'message')}
                            </div>
                          </button>
                        ) : null}
                        {message.viaBotUsername ? (
                          <div className="mb-2 text-[11px] text-[#9dd4ff]">via @{message.viaBotUsername}</div>
                        ) : null}
                        {message.media ? <div className="mb-2">{renderMediaContent(message)}</div> : null}
                        {renderDiceCard(message)}
                        {renderGameCard(message)}
                        {renderContactCard(message)}
                        {renderLocationCard(message)}
                        {renderVenueCard(message)}
                        {renderInvoiceCard(message)}
                        {renderSuccessfulPaymentCard(message)}
                        {renderPollCard(message)}
                        {message.text ? (
                          <div className="text-sm leading-6 break-words whitespace-pre-wrap [overflow-wrap:anywhere]">{renderEntityText(message.text, message.entities || message.captionEntities)}</div>
                        ) : null}
                        {renderInlineKeyboard(message)}
                        {renderReactionChips(message)}
                        <div className="mt-1 flex items-center justify-end gap-2 text-[10px] text-[#a5bfd3]">
                          <span>#{message.id}</span>
                          {message.businessConnectionId ? (
                            <span>business</span>
                          ) : null}
                          {typeof message.paidMessageStarCount === 'number' && message.paidMessageStarCount > 0 ? (
                            <span className="inline-flex items-center gap-1 text-amber-200">
                              <Star className="h-3 w-3" />
                              {message.paidMessageStarCount}
                            </span>
                          ) : null}
                          {message.editDate && !message.isInlineOrigin ? <span>edited</span> : null}
                          {chatScopeTab === 'channel' && !message.service && typeof message.views === 'number' ? (
                            <span className="inline-flex items-center gap-1">
                              <Eye className="h-3 w-3" />
                              {message.views}
                            </span>
                          ) : null}
                          <span>{formatMessageTime(message.date)}</span>
                        </div>
                      </div>
                    );
                  }

                  const lead = block.messages[0];
                  const leadIsOutgoingForSelected = isMessageOutgoingForSelected(lead);
                  const leadSenderBadges = chatScopeTab === 'group'
                    ? resolveGroupSenderBadges(lead.fromUserId)
                    : {};
                  const leadCommandTargetBotUsername = (chatScopeTab === 'group' || chatScopeTab === 'channel')
                    ? extractBotCommandTargetUsername(lead.text, lead.entities || lead.captionEntities)
                    : null;
                  const leadLinkedDiscussionSummary = chatScopeTab === 'channel'
                    ? linkedDiscussionCommentsByChannelMessageId.get(lead.id)
                    : undefined;
                  const leadLinkedDiscussionCommentsCount = leadLinkedDiscussionSummary?.comments.length || 0;
                  return (
                    <div
                      key={`album-${block.mediaGroupId}-${lead.id}`}
                      ref={(node) => { messageRefs.current[lead.id] = node; }}
                      onContextMenu={(event) => onOpenMessageMenu(event, lead.id)}
                      onClick={() => onMessageClick(lead.id)}
                      onDoubleClick={() => onMessageDoubleClick(lead.id)}
                      className={[
                        'relative min-w-0 max-w-[92%] overflow-hidden rounded-2xl px-3 py-3 shadow-lg sm:max-w-[84%] lg:max-w-[72%]',
                        selectionMode && selectedMessageIds.includes(lead.id) ? 'ring-2 ring-[#87cbff]' : '',
                        highlightedMessageId === lead.id ? 'ring-2 ring-[#f9e07f] shadow-[0_0_0_2px_rgba(249,224,127,0.35)]' : '',
                        leadIsOutgoingForSelected ? 'ml-auto rounded-br-md bg-[#2b5278]' : 'mr-auto rounded-bl-md bg-[#182533]',
                      ].join(' ')}
                    >
                      {chatScopeTab === 'group' && (!leadIsOutgoingForSelected || leadCommandTargetBotUsername) ? (
                        <div className={[
                          'mb-2 flex flex-wrap items-center gap-1.5 text-[11px] font-medium',
                          leadIsOutgoingForSelected ? 'justify-end text-[#c6e5fb]' : 'text-[#9dd4ff]',
                        ].join(' ')}>
                          {!leadIsOutgoingForSelected ? <span>{lead.fromName}</span> : null}
                          {!leadIsOutgoingForSelected && leadSenderBadges.customTitle ? (
                            <span className="rounded border border-amber-300/35 bg-amber-900/25 px-1.5 py-0.5 text-[10px] text-amber-100">
                              {leadSenderBadges.customTitle}
                            </span>
                          ) : null}
                          {!leadIsOutgoingForSelected && leadSenderBadges.tag ? (
                            <span className="rounded border border-sky-300/35 bg-sky-900/25 px-1.5 py-0.5 text-[10px] text-sky-100">
                              {leadSenderBadges.tag}
                            </span>
                          ) : null}
                          {leadCommandTargetBotUsername ? (
                            <span className="rounded border border-emerald-300/35 bg-emerald-900/25 px-1.5 py-0.5 text-[10px] text-emerald-100">
                              to @{leadCommandTargetBotUsername}
                            </span>
                          ) : null}
                        </div>
                      ) : null}
                      {chatScopeTab === 'channel' && selectedGroup?.settings?.showAuthorSignature ? (
                        <div className="mb-2 text-[11px] font-medium text-[#9dd4ff]">
                          {lead.fromName}
                        </div>
                      ) : null}
                      {chatScopeTab === 'channel' && !lead.service && leadCommandTargetBotUsername ? (
                        <div className="mb-2 text-[11px] text-[#9dd4ff]">
                          to @{leadCommandTargetBotUsername}
                        </div>
                      ) : null}
                      {!lead.service && lead.forwardedFrom ? (
                        <div className="mb-2 text-[11px] text-[#9dd4ff]">
                          Forwarded from {lead.forwardedFrom}
                        </div>
                      ) : null}
                      {!lead.service && chatScopeTab === 'channel' && activeChannelLinkedDiscussionChatId ? (
                        <div className="mb-2 rounded-lg border border-[#7fc6ff]/30 bg-[#102a3a]/55 px-2 py-1.5">
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={(event) => {
                                event.stopPropagation();
                                openLinkedDiscussionForChannelPost(lead.id);
                              }}
                              className="min-w-0 flex-1 text-left"
                            >
                              <span className="inline-flex items-center gap-1 text-[11px] font-medium text-[#bde5ff] hover:text-[#d6efff]">
                                <Reply className="h-3 w-3" />
                                Comments
                              </span>
                              <span className="block truncate text-[10px] text-[#9ecae7]">
                                {`${leadLinkedDiscussionCommentsCount} comment${leadLinkedDiscussionCommentsCount === 1 ? '' : 's'}`}
                              </span>
                            </button>
                          </div>
                        </div>
                      ) : null}
                      {!lead.service && chatScopeTab === 'group' && lead.linkedChannelMessageId && lead.linkedChannelChatId ? (
                        <button
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation();
                            openLinkedChannelPost(lead.linkedChannelChatId!, lead.linkedChannelMessageId);
                          }}
                          className="mb-2 rounded border border-[#7fc6ff]/35 bg-[#133145]/60 px-2 py-1 text-[11px] text-[#bde5ff] hover:bg-[#17405b]"
                        >
                          {`Discussion for channel post #${lead.linkedChannelMessageId}`}
                        </button>
                      ) : null}
                      {lead.replyTo ? (
                        <button
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation();
                            scrollToMessage(lead.replyTo!.messageId);
                          }}
                          className="mb-2 block w-full rounded-lg border-l-2 border-[#8ecbff] bg-black/20 px-2 py-1 text-left text-xs text-[#c7deee] hover:bg-black/30"
                        >
                          <div className="break-all font-medium text-[#9fd8ff]">Reply to {lead.replyTo.fromName} #{lead.replyTo.messageId}</div>
                          <div className="truncate">
                            {lead.replyTo.text || (lead.replyTo.hasMedia ? `[{lead.replyTo.mediaType || 'media'}]` : 'message')}
                          </div>
                        </button>
                      ) : null}
                      {lead.viaBotUsername ? (
                        <div className="mb-2 text-[11px] text-[#9dd4ff]">via @{lead.viaBotUsername}</div>
                      ) : null}
                      <div className="mb-2 grid grid-cols-2 gap-2">
                        {block.messages.map((message) => (
                          <div
                            key={message.id}
                            onContextMenu={(event) => {
                              event.stopPropagation();
                              onOpenMessageMenu(event, message.id);
                            }}
                            className="overflow-hidden rounded-xl bg-black/20"
                          >
                            {renderMediaContent(message, true)}
                          </div>
                        ))}
                      </div>

                      {lead.text ? (
                        <div className="text-sm leading-6 break-words whitespace-pre-wrap [overflow-wrap:anywhere]">{renderEntityText(lead.text, lead.captionEntities)}</div>
                      ) : null}
                      {renderDiceCard(lead)}
                      {renderGameCard(lead)}
                      {renderContactCard(lead)}
                      {renderLocationCard(lead)}
                      {renderVenueCard(lead)}
                      {renderInvoiceCard(lead)}
                      {renderSuccessfulPaymentCard(lead)}
                      {renderPollCard(lead)}
                      {renderInlineKeyboard(lead)}
                      {renderReactionChips(lead)}
                      <div className="mt-1 flex items-center justify-end gap-2 text-[10px] text-[#a5bfd3]">
                        <span>Album {block.messages.length} items</span>
                        {lead.businessConnectionId ? (
                          <span>business</span>
                        ) : null}
                        {typeof lead.paidMessageStarCount === 'number' && lead.paidMessageStarCount > 0 ? (
                          <span className="inline-flex items-center gap-1 text-amber-200">
                            <Star className="h-3 w-3" />
                            {lead.paidMessageStarCount}
                          </span>
                        ) : null}
                        {lead.editDate && !lead.isInlineOrigin ? <span>edited</span> : null}
                        {chatScopeTab === 'channel' && !lead.service && typeof lead.views === 'number' ? (
                          <span className="inline-flex items-center gap-1">
                            <Eye className="h-3 w-3" />
                            {lead.views}
                          </span>
                        ) : null}
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

          <footer className="border-t border-white/10 px-3 py-4 sm:px-4 lg:px-5">
            {!hasStarted ? (
              <button
                type="button"
                onClick={() => {
                  if (chatScopeTab === 'private') {
                    void onStart();
                  } else {
                    void onJoinSelectedGroup();
                  }
                }}
                className="w-full rounded-xl bg-[#2b5278] px-4 py-3 text-sm font-semibold tracking-wide text-white transition hover:bg-[#366892]"
              >
                {chatScopeTab === 'private'
                  ? 'START'
                  : (chatScopeTab === 'channel' ? 'JOIN CHANNEL' : 'JOIN GROUP')}
              </button>
            ) : (
              <div className="space-y-2">
                {channelPostingRestrictionReason ? (
                  <div className="rounded-xl border border-amber-300/35 bg-amber-900/25 px-3 py-2 text-xs text-amber-100">
                    {channelPostingRestrictionReason}
                  </div>
                ) : null}
                {activeChatAction ? (
                  <div className="flex items-center justify-between rounded-xl border border-[#79b7de]/35 bg-[#123149]/70 px-3 py-2 text-xs text-[#d2ebff]">
                    <span className="truncate pr-2">
                      {activeChatAction.actorName} is {formatChatActionLabel(activeChatAction.action)}...
                    </span>
                    <span className="text-[10px] text-[#a9d2ed]">chat action</span>
                  </div>
                ) : null}
                {isDiscussionThreadView && activeDiscussionCommentContext ? (
                  <div className="flex items-center justify-between rounded-xl border border-[#79b7de]/35 bg-[#123149]/70 px-3 py-2 text-xs text-[#d2ebff]">
                    <span className="truncate pr-2">
                      Comment thread for channel post #{activeDiscussionCommentContext.channelMessageId}
                    </span>
                    <span className="text-[10px] text-[#a9d2ed]">{activeDiscussionCommentContext.commentsCount} comments</span>
                  </div>
                ) : null}
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
                <div className="space-y-2 rounded-2xl border border-white/10 bg-black/15 p-2">
                {showFormattingTools ? (
                  <div className="space-y-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 px-3 py-2">
                    <div className="flex items-center justify-between gap-2 rounded-xl bg-black/20 px-3 py-2">
                      <label htmlFor="parse-mode" className="text-[11px] text-telegram-textSecondary">Parse mode</label>
                      <select
                        id="parse-mode"
                        value={composerParseMode}
                        onChange={(event) => setComposerParseMode(event.target.value as ComposerParseMode)}
                        className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                      >
                        <option value="none">None</option>
                        <option value="MarkdownV2">MarkdownV2</option>
                        <option value="Markdown">Markdown</option>
                        <option value="HTML">HTML</option>
                      </select>
                    </div>
                    {composerText.trim() ? (
                      <div className="rounded-xl bg-black/20 px-3 py-2">
                        <p className="mb-1 text-[11px] text-telegram-textSecondary">Rich preview</p>
                        <div className="text-sm leading-6 break-words whitespace-pre-wrap">{renderEntityText(composerPreview.text, composerPreview.entities)}</div>
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {inlineTrigger ? (
                  <div className="rounded-xl border border-[#4f7ea6]/55 bg-[#102235]/90 px-3 py-2">
                    <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                      <p className="text-[11px] text-[#a9d9ff]">
                        Inline mode @{selectedBot?.username || 'bot'}
                        {inlineTrigger.query ? `: ${inlineTrigger.query}` : ''}
                      </p>
                      <div className="max-w-full break-all text-[10px] text-[#9ad8ff]">
                        {isInlineModeSending ? 'loading...' : (activeInlineQueryId ? `query id: ${activeInlineQueryId}` : 'awaiting query')}
                      </div>
                    </div>
                    {inlineModeError ? (
                      <p className="mb-2 text-[11px] text-amber-200">{inlineModeError}</p>
                    ) : null}
                    {inlineResults.length > 0 ? (
                      <div className="space-y-1">
                        {inlineResults.slice(0, 8).map((item, idx) => (
                          <button
                            key={`inline-result-${idx}`}
                            type="button"
                            onClick={() => void onChooseInlineResult(item)}
                            className="block w-full rounded-md border border-white/10 bg-black/25 px-2 py-1.5 text-left text-xs text-[#d5e9f9] transition hover:border-[#85cbff]/60 hover:bg-[#1b3852]"
                          >
                            <div className="font-medium text-white">{String(item.title || item.id || `result_${idx + 1}`)}</div>
                            {item.description ? (
                              <div className="text-[11px] text-telegram-textSecondary">{String(item.description)}</div>
                            ) : null}
                          </button>
                        ))}
                        {inlineNextOffset ? (
                          <button
                            type="button"
                            onClick={() => void onLoadMoreInlineResults()}
                            disabled={isInlineModeSending}
                            className="mt-1 block w-full rounded-md border border-[#85cbff]/40 bg-[#14314a] px-2 py-1.5 text-left text-xs text-[#d5e9f9] transition hover:bg-[#1b3f5d] disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {isInlineModeSending ? 'loading more...' : 'Load more inline results'}
                          </button>
                        ) : null}
                      </div>
                    ) : (
                      <p className="text-[11px] text-telegram-textSecondary">Bot should answer via answerInlineQuery to show selectable results.</p>
                    )}
                  </div>
                ) : null}
                <div className="flex items-end gap-2 sm:gap-3">
                  <div className="flex shrink-0 flex-col items-center gap-2 rounded-2xl border border-white/10 bg-black/25 p-1.5">
                    <button
                      type="button"
                      onClick={() => {
                        setShowMediaDrawer((prev) => {
                          const next = !prev;
                          if (next) {
                            setShowFormattingTools(false);
                          }
                          return next;
                        });
                      }}
                      disabled={!hasStarted}
                      className="shrink-0 rounded-full border border-white/10 bg-black/20 p-3 text-white transition hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                      title="Open media drawer"
                    >
                      <Smile className="h-5 w-5" />
                    </button>
                    <button
                      type="button"
                      onClick={() => fileInputRef.current?.click()}
                      disabled={!hasStarted || (isChannelScope && !canPostInSelectedChannel) || (!!composerEditTarget && !composerEditTarget?.media)}
                      className="shrink-0 rounded-full border border-white/10 bg-black/20 p-3 text-white transition hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                      title="Attach media"
                    >
                      <Paperclip className="h-5 w-5" />
                    </button>
                  </div>
                <form onSubmit={onSubmitComposer} className="flex min-w-0 flex-1 flex-col gap-2">
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
                  <div className="flex flex-wrap items-center gap-2">
                    <button
                      type="button"
                      onClick={() => {
                        setShowFormattingTools((prev) => !prev);
                      }}
                      className="rounded-md border border-[#2f4e66]/60 bg-[#163041]/70 px-3 py-1 text-[11px] text-[#cfe7f8] hover:bg-[#1f3f56]"
                    >
                      {showFormattingTools ? 'Hide formatting' : 'Show formatting'}
                    </button>
                  </div>
                  <div className="flex items-end gap-2 sm:gap-3">
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
                    disabled={!hasStarted || (isChannelScope && !canPostInSelectedChannel)}
                    rows={2}
                    className="min-h-[52px] max-h-[180px] min-w-0 flex-1 resize-none rounded-2xl border border-white/10 bg-black/25 px-4 py-3 text-sm leading-6 outline-none transition focus:border-telegram-lightBlue disabled:cursor-not-allowed disabled:opacity-60"
                    placeholder={channelPostingRestrictionReason
                      ? 'Only channel owner/admin can publish posts.'
                      : (composerEditTarget
                        ? 'Edit message...'
                        : (isDiscussionThreadView
                          ? 'Write a comment...'
                        : (activeReplyKeyboard?.markup.kind === 'reply'
                          ? (activeReplyKeyboard.markup.input_field_placeholder || 'Write a message...')
                            : 'Write a message...')))}
                  />
                  <button
                    type="submit"
                    disabled={!hasStarted || isSending || (isChannelScope && !canPostInSelectedChannel)}
                    className="shrink-0 rounded-full bg-telegram-blue p-3 text-white transition hover:bg-telegram-darkBlue disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <SendHorizonal className="h-5 w-5" />
                  </button>
                  </div>
                </form>
                </div>
                {showMediaDrawer ? (
                  <div className="rounded-2xl border border-white/15 bg-[#0f2334]/95 p-2">
                    <div className="mb-2 grid grid-cols-5 gap-1 sm:grid-cols-6 lg:grid-cols-12">
                      <button
                        type="button"
                        onClick={() => setMediaDrawerTab('stickers')}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'stickers' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Smile className="h-3.5 w-3.5" />Stickers</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => setMediaDrawerTab('animations')}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'animations' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Clapperboard className="h-3.5 w-3.5" />GIFs</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => setMediaDrawerTab('voice')}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'voice' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Mic className="h-3.5 w-3.5" />Voice</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => setMediaDrawerTab('video_note')}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'video_note' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Video className="h-3.5 w-3.5" />Video Note</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('studio');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'studio' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Wrench className="h-3.5 w-3.5" />Studio</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('dice');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'dice' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Dice5 className="h-3.5 w-3.5" />Dice</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('game');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'game' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Gamepad2 className="h-3.5 w-3.5" />Game</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('contact');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'contact' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Contact className="h-3.5 w-3.5" />Contact</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('location');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'location' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><MapPin className="h-3.5 w-3.5" />Location</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('venue');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'venue' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><MapPinned className="h-3.5 w-3.5" />Venue</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('poll');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'poll' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><ShieldCheck className="h-3.5 w-3.5" />Poll</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('invoice');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'invoice' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Wallet className="h-3.5 w-3.5" />Invoice</span>
                      </button>
                    </div>

                    <div className="max-h-[44vh] overflow-y-auto pr-1">
                      {mediaDrawerTab === 'stickers' ? (
                        <div className="space-y-2">
                          <p className="text-[11px] text-[#9fc6df]">Sticker sets are auto-discovered from conversation and kept updated.</p>
                          {stickerShelf.length > 0 ? (
                            <div className="flex flex-wrap gap-1">
                              {stickerShelf.map((set) => (
                                <button
                                  key={`sticker-set-${set.name}`}
                                  type="button"
                                  onClick={() => setStickerShelfActiveSet(set.name)}
                                  className={`rounded-full border px-2 py-0.5 text-[10px] ${stickerShelfActiveSet === set.name ? 'border-[#87cfff]/70 bg-[#2b5278]/70 text-white' : 'border-white/20 bg-black/20 text-[#d9efff]'}`}
                                >
                                  {set.title}
                                </button>
                              ))}
                            </div>
                          ) : null}

                          {activeStickerSet ? (
                            <div className="grid grid-cols-4 gap-2 sm:grid-cols-6 lg:grid-cols-8">
                              {activeStickerSet.stickers.map((sticker) => {
                                const mediaUrl = mediaUrlByFileId[sticker.file_id];
                                const maybeVideo = sticker.is_video || sticker.is_animated;
                                const previewFailed = Boolean(stickerPreviewFailedByFileId[sticker.file_id]);
                                return (
                                  <button
                                    key={`sticker-item-${sticker.file_id}`}
                                    type="button"
                                    onClick={() => void sendUserMediaByFileRef('sticker', sticker.file_id)}
                                    className="flex h-16 items-center justify-center rounded-lg border border-white/10 bg-black/20 p-1 hover:bg-white/10"
                                    title={sticker.emoji || sticker.file_id}
                                  >
                                    {mediaUrl && !previewFailed ? (
                                      maybeVideo ? (
                                        <video src={mediaUrl} autoPlay muted loop playsInline controls={false} className="h-12 w-12 object-contain" />
                                      ) : (
                                        <img
                                          src={mediaUrl}
                                          alt={sticker.emoji || 'sticker'}
                                          className="h-12 w-12 object-contain"
                                          onError={() => {
                                            setStickerPreviewFailedByFileId((prev) => ({ ...prev, [sticker.file_id]: true }));
                                          }}
                                        />
                                      )
                                    ) : (
                                      <span className="text-[10px] text-[#bcd7eb]">{sticker.emoji || (sticker.is_animated ? 'ANIM' : 'load')}</span>
                                    )}
                                  </button>
                                );
                              })}
                            </div>
                          ) : (
                            <p className="text-xs text-[#a6cbe4]">No discovered sticker set yet. Send/receive a sticker to populate this panel.</p>
                          )}
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'animations' ? (
                        <div className="space-y-2">
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
                            {animationGallery.map((item) => {
                              const mediaUrl = mediaUrlByFileId[item.fileId];
                              return (
                                <button
                                  key={`animation-gallery-${item.fileId}`}
                                  type="button"
                                  onClick={() => void sendUserMediaByFileRef('animation', item.fileId)}
                                  className="overflow-hidden rounded-lg border border-white/10 bg-black/20 text-left hover:bg-white/10"
                                >
                                  {mediaUrl ? <video src={mediaUrl} autoPlay muted loop playsInline controls={false} className="h-24 w-full object-cover" /> : <div className="h-24 bg-black/40" />}
                                  <div className="px-2 py-1 text-[10px] text-[#cde7f9]">from {item.from}</div>
                                </button>
                              );
                            })}
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <input
                              type="file"
                              accept="video/*,image/gif"
                              onChange={(event) => setAnimationUploadFile(event.target.files?.[0] || null)}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => void sendUserMediaFile(animationUploadFile, 'animation')}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]"
                            >
                              Send animation
                            </button>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'voice' ? (
                        <div className="space-y-2">
                          <p className="text-xs text-[#a6cbe4]">
                            {canUseMicrophone
                              ? 'Microphone detected. Upload fallback is always available.'
                              : 'No microphone detected. Upload fallback is enabled.'}
                          </p>
                          {canUseMicrophone ? (
                            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                              <button
                                type="button"
                                onClick={() => void startVoiceRecording()}
                                disabled={isRecordingVoice}
                                className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80] disabled:opacity-60"
                              >
                                Start recording
                              </button>
                              <button
                                type="button"
                                onClick={stopVoiceRecording}
                                disabled={!isRecordingVoice}
                                className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80] disabled:opacity-60"
                              >
                                Stop recording
                              </button>
                              <button
                                type="button"
                                onClick={() => void sendRecordedVoice()}
                                disabled={!recordedVoiceBlob}
                                className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80] disabled:opacity-60"
                              >
                                Send recorded voice
                              </button>
                            </div>
                          ) : null}
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <input
                              type="file"
                              accept="audio/*"
                              onChange={(event) => setVoiceUploadFile(event.target.files?.[0] || null)}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => void sendUserMediaFile(voiceUploadFile, 'voice')}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]"
                            >
                              Send voice file
                            </button>
                          </div>
                          {isRecordingVoice ? <p className="text-[11px] text-[#cfe7f8]">Recording in progress...</p> : null}
                          {voiceRecordError ? <p className="text-[11px] text-amber-200">{voiceRecordError}</p> : null}
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'video_note' ? (
                        <div className="space-y-2">
                          <p className="text-xs text-[#a6cbe4]">
                            {canUseCamera
                              ? 'Camera detected. Upload fallback is always available.'
                              : 'No camera detected. Upload fallback is enabled.'}
                          </p>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <input
                              type="file"
                              accept="video/*"
                              onChange={(event) => setVideoNoteUploadFile(event.target.files?.[0] || null)}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => void sendUserMediaFile(videoNoteUploadFile, 'video_note')}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]"
                            >
                              Send video note
                            </button>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'dice' ? (
                        <div className="space-y-2">
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[auto_1fr_auto]">
                            <select
                              value={selectedDiceEmoji}
                              onChange={(event) => setSelectedDiceEmoji(event.target.value as (typeof DICE_EMOJIS)[number])}
                              className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-xs text-white outline-none"
                            >
                              {DICE_EMOJIS.map((item) => (
                                <option key={`dice-emoji-${item}`} value={item}>{item}</option>
                              ))}
                            </select>
                            <p className="self-center text-xs text-[#a6cbe4]">Dice type</p>
                            <button
                              type="button"
                              onClick={() => {
                                void (async () => {
                                  if (!ensureActiveForumTopicWritable()) {
                                    return;
                                  }
                                  if (!ensureDirectMessagesStarsAvailable(1)) {
                                    return;
                                  }

                                  try {
                                    await sendUserDice(selectedBotToken, {
                                      chatId: selectedChatId,
                                      messageThreadId: outboundMessageThreadId,
                                      directMessagesTopicId: activeDirectMessagesTopicId,
                                      userId: selectedUser.id,
                                      firstName: selectedUser.first_name,
                                      username: selectedUser.username,
                                      senderChatId: activeDiscussionSenderChatId,
                                      emoji: selectedDiceEmoji,
                                    });
                                    consumeDirectMessagesStars(1);
                                  } catch (error) {
                                    setErrorText(error instanceof Error ? error.message : 'Send dice failed');
                                  }
                                })();
                              }}
                              disabled={!hasStarted || isSending}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80] disabled:opacity-60"
                            >
                              Send Dice
                            </button>
                          </div>

                        </div>
                      ) : null}

                      {mediaDrawerTab === 'game' ? (
                        <div className="space-y-2">

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={gameShortNameDraft}
                              onChange={(event) => setGameShortNameDraft(event.target.value)}
                              placeholder="game short name"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => {
                                void (async () => {
                                  if (!ensureActiveForumTopicWritable()) {
                                    return;
                                  }
                                  if (!ensureDirectMessagesStarsAvailable(1)) {
                                    return;
                                  }

                                  try {
                                    const shortName = gameShortNameDraft.trim() || `game_${Math.floor(Date.now() / 1000)}`;
                                    await sendUserGame(selectedBotToken, {
                                      chatId: selectedChatId,
                                      messageThreadId: outboundMessageThreadId,
                                      directMessagesTopicId: activeDirectMessagesTopicId,
                                      userId: selectedUser.id,
                                      firstName: selectedUser.first_name,
                                      username: selectedUser.username,
                                      senderChatId: activeDiscussionSenderChatId,
                                      gameShortName: shortName,
                                    });
                                    consumeDirectMessagesStars(1);
                                  } catch (error) {
                                    setErrorText(error instanceof Error ? error.message : 'Send game failed');
                                  }
                                })();
                              }}
                              disabled={!hasStarted || isSending}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80] disabled:opacity-60"
                            >
                              Send Game
                            </button>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'contact' ? (
                        <div className="space-y-2">
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_1fr_auto]">
                            <input
                              value={shareDraft.phoneNumber}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, phoneNumber: event.target.value }))}
                              placeholder="phone number"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <input
                              value={shareDraft.contactFirstName}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, contactFirstName: event.target.value }))}
                              placeholder="contact first name"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => {
                                void (async () => {
                                  if (!ensureActiveForumTopicWritable()) {
                                    return;
                                  }
                                  if (!ensureDirectMessagesStarsAvailable(1)) {
                                    return;
                                  }

                                  try {
                                    await sendUserContact(selectedBotToken, {
                                      chatId: selectedChatId,
                                      messageThreadId: outboundMessageThreadId,
                                      directMessagesTopicId: activeDirectMessagesTopicId,
                                      userId: selectedUser.id,
                                      firstName: selectedUser.first_name,
                                      username: selectedUser.username,
                                      senderChatId: activeDiscussionSenderChatId,
                                      phoneNumber: shareDraft.phoneNumber.trim() || '+10000000000',
                                      contactFirstName: shareDraft.contactFirstName.trim() || selectedUser.first_name,
                                      contactLastName: shareDraft.contactLastName.trim() || undefined,
                                    });
                                    consumeDirectMessagesStars(1);
                                  } catch (error) {
                                    setErrorText(error instanceof Error ? error.message : 'Send contact failed');
                                  }
                                })();
                              }}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]"
                            >
                              Send Contact
                            </button>
                          </div>

                        </div>
                      ) : null}

                      {mediaDrawerTab === 'location' ? (
                        <div className="space-y-2">

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_1fr_auto]">
                            <input
                              value={shareDraft.latitude}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, latitude: event.target.value }))}
                              placeholder="latitude"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <input
                              value={shareDraft.longitude}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, longitude: event.target.value }))}
                              placeholder="longitude"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => {
                                void (async () => {
                                  if (!ensureActiveForumTopicWritable()) {
                                    return;
                                  }
                                  if (!ensureDirectMessagesStarsAvailable(1)) {
                                    return;
                                  }

                                  try {
                                    await sendUserLocation(selectedBotToken, {
                                      chatId: selectedChatId,
                                      messageThreadId: outboundMessageThreadId,
                                      directMessagesTopicId: activeDirectMessagesTopicId,
                                      userId: selectedUser.id,
                                      firstName: selectedUser.first_name,
                                      username: selectedUser.username,
                                      senderChatId: activeDiscussionSenderChatId,
                                      latitude: Number(shareDraft.latitude) || 35.6892,
                                      longitude: Number(shareDraft.longitude) || 51.389,
                                    });
                                    consumeDirectMessagesStars(1);
                                  } catch (error) {
                                    setErrorText(error instanceof Error ? error.message : 'Send location failed');
                                  }
                                })();
                              }}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]"
                            >
                              Send Location
                            </button>
                          </div>

                        </div>
                      ) : null}

                      {mediaDrawerTab === 'venue' ? (
                        <div className="space-y-2">

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <input
                              value={shareDraft.venueTitle}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, venueTitle: event.target.value }))}
                              placeholder="venue title"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <input
                              value={shareDraft.venueAddress}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, venueAddress: event.target.value }))}
                              placeholder="venue address"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_1fr_auto]">
                            <input
                              value={shareDraft.latitude}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, latitude: event.target.value }))}
                              placeholder="lat"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <input
                              value={shareDraft.longitude}
                              onChange={(event) => setShareDraft((prev) => ({ ...prev, longitude: event.target.value }))}
                              placeholder="lon"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => {
                                void (async () => {
                                  if (!ensureActiveForumTopicWritable()) {
                                    return;
                                  }
                                  if (!ensureDirectMessagesStarsAvailable(1)) {
                                    return;
                                  }

                                  try {
                                    await sendUserVenue(selectedBotToken, {
                                      chatId: selectedChatId,
                                      messageThreadId: outboundMessageThreadId,
                                      directMessagesTopicId: activeDirectMessagesTopicId,
                                      userId: selectedUser.id,
                                      firstName: selectedUser.first_name,
                                      username: selectedUser.username,
                                      senderChatId: activeDiscussionSenderChatId,
                                      latitude: Number(shareDraft.latitude) || 35.6892,
                                      longitude: Number(shareDraft.longitude) || 51.389,
                                      title: shareDraft.venueTitle.trim() || 'Venue',
                                      address: shareDraft.venueAddress.trim() || 'Unknown address',
                                    });
                                    consumeDirectMessagesStars(1);
                                  } catch (error) {
                                    setErrorText(error instanceof Error ? error.message : 'Send venue failed');
                                  }
                                })();
                              }}
                              className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]"
                            >
                              Send Venue
                            </button>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'poll' ? (
                        <div className="space-y-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 px-3 py-2">
                          <input
                            value={pollBuilder.question}
                            onChange={(event) => setPollBuilder((prev) => ({ ...prev, question: event.target.value }))}
                            placeholder="Poll title/question"
                            className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                          />
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <select
                              value={pollBuilder.type}
                              onChange={(event) => setPollBuilder((prev) => ({
                                ...prev,
                                type: event.target.value as 'regular' | 'quiz',
                                correctOptionIds: event.target.value === 'quiz'
                                  ? (prev.correctOptionIds.length > 0 ? prev.correctOptionIds : [0])
                                  : prev.correctOptionIds,
                              }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            >
                              <option value="regular">Regular</option>
                              <option value="quiz">Quiz</option>
                            </select>
                            <select
                              value={pollBuilder.isAnonymous ? 'anonymous' : 'public'}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, isAnonymous: event.target.value === 'anonymous' }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            >
                              <option value="public">Public</option>
                              <option value="anonymous">Anonymous</option>
                            </select>
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <select
                              value={pollBuilder.questionParseMode}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, questionParseMode: event.target.value as ComposerParseMode }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            >
                              <option value="none">Question mode: None</option>
                              <option value="MarkdownV2">Question mode: MarkdownV2</option>
                              <option value="Markdown">Question mode: Markdown</option>
                              <option value="HTML">Question mode: HTML</option>
                            </select>
                            <select
                              value={pollBuilder.optionsParseMode}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, optionsParseMode: event.target.value as ComposerParseMode }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            >
                              <option value="none">Option mode: None</option>
                              <option value="MarkdownV2">Option mode: MarkdownV2</option>
                              <option value="Markdown">Option mode: Markdown</option>
                              <option value="HTML">Option mode: HTML</option>
                            </select>
                            <select
                              value={pollBuilder.explanationParseMode}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, explanationParseMode: event.target.value as ComposerParseMode }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            >
                              <option value="none">Explain mode: None</option>
                              <option value="MarkdownV2">Explain mode: MarkdownV2</option>
                              <option value="Markdown">Explain mode: Markdown</option>
                              <option value="HTML">Explain mode: HTML</option>
                            </select>
                            <select
                              value={pollBuilder.descriptionParseMode}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, descriptionParseMode: event.target.value as ComposerParseMode }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            >
                              <option value="none">Description mode: None</option>
                              <option value="MarkdownV2">Description mode: MarkdownV2</option>
                              <option value="Markdown">Description mode: Markdown</option>
                              <option value="HTML">Description mode: HTML</option>
                            </select>
                          </div>
                          <div className="space-y-1">
                            {pollBuilder.options.map((option, index) => (
                              <div key={`poll-builder-option-${index}`} className="flex items-center gap-1.5">
                                <input
                                  value={option}
                                  onChange={(event) => setPollBuilder((prev) => {
                                    const nextOptions = [...prev.options];
                                    nextOptions[index] = event.target.value;
                                    return { ...prev, options: nextOptions };
                                  })}
                                  placeholder={`Option ${index + 1}`}
                                  className="flex-1 rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                                />
                                {pollBuilder.type === 'quiz' ? (
                                  <button
                                    type="button"
                                    onClick={() => setPollBuilder((prev) => {
                                      const alreadySelected = prev.correctOptionIds.includes(index);
                                      if (prev.allowsMultipleAnswers) {
                                        const nextIds = alreadySelected
                                          ? prev.correctOptionIds.filter((id) => id !== index)
                                          : [...prev.correctOptionIds, index].sort((a, b) => a - b);
                                        return {
                                          ...prev,
                                          correctOptionIds: nextIds.length > 0 ? nextIds : [index],
                                        };
                                      }

                                      return {
                                        ...prev,
                                        correctOptionIds: [index],
                                      };
                                    })}
                                    className={`rounded-md border px-2 py-1 text-[11px] ${pollBuilder.correctOptionIds.includes(index) ? 'border-emerald-300/60 bg-emerald-700/35 text-emerald-100' : 'border-[#355a76]/60 bg-[#163041]/70 text-white'}`}
                                  >
                                    Correct
                                  </button>
                                ) : null}
                                {pollBuilder.options.length > 2 ? (
                                  <button
                                    type="button"
                                    onClick={() => setPollBuilder((prev) => {
                                      const nextOptions = prev.options.filter((_, i) => i !== index);
                                      const normalizedCorrectIds = prev.correctOptionIds
                                        .filter((id) => id !== index)
                                        .map((id) => (id > index ? id - 1 : id));
                                      return {
                                        ...prev,
                                        options: nextOptions,
                                        correctOptionIds: normalizedCorrectIds.length > 0
                                          ? normalizedCorrectIds
                                          : [0],
                                      };
                                    })}
                                    className="rounded-md border border-red-300/30 bg-red-600/30 px-2 py-1 text-[11px] text-red-100"
                                  >
                                    Remove
                                  </button>
                                ) : null}
                              </div>
                            ))}
                            {pollBuilder.options.length < 10 ? (
                              <button
                                type="button"
                                onClick={() => setPollBuilder((prev) => ({ ...prev, options: [...prev.options, ''] }))}
                                className="rounded-md border border-[#355a76]/60 bg-[#163041]/70 px-2 py-1 text-[11px] text-white hover:bg-[#1f3f56]"
                              >
                                Add option
                              </button>
                            ) : null}
                          </div>
                          {pollBuilder.type === 'quiz' ? (
                            <textarea
                              value={pollBuilder.explanation}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, explanation: event.target.value }))}
                              placeholder="Quiz explanation"
                              rows={2}
                              className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                          ) : null}
                          <textarea
                            value={pollBuilder.description}
                            onChange={(event) => setPollBuilder((prev) => ({ ...prev, description: event.target.value }))}
                            placeholder="Poll description"
                            rows={2}
                            className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                          />
                          <div className="grid grid-cols-2 gap-2">
                            <input
                              type="number"
                              min={5}
                              max={2628000}
                              value={pollBuilder.openPeriod}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, openPeriod: event.target.value, closeDate: event.target.value ? '' : prev.closeDate }))}
                              placeholder="open_period (sec)"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <input
                              type="datetime-local"
                              value={pollBuilder.closeDate}
                              onChange={(event) => setPollBuilder((prev) => ({ ...prev, closeDate: event.target.value, openPeriod: event.target.value ? '' : prev.openPeriod }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                          </div>
                          <div className="flex flex-wrap items-center gap-3 text-[11px] text-white">
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={pollBuilder.allowsMultipleAnswers}
                                onChange={(event) => setPollBuilder((prev) => ({ ...prev, allowsMultipleAnswers: event.target.checked }))}
                              />
                              {pollBuilder.type === 'quiz' ? 'Multiple correct options' : 'Multiple answers'}
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={pollBuilder.allowsRevoting}
                                onChange={(event) => setPollBuilder((prev) => ({ ...prev, allowsRevoting: event.target.checked }))}
                              />
                              Allow revoting
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={pollBuilder.isClosed}
                                onChange={(event) => setPollBuilder((prev) => ({ ...prev, isClosed: event.target.checked }))}
                              />
                              Send closed
                            </label>
                          </div>
                          <div className="flex items-center justify-end">
                            <button
                              type="button"
                              onClick={() => void submitPollBuilder()}
                              disabled={!hasStarted || isSending}
                              className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              Send Poll
                            </button>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'invoice' ? (
                        <div className="space-y-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 px-3 py-2">
                          <input
                            value={invoiceBuilder.title}
                            onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, title: event.target.value }))}
                            placeholder="Invoice title"
                            className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                          />
                          <textarea
                            value={invoiceBuilder.description}
                            onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, description: event.target.value }))}
                            placeholder="Invoice description"
                            rows={2}
                            className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                          />
                          <div className="grid grid-cols-3 gap-2">
                            <input
                              type="number"
                              min={1}
                              value={invoiceBuilder.amount}
                              onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, amount: event.target.value }))}
                              placeholder="Amount"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <input
                              value={invoiceBuilder.currency}
                              onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, currency: event.target.value.toUpperCase() }))}
                              placeholder="Currency"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <input
                              value={invoiceBuilder.payload}
                              onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, payload: event.target.value }))}
                              placeholder="Payload (optional)"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                            <input
                              type="number"
                              min={0}
                              value={invoiceBuilder.maxTipAmount}
                              onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, maxTipAmount: event.target.value }))}
                              placeholder="Max tip amount"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <input
                              value={invoiceBuilder.suggestedTips}
                              onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, suggestedTips: event.target.value }))}
                              placeholder="Suggested tips (e.g. 50,100)"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none sm:col-span-2"
                            />
                          </div>
                          <input
                            value={invoiceBuilder.photoUrl}
                            onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, photoUrl: event.target.value }))}
                            placeholder="Photo URL (optional)"
                            className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                          />
                          <div className="flex flex-wrap items-center gap-3 text-[11px] text-white">
                            {invoiceBuilder.currency.trim().toUpperCase() === 'XTR' ? (
                              <span className="rounded border border-[#7ec8fb]/40 bg-[#1f3f56]/70 px-2 py-1 text-[10px] text-[#cbe9ff]">
                                XTR invoice: shipping fields are disabled
                              </span>
                            ) : null}
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.needShippingAddress}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, needShippingAddress: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Need shipping
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.isFlexible}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, isFlexible: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Flexible shipping
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.needName}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, needName: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Need name
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.needPhoneNumber}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, needPhoneNumber: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Need phone
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.needEmail}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, needEmail: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Need email
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.sendPhoneNumberToProvider}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, sendPhoneNumberToProvider: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Send phone to provider
                            </label>
                            <label className="inline-flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={invoiceBuilder.sendEmailToProvider}
                                onChange={(event) => setInvoiceBuilder((prev) => ({ ...prev, sendEmailToProvider: event.target.checked }))}
                                disabled={invoiceBuilder.currency.trim().toUpperCase() === 'XTR'}
                              />
                              Send email to provider
                            </label>
                          </div>
                          <div className="flex items-center justify-end">
                            <button
                              type="button"
                              onClick={() => void submitInvoiceBuilder()}
                              disabled={!hasStarted || isSending}
                              className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              Send Invoice
                            </button>
                          </div>
                          <div className="mt-1 rounded-xl border border-[#355a76]/60 bg-black/20 px-3 py-2 text-xs text-[#d7ecfb]">
                            <div className="mb-2 inline-flex items-center gap-1 text-[11px] text-[#9fc6df]"><Wallet className="h-3.5 w-3.5" /> Payment Lab</div>
                            <div className="space-y-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="min-w-[80px] text-[11px] text-[#9fc6df]">Fiat</span>
                                <span className="rounded border border-white/20 bg-white/5 px-2 py-1">{walletState.fiat}</span>
                                <button type="button" onClick={() => setWalletState((prev) => ({ ...prev, fiat: Math.max(prev.fiat - 1000, 0) }))} className="min-w-[78px] rounded border border-white/20 bg-white/10 px-2 py-1 text-center text-[11px]">-1000</button>
                                <button type="button" onClick={() => setWalletState((prev) => ({ ...prev, fiat: prev.fiat + 1000 }))} className="min-w-[78px] rounded border border-white/20 bg-white/10 px-2 py-1 text-center text-[11px]">+1000</button>
                              </div>
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="min-w-[80px] text-[11px] text-[#9fc6df]">Stars</span>
                                <span className="rounded border border-white/20 bg-white/5 px-2 py-1">{walletState.stars}</span>
                                <button type="button" onClick={() => setWalletState((prev) => ({ ...prev, stars: Math.max(prev.stars - 200, 0) }))} className="min-w-[78px] rounded border border-white/20 bg-white/10 px-2 py-1 text-center text-[11px]">-200⭐</button>
                                <button type="button" onClick={() => setWalletState((prev) => ({ ...prev, stars: prev.stars + 200 }))} className="min-w-[78px] rounded border border-white/20 bg-white/10 px-2 py-1 text-center text-[11px]">+200⭐</button>
                              </div>
                            </div>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'studio' ? (
                        <div className="space-y-2 text-[11px] text-[#d7ecfb]">
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <input
                              value={stickerStudio.userId}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, userId: event.target.value }))}
                              placeholder="owner user id"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <select
                              value={stickerStudio.stickerType}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, stickerType: event.target.value }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            >
                              <option value="regular">regular</option>
                              <option value="mask">mask</option>
                              <option value="custom_emoji">custom_emoji</option>
                            </select>
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <select
                              value={stickerStudio.stickerFormat}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, stickerFormat: event.target.value }))}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            >
                              <option value="static">static</option>
                              <option value="animated">animated (.tgs)</option>
                              <option value="video">video (.webm)</option>
                            </select>
                            <input
                              value={stickerStudio.emojiList}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, emojiList: event.target.value }))}
                              placeholder="emoji list csv"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <input
                              value={stickerStudio.keywords}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, keywords: event.target.value }))}
                              placeholder="keywords csv (optional)"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            {stickerStudio.stickerType === 'custom_emoji' ? (
                              <label className="flex items-center gap-2 rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-[#d4e9f8]">
                                <input
                                  type="checkbox"
                                  checked={stickerStudio.needsRepainting}
                                  onChange={(event) => setStickerStudio((prev) => ({ ...prev, needsRepainting: event.target.checked }))}
                                />
                                Needs repainting
                              </label>
                            ) : (
                              <div className="rounded-md border border-[#355a76]/40 bg-black/20 px-2 py-1 text-[11px] text-[#9ec2da]">
                                Use mask controls when type is mask.
                              </div>
                            )}
                          </div>

                          {stickerStudio.stickerType === 'mask' ? (
                            <div className="grid grid-cols-1 gap-2 sm:grid-cols-4">
                              <select
                                value={stickerStudio.maskPoint}
                                onChange={(event) => setStickerStudio((prev) => ({ ...prev, maskPoint: event.target.value }))}
                                className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                              >
                                <option value="forehead">forehead</option>
                                <option value="eyes">eyes</option>
                                <option value="mouth">mouth</option>
                                <option value="chin">chin</option>
                              </select>
                              <input
                                value={stickerStudio.maskXShift}
                                onChange={(event) => setStickerStudio((prev) => ({ ...prev, maskXShift: event.target.value }))}
                                placeholder="x_shift"
                                className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                              />
                              <input
                                value={stickerStudio.maskYShift}
                                onChange={(event) => setStickerStudio((prev) => ({ ...prev, maskYShift: event.target.value }))}
                                placeholder="y_shift"
                                className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                              />
                              <input
                                value={stickerStudio.maskScale}
                                onChange={(event) => setStickerStudio((prev) => ({ ...prev, maskScale: event.target.value }))}
                                placeholder="scale"
                                className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                              />
                            </div>
                          ) : null}
                          <hr className="border-white/15" />

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={stickerStudio.setName}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, setName: event.target.value }))}
                              placeholder="set name"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void fetchStickerSetAction()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">getStickerSet</button>
                          </div>

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={stickerStudio.setTitle}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, setTitle: event.target.value }))}
                              placeholder="set title"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void applyStickerSetMetaActions()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">set title</button>
                          </div>

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              type="file"
                              onChange={(event) => setStickerStudioThumbnailFile(event.target.files?.[0] || null)}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void applyStickerSetMetaActions()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">set thumbnail</button>
                          </div>
                          <hr className="border-white/15" />

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              type="file"
                              onChange={(event) => setStickerStudioFile(event.target.files?.[0] || null)}
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void uploadStickerAsset()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">uploadStickerFile</button>
                          </div>
                          <div className="rounded-md border border-white/20 bg-black/25 px-2 py-1 text-[10px] text-[#cfe7f8] break-all">
                            uploaded file_id: {uploadedStickerFileId || '-'}
                          </div>

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <button type="button" onClick={() => void createStickerSetAction()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]">createNewStickerSet</button>
                            <button type="button" onClick={() => void addStickerToSetAction()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-xs text-white hover:bg-[#245a80]">addStickerToSet</button>
                          </div>
                          <hr className="border-white/15" />

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={stickerStudio.targetStickerId}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, targetStickerId: event.target.value }))}
                              placeholder="target sticker file_id"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void applyStickerItemActions()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">set emoji/meta</button>
                          </div>

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={stickerStudio.position}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, position: event.target.value }))}
                              placeholder="position"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void reorderOrReplaceStickerAction()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">set position</button>
                          </div>

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={stickerStudio.oldStickerId}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, oldStickerId: event.target.value }))}
                              placeholder="old sticker file_id"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void reorderOrReplaceStickerAction()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">replace</button>
                          </div>

                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto]">
                            <input
                              value={stickerStudio.customEmojiId}
                              onChange={(event) => setStickerStudio((prev) => ({ ...prev, customEmojiId: event.target.value }))}
                              placeholder="custom emoji ids csv"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                            />
                            <button type="button" onClick={() => void queryCustomEmojiStickersAction()} className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1 text-xs text-white hover:bg-[#245a80]">getCustomEmojiStickers</button>
                          </div>

                          <div className="flex justify-end">
                            <button type="button" onClick={() => void deleteStickerActions()} className="rounded-md border border-red-400/45 bg-red-700/35 px-3 py-1 text-xs text-red-100 hover:bg-red-700/45">delete sticker/set</button>
                          </div>
                          <div className="rounded border border-white/20 bg-black/25 px-2 py-1 text-[10px] text-[#d2ebfb]">
                            <pre className="max-h-32 overflow-auto whitespace-pre-wrap break-all">{stickerStudioOutput || 'Sticker Studio output...'}</pre>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>
                ) : null}
                </div>
                {activeReplyKeyboard && activeReplyKeyboard.markup.kind === 'reply' ? (
                  <div className={`rounded-2xl border border-white/15 bg-black/20 p-2 ${activeReplyKeyboard.markup.resize_keyboard ? '' : 'pb-3'}`}>
                    <div className="space-y-1.5 overflow-x-hidden">
                      {activeReplyKeyboard.markup.keyboard.map((row, rowIndex) => (
                        <div
                          key={`rk-row-${activeReplyKeyboard.sourceMessageId}-${rowIndex}`}
                          className="grid gap-1.5"
                          style={{ gridTemplateColumns: `repeat(${Math.max(row.length, 1)}, minmax(0, 1fr))` }}
                        >
                          {row.map((button, buttonIndex) => (
                            <button
                              key={`rk-btn-${activeReplyKeyboard.sourceMessageId}-${rowIndex}-${buttonIndex}`}
                              type="button"
                              onClick={() => void onReplyKeyboardButtonPress(button)}
                              className={`rounded-xl border px-3 py-2 text-sm transition ${keyboardButtonClass(button.style, false)}`}
                              title={button.text}
                            >
                              <span className="inline-flex items-center gap-1.5">
                                {button.icon_custom_emoji_id ? (
                                  <span className="tg-premium-emoji text-[14px] leading-none" title="Premium custom emoji icon">
                                    {premiumEmojiGlyph(button.icon_custom_emoji_id)}
                                  </span>
                                ) : null}
                                <span className="line-clamp-1 min-w-0 break-all">{button.text}</span>
                                {button.request_contact ? <span className="text-[11px] opacity-80">📱</span> : null}
                                {button.request_location ? <span className="text-[11px] opacity-80">📍</span> : null}
                                {button.request_poll ? <span className="text-[11px] opacity-80">📊</span> : null}
                                {button.web_app ? <span className="text-[11px] opacity-80">🗔</span> : null}
                              </span>
                            </button>
                          ))}
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
            )}

            {errorText ? <p className="mt-2 text-xs text-red-300">{errorText}</p> : null}
            {callbackToast ? (
              <p className="mt-2 rounded-lg border border-[#84cfff]/35 bg-[#1c3f5c]/70 px-2 py-1 text-xs text-[#d7eeff]">
                {callbackToast}
              </p>
            ) : null}
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
            className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl"
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
            className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl"
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

      {keyboardRequestUsersModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <div className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="min-w-0">
                <h3 className="truncate text-sm font-semibold text-white">Select users</h3>
                <p className="truncate text-xs text-[#9ec3dc]">{keyboardRequestUsersModal.buttonText}</p>
              </div>
              <button
                type="button"
                onClick={() => setKeyboardRequestUsersModal(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <p className="mb-3 text-xs text-[#d7ecfb]">
              {(() => {
                const maxQuantity = resolveRequestUsersMaxQuantity(
                  keyboardRequestUsersModal.request,
                  Math.min(10, keyboardRequestUsersModal.candidates.length),
                );
                return maxQuantity > 1
                  ? `Pick up to ${maxQuantity} users.`
                  : 'Pick one user.';
              })()}
            </p>

            <div className="max-h-72 space-y-1 overflow-y-auto rounded-xl border border-white/10 bg-black/20 p-2">
              {keyboardRequestUsersModal.candidates.map((candidate) => {
                const maxQuantity = resolveRequestUsersMaxQuantity(
                  keyboardRequestUsersModal.request,
                  Math.min(10, keyboardRequestUsersModal.candidates.length),
                );
                const isMulti = maxQuantity > 1;
                const checked = keyboardRequestUsersModal.selectedUserIds.includes(candidate.userId);
                return (
                  <label
                    key={`request-users-candidate-${candidate.userId}`}
                    className="flex cursor-pointer items-center gap-3 rounded-lg px-2 py-2 text-sm text-[#d7ecfb] hover:bg-white/10"
                  >
                    <input
                      type={isMulti ? 'checkbox' : 'radio'}
                      name="keyboard-request-users"
                      checked={checked}
                      onChange={(event) => {
                        const isChecked = event.target.checked;
                        setKeyboardRequestUsersModal((prev) => {
                          if (!prev) {
                            return prev;
                          }
                          const limit = resolveRequestUsersMaxQuantity(
                            prev.request,
                            Math.min(10, prev.candidates.length),
                          );
                          if (limit <= 1) {
                            return {
                              ...prev,
                              selectedUserIds: isChecked ? [candidate.userId] : [],
                            };
                          }

                          if (!isChecked) {
                            return {
                              ...prev,
                              selectedUserIds: prev.selectedUserIds.filter((id) => id !== candidate.userId),
                            };
                          }

                          const withoutCandidate = prev.selectedUserIds.filter((id) => id !== candidate.userId);
                          return {
                            ...prev,
                            selectedUserIds: [...withoutCandidate, candidate.userId].slice(-limit),
                          };
                        });
                      }}
                      className="h-4 w-4 rounded border-white/30 bg-transparent text-[#6ab8ef] focus:ring-[#6ab8ef]"
                    />
                    <span className="min-w-0 flex-1">
                      <span className="block truncate text-white">
                        {candidate.firstName}
                        {candidate.isBot ? ' (bot)' : ''}
                      </span>
                      <span className="block truncate text-[11px] text-[#9ec3dc]">
                        {candidate.username ? `@${candidate.username}` : `id ${candidate.userId}`}
                      </span>
                    </span>
                  </label>
                );
              })}
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => setKeyboardRequestUsersModal(null)}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void onSubmitRequestUsersModal()}
                disabled={isSending || keyboardRequestUsersModal.selectedUserIds.length === 0}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
              >
                Share users
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {keyboardRequestChatModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <div className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="min-w-0">
                <h3 className="truncate text-sm font-semibold text-white">Select chat</h3>
                <p className="truncate text-xs text-[#9ec3dc]">{keyboardRequestChatModal.buttonText}</p>
              </div>
              <button
                type="button"
                onClick={() => setKeyboardRequestChatModal(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="max-h-72 space-y-1 overflow-y-auto rounded-xl border border-white/10 bg-black/20 p-2">
              {keyboardRequestChatModal.candidates.map((candidate) => {
                const checked = keyboardRequestChatModal.selectedChatId === candidate.id;
                return (
                  <label
                    key={`request-chat-candidate-${candidate.id}`}
                    className="flex cursor-pointer items-center gap-3 rounded-lg px-2 py-2 text-sm text-[#d7ecfb] hover:bg-white/10"
                  >
                    <input
                      type="radio"
                      name="keyboard-request-chat"
                      checked={checked}
                      onChange={() => {
                        setKeyboardRequestChatModal((prev) => (prev ? { ...prev, selectedChatId: candidate.id } : prev));
                      }}
                      className="h-4 w-4 border-white/30 bg-transparent text-[#6ab8ef] focus:ring-[#6ab8ef]"
                    />
                    <span className="min-w-0 flex-1">
                      <span className="block truncate text-white">{candidate.title}</span>
                      <span className="block truncate text-[11px] text-[#9ec3dc]">
                        {candidate.username ? `@${candidate.username}` : `id ${candidate.id}`} · {candidate.type}
                      </span>
                    </span>
                  </label>
                );
              })}
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => setKeyboardRequestChatModal(null)}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void onSubmitRequestChatModal()}
                disabled={isSending || keyboardRequestChatModal.selectedChatId === null}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
              >
                Share chat
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {showGroupActionsModal && (chatScopeTab === 'group' || chatScopeTab === 'channel') && selectedGroup ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <div className="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div className="flex min-w-0 items-center gap-2">
                {groupSettingsPage !== 'home' ? (
                  <button
                    type="button"
                    onClick={() => {
                      setGroupSettingsPage('home');
                      setExpandedGroupMemberId(null);
                    }}
                    className="rounded-full p-1.5 text-white hover:bg-white/10"
                    title="Back"
                  >
                    <ArrowLeft className="h-4 w-4" />
                  </button>
                ) : null}
                <div className="min-w-0">
                  <h3 className="truncate text-lg font-semibold text-white">{groupSettingsTitle}</h3>
                  <p className="truncate text-xs text-[#99bfd9]">{selectedGroup.title} · {selectedGroup.id}</p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => {
                  setShowGroupActionsModal(false);
                  setGroupSettingsPage('home');
                  setExpandedGroupMemberId(null);
                }}
                className="rounded-full p-1 hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="mb-3 rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
              <p>your membership: {groupMembershipStatus || 'left'}</p>
              <p>bot status: {normalizedSelectedBotMembershipStatus || 'left'}</p>
              <p>pinned messages: {selectedPinnedMessageIds.length}</p>
              {isChannelScope ? (
                <p>publishing access: {canPostInSelectedChannel ? 'allowed' : 'read-only subscriber'}</p>
              ) : null}
            </div>

            {groupSettingsPage === 'home' ? (
              <div className="space-y-3">
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <button
                    type="button"
                    onClick={() => void onJoinSelectedGroup()}
                    disabled={isGroupActionRunning || groupMembership === 'joined'}
                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                  >
                    Join as {selectedUser.first_name}
                  </button>
                  <button
                    type="button"
                    onClick={() => void onLeaveSelectedGroup()}
                    disabled={isGroupActionRunning || !canLeaveSelectedGroup}
                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-orange-200 hover:bg-white/10 disabled:opacity-40"
                  >
                    Leave {isChannelScope ? 'channel' : 'group'}
                  </button>
                </div>

                <button
                  type="button"
                  onClick={onOpenGroupProfile}
                  disabled={!canEditSelectedGroup}
                  className="flex w-full items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-3 text-left text-sm text-white hover:bg-white/10 disabled:opacity-40"
                >
                  <span>{isChannelScope ? 'Channel info / edit' : 'Group info / edit'}</span>
                  <ChevronRight className="h-3.5 w-3.5 text-[#9ec5de]" />
                </button>

                <button
                  type="button"
                  onClick={() => setGroupSettingsPage('bot-membership')}
                  className="flex w-full items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-3 text-left text-sm text-white hover:bg-white/10"
                >
                  <span>{isChannelScope ? 'Bot posting access' : 'Bot membership'}</span>
                  <ChevronRight className="h-4 w-4 text-[#9ec5de]" />
                </button>

                <button
                  type="button"
                  onClick={() => setGroupSettingsPage('discovery')}
                  disabled={!canEditSelectedGroup}
                  className="flex w-full items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-3 text-left text-sm text-white hover:bg-white/10 disabled:opacity-40"
                >
                  <span>{isChannelScope ? 'Channel discovery & invite links' : 'Discovery & invite links'}</span>
                  <ChevronRight className="h-4 w-4 text-[#9ec5de]" />
                </button>

                <button
                  type="button"
                  onClick={() => setGroupSettingsPage('members')}
                  disabled={!canEditSelectedGroup}
                  className="flex w-full items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-3 text-left text-sm text-white hover:bg-white/10 disabled:opacity-40"
                >
                  <span>{isChannelScope ? 'Channel members management' : 'Members management'}</span>
                  <ChevronRight className="h-4 w-4 text-[#9ec5de]" />
                </button>

                {!isChannelScope ? (
                  <>
                    <button
                      type="button"
                      onClick={() => setGroupSettingsPage('topics')}
                      disabled={!canManageForumTopics}
                      className="flex w-full items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-3 text-left text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      <span>Forum topics</span>
                      <ChevronRight className="h-4 w-4 text-[#9ec5de]" />
                    </button>
                    <button
                      type="button"
                      onClick={() => setGroupSettingsPage('sender-chat')}
                      disabled={!canEditSelectedGroup}
                      className="flex w-full items-center justify-between rounded-xl border border-white/15 bg-black/20 px-3 py-3 text-left text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      <span>Sender chat moderation</span>
                      <ChevronRight className="h-4 w-4 text-[#9ec5de]" />
                    </button>
                  </>
                ) : null}

                <button
                  type="button"
                  onClick={() => setGroupSettingsPage('danger-zone')}
                  className="flex w-full items-center justify-between rounded-xl border border-red-400/35 bg-red-900/20 px-3 py-3 text-left text-sm text-red-100 hover:bg-red-900/30"
                >
                  <span>{isChannelScope ? 'Channel danger zone' : 'Danger zone'}</span>
                  <ChevronRight className="h-4 w-4 text-red-200" />
                </button>
              </div>
            ) : null}

            {groupSettingsPage === 'bot-membership' ? (
              <div className="space-y-3">
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                  <button
                    type="button"
                    onClick={() => void onSetSelectedBotMembership('member')}
                    disabled={isGroupActionRunning || !canEditSelectedGroup || !canSetSelectedBotAsMember}
                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                  >
                    Set member
                  </button>
                  <button
                    type="button"
                    onClick={() => void onSetSelectedBotMembership('admin')}
                    disabled={isGroupActionRunning || !canEditSelectedGroup || !canSetSelectedBotAsAdmin}
                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                  >
                    Set admin
                  </button>
                  <button
                    type="button"
                    onClick={() => void onSetSelectedBotMembership('left')}
                    disabled={isGroupActionRunning || !canEditSelectedGroup || !isSelectedBotInGroup}
                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-orange-200 hover:bg-white/10 disabled:opacity-40"
                  >
                    Remove bot
                  </button>
                </div>
                <button
                  type="button"
                  onClick={() => void onBotLeaveByApi()}
                  disabled={isGroupActionRunning || !canEditSelectedGroup || !isSelectedBotInGroup}
                  className="w-full rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-orange-100 hover:bg-white/10 disabled:opacity-40"
                >
                  Bot leaveChat
                </button>
              </div>
            ) : null}

            {groupSettingsPage === 'discovery' ? (
              <div className="space-y-3">
                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">{isChannelScope ? 'Channel inspector' : 'Chat inspector'}</p>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                    <button
                      type="button"
                      onClick={() => void onInspectSelectedGroupChat()}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      getChat
                    </button>
                    <button
                      type="button"
                      onClick={() => void onInspectSelectedGroupAdmins()}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      getChatAdministrators
                    </button>
                    <button
                      type="button"
                      onClick={() => void onInspectSelectedGroupMemberCount()}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      getChatMemberCount
                    </button>
                  </div>
                </div>

                {isChannelScope ? (
                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Linked discussion group</p>
                    <p className="mb-2 text-[11px] text-[#aacce2]">
                      Link this channel to a discussion group/supergroup to enable Telegram-style comments under channel posts.
                    </p>

                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <select
                        value={channelDiscussionLinkDraft}
                        onChange={(event) => setChannelDiscussionLinkDraft(event.target.value)}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      >
                        <option value="">No linked discussion</option>
                        {selectedGroup?.linkedDiscussionChatId && !channelDiscussionCandidates.some((chat) => chat.id === selectedGroup.linkedDiscussionChatId) ? (
                          <option value={String(selectedGroup.linkedDiscussionChatId)}>
                            {`chat ${selectedGroup.linkedDiscussionChatId}`}
                          </option>
                        ) : null}
                        {channelDiscussionCandidates.map((chat) => (
                          <option key={`discussion-candidate-${chat.id}`} value={String(chat.id)}>
                            {`${chat.title} (${chat.type})`}
                          </option>
                        ))}
                      </select>

                      <input
                        value={channelDiscussionLinkDraft}
                        onChange={(event) => setChannelDiscussionLinkDraft(event.target.value)}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                        placeholder="discussion chat_id (empty = unlink)"
                      />
                    </div>

                    <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => void onApplyChannelDiscussionLink()}
                        disabled={isGroupActionRunning || !canEditSelectedGroup}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        Apply linked discussion
                      </button>
                      <button
                        type="button"
                        onClick={() => setChannelDiscussionLinkDraft('')}
                        disabled={isGroupActionRunning || !canEditSelectedGroup}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        Clear draft
                      </button>
                    </div>
                  </div>
                ) : null}

                {isChannelScope ? null : (
                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Privacy mode & menu button</p>

                    <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-3">
                      <div className="rounded-lg border border-white/10 bg-[#0f1c28] px-3 py-2 text-xs text-telegram-textSecondary sm:col-span-2">
                        Privacy mode: {groupPrivacyModeEnabled ? 'enabled (commands/mentions/replies only)' : 'disabled (all group messages)'}
                      </div>
                      <button
                        type="button"
                        onClick={() => void onToggleGroupPrivacyMode()}
                        disabled={isGroupPrivacyModeLoading}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        {isGroupPrivacyModeLoading
                          ? 'Updating...'
                          : (groupPrivacyModeEnabled ? 'Disable privacy mode' : 'Enable privacy mode')}
                      </button>
                    </div>

                    <div className="mt-3 grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <select
                        value={groupMenuButtonDraft.scope}
                        onChange={(event) => {
                          const scope = event.target.value === 'private-chat' ? 'private-chat' : 'default';
                          setGroupMenuButtonDraft((prev) => ({ ...prev, scope }));
                        }}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      >
                        <option value="default">default scope (all private chats)</option>
                        <option value="private-chat">specific private chat</option>
                      </select>
                      <input
                        value={groupMenuButtonDraft.targetChatId}
                        onChange={(event) => setGroupMenuButtonDraft((prev) => ({ ...prev, targetChatId: event.target.value }))}
                        disabled={groupMenuButtonDraft.scope === 'default'}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none disabled:opacity-40"
                        placeholder={`chat_id (default ${selectedUser.id})`}
                      />
                      <select
                        value={groupMenuButtonDraft.type}
                        onChange={(event) => {
                          const rawType = event.target.value;
                          const type = rawType === 'default' || rawType === 'web_app' || rawType === 'commands'
                            ? rawType
                            : 'commands';
                          setGroupMenuButtonDraft((prev) => ({ ...prev, type }));
                        }}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      >
                        <option value="commands">commands</option>
                        <option value="web_app">web_app</option>
                        <option value="default">default</option>
                      </select>
                      <div className="rounded-lg border border-white/10 bg-[#0f1c28] px-3 py-2 text-xs text-telegram-textSecondary">
                        Current summary: {groupMenuButtonSummary
                          ? `${groupMenuButtonSummary.type}${groupMenuButtonSummary.text ? ` · ${groupMenuButtonSummary.text}` : ''}${groupMenuButtonSummary.url ? ` · ${groupMenuButtonSummary.url}` : ''}`
                          : 'not loaded yet'}
                      </div>
                    </div>

                    {groupMenuButtonDraft.type === 'web_app' ? (
                      <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
                        <input
                          value={groupMenuButtonDraft.text}
                          onChange={(event) => setGroupMenuButtonDraft((prev) => ({ ...prev, text: event.target.value }))}
                          className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                          placeholder="web_app text"
                        />
                        <input
                          value={groupMenuButtonDraft.webAppUrl}
                          onChange={(event) => setGroupMenuButtonDraft((prev) => ({ ...prev, webAppUrl: event.target.value }))}
                          className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                          placeholder="https://example.com"
                        />
                      </div>
                    ) : null}

                    <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => void onSetGroupMenuButtonFromDraft()}
                        disabled={isGroupActionRunning}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        setChatMenuButton
                      </button>
                      <button
                        type="button"
                        onClick={() => void onGetGroupMenuButtonFromDraft()}
                        disabled={isGroupActionRunning}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        getChatMenuButton
                      </button>
                    </div>
                  </div>
                )}

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Invite links</p>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                    <button
                      type="button"
                      onClick={() => void onExportPrimaryInviteLink()}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      exportChatInviteLink
                    </button>
                    <button
                      type="button"
                      onClick={() => void onCreateGroupInviteLink(false)}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      createChatInviteLink
                    </button>
                    <button
                      type="button"
                      onClick={() => void onCreateGroupInviteLink(true)}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      createJoinRequestLink
                    </button>
                  </div>

                  <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
                    <input
                      value={groupInviteEditorDraft.inviteLink}
                      onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, inviteLink: event.target.value }))}
                      className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      placeholder="invite_link"
                    />
                    <input
                      value={groupInviteEditorDraft.name}
                      onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, name: event.target.value }))}
                      className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      placeholder="name"
                    />
                    <input
                      value={groupInviteEditorDraft.expireDate}
                      onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, expireDate: event.target.value }))}
                      className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      placeholder="expire_date (unix)"
                    />
                    <input
                      value={groupInviteEditorDraft.memberLimit}
                      onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, memberLimit: event.target.value }))}
                      className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      placeholder="member_limit"
                    />
                    <label className="flex items-center gap-2 rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs text-telegram-textSecondary sm:col-span-2">
                      <input
                        type="checkbox"
                        checked={groupInviteEditorDraft.createsJoinRequest}
                        onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, createsJoinRequest: event.target.checked }))}
                      />
                      creates_join_request
                    </label>
                  </div>

                  <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
                    <button
                      type="button"
                      onClick={() => void onEditInviteLinkByDraft()}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      editChatInviteLink
                    </button>
                    <button
                      type="button"
                      onClick={() => void onRevokeInviteLinkByDraft()}
                      disabled={isGroupActionRunning || !canEditSelectedGroup}
                      className="rounded-lg border border-red-400/35 bg-red-900/25 px-3 py-2 text-sm text-red-100 hover:bg-red-900/35 disabled:opacity-40"
                    >
                      revokeChatInviteLink
                    </button>
                  </div>
                </div>

                {isChannelScope ? (
                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Subscription invite links</p>
                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <input
                        value={groupInviteEditorDraft.subscriptionPeriod}
                        onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, subscriptionPeriod: event.target.value }))}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                        placeholder="subscription_period"
                      />
                      <input
                        value={groupInviteEditorDraft.subscriptionPrice}
                        onChange={(event) => setGroupInviteEditorDraft((prev) => ({ ...prev, subscriptionPrice: event.target.value }))}
                        className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                        placeholder="subscription_price"
                      />
                      <button
                        type="button"
                        onClick={() => void onCreateSubscriptionInviteLinkByDraft()}
                        disabled={isGroupActionRunning || !canEditSelectedGroup}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        createChatSubscriptionInviteLink
                      </button>
                      <button
                        type="button"
                        onClick={() => void onEditSubscriptionInviteLinkByDraft()}
                        disabled={isGroupActionRunning || !canEditSelectedGroup}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        editChatSubscriptionInviteLink
                      </button>
                    </div>
                  </div>
                ) : null}

                {selectedGroupInviteLink ? (
                  <button
                    type="button"
                    onClick={async () => {
                      try {
                        await navigator.clipboard.writeText(selectedGroupInviteLink);
                        setErrorText('Invite link copied.');
                      } catch {
                        setErrorText('Invite link copy failed.');
                      }
                    }}
                    className="w-full truncate rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-left text-[11px] text-[#bfe4ff] hover:bg-[#14283a]"
                    title={selectedGroupInviteLink}
                  >
                    Latest invite: {selectedGroupInviteLink}
                  </button>
                ) : null}

                {groupInspectorOutput ? (
                  <div className="rounded-lg border border-white/10 bg-black/20 px-3 py-2">
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <p className="text-xs uppercase tracking-wide text-[#8fb7d6]">Inspector output</p>
                      <button
                        type="button"
                        onClick={() => setGroupInspectorOutput('')}
                        className="rounded-full p-1 text-[#8fb7d6] hover:bg-white/10 hover:text-white"
                        title="Close inspector output"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </div>
                    <pre className="max-h-52 overflow-auto whitespace-pre-wrap break-words rounded-md border border-white/10 bg-[#0f1a26] p-2 text-[11px] text-[#c5e5fb]">
                      {groupInspectorOutput}
                    </pre>
                  </div>
                ) : null}
              </div>
            ) : null}

            {chatScopeTab === 'group' && groupSettingsPage === 'topics' ? (
              <div className="space-y-3">
                {!selectedGroup.isForum ? (
                  <div className="rounded-xl border border-amber-300/35 bg-amber-900/25 px-3 py-2 text-sm text-amber-100">
                    This supergroup is not configured as a forum. Enable forum topics in Group profile first.
                  </div>
                ) : null}

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-xs uppercase tracking-wide text-[#8fb7d6]">Topic creation & icon presets</p>
                      <p className="mt-1 text-[11px] text-[#aacce2]">Use the + button in chat header or create from this panel.</p>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={onQuickCreateForumTopic}
                        disabled={isGroupActionRunning || !canManageForumTopics}
                        className="rounded-lg border border-[#7cbfe9]/35 bg-[#153349] px-3 py-2 text-sm text-[#d2eeff] hover:bg-[#1b425d] disabled:opacity-40"
                      >
                        + New topic
                      </button>
                      <button
                        type="button"
                        onClick={() => void onLoadForumTopicIconStickers()}
                        disabled={isGroupActionRunning || !canManageForumTopics}
                        className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                      >
                        Load premium icons
                      </button>
                    </div>
                  </div>

                  <div className="mt-3 grid grid-cols-4 gap-2 sm:grid-cols-8">
                    {FORUM_ICON_COLOR_PRESETS.map((color) => {
                      const hex = color.toString(16).padStart(6, '0');
                      const isActiveColor = Math.floor(Number(forumTopicDraft.iconColor)) === color;
                      return (
                        <button
                          key={`forum-color-preset-${color}`}
                          type="button"
                          onClick={() => setForumTopicDraft((prev) => ({
                            ...prev,
                            iconColor: String(color),
                            iconCustomEmojiId: '',
                          }))}
                          className={`h-7 rounded-md border ${isActiveColor ? 'border-white/80' : 'border-white/20'} transition hover:scale-[1.03]`}
                          style={{ backgroundColor: `#${hex}` }}
                          title={`icon_color ${color}`}
                        />
                      );
                    })}
                  </div>

                  <div className="mt-3">
                    <label className="mb-1 block text-[11px] text-[#9ec2da]">Premium icon (custom emoji id)</label>
                    <input
                      value={forumTopicDraft.iconCustomEmojiId}
                      onChange={(event) => setForumTopicDraft((prev) => ({
                        ...prev,
                        iconCustomEmojiId: event.target.value,
                        normalEmoji: event.target.value.trim() ? '' : prev.normalEmoji,
                      }))}
                      className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      placeholder="icon_custom_emoji_id"
                    />
                  </div>

                  {forumTopicIconStickers.length > 0 ? (
                    <div className="mt-3 grid max-h-44 grid-cols-1 gap-2 overflow-auto pr-1 sm:grid-cols-2">
                      {forumTopicIconStickers.map((sticker) => {
                        const selected = Boolean(
                          sticker.custom_emoji_id
                          && sticker.custom_emoji_id === forumTopicDraft.iconCustomEmojiId,
                        );

                        return (
                          <button
                            key={sticker.file_id}
                            type="button"
                            onClick={() => setForumTopicDraft((prev) => ({
                              ...prev,
                              iconCustomEmojiId: sticker.custom_emoji_id || prev.iconCustomEmojiId,
                              normalEmoji: '',
                            }))}
                            className={`rounded-lg border px-3 py-2 text-left text-xs ${selected ? 'border-[#8dd2ff]/70 bg-[#1f4868]/75 text-white' : 'border-white/15 bg-[#0f1c28] text-[#d7ebfb] hover:bg-[#162b3d]'}`}
                            title={sticker.custom_emoji_id || sticker.file_id}
                          >
                            <div className="truncate">{sticker.emoji || 'premium icon'}</div>
                            <div className="truncate text-[10px] text-[#9cc5df]">{sticker.custom_emoji_id || sticker.file_id}</div>
                          </button>
                        );
                      })}
                    </div>
                  ) : null}
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <p className="text-xs uppercase tracking-wide text-[#8fb7d6]">Existing topics</p>
                    <span className="rounded-full border border-white/15 px-2 py-0.5 text-[11px] text-[#c7e3f6]">
                      {selectedForumTopics.length} topics
                    </span>
                  </div>

                  {selectedForumTopics.length === 0 ? (
                    <p className="rounded-lg border border-white/10 bg-[#0f1c28] px-3 py-2 text-sm text-[#a8c8de]">
                      No forum topics available yet.
                    </p>
                  ) : (
                    <div className="max-h-[52vh] space-y-2 overflow-auto pr-1">
                      {selectedForumTopics.map((topic) => {
                        const isExpanded = expandedForumTopicThreadId === topic.messageThreadId;
                        const isActive = topic.messageThreadId === activeMessageThreadId;
                        const colorHex = topic.iconColor.toString(16).padStart(6, '0');

                        return (
                          <div key={`group-topic-row-${topic.messageThreadId}`} className="rounded-xl border border-white/10 bg-[#0f1d2b]/70 p-3">
                            <button
                              type="button"
                              onClick={() => setExpandedForumTopicThreadId((prev) => (
                                prev === topic.messageThreadId ? null : topic.messageThreadId
                              ))}
                              className="flex w-full items-center justify-between text-left"
                            >
                              <div className="min-w-0">
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: `#${colorHex}` }} />
                                  <p className="truncate text-sm font-medium text-white">{topic.name}</p>
                                  {topic.iconCustomEmojiId ? (
                                    <span className="rounded border border-amber-300/35 bg-amber-900/20 px-1.5 py-0.5 text-[10px] text-amber-100">premium icon</span>
                                  ) : (
                                    <span className="rounded border border-white/15 bg-black/20 px-1.5 py-0.5 text-[10px] text-[#c6def0]">default icon</span>
                                  )}
                                  {topic.isGeneral ? <span className="rounded border border-sky-300/35 bg-sky-900/20 px-1.5 py-0.5 text-[10px] text-sky-100">general</span> : null}
                                  {topic.isClosed ? <span className="rounded border border-orange-300/35 bg-orange-900/20 px-1.5 py-0.5 text-[10px] text-orange-100">closed</span> : null}
                                  {topic.isHidden ? <span className="rounded border border-white/15 bg-black/20 px-1.5 py-0.5 text-[10px] text-[#c6def0]">hidden</span> : null}
                                  {isActive ? <span className="rounded border border-emerald-300/35 bg-emerald-900/20 px-1.5 py-0.5 text-[10px] text-emerald-100">active in chat</span> : null}
                                </div>
                                <p className="mt-1 truncate text-xs text-[#9dbfd7]">
                                  thread #{topic.messageThreadId}
                                  {topic.iconCustomEmojiId ? ` · ${topic.iconCustomEmojiId}` : ''}
                                </p>
                              </div>
                              <ChevronDown className={`h-4 w-4 text-[#9cc4de] transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                            </button>

                            {isExpanded ? (
                              <div className="mt-3 grid grid-cols-1 gap-2 border-t border-white/10 pt-3 sm:grid-cols-2">
                                <button
                                  type="button"
                                  onClick={() => selectForumTopicThread(topic.messageThreadId)}
                                  className="rounded-lg border border-[#7cbfe9]/35 bg-[#153349] px-3 py-2 text-sm text-[#d2eeff] hover:bg-[#1b425d]"
                                >
                                  Open in chat
                                </button>
                                <button
                                  type="button"
                                  onClick={() => openEditForumTopicModal(topic)}
                                  disabled={isGroupActionRunning || !canManageForumTopics}
                                  className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                                >
                                  Edit topic
                                </button>
                                <button
                                  type="button"
                                  onClick={() => void (topic.isClosed
                                    ? (topic.isGeneral ? onReopenGeneralForumTopic() : onReopenForumTopicFromDraft(topic.messageThreadId))
                                    : (topic.isGeneral ? onCloseGeneralForumTopic() : onCloseForumTopicFromDraft(topic.messageThreadId)))}
                                  disabled={isGroupActionRunning || !canManageForumTopics}
                                  className={`rounded-lg border px-3 py-2 text-sm disabled:opacity-40 ${topic.isClosed ? 'border-emerald-300/35 bg-emerald-900/20 text-emerald-100 hover:bg-emerald-900/30' : 'border-orange-300/35 bg-orange-900/20 text-orange-100 hover:bg-orange-900/30'}`}
                                >
                                  {topic.isClosed ? 'Reopen topic' : 'Close topic'}
                                </button>
                                <button
                                  type="button"
                                  onClick={() => void (topic.isGeneral
                                    ? onUnpinAllGeneralForumTopicMessages()
                                    : onUnpinAllForumTopicMessagesFromDraft(topic.messageThreadId))}
                                  disabled={isGroupActionRunning || !canManageForumTopics}
                                  className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                                >
                                  Unpin all messages
                                </button>
                                {topic.isGeneral ? (
                                  <button
                                    type="button"
                                    onClick={() => void (topic.isHidden ? onUnhideGeneralForumTopic() : onHideGeneralForumTopic())}
                                    disabled={isGroupActionRunning || !canManageForumTopics}
                                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                                  >
                                    {topic.isHidden ? 'Unhide general topic' : 'Hide general topic'}
                                  </button>
                                ) : (
                                  <button
                                    type="button"
                                    onClick={() => void onDeleteForumTopicFromDraft(topic.messageThreadId)}
                                    disabled={isGroupActionRunning || !canManageForumTopics}
                                    className="rounded-lg border border-red-300/35 bg-red-900/20 px-3 py-2 text-sm text-red-100 hover:bg-red-900/30 disabled:opacity-40"
                                  >
                                    Delete topic
                                  </button>
                                )}
                              </div>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>

                {groupInspectorOutput ? (
                  <div className="rounded-lg border border-white/10 bg-black/20 px-3 py-2">
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <p className="text-xs uppercase tracking-wide text-[#8fb7d6]">Inspector output</p>
                      <button
                        type="button"
                        onClick={() => setGroupInspectorOutput('')}
                        className="rounded-full p-1 text-[#8fb7d6] hover:bg-white/10 hover:text-white"
                        title="Close inspector output"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </div>
                    <pre className="max-h-52 overflow-auto whitespace-pre-wrap break-words rounded-md border border-white/10 bg-[#0f1a26] p-2 text-[11px] text-[#c5e5fb]">
                      {groupInspectorOutput}
                    </pre>
                  </div>
                ) : null}
              </div>
            ) : null}

            {(chatScopeTab === 'group' || chatScopeTab === 'channel') && groupSettingsPage === 'members' ? (
              <div className="space-y-3">
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <input
                    value={groupMembersFilter}
                    onChange={(event) => setGroupMembersFilter(event.target.value)}
                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                    placeholder={isChannelScope
                      ? 'Search subscriber by name / username / id'
                      : 'Search member by name / username / id'}
                  />
                  <div className="rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
                    {selectedGroupMembers.length} {isChannelScope ? 'subscriber' : 'member'} record(s)
                  </div>
                </div>

                <div className="max-h-[56vh] space-y-2 overflow-auto pr-1">
                  {selectedGroupMembers.map((member) => {
                    const statusColor = member.status === 'owner'
                      ? 'text-amber-200'
                      : member.status === 'admin'
                        ? 'text-sky-200'
                        : member.status === 'banned'
                          ? 'text-red-200'
                          : member.status === 'restricted'
                            ? 'text-orange-200'
                            : 'text-emerald-200';
                    const isExpanded = expandedGroupMemberId === member.userId;
                    const restrictionDraft = groupMemberRestrictionDraftByUserId[member.userId] || defaultGroupMemberRestrictionDraft();

                    return (
                      <div key={`group-member-${member.userId}`} className="rounded-xl border border-white/10 bg-black/20 p-3">
                        <button
                          type="button"
                          onClick={() => onToggleGroupMemberExpanded(member.userId)}
                          className="flex w-full items-center justify-between text-left"
                        >
                          <div className="min-w-0">
                            <p className="truncate text-sm font-medium text-white">
                              {member.firstName}
                              {member.isBot ? ' (bot)' : ''}
                            </p>
                            <p className="truncate text-xs text-telegram-textSecondary">
                              @{member.username || `user_${member.userId}`} · id {member.userId}
                            </p>
                            {member.customTitle || (!isChannelScope && member.tag) ? (
                              <div className="mt-1 flex flex-wrap items-center gap-1.5">
                                {member.customTitle ? (
                                  <span className="rounded border border-amber-300/35 bg-amber-900/25 px-1.5 py-0.5 text-[10px] text-amber-100">
                                    admin: {member.customTitle}
                                  </span>
                                ) : null}
                                {!isChannelScope && member.tag ? (
                                  <span className="rounded border border-sky-300/35 bg-sky-900/25 px-1.5 py-0.5 text-[10px] text-sky-100">
                                    tag: {member.tag}
                                  </span>
                                ) : null}
                              </div>
                            ) : null}
                          </div>
                          <div className="ml-2 flex items-center gap-2">
                            <span className={`rounded-full border border-white/15 px-2 py-1 text-[11px] ${statusColor}`}>
                              {member.status}
                            </span>
                            <ChevronDown className={`h-4 w-4 text-[#9cc4de] transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                          </div>
                        </button>

                        {isExpanded ? (
                          <div className="mt-3 space-y-3 border-t border-white/10 pt-3">
                            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                              <button
                                type="button"
                                onClick={() => void onBanGroupMember(member.userId)}
                                disabled={isGroupActionRunning || !canEditSelectedGroup || member.status === 'owner' || member.status === 'banned'}
                                className="rounded-lg border border-red-400/35 bg-red-900/25 px-3 py-2 text-sm text-red-100 hover:bg-red-900/35 disabled:opacity-40"
                              >
                                Ban
                              </button>
                              <button
                                type="button"
                                onClick={() => void onUnbanGroupMember(member.userId)}
                                disabled={isGroupActionRunning || !canEditSelectedGroup || member.status !== 'banned'}
                                className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                              >
                                Unban
                              </button>
                              <button
                                type="button"
                                onClick={() => void onPromoteGroupMember(member.userId, member.status !== 'admin')}
                                disabled={isGroupActionRunning || !canEditSelectedGroup || member.status === 'owner' || member.status === 'banned'}
                                className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                              >
                                {member.status === 'admin' ? 'Demote' : 'Promote'}
                              </button>
                              <button
                                type="button"
                                onClick={() => void onInspectSelectedGroupMember(member.userId)}
                                disabled={isGroupActionRunning || !canEditSelectedGroup}
                                className="rounded-lg border border-[#7cbfe9]/35 bg-[#153349] px-3 py-2 text-sm text-[#d2eeff] hover:bg-[#1b425d] disabled:opacity-40 sm:col-span-3"
                              >
                                getChatMember
                              </button>
                            </div>

                            {isChannelScope ? (
                              member.status === 'admin' ? (
                                <div className="rounded-lg border border-sky-400/30 bg-sky-900/15 p-3">
                                  <p className="mb-2 text-xs uppercase tracking-wide text-sky-100">Channel admin rights</p>
                                  <div className="grid grid-cols-1 gap-2 text-xs text-[#d7e8f5] sm:grid-cols-2">
                                    <label className="flex items-center gap-2"><input type="checkbox" checked={channelAdminRightsDraftByChatKey[selectedGroupStateKey || '']?.[member.userId]?.canManageChat ?? false} onChange={(event) => onUpdateChannelAdminRightsDraft(member.userId, { canManageChat: event.target.checked })} />can_manage_chat</label>
                                    <label className="flex items-center gap-2"><input type="checkbox" checked={channelAdminRightsDraftByChatKey[selectedGroupStateKey || '']?.[member.userId]?.canPostMessages ?? false} onChange={(event) => onUpdateChannelAdminRightsDraft(member.userId, { canPostMessages: event.target.checked })} />can_post_messages</label>
                                    <label className="flex items-center gap-2"><input type="checkbox" checked={channelAdminRightsDraftByChatKey[selectedGroupStateKey || '']?.[member.userId]?.canEditMessages ?? false} onChange={(event) => onUpdateChannelAdminRightsDraft(member.userId, { canEditMessages: event.target.checked })} />can_edit_messages</label>
                                    <label className="flex items-center gap-2"><input type="checkbox" checked={channelAdminRightsDraftByChatKey[selectedGroupStateKey || '']?.[member.userId]?.canDeleteMessages ?? false} onChange={(event) => onUpdateChannelAdminRightsDraft(member.userId, { canDeleteMessages: event.target.checked })} />can_delete_messages</label>
                                    <label className="flex items-center gap-2"><input type="checkbox" checked={channelAdminRightsDraftByChatKey[selectedGroupStateKey || '']?.[member.userId]?.canInviteUsers ?? false} onChange={(event) => onUpdateChannelAdminRightsDraft(member.userId, { canInviteUsers: event.target.checked })} />can_invite_users</label>
                                    <label className="flex items-center gap-2"><input type="checkbox" checked={channelAdminRightsDraftByChatKey[selectedGroupStateKey || '']?.[member.userId]?.canChangeInfo ?? false} onChange={(event) => onUpdateChannelAdminRightsDraft(member.userId, { canChangeInfo: event.target.checked })} />can_change_info</label>
                                  </div>
                                  <div className="mt-2">
                                    <button
                                      type="button"
                                      onClick={() => void onApplyChannelAdminRights(member.userId)}
                                      disabled={isGroupActionRunning || !canEditSelectedGroup || member.status !== 'admin'}
                                      className="rounded-lg border border-sky-300/35 bg-sky-900/25 px-3 py-2 text-sm text-sky-100 hover:bg-sky-900/35 disabled:opacity-40"
                                    >
                                      Apply admin rights
                                    </button>
                                  </div>
                                </div>
                              ) : null
                            ) : (
                              <div className="rounded-lg border border-amber-400/30 bg-amber-900/20 p-3">
                                <p className="mb-2 text-xs uppercase tracking-wide text-amber-100">Restriction controls</p>
                                <div className="grid grid-cols-1 gap-2 text-xs text-[#d7e8f5] sm:grid-cols-2">
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendMessages} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendMessages: event.target.checked })} />can_send_messages</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendAudios} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendAudios: event.target.checked })} />can_send_audios</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendDocuments} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendDocuments: event.target.checked })} />can_send_documents</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendPhotos} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendPhotos: event.target.checked })} />can_send_photos</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendVideos} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendVideos: event.target.checked })} />can_send_videos</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendVideoNotes} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendVideoNotes: event.target.checked })} />can_send_video_notes</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendVoiceNotes} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendVoiceNotes: event.target.checked })} />can_send_voice_notes</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendPolls} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendPolls: event.target.checked })} />can_send_polls</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canSendOtherMessages} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canSendOtherMessages: event.target.checked })} />can_send_other_messages</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canAddWebPagePreviews} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canAddWebPagePreviews: event.target.checked })} />can_add_web_page_previews</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canInviteUsers} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canInviteUsers: event.target.checked })} />can_invite_users</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canChangeInfo} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canChangeInfo: event.target.checked })} />can_change_info</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canPinMessages} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canPinMessages: event.target.checked })} />can_pin_messages</label>
                                  <label className="flex items-center gap-2"><input type="checkbox" checked={restrictionDraft.canManageTopics} onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { canManageTopics: event.target.checked })} />can_manage_topics</label>
                                </div>
                                <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-3">
                                  <input
                                    value={restrictionDraft.untilHours}
                                    onChange={(event) => onUpdateGroupMemberRestrictionDraft(member.userId, { untilHours: event.target.value })}
                                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none sm:col-span-1"
                                    placeholder="until hours"
                                  />
                                  <button
                                    type="button"
                                    onClick={() => void onApplyGroupMemberRestriction(member.userId)}
                                    disabled={isGroupActionRunning || !canEditSelectedGroup || member.status === 'owner' || member.status === 'banned'}
                                    className="rounded-lg border border-amber-400/35 bg-amber-900/25 px-3 py-2 text-sm text-amber-100 hover:bg-amber-900/35 disabled:opacity-40"
                                  >
                                    Apply restriction
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void onLiftGroupMemberRestriction(member.userId)}
                                    disabled={isGroupActionRunning || !canEditSelectedGroup || member.status === 'owner' || member.status === 'banned'}
                                    className="rounded-lg border border-emerald-400/35 bg-emerald-900/25 px-3 py-2 text-sm text-emerald-100 hover:bg-emerald-900/35 disabled:opacity-40"
                                  >
                                    Lift restriction
                                  </button>
                                </div>
                              </div>
                            )}

                            {isChannelScope ? null : (
                              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                                <input
                                  value={groupMemberAdminTitleByUserId[member.userId] || ''}
                                  onChange={(event) => setGroupMemberAdminTitleByUserId((prev) => ({
                                    ...prev,
                                    [member.userId]: event.target.value,
                                  }))}
                                  className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                                  placeholder="admin custom title"
                                />
                                <button
                                  type="button"
                                  onClick={() => void onSetGroupAdminTitle(member.userId, groupMemberAdminTitleByUserId[member.userId] || '')}
                                  disabled={isGroupActionRunning || !canEditSelectedGroup || member.status !== 'admin'}
                                  className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                                >
                                  setChatAdministratorCustomTitle
                                </button>
                                <input
                                  value={groupMemberTagByUserId[member.userId] || ''}
                                  onChange={(event) => setGroupMemberTagByUserId((prev) => ({
                                    ...prev,
                                    [member.userId]: event.target.value,
                                  }))}
                                  className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                                  placeholder="member tag"
                                />
                                <button
                                  type="button"
                                  onClick={() => void onSetGroupMemberTag(member.userId, groupMemberTagByUserId[member.userId])}
                                  disabled={isGroupActionRunning || !canEditSelectedGroup || (member.status !== 'member' && member.status !== 'restricted')}
                                  className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                                >
                                  setChatMemberTag
                                </button>
                              </div>
                            )}
                          </div>
                        ) : null}
                      </div>
                    );
                  })}
                </div>

                {groupInspectorOutput ? (
                  <div className="rounded-lg border border-white/10 bg-black/20 px-3 py-2">
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <p className="text-xs uppercase tracking-wide text-[#8fb7d6]">Inspector output</p>
                      <button
                        type="button"
                        onClick={() => setGroupInspectorOutput('')}
                        className="rounded-full p-1 text-[#8fb7d6] hover:bg-white/10 hover:text-white"
                        title="Close inspector output"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </div>
                    <pre className="max-h-52 overflow-auto whitespace-pre-wrap break-words rounded-md border border-white/10 bg-[#0f1a26] p-2 text-[11px] text-[#c5e5fb]">
                      {groupInspectorOutput}
                    </pre>
                  </div>
                ) : null}
              </div>
            ) : null}

            {chatScopeTab === 'group' && groupSettingsPage === 'sender-chat' ? (
              <div className="space-y-3 rounded-lg border border-white/10 bg-black/20 p-3">
                <p className="text-xs uppercase tracking-wide text-[#8fb7d6]">Sender chat moderation</p>
                <input
                  value={groupSenderChatModerationDraft}
                  onChange={(event) => setGroupSenderChatModerationDraft(event.target.value)}
                  className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                  placeholder="sender_chat_id"
                />
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <button
                    type="button"
                    onClick={() => void onBanSenderChat(true, Number(groupSenderChatModerationDraft.trim()))}
                    disabled={isGroupActionRunning || !canEditSelectedGroup}
                    className="rounded-lg border border-red-400/35 bg-red-900/25 px-3 py-2 text-sm text-red-100 hover:bg-red-900/35 disabled:opacity-40"
                  >
                    banChatSenderChat
                  </button>
                  <button
                    type="button"
                    onClick={() => void onBanSenderChat(false, Number(groupSenderChatModerationDraft.trim()))}
                    disabled={isGroupActionRunning || !canEditSelectedGroup}
                    className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                  >
                    unbanChatSenderChat
                  </button>
                </div>
              </div>
            ) : null}

            {groupSettingsPage === 'danger-zone' ? (
              <div className="space-y-3 rounded-xl border border-red-400/35 bg-red-900/20 p-3">
                <p className="text-sm text-red-100">Danger actions for this {isChannelScope ? 'channel' : 'group'}.</p>
                <button
                  type="button"
                  onClick={() => void onDeleteSelectedGroup()}
                  disabled={isGroupActionRunning || !canDeleteSelectedGroup}
                  className="w-full rounded-lg border border-red-400/35 bg-red-900/25 px-3 py-2 text-left text-sm text-red-200 hover:bg-red-900/35 disabled:opacity-40"
                >
                  Delete {isChannelScope ? 'channel' : 'group'} (owner)
                </button>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {showGroupProfileModal && selectedGroup ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <div className="max-h-[90vh] w-full max-w-lg overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between">
              <div className="flex min-w-0 items-center gap-2">
                <button
                  type="button"
                  onClick={onBackFromGroupProfile}
                  className="rounded-full p-1 hover:bg-white/10"
                  title="Back to group settings"
                >
                  <ArrowLeft className="h-4 w-4" />
                </button>
                <h3 className="truncate text-lg font-semibold">{selectedGroup.type === 'channel' ? 'Channel' : 'Group'} Info & Edit</h3>
              </div>
              <button type="button" onClick={() => setShowGroupProfileModal(false)} className="rounded-full p-1 hover:bg-white/10">
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="mb-3 rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
              <p>id: {selectedGroup.id}</p>
              <p>type: {selectedGroup.type}</p>
            </div>

            <div className="space-y-2">
              <input
                value={groupProfileDraft.title}
                onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, title: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder={selectedGroup.type === 'channel' ? 'Channel title' : 'Group title'}
              />
              <input
                value={groupProfileDraft.username}
                onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, username: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder={selectedGroup.type === 'channel' ? 'Channel username' : 'Group username'}
              />
              <textarea
                value={groupProfileDraft.description}
                onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, description: e.target.value }))}
                className="h-24 w-full resize-none rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder={selectedGroup.type === 'channel' ? 'Channel description' : 'Group description'}
              />
              {selectedGroup.type === 'channel' ? (
                <label className="flex items-center gap-2 text-sm text-telegram-textSecondary">
                  <input
                    type="checkbox"
                    checked={groupProfileDraft.showAuthorSignature}
                    onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, showAuthorSignature: e.target.checked }))}
                  />
                  Show publisher name on channel posts
                </label>
              ) : null}
              {selectedGroup.type !== 'channel' ? (
                <>
                  <label className="flex items-center gap-2 text-sm text-telegram-textSecondary">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.messageHistoryVisible}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, messageHistoryVisible: e.target.checked }))}
                    />
                    Message history visible to new members
                  </label>
                  <label className="flex items-center gap-2 text-sm text-telegram-textSecondary">
                    <span className="min-w-[120px] text-xs uppercase tracking-wide text-[#8fb7d6]">Slow mode (sec)</span>
                    <input
                      type="number"
                      min={0}
                      step={1}
                      value={groupProfileDraft.slowModeDelay}
                      onChange={(e) => setGroupProfileDraft((prev) => ({
                        ...prev,
                        slowModeDelay: Math.max(0, Math.floor(Number(e.target.value) || 0)),
                      }))}
                      className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                    />
                  </label>
                  <div className="rounded-lg border border-white/10 bg-black/20 px-3 py-2">
                    <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Permissions</p>
                    <div className="grid grid-cols-1 gap-1 text-sm text-telegram-textSecondary sm:grid-cols-2">
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendMessages}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, allowSendMessages: e.target.checked }))}
                    />
                    Send messages
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendMedia}
                      onChange={(e) => setGroupProfileDraft((prev) => ({
                        ...prev,
                        allowSendMedia: e.target.checked,
                        allowSendAudios: e.target.checked,
                        allowSendDocuments: e.target.checked,
                        allowSendPhotos: e.target.checked,
                        allowSendVideos: e.target.checked,
                        allowSendVideoNotes: e.target.checked,
                        allowSendVoiceNotes: e.target.checked,
                        allowSendOtherMessages: e.target.checked,
                        allowAddWebPagePreviews: e.target.checked,
                      }))}
                    />
                    Send all media + links
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowPolls}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, allowPolls: e.target.checked }))}
                    />
                    Send polls
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowInviteUsers}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, allowInviteUsers: e.target.checked }))}
                    />
                    Invite users
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowPinMessages}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, allowPinMessages: e.target.checked }))}
                    />
                    Pin messages
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowChangeInfo}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, allowChangeInfo: e.target.checked }))}
                    />
                    Change group info
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowManageTopics}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, allowManageTopics: e.target.checked }))}
                    />
                    Manage topics
                  </label>
                </div>

                    <p className="mb-2 mt-3 text-xs uppercase tracking-wide text-[#8fb7d6]">Media matrix</p>
                    <div className="grid grid-cols-1 gap-1 text-sm text-telegram-textSecondary sm:grid-cols-2">
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendAudios}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendAudios: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send audios
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendDocuments}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendDocuments: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send documents
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendPhotos}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendPhotos: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send photos
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendVideos}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendVideos: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send videos
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendVideoNotes}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendVideoNotes: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send video notes
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendVoiceNotes}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendVoiceNotes: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send voice notes
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowSendOtherMessages}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowSendOtherMessages: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Send other messages
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.allowAddWebPagePreviews}
                      onChange={(e) => setGroupProfileDraft((prev) => {
                        const next = { ...prev, allowAddWebPagePreviews: e.target.checked };
                        return {
                          ...next,
                          allowSendMedia: next.allowSendAudios
                            && next.allowSendDocuments
                            && next.allowSendPhotos
                            && next.allowSendVideos
                            && next.allowSendVideoNotes
                            && next.allowSendVoiceNotes
                            && next.allowSendOtherMessages
                            && next.allowAddWebPagePreviews,
                        };
                      })}
                    />
                    Add web page previews
                  </label>
                    </div>
                  </div>
                </>
              ) : (
                null
              )}

                <div className="mt-3 rounded-lg border border-white/10 bg-black/20 p-3">
                  <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Profile media</p>
                  <label className="mb-2 block text-xs text-telegram-textSecondary">Group photo</label>
                  <input
                    type="file"
                    accept="image/*"
                    onChange={(event) => {
                      const file = event.target.files?.[0] ?? null;
                      setGroupPhotoDraftFile(file);
                      if (file) {
                        setRemoveGroupPhotoDraft(false);
                      }
                    }}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-xs text-white outline-none file:mr-3 file:rounded-md file:border-0 file:bg-[#2b5278] file:px-2 file:py-1 file:text-xs file:text-white"
                  />
                  {groupPhotoDraftFile ? (
                    <p className="mt-1 text-[11px] text-[#9fc7e1]">Selected: {groupPhotoDraftFile.name}</p>
                  ) : null}
                  <label className="mt-2 flex items-center gap-2 text-xs text-telegram-textSecondary">
                    <input
                      type="checkbox"
                      checked={removeGroupPhotoDraft}
                      onChange={(event) => {
                        const checked = event.target.checked;
                        setRemoveGroupPhotoDraft(checked);
                        if (checked) {
                          setGroupPhotoDraftFile(null);
                        }
                      }}
                    />
                    deleteChatPhoto on save
                  </label>
                </div>

                {selectedGroup.type === 'supergroup' ? (
                  <div className="rounded-lg border border-white/10 bg-black/20 p-3">
                    <p className="mb-2 text-xs uppercase tracking-wide text-[#8fb7d6]">Sticker set</p>
                    <input
                      value={groupStickerSetDraft}
                      onChange={(event) => {
                        const value = event.target.value;
                        setGroupStickerSetDraft(value);
                        if (value.trim()) {
                          setRemoveGroupStickerSetDraft(false);
                        }
                      }}
                      className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                      placeholder="sticker_set_name"
                    />
                    <label className="mt-2 flex items-center gap-2 text-xs text-telegram-textSecondary">
                      <input
                        type="checkbox"
                        checked={removeGroupStickerSetDraft}
                        onChange={(event) => {
                          const checked = event.target.checked;
                          setRemoveGroupStickerSetDraft(checked);
                          if (checked) {
                            setGroupStickerSetDraft('');
                          }
                        }}
                      />
                      deleteChatStickerSet on save
                    </label>
                  </div>
                ) : null}
              
              {selectedGroup.type === 'supergroup' ? (
                <label className="flex items-center gap-2 text-sm text-telegram-textSecondary">
                  <input
                    type="checkbox"
                    checked={groupProfileDraft.isForum}
                    onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, isForum: e.target.checked }))}
                  />
                  Enable forum topics
                </label>
              ) : null}
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={onBackFromGroupProfile}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Back
              </button>
              <button
                type="button"
                onClick={() => void onSaveGroupProfile()}
                disabled={isGroupActionRunning || !groupProfileDraft.title.trim() || !canEditSelectedGroup}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
              >
                {isGroupActionRunning ? 'Saving...' : `Save ${selectedGroup.type === 'channel' ? 'channel' : 'group'}`}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {showForumTopicModal && selectedGroup && canManageForumTopics ? (
        <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/55 px-4 pb-6 sm:items-center sm:pb-0">
          <div className="w-full max-w-2xl rounded-2xl border border-white/20 bg-[#182b3c] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <h3 className="text-sm font-semibold text-white">
                  {forumTopicModalMode === 'create' ? 'Create forum topic' : `Edit topic #${forumTopicModalThreadId || ''}`}
                </h3>
                <p className="text-xs text-[#9ec3dc]">
                  {forumTopicModalMode === 'create'
                    ? 'Create a new topic with default or premium icon.'
                    : 'Update topic name and icon.'}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setShowForumTopicModal(false)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="space-y-3">
              <input
                value={forumTopicDraft.name}
                onChange={(event) => setForumTopicDraft((prev) => ({ ...prev, name: event.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Topic name"
              />

              <div className="space-y-2">
                <label className="block text-xs text-[#9ec2da]">Normal emoji prefix</label>
                <input
                  value={forumTopicDraft.normalEmoji}
                  onChange={(event) => setForumTopicDraft((prev) => ({
                    ...prev,
                    normalEmoji: event.target.value,
                    iconCustomEmojiId: event.target.value.trim() ? '' : prev.iconCustomEmojiId,
                  }))}
                  className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                  placeholder="e.g. 🚀"
                />
                <div className="flex flex-wrap gap-2">
                  {FORUM_TOPIC_EMOJI_PRESETS.map((emoji) => (
                    <button
                      key={`forum-topic-modal-emoji-${emoji}`}
                      type="button"
                      onClick={() => setForumTopicDraft((prev) => ({ ...prev, normalEmoji: emoji, iconCustomEmojiId: '' }))}
                      className={`rounded-md border px-2 py-1 text-base leading-none ${forumTopicDraft.normalEmoji === emoji ? 'border-[#8dd2ff]/75 bg-[#214a69]' : 'border-white/20 bg-black/20 hover:bg-white/10'}`}
                      title={`Use ${emoji}`}
                    >
                      {emoji}
                    </button>
                  ))}
                </div>
                <p className="text-[11px] text-[#9ec2da]">
                  Preview: {buildForumTopicNameForIconMode(
                    forumTopicDraft.name,
                    forumTopicDraft.normalEmoji,
                    forumTopicDraft.iconCustomEmojiId,
                  ) || '-'}
                </p>
                <p className="text-[11px] text-[#89b4cf]">
                  Use either normal emoji prefix or premium custom icon.
                </p>
              </div>

              <div>
                <p className="mb-1 text-xs text-[#9ec2da]">Default icon colors</p>
                <div className="grid grid-cols-4 gap-2 sm:grid-cols-8">
                  {FORUM_ICON_COLOR_PRESETS.map((color) => {
                    const hex = color.toString(16).padStart(6, '0');
                    const active = Math.floor(Number(forumTopicDraft.iconColor)) === color;
                    return (
                      <button
                        key={`forum-topic-modal-color-${color}`}
                        type="button"
                        onClick={() => setForumTopicDraft((prev) => ({
                          ...prev,
                          iconColor: String(color),
                          iconCustomEmojiId: '',
                        }))}
                        className={`h-8 rounded-md border ${active ? 'border-white/80' : 'border-white/20'} transition hover:scale-[1.03]`}
                        style={{ backgroundColor: `#${hex}` }}
                        title={`icon_color ${color}`}
                      />
                    );
                  })}
                </div>
              </div>

              <div>
                <div className="mb-1 flex items-center justify-between gap-2">
                  <p className="text-xs text-[#9ec2da]">Premium icon (custom emoji)</p>
                  <button
                    type="button"
                    onClick={() => void onLoadForumTopicIconStickers()}
                    disabled={isGroupActionRunning}
                    className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10 disabled:opacity-40"
                  >
                    Load icons
                  </button>
                </div>
                <input
                  value={forumTopicDraft.iconCustomEmojiId}
                  onChange={(event) => setForumTopicDraft((prev) => ({
                    ...prev,
                    iconCustomEmojiId: event.target.value,
                    normalEmoji: event.target.value.trim() ? '' : prev.normalEmoji,
                  }))}
                  className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                  placeholder="icon_custom_emoji_id"
                />
              </div>

              {forumTopicIconStickers.length > 0 ? (
                <div className="grid max-h-48 grid-cols-1 gap-2 overflow-auto pr-1 sm:grid-cols-2">
                  {forumTopicIconStickers.map((sticker) => {
                    const active = Boolean(
                      sticker.custom_emoji_id
                      && sticker.custom_emoji_id === forumTopicDraft.iconCustomEmojiId,
                    );

                    return (
                      <button
                        key={`forum-topic-modal-icon-${sticker.file_id}`}
                        type="button"
                        onClick={() => setForumTopicDraft((prev) => ({
                          ...prev,
                          iconCustomEmojiId: sticker.custom_emoji_id || prev.iconCustomEmojiId,
                          normalEmoji: '',
                        }))}
                        className={`rounded-lg border px-3 py-2 text-left text-xs ${active ? 'border-[#8dd2ff]/70 bg-[#1f4868]/75 text-white' : 'border-white/15 bg-[#0f1c28] text-[#d7ebfb] hover:bg-[#162b3d]'}`}
                        title={sticker.custom_emoji_id || sticker.file_id}
                      >
                        <div className="truncate">{sticker.emoji || 'premium icon'}</div>
                        <div className="truncate text-[10px] text-[#9cc5df]">{sticker.custom_emoji_id || sticker.file_id}</div>
                      </button>
                    );
                  })}
                </div>
              ) : null}
            </div>

            <div className="mt-4 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => setShowForumTopicModal(false)}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void onSubmitForumTopicModal()}
                disabled={isGroupActionRunning || !buildForumTopicNameForIconMode(
                  forumTopicDraft.name,
                  forumTopicDraft.normalEmoji,
                  forumTopicDraft.iconCustomEmojiId,
                ).trim()}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
              >
                {forumTopicModalMode === 'create' ? 'Create topic' : 'Save topic'}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {messageMenu ? (
        <div
          className="fixed z-50 w-60 max-w-[90vw] rounded-2xl border border-white/15 bg-[#132130] p-2 shadow-2xl"
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
                {target.service ? null : (
                  <button
                    type="button"
                    onClick={() => {
                      setForwardMessageId(target.id);
                      setForwardTargetChatId('');
                      setMessageMenu(null);
                    }}
                    className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
                  >
                    <Forward className="h-4 w-4" />
                    Forward
                  </button>
                )}
                {canEditMessageByActiveActor(target) ? (
                  <button
                    type="button"
                    onClick={() => onEditMessage(target)}
                    className="w-full rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
                  >
                    {target.media ? 'Edit caption/media' : 'Edit text'}
                  </button>
                ) : null}
                {(chatScopeTab === 'group' || chatScopeTab === 'channel') && canPinInSelectedGroup && !target.service ? (
                  <button
                    type="button"
                    onClick={() => {
                      if (isMessagePinned(target.id)) {
                        void onUnpinMessageById(target.id);
                      } else {
                        void onPinMessageById(target.id);
                      }
                    }}
                    disabled={isGroupActionRunning}
                    className="w-full rounded-lg px-3 py-2 text-left text-sm text-[#d7efff] hover:bg-white/10 disabled:opacity-40"
                  >
                    {isMessagePinned(target.id) ? 'Unpin message' : 'Pin message'}
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

      {forwardMessageId ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <div className="w-full max-w-sm rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between">
              <div>
                <h3 className="text-sm font-semibold text-white">Forward message</h3>
                <p className="text-xs text-[#9ec3dc]">Message id: {forwardMessageId}</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  setForwardMessageId(null);
                  setForwardTargetChatId('');
                }}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="space-y-3">
              <input
                value={forwardTargetChatId}
                onChange={(event) => setForwardTargetChatId(event.target.value)}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="target chat_id, title, or @username"
              />
              <div className="max-h-56 space-y-1 overflow-y-auto rounded-lg border border-white/10 bg-black/20 p-2">
                {filteredForwardTargets.length === 0 ? (
                  <p className="px-2 py-1 text-xs text-telegram-textSecondary">No chat matches this value.</p>
                ) : (
                  filteredForwardTargets.map((target) => {
                    const isSelected = String(target.chatId) === forwardTargetChatId.trim();
                    return (
                      <button
                        key={`forward-target-${target.chatId}`}
                        type="button"
                        onClick={() => setForwardTargetChatId(String(target.chatId))}
                        className={[
                          'flex w-full items-center justify-between gap-2 rounded-lg px-2 py-1.5 text-left text-xs',
                          isSelected ? 'bg-[#2b5278] text-white' : 'text-[#c9deed] hover:bg-white/10',
                        ].join(' ')}
                      >
                        <span className="min-w-0 truncate">
                          {target.title}
                          {target.username ? ` (@${target.username})` : ''}
                        </span>
                        <span className="shrink-0 rounded border border-white/15 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-[#8fb7d6]">
                          {target.kind}
                        </span>
                      </button>
                    );
                  })
                )}
              </div>
              <div className="flex items-center justify-end gap-2">
                <button
                  type="button"
                  onClick={() => {
                    setForwardMessageId(null);
                    setForwardTargetChatId('');
                  }}
                  className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => void onSubmitForwardMessage()}
                  disabled={isSending}
                  className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
                >
                  Forward now
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {forumTopicContextMenu && canManageForumTopics ? (
        <div
          className="fixed z-50 w-64 max-w-[90vw] rounded-2xl border border-white/15 bg-[#132130] p-2 shadow-2xl"
          style={{ left: forumTopicContextMenu.x, top: forumTopicContextMenu.y }}
          onClick={(event) => event.stopPropagation()}
        >
          <div className="mb-1 truncate px-2 pt-1 text-xs font-semibold text-white">
            {forumTopicContextMenu.topic.name}
          </div>
          <div className="mb-2 px-2 text-[11px] text-[#9ec3dc]">
            thread #{forumTopicContextMenu.topic.messageThreadId}
          </div>

          <button
            type="button"
            onClick={() => {
              selectForumTopicThread(forumTopicContextMenu.topic.messageThreadId);
              setForumTopicContextMenu(null);
            }}
            className="w-full rounded-lg px-3 py-2 text-left text-sm text-[#d7efff] hover:bg-white/10"
          >
            Open in chat
          </button>
          <button
            type="button"
            onClick={() => void onRunForumTopicContextAction('edit')}
            className="w-full rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
          >
            Edit topic
          </button>
          <button
            type="button"
            onClick={() => void onRunForumTopicContextAction(forumTopicContextMenu.topic.isClosed ? 'reopen' : 'close')}
            className="w-full rounded-lg px-3 py-2 text-left text-sm text-orange-100 hover:bg-white/10"
          >
            {forumTopicContextMenu.topic.isClosed ? 'Reopen topic' : 'Close topic'}
          </button>
          <button
            type="button"
            onClick={() => void onRunForumTopicContextAction('unpin')}
            className="w-full rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
          >
            Unpin all messages
          </button>
          {forumTopicContextMenu.topic.isGeneral ? (
            <button
              type="button"
              onClick={() => void onRunForumTopicContextAction(forumTopicContextMenu.topic.isHidden ? 'unhide' : 'hide')}
              className="w-full rounded-lg px-3 py-2 text-left text-sm text-white hover:bg-white/10"
            >
              {forumTopicContextMenu.topic.isHidden ? 'Unhide general topic' : 'Hide general topic'}
            </button>
          ) : (
            <button
              type="button"
              onClick={() => void onRunForumTopicContextAction('delete')}
              className="w-full rounded-lg px-3 py-2 text-left text-sm text-red-200 hover:bg-white/10"
            >
              Delete topic
            </button>
          )}
        </div>
      ) : null}

      {callbackModalText ? (
        <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/45 px-4 pb-8 sm:items-center sm:pb-0">
          <div className="w-full max-w-sm rounded-2xl border border-white/20 bg-[#182b3c] p-4 shadow-2xl">
            <h3 className="mb-2 text-sm font-semibold text-white">Bot Notification</h3>
            <p className="mb-4 whitespace-pre-wrap text-sm leading-6 text-[#d8ecfb]">{callbackModalText}</p>
            <div className="flex justify-end">
              <button
                type="button"
                onClick={() => setCallbackModalText(null)}
                className="rounded-lg bg-[#2b5278] px-4 py-2 text-sm font-medium text-white hover:bg-[#366892]"
              >
                OK
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {checkoutFlow && checkoutMessage?.invoice ? (
        <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/55 px-4 pb-6 sm:items-center sm:pb-0">
          <div className="w-full max-w-lg rounded-2xl border border-white/20 bg-[#182b3c] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-white">Checkout</h3>
              <button
                type="button"
                onClick={() => setCheckoutFlow(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="mb-3 grid grid-cols-3 gap-2 text-[11px]">
              {[1, 2, 3].map((step) => (
                <div
                  key={`checkout-step-${step}`}
                  className={`rounded-md border px-2 py-1 text-center ${checkoutFlow.step >= step ? 'border-[#6ab8ef]/60 bg-[#224d6f] text-white' : 'border-white/20 bg-black/20 text-[#aacbe0]'}`}
                >
                  {step === 1 ? 'Method' : step === 2 ? 'Review' : 'Confirm'}
                </div>
              ))}
            </div>

            {checkoutFlow.step === 1 ? (
              <div className="space-y-2">
                <div className="text-xs text-[#d8ecfb]">Choose payment method for {checkoutMessage.invoice.total_amount} {checkoutMessage.invoice.currency}</div>
                <div className="grid grid-cols-3 gap-2">
                  {(['wallet', 'card', 'stars'] as PaymentMethod[]).map((method) => {
                    const isStarsInvoice = checkoutMessage.invoice!.currency.toUpperCase() === 'XTR';
                    const disabled = (isStarsInvoice && method !== 'stars') || (!isStarsInvoice && method === 'stars');
                    return (
                      <button
                        key={`checkout-method-${method}`}
                        type="button"
                        disabled={disabled}
                        onClick={() => setCheckoutFlow((prev) => (prev ? { ...prev, method } : prev))}
                        className={`rounded-md border px-2 py-2 text-xs transition ${checkoutFlow.method === method ? 'border-[#7ec8fb]/60 bg-[#2b5278] text-white' : 'border-white/20 bg-white/10 text-[#d7ecfb]'} disabled:cursor-not-allowed disabled:opacity-40`}
                      >
                        {method.toUpperCase()}
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}

            {checkoutFlow.step === 2 ? (
              <div className="space-y-2 text-xs text-[#d8ecfb]">
                <div className="rounded-lg border border-white/20 bg-black/20 px-3 py-2">
                  <div>Invoice: {checkoutMessage.invoice.title}</div>
                  <div>Amount: {checkoutMessage.invoice.total_amount} {checkoutMessage.invoice.currency}</div>
                  <div>Method: {checkoutFlow.method}</div>
                </div>
                {checkoutMessage.invoice.currency.toUpperCase() !== 'XTR' ? (
                  <div className="flex items-center gap-2">
                    <span>Tip</span>
                    <input
                      type="number"
                      min={0}
                      value={checkoutFlow.tip}
                      onChange={(event) => setCheckoutFlow((prev) => (prev ? { ...prev, tip: event.target.value } : prev))}
                      className="w-32 rounded border border-white/20 bg-white/10 px-2 py-1 text-xs text-white outline-none"
                      placeholder="0"
                    />
                  </div>
                ) : null}
                <div>
                  Total debit: {
                    checkoutMessage.invoice.total_amount + (
                      checkoutMessage.invoice.currency.toUpperCase() === 'XTR'
                        ? 0
                        : Math.max(Math.floor(Number(checkoutFlow.tip || '0') || 0), 0)
                    )
                  } {checkoutMessage.invoice.currency}
                </div>
              </div>
            ) : null}

            {checkoutFlow.step === 3 ? (
              <div className="space-y-2 text-xs text-[#d8ecfb]">
                <div className="text-[11px] text-[#9fc6df]">Select payment outcome</div>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => setCheckoutFlow((prev) => (prev ? { ...prev, outcome: 'success' } : prev))}
                    className={`rounded-md border px-2 py-1 text-xs ${checkoutFlow.outcome === 'success' ? 'border-emerald-300/50 bg-emerald-700/35 text-emerald-100' : 'border-white/20 bg-white/10 text-[#d7ecfb]'}`}
                  >
                    Successful Payment
                  </button>
                  <button
                    type="button"
                    onClick={() => setCheckoutFlow((prev) => (prev ? { ...prev, outcome: 'failed' } : prev))}
                    className={`rounded-md border px-2 py-1 text-xs ${checkoutFlow.outcome === 'failed' ? 'border-red-300/40 bg-red-700/30 text-red-100' : 'border-white/20 bg-white/10 text-[#d7ecfb]'}`}
                  >
                    Failed Payment
                  </button>
                </div>
              </div>
            ) : null}

            <div className="mt-4 flex items-center justify-between gap-2">
              <button
                type="button"
                onClick={() => setCheckoutFlow((prev) => (prev && prev.step > 1 ? { ...prev, step: (prev.step - 1) as CheckoutStep } : prev))}
                disabled={checkoutFlow.step === 1}
                className="rounded-md border border-white/20 bg-white/10 px-3 py-1.5 text-xs text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                Back
              </button>
              {checkoutFlow.step < 3 ? (
                <button
                  type="button"
                  onClick={() => setCheckoutFlow((prev) => (prev ? { ...prev, step: (prev.step + 1) as CheckoutStep } : prev))}
                  className="rounded-md border border-[#6ab8ef]/50 bg-[#1f5379] px-3 py-1.5 text-xs text-white hover:bg-[#2b6a98]"
                >
                  Next
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => void commitCheckoutFlow()}
                  className="rounded-md border border-[#6ab8ef]/50 bg-[#1f5379] px-3 py-1.5 text-xs text-white hover:bg-[#2b6a98]"
                >
                  Pay Now
                </button>
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
