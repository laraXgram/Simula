import { FormEvent, MouseEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  ArrowLeft,
  BadgeCheck,
  Bug,
  ChevronLeft,
  ChevronDown,
  ChevronRight,
  Clapperboard,
  Contact,
  Dice5,
  Eye,
  Gift,
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
  PanelLeftClose,
  PanelLeftOpen,
  Paperclip,
  Plus,
  RefreshCw,
  Search,
  SendHorizonal,
  Settings2,
  ShieldCheck,
  Smile,
  Star,
  Trash2,
  UserPlus,
  Video,
  Wallet,
  Wrench,
  X,
  MessageCircle,
} from 'lucide-react';
import {
  chooseInlineResult,
  clearSimHistory,
  createSimBot,
  createSimulationGroup,
  approveChatJoinRequest,
  approveSuggestedPost,
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
  declineSuggestedPost,
  setSimBotPrivacyMode,
  setChatMenuButton,
  setMyCommands,
  getMyCommands,
  deleteMyCommands,
  setMyName,
  getMyName,
  setMyDescription,
  getMyDescription,
  setMyShortDescription,
  getMyShortDescription,
  setMyProfilePhoto,
  removeMyProfilePhoto,
  setMyDefaultAdministratorRights,
  getMyDefaultAdministratorRights,
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
  editStory,
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
  answerWebAppQuery,
  savePreparedInlineMessage,
  savePreparedKeyboardButton,
  getManagedBotToken,
  replaceManagedBotToken,
  sendChecklist,
  editMessageChecklist,
  setUserEmojiStatus,
  getUserProfilePhotos,
  getUserProfileAudios,
  getUserChatBoosts,
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
  getAvailableGifts,
  getChatGifts,
  deleteOwnedGift,
  getUserGifts,
  giftPremiumSubscription,
  purchasePaidMedia,
  setGameScore,
  sendGift,
  getSimulationBootstrap,
  openSimulationChannelDirectMessages,
  postStory,
  postStoryWithFile,
  removeSimulationBusinessConnection,
  repostStory,
  setSimulationBusinessConnection,
  deleteStory,
  editStoryWithFile,
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
  sendBotPaidMedia,
  sendBotMessage,
  setUserMessageReaction,
  setSimulationBotGroupMembership,
  setSimUserProfileAudio,
  uploadSimUserProfileAudio,
  deleteSimUserProfileAudio,
  addSimUserChatBoosts,
  removeSimUserChatBoosts,
  updateSimulationGroup,
  updateSimBot,
  upsertSimUser,
} from '../../services/botApi';
import { API_BASE_URL, DEFAULT_BOT_TOKEN } from '../../services/config';
import { useBotUpdates } from '../../hooks/useBotUpdates';
import type { GetChatMenuButtonRequest, SetChatMenuButtonRequest } from '../../types/generated/methods';
import type {
  BotCommand as GeneratedBotCommand,
  ChatAdministratorRights as GeneratedChatAdministratorRights,
  BusinessBotRights as GeneratedBusinessBotRights,
  BusinessConnection as GeneratedBusinessConnection,
  ChatShared as GeneratedChatShared,
  ChatMember as GeneratedChatMember,
  MenuButton as GeneratedMenuButton,
  MenuButtonCommands as GeneratedMenuButtonCommands,
  MenuButtonDefault as GeneratedMenuButtonDefault,
  MenuButtonWebApp as GeneratedMenuButtonWebApp,
  Gift as GeneratedGift,
  KeyboardButtonRequestManagedBot as GeneratedKeyboardButtonRequestManagedBot,
  OwnedGiftRegular as GeneratedOwnedGiftRegular,
  UserProfilePhotos as GeneratedUserProfilePhotos,
  UserProfileAudios as GeneratedUserProfileAudios,
  UserChatBoosts as GeneratedUserChatBoosts,
  User as GeneratedUser,
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
  is_premium: false,
  gift_count: 0,
};

const START_KEY = 'simula-started-chats';
const BOTS_KEY = 'simula-sim-bots';
const USERS_KEY = 'simula-sim-users';
const MESSAGES_KEY = 'simula-sim-messages';
const LAST_UPDATES_KEY = 'simula-last-update-ids';
const SELECTED_BOT_KEY = 'simula-selected-bot-token';
const SELECTED_USER_KEY = 'simula-selected-user-id';
const CHAT_SCOPE_KEY = 'simula-chat-scope';
const GROUP_CHATS_KEY = 'simula-group-chats';
const GROUP_MEMBERSHIP_KEY = 'simula-group-memberships';
const SELECTED_GROUP_BY_BOT_KEY = 'simula-selected-group-by-bot';
const GROUP_INVITE_LINKS_KEY = 'simula-group-invite-links';
const GROUP_JOIN_REQUESTS_KEY = 'simula-group-join-requests';
const GROUP_PINNED_MESSAGES_KEY = 'simula-group-pinned-messages';
const INVOICE_META_KEY = 'simula-invoice-meta-by-message';
const FORUM_TOPICS_KEY = 'simula-forum-topics-by-chat';
const SELECTED_FORUM_TOPIC_KEY = 'simula-selected-forum-topic-by-chat';
const BUSINESS_CONNECTIONS_KEY = 'simula-business-connections';
const USER_WALLETS_KEY = 'simula-user-wallets';
const PAID_MEDIA_PURCHASES_KEY = 'simula-paid-media-purchases';
const STORY_SHELF_BY_BOT_KEY = 'simula-story-shelf-by-bot';
const HIDDEN_STORY_KEYS_BY_BOT_KEY = 'simula-hidden-story-keys-by-bot';
const MANAGED_BOT_SETTINGS_BY_BOT_KEY = 'simula-managed-bot-settings-by-bot';
const USER_EMOJI_STATUS_BY_KEY = 'simula-user-emoji-status-by-key';
const CHAT_BOOST_COUNTS_BY_ACTOR_CHAT_KEY = 'simula-chat-boost-counts-by-actor-chat';
const SIDEBAR_SECTIONS_KEY = 'simula-sidebar-sections';
const GENERAL_FORUM_TOPIC_THREAD_ID = 1;
const DEFAULT_FORUM_ICON_COLOR = 0x6FB9F0;
const DEFAULT_WALLET_STATE = {
  fiat: 50000,
  stars: 2500,
};
const PREMIUM_SUBSCRIPTION_STAR_COST = 950;
const STORY_ACTIVE_PERIOD_OPTIONS = [
  { value: '21600', label: '6 hours' },
  { value: '43200', label: '12 hours' },
  { value: '86400', label: '24 hours' },
  { value: '172800', label: '48 hours' },
] as const;
type SidebarTab = 'chats' | 'users' | 'bots' | 'debugger' | 'settings';
type ChatScopeTab = 'private' | 'group' | 'channel';
type BotModalMode = 'create' | 'edit';
type UserModalMode = 'create' | 'edit';
type GroupSettingsPage = 'home' | 'bot-membership' | 'discovery' | 'topics' | 'members' | 'sender-chat' | 'danger-zone';
type ComposerParseMode = 'none' | 'MarkdownV2' | 'Markdown' | 'HTML';
type PaymentMethod = 'wallet' | 'card' | 'stars';
type CheckoutStep = 1 | 2 | 3;
type MediaDrawerTab =
  | 'stickers'
  | 'gifts'
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
  | 'checklist';

interface StoryPreviewSnapshot {
  caption?: string;
  contentRef?: string;
  contentType?: 'photo' | 'video';
}

interface SidebarSectionState {
  chats: boolean;
  privateChats: boolean;
  groupChats: boolean;
  channelChats: boolean;
  users: boolean;
  bots: boolean;
  debugger: boolean;
  settings: boolean;
}

interface DebugEventLog {
  id: string;
  at: number;
  method: string;
  path?: string;
  source: 'bot' | 'webhook';
  query?: string;
  statusCode?: number;
  durationMs?: number;
  remoteAddr?: string;
  status: 'ok' | 'error';
  request?: unknown;
  response?: unknown;
  error?: string;
}

interface RuntimeInfoState {
  api_host: string;
  api_port: string;
  web_port: string;
  database_path: string;
  storage_path: string;
  logs_path: string;
  workspace_dir: string;
  api_enabled?: boolean;
  env_file_path?: string;
  env_values?: Record<string, string>;
  service?: {
    mode: string;
    name: string;
    available: boolean;
    active: boolean;
    status: string;
    requested_mode?: string;
    note?: string;
  };
}

interface RuntimeEnvRow {
  id: string;
  key: string;
  value: string;
}

interface StoryShelfEntry {
  story: NonNullable<ChatMessage['story']>;
  updatedAt: number;
  preview?: StoryPreviewSnapshot;
}

interface DeclineSuggestedPostModalState {
  chatId: number;
  messageId: number;
  comment: string;
}

interface UserDraftState {
  first_name: string;
  last_name: string;
  username: string;
  id: string;
  phone_number: string;
  photo_url: string;
  bio: string;
  is_premium: boolean;
  business_name: string;
  business_intro: string;
  business_location: string;
  gift_count: string;
}

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

interface BotAdminRightsDraft {
  isAnonymous: boolean;
  canManageChat: boolean;
  canDeleteMessages: boolean;
  canManageVideoChats: boolean;
  canRestrictMembers: boolean;
  canPromoteMembers: boolean;
  canChangeInfo: boolean;
  canInviteUsers: boolean;
  canPostStories: boolean;
  canEditStories: boolean;
  canDeleteStories: boolean;
  canPostMessages: boolean;
  canEditMessages: boolean;
  canPinMessages: boolean;
  canManageTopics: boolean;
  canManageDirectMessages: boolean;
  canManageTags: boolean;
}

interface BotDraftState {
  first_name: string;
  username: string;
  description: string;
  short_description: string;
  profile_photo_ref: string;
  remove_profile_photo: boolean;
  commands_text: string;
  commands_language_code: string;
  group_default_admin_rights: BotAdminRightsDraft;
  channel_default_admin_rights: BotAdminRightsDraft;
}

interface ManagedBotSettingsState {
  enabled: boolean;
}

interface ManagedBotRequestModalState {
  buttonText: string;
  requestId: string;
  suggestedName: string;
  suggestedUsername: string;
}

interface UserEmojiStatusState {
  customEmojiId: string;
  expirationDate?: number;
}

interface MiniAppModalState {
  source: 'reply_keyboard_web_app' | 'inline_keyboard_web_app';
  buttonText: string;
  queryId: string;
  url: string;
}

interface ChatBoostModalState {
  chatId: number;
  chatTitle: string;
  countDraft: string;
}

interface ChecklistTaskDraft {
  id: string;
  text: string;
}

const TELEGRAM_REACTION_EMOJIS = [
  '👍', '👎', '❤', '🔥', '🎉', '😁', '🤔', '😢', '😱', '👏', '🤩', '🙏', '👌', '🤣', '💯', '⚡',
  '💔', '🥰', '🤬', '🤯', '🤮', '🥱', '😈', '😎', '🗿', '🆒', '😘', '👀', '🤝', '🍾',
];
const PAID_REACTION_KEY = '__paid_star__';
const PAID_REACTION_GLYPH = '⭐';

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

function reactionKeyFromTypePayload(raw: unknown): string | null {
  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const payload = raw as Record<string, unknown>;
  const kind = typeof payload.type === 'string'
    ? payload.type.toLowerCase()
    : 'emoji';
  if (kind === 'paid') {
    return PAID_REACTION_KEY;
  }
  if (kind !== 'emoji') {
    return null;
  }

  const emoji = typeof payload.emoji === 'string'
    ? payload.emoji.trim()
    : '';
  return emoji.length > 0 ? emoji : null;
}

function reactionKeyToPayload(key: string): { type: 'emoji'; emoji: string } | { type: 'paid' } {
  if (key === PAID_REACTION_KEY) {
    return { type: 'paid' };
  }
  return { type: 'emoji', emoji: key };
}

function renderReactionLabel(key: string): string {
  return key === PAID_REACTION_KEY ? PAID_REACTION_GLYPH : key;
}

function optionalTrimmedText(value?: string | null): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function nonNegativeInteger(value: unknown, fallback = 0): number {
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

function toDatetimeLocalInputValue(epochSeconds?: number): string {
  if (!epochSeconds || !Number.isFinite(epochSeconds) || epochSeconds <= 0) {
    return '';
  }

  const date = new Date(epochSeconds * 1000);
  const timezoneOffsetMs = date.getTimezoneOffset() * 60_000;
  const localDate = new Date(date.getTime() - timezoneOffsetMs);
  return localDate.toISOString().slice(0, 16);
}

function formatDurationShort(totalSeconds: number): string {
  const seconds = Math.max(Math.floor(totalSeconds), 0);
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  const remSeconds = seconds % 60;

  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${remSeconds}s`;
  }
  return `${remSeconds}s`;
}

function normalizeSimUser(raw?: Partial<SimUser> | null): SimUser {
  const id = nonNegativeInteger(raw?.id, DEFAULT_USER.id) || DEFAULT_USER.id;
  const firstName = optionalTrimmedText(raw?.first_name) || `User ${id}`;

  return {
    id,
    first_name: firstName,
    username: optionalTrimmedText(raw?.username),
    last_name: optionalTrimmedText(raw?.last_name),
    phone_number: optionalTrimmedText(raw?.phone_number),
    photo_url: optionalTrimmedText(raw?.photo_url),
    bio: optionalTrimmedText(raw?.bio),
    is_premium: typeof raw?.is_premium === 'boolean' ? raw.is_premium : false,
    business_name: optionalTrimmedText(raw?.business_name),
    business_intro: optionalTrimmedText(raw?.business_intro),
    business_location: optionalTrimmedText(raw?.business_location),
    gift_count: nonNegativeInteger(raw?.gift_count, 0),
    is_verified: typeof raw?.is_verified === 'boolean' ? raw.is_verified : false,
    verification_description: optionalTrimmedText(raw?.verification_description),
  };
}

function formatSimUserDisplayName(user: SimUser): string {
  const lastName = optionalTrimmedText(user.last_name);
  return lastName ? `${user.first_name} ${lastName}` : user.first_name;
}

function simUserAvatarInitials(user: SimUser): string {
  const parts = [user.first_name, optionalTrimmedText(user.last_name) || '']
    .map((item) => item.trim())
    .filter(Boolean);
  if (parts.length === 0) {
    return 'U';
  }
  if (parts.length === 1) {
    return parts[0].slice(0, 2).toUpperCase();
  }
  return `${parts[0][0]}${parts[1][0]}`.toUpperCase();
}

function emptyUserDraft(): UserDraftState {
  return {
    first_name: '',
    last_name: '',
    username: '',
    id: '',
    phone_number: '',
    photo_url: '',
    bio: '',
    is_premium: false,
    business_name: '',
    business_intro: '',
    business_location: '',
    gift_count: '0',
  };
}

function buildUserDraftFromSimUser(user: SimUser): UserDraftState {
  const normalized = normalizeSimUser(user);
  return {
    first_name: normalized.first_name,
    last_name: normalized.last_name || '',
    username: normalized.username || '',
    id: String(normalized.id),
    phone_number: normalized.phone_number || '',
    photo_url: normalized.photo_url || '',
    bio: normalized.bio || '',
    is_premium: Boolean(normalized.is_premium),
    business_name: normalized.business_name || '',
    business_intro: normalized.business_intro || '',
    business_location: normalized.business_location || '',
    gift_count: String(nonNegativeInteger(normalized.gift_count, 0)),
  };
}

function randomUserDraft(nextId: number): UserDraftState {
  const seed = Math.random().toString(36).slice(2, 7);
  return {
    first_name: `Test User ${Math.floor(Math.random() * 900 + 100)}`,
    last_name: `Profile ${Math.floor(Math.random() * 900 + 100)}`,
    username: `test_user_${seed}`,
    id: String(nextId),
    phone_number: `+1000${Math.floor(Math.random() * 9000000 + 1000000)}`,
    photo_url: '',
    bio: 'Telegram-like simulator user profile.',
    is_premium: false,
    business_name: '',
    business_intro: '',
    business_location: '',
    gift_count: '0',
  };
}

function normalizeWalletState(raw?: Partial<WalletState>): WalletState {
  const fiat = Math.floor(Number(raw?.fiat));
  const stars = Math.floor(Number(raw?.stars));
  return {
    fiat: Number.isFinite(fiat) && fiat >= 0 ? fiat : DEFAULT_WALLET_STATE.fiat,
    stars: Number.isFinite(stars) && stars >= 0 ? stars : DEFAULT_WALLET_STATE.stars,
  };
}

function randomBotIdentityDraft(): Pick<BotDraftState, 'first_name' | 'username'> {
  return {
    first_name: `Simula Bot ${Math.floor(Math.random() * 9000 + 1000)}`,
    username: `simula_${Math.random().toString(36).slice(2, 8)}`,
  };
}

function defaultBotAdminRightsDraft(): BotAdminRightsDraft {
  return {
    isAnonymous: false,
    canManageChat: false,
    canDeleteMessages: false,
    canManageVideoChats: false,
    canRestrictMembers: false,
    canPromoteMembers: false,
    canChangeInfo: false,
    canInviteUsers: false,
    canPostStories: false,
    canEditStories: false,
    canDeleteStories: false,
    canPostMessages: false,
    canEditMessages: false,
    canPinMessages: false,
    canManageTopics: false,
    canManageDirectMessages: false,
    canManageTags: false,
  };
}

function emptyBotDraft(): BotDraftState {
  return {
    first_name: '',
    username: '',
    description: '',
    short_description: '',
    profile_photo_ref: '',
    remove_profile_photo: false,
    commands_text: '',
    commands_language_code: '',
    group_default_admin_rights: defaultBotAdminRightsDraft(),
    channel_default_admin_rights: defaultBotAdminRightsDraft(),
  };
}

function defaultManagedBotSettings(): ManagedBotSettingsState {
  return {
    enabled: true,
  };
}

function defaultSidebarSections(): SidebarSectionState {
  return {
    chats: true,
    privateChats: true,
    groupChats: true,
    channelChats: true,
    users: true,
    bots: true,
    debugger: true,
    settings: true,
  };
}

function normalizeRuntimeEnvValues(raw: unknown): Record<string, string> {
  if (!raw || typeof raw !== 'object') {
    return {};
  }

  const values: Record<string, string> = {};
  Object.entries(raw as Record<string, unknown>).forEach(([key, value]) => {
    const normalizedKey = key.trim();
    if (!normalizedKey) {
      return;
    }
    values[normalizedKey] = String(value ?? '');
  });
  return values;
}

function buildRuntimeEnvRows(values: Record<string, string>): RuntimeEnvRow[] {
  const rows = Object.entries(values)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value], index) => ({
      id: `${key}-${index}`,
      key,
      value,
    }));

  return rows.length > 0 ? rows : [{ id: `env-${Date.now()}`, key: '', value: '' }];
}

function normalizeManagedBotUsernameDraft(raw: string): string {
  return raw.trim().replace(/^@+/, '').replace(/\s+/g, '');
}

function formatBotCommandsForEditor(commands: GeneratedBotCommand[]): string {
  if (!Array.isArray(commands) || commands.length === 0) {
    return '';
  }
  return commands
    .map((command) => `/${command.command} - ${command.description}`)
    .join('\n');
}

function parseBotCommandsFromEditor(raw: string): GeneratedBotCommand[] {
  const lines = raw.split(/\r?\n/);
  const parsedCommands: GeneratedBotCommand[] = [];
  const seenCommands = new Set<string>();

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index].trim();
    if (!line || line.startsWith('#')) {
      continue;
    }

    const match = line.match(/^\/?([A-Za-z0-9_]{1,32})\s*(?:-|:)\s*(.+)$/);
    if (!match) {
      throw new Error(`Invalid command at line ${index + 1}. Use format: /command - Description`);
    }

    const command = match[1].toLowerCase();
    const description = match[2].trim();

    if (!/^[a-z0-9_]{1,32}$/.test(command)) {
      throw new Error(`Command \"${command}\" is invalid. Use lowercase letters, numbers, and underscore only.`);
    }
    if (!description) {
      throw new Error(`Command description is missing at line ${index + 1}.`);
    }
    if (description.length > 256) {
      throw new Error(`Command description at line ${index + 1} exceeds 256 characters.`);
    }
    if (seenCommands.has(command)) {
      throw new Error(`Duplicate command \"${command}\" in command list.`);
    }

    seenCommands.add(command);
    parsedCommands.push({
      command,
      description,
    });
  }

  return parsedCommands;
}

function mapChatAdminRightsToDraft(rights?: Partial<GeneratedChatAdministratorRights> | null): BotAdminRightsDraft {
  const defaults = defaultBotAdminRightsDraft();
  if (!rights) {
    return defaults;
  }

  return {
    isAnonymous: Boolean(rights.is_anonymous),
    canManageChat: Boolean(rights.can_manage_chat),
    canDeleteMessages: Boolean(rights.can_delete_messages),
    canManageVideoChats: Boolean(rights.can_manage_video_chats),
    canRestrictMembers: Boolean(rights.can_restrict_members),
    canPromoteMembers: Boolean(rights.can_promote_members),
    canChangeInfo: Boolean(rights.can_change_info),
    canInviteUsers: Boolean(rights.can_invite_users),
    canPostStories: Boolean(rights.can_post_stories),
    canEditStories: Boolean(rights.can_edit_stories),
    canDeleteStories: Boolean(rights.can_delete_stories),
    canPostMessages: Boolean(rights.can_post_messages),
    canEditMessages: Boolean(rights.can_edit_messages),
    canPinMessages: Boolean(rights.can_pin_messages),
    canManageTopics: Boolean(rights.can_manage_topics),
    canManageDirectMessages: Boolean(rights.can_manage_direct_messages),
    canManageTags: Boolean(rights.can_manage_tags),
  };
}

function mapBotAdminRightsDraftToServer(
  draft: BotAdminRightsDraft,
  forChannels: boolean,
): GeneratedChatAdministratorRights {
  return {
    is_anonymous: draft.isAnonymous,
    can_manage_chat: draft.canManageChat,
    can_delete_messages: draft.canDeleteMessages,
    can_manage_video_chats: draft.canManageVideoChats,
    can_restrict_members: draft.canRestrictMembers,
    can_promote_members: draft.canPromoteMembers,
    can_change_info: draft.canChangeInfo,
    can_invite_users: draft.canInviteUsers,
    can_post_stories: draft.canPostStories,
    can_edit_stories: draft.canEditStories,
    can_delete_stories: draft.canDeleteStories,
    can_post_messages: forChannels ? draft.canPostMessages : undefined,
    can_edit_messages: forChannels ? draft.canEditMessages : undefined,
    can_pin_messages: forChannels ? undefined : draft.canPinMessages,
    can_manage_topics: forChannels ? undefined : draft.canManageTopics,
    can_manage_direct_messages: draft.canManageDirectMessages,
    can_manage_tags: draft.canManageTags,
  };
}

function normalizeOwnedGiftRegular(raw: Record<string, unknown>): GeneratedOwnedGiftRegular | null {
  const type = typeof raw.type === 'string' ? raw.type : '';
  const sendDate = Number(raw.send_date);
  const gift = raw.gift;

  if (type !== 'regular' || !gift || typeof gift !== 'object') {
    return null;
  }
  if (!Number.isFinite(sendDate) || sendDate <= 0) {
    return null;
  }

  return raw as unknown as GeneratedOwnedGiftRegular;
}

function extractGiftEmoji(gift: GeneratedGift): string {
  const emoji = gift?.sticker?.emoji;
  return typeof emoji === 'string' && emoji.trim().length > 0 ? emoji.trim() : '🎁';
}

function formatGiftSendDate(sendDate?: number): string {
  if (!Number.isFinite(sendDate)) {
    return 'unknown date';
  }
  return new Date(Number(sendDate) * 1000).toLocaleString();
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
  isVerified?: boolean;
  verificationDescription?: string;
  description?: string;
  isForum?: boolean;
  isDirectMessages?: boolean;
  parentChannelChatId?: number;
  linkedDiscussionChatId?: number;
  settings?: GroupSettingsSnapshot;
}

interface GroupSettingsSnapshot {
  showAuthorSignature: boolean;
  paidStarReactionsEnabled: boolean;
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
    paidStarReactionsEnabled: false,
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
  paid_star_reactions_enabled?: boolean;
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
    paidStarReactionsEnabled: raw?.paid_star_reactions_enabled ?? defaults.paidStarReactionsEnabled,
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
  const [isSidebarPanelOpen, setIsSidebarPanelOpen] = useState(true);
  const [sidebarSections, setSidebarSections] = useState<SidebarSectionState>(() => {
    try {
      const raw = localStorage.getItem(SIDEBAR_SECTIONS_KEY);
      const parsed = raw ? (JSON.parse(raw) as Partial<SidebarSectionState>) : {};
      const defaults = defaultSidebarSections();
      return {
        chats: typeof parsed.chats === 'boolean' ? parsed.chats : defaults.chats,
        privateChats: typeof parsed.privateChats === 'boolean' ? parsed.privateChats : defaults.privateChats,
        groupChats: typeof parsed.groupChats === 'boolean' ? parsed.groupChats : defaults.groupChats,
        channelChats: typeof parsed.channelChats === 'boolean' ? parsed.channelChats : defaults.channelChats,
        users: typeof parsed.users === 'boolean' ? parsed.users : defaults.users,
        bots: typeof parsed.bots === 'boolean' ? parsed.bots : defaults.bots,
        debugger: typeof parsed.debugger === 'boolean' ? parsed.debugger : defaults.debugger,
        settings: typeof parsed.settings === 'boolean' ? parsed.settings : defaults.settings,
      };
    } catch {
      return defaultSidebarSections();
    }
  });
  const [debugEventLogs, setDebugEventLogs] = useState<DebugEventLog[]>([]);
  const [debuggerSearch, setDebuggerSearch] = useState('');
  const [debuggerStatusFilter, setDebuggerStatusFilter] = useState<'all' | 'ok' | 'error'>('all');
  const [debuggerSourceFilter, setDebuggerSourceFilter] = useState<'all' | 'bot' | 'webhook'>('all');
  const [isDebuggerHistoryExpanded, setIsDebuggerHistoryExpanded] = useState(true);
  const [serverHealth, setServerHealth] = useState<{
    status: 'checking' | 'online' | 'offline';
    checkedAt?: number;
    error?: string;
  }>({ status: 'checking' });
  const [runtimeInfo, setRuntimeInfo] = useState<RuntimeInfoState | null>(null);
  const [runtimeEnvRows, setRuntimeEnvRows] = useState<RuntimeEnvRow[]>([]);
  const [runtimeEnvSource, setRuntimeEnvSource] = useState<Record<string, string>>({});
  const [isRuntimeEnvSaving, setIsRuntimeEnvSaving] = useState(false);
  const [runtimeServiceActionInFlight, setRuntimeServiceActionInFlight] = useState<'' | 'start' | 'stop' | 'restart'>('');
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
      const parsed = raw ? (JSON.parse(raw) as Partial<SimUser>[]) : [];
      const byId = new Map<number, SimUser>();
      parsed.forEach((user) => {
        const normalized = normalizeSimUser(user);
        byId.set(normalized.id, normalized);
      });
      const hydrated = Array.from(byId.values());
      return hydrated.length > 0 ? hydrated : [normalizeSimUser(DEFAULT_USER)];
    } catch {
      return [normalizeSimUser(DEFAULT_USER)];
    }
  });
  const [selectedUserId, setSelectedUserId] = useState<number>(() => {
    const raw = localStorage.getItem(SELECTED_USER_KEY);
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_USER.id;
  });
  const [chatSearch, setChatSearch] = useState('');
  const [usersTabSearch, setUsersTabSearch] = useState('');
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
    paidStarReactionsEnabled: false,
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
  const [showUserProfileModal, setShowUserProfileModal] = useState(false);
  const [managedBotSettingsByToken, setManagedBotSettingsByToken] = useState<Record<string, ManagedBotSettingsState>>(() => {
    try {
      const raw = localStorage.getItem(MANAGED_BOT_SETTINGS_BY_BOT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, Partial<ManagedBotSettingsState>>) : {};
      const normalized: Record<string, ManagedBotSettingsState> = {};
      Object.entries(parsed).forEach(([token, value]) => {
        normalized[token] = {
          enabled: typeof value?.enabled === 'boolean' ? value.enabled : true,
        };
      });
      return normalized;
    } catch {
      return {};
    }
  });
  const [managedBotOwnerDraft, setManagedBotOwnerDraft] = useState(String(DEFAULT_USER.id));
  const [isManagedBotTokenActionRunning, setIsManagedBotTokenActionRunning] = useState(false);
  const [managedBotRequestModal, setManagedBotRequestModal] = useState<ManagedBotRequestModalState | null>(null);
  const [miniAppModal, setMiniAppModal] = useState<MiniAppModalState | null>(null);
  const [emojiStatusDraft, setEmojiStatusDraft] = useState('');
  const [emojiStatusExpirationDraft, setEmojiStatusExpirationDraft] = useState('');
  const [profilePhotoUrlDraft, setProfilePhotoUrlDraft] = useState('');
  const [profileAudioTitleDraft, setProfileAudioTitleDraft] = useState('');
  const [profileAudioPerformerDraft, setProfileAudioPerformerDraft] = useState('');
  const [profileAudioFileDraft, setProfileAudioFileDraft] = useState<File | null>(null);
  const [userEmojiStatusByKey, setUserEmojiStatusByKey] = useState<Record<string, UserEmojiStatusState>>(() => {
    try {
      const raw = localStorage.getItem(USER_EMOJI_STATUS_BY_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, Partial<UserEmojiStatusState>>) : {};
      const normalized: Record<string, UserEmojiStatusState> = {};
      Object.entries(parsed).forEach(([key, value]) => {
        const customEmojiId = typeof value?.customEmojiId === 'string' ? value.customEmojiId.trim() : '';
        if (!customEmojiId) {
          return;
        }
        const expirationDate = Math.floor(Number(value?.expirationDate));
        normalized[key] = {
          customEmojiId,
          expirationDate: Number.isFinite(expirationDate) && expirationDate > 0 ? expirationDate : undefined,
        };
      });
      return normalized;
    } catch {
      return {};
    }
  });
  const [chatBoostCountByActorChatKey, setChatBoostCountByActorChatKey] = useState<Record<string, number>>(() => {
    try {
      const raw = localStorage.getItem(CHAT_BOOST_COUNTS_BY_ACTOR_CHAT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, unknown>) : {};
      return Object.entries(parsed).reduce<Record<string, number>>((acc, [key, value]) => {
        const count = Math.floor(Number(value));
        if (Number.isFinite(count) && count > 0) {
          acc[key] = count;
        }
        return acc;
      }, {});
    } catch {
      return {};
    }
  });
  const [chatBoostModal, setChatBoostModal] = useState<ChatBoostModalState | null>(null);
  const [emojiClockNow, setEmojiClockNow] = useState(() => Math.floor(Date.now() / 1000));
  const [userProfilePhotos, setUserProfilePhotos] = useState<GeneratedUserProfilePhotos | null>(null);
  const [userProfileAudios, setUserProfileAudios] = useState<GeneratedUserProfileAudios | null>(null);
  const [userChatBoosts, setUserChatBoosts] = useState<GeneratedUserChatBoosts | null>(null);
  const [isUserProfileDataLoading, setIsUserProfileDataLoading] = useState(false);
  const [isBoostActionRunning, setIsBoostActionRunning] = useState(false);
  const [botModalMode, setBotModalMode] = useState<BotModalMode>('create');
  const [userModalMode, setUserModalMode] = useState<UserModalMode>('create');
  const [botDraft, setBotDraft] = useState<BotDraftState>(() => emptyBotDraft());
  const [botManagedEnabledDraft, setBotManagedEnabledDraft] = useState(true);
  const [isBotModalLoading, setIsBotModalLoading] = useState(false);
  const [botDefaultCommandsByToken, setBotDefaultCommandsByToken] = useState<Record<string, GeneratedBotCommand[]>>({});
  const [isBotCommandsLoading, setIsBotCommandsLoading] = useState(false);
  const [userDraft, setUserDraft] = useState<UserDraftState>(() => emptyUserDraft());
  const [composerEditTarget, setComposerEditTarget] = useState<ChatMessage | null>(null);
  const [replyTarget, setReplyTarget] = useState<ChatMessage | null>(null);
  const [commentSourceByDiscussionChatKey, setCommentSourceByDiscussionChatKey] = useState<
    Record<string, { channelChatId: number; channelMessageId: number; discussionRootMessageId?: number }>
  >({});
  const [messageMenu, setMessageMenu] = useState<{ messageId: number; x: number; y: number } | null>(null);
  const [paidReactionModal, setPaidReactionModal] = useState<{
    chatId: number;
    messageId: number;
    currentPaidCount: number;
  } | null>(null);
  const [paidReactionAmountDraft, setPaidReactionAmountDraft] = useState('1');
  const [isPaidReactionSubmitting, setIsPaidReactionSubmitting] = useState(false);
  const [forwardMessageId, setForwardMessageId] = useState<number | null>(null);
  const [forwardTargetChatId, setForwardTargetChatId] = useState('');
  const [keyboardRequestUsersModal, setKeyboardRequestUsersModal] = useState<KeyboardRequestUsersModalState | null>(null);
  const [keyboardRequestChatModal, setKeyboardRequestChatModal] = useState<KeyboardRequestChatModalState | null>(null);
  const [chatMenuOpen, setChatMenuOpen] = useState(false);
  const [selectionMode, setSelectionMode] = useState(false);
  const [selectedMessageIds, setSelectedMessageIds] = useState<number[]>([]);
  const [copiedToken, setCopiedToken] = useState(false);
  const [selectedUploads, setSelectedUploads] = useState<File[]>([]);
  const [uploadAsPaidMedia, setUploadAsPaidMedia] = useState(false);
  const [uploadPaidStarCountDraft, setUploadPaidStarCountDraft] = useState('25');
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
  const [giftCatalog, setGiftCatalog] = useState<GeneratedGift[]>([]);
  const [ownedRegularGifts, setOwnedRegularGifts] = useState<GeneratedOwnedGiftRegular[]>([]);
  const [selectedGiftId, setSelectedGiftId] = useState('');
  const [giftMessageDraft, setGiftMessageDraft] = useState('');
  const [giftPayForUpgrade, setGiftPayForUpgrade] = useState(false);
  const [isGiftCatalogLoading, setIsGiftCatalogLoading] = useState(false);
  const [isOwnedGiftsLoading, setIsOwnedGiftsLoading] = useState(false);
  const [isGiftActionLoading, setIsGiftActionLoading] = useState(false);
  const [deletingOwnedGiftId, setDeletingOwnedGiftId] = useState<string | null>(null);
  const [purchasingPaidMediaMessageId, setPurchasingPaidMediaMessageId] = useState<number | null>(null);
  const [suggestedPostActionMessageId, setSuggestedPostActionMessageId] = useState<number | null>(null);
  const [declineSuggestedPostModal, setDeclineSuggestedPostModal] = useState<DeclineSuggestedPostModalState | null>(null);
  const [paidMediaPurchaseByActorKey, setPaidMediaPurchaseByActorKey] = useState<Record<string, boolean>>(() => {
    try {
      const raw = localStorage.getItem(PAID_MEDIA_PURCHASES_KEY);
      if (!raw) {
        return {};
      }
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      return Object.entries(parsed).reduce<Record<string, boolean>>((acc, [key, value]) => {
        if (typeof key === 'string' && typeof value === 'boolean') {
          acc[key] = value;
        }
        return acc;
      }, {});
    } catch {
      return {};
    }
  });
  const [giftPanelError, setGiftPanelError] = useState('');
  const [giftReloadTick, setGiftReloadTick] = useState(0);
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
  const [checklistBuilder, setChecklistBuilder] = useState({
    title: '',
    tasks: [
      { id: '1', text: 'First task' },
      { id: '2', text: 'Second task' },
    ] as ChecklistTaskDraft[],
    othersCanAddTasks: false,
    othersCanMarkTasksAsDone: true,
  });
  const [lastChecklistMessageIdDraft, setLastChecklistMessageIdDraft] = useState('');
  const [webAppLab, setWebAppLab] = useState({
    lastQueryId: '',
    answerMessageText: 'Mini App result from Simula',
    answerTitle: 'Mini App Result',
    answerDescription: '',
    answerUrl: '',
    preparedInlineText: 'Prepared inline message from Mini App',
    preparedInlineTitle: 'Prepared Inline',
    preparedButtonText: 'Open Mini App',
    preparedButtonUrl: 'https://example.com/mini-app',
  });
  const [lastPreparedInlineMessageId, setLastPreparedInlineMessageId] = useState('');
  const [lastPreparedKeyboardButtonId, setLastPreparedKeyboardButtonId] = useState('');
  const [isWebAppLabRunning, setIsWebAppLabRunning] = useState(false);
  const [storyBuilder, setStoryBuilder] = useState({
    mode: 'post' as 'post' | 'edit',
    contentType: 'photo' as 'photo' | 'video',
    contentRef: '',
    activePeriod: '86400',
    caption: '',
    storyId: '',
    fromChatId: '',
    fromStoryId: '',
    areasJson: '',
  });
  const [storyBuilderFile, setStoryBuilderFile] = useState<File | null>(null);
  const [isStoryActionRunning, setIsStoryActionRunning] = useState(false);
  const [showStoryComposerModal, setShowStoryComposerModal] = useState(false);
  const [storyShelf, setStoryShelf] = useState<Record<string, StoryShelfEntry>>(() => {
    try {
      const raw = localStorage.getItem(STORY_SHELF_BY_BOT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, Record<string, StoryShelfEntry>>) : {};
      return parsed[selectedBotToken] || {};
    } catch {
      return {};
    }
  });
  const [hiddenStoryKeys, setHiddenStoryKeys] = useState<Record<string, true>>(() => {
    try {
      const raw = localStorage.getItem(HIDDEN_STORY_KEYS_BY_BOT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, Record<string, true>>) : {};
      return parsed[selectedBotToken] || {};
    } catch {
      return {};
    }
  });
  const [activeStoryPreviewKey, setActiveStoryPreviewKey] = useState<string | null>(null);
  const [suggestedPostComposer, setSuggestedPostComposer] = useState({
    enabled: false,
    priceCurrency: 'XTR' as 'XTR' | 'TON',
    priceAmount: '100',
    sendDate: '',
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
  const [showStickerStudioPanel, setShowStickerStudioPanel] = useState(false);
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
  const selectedBotDefaultCommands = botDefaultCommandsByToken[selectedBotToken] || [];
  const activeManagedBotSettings = managedBotSettingsByToken[selectedBotToken] || defaultManagedBotSettings();

  const selectedUser = useMemo(
    () => availableUsers.find((user) => user.id === selectedUserId) || DEFAULT_USER,
    [availableUsers, selectedUserId],
  );
  const selectedUserDisplayName = useMemo(() => formatSimUserDisplayName(selectedUser), [selectedUser]);
  const selectedUserSecondaryLine = useMemo(() => {
    const identity = selectedUser.username ? `@${selectedUser.username}` : `id ${selectedUser.id}`;
    const phone = optionalTrimmedText(selectedUser.phone_number);
    if (phone) {
      return `${identity} · ${phone}`;
    }
    return identity;
  }, [selectedUser]);
  const selectedUserEmojiStatusKey = `${selectedBotToken}:${selectedUser.id}`;
  const selectedUserEmojiStatus = useMemo(() => {
    const status = userEmojiStatusByKey[selectedUserEmojiStatusKey];
    if (!status?.customEmojiId) {
      return null;
    }
    if (typeof status.expirationDate === 'number' && status.expirationDate <= emojiClockNow) {
      return null;
    }
    return status;
  }, [emojiClockNow, selectedUserEmojiStatusKey, userEmojiStatusByKey]);
  const selectedUserEmojiStatusRemainingText = useMemo(() => {
    if (!selectedUserEmojiStatus?.expirationDate) {
      return 'no expiration';
    }
    const remaining = selectedUserEmojiStatus.expirationDate - emojiClockNow;
    if (remaining <= 0) {
      return 'expired';
    }
    return formatDurationShort(remaining);
  }, [emojiClockNow, selectedUserEmojiStatus?.expirationDate]);
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
  const selectedBusinessRights = useMemo(
    () => Object.entries(selectedBusinessConnection?.rights || {})
      .filter(([, value]) => value === true)
      .map(([key]) => key.replace(/^can_/, '')),
    [selectedBusinessConnection],
  );
  const activeBusinessConnectionId = chatScopeTab === 'private' && selectedBusinessConnection?.is_enabled
    ? selectedBusinessConnection.id
    : undefined;

  useEffect(() => {
    if (chatScopeTab !== 'private') {
      setShowUserProfileModal(false);
    }
  }, [chatScopeTab]);

  useEffect(() => {
    setManagedBotOwnerDraft(String(selectedUser.id));
  }, [selectedUser.id]);

  useEffect(() => {
    if (!showUserProfileModal || chatScopeTab !== 'private') {
      return;
    }

    let cancelled = false;
    setIsUserProfileDataLoading(true);
    setErrorText('');

    void (async () => {
      try {
        const [photos, audios] = await Promise.all([
          getUserProfilePhotos(selectedBotToken, {
            user_id: selectedUser.id,
            limit: 100,
          }),
          getUserProfileAudios(selectedBotToken, {
            user_id: selectedUser.id,
            limit: 100,
          }),
        ]);

        if (cancelled) {
          return;
        }

        setUserProfilePhotos(photos);
        setUserProfileAudios(audios);
      } catch (error) {
        if (!cancelled) {
          setErrorText(error instanceof Error ? error.message : 'Failed to load profile media');
        }
      } finally {
        if (!cancelled) {
          setIsUserProfileDataLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [showUserProfileModal, chatScopeTab, selectedBotToken, selectedUser.id]);

  useEffect(() => {
    setUserChatBoosts(null);
  }, [selectedBotToken, selectedUser.id]);

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

  const selectedGroup = useMemo(
    () => groupChats.find((group) => group.id === selectedGroupChatId) || null,
    [groupChats, selectedGroupChatId],
  );

  const selectedActorChatBoostKey = selectedGroup
    ? `${selectedBotToken}:${selectedGroup.id}:${selectedUser.id}`
    : null;
  const selectedActorChatBoostCount = selectedActorChatBoostKey
    ? (chatBoostCountByActorChatKey[selectedActorChatBoostKey] || 0)
    : 0;

  useEffect(() => {
    if (chatScopeTab !== 'group' && chatScopeTab !== 'channel') {
      return;
    }
    if (!selectedGroup || selectedGroup.isDirectMessages) {
      return;
    }

    const targetChatId = selectedGroup.id;
    const membershipKey = `${selectedBotToken}:${targetChatId}:${selectedUser.id}`;
    const membershipStatus = groupMembershipByUser[membershipKey];
    const isJoined = isJoinedMembershipStatus(membershipStatus);

    if (!isJoined) {
      syncChatBoostCount(targetChatId, selectedUser.id, 0);
      setUserChatBoosts(null);
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const boosts = await getUserChatBoosts(selectedBotToken, {
          chat_id: targetChatId,
          user_id: selectedUser.id,
        }, selectedUser.id);

        if (cancelled) {
          return;
        }

        setUserChatBoosts(boosts);
        syncChatBoostCount(targetChatId, selectedUser.id, boosts.boosts.length);
      } catch {
        if (cancelled) {
          return;
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [
    chatScopeTab,
    groupMembershipByUser,
    selectedBotToken,
    selectedGroup?.id,
    selectedGroup?.isDirectMessages,
    selectedUser.id,
  ]);

  useEffect(() => {
    if (!showUserProfileModal || chatScopeTab !== 'private') {
      return;
    }

    setProfilePhotoUrlDraft(selectedUser.photo_url || '');
    setProfileAudioTitleDraft(selectedUserDisplayName ? `${selectedUserDisplayName} profile audio` : 'Profile audio');
    setProfileAudioPerformerDraft(selectedUserDisplayName || selectedUser.first_name);
    setProfileAudioFileDraft(null);

    const currentEmojiStatus = userEmojiStatusByKey[selectedUserEmojiStatusKey];
    const nowUnix = Math.floor(Date.now() / 1000);
    if (currentEmojiStatus?.customEmojiId && (!currentEmojiStatus.expirationDate || currentEmojiStatus.expirationDate > nowUnix)) {
      setEmojiStatusDraft(currentEmojiStatus.customEmojiId);
      setEmojiStatusExpirationDraft(toDatetimeLocalInputValue(currentEmojiStatus.expirationDate));
    } else {
      setEmojiStatusDraft('');
      setEmojiStatusExpirationDraft('');
    }
  }, [
    chatScopeTab,
    selectedUser.id,
    selectedUser.first_name,
    selectedUser.photo_url,
    selectedUserDisplayName,
    selectedUserEmojiStatusKey,
    showUserProfileModal,
    userEmojiStatusByKey,
  ]);

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
  const canManageSuggestedPostsInSelectedChat = Boolean(
    selectedGroup?.isDirectMessages
    && isSelectedUserDirectMessagesManager,
  );
  const canCreateSuggestedPostInSelectedChat = Boolean(
    selectedGroup?.isDirectMessages
    && !isSelectedUserDirectMessagesManager,
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
  const isGiftPanelOpen = showMediaDrawer && mediaDrawerTab === 'gifts';
  const selectedGift = useMemo(
    () => giftCatalog.find((gift) => gift.id === selectedGiftId) || giftCatalog[0] || null,
    [giftCatalog, selectedGiftId],
  );
  const selectedGiftChargeEstimate = selectedGift
    ? selectedGift.star_count + (giftPayForUpgrade ? (selectedGift.upgrade_star_count || 0) : 0)
    : 0;
  const canSendGiftInCurrentScope = chatScopeTab === 'private'
    || (chatScopeTab === 'channel' && selectedGroup?.type === 'channel');

  useEffect(() => {
    if (giftCatalog.length === 0) {
      if (selectedGiftId) {
        setSelectedGiftId('');
      }
      return;
    }

    if (!giftCatalog.some((gift) => gift.id === selectedGiftId)) {
      setSelectedGiftId(giftCatalog[0].id);
    }
  }, [giftCatalog, selectedGiftId]);

  useEffect(() => {
    if (!isGiftPanelOpen || !selectedBotToken) {
      return;
    }

    let cancelled = false;

    const loadGiftCatalog = async () => {
      setIsGiftCatalogLoading(true);
      setGiftPanelError('');
      try {
        const response = await getAvailableGifts(selectedBotToken);
        if (cancelled) {
          return;
        }

        setGiftCatalog(Array.isArray(response.gifts) ? response.gifts : []);
      } catch (error) {
        if (!cancelled) {
          setGiftCatalog([]);
          setGiftPanelError(error instanceof Error ? error.message : 'Failed to load available gifts');
        }
      } finally {
        if (!cancelled) {
          setIsGiftCatalogLoading(false);
        }
      }
    };

    void loadGiftCatalog();

    return () => {
      cancelled = true;
    };
  }, [isGiftPanelOpen, selectedBotToken]);

  useEffect(() => {
    if (!isGiftPanelOpen || !selectedBotToken) {
      return;
    }

    let cancelled = false;

    const loadOwnedGifts = async () => {
      setIsOwnedGiftsLoading(true);
      try {
        let regularGifts: GeneratedOwnedGiftRegular[] = [];

        if (chatScopeTab === 'private') {
          const response = await getUserGifts(selectedBotToken, {
            user_id: selectedUser.id,
            limit: 20,
          });
          regularGifts = response.gifts
            .map((gift) => normalizeOwnedGiftRegular(gift as Record<string, unknown>))
            .filter((gift): gift is GeneratedOwnedGiftRegular => Boolean(gift));
        } else if (selectedGroup) {
          const response = await getChatGifts(selectedBotToken, {
            chat_id: selectedChatId,
            limit: 20,
          });
          regularGifts = response.gifts
            .map((gift) => normalizeOwnedGiftRegular(gift as Record<string, unknown>))
            .filter((gift): gift is GeneratedOwnedGiftRegular => Boolean(gift));
        }

        if (cancelled) {
          return;
        }

        setOwnedRegularGifts(regularGifts);
      } catch (error) {
        if (!cancelled) {
          setOwnedRegularGifts([]);
          setGiftPanelError(error instanceof Error ? error.message : 'Failed to load owned gifts');
        }
      } finally {
        if (!cancelled) {
          setIsOwnedGiftsLoading(false);
        }
      }
    };

    void loadOwnedGifts();

    return () => {
      cancelled = true;
    };
  }, [
    chatScopeTab,
    giftReloadTick,
    isGiftPanelOpen,
    selectedBotToken,
    selectedChatId,
    selectedGroup,
    selectedUser.id,
  ]);

  const sendSelectedGiftFromDrawer = async () => {
    if (!selectedBotToken || !selectedGift) {
      return;
    }

    if (!canSendGiftInCurrentScope) {
      setGiftPanelError('Gifts can only be sent to users and channel chats.');
      return;
    }

    setIsGiftActionLoading(true);
    setGiftPanelError('');
    try {
      const text = giftMessageDraft.trim();
      if (chatScopeTab === 'private') {
        await sendGift(
          selectedBotToken,
          {
            user_id: selectedUser.id,
            gift_id: selectedGift.id,
            pay_for_upgrade: giftPayForUpgrade || undefined,
            text: text || undefined,
          },
          selectedUser.id,
        );

        setAvailableUsers((prev) => prev.map((user) => {
          if (user.id !== selectedUser.id) {
            return user;
          }
          return {
            ...user,
            gift_count: nonNegativeInteger(user.gift_count, 0) + 1,
          };
        }));
      } else if (selectedGroup) {
        await sendGift(
          selectedBotToken,
          {
            chat_id: selectedGroup.id,
            gift_id: selectedGift.id,
            pay_for_upgrade: giftPayForUpgrade || undefined,
            text: text || undefined,
          },
          selectedUser.id,
        );
      }

      setGiftMessageDraft('');
      setGiftPayForUpgrade(false);
      setGiftReloadTick((prev) => prev + 1);
    } catch (error) {
      setGiftPanelError(error instanceof Error ? error.message : 'Failed to send gift');
    } finally {
      setIsGiftActionLoading(false);
    }
  };

  const sendPremiumGiftFromDrawer = async () => {
    if (!selectedBotToken) {
      return;
    }

    if (chatScopeTab !== 'private') {
      setGiftPanelError('Premium subscription gifts can only be sent to users.');
      return;
    }

    setIsGiftActionLoading(true);
    setGiftPanelError('');
    try {
      const text = giftMessageDraft.trim();
      await giftPremiumSubscription(
        selectedBotToken,
        {
          user_id: selectedUser.id,
          month_count: 1,
          star_count: PREMIUM_SUBSCRIPTION_STAR_COST,
          text: text || undefined,
        },
        selectedUser.id,
      );

      setAvailableUsers((prev) => prev.map((user) => {
        if (user.id !== selectedUser.id) {
          return user;
        }
        return {
          ...user,
          is_premium: true,
          gift_count: nonNegativeInteger(user.gift_count, 0) + 1,
        };
      }));

      setGiftMessageDraft('');
      setGiftReloadTick((prev) => prev + 1);
    } catch (error) {
      setGiftPanelError(error instanceof Error ? error.message : 'Failed to gift premium subscription');
    } finally {
      setIsGiftActionLoading(false);
    }
  };

  const deleteOwnedGiftFromDrawer = async (ownedGiftId: string) => {
    if (!selectedBotToken || !ownedGiftId.trim()) {
      return;
    }

    if (chatScopeTab !== 'private' && !selectedGroup) {
      setGiftPanelError('No chat selected to delete gift from.');
      return;
    }

    setIsGiftActionLoading(true);
    setDeletingOwnedGiftId(ownedGiftId);
    setGiftPanelError('');
    try {
      await deleteOwnedGift(
        selectedBotToken,
        {
          owned_gift_id: ownedGiftId,
          user_id: chatScopeTab === 'private' ? selectedUser.id : undefined,
          chat_id: chatScopeTab !== 'private' ? selectedGroup?.id : undefined,
        },
        selectedUser.id,
      );

      setOwnedRegularGifts((prev) => prev.filter((gift) => gift.owned_gift_id !== ownedGiftId));
      if (chatScopeTab === 'private') {
        setAvailableUsers((prev) => prev.map((user) => {
          if (user.id !== selectedUser.id) {
            return user;
          }
          return {
            ...user,
            gift_count: Math.max(nonNegativeInteger(user.gift_count, 0) - 1, 0),
          };
        }));
      }
    } catch (error) {
      setGiftPanelError(error instanceof Error ? error.message : 'Failed to delete gift');
    } finally {
      setDeletingOwnedGiftId(null);
      setIsGiftActionLoading(false);
    }
  };

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
  const storyShelfStorageReadyTokenRef = useRef(selectedBotToken);

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

  const filteredUsersForManagement = useMemo(() => {
    const keyword = usersTabSearch.trim().toLowerCase();
    if (!keyword) {
      return availableUsers;
    }

    return availableUsers.filter((user) => {
      const fullName = `${user.first_name} ${user.last_name || ''}`.toLowerCase();
      const username = (user.username || '').toLowerCase();
      const idText = String(user.id);
      return fullName.includes(keyword) || username.includes(keyword) || idText.includes(keyword);
    });
  }, [availableUsers, usersTabSearch]);

  const filteredDebugEventLogs = useMemo(() => {
    const keyword = debuggerSearch.trim().toLowerCase();

    return debugEventLogs.filter((entry) => {
      if (debuggerStatusFilter !== 'all' && entry.status !== debuggerStatusFilter) {
        return false;
      }

      if (debuggerSourceFilter !== 'all' && entry.source !== debuggerSourceFilter) {
        return false;
      }

      if (!keyword) {
        return true;
      }

      const method = entry.method.toLowerCase();
      const path = (entry.path || '').toLowerCase();
      const query = (entry.query || '').toLowerCase();
      const statusCode = String(entry.statusCode || '');
      const requestText = JSON.stringify(entry.request || '').toLowerCase();
      const responseText = JSON.stringify(entry.response || '').toLowerCase();
      const errorText = (entry.error || '').toLowerCase();
      return method.includes(keyword)
        || path.includes(keyword)
        || query.includes(keyword)
        || statusCode.includes(keyword)
        || requestText.includes(keyword)
        || responseText.includes(keyword)
        || errorText.includes(keyword);
    });
  }, [debugEventLogs, debuggerSearch, debuggerStatusFilter, debuggerSourceFilter]);

  const webhookDebugCount = useMemo(
    () => debugEventLogs.filter((entry) => entry.source === 'webhook').length,
    [debugEventLogs],
  );

  const latestDebugLog = useMemo(
    () => (filteredDebugEventLogs.length > 0 ? filteredDebugEventLogs[0] : null),
    [filteredDebugEventLogs],
  );

  const historicalDebugLogs = useMemo(
    () => (filteredDebugEventLogs.length > 1 ? filteredDebugEventLogs.slice(1) : []),
    [filteredDebugEventLogs],
  );

  const runtimeEnvDraftByKey = useMemo(() => {
    const values: Record<string, string> = {};
    runtimeEnvRows.forEach((row) => {
      const key = row.key.trim();
      if (!key) {
        return;
      }
      values[key] = row.value;
    });
    return values;
  }, [runtimeEnvRows]);

  const runtimeEnvDirty = useMemo(() => {
    const sourceKeys = Object.keys(runtimeEnvSource).sort();
    const draftKeys = Object.keys(runtimeEnvDraftByKey).sort();
    if (sourceKeys.length !== draftKeys.length) {
      return true;
    }
    for (let index = 0; index < sourceKeys.length; index += 1) {
      if (sourceKeys[index] !== draftKeys[index]) {
        return true;
      }
      if (runtimeEnvSource[sourceKeys[index]] !== runtimeEnvDraftByKey[draftKeys[index]]) {
        return true;
      }
    }
    return false;
  }, [runtimeEnvDraftByKey, runtimeEnvSource]);

  const filteredGroupChatsBySearch = useMemo(() => {
    const keyword = chatSearch.trim().toLowerCase();
    const base = groupChats.filter((group) => group.type === 'group' || group.type === 'supergroup');
    if (!keyword) {
      return base;
    }

    return base.filter((group) => {
      const title = group.title.toLowerCase();
      const username = (group.username || '').toLowerCase();
      const idText = String(group.id);
      return title.includes(keyword) || username.includes(keyword) || idText.includes(keyword);
    });
  }, [chatSearch, groupChats]);

  const filteredChannelChatsBySearch = useMemo(() => {
    const keyword = chatSearch.trim().toLowerCase();
    const base = groupChats.filter((group) => group.type === 'channel' && !group.isDirectMessages);
    if (!keyword) {
      return base;
    }

    return base.filter((group) => {
      const title = group.title.toLowerCase();
      const username = (group.username || '').toLowerCase();
      const idText = String(group.id);
      return title.includes(keyword) || username.includes(keyword) || idText.includes(keyword);
    });
  }, [chatSearch, groupChats]);

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
    localStorage.setItem(SIDEBAR_SECTIONS_KEY, JSON.stringify(sidebarSections));
  }, [sidebarSections]);

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
    const timer = window.setInterval(() => {
      setEmojiClockNow(Math.floor(Date.now() / 1000));
    }, 1_000);

    return () => {
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    setUserEmojiStatusByKey((prev) => {
      let changed = false;
      const next: Record<string, UserEmojiStatusState> = {};
      Object.entries(prev).forEach(([key, status]) => {
        if (!status.customEmojiId.trim()) {
          changed = true;
          return;
        }
        if (typeof status.expirationDate === 'number' && status.expirationDate <= emojiClockNow) {
          changed = true;
          return;
        }
        next[key] = status;
      });
      return changed ? next : prev;
    });
  }, [emojiClockNow]);

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
    let disposed = false;

    setIsBotCommandsLoading(true);
    void (async () => {
      try {
        const commands = await getMyCommands(selectedBotToken);
        if (disposed) {
          return;
        }

        setBotDefaultCommandsByToken((prev) => ({
          ...prev,
          [selectedBotToken]: commands || [],
        }));
      } catch {
        if (disposed) {
          return;
        }

        setBotDefaultCommandsByToken((prev) => {
          if (Object.prototype.hasOwnProperty.call(prev, selectedBotToken)) {
            return prev;
          }
          return {
            ...prev,
            [selectedBotToken]: [],
          };
        });
      } finally {
        if (!disposed) {
          setIsBotCommandsLoading(false);
        }
      }
    })();

    return () => {
      disposed = true;
    };
  }, [selectedBotToken]);

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
    localStorage.setItem(MANAGED_BOT_SETTINGS_BY_BOT_KEY, JSON.stringify(managedBotSettingsByToken));
  }, [managedBotSettingsByToken]);

  useEffect(() => {
    localStorage.setItem(USER_EMOJI_STATUS_BY_KEY, JSON.stringify(userEmojiStatusByKey));
  }, [userEmojiStatusByKey]);

  useEffect(() => {
    localStorage.setItem(CHAT_BOOST_COUNTS_BY_ACTOR_CHAT_KEY, JSON.stringify(chatBoostCountByActorChatKey));
  }, [chatBoostCountByActorChatKey]);

  useEffect(() => {
    localStorage.setItem(USER_WALLETS_KEY, JSON.stringify(walletByUserId));
  }, [walletByUserId]);

  useEffect(() => {
    localStorage.setItem(PAID_MEDIA_PURCHASES_KEY, JSON.stringify(paidMediaPurchaseByActorKey));
  }, [paidMediaPurchaseByActorKey]);

  useEffect(() => {
    if (storyShelfStorageReadyTokenRef.current !== selectedBotToken) {
      return;
    }
    try {
      const raw = localStorage.getItem(STORY_SHELF_BY_BOT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, Record<string, StoryShelfEntry>>) : {};
      parsed[selectedBotToken] = storyShelf;
      localStorage.setItem(STORY_SHELF_BY_BOT_KEY, JSON.stringify(parsed));
    } catch {
      // Ignore persistence failures for local story shelf cache.
    }
  }, [selectedBotToken, storyShelf]);

  useEffect(() => {
    if (storyShelfStorageReadyTokenRef.current !== selectedBotToken) {
      return;
    }
    try {
      const raw = localStorage.getItem(HIDDEN_STORY_KEYS_BY_BOT_KEY);
      const parsed = raw ? (JSON.parse(raw) as Record<string, Record<string, true>>) : {};
      parsed[selectedBotToken] = hiddenStoryKeys;
      localStorage.setItem(HIDDEN_STORY_KEYS_BY_BOT_KEY, JSON.stringify(parsed));
    } catch {
      // Ignore persistence failures for local hidden-story cache.
    }
  }, [selectedBotToken, hiddenStoryKeys]);

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

  useEffect(() => {
    storyShelfStorageReadyTokenRef.current = '__loading__';
    try {
      const rawShelf = localStorage.getItem(STORY_SHELF_BY_BOT_KEY);
      const parsedShelf = rawShelf ? (JSON.parse(rawShelf) as Record<string, Record<string, StoryShelfEntry>>) : {};
      setStoryShelf(parsedShelf[selectedBotToken] || {});
    } catch {
      setStoryShelf({});
    }

    try {
      const rawHidden = localStorage.getItem(HIDDEN_STORY_KEYS_BY_BOT_KEY);
      const parsedHidden = rawHidden ? (JSON.parse(rawHidden) as Record<string, Record<string, true>>) : {};
      setHiddenStoryKeys(parsedHidden[selectedBotToken] || {});
    } catch {
      setHiddenStoryKeys({});
    }
    storyShelfStorageReadyTokenRef.current = selectedBotToken;
  }, [selectedBotToken]);

  useEffect(() => {
    setDeclineSuggestedPostModal(null);
    setActiveStoryPreviewKey(null);
    setShowStoryComposerModal(false);
  }, [selectedBotToken, selectedGroup?.id]);

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
        const nextReactionKeys = update.message_reaction.new_reaction
          .map((item) => reactionKeyFromTypePayload(item as unknown))
          .filter((value): value is string => Boolean(value));

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
            if (nextReactionKeys.length === 0) {
              delete actorReactions[actorKey];
            } else {
              actorReactions[actorKey] = nextReactionKeys;
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
          .map((item) => {
            const key = reactionKeyFromTypePayload((item as { type?: unknown }).type);
            if (!key) {
              return null;
            }
            return {
              emoji: key,
              count: item.total_count,
            };
          })
          .filter((item): item is { emoji: string; count: number } => Boolean(item))
          .map((item) => ({
            emoji: item.emoji,
            count: item.count,
          }));

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

      if (update.purchased_paid_media) {
        const paidMediaPayload = typeof update.purchased_paid_media.paid_media_payload === 'string'
          ? update.purchased_paid_media.paid_media_payload.trim()
          : '';
        const purchaser = update.purchased_paid_media.from;

        if (paidMediaPayload.length > 0 && Number.isFinite(Number(purchaser?.id))) {
          const parts = paidMediaPayload.split(':');
          if (parts.length === 3 && parts[0] === 'paid_media') {
            const purchasedChatId = Math.floor(Number(parts[1]));
            const purchasedMessageId = Math.floor(Number(parts[2]));
            if (
              Number.isFinite(purchasedChatId)
              && Number.isFinite(purchasedMessageId)
              && purchasedMessageId > 0
            ) {
              const purchaseKey = paidMediaPurchaseKeyFor(selectedBotToken, purchaser.id, purchasedChatId, purchasedMessageId);
              setPaidMediaPurchaseByActorKey((prev) => ({
                ...prev,
                [purchaseKey]: true,
              }));
            }
          }
        }

        const purchaserLabel = purchaser?.first_name?.trim()
          || (purchaser?.username ? `@${purchaser.username}` : `user_${purchaser?.id ?? 0}`);
        setCallbackToast(`${purchaserLabel} purchased paid media`);
      }

      if (update.managed_bot) {
        const owner = update.managed_bot.user;
        const managedBot = update.managed_bot.bot;
        const ownerLabel = owner.first_name?.trim()
          || (owner.username ? `@${owner.username}` : `user_${owner.id}`);
        const botLabel = managedBot.first_name?.trim()
          || (managedBot.username ? `@${managedBot.username}` : `bot_${managedBot.id}`);
        setCallbackToast(`${ownerLabel} updated managed bot ${botLabel}`);
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

      const syncSuggestedPostSourceMessage = (
        sourceMessage: { message_id?: number; chat?: { id?: number } } | undefined,
        updater: (message: ChatMessage) => ChatMessage,
      ) => {
        const sourceMessageId = Number(sourceMessage?.message_id);
        const sourceChatId = Number(sourceMessage?.chat?.id ?? payload.chat?.id);
        if (!Number.isFinite(sourceMessageId) || sourceMessageId <= 0 || !Number.isFinite(sourceChatId)) {
          return;
        }

        setMessages((prev) => prev.map((message) => {
          if (
            message.botToken !== selectedBotToken
            || message.chatId !== sourceChatId
            || message.id !== sourceMessageId
          ) {
            return message;
          }
          return updater(message);
        }));
      };

      if (payload.suggested_post_approved) {
        const approved = payload.suggested_post_approved;
        syncSuggestedPostSourceMessage(approved.suggested_post_message, (message) => ({
          ...message,
          suggestedPostInfo: {
            ...(message.suggestedPostInfo || {}),
            state: 'approved',
            price: approved.price || message.suggestedPostInfo?.price,
            send_date: approved.send_date || message.suggestedPostInfo?.send_date,
          },
          suggestedPostApproved: approved,
          suggestedPostApprovalFailed: undefined,
          suggestedPostDeclined: undefined,
        }));
      }

      if (payload.suggested_post_approval_failed) {
        const failed = payload.suggested_post_approval_failed;
        syncSuggestedPostSourceMessage(failed.suggested_post_message, (message) => ({
          ...message,
          suggestedPostInfo: {
            ...(message.suggestedPostInfo || {}),
            state: 'approval_failed',
            price: failed.price || message.suggestedPostInfo?.price,
          },
          suggestedPostApprovalFailed: failed,
        }));
      }

      if (payload.suggested_post_declined) {
        const declined = payload.suggested_post_declined;
        syncSuggestedPostSourceMessage(declined.suggested_post_message, (message) => ({
          ...message,
          suggestedPostInfo: {
            ...(message.suggestedPostInfo || {}),
            state: 'declined',
          },
          suggestedPostDeclined: declined,
          suggestedPostApproved: undefined,
          suggestedPostApprovalFailed: undefined,
        }));
      }

      if (payload.suggested_post_paid) {
        const paid = payload.suggested_post_paid;
        const paidRecord = paid as unknown as Record<string, unknown>;
        const currency = typeof paidRecord.currency === 'string'
          ? paidRecord.currency.trim().toUpperCase()
          : '';
        const rawPayoutAmount = Math.floor(Number(
          paidRecord.amount
          ?? (paidRecord.star_amount && typeof paidRecord.star_amount === 'object'
            ? (paidRecord.star_amount as Record<string, unknown>).amount
            : undefined),
        ));
        const payoutAmount = Number.isFinite(rawPayoutAmount) && rawPayoutAmount >= 0
          ? rawPayoutAmount
          : 0;
        const rawGrossAmount = Math.floor(Number(paidRecord.gross_amount));
        const grossAmount = Number.isFinite(rawGrossAmount) && rawGrossAmount > 0
          ? rawGrossAmount
          : payoutAmount;
        const proposerUserId = Math.floor(Number(paidRecord.proposer_user_id));
        const channelOwnerUserId = Math.floor(Number(paidRecord.channel_owner_user_id));

        if (
          currency === 'XTR'
          && grossAmount > 0
          && Number.isFinite(proposerUserId)
          && proposerUserId > 0
          && Number.isFinite(channelOwnerUserId)
          && channelOwnerUserId > 0
        ) {
          setWalletByUserId((prev) => {
            const deltas: Record<string, number> = {};
            const proposerKey = String(proposerUserId);
            const ownerKey = String(channelOwnerUserId);

            deltas[proposerKey] = (deltas[proposerKey] || 0) - grossAmount;
            deltas[ownerKey] = (deltas[ownerKey] || 0) + payoutAmount;

            let changed = false;
            const next = { ...prev };
            Object.entries(deltas).forEach(([userKey, delta]) => {
              if (!delta) {
                return;
              }

              const currentWallet = prev[userKey] || DEFAULT_WALLET_STATE;
              const updatedWallet = normalizeWalletState({
                ...currentWallet,
                stars: Math.max(0, currentWallet.stars + delta),
              });

              if (
                updatedWallet.fiat !== currentWallet.fiat
                || updatedWallet.stars !== currentWallet.stars
              ) {
                next[userKey] = updatedWallet;
                changed = true;
              }
            });

            return changed ? next : prev;
          });
        }

        syncSuggestedPostSourceMessage(paid.suggested_post_message, (message) => ({
          ...message,
          suggestedPostInfo: {
            ...(message.suggestedPostInfo || {}),
            state: 'paid',
          },
          suggestedPostPaid: paid,
        }));
      }

      if (payload.suggested_post_refunded) {
        const refunded = payload.suggested_post_refunded;
        syncSuggestedPostSourceMessage(refunded.suggested_post_message, (message) => ({
          ...message,
          suggestedPostInfo: {
            ...(message.suggestedPostInfo || {}),
            state: 'refunded',
          },
          suggestedPostRefunded: refunded,
        }));
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
      } else {
        const paidMediaList = (payload.paid_media as { paid_media?: unknown[] } | undefined)?.paid_media;
        const firstPaidMedia = Array.isArray(paidMediaList) && paidMediaList.length > 0
          ? paidMediaList[0]
          : undefined;
        if (firstPaidMedia && typeof firstPaidMedia === 'object') {
          const paidPayload = firstPaidMedia as Record<string, unknown>;
          const paidType = typeof paidPayload.type === 'string'
            ? paidPayload.type.toLowerCase()
            : '';

          if (paidType === 'photo') {
            const paidPhoto = Array.isArray(paidPayload.photo)
              ? (paidPayload.photo as Array<Record<string, unknown>>)
              : [];
            const paidPhotoBest = paidPhoto.length > 0 ? paidPhoto[paidPhoto.length - 1] : undefined;
            const paidPhotoFileId = typeof paidPhotoBest?.file_id === 'string'
              ? paidPhotoBest.file_id
              : undefined;
            if (paidPhotoFileId) {
              media = {
                type: 'photo',
                fileId: paidPhotoFileId,
              };
            }
          } else if (paidType === 'video') {
            const paidVideo = paidPayload.video && typeof paidPayload.video === 'object'
              ? (paidPayload.video as Record<string, unknown>)
              : undefined;
            const paidVideoFileId = typeof paidVideo?.file_id === 'string'
              ? paidVideo.file_id
              : undefined;
            if (paidVideoFileId) {
              media = {
                type: 'video',
                fileId: paidVideoFileId,
                mimeType: typeof paidVideo?.mime_type === 'string' ? paidVideo.mime_type : undefined,
                fileName: typeof paidVideo?.file_name === 'string' ? paidVideo.file_name : undefined,
              };
            }
          }
        }
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

        if (payload.gift) {
          const giftEmoji = payload.gift.gift?.sticker?.emoji?.trim() || '🎁';
          const giftId = payload.gift.gift?.id?.trim() || 'gift';
          const giftNote = payload.gift.text?.trim() || '';
          return {
            text: payload.text || giftNote || `${actorName} sent ${giftEmoji} ${giftId}`,
            service: {
              kind: 'system' as const,
              targetName: giftId,
            },
          };
        }

        if (payload.managed_bot_created?.bot) {
          const managedBot = payload.managed_bot_created.bot;
          const botLabel = managedBot.first_name?.trim()
            || (managedBot.username?.trim() ? `@${managedBot.username.trim()}` : `bot_${managedBot.id}`);
          return {
            text: payload.text || `${actorName} created managed bot ${botLabel}`,
            service: {
              kind: 'system' as const,
              targetName: botLabel,
            },
          };
        }

        if (payload.suggested_post_approved) {
          const approvedCurrency = payload.suggested_post_approved.price?.currency?.trim();
          const approvedAmount = payload.suggested_post_approved.price?.amount;
          const priceLabel = approvedCurrency && Number.isFinite(Number(approvedAmount))
            ? ` (${approvedAmount} ${approvedCurrency})`
            : '';
          return {
            text: payload.text || `${actorName} approved a suggested post${priceLabel}`,
            service: {
              kind: 'system' as const,
              targetName: 'Suggested post',
            },
          };
        }

        if (payload.suggested_post_approval_failed) {
          const failedCurrency = payload.suggested_post_approval_failed.price?.currency?.trim();
          const failedAmount = payload.suggested_post_approval_failed.price?.amount;
          const priceLabel = failedCurrency && Number.isFinite(Number(failedAmount))
            ? ` (${failedAmount} ${failedCurrency})`
            : '';
          return {
            text: payload.text || `${actorName} failed to approve a suggested post${priceLabel}`,
            service: {
              kind: 'system' as const,
              targetName: 'Suggested post',
            },
          };
        }

        if (payload.suggested_post_declined) {
          const declinedComment = payload.suggested_post_declined.comment?.trim();
          return {
            text: payload.text || (declinedComment
              ? `${actorName} declined a suggested post: ${declinedComment}`
              : `${actorName} declined a suggested post`),
            service: {
              kind: 'system' as const,
              targetName: 'Suggested post',
            },
          };
        }

        if (payload.suggested_post_paid) {
          const paidCurrency = payload.suggested_post_paid.currency?.trim();
          const paidAmount = payload.suggested_post_paid.amount ?? payload.suggested_post_paid.star_amount?.amount;
          const paidLabel = paidCurrency && Number.isFinite(Number(paidAmount))
            ? ` (${paidAmount} ${paidCurrency})`
            : (paidCurrency ? ` (${paidCurrency})` : '');
          return {
            text: payload.text || `${actorName} received payment for a suggested post${paidLabel}`,
            service: {
              kind: 'system' as const,
              targetName: 'Suggested post',
            },
          };
        }

        if (payload.suggested_post_refunded) {
          const refundedReason = payload.suggested_post_refunded.reason?.trim();
          return {
            text: payload.text || (refundedReason
              ? `${actorName} refunded a suggested post (${refundedReason})`
              : `${actorName} refunded a suggested post`),
            service: {
              kind: 'system' as const,
              targetName: 'Suggested post',
            },
          };
        }

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
      const isPaidPost = payloadRecord.is_paid_post === true;
      const rawPaidMessageStarCount = Math.floor(Number(
        payloadRecord.paid_message_star_count ?? payloadRecord.paid_star_count,
      ));
      const paidMessageStarCount = Number.isFinite(rawPaidMessageStarCount) && rawPaidMessageStarCount > 0
        ? rawPaidMessageStarCount
        : undefined;
      const rawPaidMediaPayload = typeof payloadRecord.paid_media_payload === 'string'
        ? payloadRecord.paid_media_payload.trim()
        : '';
      const paidMediaPayload = rawPaidMediaPayload || undefined;
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
        isPaidPost,
        paidMessageStarCount,
        paidMediaPayload,
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
        gift: payload.gift,
        poll: payload.poll,
        story: payload.story,
        suggestedPostInfo: payload.suggested_post_info,
        suggestedPostApproved: payload.suggested_post_approved,
        suggestedPostApprovalFailed: payload.suggested_post_approval_failed,
        suggestedPostDeclined: payload.suggested_post_declined,
        suggestedPostPaid: payload.suggested_post_paid,
        suggestedPostRefunded: payload.suggested_post_refunded,
        invoice: payload.invoice,
        successfulPayment: payload.successful_payment,
        checklist: payload.checklist,
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
            checklist: mapped.checklist ?? existing.checklist,
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

        const bootstrapUsers = (bootstrap.users.length > 0 ? bootstrap.users : [DEFAULT_USER]).map((user) => normalizeSimUser(user));
        setAvailableUsers((prev) => {
          const byId = new Map<number, SimUser>();
          [...prev, ...bootstrapUsers].forEach((user) => {
            const normalized = normalizeSimUser(user);
            byId.set(normalized.id, normalized);
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
              isVerified: Boolean(chat.is_verified),
              verificationDescription: optionalTrimmedText(chat.verification_description),
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

  const insertBotCommandIntoComposer = (command: string) => {
    const normalized = command.trim().replace(/^\//, '');
    if (!normalized) {
      return;
    }

    setComposerText((prev) => {
      const current = prev || '';
      const spacer = current.length === 0 || /\s$/.test(current) ? '' : ' ';
      return `${current}${spacer}/${normalized}`;
    });
    composerTextareaRef.current?.focus();
  };

  const sendAsUser = async (
    text: string,
    parseMode?: Exclude<ComposerParseMode, 'none'>,
    replyToMessageId?: number,
    options?: {
      suggestedPostParameters?: NonNullable<Parameters<typeof sendUserMessage>[1]['suggested_post_parameters']>;
    },
  ) => {
    const normalizedText = String(text ?? '');
    if (!normalizedText.trim() || isSending) {
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
        text: normalizedText,
        parse_mode: parseMode,
        suggested_post_parameters: options?.suggestedPostParameters,
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
    const text = String(composerText ?? '').trim();

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
        }, selectedUser.id);
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
          if (uploadAsPaidMedia) {
            const normalizedPaidStarCount = Math.floor(Number(uploadPaidStarCountDraft));
            if (!Number.isFinite(normalizedPaidStarCount) || normalizedPaidStarCount <= 0) {
              throw new Error('Paid media cost must be greater than zero.');
            }

            const hasUnsupportedPaidMedia = selectedUploads.some((file) => {
              const field = inferUploadMethod(file).field;
              return field !== 'photo' && field !== 'video';
            });
            if (hasUnsupportedPaidMedia) {
              throw new Error('Paid media supports only photo/video uploads.');
            }

            await sendBotPaidMedia(selectedBotToken, {
              chatId: selectedChatId,
              files: selectedUploads,
              starCount: normalizedPaidStarCount,
              caption: text || undefined,
              parseMode: text && composerParseMode !== 'none' ? composerParseMode : undefined,
              replyToMessageId: replyTarget?.id,
            }, selectedUser.id);
          } else {
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
        setUploadAsPaidMedia(false);
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
        setUploadAsPaidMedia(false);
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

    const suggestedPostParameters = (() => {
      if (!canCreateSuggestedPostInSelectedChat || !suggestedPostComposer.enabled) {
        return undefined;
      }

      const nextParameters: NonNullable<Parameters<typeof sendUserMessage>[1]['suggested_post_parameters']> = {};

      const priceAmountRaw = suggestedPostComposer.priceAmount.trim();
      if (priceAmountRaw) {
        const amount = Math.floor(Number(priceAmountRaw));
        if (!Number.isFinite(amount)) {
          setErrorText('Suggested post price amount is invalid.');
          return null;
        }

        const currency = suggestedPostComposer.priceCurrency;
        if (currency === 'XTR' && (amount < 5 || amount > 100000)) {
          setErrorText('XTR amount must be between 5 and 100000.');
          return null;
        }
        if (currency === 'TON' && (amount < 10000000 || amount > 10000000000000)) {
          setErrorText('TON amount must be between 10000000 and 10000000000000.');
          return null;
        }

        nextParameters.price = {
          currency,
          amount,
        };
      }

      const sendDateRaw = suggestedPostComposer.sendDate.trim();
      if (sendDateRaw) {
        const sendDate = Math.floor(new Date(sendDateRaw).getTime() / 1000);
        if (!Number.isFinite(sendDate)) {
          setErrorText('Suggested post date is invalid.');
          return null;
        }

        const now = Math.floor(Date.now() / 1000);
        const delta = sendDate - now;
        if (delta < 300 || delta > 2678400) {
          setErrorText('Suggested post date must be between 5 minutes and 30 days in the future.');
          return null;
        }

        nextParameters.send_date = sendDate;
      }

      return nextParameters;
    })();

    if (suggestedPostParameters === null) {
      return;
    }

    setComposerText('');
    await sendAsUser(
      text,
      composerParseMode === 'none' ? undefined : composerParseMode,
      resolveComposerReplyTargetId(replyTarget?.id),
      suggestedPostParameters
        ? { suggestedPostParameters }
        : undefined,
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
      }, selectedUser.id);

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
      }, selectedUser.id);

      setErrorText('');
      setInvoiceBuilder((prev) => ({
        ...prev,
        payload: '',
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Invoice send failed');
    }
  };

  const submitChecklistBuilder = async (mode: 'send' | 'edit') => {
    if (!ensureActiveForumTopicWritable()) {
      return;
    }

    if (chatScopeTab !== 'private') {
      setErrorText('Checklist flow is available only in private chats.');
      return;
    }

    if (!activeBusinessConnectionId) {
      setErrorText('Enable a business connection first, then send checklist.');
      return;
    }

    const title = checklistBuilder.title.trim();
    if (!title) {
      setErrorText('Checklist title is required.');
      return;
    }

    const normalizedTasks = checklistBuilder.tasks
      .map((task, index) => ({
        id: index + 1,
        text: task.text.trim(),
      }))
      .filter((task) => task.text.length > 0);

    if (normalizedTasks.length === 0) {
      setErrorText('Checklist must include at least one task.');
      return;
    }

    if (normalizedTasks.length > 30) {
      setErrorText('Checklist can include at most 30 tasks in this simulator.');
      return;
    }

    const checklistPayload = {
      title,
      tasks: normalizedTasks,
      others_can_add_tasks: checklistBuilder.othersCanAddTasks,
      others_can_mark_tasks_as_done: checklistBuilder.othersCanMarkTasksAsDone,
    };

    try {
      if (mode === 'send') {
        const result = await sendChecklist(selectedBotToken, {
          business_connection_id: activeBusinessConnectionId,
          chat_id: selectedChatId,
          checklist: checklistPayload,
        }, selectedUser.id);

        if (Number.isFinite(Number(result.message_id))) {
          setLastChecklistMessageIdDraft(String(result.message_id));
        }
        setCallbackToast(`Checklist sent (${normalizedTasks.length} tasks).`);
      } else {
        const messageId = Math.floor(Number(lastChecklistMessageIdDraft));
        if (!Number.isFinite(messageId) || messageId <= 0) {
          setErrorText('Checklist message_id is invalid for edit.');
          return;
        }

        await editMessageChecklist(selectedBotToken, {
          business_connection_id: activeBusinessConnectionId,
          chat_id: selectedChatId,
          message_id: messageId,
          checklist: checklistPayload,
        }, selectedUser.id);
        setCallbackToast(`Checklist #${messageId} updated.`);
      }

      setErrorText('');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Checklist action failed');
    }
  };

  const buildWebAppInlineResult = (
    title: string,
    messageText: string,
    description?: string,
    url?: string,
  ): InlineQueryResult => {
    const result: Record<string, unknown> = {
      type: 'article',
      id: `webapp_result_${Date.now()}`,
      title: title.trim() || 'Mini App Result',
      input_message_content: {
        message_text: messageText.trim() || 'Mini App result',
      },
    };

    const normalizedDescription = description?.trim();
    if (normalizedDescription) {
      result.description = normalizedDescription;
    }

    const normalizedUrl = url?.trim();
    if (normalizedUrl) {
      result.url = normalizedUrl;
    }

    return result as InlineQueryResult;
  };

  const onAnswerWebAppQueryFromLab = async () => {
    const webAppQueryId = webAppLab.lastQueryId.trim();
    if (!webAppQueryId) {
      setErrorText('web_app_query_id is required. Open a Mini App button first or paste an id.');
      return;
    }

    setIsWebAppLabRunning(true);
    setErrorText('');
    try {
      const result = await answerWebAppQuery(selectedBotToken, {
        web_app_query_id: webAppQueryId,
        result: buildWebAppInlineResult(
          webAppLab.answerTitle,
          webAppLab.answerMessageText,
          webAppLab.answerDescription,
          webAppLab.answerUrl,
        ),
      });
      const inlineMessageId = result.inline_message_id?.trim() || 'unknown';
      setCallbackToast(`answerWebAppQuery stored inline message id: ${inlineMessageId}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'answerWebAppQuery failed');
    } finally {
      setIsWebAppLabRunning(false);
    }
  };

  const onSavePreparedInlineMessageFromLab = async () => {
    setIsWebAppLabRunning(true);
    setErrorText('');
    try {
      const result = await savePreparedInlineMessage(selectedBotToken, {
        user_id: selectedUser.id,
        result: buildWebAppInlineResult(
          webAppLab.preparedInlineTitle,
          webAppLab.preparedInlineText,
          webAppLab.answerDescription,
          webAppLab.answerUrl,
        ),
        allow_user_chats: true,
        allow_bot_chats: true,
        allow_group_chats: true,
        allow_channel_chats: true,
      });
      setLastPreparedInlineMessageId(result.id);
      setCallbackToast(`Prepared inline message saved: ${result.id}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'savePreparedInlineMessage failed');
    } finally {
      setIsWebAppLabRunning(false);
    }
  };

  const onSavePreparedKeyboardButtonFromLab = async () => {
    const text = webAppLab.preparedButtonText.trim();
    if (!text) {
      setErrorText('Prepared keyboard button text is required.');
      return;
    }

    const url = webAppLab.preparedButtonUrl.trim();
    if (!url) {
      setErrorText('Prepared keyboard button Mini App URL is required.');
      return;
    }

    setIsWebAppLabRunning(true);
    setErrorText('');
    try {
      const result = await savePreparedKeyboardButton(selectedBotToken, {
        user_id: selectedUser.id,
        button: {
          text,
          web_app: {
            url,
          },
        },
      });
      setLastPreparedKeyboardButtonId(result.id);
      setCallbackToast(`Prepared keyboard button saved: ${result.id}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'savePreparedKeyboardButton failed');
    } finally {
      setIsWebAppLabRunning(false);
    }
  };

  const onRefreshUserProfileMedia = async () => {
    setIsUserProfileDataLoading(true);
    setErrorText('');
    try {
      const [photos, audios] = await Promise.all([
        getUserProfilePhotos(selectedBotToken, {
          user_id: selectedUser.id,
          limit: 100,
        }),
        getUserProfileAudios(selectedBotToken, {
          user_id: selectedUser.id,
          limit: 100,
        }),
      ]);
      setUserProfilePhotos(photos);
      setUserProfileAudios(audios);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to load profile media');
    } finally {
      setIsUserProfileDataLoading(false);
    }
  };

  const onSetUserEmojiStatusFromProfile = async (clearStatus = false) => {
    const emojiStatusCustomEmojiId = emojiStatusDraft.trim();
    if (!clearStatus && !emojiStatusCustomEmojiId) {
      setErrorText('emoji_status_custom_emoji_id is required.');
      return;
    }

    const expirationDate = emojiStatusExpirationDraft.trim()
      ? Math.floor(new Date(emojiStatusExpirationDraft).getTime() / 1000)
      : undefined;
    if (emojiStatusExpirationDraft.trim() && !Number.isFinite(expirationDate)) {
      setErrorText('Emoji status expiration date is invalid.');
      return;
    }

    setIsUserProfileDataLoading(true);
    setErrorText('');
    try {
      await setUserEmojiStatus(selectedBotToken, {
        user_id: selectedUser.id,
        emoji_status_custom_emoji_id: clearStatus ? undefined : emojiStatusCustomEmojiId,
        emoji_status_expiration_date: clearStatus ? undefined : expirationDate,
      });
      setUserEmojiStatusByKey((prev) => {
        const next = { ...prev };
        if (clearStatus || !emojiStatusCustomEmojiId) {
          delete next[selectedUserEmojiStatusKey];
        } else {
          next[selectedUserEmojiStatusKey] = {
            customEmojiId: emojiStatusCustomEmojiId,
            expirationDate,
          };
        }
        return next;
      });
      if (clearStatus) {
        setEmojiStatusDraft('');
        setEmojiStatusExpirationDraft('');
      }
      setCallbackToast(clearStatus ? 'Emoji status cleared.' : 'Emoji status updated.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to set emoji status');
    } finally {
      setIsUserProfileDataLoading(false);
    }
  };

  const onSetUserProfilePhotoFromProfile = async () => {
    const nextPhotoUrl = optionalTrimmedText(profilePhotoUrlDraft);
    setIsUserProfileDataLoading(true);
    setErrorText('');
    try {
      const saved = await upsertSimUser({
        id: selectedUser.id,
        first_name: selectedUser.first_name,
        last_name: optionalTrimmedText(selectedUser.last_name),
        username: optionalTrimmedText(selectedUser.username),
        phone_number: optionalTrimmedText(selectedUser.phone_number),
        photo_url: nextPhotoUrl,
        bio: optionalTrimmedText(selectedUser.bio),
        is_premium: Boolean(selectedUser.is_premium),
        business_name: optionalTrimmedText(selectedUser.business_name),
        business_intro: optionalTrimmedText(selectedUser.business_intro),
        business_location: optionalTrimmedText(selectedUser.business_location),
        gift_count: nonNegativeInteger(selectedUser.gift_count, 0),
      });

      const normalizedUser = normalizeSimUser(saved as SimUser);
      setAvailableUsers((prev) => prev.map((user) => (
        user.id === normalizedUser.id ? normalizedUser : user
      )));
      setProfilePhotoUrlDraft(normalizedUser.photo_url || '');
      setCallbackToast(nextPhotoUrl ? 'Profile photo updated.' : 'Profile photo cleared.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to update profile photo');
    } finally {
      setIsUserProfileDataLoading(false);
    }
  };

  const onSetUserProfileAudioFromProfile = async () => {
    const selectedFile = profileAudioFileDraft;
    const fallbackTitleFromFile = selectedFile
      ? selectedFile.name.replace(/\.[^/.]+$/, '').trim()
      : '';
    const title = profileAudioTitleDraft.trim() || fallbackTitleFromFile;
    if (!title) {
      setErrorText('Profile audio title or file is required.');
      return;
    }

    setIsUserProfileDataLoading(true);
    setErrorText('');
    try {
      if (selectedFile) {
        await uploadSimUserProfileAudio(selectedBotToken, {
          user_id: selectedUser.id,
          audio: selectedFile,
          title,
          performer: optionalTrimmedText(profileAudioPerformerDraft),
          file_name: selectedFile.name,
          mime_type: selectedFile.type || undefined,
          duration: 30,
        }, selectedUser.id);
      } else {
        await setSimUserProfileAudio(selectedBotToken, {
          user_id: selectedUser.id,
          title,
          performer: optionalTrimmedText(profileAudioPerformerDraft),
          file_name: 'profile-audio.ogg',
          duration: 30,
        });
      }

      await onRefreshUserProfileMedia();
      setProfileAudioFileDraft(null);
      if (!profileAudioTitleDraft.trim()) {
        setProfileAudioTitleDraft(title);
      }
      setCallbackToast(selectedFile ? 'Profile audio uploaded.' : 'Profile audio updated.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to set profile audio');
    } finally {
      setIsUserProfileDataLoading(false);
    }
  };

  const onDeleteUserProfileAudioFromProfile = async (fileId: string) => {
    setIsUserProfileDataLoading(true);
    setErrorText('');
    try {
      await deleteSimUserProfileAudio(selectedBotToken, {
        user_id: selectedUser.id,
        file_id: fileId,
      });
      await onRefreshUserProfileMedia();
      setCallbackToast('Profile audio deleted.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to delete profile audio');
    } finally {
      setIsUserProfileDataLoading(false);
    }
  };

  const onDeleteAllUserProfileAudiosFromProfile = async () => {
    const tracks = userProfileAudios?.audios || [];
    if (tracks.length === 0) {
      setCallbackToast('No profile audio to delete.');
      return;
    }

    setIsUserProfileDataLoading(true);
    setErrorText('');
    try {
      for (const audio of tracks) {
        await deleteSimUserProfileAudio(selectedBotToken, {
          user_id: selectedUser.id,
          file_id: audio.file_id,
        });
      }
      await onRefreshUserProfileMedia();
      setCallbackToast('All profile audios deleted.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to delete all profile audios');
    } finally {
      setIsUserProfileDataLoading(false);
    }
  };

  const syncChatBoostCount = (chatId: number, userId: number, count: number) => {
    const boostKey = `${selectedBotToken}:${chatId}:${userId}`;
    setChatBoostCountByActorChatKey((prev) => {
      const next = { ...prev };
      if (count > 0) {
        next[boostKey] = count;
      } else {
        delete next[boostKey];
      }
      return next;
    });
  };

  const fetchSelectedChatBoosts = async () => {
    if (!selectedGroup || selectedGroup.isDirectMessages) {
      return null;
    }

    const boosts = await getUserChatBoosts(selectedBotToken, {
      chat_id: selectedGroup.id,
      user_id: selectedUser.id,
    }, selectedUser.id);
    setUserChatBoosts(boosts);
    syncChatBoostCount(selectedGroup.id, selectedUser.id, boosts.boosts.length);
    return boosts;
  };

  const onRefreshChatBoostsFromModal = async () => {
    if (!selectedGroup || selectedGroup.isDirectMessages) {
      return;
    }

    setIsBoostActionRunning(true);
    setErrorText('');
    try {
      const boosts = await fetchSelectedChatBoosts();
      if (boosts) {
        setCallbackToast(`Loaded ${boosts.boosts.length} boosts for ${selectedGroup.title}.`);
      }
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to load chat boosts');
    } finally {
      setIsBoostActionRunning(false);
    }
  };

  const onOpenChatBoostModal = () => {
    if (!selectedGroup || selectedGroup.isDirectMessages) {
      return;
    }

    if (groupMembership !== 'joined') {
      setErrorText('Join the chat as this user before boosting.');
      return;
    }

    if (!selectedUser.is_premium) {
      setErrorText('Only premium users with available boosts can boost chats.');
      return;
    }

    setChatBoostModal({
      chatId: selectedGroup.id,
      chatTitle: selectedGroup.title,
      countDraft: '1',
    });

    void onRefreshChatBoostsFromModal();
  };

  const onApplyChatBoostFromModal = async () => {
    if (!chatBoostModal) {
      return;
    }

    const boostCount = Math.floor(Number(chatBoostModal.countDraft));
    if (!Number.isFinite(boostCount) || boostCount <= 0) {
      setErrorText('Boost count must be a positive number.');
      return;
    }

    if (!selectedUser.is_premium) {
      setErrorText('Only premium users can boost chats/channels.');
      return;
    }

    setIsBoostActionRunning(true);
    setErrorText('');
    try {
      const result = await addSimUserChatBoosts(selectedBotToken, {
        chat_id: chatBoostModal.chatId,
        user_id: selectedUser.id,
        count: boostCount,
      }, selectedUser.id);

      const refreshed = await fetchSelectedChatBoosts();
      setChatBoostModal((prev) => (prev
        ? {
          ...prev,
          countDraft: '1',
        }
        : prev
      ));
      setChatMenuOpen(false);
      setCallbackToast(
        `Boosted ${chatBoostModal.chatTitle} by ${result.added_count}. Total boosts: ${refreshed?.boosts.length ?? 0}.`,
      );
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to boost chat');
    } finally {
      setIsBoostActionRunning(false);
    }
  };

  const onRemoveChatBoostFromModal = async (boostId: string) => {
    if (!chatBoostModal) {
      return;
    }

    setIsBoostActionRunning(true);
    setErrorText('');
    try {
      await removeSimUserChatBoosts(selectedBotToken, {
        chat_id: chatBoostModal.chatId,
        user_id: selectedUser.id,
        boost_ids: [boostId],
      }, selectedUser.id);
      const refreshed = await fetchSelectedChatBoosts();
      setCallbackToast(`Boost removed. Remaining boosts: ${refreshed?.boosts.length ?? 0}.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to remove boost');
    } finally {
      setIsBoostActionRunning(false);
    }
  };

  const onRemoveAllChatBoostsFromModal = async () => {
    if (!chatBoostModal) {
      return;
    }

    setIsBoostActionRunning(true);
    setErrorText('');
    try {
      await removeSimUserChatBoosts(selectedBotToken, {
        chat_id: chatBoostModal.chatId,
        user_id: selectedUser.id,
        remove_all: true,
      }, selectedUser.id);
      await fetchSelectedChatBoosts();
      setCallbackToast('All boosts removed for this user in the selected chat.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Failed to clear boosts');
    } finally {
      setIsBoostActionRunning(false);
    }
  };

  const storyShelfKeyFor = (story: NonNullable<ChatMessage['story']>): string => `${story.chat.id}:${story.id}`;

  const mergeStoryPreviewSnapshots = (
    ...entries: Array<StoryPreviewSnapshot | undefined>
  ): StoryPreviewSnapshot | undefined => {
    const merged: StoryPreviewSnapshot = {};

    for (const entry of entries) {
      if (!entry) {
        continue;
      }
      if (!merged.caption && entry.caption) {
        merged.caption = entry.caption;
      }
      if (!merged.contentRef && entry.contentRef) {
        merged.contentRef = entry.contentRef;
      }
      if (!merged.contentType && entry.contentType) {
        merged.contentType = entry.contentType;
      }
    }

    if (!merged.caption && !merged.contentRef && !merged.contentType) {
      return undefined;
    }

    return merged;
  };

  const storyPreviewFromStoryPayload = (story: NonNullable<ChatMessage['story']>): StoryPreviewSnapshot | undefined => {
    const rawStory = story as unknown as Record<string, unknown>;
    const rawContent = rawStory.content;
    const content = rawContent && typeof rawContent === 'object'
      ? (rawContent as Record<string, unknown>)
      : undefined;

    const caption = typeof rawStory.caption === 'string'
      ? rawStory.caption.trim() || undefined
      : undefined;

    const rawType = typeof content?.type === 'string'
      ? content.type
      : (typeof rawStory.type === 'string' ? rawStory.type : '');
    const normalizedType = rawType.trim().toLowerCase();

    const photoRef = typeof content?.photo === 'string'
      ? content.photo.trim()
      : (typeof rawStory.photo === 'string' ? rawStory.photo.trim() : '');
    const videoRef = typeof content?.video === 'string'
      ? content.video.trim()
      : (typeof rawStory.video === 'string' ? rawStory.video.trim() : '');

    const contentType: StoryPreviewSnapshot['contentType'] = normalizedType === 'video'
      ? 'video'
      : (normalizedType === 'photo' ? 'photo' : (videoRef ? 'video' : (photoRef ? 'photo' : undefined)));
    const contentRef = contentType === 'video'
      ? (videoRef || undefined)
      : (contentType === 'photo' ? (photoRef || undefined) : undefined);

    if (!caption && !contentRef && !contentType) {
      return undefined;
    }

    return {
      caption,
      contentRef,
      contentType,
    };
  };

  const storyPreviewFromMessage = (message: ChatMessage): StoryPreviewSnapshot | undefined => {
    const caption = message.text?.trim() || undefined;
    const contentType = message.media?.type === 'video'
      ? 'video'
      : (message.media?.type === 'photo' ? 'photo' : undefined);
    const contentRef = contentType && message.media?.fileId
      ? message.media.fileId
      : undefined;
    if (!caption && !contentType && !contentRef) {
      return undefined;
    }
    return {
      caption,
      contentRef,
      contentType,
    };
  };

  const upsertStoryShelfEntry = (
    story: NonNullable<ChatMessage['story']>,
    updatedAt?: number,
    preview?: StoryPreviewSnapshot,
  ) => {
    const key = storyShelfKeyFor(story);
    const nextUpdatedAt = Number.isFinite(Number(updatedAt))
      ? Math.floor(Number(updatedAt))
      : Math.floor(Date.now() / 1000);
    const normalizedPreview = mergeStoryPreviewSnapshots(
      preview,
      storyPreviewFromStoryPayload(story),
    );

    setStoryShelf((prev) => {
      const existing = prev[key];
      return {
        ...prev,
        [key]: {
          story,
          updatedAt: nextUpdatedAt,
          preview: mergeStoryPreviewSnapshots(normalizedPreview, existing?.preview),
        },
      };
    });
    setHiddenStoryKeys((prev) => {
      if (!prev[key]) {
        return prev;
      }
      const next = { ...prev };
      delete next[key];
      return next;
    });
  };

  const hideStoryShelfEntry = (story: NonNullable<ChatMessage['story']>) => {
    const key = storyShelfKeyFor(story);
    setStoryShelf((prev) => {
      if (!prev[key]) {
        return prev;
      }
      const next = { ...prev };
      delete next[key];
      return next;
    });
    setHiddenStoryKeys((prev) => ({
      ...prev,
      [key]: true,
    }));
  };

  const isStoryOwnedByActiveUser = (story: NonNullable<ChatMessage['story']>) => story.chat.id === selectedUser.id;

  const listedStories = useMemo(() => {
    const merged = new Map<string, StoryShelfEntry>();

    for (const [key, item] of Object.entries(storyShelf)) {
      if (hiddenStoryKeys[key]) {
        continue;
      }
      merged.set(key, item);
    }

    for (const message of messages) {
      if (message.botToken !== selectedBotToken || !message.story) {
        continue;
      }
      const key = storyShelfKeyFor(message.story);
      if (hiddenStoryKeys[key]) {
        continue;
      }

      const candidatePreview = mergeStoryPreviewSnapshots(
        storyPreviewFromMessage(message),
        storyPreviewFromStoryPayload(message.story),
      );

      const candidate: StoryShelfEntry = {
        story: message.story,
        updatedAt: message.editDate || message.date,
        preview: candidatePreview,
      };
      const existing = merged.get(key);
      if (!existing || existing.updatedAt < candidate.updatedAt) {
        candidate.preview = mergeStoryPreviewSnapshots(candidate.preview, existing?.preview);
        merged.set(key, candidate);
      }
    }

    return Array.from(merged.values()).sort((left, right) => right.updatedAt - left.updatedAt);
  }, [hiddenStoryKeys, messages, selectedBotToken, storyShelf]);

  const storyPreviewMessageByKey = useMemo(() => {
    const previewByKey = new Map<string, ChatMessage>();

    for (const message of messages) {
      if (message.botToken !== selectedBotToken || !message.story) {
        continue;
      }

      const key = storyShelfKeyFor(message.story);
      const existing = previewByKey.get(key);
      if (!existing) {
        previewByKey.set(key, message);
        continue;
      }

      const existingUpdatedAt = existing.editDate || existing.date;
      const candidateUpdatedAt = message.editDate || message.date;
      if (candidateUpdatedAt >= existingUpdatedAt) {
        previewByKey.set(key, message);
      }
    }

    return previewByKey;
  }, [messages, selectedBotToken]);

  const activeStoryPreview = useMemo(() => {
    if (!activeStoryPreviewKey) {
      return null;
    }

    const entry = listedStories.find((item) => storyShelfKeyFor(item.story) === activeStoryPreviewKey);
    if (!entry) {
      return null;
    }

    return {
      entry,
      referenceMessage: storyPreviewMessageByKey.get(activeStoryPreviewKey),
    };
  }, [activeStoryPreviewKey, listedStories, storyPreviewMessageByKey]);

  useEffect(() => {
    if (!activeStoryPreviewKey) {
      return;
    }

    const stillExists = listedStories.some((item) => storyShelfKeyFor(item.story) === activeStoryPreviewKey);
    if (!stillExists) {
      setActiveStoryPreviewKey(null);
    }
  }, [activeStoryPreviewKey, listedStories]);

  const isDirectStoryPreviewSource = (value: string) => (
    value.startsWith('http://')
    || value.startsWith('https://')
    || value.startsWith('data:')
    || value.startsWith('blob:')
    || value.startsWith('/'));

  const activeStoryPreviewMediaRef = activeStoryPreview?.entry.preview?.contentRef?.trim() || '';
  const activeStoryPreviewMediaType = activeStoryPreview?.entry.preview?.contentType;
  const activeStoryPreviewMediaSource = activeStoryPreviewMediaRef
    ? (isDirectStoryPreviewSource(activeStoryPreviewMediaRef)
      ? activeStoryPreviewMediaRef
      : mediaUrlByFileId[activeStoryPreviewMediaRef])
    : undefined;
  const activeStoryReferenceMediaFileId = activeStoryPreview?.referenceMessage?.media?.fileId?.trim() || '';

  useEffect(() => {
    if (!activeStoryPreview || !activeStoryPreviewMediaRef || isDirectStoryPreviewSource(activeStoryPreviewMediaRef)) {
      return;
    }
    if (mediaUrlByFileId[activeStoryPreviewMediaRef]) {
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const url = await resolveMediaUrl(selectedBotToken, activeStoryPreviewMediaRef);
        if (cancelled) {
          return;
        }
        setMediaUrlByFileId((prev) => ({
          ...prev,
          [activeStoryPreviewMediaRef]: url,
        }));
      } catch {
        // Keep story preview responsive even if media URL resolution fails.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [activeStoryPreview, activeStoryPreviewMediaRef, mediaUrlByFileId, selectedBotToken]);

  useEffect(() => {
    if (!activeStoryReferenceMediaFileId || mediaUrlByFileId[activeStoryReferenceMediaFileId]) {
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const token = activeStoryPreview?.referenceMessage?.botToken || selectedBotToken;
        const url = await resolveMediaUrl(token, activeStoryReferenceMediaFileId);
        if (cancelled) {
          return;
        }
        setMediaUrlByFileId((prev) => ({
          ...prev,
          [activeStoryReferenceMediaFileId]: url,
        }));
      } catch {
        // Keep story preview responsive even if media URL resolution fails.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [activeStoryPreview, activeStoryReferenceMediaFileId, mediaUrlByFileId, selectedBotToken]);

  const submitStoryBuilder = async () => {
    const parseActivePeriod = (raw: string): number => {
      const value = Math.floor(Number(raw));
      return Number.isFinite(value) ? value : 0;
    };

    const parseStoryAreas = (): unknown[] | undefined => {
      const raw = storyBuilder.areasJson.trim();
      if (!raw) {
        return undefined;
      }

      const parsed = JSON.parse(raw) as unknown;
      if (!Array.isArray(parsed)) {
        throw new Error('Story areas must be a JSON array.');
      }
      return parsed;
    };

    try {
      setErrorText('');
      setIsStoryActionRunning(true);
      const isEditMode = storyBuilder.mode === 'edit';
      if (!isEditMode) {
        const contentRef = storyBuilder.contentRef.trim();
        const caption = storyBuilder.caption.trim() || undefined;
        if (!contentRef && !storyBuilderFile) {
          setErrorText('Upload a story file or provide a content reference.');
          return;
        }

        const activePeriod = parseActivePeriod(storyBuilder.activePeriod);
        const areas = parseStoryAreas();
        const story = storyBuilderFile
          ? await postStoryWithFile(
            selectedBotToken,
            {
              file: storyBuilderFile,
              content_type: storyBuilder.contentType,
              active_period: activePeriod,
              caption,
              areas,
            },
            selectedUser.id,
          )
          : await postStory(
            selectedBotToken,
            {
              content: {
                type: storyBuilder.contentType,
                [storyBuilder.contentType]: contentRef,
              } as unknown as Parameters<typeof postStory>[1]['content'],
              active_period: activePeriod,
              caption,
              areas: areas as Parameters<typeof postStory>[1]['areas'],
            },
            selectedUser.id,
          );
        upsertStoryShelfEntry(
          story,
          undefined,
          mergeStoryPreviewSnapshots(
            storyPreviewFromStoryPayload(story),
            {
              caption,
              contentRef: storyBuilderFile ? undefined : (contentRef || undefined),
              contentType: storyBuilder.contentType,
            },
          ),
        );
        setCallbackToast(`Story posted: #${story.id}`);
        setStoryBuilderFile(null);
        setStoryBuilder((prev) => ({
          ...prev,
          mode: 'post',
          contentRef: '',
          caption: '',
          areasJson: '',
        }));
        setShowStoryComposerModal(false);
        return;
      }

      const storyId = Math.floor(Number(storyBuilder.storyId));
      if (!Number.isFinite(storyId) || storyId <= 0) {
        setErrorText('story_id is required.');
        return;
      }

      const contentRef = storyBuilder.contentRef.trim();
      if (!contentRef && !storyBuilderFile) {
        setErrorText('Upload a story file or provide a content reference for edit.');
        return;
      }

      const areas = parseStoryAreas();
      const caption = storyBuilder.caption.trim() || undefined;
      const story = storyBuilderFile
        ? await editStoryWithFile(
          selectedBotToken,
          {
            story_id: storyId,
            file: storyBuilderFile,
            content_type: storyBuilder.contentType,
            caption,
            areas,
          },
          selectedUser.id,
        )
        : await editStory(
          selectedBotToken,
          {
            story_id: storyId,
            content: {
              type: storyBuilder.contentType,
              [storyBuilder.contentType]: contentRef,
            } as unknown as Parameters<typeof editStory>[1]['content'],
            caption,
            areas: areas as Parameters<typeof editStory>[1]['areas'],
          },
          selectedUser.id,
        );
      upsertStoryShelfEntry(
        story,
        undefined,
        mergeStoryPreviewSnapshots(
          storyPreviewFromStoryPayload(story),
          {
            caption,
            contentRef: storyBuilderFile ? undefined : (contentRef || undefined),
            contentType: storyBuilder.contentType,
          },
        ),
      );
      setCallbackToast(`Story updated: #${story.id}`);
      setStoryBuilderFile(null);
      setStoryBuilder((prev) => ({
        ...prev,
        mode: 'post',
        storyId: '',
        contentRef: '',
        caption: '',
        areasJson: '',
      }));
      setShowStoryComposerModal(false);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Story action failed');
    } finally {
      setIsStoryActionRunning(false);
    }
  };

  const openStoryComposerForPost = () => {
    setShowStoryComposerModal(true);
    setStoryBuilderFile(null);
    setStoryBuilder((prev) => ({
      ...prev,
      mode: 'post',
      storyId: '',
      fromChatId: '',
      fromStoryId: '',
      contentRef: '',
      caption: '',
      areasJson: '',
    }));
  };

  const openStoryEditorForReference = (story: NonNullable<ChatMessage['story']>) => {
    if (!isStoryOwnedByActiveUser(story)) {
      setErrorText('Only your own stories can be edited.');
      return;
    }

    setShowStoryComposerModal(true);
    setStoryBuilderFile(null);
    setStoryBuilder((prev) => ({
      ...prev,
      mode: 'edit',
      storyId: String(story.id),
      fromChatId: String(story.chat.id),
      fromStoryId: String(story.id),
      contentRef: '',
      caption: '',
      areasJson: '',
    }));
  };

  const onRepostStoryReference = async (story: NonNullable<ChatMessage['story']>) => {
    const activePeriod = Math.floor(Number(storyBuilder.activePeriod)) || 86400;
    const sourceKey = storyShelfKeyFor(story);
    const sourceMessagePreview = storyPreviewMessageByKey.get(sourceKey);
    const sourcePreview = mergeStoryPreviewSnapshots(
      storyShelf[sourceKey]?.preview,
      sourceMessagePreview ? storyPreviewFromMessage(sourceMessagePreview) : undefined,
      storyPreviewFromStoryPayload(story),
    );
    setIsStoryActionRunning(true);
    try {
      const repostedStory = await repostStory(
        selectedBotToken,
        {
          from_chat_id: story.chat.id,
          from_story_id: story.id,
          active_period: activePeriod,
        },
        selectedUser.id,
      );
      upsertStoryShelfEntry(
        repostedStory,
        undefined,
        mergeStoryPreviewSnapshots(storyPreviewFromStoryPayload(repostedStory), sourcePreview),
      );
      setCallbackToast(`Story reposted: #${repostedStory.id}`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Story repost failed');
    } finally {
      setIsStoryActionRunning(false);
    }
  };

  const onDeleteStoryReference = async (story: NonNullable<ChatMessage['story']>) => {
    if (!isStoryOwnedByActiveUser(story)) {
      setErrorText('Only your own stories can be deleted.');
      return;
    }

    setIsStoryActionRunning(true);
    try {
      await deleteStory(
        selectedBotToken,
        {
          story_id: story.id,
        },
        selectedUser.id,
      );
      hideStoryShelfEntry(story);
      setCallbackToast(`Story deleted: #${story.id}`);
      setStoryBuilder((prev) => (
        prev.mode === 'edit' && Number(prev.storyId) === story.id
          ? {
            ...prev,
            mode: 'post',
            storyId: '',
            contentRef: '',
            caption: '',
            areasJson: '',
          }
          : prev
      ));
      setStoryBuilderFile(null);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Story delete failed');
    } finally {
      setIsStoryActionRunning(false);
    }
  };

  const onQuickRepostStory = async (message: ChatMessage) => {
    if (!message.story) {
      return;
    }

    await onRepostStoryReference(message.story);
  };

  const onQuickDeleteStory = async (message: ChatMessage) => {
    if (!message.story) {
      return;
    }

    await onDeleteStoryReference(message.story);
  };

  const openStoryEditorFromMessage = (message: ChatMessage) => {
    if (!message.story) {
      return;
    }

    openStoryEditorForReference(message.story);
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

  const onInspectSelectedGroupBoosts = async () => {
    if (!selectedGroup || selectedGroup.isDirectMessages) {
      return;
    }

    if (groupMembership !== 'joined') {
      setErrorText('Join the chat as this user to view boosts.');
      return;
    }

    await runGroupAction(async () => {
      const boosts = await fetchSelectedChatBoosts();
      if (!boosts) {
        return;
      }
      setUserChatBoosts(boosts);
      renderInspector(`getUserChatBoosts(${selectedUser.id})`, boosts);
      setErrorText(`Loaded ${boosts.boosts.length} boosts for user ${selectedUser.id}.`);
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
      paidStarReactionsEnabled: currentSettings.paidStarReactionsEnabled,
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
        paidStarReactionsEnabled: groupProfileDraft.paidStarReactionsEnabled,
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
        paid_star_reactions_enabled: selectedGroup.type === 'channel' ? groupProfileDraft.paidStarReactionsEnabled : undefined,
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

  const loadBotDraftFromApi = async (
    token: string,
    fallback: Pick<BotDraftState, 'first_name' | 'username'>,
  ) => {
    setIsBotModalLoading(true);
    try {
      const [nameResult, descriptionResult, shortDescriptionResult, commandsResult, groupRights, channelRights] = await Promise.all([
        getMyName(token),
        getMyDescription(token),
        getMyShortDescription(token),
        getMyCommands(token),
        getMyDefaultAdministratorRights(token, { for_channels: false }),
        getMyDefaultAdministratorRights(token, { for_channels: true }),
      ]);

      const normalizedFirstName = optionalTrimmedText(nameResult?.name) || fallback.first_name;
      setBotDraft((prev) => ({
        ...prev,
        first_name: normalizedFirstName,
        username: fallback.username,
        description: optionalTrimmedText(descriptionResult?.description) || '',
        short_description: optionalTrimmedText(shortDescriptionResult?.short_description) || '',
        profile_photo_ref: '',
        remove_profile_photo: false,
        commands_text: formatBotCommandsForEditor(commandsResult || []),
        commands_language_code: '',
        group_default_admin_rights: mapChatAdminRightsToDraft(groupRights),
        channel_default_admin_rights: mapChatAdminRightsToDraft(channelRights),
      }));
      setBotDefaultCommandsByToken((prev) => ({
        ...prev,
        [token]: commandsResult || [],
      }));
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to load bot profile details');
    } finally {
      setIsBotModalLoading(false);
    }
  };

  const onCreateBot = () => {
    const randomIdentity = randomBotIdentityDraft();
    setBotModalMode('create');
    setBotManagedEnabledDraft(true);
    setBotDraft({
      ...emptyBotDraft(),
      ...randomIdentity,
      commands_text: '/start - Start the bot\n/help - Show help and usage',
    });
    setShowBotModal(true);
  };

  const randomizeBotDraft = () => {
    const randomIdentity = randomBotIdentityDraft();
    setBotDraft((prev) => ({
      ...prev,
      ...randomIdentity,
    }));
  };

  const randomizeUserDraft = () => {
    const parsedId = Math.floor(Number(userDraft.id));
    const randomId = userModalMode === 'edit' && Number.isFinite(parsedId) && parsedId > 0
      ? parsedId
      : Math.floor(Math.random() * 900000 + 10000);
    setUserDraft(randomUserDraft(randomId));
  };

  const openEditBotModal = (bot: SimBot) => {
    setBotModalMode('edit');
    setBotManagedEnabledDraft(
      managedBotSettingsByToken[bot.token]?.enabled
      ?? defaultManagedBotSettings().enabled,
    );
    setBotDraft({
      ...emptyBotDraft(),
      first_name: bot.first_name,
      username: bot.username,
    });
    setSelectedBotToken(bot.token);
    setShowBotModal(true);
    void loadBotDraftFromApi(bot.token, {
      first_name: bot.first_name,
      username: bot.username,
    });
  };

  const onRunManagedBotTokenAction = async (action: 'get' | 'replace') => {
    const ownerUserId = Math.floor(Number(managedBotOwnerDraft));
    if (!Number.isFinite(ownerUserId) || ownerUserId <= 0) {
      setErrorText('Managed bot owner user_id is invalid.');
      return;
    }

    setIsManagedBotTokenActionRunning(true);
    setErrorText('');
    try {
      if (action === 'get') {
        await getManagedBotToken(selectedBotToken, {
          user_id: ownerUserId,
        });
        setCallbackToast(`Managed bot token fetched for user ${ownerUserId}.`);
      } else {
        await replaceManagedBotToken(selectedBotToken, {
          user_id: ownerUserId,
        });
        setCallbackToast(`Managed bot token rotated for user ${ownerUserId}.`);
      }
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Managed bot token action failed');
    } finally {
      setIsManagedBotTokenActionRunning(false);
    }
  };

  const onSelectSidebarTab = (tab: SidebarTab) => {
    if (activeTab === tab) {
      setIsSidebarPanelOpen((prev) => !prev);
      return;
    }
    setActiveTab(tab);
    setIsSidebarPanelOpen(true);
    if (tab === 'debugger') {
      void onLoadRuntimeLogs();
    }
    if (tab === 'settings') {
      void onLoadRuntimeInfo();
      void onCheckServerHealth();
    }
  };

  const onToggleSidebarSection = (section: keyof SidebarSectionState) => {
    setSidebarSections((prev) => ({
      ...prev,
      [section]: !prev[section],
    }));
  };

  const onCheckServerHealth = useCallback(async () => {
    const apiBase = API_BASE_URL.replace(/\/$/, '');

    setServerHealth((prev) => ({
      ...prev,
      status: 'checking',
      error: undefined,
    }));
    try {
      const response = await fetch(`${apiBase}/health`);
      if (!response.ok) {
        throw new Error(`health check failed (${response.status})`);
      }
      setServerHealth({
        status: 'online',
        checkedAt: Date.now(),
      });
    } catch (error) {
      setServerHealth({
        status: 'offline',
        checkedAt: Date.now(),
        error: error instanceof Error ? error.message : 'server unreachable',
      });
    }
  }, []);

  const onLoadRuntimeLogs = useCallback(async () => {
    const apiBase = API_BASE_URL.replace(/\/$/, '');
    try {
      const response = await fetch(`${apiBase}/client-api/runtime/logs?limit=300`);
      if (!response.ok) {
        throw new Error(`runtime logs failed (${response.status})`);
      }

      const payload = await response.json();
      const items = Array.isArray(payload?.result?.items)
        ? payload.result.items as Array<Record<string, unknown>>
        : [];

      const mapped: DebugEventLog[] = items
        .filter((entry) => {
          const path = String(entry.path || '');
          return path.startsWith('/bot') || path.startsWith('/webhook');
        })
        .map((entry, index) => {
        const path = String(entry.path || '');
        const responsePayload = entry.response;
        const statusCode = Number(entry.status || 0);
        const inferredError = typeof responsePayload === 'object'
          && responsePayload
          && (responsePayload as Record<string, unknown>).ok === false;
        const description = typeof responsePayload === 'object' && responsePayload
          ? (responsePayload as Record<string, unknown>).description
          : undefined;

        return {
          id: String(entry.id || `${Date.now()}-${index}`),
          at: Number(entry.at || Date.now()),
          method: String(entry.method || 'UNKNOWN'),
          path,
          source: path.startsWith('/webhook') ? 'webhook' : 'bot',
          query: typeof entry.query === 'string' ? entry.query : undefined,
          statusCode,
          durationMs: Number(entry.duration_ms || 0),
          remoteAddr: typeof entry.remote_addr === 'string' ? entry.remote_addr : undefined,
          status: inferredError || statusCode >= 400 ? 'error' : 'ok',
          request: entry.request,
          response: entry.response,
          error: typeof description === 'string' ? description : undefined,
        };
      });

      setDebugEventLogs(mapped);
    } catch {
      // Keep debugger usable with last-known logs when runtime endpoint is unavailable.
    }
  }, []);

  const onLoadRuntimeInfo = useCallback(async () => {
    const apiBase = API_BASE_URL.replace(/\/$/, '');
    try {
      const response = await fetch(`${apiBase}/client-api/runtime/info`);
      if (!response.ok) {
        throw new Error(`runtime info failed (${response.status})`);
      }
      const payload = await response.json();
      const runtime = payload?.runtime as Partial<RuntimeInfoState> | undefined;
      if (!runtime) {
        return;
      }
      const envValues = normalizeRuntimeEnvValues(runtime.env_values);
      const rawService = runtime.service && typeof runtime.service === 'object'
        ? runtime.service as Record<string, unknown>
        : undefined;
      setRuntimeInfo({
        api_host: String(runtime.api_host || ''),
        api_port: String(runtime.api_port || ''),
        web_port: String(runtime.web_port || ''),
        database_path: String(runtime.database_path || ''),
        storage_path: String(runtime.storage_path || ''),
        logs_path: String(runtime.logs_path || ''),
        workspace_dir: String(runtime.workspace_dir || ''),
        api_enabled: Boolean(runtime.api_enabled),
        env_file_path: typeof runtime.env_file_path === 'string' ? runtime.env_file_path : undefined,
        env_values: envValues,
        service: rawService ? {
          mode: String(rawService.mode || ''),
          name: String(rawService.name || ''),
          available: Boolean(rawService.available),
          active: Boolean(rawService.active),
          status: String(rawService.status || 'unknown'),
          requested_mode: typeof rawService.requested_mode === 'string' ? rawService.requested_mode : undefined,
          note: typeof rawService.note === 'string' ? rawService.note : undefined,
        } : undefined,
      });

      if (!runtimeEnvDirty) {
        setRuntimeEnvSource(envValues);
        setRuntimeEnvRows(buildRuntimeEnvRows(envValues));
      }
    } catch {
      // Keep settings usable even when runtime info endpoint is unreachable.
    }
  }, [runtimeEnvDirty]);

  const onRuntimeServiceAction = async (action: 'start' | 'stop' | 'restart') => {
    setRuntimeServiceActionInFlight(action);
    setErrorText('');
    try {
      const apiBase = API_BASE_URL.replace(/\/$/, '');
      const response = await fetch(`${apiBase}/client-api/runtime/service`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ action }),
      });

      const payload = await response.json();
      if (!payload?.ok) {
        throw new Error(payload?.description || `Unable to ${action} service`);
      }

      await onLoadRuntimeInfo();
      await onCheckServerHealth();
      setCallbackToast(`Service action executed: ${action}.`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : `Service ${action} failed`);
    } finally {
      setRuntimeServiceActionInFlight('');
    }
  };

  const onClearRuntimeLogs = async () => {
    setErrorText('');
    try {
      const apiBase = API_BASE_URL.replace(/\/$/, '');
      const response = await fetch(`${apiBase}/client-api/runtime/logs/clear`, {
        method: 'POST',
      });
      const payload = await response.json();
      if (!payload?.ok) {
        throw new Error(payload?.description || 'Unable to clear debugger logs');
      }
      await onLoadRuntimeLogs();
      setCallbackToast('Debugger logs cleared.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to clear debugger logs');
    }
  };

  const onAddRuntimeEnvRow = () => {
    setRuntimeEnvRows((prev) => [...prev, { id: `env-${Date.now()}`, key: '', value: '' }]);
  };

  const onUpdateRuntimeEnvRow = (id: string, field: 'key' | 'value', value: string) => {
    setRuntimeEnvRows((prev) => prev.map((row) => (
      row.id === id
        ? {
          ...row,
          [field]: value,
        }
        : row
    )));
  };

  const onRemoveRuntimeEnvRow = (id: string) => {
    setRuntimeEnvRows((prev) => {
      const next = prev.filter((row) => row.id !== id);
      return next.length > 0 ? next : [{ id: `env-${Date.now()}`, key: '', value: '' }];
    });
  };

  const onSaveRuntimeEnv = async () => {
    setIsRuntimeEnvSaving(true);
    setErrorText('');
    try {
      const payloadValues: Record<string, string | null> = {};
      Object.keys(runtimeEnvSource).forEach((key) => {
        if (!(key in runtimeEnvDraftByKey)) {
          payloadValues[key] = null;
        }
      });
      Object.entries(runtimeEnvDraftByKey).forEach(([key, value]) => {
        payloadValues[key] = value;
      });

      if (Object.keys(payloadValues).length === 0) {
        setCallbackToast('No env changes to save.');
        setIsRuntimeEnvSaving(false);
        return;
      }

      const apiBase = API_BASE_URL.replace(/\/$/, '');
      const response = await fetch(`${apiBase}/client-api/runtime/env`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ values: payloadValues }),
      });
      const payload = await response.json();
      if (!payload?.ok) {
        throw new Error(payload?.description || 'Unable to save env values');
      }

      const nextValues = normalizeRuntimeEnvValues(payload?.result?.values);
      setRuntimeEnvSource(nextValues);
      setRuntimeEnvRows(buildRuntimeEnvRows(nextValues));
      setCallbackToast('.env saved.');
      await onLoadRuntimeInfo();
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Saving env failed');
    } finally {
      setIsRuntimeEnvSaving(false);
    }
  };

  useEffect(() => {
    void onCheckServerHealth();
    void onLoadRuntimeInfo();
    void onLoadRuntimeLogs();

    const timer = window.setInterval(() => {
      void onCheckServerHealth();
      void onLoadRuntimeInfo();
      void onLoadRuntimeLogs();
    }, 15_000);

    return () => {
      clearInterval(timer);
    };
  }, [onCheckServerHealth, onLoadRuntimeInfo, onLoadRuntimeLogs]);

  useEffect(() => {
    void onLoadRuntimeLogs();
    const timer = window.setInterval(() => {
      void onLoadRuntimeLogs();
    }, 1000);

    return () => {
      clearInterval(timer);
    };
  }, [onLoadRuntimeLogs]);

  const openCreateUserModal = () => {
    const randomId = Math.floor(Math.random() * 900000 + 10000);
    setUserModalMode('create');
    setUserDraft(randomUserDraft(randomId));
    setBusinessDraftBotToken(selectedBotToken);
    setBusinessConnectionDraftId('');
    setBusinessConnectionDraftEnabled(true);
    setBusinessRightsDraft(defaultBusinessRightsDraft());
    setShowUserModal(true);
  };

  const openEditUserModal = (user: SimUser) => {
    const businessStateKey = `${selectedBotToken}:${user.id}`;
    const connection = businessConnectionByUserKey[businessStateKey];
    setUserModalMode('edit');
    setUserDraft(buildUserDraftFromSimUser(user));
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

    const normalizedFirstName = botDraft.first_name.trim();
    const normalizedUsername = botDraft.username.trim();
    if (!normalizedFirstName || !normalizedUsername) {
      setErrorText('Bot first name and username are required.');
      setIsBootstrapping(false);
      return;
    }

    let parsedCommands: GeneratedBotCommand[] = [];
    try {
      parsedCommands = parseBotCommandsFromEditor(botDraft.commands_text);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Invalid command format');
      setIsBootstrapping(false);
      return;
    }

    const commandLanguageCode = optionalTrimmedText(botDraft.commands_language_code);
    let targetToken = selectedBotToken;

    try {
      if (botModalMode === 'create') {
        const created = await createSimBot({
          first_name: normalizedFirstName,
          username: normalizedUsername,
        });

        const bot: SimBot = {
          id: created.id,
          token: created.token,
          username: created.username,
          first_name: created.first_name,
        };

        setAvailableBots((prev) => [...prev, bot]);
        setSelectedBotToken(bot.token);
        targetToken = bot.token;
      } else {
        const updated = await updateSimBot(selectedBotToken, {
          first_name: normalizedFirstName,
          username: normalizedUsername,
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

      await setMyName(targetToken, {
        name: normalizedFirstName,
      });
      await setMyDescription(targetToken, {
        description: optionalTrimmedText(botDraft.description),
      });
      await setMyShortDescription(targetToken, {
        short_description: optionalTrimmedText(botDraft.short_description),
      });

      if (botDraft.remove_profile_photo) {
        await removeMyProfilePhoto(targetToken);
      } else {
        const profilePhotoRef = optionalTrimmedText(botDraft.profile_photo_ref);
        if (profilePhotoRef) {
          await setMyProfilePhoto(targetToken, {
            photo: {
              type: 'static',
              photo: profilePhotoRef,
            },
          });
        }
      }

      if (parsedCommands.length > 0) {
        await setMyCommands(targetToken, {
          commands: parsedCommands,
          language_code: commandLanguageCode,
        });
      } else {
        await deleteMyCommands(targetToken, {
          language_code: commandLanguageCode,
        });
      }

      await setMyDefaultAdministratorRights(targetToken, {
        rights: mapBotAdminRightsDraftToServer(botDraft.group_default_admin_rights, false),
        for_channels: false,
      });
      await setMyDefaultAdministratorRights(targetToken, {
        rights: mapBotAdminRightsDraftToServer(botDraft.channel_default_admin_rights, true),
        for_channels: true,
      });

      setBotDefaultCommandsByToken((prev) => ({
        ...prev,
        [targetToken]: parsedCommands,
      }));
      setManagedBotSettingsByToken((prev) => ({
        ...prev,
        [targetToken]: {
          enabled: botManagedEnabledDraft,
        },
      }));

      setShowBotModal(false);
      setBotDraft(emptyBotDraft());
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
      const parsedGiftCount = Math.floor(Number(userDraft.gift_count));
      const saved = await upsertSimUser({
        id,
        first_name: userDraft.first_name,
        last_name: optionalTrimmedText(userDraft.last_name),
        username: userDraft.username,
        phone_number: optionalTrimmedText(userDraft.phone_number),
        photo_url: optionalTrimmedText(userDraft.photo_url),
        bio: optionalTrimmedText(userDraft.bio),
        is_premium: userDraft.is_premium,
        business_name: optionalTrimmedText(userDraft.business_name),
        business_intro: optionalTrimmedText(userDraft.business_intro),
        business_location: optionalTrimmedText(userDraft.business_location),
        gift_count: Number.isFinite(parsedGiftCount) && parsedGiftCount >= 0 ? parsedGiftCount : 0,
      });

      const normalized = normalizeSimUser(saved as SimUser);

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
      setUserDraft(emptyUserDraft());
      setShowUserModal(false);
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
      managedBotRequestPayload?: GeneratedKeyboardButtonRequestManagedBot;
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
        managed_bot_request: options.managedBotRequestPayload,
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

  const registerWebAppQueryContext = (
    source: 'reply_keyboard_web_app' | 'inline_keyboard_web_app',
    buttonText: string,
    url: string,
  ): string => {
    const nextQueryId = `webapp_${source}_${selectedUser.id}_${Date.now()}`;
    setMiniAppModal({
      source,
      buttonText,
      queryId: nextQueryId,
      url,
    });
    setWebAppLab((prev) => ({
      ...prev,
      lastQueryId: nextQueryId,
      answerUrl: prev.answerUrl || url,
      preparedButtonUrl: prev.preparedButtonUrl || url,
    }));
    setCallbackToast(`Mini App opened via "${buttonText}". web_app_query_id: ${nextQueryId}`);
    return nextQueryId;
  };

  const onSubmitManagedBotRequestModal = async () => {
    if (!managedBotRequestModal) {
      return;
    }

    const requestId = Math.floor(Number(managedBotRequestModal.requestId));
    if (!Number.isFinite(requestId) || requestId <= 0) {
      setErrorText('request_managed_bot.request_id is invalid.');
      return;
    }

    const suggestedName = managedBotRequestModal.suggestedName.trim();
    const suggestedUsername = normalizeManagedBotUsernameDraft(managedBotRequestModal.suggestedUsername);
    if (suggestedUsername && !/^[A-Za-z0-9_]{4,32}$/.test(suggestedUsername)) {
      setErrorText('suggested_username must contain 4-32 letters, numbers, or underscore.');
      return;
    }

    const payload: GeneratedKeyboardButtonRequestManagedBot = {
      request_id: requestId,
      suggested_name: suggestedName || undefined,
      suggested_username: suggestedUsername || undefined,
    };

    const displayName = suggestedName || 'Managed bot';
    const outgoingText = suggestedUsername
      ? `🤖 Requested managed bot: ${displayName} (@${suggestedUsername})`
      : `🤖 Requested managed bot: ${displayName}`;

    const sent = await sendStructuredReplyKeyboardMessage(outgoingText, {
      managedBotRequestPayload: payload,
    });

    if (sent) {
      setManagedBotRequestModal(null);
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
    let managedBotRequestPayload: GeneratedKeyboardButtonRequestManagedBot | undefined;

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
    } else if (button.request_managed_bot) {
      const request = button.request_managed_bot;
      if (!Number.isFinite(request.request_id) || request.request_id <= 0) {
        setErrorText('request_managed_bot.request_id is invalid.');
        return;
      }

      if (!activeManagedBotSettings.enabled) {
        setErrorText('request_managed_bot is disabled in this bot settings profile.');
        return;
      }

      setManagedBotRequestModal({
        buttonText: text,
        requestId: String(request.request_id),
        suggestedName: request.suggested_name?.trim() || 'Managed bot',
        suggestedUsername: request.suggested_username?.trim().replace(/^@+/, '') || '',
      });
      setMessageMenu(null);
      return;
    }

    if (button.web_app?.url) {
      const webAppQueryId = registerWebAppQueryContext('reply_keyboard_web_app', text, button.web_app.url);
      webAppDataPayload = {
        button_text: text,
        data: JSON.stringify({
          source: 'reply_keyboard_web_app',
          url: button.web_app.url,
          web_app_query_id: webAppQueryId,
          actor_user_id: selectedUser.id,
          actor_username: selectedUser.username || null,
          timestamp: Math.floor(Date.now() / 1000),
        }),
      };
    }

    const hasStructuredPayload = Boolean(
      usersSharedPayload
      || chatSharedPayload
      || webAppDataPayload
      || managedBotRequestPayload,
    );
    if (hasStructuredPayload) {
      await sendStructuredReplyKeyboardMessage(outgoingText, {
        usersSharedPayload,
        chatSharedPayload,
        webAppDataPayload,
        managedBotRequestPayload,
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

  const paidMediaPurchaseKeyFor = (botToken: string, actorUserId: number, chatId: number, messageId: number): string => (
    `${botToken}:${actorUserId}:${chatId}:${messageId}`
  );

  const paidMediaPurchaseLegacyKeyFor = (actorUserId: number, chatId: number, messageId: number): string => (
    `${actorUserId}:${chatId}:${messageId}`
  );

  const isPaidMediaPurchasedForActor = (botToken: string, actorUserId: number, chatId: number, messageId: number): boolean => {
    const key = paidMediaPurchaseKeyFor(botToken, actorUserId, chatId, messageId);
    if (paidMediaPurchaseByActorKey[key]) {
      return true;
    }

    const legacyKey = paidMediaPurchaseLegacyKeyFor(actorUserId, chatId, messageId);
    return Boolean(paidMediaPurchaseByActorKey[legacyKey]);
  };

  const submitPaidReactionFromModal = async () => {
    if (!paidReactionModal) {
      return;
    }

    const targetMessage = messages.find((message) => (
      message.botToken === selectedBotToken
      && message.chatId === paidReactionModal.chatId
      && message.id === paidReactionModal.messageId
    ));

    if (!targetMessage) {
      setPaidReactionModal(null);
      return;
    }

    const relatedChannel = groupChats.find((chat) => (
      chat.id === targetMessage.chatId
      && chat.type === 'channel'
      && !chat.isDirectMessages
    ));
    if (!relatedChannel?.settings?.paidStarReactionsEnabled) {
      setErrorText('Paid star reactions are disabled for this channel.');
      return;
    }

    const requestedAmount = Math.floor(Number(paidReactionAmountDraft));
    if (!Number.isFinite(requestedAmount) || requestedAmount <= 0) {
      setErrorText('Paid reaction stars must be a positive number.');
      return;
    }

    if (walletState.stars < requestedAmount) {
      setErrorText(`Not enough stars. You need ${requestedAmount}⭐ for this paid reaction.`);
      return;
    }

    const actorKey = `${selectedUser.id}:0`;
    const current = targetMessage.actorReactions?.[actorKey] || [];
    const currentPaidCount = current.filter((item) => item === PAID_REACTION_KEY).length;
    const currentEmojiReactions = current.filter((item) => item !== PAID_REACTION_KEY);
    const nextReaction = [
      ...currentEmojiReactions,
      ...Array(currentPaidCount + requestedAmount).fill(PAID_REACTION_KEY),
    ];

    try {
      setIsPaidReactionSubmitting(true);
      await setUserMessageReaction(selectedBotToken, {
        chat_id: targetMessage.chatId,
        message_id: targetMessage.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        reaction: nextReaction.map((item) => reactionKeyToPayload(item)),
      });
      setWalletState((prev) => ({
        ...prev,
        stars: Math.max(prev.stars - requestedAmount, 0),
      }));
      setPaidReactionModal(null);
      setPaidReactionAmountDraft('1');
      setMessageMenu(null);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Reaction failed');
    } finally {
      setIsPaidReactionSubmitting(false);
    }
  };

  const onReactToMessage = async (message: ChatMessage, reactionKey: string) => {
    const actorKey = `${selectedUser.id}:0`;
    const current = message.actorReactions?.[actorKey] || [];
    const currentPaidCount = current.filter((item) => item === PAID_REACTION_KEY).length;
    const currentEmojiReactions = current.filter((item) => item !== PAID_REACTION_KEY);

    let nextReaction: string[] = [];

    if (reactionKey === PAID_REACTION_KEY) {
      const isPaidEnabledInChannel = Boolean(
        selectedGroup
        && selectedGroup.type === 'channel'
        && !selectedGroup.isDirectMessages
        && selectedGroup.id === message.chatId
        && selectedGroup.settings?.paidStarReactionsEnabled,
      );
      if (!isPaidEnabledInChannel) {
        setErrorText('Paid star reactions are disabled for this channel.');
        return;
      }

      setPaidReactionModal({
        chatId: message.chatId,
        messageId: message.id,
        currentPaidCount,
      });
      setPaidReactionAmountDraft('1');
      setMessageMenu(null);
      return;
    } else {
      const nextEmojiReactions = currentEmojiReactions.includes(reactionKey)
        ? currentEmojiReactions.filter((item) => item !== reactionKey)
        : [reactionKey];
      nextReaction = [
        ...nextEmojiReactions,
        ...Array(currentPaidCount).fill(PAID_REACTION_KEY),
      ];
    }

    try {
      await setUserMessageReaction(selectedBotToken, {
        chat_id: message.chatId,
        message_id: message.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        reaction: nextReaction.map((item) => reactionKeyToPayload(item)),
      });
      setMessageMenu(null);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Reaction failed');
    }
  };

  const onPurchasePaidMedia = async (message: ChatMessage) => {
    if (!message.isPaidPost) {
      return;
    }

    const starCost = Number(message.paidMessageStarCount || 0);
    if (!Number.isFinite(starCost) || starCost <= 0) {
      return;
    }

    const purchaseKey = paidMediaPurchaseKeyFor(selectedBotToken, selectedUser.id, message.chatId, message.id);
    if (isPaidMediaPurchasedForActor(selectedBotToken, selectedUser.id, message.chatId, message.id)) {
      return;
    }

    if (walletState.stars < starCost) {
      setErrorText(`Not enough stars. You need ${starCost}⭐ to purchase this paid post.`);
      return;
    }

    try {
      setPurchasingPaidMediaMessageId(message.id);
      const purchaseResult = await purchasePaidMedia(selectedBotToken, {
        chat_id: message.chatId,
        message_id: message.id,
        user_id: selectedUser.id,
        first_name: selectedUser.first_name,
        username: selectedUser.username,
        paid_media_payload: message.paidMediaPayload,
      });

      if (!purchaseResult.already_purchased) {
        setWalletState((prev) => ({
          ...prev,
          stars: Math.max(prev.stars - starCost, 0),
        }));
      }

      setPaidMediaPurchaseByActorKey((prev) => ({
        ...prev,
        [purchaseKey]: true,
      }));

      setCallbackToast(
        purchaseResult.already_purchased
          ? 'You already purchased this paid media.'
          : `Paid media unlocked for ${starCost}⭐`,
      );
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Paid media purchase failed');
    } finally {
      setPurchasingPaidMediaMessageId(null);
    }
  };

  const onApproveSuggestedPostMessage = async (message: ChatMessage) => {
    if (!selectedGroup?.isDirectMessages) {
      setErrorText('Suggested post moderation is only available in direct messages chats.');
      return;
    }
    if (!canManageSuggestedPostsInSelectedChat) {
      setErrorText('Only channel owner/admin with direct-messages rights can approve suggested posts.');
      return;
    }

    const now = Math.floor(Date.now() / 1000);
    setErrorText('');
    setSuggestedPostActionMessageId(message.id);
    try {
      await approveSuggestedPost(
        selectedBotToken,
        {
          chat_id: message.chatId,
          message_id: message.id,
        },
        selectedUser.id,
      );
      setMessages((prev) => prev.map((item) => {
        if (item.botToken !== selectedBotToken || item.chatId !== message.chatId || item.id !== message.id) {
          return item;
        }
        const approvedSendDate = item.suggestedPostInfo?.send_date || now;
        const approvedPrice = item.suggestedPostInfo?.price || item.suggestedPostApproved?.price;
        return {
          ...item,
          suggestedPostInfo: {
            ...(item.suggestedPostInfo || {}),
            state: 'approved',
            send_date: approvedSendDate,
          },
          suggestedPostApproved: {
            ...(item.suggestedPostApproved || {}),
            send_date: approvedSendDate,
            price: approvedPrice,
          },
          suggestedPostApprovalFailed: undefined,
          suggestedPostDeclined: undefined,
          suggestedPostPaid: undefined,
          suggestedPostRefunded: undefined,
        };
      }));
      setCallbackToast('Suggested post approved.');
      setMessageMenu(null);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to approve suggested post');
    } finally {
      setSuggestedPostActionMessageId(null);
    }
  };

  const onDeclineSuggestedPostMessage = async (message: ChatMessage, comment?: string): Promise<boolean> => {
    if (!selectedGroup?.isDirectMessages) {
      setErrorText('Suggested post moderation is only available in direct messages chats.');
      return false;
    }
    if (!canManageSuggestedPostsInSelectedChat) {
      setErrorText('Only channel owner/admin with direct-messages rights can decline suggested posts.');
      return false;
    }

    const normalizedComment = comment?.trim();
    if (normalizedComment && normalizedComment.length > 128) {
      setErrorText('Decline comment must be 128 characters or less.');
      return false;
    }

    setErrorText('');
    setSuggestedPostActionMessageId(message.id);
    try {
      await declineSuggestedPost(
        selectedBotToken,
        {
          chat_id: message.chatId,
          message_id: message.id,
          comment: normalizedComment || undefined,
        },
        selectedUser.id,
      );
      setMessages((prev) => prev.map((item) => {
        if (item.botToken !== selectedBotToken || item.chatId !== message.chatId || item.id !== message.id) {
          return item;
        }
        return {
          ...item,
          suggestedPostInfo: {
            ...(item.suggestedPostInfo || {}),
            state: 'declined',
          },
          suggestedPostApproved: undefined,
          suggestedPostApprovalFailed: undefined,
          suggestedPostDeclined: {
            ...(item.suggestedPostDeclined || {}),
            comment: normalizedComment || undefined,
          },
          suggestedPostPaid: undefined,
          suggestedPostRefunded: undefined,
        };
      }));
      setCallbackToast('Suggested post declined.');
      setMessageMenu(null);
      return true;
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'Unable to decline suggested post');
      return false;
    } finally {
      setSuggestedPostActionMessageId(null);
    }
  };

  const openDeclineSuggestedPostModal = (message: ChatMessage) => {
    if (!selectedGroup?.isDirectMessages) {
      setErrorText('Suggested post moderation is only available in direct messages chats.');
      return;
    }
    if (!canManageSuggestedPostsInSelectedChat) {
      setErrorText('Only channel owner/admin with direct-messages rights can decline suggested posts.');
      return;
    }

    setErrorText('');
    setDeclineSuggestedPostModal({
      chatId: message.chatId,
      messageId: message.id,
      comment: message.suggestedPostDeclined?.comment || '',
    });
    setMessageMenu(null);
  };

  const submitDeclineSuggestedPostFromModal = async () => {
    if (!declineSuggestedPostModal || !selectedGroup) {
      return;
    }

    const target = messages.find((message) => (
      message.botToken === selectedBotToken
      && message.chatId === declineSuggestedPostModal.chatId
      && message.id === declineSuggestedPostModal.messageId
    ));

    if (!target) {
      setErrorText('Suggested post message was not found.');
      return;
    }

    const success = await onDeclineSuggestedPostMessage(target, declineSuggestedPostModal.comment);
    if (success) {
      setDeclineSuggestedPostModal(null);
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
      registerWebAppQueryContext('inline_keyboard_web_app', button.text || 'web_app', button.web_app.url);
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

  const formatDebugValue = (value: unknown): string => {
    if (typeof value === 'undefined') {
      return 'undefined';
    }
    if (value === null) {
      return 'null';
    }
    if (typeof value === 'string') {
      return value;
    }
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  };

  const copyDebugLogPart = async (label: string, value: unknown) => {
    try {
      await navigator.clipboard.writeText(formatDebugValue(value));
      setCallbackToast(`${label} copied.`);
    } catch {
      setErrorText(`${label} copy failed`);
    }
  };

  const copyWholeDebugLog = async (entry: DebugEventLog) => {
    const payload = {
      at: new Date(entry.at).toISOString(),
      source: entry.source,
      method: entry.method,
      path: entry.path,
      query: entry.query,
      status_code: entry.statusCode,
      duration_ms: entry.durationMs,
      remote_addr: entry.remoteAddr,
      status: entry.status,
      request: entry.request,
      response: entry.response,
      error: entry.error,
    };
    await copyDebugLogPart('Debug log', payload);
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

  const renderSuggestedPostCard = (message: ChatMessage) => {
    const info = message.suggestedPostInfo;
    const approved = message.suggestedPostApproved;
    const approvalFailed = message.suggestedPostApprovalFailed;
    const declined = message.suggestedPostDeclined;
    const paid = message.suggestedPostPaid;
    const refunded = message.suggestedPostRefunded;

    if (!info && !approved && !approvalFailed && !declined && !paid && !refunded) {
      return null;
    }

    const rawState = (
      info?.state
      || (refunded
        ? 'refunded'
        : (paid
          ? 'paid'
          : (approved
            ? 'approved'
            : (approvalFailed
              ? 'approval_failed'
              : (declined ? 'declined' : 'pending')))))
    ).trim();
    const normalizedState = rawState ? rawState.toLowerCase() : 'pending';
    const displayState = normalizedState
      .split('_')
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
    const resolvedPrice = info?.price || approved?.price || approvalFailed?.price;
    const resolvedSendDate = info?.send_date || approved?.send_date;
    const priceLabel = resolvedPrice
      ? `${resolvedPrice.amount} ${resolvedPrice.currency}`
      : null;
    const paidAmount = paid?.amount ?? paid?.star_amount?.amount;
    const paidLabel = paid
      ? `${Number.isFinite(Number(paidAmount)) ? `${paidAmount} ` : ''}${paid.currency}`.trim()
      : null;
    const isPending = normalizedState === 'pending' || normalizedState === 'approval_failed';
    const actionInProgress = suggestedPostActionMessageId === message.id;
    const canManageActions = !message.service
      && canManageSuggestedPostsInSelectedChat
      && isPending
      && selectedGroup?.isDirectMessages
      && message.chatId === selectedGroup.id;
    const stateBadgeClass = normalizedState === 'approved'
      ? 'border-emerald-300/45 bg-emerald-900/30 text-emerald-100'
      : normalizedState === 'declined'
        ? 'border-red-300/45 bg-red-900/30 text-red-100'
        : normalizedState === 'approval_failed'
          ? 'border-amber-300/45 bg-amber-900/30 text-amber-100'
          : normalizedState === 'paid'
            ? 'border-cyan-300/45 bg-cyan-900/30 text-cyan-100'
            : normalizedState === 'refunded'
              ? 'border-orange-300/45 bg-orange-900/30 text-orange-100'
              : 'border-[#7cc8ff]/45 bg-[#21506f]/70 text-[#d6eeff]';

    return (
      <div className="mb-2 rounded-xl border border-[#5e89a7]/45 bg-[#14324a]/80 p-2.5 text-[#d9edfb]">
        <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
          <span className="rounded border border-white/20 bg-white/10 px-1.5 py-0.5">Suggested post</span>
          <span className={`rounded border px-1.5 py-0.5 ${stateBadgeClass}`}>{displayState}</span>
          {priceLabel ? (
            <span className="rounded border border-emerald-300/35 bg-emerald-900/25 px-1.5 py-0.5 text-emerald-100">{priceLabel}</span>
          ) : null}
          {paidLabel ? (
            <span className="rounded border border-cyan-300/35 bg-cyan-900/25 px-1.5 py-0.5 text-cyan-100">paid {paidLabel}</span>
          ) : null}
          {typeof resolvedSendDate === 'number' ? (
            <span className="text-[#9dc4dc]">send {formatMessageTime(resolvedSendDate)}</span>
          ) : null}
        </div>
        {approvalFailed?.price ? (
          <div className="mt-1 text-[11px] text-amber-100">
            Approval failed for {approvalFailed.price.amount} {approvalFailed.price.currency}.
          </div>
        ) : null}
        {declined?.comment ? (
          <div className="mt-1 text-[11px] text-[#b8d7eb]">Reason: {declined.comment}</div>
        ) : null}
        {refunded?.reason ? (
          <div className="mt-1 text-[11px] text-orange-100">Refund reason: {refunded.reason}</div>
        ) : null}
        {canManageActions ? (
          <div className="mt-2 flex items-center justify-end gap-1.5">
            <button
              type="button"
              onClick={() => void onApproveSuggestedPostMessage(message)}
              disabled={actionInProgress}
              className="rounded-md border border-emerald-300/45 bg-emerald-900/30 px-2 py-1 text-[11px] text-emerald-100 hover:bg-emerald-900/40 disabled:opacity-60"
            >
              {actionInProgress ? 'Working...' : 'Approve'}
            </button>
            <button
              type="button"
              onClick={() => openDeclineSuggestedPostModal(message)}
              disabled={actionInProgress}
              className="rounded-md border border-red-300/40 bg-red-900/25 px-2 py-1 text-[11px] text-red-100 hover:bg-red-900/35 disabled:opacity-60"
            >
              Decline
            </button>
          </div>
        ) : null}
      </div>
    );
  };

  const renderStoryCard = (message: ChatMessage) => {
    if (!message.story) {
      return null;
    }

    const storyOwner = message.story.chat.title
      || (message.story.chat.username ? `@${message.story.chat.username}` : `chat ${message.story.chat.id}`);
    const canEditOwnStory = isStoryOwnedByActiveUser(message.story);

    return (
      <div className="mb-2 rounded-xl border border-[#4f7ea0]/45 bg-[#17374d]/75 p-3 text-[#dcf0ff]">
        <div className="text-sm font-semibold">Story reference</div>
        <div className="mt-1 text-xs">#{message.story.id} from {storyOwner}</div>
        <div className="mt-2 flex flex-wrap items-center justify-end gap-1.5">
          <button
            type="button"
            onClick={() => setActiveStoryPreviewKey(storyShelfKeyFor(message.story!))}
            className="rounded-md border border-[#8fd3ff]/45 bg-[#1f5379]/55 px-2 py-1 text-[11px] text-[#d7efff] hover:bg-[#2b6a98]"
          >
            Preview
          </button>
          {canEditOwnStory ? (
            <>
              <button
                type="button"
                onClick={() => openStoryEditorFromMessage(message)}
                disabled={isStoryActionRunning}
                className="rounded-md border border-[#7ec8fb]/45 bg-[#1f5379] px-2 py-1 text-[11px] text-[#d7efff] hover:bg-[#2b6a98] disabled:opacity-60"
              >
                Edit
              </button>
              <button
                type="button"
                onClick={() => void onQuickDeleteStory(message)}
                disabled={isStoryActionRunning}
                className="rounded-md border border-red-300/35 bg-red-900/25 px-2 py-1 text-[11px] text-red-100 hover:bg-red-900/35 disabled:opacity-60"
              >
                Delete
              </button>
            </>
          ) : (
            <button
              type="button"
              onClick={() => void onQuickRepostStory(message)}
              disabled={isStoryActionRunning}
              className="rounded-md border border-[#7ec8fb]/45 bg-[#1f5379] px-2 py-1 text-[11px] text-[#d7efff] hover:bg-[#2b6a98] disabled:opacity-60"
            >
              Repost
            </button>
          )}
        </div>
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

  const buildChecklistActorUser = (): GeneratedUser => ({
    id: selectedUser.id,
    is_bot: false,
    first_name: selectedUser.first_name || 'User',
    last_name: optionalTrimmedText(selectedUser.last_name),
    username: optionalTrimmedText(selectedUser.username),
    is_premium: Boolean(selectedUser.is_premium),
  });

  const onToggleChecklistTask = (message: ChatMessage, taskId: number) => {
    if (!message.checklist) {
      return;
    }

    const canMarkTask = Boolean(message.checklist.others_can_mark_tasks_as_done) || message.isOutgoing;
    if (!canMarkTask) {
      setErrorText('Only the checklist sender can mark tasks as done in this chat.');
      return;
    }

    const actor = buildChecklistActorUser();
    const now = Math.floor(Date.now() / 1000);

    setMessages((prev) => prev.map((item) => {
      if (item.id !== message.id || item.chatId !== message.chatId || item.botToken !== message.botToken || !item.checklist) {
        return item;
      }

      return {
        ...item,
        checklist: {
          ...item.checklist,
          tasks: item.checklist.tasks.map((task) => {
            if (task.id !== taskId) {
              return task;
            }

            const isDone = Number.isFinite(Number(task.completion_date)) && Number(task.completion_date) > 0;
            if (isDone) {
              return {
                ...task,
                completed_by_user: undefined,
                completed_by_chat: undefined,
                completion_date: undefined,
              };
            }

            return {
              ...task,
              completed_by_user: actor,
              completed_by_chat: undefined,
              completion_date: now,
            };
          }),
        },
      };
    }));
  };

  const renderChecklistCard = (message: ChatMessage) => {
    if (!message.checklist) {
      return null;
    }

    const canMarkTask = Boolean(message.checklist.others_can_mark_tasks_as_done) || message.isOutgoing;

    return (
      <div className="mb-2 rounded-xl border border-[#4f7ea0]/45 bg-[#17374d]/75 p-3 text-[#dcf0ff]">
        <div className="mb-2 text-sm font-semibold text-white">✅ {message.checklist.title}</div>
        <div className="space-y-1.5">
          {message.checklist.tasks.map((task) => {
            const done = Number.isFinite(Number(task.completion_date)) && Number(task.completion_date) > 0;
            return (
              <div key={`checklist-task-${message.id}-${task.id}`} className="flex items-start gap-2 text-xs">
                <button
                  type="button"
                  onClick={() => onToggleChecklistTask(message, task.id)}
                  disabled={!canMarkTask}
                  className={`mt-0.5 inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full border ${done ? 'border-emerald-300/65 bg-emerald-700/35 text-emerald-100' : 'border-white/30 bg-black/20 text-[#cbe4f7]'} ${canMarkTask ? 'hover:opacity-90' : 'cursor-not-allowed opacity-60'}`}
                  title={canMarkTask ? 'Toggle task completion' : 'Only sender can mark tasks'}
                >
                  {done ? '✓' : '•'}
                </button>
                <div className="min-w-0 flex-1">
                  <p className={`${done ? 'text-[#b9d3e5] line-through' : 'text-[#e1f3ff]'}`}>{task.text}</p>
                  {done ? (
                    <p className="mt-0.5 text-[10px] text-[#98bed9]">
                      done by {task.completed_by_user?.first_name || task.completed_by_chat?.title || 'unknown'}
                    </p>
                  ) : null}
                </div>
              </div>
            );
          })}
        </div>
        <div className="mt-2 flex flex-wrap items-center gap-1.5 text-[10px] text-[#9ec3dc]">
          {message.checklist.others_can_add_tasks ? <span className="rounded border border-white/20 bg-black/20 px-1.5 py-0.5">others can add tasks</span> : null}
          {message.checklist.others_can_mark_tasks_as_done ? <span className="rounded border border-white/20 bg-black/20 px-1.5 py-0.5">others can mark done</span> : null}
          {!canMarkTask ? <span className="rounded border border-white/20 bg-black/20 px-1.5 py-0.5">sender-only completion</span> : null}
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

  const renderGiftCard = (message: ChatMessage) => {
    if (!message.gift) {
      return null;
    }

    const gift = message.gift.gift;
    const emoji = gift.sticker?.emoji?.trim() || '🎁';
    const giftId = gift.id?.trim() || 'gift';
    const starCount = Number.isFinite(Number(gift.star_count))
      ? Math.max(Math.trunc(Number(gift.star_count)), 0)
      : 0;
    const note = message.gift.text?.trim() || '';

    return (
      <div className="mb-2 rounded-2xl border border-amber-200/35 bg-gradient-to-br from-[#4b2f1d]/90 via-[#6e3e25]/85 to-[#2f2638]/85 p-3 text-[#fff4d8] shadow-[0_10px_24px_rgba(0,0,0,0.28)]">
        <div className="flex items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl border border-white/25 bg-white/10 text-2xl">
            {emoji}
          </div>
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm font-semibold text-white">{giftId}</div>
            <div className="mt-0.5 text-[11px] text-[#ffe3ad]">
              {starCount > 0 ? `${starCount}⭐ gift` : 'Gift sent'}
            </div>
          </div>
        </div>
        {note ? (
          <div className="mt-2 rounded-lg border border-white/20 bg-black/20 px-2 py-1.5 text-[11px] text-[#fff4db]">
            “{note}”
          </div>
        ) : null}
      </div>
    );
  };

  const renderMediaContent = (message: ChatMessage, compact = false) => {
    if (!message.media) {
      return null;
    }

    const requiresPurchase = message.isPaidPost === true
      && typeof message.paidMessageStarCount === 'number'
      && message.paidMessageStarCount > 0
      && message.fromUserId !== selectedUser.id;
    if (requiresPurchase && !isPaidMediaPurchasedForActor(selectedBotToken, selectedUser.id, message.chatId, message.id)) {
      return (
        <div className={compact
          ? 'flex h-40 w-full flex-col items-center justify-center gap-2 rounded-xl border border-amber-300/35 bg-[linear-gradient(160deg,rgba(41,21,8,0.95),rgba(88,45,16,0.82),rgba(22,24,36,0.88))] p-3 text-center text-amber-100 shadow-[0_12px_28px_rgba(0,0,0,0.32)]'
          : 'flex min-h-[180px] w-full max-w-[320px] flex-col items-center justify-center gap-2 rounded-xl border border-amber-300/35 bg-[linear-gradient(160deg,rgba(41,21,8,0.95),rgba(88,45,16,0.82),rgba(22,24,36,0.88))] p-4 text-center text-amber-100 shadow-[0_16px_34px_rgba(0,0,0,0.34)]'}
        >
          <div className="text-xl">🔒</div>
          <div className="text-sm font-semibold text-amber-50">Paid media locked</div>
          <div className="text-[11px] leading-5 text-amber-100/85">
            Unlock this post to reveal the media.
          </div>
          {typeof message.paidMessageStarCount === 'number' && message.paidMessageStarCount > 0 ? (
            <div className="inline-flex items-center gap-1 rounded-full border border-amber-200/45 bg-black/20 px-2 py-0.5 text-[11px]">
              <Star className="h-3 w-3" />
              {message.paidMessageStarCount}
            </div>
          ) : null}
        </div>
      );
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
          const reactionLabel = renderReactionLabel(reaction.emoji);
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
              <span className="mr-1">{reactionLabel}</span>
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
        <aside className={`shrink-0 border-r border-white/10 bg-[#152434]/95 transition-all ${isSidebarPanelOpen
          ? ((activeTab === 'debugger' || activeTab === 'settings')
            ? 'w-[min(97vw,520px)] sm:w-[420px] lg:w-[460px]'
            : 'w-[min(92vw,340px)] sm:w-[340px]')
          : 'w-[70px]'} flex`}>
          <div className="flex w-[70px] flex-col items-center border-r border-white/10 bg-[#0f1a26] py-3">
            <button
              type="button"
              onClick={() => setIsSidebarPanelOpen((prev) => !prev)}
              className="mb-3 rounded-lg border border-white/15 bg-black/20 p-2 text-white hover:bg-white/10"
              title={isSidebarPanelOpen ? 'Collapse sidebar' : 'Expand sidebar'}
            >
              {isSidebarPanelOpen ? <PanelLeftClose className="h-4 w-4" /> : <PanelLeftOpen className="h-4 w-4" />}
            </button>

            <div className="flex flex-col gap-2">
              <button
                type="button"
                onClick={() => onSelectSidebarTab('chats')}
                className={`rounded-lg p-2 ${activeTab === 'chats' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary hover:bg-white/10 hover:text-white'}`}
                title="Chats"
              >
                <MessageCircle className="h-5 w-5" />
              </button>
              <button
                type="button"
                onClick={() => onSelectSidebarTab('users')}
                className={`rounded-lg p-2 ${activeTab === 'users' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary hover:bg-white/10 hover:text-white'}`}
                title="Users"
              >
                <UserPlus className="h-5 w-5" />
              </button>
              <button
                type="button"
                onClick={() => onSelectSidebarTab('bots')}
                className={`rounded-lg p-2 ${activeTab === 'bots' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary hover:bg-white/10 hover:text-white'}`}
                title="Bots"
              >
                <Bot className="h-5 w-5" />
              </button>
              <button
                type="button"
                onClick={() => onSelectSidebarTab('debugger')}
                className={`rounded-lg p-2 ${activeTab === 'debugger' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary hover:bg-white/10 hover:text-white'}`}
                title="Debugger"
              >
                <Bug className="h-5 w-5" />
              </button>
              <button
                type="button"
                onClick={() => onSelectSidebarTab('settings')}
                className={`rounded-lg p-2 ${activeTab === 'settings' ? 'bg-[#2b5278] text-white' : 'text-telegram-textSecondary hover:bg-white/10 hover:text-white'}`}
                title="Settings"
              >
                <Settings2 className="h-5 w-5" />
              </button>
            </div>
          </div>

          {isSidebarPanelOpen ? (
            <div className="min-w-0 flex-1 overflow-y-auto">
              <div className="border-b border-white/10 px-4 py-3">
                <div className="mb-1 flex items-center justify-between">
                  <h1 className="text-sm font-semibold tracking-wide text-white">
                    {activeTab === 'chats'
                      ? 'Chats'
                      : activeTab === 'users'
                        ? 'Users'
                      : activeTab === 'bots'
                        ? 'Bots'
                        : activeTab === 'debugger'
                          ? 'Debugger'
                          : 'Settings'}
                  </h1>
                  <ShieldCheck className="h-4 w-4 text-[#66c1ff]" />
                </div>
                <p className="truncate text-[11px] text-telegram-textSecondary">Bot: {selectedBot?.first_name || 'Loading'} · @{selectedBot?.username || 'unknown'}</p>
                <p className="truncate text-[10px] text-[#9ec3dc]">Token: {selectedBotToken}</p>
              </div>

              <div className="space-y-3 p-3">
                {copiedToken ? <p className="text-[11px] text-[#9bd1f5]">Token copied.</p> : null}

                {activeTab === 'chats' ? (
                  <>
                    <div className="flex items-center gap-2 rounded-xl bg-white/5 px-3 py-2 text-sm text-telegram-textSecondary">
                      <Search className="h-4 w-4" />
                      <input
                        value={chatSearch}
                        onChange={(e) => setChatSearch(e.target.value)}
                        className="w-full bg-transparent text-sm text-white outline-none placeholder:text-telegram-textSecondary"
                        placeholder="Search chats"
                      />
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 p-2">
                      <button
                        type="button"
                        onClick={() => onToggleSidebarSection('privateChats')}
                        className="flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-xs font-medium text-white hover:bg-white/10"
                      >
                        <span>Private</span>
                        <ChevronDown className={`h-4 w-4 transition-transform ${sidebarSections.privateChats ? 'rotate-0' : '-rotate-90'}`} />
                      </button>
                      {sidebarSections.privateChats ? (
                        <div className="mt-2 space-y-1.5">
                          {filteredUsers.map((user) => {
                            const isActive = chatScopeTab === 'private' && user.id === selectedUserId;
                            const userChatKey = `${selectedBotToken}:${user.id}`;
                            const started = Boolean(startedChats[userChatKey]);
                            return (
                              <button
                                key={user.id}
                                type="button"
                                onClick={() => {
                                  setChatScopeTab('private');
                                  setSelectedUserId(user.id);
                                }}
                                className={`w-full rounded-lg border px-2.5 py-2 text-left transition ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20 hover:bg-black/30'}`}
                              >
                                <div className="flex items-center justify-between gap-2">
                                  <p className="truncate text-sm font-medium text-white">
                                    {user.first_name}
                                    {user.is_verified ? (
                                      <span className="ml-1 inline-flex align-middle text-sky-200"><BadgeCheck className="h-3.5 w-3.5" /></span>
                                    ) : null}
                                  </p>
                                  <span className="text-[10px] text-[#b5cfdf]">{started ? 'Started' : 'Tap to chat'}</span>
                                </div>
                                <p className="truncate text-[11px] text-telegram-textSecondary">@{user.username || `user_${user.id}`}</p>
                              </button>
                            );
                          })}
                        </div>
                      ) : null}
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 p-2">
                      <button
                        type="button"
                        onClick={() => onToggleSidebarSection('groupChats')}
                        className="flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-xs font-medium text-white hover:bg-white/10"
                      >
                        <span>Groups</span>
                        <ChevronDown className={`h-4 w-4 transition-transform ${sidebarSections.groupChats ? 'rotate-0' : '-rotate-90'}`} />
                      </button>
                      {sidebarSections.groupChats ? (
                        <div className="mt-2 space-y-1.5">
                          {filteredGroupChatsBySearch.map((group) => {
                            const isActive = chatScopeTab === 'group' && group.id === selectedGroupChatId;
                            const memberState = groupMembershipByUser[`${selectedBotToken}:${group.id}:${selectedUser.id}`] || 'unknown';
                            return (
                              <button
                                key={group.id}
                                type="button"
                                onClick={() => {
                                  setChatScopeTab('group');
                                  setSelectedGroupChatId(group.id);
                                }}
                                className={`w-full rounded-lg border px-2.5 py-2 text-left transition ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20 hover:bg-black/30'}`}
                              >
                                <div className="flex items-center justify-between gap-2">
                                  <p className="truncate text-sm font-medium text-white">
                                    {group.title}
                                    {group.isVerified ? (
                                      <span className="ml-1 inline-flex align-middle text-sky-200"><BadgeCheck className="h-3.5 w-3.5" /></span>
                                    ) : null}
                                  </p>
                                  <span className="text-[10px] text-[#b5cfdf]">{group.type}</span>
                                </div>
                                <p className="truncate text-[11px] text-telegram-textSecondary">{group.username ? `@${group.username}` : `id ${group.id}`} · {memberState}</p>
                              </button>
                            );
                          })}
                        </div>
                      ) : null}
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 p-2">
                      <button
                        type="button"
                        onClick={() => onToggleSidebarSection('channelChats')}
                        className="flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-xs font-medium text-white hover:bg-white/10"
                      >
                        <span>Channels</span>
                        <ChevronDown className={`h-4 w-4 transition-transform ${sidebarSections.channelChats ? 'rotate-0' : '-rotate-90'}`} />
                      </button>
                      {sidebarSections.channelChats ? (
                        <div className="mt-2 space-y-1.5">
                          {filteredChannelChatsBySearch.map((group) => {
                            const isActive = chatScopeTab === 'channel' && group.id === selectedGroupChatId;
                            const memberState = groupMembershipByUser[`${selectedBotToken}:${group.id}:${selectedUser.id}`] || 'unknown';
                            return (
                              <button
                                key={group.id}
                                type="button"
                                onClick={() => {
                                  setChatScopeTab('channel');
                                  setSelectedGroupChatId(group.id);
                                }}
                                className={`w-full rounded-lg border px-2.5 py-2 text-left transition ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20 hover:bg-black/30'}`}
                              >
                                <div className="flex items-center justify-between gap-2">
                                  <p className="truncate text-sm font-medium text-white">
                                    {group.title}
                                    {group.isVerified ? (
                                      <span className="ml-1 inline-flex align-middle text-sky-200"><BadgeCheck className="h-3.5 w-3.5" /></span>
                                    ) : null}
                                  </p>
                                  <span className="text-[10px] text-[#b5cfdf]">channel</span>
                                </div>
                                <p className="truncate text-[11px] text-telegram-textSecondary">{group.username ? `@${group.username}` : `id ${group.id}`} · {memberState}</p>
                              </button>
                            );
                          })}
                        </div>
                      ) : null}
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 p-3 text-xs">
                      <div className="mb-2 grid grid-cols-2 gap-2">
                        <button
                          type="button"
                          onClick={() => {
                            setChatScopeTab('group');
                            setGroupDraft((prev) => ({ ...prev, type: prev.type === 'channel' ? 'supergroup' : prev.type }));
                          }}
                          className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'group' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-telegram-textSecondary'}`}
                        >
                          Group
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setChatScopeTab('channel');
                            setGroupDraft((prev) => ({ ...prev, type: 'channel', isForum: false }));
                          }}
                          className={`rounded-md px-2 py-1.5 ${chatScopeTab === 'channel' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-telegram-textSecondary'}`}
                        >
                          Channel
                        </button>
                      </div>

                      {(chatScopeTab === 'group' || chatScopeTab === 'channel') ? (
                        <>
                          <button
                            type="button"
                            onClick={() => setShowCreateGroupForm((prev) => !prev)}
                            className="mb-2 w-full rounded-lg border border-white/15 bg-black/20 px-2.5 py-2 text-left text-xs text-white hover:bg-black/30"
                          >
                            {showCreateGroupForm
                              ? `Close ${chatScopeTab === 'channel' ? 'channel' : 'group'} creator`
                              : `Create new ${chatScopeTab === 'channel' ? 'channel' : 'group'}`}
                          </button>

                          {showCreateGroupForm ? (
                            <div className="space-y-2">
                              <input
                                value={groupDraft.title}
                                onChange={(e) => setGroupDraft((prev) => ({ ...prev, title: e.target.value }))}
                                className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                                placeholder={chatScopeTab === 'channel' ? 'Channel title' : 'Group title'}
                              />
                              <input
                                value={groupDraft.username}
                                onChange={(e) => setGroupDraft((prev) => ({ ...prev, username: e.target.value }))}
                                className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                                placeholder="public username"
                              />
                              <input
                                value={groupDraft.description}
                                onChange={(e) => setGroupDraft((prev) => ({ ...prev, description: e.target.value }))}
                                className="w-full rounded-lg border border-white/15 bg-[#0f1a26] px-2 py-1.5 text-white outline-none"
                                placeholder="description"
                              />
                              <button
                                type="button"
                                onClick={() => void onCreateGroup()}
                                disabled={isCreatingGroup || !groupDraft.title.trim()}
                                className="w-full rounded-lg bg-[#2b5278] px-3 py-2 text-white disabled:opacity-50"
                              >
                                {isCreatingGroup ? 'Creating...' : (chatScopeTab === 'channel' ? 'Create Channel' : 'Create Group')}
                              </button>
                            </div>
                          ) : null}

                          <div className="mt-2 flex items-center gap-2">
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
                        </>
                      ) : null}
                    </div>
                  </>
                ) : null}

                {activeTab === 'users' ? (
                  <>
                    <div className="flex items-center gap-2 rounded-xl border border-white/10 bg-black/20 p-2">
                      <div className="flex min-w-0 flex-1 items-center gap-2 rounded-lg bg-[#0f1c28] px-2 py-1.5 text-xs text-telegram-textSecondary">
                        <Search className="h-3.5 w-3.5" />
                        <input
                          value={usersTabSearch}
                          onChange={(event) => setUsersTabSearch(event.target.value)}
                          className="w-full bg-transparent text-xs text-white outline-none placeholder:text-telegram-textSecondary"
                          placeholder="Search users"
                        />
                      </div>
                      <button
                        type="button"
                        onClick={openCreateUserModal}
                        className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2.5 py-1.5 text-xs text-white hover:bg-[#245a80]"
                      >
                        Create
                      </button>
                    </div>

                    <div className="space-y-2">
                      {filteredUsersForManagement.map((user) => {
                        const isActive = user.id === selectedUserId;
                        return (
                          <div
                            key={user.id}
                            className={`rounded-xl border px-3 py-2 ${isActive ? 'border-[#5ca9df] bg-[#2b5278]/60' : 'border-white/10 bg-black/20'}`}
                          >
                            <div className="flex items-start justify-between gap-2">
                              <button
                                type="button"
                                onClick={() => {
                                  setChatScopeTab('private');
                                  setSelectedUserId(user.id);
                                }}
                                className="min-w-0 flex-1 text-left"
                              >
                                <p className="truncate font-medium text-white">
                                  {formatSimUserDisplayName(user)}
                                  {user.is_verified ? (
                                    <span className="ml-1 inline-flex align-middle text-sky-200"><BadgeCheck className="h-3.5 w-3.5" /></span>
                                  ) : null}
                                </p>
                                <p className="truncate text-xs text-telegram-textSecondary">@{user.username || `user_${user.id}`}</p>
                                <p className="mt-1 text-[11px] text-[#aac4d7]">id: {user.id}{user.is_premium ? ' · Premium' : ''}</p>
                              </button>
                              <button
                                type="button"
                                onClick={() => openEditUserModal(user)}
                                className="rounded-full p-1 text-telegram-textSecondary hover:bg-white/10 hover:text-white"
                                title="Edit user"
                              >
                                <Pencil className="h-4 w-4" />
                              </button>
                            </div>
                          </div>
                        );
                      })}
                      {filteredUsersForManagement.length === 0 ? (
                        <p className="rounded-xl border border-white/10 bg-black/20 px-3 py-3 text-xs text-telegram-textSecondary">
                          No users found.
                        </p>
                      ) : null}
                    </div>
                  </>
                ) : null}

                {activeTab === 'bots' ? (
                  <>
                    <div className="flex items-center gap-2 rounded-xl border border-white/10 bg-black/20 p-2">
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
                        onClick={() => onCreateBot()}
                        className="rounded-full bg-[#2f6ea1] p-2 text-white hover:bg-[#3b82bf]"
                        title="Create bot"
                      >
                        <Plus className="h-4 w-4" />
                      </button>
                    </div>

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
                  </>
                ) : null}

                {activeTab === 'debugger' ? (
                  <div className="space-y-3">
                    <div className="rounded-xl border border-white/10 bg-black/20 px-3 py-2">
                      <div className="flex items-center justify-between gap-2">
                        <p className="text-xs text-[#cfe7f8]">
                          Request/Response logs ({filteredDebugEventLogs.length}/{debugEventLogs.length})
                        </p>
                        <div className="flex flex-wrap items-center justify-end gap-2">
                          <button
                            type="button"
                            onClick={() => void copyDebugLogPart(
                              'Filtered logs',
                              filteredDebugEventLogs.map((entry) => ({
                                at: new Date(entry.at).toISOString(),
                                source: entry.source,
                                method: entry.method,
                                status: entry.status,
                                request: entry.request,
                                response: entry.response,
                                error: entry.error,
                              })),
                            )}
                            disabled={filteredDebugEventLogs.length === 0}
                            className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10 disabled:opacity-50"
                          >
                            Copy filtered
                          </button>
                          <button
                            type="button"
                            onClick={() => void onClearRuntimeLogs()}
                            className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                          >
                            Clear
                          </button>
                        </div>
                      </div>

                      <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                        <div className="flex min-w-0 flex-1 items-center gap-2 rounded-lg bg-[#0f1c28] px-2 py-1.5 text-xs text-telegram-textSecondary">
                          <Search className="h-3.5 w-3.5" />
                          <input
                            value={debuggerSearch}
                            onChange={(event) => setDebuggerSearch(event.target.value)}
                            className="w-full bg-transparent text-xs text-white outline-none placeholder:text-telegram-textSecondary"
                            placeholder="Search method, request or response"
                          />
                        </div>
                        <select
                          value={debuggerStatusFilter}
                          onChange={(event) => setDebuggerStatusFilter(event.target.value as 'all' | 'ok' | 'error')}
                          className="rounded-lg border border-white/15 bg-[#0f1c28] px-2 py-1.5 text-xs text-white outline-none"
                        >
                          <option value="all">all statuses</option>
                          <option value="ok">ok</option>
                          <option value="error">error</option>
                        </select>
                        <select
                          value={debuggerSourceFilter}
                          onChange={(event) => setDebuggerSourceFilter(event.target.value as 'all' | 'bot' | 'webhook')}
                          className="rounded-lg border border-white/15 bg-[#0f1c28] px-2 py-1.5 text-xs text-white outline-none"
                        >
                          <option value="all">all sources</option>
                          <option value="bot">telegram api</option>
                          <option value="webhook">webhook</option>
                        </select>
                      </div>
                      {webhookDebugCount === 0 ? (
                        <p className="mt-2 rounded-lg border border-amber-300/25 bg-amber-900/10 px-2 py-1.5 text-[11px] text-amber-100">
                          No webhook dispatch log yet. Make sure selected bot has setWebhook configured.
                        </p>
                      ) : null}
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 px-3 py-3">
                      <div className="mb-2 flex items-center justify-between gap-2">
                        <p className="text-xs font-medium text-white">Latest Request (Live)</p>
                        {latestDebugLog ? (
                          <div className="flex items-center gap-1">
                            <span className="rounded-full border border-sky-300/30 bg-sky-900/20 px-2 py-0.5 text-[10px] text-sky-100">
                              {latestDebugLog.source}
                            </span>
                            <span className={`rounded-full border px-2 py-0.5 text-[10px] ${latestDebugLog.status === 'ok' ? 'border-emerald-300/30 bg-emerald-900/20 text-emerald-100' : 'border-red-300/30 bg-red-900/20 text-red-100'}`}>
                              {latestDebugLog.status}
                            </span>
                          </div>
                        ) : null}
                      </div>

                      {latestDebugLog ? (
                        <div className="space-y-2 text-[11px]">
                          <div className="flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() => void copyDebugLogPart('Request payload', latestDebugLog.request)}
                              className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                            >
                              Copy request
                            </button>
                            <button
                              type="button"
                              onClick={() => void copyDebugLogPart(latestDebugLog.status === 'ok' ? 'Response payload' : 'Error payload', latestDebugLog.status === 'ok' ? latestDebugLog.response : (latestDebugLog.error || 'Unknown error'))}
                              className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                            >
                              Copy {latestDebugLog.status === 'ok' ? 'response' : 'error'}
                            </button>
                            <button
                              type="button"
                              onClick={() => void copyWholeDebugLog(latestDebugLog)}
                              className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                            >
                              Copy full log
                            </button>
                          </div>

                          <div className="rounded-lg border border-white/10 bg-[#0f1c28] px-2 py-2 text-[11px] text-[#d8ecfb]">
                            <p className="break-all text-white">
                              <span className="mr-1 rounded border border-white/20 bg-black/20 px-1 py-0.5 text-[10px]">{latestDebugLog.method}</span>
                              {latestDebugLog.path || '/'}
                            </p>
                            {latestDebugLog.query ? (
                              <details className="mt-1 rounded border border-white/10 bg-black/20 px-2 py-1">
                                <summary className="cursor-pointer text-[10px] text-[#9ec3dc]">Query string</summary>
                                <pre className="mt-1 max-h-24 overflow-auto whitespace-pre-wrap break-all text-[#d7ecfb]">{latestDebugLog.query}</pre>
                              </details>
                            ) : null}
                            <p className="mt-1 text-[#9ec3dc]">
                              {new Date(latestDebugLog.at).toLocaleString()}
                              {typeof latestDebugLog.statusCode === 'number' && latestDebugLog.statusCode > 0 ? ` · status ${latestDebugLog.statusCode}` : ''}
                              {typeof latestDebugLog.durationMs === 'number' && latestDebugLog.durationMs > 0 ? ` · ${latestDebugLog.durationMs}ms` : ''}
                            </p>
                            {latestDebugLog.remoteAddr ? (
                              <p className="mt-1 break-all text-[#9ec3dc]">remote: {latestDebugLog.remoteAddr}</p>
                            ) : null}
                          </div>

                          <div className="grid grid-cols-1 gap-2">
                            <div>
                              <p className="mb-1 text-[#9ec3dc]">Request</p>
                              <pre className="max-h-60 overflow-auto whitespace-pre-wrap break-all rounded border border-white/10 bg-black/30 p-2 text-[#d7ecfb]">{formatDebugValue(latestDebugLog.request)}</pre>
                            </div>
                            <div>
                              <p className="mb-1 text-[#9ec3dc]">{latestDebugLog.status === 'ok' ? 'Response' : 'Error'}</p>
                              <pre className={`max-h-60 overflow-auto whitespace-pre-wrap break-all rounded border p-2 ${latestDebugLog.status === 'ok' ? 'border-white/10 bg-black/30 text-[#d7ecfb]' : 'border-red-300/25 bg-red-900/20 text-red-100'}`}>{latestDebugLog.status === 'ok' ? formatDebugValue(latestDebugLog.response) : (latestDebugLog.error || 'Unknown error')}</pre>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <p className="rounded-lg border border-white/10 bg-[#0f1c28] px-3 py-3 text-xs text-telegram-textSecondary">
                          No live requests yet. Any server call will appear here instantly.
                        </p>
                      )}
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20">
                      <button
                        type="button"
                        onClick={() => setIsDebuggerHistoryExpanded((prev) => !prev)}
                        className="flex w-full items-center justify-between px-3 py-2 text-left text-xs font-medium text-white hover:bg-white/5"
                      >
                        <span>History ({historicalDebugLogs.length})</span>
                        <ChevronDown className={`h-4 w-4 transition-transform ${isDebuggerHistoryExpanded ? 'rotate-0' : '-rotate-90'}`} />
                      </button>

                      {isDebuggerHistoryExpanded ? (
                        <div className="max-h-[40vh] space-y-2 overflow-y-auto px-3 pb-3">
                          {historicalDebugLogs.map((entry) => (
                            <details
                              key={entry.id}
                              className={`rounded-lg border px-2.5 py-2 ${entry.status === 'ok' ? 'border-emerald-300/25 bg-emerald-900/10' : 'border-red-300/25 bg-red-900/10'}`}
                            >
                              <summary className="cursor-pointer list-none text-xs text-white">
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="rounded-full border border-sky-300/30 bg-sky-900/20 px-1.5 py-0.5 text-[10px] text-sky-100">{entry.source}</span>
                                  <span className="rounded-full border border-white/15 px-1.5 py-0.5 text-[10px]">{entry.status}</span>
                                  <span className="rounded border border-white/20 bg-black/20 px-1 py-0.5 text-[10px]">{entry.method}</span>
                                  <span className="text-[#9ec3dc]">{new Date(entry.at).toLocaleTimeString()}</span>
                                </div>
                                <p className="mt-1 break-all text-[11px] text-[#d7ecfb]">{entry.path || '/'}{entry.query ? `?${entry.query}` : ''}</p>
                              </summary>

                              <div className="mt-2 space-y-2">
                                <div className="grid grid-cols-1 gap-2">
                                  <div>
                                    <p className="mb-1 text-[10px] text-[#9ec3dc]">Request</p>
                                    <pre className="max-h-36 overflow-auto whitespace-pre-wrap break-all rounded border border-white/10 bg-black/30 p-2 text-[10px] text-[#d7ecfb]">{formatDebugValue(entry.request)}</pre>
                                  </div>
                                  <div>
                                    <p className="mb-1 text-[10px] text-[#9ec3dc]">{entry.status === 'ok' ? 'Response' : 'Error'}</p>
                                    <pre className={`max-h-36 overflow-auto whitespace-pre-wrap break-all rounded border p-2 text-[10px] ${entry.status === 'ok' ? 'border-white/10 bg-black/30 text-[#d7ecfb]' : 'border-red-300/25 bg-red-900/20 text-red-100'}`}>{entry.status === 'ok' ? formatDebugValue(entry.response) : (entry.error || 'Unknown error')}</pre>
                                  </div>
                                </div>
                                <button
                                  type="button"
                                  onClick={() => void copyWholeDebugLog(entry)}
                                  className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                                >
                                  Copy full log
                                </button>
                              </div>
                            </details>
                          ))}
                          {historicalDebugLogs.length === 0 ? (
                            <p className="rounded-lg border border-white/10 bg-[#0f1c28] px-3 py-2 text-xs text-telegram-textSecondary">
                              No historical logs for current filter.
                            </p>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  </div>
                ) : null}

                {activeTab === 'settings' ? (
                  <div className="space-y-3">
                    <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                      <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Server Service Control</p>
                      <div className="space-y-2 text-xs">
                        <p className="rounded-lg border border-white/10 bg-[#0f1c28] px-2 py-2 text-[#d8ecfb]">
                          API Base: <span className="text-white">{API_BASE_URL}</span>
                        </p>

                        <div className="rounded-lg border border-white/10 bg-[#0f1c28] px-2 py-2 text-[11px] text-[#d8ecfb]">
                          <p>
                            Health: {' '}
                            <span className={serverHealth.status === 'online' ? 'text-emerald-300' : serverHealth.status === 'checking' ? 'text-amber-300' : 'text-red-300'}>
                              {serverHealth.status}
                            </span>
                          </p>
                          <p className="mt-1">
                            Service: <span className={runtimeInfo?.service?.active ? 'text-emerald-300' : 'text-red-300'}>{runtimeInfo?.service?.status || 'unknown'}</span>
                          </p>
                          <p className="mt-1 text-[#9ec3dc]">
                            Manager: {runtimeInfo?.service?.mode || 'unknown'}
                            {runtimeInfo?.service?.requested_mode ? ` (requested: ${runtimeInfo.service.requested_mode})` : ''}
                            {' '}· Name: {runtimeInfo?.service?.name || 'not-set'}
                            {' '}· Available: {runtimeInfo?.service?.available ? 'yes' : 'no'}
                          </p>
                          {runtimeInfo?.service?.note ? (
                            <p className="mt-1 break-words text-amber-200">{runtimeInfo.service.note}</p>
                          ) : null}
                          {serverHealth.error ? <p className="mt-1 text-red-200">{serverHealth.error}</p> : null}
                          {serverHealth.checkedAt ? <p className="mt-1 text-[#9ec3dc]">checked at {new Date(serverHealth.checkedAt).toLocaleTimeString()}</p> : null}
                        </div>

                        <div className="flex flex-wrap items-center gap-2">
                          <button
                            type="button"
                            onClick={() => void onCheckServerHealth()}
                            className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                          >
                            <span className="inline-flex items-center gap-1"><RefreshCw className="h-3 w-3" /> Refresh status</span>
                          </button>
                          <button
                            type="button"
                            onClick={() => void onRuntimeServiceAction('start')}
                            disabled={runtimeServiceActionInFlight !== ''}
                            className="rounded-md border border-emerald-300/35 bg-emerald-900/20 px-2 py-1 text-[11px] text-emerald-100 hover:bg-emerald-900/30 disabled:opacity-50"
                          >
                            {runtimeServiceActionInFlight === 'start' ? 'Starting...' : 'Start service'}
                          </button>
                          <button
                            type="button"
                            onClick={() => void onRuntimeServiceAction('stop')}
                            disabled={runtimeServiceActionInFlight !== ''}
                            className="rounded-md border border-red-300/35 bg-red-900/20 px-2 py-1 text-[11px] text-red-100 hover:bg-red-900/30 disabled:opacity-50"
                          >
                            {runtimeServiceActionInFlight === 'stop' ? 'Stopping...' : 'Stop service'}
                          </button>
                          <button
                            type="button"
                            onClick={() => void onRuntimeServiceAction('restart')}
                            disabled={runtimeServiceActionInFlight !== ''}
                            className="rounded-md border border-amber-300/35 bg-amber-900/20 px-2 py-1 text-[11px] text-amber-100 hover:bg-amber-900/30 disabled:opacity-50"
                          >
                            {runtimeServiceActionInFlight === 'restart' ? 'Restarting...' : 'Restart service'}
                          </button>
                        </div>
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 p-3 text-xs text-[#d8ecfb]">
                      <div className="mb-2 flex items-center justify-between gap-2">
                        <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Environment Variables</p>
                        <button
                          type="button"
                          onClick={onAddRuntimeEnvRow}
                          className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                        >
                          Add variable
                        </button>
                      </div>

                      <p className="mb-2 break-all rounded-lg border border-white/10 bg-[#0f1c28] px-2 py-2 text-[11px] text-[#9ec3dc]">
                        file: {runtimeInfo?.env_file_path || '.env'}
                      </p>

                      <div className="max-h-[32vh] space-y-2 overflow-y-auto pr-1">
                        {runtimeEnvRows.map((row) => (
                          <div key={row.id} className="grid grid-cols-1 gap-2 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
                            <input
                              value={row.key}
                              onChange={(event) => onUpdateRuntimeEnvRow(row.id, 'key', event.target.value)}
                              placeholder="KEY"
                              className="rounded-lg border border-white/15 bg-[#0f1c28] px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <input
                              value={row.value}
                              onChange={(event) => onUpdateRuntimeEnvRow(row.id, 'value', event.target.value)}
                              placeholder="value"
                              className="rounded-lg border border-white/15 bg-[#0f1c28] px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => onRemoveRuntimeEnvRow(row.id)}
                              className="w-full rounded-lg border border-red-300/30 bg-red-900/20 px-2 py-1.5 text-xs text-red-100 hover:bg-red-900/30 xl:w-auto"
                            >
                              Remove
                            </button>
                          </div>
                        ))}
                      </div>

                      <div className="mt-3 flex flex-wrap items-center gap-2">
                        <button
                          type="button"
                          onClick={() => void onSaveRuntimeEnv()}
                          disabled={!runtimeEnvDirty || isRuntimeEnvSaving}
                          className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2.5 py-1.5 text-[11px] text-white hover:bg-[#245a80] disabled:opacity-50"
                        >
                          {isRuntimeEnvSaving ? 'Saving...' : 'Save .env'}
                        </button>
                        <span className={`text-[11px] ${runtimeEnvDirty ? 'text-amber-200' : 'text-[#9ec3dc]'}`}>
                          {runtimeEnvDirty ? 'Unsaved changes' : 'Synced'}
                        </span>
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/20 p-3 text-xs text-[#d8ecfb]">
                      <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Runtime Paths</p>
                      <p className="break-all">Database: {runtimeInfo?.database_path || 'simula.db'}</p>
                      <p className="mt-1 break-all">Storage: {runtimeInfo?.storage_path || 'files'}</p>
                      <p className="mt-1 break-all">Logs: {runtimeInfo?.logs_path || 'stdout (env_logger)'}</p>
                      <p className="mt-1 break-all">Workspace: {runtimeInfo?.workspace_dir || '-'}</p>
                      <p className="mt-1 break-all">API bind: {runtimeInfo?.api_host || '127.0.0.1'}:{runtimeInfo?.api_port || '8081'}</p>
                      <p className="mt-1 break-all">Web app port: {runtimeInfo?.web_port || '8888'}</p>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}
        </aside>

        <section className="flex min-w-0 flex-1 flex-col bg-[#0f1e2d]/70">
          <header className="flex flex-wrap items-center justify-between gap-2 border-b border-white/10 bg-[#1a2a3b]/70 px-3 py-3 sm:px-4 lg:px-5">
            <div className="flex min-w-0 items-center gap-3">
              <button
                type="button"
                onClick={() => {
                  if (chatScopeTab === 'private') {
                    setShowUserProfileModal(true);
                  }
                }}
                className={`flex min-w-0 items-center gap-3 rounded-xl px-1 py-1 text-left ${chatScopeTab === 'private' ? 'hover:bg-white/5' : ''}`}
                title={chatScopeTab === 'private' ? 'Open user profile' : 'Current chat info'}
              >
                <div className="flex h-10 w-10 items-center justify-center overflow-hidden rounded-full bg-[#2b5278]">
                  {chatScopeTab === 'private' && selectedUser.photo_url ? (
                    <img src={selectedUser.photo_url} alt={selectedUserDisplayName} className="h-full w-full object-cover" />
                  ) : chatScopeTab === 'private' ? (
                    <span className="text-sm font-semibold text-[#d7edff]">{simUserAvatarInitials(selectedUser)}</span>
                  ) : (
                    <Bot className="h-5 w-5" />
                  )}
                </div>
                <div className="min-w-0">
                  <h2 className="flex min-w-0 items-center gap-1.5 font-semibold">
                    <span className="truncate">
                      {chatScopeTab === 'private'
                        ? selectedUserDisplayName
                        : (selectedGroup?.title || (chatScopeTab === 'channel' ? 'Channel' : 'Group'))}
                    </span>
                    {chatScopeTab === 'private' && selectedUser.is_verified ? (
                      <span className="inline-flex items-center rounded-full border border-sky-300/45 bg-sky-700/25 px-1.5 py-0.5 text-[10px] text-sky-100" title={selectedUser.verification_description || 'Verified user'}>
                        <BadgeCheck className="h-3 w-3" />
                      </span>
                    ) : null}
                    {(chatScopeTab === 'group' || chatScopeTab === 'channel') && selectedGroup?.isVerified ? (
                      <span className="inline-flex items-center rounded-full border border-sky-300/45 bg-sky-700/25 px-1.5 py-0.5 text-[10px] text-sky-100" title={selectedGroup.verificationDescription || 'Verified chat'}>
                        <BadgeCheck className="h-3 w-3" />
                      </span>
                    ) : null}
                    {chatScopeTab === 'private' && selectedUserEmojiStatus ? (
                      <span className="inline-flex items-center gap-1 rounded-full border border-amber-300/45 bg-amber-700/25 px-1.5 py-0.5 text-[10px] text-amber-100">
                        <span className="tg-premium-emoji text-[12px] leading-none" title="Active emoji status">
                          {premiumEmojiGlyph(selectedUserEmojiStatus.customEmojiId)}
                        </span>
                        <Star className="h-3 w-3" />
                      </span>
                    ) : null}
                  </h2>
                  <p className="truncate text-xs text-telegram-textSecondary">
                    {chatScopeTab === 'private'
                      ? `${selectedUserSecondaryLine}${selectedUser.is_premium ? ' · Premium' : ''}${selectedUser.is_verified ? ' · Verified' : ''}${selectedUserEmojiStatus ? ` · emoji: ${selectedUserEmojiStatusRemainingText}` : ''}`
                      : `@${selectedBot?.username || 'unknown'} · ${isDiscussionThreadView
                        ? `Discussion · ${activeDiscussionCommentContext?.commentsCount || 0} comments`
                        : (chatScopeTab === 'channel' ? 'Channel chat' : 'Group chat')}${selectedGroup?.isVerified ? ' · Verified' : ''}${selectedActorChatBoostCount > 0 ? ` · boosts: ${selectedActorChatBoostCount}` : ''}`}
                    {chatScopeTab === 'group' && !isDiscussionThreadView && (selectedGroup?.isForum || selectedGroup?.isDirectMessages) && activeForumTopic
                      ? ` · ${selectedGroup?.isDirectMessages ? 'DM Topic' : 'Topic'}: ${activeForumTopic.name}`
                      : ''}
                  </p>
                </div>
              </button>
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
                    {formatSimUserDisplayName(user)} ({user.id})
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
                  {(chatScopeTab === 'group' || chatScopeTab === 'channel') && !selectedGroup?.isDirectMessages ? (
                    <button
                      type="button"
                      onClick={() => {
                        onOpenChatBoostModal();
                        setChatMenuOpen(false);
                      }}
                      disabled={!selectedUser.is_premium || !selectedGroup || groupMembership !== 'joined'}
                      className="w-full rounded-lg px-3 py-2 text-left text-sm text-amber-200 hover:bg-white/10 disabled:opacity-40"
                    >
                      Boost {chatScopeTab === 'channel' ? 'channel' : 'group'}
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
                {listedStories.length > 0 ? (
                  <div className="mb-1 flex items-center gap-2 overflow-x-auto pb-1">
                    <button
                      type="button"
                      onClick={openStoryComposerForPost}
                      className="shrink-0 rounded-full border border-[#7ec8fb]/55 bg-[#1f5379] px-3 py-1 text-[11px] font-medium text-[#d7efff] hover:bg-[#2b6a98]"
                    >
                      + Story
                    </button>
                    <div className="flex gap-2">
                      {listedStories.map((entry) => {
                        const story = entry.story;
                        const storyKey = storyShelfKeyFor(story);
                        const referenceMessage = storyPreviewMessageByKey.get(storyKey);
                        const ownerLabel = story.chat.title
                          || (story.chat.username ? `@${story.chat.username}` : `chat ${story.chat.id}`);
                        const previewText = referenceMessage?.text?.trim()
                          || entry.preview?.caption
                          || (referenceMessage?.media ? `[${referenceMessage.media.type}]` : 'tap');
                        const active = activeStoryPreviewKey === storyKey;
                        const avatarLabel = ownerLabel.trim() ? ownerLabel.trim().charAt(0).toUpperCase() : 'S';

                        return (
                          <button
                            key={`story-strip-${storyKey}`}
                            type="button"
                            onClick={() => setActiveStoryPreviewKey(storyKey)}
                            className={[
                              'w-[74px] shrink-0 rounded-xl border px-1.5 py-1 text-center transition',
                              active
                                ? 'border-[#9bd8ff]/80 bg-[#255575] text-white'
                                : 'border-white/20 bg-black/25 text-[#d6ebfb] hover:bg-white/10',
                            ].join(' ')}
                          >
                            <div className="mx-auto mb-1 flex h-10 w-10 items-center justify-center rounded-full border border-[#8fd4ff]/60 bg-[#163a55] text-xs font-semibold">
                              {avatarLabel}
                            </div>
                            <div className="truncate text-[10px] font-medium">{ownerLabel}</div>
                            <div className="truncate text-[9px] text-[#9cc8e3]">{previewText}</div>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                ) : (
                  <div className="mb-1 flex justify-end">
                    <button
                      type="button"
                      onClick={openStoryComposerForPost}
                      className="rounded-full border border-[#7ec8fb]/45 bg-[#1f5379]/80 px-3 py-1 text-[11px] text-[#d7efff] hover:bg-[#2b6a98]"
                    >
                      + Story
                    </button>
                  </div>
                )}
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
                        {renderGiftCard(message)}
                        {renderStoryCard(message)}
                        {renderSuggestedPostCard(message)}
                        {renderInvoiceCard(message)}
                        {renderSuccessfulPaymentCard(message)}
                        {renderChecklistCard(message)}
                        {renderPollCard(message)}
                        {message.text ? (
                          <div className="text-sm leading-6 break-words whitespace-pre-wrap [overflow-wrap:anywhere]">{renderEntityText(message.text, message.entities || message.captionEntities)}</div>
                        ) : null}
                        {renderInlineKeyboard(message)}
                        {renderReactionChips(message)}
                        {message.isPaidPost === true && typeof message.paidMessageStarCount === 'number' && message.paidMessageStarCount > 0 ? (() => {
                          const alreadyPurchased = isPaidMediaPurchasedForActor(
                            selectedBotToken,
                            selectedUser.id,
                            message.chatId,
                            message.id,
                          );
                          const isOwnPaidPost = message.fromUserId === selectedUser.id;
                          if (isOwnPaidPost) {
                            return null;
                          }

                          return (
                            <div className="mt-2 flex justify-end">
                              <button
                                type="button"
                                onClick={() => void onPurchasePaidMedia(message)}
                                disabled={alreadyPurchased || purchasingPaidMediaMessageId === message.id}
                                className="rounded-md border border-amber-200/45 bg-amber-900/25 px-2 py-1 text-[11px] text-amber-100 hover:bg-amber-900/35 disabled:opacity-60"
                              >
                                {alreadyPurchased
                                  ? 'Purchased'
                                  : purchasingPaidMediaMessageId === message.id
                                    ? 'Purchasing...'
                                    : `Unlock · ${message.paidMessageStarCount}⭐`}
                              </button>
                            </div>
                          );
                        })() : null}
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
                      {renderStoryCard(lead)}
                      {renderSuggestedPostCard(lead)}
                      {renderInvoiceCard(lead)}
                      {renderSuccessfulPaymentCard(lead)}
                      {renderChecklistCard(lead)}
                      {renderPollCard(lead)}
                      {renderInlineKeyboard(lead)}
                      {renderReactionChips(lead)}
                      {lead.isPaidPost === true && typeof lead.paidMessageStarCount === 'number' && lead.paidMessageStarCount > 0 ? (() => {
                        const alreadyPurchased = isPaidMediaPurchasedForActor(
                          selectedBotToken,
                          selectedUser.id,
                          lead.chatId,
                          lead.id,
                        );
                        const isOwnPaidPost = lead.fromUserId === selectedUser.id;
                        if (isOwnPaidPost) {
                          return null;
                        }

                        return (
                          <div className="mt-2 flex justify-end">
                            <button
                              type="button"
                              onClick={() => void onPurchasePaidMedia(lead)}
                              disabled={alreadyPurchased || purchasingPaidMediaMessageId === lead.id}
                              className="rounded-md border border-amber-200/45 bg-amber-900/25 px-2 py-1 text-[11px] text-amber-100 hover:bg-amber-900/35 disabled:opacity-60"
                            >
                              {alreadyPurchased
                                ? 'Purchased'
                                : purchasingPaidMediaMessageId === lead.id
                                  ? 'Purchasing...'
                                  : `Unlock · ${lead.paidMessageStarCount}⭐`}
                            </button>
                          </div>
                        );
                      })() : null}
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
                      onClick={() => {
                        setSelectedUploads([]);
                        setUploadAsPaidMedia(false);
                      }}
                      className="rounded-md border border-white/15 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                    >
                      Remove
                    </button>
                  </div>
                ) : null}
                {canCreateSuggestedPostInSelectedChat ? (
                  <div className="rounded-xl border border-[#4f7ea6]/50 bg-[#10283d]/90 px-3 py-2 text-xs text-[#d4ebfb]">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <label className="inline-flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={suggestedPostComposer.enabled}
                          onChange={(event) => setSuggestedPostComposer((prev) => ({
                            ...prev,
                            enabled: event.target.checked,
                          }))}
                        />
                        Send as suggested post
                      </label>
                      <span className="text-[11px] text-[#9ecae5]">For channel DM members</span>
                    </div>
                    {suggestedPostComposer.enabled ? (
                      <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                        <label className="flex items-center gap-2 rounded-md border border-white/15 bg-black/20 px-2 py-1.5">
                          <select
                            value={suggestedPostComposer.priceCurrency}
                            onChange={(event) => setSuggestedPostComposer((prev) => ({
                              ...prev,
                              priceCurrency: event.target.value as 'XTR' | 'TON',
                            }))}
                            className="rounded border border-white/20 bg-white/5 px-2 py-1 text-xs text-white outline-none"
                          >
                            <option value="XTR">XTR</option>
                            <option value="TON">TON</option>
                          </select>
                          <input
                            type="number"
                            min={0}
                            step={1}
                            value={suggestedPostComposer.priceAmount}
                            onChange={(event) => setSuggestedPostComposer((prev) => ({
                              ...prev,
                              priceAmount: event.target.value,
                            }))}
                            className="w-full rounded border border-white/20 bg-white/5 px-2 py-1 text-xs text-white outline-none"
                            placeholder="optional price"
                          />
                        </label>
                        <label className="flex items-center gap-2 rounded-md border border-white/15 bg-black/20 px-2 py-1.5">
                          <span>Send at</span>
                          <input
                            type="datetime-local"
                            value={suggestedPostComposer.sendDate}
                            onChange={(event) => setSuggestedPostComposer((prev) => ({
                              ...prev,
                              sendDate: event.target.value,
                            }))}
                            className="w-full rounded border border-white/20 bg-white/5 px-2 py-1 text-xs text-white outline-none"
                          />
                        </label>
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {selectedUploads.length > 0 && chatScopeTab === 'channel' ? (
                  <div className="rounded-xl border border-white/15 bg-black/20 px-3 py-2 text-xs text-telegram-textSecondary">
                    <div className="flex flex-wrap items-center gap-3">
                      <label className="inline-flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={uploadAsPaidMedia}
                          onChange={(event) => setUploadAsPaidMedia(event.target.checked)}
                        />
                        Send as paid media
                      </label>
                      <label className="inline-flex items-center gap-2">
                        <span>Cost</span>
                        <input
                          type="number"
                          min={1}
                          step={1}
                          value={uploadPaidStarCountDraft}
                          onChange={(event) => setUploadPaidStarCountDraft(event.target.value)}
                          disabled={!uploadAsPaidMedia}
                          className="w-20 rounded border border-white/20 bg-white/5 px-2 py-1 text-xs text-white outline-none disabled:opacity-50"
                        />
                        <span>⭐</span>
                      </label>
                    </div>
                    {uploadAsPaidMedia ? (
                      <p className="mt-2 text-[11px] text-[#b7d8ee]">
                        Paid media supports photo/video uploads only and sends them as a locked paid post.
                      </p>
                    ) : null}
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
                {(isBotCommandsLoading || selectedBotDefaultCommands.length > 0) ? (
                  <div className="rounded-xl border border-[#355a76]/55 bg-[#0f2334]/85 px-3 py-2">
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <p className="text-[11px] text-[#a9d9ff]">Default bot commands</p>
                      <span className="text-[10px] text-[#8db7d3]">
                        {isBotCommandsLoading ? 'loading...' : `${selectedBotDefaultCommands.length} command(s)`}
                      </span>
                    </div>
                    {selectedBotDefaultCommands.length > 0 ? (
                      <div className="flex flex-wrap gap-1.5">
                        {selectedBotDefaultCommands.slice(0, 24).map((command) => (
                          <button
                            key={`default-command-${command.command}`}
                            type="button"
                            onClick={() => insertBotCommandIntoComposer(command.command)}
                            title={command.description}
                            className="rounded-md border border-[#4f7ea6]/55 bg-[#173a55] px-2 py-1 text-[11px] text-[#d7edff] hover:bg-[#214c6f]"
                          >
                            /{command.command}
                          </button>
                        ))}
                      </div>
                    ) : (
                      <p className="text-[11px] text-telegram-textSecondary">No default commands configured for this bot yet.</p>
                    )}
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
                      if (files.length === 0) {
                        setUploadAsPaidMedia(false);
                      }
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
                          setMediaDrawerTab('gifts');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'gifts' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><Gift className="h-3.5 w-3.5" />Gifts</span>
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
                      <button
                        type="button"
                        onClick={() => {
                          setMediaDrawerTab('checklist');
                        }}
                        className={`rounded-lg px-2 py-1 text-[11px] ${mediaDrawerTab === 'checklist' ? 'bg-[#2b5278] text-white' : 'bg-black/20 text-[#d8ecfb]'}`}
                      >
                        <span className="inline-flex items-center gap-1"><ShieldCheck className="h-3.5 w-3.5" />Checklist</span>
                      </button>
                    </div>

                    <div className="max-h-[44vh] overflow-y-auto pr-1">
                      {mediaDrawerTab === 'stickers' ? (
                        <div className="space-y-2">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <p className="text-[11px] text-[#9fc6df]">Sticker sets are auto-discovered from conversation and kept updated.</p>
                            <button
                              type="button"
                              onClick={() => setShowStickerStudioPanel((prev) => !prev)}
                              className="inline-flex items-center gap-1 rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2 py-1 text-[11px] text-white hover:bg-[#245a80]"
                            >
                              <Wrench className="h-3.5 w-3.5" />
                              {showStickerStudioPanel ? 'Hide Studio' : 'Open Studio'}
                            </button>
                          </div>
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

                      {mediaDrawerTab === 'gifts' ? (
                        <div className="space-y-3 text-xs text-[#d7ecfb]">
                          <div className="grid grid-cols-1 gap-2">
                            <div className="rounded-xl border border-white/15 bg-black/20 px-3 py-2">
                              <p className="text-[11px] text-[#9fc6df]">Gift target</p>
                              <p className="mt-1 text-sm text-white">
                                {chatScopeTab === 'private'
                                  ? selectedUserDisplayName
                                  : (selectedGroup?.title || 'No chat selected')}
                              </p>
                              <p className="mt-1 text-[11px] text-[#c8e4f6]">
                                Scope: {chatScopeTab === 'private' ? 'User gifts' : 'Chat gifts'}
                              </p>
                            </div>
                          </div>

                          <div className="rounded-xl border border-white/15 bg-black/20 px-3 py-2">
                            <div className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
                              <select
                                value={selectedGiftId}
                                onChange={(event) => setSelectedGiftId(event.target.value)}
                                disabled={isGiftCatalogLoading || giftCatalog.length === 0 || isGiftActionLoading}
                                className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                              >
                                {giftCatalog.map((gift) => (
                                  <option key={`gift-option-${gift.id}`} value={gift.id}>
                                    {extractGiftEmoji(gift)} {gift.id} · {gift.star_count}⭐
                                  </option>
                                ))}
                              </select>
                              <input
                                type="text"
                                value={giftMessageDraft}
                                onChange={(event) => setGiftMessageDraft(event.target.value)}
                                placeholder="Gift message (optional)"
                                className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                              />
                            </div>

                            <div className="mt-2 flex flex-wrap items-center justify-between gap-2">
                              <label className="inline-flex items-center gap-2 text-[11px] text-[#c8e4f6]">
                                <input
                                  type="checkbox"
                                  checked={giftPayForUpgrade}
                                  onChange={(event) => setGiftPayForUpgrade(event.target.checked)}
                                  disabled={!selectedGift?.upgrade_star_count || selectedGift.upgrade_star_count <= 0}
                                  className="h-3.5 w-3.5"
                                />
                                Prepay upgrade
                              </label>

                              <div className="flex flex-wrap items-center gap-2">
                                <button
                                  type="button"
                                  onClick={() => void sendSelectedGiftFromDrawer()}
                                  disabled={!selectedGift || !canSendGiftInCurrentScope || isGiftActionLoading}
                                  className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-3 py-1.5 text-[11px] text-white hover:bg-[#245a80] disabled:opacity-60"
                                >
                                  Send gift ({selectedGiftChargeEstimate}⭐)
                                </button>
                                {chatScopeTab === 'private' ? (
                                  <button
                                    type="button"
                                    onClick={() => void sendPremiumGiftFromDrawer()}
                                    disabled={isGiftActionLoading}
                                    className="rounded-md border border-[#8462d1]/70 bg-[#3b2a67] px-3 py-1.5 text-[11px] text-white hover:bg-[#4a357f] disabled:opacity-60"
                                  >
                                    Gift Premium (1m · {PREMIUM_SUBSCRIPTION_STAR_COST}⭐)
                                  </button>
                                ) : null}
                              </div>
                            </div>

                            {!canSendGiftInCurrentScope ? (
                              <p className="mt-2 text-[11px] text-amber-200">
                                Switch to a private chat or a channel scope to send gifts.
                              </p>
                            ) : null}
                            {giftPanelError ? <p className="mt-2 text-[11px] text-amber-200">{giftPanelError}</p> : null}
                          </div>

                          <div className="rounded-xl border border-white/15 bg-black/20 px-3 py-2">
                            <div className="mb-2 flex items-center justify-between">
                              <p className="text-[11px] text-[#9fc6df]">Available gifts</p>
                              {isGiftCatalogLoading ? <span className="text-[11px] text-[#c8e4f6]">Loading...</span> : null}
                            </div>
                            <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
                              {giftCatalog.map((gift) => (
                                <button
                                  key={`gift-card-${gift.id}`}
                                  type="button"
                                  onClick={() => setSelectedGiftId(gift.id)}
                                  className={`rounded-lg border px-2 py-2 text-left ${selectedGift?.id === gift.id ? 'border-[#87cfff]/70 bg-[#2b5278]/70 text-white' : 'border-white/20 bg-black/20 text-[#d9efff]'}`}
                                >
                                  <p className="text-lg leading-none">{extractGiftEmoji(gift)}</p>
                                  <p className="mt-1 truncate text-[11px]">{gift.id}</p>
                                  <p className="mt-1 text-[10px] text-[#b8d8ee]">{gift.star_count}⭐</p>
                                </button>
                              ))}
                            </div>
                            {!isGiftCatalogLoading && giftCatalog.length === 0 ? (
                              <p className="mt-2 text-[11px] text-[#a6cbe4]">No gifts available for this bot.</p>
                            ) : null}
                          </div>

                          <div className="rounded-xl border border-white/15 bg-black/20 px-3 py-2">
                            <div className="mb-2 flex items-center justify-between">
                              <p className="text-[11px] text-[#9fc6df]">Owned gifts</p>
                              {isOwnedGiftsLoading ? <span className="text-[11px] text-[#c8e4f6]">Syncing...</span> : null}
                            </div>
                            {isOwnedGiftsLoading ? null : (
                              ownedRegularGifts.length > 0 ? (
                                <div className="space-y-1.5">
                                  {ownedRegularGifts.map((gift) => {
                                    const ownedGiftId = gift.owned_gift_id;
                                    return (
                                      <div
                                        key={`owned-gift-${ownedGiftId || `${gift.gift.id}-${gift.send_date}`}`}
                                        className="rounded-md border border-white/10 bg-black/25 px-2 py-1.5"
                                      >
                                        <div className="flex items-start justify-between gap-2">
                                          <div className="min-w-0 flex-1">
                                            <p className="text-[11px] text-white">
                                              {extractGiftEmoji(gift.gift)} {gift.gift.id} · {gift.gift.star_count}⭐
                                            </p>
                                            <p className="mt-0.5 text-[10px] text-[#b8d8ee]">
                                              {gift.sender_user?.first_name ? `From ${gift.sender_user.first_name}` : 'From unknown'} · {formatGiftSendDate(gift.send_date)}
                                            </p>
                                            {gift.text ? <p className="mt-0.5 text-[10px] text-[#d7ecfb]">“{gift.text}”</p> : null}
                                          </div>
                                          {ownedGiftId ? (
                                            <button
                                              type="button"
                                              onClick={() => void deleteOwnedGiftFromDrawer(ownedGiftId)}
                                              disabled={isGiftActionLoading}
                                              className="mt-0.5 shrink-0 self-start rounded border border-red-300/35 bg-red-900/25 px-2 py-1 text-[10px] text-red-100 hover:bg-red-900/35 disabled:opacity-60"
                                            >
                                              {deletingOwnedGiftId === ownedGiftId ? 'Deleting...' : 'Delete gift'}
                                            </button>
                                          ) : null}
                                        </div>
                                      </div>
                                    );
                                  })}
                                </div>
                              ) : (
                                <p className="text-[11px] text-[#a6cbe4]">No owned gifts found for the current target.</p>
                              )
                            )}
                          </div>

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

                      {mediaDrawerTab === 'checklist' ? (
                        <div className="space-y-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 px-3 py-2 text-xs text-[#d7ecfb]">
                          <p className="text-[11px] text-[#9fc6df]">
                            Uses sendChecklist/editMessageChecklist with generated checklist.tasks payload.
                          </p>
                          <p className="text-[11px] text-[#9fc6df]">
                            business_connection: {activeBusinessConnectionId || 'none'}
                          </p>
                          <input
                            value={checklistBuilder.title}
                            onChange={(event) => setChecklistBuilder((prev) => ({ ...prev, title: event.target.value }))}
                            placeholder="Checklist title"
                            className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                          />
                          <div className="space-y-1">
                            {checklistBuilder.tasks.map((task, index) => (
                              <div key={`checklist-task-${task.id}-${index}`} className="grid grid-cols-[64px_minmax(0,1fr)_auto] items-center gap-2">
                                <input
                                  value={task.id}
                                  onChange={(event) => setChecklistBuilder((prev) => {
                                    const next = [...prev.tasks];
                                    next[index] = { ...next[index], id: event.target.value };
                                    return { ...prev, tasks: next };
                                  })}
                                  placeholder="id"
                                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                                />
                                <input
                                  value={task.text}
                                  onChange={(event) => setChecklistBuilder((prev) => {
                                    const next = [...prev.tasks];
                                    next[index] = { ...next[index], text: event.target.value };
                                    return { ...prev, tasks: next };
                                  })}
                                  placeholder={`Task ${index + 1}`}
                                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1 text-xs text-white outline-none"
                                />
                                <button
                                  type="button"
                                  onClick={() => setChecklistBuilder((prev) => ({
                                    ...prev,
                                    tasks: prev.tasks.length > 1 ? prev.tasks.filter((_, taskIndex) => taskIndex !== index) : prev.tasks,
                                  }))}
                                  disabled={checklistBuilder.tasks.length <= 1}
                                  className="rounded-md border border-red-300/35 bg-red-700/25 px-2 py-1 text-[11px] text-red-100 disabled:opacity-50"
                                >
                                  Remove
                                </button>
                              </div>
                            ))}
                          </div>
                          <div className="flex flex-wrap items-center gap-2">
                            <button
                              type="button"
                              onClick={() => setChecklistBuilder((prev) => ({
                                ...prev,
                                tasks: [...prev.tasks, { id: String(prev.tasks.length + 1), text: '' }],
                              }))}
                              className="rounded-md border border-[#355a76]/60 bg-[#163041]/70 px-2 py-1 text-[11px] text-white hover:bg-[#1f3f56]"
                            >
                              Add task
                            </button>
                            <label className="inline-flex items-center gap-1 text-[11px] text-white">
                              <input
                                type="checkbox"
                                checked={checklistBuilder.othersCanAddTasks}
                                onChange={(event) => setChecklistBuilder((prev) => ({ ...prev, othersCanAddTasks: event.target.checked }))}
                              />
                              others_can_add_tasks
                            </label>
                            <label className="inline-flex items-center gap-1 text-[11px] text-white">
                              <input
                                type="checkbox"
                                checked={checklistBuilder.othersCanMarkTasksAsDone}
                                onChange={(event) => setChecklistBuilder((prev) => ({ ...prev, othersCanMarkTasksAsDone: event.target.checked }))}
                              />
                              others_can_mark_tasks_as_done
                            </label>
                          </div>
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_auto_auto]">
                            <input
                              value={lastChecklistMessageIdDraft}
                              onChange={(event) => setLastChecklistMessageIdDraft(event.target.value)}
                              placeholder="Checklist message_id for edit"
                              className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                            />
                            <button
                              type="button"
                              onClick={() => void submitChecklistBuilder('send')}
                              disabled={!hasStarted || isSending}
                              className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:opacity-60"
                            >
                              sendChecklist
                            </button>
                            <button
                              type="button"
                              onClick={() => void submitChecklistBuilder('edit')}
                              disabled={!hasStarted || isSending}
                              className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:opacity-60"
                            >
                              editChecklist
                            </button>
                          </div>
                        </div>
                      ) : null}

                      {mediaDrawerTab === 'stickers' && showStickerStudioPanel ? (
                        <div className="space-y-2 text-[11px] text-[#d7ecfb]">
                          <p className="text-[11px] uppercase tracking-wide text-[#9fc6df]">Sticker Studio</p>
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
            className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl"
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

            {isBotModalLoading ? (
              <p className="mb-3 rounded-lg border border-[#4f7ea6]/40 bg-[#0f2334]/80 px-3 py-2 text-xs text-[#cfe8ff]">
                Loading bot profile and command settings...
              </p>
            ) : null}

            <div className="space-y-3">
              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Identity</p>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
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
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Profile</p>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <textarea
                    value={botDraft.description}
                    onChange={(event) => setBotDraft((prev) => ({ ...prev, description: event.target.value }))}
                    rows={3}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none sm:col-span-2"
                    placeholder="Bot description (setMyDescription)"
                  />
                  <input
                    value={botDraft.short_description}
                    onChange={(event) => setBotDraft((prev) => ({ ...prev, short_description: event.target.value }))}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none sm:col-span-2"
                    placeholder="Short description (setMyShortDescription)"
                  />
                  <input
                    value={botDraft.profile_photo_ref}
                    onChange={(event) => setBotDraft((prev) => ({
                      ...prev,
                      profile_photo_ref: event.target.value,
                      remove_profile_photo: false,
                    }))}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none sm:col-span-2"
                    placeholder="Profile photo URL / local path / file_id"
                  />
                  <label className="sm:col-span-2 inline-flex items-center gap-2 rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs text-[#d7ecfb]">
                    <input
                      type="checkbox"
                      checked={botDraft.remove_profile_photo}
                      onChange={(event) => setBotDraft((prev) => ({
                        ...prev,
                        remove_profile_photo: event.target.checked,
                        profile_photo_ref: event.target.checked ? '' : prev.profile_photo_ref,
                      }))}
                    />
                    Remove profile photo on save
                  </label>
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Default Commands</p>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-[180px_minmax(0,1fr)]">
                  <input
                    value={botDraft.commands_language_code}
                    onChange={(event) => setBotDraft((prev) => ({ ...prev, commands_language_code: event.target.value }))}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                    placeholder="language code (optional)"
                  />
                  <textarea
                    value={botDraft.commands_text}
                    onChange={(event) => setBotDraft((prev) => ({ ...prev, commands_text: event.target.value }))}
                    rows={6}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 font-mono text-xs outline-none"
                    placeholder="/start - Start the bot\n/help - Show help"
                  />
                </div>
                <p className="mt-2 text-[11px] text-[#9ec3dc]">Telegram format: /command - Description. Leave empty to delete default commands.</p>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Managed Bot</p>
                <label className="inline-flex items-center gap-2 rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs text-[#d7ecfb]">
                  <input
                    type="checkbox"
                    checked={botManagedEnabledDraft}
                    onChange={(event) => setBotManagedEnabledDraft(event.target.checked)}
                  />
                  Enable reply keyboard request_managed_bot flow
                </label>

                {botModalMode === 'edit' ? (
                  <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-[160px_minmax(0,1fr)_auto_auto]">
                    <input
                      value={managedBotOwnerDraft}
                      onChange={(event) => setManagedBotOwnerDraft(event.target.value)}
                      className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                      placeholder="owner user_id"
                    />
                    <p className="self-center text-[11px] text-[#9ec3dc]">
                      Generate/rotate managed bot token for this owner
                    </p>
                    <button
                      type="button"
                      onClick={() => void onRunManagedBotTokenAction('get')}
                      disabled={isManagedBotTokenActionRunning}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-xs text-white hover:bg-white/10 disabled:opacity-50"
                    >
                      getManagedBotToken
                    </button>
                    <button
                      type="button"
                      onClick={() => void onRunManagedBotTokenAction('replace')}
                      disabled={isManagedBotTokenActionRunning}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-xs text-white hover:bg-white/10 disabled:opacity-50"
                    >
                      replaceManagedBotToken
                    </button>
                  </div>
                ) : (
                  <p className="mt-2 text-[11px] text-[#9ec3dc]">
                    Save the bot first, then use token actions for managed bots.
                  </p>
                )}
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Default Admin Rights</p>
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                  <div className="rounded-lg border border-white/10 bg-[#0f1c28] p-3">
                    <p className="mb-2 text-xs text-[#b8d8ee]">Groups/Supergroups</p>
                    <div className="grid grid-cols-2 gap-1.5 text-[11px] text-[#d7ecfb]">
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.isAnonymous} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, isAnonymous: event.target.checked } }))} />anonymous</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canManageChat} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canManageChat: event.target.checked } }))} />manage chat</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canDeleteMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canDeleteMessages: event.target.checked } }))} />delete messages</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canManageVideoChats} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canManageVideoChats: event.target.checked } }))} />video chats</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canRestrictMembers} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canRestrictMembers: event.target.checked } }))} />restrict members</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canPromoteMembers} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canPromoteMembers: event.target.checked } }))} />promote members</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canChangeInfo} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canChangeInfo: event.target.checked } }))} />change info</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canInviteUsers} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canInviteUsers: event.target.checked } }))} />invite users</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canPinMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canPinMessages: event.target.checked } }))} />pin messages</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canManageTopics} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canManageTopics: event.target.checked } }))} />manage topics</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canManageDirectMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canManageDirectMessages: event.target.checked } }))} />manage DMs</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canManageTags} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canManageTags: event.target.checked } }))} />manage tags</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canPostStories} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canPostStories: event.target.checked } }))} />post stories</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canEditStories} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canEditStories: event.target.checked } }))} />edit stories</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.group_default_admin_rights.canDeleteStories} onChange={(event) => setBotDraft((prev) => ({ ...prev, group_default_admin_rights: { ...prev.group_default_admin_rights, canDeleteStories: event.target.checked } }))} />delete stories</label>
                    </div>
                  </div>

                  <div className="rounded-lg border border-white/10 bg-[#0f1c28] p-3">
                    <p className="mb-2 text-xs text-[#b8d8ee]">Channels</p>
                    <div className="grid grid-cols-2 gap-1.5 text-[11px] text-[#d7ecfb]">
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.isAnonymous} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, isAnonymous: event.target.checked } }))} />anonymous</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canManageChat} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canManageChat: event.target.checked } }))} />manage chat</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canDeleteMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canDeleteMessages: event.target.checked } }))} />delete messages</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canManageVideoChats} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canManageVideoChats: event.target.checked } }))} />video chats</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canRestrictMembers} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canRestrictMembers: event.target.checked } }))} />restrict members</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canPromoteMembers} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canPromoteMembers: event.target.checked } }))} />promote members</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canChangeInfo} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canChangeInfo: event.target.checked } }))} />change info</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canInviteUsers} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canInviteUsers: event.target.checked } }))} />invite users</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canPostMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canPostMessages: event.target.checked } }))} />post messages</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canEditMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canEditMessages: event.target.checked } }))} />edit messages</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canManageDirectMessages} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canManageDirectMessages: event.target.checked } }))} />manage DMs</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canManageTags} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canManageTags: event.target.checked } }))} />manage tags</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canPostStories} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canPostStories: event.target.checked } }))} />post stories</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canEditStories} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canEditStories: event.target.checked } }))} />edit stories</label>
                      <label className="inline-flex items-center gap-1"><input type="checkbox" checked={botDraft.channel_default_admin_rights.canDeleteStories} onChange={(event) => setBotDraft((prev) => ({ ...prev, channel_default_admin_rights: { ...prev.channel_default_admin_rights, canDeleteStories: event.target.checked } }))} />delete stories</label>
                    </div>
                  </div>
                </div>
              </div>
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
                disabled={isBootstrapping || isBotModalLoading}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892]"
              >
                {botModalMode === 'create' ? 'Create Bot' : 'Save Changes'}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {showUserProfileModal && chatScopeTab === 'private' ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 px-4">
          <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-base font-semibold text-white">Profile</h3>
              <button
                type="button"
                onClick={() => setShowUserProfileModal(false)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-black/20 p-3">
              <div className="flex h-16 w-16 items-center justify-center overflow-hidden rounded-full bg-[#2b5278]">
                {selectedUser.photo_url ? (
                  <img src={selectedUser.photo_url} alt={selectedUserDisplayName} className="h-full w-full object-cover" />
                ) : (
                  <span className="text-lg font-semibold text-[#d7edff]">{simUserAvatarInitials(selectedUser)}</span>
                )}
              </div>
              <div className="min-w-0">
                <div className="flex min-w-0 items-center gap-1.5">
                  <p className="truncate text-base font-semibold text-white">{selectedUserDisplayName}</p>
                  {selectedUserEmojiStatus ? (
                    <span className="inline-flex items-center gap-1 rounded-full border border-amber-300/45 bg-amber-700/25 px-1.5 py-0.5 text-[10px] text-amber-100">
                      <span className="tg-premium-emoji text-[12px] leading-none" title="Active emoji status">
                        {premiumEmojiGlyph(selectedUserEmojiStatus.customEmojiId)}
                      </span>
                      <Star className="h-3 w-3" />
                    </span>
                  ) : null}
                </div>
                <p className="truncate text-xs text-[#a7cae0]">{selectedUserSecondaryLine}</p>
                <p className="mt-1 text-[11px] text-[#8fb7d6]">id: {selectedUser.id}</p>
                {selectedUserEmojiStatus ? (
                  <p className="mt-1 text-[11px] text-[#9ec3dc]">Emoji status expires in {selectedUserEmojiStatusRemainingText}</p>
                ) : null}
              </div>
            </div>

            <div className="mt-3 space-y-2 text-xs text-[#d4e9f7]">
              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">About</p>
                <p className="mt-1 whitespace-pre-wrap break-words">{selectedUser.bio || 'No bio set.'}</p>
              </div>

              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Account</p>
                  <p className="mt-1">Premium: {selectedUser.is_premium ? 'Yes' : 'No'}</p>
                  <p className="mt-1">Gifts: {nonNegativeInteger(selectedUser.gift_count, 0)}</p>
                  <p className="mt-1">Stars wallet: {walletState.stars}</p>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Business</p>
                  <p className="mt-1 truncate">Name: {selectedUser.business_name || '-'}</p>
                  <p className="mt-1 truncate">Location: {selectedUser.business_location || '-'}</p>
                  <p className="mt-1 truncate">Connection: {selectedBusinessConnection?.id || 'none'}</p>
                  <p className="mt-1">Rights: {selectedBusinessRights.length > 0 ? selectedBusinessRights.join(', ') : 'none'}</p>
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Business Intro</p>
                <p className="mt-1 whitespace-pre-wrap break-words">{selectedUser.business_intro || 'No business intro set.'}</p>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Profile Photos</p>
                  <button
                    type="button"
                    onClick={() => void onRefreshUserProfileMedia()}
                    disabled={isUserProfileDataLoading}
                    className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10 disabled:opacity-50"
                  >
                    Refresh
                  </button>
                </div>

                <p className="text-[11px] text-[#9ec3dc]">Total photos: {userProfilePhotos?.total_count ?? 0}</p>
                <div className="mt-1 max-h-28 space-y-1 overflow-y-auto rounded-lg border border-white/10 bg-[#0f1c28] p-2 text-[11px] text-[#d7ecfb]">
                  {(userProfilePhotos?.photos || []).map((group, index) => (
                    <p key={`profile-photo-${index}`} className="truncate">{group[0]?.file_id || '-'}</p>
                  ))}
                  {(userProfilePhotos?.photos || []).length === 0 ? <p className="text-[#9ec3dc]">No profile photos</p> : null}
                </div>

                <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_auto_auto]">
                  <input
                    value={profilePhotoUrlDraft}
                    onChange={(event) => setProfilePhotoUrlDraft(event.target.value)}
                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs outline-none"
                    placeholder="Profile photo URL"
                  />
                  <button
                    type="button"
                    onClick={() => void onSetUserProfilePhotoFromProfile()}
                    disabled={isUserProfileDataLoading}
                    className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2.5 py-1 text-[11px] text-white hover:bg-[#245a80] disabled:opacity-50"
                  >
                    Set photo
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setProfilePhotoUrlDraft('');
                      void onSetUserProfilePhotoFromProfile();
                    }}
                    disabled={isUserProfileDataLoading}
                    className="rounded-md border border-red-300/40 bg-red-700/25 px-2.5 py-1 text-[11px] text-red-100 hover:bg-red-700/35 disabled:opacity-50"
                  >
                    Clear photo
                  </button>
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Profile Music</p>
                  <div className="flex items-center gap-1.5">
                    <button
                      type="button"
                      onClick={() => void onDeleteAllUserProfileAudiosFromProfile()}
                      disabled={isUserProfileDataLoading || (userProfileAudios?.audios.length ?? 0) === 0}
                      className="rounded-md border border-red-300/40 bg-red-700/25 px-2 py-1 text-[11px] text-red-100 hover:bg-red-700/35 disabled:opacity-50"
                    >
                      Clear all
                    </button>
                    <button
                      type="button"
                      onClick={() => void onRefreshUserProfileMedia()}
                      disabled={isUserProfileDataLoading}
                      className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10 disabled:opacity-50"
                    >
                      Refresh
                    </button>
                  </div>
                </div>

                <p className="text-[11px] text-[#9ec3dc]">Total tracks: {userProfileAudios?.total_count ?? 0}</p>
                <div className="mt-1 max-h-32 space-y-1 overflow-y-auto rounded-lg border border-white/10 bg-[#0f1c28] p-2">
                  {(userProfileAudios?.audios || []).map((audio) => (
                    <div key={`profile-audio-row-${audio.file_id}`} className="flex items-center justify-between gap-2 rounded border border-white/10 bg-black/20 px-2 py-1">
                      <div className="min-w-0 text-[11px] text-[#d7ecfb]">
                        <p className="truncate">{audio.title || audio.file_name || audio.file_id}</p>
                        <p className="truncate text-[10px] text-[#9ec3dc]">{audio.performer || '-'} · {audio.duration}s · {audio.file_id}</p>
                      </div>
                      <button
                        type="button"
                        onClick={() => void onDeleteUserProfileAudioFromProfile(audio.file_id)}
                        disabled={isUserProfileDataLoading}
                        className="rounded-md border border-red-300/40 bg-red-700/25 px-2 py-1 text-[10px] text-red-100 hover:bg-red-700/35 disabled:opacity-50"
                      >
                        Delete
                      </button>
                    </div>
                  ))}
                  {(userProfileAudios?.audios || []).length === 0 ? <p className="text-[11px] text-[#9ec3dc]">No profile audios</p> : null}
                </div>

                <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                  <input
                    value={profileAudioTitleDraft}
                    onChange={(event) => setProfileAudioTitleDraft(event.target.value)}
                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs outline-none"
                    placeholder="Profile audio title"
                  />
                  <input
                    value={profileAudioPerformerDraft}
                    onChange={(event) => setProfileAudioPerformerDraft(event.target.value)}
                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs outline-none"
                    placeholder="Performer (optional)"
                  />
                </div>

                <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_auto]">
                  <input
                    type="file"
                    accept="audio/*"
                    onChange={(event) => {
                      const file = event.target.files?.[0] ?? null;
                      setProfileAudioFileDraft(file);
                      if (file && !profileAudioTitleDraft.trim()) {
                        const titleFromFile = file.name.replace(/\.[^/.]+$/, '').trim();
                        if (titleFromFile) {
                          setProfileAudioTitleDraft(titleFromFile);
                        }
                      }
                    }}
                    className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs outline-none file:mr-3 file:rounded-md file:border-0 file:bg-[#1a4868] file:px-2 file:py-1 file:text-xs file:text-white"
                  />
                  <button
                    type="button"
                    onClick={() => void onSetUserProfileAudioFromProfile()}
                    disabled={isUserProfileDataLoading}
                    className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2.5 py-1 text-[11px] text-white hover:bg-[#245a80] disabled:opacity-50"
                  >
                    Add/Upload audio
                  </button>
                </div>
                {profileAudioFileDraft ? (
                  <p className="mt-1 text-[11px] text-[#9ec3dc]">Selected file: {profileAudioFileDraft.name}</p>
                ) : null}
              </div>

              <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[11px] uppercase tracking-wide text-[#8fb7d6]">Emoji Status</p>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <input
                    value={emojiStatusDraft}
                    onChange={(event) => setEmojiStatusDraft(event.target.value)}
                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs outline-none"
                    placeholder="emoji_status_custom_emoji_id"
                  />
                  <input
                    type="datetime-local"
                    value={emojiStatusExpirationDraft}
                    onChange={(event) => setEmojiStatusExpirationDraft(event.target.value)}
                    className="rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-xs outline-none"
                  />
                </div>
                <div className="mt-2 rounded-lg border border-white/10 bg-[#0f1c28] px-3 py-2 text-[11px] text-[#cfe7f8]">
                  {selectedUserEmojiStatus ? (
                    <p>
                      Active: <span className="tg-premium-emoji">{premiumEmojiGlyph(selectedUserEmojiStatus.customEmojiId)}</span>{' '}
                      {selectedUserEmojiStatus.customEmojiId}
                      {' · expires in '}
                      {selectedUserEmojiStatusRemainingText}
                    </p>
                  ) : (
                    <p>No active emoji status.</p>
                  )}
                </div>
                <div className="mt-2 flex flex-wrap items-center justify-end gap-2">
                  <button
                    type="button"
                    onClick={() => void onSetUserEmojiStatusFromProfile(true)}
                    disabled={isUserProfileDataLoading}
                    className="rounded-md border border-red-300/40 bg-red-700/25 px-2.5 py-1 text-[11px] text-red-100 hover:bg-red-700/35 disabled:opacity-50"
                  >
                    Clear status
                  </button>
                  <button
                    type="button"
                    onClick={() => void onSetUserEmojiStatusFromProfile(false)}
                    disabled={isUserProfileDataLoading}
                    className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2.5 py-1 text-[11px] text-white hover:bg-[#245a80] disabled:opacity-50"
                  >
                    setUserEmojiStatus
                  </button>
                </div>
              </div>
            </div>

            <div className="mt-4 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => setShowUserProfileModal(false)}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Close
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowUserProfileModal(false);
                  openEditUserModal(selectedUser);
                }}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892]"
              >
                Edit User
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {showUserModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void commitUserModal();
            }}
            className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl"
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

            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
              <input
                value={userDraft.first_name}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, first_name: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="First name"
              />
              <input
                value={userDraft.last_name}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, last_name: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Last name"
              />
              <input
                value={userDraft.username}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, username: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="username (optional)"
              />
              <input
                value={userDraft.phone_number}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, phone_number: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Phone number"
              />
              <input
                value={userDraft.id}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, id: e.target.value }))}
                disabled={userModalMode === 'edit'}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="user id (optional)"
              />
              <input
                value={userDraft.gift_count}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, gift_count: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Gift count"
              />
              <input
                value={userDraft.photo_url}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, photo_url: e.target.value }))}
                className="sm:col-span-2 w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Photo URL"
              />
              <textarea
                value={userDraft.bio}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, bio: e.target.value }))}
                className="sm:col-span-2 min-h-[72px] w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Bio"
              />
              <input
                value={userDraft.business_name}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, business_name: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Business name"
              />
              <input
                value={userDraft.business_location}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, business_location: e.target.value }))}
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Business location"
              />
              <textarea
                value={userDraft.business_intro}
                onChange={(e) => setUserDraft((prev) => ({ ...prev, business_intro: e.target.value }))}
                className="sm:col-span-2 min-h-[64px] w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
                placeholder="Business intro"
              />
              <label className="sm:col-span-2 inline-flex items-center gap-2 rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm text-[#d7ecfb]">
                <input
                  type="checkbox"
                  checked={userDraft.is_premium}
                  onChange={(e) => setUserDraft((prev) => ({ ...prev, is_premium: e.target.checked }))}
                />
                Premium account
              </label>

              <div className="sm:col-span-2 rounded-xl border border-white/15 bg-[#0f1c28] p-3 text-xs text-[#cfe7f8]">
                <p className="text-[11px] uppercase tracking-wide text-[#8fb7d6]">Business Connection</p>
                <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <input
                    value={businessConnectionDraftId}
                    onChange={(e) => setBusinessConnectionDraftId(e.target.value)}
                    className="w-full rounded-lg border border-white/15 bg-[#0b1722] px-3 py-2 text-xs text-white outline-none"
                    placeholder="business connection id"
                  />
                  <label className="inline-flex items-center gap-2 rounded-lg border border-white/15 bg-[#0b1722] px-3 py-2 text-xs text-[#d7ecfb]">
                    <input
                      type="checkbox"
                      checked={businessConnectionDraftEnabled}
                      onChange={(e) => setBusinessConnectionDraftEnabled(e.target.checked)}
                    />
                    enabled
                  </label>
                </div>
                <div className="mt-2 flex flex-wrap items-center justify-end gap-2">
                  <button
                    type="button"
                    onClick={() => void onRemoveSelectedUserBusinessConnection()}
                    disabled={userModalMode === 'create' || isBusinessActionRunning}
                    className="rounded-md border border-red-300/40 bg-red-700/25 px-2.5 py-1 text-[11px] text-red-100 hover:bg-red-700/35 disabled:opacity-50"
                  >
                    Remove
                  </button>
                  <button
                    type="button"
                    onClick={() => void onSaveSelectedUserBusinessConnection()}
                    disabled={userModalMode === 'create' || isBusinessActionRunning}
                    className="rounded-md border border-[#4e84aa]/60 bg-[#1a4868] px-2.5 py-1 text-[11px] text-white hover:bg-[#245a80] disabled:opacity-50"
                  >
                    {isBusinessActionRunning ? 'Saving...' : 'Save connection'}
                  </button>
                </div>
              </div>
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

      {managedBotRequestModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4">
          <div className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="min-w-0">
                <h3 className="truncate text-sm font-semibold text-white">Request managed bot</h3>
                <p className="truncate text-xs text-[#9ec3dc]">{managedBotRequestModal.buttonText}</p>
              </div>
              <button
                type="button"
                onClick={() => setManagedBotRequestModal(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="space-y-2">
              <input
                value={managedBotRequestModal.requestId}
                onChange={(event) => setManagedBotRequestModal((prev) => (
                  prev
                    ? {
                      ...prev,
                      requestId: event.target.value,
                    }
                    : prev
                ))}
                placeholder="request_id"
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
              />
              <input
                value={managedBotRequestModal.suggestedName}
                onChange={(event) => setManagedBotRequestModal((prev) => (
                  prev
                    ? {
                      ...prev,
                      suggestedName: event.target.value,
                    }
                    : prev
                ))}
                placeholder="suggested_name"
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
              />
              <input
                value={managedBotRequestModal.suggestedUsername}
                onChange={(event) => setManagedBotRequestModal((prev) => (
                  prev
                    ? {
                      ...prev,
                      suggestedUsername: event.target.value,
                    }
                    : prev
                ))}
                placeholder="suggested_username"
                className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
              />
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => setManagedBotRequestModal(null)}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void onSubmitManagedBotRequestModal()}
                disabled={isSending}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
              >
                Request managed bot
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {miniAppModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 px-4">
          <div className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="min-w-0">
                <h3 className="truncate text-base font-semibold text-white">Built-in Mini App</h3>
                <p className="truncate text-xs text-[#9ec3dc]">
                  {miniAppModal.buttonText} · {miniAppModal.source}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setMiniAppModal(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="mb-3 grid grid-cols-1 gap-2 rounded-xl border border-white/10 bg-black/20 p-3 text-[11px] text-[#cfe7f8] sm:grid-cols-2">
              <p className="truncate">web_app_query_id: <span className="text-white">{miniAppModal.queryId}</span></p>
              <p className="truncate">url: <span className="text-white">{miniAppModal.url}</span></p>
            </div>

            <div className="space-y-2 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 px-3 py-2 text-xs text-[#d7ecfb]">
              <input
                value={webAppLab.lastQueryId}
                onChange={(event) => setWebAppLab((prev) => ({ ...prev, lastQueryId: event.target.value }))}
                placeholder="web_app_query_id"
                className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
              />
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <input
                  value={webAppLab.answerTitle}
                  onChange={(event) => setWebAppLab((prev) => ({ ...prev, answerTitle: event.target.value }))}
                  placeholder="answer result title"
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
                <input
                  value={webAppLab.answerUrl}
                  onChange={(event) => setWebAppLab((prev) => ({ ...prev, answerUrl: event.target.value }))}
                  placeholder="result URL (optional)"
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
              </div>
              <textarea
                value={webAppLab.answerMessageText}
                onChange={(event) => setWebAppLab((prev) => ({ ...prev, answerMessageText: event.target.value }))}
                rows={2}
                placeholder="answerWebAppQuery message_text"
                className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
              />
              <input
                value={webAppLab.answerDescription}
                onChange={(event) => setWebAppLab((prev) => ({ ...prev, answerDescription: event.target.value }))}
                placeholder="result description (optional)"
                className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
              />
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                <button
                  type="button"
                  onClick={() => void onAnswerWebAppQueryFromLab()}
                  disabled={isWebAppLabRunning}
                  className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:opacity-60"
                >
                  answerWebAppQuery
                </button>
                <button
                  type="button"
                  onClick={() => void onSavePreparedInlineMessageFromLab()}
                  disabled={isWebAppLabRunning}
                  className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:opacity-60"
                >
                  savePreparedInline
                </button>
                <button
                  type="button"
                  onClick={() => void onSavePreparedKeyboardButtonFromLab()}
                  disabled={isWebAppLabRunning}
                  className="rounded-md border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-1.5 text-xs text-white hover:bg-[#2f6f9f] disabled:opacity-60"
                >
                  savePreparedButton
                </button>
              </div>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <input
                  value={webAppLab.preparedInlineTitle}
                  onChange={(event) => setWebAppLab((prev) => ({ ...prev, preparedInlineTitle: event.target.value }))}
                  placeholder="prepared inline title"
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
                <input
                  value={webAppLab.preparedInlineText}
                  onChange={(event) => setWebAppLab((prev) => ({ ...prev, preparedInlineText: event.target.value }))}
                  placeholder="prepared inline message text"
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
                <input
                  value={webAppLab.preparedButtonText}
                  onChange={(event) => setWebAppLab((prev) => ({ ...prev, preparedButtonText: event.target.value }))}
                  placeholder="prepared keyboard button text"
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
                <input
                  value={webAppLab.preparedButtonUrl}
                  onChange={(event) => setWebAppLab((prev) => ({ ...prev, preparedButtonUrl: event.target.value }))}
                  placeholder="prepared keyboard button web_app URL"
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
              </div>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <p className="rounded-md border border-white/15 bg-black/25 px-2 py-1 text-[11px] text-[#cfe7f8] break-all">
                  prepared inline id: {lastPreparedInlineMessageId || '-'}
                </p>
                <p className="rounded-md border border-white/15 bg-black/25 px-2 py-1 text-[11px] text-[#cfe7f8] break-all">
                  prepared keyboard id: {lastPreparedKeyboardButtonId || '-'}
                </p>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {chatBoostModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 px-4">
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="min-w-0">
                <h3 className="truncate text-sm font-semibold text-white">Boost chat</h3>
                <p className="truncate text-xs text-[#9ec3dc]">{chatBoostModal.chatTitle}</p>
              </div>
              <button
                type="button"
                onClick={() => setChatBoostModal(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <p className="mb-2 text-xs text-[#cfe7f8]">
              Set how many boosts you want to apply as this premium user.
            </p>
            <input
              type="number"
              min={1}
              value={chatBoostModal.countDraft}
              onChange={(event) => setChatBoostModal((prev) => (
                prev
                  ? {
                    ...prev,
                    countDraft: event.target.value,
                  }
                  : prev
              ))}
              className="w-full rounded-lg border border-white/15 bg-[#0f1c28] px-3 py-2 text-sm outline-none"
              placeholder="boost count"
            />

            <div className="mt-3 rounded-lg border border-white/10 bg-black/20 p-2">
              <div className="mb-2 flex items-center justify-between gap-2">
                <p className="text-[11px] text-[#cfe7f8]">Current boosts for this user: {userChatBoosts?.boosts.length ?? 0}</p>
                <button
                  type="button"
                  onClick={() => void onRefreshChatBoostsFromModal()}
                  disabled={isBoostActionRunning}
                  className="rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10 disabled:opacity-50"
                >
                  Refresh
                </button>
              </div>

              <div className="max-h-40 space-y-1 overflow-y-auto">
                {(userChatBoosts?.boosts || []).map((boost) => (
                  <div
                    key={`boost-modal-${boost.boost_id}`}
                    className="flex items-center justify-between gap-2 rounded-md border border-white/10 bg-[#0f1c28] px-2 py-1"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-[11px] text-white">{boost.boost_id}</p>
                      <p className="truncate text-[10px] text-[#9fc7e1]">expires {new Date(boost.expiration_date * 1000).toLocaleString()}</p>
                    </div>
                    <button
                      type="button"
                      onClick={() => void onRemoveChatBoostFromModal(boost.boost_id)}
                      disabled={isBoostActionRunning}
                      className="rounded-md border border-red-300/40 bg-red-700/25 px-2 py-1 text-[10px] text-red-100 hover:bg-red-700/35 disabled:opacity-50"
                    >
                      Delete
                    </button>
                  </div>
                ))}
                {(userChatBoosts?.boosts || []).length === 0 ? (
                  <p className="text-[11px] text-[#9fc7e1]">No boosts recorded yet.</p>
                ) : null}
              </div>
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => void onRemoveAllChatBoostsFromModal()}
                disabled={isBoostActionRunning || (userChatBoosts?.boosts.length ?? 0) === 0}
                className="rounded-lg border border-red-300/35 bg-red-700/20 px-3 py-2 text-sm text-red-100 hover:bg-red-700/30 disabled:opacity-50"
              >
                Remove all
              </button>
              <button
                type="button"
                onClick={() => setChatBoostModal(null)}
                className="rounded-lg border border-white/15 px-3 py-2 text-sm text-white hover:bg-white/10"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void onApplyChatBoostFromModal()}
                disabled={isBoostActionRunning}
                className="rounded-lg bg-[#2b5278] px-3 py-2 text-sm font-medium text-white hover:bg-[#366892] disabled:opacity-50"
              >
                Apply boost
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
                    <button
                      type="button"
                      onClick={() => void onInspectSelectedGroupBoosts()}
                      disabled={isGroupActionRunning || groupMembership !== 'joined'}
                      className="rounded-lg border border-white/15 bg-black/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-40"
                    >
                      getUserChatBoosts
                    </button>
                  </div>
                  <p className="mt-2 text-[11px] text-[#9ec3dc]">
                    Latest boosts for current user: {userChatBoosts?.boosts.length ?? 0}
                  </p>
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
                <>
                  <label className="flex items-center gap-2 text-sm text-telegram-textSecondary">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.showAuthorSignature}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, showAuthorSignature: e.target.checked }))}
                    />
                    Show publisher name on channel posts
                  </label>
                  <label className="flex items-center gap-2 text-sm text-telegram-textSecondary">
                    <input
                      type="checkbox"
                      checked={groupProfileDraft.paidStarReactionsEnabled}
                      onChange={(e) => setGroupProfileDraft((prev) => ({ ...prev, paidStarReactionsEnabled: e.target.checked }))}
                    />
                    Enable paid star reactions on channel posts
                  </label>
                </>
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

      {showStoryComposerModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 px-4">
          <div className="w-full max-w-lg rounded-2xl border border-white/20 bg-[#182b3c] p-4 shadow-2xl">
            <div className="mb-3 flex items-start justify-between gap-3">
              <div>
                <h3 className="text-sm font-semibold text-white">
                  {storyBuilder.mode === 'edit' ? 'Edit Story' : 'Post Story'}
                </h3>
                <p className="mt-1 text-xs text-[#9ec3dc]">Story tools moved out of media drawer.</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  if (!isStoryActionRunning) {
                    setShowStoryComposerModal(false);
                  }
                }}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="space-y-3 rounded-xl border border-[#2f4e66]/55 bg-[#102638]/80 px-3 py-2">
              {storyBuilder.mode === 'post' ? (
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                  <select
                    value={storyBuilder.activePeriod}
                    onChange={(event) => setStoryBuilder((prev) => ({ ...prev, activePeriod: event.target.value }))}
                    className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                  >
                    {STORY_ACTIVE_PERIOD_OPTIONS.map((option) => (
                      <option key={`story-period-modal-${option.value}`} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                  <div className="rounded-md border border-[#355a76]/40 bg-black/20 px-2 py-1.5 text-[11px] text-[#a9cee5]">
                    Story lifetime before auto-expire.
                  </div>
                </div>
              ) : null}

              <div className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_auto]">
                <input
                  type="file"
                  accept="image/*,video/*"
                  onClick={(event) => {
                    (event.currentTarget as HTMLInputElement).value = '';
                  }}
                  onChange={(event) => {
                    const nextFile = event.target.files?.[0] || null;
                    setStoryBuilderFile(nextFile);
                    if (nextFile?.type.startsWith('video/')) {
                      setStoryBuilder((prev) => ({ ...prev, contentType: 'video' }));
                    } else if (nextFile?.type.startsWith('image/')) {
                      setStoryBuilder((prev) => ({ ...prev, contentType: 'photo' }));
                    }
                  }}
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
                <select
                  value={storyBuilder.contentType}
                  onChange={(event) => setStoryBuilder((prev) => ({ ...prev, contentType: event.target.value as 'photo' | 'video' }))}
                  className="rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                >
                  <option value="photo">Photo</option>
                  <option value="video">Video</option>
                </select>
              </div>

              {storyBuilderFile ? (
                <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-[#355a76]/45 bg-black/20 px-2 py-1.5 text-[11px] text-[#c8e4f6]">
                  <span className="truncate">Selected file: {storyBuilderFile.name}</span>
                  <button
                    type="button"
                    onClick={() => setStoryBuilderFile(null)}
                    className="rounded border border-white/20 bg-white/10 px-2 py-0.5 text-[10px] text-white hover:bg-white/15"
                  >
                    Clear file
                  </button>
                </div>
              ) : null}

              <input
                value={storyBuilder.contentRef}
                onChange={(event) => setStoryBuilder((prev) => ({ ...prev, contentRef: event.target.value }))}
                placeholder={storyBuilder.mode === 'edit'
                  ? 'New file_id/url reference for edit (or upload file)'
                  : 'Optional file_id/url reference (used when no file selected)'}
                className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
              />

              {storyBuilder.mode === 'edit' ? (
                <input
                  value={storyBuilder.storyId}
                  readOnly
                  className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-[#bddcf0] outline-none"
                />
              ) : null}

              <textarea
                value={storyBuilder.caption}
                onChange={(event) => setStoryBuilder((prev) => ({ ...prev, caption: event.target.value }))}
                placeholder="caption (optional)"
                rows={2}
                className="w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
              />

              <details className="rounded-md border border-[#355a76]/45 bg-black/20 px-2 py-1.5 text-[11px] text-[#c8e4f6]">
                <summary className="cursor-pointer select-none">Advanced: areas JSON</summary>
                <textarea
                  value={storyBuilder.areasJson}
                  onChange={(event) => setStoryBuilder((prev) => ({ ...prev, areasJson: event.target.value }))}
                  placeholder="areas JSON array (optional)"
                  rows={2}
                  className="mt-2 w-full rounded-md border border-[#355a76]/60 bg-black/30 px-2 py-1.5 text-xs text-white outline-none"
                />
              </details>

              <div className="flex items-center justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setShowStoryComposerModal(false)}
                  disabled={isStoryActionRunning}
                  className="rounded-lg border border-white/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-60"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => void submitStoryBuilder()}
                  disabled={!hasStarted || isSending || isStoryActionRunning}
                  className="rounded-lg border border-[#2f7fb4]/60 bg-[#22567c] px-3 py-2 text-sm text-white hover:bg-[#2f6f9f] disabled:opacity-60"
                >
                  {isStoryActionRunning
                    ? 'Working...'
                    : (storyBuilder.mode === 'edit' ? 'Update story' : 'Post story')}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {activeStoryPreview ? (
        <div
          className="fixed inset-0 z-50 flex items-end justify-center bg-black/60 px-4 pb-6 sm:items-center sm:pb-0"
          onClick={() => setActiveStoryPreviewKey(null)}
        >
          <div
            className="w-full max-w-md rounded-2xl border border-white/20 bg-[#152434] p-4 shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="mb-3 flex items-start justify-between gap-3">
              <div>
                <h3 className="text-sm font-semibold text-white">Story Preview</h3>
                <p className="mt-1 text-xs text-[#9ec3dc]">
                  {activeStoryPreview.entry.story.chat.title
                    || (activeStoryPreview.entry.story.chat.username
                      ? `@${activeStoryPreview.entry.story.chat.username}`
                      : `chat ${activeStoryPreview.entry.story.chat.id}`)}
                  {` · #${activeStoryPreview.entry.story.id}`}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setActiveStoryPreviewKey(null)}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            {activeStoryPreview.referenceMessage ? (
              <div className="space-y-2">
                {activeStoryPreview.referenceMessage.media ? (
                  <div className="overflow-hidden rounded-xl border border-white/15 bg-black/25 p-2">
                    {renderMediaContent(activeStoryPreview.referenceMessage)}
                  </div>
                ) : null}
                {activeStoryPreview.referenceMessage.text ? (
                  <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-2 text-sm leading-6 text-[#d8ecfb] break-words whitespace-pre-wrap [overflow-wrap:anywhere]">
                    {renderEntityText(
                      activeStoryPreview.referenceMessage.text,
                      activeStoryPreview.referenceMessage.entities || activeStoryPreview.referenceMessage.captionEntities,
                    )}
                  </div>
                ) : null}
                {!activeStoryPreview.referenceMessage.media && !activeStoryPreview.referenceMessage.text ? (
                  <>
                    {activeStoryPreviewMediaSource && activeStoryPreviewMediaType === 'photo' ? (
                      <img
                        src={activeStoryPreviewMediaSource}
                        alt="story preview"
                        className="max-h-72 w-full rounded-xl border border-white/15 object-cover"
                      />
                    ) : null}
                    {activeStoryPreviewMediaSource && activeStoryPreviewMediaType === 'video' ? (
                      <video
                        src={activeStoryPreviewMediaSource}
                        controls
                        className="max-h-72 w-full rounded-xl border border-white/15"
                      />
                    ) : null}
                    {activeStoryPreviewMediaRef && !activeStoryPreviewMediaSource ? (
                      <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-3 text-xs text-[#b8d7eb]">
                        Loading story media preview...
                      </div>
                    ) : null}
                    {activeStoryPreview.entry.preview?.caption ? (
                      <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-2 text-sm leading-6 text-[#d8ecfb] break-words whitespace-pre-wrap [overflow-wrap:anywhere]">
                        {activeStoryPreview.entry.preview.caption}
                      </div>
                    ) : null}
                    {!activeStoryPreviewMediaRef && !activeStoryPreview.entry.preview?.caption ? (
                      <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-3 text-xs text-[#b8d7eb]">
                        Story reference exists, but no cached preview payload is available yet.
                      </div>
                    ) : null}
                  </>
                ) : null}
              </div>
            ) : (
              <div className="space-y-2">
                {activeStoryPreviewMediaSource && activeStoryPreviewMediaType === 'photo' ? (
                  <img
                    src={activeStoryPreviewMediaSource}
                    alt="story preview"
                    className="max-h-72 w-full rounded-xl border border-white/15 object-cover"
                  />
                ) : null}
                {activeStoryPreviewMediaSource && activeStoryPreviewMediaType === 'video' ? (
                  <video
                    src={activeStoryPreviewMediaSource}
                    controls
                    className="max-h-72 w-full rounded-xl border border-white/15"
                  />
                ) : null}
                {activeStoryPreviewMediaRef && !activeStoryPreviewMediaSource ? (
                  <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-3 text-xs text-[#b8d7eb]">
                    Loading story media preview...
                  </div>
                ) : null}
                {activeStoryPreview.entry.preview?.caption ? (
                  <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-2 text-sm leading-6 text-[#d8ecfb] break-words whitespace-pre-wrap [overflow-wrap:anywhere]">
                    {activeStoryPreview.entry.preview.caption}
                  </div>
                ) : null}
                {!activeStoryPreviewMediaRef && !activeStoryPreview.entry.preview?.caption ? (
                  <div className="rounded-xl border border-white/15 bg-black/25 px-3 py-3 text-xs text-[#b8d7eb]">
                    Preview is limited for this story, but actions are available below.
                  </div>
                ) : null}
              </div>
            )}

            <div className="mt-3 flex flex-wrap items-center justify-end gap-2">
              {activeStoryPreview.referenceMessage ? (
                <button
                  type="button"
                  onClick={() => {
                    scrollToMessage(activeStoryPreview.referenceMessage!.id);
                    setActiveStoryPreviewKey(null);
                  }}
                  className="rounded-lg border border-white/20 bg-white/10 px-3 py-2 text-xs text-white hover:bg-white/15"
                >
                  Jump to message
                </button>
              ) : null}
              {isStoryOwnedByActiveUser(activeStoryPreview.entry.story) ? (
                <>
                  <button
                    type="button"
                    onClick={() => {
                      openStoryEditorForReference(activeStoryPreview.entry.story);
                      setActiveStoryPreviewKey(null);
                    }}
                    disabled={isStoryActionRunning}
                    className="rounded-lg border border-[#7ec8fb]/45 bg-[#1f5379] px-3 py-2 text-xs text-[#d7efff] hover:bg-[#2b6a98] disabled:opacity-60"
                  >
                    Edit story
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setActiveStoryPreviewKey(null);
                      void onDeleteStoryReference(activeStoryPreview.entry.story);
                    }}
                    disabled={isStoryActionRunning}
                    className="rounded-lg border border-red-300/40 bg-red-900/25 px-3 py-2 text-xs text-red-100 hover:bg-red-900/35 disabled:opacity-60"
                  >
                    Delete story
                  </button>
                </>
              ) : (
                <button
                  type="button"
                  onClick={() => {
                    setActiveStoryPreviewKey(null);
                    void onRepostStoryReference(activeStoryPreview.entry.story);
                  }}
                  disabled={isStoryActionRunning}
                  className="rounded-lg border border-[#7ec8fb]/45 bg-[#1f5379] px-3 py-2 text-xs text-[#d7efff] hover:bg-[#2b6a98] disabled:opacity-60"
                >
                  Repost story
                </button>
              )}
            </div>
          </div>
        </div>
      ) : null}

      {declineSuggestedPostModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 px-4">
          <div className="w-full max-w-sm rounded-2xl border border-white/15 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-start justify-between gap-3">
              <div>
                <h3 className="text-sm font-semibold text-white">Decline Suggested Post</h3>
                <p className="mt-1 text-xs text-[#9ec3dc]">Reason is optional, up to 128 characters.</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  if (suggestedPostActionMessageId !== declineSuggestedPostModal.messageId) {
                    setDeclineSuggestedPostModal(null);
                  }
                }}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <form
              className="space-y-3"
              onSubmit={(event) => {
                event.preventDefault();
                void submitDeclineSuggestedPostFromModal();
              }}
            >
              <label className="block text-xs text-[#cbe6f8]">
                <span className="mb-1 block">Reason</span>
                <textarea
                  value={declineSuggestedPostModal.comment}
                  onChange={(event) => setDeclineSuggestedPostModal((prev) => (
                    prev
                      ? {
                        ...prev,
                        comment: event.target.value.slice(0, 128),
                      }
                      : prev
                  ))}
                  rows={4}
                  className="w-full resize-none rounded-lg border border-[#4f7a99]/60 bg-[#0f1c28] px-3 py-2 text-sm text-white outline-none focus:border-[#83c8ff]/70"
                  placeholder="Optional note for creator"
                  autoFocus
                />
                <span className="mt-1 block text-right text-[10px] text-[#9ec3dc]">
                  {declineSuggestedPostModal.comment.length}/128
                </span>
              </label>

              <div className="flex items-center justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setDeclineSuggestedPostModal(null)}
                  disabled={suggestedPostActionMessageId === declineSuggestedPostModal.messageId}
                  className="rounded-lg border border-white/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-60"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={suggestedPostActionMessageId === declineSuggestedPostModal.messageId}
                  className="rounded-lg border border-red-300/45 bg-red-900/30 px-3 py-2 text-sm font-medium text-red-100 hover:bg-red-900/40 disabled:opacity-60"
                >
                  {suggestedPostActionMessageId === declineSuggestedPostModal.messageId ? 'Declining...' : 'Decline post'}
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {paidReactionModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 px-4">
          <div className="w-full max-w-sm rounded-2xl border border-white/15 bg-[#152434] p-4 shadow-2xl">
            <div className="mb-3 flex items-start justify-between gap-3">
              <div>
                <h3 className="text-sm font-semibold text-white">Send Paid Reaction</h3>
                <p className="mt-1 text-xs text-[#9ec3dc]">Choose stars</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  if (!isPaidReactionSubmitting) {
                    setPaidReactionModal(null);
                  }
                }}
                className="rounded-full p-1 text-white hover:bg-white/10"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="rounded-xl border border-amber-200/25 bg-amber-900/15 px-3 py-2 text-xs text-amber-100">
              <div className="flex items-center justify-between">
                <span>Already sent on this message</span>
                <span className="font-semibold">{paidReactionModal.currentPaidCount}⭐</span>
              </div>
              <div className="mt-1 flex items-center justify-between text-amber-50/90">
                <span>Wallet balance</span>
                <span>{walletState.stars}⭐</span>
              </div>
            </div>

            <form
              className="mt-3 space-y-3"
              onSubmit={(event) => {
                event.preventDefault();
                void submitPaidReactionFromModal();
              }}
            >
              <label className="block text-xs text-[#cbe6f8]">
                <span className="mb-1 block">Stars to send</span>
                <input
                  type="number"
                  min={1}
                  step={1}
                  inputMode="numeric"
                  value={paidReactionAmountDraft}
                  onChange={(event) => setPaidReactionAmountDraft(event.target.value)}
                  className="w-full rounded-lg border border-[#4f7a99]/60 bg-[#0f1c28] px-3 py-2 text-sm text-white outline-none focus:border-[#83c8ff]/70"
                  placeholder="1"
                  autoFocus
                />
              </label>

              <div className="flex flex-wrap gap-2">
                {[1, 5, 10, 25].map((amount) => (
                  <button
                    key={`paid-reaction-quick-${amount}`}
                    type="button"
                    onClick={() => setPaidReactionAmountDraft(String(amount))}
                    className="rounded-md border border-white/20 bg-black/20 px-2.5 py-1 text-[11px] text-[#d7ecfb] hover:bg-white/10"
                  >
                    {amount}⭐
                  </button>
                ))}
              </div>

              <div className="flex items-center justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setPaidReactionModal(null)}
                  disabled={isPaidReactionSubmitting}
                  className="rounded-lg border border-white/20 px-3 py-2 text-sm text-white hover:bg-white/10 disabled:opacity-60"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isPaidReactionSubmitting}
                  className="rounded-lg border border-amber-200/50 bg-amber-800/45 px-3 py-2 text-sm font-medium text-amber-50 hover:bg-amber-800/60 disabled:opacity-60"
                >
                  {isPaidReactionSubmitting ? 'Sending...' : 'Send reaction'}
                </button>
              </div>
            </form>
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
                  {[
                    ...TELEGRAM_REACTION_EMOJIS.slice(0, 24),
                    ...((selectedGroup
                      && selectedGroup.type === 'channel'
                      && !selectedGroup.isDirectMessages
                      && selectedGroup.id === target.chatId
                      && selectedGroup.settings?.paidStarReactionsEnabled)
                      ? [PAID_REACTION_KEY]
                      : []),
                  ].map((reactionKey) => {
                    const actorKey = `${selectedUser.id}:0`;
                    const actorReactions = target.actorReactions?.[actorKey] || [];
                    const selected = actorReactions.includes(reactionKey);
                    const paidCount = actorReactions.filter((item) => item === PAID_REACTION_KEY).length;
                    const reactionLabel = reactionKey === PAID_REACTION_KEY && paidCount > 0
                      ? `${renderReactionLabel(reactionKey)}×${paidCount}`
                      : renderReactionLabel(reactionKey);
                    return (
                    <button
                      key={`${target.id}-${reactionKey}`}
                      type="button"
                      onClick={() => void onReactToMessage(target, reactionKey)}
                      className={[
                        'rounded-lg border px-1 py-1 text-sm transition',
                        selected
                          ? 'border-[#86d3ff] bg-[#4f86ad]/80'
                          : 'border-white/15 bg-black/20 hover:bg-white/10',
                      ].join(' ')}
                    >
                      {reactionLabel}
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
                {(() => {
                  const suggestedState = target.suggestedPostInfo?.state?.trim().toLowerCase() || '';
                  const canModerate = !target.service
                    && canManageSuggestedPostsInSelectedChat
                    && selectedGroup?.isDirectMessages
                    && target.chatId === selectedGroup.id
                    && (suggestedState === 'pending' || suggestedState === 'approval_failed');

                  if (!canModerate) {
                    return null;
                  }

                  const inProgress = suggestedPostActionMessageId === target.id;

                  return (
                    <>
                      <button
                        type="button"
                        onClick={() => void onApproveSuggestedPostMessage(target)}
                        disabled={inProgress}
                        className="w-full rounded-lg px-3 py-2 text-left text-sm text-emerald-100 hover:bg-white/10 disabled:opacity-50"
                      >
                        {inProgress ? 'Working...' : 'Approve suggested post'}
                      </button>
                      <button
                        type="button"
                        onClick={() => openDeclineSuggestedPostModal(target)}
                        disabled={inProgress}
                        className="w-full rounded-lg px-3 py-2 text-left text-sm text-red-100 hover:bg-white/10 disabled:opacity-50"
                      >
                        Decline suggested post
                      </button>
                    </>
                  );
                })()}
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
