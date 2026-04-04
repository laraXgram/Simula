import type {
  CallbackQuery as GeneratedCallbackQuery,
  ChosenInlineResult as GeneratedChosenInlineResult,
  InlineKeyboardButton as GeneratedInlineKeyboardButton,
  InlineKeyboardMarkup as GeneratedInlineKeyboardMarkup,
  InlineQuery as GeneratedInlineQuery,
  InlineQueryResult as GeneratedInlineQueryResult,
  KeyboardButton as GeneratedKeyboardButton,
  Message as GeneratedMessage,
  MessageEntity as GeneratedMessageEntity,
  Invoice as GeneratedInvoice,
  Contact as GeneratedContact,
  Location as GeneratedLocation,
  Venue as GeneratedVenue,
  Dice as GeneratedDice,
  Game as GeneratedGame,
  SuccessfulPayment as GeneratedSuccessfulPayment,
  Poll as GeneratedPoll,
  Chat as GeneratedChat,
  ChatPermissions as GeneratedChatPermissions,
  MessageReactionCountUpdated as GeneratedMessageReactionCountUpdated,
  MessageReactionUpdated as GeneratedMessageReactionUpdated,
  ReplyKeyboardMarkup as GeneratedReplyKeyboardMarkup,
  ReplyKeyboardRemove as GeneratedReplyKeyboardRemove,
  Update as GeneratedUpdate,
} from './generated/types';

export interface ChatItem {
  id: number;
  title: string;
  handle?: string;
}

export interface SimBot {
  id: number;
  token: string;
  username: string;
  first_name: string;
}

export interface SimUser {
  id: number;
  username?: string;
  first_name: string;
}

export interface SimChatMembership {
  chat_id: number;
  user_id: number;
  status: string;
  role: string;
  custom_title?: string;
  tag?: string;
}

export interface SimChatJoinRequest {
  chat_id: number;
  user_id: number;
  invite_link?: string;
  status: string;
  date: number;
  first_name?: string;
  username?: string;
}

export interface SimChatSettings {
  chat_id: number;
  description?: string;
  message_history_visible: boolean;
  slow_mode_delay: number;
  permissions: GeneratedChatPermissions;
}

export interface SimBootstrapResponse {
  bot: SimBot;
  users: SimUser[];
  chats?: GeneratedChat[];
  chat_settings?: SimChatSettings[];
  memberships?: SimChatMembership[];
  join_requests?: SimChatJoinRequest[];
}

export interface ChatMessage {
  id: number;
  botToken: string;
  chatId: number;
  text: string;
  date: number;
  isOutgoing: boolean;
  fromName: string;
  fromUserId: number;
  isInlineOrigin?: boolean;
  viaBotUsername?: string;
  poll?: GeneratedPoll;
  contact?: GeneratedContact;
  location?: GeneratedLocation;
  venue?: GeneratedVenue;
  dice?: GeneratedDice;
  game?: GeneratedGame;
  invoice?: GeneratedInvoice;
  successfulPayment?: GeneratedSuccessfulPayment;
  media?: {
    type: 'photo' | 'video' | 'audio' | 'voice' | 'document' | 'sticker' | 'animation' | 'video_note';
    fileId: string;
    mimeType?: string;
    fileName?: string;
    setName?: string;
  };
  mediaGroupId?: string;
  replyTo?: {
    messageId: number;
    fromName: string;
    text?: string;
    hasMedia?: boolean;
    mediaType?: 'photo' | 'video' | 'audio' | 'voice' | 'document' | 'sticker' | 'animation' | 'video_note';
  };
  entities?: MessageEntity[];
  captionEntities?: MessageEntity[];
  replyMarkup?: BotReplyMarkup;
  editDate?: number;
  reactionCounts?: Array<{
    emoji: string;
    count: number;
  }>;
  actorReactions?: Record<string, string[]>;
  service?: {
    kind: 'join' | 'leave' | 'member_update' | 'system';
    targetName?: string;
    oldStatus?: string;
    newStatus?: string;
  };
}

export type ReplyKeyboardButton = GeneratedKeyboardButton;
export type ReplyKeyboardMarkup = GeneratedReplyKeyboardMarkup;
export type ReplyKeyboardRemove = GeneratedReplyKeyboardRemove;
export type InlineKeyboardButton = GeneratedInlineKeyboardButton;
export type InlineKeyboardMarkup = GeneratedInlineKeyboardMarkup;
export type InlineQueryResult = GeneratedInlineQueryResult;

export type BotReplyMarkup =
  | ({ kind: 'reply' } & ReplyKeyboardMarkup)
  | ({ kind: 'inline' } & InlineKeyboardMarkup)
  | ({ kind: 'remove' } & ReplyKeyboardRemove)
  | ({ kind: 'other'; raw: Record<string, unknown> });

export type MessageEntity = GeneratedMessageEntity;

export type BotUpdateMessage = Omit<GeneratedMessage, 'reply_markup' | 'reply_to_message'> & {
  reply_markup?: Record<string, unknown>;
  reply_to_message?: BotUpdateMessage;
};

export type BotUpdate = Omit<GeneratedUpdate,
  'message'
  | 'edited_message'
  | 'inline_query'
  | 'callback_query'
  | 'chosen_inline_result'
  | 'message_reaction'
  | 'message_reaction_count'
> & {
  message?: BotUpdateMessage;
  edited_message?: BotUpdateMessage;
  inline_query?: GeneratedInlineQuery;
  callback_query?: Omit<GeneratedCallbackQuery, 'message'> & {
    message?: BotUpdateMessage;
  };
  chosen_inline_result?: GeneratedChosenInlineResult;
  message_reaction?: GeneratedMessageReactionUpdated;
  message_reaction_count?: GeneratedMessageReactionCountUpdated;
};

export interface SimChatActionEvent {
  sim_event: 'chat_action';
  chat_id: number;
  action: string;
  from_user_id?: number;
  from_name?: string;
  date?: number;
}

export interface SimInvoiceMetaEvent {
  sim_event: 'invoice_meta';
  chat_id: number;
  message_id: number;
  invoice_meta?: {
    photo_url?: string;
    max_tip_amount?: number;
    suggested_tip_amounts?: number[];
    need_name?: boolean;
    need_phone_number?: boolean;
    need_email?: boolean;
    need_shipping_address?: boolean;
    is_flexible?: boolean;
    send_phone_number_to_provider?: boolean;
    send_email_to_provider?: boolean;
  };
}

export type SimRealtimeEvent = SimChatActionEvent | SimInvoiceMetaEvent;
