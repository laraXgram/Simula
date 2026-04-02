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

export interface SimBootstrapResponse {
  bot: SimBot;
  users: SimUser[];
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
  media?: {
    type: 'photo' | 'video' | 'audio' | 'voice' | 'document';
    fileId: string;
    mimeType?: string;
    fileName?: string;
  };
  mediaGroupId?: string;
  replyTo?: {
    messageId: number;
    fromName: string;
    text?: string;
    hasMedia?: boolean;
    mediaType?: 'photo' | 'video' | 'audio' | 'voice' | 'document';
  };
  entities?: MessageEntity[];
  captionEntities?: MessageEntity[];
  editDate?: number;
  reactionCounts?: Array<{
    emoji: string;
    count: number;
  }>;
  actorReactions?: Record<string, string[]>;
}

export interface MessageEntity {
  type: string;
  offset: number;
  length: number;
  url?: string;
  language?: string;
}

interface BotApiFileRef {
  file_id: string;
  file_unique_id: string;
  file_size?: number;
  mime_type?: string;
  file_name?: string;
}

export interface BotUpdateMessage {
  message_id: number;
  date: number;
  edit_date?: number;
  text?: string;
  caption?: string;
  chat: {
    id: number;
    type: string;
  };
  from?: {
    id: number;
    is_bot: boolean;
    first_name: string;
    username?: string;
  };
  photo?: BotApiFileRef[];
  video?: BotApiFileRef;
  audio?: BotApiFileRef;
  voice?: BotApiFileRef;
  document?: BotApiFileRef;
  media_group_id?: string;
  reply_to_message?: BotUpdateMessage;
  entities?: MessageEntity[];
  caption_entities?: MessageEntity[];
}

export interface BotUpdate {
  update_id: number;
  message?: BotUpdateMessage;
  edited_message?: BotUpdateMessage;
  message_reaction?: {
    chat: {
      id: number;
      type: string;
    };
    message_id: number;
    user?: {
      id: number;
      is_bot: boolean;
      first_name: string;
      username?: string;
    };
    date: number;
    old_reaction: Array<{ type: 'emoji'; emoji: string }>;
    new_reaction: Array<{ type: 'emoji'; emoji: string }>;
  };
  message_reaction_count?: {
    chat: {
      id: number;
      type: string;
    };
    message_id: number;
    date: number;
    reactions: Array<{
      type: { type: 'emoji'; emoji: string };
      total_count: number;
    }>;
  };
}
