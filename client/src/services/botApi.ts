import { API_BASE_URL } from './config';
import { SimBootstrapResponse } from '../types/app';
import type {
  AnswerCallbackQueryRequest,
  EditMessageCaptionRequest,
  EditMessageMediaRequest,
  EditMessageTextRequest,
  SendInvoiceRequest,
  SendPollRequest,
  SetMessageReactionRequest,
  StopPollRequest,
} from '../types/generated/methods';
import type { InlineQueryResult, InlineQueryResultsButton, Message } from '../types/generated/types';

interface InlineQueryAnswerResult {
  inline_query_id: string;
  answered: boolean;
  answered_at?: number;
  answer?: {
    results?: InlineQueryResult[];
    cache_time?: number;
    is_personal?: boolean;
    next_offset?: string;
    button?: InlineQueryResultsButton;
  };
}

interface CallbackQueryAnswerResult {
  callback_query_id: string;
  answered: boolean;
  answered_at?: number;
  answer?: {
    text?: string;
    show_alert?: boolean;
    url?: string;
    cache_time?: number;
  };
}

export interface PollVoterInfo {
  user_id: number;
  first_name: string;
  username?: string;
  option_ids: number[];
}

export interface PollVotersResult {
  poll_id: string;
  anonymous: boolean;
  voters: PollVoterInfo[];
}

export async function callBotMethod<T>(
  token: string,
  method: string,
  params: object = {},
): Promise<T> {
  const response = await fetch(`${API_BASE_URL}/bot${token}/${method}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(params),
  });

  const data = await response.json();

  if (!data.ok) {
    throw new Error(data.description || 'Unknown Telegram API error');
  }

  return data.result as T;
}

export async function getSimulationBootstrap(token: string): Promise<SimBootstrapResponse> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/bootstrap`);
  const data = await response.json();

  if (!data.ok) {
    throw new Error(data.description || 'Unable to bootstrap simulation');
  }

  return data.result as SimBootstrapResponse;
}

export async function sendUserMessage(token: string, payload: {
  chat_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  text: string;
  parse_mode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  reply_to_message_id?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserMessage`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user message');
  }

  return data.result;
}

export async function sendPoll(token: string, payload: SendPollRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendPoll', payload);
}

export async function sendInvoice(token: string, payload: SendInvoiceRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendInvoice', payload);
}

export async function stopPoll(token: string, payload: StopPollRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'stopPoll', payload);
}

export async function votePoll(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  option_ids: number[];
}): Promise<boolean> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/votePoll`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to vote poll');
  }

  return Boolean(data.result);
}

export async function getPollVoters(
  token: string,
  chatId: number,
  messageId: number,
): Promise<PollVotersResult> {
  const response = await fetch(
    `${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/pollVoters?chat_id=${encodeURIComponent(String(chatId))}&message_id=${encodeURIComponent(String(messageId))}`,
  );

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to get poll voters');
  }

  return data.result as PollVotersResult;
}

export async function setBotMessageReaction(token: string, payload: {
  chat_id: number;
  message_id: number;
  reaction: Array<{ type: 'emoji'; emoji: string }>;
}) {
  return callBotMethod<boolean>(token, 'setMessageReaction', payload as SetMessageReactionRequest);
}

export async function setUserMessageReaction(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  reaction: Array<{ type: 'emoji'; emoji: string }>;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/setUserReaction`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to set user reaction');
  }

  return data.result;
}

export async function pressInlineButton(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  callback_data: string;
}): Promise<{ ok: boolean; callback_query_id: string }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/pressInlineButton`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to press inline button');
  }

  return data.result as { ok: boolean; callback_query_id: string };
}

export async function answerCallbackQuery(token: string, payload: AnswerCallbackQueryRequest) {
  return callBotMethod<boolean>(token, 'answerCallbackQuery', payload);
}

export async function sendInlineQuery(token: string, payload: {
  chat_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  query: string;
  offset?: string;
}): Promise<{ inline_query_id: string }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendInlineQuery`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send inline query');
  }

  return data.result as { inline_query_id: string };
}

export async function getInlineQueryAnswer(token: string, inlineQueryId: string): Promise<InlineQueryAnswerResult> {
  const response = await fetch(
    `${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/inlineQueryAnswer?inline_query_id=${encodeURIComponent(inlineQueryId)}`,
  );

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to get inline query answer');
  }

  return data.result as InlineQueryAnswerResult;
}

export async function chooseInlineResult(token: string, payload: {
  inline_query_id: string;
  result_id: string;
}): Promise<{ message_id: number; result_id: string }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/chooseInlineResult`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to choose inline result');
  }

  return data.result as { message_id: number; result_id: string };
}

export async function getCallbackQueryAnswer(token: string, callbackQueryId: string): Promise<CallbackQueryAnswerResult> {
  const response = await fetch(
    `${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/callbackQueryAnswer?callback_query_id=${encodeURIComponent(callbackQueryId)}`,
  );

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to get callback query answer');
  }

  return data.result as CallbackQueryAnswerResult;
}

export async function sendUserMedia(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  file: File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  replyToMessageId?: number;
}) {
  const inferMediaKind = (file: File): 'photo' | 'video' | 'audio' | 'voice' | 'document' => {
    const mime = file.type.toLowerCase();
    if (mime.startsWith('image/')) {
      return 'photo';
    }
    if (mime.startsWith('video/')) {
      return 'video';
    }
    if (mime.startsWith('audio/ogg') || mime.includes('opus')) {
      return 'voice';
    }
    if (mime.startsWith('audio/')) {
      return 'audio';
    }
    return 'document';
  };

  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));
  formData.append('user_id', String(payload.userId));
  formData.append('first_name', payload.firstName);
  if (payload.username) {
    formData.append('username', payload.username);
  }
  if (payload.replyToMessageId) {
    formData.append('reply_to_message_id', String(payload.replyToMessageId));
  }

  const mediaKind = inferMediaKind(payload.file);
  formData.append('media_kind', mediaKind);
  formData.append('media', payload.file, payload.file.name);

  if (payload.caption?.trim()) {
    formData.append('caption', payload.caption.trim());
    if (payload.parseMode) {
      formData.append('parse_mode', payload.parseMode);
    }
  }

  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserMedia`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user media');
  }

  return data.result;
}

export async function createSimBot(payload: {
  first_name?: string;
  username?: string;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bots/create`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to create bot');
  }

  return data.result;
}

export async function updateSimBot(
  token: string,
  payload: {
    first_name?: string;
    username?: string;
  },
) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/update`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to update bot');
  }

  return data.result;
}

export async function upsertSimUser(payload: {
  id?: number;
  first_name?: string;
  username?: string;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/users/upsert`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to save user');
  }

  return data.result;
}

export async function clearSimHistory(token: string, chatId: number) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/clearHistory`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ chat_id: chatId }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to clear history');
  }

  return data.result as { deleted_count: number };
}

export async function editBotMessageText(token: string, payload: EditMessageTextRequest) {
  return callBotMethod(token, 'editMessageText', payload);
}

export async function editBotMessageCaption(token: string, payload: EditMessageCaptionRequest) {
  return callBotMethod(token, 'editMessageCaption', payload);
}

export async function editBotMessageMedia(token: string, payload: {
  chat_id: EditMessageMediaRequest['chat_id'];
  message_id: EditMessageMediaRequest['message_id'];
  mediaType: 'photo' | 'video' | 'audio' | 'document';
  file: File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  const formData = new FormData();
  formData.append('chat_id', String(payload.chat_id));
  formData.append('message_id', String(payload.message_id));

  const attachName = 'media_file';
  formData.append(attachName, payload.file, payload.file.name);

  const media: Record<string, unknown> = {
    type: payload.mediaType,
    media: `attach://${attachName}`,
  };

  if (payload.caption !== undefined) {
    media.caption = payload.caption;
  }

  if (payload.parseMode) {
    media.parse_mode = payload.parseMode;
  }

  formData.append('media', JSON.stringify(media));

  const response = await fetch(`${API_BASE_URL}/bot${token}/editMessageMedia`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to edit media message');
  }

  return data.result;
}

export async function editUserMessageMedia(token: string, payload: {
  chatId: number;
  messageId: number;
  mediaKind: 'photo' | 'video' | 'audio' | 'voice' | 'document';
  file: File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));
  formData.append('message_id', String(payload.messageId));
  formData.append('media_kind', payload.mediaKind);
  formData.append('media', payload.file, payload.file.name);

  if (payload.caption !== undefined) {
    formData.append('caption', payload.caption);
  }

  if (payload.parseMode) {
    formData.append('parse_mode', payload.parseMode);
  }

  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/editUserMessageMedia`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to edit user media message');
  }

  return data.result;
}

export async function deleteBotMessage(token: string, payload: {
  chat_id: number;
  message_id: number;
}) {
  return callBotMethod<boolean>(token, 'deleteMessage', payload);
}

export async function deleteBotMessages(token: string, payload: {
  chat_id: number;
  message_ids: number[];
}) {
  return callBotMethod<boolean>(token, 'deleteMessages', payload);
}

export async function getBotFile(token: string, fileId: string): Promise<{
  file_id: string;
  file_unique_id: string;
  file_size?: number;
  file_path?: string;
}> {
  return callBotMethod(token, 'getFile', { file_id: fileId });
}

export async function sendBotMediaFile(token: string, payload: {
  chatId: number;
  method: 'sendPhoto' | 'sendVideo' | 'sendAudio' | 'sendVoice' | 'sendDocument';
  field: 'photo' | 'video' | 'audio' | 'voice' | 'document';
  file: File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));
  formData.append(payload.field, payload.file, payload.file.name);
  if (payload.caption?.trim()) {
    formData.append('caption', payload.caption.trim());
    if (payload.parseMode) {
      formData.append('parse_mode', payload.parseMode);
    }
  }

  const response = await fetch(`${API_BASE_URL}/bot${token}/${payload.method}`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to upload media');
  }

  return data.result;
}

type MediaGroupItem = {
  type: 'photo' | 'video' | 'audio' | 'document';
  file: File;
};

function inferMediaGroupItem(file: File): MediaGroupItem {
  const mime = file.type.toLowerCase();
  if (mime.startsWith('image/')) {
    return { type: 'photo', file };
  }
  if (mime.startsWith('video/')) {
    return { type: 'video', file };
  }
  if (mime.startsWith('audio/')) {
    return { type: 'audio', file };
  }
  return { type: 'document', file };
}

export async function sendBotMediaGroup(token: string, payload: {
  chatId: number;
  files: File[];
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  if (payload.files.length < 2 || payload.files.length > 10) {
    throw new Error('Media group requires 2 to 10 files');
  }

  const items = payload.files.map(inferMediaGroupItem);
  const types = new Set(items.map((item) => item.type));

  if (types.has('audio') && types.size > 1) {
    throw new Error('Audio album can only contain audio files');
  }

  if (types.has('document') && types.size > 1) {
    throw new Error('Document album can only contain document files');
  }

  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));

  const media = items.map((item, index) => {
    const attachName = `file${index}`;
    formData.append(attachName, item.file, item.file.name);

    return {
      type: item.type,
      media: `attach://${attachName}`,
      ...(index === 0 && payload.caption?.trim()
        ? {
          caption: payload.caption.trim(),
          ...(payload.parseMode ? { parse_mode: payload.parseMode } : {}),
        }
        : {}),
    };
  });

  formData.append('media', JSON.stringify(media));

  const response = await fetch(`${API_BASE_URL}/bot${token}/sendMediaGroup`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to upload media group');
  }

  return data.result;
}
