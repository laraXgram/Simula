import { API_BASE_URL } from './config';
import { SimBootstrapResponse } from '../types/app';
import type {
  AddStickerToSetRequest,
  AnswerCallbackQueryRequest,
  CreateNewStickerSetRequest,
  DeleteStickerFromSetRequest,
  DeleteStickerSetRequest,
  EditMessageCaptionRequest,
  EditMessageLiveLocationRequest,
  EditMessageMediaRequest,
  StopMessageLiveLocationRequest,
  EditMessageTextRequest,
  GetGameHighScoresRequest,
  GetCustomEmojiStickersRequest,
  GetStickerSetRequest,
  ReplaceStickerInSetRequest,
  SendAnimationRequest,
  SendContactRequest,
  SendDiceRequest,
  SendGameRequest,
  SendInvoiceRequest,
  SendLocationRequest,
  SendPollRequest,
  SendStickerRequest,
  SendVenueRequest,
  SendVideoNoteRequest,
  SetGameScoreRequest,
  SetCustomEmojiStickerSetThumbnailRequest,
  SetMessageReactionRequest,
  SetStickerEmojiListRequest,
  SetStickerKeywordsRequest,
  SetStickerMaskPositionRequest,
  SetStickerPositionInSetRequest,
  SetStickerSetThumbnailRequest,
  SetStickerSetTitleRequest,
  StopPollRequest,
  UploadStickerFileRequest,
} from '../types/generated/methods';
import type { File as TgFile, GameHighScore, InlineQueryResult, InlineQueryResultsButton, Message, Sticker, StickerSet } from '../types/generated/types';

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

export interface SimPayInvoiceResult {
  status: 'success' | 'failed';
  pre_checkout_query_id: string;
  message_id?: number;
  payment_method: string;
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

export async function payInvoice(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  payment_method: 'wallet' | 'card' | 'stars';
  outcome: 'success' | 'failed';
  tip_amount?: number;
}): Promise<SimPayInvoiceResult> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/payInvoice`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to pay invoice');
  }

  return data.result as SimPayInvoiceResult;
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
  file: globalThis.File;
  mediaKind?: 'photo' | 'video' | 'audio' | 'voice' | 'document' | 'sticker' | 'animation' | 'video_note';
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  replyToMessageId?: number;
}) {
  const inferMediaKind = (file: globalThis.File): 'photo' | 'video' | 'audio' | 'voice' | 'document' => {
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

  const mediaKind = payload.mediaKind || inferMediaKind(payload.file);
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

export async function sendUserMediaByReference(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  mediaKind: 'sticker' | 'animation' | 'video_note' | 'voice';
  media: string;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  replyToMessageId?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserMedia`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: payload.chatId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      media_kind: payload.mediaKind,
      media: payload.media,
      caption: payload.caption,
      parse_mode: payload.parseMode,
      reply_to_message_id: payload.replyToMessageId,
    }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send referenced user media');
  }

  return data.result;
}

export async function sendUserDice(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  emoji?: string;
  replyToMessageId?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserDice`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: payload.chatId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      emoji: payload.emoji,
      reply_to_message_id: payload.replyToMessageId,
    }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user dice');
  }
  return data.result as Message;
}

export async function sendUserGame(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  gameShortName: string;
  replyToMessageId?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserGame`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: payload.chatId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      game_short_name: payload.gameShortName,
      reply_to_message_id: payload.replyToMessageId,
    }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user game');
  }
  return data.result as Message;
}

export async function sendUserContact(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  phoneNumber: string;
  contactFirstName: string;
  contactLastName?: string;
  vcard?: string;
  replyToMessageId?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserContact`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: payload.chatId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      phone_number: payload.phoneNumber,
      contact_first_name: payload.contactFirstName,
      contact_last_name: payload.contactLastName,
      vcard: payload.vcard,
      reply_to_message_id: payload.replyToMessageId,
    }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user contact');
  }
  return data.result as Message;
}

export async function sendUserLocation(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  latitude: number;
  longitude: number;
  horizontalAccuracy?: number;
  livePeriod?: number;
  heading?: number;
  proximityAlertRadius?: number;
  replyToMessageId?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserLocation`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: payload.chatId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      latitude: payload.latitude,
      longitude: payload.longitude,
      horizontal_accuracy: payload.horizontalAccuracy,
      live_period: payload.livePeriod,
      heading: payload.heading,
      proximity_alert_radius: payload.proximityAlertRadius,
      reply_to_message_id: payload.replyToMessageId,
    }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user location');
  }
  return data.result as Message;
}

export async function sendUserVenue(token: string, payload: {
  chatId: number;
  userId: number;
  firstName: string;
  username?: string;
  latitude: number;
  longitude: number;
  title: string;
  address: string;
  replyToMessageId?: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserVenue`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: payload.chatId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      latitude: payload.latitude,
      longitude: payload.longitude,
      title: payload.title,
      address: payload.address,
      reply_to_message_id: payload.replyToMessageId,
    }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user venue');
  }
  return data.result as Message;
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
  file: globalThis.File;
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

export async function editMessageLiveLocation(token: string, payload: EditMessageLiveLocationRequest): Promise<Message | boolean> {
  return callBotMethod<Message | boolean>(token, 'editMessageLiveLocation', payload);
}

export async function stopMessageLiveLocation(token: string, payload: StopMessageLiveLocationRequest): Promise<Message | boolean> {
  return callBotMethod<Message | boolean>(token, 'stopMessageLiveLocation', payload);
}

export async function editUserMessageMedia(token: string, payload: {
  chatId: number;
  messageId: number;
  mediaKind: 'photo' | 'video' | 'audio' | 'voice' | 'document';
  file: globalThis.File;
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
  method: 'sendPhoto' | 'sendVideo' | 'sendAudio' | 'sendVoice' | 'sendDocument' | 'sendSticker' | 'sendAnimation' | 'sendVideoNote';
  field: 'photo' | 'video' | 'audio' | 'voice' | 'document' | 'sticker' | 'animation' | 'video_note';
  file: globalThis.File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  emoji?: string;
  duration?: number;
  width?: number;
  height?: number;
  length?: number;
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

  if (payload.emoji && payload.field === 'sticker') {
    formData.append('emoji', payload.emoji);
  }
  if (typeof payload.duration === 'number') {
    formData.append('duration', String(payload.duration));
  }
  if (typeof payload.width === 'number') {
    formData.append('width', String(payload.width));
  }
  if (typeof payload.height === 'number') {
    formData.append('height', String(payload.height));
  }
  if (typeof payload.length === 'number') {
    formData.append('length', String(payload.length));
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

export async function sendSticker(token: string, payload: SendStickerRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendSticker', payload);
}

export async function sendAnimation(token: string, payload: SendAnimationRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendAnimation', payload);
}

export async function sendVideoNote(token: string, payload: SendVideoNoteRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendVideoNote', payload);
}

export async function sendDice(token: string, payload: SendDiceRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendDice', payload);
}

export async function sendContact(token: string, payload: SendContactRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendContact', payload);
}

export async function sendLocation(token: string, payload: SendLocationRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendLocation', payload);
}

export async function sendVenue(token: string, payload: SendVenueRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendVenue', payload);
}

export async function sendGame(token: string, payload: SendGameRequest): Promise<Message> {
  return callBotMethod<Message>(token, 'sendGame', payload);
}

export async function setGameScore(token: string, payload: SetGameScoreRequest): Promise<Message | boolean> {
  return callBotMethod<Message | boolean>(token, 'setGameScore', payload);
}

export async function getGameHighScores(token: string, payload: GetGameHighScoresRequest): Promise<GameHighScore[]> {
  return callBotMethod<GameHighScore[]>(token, 'getGameHighScores', payload);
}

export async function getStickerSet(token: string, payload: GetStickerSetRequest): Promise<StickerSet> {
  return callBotMethod<StickerSet>(token, 'getStickerSet', payload);
}

export async function getCustomEmojiStickers(token: string, payload: GetCustomEmojiStickersRequest): Promise<Sticker[]> {
  return callBotMethod<Sticker[]>(token, 'getCustomEmojiStickers', payload);
}

export async function uploadStickerFile(token: string, payload: UploadStickerFileRequest): Promise<TgFile> {
  const formData = new FormData();
  formData.append('user_id', String(payload.user_id));
  formData.append('sticker_format', payload.sticker_format);

  const stickerValue = payload.sticker as unknown;
  const extra = (typeof stickerValue === 'object' && stickerValue !== null && 'extra' in stickerValue)
    ? (stickerValue as { extra?: unknown }).extra
    : undefined;
  if (extra instanceof window.File) {
    formData.append('sticker', extra, extra.name);
  } else if (typeof extra === 'string') {
    formData.append('sticker', extra);
  } else if (typeof payload.sticker === 'string') {
    formData.append('sticker', payload.sticker);
  } else {
    throw new Error('uploadStickerFile requires sticker file or path');
  }

  const response = await fetch(`${API_BASE_URL}/bot${token}/uploadStickerFile`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to upload sticker file');
  }
  return data.result as TgFile;
}

export async function createNewStickerSet(token: string, payload: CreateNewStickerSetRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'createNewStickerSet', payload);
}

export async function addStickerToSet(token: string, payload: AddStickerToSetRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'addStickerToSet', payload);
}

export async function setStickerPositionInSet(token: string, payload: SetStickerPositionInSetRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setStickerPositionInSet', payload);
}

export async function deleteStickerFromSet(token: string, payload: DeleteStickerFromSetRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteStickerFromSet', payload);
}

export async function replaceStickerInSet(token: string, payload: ReplaceStickerInSetRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'replaceStickerInSet', payload);
}

export async function setStickerEmojiList(token: string, payload: SetStickerEmojiListRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setStickerEmojiList', payload);
}

export async function setStickerKeywords(token: string, payload: SetStickerKeywordsRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setStickerKeywords', payload);
}

export async function setStickerMaskPosition(token: string, payload: SetStickerMaskPositionRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setStickerMaskPosition', payload);
}

export async function setStickerSetTitle(token: string, payload: SetStickerSetTitleRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setStickerSetTitle', payload);
}

export async function setStickerSetThumbnail(token: string, payload: SetStickerSetThumbnailRequest): Promise<boolean> {
  const formData = new FormData();
  formData.append('name', payload.name);
  formData.append('user_id', String(payload.user_id));
  formData.append('format', payload.format);

  if (payload.thumbnail !== undefined && payload.thumbnail !== null) {
    const thumb = payload.thumbnail as unknown;
    if (thumb instanceof window.File) {
      formData.append('thumbnail', thumb, thumb.name);
    } else if (typeof thumb === 'string') {
      formData.append('thumbnail', thumb);
    }
  }

  const response = await fetch(`${API_BASE_URL}/bot${token}/setStickerSetThumbnail`, {
    method: 'POST',
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to set sticker set thumbnail');
  }

  return Boolean(data.result);
}

export async function setCustomEmojiStickerSetThumbnail(token: string, payload: SetCustomEmojiStickerSetThumbnailRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setCustomEmojiStickerSetThumbnail', payload);
}

export async function deleteStickerSet(token: string, payload: DeleteStickerSetRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteStickerSet', payload);
}

type MediaGroupItem = {
  type: 'photo' | 'video' | 'audio' | 'document';
  file: globalThis.File;
};

function inferMediaGroupItem(file: globalThis.File): MediaGroupItem {
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
  files: globalThis.File[];
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
