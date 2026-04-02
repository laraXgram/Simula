import { API_BASE_URL } from './config';
import { SimBootstrapResponse } from '../types/app';

export async function callBotMethod<T>(
  token: string,
  method: string,
  params: Record<string, unknown> = {},
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

export async function setBotMessageReaction(token: string, payload: {
  chat_id: number;
  message_id: number;
  reaction: Array<{ type: 'emoji'; emoji: string }>;
}) {
  return callBotMethod<boolean>(token, 'setMessageReaction', payload);
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

export async function editBotMessageText(token: string, payload: {
  chat_id: number;
  message_id: number;
  text: string;
  parse_mode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  return callBotMethod(token, 'editMessageText', payload);
}

export async function editBotMessageCaption(token: string, payload: {
  chat_id: number;
  message_id: number;
  caption?: string;
  parse_mode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  return callBotMethod(token, 'editMessageCaption', payload);
}

export async function editBotMessageMedia(token: string, payload: {
  chatId: number;
  messageId: number;
  mediaType: 'photo' | 'video' | 'audio' | 'document';
  file: File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}) {
  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));
  formData.append('message_id', String(payload.messageId));

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
