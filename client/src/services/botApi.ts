import { API_BASE_URL } from './config';
import { SimBootstrapResponse } from '../types/app';
import type {
  AddStickerToSetRequest,
  CloseRequest,
  AnswerCallbackQueryRequest,
  AnswerWebAppQueryRequest,
  BanChatMemberRequest,
  BanChatSenderChatRequest,
  ApproveChatJoinRequestRequest,
  ApproveSuggestedPostRequest,
  CreateChatInviteLinkRequest,
  CreateChatSubscriptionInviteLinkRequest,
  CreateNewStickerSetRequest,
  CopyMessageRequest,
  CopyMessagesRequest,
  DeclineSuggestedPostRequest,
  DeclineChatJoinRequestRequest,
  DeleteStoryRequest,
  DeleteChatPhotoRequest,
  DeleteChatStickerSetRequest,
  DeleteForumTopicRequest,
  DeleteStickerFromSetRequest,
  DeleteStickerSetRequest,
  EditChatInviteLinkRequest,
  EditChatSubscriptionInviteLinkRequest,
  EditMessageCaptionRequest,
  EditMessageChecklistRequest,
  EditMessageLiveLocationRequest,
  EditMessageMediaRequest,
  EditStoryRequest,
  EditGeneralForumTopicRequest,
  EditForumTopicRequest,
  ExportChatInviteLinkRequest,
  GetForumTopicIconStickersRequest,
  GetChatAdministratorsRequest,
  GetMyCommandsRequest,
  GetMyDefaultAdministratorRightsRequest,
  GetMyDescriptionRequest,
  GetMyNameRequest,
  GetMyShortDescriptionRequest,
  GetChatGiftsRequest,
  GetChatMenuButtonRequest,
  GetChatMemberCountRequest,
  GetChatMemberRequest,
  GetChatRequest,
  GetWebhookInfoRequest,
  GetAvailableGiftsRequest,
  StopMessageLiveLocationRequest,
  EditMessageTextRequest,
  GetGameHighScoresRequest,
  GetManagedBotTokenRequest,
  GetUserGiftsRequest,
  GetUserChatBoostsRequest,
  GetUserProfileAudiosRequest,
  GetUserProfilePhotosRequest,
  GiftPremiumSubscriptionRequest,
  ConvertGiftToStarsRequest,
  UpgradeGiftRequest,
  TransferGiftRequest,
  GetCustomEmojiStickersRequest,
  GetStickerSetRequest,
  ForwardMessageRequest,
  ForwardMessagesRequest,
  LeaveChatRequest,
  PinChatMessageRequest,
  PostStoryRequest,
  PromoteChatMemberRequest,
  RepostStoryRequest,
  ReopenForumTopicRequest,
  ReopenGeneralForumTopicRequest,
  RemoveChatVerificationRequest,
  RemoveUserVerificationRequest,
  ReplaceManagedBotTokenRequest,
  ReplaceStickerInSetRequest,
  RevokeChatInviteLinkRequest,
  RestrictChatMemberRequest,
  SendAnimationRequest,
  SendChatActionRequest,
  SendContactRequest,
  SendDiceRequest,
  SendGameRequest,
  SendInvoiceRequest,
  SendLocationRequest,
  SendMessageRequest,
  SendChecklistRequest,
  SendPollRequest,
  SendStickerRequest,
  SavePreparedInlineMessageRequest,
  SavePreparedKeyboardButtonRequest,
  SendVenueRequest,
  SendVideoNoteRequest,
  DeleteMyCommandsRequest,
  RemoveMyProfilePhotoRequest,
  SetChatAdministratorCustomTitleRequest,
  SetChatDescriptionRequest,
  SetChatMemberTagRequest,
  SetChatMenuButtonRequest,
  SetChatPermissionsRequest,
  SetChatStickerSetRequest,
  SetChatTitleRequest,
  SetMyCommandsRequest,
  SetMyDefaultAdministratorRightsRequest,
  SetMyDescriptionRequest,
  SetMyNameRequest,
  SetMyProfilePhotoRequest,
  SetMyShortDescriptionRequest,
  SetPassportDataErrorsRequest,
  SetUserEmojiStatusRequest,
  SetGameScoreRequest,
  SetCustomEmojiStickerSetThumbnailRequest,
  SetMessageReactionRequest,
  SetStickerEmojiListRequest,
  SetStickerKeywordsRequest,
  SetStickerMaskPositionRequest,
  SetStickerPositionInSetRequest,
  SetStickerSetThumbnailRequest,
  SetStickerSetTitleRequest,
  SendGiftRequest,
  StopPollRequest,
  HideGeneralForumTopicRequest,
  CloseForumTopicRequest,
  CloseGeneralForumTopicRequest,
  CreateForumTopicRequest,
  UnhideGeneralForumTopicRequest,
  UnpinAllForumTopicMessagesRequest,
  UnpinAllGeneralForumTopicMessagesRequest,
  UnbanChatMemberRequest,
  UnbanChatSenderChatRequest,
  UnpinAllChatMessagesRequest,
  UnpinChatMessageRequest,
  UploadStickerFileRequest,
  VerifyChatRequest,
  VerifyUserRequest,
  LogOutRequest,
} from '../types/generated/methods';
import type { BotCommand, BotDescription, BotName, BotShortDescription, BusinessBotRights, BusinessConnection as GeneratedBusinessConnection, Chat as GeneratedChat, ChatAdministratorRights, ChatFullInfo, ChatInviteLink, ChatMember, ChatPermissions, ChatShared, File as TgFile, ForumTopic, GameHighScore, InlineQueryResult, InlineQueryResultsButton, KeyboardButtonRequestManagedBot, MenuButton, Message, PreparedInlineMessage, PreparedKeyboardButton, SentWebAppMessage, Sticker, StickerSet, SuggestedPostParameters, User as GeneratedUser, UserChatBoosts, UserProfileAudios, UserProfilePhotos, UsersShared, WebAppData, WebhookInfo } from '../types/generated/types';
import type { Story } from '../types/generated/types';

import type { Gifts, OwnedGifts } from '../types/generated/types';

export interface SimCreateGroupResult {
  chat: GeneratedChat;
  owner: GeneratedUser;
  members: GeneratedUser[];
  settings: {
    show_author_signature?: boolean;
    message_history_visible: boolean;
    slow_mode_delay: number;
    permissions: ChatPermissions;
  };
}

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

interface BotMethodCallOptions {
  actorUserId?: number;
}

function emitBotApiDebugEvent(detail: {
  at: number;
  token: string;
  method: string;
  request: unknown;
  ok: boolean;
  response?: unknown;
  error?: string;
}) {
  if (typeof window === 'undefined') {
    return;
  }

  try {
    window.dispatchEvent(new CustomEvent('simula:bot-api-log', { detail }));
  } catch {
    // Ignore debug event failures to keep API calls unaffected.
  }
}

export interface SimPurchasePaidMediaResult {
  status: 'success';
  paid_media_payload: string;
  star_count: number;
  already_purchased?: boolean;
}

function normalizeTextInput(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }

  if (value === null || value === undefined) {
    return '';
  }

  return String(value);
}

function actorHeader(actorUserId?: number): Record<string, string> {
  if (typeof actorUserId === 'number' && Number.isFinite(actorUserId)) {
    return {
      'X-Simula-Actor-User-Id': String(Math.trunc(actorUserId)),
    };
  }

  return {};
}

export async function callBotMethod<T>(
  token: string,
  method: string,
  params: object = {},
  options: BotMethodCallOptions = {},
): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };

  if (typeof options.actorUserId === 'number' && Number.isFinite(options.actorUserId)) {
    headers['X-Simula-Actor-User-Id'] = String(Math.trunc(options.actorUserId));
  }

  let emitted = false;
  try {
    const response = await fetch(`${API_BASE_URL}/bot${token}/${method}`, {
      method: 'POST',
      headers,
      body: JSON.stringify(params),
    });

    const data = await response.json();

    if (!data.ok) {
      const errorMessage = data.description || 'Unknown Telegram API error';
      emitBotApiDebugEvent({
        at: Date.now(),
        token,
        method,
        request: params,
        ok: false,
        error: errorMessage,
      });
      emitted = true;
      throw new Error(errorMessage);
    }

    emitBotApiDebugEvent({
      at: Date.now(),
      token,
      method,
      request: params,
      ok: true,
      response: data.result,
    });
    emitted = true;
    return data.result as T;
  } catch (error) {
    if (!emitted) {
      emitBotApiDebugEvent({
        at: Date.now(),
        token,
        method,
        request: params,
        ok: false,
        error: error instanceof Error ? error.message : 'Network/API error',
      });
    }
    throw error;
  }
}

export async function getSimulationBootstrap(token: string): Promise<SimBootstrapResponse> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/bootstrap`);
  const data = await response.json();

  if (!data.ok) {
    throw new Error(data.description || 'Unable to bootstrap simulation');
  }

  return data.result as SimBootstrapResponse;
}

export async function createSimulationGroup(token: string, payload: {
  title: string;
  chat_type?: 'group' | 'supergroup' | 'channel';
  owner_user_id?: number;
  owner_first_name?: string;
  owner_username?: string;
  initial_member_ids?: number[];
  username?: string;
  description?: string;
  is_forum?: boolean;
  show_author_signature?: boolean;
  message_history_visible?: boolean;
  slow_mode_delay?: number;
}): Promise<SimCreateGroupResult> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/create`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to create simulation group');
  }

  return data.result as SimCreateGroupResult;
}

export async function openSimulationChannelDirectMessages(token: string, payload: {
  channel_chat_id: number;
  user_id?: number;
  first_name?: string;
  username?: string;
}): Promise<{
  chat: GeneratedChat;
  parent_chat: GeneratedChat;
  topics: Array<{
    topic_id: number;
    user_id: number;
    name: string;
    updated_at?: number;
  }>;
}> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/channels/direct-messages/open`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to open channel direct messages');
  }

  return data.result as {
    chat: GeneratedChat;
    parent_chat: GeneratedChat;
    topics: Array<{
      topic_id: number;
      user_id: number;
      name: string;
      updated_at?: number;
    }>;
  };
}

export async function setSimulationBusinessConnection(token: string, payload: {
  user_id?: number;
  first_name?: string;
  username?: string;
  business_connection_id?: string;
  enabled?: boolean;
  rights?: BusinessBotRights;
}): Promise<GeneratedBusinessConnection> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/business/connection`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to configure business connection');
  }

  return data.result as GeneratedBusinessConnection;
}

export async function removeSimulationBusinessConnection(token: string, payload: {
  user_id?: number;
  business_connection_id?: string;
}): Promise<{
  deleted: boolean;
  business_connection_id?: string;
  user_id?: number;
}> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/business/connection/remove`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to remove business connection');
  }

  return data.result as {
    deleted: boolean;
    business_connection_id?: string;
    user_id?: number;
  };
}

export async function joinSimulationGroup(token: string, payload: {
  chat_id: number;
  user_id?: number;
  first_name?: string;
  username?: string;
}): Promise<{ joined: boolean; pending?: boolean; reason?: string; chat_id: number; user_id: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/join`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to join simulation group');
  }

  return data.result as { joined: boolean; pending?: boolean; reason?: string; chat_id: number; user_id: number };
}

export async function leaveSimulationGroup(token: string, payload: {
  chat_id: number;
  user_id?: number;
  first_name?: string;
  username?: string;
}): Promise<{ left: boolean; reason?: string; chat_id: number; user_id: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/leave`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to leave simulation group');
  }

  return data.result as { left: boolean; reason?: string; chat_id: number; user_id: number };
}

export async function updateSimulationGroup(token: string, payload: {
  chat_id: number;
  user_id?: number;
  actor_first_name?: string;
  actor_username?: string;
  title?: string;
  username?: string;
  description?: string;
  is_forum?: boolean;
  show_author_signature?: boolean;
  paid_star_reactions_enabled?: boolean;
  linked_chat_id?: number;
  direct_messages_enabled?: boolean;
  direct_messages_star_count?: number;
  message_history_visible?: boolean;
  slow_mode_delay?: number;
  permissions?: ChatPermissions;
}): Promise<{
  chat: GeneratedChat;
  settings?: {
    description?: string;
    show_author_signature?: boolean;
    paid_star_reactions_enabled?: boolean;
    linked_chat_id?: number;
    direct_messages_enabled?: boolean;
    direct_messages_star_count?: number;
    message_history_visible: boolean;
    slow_mode_delay: number;
    permissions: ChatPermissions;
  };
}> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/update`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to update simulation group');
  }

  return data.result as {
    chat: GeneratedChat;
    settings?: {
      description?: string;
      show_author_signature?: boolean;
      paid_star_reactions_enabled?: boolean;
      linked_chat_id?: number;
      direct_messages_enabled?: boolean;
      direct_messages_star_count?: number;
      message_history_visible: boolean;
      slow_mode_delay: number;
      permissions: ChatPermissions;
    };
  };
}

export async function setSimulationBotGroupMembership(token: string, payload: {
  chat_id: number;
  actor_user_id?: number;
  actor_first_name?: string;
  actor_username?: string;
  status: 'member' | 'admin' | 'administrator' | 'left' | 'remove';
}): Promise<{ changed: boolean; chat_id: number; status: string }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/bot-membership`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to update bot group membership');
  }

  return data.result as { changed: boolean; chat_id: number; status: string };
}

export async function banChatMember(token: string, payload: BanChatMemberRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'banChatMember', payload, { actorUserId });
}

export async function unbanChatMember(token: string, payload: UnbanChatMemberRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unbanChatMember', payload, { actorUserId });
}

export async function restrictChatMember(token: string, payload: RestrictChatMemberRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'restrictChatMember', payload, { actorUserId });
}

export async function promoteChatMember(token: string, payload: PromoteChatMemberRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'promoteChatMember', payload, { actorUserId });
}

export async function setChatAdministratorCustomTitle(
  token: string,
  payload: SetChatAdministratorCustomTitleRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatAdministratorCustomTitle', payload, { actorUserId });
}

export async function setChatMemberTag(token: string, payload: SetChatMemberTagRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatMemberTag', payload, { actorUserId });
}

export async function banChatSenderChat(token: string, payload: BanChatSenderChatRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'banChatSenderChat', payload, { actorUserId });
}

export async function unbanChatSenderChat(token: string, payload: UnbanChatSenderChatRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unbanChatSenderChat', payload, { actorUserId });
}

export async function setChatTitle(token: string, payload: SetChatTitleRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatTitle', payload, { actorUserId });
}

export async function setChatDescription(token: string, payload: SetChatDescriptionRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatDescription', payload, { actorUserId });
}

export async function setChatPermissions(token: string, payload: SetChatPermissionsRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatPermissions', payload, { actorUserId });
}

export async function setChatPhoto(token: string, payload: {
  chat_id: number | string;
  photo: globalThis.File | string;
}, actorUserId?: number): Promise<boolean> {
  const formData = new FormData();
  formData.append('chat_id', String(payload.chat_id));
  if (payload.photo instanceof window.File) {
    formData.append('photo', payload.photo, payload.photo.name);
  } else {
    formData.append('photo', payload.photo);
  }

  const headers: Record<string, string> = {};
  if (typeof actorUserId === 'number' && Number.isFinite(actorUserId)) {
    headers['X-Simula-Actor-User-Id'] = String(Math.trunc(actorUserId));
  }

  const response = await fetch(`${API_BASE_URL}/bot${token}/setChatPhoto`, {
    method: 'POST',
    headers: Object.keys(headers).length > 0 ? headers : undefined,
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to set chat photo');
  }
  return Boolean(data.result);
}

export async function deleteChatPhoto(token: string, payload: DeleteChatPhotoRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteChatPhoto', payload, { actorUserId });
}

export async function setChatStickerSet(token: string, payload: SetChatStickerSetRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatStickerSet', payload, { actorUserId });
}

export async function deleteChatStickerSet(token: string, payload: DeleteChatStickerSetRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteChatStickerSet', payload, { actorUserId });
}

export async function getForumTopicIconStickers(
  token: string,
  payload: GetForumTopicIconStickersRequest = {},
): Promise<Sticker[]> {
  return callBotMethod<Sticker[]>(token, 'getForumTopicIconStickers', payload);
}

export async function createForumTopic(
  token: string,
  payload: CreateForumTopicRequest,
  actorUserId?: number,
): Promise<ForumTopic> {
  return callBotMethod<ForumTopic>(token, 'createForumTopic', payload, { actorUserId });
}

export async function editForumTopic(
  token: string,
  payload: EditForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'editForumTopic', payload, { actorUserId });
}

export async function closeForumTopic(
  token: string,
  payload: CloseForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'closeForumTopic', payload, { actorUserId });
}

export async function reopenForumTopic(
  token: string,
  payload: ReopenForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'reopenForumTopic', payload, { actorUserId });
}

export async function deleteForumTopic(
  token: string,
  payload: DeleteForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteForumTopic', payload, { actorUserId });
}

export async function unpinAllForumTopicMessages(
  token: string,
  payload: UnpinAllForumTopicMessagesRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unpinAllForumTopicMessages', payload, { actorUserId });
}

export async function editGeneralForumTopic(
  token: string,
  payload: EditGeneralForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'editGeneralForumTopic', payload, { actorUserId });
}

export async function closeGeneralForumTopic(
  token: string,
  payload: CloseGeneralForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'closeGeneralForumTopic', payload, { actorUserId });
}

export async function reopenGeneralForumTopic(
  token: string,
  payload: ReopenGeneralForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'reopenGeneralForumTopic', payload, { actorUserId });
}

export async function hideGeneralForumTopic(
  token: string,
  payload: HideGeneralForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'hideGeneralForumTopic', payload, { actorUserId });
}

export async function unhideGeneralForumTopic(
  token: string,
  payload: UnhideGeneralForumTopicRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unhideGeneralForumTopic', payload, { actorUserId });
}

export async function unpinAllGeneralForumTopicMessages(
  token: string,
  payload: UnpinAllGeneralForumTopicMessagesRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unpinAllGeneralForumTopicMessages', payload, { actorUserId });
}

export async function pinChatMessage(token: string, payload: PinChatMessageRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'pinChatMessage', payload, { actorUserId });
}

export async function unpinChatMessage(token: string, payload: UnpinChatMessageRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unpinChatMessage', payload, { actorUserId });
}

export async function unpinAllChatMessages(token: string, payload: UnpinAllChatMessagesRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'unpinAllChatMessages', payload, { actorUserId });
}

export async function leaveChat(token: string, payload: LeaveChatRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'leaveChat', payload);
}

export async function getChat(token: string, payload: GetChatRequest, actorUserId?: number): Promise<ChatFullInfo> {
  return callBotMethod<ChatFullInfo>(token, 'getChat', payload, { actorUserId });
}

export async function getChatAdministrators(token: string, payload: GetChatAdministratorsRequest, actorUserId?: number): Promise<ChatMember[]> {
  return callBotMethod<ChatMember[]>(token, 'getChatAdministrators', payload, { actorUserId });
}

export async function getChatMemberCount(token: string, payload: GetChatMemberCountRequest, actorUserId?: number): Promise<number> {
  return callBotMethod<number>(token, 'getChatMemberCount', payload, { actorUserId });
}

export async function getChatMember(token: string, payload: GetChatMemberRequest, actorUserId?: number): Promise<ChatMember> {
  return callBotMethod<ChatMember>(token, 'getChatMember', payload, { actorUserId });
}

export async function exportChatInviteLink(token: string, payload: ExportChatInviteLinkRequest, actorUserId?: number): Promise<string> {
  return callBotMethod<string>(token, 'exportChatInviteLink', payload, { actorUserId });
}

export async function createChatInviteLink(token: string, payload: CreateChatInviteLinkRequest, actorUserId?: number): Promise<ChatInviteLink> {
  return callBotMethod<ChatInviteLink>(token, 'createChatInviteLink', payload, { actorUserId });
}

export async function editChatInviteLink(token: string, payload: EditChatInviteLinkRequest, actorUserId?: number): Promise<ChatInviteLink> {
  return callBotMethod<ChatInviteLink>(token, 'editChatInviteLink', payload, { actorUserId });
}

export async function revokeChatInviteLink(token: string, payload: RevokeChatInviteLinkRequest, actorUserId?: number): Promise<ChatInviteLink> {
  return callBotMethod<ChatInviteLink>(token, 'revokeChatInviteLink', payload, { actorUserId });
}

export async function createChatSubscriptionInviteLink(
  token: string,
  payload: CreateChatSubscriptionInviteLinkRequest,
  actorUserId?: number,
): Promise<ChatInviteLink> {
  return callBotMethod<ChatInviteLink>(token, 'createChatSubscriptionInviteLink', payload, { actorUserId });
}

export async function editChatSubscriptionInviteLink(
  token: string,
  payload: EditChatSubscriptionInviteLinkRequest,
  actorUserId?: number,
): Promise<ChatInviteLink> {
  return callBotMethod<ChatInviteLink>(token, 'editChatSubscriptionInviteLink', payload, { actorUserId });
}

export async function approveChatJoinRequest(token: string, payload: ApproveChatJoinRequestRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'approveChatJoinRequest', payload, { actorUserId });
}

export async function declineChatJoinRequest(token: string, payload: DeclineChatJoinRequestRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'declineChatJoinRequest', payload, { actorUserId });
}

export async function sendChatAction(token: string, payload: SendChatActionRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'sendChatAction', payload, { actorUserId });
}

export async function setMyCommands(token: string, payload: SetMyCommandsRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setMyCommands', payload, { actorUserId });
}

export async function getMyCommands(token: string, payload: GetMyCommandsRequest = {}, actorUserId?: number): Promise<BotCommand[]> {
  return callBotMethod<BotCommand[]>(token, 'getMyCommands', payload, { actorUserId });
}

export async function deleteMyCommands(token: string, payload: DeleteMyCommandsRequest = {}, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteMyCommands', payload, { actorUserId });
}

export async function setMyName(token: string, payload: SetMyNameRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setMyName', payload, { actorUserId });
}

export async function getMyName(token: string, payload: GetMyNameRequest = {}, actorUserId?: number): Promise<BotName> {
  return callBotMethod<BotName>(token, 'getMyName', payload, { actorUserId });
}

export async function setMyDescription(token: string, payload: SetMyDescriptionRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setMyDescription', payload, { actorUserId });
}

export async function getMyDescription(token: string, payload: GetMyDescriptionRequest = {}, actorUserId?: number): Promise<BotDescription> {
  return callBotMethod<BotDescription>(token, 'getMyDescription', payload, { actorUserId });
}

export async function setMyShortDescription(token: string, payload: SetMyShortDescriptionRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setMyShortDescription', payload, { actorUserId });
}

export async function getMyShortDescription(token: string, payload: GetMyShortDescriptionRequest = {}, actorUserId?: number): Promise<BotShortDescription> {
  return callBotMethod<BotShortDescription>(token, 'getMyShortDescription', payload, { actorUserId });
}

export async function setMyProfilePhoto(token: string, payload: SetMyProfilePhotoRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setMyProfilePhoto', payload, { actorUserId });
}

export async function removeMyProfilePhoto(token: string, payload: RemoveMyProfilePhotoRequest = {}, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'removeMyProfilePhoto', payload, { actorUserId });
}

export async function setMyDefaultAdministratorRights(
  token: string,
  payload: SetMyDefaultAdministratorRightsRequest,
  actorUserId?: number,
): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setMyDefaultAdministratorRights', payload, { actorUserId });
}

export async function getMyDefaultAdministratorRights(
  token: string,
  payload: GetMyDefaultAdministratorRightsRequest = {},
  actorUserId?: number,
): Promise<ChatAdministratorRights> {
  return callBotMethod<ChatAdministratorRights>(token, 'getMyDefaultAdministratorRights', payload, { actorUserId });
}

export async function setChatMenuButton(token: string, payload: SetChatMenuButtonRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setChatMenuButton', payload, { actorUserId });
}

export async function getChatMenuButton(token: string, payload: GetChatMenuButtonRequest, actorUserId?: number): Promise<MenuButton> {
  return callBotMethod<MenuButton>(token, 'getChatMenuButton', payload, { actorUserId });
}

export async function getSimBotPrivacyMode(token: string): Promise<{ enabled: boolean }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/privacy-mode`);
  const data = await response.json();

  if (!data.ok) {
    throw new Error(data.description || 'Unable to load bot privacy mode');
  }

  return data.result as { enabled: boolean };
}

export async function setSimBotPrivacyMode(token: string, enabled: boolean): Promise<{ enabled: boolean }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/privacy-mode`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ enabled }),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to update bot privacy mode');
  }

  return data.result as { enabled: boolean };
}

export async function deleteSimulationGroup(token: string, payload: {
  chat_id: number;
  user_id: number;
  actor_first_name?: string;
  actor_username?: string;
}): Promise<{ deleted: boolean; chat_id: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/delete`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to delete simulation group');
  }

  return data.result as { deleted: boolean; chat_id: number };
}

export async function createSimulationGroupInviteLink(token: string, payload: {
  chat_id: number;
  user_id?: number;
  actor_first_name?: string;
  actor_username?: string;
  creates_join_request?: boolean;
  name?: string;
  expire_date?: number;
  member_limit?: number;
}): Promise<ChatInviteLink> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/invite/create`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to create group invite link');
  }

  return data.result as ChatInviteLink;
}

export async function joinSimulationGroupByInviteLink(token: string, payload: {
  invite_link: string;
  user_id?: number;
  first_name?: string;
  username?: string;
}): Promise<{ joined: boolean; pending?: boolean; reason?: string; chat_id: number; chat_type?: 'group' | 'supergroup' | 'channel'; user_id: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/invite/join`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to join by invite link');
  }

  return data.result as { joined: boolean; pending?: boolean; reason?: string; chat_id: number; chat_type?: 'group' | 'supergroup' | 'channel'; user_id: number };
}

export async function markSimulationChannelMessageView(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id?: number;
  first_name?: string;
  username?: string;
}): Promise<{ chat_id: number; chat_type: 'channel'; message_id: number; user_id: number; views: number; incremented: boolean; window_seconds: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/channels/views/mark`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to update channel views');
  }

  return data.result as { chat_id: number; chat_type: 'channel'; message_id: number; user_id: number; views: number; incremented: boolean; window_seconds: number };
}

export async function approveSimulationGroupJoinRequest(token: string, payload: {
  chat_id: number;
  user_id: number;
  actor_user_id?: number;
  actor_first_name?: string;
  actor_username?: string;
}): Promise<{ approved: boolean; joined?: boolean; reason?: string; chat_id: number; user_id: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/join-requests/approve`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to approve join request');
  }

  return data.result as { approved: boolean; joined?: boolean; reason?: string; chat_id: number; user_id: number };
}

export async function declineSimulationGroupJoinRequest(token: string, payload: {
  chat_id: number;
  user_id: number;
  actor_user_id?: number;
  actor_first_name?: string;
  actor_username?: string;
}): Promise<{ declined: boolean; reason?: string; chat_id: number; user_id: number }> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/groups/join-requests/decline`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to decline join request');
  }

  return data.result as { declined: boolean; reason?: string; chat_id: number; user_id: number };
}

export async function sendUserMessage(token: string, payload: {
  chat_id: number;
  message_thread_id?: number;
  direct_messages_topic_id?: number;
  user_id: number;
  first_name: string;
  username?: string;
  sender_chat_id?: number;
  text: string;
  parse_mode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  suggested_post_parameters?: SuggestedPostParameters;
  reply_to_message_id?: number;
  users_shared?: UsersShared;
  chat_shared?: ChatShared;
  web_app_data?: WebAppData;
  managed_bot_request?: KeyboardButtonRequestManagedBot;
}) {
  const normalizedPayload = {
    ...payload,
    business_connection_id: undefined,
    text: normalizeTextInput(payload.text),
  };

  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/sendUserMessage`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(normalizedPayload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send user message');
  }

  return data.result;
}

export async function sendPoll(token: string, payload: SendPollRequest, actorUserId?: number): Promise<Message> {
  return callBotMethod<Message>(token, 'sendPoll', payload, { actorUserId });
}

export async function getAvailableGifts(token: string, payload: GetAvailableGiftsRequest = {}): Promise<Gifts> {
  return callBotMethod<Gifts>(token, 'getAvailableGifts', payload);
}

export async function sendGift(token: string, payload: SendGiftRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'sendGift', payload, { actorUserId });
}

export async function giftPremiumSubscription(token: string, payload: GiftPremiumSubscriptionRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'giftPremiumSubscription', payload, { actorUserId });
}

export async function getUserGifts(token: string, payload: GetUserGiftsRequest): Promise<OwnedGifts> {
  return callBotMethod<OwnedGifts>(token, 'getUserGifts', payload);
}

export async function getChatGifts(token: string, payload: GetChatGiftsRequest): Promise<OwnedGifts> {
  return callBotMethod<OwnedGifts>(token, 'getChatGifts', payload);
}

export async function deleteOwnedGift(token: string, payload: {
  owned_gift_id: string;
  user_id?: number;
  chat_id?: number;
}, actorUserId?: number): Promise<boolean> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/deleteOwnedGift`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...actorHeader(actorUserId),
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to delete owned gift');
  }

  return data.result as boolean;
}

export async function convertGiftToStars(token: string, payload: ConvertGiftToStarsRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'convertGiftToStars', payload);
}

export async function upgradeGift(token: string, payload: UpgradeGiftRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'upgradeGift', payload);
}

export async function transferGift(token: string, payload: TransferGiftRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'transferGift', payload);
}

export async function purchasePaidMedia(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id?: number;
  first_name?: string;
  username?: string;
  paid_media_payload?: string;
}): Promise<SimPurchasePaidMediaResult> {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/purchasePaidMedia`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to purchase paid media');
  }

  return data.result as SimPurchasePaidMediaResult;
}

export async function sendInvoice(token: string, payload: SendInvoiceRequest, actorUserId?: number): Promise<Message> {
  return callBotMethod<Message>(token, 'sendInvoice', payload, { actorUserId });
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

type SimReactionPayloadItem =
  | { type: 'emoji'; emoji: string }
  | { type: 'paid' };

export async function setBotMessageReaction(token: string, payload: {
  chat_id: number;
  message_id: number;
  reaction: SimReactionPayloadItem[];
}) {
  return callBotMethod<boolean>(token, 'setMessageReaction', payload as SetMessageReactionRequest);
}

export async function setUserMessageReaction(token: string, payload: {
  chat_id: number;
  message_id: number;
  user_id: number;
  first_name: string;
  username?: string;
  reaction: SimReactionPayloadItem[];
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

export async function getWebhookInfo(token: string, payload: GetWebhookInfoRequest = {}): Promise<WebhookInfo> {
  return callBotMethod<WebhookInfo>(token, 'getWebhookInfo', payload);
}

export async function setWebhook(token: string, payload: {
  url: string;
  secret_token?: string;
  ip_address?: string;
  max_connections?: number;
  allowed_updates?: string[];
  drop_pending_updates?: boolean;
}): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setWebhook', payload);
}

export async function deleteWebhook(token: string, payload: {
  drop_pending_updates?: boolean;
} = {}): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteWebhook', payload);
}

export async function logOut(token: string, payload: LogOutRequest = {}): Promise<boolean> {
  return callBotMethod<boolean>(token, 'logOut', payload);
}

export async function closeBotSession(token: string, payload: CloseRequest = {}): Promise<boolean> {
  return callBotMethod<boolean>(token, 'close', payload);
}

export async function verifyUser(token: string, payload: VerifyUserRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'verifyUser', payload);
}

export async function verifyChat(token: string, payload: VerifyChatRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'verifyChat', payload);
}

export async function removeUserVerification(token: string, payload: RemoveUserVerificationRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'removeUserVerification', payload);
}

export async function removeChatVerification(token: string, payload: RemoveChatVerificationRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'removeChatVerification', payload);
}

export async function answerWebAppQuery(token: string, payload: AnswerWebAppQueryRequest): Promise<SentWebAppMessage> {
  return callBotMethod<SentWebAppMessage>(token, 'answerWebAppQuery', payload);
}

export async function savePreparedInlineMessage(
  token: string,
  payload: SavePreparedInlineMessageRequest,
): Promise<PreparedInlineMessage> {
  return callBotMethod<PreparedInlineMessage>(token, 'savePreparedInlineMessage', payload);
}

export async function savePreparedKeyboardButton(
  token: string,
  payload: SavePreparedKeyboardButtonRequest,
): Promise<PreparedKeyboardButton> {
  return callBotMethod<PreparedKeyboardButton>(token, 'savePreparedKeyboardButton', payload);
}

export async function getManagedBotToken(token: string, payload: GetManagedBotTokenRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'getManagedBotToken', payload);
}

export async function replaceManagedBotToken(token: string, payload: ReplaceManagedBotTokenRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'replaceManagedBotToken', payload);
}

export async function sendChecklist(token: string, payload: SendChecklistRequest, actorUserId?: number): Promise<Message> {
  return callBotMethod<Message>(token, 'sendChecklist', payload, { actorUserId });
}

export async function editMessageChecklist(token: string, payload: EditMessageChecklistRequest, actorUserId?: number): Promise<Message> {
  return callBotMethod<Message>(token, 'editMessageChecklist', payload, { actorUserId });
}

export async function setPassportDataErrors(token: string, payload: SetPassportDataErrorsRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setPassportDataErrors', payload);
}

export async function setUserEmojiStatus(token: string, payload: SetUserEmojiStatusRequest): Promise<boolean> {
  return callBotMethod<boolean>(token, 'setUserEmojiStatus', payload);
}

export async function getUserProfilePhotos(token: string, payload: GetUserProfilePhotosRequest): Promise<UserProfilePhotos> {
  return callBotMethod<UserProfilePhotos>(token, 'getUserProfilePhotos', payload);
}

export async function getUserProfileAudios(token: string, payload: GetUserProfileAudiosRequest): Promise<UserProfileAudios> {
  return callBotMethod<UserProfileAudios>(token, 'getUserProfileAudios', payload);
}

export async function getUserChatBoosts(token: string, payload: GetUserChatBoostsRequest, actorUserId?: number): Promise<UserChatBoosts> {
  return callBotMethod<UserChatBoosts>(token, 'getUserChatBoosts', payload, { actorUserId });
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
  if (typeof payload.messageThreadId === 'number' && Number.isFinite(payload.messageThreadId)) {
    formData.append('message_thread_id', String(Math.trunc(payload.messageThreadId)));
  }
  if (typeof payload.directMessagesTopicId === 'number' && Number.isFinite(payload.directMessagesTopicId) && payload.directMessagesTopicId > 0) {
    formData.append('direct_messages_topic_id', String(Math.trunc(payload.directMessagesTopicId)));
  }
  formData.append('user_id', String(payload.userId));
  formData.append('first_name', payload.firstName);
  if (payload.username) {
    formData.append('username', payload.username);
  }
  if (typeof payload.senderChatId === 'number' && Number.isFinite(payload.senderChatId) && payload.senderChatId > 0) {
    formData.append('sender_chat_id', String(Math.trunc(payload.senderChatId)));
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
      message_thread_id: payload.messageThreadId,
      direct_messages_topic_id: payload.directMessagesTopicId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      sender_chat_id: payload.senderChatId,
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
      message_thread_id: payload.messageThreadId,
      direct_messages_topic_id: payload.directMessagesTopicId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      sender_chat_id: payload.senderChatId,
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
      message_thread_id: payload.messageThreadId,
      direct_messages_topic_id: payload.directMessagesTopicId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      sender_chat_id: payload.senderChatId,
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
      message_thread_id: payload.messageThreadId,
      direct_messages_topic_id: payload.directMessagesTopicId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      sender_chat_id: payload.senderChatId,
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
      message_thread_id: payload.messageThreadId,
      direct_messages_topic_id: payload.directMessagesTopicId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      sender_chat_id: payload.senderChatId,
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
  messageThreadId?: number;
  directMessagesTopicId?: number;
  userId: number;
  firstName: string;
  username?: string;
  senderChatId?: number;
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
      message_thread_id: payload.messageThreadId,
      direct_messages_topic_id: payload.directMessagesTopicId,
      user_id: payload.userId,
      first_name: payload.firstName,
      username: payload.username,
      sender_chat_id: payload.senderChatId,
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
  last_name?: string;
  username?: string;
  phone_number?: string;
  photo_url?: string;
  bio?: string;
  is_premium?: boolean;
  business_name?: string;
  business_intro?: string;
  business_location?: string;
  gift_count?: number;
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

export async function deleteSimUser(payload: {
  id: number;
}) {
  const response = await fetch(`${API_BASE_URL}/client-api/users/delete`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to delete user');
  }

  return data.result as { deleted: boolean; id: number };
}

export async function setSimUserProfileAudio(
  token: string,
  payload: {
    user_id: number;
    title?: string;
    performer?: string;
    file_name?: string;
    mime_type?: string;
    file_size?: number;
    duration?: number;
  },
) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/users/profile-audio`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to set profile audio');
  }

  return data.result as {
    user_id: number;
    file_id: string;
    file_unique_id: string;
    title: string;
    file_name: string;
    mime_type: string;
    file_size?: number;
    performer?: string;
    duration: number;
  };
}

export async function uploadSimUserProfileAudio(
  token: string,
  payload: {
    user_id: number;
    audio: globalThis.File;
    title?: string;
    performer?: string;
    file_name?: string;
    mime_type?: string;
    duration?: number;
  },
  actorUserId?: number,
) {
  const formData = new FormData();
  formData.append('user_id', String(payload.user_id));
  formData.append('audio', payload.audio, payload.audio.name);

  if (payload.title?.trim()) {
    formData.append('title', payload.title.trim());
  }
  if (payload.performer?.trim()) {
    formData.append('performer', payload.performer.trim());
  }
  if (payload.file_name?.trim()) {
    formData.append('file_name', payload.file_name.trim());
  }
  if (payload.mime_type?.trim()) {
    formData.append('mime_type', payload.mime_type.trim());
  }
  if (typeof payload.duration === 'number' && Number.isFinite(payload.duration)) {
    formData.append('duration', String(Math.trunc(payload.duration)));
  }

  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/users/profile-audio/upload`, {
    method: 'POST',
    headers: {
      ...actorHeader(actorUserId),
    },
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to upload profile audio');
  }

  return data.result as {
    user_id: number;
    file_id: string;
    file_unique_id: string;
    file_path: string;
    title: string;
    performer?: string;
    file_name: string;
    mime_type: string;
    file_size?: number;
    duration: number;
  };
}

export async function deleteSimUserProfileAudio(
  token: string,
  payload: {
    user_id: number;
    file_id: string;
  },
) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/users/profile-audio/delete`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to delete profile audio');
  }

  return data.result as {
    deleted: boolean;
    user_id: number;
    file_id: string;
  };
}

export async function addSimUserChatBoosts(
  token: string,
  payload: {
    chat_id: number;
    user_id: number;
    count?: number;
    duration_days?: number;
  },
  actorUserId?: number,
) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/users/chat-boosts/add`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...actorHeader(actorUserId),
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to add chat boosts');
  }

  return data.result as {
    added_count: number;
    boost_ids: string[];
    chat_id: number;
    user_id: number;
  };
}

export async function removeSimUserChatBoosts(
  token: string,
  payload: {
    chat_id: number;
    user_id: number;
    boost_ids?: string[];
    remove_all?: boolean;
  },
  actorUserId?: number,
) {
  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/users/chat-boosts/remove`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...actorHeader(actorUserId),
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to remove chat boosts');
  }

  return data.result as {
    removed_count: number;
    boost_ids: string[];
    chat_id: number;
    user_id: number;
  };
}

export async function clearSimHistory(token: string, chatId: number, messageThreadId?: number) {
  const payload: Record<string, number> = {
    chat_id: chatId,
  };
  if (typeof messageThreadId === 'number' && Number.isFinite(messageThreadId) && messageThreadId > 0) {
    payload.message_thread_id = Math.floor(messageThreadId);
  }

  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/clearHistory`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to clear history');
  }

  return data.result as { deleted_count: number };
}

export async function sendBotMessage(token: string, payload: SendMessageRequest, actorUserId?: number) {
  const normalizedPayload: SendMessageRequest = {
    ...payload,
    text: normalizeTextInput(payload.text),
  };

  return callBotMethod<Message>(token, 'sendMessage', normalizedPayload, { actorUserId });
}

type StoryRequestWithOptionalBusinessConnection<T extends { business_connection_id: string }> =
  Omit<T, 'business_connection_id'> & { business_connection_id?: string };

type PostStoryRequestPayload = StoryRequestWithOptionalBusinessConnection<PostStoryRequest>;
type RepostStoryRequestPayload = StoryRequestWithOptionalBusinessConnection<RepostStoryRequest>;
type EditStoryRequestPayload = StoryRequestWithOptionalBusinessConnection<EditStoryRequest>;
type DeleteStoryRequestPayload = StoryRequestWithOptionalBusinessConnection<DeleteStoryRequest>;

export async function postStory(token: string, payload: PostStoryRequestPayload, actorUserId?: number): Promise<Story> {
  return callBotMethod<Story>(token, 'postStory', payload, { actorUserId });
}

export async function repostStory(token: string, payload: RepostStoryRequestPayload, actorUserId?: number): Promise<Story> {
  return callBotMethod<Story>(token, 'repostStory', payload, { actorUserId });
}

export async function editStory(token: string, payload: EditStoryRequestPayload, actorUserId?: number): Promise<Story> {
  return callBotMethod<Story>(token, 'editStory', payload, { actorUserId });
}

export async function deleteStory(token: string, payload: DeleteStoryRequestPayload, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'deleteStory', payload, { actorUserId });
}

export async function postStoryWithFile(token: string, payload: {
  business_connection_id?: string;
  file: globalThis.File;
  content_type: 'photo' | 'video';
  active_period: number;
  caption?: string;
  areas?: unknown[];
}, actorUserId?: number): Promise<Story> {
  const formData = new FormData();
  const businessConnectionId = payload.business_connection_id?.trim();
  if (businessConnectionId) {
    formData.append('business_connection_id', businessConnectionId);
  }
  formData.append('active_period', String(Math.trunc(payload.active_period)));
  formData.append('story_file', payload.file, payload.file.name);
  formData.append('content', JSON.stringify({
    type: payload.content_type,
    [payload.content_type]: 'attach://story_file',
  }));

  if (payload.caption?.trim()) {
    formData.append('caption', payload.caption.trim());
  }
  if (Array.isArray(payload.areas)) {
    formData.append('areas', JSON.stringify(payload.areas));
  }

  const response = await fetch(`${API_BASE_URL}/bot${encodeURIComponent(token)}/postStory`, {
    method: 'POST',
    headers: actorHeader(actorUserId),
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to post story');
  }

  return data.result as Story;
}

export async function editStoryWithFile(token: string, payload: {
  business_connection_id?: string;
  story_id: number;
  file: globalThis.File;
  content_type: 'photo' | 'video';
  caption?: string;
  areas?: unknown[];
}, actorUserId?: number): Promise<Story> {
  const formData = new FormData();
  const businessConnectionId = payload.business_connection_id?.trim();
  if (businessConnectionId) {
    formData.append('business_connection_id', businessConnectionId);
  }
  formData.append('story_id', String(Math.trunc(payload.story_id)));
  formData.append('story_file', payload.file, payload.file.name);
  formData.append('content', JSON.stringify({
    type: payload.content_type,
    [payload.content_type]: 'attach://story_file',
  }));

  if (payload.caption?.trim()) {
    formData.append('caption', payload.caption.trim());
  }
  if (Array.isArray(payload.areas)) {
    formData.append('areas', JSON.stringify(payload.areas));
  }

  const response = await fetch(`${API_BASE_URL}/bot${encodeURIComponent(token)}/editStory`, {
    method: 'POST',
    headers: actorHeader(actorUserId),
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to edit story');
  }

  return data.result as Story;
}

export async function approveSuggestedPost(token: string, payload: ApproveSuggestedPostRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'approveSuggestedPost', payload, { actorUserId });
}

export async function declineSuggestedPost(token: string, payload: DeclineSuggestedPostRequest, actorUserId?: number): Promise<boolean> {
  return callBotMethod<boolean>(token, 'declineSuggestedPost', payload, { actorUserId });
}

export async function forwardMessage(token: string, payload: ForwardMessageRequest, actorUserId?: number) {
  return callBotMethod<Message>(token, 'forwardMessage', payload, { actorUserId });
}

export async function forwardMessages(token: string, payload: ForwardMessagesRequest, actorUserId?: number) {
  return callBotMethod<Array<{ message_id: number }>>(token, 'forwardMessages', payload, { actorUserId });
}

export async function copyMessage(token: string, payload: CopyMessageRequest, actorUserId?: number) {
  return callBotMethod<{ message_id: number }>(token, 'copyMessage', payload, { actorUserId });
}

export async function copyMessages(token: string, payload: CopyMessagesRequest, actorUserId?: number) {
  return callBotMethod<Array<{ message_id: number }>>(token, 'copyMessages', payload, { actorUserId });
}

export async function editBotMessageText(token: string, payload: EditMessageTextRequest, actorUserId?: number) {
  return callBotMethod(token, 'editMessageText', payload, { actorUserId });
}

export async function editBotMessageCaption(token: string, payload: EditMessageCaptionRequest, actorUserId?: number) {
  return callBotMethod(token, 'editMessageCaption', payload, { actorUserId });
}

export async function editBotMessageMedia(token: string, payload: {
  chat_id: EditMessageMediaRequest['chat_id'];
  message_id: EditMessageMediaRequest['message_id'];
  mediaType: 'photo' | 'video' | 'audio' | 'document';
  file: globalThis.File;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}, actorUserId?: number) {
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

  const headers: Record<string, string> = {};
  if (typeof actorUserId === 'number' && Number.isFinite(actorUserId)) {
    headers['X-Simula-Actor-User-Id'] = String(Math.trunc(actorUserId));
  }

  const response = await fetch(`${API_BASE_URL}/bot${token}/editMessageMedia`, {
    method: 'POST',
    headers,
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
}, actorUserId?: number) {
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

  const headers: Record<string, string> = {};
  if (typeof actorUserId === 'number' && Number.isFinite(actorUserId)) {
    headers['X-Simula-Actor-User-Id'] = String(Math.trunc(actorUserId));
  }

  const response = await fetch(`${API_BASE_URL}/client-api/bot${encodeURIComponent(token)}/editUserMessageMedia`, {
    method: 'POST',
    headers,
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
}, actorUserId?: number) {
  return callBotMethod<boolean>(token, 'deleteMessage', payload, { actorUserId });
}

export async function deleteBotMessages(token: string, payload: {
  chat_id: number;
  message_ids: number[];
}, actorUserId?: number) {
  return callBotMethod<boolean>(token, 'deleteMessages', payload, { actorUserId });
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
  replyToMessageId?: number;
}, actorUserId?: number) {
  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));
  formData.append(payload.field, payload.file, payload.file.name);
  if (typeof payload.replyToMessageId === 'number' && Number.isFinite(payload.replyToMessageId)) {
    formData.append('reply_to_message_id', String(Math.trunc(payload.replyToMessageId)));
  }
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

  const headers: Record<string, string> = {};
  if (typeof actorUserId === 'number' && Number.isFinite(actorUserId)) {
    headers['X-Simula-Actor-User-Id'] = String(Math.trunc(actorUserId));
  }

  const response = await fetch(`${API_BASE_URL}/bot${encodeURIComponent(token)}/${payload.method}`, {
    method: 'POST',
    headers,
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
    const extra = (typeof thumb === 'object' && thumb !== null && 'extra' in thumb)
      ? (thumb as { extra?: unknown }).extra
      : undefined;
    if (thumb instanceof window.File) {
      formData.append('thumbnail', thumb, thumb.name);
    } else if (extra instanceof window.File) {
      formData.append('thumbnail', extra, extra.name);
    } else if (typeof extra === 'string') {
      formData.append('thumbnail', extra);
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

type PaidMediaItem = {
  type: 'photo' | 'video';
  file: globalThis.File;
};

function inferPaidMediaItem(file: globalThis.File): PaidMediaItem | null {
  const mime = file.type.toLowerCase();
  if (mime.startsWith('image/')) {
    return { type: 'photo', file };
  }
  if (mime.startsWith('video/')) {
    return { type: 'video', file };
  }
  return null;
}

export async function sendBotPaidMedia(token: string, payload: {
  chatId: number;
  files: globalThis.File[];
  starCount: number;
  caption?: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  replyToMessageId?: number;
}, actorUserId?: number) {
  if (payload.files.length < 1 || payload.files.length > 10) {
    throw new Error('Paid media requires 1 to 10 files');
  }

  const normalizedStarCount = Math.floor(Number(payload.starCount));
  if (!Number.isFinite(normalizedStarCount) || normalizedStarCount <= 0) {
    throw new Error('Paid media star count must be greater than zero');
  }

  const formData = new FormData();
  formData.append('chat_id', String(payload.chatId));
  formData.append('star_count', String(normalizedStarCount));
  if (typeof payload.replyToMessageId === 'number' && Number.isFinite(payload.replyToMessageId)) {
    formData.append('reply_parameters', JSON.stringify({ message_id: Math.trunc(payload.replyToMessageId) }));
  }
  if (payload.caption?.trim()) {
    formData.append('caption', payload.caption.trim());
    if (payload.parseMode) {
      formData.append('parse_mode', payload.parseMode);
    }
  }

  const media = payload.files.map((file, index) => {
    const mapped = inferPaidMediaItem(file);
    if (!mapped) {
      throw new Error('Paid media supports only photo and video files');
    }

    const attachName = `paid_media_${index}`;
    formData.append(attachName, mapped.file, mapped.file.name);
    return {
      type: mapped.type,
      media: `attach://${attachName}`,
    };
  });

  formData.append('media', JSON.stringify(media));

  const headers: Record<string, string> = {};
  if (typeof actorUserId === 'number' && Number.isFinite(actorUserId)) {
    headers['X-Simula-Actor-User-Id'] = String(Math.trunc(actorUserId));
  }

  const response = await fetch(`${API_BASE_URL}/bot${encodeURIComponent(token)}/sendPaidMedia`, {
    method: 'POST',
    headers,
    body: formData,
  });

  const data = await response.json();
  if (!data.ok) {
    throw new Error(data.description || 'Unable to send paid media');
  }

  return data.result;
}
