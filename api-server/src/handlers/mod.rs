use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::{json, Map, Value};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::database::{
    ensure_bot, ensure_chat, lock_db, push_runtime_request_log, AppState,
    RuntimeRequestLogEntry,
};
use crate::generated::methods::{
    AddStickerToSetRequest, AnswerCallbackQueryRequest, AnswerInlineQueryRequest,
    AnswerWebAppQueryRequest,
    ApproveChatJoinRequestRequest,
    ApproveSuggestedPostRequest,
    AnswerPreCheckoutQueryRequest, AnswerShippingQueryRequest, BanChatMemberRequest,
    BanChatSenderChatRequest, CloseRequest, CreateInvoiceLinkRequest, CreateNewStickerSetRequest,
    CopyMessageRequest, CopyMessagesRequest,
    CreateChatInviteLinkRequest, CreateChatSubscriptionInviteLinkRequest,
    DeleteChatPhotoRequest, DeleteChatStickerSetRequest, DeleteMessageRequest,
    DeleteMessagesRequest, DeleteMyCommandsRequest, DeleteStickerFromSetRequest, DeleteStickerSetRequest,
    DeleteWebhookRequest, EditMessageCaptionRequest, EditMessageChecklistRequest,
    EditMessageLiveLocationRequest,
    EditChatInviteLinkRequest, EditChatSubscriptionInviteLinkRequest,
    DeclineSuggestedPostRequest,
    DeclineChatJoinRequestRequest, ExportChatInviteLinkRequest,
    EditMessageMediaRequest, EditMessageReplyMarkupRequest, EditMessageTextRequest,
    EditUserStarSubscriptionRequest, GetCustomEmojiStickersRequest, GetFileRequest,
    GetChatRequest, GetChatAdministratorsRequest, GetChatMemberCountRequest,
    GetChatMemberRequest, GetChatMenuButtonRequest, GetForumTopicIconStickersRequest,
    GetAvailableGiftsRequest, GetChatGiftsRequest, GetGameHighScoresRequest,
    GetWebhookInfoRequest,
    GetMeRequest, GetMyCommandsRequest, GetMyDefaultAdministratorRightsRequest,
    GetMyDescriptionRequest, GetMyNameRequest, GetMyShortDescriptionRequest,
    GetMyStarBalanceRequest,
    GetManagedBotTokenRequest,
    GetStarTransactionsRequest, GetStickerSetRequest, GetUpdatesRequest, LeaveChatRequest,
    GetUserGiftsRequest, GiftPremiumSubscriptionRequest,
    GetUserProfileAudiosRequest, GetUserProfilePhotosRequest, GetUserChatBoostsRequest,
    ConvertGiftToStarsRequest, UpgradeGiftRequest, TransferGiftRequest,
    GetBusinessConnectionRequest,
    ForwardMessageRequest, ForwardMessagesRequest,
    PinChatMessageRequest, PromoteChatMemberRequest, RefundStarPaymentRequest,
    RemoveChatVerificationRequest,
    RemoveMyProfilePhotoRequest,
    RemoveUserVerificationRequest,
    RevokeChatInviteLinkRequest,
    ReplaceManagedBotTokenRequest,
    ReplaceStickerInSetRequest, RestrictChatMemberRequest, SendAnimationRequest,
    SendAudioRequest, SendContactRequest, SendDiceRequest, SendDocumentRequest,
    SendChatActionRequest, SendChecklistRequest, SendGameRequest, SendInvoiceRequest,
    SendLocationRequest, SendMediaGroupRequest,
    SendPaidMediaRequest,
    SendGiftRequest, SendMessageDraftRequest, SendMessageRequest, SendPhotoRequest,
    SendPollRequest, SendStickerRequest,
    SavePreparedInlineMessageRequest, SavePreparedKeyboardButtonRequest,
    SendVenueRequest, SendVideoNoteRequest, SendVideoRequest, SendVoiceRequest,
    SetChatAdministratorCustomTitleRequest, SetChatDescriptionRequest,
    SetChatMemberTagRequest, SetChatPermissionsRequest, SetChatStickerSetRequest,
    SetChatMenuButtonRequest, SetChatTitleRequest, SetCustomEmojiStickerSetThumbnailRequest, SetGameScoreRequest,
    SetMessageReactionRequest, SetStickerEmojiListRequest, SetStickerKeywordsRequest,
    SetMyCommandsRequest, SetMyDefaultAdministratorRightsRequest,
    SetPassportDataErrorsRequest,
    SetMyDescriptionRequest, SetMyNameRequest, SetMyProfilePhotoRequest,
    SetMyShortDescriptionRequest,
    SetUserEmojiStatusRequest,
    SetBusinessAccountNameRequest, SetBusinessAccountUsernameRequest,
    SetBusinessAccountBioRequest, SetBusinessAccountProfilePhotoRequest,
    RemoveBusinessAccountProfilePhotoRequest, ReadBusinessMessageRequest,
    DeleteBusinessMessagesRequest, SetBusinessAccountGiftSettingsRequest,
    GetBusinessAccountStarBalanceRequest, TransferBusinessAccountStarsRequest,
    GetBusinessAccountGiftsRequest,
    SetStickerMaskPositionRequest, SetStickerPositionInSetRequest,
    SetStickerSetThumbnailRequest, SetStickerSetTitleRequest, SetWebhookRequest,
    VerifyChatRequest, VerifyUserRequest,
    LogOutRequest,
    StopMessageLiveLocationRequest, StopPollRequest, UnbanChatMemberRequest,
    UnbanChatSenderChatRequest, UnpinAllChatMessagesRequest, UnpinChatMessageRequest,
    CreateForumTopicRequest, EditForumTopicRequest, CloseForumTopicRequest,
    ReopenForumTopicRequest, DeleteForumTopicRequest, UnpinAllForumTopicMessagesRequest,
    EditGeneralForumTopicRequest, CloseGeneralForumTopicRequest,
    ReopenGeneralForumTopicRequest, HideGeneralForumTopicRequest,
    UnhideGeneralForumTopicRequest, UnpinAllGeneralForumTopicMessagesRequest,
};
use crate::generated::types::{AcceptedGiftTypes, Animation, Audio, BotCommand, BotCommandScope, BotDescription, BotName, BotShortDescription, BusinessBotRights, BusinessConnection, BusinessMessagesDeleted, CallbackQuery, Chat, ChatAdministratorRights, ChatBoost, ChatBoostSource, ChatFullInfo, ChatInviteLink, ChatJoinRequest, ChatMember, ChatMemberAdministrator, ChatMemberBanned, ChatMemberLeft, ChatMemberMember, ChatMemberOwner, ChatMemberRestricted, ChatMemberUpdated, ChatPermissions, ChatPhoto, ChatShared, Checklist, ChecklistTask, ChosenInlineResult, Contact, Dice, DirectMessagesTopic, Document, File, ForumTopic, Game, GameHighScore, Gift, GiftBackground, Gifts, InlineKeyboardMarkup, InlineQuery, InputChecklist, InputChecklistTask, InputSticker, Invoice, KeyboardButtonRequestManagedBot, Location, ManagedBotCreated, ManagedBotUpdated, MaskPosition, MaybeInaccessibleMessage, MenuButton, Message, MessageEntity, MessageReactionCountUpdated, MessageReactionUpdated, OrderInfo, OwnedGift, OwnedGifts, PaidMediaPurchased, PhotoSize, Poll, PollAnswer, PollOption, PreCheckoutQuery, PreparedInlineMessage, PreparedKeyboardButton, ReactionCount, ReactionType, ReplyKeyboardMarkup, ReplyKeyboardRemove, SentWebAppMessage, ShippingAddress, ShippingQuery, StarAmount, Sticker, StickerSet, StoryArea, SuggestedPostInfo, SuggestedPostParameters, SuggestedPostPrice, SuccessfulPayment, Update, User, UserChatBoosts, UserProfileAudios, UserProfilePhotos, UsersShared, Venue, Video, VideoNote, Voice, WebAppData, WebhookInfo};
use crate::types::{strip_nulls, ApiError, ApiResult};

thread_local! {
    static REQUEST_ACTOR_USER_ID: RefCell<Option<i64>> = RefCell::new(None);
}

pub fn with_request_actor_user_id<T>(actor_user_id: Option<i64>, action: impl FnOnce() -> T) -> T {
    REQUEST_ACTOR_USER_ID.with(|slot| {
        let previous = slot.replace(actor_user_id);
        let result = action();
        slot.replace(previous);
        result
    })
}

fn current_request_actor_user_id() -> Option<i64> {
    REQUEST_ACTOR_USER_ID.with(|slot| *slot.borrow())
}

#[derive(Deserialize)]
pub struct SimSendUserMessageRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub business_connection_id: Option<String>,
    pub text: String,
    pub parse_mode: Option<String>,
    pub suggested_post_parameters: Option<SuggestedPostParameters>,
    pub reply_to_message_id: Option<i64>,
    pub users_shared: Option<UsersShared>,
    pub chat_shared: Option<ChatShared>,
    pub web_app_data: Option<WebAppData>,
    pub managed_bot_request: Option<KeyboardButtonRequestManagedBot>,
}

#[derive(Deserialize)]
pub struct SimSendUserMediaRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub business_connection_id: Option<String>,
    pub media_kind: String,
    pub media: Value,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimEditUserMediaRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub media_kind: String,
    pub media: Value,
    pub caption: Option<Value>,
    pub parse_mode: Option<String>,
}

#[derive(Deserialize)]
pub struct SimCreateBotRequest {
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpdateBotRequest {
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimSetPrivacyModeRequest {
    pub enabled: bool,
}

#[derive(Deserialize)]
pub struct SimSetBusinessConnectionRequest {
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub business_connection_id: Option<String>,
    pub enabled: Option<bool>,
    pub rights: Option<BusinessBotRights>,
}

#[derive(Deserialize)]
pub struct SimRemoveBusinessConnectionRequest {
    pub user_id: Option<i64>,
    pub business_connection_id: Option<String>,
}

#[derive(Deserialize)]
pub struct SimOpenChannelDirectMessagesRequest {
    pub channel_chat_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpsertUserRequest {
    pub id: Option<i64>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone_number: Option<String>,
    pub photo_url: Option<String>,
    pub bio: Option<String>,
    pub is_premium: Option<bool>,
    pub business_name: Option<String>,
    pub business_intro: Option<String>,
    pub business_location: Option<String>,
    pub gift_count: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimDeleteUserRequest {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct SimSetUserProfileAudioRequest {
    pub user_id: i64,
    pub title: Option<String>,
    pub performer: Option<String>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<i64>,
    pub duration: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimDeleteUserProfileAudioRequest {
    pub user_id: i64,
    pub file_id: String,
}

#[derive(Deserialize)]
pub struct SimUploadUserProfileAudioRequest {
    pub user_id: i64,
    pub audio: Value,
    pub title: Option<String>,
    pub performer: Option<String>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub duration: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimAddUserChatBoostsRequest {
    pub chat_id: i64,
    pub user_id: i64,
    pub count: Option<i64>,
    pub duration_days: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimRemoveUserChatBoostsRequest {
    pub chat_id: i64,
    pub user_id: i64,
    pub boost_ids: Option<Vec<String>>,
    pub remove_all: Option<bool>,
}

#[derive(Deserialize)]
pub struct SimCreateGroupRequest {
    pub title: String,
    pub chat_type: Option<String>,
    pub owner_user_id: Option<i64>,
    pub owner_first_name: Option<String>,
    pub owner_username: Option<String>,
    pub initial_member_ids: Option<Vec<i64>>,
    pub username: Option<String>,
    pub description: Option<String>,
    pub is_forum: Option<bool>,
    pub show_author_signature: Option<bool>,
    pub message_history_visible: Option<bool>,
    pub slow_mode_delay: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimJoinGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimMarkChannelMessageViewRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimLeaveGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpdateGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
    pub title: Option<String>,
    pub username: Option<String>,
    pub description: Option<String>,
    pub is_forum: Option<bool>,
    pub show_author_signature: Option<bool>,
    pub paid_star_reactions_enabled: Option<bool>,
    pub linked_chat_id: Option<i64>,
    pub direct_messages_enabled: Option<bool>,
    pub direct_messages_star_count: Option<i64>,
    pub message_history_visible: Option<bool>,
    pub slow_mode_delay: Option<i64>,
    pub permissions: Option<ChatPermissions>,
}

#[derive(Deserialize)]
pub struct SimDeleteGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimCreateGroupInviteLinkRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
    pub creates_join_request: Option<bool>,
    pub name: Option<String>,
    pub expire_date: Option<i64>,
    pub member_limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimJoinGroupByInviteLinkRequest {
    pub invite_link: String,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimResolveJoinRequestRequest {
    pub chat_id: i64,
    pub user_id: i64,
    pub actor_user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimSetBotGroupMembershipRequest {
    pub chat_id: i64,
    pub actor_user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
    pub status: String,
}

#[derive(Deserialize)]
pub struct SimClearHistoryRequest {
    pub chat_id: i64,
    pub message_thread_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSetUserReactionRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub reaction: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub struct SimPressInlineButtonRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub callback_data: String,
}

#[derive(Deserialize)]
pub struct SimSendInlineQueryRequest {
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub query: String,
    pub offset: Option<String>,
}

#[derive(Deserialize)]
pub struct SimChooseInlineResultRequest {
    pub inline_query_id: String,
    pub result_id: String,
}

#[derive(Deserialize)]
pub struct SimVotePollRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub option_ids: Vec<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserDiceRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub emoji: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserGameRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub game_short_name: String,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserContactRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub phone_number: String,
    pub contact_first_name: String,
    pub contact_last_name: Option<String>,
    pub vcard: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserLocationRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub latitude: f64,
    pub longitude: f64,
    pub horizontal_accuracy: Option<f64>,
    pub live_period: Option<i64>,
    pub heading: Option<i64>,
    pub proximity_alert_radius: Option<i64>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserVenueRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub latitude: f64,
    pub longitude: f64,
    pub title: String,
    pub address: String,
    pub foursquare_id: Option<String>,
    pub foursquare_type: Option<String>,
    pub google_place_id: Option<String>,
    pub google_place_type: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimPayInvoiceRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub payment_method: Option<String>,
    pub outcome: Option<String>,
    pub tip_amount: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimPurchasePaidMediaRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub paid_media_payload: Option<String>,
}

pub fn handle_sim_get_poll_voters(
    state: &Data<AppState>,
    token: &str,
    chat_id: i64,
    message_id: i64,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;
    let chat_key = chat_id.to_string();

    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT id, is_anonymous FROM polls WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, is_anonymous)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    if is_anonymous == 1 {
        return Ok(json!({
            "poll_id": poll_id,
            "anonymous": true,
            "voters": [],
        }));
    }

    let mut stmt = conn
        .prepare(
            "SELECT v.voter_user_id, u.first_name, u.username, v.option_ids_json
             FROM poll_votes v
             LEFT JOIN users u ON u.id = v.voter_user_id
             WHERE v.poll_id = ?1
             ORDER BY v.updated_at ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![poll_id.clone()], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut voters = Vec::new();
    for row in rows {
        let (user_id, first_name, username, option_ids_json) = row.map_err(ApiError::internal)?;
        let option_ids: Vec<i64> = serde_json::from_str(&option_ids_json).unwrap_or_default();
        voters.push(json!({
            "user_id": user_id,
            "first_name": first_name.unwrap_or_else(|| "User".to_string()),
            "username": username,
            "option_ids": option_ids,
        }));
    }

    Ok(json!({
        "poll_id": poll_id,
        "anonymous": false,
        "voters": voters,
    }))
}


pub fn dispatch_method(
    state: &Data<AppState>,
    token: &str,
    method: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    match method.to_ascii_lowercase().as_str() {
        "getme" => handle_get_me(state, token, &params),
        "sendmessage" => handle_send_message(state, token, &params),
        "forwardmessage" => handle_forward_message(state, token, &params),
        "forwardmessages" => handle_forward_messages(state, token, &params),
        "copymessage" => handle_copy_message(state, token, &params),
        "copymessages" => handle_copy_messages(state, token, &params),
        "sendphoto" => handle_send_photo(state, token, &params),
        "sendaudio" => handle_send_audio(state, token, &params),
        "senddocument" => handle_send_document(state, token, &params),
        "sendvideo" => handle_send_video(state, token, &params),
        "sendvoice" => handle_send_voice(state, token, &params),
        "sendcontact" => handle_send_contact(state, token, &params),
        "sendlocation" => handle_send_location(state, token, &params),
        "sendvenue" => handle_send_venue(state, token, &params),
        "sendchataction" => handle_send_chat_action(state, token, &params),
        "senddice" => handle_send_dice(state, token, &params),
        "sendgame" => handle_send_game(state, token, &params),
        "sendanimation" => handle_send_animation(state, token, &params),
        "sendvideonote" => handle_send_video_note(state, token, &params),
        "sendsticker" => handle_send_sticker(state, token, &params),
        "sendpoll" => handle_send_poll(state, token, &params),
        "sendchecklist" => handle_send_checklist(state, token, &params),
        "sendinvoice" => handle_send_invoice(state, token, &params),
        "sendpaidmedia" => handle_send_paid_media(state, token, &params),
        "sendmediagroup" => handle_send_media_group(state, token, &params),
        "sendmessagedraft" => handle_send_message_draft(state, token, &params),
        "editmessagetext" => handle_edit_message_text(state, token, &params),
        "editmessagecaption" => handle_edit_message_caption(state, token, &params),
        "editmessagemedia" => handle_edit_message_media(state, token, &params),
        "editmessagelivelocation" => handle_edit_message_live_location(state, token, &params),
        "stopmessagelivelocation" => handle_stop_message_live_location(state, token, &params),
        "editmessagechecklist" => handle_edit_message_checklist(state, token, &params),
        "editmessagereplymarkup" => handle_edit_message_reply_markup(state, token, &params),
        "deletemessage" => handle_delete_message(state, token, &params),
        "deletemessages" => handle_delete_messages(state, token, &params),
        "banchatmember" => handle_ban_chat_member(state, token, &params),
        "unbanchatmember" => handle_unban_chat_member(state, token, &params),
        "restrictchatmember" => handle_restrict_chat_member(state, token, &params),
        "promotechatmember" => handle_promote_chat_member(state, token, &params),
        "setchatadministratorcustomtitle" => handle_set_chat_administrator_custom_title(state, token, &params),
        "setchatmembertag" => handle_set_chat_member_tag(state, token, &params),
        "banchatsenderchat" => handle_ban_chat_sender_chat(state, token, &params),
        "unbanchatsenderchat" => handle_unban_chat_sender_chat(state, token, &params),
        "setchatpermissions" => handle_set_chat_permissions(state, token, &params),
        "exportchatinvitelink" => handle_export_chat_invite_link(state, token, &params),
        "createchatinvitelink" => handle_create_chat_invite_link(state, token, &params),
        "editchatinvitelink" => handle_edit_chat_invite_link(state, token, &params),
        "revokechatinvitelink" => handle_revoke_chat_invite_link(state, token, &params),
        "createchatsubscriptioninvitelink" => handle_create_chat_subscription_invite_link(state, token, &params),
        "editchatsubscriptioninvitelink" => handle_edit_chat_subscription_invite_link(state, token, &params),
        "approvechatjoinrequest" => handle_approve_chat_join_request(state, token, &params),
        "declinechatjoinrequest" => handle_decline_chat_join_request(state, token, &params),
        "getchat" => handle_get_chat(state, token, &params),
        "getchatadministrators" => handle_get_chat_administrators(state, token, &params),
        "getchatmembercount" => handle_get_chat_member_count(state, token, &params),
        "getchatmember" => handle_get_chat_member(state, token, &params),
        "getbusinessconnection" => handle_get_business_connection(state, token, &params),
        "getmanagedbottoken" => handle_get_managed_bot_token(state, token, &params),
        "replacemanagedbottoken" => handle_replace_managed_bot_token(state, token, &params),
        "getuserchatboosts" => handle_get_user_chat_boosts(state, token, &params),
        "setchatmenubutton" => handle_set_chat_menu_button(state, token, &params),
        "getchatmenubutton" => handle_get_chat_menu_button(state, token, &params),
        "setchatphoto" => handle_set_chat_photo(state, token, &params),
        "deletechatphoto" => handle_delete_chat_photo(state, token, &params),
        "setchattitle" => handle_set_chat_title(state, token, &params),
        "setchatdescription" => handle_set_chat_description(state, token, &params),
        "setchatstickerset" => handle_set_chat_sticker_set(state, token, &params),
        "deletechatstickerset" => handle_delete_chat_sticker_set(state, token, &params),
        "getforumtopiciconstickers" => handle_get_forum_topic_icon_stickers(state, token, &params),
        "createforumtopic" => handle_create_forum_topic(state, token, &params),
        "editforumtopic" => handle_edit_forum_topic(state, token, &params),
        "closeforumtopic" => handle_close_forum_topic(state, token, &params),
        "reopenforumtopic" => handle_reopen_forum_topic(state, token, &params),
        "deleteforumtopic" => handle_delete_forum_topic(state, token, &params),
        "unpinallforumtopicmessages" => {
            handle_unpin_all_forum_topic_messages(state, token, &params)
        }
        "editgeneralforumtopic" => handle_edit_general_forum_topic(state, token, &params),
        "closegeneralforumtopic" => handle_close_general_forum_topic(state, token, &params),
        "reopengeneralforumtopic" => handle_reopen_general_forum_topic(state, token, &params),
        "hidegeneralforumtopic" => handle_hide_general_forum_topic(state, token, &params),
        "unhidegeneralforumtopic" => handle_unhide_general_forum_topic(state, token, &params),
        "unpinallgeneralforumtopicmessages" => {
            handle_unpin_all_general_forum_topic_messages(state, token, &params)
        }
        "pinchatmessage" => handle_pin_chat_message(state, token, &params),
        "unpinchatmessage" => handle_unpin_chat_message(state, token, &params),
        "unpinallchatmessages" => handle_unpin_all_chat_messages(state, token, &params),
        "leavechat" => handle_leave_chat(state, token, &params),
        "getfile" => handle_get_file(state, token, &params),
        "getuserprofilephotos" => handle_get_user_profile_photos(state, token, &params),
        "getuserprofileaudios" => handle_get_user_profile_audios(state, token, &params),
        "setuseremojistatus" => handle_set_user_emoji_status(state, token, &params),
        "getupdates" => handle_get_updates(state, token, &params),
        "getwebhookinfo" => handle_get_webhook_info(state, token, &params),
        "setwebhook" => handle_set_webhook(state, token, &params),
        "deletewebhook" => handle_delete_webhook(state, token, &params),
        "logout" => handle_log_out(state, token, &params),
        "close" => handle_close(state, token, &params),
        "setmessagereaction" => handle_set_message_reaction(state, token, &params),
        "stoppoll" => handle_stop_poll(state, token, &params),
        "approvesuggestedpost" => handle_approve_suggested_post(state, token, &params),
        "declinesuggestedpost" => handle_decline_suggested_post(state, token, &params),
        "answercallbackquery" => handle_answer_callback_query(state, token, &params),
        "answerwebappquery" => handle_answer_web_app_query(state, token, &params),
        "answerinlinequery" => handle_answer_inline_query(state, token, &params),
        "answershippingquery" => handle_answer_shipping_query(state, token, &params),
        "answerprecheckoutquery" => handle_answer_pre_checkout_query(state, token, &params),
        "createinvoicelink" => handle_create_invoice_link(state, token, &params),
        "getmystarbalance" => handle_get_my_star_balance(state, token, &params),
        "getstartransactions" => handle_get_star_transactions(state, token, &params),
        "setmycommands" => handle_set_my_commands(state, token, &params),
        "getmycommands" => handle_get_my_commands(state, token, &params),
        "deletemycommands" => handle_delete_my_commands(state, token, &params),
        "setmyname" => handle_set_my_name(state, token, &params),
        "getmyname" => handle_get_my_name(state, token, &params),
        "setmydescription" => handle_set_my_description(state, token, &params),
        "getmydescription" => handle_get_my_description(state, token, &params),
        "setmyshortdescription" => handle_set_my_short_description(state, token, &params),
        "getmyshortdescription" => handle_get_my_short_description(state, token, &params),
        "setmyprofilephoto" => handle_set_my_profile_photo(state, token, &params),
        "removemyprofilephoto" => handle_remove_my_profile_photo(state, token, &params),
        "setmydefaultadministratorrights" => {
            handle_set_my_default_administrator_rights(state, token, &params)
        }
        "getmydefaultadministratorrights" => {
            handle_get_my_default_administrator_rights(state, token, &params)
        }
        "refundstarpayment" => handle_refund_star_payment(state, token, &params),
        "edituserstarsubscription" => handle_edit_user_star_subscription(state, token, &params),
        "savepreparedinlinemessage" => handle_save_prepared_inline_message(state, token, &params),
        "savepreparedkeyboardbutton" => handle_save_prepared_keyboard_button(state, token, &params),
        "setpassportdataerrors" => handle_set_passport_data_errors(state, token, &params),
        "verifyuser" => handle_verify_user(state, token, &params),
        "verifychat" => handle_verify_chat(state, token, &params),
        "removeuserverification" => handle_remove_user_verification(state, token, &params),
        "removechatverification" => handle_remove_chat_verification(state, token, &params),
        "getstickerset" => handle_get_sticker_set(state, token, &params),
        "getcustomemojistickers" => handle_get_custom_emoji_stickers(state, token, &params),
        "uploadstickerfile" => handle_upload_sticker_file(state, token, &params),
        "createnewstickerset" => handle_create_new_sticker_set(state, token, &params),
        "addstickertoset" => handle_add_sticker_to_set(state, token, &params),
        "setstickerpositioninset" => handle_set_sticker_position_in_set(state, token, &params),
        "deletestickerfromset" => handle_delete_sticker_from_set(state, token, &params),
        "replacestickerinset" => handle_replace_sticker_in_set(state, token, &params),
        "setgamescore" => handle_set_game_score(state, token, &params),
        "getgamehighscores" => handle_get_game_high_scores(state, token, &params),
        "readbusinessmessage" => handle_read_business_message(state, token, &params),
        "deletebusinessmessages" => handle_delete_business_messages(state, token, &params),
        "setbusinessaccountname" => handle_set_business_account_name(state, token, &params),
        "setbusinessaccountusername" => handle_set_business_account_username(state, token, &params),
        "setbusinessaccountbio" => handle_set_business_account_bio(state, token, &params),
        "setbusinessaccountprofilephoto" => handle_set_business_account_profile_photo(state, token, &params),
        "removebusinessaccountprofilephoto" => {
            handle_remove_business_account_profile_photo(state, token, &params)
        }
        "setbusinessaccountgiftsettings" => {
            handle_set_business_account_gift_settings(state, token, &params)
        }
        "getbusinessaccountstarbalance" => {
            handle_get_business_account_star_balance(state, token, &params)
        }
        "transferbusinessaccountstars" => {
            handle_transfer_business_account_stars(state, token, &params)
        }
        "getbusinessaccountgifts" => handle_get_business_account_gifts(state, token, &params),
        "getavailablegifts" => handle_get_available_gifts(state, token, &params),
        "sendgift" => handle_send_gift(state, token, &params),
        "giftpremiumsubscription" => handle_gift_premium_subscription(state, token, &params),
        "getusergifts" => handle_get_user_gifts(state, token, &params),
        "getchatgifts" => handle_get_chat_gifts(state, token, &params),
        "convertgifttostars" => handle_convert_gift_to_stars(state, token, &params),
        "upgradegift" => handle_upgrade_gift(state, token, &params),
        "transfergift" => handle_transfer_gift(state, token, &params),
        "poststory" => handle_post_story(state, token, &params),
        "repoststory" => handle_repost_story(state, token, &params),
        "editstory" => handle_edit_story(state, token, &params),
        "deletestory" => handle_delete_story(state, token, &params),
        "deleteownedgift" => handle_delete_owned_gift(state, token, &params),
        "setstickeremojilist" => handle_set_sticker_emoji_list(state, token, &params),
        "setstickerkeywords" => handle_set_sticker_keywords(state, token, &params),
        "setstickermaskposition" => handle_set_sticker_mask_position(state, token, &params),
        "setstickersettitle" => handle_set_sticker_set_title(state, token, &params),
        "setstickersetthumbnail" => handle_set_sticker_set_thumbnail(state, token, &params),
        "setcustomemojistickersetthumbnail" => handle_set_custom_emoji_sticker_set_thumbnail(state, token, &params),
        "deletestickerset" => handle_delete_sticker_set(state, token, &params),
        _ => Err(ApiError::not_found(format!("method {} not found", method))),
    }
}

fn handle_ban_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: BanChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support tags"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't ban itself"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let old_status = old_record
        .as_ref()
        .map(|record| record.status.clone())
        .unwrap_or_else(|| "left".to_string());

    if old_status == "owner" {
        return Err(ApiError::bad_request("can't ban chat owner"));
    }
    if old_status == "banned" {
        return Ok(json!(true));
    }

    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;
    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        "banned",
        "banned",
        None,
        None,
        request.until_date,
        None,
        None,
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        &old_status,
        "banned",
        old_record.as_ref(),
        new_record.as_ref(),
        now,
    )?;

    if is_active_chat_member_status(old_status.as_str()) {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "left_chat_member".to_string(),
            serde_json::to_value(target.clone()).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            service_text_left_chat_member(&actor, &target),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

fn handle_unban_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnbanChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support tags"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't unban itself"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let Some(old_record) = old_record else {
        return Ok(json!(true));
    };
    let old_status = old_record.status.clone();

    if old_status != "banned" {
        if request.only_if_banned.unwrap_or(false) {
            return Ok(json!(true));
        }
        return Ok(json!(true));
    }

    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;
    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        "left",
        "left",
        None,
        None,
        None,
        None,
        None,
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        "banned",
        "left",
        Some(&old_record),
        new_record.as_ref(),
        now,
    )?;

    Ok(json!(true))
}

fn handle_restrict_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RestrictChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support restrictions"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't restrict itself"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let old_status = old_record
        .as_ref()
        .map(|record| record.status.clone())
        .unwrap_or_else(|| "left".to_string());

    if old_status == "owner" {
        return Err(ApiError::bad_request("can't restrict chat owner"));
    }
    if old_status == "banned" {
        return Err(ApiError::bad_request("can't restrict banned user"));
    }

    let now = Utc::now().timestamp();
    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;

    let permissions = request.permissions;
    let full_permissions = permission_enabled(permissions.can_send_messages, false)
        && permission_enabled(permissions.can_send_audios, false)
        && permission_enabled(permissions.can_send_documents, false)
        && permission_enabled(permissions.can_send_photos, false)
        && permission_enabled(permissions.can_send_videos, false)
        && permission_enabled(permissions.can_send_video_notes, false)
        && permission_enabled(permissions.can_send_voice_notes, false)
        && permission_enabled(permissions.can_send_polls, false)
        && permission_enabled(permissions.can_send_other_messages, false)
        && permission_enabled(permissions.can_add_web_page_previews, false)
        && permission_enabled(permissions.can_invite_users, false)
        && permission_enabled(permissions.can_change_info, false)
        && permission_enabled(permissions.can_pin_messages, false)
        && permission_enabled(permissions.can_manage_topics, false);

    let restriction_expired = request.until_date.map(|until| until > 0 && until <= now).unwrap_or(false);
    let next_status = if full_permissions && restriction_expired {
        "member"
    } else if full_permissions && request.until_date.is_none() {
        "member"
    } else {
        "restricted"
    };

    let tag = old_record.as_ref().and_then(|record| record.tag.clone());
    let permissions_json = if next_status == "restricted" {
        Some(serde_json::to_string(&permissions).map_err(ApiError::internal)?)
    } else {
        None
    };

    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        next_status,
        next_status,
        old_record
            .as_ref()
            .and_then(|record| record.joined_at)
            .or(Some(now)),
        permissions_json.as_deref(),
        if next_status == "restricted" {
            request.until_date
        } else {
            None
        },
        None,
        tag.as_deref(),
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        &old_status,
        next_status,
        old_record.as_ref(),
        new_record.as_ref(),
        now,
    )?;

    Ok(json!(true))
}

fn handle_promote_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: PromoteChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let is_channel_chat = sim_chat.chat_type == "channel";
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't change its own admin role with promoteChatMember"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let old_status = old_record
        .as_ref()
        .map(|record| record.status.clone())
        .unwrap_or_else(|| "left".to_string());
    if old_status == "owner" {
        return Err(ApiError::bad_request("can't change chat owner role"));
    }

    let should_promote = request.is_anonymous.unwrap_or(false)
        || request.can_manage_chat.unwrap_or(false)
        || request.can_delete_messages.unwrap_or(false)
        || request.can_manage_video_chats.unwrap_or(false)
        || request.can_restrict_members.unwrap_or(false)
        || request.can_promote_members.unwrap_or(false)
        || request.can_change_info.unwrap_or(false)
        || request.can_invite_users.unwrap_or(false)
        || request.can_post_stories.unwrap_or(false)
        || request.can_edit_stories.unwrap_or(false)
        || request.can_delete_stories.unwrap_or(false)
        || request.can_post_messages.unwrap_or(false)
        || request.can_edit_messages.unwrap_or(false)
        || request.can_pin_messages.unwrap_or(false)
        || request.can_manage_topics.unwrap_or(false)
        || request.can_manage_direct_messages.unwrap_or(false)
        || request.can_manage_tags.unwrap_or(false);

    let new_status = if should_promote { "admin" } else { "member" };
    let desired_channel_admin_rights = if is_channel_chat && new_status == "admin" {
        Some(channel_admin_rights_from_promote_request(&request))
    } else {
        None
    };
    let channel_rights_changed = if is_channel_chat && old_status == "admin" && new_status == "admin" {
        let existing_rights = old_record
            .as_ref()
            .map(|record| parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
            .unwrap_or_else(channel_admin_rights_full_access);
        let desired_rights = desired_channel_admin_rights
            .clone()
            .unwrap_or_else(|| existing_rights.clone());
        existing_rights != desired_rights
    } else {
        false
    };

    if old_status == new_status && !channel_rights_changed {
        return Ok(json!(true));
    }

    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;
    let now = Utc::now().timestamp();
    let joined_at = old_record
        .as_ref()
        .and_then(|record| record.joined_at)
        .or(Some(now));

    let custom_title = if is_channel_chat {
        None
    } else if new_status == "admin" {
        old_record
            .as_ref()
            .and_then(|record| record.custom_title.as_deref())
    } else {
        None
    };
    let tag = if is_channel_chat {
        None
    } else {
        old_record.as_ref().and_then(|record| record.tag.as_deref())
    };

    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        new_status,
        new_status,
        joined_at,
        None,
        None,
        custom_title,
        tag,
        now,
    )?;

    if is_channel_chat {
        let admin_rights_json = if new_status == "admin" {
            let rights = desired_channel_admin_rights
                .clone()
                .or_else(|| {
                    old_record
                        .as_ref()
                        .map(|record| parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
                })
                .unwrap_or_else(channel_admin_rights_full_access);
            Some(serde_json::to_string(&rights).map_err(ApiError::internal)?)
        } else {
            None
        };
        conn.execute(
            "UPDATE sim_chat_members
             SET admin_rights_json = ?1
             WHERE bot_id = ?2 AND chat_key = ?3 AND user_id = ?4",
            params![admin_rights_json, bot.id, &chat_key, request.user_id],
        )
        .map_err(ApiError::internal)?;
    }

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        &old_status,
        new_status,
        old_record.as_ref(),
        new_record.as_ref(),
        now,
    )?;

    Ok(json!(true))
}

fn handle_set_chat_administrator_custom_title(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatAdministratorCustomTitleRequest = parse_request(params)?;
    let custom_title = request.custom_title.trim().to_string();
    if custom_title.is_empty() {
        return Err(ApiError::bad_request("custom_title is empty"));
    }
    if custom_title.chars().count() > 16 {
        return Err(ApiError::bad_request("custom_title is too long"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support custom titles"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let Some(old_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::not_found("chat member not found"));
    };
    if old_record.status != "admin" {
        return Err(ApiError::bad_request("user is not an administrator"));
    }
    if old_record.custom_title.as_deref() == Some(custom_title.as_str()) {
        return Ok(json!(true));
    }

    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        "admin",
        "admin",
        old_record.joined_at,
        old_record.permissions_json.as_deref(),
        old_record.until_date,
        Some(custom_title.as_str()),
        old_record.tag.as_deref(),
        now,
    )?;

    let Some(new_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::internal("failed to load updated chat member"));
    };

    let target_user = ensure_sim_user_record(&mut conn, request.user_id)?;
    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_user, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        "admin",
        "admin",
        Some(&old_record),
        Some(&new_record),
        now,
    )?;

    Ok(json!(true))
}

fn handle_set_chat_member_tag(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatMemberTagRequest = parse_request(params)?;
    let normalized_tag = request
        .tag
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(tag) = normalized_tag.as_ref() {
        if tag.chars().count() > 32 {
            return Err(ApiError::bad_request("tag is too long"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support tags"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let Some(old_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::not_found("chat member not found"));
    };
    if !matches!(old_record.status.as_str(), "member" | "restricted") {
        return Err(ApiError::bad_request("tag can only be set for regular or restricted members"));
    }
    if old_record.tag == normalized_tag {
        return Ok(json!(true));
    }

    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        old_record.status.as_str(),
        old_record.role.as_str(),
        old_record.joined_at,
        old_record.permissions_json.as_deref(),
        old_record.until_date,
        old_record.custom_title.as_deref(),
        normalized_tag.as_deref(),
        now,
    )?;

    let Some(new_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::internal("failed to load updated chat member"));
    };

    let target_user = ensure_sim_user_record(&mut conn, request.user_id)?;
    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_user, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        old_record.status.as_str(),
        new_record.status.as_str(),
        Some(&old_record),
        Some(&new_record),
        now,
    )?;

    Ok(json!(true))
}

fn handle_ban_chat_sender_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: BanChatSenderChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_can_manage_sender_chat_in_linked_context(
        &mut conn,
        &bot,
        &chat_key,
        &sim_chat,
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_banned_sender_chats (bot_id, chat_key, sender_chat_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(bot_id, chat_key, sender_chat_id)
         DO UPDATE SET updated_at = excluded.updated_at",
        params![bot.id, &chat_key, request.sender_chat_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_unban_chat_sender_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnbanChatSenderChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_can_manage_sender_chat_in_linked_context(
        &mut conn,
        &bot,
        &chat_key,
        &sim_chat,
    )?;

    conn.execute(
        "DELETE FROM sim_banned_sender_chats
         WHERE bot_id = ?1 AND chat_key = ?2 AND sender_chat_id = ?3",
        params![bot.id, &chat_key, request.sender_chat_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_chat_permissions(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatPermissionsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let permissions_json = serde_json::to_string(&request.permissions).map_err(ApiError::internal)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "UPDATE sim_chats
         SET permissions_json = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![permissions_json, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_chat_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let chat_id = params
        .get("chat_id")
        .ok_or_else(|| ApiError::bad_request("missing field `chat_id`"))?;
    let photo = params
        .get("photo")
        .ok_or_else(|| ApiError::bad_request("missing field `photo`"))?;
    let normalized_photo = parse_input_file_value(photo, "photo")?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let stored_file = resolve_media_file_with_conn(&mut conn, bot.id, &normalized_photo, "photo")?;
    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET photo_file_id = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![stored_file.file_id, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert("new_chat_photo".to_string(), Value::Bool(true));
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} changed the group photo", display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(json!(true))
}

fn handle_delete_chat_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteChatPhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let had_photo: Option<String> = conn
        .query_row(
            "SELECT photo_file_id FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET photo_file_id = NULL, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    if had_photo.is_some() {
        let chat = build_chat_from_group_record(&sim_chat);
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("delete_chat_photo".to_string(), Value::Bool(true));
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            format!("{} deleted the group photo", display_name_for_service_user(&actor)),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

fn handle_set_chat_title(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatTitleRequest = parse_request(params)?;
    let title = request.title.trim().to_string();
    if title.is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if title.chars().count() > 128 {
        return Err(ApiError::bad_request("title is too long"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let now = Utc::now().timestamp();
    let title_changed = sim_chat.title.as_deref() != Some(title.as_str());

    conn.execute(
        "UPDATE chats SET title = ?1 WHERE chat_key = ?2",
        params![&title, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE sim_chats
         SET title = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![&title, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    if title_changed {
        let mut chat = build_chat_from_group_record(&sim_chat);
        chat.title = Some(title.clone());
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("new_chat_title".to_string(), Value::String(title.clone()));
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            service_text_group_title_changed(&actor, &title),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

fn handle_set_chat_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatDescriptionRequest = parse_request(params)?;
    let description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(value) = description.as_ref() {
        if value.chars().count() > 255 {
            return Err(ApiError::bad_request("description is too long"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET description = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![description, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_chat_sticker_set(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatStickerSetRequest = parse_request(params)?;
    let set_name = request.sticker_set_name.trim().to_string();
    if set_name.is_empty() {
        return Err(ApiError::bad_request("sticker_set_name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request("sticker set can only be set for supergroups"));
    }

    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, set_name],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::bad_request("sticker set was not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET sticker_set_name = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![set_name, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_delete_chat_sticker_set(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteChatStickerSetRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request("sticker set can only be removed for supergroups"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET sticker_set_name = NULL, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn menu_button_scope_key(chat_id: Option<i64>) -> String {
    match chat_id {
        Some(value) => format!("chat:{}", value),
        None => "default".to_string(),
    }
}

fn chat_id_to_chat_key(chat_id: i64) -> String {
    format!("chat:{}", chat_id)
}

fn default_menu_button() -> MenuButton {
    MenuButton {
        extra: json!({ "type": "default" }),
    }
}

fn is_supported_chat_action(action: &str) -> bool {
    matches!(
        action,
        "typing"
            | "upload_photo"
            | "record_video"
            | "upload_video"
            | "record_voice"
            | "upload_voice"
            | "upload_document"
            | "choose_sticker"
            | "find_location"
            | "record_video_note"
            | "upload_video_note"
    )
}

fn publish_sim_client_event(
    state: &Data<AppState>,
    token: &str,
    event_payload: Value,
) {
    state.ws_hub.publish_json(token, &event_payload);
}

fn handle_send_chat_action(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendChatActionRequest = parse_request(params)?;

    if request.business_connection_id.is_some() {
        return Err(ApiError::bad_request("business_connection_id is not supported in simulator"));
    }

    let normalized_action = request.action.trim().to_ascii_lowercase();
    if normalized_action.is_empty() {
        return Err(ApiError::bad_request("action is empty"));
    }
    if !is_supported_chat_action(&normalized_action) {
        return Err(ApiError::bad_request("unsupported action type"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chat_id_as_i64(&request.chat_id, &chat_key);
    let sim_chat = load_sim_chat_record(&mut conn, bot.id, &chat_key)?;

    if sim_chat.is_none() && chat_id <= 0 {
        return Err(ApiError::not_found("chat not found"));
    }

    let chat_type = sim_chat
        .as_ref()
        .map(|chat| chat.chat_type.as_str())
        .unwrap_or("private");

    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    let actor_name = if actor_user_id == bot.id {
        bot.first_name.clone()
    } else {
        ensure_sim_user_record(&mut conn, actor_user_id)?.first_name
    };

    if chat_type != "private" {
        ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &chat_key,
            actor_user_id,
            ChatSendKind::Other,
        )?;
    }

    let target_chat_id = sim_chat.as_ref().map(|chat| chat.chat_id).unwrap_or(chat_id);
    publish_sim_client_event(
        state,
        token,
        json!({
            "sim_event": "chat_action",
            "chat_id": target_chat_id,
            "action": normalized_action,
            "from_user_id": actor_user_id,
            "from_name": actor_name,
            "date": Utc::now().timestamp(),
        }),
    );

    Ok(Value::Bool(true))
}

fn handle_set_chat_menu_button(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatMenuButtonRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if let Some(chat_id) = request.chat_id {
        let chat_key = chat_id_to_chat_key(chat_id);
        if let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? {
            if sim_chat.chat_type != "private" {
                return Err(ApiError::bad_request("chat menu button can only be set for private chats"));
            }
        } else if chat_id <= 0 {
            return Err(ApiError::bad_request("chat menu button can only be set for private chats"));
        }
    }

    let menu_button_value = if let Some(menu_button) = request.menu_button {
        serde_json::to_value(menu_button).map_err(ApiError::internal)?
    } else {
        serde_json::to_value(default_menu_button()).map_err(ApiError::internal)?
    };

    if !menu_button_value.is_object() {
        return Err(ApiError::bad_request("menu_button is invalid"));
    }

    let now = Utc::now().timestamp();
    let scope_key = menu_button_scope_key(request.chat_id);
    conn.execute(
        "INSERT INTO bot_chat_menu_buttons (bot_id, scope_key, menu_button_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, scope_key)
         DO UPDATE SET
            menu_button_json = excluded.menu_button_json,
            updated_at = excluded.updated_at",
        params![bot.id, scope_key, menu_button_value.to_string(), now],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::Bool(true))
}

fn handle_get_chat_menu_button(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatMenuButtonRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if let Some(chat_id) = request.chat_id {
        let chat_key = chat_id_to_chat_key(chat_id);
        if let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? {
            if sim_chat.chat_type != "private" {
                return Err(ApiError::bad_request("chat menu button is only available in private chats"));
            }
        } else if chat_id <= 0 {
            return Err(ApiError::bad_request("chat menu button is only available in private chats"));
        }
    }

    let scoped_key = menu_button_scope_key(request.chat_id);
    let mut stored: Option<String> = conn
        .query_row(
            "SELECT menu_button_json
             FROM bot_chat_menu_buttons
             WHERE bot_id = ?1 AND scope_key = ?2",
            params![bot.id, &scoped_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if stored.is_none() && request.chat_id.is_some() {
        stored = conn
            .query_row(
                "SELECT menu_button_json
                 FROM bot_chat_menu_buttons
                 WHERE bot_id = ?1 AND scope_key = 'default'",
                params![bot.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
    }

    let menu_button = if let Some(raw) = stored {
        serde_json::from_str::<MenuButton>(&raw).unwrap_or_else(|_| default_menu_button())
    } else {
        default_menu_button()
    };

    serde_json::to_value(menu_button).map_err(ApiError::internal)
}

fn load_latest_pinned_message_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<i64>, ApiError> {
    conn
        .query_row(
            "SELECT message_id
             FROM sim_chat_pinned_messages
             WHERE bot_id = ?1 AND chat_key = ?2
             ORDER BY pinned_at DESC, message_id DESC
             LIMIT 1",
            params![bot_id, chat_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)
}

fn sync_latest_pinned_message_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    updated_at: i64,
) -> Result<Option<i64>, ApiError> {
    let latest = load_latest_pinned_message_id(conn, bot_id, chat_key)?;
    conn.execute(
        "UPDATE sim_chats
         SET pinned_message_id = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![latest, updated_at, bot_id, chat_key],
    )
    .map_err(ApiError::internal)?;
    Ok(latest)
}

fn handle_pin_chat_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: PinChatMessageRequest = parse_request(params)?;
    if request.business_connection_id.is_some() {
        return Err(ApiError::bad_request("business_connection_id is not supported in simulator"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, request.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if exists.is_none() {
        return Err(ApiError::bad_request("message to pin was not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "DELETE FROM sim_chat_pinned_messages
         WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
        params![bot.id, &chat_key, request.message_id],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "INSERT INTO sim_chat_pinned_messages (bot_id, chat_key, message_id, pinned_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![bot.id, &chat_key, request.message_id, now],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "UPDATE sim_chats
         SET pinned_message_id = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![request.message_id, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let pinned_message = load_message_value(&mut conn, &bot, request.message_id)?;
    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert("pinned_message".to_string(), pinned_message);
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} pinned a message", display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(json!(true))
}

fn handle_unpin_chat_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinChatMessageRequest = parse_request(params)?;
    if request.business_connection_id.is_some() {
        return Err(ApiError::bad_request("business_connection_id is not supported in simulator"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let target_message_id = if let Some(message_id) = request.message_id {
        Some(message_id)
    } else {
        load_latest_pinned_message_id(&mut conn, bot.id, &chat_key)?
    };

    if let Some(message_id) = target_message_id {
        conn.execute(
            "DELETE FROM sim_chat_pinned_messages
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, message_id],
        )
        .map_err(ApiError::internal)?;
    }

    let now = Utc::now().timestamp();
    let _ = sync_latest_pinned_message_id(&mut conn, bot.id, &chat_key, now)?;

    Ok(json!(true))
}

fn handle_unpin_all_chat_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinAllChatMessagesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "DELETE FROM sim_chat_pinned_messages WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "UPDATE sim_chats
         SET pinned_message_id = NULL, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_leave_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: LeaveChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, bot.id)?;
    let Some(old_record) = old_record else {
        return Ok(json!(true));
    };
    let old_status = old_record.status.clone();

    if old_status == "left" || old_status == "banned" {
        return Ok(json!(true));
    }
    if old_status == "owner" {
        return Err(ApiError::bad_request("chat owner can't leave the chat"));
    }
    if !is_active_chat_member_status(old_status.as_str()) {
        return Ok(json!(true));
    }

    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        bot.id,
        "left",
        "left",
        None,
        None,
        None,
        None,
        None,
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, bot.id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let bot_user = build_bot_user(&bot);

    emit_my_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &bot_user,
        &old_status,
        "left",
        Some(&old_record),
        new_record.as_ref(),
        now,
    )?;

    if sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "left_chat_member".to_string(),
            serde_json::to_value(bot_user.clone()).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &bot_user,
            now,
            service_text_left_chat_member(&bot_user, &bot_user),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

fn handle_get_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let requested_chat_key = value_to_chat_key(&request.chat_id)?;
    let requested_chat_id = chat_id_as_i64(&request.chat_id, &requested_chat_key);
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &requested_chat_key)?
        .or(load_sim_chat_record_by_chat_id(&mut conn, bot.id, requested_chat_id)?) else {
        return Err(ApiError::not_found("chat not found"));
    };
    let chat_key = sim_chat.chat_key.clone();
    let is_direct_messages = is_direct_messages_chat(&sim_chat);

    if sim_chat.chat_type != "private" {
        if is_direct_messages {
            let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
            let parent_channel_chat_id = sim_chat
                .parent_channel_chat_id
                .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
            ensure_channel_member_can_manage_direct_messages(
                &mut conn,
                bot.id,
                &parent_channel_chat_id.to_string(),
                actor_user_id,
            )?;
        } else {
            ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;
        }
    }

    let row: Option<(Option<String>, Option<String>, i64, i64, Option<String>, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT description, permissions_json, message_history_visible, slow_mode_delay, photo_file_id, sticker_set_name, pinned_message_id
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((description, permissions_json, message_history_visible, slow_mode_delay, photo_file_id, sticker_set_name, pinned_message_id)) = row else {
        return Err(ApiError::not_found("chat not found"));
    };

    let photo = if let Some(file_id) = photo_file_id {
        let file_unique_id: Option<String> = conn
            .query_row(
                "SELECT file_unique_id FROM files WHERE bot_id = ?1 AND file_id = ?2",
                params![bot.id, &file_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        let unique = file_unique_id.unwrap_or_else(|| file_id.clone());
        Some(ChatPhoto {
            small_file_id: file_id.clone(),
            small_file_unique_id: unique.clone(),
            big_file_id: file_id,
            big_file_unique_id: unique,
        })
    } else {
        None
    };

    let latest_pinned_message_id = load_latest_pinned_message_id(&mut conn, bot.id, &chat_key)?
        .or(pinned_message_id);

    let pinned_message = if let Some(message_id) = latest_pinned_message_id {
        load_message_value(&mut conn, &bot, message_id)
            .ok()
            .and_then(|value| serde_json::from_value::<Message>(value).ok())
    } else {
        None
    };

    let permissions = if sim_chat.chat_type == "private" || is_direct_messages {
        None
    } else {
        Some(
            permissions_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
                .unwrap_or_else(default_group_permissions),
        )
    };

    let primary_invite_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT invite_link, creates_join_request
             FROM sim_chat_invite_links
             WHERE bot_id = ?1 AND chat_key = ?2 AND is_primary = 1 AND is_revoked = 0
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, &chat_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let linked_chat_id = if is_direct_messages {
        None
    } else {
        resolve_linked_chat_id_for_chat(&mut conn, bot.id, &sim_chat)?
    };
    let parent_chat = if is_direct_messages {
        let parent_channel_chat_id = sim_chat.parent_channel_chat_id.unwrap_or_default();
        let parent_channel_chat = load_sim_chat_record(&mut conn, bot.id, &parent_channel_chat_id.to_string())?
            .ok_or_else(|| ApiError::not_found("parent channel not found"))?;
        Some(build_chat_from_group_record(&parent_channel_chat))
    } else {
        None
    };

    let chat_full = ChatFullInfo {
        id: sim_chat.chat_id,
        r#type: sim_chat.chat_type.clone(),
        title: sim_chat.title.clone(),
        username: sim_chat.username.clone(),
        first_name: None,
        last_name: None,
        is_forum: if sim_chat.chat_type == "supergroup" && !is_direct_messages {
            Some(sim_chat.is_forum)
        } else {
            None
        },
        is_direct_messages: if is_direct_messages { Some(true) } else { None },
        accent_color_id: 0,
        max_reaction_count: 11,
        photo,
        active_usernames: None,
        birthdate: None,
        business_intro: None,
        business_location: None,
        business_opening_hours: None,
        personal_chat: None,
        parent_chat,
        available_reactions: None,
        background_custom_emoji_id: None,
        profile_accent_color_id: None,
        profile_background_custom_emoji_id: None,
        emoji_status_custom_emoji_id: None,
        emoji_status_expiration_date: None,
        bio: None,
        has_private_forwards: None,
        has_restricted_voice_and_video_messages: None,
        join_to_send_messages: None,
        join_by_request: primary_invite_row.as_ref().map(|(_, creates_join_request)| *creates_join_request == 1),
        description,
        invite_link: primary_invite_row.as_ref().map(|(invite_link, _)| invite_link.clone()),
        pinned_message,
        permissions,
        accepted_gift_types: AcceptedGiftTypes {
            unlimited_gifts: true,
            limited_gifts: true,
            unique_gifts: true,
            premium_subscription: true,
            gifts_from_channels: true,
        },
        can_send_paid_media: None,
        slow_mode_delay: if sim_chat.chat_type == "private" || is_direct_messages {
            None
        } else {
            Some(slow_mode_delay.max(0))
        },
        unrestrict_boost_count: None,
        message_auto_delete_time: None,
        has_aggressive_anti_spam_enabled: None,
        has_hidden_members: None,
        has_protected_content: None,
        has_visible_history: if sim_chat.chat_type == "private" || is_direct_messages {
            None
        } else {
            Some(message_history_visible != 0)
        },
        sticker_set_name,
        can_set_sticker_set: if sim_chat.chat_type == "supergroup" {
            Some(true)
        } else {
            None
        },
        custom_emoji_sticker_set_name: None,
        linked_chat_id,
        location: None,
        rating: None,
        first_profile_audio: None,
        unique_gift_colors: None,
        paid_message_star_count: None,
    };

    let mut chat_value = serde_json::to_value(chat_full).map_err(ApiError::internal)?;
    let verification_description =
        load_chat_verification_description(&mut conn, bot.id, &chat_key)?;
    if let Some(object) = chat_value.as_object_mut() {
        object.insert(
            "is_verified".to_string(),
            Value::Bool(verification_description.is_some()),
        );
        if let Some(description) = verification_description {
            object.insert(
                "verification_description".to_string(),
                Value::String(description),
            );
        }
    }

    Ok(chat_value)
}

fn handle_get_chat_administrators(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatAdministratorsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("private chats do not have administrators list"));
    }

    let rows: Vec<(i64, SimChatMemberRecord)> = {
        let mut stmt = conn
            .prepare(
                "SELECT user_id, status, role, permissions_json, admin_rights_json, until_date, custom_title, tag, joined_at
                 FROM sim_chat_members
                 WHERE bot_id = ?1 AND chat_key = ?2 AND status IN ('owner','admin')
                 ORDER BY CASE status WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, user_id ASC",
            )
            .map_err(ApiError::internal)?;

        let mapped = stmt
            .query_map(params![bot.id, &chat_key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    SimChatMemberRecord {
                        status: row.get(1)?,
                        role: row.get(2)?,
                        permissions_json: row.get(3)?,
                        admin_rights_json: row.get(4)?,
                        until_date: row.get(5)?,
                        custom_title: row.get(6)?,
                        tag: row.get(7)?,
                        joined_at: row.get(8)?,
                    },
                ))
            })
            .map_err(ApiError::internal)?;

        let mut collected: Vec<(i64, SimChatMemberRecord)> = Vec::new();
        for row in mapped {
            collected.push(row.map_err(ApiError::internal)?);
        }
        collected
    };

    let mut admins: Vec<ChatMember> = Vec::new();
    for (user_id, record) in rows {
        if user_id == bot.id {
            continue;
        }
        let user = build_user_from_sim_record(&ensure_sim_user_record(&mut conn, user_id)?, false);
        admins.push(chat_member_from_record(&record, &user, sim_chat.chat_type.as_str())?);
    }

    serde_json::to_value(admins).map_err(ApiError::internal)
}

fn handle_get_chat_member_count(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatMemberCountRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM sim_chat_members
             WHERE bot_id = ?1 AND chat_key = ?2 AND status IN ('owner','admin','member','restricted')",
            params![bot.id, &chat_key],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    serde_json::to_value(count).map_err(ApiError::internal)
}

fn handle_get_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let member_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let user = build_user_from_sim_record(&ensure_sim_user_record(&mut conn, request.user_id)?, false);

    let member = if let Some(record) = member_record {
        chat_member_from_record(&record, &user, sim_chat.chat_type.as_str())?
    } else {
        chat_member_from_status("left", &user)?
    };

    serde_json::to_value(member).map_err(ApiError::internal)
}

fn forum_topic_default_icon_color() -> i64 {
    0x6FB9F0
}

fn is_allowed_forum_topic_icon_color(value: i64) -> bool {
    matches!(
        value,
        0x6FB9F0 | 0xFFD67E | 0xCB86DB | 0x8EEE98 | 0xFF93B2 | 0xFB6F5F
    )
}

fn normalize_sim_parse_mode(value: Option<&str>) -> Option<String> {
    let normalized = value?.trim();
    if normalized.is_empty() {
        return None;
    }

    if normalized.eq_ignore_ascii_case("markdownv2") {
        return Some("MarkdownV2".to_string());
    }
    if normalized.eq_ignore_ascii_case("markdown") {
        return Some("Markdown".to_string());
    }
    if normalized.eq_ignore_ascii_case("html") {
        return Some("HTML".to_string());
    }

    None
}

fn resolve_forum_message_thread_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    requested_message_thread_id: Option<i64>,
) -> Result<Option<i64>, ApiError> {
    if is_direct_messages_chat(sim_chat) {
        if requested_message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not available in channel direct messages chats",
            ));
        }
        return Ok(None);
    }

    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        if requested_message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is available only in forum supergroups",
            ));
        }
        return Ok(None);
    }

    let thread_id = requested_message_thread_id.unwrap_or(1);
    if thread_id <= 0 {
        return Err(ApiError::bad_request("message_thread_id is invalid"));
    }

    if thread_id == 1 {
        let (_, is_closed, is_hidden) = ensure_general_forum_topic_state(conn, bot_id, &sim_chat.chat_key)?;
        if is_closed {
            return Err(ApiError::bad_request("general forum topic is closed"));
        }
        if is_hidden {
            return Err(ApiError::bad_request("general forum topic is hidden"));
        }
        return Ok(Some(thread_id));
    }

    let topic_is_closed: Option<i64> = conn
        .query_row(
            "SELECT is_closed FROM forum_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot_id, &sim_chat.chat_key, thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if topic_is_closed.is_none() {
        return Err(ApiError::not_found("forum topic not found"));
    }

    if topic_is_closed.unwrap_or_default() == 1 {
        return Err(ApiError::bad_request("forum topic is closed"));
    }

    Ok(Some(thread_id))
}

fn resolve_forum_message_thread_for_chat_key(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    requested_message_thread_id: Option<i64>,
) -> Result<Option<i64>, ApiError> {
    if let Some(sim_chat) = load_sim_chat_record(conn, bot_id, chat_key)? {
        return resolve_forum_message_thread_id(conn, bot_id, &sim_chat, requested_message_thread_id);
    }

    if requested_message_thread_id.is_some() {
        return Err(ApiError::bad_request(
            "message_thread_id is available only in forum supergroups",
        ));
    }

    Ok(None)
}

fn resolve_forum_supergroup_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_id: &Value,
) -> Result<(String, SimChatRecord, Chat, User), ApiError> {
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(conn, bot.id, chat_id)?;
    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        return Err(ApiError::bad_request(
            "forum topics are available only in forum supergroups",
        ));
    }

    let actor = resolve_chat_admin_actor(conn, bot, &chat_key)?;
    let chat = build_chat_from_group_record(&sim_chat);
    Ok((chat_key, sim_chat, chat, actor))
}

fn load_forum_topic(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_thread_id: i64,
) -> Result<Option<ForumTopic>, ApiError> {
    conn.query_row(
        "SELECT name, icon_color, icon_custom_emoji_id
         FROM forum_topics
         WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
        params![bot_id, chat_key, message_thread_id],
        |row| {
            Ok(ForumTopic {
                message_thread_id,
                name: row.get(0)?,
                icon_color: row.get(1)?,
                icon_custom_emoji_id: row.get(2)?,
                is_name_implicit: None,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn ensure_general_forum_topic_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(String, bool, bool), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO forum_topic_general_states (bot_id, chat_key, name, is_closed, is_hidden, updated_at)
         VALUES (?1, ?2, 'General', 0, 0, ?3)
         ON CONFLICT(bot_id, chat_key) DO NOTHING",
        params![bot_id, chat_key, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT name, is_closed, is_hidden FROM forum_topic_general_states
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? == 1,
                row.get::<_, i64>(2)? == 1,
            ))
        },
    )
    .map_err(ApiError::internal)
}

fn handle_get_forum_topic_icon_stickers(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetForumTopicIconStickersRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let mut stmt = conn
        .prepare(
            "SELECT file_id, file_unique_id, sticker_type, width, height, is_animated, is_video,
                    emoji, set_name, mask_position_json, custom_emoji_id, needs_repainting
             FROM stickers
             WHERE bot_id = ?1 AND custom_emoji_id IS NOT NULL
             ORDER BY updated_at DESC
             LIMIT 64",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id], |row| {
            Ok(Sticker {
                file_id: row.get(0)?,
                file_unique_id: row.get(1)?,
                r#type: row.get(2)?,
                width: row.get(3)?,
                height: row.get(4)?,
                is_animated: row.get::<_, i64>(5)? == 1,
                is_video: row.get::<_, i64>(6)? == 1,
                thumbnail: None,
                emoji: row.get(7)?,
                set_name: row.get(8)?,
                premium_animation: None,
                mask_position: row
                    .get::<_, Option<String>>(9)?
                    .and_then(|raw| serde_json::from_str(&raw).ok()),
                custom_emoji_id: row.get(10)?,
                needs_repainting: Some(row.get::<_, i64>(11)? == 1),
                file_size: None,
            })
        })
        .map_err(ApiError::internal)?;

    let mut stickers: Vec<Sticker> = Vec::new();
    for row in rows {
        stickers.push(row.map_err(ApiError::internal)?);
    }

    serde_json::to_value(stickers).map_err(ApiError::internal)
}

fn handle_create_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateForumTopicRequest = parse_request(params)?;
    let name = request.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let icon_color = request.icon_color.unwrap_or_else(forum_topic_default_icon_color);
    if !is_allowed_forum_topic_icon_color(icon_color) {
        return Err(ApiError::bad_request("icon_color is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let mut message_thread_id =
        ((Utc::now().timestamp_millis().unsigned_abs() % 2_000_000_000) as i64).max(2);
    for _ in 0..8 {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM forum_topics
                 WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
                params![bot.id, &chat_key, message_thread_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            break;
        }
        message_thread_id += 1;
    }

    let now = Utc::now().timestamp();
    let icon_custom_emoji_id = request
        .icon_custom_emoji_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    conn.execute(
        "INSERT INTO forum_topics
         (bot_id, chat_key, message_thread_id, name, icon_color, icon_custom_emoji_id, is_closed, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7)",
        params![
            bot.id,
            &chat_key,
            message_thread_id,
            name,
            icon_color,
            icon_custom_emoji_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(message_thread_id));
    service_fields.insert(
        "forum_topic_created".to_string(),
        json!({
            "name": name,
            "icon_color": icon_color,
            "icon_custom_emoji_id": icon_custom_emoji_id,
        }),
    );
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} created the topic \"{}\"", display_name_for_service_user(&actor), name),
        service_fields,
    )?;

    let topic = ForumTopic {
        message_thread_id,
        name: name.to_string(),
        icon_color,
        icon_custom_emoji_id,
        is_name_implicit: None,
    };

    serde_json::to_value(topic).map_err(ApiError::internal)
}

fn handle_edit_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(current_topic) = load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    let next_name = if let Some(raw_name) = request.name.as_deref() {
        let trimmed = raw_name.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("name is empty"));
        }
        trimmed.to_string()
    } else {
        current_topic.name.clone()
    };

    let next_icon_custom_emoji_id = if let Some(raw_icon) = request.icon_custom_emoji_id.as_deref() {
        let trimmed = raw_icon.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    } else {
        current_topic.icon_custom_emoji_id.clone()
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE forum_topics
         SET name = ?1,
             icon_custom_emoji_id = ?2,
             updated_at = ?3
         WHERE bot_id = ?4 AND chat_key = ?5 AND message_thread_id = ?6",
        params![
            next_name,
            next_icon_custom_emoji_id,
            now,
            bot.id,
            &chat_key,
            request.message_thread_id,
        ],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert(
        "message_thread_id".to_string(),
        Value::from(request.message_thread_id),
    );
    service_fields.insert(
        "forum_topic_edited".to_string(),
        json!({
            "name": if next_name != current_topic.name { Some(next_name.clone()) } else { None::<String> },
            "icon_custom_emoji_id": if request.icon_custom_emoji_id.is_some() {
                next_icon_custom_emoji_id.clone()
            } else {
                None
            },
        }),
    );

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} edited topic \"{}\"",
            display_name_for_service_user(&actor),
            next_name
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_close_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CloseForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(_) = load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE forum_topics
         SET is_closed = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3 AND message_thread_id = ?4",
        params![now, bot.id, &chat_key, request.message_thread_id],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert(
        "message_thread_id".to_string(),
        Value::from(request.message_thread_id),
    );
    service_fields.insert("forum_topic_closed".to_string(), json!({}));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} closed a topic", display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_reopen_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReopenForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(_) = load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE forum_topics
         SET is_closed = 0, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3 AND message_thread_id = ?4",
        params![now, bot.id, &chat_key, request.message_thread_id],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert(
        "message_thread_id".to_string(),
        Value::from(request.message_thread_id),
    );
    service_fields.insert("forum_topic_reopened".to_string(), json!({}));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} reopened a topic", display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_delete_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;

    if is_direct_messages_chat(&sim_chat) {
        if request.message_thread_id <= 0 {
            return Err(ApiError::bad_request("message_thread_id is invalid"));
        }

        let _topic = load_direct_messages_topic_record(
            &mut conn,
            bot.id,
            &chat_key,
            request.message_thread_id,
        )?
        .ok_or_else(|| ApiError::not_found("direct messages topic not found"))?;

        let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
        let parent_channel_chat_id = sim_chat
            .parent_channel_chat_id
            .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
        let parent_channel_chat_key = parent_channel_chat_id.to_string();
        let actor_can_manage_direct_messages = ensure_channel_member_can_manage_direct_messages(
            &mut conn,
            bot.id,
            &parent_channel_chat_key,
            actor_user_id,
        )
        .is_ok();

        if !actor_can_manage_direct_messages {
            return Err(ApiError::bad_request(
                "not enough rights to delete this direct messages topic",
            ));
        }

        let topic_message_ids = collect_message_ids_for_thread(
            &mut conn,
            bot.id,
            &chat_key,
            request.message_thread_id,
        )?;
        let _ = delete_messages_with_dependencies(
            &mut conn,
            bot.id,
            &chat_key,
            sim_chat.chat_id,
            &topic_message_ids,
        )?;

        conn.execute(
            "DELETE FROM sim_direct_message_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND topic_id = ?3",
            params![bot.id, &chat_key, request.message_thread_id],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "DELETE FROM sim_message_drafts
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot.id, &chat_key, request.message_thread_id],
        )
        .map_err(ApiError::internal)?;

        return Ok(Value::Bool(true));
    }

    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        return Err(ApiError::bad_request(
            "forum topics are available only in forum supergroups",
        ));
    }

    let _actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let deleted = conn
        .execute(
            "DELETE FROM forum_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot.id, &chat_key, request.message_thread_id],
        )
        .map_err(ApiError::internal)?;
    if deleted == 0 {
        return Err(ApiError::not_found("forum topic not found"));
    }

    let topic_message_ids = collect_message_ids_for_thread(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let _ = delete_messages_with_dependencies(
        &mut conn,
        bot.id,
        &chat_key,
        sim_chat.chat_id,
        &topic_message_ids,
    )?;

    Ok(Value::Bool(true))
}

fn handle_unpin_all_forum_topic_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinAllForumTopicMessagesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, _chat, _actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(_) = load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    Ok(Value::Bool(true))
}

fn handle_edit_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditGeneralForumTopicRequest = parse_request(params)?;
    let name = request.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    let (current_name, _, _) = ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET name = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![name, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert(
        "forum_topic_edited".to_string(),
        json!({
            "name": if name != current_name {
                Some(name.to_string())
            } else {
                None::<String>
            },
            "icon_custom_emoji_id": None::<String>,
        }),
    );

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} edited topic \"{}\"",
            display_name_for_service_user(&actor),
            name
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_close_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CloseGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_closed = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("forum_topic_closed".to_string(), json!({}));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} closed the General topic",
            display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_reopen_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReopenGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_closed = 0, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("forum_topic_reopened".to_string(), json!({}));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} reopened the General topic",
            display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_hide_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: HideGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_hidden = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("general_forum_topic_hidden".to_string(), json!({}));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} hid the General topic",
            display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_unhide_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnhideGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_hidden = 0, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("general_forum_topic_unhidden".to_string(), json!({}));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} unhid the General topic",
            display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

fn handle_unpin_all_general_forum_topic_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinAllGeneralForumTopicMessagesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, _chat, _actor) =
        resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    Ok(Value::Bool(true))
}

fn handle_export_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ExportChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_invite_links
         SET is_revoked = 1, updated_at = ?3
         WHERE bot_id = ?1 AND chat_key = ?2 AND is_primary = 1 AND is_revoked = 0",
        params![bot.id, &chat_key, now],
    )
    .map_err(ApiError::internal)?;

    let invite_link = generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 0, 1, 0, NULL, NULL, NULL, NULL, NULL, ?5, ?5)",
        params![bot.id, &chat_key, &invite_link, actor.id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::String(invite_link))
}

fn handle_create_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let now = Utc::now().timestamp();
    let creates_join_request = request.creates_join_request.unwrap_or(false);
    let name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let expire_date = request.expire_date.filter(|value| *value > now);
    let member_limit = request.member_limit.filter(|value| *value > 0);

    if creates_join_request && member_limit.is_some() {
        return Err(ApiError::bad_request("member_limit can't be used when creates_join_request is true"));
    }

    let invite_link = generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7, ?8, NULL, NULL, ?9, ?9)",
        params![
            bot.id,
            &chat_key,
            &invite_link,
            actor.id,
            if creates_join_request { 1 } else { 0 },
            name,
            expire_date,
            member_limit,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let record = SimChatInviteLinkRecord {
        invite_link,
        creator_user_id: actor.id,
        creates_join_request,
        is_primary: false,
        is_revoked: false,
        name,
        expire_date,
        member_limit,
        subscription_period: None,
        subscription_price: None,
    };

    let pending_count = pending_join_request_count_for_link(&mut conn, bot.id, &chat_key, &record.invite_link)?;
    let invite = chat_invite_link_from_record(actor, &record, Some(pending_count));
    serde_json::to_value(invite).map_err(ApiError::internal)
}

fn handle_edit_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let Some(existing) = load_invite_link_record(&mut conn, bot.id, &chat_key, &request.invite_link)? else {
        return Err(ApiError::not_found("invite link not found"));
    };
    if existing.is_primary {
        return Err(ApiError::bad_request("primary invite link can't be edited with editChatInviteLink"));
    }
    if existing.creator_user_id != actor.id {
        return Err(ApiError::bad_request("invite link wasn't created by this actor"));
    }
    if existing.is_revoked {
        return Err(ApiError::bad_request("invite link is revoked"));
    }

    let now = Utc::now().timestamp();
    let name = request
        .name
        .map(|value| value.trim().to_string())
        .or(existing.name.clone())
        .filter(|value| !value.is_empty());
    let expire_date = match request.expire_date {
        Some(value) if value > now => Some(value),
        Some(_) => None,
        None => existing.expire_date,
    };
    let member_limit = match request.member_limit {
        Some(value) if value > 0 => Some(value),
        Some(_) => None,
        None => existing.member_limit,
    };
    let creates_join_request = request
        .creates_join_request
        .unwrap_or(existing.creates_join_request);

    if creates_join_request && member_limit.is_some() {
        return Err(ApiError::bad_request("member_limit can't be used when creates_join_request is true"));
    }

    conn.execute(
        "UPDATE sim_chat_invite_links
         SET creates_join_request = ?1,
             name = ?2,
             expire_date = ?3,
             member_limit = ?4,
             updated_at = ?5
         WHERE bot_id = ?6 AND chat_key = ?7 AND invite_link = ?8",
        params![
            if creates_join_request { 1 } else { 0 },
            name,
            expire_date,
            member_limit,
            now,
            bot.id,
            &chat_key,
            &request.invite_link,
        ],
    )
    .map_err(ApiError::internal)?;

    let updated = SimChatInviteLinkRecord {
        invite_link: existing.invite_link,
        creator_user_id: existing.creator_user_id,
        creates_join_request,
        is_primary: existing.is_primary,
        is_revoked: existing.is_revoked,
        name,
        expire_date,
        member_limit,
        subscription_period: existing.subscription_period,
        subscription_price: existing.subscription_price,
    };

    let pending_count = pending_join_request_count_for_link(&mut conn, bot.id, &chat_key, &updated.invite_link)?;
    let invite = chat_invite_link_from_record(
        resolve_invite_creator_user(&mut conn, &bot, updated.creator_user_id)?,
        &updated,
        Some(pending_count),
    );
    serde_json::to_value(invite).map_err(ApiError::internal)
}

fn handle_revoke_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RevokeChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let Some(existing) = load_invite_link_record(&mut conn, bot.id, &chat_key, &request.invite_link)? else {
        return Err(ApiError::not_found("invite link not found"));
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_invite_links
         SET is_revoked = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3 AND invite_link = ?4",
        params![now, bot.id, &chat_key, &request.invite_link],
    )
    .map_err(ApiError::internal)?;

    if existing.is_primary {
        let new_primary_link = generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
        conn.execute(
            "INSERT INTO sim_chat_invite_links
             (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 0, 1, 0, NULL, NULL, NULL, NULL, NULL, ?5, ?5)",
            params![bot.id, &chat_key, &new_primary_link, actor.id, now],
        )
        .map_err(ApiError::internal)?;
    }

    let revoked = SimChatInviteLinkRecord {
        invite_link: existing.invite_link,
        creator_user_id: existing.creator_user_id,
        creates_join_request: existing.creates_join_request,
        is_primary: existing.is_primary,
        is_revoked: true,
        name: existing.name,
        expire_date: existing.expire_date,
        member_limit: existing.member_limit,
        subscription_period: existing.subscription_period,
        subscription_price: existing.subscription_price,
    };

    let pending_count = pending_join_request_count_for_link(&mut conn, bot.id, &chat_key, &revoked.invite_link)?;
    let invite = chat_invite_link_from_record(
        resolve_invite_creator_user(&mut conn, &bot, revoked.creator_user_id)?,
        &revoked,
        Some(pending_count),
    );
    serde_json::to_value(invite).map_err(ApiError::internal)
}

fn handle_create_chat_subscription_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateChatSubscriptionInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("subscription invite links are only available for channels"));
    }
    ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    if request.subscription_period <= 0 {
        return Err(ApiError::bad_request("subscription_period must be greater than zero"));
    }
    if request.subscription_price <= 0 {
        return Err(ApiError::bad_request("subscription_price must be greater than zero"));
    }

    let name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let now = Utc::now().timestamp();
    let invite_link = generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 0, 0, 0, ?5, NULL, NULL, ?6, ?7, ?8, ?8)",
        params![
            bot.id,
            &chat_key,
            &invite_link,
            actor.id,
            name,
            request.subscription_period,
            request.subscription_price,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let record = SimChatInviteLinkRecord {
        invite_link,
        creator_user_id: actor.id,
        creates_join_request: false,
        is_primary: false,
        is_revoked: false,
        name,
        expire_date: None,
        member_limit: None,
        subscription_period: Some(request.subscription_period),
        subscription_price: Some(request.subscription_price),
    };
    let invite = chat_invite_link_from_record(actor, &record, Some(0));
    serde_json::to_value(invite).map_err(ApiError::internal)
}

fn handle_edit_chat_subscription_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditChatSubscriptionInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("subscription invite links are only available for channels"));
    }
    ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;

    let Some(existing) = load_invite_link_record(&mut conn, bot.id, &chat_key, &request.invite_link)? else {
        return Err(ApiError::not_found("invite link not found"));
    };
    if existing.creator_user_id != actor.id {
        return Err(ApiError::bad_request("invite link wasn't created by this actor"));
    }
    if existing.subscription_period.is_none() || existing.subscription_price.is_none() {
        return Err(ApiError::bad_request("invite link is not a subscription link"));
    }

    let now = Utc::now().timestamp();
    let name = request
        .name
        .map(|value| value.trim().to_string())
        .or(existing.name.clone())
        .filter(|value| !value.is_empty());

    conn.execute(
        "UPDATE sim_chat_invite_links
         SET name = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4 AND invite_link = ?5",
        params![name, now, bot.id, &chat_key, &request.invite_link],
    )
    .map_err(ApiError::internal)?;

    let updated = SimChatInviteLinkRecord {
        invite_link: existing.invite_link,
        creator_user_id: existing.creator_user_id,
        creates_join_request: existing.creates_join_request,
        is_primary: existing.is_primary,
        is_revoked: existing.is_revoked,
        name,
        expire_date: existing.expire_date,
        member_limit: existing.member_limit,
        subscription_period: existing.subscription_period,
        subscription_price: existing.subscription_price,
    };
    let invite = chat_invite_link_from_record(
        resolve_invite_creator_user(&mut conn, &bot, updated.creator_user_id)?,
        &updated,
        Some(0),
    );
    serde_json::to_value(invite).map_err(ApiError::internal)
}

fn handle_approve_chat_join_request(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ApproveChatJoinRequestRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor_user_id)?;
    }

    let request_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT invite_link, status
             FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, request.user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((invite_link, status)) = request_row else {
        return Err(ApiError::not_found("join request not found"));
    };
    if status != "pending" {
        return Err(ApiError::bad_request("join request is not pending"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'approved', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, request.user_id, now],
    )
    .map_err(ApiError::internal)?;

    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, request.user_id)?;
    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(Value::Bool(true));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    let target_user = ensure_sim_user_record(&mut conn, request.user_id)?;
    let invite = if let Some(raw_link) = invite_link {
        if let Some(record) = load_invite_link_record(&mut conn, bot.id, &chat_key, &raw_link)? {
            Some(chat_invite_link_from_record(
                resolve_invite_creator_user(&mut conn, &bot, record.creator_user_id)?,
                &record,
                None,
            ))
        } else {
            None
        }
    } else {
        None
    };

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &target_user,
        current_status.as_deref(),
        invite,
        Some(true),
    )?;

    Ok(Value::Bool(true))
}

fn handle_decline_chat_join_request(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeclineChatJoinRequestRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor_user_id)?;
    }

    let status: Option<String> = conn
        .query_row(
            "SELECT status
             FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, request.user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(current_status) = status else {
        return Err(ApiError::not_found("join request not found"));
    };
    if current_status != "pending" {
        return Err(ApiError::bad_request("join request is not pending"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'declined', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, request.user_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::Bool(true))
}

fn handle_create_invoice_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateInvoiceLinkRequest = parse_request(params)?;
    let max_tip_amount = request.max_tip_amount.unwrap_or(0);
    let suggested_tip_amounts = request.suggested_tip_amounts.clone().unwrap_or_default();

    let normalized_currency = request.currency.trim().to_ascii_uppercase();
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.description.trim().is_empty() {
        return Err(ApiError::bad_request("description is empty"));
    }
    if request.payload.trim().is_empty() {
        return Err(ApiError::bad_request("payload is empty"));
    }
    if normalized_currency.is_empty() {
        return Err(ApiError::bad_request("currency is empty"));
    }
    if request.prices.is_empty() {
        return Err(ApiError::bad_request("prices must include at least one item"));
    }
    if max_tip_amount < 0 {
        return Err(ApiError::bad_request("max_tip_amount must be non-negative"));
    }
    if let Some(photo_size) = request.photo_size {
        if photo_size <= 0 {
            return Err(ApiError::bad_request("photo_size must be greater than zero"));
        }
    }
    if let Some(photo_width) = request.photo_width {
        if photo_width <= 0 {
            return Err(ApiError::bad_request("photo_width must be greater than zero"));
        }
    }
    if let Some(photo_height) = request.photo_height {
        if photo_height <= 0 {
            return Err(ApiError::bad_request("photo_height must be greater than zero"));
        }
    }

    if request.is_flexible.unwrap_or(false) && !request.need_shipping_address.unwrap_or(false) {
        return Err(ApiError::bad_request("is_flexible requires need_shipping_address=true"));
    }

    if !suggested_tip_amounts.is_empty() {
        if suggested_tip_amounts.len() > 4 {
            return Err(ApiError::bad_request("suggested_tip_amounts can have at most 4 values"));
        }
        if max_tip_amount <= 0 {
            return Err(ApiError::bad_request("max_tip_amount must be positive when suggested_tip_amounts is set"));
        }

        let mut previous = 0;
        for tip in &suggested_tip_amounts {
            if *tip <= 0 {
                return Err(ApiError::bad_request("suggested_tip_amounts values must be greater than zero"));
            }
            if *tip > max_tip_amount {
                return Err(ApiError::bad_request("suggested_tip_amounts values must be <= max_tip_amount"));
            }
            if *tip <= previous {
                return Err(ApiError::bad_request("suggested_tip_amounts must be strictly increasing"));
            }
            previous = *tip;
        }
    }

    let is_stars_invoice = normalized_currency == "XTR";
    let provider_token = request
        .provider_token
        .as_deref()
        .map(str::trim)
        .unwrap_or("");

    if is_stars_invoice {
        if !provider_token.is_empty() {
            return Err(ApiError::bad_request("provider_token must be empty for Telegram Stars invoices"));
        }
        if request.prices.len() != 1 {
            return Err(ApiError::bad_request("prices must contain exactly one item for Telegram Stars invoices"));
        }
        if max_tip_amount > 0 || !suggested_tip_amounts.is_empty() {
            return Err(ApiError::bad_request("tip fields are not supported for Telegram Stars invoices"));
        }
        if request.need_name.unwrap_or(false)
            || request.need_phone_number.unwrap_or(false)
            || request.need_email.unwrap_or(false)
            || request.need_shipping_address.unwrap_or(false)
            || request.send_phone_number_to_provider.unwrap_or(false)
            || request.send_email_to_provider.unwrap_or(false)
            || request.is_flexible.unwrap_or(false)
        {
            return Err(ApiError::bad_request("shipping/contact collection fields are not supported for Telegram Stars invoices"));
        }
    } else if provider_token.is_empty() {
        return Err(ApiError::bad_request("provider_token is required for non-Stars invoices"));
    }

    for price in &request.prices {
        if price.label.trim().is_empty() {
            return Err(ApiError::bad_request("price label is empty"));
        }
        if price.amount <= 0 {
            return Err(ApiError::bad_request("price amount must be greater than zero"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let slug = request
        .payload
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    Ok(json!(format!("https://laragram.local/invoice/{}/{}", bot.id, slug)))
}

fn handle_get_my_star_balance(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetMyStarBalanceRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let balance: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(amount), 0) FROM star_transactions_ledger WHERE bot_id = ?1",
            params![bot.id],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    Ok(json!({
        "amount": balance,
    }))
}

fn handle_get_star_transactions(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetStarTransactionsRequest = parse_request(params)?;
    let offset = request.offset.unwrap_or(0).max(0);
    let limit = request.limit.unwrap_or(20).clamp(1, 100);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, amount, date
             FROM star_transactions_ledger
             WHERE bot_id = ?1
             ORDER BY date DESC
             LIMIT ?2 OFFSET ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, limit, offset], |r| {
            Ok(json!({
                "id": r.get::<_, String>(0)?,
                "amount": r.get::<_, i64>(1)?,
                "date": r.get::<_, i64>(2)?,
            }))
        })
        .map_err(ApiError::internal)?;

    let mut transactions = Vec::new();
    for row in rows {
        transactions.push(row.map_err(ApiError::internal)?);
    }

    Ok(json!({ "transactions": transactions }))
}

fn handle_refund_star_payment(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RefundStarPaymentRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let original_amount: Option<i64> = conn
        .query_row(
            "SELECT amount FROM star_transactions_ledger
             WHERE bot_id = ?1 AND user_id = ?2 AND telegram_payment_charge_id = ?3 AND kind = 'payment'",
            params![bot.id, request.user_id, request.telegram_payment_charge_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(amount) = original_amount else {
        return Err(ApiError::bad_request("star payment not found for refund"));
    };

    let already_refunded: Option<String> = conn
        .query_row(
            "SELECT id FROM star_transactions_ledger
             WHERE bot_id = ?1 AND user_id = ?2 AND telegram_payment_charge_id = ?3 AND kind = 'refund'",
            params![bot.id, request.user_id, request.telegram_payment_charge_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if already_refunded.is_some() {
        return Err(ApiError::bad_request("star payment already refunded"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'refund')",
        params![
            format!("refund_{}", generate_telegram_numeric_id()),
            bot.id,
            request.user_id,
            request.telegram_payment_charge_id,
            -amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_edit_user_star_subscription(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditUserStarSubscriptionRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO star_subscriptions
         (bot_id, user_id, telegram_payment_charge_id, is_canceled, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id, telegram_payment_charge_id)
         DO UPDATE SET is_canceled = excluded.is_canceled, updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            request.telegram_payment_charge_id,
            if request.is_canceled { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_answer_callback_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let mut normalized = params.clone();
    if let Some(raw) = params.get("show_alert") {
        if let Some(loose) = value_to_optional_bool_loose(raw) {
            normalized.insert("show_alert".to_string(), Value::Bool(loose));
        }
    }

    let request: AnswerCallbackQueryRequest = parse_request(&normalized)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let callback_row: Option<String> = conn
        .query_row(
            "SELECT id FROM callback_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.callback_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if callback_row.is_none() {
        return Err(ApiError::not_found("callback query not found"));
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE callback_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.callback_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_answer_web_app_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerWebAppQueryRequest = parse_request(params)?;
    let web_app_query_id = request.web_app_query_id.trim();
    if web_app_query_id.is_empty() {
        return Err(ApiError::bad_request("web_app_query_id is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_web_app_query_answers_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let inline_message_id = generate_telegram_numeric_id();
    let result_json = serde_json::to_string(&request.result).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_web_app_query_answers
         (bot_id, web_app_query_id, inline_message_id, result_json, answered_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, web_app_query_id)
         DO UPDATE SET
            inline_message_id = excluded.inline_message_id,
            result_json = excluded.result_json,
            answered_at = excluded.answered_at",
        params![
            bot.id,
            web_app_query_id,
            inline_message_id,
            result_json,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(SentWebAppMessage {
        inline_message_id: Some(inline_message_id),
    })
    .map_err(ApiError::internal)
}

fn handle_save_prepared_inline_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SavePreparedInlineMessageRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let allow_user_chats = request.allow_user_chats.unwrap_or(true);
    let allow_bot_chats = request.allow_bot_chats.unwrap_or(true);
    let allow_group_chats = request.allow_group_chats.unwrap_or(true);
    let allow_channel_chats = request.allow_channel_chats.unwrap_or(true);

    if !(allow_user_chats || allow_bot_chats || allow_group_chats || allow_channel_chats) {
        return Err(ApiError::bad_request(
            "at least one allow_* chat target must be true",
        ));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_prepared_inline_messages_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let prepared_id = generate_telegram_numeric_id();
    let expiration_date = now + (24 * 60 * 60);

    conn.execute(
        "INSERT INTO sim_prepared_inline_messages
         (bot_id, id, user_id, result_json,
          allow_user_chats, allow_bot_chats, allow_group_chats, allow_channel_chats,
          expiration_date, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
        params![
            bot.id,
            &prepared_id,
            request.user_id,
            serde_json::to_string(&request.result).map_err(ApiError::internal)?,
            if allow_user_chats { 1 } else { 0 },
            if allow_bot_chats { 1 } else { 0 },
            if allow_group_chats { 1 } else { 0 },
            if allow_channel_chats { 1 } else { 0 },
            expiration_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(PreparedInlineMessage {
        id: prepared_id,
        expiration_date,
    })
    .map_err(ApiError::internal)
}

fn handle_save_prepared_keyboard_button(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SavePreparedKeyboardButtonRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.button.text.trim().is_empty() {
        return Err(ApiError::bad_request("button.text is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_prepared_keyboard_buttons_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let prepared_id = generate_telegram_numeric_id();

    conn.execute(
        "INSERT INTO sim_prepared_keyboard_buttons
         (bot_id, id, user_id, button_json, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
        params![
            bot.id,
            &prepared_id,
            request.user_id,
            serde_json::to_string(&request.button).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(PreparedKeyboardButton { id: prepared_id }).map_err(ApiError::internal)
}

fn handle_answer_inline_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerInlineQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT query, from_user_id FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((query_text, from_user_id)) = query_row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE inline_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.inline_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    let cache_time = request.cache_time.unwrap_or(300).max(0);
    let is_personal = request.is_personal.unwrap_or(false);
    let query_offset: String = conn
        .query_row(
            "SELECT offset FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.inline_query_id, bot.id],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    if cache_time > 0 {
        let expires_at = now + cache_time;
        let cache_user_id = if is_personal { from_user_id } else { -1 };
        conn.execute(
            "INSERT INTO inline_query_cache
             (bot_id, query, offset, from_user_id, answer_json, cache_time, expires_at, is_personal, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(bot_id, query, offset, from_user_id)
             DO UPDATE SET answer_json = excluded.answer_json,
                           cache_time = excluded.cache_time,
                           expires_at = excluded.expires_at,
                           is_personal = excluded.is_personal,
                           created_at = excluded.created_at",
            params![
                bot.id,
                query_text,
                query_offset,
                cache_user_id,
                answer_payload.to_string(),
                cache_time,
                expires_at,
                if is_personal { 1 } else { 0 },
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

fn handle_answer_shipping_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerShippingQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<String> = conn
        .query_row(
            "SELECT id FROM shipping_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.shipping_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if query_row.is_none() {
        return Err(ApiError::not_found("shipping query not found"));
    }

    if !request.ok {
        let has_error_message = request
            .error_message
            .as_ref()
            .map(|text| !text.trim().is_empty())
            .unwrap_or(false);
        if !has_error_message {
            return Err(ApiError::bad_request("error_message is required when ok is false"));
        }
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE shipping_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.shipping_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_answer_pre_checkout_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerPreCheckoutQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<String> = conn
        .query_row(
            "SELECT id FROM pre_checkout_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.pre_checkout_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if query_row.is_none() {
        return Err(ApiError::not_found("pre checkout query not found"));
    }

    if !request.ok {
        let has_error_message = request
            .error_message
            .as_ref()
            .map(|text| !text.trim().is_empty())
            .unwrap_or(false);
        if !has_error_message {
            return Err(ApiError::bad_request("error_message is required when ok is false"));
        }
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE pre_checkout_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.pre_checkout_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_me(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let _request: GetMeRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let user = User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name,
        last_name: None,
        username: Some(bot.username),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: Some(true),
        can_read_all_group_messages: Some(false),
        supports_inline_queries: Some(false),
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    Ok(serde_json::to_value(user).map_err(ApiError::internal)?)
}

fn ensure_sim_bot_commands_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_commands (
            bot_id         INTEGER NOT NULL,
            scope_key      TEXT NOT NULL,
            language_code  TEXT NOT NULL,
            commands_json  TEXT NOT NULL,
            updated_at     INTEGER NOT NULL,
            PRIMARY KEY (bot_id, scope_key, language_code),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_bot_profile_texts_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_profile_texts (
            bot_id             INTEGER NOT NULL,
            language_code      TEXT NOT NULL,
            name               TEXT,
            description        TEXT,
            short_description  TEXT,
            updated_at         INTEGER NOT NULL,
            PRIMARY KEY (bot_id, language_code),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_bot_profile_photos_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_profile_photos (
            bot_id        INTEGER PRIMARY KEY,
            file_id       TEXT NOT NULL,
            media_kind    TEXT NOT NULL,
            updated_at    INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_bot_default_admin_rights_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_default_admin_rights (
            bot_id        INTEGER NOT NULL,
            for_channels  INTEGER NOT NULL,
            rights_json   TEXT,
            updated_at    INTEGER NOT NULL,
            PRIMARY KEY (bot_id, for_channels),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_managed_bots_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_managed_bots (
            bot_id          INTEGER NOT NULL,
            owner_user_id   INTEGER NOT NULL,
            managed_bot_id  INTEGER NOT NULL,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL,
            PRIMARY KEY (bot_id, owner_user_id),
            UNIQUE (bot_id, managed_bot_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(managed_bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_prepared_inline_messages_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_prepared_inline_messages (
            bot_id               INTEGER NOT NULL,
            id                   TEXT NOT NULL,
            user_id              INTEGER NOT NULL,
            result_json          TEXT NOT NULL,
            allow_user_chats     INTEGER NOT NULL DEFAULT 1,
            allow_bot_chats      INTEGER NOT NULL DEFAULT 1,
            allow_group_chats    INTEGER NOT NULL DEFAULT 1,
            allow_channel_chats  INTEGER NOT NULL DEFAULT 1,
            expiration_date      INTEGER NOT NULL,
            created_at           INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_prepared_keyboard_buttons_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_prepared_keyboard_buttons (
            bot_id       INTEGER NOT NULL,
            id           TEXT NOT NULL,
            user_id      INTEGER NOT NULL,
            button_json  TEXT NOT NULL,
            created_at   INTEGER NOT NULL,
            updated_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_web_app_query_answers_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_web_app_query_answers (
            bot_id            INTEGER NOT NULL,
            web_app_query_id  TEXT NOT NULL,
            inline_message_id TEXT,
            result_json       TEXT NOT NULL,
            answered_at       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, web_app_query_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_passport_data_errors_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_passport_data_errors (
            bot_id       INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            errors_json  TEXT NOT NULL,
            updated_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_user_emoji_statuses_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_emoji_statuses (
            bot_id                           INTEGER NOT NULL,
            user_id                          INTEGER NOT NULL,
            emoji_status_custom_emoji_id     TEXT,
            emoji_status_expiration_date     INTEGER,
            updated_at                       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_user_profile_photos_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_profile_photos (
            bot_id        INTEGER NOT NULL,
            user_id       INTEGER NOT NULL,
            file_id       TEXT NOT NULL,
            file_unique_id TEXT NOT NULL,
            width         INTEGER NOT NULL,
            height        INTEGER NOT NULL,
            file_size     INTEGER,
            position      INTEGER NOT NULL DEFAULT 0,
            created_at    INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id, file_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_user_profile_photos_order
            ON sim_user_profile_photos (bot_id, user_id, position ASC, created_at DESC, file_id ASC);",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_user_profile_audios_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_profile_audios (
            bot_id         INTEGER NOT NULL,
            user_id        INTEGER NOT NULL,
            file_id        TEXT NOT NULL,
            file_unique_id TEXT NOT NULL,
            duration       INTEGER NOT NULL,
            performer      TEXT,
            title          TEXT,
            file_name      TEXT,
            mime_type      TEXT,
            file_size      INTEGER,
            position       INTEGER NOT NULL DEFAULT 0,
            created_at     INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id, file_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_user_profile_audios_order
            ON sim_user_profile_audios (bot_id, user_id, position ASC, created_at DESC, file_id ASC);",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_user_chat_boosts_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_chat_boosts (
            bot_id           INTEGER NOT NULL,
            chat_key         TEXT NOT NULL,
            user_id          INTEGER NOT NULL,
            boost_id         TEXT NOT NULL,
            add_date         INTEGER NOT NULL,
            expiration_date  INTEGER NOT NULL,
            source_json      TEXT NOT NULL,
            created_at       INTEGER NOT NULL,
            updated_at       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, user_id, boost_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_user_chat_boosts_lookup
            ON sim_user_chat_boosts (bot_id, chat_key, user_id, expiration_date DESC, add_date DESC);",
    )
    .map_err(ApiError::internal)
}

fn ensure_sim_verifications_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_verifications (
            bot_id               INTEGER NOT NULL,
            user_id              INTEGER NOT NULL,
            custom_description   TEXT,
            verified_at          INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE TABLE IF NOT EXISTS sim_chat_verifications (
            bot_id               INTEGER NOT NULL,
            chat_key             TEXT NOT NULL,
            custom_description   TEXT,
            verified_at          INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );",
    )
    .map_err(ApiError::internal)
}

fn normalize_verification_custom_description(
    value: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let normalized = value
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);

    if let Some(text) = normalized.as_deref() {
        if text.chars().count() > 70 {
            return Err(ApiError::bad_request(
                "custom_description must be at most 70 characters",
            ));
        }
    }

    Ok(normalized)
}

fn load_chat_verification_description(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<String>, ApiError> {
    conn.query_row(
        "SELECT custom_description
         FROM sim_chat_verifications
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| row.get(0),
    )
    .optional()
    .map_err(ApiError::internal)
}

fn managed_bot_user_from_record(record: &SimManagedBotRecord) -> User {
    User {
        id: record.managed_bot_id,
        is_bot: true,
        first_name: record.managed_bot_first_name.clone(),
        last_name: None,
        username: Some(record.managed_bot_username.clone()),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: Some(true),
        can_read_all_group_messages: Some(false),
        supports_inline_queries: Some(false),
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    }
}

fn build_user_with_manage_bots(record: &SimUserRecord) -> User {
    let mut user = build_user_from_sim_record(record, false);
    user.can_manage_bots = Some(true);
    user
}

fn load_managed_bot_record(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
) -> Result<Option<SimManagedBotRecord>, ApiError> {
    conn.query_row(
        "SELECT m.owner_user_id, m.managed_bot_id, b.token, b.username, b.first_name, m.created_at, m.updated_at
         FROM sim_managed_bots m
         INNER JOIN bots b ON b.id = m.managed_bot_id
         WHERE m.bot_id = ?1 AND m.owner_user_id = ?2",
        params![manager_bot_id, owner_user_id],
        |row| {
            Ok(SimManagedBotRecord {
                owner_user_id: row.get(0)?,
                managed_bot_id: row.get(1)?,
                managed_token: row.get(2)?,
                managed_bot_username: row.get(3)?,
                managed_bot_first_name: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn create_managed_bot_record(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
    suggested_name: Option<&str>,
    suggested_username: Option<&str>,
) -> Result<SimManagedBotRecord, ApiError> {
    let token = generate_telegram_token();
    let suffix = token_suffix(&token);
    let now = Utc::now().timestamp();

    let first_name = suggested_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("Managed Bot {}", &suffix[..4]));

    let username = suggested_username
        .map(sanitize_username)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("managed_{}", suffix));

    conn.execute(
        "INSERT INTO bots (token, username, first_name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![token, username, first_name, now],
    )
    .map_err(ApiError::internal)?;

    let managed_bot_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO sim_managed_bots (bot_id, owner_user_id, managed_bot_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)",
        params![manager_bot_id, owner_user_id, managed_bot_id, now],
    )
    .map_err(ApiError::internal)?;

    load_managed_bot_record(conn, manager_bot_id, owner_user_id)?
        .ok_or_else(|| ApiError::internal("failed to create managed bot record"))
}

fn ensure_managed_bot_record(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
    suggested_name: Option<&str>,
    suggested_username: Option<&str>,
) -> Result<SimManagedBotRecord, ApiError> {
    if owner_user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    if let Some(existing) = load_managed_bot_record(conn, manager_bot_id, owner_user_id)? {
        return Ok(existing);
    }

    create_managed_bot_record(
        conn,
        manager_bot_id,
        owner_user_id,
        suggested_name,
        suggested_username,
    )
}

fn rotate_managed_bot_token(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
) -> Result<SimManagedBotRecord, ApiError> {
    let current = load_managed_bot_record(conn, manager_bot_id, owner_user_id)?
        .ok_or_else(|| ApiError::not_found("managed bot not found"))?;

    let now = Utc::now().timestamp();
    let new_token = generate_telegram_token();
    conn.execute(
        "UPDATE bots SET token = ?1 WHERE id = ?2",
        params![new_token, current.managed_bot_id],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE sim_managed_bots
         SET updated_at = ?1
         WHERE bot_id = ?2 AND owner_user_id = ?3",
        params![now, manager_bot_id, owner_user_id],
    )
    .map_err(ApiError::internal)?;

    load_managed_bot_record(conn, manager_bot_id, owner_user_id)?
        .ok_or_else(|| ApiError::internal("managed bot update failed"))
}

fn normalize_input_checklist_task(task: &InputChecklistTask) -> Result<ChecklistTask, ApiError> {
    if task.id <= 0 {
        return Err(ApiError::bad_request("checklist task id must be greater than zero"));
    }

    let explicit_entities = task
        .text_entities
        .as_ref()
        .and_then(|value| serde_json::to_value(value).ok());
    let (text, text_entities_value) = parse_formatted_text(
        &task.text,
        task.parse_mode.as_deref(),
        explicit_entities,
    );

    if text.trim().is_empty() {
        return Err(ApiError::bad_request("checklist task text is empty"));
    }
    if text.chars().count() > 300 {
        return Err(ApiError::bad_request("checklist task text is too long"));
    }

    let text_entities = text_entities_value
        .and_then(|value| serde_json::from_value::<Vec<MessageEntity>>(value).ok());

    Ok(ChecklistTask {
        id: task.id,
        text,
        text_entities,
        completed_by_user: None,
        completed_by_chat: None,
        completion_date: None,
    })
}

fn normalize_input_checklist(input: &InputChecklist) -> Result<Checklist, ApiError> {
    let explicit_title_entities = input
        .title_entities
        .as_ref()
        .and_then(|value| serde_json::to_value(value).ok());
    let (title, title_entities_value) = parse_formatted_text(
        &input.title,
        input.parse_mode.as_deref(),
        explicit_title_entities,
    );

    if title.trim().is_empty() {
        return Err(ApiError::bad_request("checklist title is empty"));
    }
    if title.chars().count() > 255 {
        return Err(ApiError::bad_request("checklist title is too long"));
    }
    if input.tasks.is_empty() {
        return Err(ApiError::bad_request("checklist must include at least one task"));
    }
    if input.tasks.len() > 30 {
        return Err(ApiError::bad_request("checklist can include at most 30 tasks"));
    }

    let mut task_ids = HashSet::<i64>::new();
    let mut tasks = Vec::<ChecklistTask>::with_capacity(input.tasks.len());
    for task in &input.tasks {
        if !task_ids.insert(task.id) {
            return Err(ApiError::bad_request("checklist task ids must be unique"));
        }
        tasks.push(normalize_input_checklist_task(task)?);
    }

    let title_entities = title_entities_value
        .and_then(|value| serde_json::from_value::<Vec<MessageEntity>>(value).ok());

    Ok(Checklist {
        title,
        title_entities,
        tasks,
        others_can_add_tasks: input.others_can_add_tasks,
        others_can_mark_tasks_as_done: input.others_can_mark_tasks_as_done,
    })
}

fn normalize_profile_pagination(
    offset: Option<i64>,
    limit: Option<i64>,
) -> Result<(usize, usize), ApiError> {
    let normalized_offset = offset.unwrap_or(0);
    if normalized_offset < 0 {
        return Err(ApiError::bad_request("offset must be non-negative"));
    }

    let normalized_limit = limit.unwrap_or(100);
    if !(1..=100).contains(&normalized_limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }

    Ok((normalized_offset as usize, normalized_limit as usize))
}

fn normalize_bot_language_code(language_code: Option<&str>) -> Result<String, ApiError> {
    let normalized = language_code
        .map(str::trim)
        .unwrap_or("")
        .to_ascii_lowercase();

    if normalized.chars().count() > 32 {
        return Err(ApiError::bad_request(
            "language_code must be at most 32 characters",
        ));
    }

    if !normalized.is_empty()
        && !normalized
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(ApiError::bad_request("language_code is invalid"));
    }

    Ok(normalized)
}

fn normalize_bot_command_scope_key(scope: Option<&BotCommandScope>) -> Result<String, ApiError> {
    let Some(scope) = scope else {
        return Ok("default".to_string());
    };

    let object = scope
        .extra
        .as_object()
        .ok_or_else(|| ApiError::bad_request("scope must be a JSON object"))?;
    let scope_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("scope.type is required"))?;

    match scope_type.as_str() {
        "default" => Ok("default".to_string()),
        "all_private_chats" => Ok("all_private_chats".to_string()),
        "all_group_chats" => Ok("all_group_chats".to_string()),
        "all_chat_administrators" => Ok("all_chat_administrators".to_string()),
        "chat" => {
            let chat_id = object
                .get("chat_id")
                .ok_or_else(|| ApiError::bad_request("scope.chat_id is required"))?;
            let chat_key = value_to_chat_key(chat_id)?;
            Ok(format!("chat:{}", chat_key))
        }
        "chat_administrators" => {
            let chat_id = object
                .get("chat_id")
                .ok_or_else(|| ApiError::bad_request("scope.chat_id is required"))?;
            let chat_key = value_to_chat_key(chat_id)?;
            Ok(format!("chat_administrators:{}", chat_key))
        }
        "chat_member" => {
            let chat_id = object
                .get("chat_id")
                .ok_or_else(|| ApiError::bad_request("scope.chat_id is required"))?;
            let user_id = object
                .get("user_id")
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse::<i64>().ok()))
                })
                .ok_or_else(|| ApiError::bad_request("scope.user_id is required"))?;
            if user_id <= 0 {
                return Err(ApiError::bad_request("scope.user_id must be greater than zero"));
            }
            let chat_key = value_to_chat_key(chat_id)?;
            Ok(format!("chat_member:{}:{}", chat_key, user_id))
        }
        _ => Err(ApiError::bad_request("unsupported scope.type for bot commands")),
    }
}

fn normalize_bot_commands_payload(commands: &[BotCommand]) -> Result<Vec<BotCommand>, ApiError> {
    if commands.is_empty() {
        return Err(ApiError::bad_request("commands must include at least one item"));
    }
    if commands.len() > 100 {
        return Err(ApiError::bad_request("commands must include at most 100 items"));
    }

    let mut seen_commands = HashSet::<String>::new();
    let mut normalized = Vec::<BotCommand>::with_capacity(commands.len());
    for item in commands {
        let command = item.command.trim().to_ascii_lowercase();
        if command.is_empty() {
            return Err(ApiError::bad_request("command is empty"));
        }
        if command.chars().count() > 32 {
            return Err(ApiError::bad_request(
                "command length must be at most 32 characters",
            ));
        }
        if !command
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        {
            return Err(ApiError::bad_request(
                "command must contain only lowercase letters, digits, and underscores",
            ));
        }

        let description = item.description.trim();
        if description.is_empty() {
            return Err(ApiError::bad_request("command description is empty"));
        }
        if description.chars().count() > 256 {
            return Err(ApiError::bad_request(
                "command description length must be at most 256 characters",
            ));
        }

        if !seen_commands.insert(command.clone()) {
            return Err(ApiError::bad_request("duplicate command in commands list"));
        }

        normalized.push(BotCommand {
            command,
            description: description.to_string(),
        });
    }

    Ok(normalized)
}

fn load_bot_profile_text_value(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    language_code: &str,
    column_name: &str,
) -> Result<Option<String>, ApiError> {
    let sql = format!(
        "SELECT {} FROM sim_bot_profile_texts WHERE bot_id = ?1 AND language_code = ?2",
        column_name
    );

    let scoped_value: Option<Option<String>> = conn
        .query_row(&sql, params![bot_id, language_code], |row| row.get(0))
        .optional()
        .map_err(ApiError::internal)?;
    if let Some(value) = scoped_value.flatten() {
        return Ok(Some(value));
    }

    if language_code.is_empty() {
        return Ok(None);
    }

    let default_value: Option<Option<String>> = conn
        .query_row(&sql, params![bot_id, ""], |row| row.get(0))
        .optional()
        .map_err(ApiError::internal)?;

    Ok(default_value.flatten())
}

fn default_bot_administrator_rights(for_channels: bool) -> ChatAdministratorRights {
    ChatAdministratorRights {
        is_anonymous: false,
        can_manage_chat: false,
        can_delete_messages: false,
        can_manage_video_chats: false,
        can_restrict_members: false,
        can_promote_members: false,
        can_change_info: false,
        can_invite_users: false,
        can_post_stories: false,
        can_edit_stories: false,
        can_delete_stories: false,
        can_post_messages: if for_channels { Some(false) } else { None },
        can_edit_messages: if for_channels { Some(false) } else { None },
        can_pin_messages: if for_channels { None } else { Some(false) },
        can_manage_topics: if for_channels { None } else { Some(false) },
        can_manage_direct_messages: Some(false),
        can_manage_tags: Some(false),
    }
}

fn extract_bot_profile_photo_media_input(raw: &Value) -> Result<(&'static str, Value), ApiError> {
    let Some(obj) = raw.as_object() else {
        return Err(ApiError::bad_request(
            "photo must be a valid InputProfilePhoto object",
        ));
    };

    let photo_type = obj
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("photo.type is required"))?;

    match photo_type.as_str() {
        "static" => {
            let photo = obj
                .get("photo")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| ApiError::bad_request("photo.photo is required"))?;
            Ok(("photo", Value::String(photo.to_string())))
        }
        "animated" => {
            let animation = obj
                .get("animation")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| ApiError::bad_request("photo.animation is required"))?;
            Ok(("animation", Value::String(animation.to_string())))
        }
        _ => Err(ApiError::bad_request(
            "photo.type must be one of: static, animated",
        )),
    }
}

fn handle_set_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyCommandsRequest = parse_request(params)?;
    let normalized_commands = normalize_bot_commands_payload(&request.commands)?;
    let scope_key = normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_commands_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_commands (bot_id, scope_key, language_code, commands_json, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, scope_key, language_code)
         DO UPDATE SET
            commands_json = excluded.commands_json,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            &scope_key,
            &language_code,
            serde_json::to_string(&normalized_commands).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyCommandsRequest = parse_request(params)?;
    let scope_key = normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_commands_storage(&mut conn)?;

    let mut commands_json: Option<String> = conn
        .query_row(
            "SELECT commands_json
             FROM sim_bot_commands
             WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ?3",
            params![bot.id, &scope_key, &language_code],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if commands_json.is_none() && !language_code.is_empty() {
        commands_json = conn
            .query_row(
                "SELECT commands_json
                 FROM sim_bot_commands
                 WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ''",
                params![bot.id, &scope_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
    }

    let commands = commands_json
        .map(|raw| serde_json::from_str::<Vec<BotCommand>>(&raw).map_err(ApiError::internal))
        .transpose()?
        .unwrap_or_default();

    Ok(serde_json::to_value(commands).map_err(ApiError::internal)?)
}

fn handle_delete_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMyCommandsRequest = parse_request(params)?;
    let scope_key = normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_commands_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_bot_commands WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ?3",
        params![bot.id, &scope_key, &language_code],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_my_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyNameRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;
    let normalized_name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(name) = normalized_name.as_ref() {
        if name.chars().count() > 64 {
            return Err(ApiError::bad_request(
                "name must be at most 64 characters",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_texts (bot_id, language_code, name, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, language_code)
         DO UPDATE SET
            name = excluded.name,
            updated_at = excluded.updated_at",
        params![bot.id, &language_code, normalized_name.clone(), now],
    )
    .map_err(ApiError::internal)?;

    if language_code.is_empty() {
        if let Some(name) = normalized_name.as_ref() {
            conn.execute(
                "UPDATE bots SET first_name = ?1 WHERE id = ?2",
                params![name, bot.id],
            )
            .map_err(ApiError::internal)?;
        }
    }

    Ok(json!(true))
}

fn handle_get_my_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyNameRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let name = load_bot_profile_text_value(&mut conn, bot.id, &language_code, "name")?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or(bot.first_name);

    Ok(serde_json::to_value(BotName { name }).map_err(ApiError::internal)?)
}

fn handle_set_my_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyDescriptionRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;
    let normalized_description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(description) = normalized_description.as_ref() {
        if description.chars().count() > 512 {
            return Err(ApiError::bad_request(
                "description must be at most 512 characters",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_texts (bot_id, language_code, description, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, language_code)
         DO UPDATE SET
            description = excluded.description,
            updated_at = excluded.updated_at",
        params![bot.id, &language_code, normalized_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_my_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyDescriptionRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let description = load_bot_profile_text_value(&mut conn, bot.id, &language_code, "description")?
        .unwrap_or_default();

    Ok(serde_json::to_value(BotDescription { description }).map_err(ApiError::internal)?)
}

fn handle_set_my_short_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyShortDescriptionRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;
    let normalized_short_description = request
        .short_description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(short_description) = normalized_short_description.as_ref() {
        if short_description.chars().count() > 120 {
            return Err(ApiError::bad_request(
                "short_description must be at most 120 characters",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_texts (bot_id, language_code, short_description, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, language_code)
         DO UPDATE SET
            short_description = excluded.short_description,
            updated_at = excluded.updated_at",
        params![bot.id, &language_code, normalized_short_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_my_short_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyShortDescriptionRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let short_description =
        load_bot_profile_text_value(&mut conn, bot.id, &language_code, "short_description")?
            .unwrap_or_default();

    Ok(serde_json::to_value(BotShortDescription { short_description }).map_err(ApiError::internal)?)
}

fn handle_set_my_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyProfilePhotoRequest = parse_request(params)?;
    let (media_kind, media_input) = extract_bot_profile_photo_media_input(&request.photo.extra)?;
    let file = resolve_media_file(state, token, &media_input, media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_photos_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_photos (bot_id, file_id, media_kind, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id)
         DO UPDATE SET
            file_id = excluded.file_id,
            media_kind = excluded.media_kind,
            updated_at = excluded.updated_at",
        params![bot.id, file.file_id, media_kind, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_remove_my_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: RemoveMyProfilePhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_photos_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_bot_profile_photos WHERE bot_id = ?1",
        params![bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_my_default_administrator_rights(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyDefaultAdministratorRightsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_default_admin_rights_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let for_channels = request.for_channels.unwrap_or(false);
    let rights_json = request
        .rights
        .as_ref()
        .map(|rights| serde_json::to_string(rights).map_err(ApiError::internal))
        .transpose()?;

    conn.execute(
        "INSERT INTO sim_bot_default_admin_rights (bot_id, for_channels, rights_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, for_channels)
         DO UPDATE SET
            rights_json = excluded.rights_json,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            if for_channels { 1 } else { 0 },
            rights_json,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_my_default_administrator_rights(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyDefaultAdministratorRightsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_default_admin_rights_storage(&mut conn)?;

    let for_channels = request.for_channels.unwrap_or(false);
    let raw_rights: Option<Option<String>> = conn
        .query_row(
            "SELECT rights_json
             FROM sim_bot_default_admin_rights
             WHERE bot_id = ?1 AND for_channels = ?2",
            params![bot.id, if for_channels { 1 } else { 0 }],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let rights = raw_rights
        .flatten()
        .and_then(|raw| serde_json::from_str::<ChatAdministratorRights>(&raw).ok())
        .unwrap_or_else(|| default_bot_administrator_rights(for_channels));

    Ok(serde_json::to_value(rights).map_err(ApiError::internal)?)
}

fn extract_business_profile_photo_media_input(raw: &Value) -> Result<Value, ApiError> {
    let Some(obj) = raw.as_object() else {
        return Err(ApiError::bad_request("photo must be a valid InputProfilePhoto object"));
    };

    if let Some(photo) = obj.get("photo").and_then(Value::as_str) {
        let trimmed = photo.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("photo is empty"));
        }
        return Ok(Value::String(trimmed.to_string()));
    }

    if let Some(animation) = obj.get("animation").and_then(Value::as_str) {
        let trimmed = animation.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("animation is empty"));
        }
        return Ok(Value::String(trimmed.to_string()));
    }

    Err(ApiError::bad_request("photo must contain photo or animation"))
}

fn load_business_connection_or_404(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    business_connection_id: &str,
) -> Result<SimBusinessConnectionRecord, ApiError> {
    let normalized = normalize_business_connection_id(Some(business_connection_id))
        .ok_or_else(|| ApiError::bad_request("business_connection_id is empty"))?;
    load_sim_business_connection_by_id(conn, bot_id, &normalized)?
        .ok_or_else(|| ApiError::not_found("business connection not found"))
}

fn resolve_story_business_connection_for_request(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    raw_business_connection_id: Option<&str>,
) -> Result<(SimBusinessConnectionRecord, BusinessConnection), ApiError> {
    let normalized_connection_id = normalize_business_connection_id(raw_business_connection_id);
    let record = if let Some(connection_id) = normalized_connection_id.as_deref() {
        load_business_connection_or_404(conn, bot_id, connection_id)?
    } else {
        let actor_user_id = current_request_actor_user_id().ok_or_else(|| {
            ApiError::bad_request(
                "business_connection_id is required when actor user context is unavailable",
            )
        })?;
        let actor_user = ensure_user(conn, Some(actor_user_id), None, None)?;
        let loaded = match load_sim_business_connection_for_user(conn, bot_id, actor_user.id)? {
            Some(existing) => existing,
            None => {
                let default_connection_id = default_business_connection_id(bot_id, actor_user.id);
                upsert_sim_business_connection(
                    conn,
                    bot_id,
                    &default_connection_id,
                    actor_user.id,
                    actor_user.id,
                    &default_business_bot_rights(),
                    true,
                )?
            }
        };

        let mut rights = parse_business_bot_rights_json(&loaded.rights_json);
        let mut should_upsert = false;
        if rights.can_manage_stories != Some(true) {
            rights.can_manage_stories = Some(true);
            should_upsert = true;
        }
        if !loaded.is_enabled {
            should_upsert = true;
        }

        if should_upsert {
            upsert_sim_business_connection(
                conn,
                bot_id,
                &loaded.connection_id,
                loaded.user_id,
                loaded.user_chat_id,
                &rights,
                true,
            )?
        } else {
            loaded
        }
    };

    if !record.is_enabled {
        return Err(ApiError::bad_request("business connection is disabled"));
    }

    let connection = build_business_connection(conn, bot_id, &record)?;
    if normalized_connection_id.is_some() {
        ensure_business_right(
            &connection,
            |rights| rights.can_manage_stories,
            "not enough rights to manage stories",
        )?;
    }

    Ok((record, connection))
}

fn resolve_outbound_business_connection_for_bot_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat: &Chat,
    raw_business_connection_id: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let Some(connection_id) = normalize_business_connection_id(raw_business_connection_id) else {
        return Ok(None);
    };

    let record = load_business_connection_or_404(conn, bot_id, &connection_id)?;
    if !record.is_enabled {
        return Err(ApiError::bad_request("business connection is disabled"));
    }

    if chat.r#type != "private" || chat.id != record.user_chat_id {
        return Err(ApiError::bad_request(
            "business connection does not match target private chat",
        ));
    }

    let connection = build_business_connection(conn, bot_id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_reply,
        "not enough rights to send business messages",
    )?;

    Ok(Some(record.connection_id))
}

fn handle_get_business_connection(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetBusinessConnectionRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    serde_json::to_value(connection).map_err(ApiError::internal)
}

fn handle_get_managed_bot_token(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetManagedBotTokenRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_managed_bots_storage(&mut conn)?;

    let owner = ensure_sim_user_record(&mut conn, request.user_id)?;
    let _ = ensure_managed_bot_record(&mut conn, bot.id, owner.id, None, None)?;
    Ok(json!(true))
}

fn handle_replace_managed_bot_token(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReplaceManagedBotTokenRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_managed_bots_storage(&mut conn)?;

    let owner = ensure_sim_user_record(&mut conn, request.user_id)?;
    let _ = ensure_managed_bot_record(&mut conn, bot.id, owner.id, None, None)?;
    let record = rotate_managed_bot_token(&mut conn, bot.id, owner.id)?;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: Some(ManagedBotUpdated {
            user: build_user_with_manage_bots(&owner),
            bot: managed_bot_user_from_record(&record),
        }),
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(json!(true))
}

fn handle_read_business_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReadBusinessMessageRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_read_messages,
        "not enough rights to read business messages",
    )?;

    if request.chat_id != record.user_chat_id {
        return Err(ApiError::bad_request("chat_id does not belong to the business connection"));
    }

    let chat_key = request.chat_id.to_string();
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, request.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_business_read_messages (bot_id, connection_id, chat_id, message_id, read_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, connection_id, chat_id, message_id)
         DO UPDATE SET read_at = excluded.read_at",
        params![bot.id, record.connection_id, request.chat_id, request.message_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_delete_business_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteBusinessMessagesRequest = parse_request(params)?;
    if request.message_ids.is_empty() {
        return Err(ApiError::bad_request("message_ids must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;

    let can_delete_sent = business_right_enabled(&connection.rights, |rights| rights.can_delete_sent_messages);
    let can_delete_all = business_right_enabled(&connection.rights, |rights| rights.can_delete_all_messages);
    if !can_delete_sent && !can_delete_all {
        return Err(ApiError::bad_request("not enough rights to delete business messages"));
    }

    let chat_key = record.user_chat_id.to_string();
    let mut deleted_ids: Vec<i64> = Vec::new();
    for message_id in &request.message_ids {
        conn.execute(
            "DELETE FROM sim_message_drafts WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, message_id],
        )
        .map_err(ApiError::internal)?;

        let deleted = conn
            .execute(
                "DELETE FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
                params![bot.id, &chat_key, message_id],
            )
            .map_err(ApiError::internal)?;
        if deleted > 0 {
            deleted_ids.push(*message_id);
        }
    }

    if !deleted_ids.is_empty() {
        let user_record = ensure_sim_user_record(&mut conn, record.user_id)?;
        let chat = Chat {
            id: record.user_chat_id,
            r#type: "private".to_string(),
            title: None,
            username: user_record.username.clone(),
            first_name: Some(user_record.first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        };
        let deleted_payload = BusinessMessagesDeleted {
            business_connection_id: record.connection_id.clone(),
            chat,
            message_ids: deleted_ids,
        };

        let update_value = serde_json::to_value(Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: Some(deleted_payload),
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
            managed_bot: None,
        })
        .map_err(ApiError::internal)?;
        persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    }

    Ok(json!(true))
}

fn handle_set_business_account_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountNameRequest = parse_request(params)?;
    let first_name = request.first_name.trim();
    if first_name.is_empty() {
        return Err(ApiError::bad_request("first_name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_edit_name,
        "not enough rights to edit business account name",
    )?;

    conn.execute(
        "UPDATE users SET first_name = ?1 WHERE id = ?2",
        params![first_name, record.user_id],
    )
    .map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET last_name = excluded.last_name, updated_at = excluded.updated_at",
        params![bot.id, record.user_id, request.last_name, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_business_account_username(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountUsernameRequest = parse_request(params)?;
    if let Some(username) = request.username.as_deref() {
        if username.trim().is_empty() {
            return Err(ApiError::bad_request("username is empty"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_edit_username,
        "not enough rights to edit business account username",
    )?;

    conn.execute(
        "UPDATE users SET username = ?1 WHERE id = ?2",
        params![request.username, record.user_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_business_account_bio(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountBioRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_edit_bio,
        "not enough rights to edit business account bio",
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
         VALUES (?1, ?2, NULL, ?3, NULL, NULL, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET bio = excluded.bio, updated_at = excluded.updated_at",
        params![bot.id, record.user_id, request.bio, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_business_account_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountProfilePhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_edit_profile_photo,
        "not enough rights to edit business account profile photo",
    )?;

    let photo_input = extract_business_profile_photo_media_input(&request.photo.extra)?;
    let file = resolve_media_file_with_conn(&mut conn, bot.id, &photo_input, "photo")?;
    let is_public = request.is_public.unwrap_or(false);
    let private_photo_file_id = if is_public {
        None
    } else {
        Some(file.file_id.clone())
    };
    let public_photo_file_id = if is_public {
        Some(file.file_id.clone())
    } else {
        None
    };
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
         VALUES (?1, ?2, NULL, NULL, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET
            profile_photo_file_id = CASE WHEN ?6 = 1 THEN sim_business_account_profiles.profile_photo_file_id ELSE excluded.profile_photo_file_id END,
            public_profile_photo_file_id = CASE WHEN ?6 = 1 THEN excluded.public_profile_photo_file_id ELSE sim_business_account_profiles.public_profile_photo_file_id END,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            record.user_id,
            private_photo_file_id,
            public_photo_file_id,
            now,
            if is_public { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_remove_business_account_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RemoveBusinessAccountProfilePhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_edit_profile_photo,
        "not enough rights to edit business account profile photo",
    )?;

    let is_public = request.is_public.unwrap_or(false);
    let now = Utc::now().timestamp();
    if is_public {
        conn.execute(
            "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
             VALUES (?1, ?2, NULL, NULL, NULL, NULL, ?3)
             ON CONFLICT(bot_id, user_id)
             DO UPDATE SET public_profile_photo_file_id = NULL, updated_at = excluded.updated_at",
            params![bot.id, record.user_id, now],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
             VALUES (?1, ?2, NULL, NULL, NULL, NULL, ?3)
             ON CONFLICT(bot_id, user_id)
             DO UPDATE SET profile_photo_file_id = NULL, updated_at = excluded.updated_at",
            params![bot.id, record.user_id, now],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

fn handle_set_business_account_gift_settings(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountGiftSettingsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_change_gift_settings,
        "not enough rights to edit business gift settings",
    )?;

    let accepted_types_json = serde_json::to_string(&request.accepted_gift_types).map_err(ApiError::internal)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_business_connections
         SET gift_settings_show_button = ?1,
             gift_settings_types_json = ?2,
             updated_at = ?3
         WHERE bot_id = ?4 AND connection_id = ?5",
        params![
            if request.show_gift_button { 1 } else { 0 },
            accepted_types_json,
            now,
            bot.id,
            record.connection_id,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_business_account_star_balance(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetBusinessAccountStarBalanceRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_view_gifts_and_stars,
        "not enough rights to view business stars",
    )?;

    let result = StarAmount {
        amount: record.star_balance.max(0),
        nanostar_amount: None,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

fn handle_transfer_business_account_stars(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: TransferBusinessAccountStarsRequest = parse_request(params)?;
    if request.star_count <= 0 {
        return Err(ApiError::bad_request("star_count must be greater than zero"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_transfer_stars,
        "not enough rights to transfer business stars",
    )?;

    if request.star_count > record.star_balance {
        return Err(ApiError::bad_request("not enough stars in business account balance"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_business_connections
         SET star_balance = star_balance - ?1,
             updated_at = ?2
         WHERE bot_id = ?3 AND connection_id = ?4",
        params![request.star_count, now, bot.id, record.connection_id],
    )
    .map_err(ApiError::internal)?;

    let transfer_charge_id = format!(
        "business_transfer_{}_{}",
        request.business_connection_id,
        generate_telegram_numeric_id(),
    );
    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'business_transfer')",
        params![
            format!("business_transfer_{}", generate_telegram_numeric_id()),
            bot.id,
            record.user_id,
            transfer_charge_id,
            request.star_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_business_account_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetBusinessAccountGiftsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_view_gifts_and_stars,
        "not enough rights to view business gifts",
    )?;

    let _gift_types = parse_business_accepted_gift_types_json(record.gift_settings_types_json.as_deref());
    let _show_gift_button = record.gift_settings_show_button;

    let result = OwnedGifts {
        total_count: 0,
        gifts: Vec::<OwnedGift>::new(),
        next_offset: None,
    };
    serde_json::to_value(result).map_err(ApiError::internal)
}

#[derive(Debug, Clone)]
struct SimGiftCatalogEntry {
    gift: Gift,
    is_unlimited: bool,
    is_from_blockchain: bool,
}

#[derive(Debug, Clone)]
struct SimOwnedGiftRecord {
    owned_gift_id: String,
    sender_user_id: Option<i64>,
    gift_id: String,
    gift_json: String,
    gift_star_count: i64,
    is_unique: bool,
    is_unlimited: bool,
    is_from_blockchain: bool,
    send_date: i64,
    text: Option<String>,
    entities_json: Option<String>,
    is_private: bool,
    is_saved: bool,
    can_be_upgraded: bool,
    was_refunded: bool,
    convert_star_count: Option<i64>,
    prepaid_upgrade_star_count: Option<i64>,
    is_upgrade_separate: bool,
    unique_gift_number: Option<i64>,
    transfer_star_count: Option<i64>,
    next_transfer_date: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct SimOwnedGiftFilterOptions {
    exclude_unsaved: bool,
    exclude_saved: bool,
    exclude_unlimited: bool,
    exclude_limited_upgradable: bool,
    exclude_limited_non_upgradable: bool,
    exclude_from_blockchain: bool,
    exclude_unique: bool,
    sort_by_price: bool,
}

fn build_sim_gift_sticker(gift_id: &str, emoji: &str, set_name: &str) -> Sticker {
    Sticker {
        file_id: format!("gift_sticker_{}", gift_id),
        file_unique_id: generate_telegram_file_unique_id(),
        r#type: "regular".to_string(),
        width: 512,
        height: 512,
        is_animated: false,
        is_video: false,
        thumbnail: None,
        emoji: Some(emoji.to_string()),
        set_name: Some(set_name.to_string()),
        premium_animation: None,
        mask_position: None,
        custom_emoji_id: None,
        needs_repainting: None,
        file_size: Some(1024),
    }
}

fn build_sim_catalog_gift(
    gift_id: &str,
    star_count: i64,
    upgrade_star_count: Option<i64>,
    is_premium: bool,
    is_unlimited: bool,
    is_from_blockchain: bool,
    emoji: &str,
    set_name: &str,
) -> SimGiftCatalogEntry {
    let total_count = if is_unlimited { None } else { Some(20_000) };
    let remaining_count = if is_unlimited { None } else { Some(13_000) };
    SimGiftCatalogEntry {
        gift: Gift {
            id: gift_id.to_string(),
            sticker: build_sim_gift_sticker(gift_id, emoji, set_name),
            star_count,
            upgrade_star_count,
            is_premium: Some(is_premium),
            has_colors: Some(true),
            total_count,
            remaining_count,
            personal_total_count: if is_unlimited { None } else { Some(3) },
            personal_remaining_count: if is_unlimited { None } else { Some(3) },
            background: Some(GiftBackground {
                center_color: 0x7EC8FF,
                edge_color: 0x285B8C,
                text_color: 0xFFFFFF,
            }),
            unique_gift_variant_count: if is_unlimited { None } else { Some(120) },
            publisher_chat: None,
        },
        is_unlimited,
        is_from_blockchain,
    }
}

fn sim_available_gift_catalog() -> Vec<SimGiftCatalogEntry> {
    vec![
        build_sim_catalog_gift(
            "gift_rose",
            45,
            Some(120),
            false,
            false,
            false,
            "🌹",
            "laragram_gifts",
        ),
        build_sim_catalog_gift(
            "gift_star_box",
            120,
            Some(240),
            false,
            true,
            false,
            "🎁",
            "laragram_gifts",
        ),
        build_sim_catalog_gift(
            "gift_premium_badge",
            950,
            None,
            true,
            false,
            false,
            "💎",
            "laragram_gifts",
        ),
    ]
}

fn find_sim_catalog_gift(gift_id: &str) -> Option<SimGiftCatalogEntry> {
    sim_available_gift_catalog()
        .into_iter()
        .find(|entry| entry.gift.id == gift_id)
}

fn fallback_sim_gift(gift_id: &str) -> Gift {
    find_sim_catalog_gift(gift_id)
        .map(|entry| entry.gift)
        .unwrap_or_else(|| {
            build_sim_catalog_gift(
                gift_id,
                100,
                Some(200),
                false,
                true,
                false,
                "🎁",
                "laragram_gifts",
            )
            .gift
        })
}

fn parse_owned_gifts_offset(offset: Option<&str>) -> usize {
    offset
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

fn parse_owned_gifts_limit(limit: Option<i64>) -> usize {
    limit.unwrap_or(20).clamp(1, 100) as usize
}

fn load_bot_star_balance(conn: &mut rusqlite::Connection, bot_id: i64) -> Result<i64, ApiError> {
    conn.query_row(
        "SELECT COALESCE(SUM(amount), 0) FROM star_transactions_ledger WHERE bot_id = ?1",
        params![bot_id],
        |row| row.get(0),
    )
    .map_err(ApiError::internal)
}

fn ensure_bot_star_balance_for_charge(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    required_star_count: i64,
    now: i64,
) -> Result<(), ApiError> {
    if required_star_count <= 0 {
        return Ok(());
    }

    let current_balance = load_bot_star_balance(conn, bot_id)?;
    if current_balance >= required_star_count {
        return Ok(());
    }

    let top_up_amount = required_star_count.saturating_sub(current_balance);
    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'simulator_bot_topup')",
        params![
            format!("sim_topup_{}", generate_telegram_numeric_id()),
            bot_id,
            bot_id,
            format!("sim_topup_charge_{}", generate_telegram_numeric_id()),
            top_up_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn load_owned_gift_records(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    owner_user_id: Option<i64>,
    owner_chat_id: Option<i64>,
) -> Result<Vec<SimOwnedGiftRecord>, ApiError> {
    let mut records = Vec::new();

    if let Some(user_id) = owner_user_id {
        let mut stmt = conn
            .prepare(
                "SELECT owned_gift_id, sender_user_id, gift_id, gift_json, gift_star_count,
                        is_unique, is_unlimited, is_from_blockchain,
                        send_date, text, entities_json,
                        is_private, is_saved, can_be_upgraded, was_refunded,
                        convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
                        unique_gift_number, transfer_star_count, next_transfer_date
                 FROM sim_owned_gifts
                 WHERE bot_id = ?1 AND owner_user_id = ?2
                 ORDER BY send_date DESC, owned_gift_id DESC",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![bot_id, user_id], |row| {
                Ok(SimOwnedGiftRecord {
                    owned_gift_id: row.get(0)?,
                    sender_user_id: row.get(1)?,
                    gift_id: row.get(2)?,
                    gift_json: row.get(3)?,
                    gift_star_count: row.get(4)?,
                    is_unique: row.get::<_, i64>(5)? == 1,
                    is_unlimited: row.get::<_, i64>(6)? == 1,
                    is_from_blockchain: row.get::<_, i64>(7)? == 1,
                    send_date: row.get(8)?,
                    text: row.get(9)?,
                    entities_json: row.get(10)?,
                    is_private: row.get::<_, i64>(11)? == 1,
                    is_saved: row.get::<_, i64>(12)? == 1,
                    can_be_upgraded: row.get::<_, i64>(13)? == 1,
                    was_refunded: row.get::<_, i64>(14)? == 1,
                    convert_star_count: row.get(15)?,
                    prepaid_upgrade_star_count: row.get(16)?,
                    is_upgrade_separate: row.get::<_, i64>(17)? == 1,
                    unique_gift_number: row.get(18)?,
                    transfer_star_count: row.get(19)?,
                    next_transfer_date: row.get(20)?,
                })
            })
            .map_err(ApiError::internal)?;

        for row in rows {
            records.push(row.map_err(ApiError::internal)?);
        }

        return Ok(records);
    }

    if let Some(chat_id) = owner_chat_id {
        let mut stmt = conn
            .prepare(
                "SELECT owned_gift_id, sender_user_id, gift_id, gift_json, gift_star_count,
                        is_unique, is_unlimited, is_from_blockchain,
                        send_date, text, entities_json,
                        is_private, is_saved, can_be_upgraded, was_refunded,
                        convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
                        unique_gift_number, transfer_star_count, next_transfer_date
                 FROM sim_owned_gifts
                 WHERE bot_id = ?1 AND owner_chat_id = ?2
                 ORDER BY send_date DESC, owned_gift_id DESC",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![bot_id, chat_id], |row| {
                Ok(SimOwnedGiftRecord {
                    owned_gift_id: row.get(0)?,
                    sender_user_id: row.get(1)?,
                    gift_id: row.get(2)?,
                    gift_json: row.get(3)?,
                    gift_star_count: row.get(4)?,
                    is_unique: row.get::<_, i64>(5)? == 1,
                    is_unlimited: row.get::<_, i64>(6)? == 1,
                    is_from_blockchain: row.get::<_, i64>(7)? == 1,
                    send_date: row.get(8)?,
                    text: row.get(9)?,
                    entities_json: row.get(10)?,
                    is_private: row.get::<_, i64>(11)? == 1,
                    is_saved: row.get::<_, i64>(12)? == 1,
                    can_be_upgraded: row.get::<_, i64>(13)? == 1,
                    was_refunded: row.get::<_, i64>(14)? == 1,
                    convert_star_count: row.get(15)?,
                    prepaid_upgrade_star_count: row.get(16)?,
                    is_upgrade_separate: row.get::<_, i64>(17)? == 1,
                    unique_gift_number: row.get(18)?,
                    transfer_star_count: row.get(19)?,
                    next_transfer_date: row.get(20)?,
                })
            })
            .map_err(ApiError::internal)?;

        for row in rows {
            records.push(row.map_err(ApiError::internal)?);
        }
    }

    Ok(records)
}

fn apply_owned_gift_filters(
    mut records: Vec<SimOwnedGiftRecord>,
    options: &SimOwnedGiftFilterOptions,
) -> Vec<SimOwnedGiftRecord> {
    records.retain(|record| {
        if options.exclude_unique && record.is_unique {
            return false;
        }
        if options.exclude_unsaved && !record.is_saved {
            return false;
        }
        if options.exclude_saved && record.is_saved {
            return false;
        }
        if options.exclude_unlimited && record.is_unlimited {
            return false;
        }

        let is_limited = !record.is_unlimited;
        if options.exclude_limited_upgradable && is_limited && record.can_be_upgraded {
            return false;
        }
        if options.exclude_limited_non_upgradable && is_limited && !record.can_be_upgraded {
            return false;
        }
        if options.exclude_from_blockchain && record.is_from_blockchain {
            return false;
        }

        true
    });

    if options.sort_by_price {
        records.sort_by(|a, b| {
            b.gift_star_count
                .cmp(&a.gift_star_count)
                .then_with(|| b.send_date.cmp(&a.send_date))
                .then_with(|| b.owned_gift_id.cmp(&a.owned_gift_id))
        });
    } else {
        records.sort_by(|a, b| {
            b.send_date
                .cmp(&a.send_date)
                .then_with(|| b.owned_gift_id.cmp(&a.owned_gift_id))
        });
    }

    records
}

fn map_owned_gift_record(
    conn: &mut rusqlite::Connection,
    record: &SimOwnedGiftRecord,
) -> Result<OwnedGift, ApiError> {
    let gift = serde_json::from_str::<Gift>(&record.gift_json)
        .unwrap_or_else(|_| fallback_sim_gift(&record.gift_id));
    let entities = record
        .entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<MessageEntity>>(raw).ok());

    let sender_user = if let Some(sender_user_id) = record.sender_user_id {
        load_sim_user_record(conn, sender_user_id)?
            .map(|user| build_user_from_sim_record(&user, false))
    } else {
        None
    };

    let mut payload = Map::<String, Value>::new();
    payload.insert("type".to_string(), Value::String("regular".to_string()));
    payload.insert(
        "gift".to_string(),
        serde_json::to_value(gift).map_err(ApiError::internal)?,
    );
    payload.insert(
        "owned_gift_id".to_string(),
        Value::String(record.owned_gift_id.clone()),
    );
    payload.insert("send_date".to_string(), Value::from(record.send_date));
    payload.insert("is_private".to_string(), Value::Bool(record.is_private));
    payload.insert("is_saved".to_string(), Value::Bool(record.is_saved));
    payload.insert(
        "can_be_upgraded".to_string(),
        Value::Bool(record.can_be_upgraded),
    );
    payload.insert("was_refunded".to_string(), Value::Bool(record.was_refunded));
    payload.insert(
        "is_upgrade_separate".to_string(),
        Value::Bool(record.is_upgrade_separate),
    );

    if let Some(sender) = sender_user {
        payload.insert(
            "sender_user".to_string(),
            serde_json::to_value(sender).map_err(ApiError::internal)?,
        );
    }
    if let Some(text) = record.text.as_ref() {
        payload.insert("text".to_string(), Value::String(text.clone()));
    }
    if let Some(entities) = entities {
        payload.insert(
            "entities".to_string(),
            serde_json::to_value(entities).map_err(ApiError::internal)?,
        );
    }
    if let Some(value) = record.convert_star_count {
        payload.insert("convert_star_count".to_string(), Value::from(value));
    }
    if let Some(value) = record.prepaid_upgrade_star_count {
        payload.insert("prepaid_upgrade_star_count".to_string(), Value::from(value));
    }
    if let Some(value) = record.unique_gift_number {
        payload.insert("unique_gift_number".to_string(), Value::from(value));
    }
    if let Some(value) = record.transfer_star_count {
        payload.insert("transfer_star_count".to_string(), Value::from(value));
    }
    if let Some(value) = record.next_transfer_date {
        payload.insert("next_transfer_date".to_string(), Value::from(value));
    }

    Ok(OwnedGift {
        extra: Value::Object(payload),
    })
}

fn handle_get_available_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetAvailableGiftsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let _bot = ensure_bot(&mut conn, token)?;

    let result = Gifts {
        gifts: sim_available_gift_catalog()
            .into_iter()
            .map(|entry| entry.gift)
            .collect(),
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

fn handle_send_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendGiftRequest = parse_request(params)?;

    if request.user_id.is_some() == request.chat_id.is_some() {
        return Err(ApiError::bad_request(
            "exactly one of user_id or chat_id must be provided",
        ));
    }

    let gift_entry = find_sim_catalog_gift(request.gift_id.as_str())
        .ok_or_else(|| ApiError::bad_request("gift_id is invalid"))?;

    let base_star_count = gift_entry.gift.star_count.max(0);
    let upgrade_star_count = gift_entry.gift.upgrade_star_count.unwrap_or(0).max(0);
    let pay_for_upgrade = request.pay_for_upgrade.unwrap_or(false);

    if pay_for_upgrade && upgrade_star_count <= 0 {
        return Err(ApiError::bad_request(
            "selected gift doesn't support prepaid upgrade",
        ));
    }

    let charge_star_count = if pay_for_upgrade {
        base_star_count.saturating_add(upgrade_star_count)
    } else {
        base_star_count
    };
    if charge_star_count <= 0 {
        return Err(ApiError::bad_request("gift star_count is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sender = ensure_user(&mut conn, current_request_actor_user_id(), None, None)?;

    let mut owner_user_id: Option<i64> = None;
    let mut owner_chat_id: Option<i64> = None;
    let mut gift_message_chat: Option<(String, Chat)> = None;

    if let Some(user_id) = request.user_id {
        if user_id <= 0 {
            return Err(ApiError::bad_request("user_id is invalid"));
        }
        let recipient = ensure_user(&mut conn, Some(user_id), None, None)?;
        owner_user_id = Some(recipient.id);

        let chat_key = recipient.id.to_string();
        ensure_chat(&mut conn, &chat_key)?;
        gift_message_chat = Some((
            chat_key,
            Chat {
                id: recipient.id,
                r#type: "private".to_string(),
                title: None,
                username: recipient.username.clone(),
                first_name: Some(recipient.first_name.clone()),
                last_name: recipient.last_name.clone(),
                is_forum: None,
                is_direct_messages: None,
            },
        ));
    } else if let Some(chat_value) = request.chat_id.as_ref() {
        let chat_key = value_to_chat_key(chat_value)?;
        let chat_id = chat_id_as_i64(chat_value, &chat_key);
        let sim_chat = load_sim_chat_record(&mut conn, bot.id, &chat_key)?
            .or(load_sim_chat_record_by_chat_id(&mut conn, bot.id, chat_id)?)
            .ok_or_else(|| ApiError::not_found("chat not found"))?;

        if sim_chat.chat_type != "channel" {
            return Err(ApiError::bad_request(
                "chat_id must refer to a channel chat",
            ));
        }

        owner_chat_id = Some(sim_chat.chat_id);
        ensure_chat(&mut conn, &sim_chat.chat_key)?;
        gift_message_chat = Some((
            sim_chat.chat_key.clone(),
            build_chat_from_group_record(&sim_chat),
        ));
    }

    let now = Utc::now().timestamp();
    ensure_bot_star_balance_for_charge(&mut conn, bot.id, charge_star_count, now)?;

    let owned_gift_id = format!("owned_gift_{}", generate_telegram_numeric_id());
    let ledger_user_id = owner_user_id.unwrap_or(sender.id);
    let ledger_charge_id = format!("gift_send_{}", owned_gift_id);
    let text = request
        .text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let gift_message_text = text.clone();
    let entities_json = request
        .text_entities
        .as_ref()
        .map(|entities| serde_json::to_string(entities).map_err(ApiError::internal))
        .transpose()?;
    let gift_json = serde_json::to_string(&gift_entry.gift).map_err(ApiError::internal)?;
    let can_be_upgraded = upgrade_star_count > 0;

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_send')",
        params![
            format!("gift_send_{}", generate_telegram_numeric_id()),
            bot.id,
            ledger_user_id,
            ledger_charge_id,
            -charge_star_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_owned_gifts
         (bot_id, owned_gift_id, owner_user_id, owner_chat_id, sender_user_id,
          gift_id, gift_json, gift_star_count, is_unique, is_unlimited, is_from_blockchain,
          send_date, text, entities_json, is_private, is_saved, can_be_upgraded, was_refunded,
          convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
          unique_gift_number, transfer_star_count, next_transfer_date, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5,
                 ?6, ?7, ?8, 0, ?9, ?10,
                 ?11, ?12, ?13, 0, 0, ?14, 0,
                 ?15, ?16, ?17,
                 NULL, NULL, NULL, ?18, ?18)",
        params![
            bot.id,
            owned_gift_id.clone(),
            owner_user_id,
            owner_chat_id,
            sender.id,
            gift_entry.gift.id,
            gift_json,
            gift_entry.gift.star_count,
            if gift_entry.is_unlimited { 1 } else { 0 },
            if gift_entry.is_from_blockchain { 1 } else { 0 },
            now,
            text.clone(),
            entities_json,
            if can_be_upgraded { 1 } else { 0 },
            if can_be_upgraded {
                Some((gift_entry.gift.star_count / 2).max(1))
            } else {
                None
            },
            if pay_for_upgrade {
                Some(upgrade_star_count)
            } else {
                None
            },
            if can_be_upgraded && !pay_for_upgrade { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    if let Some(target_user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = COALESCE(gift_count, 0) + 1
             WHERE id = ?1",
            params![target_user_id],
        )
        .map_err(ApiError::internal)?;
    }

    if let Some((chat_key, chat)) = gift_message_chat {
        let sender_user = build_user_from_sim_record(&sender, false);
        let mut gift_payload = Map::<String, Value>::new();
        gift_payload.insert(
            "gift".to_string(),
            serde_json::to_value(&gift_entry.gift).map_err(ApiError::internal)?,
        );
        gift_payload.insert(
            "owned_gift_id".to_string(),
            Value::String(owned_gift_id.clone()),
        );
        gift_payload.insert(
            "can_be_upgraded".to_string(),
            Value::Bool(can_be_upgraded),
        );
        if can_be_upgraded {
            gift_payload.insert(
                "convert_star_count".to_string(),
                Value::from((gift_entry.gift.star_count / 2).max(1)),
            );
        }
        if pay_for_upgrade {
            gift_payload.insert(
                "prepaid_upgrade_star_count".to_string(),
                Value::from(upgrade_star_count),
            );
        }
        gift_payload.insert(
            "is_upgrade_separate".to_string(),
            Value::Bool(can_be_upgraded && !pay_for_upgrade),
        );
        if let Some(gift_text) = gift_message_text {
            gift_payload.insert("text".to_string(), Value::String(gift_text));
        }
        if let Some(entities) = request.text_entities.as_ref() {
            gift_payload.insert(
                "entities".to_string(),
                serde_json::to_value(entities).map_err(ApiError::internal)?,
            );
        }

        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("gift".to_string(), Value::Object(gift_payload));

        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &sender_user,
            now,
            format!(
                "{} sent a gift",
                display_name_for_service_user(&sender_user)
            ),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

fn handle_delete_owned_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let owned_gift_id = params
        .get("owned_gift_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("owned_gift_id is required"))?
        .to_string();

    let requested_user_id = params
        .get("user_id")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0);

    let requested_chat_id = if let Some(chat_value) = params.get("chat_id") {
        let chat_key = value_to_chat_key(chat_value)?;
        Some(chat_id_as_i64(chat_value, &chat_key))
    } else {
        None
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let owned_record: Option<(Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT owner_user_id, owner_chat_id
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, &owned_gift_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((owner_user_id, owner_chat_id)) = owned_record else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if let Some(expected_user_id) = requested_user_id {
        if owner_user_id != Some(expected_user_id) {
            return Err(ApiError::bad_request(
                "owned gift does not belong to requested user",
            ));
        }
    }

    if let Some(expected_chat_id) = requested_chat_id {
        if owner_chat_id != Some(expected_chat_id) {
            return Err(ApiError::bad_request(
                "owned gift does not belong to requested chat",
            ));
        }
    }

    conn.execute(
        "DELETE FROM sim_owned_gifts WHERE bot_id = ?1 AND owned_gift_id = ?2",
        params![bot.id, &owned_gift_id],
    )
    .map_err(ApiError::internal)?;

    if let Some(user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = CASE
                 WHEN COALESCE(gift_count, 0) > 0 THEN gift_count - 1
                 ELSE 0
             END
             WHERE id = ?1",
            params![user_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

fn handle_convert_gift_to_stars(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ConvertGiftToStarsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(
        &mut conn,
        bot.id,
        &request.business_connection_id,
    )?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_convert_gifts_to_stars,
        "not enough rights to convert gifts to stars",
    )?;

    let owned_row: Option<(Option<i64>, Option<i64>, i64, i64, Option<i64>, i64)> = conn
        .query_row(
            "SELECT owner_user_id, owner_chat_id, is_unique, was_refunded, convert_star_count, gift_star_count
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, request.owned_gift_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((owner_user_id, _owner_chat_id, is_unique, was_refunded, convert_star_count, gift_star_count)) = owned_row else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if is_unique == 1 {
        return Err(ApiError::bad_request(
            "only regular gifts can be converted to stars",
        ));
    }
    if was_refunded == 1 {
        return Err(ApiError::bad_request("gift has already been converted"));
    }

    let resolved_convert_amount = convert_star_count
        .unwrap_or_else(|| (gift_star_count / 2).max(1))
        .max(1);
    let now = Utc::now().timestamp();

    conn.execute(
        "UPDATE sim_business_connections
         SET star_balance = star_balance + ?1,
             updated_at = ?2
         WHERE bot_id = ?3 AND connection_id = ?4",
        params![
            resolved_convert_amount,
            now,
            bot.id,
            record.connection_id,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_convert')",
        params![
            format!("gift_convert_{}", generate_telegram_numeric_id()),
            bot.id,
            record.user_id,
            format!("gift_convert_{}", generate_telegram_numeric_id()),
            resolved_convert_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM sim_owned_gifts WHERE bot_id = ?1 AND owned_gift_id = ?2",
        params![bot.id, request.owned_gift_id],
    )
    .map_err(ApiError::internal)?;

    if let Some(user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = CASE
                 WHEN COALESCE(gift_count, 0) > 0 THEN gift_count - 1
                 ELSE 0
             END
             WHERE id = ?1",
            params![user_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

fn handle_upgrade_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UpgradeGiftRequest = parse_request(params)?;
    if request.star_count.unwrap_or(0) < 0 {
        return Err(ApiError::bad_request("star_count must be non-negative"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(
        &mut conn,
        bot.id,
        &request.business_connection_id,
    )?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_transfer_and_upgrade_gifts,
        "not enough rights to upgrade gifts",
    )?;

    let owned_row: Option<(String, i64, i64, i64, Option<i64>, i64, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT gift_json, is_unique, was_refunded, can_be_upgraded,
                    prepaid_upgrade_star_count, gift_star_count,
                    unique_gift_number, transfer_star_count
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, request.owned_gift_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((gift_json, is_unique, was_refunded, can_be_upgraded, prepaid_upgrade_star_count, gift_star_count, unique_gift_number, transfer_star_count)) = owned_row else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if was_refunded == 1 {
        return Err(ApiError::bad_request("gift has already been converted"));
    }
    if is_unique == 1 {
        return Ok(json!(true));
    }
    if can_be_upgraded != 1 {
        return Err(ApiError::bad_request("gift cannot be upgraded"));
    }

    let resolved_upgrade_cost = request
        .star_count
        .unwrap_or_else(|| prepaid_upgrade_star_count.unwrap_or((gift_star_count / 2).max(1)))
        .max(0);

    if resolved_upgrade_cost > record.star_balance {
        return Err(ApiError::bad_request(
            "not enough stars in business account balance",
        ));
    }

    let now = Utc::now().timestamp();

    if resolved_upgrade_cost > 0 {
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance - ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![resolved_upgrade_cost, now, bot.id, record.connection_id],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_upgrade')",
            params![
                format!("gift_upgrade_{}", generate_telegram_numeric_id()),
                bot.id,
                record.user_id,
                format!("gift_upgrade_{}", generate_telegram_numeric_id()),
                -resolved_upgrade_cost,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let mut gift = serde_json::from_str::<Gift>(&gift_json)
        .unwrap_or_else(|_| fallback_sim_gift("gift_upgraded"));
    let generated_unique_number = generate_telegram_numeric_id()
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse::<i64>()
        .ok()
        .unwrap_or_else(|| Utc::now().timestamp_micros().unsigned_abs() as i64);
    let unique_number = unique_gift_number.unwrap_or(generated_unique_number);
    if !request.keep_original_details.unwrap_or(false) {
        gift.id = format!("{}_u{}", gift.id, unique_number);
    }
    gift.total_count = Some(1);
    gift.remaining_count = Some(1);
    gift.personal_total_count = Some(1);
    gift.personal_remaining_count = Some(1);
    gift.unique_gift_variant_count = Some(1);
    gift.has_colors = Some(true);

    let next_transfer_star_count = transfer_star_count
        .unwrap_or_else(|| (resolved_upgrade_cost / 2).max(1))
        .max(1);

    conn.execute(
        "UPDATE sim_owned_gifts
         SET gift_id = ?1,
             gift_json = ?2,
             is_unique = 1,
             can_be_upgraded = 0,
             is_upgrade_separate = 0,
             prepaid_upgrade_star_count = ?3,
             unique_gift_number = ?4,
             transfer_star_count = ?5,
             next_transfer_date = ?6,
             updated_at = ?7
         WHERE bot_id = ?8 AND owned_gift_id = ?9",
        params![
            gift.id,
            serde_json::to_string(&gift).map_err(ApiError::internal)?,
            resolved_upgrade_cost,
            unique_number,
            next_transfer_star_count,
            now,
            now,
            bot.id,
            request.owned_gift_id,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_transfer_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: TransferGiftRequest = parse_request(params)?;
    if request.new_owner_chat_id == 0 {
        return Err(ApiError::bad_request("new_owner_chat_id is invalid"));
    }
    if request.star_count.unwrap_or(0) < 0 {
        return Err(ApiError::bad_request("star_count must be non-negative"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = load_business_connection_or_404(
        &mut conn,
        bot.id,
        &request.business_connection_id,
    )?;
    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_transfer_and_upgrade_gifts,
        "not enough rights to transfer gifts",
    )?;

    let owned_row: Option<(Option<i64>, Option<i64>, i64, i64, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT owner_user_id, owner_chat_id, is_unique, was_refunded,
                    transfer_star_count, next_transfer_date
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, request.owned_gift_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((owner_user_id, owner_chat_id, is_unique, was_refunded, transfer_star_count, next_transfer_date)) = owned_row else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if is_unique != 1 {
        return Err(ApiError::bad_request("only unique gifts can be transferred"));
    }
    if was_refunded == 1 {
        return Err(ApiError::bad_request("gift has already been converted"));
    }

    let now = Utc::now().timestamp();
    if let Some(next_allowed_transfer) = next_transfer_date {
        if next_allowed_transfer > now {
            return Err(ApiError::bad_request(
                "gift cannot be transferred yet",
            ));
        }
    }

    let mut next_owner_user_id: Option<i64> = None;
    let mut next_owner_chat_id: Option<i64> = None;

    if let Some(sim_chat) = load_sim_chat_record_by_chat_id(
        &mut conn,
        bot.id,
        request.new_owner_chat_id,
    )? {
        if sim_chat.chat_type == "private" {
            let recipient = ensure_user(&mut conn, Some(sim_chat.chat_id), None, None)?;
            next_owner_user_id = Some(recipient.id);
        } else {
            next_owner_chat_id = Some(sim_chat.chat_id);
        }
    } else {
        let recipient = ensure_user(&mut conn, Some(request.new_owner_chat_id), None, None)?;
        next_owner_user_id = Some(recipient.id);
    }

    if owner_user_id == next_owner_user_id && owner_chat_id == next_owner_chat_id {
        return Ok(json!(true));
    }

    let resolved_transfer_cost = request
        .star_count
        .unwrap_or(transfer_star_count.unwrap_or(0))
        .max(0);
    if resolved_transfer_cost > record.star_balance {
        return Err(ApiError::bad_request(
            "not enough stars in business account balance",
        ));
    }

    if resolved_transfer_cost > 0 {
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance - ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![resolved_transfer_cost, now, bot.id, record.connection_id],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_transfer')",
            params![
                format!("gift_transfer_{}", generate_telegram_numeric_id()),
                bot.id,
                record.user_id,
                format!("gift_transfer_{}", generate_telegram_numeric_id()),
                -resolved_transfer_cost,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let next_transfer_cost = transfer_star_count
        .unwrap_or_else(|| resolved_transfer_cost.max(1))
        .max(1);

    conn.execute(
        "UPDATE sim_owned_gifts
         SET owner_user_id = ?1,
             owner_chat_id = ?2,
             sender_user_id = ?3,
             is_saved = 0,
             transfer_star_count = ?4,
             next_transfer_date = ?5,
             updated_at = ?6
         WHERE bot_id = ?7 AND owned_gift_id = ?8",
        params![
            next_owner_user_id,
            next_owner_chat_id,
            connection.user.id,
            next_transfer_cost,
            now.saturating_add(86_400),
            now,
            bot.id,
            request.owned_gift_id,
        ],
    )
    .map_err(ApiError::internal)?;

    if let Some(previous_owner_user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = CASE
                 WHEN COALESCE(gift_count, 0) > 0 THEN gift_count - 1
                 ELSE 0
             END
             WHERE id = ?1",
            params![previous_owner_user_id],
        )
        .map_err(ApiError::internal)?;
    }

    if let Some(current_owner_user_id) = next_owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = COALESCE(gift_count, 0) + 1
             WHERE id = ?1",
            params![current_owner_user_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

#[derive(Debug, Clone)]
struct SimBusinessStoryRecord {
    business_connection_id: String,
    story_id: i64,
    owner_chat_id: i64,
    content_json: String,
    caption: Option<String>,
    caption_entities_json: Option<String>,
    areas_json: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostStoryCompatRequest {
    business_connection_id: Option<String>,
    content: crate::generated::types::InputStoryContent,
    active_period: i64,
    caption: Option<String>,
    parse_mode: Option<String>,
    caption_entities: Option<Vec<MessageEntity>>,
    areas: Option<Vec<StoryArea>>,
    post_to_chat_page: Option<bool>,
    protect_content: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepostStoryCompatRequest {
    business_connection_id: Option<String>,
    from_chat_id: i64,
    from_story_id: i64,
    active_period: i64,
    post_to_chat_page: Option<bool>,
    protect_content: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditStoryCompatRequest {
    business_connection_id: Option<String>,
    story_id: i64,
    content: crate::generated::types::InputStoryContent,
    caption: Option<String>,
    parse_mode: Option<String>,
    caption_entities: Option<Vec<MessageEntity>>,
    areas: Option<Vec<StoryArea>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteStoryCompatRequest {
    business_connection_id: Option<String>,
    story_id: i64,
}

fn ensure_story_active_period(active_period: i64) -> Result<(), ApiError> {
    match active_period {
        21_600 | 43_200 | 86_400 | 172_800 => Ok(()),
        _ => Err(ApiError::bad_request(
            "active_period must be one of 21600, 43200, 86400, 172800",
        )),
    }
}

fn validate_story_content_payload(content: &Value) -> Result<(), ApiError> {
    let object = content
        .as_object()
        .ok_or_else(|| ApiError::bad_request("content must be a JSON object"))?;

    let content_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase())
        .ok_or_else(|| ApiError::bad_request("content.type is required"))?;

    match content_type.as_str() {
        "photo" => {
            let has_photo = object
                .get("photo")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some();
            if !has_photo {
                return Err(ApiError::bad_request("content.photo is required"));
            }
            Ok(())
        }
        "video" => {
            let has_video = object
                .get("video")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some();
            if !has_video {
                return Err(ApiError::bad_request("content.video is required"));
            }

            if let Some(duration) = object.get("duration").and_then(Value::as_f64) {
                if !(0.0..=60.0).contains(&duration) {
                    return Err(ApiError::bad_request(
                        "content.duration must be between 0 and 60",
                    ));
                }
            }

            Ok(())
        }
        _ => Err(ApiError::bad_request(
            "content.type must be one of: photo, video",
        )),
    }
}

fn validate_story_areas_payload(areas: Option<&Vec<StoryArea>>) -> Result<(), ApiError> {
    let Some(areas) = areas else {
        return Ok(());
    };

    if areas.len() > 10 {
        return Err(ApiError::bad_request("a story can contain at most 10 areas"));
    }

    let mut location_count = 0;
    let mut suggested_reaction_count = 0;
    let mut link_count = 0;
    let mut weather_count = 0;
    let mut unique_gift_count = 0;

    for area in areas {
        let position = &area.position;
        if !position.x_percentage.is_finite()
            || position.x_percentage < 0.0
            || position.x_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.x_percentage must be between 0 and 100",
            ));
        }
        if !position.y_percentage.is_finite()
            || position.y_percentage < 0.0
            || position.y_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.y_percentage must be between 0 and 100",
            ));
        }
        if !position.width_percentage.is_finite()
            || position.width_percentage <= 0.0
            || position.width_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.width_percentage must be between 0 and 100",
            ));
        }
        if !position.height_percentage.is_finite()
            || position.height_percentage <= 0.0
            || position.height_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.height_percentage must be between 0 and 100",
            ));
        }
        if !position.rotation_angle.is_finite() || position.rotation_angle.abs() > 360.0 {
            return Err(ApiError::bad_request(
                "story area position.rotation_angle must be finite and between -360 and 360",
            ));
        }
        if !position.corner_radius_percentage.is_finite()
            || position.corner_radius_percentage < 0.0
            || position.corner_radius_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.corner_radius_percentage must be between 0 and 100",
            ));
        }

        let area_object = area
            .r#type
            .extra
            .as_object()
            .ok_or_else(|| ApiError::bad_request("story area payload is invalid"))?;
        let area_type = area
            .r#type
            .extra
            .get("type")
            .and_then(Value::as_str)
            .map(|value| value.to_ascii_lowercase())
            .ok_or_else(|| ApiError::bad_request("story area type is invalid"))?;

        match area_type.as_str() {
            "location" => {
                location_count += 1;
                if location_count > 10 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 10 location areas",
                    ));
                }

                let latitude = area_object
                    .get("latitude")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| ApiError::bad_request("location area latitude is required"))?;
                let longitude = area_object
                    .get("longitude")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| ApiError::bad_request("location area longitude is required"))?;

                if !latitude.is_finite() || !(-90.0..=90.0).contains(&latitude) {
                    return Err(ApiError::bad_request(
                        "location area latitude must be between -90 and 90",
                    ));
                }
                if !longitude.is_finite() || !(-180.0..=180.0).contains(&longitude) {
                    return Err(ApiError::bad_request(
                        "location area longitude must be between -180 and 180",
                    ));
                }
            }
            "suggested_reaction" => {
                suggested_reaction_count += 1;
                if suggested_reaction_count > 5 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 5 suggested reaction areas",
                    ));
                }

                let reaction_type = area_object
                    .get("reaction_type")
                    .and_then(Value::as_object)
                    .ok_or_else(|| ApiError::bad_request("suggested_reaction.reaction_type is required"))?;
                let reaction_kind = reaction_type
                    .get("type")
                    .and_then(Value::as_str)
                    .map(|value| value.to_ascii_lowercase())
                    .ok_or_else(|| ApiError::bad_request("suggested_reaction.reaction_type.type is required"))?;

                match reaction_kind.as_str() {
                    "emoji" => {
                        let emoji = reaction_type
                            .get("emoji")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        if emoji.is_empty() {
                            return Err(ApiError::bad_request(
                                "suggested_reaction emoji value is required",
                            ));
                        }
                    }
                    "custom_emoji" => {
                        let custom_emoji_id = reaction_type
                            .get("custom_emoji_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        if custom_emoji_id.is_empty() {
                            return Err(ApiError::bad_request(
                                "suggested_reaction custom_emoji_id is required",
                            ));
                        }
                    }
                    "paid" => {}
                    _ => {
                        return Err(ApiError::bad_request(
                            "suggested_reaction reaction_type.type is invalid",
                        ));
                    }
                }
            }
            "link" => {
                link_count += 1;
                if link_count > 3 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 3 link areas",
                    ));
                }

                let url = area_object
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if url.is_empty() {
                    return Err(ApiError::bad_request("link area url is required"));
                }
                if !(url.starts_with("https://")
                    || url.starts_with("http://")
                    || url.starts_with("tg://"))
                {
                    return Err(ApiError::bad_request(
                        "link area url must start with https://, http://, or tg://",
                    ));
                }
            }
            "weather" => {
                weather_count += 1;
                if weather_count > 3 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 3 weather areas",
                    ));
                }

                let temperature = area_object
                    .get("temperature")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| ApiError::bad_request("weather area temperature is required"))?;
                if !temperature.is_finite() || !(-100.0..=100.0).contains(&temperature) {
                    return Err(ApiError::bad_request(
                        "weather area temperature must be between -100 and 100",
                    ));
                }

                let emoji = area_object
                    .get("emoji")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if emoji.is_empty() {
                    return Err(ApiError::bad_request("weather area emoji is required"));
                }

                let background_color = area_object
                    .get("background_color")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| ApiError::bad_request("weather area background_color is required"))?;
                if !(0..=0xFFFFFF).contains(&background_color) {
                    return Err(ApiError::bad_request(
                        "weather area background_color must be between 0 and 16777215",
                    ));
                }
            }
            "unique_gift" => {
                unique_gift_count += 1;
                if unique_gift_count > 1 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 1 unique gift area",
                    ));
                }

                let name = area_object
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if name.is_empty() {
                    return Err(ApiError::bad_request("unique_gift area name is required"));
                }
            }
            _ => {
                return Err(ApiError::bad_request("story area type is not supported"));
            }
        }
    }

    Ok(())
}

fn ensure_sim_story_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_business_stories (
            bot_id INTEGER NOT NULL,
            business_connection_id TEXT NOT NULL,
            story_id INTEGER NOT NULL,
            owner_chat_id INTEGER NOT NULL,
            content_json TEXT NOT NULL,
            caption TEXT,
            caption_entities_json TEXT,
            areas_json TEXT,
            active_period INTEGER NOT NULL,
            post_to_chat_page INTEGER NOT NULL DEFAULT 0,
            protect_content INTEGER NOT NULL DEFAULT 0,
            source_chat_id INTEGER,
            source_story_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (bot_id, business_connection_id, story_id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_business_stories_chat_story
            ON sim_business_stories (bot_id, owner_chat_id, story_id);",
    )
    .map_err(ApiError::internal)
}

fn next_story_id_for_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    business_connection_id: &str,
) -> Result<i64, ApiError> {
    let max_story_id: Option<i64> = conn
        .query_row(
            "SELECT MAX(story_id)
             FROM sim_business_stories
             WHERE bot_id = ?1 AND business_connection_id = ?2",
            params![bot_id, business_connection_id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    Ok(max_story_id.unwrap_or(0) + 1)
}

fn load_story_record_for_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    business_connection_id: &str,
    story_id: i64,
) -> Result<Option<SimBusinessStoryRecord>, ApiError> {
    conn.query_row(
        "SELECT business_connection_id, story_id, owner_chat_id,
                content_json, caption, caption_entities_json, areas_json
         FROM sim_business_stories
         WHERE bot_id = ?1 AND business_connection_id = ?2 AND story_id = ?3",
        params![bot_id, business_connection_id, story_id],
        |row| {
            Ok(SimBusinessStoryRecord {
                business_connection_id: row.get(0)?,
                story_id: row.get(1)?,
                owner_chat_id: row.get(2)?,
                content_json: row.get(3)?,
                caption: row.get(4)?,
                caption_entities_json: row.get(5)?,
                areas_json: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn load_story_record_for_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    owner_chat_id: i64,
    story_id: i64,
) -> Result<Option<SimBusinessStoryRecord>, ApiError> {
    conn.query_row(
        "SELECT business_connection_id, story_id, owner_chat_id,
                content_json, caption, caption_entities_json, areas_json
         FROM sim_business_stories
         WHERE bot_id = ?1 AND owner_chat_id = ?2 AND story_id = ?3",
        params![bot_id, owner_chat_id, story_id],
        |row| {
            Ok(SimBusinessStoryRecord {
                business_connection_id: row.get(0)?,
                story_id: row.get(1)?,
                owner_chat_id: row.get(2)?,
                content_json: row.get(3)?,
                caption: row.get(4)?,
                caption_entities_json: row.get(5)?,
                areas_json: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn story_chat_for_business_connection(connection: &BusinessConnection) -> Chat {
    Chat {
        id: connection.user.id,
        r#type: "private".to_string(),
        title: None,
        username: connection.user.username.clone(),
        first_name: Some(connection.user.first_name.clone()),
        last_name: connection.user.last_name.clone(),
        is_forum: None,
        is_direct_messages: None,
    }
}

fn normalize_story_content_payload(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
    content: &Value,
) -> Result<Value, ApiError> {
    let mut object = content
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::bad_request("content must be a JSON object"))?;

    let content_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("content.type is required"))?;

    let media_field = match content_type.as_str() {
        "photo" => "photo",
        "video" => "video",
        _ => {
            return Err(ApiError::bad_request(
                "content.type must be one of: photo, video",
            ));
        }
    };

    let media_ref = object
        .get(media_field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("content.{} is required", media_field)))?;

    let resolved_input = if let Some(attach_key) = media_ref.strip_prefix("attach://") {
        let attach_key = attach_key.trim();
        if attach_key.is_empty() {
            return Err(ApiError::bad_request(format!(
                "content.{} attachment reference is invalid",
                media_field
            )));
        }

        params.get(attach_key).cloned().ok_or_else(|| {
            ApiError::bad_request(format!(
                "content.{} attachment '{}' was not provided",
                media_field, attach_key
            ))
        })?
    } else {
        Value::String(media_ref.to_string())
    };

    let stored_file = resolve_media_file(state, token, &resolved_input, media_field)?;
    object.insert(
        media_field.to_string(),
        Value::String(stored_file.file_id),
    );
    object.insert("type".to_string(), Value::String(content_type));

    Ok(Value::Object(object))
}

fn build_story_response_payload(
    chat: Chat,
    story_id: i64,
    content: Option<&Value>,
    caption: Option<&str>,
) -> Value {
    let mut payload = json!({
        "chat": chat,
        "id": story_id,
    });

    if let Some(content_value) = content {
        payload["content"] = content_value.clone();
    }
    if let Some(value) = caption {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            payload["caption"] = Value::String(trimmed.to_string());
        }
    }

    payload
}

fn ensure_sim_suggested_posts_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_suggested_posts (
            bot_id INTEGER NOT NULL,
            chat_key TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            send_date INTEGER,
            comment TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id)
        );",
    )
    .map_err(ApiError::internal)
}

fn load_suggested_post_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
) -> Result<Option<(String, Option<i64>)>, ApiError> {
    conn.query_row(
        "SELECT state, send_date
         FROM sim_suggested_posts
         WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
        params![bot_id, chat_key, message_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(ApiError::internal)
}

fn upsert_suggested_post_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    state: &str,
    send_date: Option<i64>,
    comment: Option<&str>,
    updated_at: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_suggested_posts
         (bot_id, chat_key, message_id, state, send_date, comment, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(bot_id, chat_key, message_id)
         DO UPDATE SET
            state = excluded.state,
            send_date = excluded.send_date,
            comment = excluded.comment,
            updated_at = excluded.updated_at",
        params![
            bot_id,
            chat_key,
            message_id,
            state,
            send_date,
            comment,
            updated_at,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn load_direct_messages_chat_for_request(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
) -> Result<SimChatRecord, ApiError> {
    let chat_key = chat_id.to_string();
    let chat = load_sim_chat_record(conn, bot_id, &chat_key)?
        .or(load_sim_chat_record_by_chat_id(conn, bot_id, chat_id)?)
        .ok_or_else(|| ApiError::not_found("chat not found"))?;

    if !is_direct_messages_chat(&chat) {
        return Err(ApiError::bad_request(
            "chat_id must be a channel direct messages chat",
        ));
    }

    Ok(chat)
}

fn load_suggested_post_message_for_service(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat_id: i64,
    message_id: i64,
) -> Result<Value, ApiError> {
    let message_value = load_message_value(conn, bot, message_id)?;
    let belongs_to_chat = message_value
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(Value::as_i64)
        .map(|chat_id| chat_id == direct_messages_chat_id)
        .unwrap_or(false);

    if !belongs_to_chat {
        return Err(ApiError::bad_request("suggested post message was not found"));
    }

    let is_suggested_post = message_value
        .get("suggested_post_info")
        .is_some()
        || message_value
            .get("suggested_post_parameters")
            .is_some();
    if !is_suggested_post {
        return Err(ApiError::bad_request("message is not a suggested post"));
    }

    Ok(message_value)
}

fn extract_suggested_post_price_from_message(message_value: &Value) -> Option<Value> {
    let info_price = message_value
        .get("suggested_post_info")
        .and_then(|info| info.get("price"));
    if info_price.is_some() {
        return info_price.cloned();
    }

    message_value
        .get("suggested_post_parameters")
        .and_then(|params| params.get("price"))
        .cloned()
}

fn extract_suggested_post_send_date_from_message(message_value: &Value) -> Option<i64> {
    let info_send_date = message_value
        .get("suggested_post_info")
        .and_then(|info| info.get("send_date"))
        .and_then(Value::as_i64);
    if info_send_date.is_some() {
        return info_send_date;
    }

    message_value
        .get("suggested_post_parameters")
        .and_then(|params| params.get("send_date"))
        .and_then(Value::as_i64)
}

fn extract_suggested_post_price_currency_amount(
    message_value: &Value,
) -> Option<(String, i64)> {
    let price = extract_suggested_post_price_from_message(message_value)?;
    let currency = price
        .get("currency")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_ascii_uppercase();
    let amount = price.get("amount").and_then(Value::as_i64)?;
    Some((currency, amount))
}

fn load_channel_owner_user_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat_key: &str,
) -> Result<Option<i64>, ApiError> {
    conn.query_row(
        "SELECT user_id
         FROM sim_chat_members
         WHERE bot_id = ?1 AND chat_key = ?2 AND status = 'owner'
         LIMIT 1",
        params![bot_id, channel_chat_key],
        |row| row.get(0),
    )
    .optional()
    .map_err(ApiError::internal)
}

fn ensure_sim_business_connection_for_user(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    user_id: i64,
) -> Result<SimBusinessConnectionRecord, ApiError> {
    if let Some(existing) = load_sim_business_connection_for_user(conn, bot_id, user_id)? {
        return Ok(existing);
    }

    let user = ensure_user(conn, Some(user_id), None, None)?;
    let connection_id = default_business_connection_id(bot_id, user.id);
    upsert_sim_business_connection(
        conn,
        bot_id,
        &connection_id,
        user.id,
        user.id,
        &default_business_bot_rights(),
        true,
    )
}

fn settle_suggested_post_price_for_publication(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    proposer_user_id: i64,
    channel_owner_user_id: i64,
    price: Option<(String, i64)>,
) -> Result<Option<(String, i64, i64, i64)>, ApiError> {
    let Some((currency, gross_amount)) = price else {
        return Ok(None);
    };

    if gross_amount <= 0 {
        return Ok(None);
    }

    if currency != "XTR" {
        return Ok(Some((currency, gross_amount, gross_amount, 0)));
    }

    let now = Utc::now().timestamp();
    let proposer_connection =
        ensure_sim_business_connection_for_user(conn, bot_id, proposer_user_id)?;
    let owner_connection =
        ensure_sim_business_connection_for_user(conn, bot_id, channel_owner_user_id)?;

    if proposer_connection.star_balance < gross_amount {
        let top_up = gross_amount.saturating_sub(proposer_connection.star_balance);
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance + ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![
                top_up,
                now,
                bot_id,
                proposer_connection.connection_id,
            ],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'suggested_post_auto_topup')",
            params![
                format!("suggested_post_topup_{}", generate_telegram_numeric_id()),
                bot_id,
                proposer_user_id,
                format!("suggested_post_topup_{}", generate_telegram_numeric_id()),
                top_up,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    conn.execute(
        "UPDATE sim_business_connections
         SET star_balance = star_balance - ?1,
             updated_at = ?2
         WHERE bot_id = ?3 AND connection_id = ?4",
        params![
            gross_amount,
            now,
            bot_id,
            proposer_connection.connection_id,
        ],
    )
    .map_err(ApiError::internal)?;

    let payout_amount = gross_amount.saturating_mul(80) / 100;
    let fee_amount = gross_amount.saturating_sub(payout_amount);

    if payout_amount > 0 {
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance + ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![
                payout_amount,
                now,
                bot_id,
                owner_connection.connection_id,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'suggested_post_payment')",
        params![
            format!("suggested_post_debit_{}", generate_telegram_numeric_id()),
            bot_id,
            proposer_user_id,
            format!("suggested_post_payment_{}", generate_telegram_numeric_id()),
            -gross_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    if payout_amount > 0 {
        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'suggested_post_payout')",
            params![
                format!("suggested_post_credit_{}", generate_telegram_numeric_id()),
                bot_id,
                channel_owner_user_id,
                format!("suggested_post_payout_{}", generate_telegram_numeric_id()),
                payout_amount,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(Some((currency, gross_amount, payout_amount, fee_amount)))
}

fn publish_suggested_post_to_parent_channel(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat: &SimChatRecord,
    source_message_id: i64,
    actor_user_id: i64,
) -> Result<Value, ApiError> {
    let parent_channel_chat_id = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;

    with_request_actor_user_id(Some(actor_user_id), || {
        let source_message = resolve_source_message_for_transport(
            conn,
            bot,
            &Value::String(direct_messages_chat.chat_key.clone()),
            source_message_id,
            false,
        )?;

        let send_kind = send_kind_from_transport_source_message(&source_message);
        let (destination_chat_key, destination_chat) = resolve_bot_outbound_chat(
            conn,
            bot.id,
            &Value::from(parent_channel_chat_id),
            send_kind,
        )?;
        let sender_user = resolve_transport_sender_user(
            conn,
            bot,
            &destination_chat_key,
            &destination_chat,
            send_kind,
        )?;

        let mut message_value = source_message;
        let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;
        let object = message_value
            .as_object_mut()
            .ok_or_else(|| ApiError::internal("suggested post payload is invalid"))?;

        object.remove("forward_origin");
        object.remove("is_automatic_forward");
        object.remove("reply_to_message");
        object.remove("edit_date");
        object.remove("views");
        object.remove("author_signature");
        object.remove("sender_chat");
        object.remove("message_thread_id");
        object.remove("is_topic_message");
        object.remove("direct_messages_topic");
        object.remove("business_connection_id");
        object.remove("paid_message_star_count");
        object.remove("suggested_post_info");
        object.remove("suggested_post_parameters");
        object.remove("suggested_post_approved");
        object.remove("suggested_post_approval_failed");
        object.remove("suggested_post_declined");
        object.remove("suggested_post_paid");
        object.remove("suggested_post_refunded");
        object.insert("from".to_string(), sender_user_value);

        persist_transported_message(
            state,
            conn,
            token,
            bot,
            &destination_chat_key,
            &destination_chat,
            &sender_user,
            message_value,
            None,
        )
    })
}

fn finalize_due_suggested_post_if_ready(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat: &SimChatRecord,
    message_id: i64,
    actor_user_id: i64,
) -> Result<bool, ApiError> {
    let now = Utc::now().timestamp();
    let Some((current_state, send_date)) = load_suggested_post_state(
        conn,
        bot.id,
        &direct_messages_chat.chat_key,
        message_id,
    )?
    else {
        return Ok(false);
    };

    if current_state != "approved" {
        return Ok(false);
    }

    let effective_send_date = send_date.unwrap_or(now);
    if effective_send_date > now {
        return Ok(false);
    }

    let suggested_message = load_suggested_post_message_for_service(
        conn,
        bot,
        direct_messages_chat.chat_id,
        message_id,
    )?;

    let parent_channel_chat_key = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?
        .to_string();
    let channel_owner_user_id = load_channel_owner_user_id(conn, bot.id, &parent_channel_chat_key)?
        .unwrap_or(actor_user_id);
    let proposer_user_id = suggested_message
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or(actor_user_id);

    let channel_post_message = publish_suggested_post_to_parent_channel(
        state,
        conn,
        token,
        bot,
        direct_messages_chat,
        message_id,
        actor_user_id,
    )?;

    let payment = settle_suggested_post_price_for_publication(
        conn,
        bot.id,
        proposer_user_id,
        channel_owner_user_id,
        extract_suggested_post_price_currency_amount(&suggested_message),
    )?;

    upsert_suggested_post_state(
        conn,
        bot.id,
        &direct_messages_chat.chat_key,
        message_id,
        "paid",
        Some(effective_send_date),
        None,
        now,
    )?;

    let actor = if actor_user_id == bot.id {
        build_bot_user(bot)
    } else {
        let actor_record = ensure_user(conn, Some(actor_user_id), None, None)?;
        build_user_from_sim_record(&actor_record, false)
    };

    let mut paid_payload = Map::<String, Value>::new();
    paid_payload.insert(
        "suggested_post_message".to_string(),
        suggested_message,
    );
    paid_payload.insert(
        "published_channel_post".to_string(),
        channel_post_message,
    );
    paid_payload.insert(
        "send_date".to_string(),
        Value::from(effective_send_date),
    );

    if let Some((currency, gross_amount, payout_amount, fee_amount)) = payment {
        paid_payload.insert("currency".to_string(), Value::String(currency));
        paid_payload.insert("amount".to_string(), Value::from(payout_amount));
        paid_payload.insert("gross_amount".to_string(), Value::from(gross_amount));
        paid_payload.insert("fee_amount".to_string(), Value::from(fee_amount));
        paid_payload.insert(
            "proposer_user_id".to_string(),
            Value::from(proposer_user_id),
        );
        paid_payload.insert(
            "channel_owner_user_id".to_string(),
            Value::from(channel_owner_user_id),
        );
    }

    let mut paid_service_fields = Map::<String, Value>::new();
    paid_service_fields.insert(
        "suggested_post_paid".to_string(),
        Value::Object(paid_payload),
    );
    let direct_messages_chat_obj = build_chat_from_group_record(direct_messages_chat);
    emit_service_message_update(
        state,
        conn,
        token,
        bot.id,
        &direct_messages_chat.chat_key,
        &direct_messages_chat_obj,
        &actor,
        now,
        format!(
            "{} published a suggested post",
            display_name_for_service_user(&actor)
        ),
        paid_service_fields,
    )?;

    Ok(true)
}

fn emit_suggested_post_refunded_updates_before_delete(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat: &SimChatRecord,
    message_ids: &[i64],
) -> Result<(), ApiError> {
    if message_ids.is_empty() {
        return Ok(());
    }

    ensure_sim_suggested_posts_storage(conn)?;

    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    let actor = if actor_user_id == bot.id {
        build_bot_user(bot)
    } else {
        let actor_record = ensure_user(conn, Some(actor_user_id), None, None)?;
        build_user_from_sim_record(&actor_record, false)
    };

    let now = Utc::now().timestamp();
    let direct_messages_chat_obj = build_chat_from_group_record(direct_messages_chat);

    for message_id in message_ids {
        let Some((current_state, send_date)) = load_suggested_post_state(
            conn,
            bot.id,
            &direct_messages_chat.chat_key,
            *message_id,
        )?
        else {
            continue;
        };

        if current_state != "paid" {
            continue;
        }

        let Ok(suggested_message) = load_suggested_post_message_for_service(
            conn,
            bot,
            direct_messages_chat.chat_id,
            *message_id,
        )
        else {
            continue;
        };

        upsert_suggested_post_state(
            conn,
            bot.id,
            &direct_messages_chat.chat_key,
            *message_id,
            "refunded",
            send_date,
            Some("deleted_message"),
            now,
        )?;

        let mut refunded_payload = Map::<String, Value>::new();
        refunded_payload.insert("suggested_post_message".to_string(), suggested_message);
        refunded_payload.insert("reason".to_string(), Value::String("deleted_message".to_string()));

        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "suggested_post_refunded".to_string(),
            Value::Object(refunded_payload),
        );
        emit_service_message_update(
            state,
            conn,
            token,
            bot.id,
            &direct_messages_chat.chat_key,
            &direct_messages_chat_obj,
            &actor,
            now,
            format!(
                "{} refunded a suggested post",
                display_name_for_service_user(&actor)
            ),
            service_fields,
        )?;
    }

    Ok(())
}

fn handle_post_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: PostStoryCompatRequest =
        parse_request_ignoring_prefixed_fields(params, &["story_file"])?;

    let normalized_story_content = normalize_story_content_payload(
        state,
        token,
        params,
        &request.content.extra,
    )?;

    ensure_story_active_period(request.active_period)?;
    validate_story_content_payload(&normalized_story_content)?;
    validate_story_areas_payload(request.areas.as_ref())?;

    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .map(|entities| serde_json::to_value(entities).map_err(ApiError::internal))
        .transpose()?;
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );

    if let Some(value) = caption.as_ref() {
        if value.chars().count() > 2048 {
            return Err(ApiError::bad_request("caption is too long"));
        }
    }

    let normalized_content = crate::generated::types::InputStoryContent {
        extra: normalized_story_content.clone(),
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (record, connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        request.business_connection_id.as_deref(),
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let story_id = next_story_id_for_connection(&mut conn, bot.id, &record.connection_id)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_business_stories
         (bot_id, business_connection_id, story_id, owner_chat_id,
          content_json, caption, caption_entities_json, areas_json,
          active_period, post_to_chat_page, protect_content,
          source_chat_id, source_story_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4,
                 ?5, ?6, ?7, ?8,
                 ?9, ?10, ?11,
                 NULL, NULL, ?12, ?12)",
        params![
            bot.id,
            &record.connection_id,
            story_id,
            connection.user.id,
            serde_json::to_string(&normalized_content).map_err(ApiError::internal)?,
            caption,
            caption_entities.map(|value| value.to_string()),
            request
                .areas
                .as_ref()
                .map(|areas| serde_json::to_string(areas).map_err(ApiError::internal))
                .transpose()?,
            request.active_period,
            if request.post_to_chat_page.unwrap_or(false) {
                1
            } else {
                0
            },
            if request.protect_content.unwrap_or(false) {
                1
            } else {
                0
            },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(build_story_response_payload(
        story_chat_for_business_connection(&connection),
        story_id,
        Some(&normalized_story_content),
        caption.as_deref(),
    ))
}

fn handle_repost_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RepostStoryCompatRequest = parse_request(params)?;
    ensure_story_active_period(request.active_period)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (target_record, target_connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        request.business_connection_id.as_deref(),
    )?;

    let source_connection_record = load_sim_business_connection_for_user(
        &mut conn,
        bot.id,
        request.from_chat_id,
    )?
    .ok_or_else(|| ApiError::bad_request("source business account is not managed by this bot"))?;
    let source_connection =
        build_business_connection(&mut conn, bot.id, &source_connection_record)?;
    ensure_business_right(
        &source_connection,
        |rights| rights.can_manage_stories,
        "not enough rights to manage source stories",
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let source_story = load_story_record_for_chat(
        &mut conn,
        bot.id,
        request.from_chat_id,
        request.from_story_id,
    )?
    .ok_or_else(|| ApiError::bad_request("source story was not found"))?;

    let story_id = next_story_id_for_connection(&mut conn, bot.id, &target_record.connection_id)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_business_stories
         (bot_id, business_connection_id, story_id, owner_chat_id,
          content_json, caption, caption_entities_json, areas_json,
          active_period, post_to_chat_page, protect_content,
          source_chat_id, source_story_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4,
                 ?5, ?6, ?7, ?8,
                 ?9, ?10, ?11,
                 ?12, ?13, ?14, ?14)",
        params![
            bot.id,
            &target_record.connection_id,
            story_id,
            target_connection.user.id,
            &source_story.content_json,
            &source_story.caption,
            &source_story.caption_entities_json,
            &source_story.areas_json,
            request.active_period,
            if request.post_to_chat_page.unwrap_or(false) {
                1
            } else {
                0
            },
            if request.protect_content.unwrap_or(false) {
                1
            } else {
                0
            },
            request.from_chat_id,
            request.from_story_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let source_content_value = serde_json::from_str::<Value>(&source_story.content_json)
        .unwrap_or_else(|_| Value::Null);
    Ok(build_story_response_payload(
        story_chat_for_business_connection(&target_connection),
        story_id,
        if source_content_value.is_null() {
            None
        } else {
            Some(&source_content_value)
        },
        source_story.caption.as_deref(),
    ))
}

fn handle_edit_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditStoryCompatRequest =
        parse_request_ignoring_prefixed_fields(params, &["story_file"])?;

    let normalized_story_content = normalize_story_content_payload(
        state,
        token,
        params,
        &request.content.extra,
    )?;

    validate_story_content_payload(&normalized_story_content)?;
    validate_story_areas_payload(request.areas.as_ref())?;

    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .map(|entities| serde_json::to_value(entities).map_err(ApiError::internal))
        .transpose()?;
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );

    if let Some(value) = caption.as_ref() {
        if value.chars().count() > 2048 {
            return Err(ApiError::bad_request("caption is too long"));
        }
    }

    let normalized_content = crate::generated::types::InputStoryContent {
        extra: normalized_story_content.clone(),
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (record, connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        request.business_connection_id.as_deref(),
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let existing = load_story_record_for_connection(
        &mut conn,
        bot.id,
        &record.connection_id,
        request.story_id,
    )?;
    if existing.is_none() {
        return Err(ApiError::bad_request("story was not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_business_stories
         SET content_json = ?1,
             caption = ?2,
             caption_entities_json = ?3,
             areas_json = ?4,
             updated_at = ?5
         WHERE bot_id = ?6 AND business_connection_id = ?7 AND story_id = ?8",
        params![
            serde_json::to_string(&normalized_content).map_err(ApiError::internal)?,
            caption,
            caption_entities.map(|value| value.to_string()),
            request
                .areas
                .as_ref()
                .map(|areas| serde_json::to_string(areas).map_err(ApiError::internal))
                .transpose()?,
            now,
            bot.id,
            &record.connection_id,
            request.story_id,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(build_story_response_payload(
        story_chat_for_business_connection(&connection),
        request.story_id,
        Some(&normalized_story_content),
        caption.as_deref(),
    ))
}

fn handle_delete_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteStoryCompatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (record, _connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        request.business_connection_id.as_deref(),
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let deleted = conn
        .execute(
            "DELETE FROM sim_business_stories
             WHERE bot_id = ?1 AND business_connection_id = ?2 AND story_id = ?3",
            params![bot.id, &record.connection_id, request.story_id],
        )
        .map_err(ApiError::internal)?;

    if deleted == 0 {
        return Err(ApiError::bad_request("story was not found"));
    }

    Ok(json!(true))
}

fn handle_approve_suggested_post(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ApproveSuggestedPostRequest = parse_request(params)?;

    let now = Utc::now().timestamp();
    if let Some(send_date) = request.send_date {
        if send_date - now > 2_678_400 {
            return Err(ApiError::bad_request(
                "send_date must be at most 30 days in the future",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let direct_messages_chat = load_direct_messages_chat_for_request(
        &mut conn,
        bot.id,
        request.chat_id,
    )?;

    let parent_channel_chat_key = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?
        .to_string();
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    ensure_channel_member_can_approve_suggested_posts(
        &mut conn,
        bot.id,
        &parent_channel_chat_key,
        actor_user_id,
    )?;

    let suggested_message = load_suggested_post_message_for_service(
        &mut conn,
        &bot,
        direct_messages_chat.chat_id,
        request.message_id,
    )?;

    ensure_sim_suggested_posts_storage(&mut conn)?;
    let existing = load_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
    )?;

    if let Some((current_state, existing_send_date)) = existing.as_ref() {
        if current_state == "declined" {
            return Err(ApiError::bad_request(
                "suggested post is already declined",
            ));
        }
        if current_state == "paid" {
            return Ok(json!(true));
        }
        if current_state == "refunded" {
            return Err(ApiError::bad_request(
                "suggested post is already refunded",
            ));
        }
        if current_state == "approved" && request.send_date.is_none() {
            let effective_send_date = existing_send_date.unwrap_or(now);
            if effective_send_date <= now {
                let _ = finalize_due_suggested_post_if_ready(
                    state,
                    &mut conn,
                    token,
                    &bot,
                    &direct_messages_chat,
                    request.message_id,
                    actor_user_id,
                )?;
            }
            return Ok(json!(true));
        }
    }

    let resolved_send_date = request
        .send_date
        .or_else(|| existing.as_ref().and_then(|(_, send_date)| *send_date))
        .or_else(|| extract_suggested_post_send_date_from_message(&suggested_message));
    if let Some(send_date) = resolved_send_date {
        if send_date - now > 2_678_400 {
            return Err(ApiError::bad_request(
                "send_date must be at most 30 days in the future",
            ));
        }

        if send_date < now {
            let Some(price) = extract_suggested_post_price_from_message(&suggested_message) else {
                return Err(ApiError::bad_request(
                    "suggested post send_date is in the past",
                ));
            };

            upsert_suggested_post_state(
                &mut conn,
                bot.id,
                &direct_messages_chat.chat_key,
                request.message_id,
                "approval_failed",
                Some(send_date),
                Some("send_date_in_past"),
                now,
            )?;

            let actor = if actor_user_id == bot.id {
                build_bot_user(&bot)
            } else {
                let actor_record = ensure_user(&mut conn, Some(actor_user_id), None, None)?;
                build_user_from_sim_record(&actor_record, false)
            };
            let mut approval_failed_payload = Map::<String, Value>::new();
            approval_failed_payload.insert(
                "suggested_post_message".to_string(),
                suggested_message.clone(),
            );
            approval_failed_payload.insert("price".to_string(), price);

            let mut service_fields = Map::<String, Value>::new();
            service_fields.insert(
                "suggested_post_approval_failed".to_string(),
                Value::Object(approval_failed_payload),
            );
            let direct_messages_chat_obj = build_chat_from_group_record(&direct_messages_chat);
            emit_service_message_update(
                state,
                &mut conn,
                token,
                bot.id,
                &direct_messages_chat.chat_key,
                &direct_messages_chat_obj,
                &actor,
                now,
                format!(
                    "{} failed to approve a suggested post",
                    display_name_for_service_user(&actor)
                ),
                service_fields,
            )?;

            return Ok(json!(true));
        }
    }
    let approved_send_date = resolved_send_date.unwrap_or(now);

    upsert_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
        "approved",
        Some(approved_send_date),
        None,
        now,
    )?;

    let actor = if actor_user_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = ensure_user(&mut conn, Some(actor_user_id), None, None)?;
        build_user_from_sim_record(&actor_record, false)
    };
    let mut approved_payload = Map::<String, Value>::new();
    approved_payload.insert(
        "suggested_post_message".to_string(),
        suggested_message.clone(),
    );
    approved_payload.insert("send_date".to_string(), Value::from(approved_send_date));
    if let Some(price) = extract_suggested_post_price_from_message(
        approved_payload
            .get("suggested_post_message")
            .unwrap_or(&Value::Null),
    ) {
        approved_payload.insert("price".to_string(), price);
    }

    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert(
        "suggested_post_approved".to_string(),
        Value::Object(approved_payload),
    );
    let direct_messages_chat_obj = build_chat_from_group_record(&direct_messages_chat);
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &direct_messages_chat.chat_key,
        &direct_messages_chat_obj,
        &actor,
        now,
        format!(
            "{} approved a suggested post",
            display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    if approved_send_date <= now {
        let _ = finalize_due_suggested_post_if_ready(
            state,
            &mut conn,
            token,
            &bot,
            &direct_messages_chat,
            request.message_id,
            actor_user_id,
        )?;
    }

    Ok(json!(true))
}

fn handle_decline_suggested_post(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeclineSuggestedPostRequest = parse_request(params)?;

    if let Some(comment) = request.comment.as_ref() {
        if comment.chars().count() > 128 {
            return Err(ApiError::bad_request("comment is too long"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let direct_messages_chat = load_direct_messages_chat_for_request(
        &mut conn,
        bot.id,
        request.chat_id,
    )?;

    let parent_channel_chat_key = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?
        .to_string();
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    ensure_channel_member_can_manage_direct_messages(
        &mut conn,
        bot.id,
        &parent_channel_chat_key,
        actor_user_id,
    )?;

    let suggested_message = load_suggested_post_message_for_service(
        &mut conn,
        &bot,
        direct_messages_chat.chat_id,
        request.message_id,
    )?;

    ensure_sim_suggested_posts_storage(&mut conn)?;
    let existing = load_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
    )?;

    if let Some((state, _)) = existing.as_ref() {
        if state == "approved" {
            return Err(ApiError::bad_request(
                "suggested post is already approved",
            ));
        }
        if state == "paid" {
            return Err(ApiError::bad_request(
                "suggested post is already paid",
            ));
        }
        if state == "refunded" {
            return Ok(json!(true));
        }
        if state == "declined" {
            return Ok(json!(true));
        }
    }

    let now = Utc::now().timestamp();
    let normalized_comment = request
        .comment
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    upsert_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
        "declined",
        None,
        normalized_comment,
        now,
    )?;

    let actor = if actor_user_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = ensure_user(&mut conn, Some(actor_user_id), None, None)?;
        build_user_from_sim_record(&actor_record, false)
    };
    let mut declined_payload = Map::<String, Value>::new();
    declined_payload.insert("suggested_post_message".to_string(), suggested_message);
    if let Some(comment) = normalized_comment {
        declined_payload.insert("comment".to_string(), Value::String(comment.to_string()));
    }

    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert(
        "suggested_post_declined".to_string(),
        Value::Object(declined_payload),
    );
    let direct_messages_chat_obj = build_chat_from_group_record(&direct_messages_chat);
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &direct_messages_chat.chat_key,
        &direct_messages_chat_obj,
        &actor,
        now,
        format!(
            "{} declined a suggested post",
            display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(json!(true))
}

fn handle_gift_premium_subscription(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GiftPremiumSubscriptionRequest = parse_request(params)?;

    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.month_count <= 0 {
        return Err(ApiError::bad_request("month_count must be greater than zero"));
    }
    if request.star_count <= 0 {
        return Err(ApiError::bad_request("star_count must be greater than zero"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let recipient = ensure_user(&mut conn, Some(request.user_id), None, None)?;
    let sender = ensure_user(&mut conn, current_request_actor_user_id(), None, None)?;

    let now = Utc::now().timestamp();
    ensure_bot_star_balance_for_charge(&mut conn, bot.id, request.star_count, now)?;

    let premium_charge_id = format!(
        "gift_premium_subscription_{}",
        generate_telegram_numeric_id(),
    );

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_premium_subscription')",
        params![
            format!("gift_premium_{}", generate_telegram_numeric_id()),
            bot.id,
            recipient.id,
            premium_charge_id,
            -request.star_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO star_subscriptions
         (bot_id, user_id, telegram_payment_charge_id, is_canceled, updated_at)
         VALUES (?1, ?2, ?3, 0, ?4)
         ON CONFLICT(bot_id, user_id, telegram_payment_charge_id)
         DO UPDATE SET is_canceled = 0, updated_at = excluded.updated_at",
        params![bot.id, recipient.id, premium_charge_id, now],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE users
         SET is_premium = 1,
             gift_count = COALESCE(gift_count, 0) + 1
         WHERE id = ?1",
        params![recipient.id],
    )
    .map_err(ApiError::internal)?;

    let premium_gift_id = format!("premium_subscription_{}m", request.month_count);
    let premium_gift = Gift {
        id: premium_gift_id.clone(),
        sticker: build_sim_gift_sticker("premium_subscription", "💎", "laragram_premium_gifts"),
        star_count: request.star_count,
        upgrade_star_count: None,
        is_premium: Some(true),
        has_colors: Some(true),
        total_count: None,
        remaining_count: None,
        personal_total_count: None,
        personal_remaining_count: None,
        background: Some(GiftBackground {
            center_color: 0x7A7BFF,
            edge_color: 0x2A2E8F,
            text_color: 0xFFFFFF,
        }),
        unique_gift_variant_count: None,
        publisher_chat: None,
    };

    let premium_owned_gift_id = format!("owned_gift_{}", generate_telegram_numeric_id());
    let premium_text = request
        .text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let premium_entities_json = request
        .text_entities
        .as_ref()
        .map(|entities| serde_json::to_string(entities).map_err(ApiError::internal))
        .transpose()?;

    conn.execute(
        "INSERT INTO sim_owned_gifts
         (bot_id, owned_gift_id, owner_user_id, owner_chat_id, sender_user_id,
          gift_id, gift_json, gift_star_count, is_unique, is_unlimited, is_from_blockchain,
          send_date, text, entities_json, is_private, is_saved, can_be_upgraded, was_refunded,
          convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
          unique_gift_number, transfer_star_count, next_transfer_date, created_at, updated_at)
         VALUES (?1, ?2, ?3, NULL, ?4,
                 ?5, ?6, ?7, 0, 1, 0,
                 ?8, ?9, ?10, 0, 0, 0, 0,
                 NULL, NULL, 0,
                 NULL, NULL, NULL, ?8, ?8)",
        params![
            bot.id,
            premium_owned_gift_id.clone(),
            recipient.id,
            sender.id,
            premium_gift_id,
            serde_json::to_string(&premium_gift).map_err(ApiError::internal)?,
            request.star_count,
            now,
            premium_text.clone(),
            premium_entities_json,
        ],
    )
    .map_err(ApiError::internal)?;

    let sender_user = build_user_from_sim_record(&sender, false);
    let recipient_chat_key = recipient.id.to_string();
    ensure_chat(&mut conn, &recipient_chat_key)?;
    let recipient_chat = Chat {
        id: recipient.id,
        r#type: "private".to_string(),
        title: None,
        username: recipient.username.clone(),
        first_name: Some(recipient.first_name.clone()),
        last_name: recipient.last_name.clone(),
        is_forum: None,
        is_direct_messages: None,
    };

    let mut gift_payload = Map::<String, Value>::new();
    gift_payload.insert(
        "gift".to_string(),
        serde_json::to_value(&premium_gift).map_err(ApiError::internal)?,
    );
    gift_payload.insert(
        "owned_gift_id".to_string(),
        Value::String(premium_owned_gift_id),
    );
    gift_payload.insert("can_be_upgraded".to_string(), Value::Bool(false));
    gift_payload.insert("is_upgrade_separate".to_string(), Value::Bool(false));
    if let Some(text) = premium_text {
        gift_payload.insert("text".to_string(), Value::String(text));
    }
    if let Some(entities) = request.text_entities.as_ref() {
        gift_payload.insert(
            "entities".to_string(),
            serde_json::to_value(entities).map_err(ApiError::internal)?,
        );
    }

    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert("gift".to_string(), Value::Object(gift_payload));

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &recipient_chat_key,
        &recipient_chat,
        &sender_user,
        now,
        format!(
            "{} sent a gift",
            display_name_for_service_user(&sender_user)
        ),
        service_fields,
    )?;

    Ok(json!(true))
}

fn handle_get_user_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserGiftsRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if load_sim_user_record(&mut conn, request.user_id)?.is_none() {
        return Err(ApiError::not_found("user not found"));
    }

    let filter_options = SimOwnedGiftFilterOptions {
        exclude_unsaved: false,
        exclude_saved: false,
        exclude_unlimited: request.exclude_unlimited.unwrap_or(false),
        exclude_limited_upgradable: request.exclude_limited_upgradable.unwrap_or(false),
        exclude_limited_non_upgradable: request.exclude_limited_non_upgradable.unwrap_or(false),
        exclude_from_blockchain: request.exclude_from_blockchain.unwrap_or(false),
        exclude_unique: request.exclude_unique.unwrap_or(false),
        sort_by_price: request.sort_by_price.unwrap_or(false),
    };

    let records = load_owned_gift_records(&mut conn, bot.id, Some(request.user_id), None)?;
    let filtered = apply_owned_gift_filters(records, &filter_options);
    let total_count = filtered.len() as i64;
    let offset = parse_owned_gifts_offset(request.offset.as_deref());
    let limit = parse_owned_gifts_limit(request.limit);

    let page_records = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let next_offset = if offset + page_records.len() < total_count as usize {
        Some((offset + page_records.len()).to_string())
    } else {
        None
    };

    let gifts = page_records
        .iter()
        .map(|record| map_owned_gift_record(&mut conn, record))
        .collect::<Result<Vec<_>, _>>()?;

    let result = OwnedGifts {
        total_count,
        gifts,
        next_offset,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

fn handle_get_chat_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatGiftsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chat_id_as_i64(&request.chat_id, &chat_key);
    let sim_chat = load_sim_chat_record(&mut conn, bot.id, &chat_key)?
        .or(load_sim_chat_record_by_chat_id(&mut conn, bot.id, chat_id)?)
        .ok_or_else(|| ApiError::not_found("chat not found"))?;

    let filter_options = SimOwnedGiftFilterOptions {
        exclude_unsaved: request.exclude_unsaved.unwrap_or(false),
        exclude_saved: request.exclude_saved.unwrap_or(false),
        exclude_unlimited: request.exclude_unlimited.unwrap_or(false),
        exclude_limited_upgradable: request.exclude_limited_upgradable.unwrap_or(false),
        exclude_limited_non_upgradable: request.exclude_limited_non_upgradable.unwrap_or(false),
        exclude_from_blockchain: request.exclude_from_blockchain.unwrap_or(false),
        exclude_unique: request.exclude_unique.unwrap_or(false),
        sort_by_price: request.sort_by_price.unwrap_or(false),
    };

    let records = load_owned_gift_records(&mut conn, bot.id, None, Some(sim_chat.chat_id))?;
    let filtered = apply_owned_gift_filters(records, &filter_options);
    let total_count = filtered.len() as i64;
    let offset = parse_owned_gifts_offset(request.offset.as_deref());
    let limit = parse_owned_gifts_limit(request.limit);

    let page_records = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let next_offset = if offset + page_records.len() < total_count as usize {
        Some((offset + page_records.len()).to_string())
    } else {
        None
    };

    let gifts = page_records
        .iter()
        .map(|record| map_owned_gift_record(&mut conn, record))
        .collect::<Result<Vec<_>, _>>()?;

    let result = OwnedGifts {
        total_count,
        gifts,
        next_offset,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

fn handle_send_message(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendMessageRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_entities = request
        .entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let should_auto_detect_entities = explicit_entities.is_none();
    let (parsed_text, parsed_entities) = parse_formatted_text(
        &request.text,
        request.parse_mode.as_deref(),
        explicit_entities,
    );
    let parsed_entities = if should_auto_detect_entities {
        merge_auto_message_entities(&parsed_text, parsed_entities)
    } else {
        parsed_entities
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat) = resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &request.chat_id,
        ChatSendKind::Text,
    )?;
    let business_connection_id = resolve_outbound_business_connection_for_bot_message(
        &mut conn,
        bot.id,
        &chat,
        request.business_connection_id.as_deref(),
    )?;
    let message_thread_id = resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Text,
    )?;

    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        request.reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, parsed_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let base_message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender,
        "text": parsed_text,
    });

    let mut base_message_json = base_message_json;
    if let Some(entities) = parsed_entities {
        base_message_json["entities"] = entities;
    }
    if let Some(thread_id) = message_thread_id {
        base_message_json["message_thread_id"] = Value::from(thread_id);
        base_message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        base_message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if let Some(mode) = sim_parse_mode {
        base_message_json["sim_parse_mode"] = Value::String(mode);
    }
    let message: Message = serde_json::from_value(base_message_json).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let is_business_message = business_connection_id.is_some();

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post || is_business_message {
            None
        } else {
            Some(message.clone())
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(message.clone())
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: if is_business_message {
            Some(message.clone())
        } else {
            None
        },
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();

    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }
    if is_business_message {
        update_value["business_message"] = message_value.clone();
    } else if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update.clone());

    if is_channel_post {
        ensure_linked_discussion_forward_for_channel_post(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        )?;
    }

    Ok(message_value)
}

fn handle_send_message_draft(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let mut normalized_params = params.clone();
    if let Some(raw_text) = normalized_params.get("text").cloned() {
        if !raw_text.is_string() {
            if let Some(text) = value_to_optional_string(&raw_text) {
                normalized_params.insert("text".to_string(), Value::String(text));
            }
        }
    }

    let request: SendMessageDraftRequest = parse_request(&normalized_params)?;
    if request.draft_id <= 0 {
        return Err(ApiError::bad_request("draft_id is invalid"));
    }

    let chat_id_value = Value::from(request.chat_id);
    let (chat_key, resolved_message_thread_id, existing_message_id) = {
        let mut conn = lock_db(state)?;
        let bot = ensure_bot(&mut conn, token)?;

        let (chat_key, chat) = resolve_bot_outbound_chat(
            &mut conn,
            bot.id,
            &chat_id_value,
            ChatSendKind::Text,
        )?;
        if chat.r#type != "private" {
            return Err(ApiError::bad_request(
                "sendMessageDraft is available only in private chats",
            ));
        }
        let resolved_message_thread_id = resolve_forum_message_thread_for_chat_key(
            &mut conn,
            bot.id,
            &chat_key,
            request.message_thread_id,
        )?;
        let message_thread_scope = resolved_message_thread_id.unwrap_or(0);

        let existing_message_id: Option<i64> = conn
            .query_row(
                "SELECT message_id
                 FROM sim_message_drafts
                 WHERE bot_id = ?1
                   AND chat_key = ?2
                   AND message_thread_id = ?3
                   AND draft_id = ?4",
                params![bot.id, &chat_key, message_thread_scope, request.draft_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        (chat_key, resolved_message_thread_id, existing_message_id)
    };

    let message_thread_scope = resolved_message_thread_id.unwrap_or(0);

    if let Some(message_id) = existing_message_id {
        let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
        let explicit_entities = request
            .entities
            .as_ref()
            .and_then(|v| serde_json::to_value(v).ok());
        let should_auto_detect_entities = explicit_entities.is_none();
        let (parsed_text, parsed_entities) = parse_formatted_text(
            &request.text,
            request.parse_mode.as_deref(),
            explicit_entities,
        );
        let parsed_entities = if should_auto_detect_entities {
            merge_auto_message_entities(&parsed_text, parsed_entities)
        } else {
            parsed_entities
        };

        if parsed_text.trim().is_empty() {
            return Err(ApiError::bad_request("text is empty"));
        }

        let mut conn = lock_db(state)?;
        let bot = ensure_bot(&mut conn, token)?;
        let updated = conn
            .execute(
                "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
                params![parsed_text, bot.id, &chat_key, message_id],
            )
            .map_err(ApiError::internal)?;

        if updated == 0 {
            conn.execute(
                "DELETE FROM sim_message_drafts
                 WHERE bot_id = ?1
                   AND chat_key = ?2
                   AND message_thread_id = ?3
                   AND draft_id = ?4",
                params![bot.id, &chat_key, message_thread_scope, request.draft_id],
            )
            .map_err(ApiError::internal)?;
        } else {
            let mut streamed_message = load_message_value(&mut conn, &bot, message_id)?;
            if let Some(entities) = parsed_entities {
                streamed_message["entities"] = entities;
            } else {
                streamed_message.as_object_mut().map(|obj| obj.remove("entities"));
            }
            if let Some(mode) = sim_parse_mode {
                streamed_message["sim_parse_mode"] = Value::String(mode);
            } else {
                streamed_message
                    .as_object_mut()
                    .map(|obj| obj.remove("sim_parse_mode"));
            }
            streamed_message.as_object_mut().map(|obj| obj.remove("edit_date"));

            let update_value = serde_json::to_value(Update {
                update_id: 0,
                message: Some(
                    serde_json::from_value(streamed_message.clone()).map_err(ApiError::internal)?,
                ),
                edited_message: None,
                channel_post: None,
                edited_channel_post: None,
                business_connection: None,
                business_message: None,
                edited_business_message: None,
                deleted_business_messages: None,
                message_reaction: None,
                message_reaction_count: None,
                inline_query: None,
                chosen_inline_result: None,
                callback_query: None,
                shipping_query: None,
                pre_checkout_query: None,
                purchased_paid_media: None,
                poll: None,
                poll_answer: None,
                my_chat_member: None,
                chat_member: None,
                chat_join_request: None,
                chat_boost: None,
                removed_chat_boost: None,
                managed_bot: None,
            })
            .map_err(ApiError::internal)?;
            persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

            let now = Utc::now().timestamp();
            conn.execute(
                "UPDATE sim_message_drafts
                 SET updated_at = ?1
                 WHERE bot_id = ?2
                   AND chat_key = ?3
                   AND message_thread_id = ?4
                   AND draft_id = ?5",
                params![
                    now,
                    bot.id,
                    &chat_key,
                    message_thread_scope,
                    request.draft_id,
                ],
            )
            .map_err(ApiError::internal)?;

            return Ok(Value::Bool(true));
        }
    }

    let mut send_params = HashMap::<String, Value>::new();
    send_params.insert("chat_id".to_string(), chat_id_value);
    if let Some(thread_id) = resolved_message_thread_id {
        send_params.insert("message_thread_id".to_string(), Value::from(thread_id));
    }
    send_params.insert("text".to_string(), Value::String(request.text));

    if let Some(parse_mode) = request.parse_mode {
        send_params.insert("parse_mode".to_string(), Value::String(parse_mode));
    }

    if let Some(entities) = request.entities {
        send_params.insert(
            "entities".to_string(),
            serde_json::to_value(entities).map_err(ApiError::internal)?,
        );
    }

    let created_message = handle_send_message(state, token, &send_params)?;
    let message_id = created_message
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("sendMessageDraft failed to return a message_id"))?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_message_drafts
         (bot_id, chat_key, message_thread_id, draft_id, message_id, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(bot_id, chat_key, message_thread_id, draft_id)
         DO UPDATE SET
            message_id = excluded.message_id,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            &chat_key,
            message_thread_scope,
            request.draft_id,
            message_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::Bool(true))
}

fn handle_forward_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ForwardMessageRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    forward_message_internal(
        state,
        &mut conn,
        token,
        &bot,
        &request.from_chat_id,
        &request.chat_id,
        request.message_id,
        request.message_thread_id,
        request.protect_content,
    )
}

fn handle_forward_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ForwardMessagesRequest = parse_request(params)?;
    if request.message_ids.is_empty() {
        return Err(ApiError::bad_request("message_ids must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut forwarded = Vec::new();

    for message_id in request.message_ids {
        match forward_message_internal(
            state,
            &mut conn,
            token,
            &bot,
            &request.from_chat_id,
            &request.chat_id,
            message_id,
            request.message_thread_id,
            request.protect_content,
        ) {
            Ok(forwarded_message) => {
                if let Some(id) = forwarded_message.get("message_id").and_then(Value::as_i64) {
                    forwarded.push(json!({ "message_id": id }));
                }
            }
            Err(error) => {
                if error.code >= 500 {
                    return Err(error);
                }
            }
        }
    }

    Ok(Value::Array(forwarded))
}

fn handle_copy_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CopyMessageRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|entities| serde_json::to_value(entities).ok());
    let (caption_override, caption_entities_override) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let copied_message = copy_message_internal(
        state,
        &mut conn,
        token,
        &bot,
        &request.from_chat_id,
        &request.chat_id,
        request.message_id,
        request.message_thread_id,
        caption_override,
        caption_entities_override,
        false,
        request.show_caption_above_media,
        request.reply_markup,
        request.protect_content,
        None,
        None,
        None,
        None,
        false,
    )?;

    let message_id = copied_message
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("copyMessage result is missing message_id"))?;

    Ok(json!({ "message_id": message_id }))
}

fn handle_copy_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CopyMessagesRequest = parse_request(params)?;
    if request.message_ids.is_empty() {
        return Err(ApiError::bad_request("message_ids must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut copied = Vec::new();

    for message_id in request.message_ids {
        match copy_message_internal(
            state,
            &mut conn,
            token,
            &bot,
            &request.from_chat_id,
            &request.chat_id,
            message_id,
            request.message_thread_id,
            None,
            None,
            request.remove_caption.unwrap_or(false),
            None,
            None,
            request.protect_content,
            None,
            None,
            None,
            None,
            false,
        ) {
            Ok(copied_message) => {
                if let Some(id) = copied_message.get("message_id").and_then(Value::as_i64) {
                    copied.push(json!({ "message_id": id }));
                }
            }
            Err(error) => {
                if error.code >= 500 {
                    return Err(error);
                }
            }
        }
    }

    Ok(Value::Array(copied))
}

#[derive(Clone)]
struct LinkedDiscussionTransportContext {
    channel_chat_key: String,
    channel_message_id: i64,
    discussion_root_message_id: Option<i64>,
}

fn forward_message_internal(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    to_chat_id_value: &Value,
    source_message_id: i64,
    message_thread_id: Option<i64>,
    protect_content: Option<bool>,
) -> Result<Value, ApiError> {
    let source_message = resolve_source_message_for_transport(
        conn,
        bot,
        from_chat_id_value,
        source_message_id,
        false,
    )?;

    let send_kind = send_kind_from_transport_source_message(&source_message);
    let (destination_chat_key, destination_chat) =
        resolve_bot_outbound_chat(conn, bot.id, to_chat_id_value, send_kind)?;
    let sender_user = resolve_transport_sender_user(
        conn,
        bot,
        &destination_chat_key,
        &destination_chat,
        send_kind,
    )?;
    let resolved_thread_id = resolve_forum_message_thread_for_chat_key(
        conn,
        bot.id,
        &destination_chat_key,
        message_thread_id,
    )?;

    let mut message_value = source_message;
    let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;
    let forward_origin = message_value
        .get("forward_origin")
        .cloned()
        .unwrap_or_else(|| build_forward_origin_from_source_message(&message_value));

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("forwarded message payload is invalid"))?;
    object.remove("reply_to_message");
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");
    object.remove("sender_chat");
    object.remove("is_automatic_forward");
    object.insert("from".to_string(), sender_user_value);
    object.insert("forward_origin".to_string(), forward_origin);

    if let Some(thread_id) = resolved_thread_id {
        object.insert("message_thread_id".to_string(), Value::from(thread_id));
        object.insert("is_topic_message".to_string(), Value::Bool(true));
    } else {
        object.remove("message_thread_id");
        object.remove("is_topic_message");
    }

    if let Some(should_protect) = protect_content {
        object.insert(
            "has_protected_content".to_string(),
            Value::Bool(should_protect),
        );
    }

    persist_transported_message(
        state,
        conn,
        token,
        bot,
        &destination_chat_key,
        &destination_chat,
        &sender_user,
        message_value,
        None,
    )
}

fn copy_message_internal(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    to_chat_id_value: &Value,
    source_message_id: i64,
    message_thread_id: Option<i64>,
    caption_override: Option<String>,
    caption_entities_override: Option<Value>,
    remove_caption: bool,
    show_caption_above_media: Option<bool>,
    reply_markup_override: Option<Value>,
    protect_content: Option<bool>,
    reply_to_message_id_override: Option<i64>,
    sender_chat_override: Option<Value>,
    is_automatic_forward_override: Option<bool>,
    linked_discussion_context: Option<LinkedDiscussionTransportContext>,
    skip_source_membership_check: bool,
) -> Result<Value, ApiError> {
    let source_message = resolve_source_message_for_transport(
        conn,
        bot,
        from_chat_id_value,
        source_message_id,
        skip_source_membership_check,
    )?;

    let send_kind = send_kind_from_transport_source_message(&source_message);
    let (destination_chat_key, destination_chat) =
        resolve_bot_outbound_chat(conn, bot.id, to_chat_id_value, send_kind)?;
    let sender_user = resolve_transport_sender_user(
        conn,
        bot,
        &destination_chat_key,
        &destination_chat,
        send_kind,
    )?;
    let resolved_thread_id = resolve_forum_message_thread_for_chat_key(
        conn,
        bot.id,
        &destination_chat_key,
        message_thread_id,
    )?;

    let normalized_reply_markup = match reply_markup_override {
        Some(markup) => {
            handle_reply_markup_state(conn, bot.id, &destination_chat_key, Some(&markup))?
        }
        None => None,
    };
    let reply_to_message_value = reply_to_message_id_override
        .map(|reply_id| load_reply_message_for_chat(conn, bot, &destination_chat_key, reply_id))
        .transpose()?;

    let mut message_value = source_message;
    let source_has_media = message_has_media(&message_value);
    let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("copied message payload is invalid"))?;
    object.remove("forward_origin");
    object.remove("is_automatic_forward");
    object.remove("reply_to_message");
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");
    object.remove("sender_chat");
    object.insert("from".to_string(), sender_user_value);

    if let Some(sender_chat_value) = sender_chat_override {
        object.insert("sender_chat".to_string(), sender_chat_value);
    }
    if let Some(is_automatic_forward) = is_automatic_forward_override {
        object.insert(
            "is_automatic_forward".to_string(),
            Value::Bool(is_automatic_forward),
        );
    }

    if source_has_media {
        if remove_caption {
            object.remove("caption");
            object.remove("caption_entities");
        } else if let Some(caption) = caption_override {
            object.insert("caption".to_string(), Value::String(caption));
            if let Some(entities) = caption_entities_override {
                object.insert("caption_entities".to_string(), entities);
            } else {
                object.remove("caption_entities");
            }
        }

        if let Some(show_above) = show_caption_above_media {
            object.insert("show_caption_above_media".to_string(), Value::Bool(show_above));
        }
    }

    if let Some(markup) = normalized_reply_markup {
        object.insert("reply_markup".to_string(), markup);
    }
    if let Some(reply_value) = reply_to_message_value {
        object.insert("reply_to_message".to_string(), reply_value);
    }

    if let Some(thread_id) = resolved_thread_id {
        object.insert("message_thread_id".to_string(), Value::from(thread_id));
        object.insert("is_topic_message".to_string(), Value::Bool(true));
    } else {
        object.remove("message_thread_id");
        object.remove("is_topic_message");
    }

    if let Some(should_protect) = protect_content {
        object.insert(
            "has_protected_content".to_string(),
            Value::Bool(should_protect),
        );
    }

    persist_transported_message(
        state,
        conn,
        token,
        bot,
        &destination_chat_key,
        &destination_chat,
        &sender_user,
        message_value,
        linked_discussion_context.as_ref(),
    )
}

fn resolve_transport_sender_user(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    send_kind: ChatSendKind,
) -> Result<User, ApiError> {
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    if actor_user_id == bot.id {
        return Ok(build_bot_user(bot));
    }

    let actor_record = ensure_sim_user_record(conn, actor_user_id)?;
    if destination_chat.r#type != "private" {
        ensure_sender_can_send_in_chat(conn, bot.id, destination_chat_key, actor_user_id, send_kind)?;
    }

    Ok(build_user_from_sim_record(&actor_record, false))
}

fn resolve_sender_for_bot_outbound_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    send_kind: ChatSendKind,
) -> Result<User, ApiError> {
    if destination_chat.r#type == "channel" {
        return resolve_transport_sender_user(
            conn,
            bot,
            destination_chat_key,
            destination_chat,
            send_kind,
        );
    }

    Ok(build_bot_user(bot))
}

fn resolve_source_message_for_transport(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    source_message_id: i64,
    skip_source_membership_check: bool,
) -> Result<Value, ApiError> {
    let source_chat_key = value_to_chat_key(from_chat_id_value)?;
    ensure_chat(conn, &source_chat_key)?;

    if let Some(sim_chat) = load_sim_chat_record(conn, bot.id, &source_chat_key)? {
        if sim_chat.chat_type != "private" && !skip_source_membership_check {
            ensure_sender_is_chat_member(conn, bot.id, &source_chat_key, bot.id)?;
            if let Some(actor_user_id) = current_request_actor_user_id() {
                if actor_user_id != bot.id {
                    ensure_sender_is_chat_member(conn, bot.id, &source_chat_key, actor_user_id)?;
                }
            }
        }
    }

    let source_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &source_chat_key, source_message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if source_exists.is_none() {
        return Err(ApiError::bad_request("source message was not found"));
    }

    let source_message = load_message_value(conn, bot, source_message_id)?;

    if is_service_message_for_transport(&source_message) {
        return Err(ApiError::bad_request(
            "service messages can't be forwarded or copied",
        ));
    }

    if !message_has_transportable_content(&source_message) {
        return Err(ApiError::bad_request(
            "message content can't be forwarded or copied",
        ));
    }

    if source_message
        .get("has_protected_content")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "message has protected content and can't be forwarded or copied",
        ));
    }

    Ok(source_message)
}

fn is_service_message_for_transport(message: &Value) -> bool {
    [
        "new_chat_members",
        "left_chat_member",
        "new_chat_title",
        "new_chat_photo",
        "delete_chat_photo",
        "group_chat_created",
        "supergroup_chat_created",
        "channel_chat_created",
        "message_auto_delete_timer_changed",
        "pinned_message",
        "forum_topic_created",
        "forum_topic_edited",
        "forum_topic_closed",
        "forum_topic_reopened",
        "general_forum_topic_hidden",
        "general_forum_topic_unhidden",
        "write_access_allowed",
        "users_shared",
        "chat_shared",
        "giveaway_created",
        "video_chat_started",
        "video_chat_ended",
        "video_chat_participants_invited",
    ]
    .iter()
    .any(|key| message.get(*key).is_some())
}

fn message_has_transportable_content(message: &Value) -> bool {
    [
        "text",
        "photo",
        "video",
        "audio",
        "voice",
        "document",
        "animation",
        "video_note",
        "sticker",
        "poll",
        "dice",
        "game",
        "contact",
        "location",
        "venue",
        "invoice",
        "paid_media",
    ]
    .iter()
    .any(|key| message.get(*key).is_some())
}

fn forward_channel_post_to_linked_discussion_best_effort(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    channel_chat_key: &str,
    channel_message_value: &Value,
) {
    if let Err(error) = ensure_linked_discussion_forward_for_channel_post(
        state,
        conn,
        token,
        bot,
        channel_chat_key,
        channel_message_value,
    ) {
        eprintln!(
            "linked discussion auto-forward failed for chat {}: {}",
            channel_chat_key, error.description
        );
    }
}

fn send_kind_from_transport_source_message(message: &Value) -> ChatSendKind {
    if message.get("text").is_some() {
        return ChatSendKind::Text;
    }
    if message.get("photo").is_some() {
        return ChatSendKind::Photo;
    }
    if message.get("video").is_some() {
        return ChatSendKind::Video;
    }
    if message.get("audio").is_some() {
        return ChatSendKind::Audio;
    }
    if message.get("voice").is_some() {
        return ChatSendKind::Voice;
    }
    if message.get("document").is_some() {
        return ChatSendKind::Document;
    }
    if message.get("video_note").is_some() {
        return ChatSendKind::VideoNote;
    }
    if message.get("poll").is_some() {
        return ChatSendKind::Poll;
    }
    if message.get("invoice").is_some() {
        return ChatSendKind::Invoice;
    }

    ChatSendKind::Other
}

fn build_forward_origin_from_source_message(source_message: &Value) -> Value {
    let source_date = source_message
        .get("date")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| Utc::now().timestamp());

    if let Some(source_chat) = source_message.get("chat") {
        let chat_type = source_chat
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if chat_type == "channel" {
            return json!({
                "type": "channel",
                "date": source_date,
                "chat": source_chat,
                "message_id": source_message.get("message_id").and_then(Value::as_i64).unwrap_or(0),
                "author_signature": source_message.get("author_signature").cloned().unwrap_or(Value::Null),
            });
        }
    }

    if let Some(sender_chat) = source_message.get("sender_chat") {
        return json!({
            "type": "chat",
            "date": source_date,
            "sender_chat": sender_chat,
            "author_signature": source_message.get("author_signature").cloned().unwrap_or(Value::Null),
        });
    }

    if let Some(sender_user) = source_message.get("from") {
        return json!({
            "type": "user",
            "date": source_date,
            "sender_user": sender_user,
        });
    }

    json!({
        "type": "hidden_user",
        "date": source_date,
        "sender_user_name": source_message
            .get("author_signature")
            .and_then(Value::as_str)
            .unwrap_or("Hidden User"),
    })
}

fn persist_transported_message(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    sender_user: &User,
    mut message_value: Value,
    linked_discussion_context: Option<&LinkedDiscussionTransportContext>,
) -> Result<Value, ApiError> {
    let now = Utc::now().timestamp();
    let persisted_text = message_value
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message_value.get("caption").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, destination_chat_key, sender_user.id, persisted_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let destination_chat_value = serde_json::to_value(destination_chat).map_err(ApiError::internal)?;
    let sender_user_value = serde_json::to_value(sender_user).map_err(ApiError::internal)?;

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("transported message payload is invalid"))?;
    object.insert("message_id".to_string(), Value::from(message_id));
    object.insert("date".to_string(), Value::from(now));
    object.insert("chat".to_string(), destination_chat_value);
    object.insert("from".to_string(), sender_user_value);
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");

    if let Some(context) = linked_discussion_context {
        let discussion_root_message_id = context
            .discussion_root_message_id
            .unwrap_or(message_id);
        object.insert(
            "linked_channel_chat_id".to_string(),
            Value::String(context.channel_chat_key.clone()),
        );
        object.insert(
            "linked_channel_message_id".to_string(),
            Value::from(context.channel_message_id),
        );
        object.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
    }

    let update_value = if destination_chat.r#type == "channel" {
        json!({
            "update_id": 0,
            "channel_post": message_value.clone(),
        })
    } else {
        json!({
            "update_id": 0,
            "message": message_value.clone(),
        })
    };

    persist_and_dispatch_update(state, conn, token, bot.id, update_value)?;
    Ok(message_value)
}

fn handle_send_photo(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendPhotoRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.photo, "photo")?;
    let photo = json!([
        {
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "width": 1280,
            "height": 720,
            "file_size": file.file_size,
        }
    ]);

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "photo",
        photo,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_audio(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendAudioRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.audio, "audio")?;

    let audio = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "duration": request.duration.unwrap_or(0),
        "performer": request.performer,
        "title": request.title,
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "audio",
        audio,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_document(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendDocumentRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.document, "document")?;

    let document = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "document",
        document,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_video(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVideoRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.video, "video")?;

    let video = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "width": request.width.unwrap_or(1280),
        "height": request.height.unwrap_or(720),
        "duration": request.duration.unwrap_or(0),
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "video",
        video,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_voice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVoiceRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.voice, "voice")?;

    let voice = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "duration": request.duration.unwrap_or(0),
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "voice",
        voice,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_contact(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendContactRequest = parse_request(params)?;
    if request.phone_number.trim().is_empty() {
        return Err(ApiError::bad_request("phone_number is empty"));
    }
    if request.first_name.trim().is_empty() {
        return Err(ApiError::bad_request("first_name is empty"));
    }

    let contact = Contact {
        phone_number: request.phone_number,
        first_name: request.first_name,
        last_name: request.last_name,
        user_id: None,
        vcard: request.vcard,
    };

    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "contact",
        serde_json::to_value(contact).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

fn handle_send_location(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendLocationRequest = parse_request(params)?;

    let location = Location {
        latitude: request.latitude,
        longitude: request.longitude,
        horizontal_accuracy: request.horizontal_accuracy,
        live_period: request.live_period,
        heading: request.heading,
        proximity_alert_radius: request.proximity_alert_radius,
    };

    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "location",
        serde_json::to_value(location).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

fn handle_send_venue(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVenueRequest = parse_request(params)?;
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.address.trim().is_empty() {
        return Err(ApiError::bad_request("address is empty"));
    }

    let venue = Venue {
        location: Location {
            latitude: request.latitude,
            longitude: request.longitude,
            horizontal_accuracy: None,
            live_period: None,
            heading: None,
            proximity_alert_radius: None,
        },
        title: request.title,
        address: request.address,
        foursquare_id: request.foursquare_id,
        foursquare_type: request.foursquare_type,
        google_place_id: request.google_place_id,
        google_place_type: request.google_place_type,
    };

    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "venue",
        serde_json::to_value(venue).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

fn handle_send_dice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendDiceRequest = parse_request(params)?;
    let emoji = request.emoji.unwrap_or_else(|| "🎲".to_string());

    let max_value = match emoji.as_str() {
        "🎯" | "🎲" | "🏀" | "🎳" => 6,
        "⚽" | "🏐" => 5,
        "🎰" => 64,
        _ => return Err(ApiError::bad_request("unsupported dice emoji")),
    };
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default().unsigned_abs();
    let value = (now_nanos % (max_value as u64)) as i64 + 1;

    let dice = Dice { emoji, value };
    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "dice",
        serde_json::to_value(dice).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

fn handle_send_game(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendGameRequest = parse_request(params)?;
    if request.game_short_name.trim().is_empty() {
        return Err(ApiError::bad_request("game_short_name is empty"));
    }

    let game = Game {
        title: request.game_short_name.clone(),
        description: format!("Game {}", request.game_short_name),
        photo: vec![PhotoSize {
            file_id: generate_telegram_file_id("game_photo"),
            file_unique_id: generate_telegram_file_unique_id(),
            width: 512,
            height: 512,
            file_size: None,
        }],
        text: None,
        text_entities: None,
        animation: None,
    };

    let message_value = send_payload_message(
        state,
        token,
        &Value::from(request.chat_id),
        None,
        None,
        request.reply_markup.as_ref().and_then(|m| serde_json::to_value(m).ok()),
        "game",
        serde_json::to_value(&game).map_err(ApiError::internal)?,
        request.message_thread_id,
    )?;

    let message_id = message_value
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("missing message_id in sendGame result"))?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = request.chat_id.to_string();
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO games (bot_id, chat_key, message_id, game_short_name, title, description, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            bot.id,
            chat_key,
            message_id,
            request.game_short_name,
            game.title,
            game.description,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(message_value)
}

fn handle_send_animation(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendAnimationRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let animation_input = parse_input_file_value(&request.animation, "animation")?;
    let file = resolve_media_file(state, token, &animation_input, "animation")?;

    let animation = serde_json::to_value(Animation {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        width: request.width.unwrap_or(512),
        height: request.height.unwrap_or(512),
        duration: request.duration.unwrap_or(0),
        thumbnail: None,
        file_name: Some(file.file_path.split('/').last().unwrap_or("animation.mp4").to_string()),
        mime_type: file.mime_type,
        file_size: file.file_size,
    })
    .map_err(ApiError::internal)?;

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "animation",
        animation,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_video_note(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVideoNoteRequest = parse_request(params)?;
    let video_note_input = parse_input_file_value(&request.video_note, "video_note")?;
    let file = resolve_media_file(state, token, &video_note_input, "video_note")?;

    let length = request.length.unwrap_or(384).max(1);
    let video_note = serde_json::to_value(VideoNote {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        length,
        duration: request.duration.unwrap_or(0),
        thumbnail: None,
        file_size: file.file_size,
    })
    .map_err(ApiError::internal)?;

    send_media_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "video_note",
        video_note,
        request.message_thread_id,
        None,
    )
}

fn handle_send_sticker(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendStickerRequest = parse_request(params)?;
    let sticker_input = parse_input_file_value(&request.sticker, "sticker")?;
    let file = resolve_media_file(state, token, &sticker_input, "sticker")?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sticker_meta = load_sticker_meta(&mut conn, bot.id, &file.file_id)?;
    drop(conn);

    let format = sticker_meta
        .as_ref()
        .map(|m| m.format.as_str())
        .or_else(|| infer_sticker_format_from_file(&file))
        .unwrap_or("static");
    let is_animated = format == "animated";
    let is_video = format == "video";

    let sticker = Sticker {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        r#type: sticker_meta
            .as_ref()
            .map(|m| m.sticker_type.clone())
            .unwrap_or_else(|| "regular".to_string()),
        width: 512,
        height: 512,
        is_animated,
        is_video,
        thumbnail: None,
        emoji: request.emoji.or_else(|| sticker_meta.as_ref().and_then(|m| m.emoji.clone())),
        set_name: sticker_meta.as_ref().and_then(|m| m.set_name.clone()),
        premium_animation: None,
        mask_position: sticker_meta
            .as_ref()
            .and_then(|m| m.mask_position_json.as_ref())
            .and_then(|raw| serde_json::from_str::<MaskPosition>(raw).ok()),
        custom_emoji_id: sticker_meta.as_ref().and_then(|m| m.custom_emoji_id.clone()),
        needs_repainting: sticker_meta.as_ref().map(|m| m.needs_repainting),
        file_size: file.file_size,
    };

    send_media_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "sticker",
        serde_json::to_value(sticker).map_err(ApiError::internal)?,
        request.message_thread_id,
        None,
    )
}

fn handle_get_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetStickerSetRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_row: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT title, sticker_type, COALESCE(needs_repainting, 0)
             FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((title, sticker_type, _needs_repainting)) = set_row else {
        return Err(ApiError::not_found("sticker set not found"));
    };

    let stickers = load_set_stickers(&mut conn, bot.id, &request.name)?;
    let result = StickerSet {
        name: request.name,
        title,
        sticker_type,
        stickers,
        thumbnail: None,
    };

    Ok(serde_json::to_value(result).map_err(ApiError::internal)?)
}

fn handle_get_custom_emoji_stickers(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetCustomEmojiStickersRequest = parse_request(params)?;
    if request.custom_emoji_ids.is_empty() {
        return Ok(json!([]));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let placeholders = std::iter::repeat("?")
        .take(request.custom_emoji_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT file_id, file_unique_id, set_name, sticker_type, format, emoji, mask_position_json, custom_emoji_id, needs_repainting
         FROM stickers WHERE bot_id = ? AND custom_emoji_id IN ({})",
        placeholders,
    );

    let mut bind_values = Vec::with_capacity(1 + request.custom_emoji_ids.len());
    bind_values.push(Value::from(bot.id));
    for item in &request.custom_emoji_ids {
        bind_values.push(Value::from(item.clone()));
    }

    let mut stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)),
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, Option<String>>(7)?,
                    r.get::<_, i64>(8)?,
                ))
            },
        )
        .map_err(ApiError::internal)?;

    let mut stickers = Vec::new();
    for row in rows {
        let (file_id, file_unique_id, set_name, sticker_type, format, emoji, mask_position_json, custom_emoji_id, needs_repainting) = row.map_err(ApiError::internal)?;
        stickers.push(sticker_from_row(
            file_id,
            file_unique_id,
            set_name,
            sticker_type,
            format,
            emoji,
            mask_position_json,
            custom_emoji_id,
            needs_repainting == 1,
            None,
        ));
    }

    Ok(serde_json::to_value(stickers).map_err(ApiError::internal)?)
}

fn handle_upload_sticker_file(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let user_id = params
        .get("user_id")
        .and_then(|value| value.as_i64().or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok())))
        .ok_or_else(|| ApiError::bad_request("user_id is required"))?;
    let sticker_format = params
        .get("sticker_format")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_request("sticker_format is required"))?;
    let requested_format = normalize_sticker_format(&sticker_format)?;
    let sticker_raw = params
        .get("sticker")
        .ok_or_else(|| ApiError::bad_request("sticker is required"))?;
    let sticker_input = parse_input_file_value(sticker_raw, "sticker")?;
    let file = resolve_media_file(state, token, &sticker_input, "sticker")?;
    let format = infer_sticker_format_from_file(&file).unwrap_or(requested_format);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let (is_animated, is_video) = sticker_format_flags(format);

    conn.execute(
        "INSERT INTO stickers
         (bot_id, file_id, file_unique_id, set_name, sticker_type, format, width, height, is_animated, is_video,
          emoji, emoji_list_json, keywords_json, mask_position_json, custom_emoji_id, needs_repainting, position, created_at, updated_at)
         VALUES (?1, ?2, ?3, NULL, 'regular', ?4, 512, 512, ?5, ?6, NULL, NULL, NULL, NULL, NULL, 0, 0, ?7, ?7)
         ON CONFLICT(bot_id, file_id) DO UPDATE SET
            format = excluded.format,
            is_animated = excluded.is_animated,
            is_video = excluded.is_video,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            file.file_id,
            file.file_unique_id,
            format,
            if is_animated { 1 } else { 0 },
            if is_video { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let _ = user_id;

    let result = File {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        file_size: file.file_size,
        file_path: Some(file.file_path),
    };

    Ok(serde_json::to_value(result).map_err(ApiError::internal)?)
}

fn handle_create_new_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: CreateNewStickerSetRequest = parse_request(params)?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.stickers.is_empty() {
        return Err(ApiError::bad_request("stickers must include at least one item"));
    }

    let sticker_type = normalize_sticker_type(request.sticker_type.as_deref().unwrap_or("regular"))?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_some() {
        return Err(ApiError::bad_request("sticker set already exists"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sticker_sets
         (bot_id, name, title, sticker_type, needs_repainting, owner_user_id, thumbnail_file_id, thumbnail_format, custom_emoji_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, ?7, ?7)",
        params![
            bot.id,
            request.name,
            request.title,
            sticker_type,
            if request.needs_repainting.unwrap_or(false) { 1 } else { 0 },
            request.user_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    for (index, sticker_input) in request.stickers.iter().enumerate() {
        upsert_set_sticker(
            state,
            &mut conn,
            &bot,
            &request.name,
            sticker_type,
            request.needs_repainting.unwrap_or(false),
            sticker_input,
            index as i64,
        )?;
    }

    Ok(json!(true))
}

fn handle_add_sticker_to_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: AddStickerToSetRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT sticker_type, COALESCE(needs_repainting, 0) FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((sticker_type, needs_repainting)) = row else {
        return Err(ApiError::not_found("sticker set not found"));
    };

    let next_position: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM stickers WHERE bot_id = ?1 AND set_name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    upsert_set_sticker(
        state,
        &mut conn,
        &bot,
        &request.name,
        &sticker_type,
        needs_repainting == 1,
        &request.sticker,
        next_position,
    )?;

    Ok(json!(true))
}

fn handle_set_sticker_position_in_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerPositionInSetRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_name: Option<String> = conn
        .query_row(
            "SELECT set_name FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(set_name) = set_name else {
        return Err(ApiError::not_found("sticker not found in set"));
    };

    let mut stmt = conn
        .prepare(
            "SELECT file_id FROM stickers WHERE bot_id = ?1 AND set_name = ?2 ORDER BY position ASC, created_at ASC",
        )
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(params![bot.id, set_name], |r| r.get::<_, String>(0))
        .map_err(ApiError::internal)?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(ApiError::internal)?);
    }

    let current_index = ids.iter().position(|id| id == &request.sticker)
        .ok_or_else(|| ApiError::not_found("sticker not found in set"))?;
    let target = request.position.clamp(0, (ids.len().saturating_sub(1)) as i64) as usize;

    let moved = ids.remove(current_index);
    ids.insert(target, moved);

    let now = Utc::now().timestamp();
    for (idx, file_id) in ids.iter().enumerate() {
        conn.execute(
            "UPDATE stickers SET position = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![idx as i64, now, bot.id, file_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

fn handle_delete_sticker_from_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteStickerFromSetRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_name: Option<String> = conn
        .query_row(
            "SELECT set_name FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(set_name) = set_name else {
        return Err(ApiError::not_found("sticker not found"));
    };

    let deleted = conn
        .execute(
            "DELETE FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
        )
        .map_err(ApiError::internal)?;
    if deleted == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    compact_sticker_positions(&mut conn, bot.id, &set_name)?;
    Ok(json!(true))
}

fn handle_replace_sticker_in_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: ReplaceStickerInSetRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let old_row: Option<(i64, String, i64)> = conn
        .query_row(
            "SELECT position, set_name, COALESCE(needs_repainting, 0)
             FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.old_sticker],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((position, set_name, needs_repainting)) = old_row else {
        return Err(ApiError::not_found("old_sticker not found"));
    };

    let set_type: String = conn
        .query_row(
            "SELECT sticker_type FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
        params![bot.id, request.old_sticker],
    )
    .map_err(ApiError::internal)?;

    upsert_set_sticker(
        state,
        &mut conn,
        &bot,
        &set_name,
        &set_type,
        needs_repainting == 1,
        &request.sticker,
        position,
    )?;

    compact_sticker_positions(&mut conn, bot.id, &set_name)?;
    Ok(json!(true))
}

fn handle_set_sticker_emoji_list(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerEmojiListRequest = parse_request(params)?;
    if request.emoji_list.is_empty() {
        return Err(ApiError::bad_request("emoji_list must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE stickers
             SET emoji = ?1, emoji_list_json = ?2, updated_at = ?3
             WHERE bot_id = ?4 AND file_id = ?5",
            params![
                request.emoji_list[0].clone(),
                serde_json::to_string(&request.emoji_list).map_err(ApiError::internal)?,
                now,
                bot.id,
                request.sticker,
            ],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    Ok(json!(true))
}

fn handle_set_sticker_keywords(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerKeywordsRequest = parse_request(params)?;
    let keywords_json = request
        .keywords
        .as_ref()
        .map(|k| serde_json::to_string(k).map_err(ApiError::internal))
        .transpose()?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE stickers SET keywords_json = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![keywords_json, now, bot.id, request.sticker],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    Ok(json!(true))
}

fn handle_set_sticker_mask_position(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerMaskPositionRequest = parse_request(params)?;
    let mask_json = request
        .mask_position
        .as_ref()
        .map(|m| serde_json::to_string(m).map_err(ApiError::internal))
        .transpose()?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE stickers SET mask_position_json = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![mask_json, now, bot.id, request.sticker],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    Ok(json!(true))
}

fn handle_set_sticker_set_title(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerSetTitleRequest = parse_request(params)?;
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets SET title = ?1, updated_at = ?2 WHERE bot_id = ?3 AND name = ?4",
            params![request.title, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

fn handle_set_sticker_set_thumbnail(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerSetThumbnailRequest = parse_request(params)?;
    let format = normalize_sticker_format(&request.format)?;
    let thumbnail_file_id = if let Some(value) = request.thumbnail {
        let normalized = parse_input_file_value(&value, "thumbnail")?;
        Some(resolve_media_file(state, token, &normalized, "thumbnail")?.file_id)
    } else {
        None
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets
             SET thumbnail_file_id = ?1, thumbnail_format = ?2, updated_at = ?3
             WHERE bot_id = ?4 AND name = ?5",
            params![thumbnail_file_id, format, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

fn handle_set_custom_emoji_sticker_set_thumbnail(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetCustomEmojiStickerSetThumbnailRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets SET custom_emoji_id = ?1, updated_at = ?2 WHERE bot_id = ?3 AND name = ?4",
            params![request.custom_emoji_id, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

fn handle_delete_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteStickerSetRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let deleted = conn
        .execute(
            "DELETE FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if deleted == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    conn.execute(
        "DELETE FROM stickers WHERE bot_id = ?1 AND set_name = ?2",
        params![bot.id, request.name],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_send_invoice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendInvoiceRequest = parse_request(params)?;
    let normalized_currency = request.currency.trim().to_ascii_uppercase();
    let max_tip_amount = request.max_tip_amount.unwrap_or(0);
    let suggested_tip_amounts = request.suggested_tip_amounts.clone().unwrap_or_default();

    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.description.trim().is_empty() {
        return Err(ApiError::bad_request("description is empty"));
    }
    if request.payload.trim().is_empty() {
        return Err(ApiError::bad_request("payload is empty"));
    }
    if normalized_currency.is_empty() {
        return Err(ApiError::bad_request("currency is empty"));
    }
    if request.prices.is_empty() {
        return Err(ApiError::bad_request("prices must include at least one item"));
    }
    if max_tip_amount < 0 {
        return Err(ApiError::bad_request("max_tip_amount must be non-negative"));
    }

    if let Some(photo_size) = request.photo_size {
        if photo_size <= 0 {
            return Err(ApiError::bad_request("photo_size must be greater than zero"));
        }
    }
    if let Some(photo_width) = request.photo_width {
        if photo_width <= 0 {
            return Err(ApiError::bad_request("photo_width must be greater than zero"));
        }
    }
    if let Some(photo_height) = request.photo_height {
        if photo_height <= 0 {
            return Err(ApiError::bad_request("photo_height must be greater than zero"));
        }
    }

    if request.is_flexible.unwrap_or(false) && !request.need_shipping_address.unwrap_or(false) {
        return Err(ApiError::bad_request("is_flexible requires need_shipping_address=true"));
    }

    if !suggested_tip_amounts.is_empty() {
        if suggested_tip_amounts.len() > 4 {
            return Err(ApiError::bad_request("suggested_tip_amounts can have at most 4 values"));
        }
        if max_tip_amount <= 0 {
            return Err(ApiError::bad_request("max_tip_amount must be positive when suggested_tip_amounts is set"));
        }

        let mut previous = 0;
        for tip in &suggested_tip_amounts {
            if *tip <= 0 {
                return Err(ApiError::bad_request("suggested_tip_amounts values must be greater than zero"));
            }
            if *tip > max_tip_amount {
                return Err(ApiError::bad_request("suggested_tip_amounts values must be <= max_tip_amount"));
            }
            if *tip <= previous {
                return Err(ApiError::bad_request("suggested_tip_amounts must be strictly increasing"));
            }
            previous = *tip;
        }
    }

    let is_stars_invoice = normalized_currency == "XTR";
    let provider_token = request
        .provider_token
        .as_deref()
        .map(str::trim)
        .unwrap_or("");

    if is_stars_invoice {
        if !provider_token.is_empty() {
            return Err(ApiError::bad_request("provider_token must be empty for Telegram Stars invoices"));
        }
        if request.prices.len() != 1 {
            return Err(ApiError::bad_request("prices must contain exactly one item for Telegram Stars invoices"));
        }
        if max_tip_amount > 0 || !suggested_tip_amounts.is_empty() {
            return Err(ApiError::bad_request("tip fields are not supported for Telegram Stars invoices"));
        }
        if request.need_name.unwrap_or(false)
            || request.need_phone_number.unwrap_or(false)
            || request.need_email.unwrap_or(false)
            || request.need_shipping_address.unwrap_or(false)
            || request.send_phone_number_to_provider.unwrap_or(false)
            || request.send_email_to_provider.unwrap_or(false)
            || request.is_flexible.unwrap_or(false)
        {
            return Err(ApiError::bad_request("shipping/contact collection fields are not supported for Telegram Stars invoices"));
        }
    } else if provider_token.is_empty() {
        return Err(ApiError::bad_request("provider_token is required for non-Stars invoices"));
    }

    let mut total_amount: i64 = 0;
    for price in &request.prices {
        if price.label.trim().is_empty() {
            return Err(ApiError::bad_request("price label is empty"));
        }
        if price.amount <= 0 {
            return Err(ApiError::bad_request("price amount must be greater than zero"));
        }

        total_amount = total_amount
            .checked_add(price.amount)
            .ok_or_else(|| ApiError::bad_request("total amount overflow"))?;
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat) = resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &request.chat_id,
        ChatSendKind::Invoice,
    )?;
    let message_thread_id = resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Invoice,
    )?;

    let reply_markup_value = request
        .reply_markup
        .as_ref()
        .and_then(|markup| serde_json::to_value(markup).ok());

    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup_value.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, sender.id, request.description, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_value = load_message_value(&mut conn, &bot, message_id)?;
    message_value.as_object_mut().map(|obj| obj.remove("text"));
    if let Some(thread_id) = message_thread_id {
        message_value["message_thread_id"] = Value::from(thread_id);
        message_value["is_topic_message"] = Value::Bool(true);
    }
    let message_chat_id = message_value
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or_else(|| chat_id_as_i64(&request.chat_id, &chat_key));

    let invoice_title = request.title.clone();
    let invoice_description = request.description.clone();
    let invoice_payload = request.payload.clone();
    let invoice_currency = normalized_currency;

    let start_parameter = request.start_parameter.clone().unwrap_or_default();

    let invoice = Invoice {
        title: invoice_title.clone(),
        description: invoice_description.clone(),
        start_parameter,
        currency: invoice_currency.clone(),
        total_amount,
    };

    message_value["invoice"] = serde_json::to_value(invoice).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT OR REPLACE INTO invoices
         (bot_id, chat_key, message_id, title, description, payload, currency, total_amount,
          max_tip_amount, suggested_tip_amounts_json, start_parameter, provider_data,
          photo_url, photo_size, photo_width, photo_height,
          need_name, need_phone_number, need_email, need_shipping_address,
          send_phone_number_to_provider, send_email_to_provider,
          is_flexible, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                 ?9, ?10, ?11, ?12,
                 ?13, ?14, ?15, ?16,
                 ?17, ?18, ?19, ?20,
                 ?21, ?22,
                 ?23, ?24)",
        params![
            bot.id,
            chat_key,
            message_id,
            invoice_title,
            invoice_description,
            invoice_payload,
            invoice_currency,
            total_amount,
            max_tip_amount,
            if suggested_tip_amounts.is_empty() {
                None::<String>
            } else {
                Some(serde_json::to_string(&suggested_tip_amounts).map_err(ApiError::internal)?)
            },
            request.start_parameter,
            request.provider_data,
            request.photo_url,
            request.photo_size,
            request.photo_width,
            request.photo_height,
            if request.need_name.unwrap_or(false) { 1 } else { 0 },
            if request.need_phone_number.unwrap_or(false) { 1 } else { 0 },
            if request.need_email.unwrap_or(false) { 1 } else { 0 },
            if request.need_shipping_address.unwrap_or(false) { 1 } else { 0 },
            if request.send_phone_number_to_provider.unwrap_or(false) { 1 } else { 0 },
            if request.send_email_to_provider.unwrap_or(false) { 1 } else { 0 },
            if request.is_flexible.unwrap_or(false) { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = load_message_value(&mut conn, &bot, reply_parameters.message_id) {
            let belongs_to_chat = reply_value
                .get("chat")
                .and_then(|v| v.get("id"))
                .and_then(Value::as_i64)
                .map(|chat_id| chat_id.to_string() == reply_chat_key)
                .unwrap_or(false);

            if belongs_to_chat {
                message_value["reply_to_message"] = reply_value;
            } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
                return Err(ApiError::bad_request("replied message not found"));
            }
        } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
            return Err(ApiError::bad_request("replied message not found"));
        }
    }

    let is_channel_post = chat.r#type == "channel";
    let update_value = if is_channel_post {
        json!({
            "update_id": 0,
            "channel_post": message_value.clone(),
        })
    } else {
        json!({
            "update_id": 0,
            "message": message_value.clone(),
        })
    };

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    publish_sim_client_event(
        state,
        token,
        json!({
            "sim_event": "invoice_meta",
            "chat_id": message_chat_id,
            "message_id": message_id,
            "invoice_meta": {
                "photo_url": request.photo_url,
                "max_tip_amount": max_tip_amount,
                "suggested_tip_amounts": if suggested_tip_amounts.is_empty() { Value::Null } else { json!(suggested_tip_amounts) },
                "need_name": request.need_name.unwrap_or(false),
                "need_phone_number": request.need_phone_number.unwrap_or(false),
                "need_email": request.need_email.unwrap_or(false),
                "need_shipping_address": request.need_shipping_address.unwrap_or(false),
                "is_flexible": request.is_flexible.unwrap_or(false),
                "send_phone_number_to_provider": request.send_phone_number_to_provider.unwrap_or(false),
                "send_email_to_provider": request.send_email_to_provider.unwrap_or(false)
            }
        }),
    );
    Ok(message_value)
}

fn handle_send_checklist(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendChecklistRequest = parse_request_with_legacy_checklist(params)?;
    if request.business_connection_id.trim().is_empty() {
        return Err(ApiError::bad_request("business_connection_id is empty"));
    }

    let checklist = normalize_input_checklist(&request.checklist)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_id_value = Value::from(request.chat_id);
    let (chat_key, chat) = resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &chat_id_value,
        ChatSendKind::Other,
    )?;
    if chat.r#type != "private" {
        return Err(ApiError::bad_request(
            "sendChecklist is available only in private chats",
        ));
    }

    let business_connection_id = resolve_outbound_business_connection_for_bot_message(
        &mut conn,
        bot.id,
        &chat,
        Some(request.business_connection_id.as_str()),
    )?
    .ok_or_else(|| ApiError::bad_request("business_connection_id is empty"))?;

    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Other,
    )?;

    let reply_markup_value = request
        .reply_markup
        .as_ref()
        .and_then(|markup| serde_json::to_value(markup).ok());
    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup_value.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, &checklist.title, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_value = load_message_value(&mut conn, &bot, message_id)?;
    message_value["checklist"] = serde_json::to_value(&checklist).map_err(ApiError::internal)?;
    message_value["business_connection_id"] = Value::String(business_connection_id);

    if let Some(message_effect_id) = request
        .message_effect_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        message_value["message_effect_id"] = Value::String(message_effect_id.to_string());
    }

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = load_message_value(&mut conn, &bot, reply_parameters.message_id) {
            let belongs_to_chat = reply_value
                .get("chat")
                .and_then(|v| v.get("id"))
                .and_then(Value::as_i64)
                .map(|chat_id| chat_id.to_string() == reply_chat_key)
                .unwrap_or(false);

            if belongs_to_chat {
                message_value["reply_to_message"] = reply_value;
            } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
                return Err(ApiError::bad_request("replied message not found"));
            }
        } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
            return Err(ApiError::bad_request("replied message not found"));
        }
    }

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?),
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(message_value)
}

fn handle_send_poll(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendPollRequest = parse_request(params)?;
    let explicit_question_entities = request
        .question_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (question, question_entities) = parse_formatted_text(
        &request.question,
        request.question_parse_mode.as_deref(),
        explicit_question_entities,
    );

    if question.trim().is_empty() {
        return Err(ApiError::bad_request("question is empty"));
    }
    if question.chars().count() > 300 {
        return Err(ApiError::bad_request("question is too long"));
    }

    if request.options.len() < 2 || request.options.len() > 10 {
        return Err(ApiError::bad_request("options must include 2-10 items"));
    }

    if request.open_period.is_some() && request.close_date.is_some() {
        return Err(ApiError::bad_request("open_period and close_date are mutually exclusive"));
    }

    if let Some(open_period) = request.open_period {
        if !(5..=2_628_000).contains(&open_period) {
            return Err(ApiError::bad_request("open_period must be between 5 and 2628000"));
        }
    }

    let now = Utc::now().timestamp();
    if let Some(close_date) = request.close_date {
        let delta = close_date - now;
        if !(5..=2_628_000).contains(&delta) {
            return Err(ApiError::bad_request("close_date must be 5-2628000 seconds in the future"));
        }
    }

    let poll_type = request
        .type_param
        .clone()
        .unwrap_or_else(|| "regular".to_string());
    if poll_type != "regular" && poll_type != "quiz" {
        return Err(ApiError::bad_request("poll type must be regular or quiz"));
    }

    let allows_multiple_answers = request.allows_multiple_answers.unwrap_or(false);

    let correct_option_ids = request.correct_option_ids.clone();
    if poll_type == "quiz" {
        let Some(correct_ids) = correct_option_ids.as_ref() else {
            return Err(ApiError::bad_request("quiz poll requires correct_option_ids"));
        };
        if correct_ids.is_empty() {
            return Err(ApiError::bad_request("correct_option_ids must not be empty"));
        }
        if !allows_multiple_answers && correct_ids.len() != 1 {
            return Err(ApiError::bad_request(
                "quiz poll must have exactly one correct option when allows_multiple_answers is false",
            ));
        }
        for idx in correct_ids {
            if *idx < 0 || *idx >= request.options.len() as i64 {
                return Err(ApiError::bad_request("correct_option_ids contains out-of-range value"));
            }
        }
    } else if correct_option_ids.is_some() {
        return Err(ApiError::bad_request("correct_option_ids is allowed only for quiz polls"));
    }

    let allows_revoting = request
        .allows_revoting
        .unwrap_or_else(|| poll_type != "quiz");

    let explicit_explanation_entities = request
        .explanation_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (explanation, explanation_entities) = parse_optional_formatted_text(
        request.explanation.as_deref(),
        request.explanation_parse_mode.as_deref(),
        explicit_explanation_entities,
    );

    if poll_type == "quiz" {
        if let Some(exp) = explanation.as_ref() {
            if exp.chars().count() > 200 {
                return Err(ApiError::bad_request("explanation is too long"));
            }
        }
    }

    let explicit_description_entities = request
        .description_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (description, description_entities) = parse_optional_formatted_text(
        request.description.as_deref(),
        request.description_parse_mode.as_deref(),
        explicit_description_entities,
    );

    let mut poll_options: Vec<PollOption> = Vec::with_capacity(request.options.len());
    for item in &request.options {
        let explicit_option_entities = item
            .text_entities
            .as_ref()
            .and_then(|v| serde_json::to_value(v).ok());
        let (option_text, option_entities) = parse_formatted_text(
            &item.text,
            item.text_parse_mode.as_deref(),
            explicit_option_entities,
        );

        if option_text.trim().is_empty() {
            return Err(ApiError::bad_request("poll option text is empty"));
        }
        if option_text.chars().count() > 100 {
            return Err(ApiError::bad_request("poll option text is too long"));
        }

        let text_entities = option_entities
            .and_then(|value| serde_json::from_value(value).ok());

        poll_options.push(PollOption {
            persistent_id: generate_telegram_numeric_id(),
            text: option_text,
            text_entities,
            voter_count: 0,
            added_by_user: None,
            added_by_chat: None,
            addition_date: None,
        });
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, chat) = resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &request.chat_id,
        ChatSendKind::Poll,
    )?;
    let message_thread_id = resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Poll,
    )?;

    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        request.reply_markup.as_ref(),
    )?;

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, sender.id, question, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let poll_id = generate_telegram_numeric_id();
    let correct_option_id_for_storage = correct_option_ids
        .as_ref()
        .and_then(|ids| ids.first().copied());
    let poll = Poll {
        id: poll_id.clone(),
        question,
        question_entities: question_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
        options: poll_options.clone(),
        total_voter_count: 0,
        is_closed: request.is_closed.unwrap_or(false),
        is_anonymous: request.is_anonymous.unwrap_or(true),
        r#type: poll_type,
        allows_multiple_answers,
        allows_revoting,
        correct_option_ids,
        explanation,
        explanation_entities: explanation_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
        open_period: request.open_period,
        close_date: request.close_date,
        description,
        description_entities: description_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
    };

    conn.execute(
        "INSERT INTO polls (id, bot_id, chat_key, message_id, question, options_json, total_voter_count, is_closed, is_anonymous, poll_type, allows_multiple_answers, allows_revoting, correct_option_id, correct_option_ids_json, explanation, description, open_period, close_date, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
        params![
            poll.id,
            bot.id,
            chat_key,
            message_id,
            poll.question,
            serde_json::to_string(&poll.options).map_err(ApiError::internal)?,
            poll.total_voter_count,
            if poll.is_closed { 1 } else { 0 },
            if poll.is_anonymous { 1 } else { 0 },
            poll.r#type,
            if poll.allows_multiple_answers { 1 } else { 0 },
            if poll.allows_revoting { 1 } else { 0 },
            correct_option_id_for_storage,
            poll.correct_option_ids
                .as_ref()
                .map(|ids| serde_json::to_string(ids).map_err(ApiError::internal))
                .transpose()?,
            poll.explanation,
            poll.description,
            poll.open_period,
            poll.close_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT OR REPLACE INTO poll_metadata (poll_id, question_entities_json, explanation_entities_json, description_entities_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            poll.id,
            question_entities
                .as_ref()
                .and_then(Value::as_array)
                .map(|_| question_entities.as_ref().unwrap().to_string()),
            explanation_entities
                .as_ref()
                .and_then(Value::as_array)
                .map(|_| explanation_entities.as_ref().unwrap().to_string()),
            description_entities
                .as_ref()
                .and_then(Value::as_array)
                .map(|_| description_entities.as_ref().unwrap().to_string()),
        ],
    )
    .map_err(ApiError::internal)?;

    let mut message_value = load_message_value(&mut conn, &bot, message_id)?;
    message_value["poll"] = serde_json::to_value(&poll).map_err(ApiError::internal)?;
    message_value.as_object_mut().map(|obj| obj.remove("text"));
    message_value.as_object_mut().map(|obj| obj.remove("edit_date"));
    if let Some(thread_id) = message_thread_id {
        message_value["message_thread_id"] = Value::from(thread_id);
        message_value["is_topic_message"] = Value::Bool(true);
    }

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = load_message_value(&mut conn, &bot, reply_parameters.message_id) {
            let belongs_to_chat = reply_value
                .get("chat")
                .and_then(|v| v.get("id"))
                .and_then(Value::as_i64)
                .map(|chat_id| chat_id.to_string() == reply_chat_key)
                .unwrap_or(false);

            if belongs_to_chat {
                message_value["reply_to_message"] = reply_value;
            } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
                return Err(ApiError::bad_request("replied message not found"));
            }
        } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
            return Err(ApiError::bad_request("replied message not found"));
        }
    }

    let is_channel_post = chat.r#type == "channel";
    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: if is_channel_post {
            None
        } else {
            Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?)
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(message_value)
}

fn handle_send_paid_media(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendPaidMediaRequest =
        parse_request_ignoring_prefixed_fields(params, &["paid_media_"])?;

    if request.star_count <= 0 {
        return Err(ApiError::bad_request(
            "star_count must be greater than zero",
        ));
    }

    if request.media.is_empty() || request.media.len() > 10 {
        return Err(ApiError::bad_request("media must include 1-10 items"));
    }

    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .map(|entities| serde_json::to_value(entities).map_err(ApiError::internal))
        .transpose()?;
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());

    let mut paid_media_items = Vec::<Value>::with_capacity(request.media.len());
    for raw_item in &request.media {
        let item = raw_item
            .extra
            .as_object()
            .ok_or_else(|| ApiError::bad_request("paid media item must be an object"))?;

        let media_type = item
            .get("type")
            .and_then(Value::as_str)
            .map(|value| value.to_ascii_lowercase())
            .ok_or_else(|| ApiError::bad_request("paid media item type is required"))?;
        let media_ref = item
            .get("media")
            .ok_or_else(|| ApiError::bad_request("paid media item media is required"))?;

        let mapped = match media_type.as_str() {
            "photo" => {
                let file = resolve_media_file(state, token, media_ref, "photo")?;
                json!({
                    "type": "photo",
                    "photo": [{
                        "file_id": file.file_id,
                        "file_unique_id": file.file_unique_id,
                        "width": 1280,
                        "height": 720,
                        "file_size": file.file_size,
                    }],
                })
            }
            "video" => {
                let file = resolve_media_file(state, token, media_ref, "video")?;
                json!({
                    "type": "video",
                    "video": {
                        "file_id": file.file_id,
                        "file_unique_id": file.file_unique_id,
                        "width": item.get("width").and_then(Value::as_i64).unwrap_or(1280),
                        "height": item.get("height").and_then(Value::as_i64).unwrap_or(720),
                        "duration": item.get("duration").and_then(Value::as_i64).unwrap_or(0),
                        "mime_type": file.mime_type,
                        "file_size": file.file_size,
                    }
                })
            }
            _ => {
                return Err(ApiError::bad_request(
                    "sendPaidMedia supports only photo and video",
                ));
            }
        };

        paid_media_items.push(mapped);
    }

    let payload = json!({
        "star_count": request.star_count,
        "paid_media": paid_media_items,
    });

    send_paid_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        payload,
        request.star_count,
        request.show_caption_above_media,
        request.message_thread_id,
        sim_parse_mode,
    )
}

fn handle_send_media_group(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendMediaGroupRequest = parse_request(params)?;

    if request.media.len() < 2 || request.media.len() > 10 {
        return Err(ApiError::bad_request("media must include 2-10 items"));
    }

    let media_types: Vec<String> = request
        .media
        .iter()
        .map(|item| {
            item.get("type")
                .and_then(Value::as_str)
                .map(|t| t.to_ascii_lowercase())
                .unwrap_or_default()
        })
        .collect();

    if media_types.iter().any(|t| t.is_empty()) {
        return Err(ApiError::bad_request("every media item must include type"));
    }

    if media_types
        .iter()
        .any(|t| t != "photo" && t != "video" && t != "audio" && t != "document")
    {
        return Err(ApiError::bad_request(
            "sendMediaGroup supports only photo, video, audio, and document",
        ));
    }

    let has_audio = media_types.iter().any(|t| t == "audio");
    let has_document = media_types.iter().any(|t| t == "document");

    if has_audio && media_types.iter().any(|t| t != "audio") {
        return Err(ApiError::bad_request(
            "audio media groups can contain only audio items",
        ));
    }

    if has_document && media_types.iter().any(|t| t != "document") {
        return Err(ApiError::bad_request(
            "document media groups can contain only document items",
        ));
    }

    let media_group_id = generate_telegram_numeric_id();
    let mut result = Vec::with_capacity(request.media.len());

    for raw_item in &request.media {
        let item = raw_item
            .as_object()
            .ok_or_else(|| ApiError::bad_request("media item must be an object"))?;

        let media_type = item
            .get("type")
            .and_then(Value::as_str)
            .map(|v| v.to_ascii_lowercase())
            .ok_or_else(|| ApiError::bad_request("media item type is required"))?;

        let media_ref = item
            .get("media")
            .ok_or_else(|| ApiError::bad_request("media item media is required"))?;

        let explicit_caption_entities = item.get("caption_entities").cloned();
        let parse_mode = item.get("parse_mode").and_then(Value::as_str);
        let sim_parse_mode = normalize_sim_parse_mode(parse_mode);
        let (caption, caption_entities) = parse_optional_formatted_text(
            item.get("caption").and_then(Value::as_str),
            parse_mode,
            explicit_caption_entities,
        );

        let value = match media_type.as_str() {
            "photo" => {
                let file = resolve_media_file(state, token, media_ref, "photo")?;
                let payload = json!([
                    {
                        "file_id": file.file_id,
                        "file_unique_id": file.file_unique_id,
                        "width": 1280,
                        "height": 720,
                        "file_size": file.file_size,
                    }
                ]);
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    None,
                    "photo",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
                )?
            }
            "video" => {
                let file = resolve_media_file(state, token, media_ref, "video")?;
                let payload = json!({
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "width": item.get("width").and_then(Value::as_i64).unwrap_or(1280),
                    "height": item.get("height").and_then(Value::as_i64).unwrap_or(720),
                    "duration": item.get("duration").and_then(Value::as_i64).unwrap_or(0),
                    "mime_type": file.mime_type,
                    "file_size": file.file_size,
                });
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    None,
                    "video",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
                )?
            }
            "audio" => {
                let file = resolve_media_file(state, token, media_ref, "audio")?;
                let payload = json!({
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "duration": item.get("duration").and_then(Value::as_i64).unwrap_or(0),
                    "performer": item.get("performer").and_then(Value::as_str),
                    "title": item.get("title").and_then(Value::as_str),
                    "mime_type": file.mime_type,
                    "file_size": file.file_size,
                });
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    None,
                    "audio",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
                )?
            }
            "document" => {
                let file = resolve_media_file(state, token, media_ref, "document")?;
                let payload = json!({
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
                    "mime_type": file.mime_type,
                    "file_size": file.file_size,
                });
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    None,
                    "document",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
                )?
            }
            _ => {
                return Err(ApiError::bad_request(
                    "sendMediaGroup supports only photo, video, audio, and document",
                ));
            }
        };

        result.push(value);
    }

    Ok(Value::Array(result))
}

fn resolve_game_target_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    request_chat_id: Option<i64>,
    request_message_id: Option<i64>,
    inline_message_id: Option<&str>,
) -> Result<(String, i64), ApiError> {
    if let Some(inline_id) = inline_message_id {
        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT chat_key, message_id FROM inline_messages WHERE inline_message_id = ?1 AND bot_id = ?2",
                params![inline_id, bot_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;
        return row.ok_or_else(|| ApiError::not_found("inline message not found"));
    }

    let chat_id = request_chat_id.ok_or_else(|| ApiError::bad_request("chat_id is required"))?;
    let message_id = request_message_id.ok_or_else(|| ApiError::bad_request("message_id is required"))?;
    Ok((chat_id.to_string(), message_id))
}

fn handle_set_game_score(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetGameScoreRequest = parse_request(params)?;
    if request.score < 0 {
        return Err(ApiError::bad_request("score must be non-negative"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, message_id) = resolve_game_target_message(
        &mut conn,
        bot.id,
        request.chat_id,
        request.message_id,
        request.inline_message_id.as_deref(),
    )?;

    let mut target_message = load_message_value(&mut conn, &bot, message_id)?;
    if target_message.get("game").is_none() {
        return Err(ApiError::bad_request("target message is not a game message"));
    }

    ensure_user(&mut conn, Some(request.user_id), None, None)?;

    let existing_score: Option<i64> = conn
        .query_row(
            "SELECT score FROM game_scores WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND user_id = ?4",
            params![bot.id, chat_key, message_id, request.user_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let can_override = request.force.unwrap_or(false);
    if let Some(current) = existing_score {
        if request.score < current && !can_override {
            return Err(ApiError::bad_request("new score must be greater than or equal to current score unless force is true"));
        }
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO game_scores (bot_id, chat_key, message_id, user_id, score, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(bot_id, chat_key, message_id, user_id)
         DO UPDATE SET score = excluded.score, updated_at = excluded.updated_at",
        params![bot.id, chat_key, message_id, request.user_id, request.score, now],
    )
    .map_err(ApiError::internal)?;

    if request.inline_message_id.is_some() {
      return Ok(json!(true));
    }

    if request.disable_edit_message.unwrap_or(false) {
        return Ok(json!(true));
    }

    target_message["edit_date"] = json!(now);
    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: serde_json::from_value(target_message.clone()).ok(),
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(target_message)
}

fn handle_get_game_high_scores(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetGameHighScoresRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, message_id) = resolve_game_target_message(
        &mut conn,
        bot.id,
        request.chat_id,
        request.message_id,
        request.inline_message_id.as_deref(),
    )?;

    let target_message = load_message_value(&mut conn, &bot, message_id)?;
    if target_message.get("game").is_none() {
        return Err(ApiError::bad_request("target message is not a game message"));
    }

    let mut stmt = conn
        .prepare(
            "SELECT gs.user_id, gs.score, u.first_name, u.username
             FROM game_scores gs
             LEFT JOIN users u ON u.id = gs.user_id
             WHERE gs.bot_id = ?1 AND gs.chat_key = ?2 AND gs.message_id = ?3
             ORDER BY gs.score DESC, gs.updated_at ASC, gs.user_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, chat_key, message_id], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut scores: Vec<GameHighScore> = Vec::new();
    for (idx, row) in rows.enumerate() {
        let (user_id, score, first_name, username) = row.map_err(ApiError::internal)?;
        scores.push(GameHighScore {
            position: idx as i64 + 1,
            user: User {
                id: user_id,
                is_bot: false,
                first_name: first_name.unwrap_or_else(|| format!("User {}", user_id)),
                last_name: None,
                username,
                language_code: None,
                is_premium: None,
                added_to_attachment_menu: None,
                can_join_groups: None,
                can_read_all_group_messages: None,
                supports_inline_queries: None,
                can_connect_to_business: None,
                has_main_web_app: None,
                has_topics_enabled: None,
                allows_users_to_create_topics: None,
        can_manage_bots: None,
            },
            score,
        });
    }

    Ok(serde_json::to_value(scores).map_err(ApiError::internal)?)
}

fn handle_get_file(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetFileRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, String, Option<i64>, String)> = conn
        .query_row(
            "SELECT file_id, file_unique_id, file_size, file_path FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.file_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((file_id, file_unique_id, file_size, file_path)) = row else {
        return Err(ApiError::not_found("file not found"));
    };

    Ok(json!({
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "file_path": file_path
    }))
}

fn handle_get_user_profile_photos(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserProfilePhotosRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    let (offset, limit) = normalize_profile_pagination(request.offset, request.limit)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_profile_photos_storage(&mut conn)?;

    let existing_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sim_user_profile_photos WHERE bot_id = ?1 AND user_id = ?2",
            params![bot.id, request.user_id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    if existing_count == 0 {
        let user_photo_url: Option<String> = conn
            .query_row(
                "SELECT photo_url FROM users WHERE id = ?1",
                params![request.user_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten();

        let business_profile_photo_id: Option<String> = conn
            .query_row(
                "SELECT COALESCE(public_profile_photo_file_id, profile_photo_file_id)
                 FROM sim_business_account_profiles
                 WHERE bot_id = ?1 AND user_id = ?2",
                params![bot.id, request.user_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten();

        if user_photo_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
            || business_profile_photo_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some()
        {
            let now = Utc::now().timestamp();
            let file_id = business_profile_photo_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| generate_telegram_file_id("profile_photo"));

            let file_unique_id = generate_telegram_file_unique_id();
            conn.execute(
                "INSERT OR IGNORE INTO sim_user_profile_photos
                 (bot_id, user_id, file_id, file_unique_id, width, height, file_size, position, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 0, ?7)",
                params![bot.id, request.user_id, file_id, file_unique_id, 640, 640, now],
            )
            .map_err(ApiError::internal)?;
        }
    }

    let mut stmt = conn
        .prepare(
            "SELECT file_id, file_unique_id, width, height, file_size
             FROM sim_user_profile_photos
             WHERE bot_id = ?1 AND user_id = ?2
             ORDER BY position ASC, created_at DESC, file_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, request.user_id], |row| {
            Ok(PhotoSize {
                file_id: row.get(0)?,
                file_unique_id: row.get(1)?,
                width: row.get(2)?,
                height: row.get(3)?,
                file_size: row.get(4)?,
            })
        })
        .map_err(ApiError::internal)?;

    let all_photos = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    let total_count = all_photos.len() as i64;

    let photos = all_photos
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|photo| vec![photo])
        .collect::<Vec<Vec<PhotoSize>>>();

    serde_json::to_value(UserProfilePhotos { total_count, photos }).map_err(ApiError::internal)
}

fn handle_get_user_profile_audios(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserProfileAudiosRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    let (offset, limit) = normalize_profile_pagination(request.offset, request.limit)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let mut stmt = conn
        .prepare(
            "SELECT file_id, file_unique_id, duration, performer, title, file_name, mime_type, file_size
             FROM sim_user_profile_audios
             WHERE bot_id = ?1 AND user_id = ?2
             ORDER BY position ASC, created_at DESC, file_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, request.user_id], |row| {
            Ok(Audio {
                file_id: row.get(0)?,
                file_unique_id: row.get(1)?,
                duration: row.get(2)?,
                performer: row.get(3)?,
                title: row.get(4)?,
                file_name: row.get(5)?,
                mime_type: row.get(6)?,
                file_size: row.get(7)?,
                thumbnail: None,
            })
        })
        .map_err(ApiError::internal)?;

    let all_audios = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    let total_count = all_audios.len() as i64;

    let audios = all_audios
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<Audio>>();

    serde_json::to_value(UserProfileAudios { total_count, audios }).map_err(ApiError::internal)
}

fn handle_set_user_emoji_status(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetUserEmojiStatusRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let custom_emoji_id = request
        .emoji_status_custom_emoji_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if request.emoji_status_expiration_date.is_some() && custom_emoji_id.is_none() {
        return Err(ApiError::bad_request(
            "emoji_status_expiration_date requires emoji_status_custom_emoji_id",
        ));
    }

    let now = Utc::now().timestamp();
    if let Some(expiration_date) = request.emoji_status_expiration_date {
        if expiration_date <= now {
            return Err(ApiError::bad_request(
                "emoji_status_expiration_date must be in the future",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_emoji_statuses_storage(&mut conn)?;

    conn.execute(
        "INSERT INTO sim_user_emoji_statuses
         (bot_id, user_id, emoji_status_custom_emoji_id, emoji_status_expiration_date, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET
            emoji_status_custom_emoji_id = excluded.emoji_status_custom_emoji_id,
            emoji_status_expiration_date = excluded.emoji_status_expiration_date,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            custom_emoji_id,
            request.emoji_status_expiration_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_set_passport_data_errors(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetPassportDataErrorsRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.errors.is_empty() {
        return Err(ApiError::bad_request("errors must include at least one item"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_passport_data_errors_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let mut normalized_errors = Vec::<Value>::with_capacity(request.errors.len());
    for error in request.errors {
        let error_value = error.extra;
        let source = error_value
            .get("source")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let element_type = error_value
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let message = error_value
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");

        if source.is_empty() {
            return Err(ApiError::bad_request("passport error source is required"));
        }
        if element_type.is_empty() {
            return Err(ApiError::bad_request("passport error type is required"));
        }
        if message.is_empty() {
            return Err(ApiError::bad_request("passport error message is required"));
        }

        normalized_errors.push(error_value);
    }

    conn.execute(
        "INSERT INTO sim_passport_data_errors (bot_id, user_id, errors_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET errors_json = excluded.errors_json, updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            serde_json::to_string(&normalized_errors).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_get_user_chat_boosts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserChatBoostsRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_chat_boosts_storage(&mut conn)?;

    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, request.user_id)?;

    ensure_sim_user_record(&mut conn, request.user_id)?;

    let mut stmt = conn
        .prepare(
            "SELECT boost_id, add_date, expiration_date, source_json
             FROM sim_user_chat_boosts
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3
             ORDER BY expiration_date DESC, add_date DESC, boost_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, &chat_key, request.user_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut boosts = Vec::<ChatBoost>::new();
    for row in rows {
        let (boost_id, add_date, expiration_date, source_json) = row.map_err(ApiError::internal)?;
        let source = serde_json::from_str::<ChatBoostSource>(&source_json)
            .map_err(ApiError::internal)?;
        boosts.push(ChatBoost {
            boost_id,
            add_date,
            expiration_date,
            source,
        });
    }

    serde_json::to_value(UserChatBoosts { boosts }).map_err(ApiError::internal)
}

fn handle_stop_poll(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: StopPollRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;

    let row: Option<(
        String,
        String,
        String,
        i64,
        i64,
        i64,
        String,
        i64,
        i64,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = conn
        .query_row(
            "SELECT p.id, p.question, p.options_json, p.total_voter_count, p.is_closed, p.is_anonymous, p.poll_type,
                    p.allows_multiple_answers, p.allows_revoting, p.correct_option_id, p.correct_option_ids_json,
                    p.explanation, p.description, p.open_period, p.close_date,
                    m.question_entities_json, m.explanation_entities_json, m.description_entities_json
             FROM polls p
             LEFT JOIN poll_metadata m ON m.poll_id = p.id
             WHERE p.bot_id = ?1 AND p.chat_key = ?2 AND p.message_id = ?3",
            params![bot.id, chat_key, request.message_id],
            |r| Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
                r.get(15)?,
                r.get(16)?,
                r.get(17)?,
            )),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, question, options_json, total_voter_count, _is_closed, is_anonymous, poll_type, allows_multiple_answers, allows_revoting, correct_option_id, correct_option_ids_json, explanation, description, open_period, close_date, question_entities_json, explanation_entities_json, description_entities_json)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    conn.execute(
        "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2",
        params![poll_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    let options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
    let question_entities = question_entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());
    let explanation_entities = explanation_entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());
    let description_entities = description_entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());
    let correct_option_ids = correct_option_ids_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
        .or_else(|| correct_option_id.map(|id| vec![id]));
    let poll = Poll {
        id: poll_id,
        question,
        question_entities,
        options,
        total_voter_count,
        is_closed: true,
        is_anonymous: is_anonymous == 1,
        r#type: poll_type,
        allows_multiple_answers: allows_multiple_answers == 1,
        allows_revoting: allows_revoting == 1,
        correct_option_ids,
        explanation,
        explanation_entities,
        open_period,
        close_date,
        description,
        description_entities,
    };

    let mut edited_message = load_message_value(&mut conn, &bot, request.message_id)?;
    edited_message["poll"] = serde_json::to_value(&poll).map_err(ApiError::internal)?;
    edited_message.as_object_mut().map(|obj| obj.remove("text"));
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;
    Ok(edited_message)
}

pub fn handle_auto_close_due_polls(state: &Data<AppState>) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    let mut conn = lock_db(state)?;

    let due_rows: Vec<(
        i64,
        String,
        String,
        String,
        String,
        i64,
        i64,
        String,
        i64,
        i64,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = {
        let mut stmt = conn
            .prepare(
                "SELECT p.bot_id, b.token, p.id, p.question, p.options_json, p.total_voter_count, p.is_anonymous, p.poll_type,
                        p.allows_multiple_answers, p.allows_revoting, p.correct_option_id, p.correct_option_ids_json,
                        p.explanation, p.description, p.open_period, p.close_date,
                        m.question_entities_json, m.explanation_entities_json, m.description_entities_json
                 FROM polls p
                 INNER JOIN bots b ON b.id = p.bot_id
                 LEFT JOIN poll_metadata m ON m.poll_id = p.id
                 WHERE p.is_closed = 0
                 AND (
                    (p.close_date IS NOT NULL AND p.close_date <= ?1)
                    OR
                    (p.open_period IS NOT NULL AND p.created_at + p.open_period <= ?1)
                 )",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![now], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, i64>(9)?,
                    r.get::<_, Option<i64>>(10)?,
                    r.get::<_, Option<String>>(11)?,
                    r.get::<_, Option<String>>(12)?,
                    r.get::<_, Option<String>>(13)?,
                    r.get::<_, Option<i64>>(14)?,
                    r.get::<_, Option<i64>>(15)?,
                    r.get::<_, Option<String>>(16)?,
                    r.get::<_, Option<String>>(17)?,
                    r.get::<_, Option<String>>(18)?,
                ))
            })
            .map_err(ApiError::internal)?;

        let mut collected = Vec::new();
        for row in rows {
            collected.push(row.map_err(ApiError::internal)?);
        }
        collected
    };

    for (
        bot_id,
        token,
        poll_id,
        question,
        options_json,
        total_voter_count,
        is_anonymous,
        poll_type,
        allows_multiple_answers,
        allows_revoting,
        correct_option_id,
        correct_option_ids_json,
        explanation,
        description,
        open_period,
        close_date,
        question_entities_json,
        explanation_entities_json,
        description_entities_json,
    ) in due_rows
    {
        conn.execute(
            "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2 AND is_closed = 0",
            params![poll_id, bot_id],
        )
        .map_err(ApiError::internal)?;

        let options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
        let correct_option_ids = correct_option_ids_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
            .or_else(|| correct_option_id.map(|id| vec![id]));
        let poll = Poll {
            id: poll_id,
            question,
            question_entities: question_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            options,
            total_voter_count,
            is_closed: true,
            is_anonymous: is_anonymous == 1,
            r#type: poll_type,
            allows_multiple_answers: allows_multiple_answers == 1,
            allows_revoting: allows_revoting == 1,
            correct_option_ids,
            explanation,
            explanation_entities: explanation_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            open_period,
            close_date,
            description,
            description_entities: description_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
        };

        let update_value = serde_json::to_value(Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: Some(poll),
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
        managed_bot: None,
        })
        .map_err(ApiError::internal)?;

        persist_and_dispatch_update(state, &mut conn, &token, bot_id, update_value)?;
    }

    Ok(())
}

pub fn handle_auto_publish_due_suggested_posts(state: &Data<AppState>) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    let mut conn = lock_db(state)?;
    ensure_sim_suggested_posts_storage(&mut conn)?;

    let due_rows: Vec<(String, i64, String, i64)> = {
        let mut stmt = conn
            .prepare(
                "SELECT b.token, sp.bot_id, sp.chat_key, sp.message_id
                 FROM sim_suggested_posts sp
                 INNER JOIN bots b ON b.id = sp.bot_id
                 WHERE sp.state = 'approved'
                   AND COALESCE(sp.send_date, 0) <= ?1
                 ORDER BY sp.updated_at ASC
                 LIMIT 256",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(ApiError::internal)?;

        let mut collected = Vec::new();
        for row in rows {
            collected.push(row.map_err(ApiError::internal)?);
        }
        collected
    };

    for (token, bot_id, chat_key, message_id) in due_rows {
        let bot = ensure_bot(&mut conn, &token)?;
        if bot.id != bot_id {
            continue;
        }

        let Some(direct_messages_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
            continue;
        };
        if !is_direct_messages_chat(&direct_messages_chat) {
            continue;
        }

        let actor_user_id = direct_messages_chat
            .parent_channel_chat_id
            .and_then(|channel_chat_id| {
                load_channel_owner_user_id(&mut conn, bot.id, &channel_chat_id.to_string())
                    .ok()
                    .flatten()
            })
            .unwrap_or(bot.id);

        if let Err(error) = finalize_due_suggested_post_if_ready(
            state,
            &mut conn,
            &token,
            &bot,
            &direct_messages_chat,
            message_id,
            actor_user_id,
        ) {
            let failure_reason = match error.description.as_str() {
                "source message was not found" => Some("source_message_missing"),
                "suggested post message was not found" => Some("suggested_post_message_missing"),
                "message is not a suggested post" => Some("invalid_suggested_post_source"),
                _ => None,
            };

            if let Some(reason) = failure_reason {
                let existing_send_date = load_suggested_post_state(
                    &mut conn,
                    bot.id,
                    &direct_messages_chat.chat_key,
                    message_id,
                )?
                .and_then(|(_, send_date)| send_date)
                .or(Some(now));

                upsert_suggested_post_state(
                    &mut conn,
                    bot.id,
                    &direct_messages_chat.chat_key,
                    message_id,
                    "approval_failed",
                    existing_send_date,
                    Some(reason),
                    now,
                )?;
            }

            eprintln!(
                "auto-publish suggested post failed for bot {} message {}: {}",
                bot.id, message_id, error.description
            );
        }
    }

    Ok(())
}

pub fn handle_sim_vote_poll(
    state: &Data<AppState>,
    token: &str,
    body: SimVotePollRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let row: Option<(
        String,
        String,
        String,
        i64,
        i64,
        i64,
        String,
        i64,
        i64,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = conn
        .query_row(
            "SELECT p.id, p.question, p.options_json, p.total_voter_count, p.is_closed, p.is_anonymous, p.poll_type,
                    p.allows_multiple_answers, p.allows_revoting, p.correct_option_id, p.correct_option_ids_json,
                    p.explanation, p.description, p.open_period, p.close_date, p.created_at,
                    m.question_entities_json, m.explanation_entities_json, m.description_entities_json
             FROM polls p
             LEFT JOIN poll_metadata m ON m.poll_id = p.id
             WHERE p.bot_id = ?1 AND p.chat_key = ?2 AND p.message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
                r.get(15)?,
                r.get(16)?,
                r.get(17)?,
                r.get(18)?,
            )),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, question, options_json, _total_voter_count, is_closed, is_anonymous, poll_type, allows_multiple_answers, allows_revoting, correct_option_id, correct_option_ids_json, explanation, description, open_period, close_date, created_at, question_entities_json, explanation_entities_json, description_entities_json)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    let now = Utc::now().timestamp();
    let auto_closed = close_date.map(|ts| now >= ts).unwrap_or(false)
        || open_period.map(|p| now >= created_at + p).unwrap_or(false);

    if is_closed == 1 || auto_closed {
        if auto_closed && is_closed == 0 {
            conn.execute(
                "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2",
                params![poll_id, bot.id],
            )
            .map_err(ApiError::internal)?;
        }
        return Err(ApiError::bad_request("poll is closed"));
    }

    let mut option_ids = body.option_ids.clone();
    option_ids.sort_unstable();
    option_ids.dedup();

    if poll_type == "quiz" {
        if option_ids.is_empty() {
            return Err(ApiError::bad_request("quiz polls do not allow vote retraction"));
        }
        if allows_multiple_answers == 0 && option_ids.len() != 1 {
            return Err(ApiError::bad_request("quiz polls accept exactly one option"));
        }
    }

    let existing_vote: Option<Vec<i64>> = conn
        .query_row(
            "SELECT option_ids_json FROM poll_votes WHERE poll_id = ?1 AND voter_user_id = ?2",
            params![poll_id, user.id],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .map(|raw| serde_json::from_str::<Vec<i64>>(&raw).unwrap_or_default());

    if allows_revoting == 0 {
        if let Some(previous) = existing_vote.as_ref() {
            if previous != &option_ids {
                return Err(ApiError::bad_request("poll vote cannot be changed"));
            }
        }
    }

    let mut options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
    let max_index = options.len() as i64;
    if option_ids.iter().any(|v| *v < 0 || *v >= max_index) {
        return Err(ApiError::bad_request("option_ids contains invalid index"));
    }

    if allows_multiple_answers == 0 && option_ids.len() > 1 {
        return Err(ApiError::bad_request("poll accepts only one option"));
    }

    if option_ids.is_empty() {
        conn.execute(
            "DELETE FROM poll_votes WHERE poll_id = ?1 AND voter_user_id = ?2",
            params![poll_id, user.id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT OR REPLACE INTO poll_votes (poll_id, voter_user_id, option_ids_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                poll_id,
                user.id,
                serde_json::to_string(&option_ids).map_err(ApiError::internal)?,
                Utc::now().timestamp(),
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let (total_voter_count, counts) = {
        let mut total_voter_count: i64 = 0;
        let mut counts = vec![0i64; options.len()];
        let mut stmt = conn
            .prepare("SELECT option_ids_json FROM poll_votes WHERE poll_id = ?1")
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![poll_id], |r| r.get::<_, String>(0))
            .map_err(ApiError::internal)?;

        for row in rows {
            let raw = row.map_err(ApiError::internal)?;
            let ids: Vec<i64> = serde_json::from_str(&raw).unwrap_or_default();
            total_voter_count += 1;
            for id in ids {
                if let Some(slot) = counts.get_mut(id as usize) {
                    *slot += 1;
                }
            }
        }

        (total_voter_count, counts)
    };

    for (idx, option) in options.iter_mut().enumerate() {
        option.voter_count = counts[idx];
    }

    conn.execute(
        "UPDATE polls SET options_json = ?1, total_voter_count = ?2 WHERE id = ?3",
        params![serde_json::to_string(&options).map_err(ApiError::internal)?, total_voter_count, poll_id],
    )
    .map_err(ApiError::internal)?;

    let correct_option_ids = correct_option_ids_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
        .or_else(|| correct_option_id.map(|id| vec![id]));
    let poll = Poll {
        id: poll_id.clone(),
        question,
        question_entities: question_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        options,
        total_voter_count,
        is_closed: false,
        is_anonymous: is_anonymous == 1,
        r#type: poll_type,
        allows_multiple_answers: allows_multiple_answers == 1,
        allows_revoting: allows_revoting == 1,
        correct_option_ids,
        explanation,
        explanation_entities: explanation_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        open_period,
        close_date,
        description,
        description_entities: description_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
    };

    let poll_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: Some(poll.clone()),
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, poll_update)?;

    if is_anonymous == 1 {
        return Ok(serde_json::to_value(true).map_err(ApiError::internal)?);
    }

    let option_persistent_ids = option_ids
        .iter()
        .filter_map(|id| poll.options.get(*id as usize).map(|option| option.persistent_id.clone()))
        .collect::<Vec<String>>();

    let poll_answer_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: Some(PollAnswer {
            poll_id,
            voter_chat: None,
            user: Some(User {
                id: user.id,
                is_bot: false,
                first_name: user.first_name,
                last_name: None,
                username: user.username,
                language_code: None,
                is_premium: None,
                added_to_attachment_menu: None,
                can_join_groups: None,
                can_read_all_group_messages: None,
                supports_inline_queries: None,
                can_connect_to_business: None,
                has_main_web_app: None,
                has_topics_enabled: None,
                allows_users_to_create_topics: None,
        can_manage_bots: None,
            }),
            option_ids,
            option_persistent_ids,
        }),
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, poll_answer_update)?;

    Ok(serde_json::to_value(true).map_err(ApiError::internal)?)
}

pub fn handle_sim_pay_invoice(
    state: &Data<AppState>,
    token: &str,
    body: SimPayInvoiceRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let invoice_row: Option<(String, String, String, i64, i64, i64, i64, i64, i64, i64)> = conn
        .query_row(
            "SELECT title, payload, currency, total_amount, need_shipping_address, is_flexible, max_tip_amount,
                    need_name, need_phone_number, need_email
             FROM invoices
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((invoice_title, invoice_payload, currency_raw, invoice_total_amount, need_shipping_address, is_flexible, max_tip_amount, need_name, need_phone_number, need_email)) = invoice_row else {
        return Err(ApiError::not_found("invoice not found"));
    };
    let currency = currency_raw.trim().to_ascii_uppercase();
    let is_stars_invoice = currency == "XTR";

    let payment_method = body
        .payment_method
        .unwrap_or_else(|| "wallet".to_string())
        .trim()
        .to_ascii_lowercase();
    let outcome = body
        .outcome
        .unwrap_or_else(|| "success".to_string())
        .trim()
        .to_ascii_lowercase();

    if outcome != "success" && outcome != "failed" {
        return Err(ApiError::bad_request("outcome must be success or failed"));
    }

    if payment_method != "wallet" && payment_method != "card" && payment_method != "stars" {
        return Err(ApiError::bad_request("payment_method must be wallet, card, or stars"));
    }

    if is_stars_invoice && payment_method != "stars" {
        return Err(ApiError::bad_request("Telegram Stars invoice must be paid using payment_method=stars"));
    }

    if !is_stars_invoice && payment_method == "stars" {
        return Err(ApiError::bad_request("Non-Stars invoice cannot be paid using payment_method=stars"));
    }

    let tip_amount = body.tip_amount.unwrap_or(0);
    if tip_amount < 0 {
        return Err(ApiError::bad_request("tip_amount must be non-negative"));
    }
    if is_stars_invoice && tip_amount > 0 {
        return Err(ApiError::bad_request("tip_amount is not supported for Telegram Stars invoices"));
    }
    if tip_amount > max_tip_amount {
        return Err(ApiError::bad_request("tip_amount exceeds invoice max_tip_amount"));
    }
    let total_amount = invoice_total_amount
        .checked_add(tip_amount)
        .ok_or_else(|| ApiError::bad_request("total amount overflow"))?;

    let from_user = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    let now = Utc::now().timestamp();
    let mut selected_shipping_option_id: Option<String> = None;

    if need_shipping_address == 1 {
        let shipping_query_id = generate_telegram_numeric_id();
        let shipping_address = ShippingAddress {
            country_code: "US".to_string(),
            state: "CA".to_string(),
            city: "San Francisco".to_string(),
            street_line1: "Market Street".to_string(),
            street_line2: "Suite 100".to_string(),
            post_code: "94103".to_string(),
        };

        conn.execute(
            "INSERT INTO shipping_queries
             (id, bot_id, chat_key, from_user_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                shipping_query_id,
                bot.id,
                chat_key,
                user.id,
                invoice_payload,
                now,
            ],
        )
        .map_err(ApiError::internal)?;

        let shipping_update = serde_json::to_value(Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: Some(ShippingQuery {
                id: shipping_query_id.clone(),
                from: from_user.clone(),
                invoice_payload: invoice_payload.clone(),
                shipping_address,
            }),
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
        managed_bot: None,
        })
        .map_err(ApiError::internal)?;
        persist_and_dispatch_update(state, &mut conn, token, bot.id, shipping_update)?;

        let mut answer_json: Option<String> = None;
        for _ in 0..15 {
            answer_json = conn
                .query_row(
                    "SELECT COALESCE(answer_json, '') FROM shipping_queries WHERE id = ?1 AND bot_id = ?2",
                    params![shipping_query_id, bot.id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(ApiError::internal)?;

            if answer_json.as_ref().map(|value| !value.trim().is_empty()).unwrap_or(false) {
                break;
            }

            std::thread::sleep(Duration::from_millis(120));
        }

        let Some(answer_raw) = answer_json.filter(|value| !value.trim().is_empty()) else {
            return Err(ApiError::bad_request("shipping_query pending; call answerShippingQuery, then retry payment"));
        };

        let shipping_answer: AnswerShippingQueryRequest = serde_json::from_str(&answer_raw)
            .map_err(|_| ApiError::bad_request("invalid answerShippingQuery payload"))?;

        if !shipping_answer.ok {
            return Err(ApiError::bad_request(
                shipping_answer
                    .error_message
                    .unwrap_or_else(|| "shipping query was rejected".to_string()),
            ));
        }

        selected_shipping_option_id = shipping_answer
            .shipping_options
            .as_ref()
            .and_then(|options| options.first())
            .map(|option| option.id.clone());

        if is_flexible == 1 && selected_shipping_option_id.is_none() {
            return Err(ApiError::bad_request("flexible shipping requires at least one shipping option in answerShippingQuery"));
        }
    }

    let pre_checkout_query_id = generate_telegram_numeric_id();

    let order_info = if need_name == 1 || need_phone_number == 1 || need_email == 1 || need_shipping_address == 1 {
        Some(OrderInfo {
            name: if need_name == 1 { Some(user.first_name.clone()) } else { None },
            phone_number: if need_phone_number == 1 { Some("+10000000000".to_string()) } else { None },
            email: if need_email == 1 {
                Some(format!("{}@laragram.local", user.username.clone().unwrap_or_else(|| format!("user{}", user.id))))
            } else {
                None
            },
            shipping_address: if need_shipping_address == 1 {
                Some(ShippingAddress {
                    country_code: "US".to_string(),
                    state: "CA".to_string(),
                    city: "San Francisco".to_string(),
                    street_line1: "Market Street".to_string(),
                    street_line2: "Suite 100".to_string(),
                    post_code: "94103".to_string(),
                })
            } else {
                None
            },
        })
    } else {
        None
    };

    conn.execute(
        "INSERT INTO pre_checkout_queries
         (id, bot_id, chat_key, from_user_id, payload, currency, total_amount, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            pre_checkout_query_id,
            bot.id,
            chat_key,
            user.id,
            invoice_payload,
            currency,
            total_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let pre_checkout_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: Some(PreCheckoutQuery {
            id: pre_checkout_query_id.clone(),
            from: from_user.clone(),
            currency: currency.clone(),
            total_amount,
            invoice_payload: invoice_payload.clone(),
            shipping_option_id: selected_shipping_option_id.clone(),
            order_info: order_info.clone(),
        }),
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, pre_checkout_update)?;

    if outcome == "failed" {
        return Ok(json!({
            "status": "failed",
            "pre_checkout_query_id": pre_checkout_query_id,
            "payment_method": payment_method,
        }));
    }

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, body.chat_id.to_string(), user.id, format!("Paid: {}", invoice_title), now],
    )
    .map_err(ApiError::internal)?;

    let paid_message_id = conn.last_insert_rowid();
    let mut paid_message = load_message_value(&mut conn, &bot, paid_message_id)?;
    paid_message.as_object_mut().map(|obj| obj.remove("text"));

    let successful_payment = SuccessfulPayment {
        currency,
        total_amount,
        invoice_payload,
        subscription_expiration_date: None,
        is_recurring: None,
        is_first_recurring: None,
        shipping_option_id: selected_shipping_option_id,
        order_info,
        telegram_payment_charge_id: format!("tg_{}_{}", payment_method, generate_telegram_numeric_id()),
        provider_payment_charge_id: format!("provider_{}_{}", payment_method, generate_telegram_numeric_id()),
    };

    if payment_method == "stars" {
        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'payment')",
            params![
                format!("pay_{}", generate_telegram_numeric_id()),
                bot.id,
                user.id,
                successful_payment.telegram_payment_charge_id.clone(),
                total_amount,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    paid_message["successful_payment"] = serde_json::to_value(successful_payment).map_err(ApiError::internal)?;
    let is_channel_post = paid_message
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(Value::as_str)
        == Some("channel");

    let paid_update = serde_json::to_value(Update {
        update_id: 0,
        message: if is_channel_post {
            None
        } else {
            Some(serde_json::from_value(paid_message.clone()).map_err(ApiError::internal)?)
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(serde_json::from_value(paid_message.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, paid_update)?;

    conn.execute(
        "UPDATE invoices SET paid_at = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
        params![now, bot.id, body.chat_id.to_string(), body.message_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "status": "success",
        "pre_checkout_query_id": pre_checkout_query_id,
        "message_id": paid_message_id,
        "payment_method": payment_method,
    }))
}

pub fn handle_sim_purchase_paid_media(
    state: &Data<AppState>,
    token: &str,
    body: SimPurchasePaidMediaRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if exists.is_none() {
        return Err(ApiError::not_found("paid media message not found"));
    }

    let message_value = load_message_value(&mut conn, &bot, body.message_id)?;
    let is_paid_post = message_value
        .get("is_paid_post")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !is_paid_post {
        return Err(ApiError::bad_request("message is not a paid media post"));
    }

    let paid_star_count = message_value
        .get("paid_star_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if paid_star_count <= 0 {
        return Err(ApiError::bad_request("paid media star count is invalid"));
    }

    let paid_media_payload = body
        .paid_media_payload
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            message_value
                .get("paid_media_payload")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("paid_media:{}:{}", body.chat_id, body.message_id));

    let now = Utc::now().timestamp();
    let purchase_charge_id = format!("paid_media_purchase:{}:{}", paid_media_payload, user.id);
    let already_purchased = match conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'paid_media_purchase')",
        params![
            format!("paid_media_purchase_{}", generate_telegram_numeric_id()),
            bot.id,
            user.id,
            purchase_charge_id,
            paid_star_count,
            now,
        ],
    ) {
        Ok(_) => false,
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            true
        }
        Err(error) => return Err(ApiError::internal(error)),
    };

    if !already_purchased {
        let purchaser = User {
            id: user.id,
            is_bot: false,
            first_name: user.first_name,
            last_name: None,
            username: user.username,
            language_code: None,
            is_premium: None,
            added_to_attachment_menu: None,
            can_join_groups: None,
            can_read_all_group_messages: None,
            supports_inline_queries: None,
            can_connect_to_business: None,
            has_main_web_app: None,
            has_topics_enabled: None,
            allows_users_to_create_topics: None,
            can_manage_bots: None,
        };

        let purchased_update = serde_json::to_value(Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: Some(PaidMediaPurchased {
                from: purchaser,
                paid_media_payload: paid_media_payload.clone(),
            }),
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
            managed_bot: None,
        })
        .map_err(ApiError::internal)?;
        persist_and_dispatch_update(state, &mut conn, token, bot.id, purchased_update)?;
    }

    Ok(json!({
        "status": "success",
        "paid_media_payload": paid_media_payload,
        "star_count": paid_star_count,
        "already_purchased": already_purchased,
    }))
}

fn send_media_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    media_field: &str,
    media_payload: Value,
    message_thread_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    send_media_message_with_group(
        state,
        token,
        chat_id_value,
        caption,
        caption_entities,
        reply_markup,
        media_field,
        media_payload,
        None,
        message_thread_id,
        sim_parse_mode,
    )
}

fn send_paid_media_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    paid_media_payload: Value,
    paid_star_count: i64,
    show_caption_above_media: Option<bool>,
    message_thread_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat) = resolve_bot_outbound_chat(&mut conn, bot.id, chat_id_value, ChatSendKind::Other)?;
    let resolved_thread_id = resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Other,
    )?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let paid_media_payload_id = format!("paid_media:{}:{}", chat.id, message_id);

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender,
        "is_paid_post": true,
        "paid_media_payload": paid_media_payload_id,
        "paid_media": paid_media_payload,
    });

    if paid_star_count > 0 {
        base["paid_star_count"] = Value::from(paid_star_count);
    }
    if let Some(c) = caption {
        base["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        base["caption_entities"] = entities;
    }
    if let Some(show_above) = show_caption_above_media {
        base["show_caption_above_media"] = Value::Bool(show_above);
    }
    if let Some(thread_id) = resolved_thread_id {
        base["message_thread_id"] = Value::from(thread_id);
        base["is_topic_message"] = Value::Bool(true);
    }
    if let Some(mode) = sim_parse_mode {
        base["sim_parse_mode"] = Value::String(mode);
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post { None } else { Some(message.clone()) },
        edited_message: None,
        channel_post: if is_channel_post { Some(message) } else { None },
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update.clone());

    if is_channel_post {
        forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

fn send_payload_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    text: Option<String>,
    entities: Option<Value>,
    reply_markup: Option<Value>,
    payload_field: &str,
    payload_value: Value,
    message_thread_id: Option<i64>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let send_kind = send_kind_from_payload_field(payload_field);
    let (chat_key, chat) = resolve_bot_outbound_chat(&mut conn, bot.id, chat_id_value, send_kind)?;
    let resolved_thread_id = resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        send_kind,
    )?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, text.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender
    });

    base[payload_field] = payload_value;
    if let Some(t) = text {
        base["text"] = Value::String(t);
    }
    if let Some(e) = entities {
        base["entities"] = e;
    }
    if let Some(thread_id) = resolved_thread_id {
        base["message_thread_id"] = Value::from(thread_id);
        base["is_topic_message"] = Value::Bool(true);
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post { None } else { Some(message.clone()) },
        edited_message: None,
        channel_post: if is_channel_post { Some(message) } else { None },
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update.clone());

    if is_channel_post {
        forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

fn send_media_message_with_group(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    media_field: &str,
    media_payload: Value,
    media_group_id: Option<&str>,
    message_thread_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let send_kind = send_kind_from_media_field(media_field);
    let (chat_key, chat) = resolve_bot_outbound_chat(&mut conn, bot.id, chat_id_value, send_kind)?;
    let resolved_thread_id = resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        send_kind,
    )?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender
    });

    base[media_field] = media_payload;
    if let Some(c) = caption {
        base["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        base["caption_entities"] = entities;
    }
    if let Some(group_id) = media_group_id {
        base["media_group_id"] = Value::String(group_id.to_string());
    }
    if let Some(thread_id) = resolved_thread_id {
        base["message_thread_id"] = Value::from(thread_id);
        base["is_topic_message"] = Value::Bool(true);
    }
    if let Some(mode) = sim_parse_mode {
        base["sim_parse_mode"] = Value::String(mode);
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post { None } else { Some(message.clone()) },
        edited_message: None,
        channel_post: if is_channel_post { Some(message) } else { None },
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update.clone());

    if is_channel_post {
        forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

fn save_linked_discussion_mapping(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
    discussion_root_message_id: i64,
    channel_chat_key: &str,
    channel_message_id: i64,
    now: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_linked_discussion_messages
         (bot_id, discussion_chat_key, discussion_message_id, discussion_root_message_id, channel_chat_key, channel_message_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
         ON CONFLICT(bot_id, discussion_chat_key, discussion_message_id)
         DO UPDATE SET
             discussion_root_message_id = excluded.discussion_root_message_id,
             channel_chat_key = excluded.channel_chat_key,
             channel_message_id = excluded.channel_message_id,
             updated_at = excluded.updated_at",
        params![
            bot_id,
            discussion_chat_key,
            discussion_message_id,
            discussion_root_message_id,
            channel_chat_key,
            channel_message_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn load_linked_discussion_mapping_for_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
) -> Result<Option<(String, i64, i64)>, ApiError> {
    conn.query_row(
        "SELECT channel_chat_key, channel_message_id, discussion_root_message_id
         FROM sim_linked_discussion_messages
         WHERE bot_id = ?1 AND discussion_chat_key = ?2 AND discussion_message_id = ?3",
        params![bot_id, discussion_chat_key, discussion_message_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
    .map_err(ApiError::internal)
}

fn is_reply_to_linked_discussion_root_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    reply_to_message_id: Option<i64>,
) -> Result<bool, ApiError> {
    let Some(reply_id) = reply_to_message_id else {
        return Ok(false);
    };

    Ok(load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        reply_id,
    )?
    .is_some())
}

fn ensure_linked_discussion_forward_for_channel_post(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    channel_chat_key: &str,
    channel_message_value: &Value,
) -> Result<(), ApiError> {
    let Some(channel_sim_chat) = load_sim_chat_record(conn, bot.id, channel_chat_key)? else {
        return Ok(());
    };
    if channel_sim_chat.chat_type != "channel" {
        return Ok(());
    }

    let Some(linked_discussion_chat_id) = channel_sim_chat.linked_discussion_chat_id else {
        return Ok(());
    };
    let linked_discussion_chat_key = linked_discussion_chat_id.to_string();
    let Some(linked_discussion_chat) = load_sim_chat_record(conn, bot.id, &linked_discussion_chat_key)? else {
        return Ok(());
    };
    if linked_discussion_chat.chat_type != "group" && linked_discussion_chat.chat_type != "supergroup" {
        return Ok(());
    }

    let channel_message_id = channel_message_value
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("channel_post missing message_id"))?;

    if is_service_message_for_transport(channel_message_value)
        || !message_has_transportable_content(channel_message_value)
    {
        return Ok(());
    }

    let existing_forward: Option<(i64, i64)> = conn
        .query_row(
            "SELECT discussion_message_id, discussion_root_message_id
             FROM sim_linked_discussion_messages
             WHERE bot_id = ?1
               AND channel_chat_key = ?2
               AND channel_message_id = ?3
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, channel_chat_key, channel_message_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if existing_forward.is_some() {
        return Ok(());
    }

    let sender_chat_override = channel_message_value
        .get("sender_chat")
        .cloned()
        .or_else(|| channel_message_value.get("chat").cloned());
    let linked_discussion_context = LinkedDiscussionTransportContext {
        channel_chat_key: channel_chat_key.to_string(),
        channel_message_id,
        discussion_root_message_id: None,
    };
    let discussion_reply_to_message_id = channel_message_value
        .get("reply_to_message")
        .and_then(Value::as_object)
        .and_then(|reply| reply.get("message_id"))
        .and_then(Value::as_i64)
        .map(|parent_channel_message_id| {
            conn.query_row(
                "SELECT discussion_root_message_id
                 FROM sim_linked_discussion_messages
                 WHERE bot_id = ?1
                   AND discussion_chat_key = ?2
                   AND channel_chat_key = ?3
                   AND channel_message_id = ?4
                 ORDER BY updated_at DESC
                 LIMIT 1",
                params![
                    bot.id,
                    &linked_discussion_chat_key,
                    channel_chat_key,
                    parent_channel_message_id,
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)
        })
        .transpose()?
        .flatten();

    let forwarded_value = with_request_actor_user_id(Some(bot.id), || {
        copy_message_internal(
            state,
            conn,
            token,
            bot,
            &Value::String(channel_chat_key.to_string()),
            &Value::from(linked_discussion_chat_id),
            channel_message_id,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            discussion_reply_to_message_id,
            sender_chat_override,
            Some(true),
            Some(linked_discussion_context),
            true,
        )
    })?;

    let discussion_message_id = forwarded_value
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("forwarded discussion message missing message_id"))?;
    let now = Utc::now().timestamp();

    save_linked_discussion_mapping(
        conn,
        bot.id,
        &linked_discussion_chat_key,
        discussion_message_id,
        discussion_message_id,
        channel_chat_key,
        channel_message_id,
        now,
    )?;

    Ok(())
}

fn enrich_reply_with_linked_channel_context(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    reply_to_message_id: i64,
    message_json: &mut Value,
) -> Result<(), ApiError> {
    let reply_mapping = load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        reply_to_message_id,
    )?;

    let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = reply_mapping else {
        return Ok(());
    };

    if let Some(reply_obj) = message_json
        .get_mut("reply_to_message")
        .and_then(Value::as_object_mut)
    {
        reply_obj.insert("linked_channel_message_id".to_string(), Value::from(channel_message_id));
        reply_obj.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
        reply_obj.insert("linked_channel_chat_id".to_string(), Value::String(channel_chat_key.clone()));
    }

    if let Some(obj) = message_json.as_object_mut() {
        obj.insert("linked_channel_message_id".to_string(), Value::from(channel_message_id));
        obj.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
        obj.insert("linked_channel_chat_id".to_string(), Value::String(channel_chat_key));
    }

    Ok(())
}

fn enrich_message_with_linked_channel_context(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
    message_json: &mut Value,
) -> Result<(), ApiError> {
    let mapping = load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        discussion_message_id,
    )?;

    let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = mapping else {
        return Ok(());
    };

    if let Some(obj) = message_json.as_object_mut() {
        obj.insert("linked_channel_message_id".to_string(), Value::from(channel_message_id));
        obj.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
        obj.insert("linked_channel_chat_id".to_string(), Value::String(channel_chat_key));
    }

    Ok(())
}

fn map_discussion_message_to_channel_post_if_needed(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
    reply_to_message_id: Option<i64>,
) -> Result<(), ApiError> {
    let Some(reply_id) = reply_to_message_id else {
        return Ok(());
    };

    let reply_mapping = load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        reply_id,
    )?;

    let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = reply_mapping else {
        return Ok(());
    };

    let now = Utc::now().timestamp();
    save_linked_discussion_mapping(
        conn,
        bot_id,
        discussion_chat_key,
        discussion_message_id,
        discussion_root_message_id,
        &channel_chat_key,
        channel_message_id,
        now,
    )?;

    Ok(())
}

#[derive(Debug)]
struct StoredFile {
    file_id: String,
    file_unique_id: String,
    file_path: String,
    mime_type: Option<String>,
    file_size: Option<i64>,
}

#[derive(Debug, Clone)]
struct StickerMeta {
    set_name: Option<String>,
    sticker_type: String,
    format: String,
    emoji: Option<String>,
    mask_position_json: Option<String>,
    custom_emoji_id: Option<String>,
    needs_repainting: bool,
}

fn normalize_sticker_format(value: &str) -> Result<&'static str, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "static" => Ok("static"),
        "animated" => Ok("animated"),
        "video" => Ok("video"),
        _ => Err(ApiError::bad_request("sticker format must be static, animated, or video")),
    }
}

fn normalize_sticker_type(value: &str) -> Result<&'static str, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "regular" => Ok("regular"),
        "mask" => Ok("mask"),
        "custom_emoji" => Ok("custom_emoji"),
        _ => Err(ApiError::bad_request("sticker_type must be regular, mask, or custom_emoji")),
    }
}

fn sticker_format_flags(format: &str) -> (bool, bool) {
    match format {
        "animated" => (true, false),
        "video" => (false, true),
        _ => (false, false),
    }
}

fn derive_custom_emoji_id(bot_id: i64, file_unique_id: &str) -> String {
    let mut hash: u64 = 1469598103934665603;
    for byte in file_unique_id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash ^= bot_id as u64;
    hash = hash.wrapping_mul(1099511628211);
    hash.to_string()
}

fn infer_sticker_format_from_file(file: &StoredFile) -> Option<&'static str> {
    let mime = file
        .mime_type
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let path = file.file_path.to_ascii_lowercase();

    if mime.contains("webm") || path.ends_with(".webm") {
        return Some("video");
    }
    if mime.contains("x-tgsticker") || path.ends_with(".tgs") {
        return Some("animated");
    }
    if mime.contains("webp") || path.ends_with(".webp") {
        return Some("static");
    }

    None
}

fn load_sticker_meta(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    file_id: &str,
) -> Result<Option<StickerMeta>, ApiError> {
    conn.query_row(
        "SELECT set_name, sticker_type, format, emoji, mask_position_json, custom_emoji_id, COALESCE(needs_repainting, 0)
         FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
        params![bot_id, file_id],
        |r| {
            Ok(StickerMeta {
                set_name: r.get(0)?,
                sticker_type: r.get(1)?,
                format: r.get(2)?,
                emoji: r.get(3)?,
                mask_position_json: r.get(4)?,
                custom_emoji_id: r.get(5)?,
                needs_repainting: r.get::<_, i64>(6)? == 1,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn sticker_from_row(
    file_id: String,
    file_unique_id: String,
    set_name: Option<String>,
    sticker_type: String,
    format: String,
    emoji: Option<String>,
    mask_position_json: Option<String>,
    custom_emoji_id: Option<String>,
    needs_repainting: bool,
    file_size: Option<i64>,
) -> Sticker {
    let (is_animated, is_video) = sticker_format_flags(&format);
    Sticker {
        file_id,
        file_unique_id,
        r#type: sticker_type,
        width: 512,
        height: 512,
        is_animated,
        is_video,
        thumbnail: None,
        emoji,
        set_name,
        premium_animation: None,
        mask_position: mask_position_json.and_then(|raw| serde_json::from_str::<MaskPosition>(&raw).ok()),
        custom_emoji_id,
        needs_repainting: Some(needs_repainting),
        file_size,
    }
}

fn load_set_stickers(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    set_name: &str,
) -> Result<Vec<Sticker>, ApiError> {
    let mut stmt = conn
        .prepare(
            "SELECT s.file_id, s.file_unique_id, s.set_name, s.sticker_type, s.format, s.emoji, s.mask_position_json,
                    s.custom_emoji_id, COALESCE(s.needs_repainting, 0), f.file_size
             FROM stickers s
             LEFT JOIN files f ON f.bot_id = s.bot_id AND f.file_id = s.file_id
             WHERE s.bot_id = ?1 AND s.set_name = ?2
             ORDER BY s.position ASC, s.created_at ASC",
        )
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(params![bot_id, set_name], |r| {
            Ok(sticker_from_row(
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get::<_, i64>(8)? == 1,
                r.get(9)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut stickers = Vec::new();
    for row in rows {
        stickers.push(row.map_err(ApiError::internal)?);
    }
    Ok(stickers)
}

fn upsert_set_sticker(
    _state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    set_name: &str,
    sticker_type: &str,
    needs_repainting: bool,
    input: &InputSticker,
    position: i64,
) -> Result<(), ApiError> {
    if input.emoji_list.is_empty() {
        return Err(ApiError::bad_request("input sticker must include at least one emoji"));
    }

    let requested_format = normalize_sticker_format(&input.format)?;
    let file = resolve_media_file_with_conn(conn, bot.id, &Value::String(input.sticker.clone()), "sticker")?;
    let format = infer_sticker_format_from_file(&file).unwrap_or(requested_format);
    let now = Utc::now().timestamp();
    let (is_animated, is_video) = sticker_format_flags(format);

    let existing_custom_emoji_id: Option<String> = conn
        .query_row(
            "SELECT custom_emoji_id FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, file.file_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .flatten();
    let custom_emoji_id = if sticker_type == "custom_emoji" {
        existing_custom_emoji_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| Some(derive_custom_emoji_id(bot.id, &file.file_unique_id)))
    } else {
        None
    };

    let mask_json = input
        .mask_position
        .as_ref()
        .map(|m| serde_json::to_string(m).map_err(ApiError::internal))
        .transpose()?;
    let keywords_json = input
        .keywords
        .as_ref()
        .map(|k| serde_json::to_string(k).map_err(ApiError::internal))
        .transpose()?;
    let emoji_json = serde_json::to_string(&input.emoji_list).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO stickers
         (bot_id, file_id, file_unique_id, set_name, sticker_type, format, width, height, is_animated, is_video,
          emoji, emoji_list_json, keywords_json, mask_position_json, custom_emoji_id, needs_repainting, position, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 512, 512, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?16)
         ON CONFLICT(bot_id, file_id) DO UPDATE SET
             set_name = excluded.set_name,
             sticker_type = excluded.sticker_type,
             format = excluded.format,
             is_animated = excluded.is_animated,
             is_video = excluded.is_video,
             emoji = excluded.emoji,
             emoji_list_json = excluded.emoji_list_json,
             keywords_json = excluded.keywords_json,
             mask_position_json = excluded.mask_position_json,
             custom_emoji_id = excluded.custom_emoji_id,
             needs_repainting = excluded.needs_repainting,
             position = excluded.position,
             updated_at = excluded.updated_at",
        params![
            bot.id,
            file.file_id,
            file.file_unique_id,
            set_name,
            sticker_type,
            format,
            if is_animated { 1 } else { 0 },
            if is_video { 1 } else { 0 },
            input.emoji_list[0].clone(),
            emoji_json,
            keywords_json,
            mask_json,
            custom_emoji_id,
            if needs_repainting { 1 } else { 0 },
            position,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn compact_sticker_positions(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    set_name: &str,
) -> Result<(), ApiError> {
    let mut stmt = conn
        .prepare("SELECT file_id FROM stickers WHERE bot_id = ?1 AND set_name = ?2 ORDER BY position ASC, created_at ASC")
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(params![bot_id, set_name], |r| r.get::<_, String>(0))
        .map_err(ApiError::internal)?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(ApiError::internal)?);
    }

    let now = Utc::now().timestamp();
    for (idx, file_id) in ids.iter().enumerate() {
        conn.execute(
            "UPDATE stickers SET position = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![idx as i64, now, bot_id, file_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(())
}

fn resolve_media_file(
    state: &Data<AppState>,
    token: &str,
    input: &Value,
    media_kind: &str,
) -> Result<StoredFile, ApiError> {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    resolve_media_file_with_conn(&mut conn, bot.id, input, media_kind)
}

fn parse_input_file_value(input: &Value, field: &str) -> Result<Value, ApiError> {
    match input {
        Value::String(_) => Ok(input.clone()),
        Value::Object(obj) => {
            if let Some(extra) = obj.get("extra") {
                return match extra {
                    Value::String(_) => Ok(extra.clone()),
                    _ => Err(ApiError::bad_request(format!("{field} extra must be string"))),
                };
            }

            if let Some(media) = obj.get("media") {
                return match media {
                    Value::String(_) => Ok(media.clone()),
                    _ => Err(ApiError::bad_request(format!("{field} media must be string"))),
                };
            }

            Err(ApiError::bad_request(format!("{field} must be a string or InputFile object")))
        }
        _ => Err(ApiError::bad_request(format!("{field} must be a string or InputFile object"))),
    }
}

fn resolve_media_file_with_conn(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    input: &Value,
    media_kind: &str,
) -> Result<StoredFile, ApiError> {
    let input_text = input
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("{} is invalid", media_kind)))?;

    if input_text.starts_with("http://") || input_text.starts_with("https://") {
        let (bytes, mime) = download_remote_file(&input_text)?;
        return store_binary_file(conn, bot_id, &bytes, mime.as_deref(), Some(input_text));
    }

    let local_candidate = if let Some(path) = input_text.strip_prefix("file://") {
        path.to_string()
    } else {
        input_text.clone()
    };

    if !local_candidate.is_empty() && Path::new(&local_candidate).exists() {
        let bytes = fs::read(&local_candidate).map_err(ApiError::internal)?;
        if bytes.is_empty() {
            return Err(ApiError::bad_request("uploaded file is empty"));
        }
        return store_binary_file(
            conn,
            bot_id,
            &bytes,
            None,
            Some(local_candidate),
        );
    }

    let existing: Option<(String, String, String, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT file_id, file_unique_id, file_path, mime_type, file_size
             FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot_id, input_text],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some((file_id, file_unique_id, file_path, mime_type, file_size)) = existing {
        return Ok(StoredFile {
            file_id,
            file_unique_id,
            file_path,
            mime_type,
            file_size,
        });
    }

    let now = Utc::now().timestamp();
    let file_id = input_text.clone();
    let file_unique_id = uuid::Uuid::new_v4().simple().to_string();
    let file_path = format!("virtual/{}/{}", bot_id, file_id.replace('/', "_"));

    conn.execute(
        "INSERT INTO files (bot_id, file_id, file_unique_id, file_path, local_path, mime_type, file_size, source, created_at)
         VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, ?5, ?6)",
        params![bot_id, file_id, file_unique_id, file_path, input_text, now],
    )
    .map_err(ApiError::internal)?;

    Ok(StoredFile {
        file_id,
        file_unique_id,
        file_path,
        mime_type: None,
        file_size: None,
    })
}

fn download_remote_file(url: &str) -> Result<(Vec<u8>, Option<String>), ApiError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(ApiError::internal)?;

    let response = client
        .get(url)
        .send()
        .map_err(|_| ApiError::bad_request("failed to fetch remote file"))?;

    if !response.status().is_success() {
        return Err(ApiError::bad_request("remote file url returned non-200 status"));
    }

    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let bytes = response.bytes().map_err(ApiError::internal)?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request("remote file is empty"));
    }

    Ok((bytes.to_vec(), mime))
}

fn store_binary_file(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    bytes: &[u8],
    mime_type: Option<&str>,
    source: Option<String>,
) -> Result<StoredFile, ApiError> {
    let now = Utc::now().timestamp();
    let file_id = generate_telegram_file_id("file");
    let file_unique_id = generate_telegram_file_unique_id();
    let file_path = format!("media/{}/{}", bot_id, file_id);

    let base_dir = media_storage_root();
    fs::create_dir_all(&base_dir).map_err(ApiError::internal)?;
    let local_path = base_dir.join(&file_id);
    fs::write(&local_path, bytes).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO files (bot_id, file_id, file_unique_id, file_path, local_path, mime_type, file_size, source, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            bot_id,
            file_id,
            file_unique_id,
            file_path,
            local_path.to_string_lossy().to_string(),
            mime_type,
            bytes.len() as i64,
            source,
            now
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(StoredFile {
        file_id,
        file_unique_id,
        file_path,
        mime_type: mime_type.map(|m| m.to_string()),
        file_size: Some(bytes.len() as i64),
    })
}

fn media_storage_root() -> PathBuf {
    std::env::var("FILE_STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new("files").to_path_buf())
}

pub fn handle_download_file(
    state: &Data<AppState>,
    token: &str,
    file_path: &str,
) -> Result<(Vec<u8>, Option<String>), ApiError> {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT local_path, mime_type FROM files WHERE bot_id = ?1 AND file_path = ?2",
            params![bot.id, file_path],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((local_path, mime_type)) = row else {
        return Err(ApiError::not_found("file not found"));
    };

    let Some(path) = local_path else {
        return Err(ApiError::bad_request("file is not available for local download"));
    };

    let bytes = fs::read(path).map_err(ApiError::internal)?;
    Ok((bytes, mime_type))
}

pub fn handle_sim_bootstrap(state: &Data<AppState>, token: &str) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_default_user(&mut conn)?;
    ensure_sim_verifications_storage(&mut conn)?;

    let mut users_stmt = conn
        .prepare(
            "SELECT u.id, u.username, u.first_name, u.last_name, u.phone_number, u.photo_url, u.bio, u.is_premium,
                    u.business_name, u.business_intro, u.business_location, u.gift_count,
                    v.custom_description
             FROM users u
             LEFT JOIN sim_user_verifications v
                ON v.bot_id = ?1 AND v.user_id = u.id
             ORDER BY u.id ASC",
        )
        .map_err(ApiError::internal)?;
    let users_rows = users_stmt
        .query_map(params![bot.id], |row| {
            let verification_description: Option<String> = row.get(12)?;
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "username": row.get::<_, Option<String>>(1)?,
                "first_name": row.get::<_, String>(2)?,
                "last_name": row.get::<_, Option<String>>(3)?,
                "phone_number": row.get::<_, Option<String>>(4)?,
                "photo_url": row.get::<_, Option<String>>(5)?,
                "bio": row.get::<_, Option<String>>(6)?,
                "is_premium": row.get::<_, i64>(7)? == 1,
                "business_name": row.get::<_, Option<String>>(8)?,
                "business_intro": row.get::<_, Option<String>>(9)?,
                "business_location": row.get::<_, Option<String>>(10)?,
                "gift_count": row.get::<_, i64>(11)?,
                "is_verified": verification_description.is_some(),
                "verification_description": verification_description,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut users = Vec::<Value>::new();
    for row in users_rows {
        users.push(row.map_err(ApiError::internal)?);
    }

    let mut chats_stmt = conn
        .prepare(
                        "SELECT c.chat_id, c.chat_type, c.title, c.username, c.is_forum, c.is_direct_messages,
                                cv.custom_description
                         FROM sim_chats c
                         LEFT JOIN sim_chats parent
                             ON parent.bot_id = c.bot_id
                            AND parent.chat_type = 'channel'
                            AND c.parent_channel_chat_id = parent.chat_id
                         LEFT JOIN sim_chat_verifications cv
                            ON cv.bot_id = c.bot_id AND cv.chat_key = c.chat_key
                         WHERE c.bot_id = ?1
                             AND (
                                        COALESCE(c.is_direct_messages, 0) = 0
                                        OR COALESCE(parent.direct_messages_enabled, 0) = 1
                             )
                         ORDER BY c.chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let chats_rows = chats_stmt
        .query_map(params![bot.id], |row| {
            let chat = Chat {
                id: row.get(0)?,
                r#type: row.get(1)?,
                title: row.get(2)?,
                username: row.get(3)?,
                first_name: None,
                last_name: None,
                is_forum: Some(row.get::<_, i64>(4)? == 1),
                is_direct_messages: if row.get::<_, i64>(5)? == 1 {
                    Some(true)
                } else {
                    None
                },
            };
            let verification_description: Option<String> = row.get(6)?;
            Ok((chat, verification_description))
        })
        .map_err(ApiError::internal)?;
    let mut chats = Vec::<Value>::new();
    for row in chats_rows {
        let (chat, verification_description) = row.map_err(ApiError::internal)?;
        let mut chat_value = serde_json::to_value(chat).map_err(ApiError::internal)?;
        if let Some(object) = chat_value.as_object_mut() {
            object.insert(
                "is_verified".to_string(),
                Value::Bool(verification_description.is_some()),
            );
            if let Some(description) = verification_description {
                object.insert(
                    "verification_description".to_string(),
                    Value::String(description),
                );
            }
        }
        chats.push(chat_value);
    }

    let mut channel_direct_messages_stmt = conn
        .prepare(
                        "SELECT c.parent_channel_chat_id, c.chat_id
                         FROM sim_chats c
                         INNER JOIN sim_chats parent
                             ON parent.bot_id = c.bot_id
                            AND parent.chat_type = 'channel'
                            AND c.parent_channel_chat_id = parent.chat_id
                         WHERE c.bot_id = ?1
                             AND c.is_direct_messages = 1
                             AND c.parent_channel_chat_id IS NOT NULL
                             AND parent.direct_messages_enabled = 1
                         ORDER BY c.chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let channel_direct_messages_rows = channel_direct_messages_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "channel_chat_id": row.get::<_, i64>(0)?,
                "direct_messages_chat_id": row.get::<_, i64>(1)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut channel_direct_messages = Vec::<Value>::new();
    for row in channel_direct_messages_rows {
        channel_direct_messages.push(row.map_err(ApiError::internal)?);
    }

    let mut chat_settings_stmt = conn
        .prepare(
            "SELECT chat_id, description, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id, message_history_visible, slow_mode_delay, permissions_json, direct_messages_enabled, direct_messages_star_count
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_type IN ('group', 'supergroup', 'channel')
             ORDER BY chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let chat_settings_rows = chat_settings_stmt
        .query_map(params![bot.id], |row| {
            let permissions_raw: Option<String> = row.get(7)?;
            let permissions = permissions_raw
                .as_deref()
                .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
                .unwrap_or_else(default_group_permissions);
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "description": row.get::<_, Option<String>>(1)?,
                "show_author_signature": row.get::<_, i64>(2)? == 1,
                "paid_star_reactions_enabled": row.get::<_, i64>(3)? == 1,
                "linked_chat_id": row.get::<_, Option<i64>>(4)?,
                "message_history_visible": row.get::<_, i64>(5)? == 1,
                "slow_mode_delay": row.get::<_, i64>(6)?,
                "direct_messages_enabled": row.get::<_, i64>(8)? == 1,
                "direct_messages_star_count": row.get::<_, i64>(9)?,
                "permissions": permissions,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut chat_settings = Vec::<Value>::new();
    for row in chat_settings_rows {
        chat_settings.push(row.map_err(ApiError::internal)?);
    }

    let mut memberships_stmt = conn
        .prepare(
                        "SELECT c.chat_id, m.user_id, m.status, m.role, m.custom_title, m.tag
             FROM sim_chat_members m
             INNER JOIN sim_chats c
               ON c.bot_id = m.bot_id AND c.chat_key = m.chat_key
             WHERE m.bot_id = ?1
             ORDER BY c.chat_id ASC, m.user_id ASC",
        )
        .map_err(ApiError::internal)?;
    let memberships_rows = memberships_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "user_id": row.get::<_, i64>(1)?,
                "status": row.get::<_, String>(2)?,
                "role": row.get::<_, String>(3)?,
                "custom_title": row.get::<_, Option<String>>(4)?,
                "tag": row.get::<_, Option<String>>(5)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut memberships = Vec::<Value>::new();
    for row in memberships_rows {
        memberships.push(row.map_err(ApiError::internal)?);
    }

    let mut join_requests_stmt = conn
        .prepare(
            "SELECT c.chat_id, r.user_id, r.invite_link, r.status, r.created_at, u.first_name, u.username
             FROM sim_chat_join_requests r
             INNER JOIN sim_chats c
               ON c.bot_id = r.bot_id AND c.chat_key = r.chat_key
             LEFT JOIN users u
               ON u.id = r.user_id
             WHERE r.bot_id = ?1
             ORDER BY r.created_at ASC",
        )
        .map_err(ApiError::internal)?;
    let join_requests_rows = join_requests_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "user_id": row.get::<_, i64>(1)?,
                "invite_link": row.get::<_, Option<String>>(2)?,
                "status": row.get::<_, String>(3)?,
                "date": row.get::<_, i64>(4)?,
                "first_name": row.get::<_, Option<String>>(5)?,
                "username": row.get::<_, Option<String>>(6)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut join_requests = Vec::<Value>::new();
    for row in join_requests_rows {
        join_requests.push(row.map_err(ApiError::internal)?);
    }

    let mut forum_topics_stmt = conn
        .prepare(
            "SELECT c.chat_id, t.message_thread_id, t.name, t.icon_color, t.icon_custom_emoji_id,
                    t.is_closed, t.updated_at
             FROM forum_topics t
             INNER JOIN sim_chats c
               ON c.bot_id = t.bot_id AND c.chat_key = t.chat_key
             WHERE t.bot_id = ?1
             ORDER BY c.chat_id ASC, t.message_thread_id ASC",
        )
        .map_err(ApiError::internal)?;
    let forum_topics_rows = forum_topics_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "message_thread_id": row.get::<_, i64>(1)?,
                "name": row.get::<_, String>(2)?,
                "icon_color": row.get::<_, i64>(3)?,
                "icon_custom_emoji_id": row.get::<_, Option<String>>(4)?,
                "is_closed": row.get::<_, i64>(5)? == 1,
                "is_hidden": false,
                "is_general": false,
                "updated_at": row.get::<_, i64>(6)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut forum_topics = Vec::<Value>::new();
    for row in forum_topics_rows {
        forum_topics.push(row.map_err(ApiError::internal)?);
    }

    let mut general_topics_stmt = conn
        .prepare(
            "SELECT c.chat_id,
                    COALESCE(g.name, 'General') AS name,
                    COALESCE(g.is_closed, 0) AS is_closed,
                    COALESCE(g.is_hidden, 0) AS is_hidden,
                    COALESCE(g.updated_at, CAST(strftime('%s','now') AS INTEGER)) AS updated_at
             FROM sim_chats c
             LEFT JOIN forum_topic_general_states g
               ON g.bot_id = c.bot_id AND g.chat_key = c.chat_key
                         WHERE c.bot_id = ?1
                             AND c.chat_type = 'supergroup'
                             AND c.is_forum = 1
                             AND COALESCE(c.is_direct_messages, 0) = 0
             ORDER BY c.chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let general_topics_rows = general_topics_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "message_thread_id": 1,
                "name": row.get::<_, String>(1)?,
                "icon_color": forum_topic_default_icon_color(),
                "icon_custom_emoji_id": Value::Null,
                "is_closed": row.get::<_, i64>(2)? == 1,
                "is_hidden": row.get::<_, i64>(3)? == 1,
                "is_general": true,
                "updated_at": row.get::<_, i64>(4)?,
            }))
        })
        .map_err(ApiError::internal)?;
    for row in general_topics_rows {
        forum_topics.push(row.map_err(ApiError::internal)?);
    }

    let mut direct_topics_stmt = conn
        .prepare(
            "SELECT c.chat_id, t.topic_id, u.first_name, u.username, t.updated_at
             FROM sim_direct_message_topics t
             INNER JOIN sim_chats c
               ON c.bot_id = t.bot_id AND c.chat_key = t.chat_key
                         INNER JOIN sim_chats parent
                             ON parent.bot_id = c.bot_id
                            AND parent.chat_type = 'channel'
                            AND c.parent_channel_chat_id = parent.chat_id
             LEFT JOIN users u
               ON u.id = t.user_id
                         WHERE t.bot_id = ?1
                             AND parent.direct_messages_enabled = 1
             ORDER BY c.chat_id ASC, t.updated_at DESC, t.topic_id ASC",
        )
        .map_err(ApiError::internal)?;
    let direct_topics_rows = direct_topics_stmt
        .query_map(params![bot.id], |row| {
            let topic_id: i64 = row.get(1)?;
            let first_name: Option<String> = row.get(2)?;
            let username: Option<String> = row.get(3)?;
            let label = first_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    username
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(|value| format!("@{}", value))
                        .unwrap_or_else(|| format!("User {}", topic_id))
                });

            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "message_thread_id": topic_id,
                "name": label,
                "icon_color": forum_topic_default_icon_color(),
                "icon_custom_emoji_id": Value::Null,
                "is_closed": false,
                "is_hidden": false,
                "is_general": false,
                "updated_at": row.get::<_, i64>(4)?,
            }))
        })
        .map_err(ApiError::internal)?;
    for row in direct_topics_rows {
        forum_topics.push(row.map_err(ApiError::internal)?);
    }

    Ok(json!({
        "bot": {
            "id": bot.id,
            "token": token,
            "username": bot.username,
            "first_name": bot.first_name
        },
        "users": users,
        "chats": chats,
        "channel_direct_messages": channel_direct_messages,
        "chat_settings": chat_settings,
        "memberships": memberships,
        "join_requests": join_requests,
        "forum_topics": forum_topics
    }))
}

pub fn handle_sim_get_privacy_mode(
    state: &Data<AppState>,
    token: &str,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let enabled = load_bot_privacy_mode_enabled(&mut conn, bot.id)?;

    Ok(json!({
        "enabled": enabled,
    }))
}

pub fn handle_sim_set_privacy_mode(
    state: &Data<AppState>,
    token: &str,
    body: SimSetPrivacyModeRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    set_bot_privacy_mode_enabled(&mut conn, bot.id, body.enabled)?;

    Ok(json!({
        "enabled": body.enabled,
    }))
}

pub fn handle_sim_set_business_connection(
    state: &Data<AppState>,
    token: &str,
    body: SimSetBusinessConnectionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let connection_id = normalize_business_connection_id(body.business_connection_id.as_deref())
        .unwrap_or_else(|| default_business_connection_id(bot.id, user.id));
    let rights = body.rights.unwrap_or_else(default_business_bot_rights);
    let enabled = body.enabled.unwrap_or(true);

    let record = upsert_sim_business_connection(
        &mut conn,
        bot.id,
        &connection_id,
        user.id,
        user.id,
        &rights,
        enabled,
    )?;

    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: Some(connection.clone()),
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    serde_json::to_value(connection).map_err(ApiError::internal)
}

pub fn handle_sim_remove_business_connection(
    state: &Data<AppState>,
    token: &str,
    body: SimRemoveBusinessConnectionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let requested_connection_id = normalize_business_connection_id(body.business_connection_id.as_deref());
    let record = if let Some(connection_id) = requested_connection_id.as_deref() {
        if let Some(found) = load_sim_business_connection_by_id(&mut conn, bot.id, connection_id)? {
            found
        } else if let Some(user_id) = body.user_id {
            load_sim_business_connection_for_user(&mut conn, bot.id, user_id)?
                .ok_or_else(|| ApiError::not_found("business connection not found"))?
        } else {
            return Err(ApiError::not_found("business connection not found"));
        }
    } else {
        let user_id = body
            .user_id
            .ok_or_else(|| ApiError::bad_request("user_id is required when business_connection_id is omitted"))?;
        load_sim_business_connection_for_user(&mut conn, bot.id, user_id)?
            .ok_or_else(|| ApiError::not_found("business connection not found"))?
    };

    if let Some(user_id) = body.user_id {
        if record.user_id != user_id {
            return Err(ApiError::bad_request(
                "business connection does not belong to the provided user_id",
            ));
        }
    }

    let connection = build_business_connection(&mut conn, bot.id, &record)?;

    conn.execute(
        "DELETE FROM sim_business_read_messages WHERE bot_id = ?1 AND connection_id = ?2",
        params![bot.id, &record.connection_id],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_business_connections WHERE bot_id = ?1 AND connection_id = ?2",
        params![bot.id, &record.connection_id],
    )
    .map_err(ApiError::internal)?;

    let mut disabled_connection = connection;
    disabled_connection.is_enabled = false;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: Some(disabled_connection),
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "deleted": true,
        "business_connection_id": record.connection_id,
        "user_id": record.user_id,
    }))
}

pub fn handle_sim_open_channel_direct_messages(
    state: &Data<AppState>,
    token: &str,
    body: SimOpenChannelDirectMessagesRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let actor_user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let channel_chat_key = body.channel_chat_id.to_string();
    let channel_chat = load_sim_chat_record(&mut conn, bot.id, &channel_chat_key)?
        .ok_or_else(|| ApiError::not_found("channel not found"))?;
    if channel_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("channel_chat_id must point to a channel"));
    }
    if !channel_chat.direct_messages_enabled {
        return Err(ApiError::bad_request("channel direct messages are disabled"));
    }

    let channel_member = load_chat_member_record(&mut conn, bot.id, &channel_chat_key, actor_user.id)?
        .ok_or_else(|| ApiError::bad_request("join channel first to open direct messages"))?;
    if !is_active_chat_member_status(&channel_member.status) {
        return Err(ApiError::bad_request("join channel first to open direct messages"));
    }

    let can_manage_inbox = channel_member.status == "owner"
        || (
            channel_member.status == "admin"
                && channel_admin_has_direct_messages_permission(
                    channel_member.admin_rights_json.as_deref(),
                )
        );

    let dm_chat = ensure_channel_direct_messages_chat(&mut conn, bot.id, &channel_chat)?;
    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &dm_chat.chat_key,
        actor_user.id,
        if can_manage_inbox { "admin" } else { "member" },
        if can_manage_inbox { "admin" } else { "member" },
        Some(now),
        None,
        None,
        None,
        None,
        now,
    )?;

    if !can_manage_inbox {
        upsert_direct_messages_topic(
            &mut conn,
            bot.id,
            &dm_chat.chat_key,
            actor_user.id,
            actor_user.id,
            None,
            None,
        )?;
    }

    let dm_topics = load_direct_messages_topics_for_chat_json(&mut conn, bot.id, &dm_chat.chat_key)?;
    Ok(json!({
        "chat": build_chat_from_group_record(&dm_chat),
        "parent_chat": build_chat_from_group_record(&channel_chat),
        "topics": dm_topics,
    }))
}

pub fn handle_sim_send_user_message(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserMessageRequest,
) -> ApiResult {
    if body.text.trim().is_empty() {
        return Err(ApiError::bad_request("text is empty"));
    }

    let (parsed_text, parsed_entities) = parse_formatted_text(
        &body.text,
        body.parse_mode.as_deref(),
        None,
    );
    let parsed_entities = merge_auto_message_entities(&parsed_text, parsed_entities);
    let sim_parse_mode = normalize_sim_parse_mode(body.parse_mode.as_deref());

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(
        &mut conn,
        body.user_id,
        body.first_name,
        body.username,
    )?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let sim_chat = resolve_sim_chat_for_user_message(&mut conn, bot.id, chat_id, &user)?;
    let is_direct_messages = is_direct_messages_chat(&sim_chat);
    if !is_direct_messages && sim_chat.chat_type != "private" {
        ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &sim_chat.chat_key,
            user.id,
            ChatSendKind::Text,
        )?;
    }

    let mut sender_chat_json: Option<Value> = None;
    let resolved_thread_id: Option<i64>;
    let mut direct_messages_topic_json: Option<Value> = None;

    if is_direct_messages {
        if body.message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not supported in channel direct messages simulation",
            ));
        }
        if body.sender_chat_id.is_some() {
            return Err(ApiError::bad_request(
                "sender_chat_id is not supported in channel direct messages simulation",
            ));
        }

        let (topic_id, topic_value, forced_sender_chat) = resolve_direct_messages_topic_for_sender(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.direct_messages_topic_id,
        )?;
        resolved_thread_id = Some(topic_id);
        direct_messages_topic_json = Some(topic_value);
        if let Some(forced_sender_chat) = forced_sender_chat {
            sender_chat_json = Some(serde_json::to_value(forced_sender_chat).map_err(ApiError::internal)?);
        }
    } else {
        if body.direct_messages_topic_id.is_some() {
            return Err(ApiError::bad_request(
                "direct_messages_topic_id is available only in channel direct messages chats",
            ));
        }
        let sender_chat = resolve_sender_chat_for_sim_user_message(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.sender_chat_id,
            ChatSendKind::Text,
        )?;
        sender_chat_json = sender_chat
            .as_ref()
            .map(|chat| serde_json::to_value(chat).map_err(ApiError::internal))
            .transpose()?;
        resolved_thread_id = resolve_forum_message_thread_id(
            &mut conn,
            bot.id,
            &sim_chat,
            body.message_thread_id,
        )?;
    }

    let business_connection_record = normalize_business_connection_id(body.business_connection_id.as_deref())
        .map(|connection_id| load_business_connection_or_404(&mut conn, bot.id, &connection_id))
        .transpose()?;
    let business_connection_id = if let Some(record) = business_connection_record.as_ref() {
        if is_direct_messages {
            return Err(ApiError::bad_request(
                "business_connection_id is not supported in channel direct messages simulation",
            ));
        }
        if !record.is_enabled {
            return Err(ApiError::bad_request("business connection is disabled"));
        }
        if sim_chat.chat_type != "private" || sim_chat.chat_id != record.user_chat_id || user.id != record.user_id {
            return Err(ApiError::bad_request("business connection does not match chat/user"));
        }
        Some(record.connection_id.clone())
    } else {
        None
    };

    let mut managed_bot_created: Option<ManagedBotCreated> = None;
    let mut managed_bot_update: Option<ManagedBotUpdated> = None;
    if let Some(managed_bot_request) = body.managed_bot_request.as_ref() {
        if managed_bot_request.request_id <= 0 {
            return Err(ApiError::bad_request("request_managed_bot.request_id is invalid"));
        }

        if is_direct_messages || sim_chat.chat_type != "private" {
            return Err(ApiError::bad_request(
                "request_managed_bot is available only in private chats",
            ));
        }

        ensure_sim_managed_bots_storage(&mut conn)?;
        let managed_bot = ensure_managed_bot_record(
            &mut conn,
            bot.id,
            user.id,
            managed_bot_request.suggested_name.as_deref(),
            managed_bot_request.suggested_username.as_deref(),
        )?;

        let managed_bot_user = managed_bot_user_from_record(&managed_bot);
        managed_bot_created = Some(ManagedBotCreated {
            bot: managed_bot_user.clone(),
        });
        managed_bot_update = Some(ManagedBotUpdated {
            user: build_user_with_manage_bots(&user),
            bot: managed_bot_user,
        });
    }

    let mut suggested_post_parameters = body.suggested_post_parameters.clone();
    let mut suggested_post_info: Option<SuggestedPostInfo> = None;
    if let Some(parameters) = suggested_post_parameters.as_mut() {
        if !is_direct_messages {
            return Err(ApiError::bad_request(
                "suggested_post_parameters is available only in channel direct messages chats",
            ));
        }

        let now = Utc::now().timestamp();

        if let Some(price) = parameters.price.as_ref() {
            let normalized_currency = price.currency.trim().to_ascii_uppercase();
            let normalized_amount = price.amount;

            match normalized_currency.as_str() {
                "XTR" => {
                    if !(5..=100_000).contains(&normalized_amount) {
                        return Err(ApiError::bad_request(
                            "suggested post XTR amount must be between 5 and 100000",
                        ));
                    }
                }
                "TON" => {
                    if !(10_000_000..=10_000_000_000_000).contains(&normalized_amount) {
                        return Err(ApiError::bad_request(
                            "suggested post TON amount must be between 10000000 and 10000000000000",
                        ));
                    }
                }
                _ => {
                    return Err(ApiError::bad_request(
                        "suggested post price currency must be XTR or TON",
                    ));
                }
            }

            parameters.price = Some(SuggestedPostPrice {
                currency: normalized_currency,
                amount: normalized_amount,
            });
        }

        if let Some(send_date) = parameters.send_date {
            let delta = send_date - now;
            if !(300..=2_678_400).contains(&delta) {
                return Err(ApiError::bad_request(
                    "suggested post send_date must be between 5 minutes and 30 days in the future",
                ));
            }
        }

        suggested_post_info = Some(SuggestedPostInfo {
            state: "pending".to_string(),
            price: parameters.price.clone(),
            send_date: parameters.send_date,
        });
    }

    let chat_key = sim_chat.chat_key.clone();
    let direct_messages_star_count = if is_direct_messages {
        direct_messages_star_count_for_chat(&mut conn, bot.id, &sim_chat)?
    } else {
        0
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, parsed_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    if is_direct_messages {
        if let Some(topic_id) = resolved_thread_id {
            let topic_owner_user_id = load_direct_messages_topic_record(&mut conn, bot.id, &chat_key, topic_id)?
                .map(|record| record.user_id)
                .unwrap_or(user.id);
            upsert_direct_messages_topic(
                &mut conn,
                bot.id,
                &chat_key,
                topic_id,
                topic_owner_user_id,
                Some(message_id),
                Some(now),
            )?;
        }
    }

    if let Some(info) = suggested_post_info.as_ref() {
        ensure_sim_suggested_posts_storage(&mut conn)?;
        upsert_suggested_post_state(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            "pending",
            info.send_date,
            None,
            now,
        )?;
    }

    let from = build_user_from_sim_record(&user, false);
    let chat = chat_from_sim_record(&sim_chat, &user);

    let message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
        "text": parsed_text,
    });

    let mut message_json = message_json;
    if let Some(sender_chat_value) = sender_chat_json {
        message_json["sender_chat"] = sender_chat_value;
    }
    if let Some(reply_id) = body.reply_to_message_id {
        let reply_value = load_reply_message_for_chat(&mut conn, &bot, &chat_key, reply_id)?;
        message_json["reply_to_message"] = reply_value;
        enrich_reply_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            reply_id,
            &mut message_json,
        )?;
    }
    if let Some(entities) = parsed_entities {
        message_json["entities"] = entities;
    }
    if let Some(thread_id) = resolved_thread_id {
        message_json["message_thread_id"] = Value::from(thread_id);
        message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(direct_messages_topic) = direct_messages_topic_json {
        message_json["direct_messages_topic"] = direct_messages_topic;
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if let Some(parameters) = suggested_post_parameters.as_ref() {
        message_json["suggested_post_parameters"] =
            serde_json::to_value(parameters).map_err(ApiError::internal)?;
    }
    if let Some(info) = suggested_post_info.as_ref() {
        message_json["suggested_post_info"] =
            serde_json::to_value(info).map_err(ApiError::internal)?;
    }
    if is_direct_messages && direct_messages_star_count > 0 {
        message_json["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if let Some(mode) = sim_parse_mode {
        message_json["sim_parse_mode"] = Value::String(mode);
    }
    if let Some(users_shared) = body.users_shared {
        message_json["users_shared"] = serde_json::to_value(users_shared).map_err(ApiError::internal)?;
    }
    if let Some(chat_shared) = body.chat_shared {
        message_json["chat_shared"] = serde_json::to_value(chat_shared).map_err(ApiError::internal)?;
    }
    if let Some(web_app_data) = body.web_app_data {
        message_json["web_app_data"] = serde_json::to_value(web_app_data).map_err(ApiError::internal)?;
    }
    if let Some(created) = managed_bot_created.as_ref() {
        message_json["managed_bot_created"] =
            serde_json::to_value(created).map_err(ApiError::internal)?;
    }

    let is_channel_post = sim_chat.chat_type == "channel";
    if !is_channel_post && !is_direct_messages {
        map_discussion_message_to_channel_post_if_needed(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            body.reply_to_message_id,
        )?;
        enrich_message_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            &mut message_json,
        )?;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;
    let is_business_message = business_connection_id.is_some();
    let mut bot_visible = if is_direct_messages || is_business_message {
        true
    } else {
        should_emit_user_generated_update_to_bot(
            &mut conn,
            &bot,
            &sim_chat.chat_type,
            user.id,
            &message,
        )?
    };
    if !bot_visible && !is_direct_messages && (sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup") {
        bot_visible = is_reply_to_linked_discussion_root_message(
            &mut conn,
            bot.id,
            &chat_key,
            body.reply_to_message_id,
        )?;
    }
    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post || is_business_message {
            None
        } else {
            Some(message.clone())
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(message.clone())
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: if is_business_message {
            Some(message.clone())
        } else {
            None
        },
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json, bot_visible) VALUES (?1, ?2, ?3)",
        params![
            bot.id,
            serde_json::to_string(&update_stub).map_err(ApiError::internal)?,
            if bot_visible { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if is_direct_messages && direct_messages_star_count > 0 {
        message_value["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if is_business_message {
        update_value["business_message"] = message_value.clone();
    } else if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    if bot_visible {
        dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);
    }

    if is_channel_post {
        forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    if let Some(managed_bot) = managed_bot_update {
        let managed_update = serde_json::to_value(Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
            managed_bot: Some(managed_bot),
        })
        .map_err(ApiError::internal)?;

        persist_and_dispatch_update(state, &mut conn, token, bot.id, managed_update)?;
    }

    Ok(message_value)
}

pub fn handle_sim_send_user_media(
    state: &Data<AppState>,
    token: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    let body: SimSendUserMediaRequest = parse_request(&params)?;

    let media_kind = body.media_kind.to_ascii_lowercase();
    if !["photo", "video", "audio", "voice", "document", "sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        return Err(ApiError::bad_request("unsupported media_kind"));
    }

    let (caption, caption_entities) = parse_optional_formatted_text(
        body.caption.as_deref(),
        body.parse_mode.as_deref(),
        None,
    );
    let caption_entities = if let Some(caption_text) = caption.as_deref() {
        merge_auto_message_entities(caption_text, caption_entities)
    } else {
        None
    };
    let sim_parse_mode = normalize_sim_parse_mode(body.parse_mode.as_deref());

    let media_input = if ["sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        parse_input_file_value(&body.media, &media_kind)?
    } else {
        body.media.clone()
    };

    let file = resolve_media_file(state, token, &media_input, &media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sticker_meta = if media_kind == "sticker" {
        load_sticker_meta(&mut conn, bot.id, &file.file_id)?
    } else {
        None
    };

    let media_value = match media_kind.as_str() {
        "photo" => serde_json::to_value(vec![PhotoSize {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            file_size: file.file_size,
        }])
        .map_err(ApiError::internal)?,
        "video" => serde_json::to_value(Video {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            duration: 0,
            thumbnail: None,
            cover: None,
            start_timestamp: None,
            qualities: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "audio" => serde_json::to_value(Audio {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            performer: None,
            title: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
            thumbnail: None,
        })
        .map_err(ApiError::internal)?,
        "voice" => serde_json::to_value(Voice {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "document" => serde_json::to_value(Document {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("document.bin").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "animation" => serde_json::to_value(Animation {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 512,
            height: 512,
            duration: 0,
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("animation.mp4").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "video_note" => serde_json::to_value(VideoNote {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            length: 384,
            duration: 0,
            thumbnail: None,
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "sticker" => {
            let format = sticker_meta
                .as_ref()
                .map(|m| m.format.as_str())
                .or_else(|| infer_sticker_format_from_file(&file))
                .unwrap_or("static");
            let is_animated = format == "animated";
            let is_video = format == "video";

            serde_json::to_value(Sticker {
                file_id: file.file_id.clone(),
                file_unique_id: file.file_unique_id.clone(),
                r#type: sticker_meta
                    .as_ref()
                    .map(|m| m.sticker_type.clone())
                    .unwrap_or_else(|| "regular".to_string()),
                width: 512,
                height: 512,
                is_animated,
                is_video,
                thumbnail: None,
                emoji: sticker_meta.as_ref().and_then(|m| m.emoji.clone()),
                set_name: sticker_meta.as_ref().and_then(|m| m.set_name.clone()),
                premium_animation: None,
                mask_position: sticker_meta
                    .as_ref()
                    .and_then(|m| m.mask_position_json.as_ref())
                    .and_then(|raw| serde_json::from_str::<MaskPosition>(raw).ok()),
                custom_emoji_id: sticker_meta.as_ref().and_then(|m| m.custom_emoji_id.clone()),
                needs_repainting: sticker_meta.as_ref().map(|m| m.needs_repainting),
                file_size: file.file_size,
            })
            .map_err(ApiError::internal)?
        }
        _ => return Err(ApiError::bad_request("unsupported media_kind")),
    };

    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let sim_chat = resolve_sim_chat_for_user_message(&mut conn, bot.id, chat_id, &user)?;
    let send_kind = send_kind_from_sim_user_media_kind(media_kind.as_str());
    let is_direct_messages = is_direct_messages_chat(&sim_chat);
    if !is_direct_messages && sim_chat.chat_type != "private" {
        ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &sim_chat.chat_key,
            user.id,
            send_kind,
        )?;
    }

    let mut sender_chat_json: Option<Value> = None;
    let resolved_thread_id: Option<i64>;
    let mut direct_messages_topic_json: Option<Value> = None;

    if is_direct_messages {
        if body.message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not supported in channel direct messages simulation",
            ));
        }
        if body.sender_chat_id.is_some() {
            return Err(ApiError::bad_request(
                "sender_chat_id is not supported in channel direct messages simulation",
            ));
        }
        let (topic_id, topic_value, forced_sender_chat) = resolve_direct_messages_topic_for_sender(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.direct_messages_topic_id,
        )?;
        resolved_thread_id = Some(topic_id);
        direct_messages_topic_json = Some(topic_value);
        if let Some(forced_sender_chat) = forced_sender_chat {
            sender_chat_json = Some(serde_json::to_value(forced_sender_chat).map_err(ApiError::internal)?);
        }
    } else {
        if body.direct_messages_topic_id.is_some() {
            return Err(ApiError::bad_request(
                "direct_messages_topic_id is available only in channel direct messages chats",
            ));
        }
        let sender_chat = resolve_sender_chat_for_sim_user_message(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.sender_chat_id,
            send_kind,
        )?;
        sender_chat_json = sender_chat
            .as_ref()
            .map(|chat| serde_json::to_value(chat).map_err(ApiError::internal))
            .transpose()?;
        resolved_thread_id = resolve_forum_message_thread_id(
            &mut conn,
            bot.id,
            &sim_chat,
            body.message_thread_id,
        )?;
    }

    let business_connection_record = normalize_business_connection_id(body.business_connection_id.as_deref())
        .map(|connection_id| load_business_connection_or_404(&mut conn, bot.id, &connection_id))
        .transpose()?;
    let business_connection_id = if let Some(record) = business_connection_record.as_ref() {
        if is_direct_messages {
            return Err(ApiError::bad_request(
                "business_connection_id is not supported in channel direct messages simulation",
            ));
        }
        if !record.is_enabled {
            return Err(ApiError::bad_request("business connection is disabled"));
        }
        if sim_chat.chat_type != "private" || sim_chat.chat_id != record.user_chat_id || user.id != record.user_id {
            return Err(ApiError::bad_request("business connection does not match chat/user"));
        }
        Some(record.connection_id.clone())
    } else {
        None
    };

    let chat_key = sim_chat.chat_key.clone();
    let direct_messages_star_count = if is_direct_messages {
        direct_messages_star_count_for_chat(&mut conn, bot.id, &sim_chat)?
    } else {
        0
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    if is_direct_messages {
        if let Some(topic_id) = resolved_thread_id {
            let topic_owner_user_id = load_direct_messages_topic_record(&mut conn, bot.id, &chat_key, topic_id)?
                .map(|record| record.user_id)
                .unwrap_or(user.id);
            upsert_direct_messages_topic(
                &mut conn,
                bot.id,
                &chat_key,
                topic_id,
                topic_owner_user_id,
                Some(message_id),
                Some(now),
            )?;
        }
    }

    let from = build_user_from_sim_record(&user, false);
    let chat = chat_from_sim_record(&sim_chat, &user);

    let mut message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
    });

    if let Some(sender_chat_value) = sender_chat_json {
        message_json["sender_chat"] = sender_chat_value;
    }

    message_json[media_kind.as_str()] = media_value;
    if let Some(c) = caption {
        message_json["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        message_json["caption_entities"] = entities;
    }
    if let Some(thread_id) = resolved_thread_id {
        message_json["message_thread_id"] = Value::from(thread_id);
        message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(direct_messages_topic) = direct_messages_topic_json {
        message_json["direct_messages_topic"] = direct_messages_topic;
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if is_direct_messages && direct_messages_star_count > 0 {
        message_json["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if let Some(mode) = sim_parse_mode {
        message_json["sim_parse_mode"] = Value::String(mode);
    }
    if let Some(reply_id) = body.reply_to_message_id {
        let reply_value = load_reply_message_for_chat(&mut conn, &bot, &chat_key, reply_id)?;
        message_json["reply_to_message"] = reply_value;
        enrich_reply_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            reply_id,
            &mut message_json,
        )?;
    }

    let is_channel_post = sim_chat.chat_type == "channel";
    if !is_channel_post && !is_direct_messages {
        map_discussion_message_to_channel_post_if_needed(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            body.reply_to_message_id,
        )?;
        enrich_message_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            &mut message_json,
        )?;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;
    let is_business_message = business_connection_id.is_some();
    let mut bot_visible = if is_direct_messages || is_business_message {
        true
    } else {
        should_emit_user_generated_update_to_bot(
            &mut conn,
            &bot,
            &sim_chat.chat_type,
            user.id,
            &message,
        )?
    };
    if !bot_visible && !is_direct_messages && (sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup") {
        bot_visible = is_reply_to_linked_discussion_root_message(
            &mut conn,
            bot.id,
            &chat_key,
            body.reply_to_message_id,
        )?;
    }
    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post || is_business_message {
            None
        } else {
            Some(message.clone())
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(message.clone())
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: if is_business_message {
            Some(message.clone())
        } else {
            None
        },
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json, bot_visible) VALUES (?1, ?2, ?3)",
        params![
            bot.id,
            serde_json::to_string(&update_stub).map_err(ApiError::internal)?,
            if bot_visible { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if is_direct_messages && direct_messages_star_count > 0 {
        message_value["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if is_business_message {
        update_value["business_message"] = message_value.clone();
    } else if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    if bot_visible {
        dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);
    }

    if is_channel_post {
        forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

pub fn handle_sim_edit_user_message_media(
    state: &Data<AppState>,
    token: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    let body: SimEditUserMediaRequest = parse_request(&params)?;
    let sim_parse_mode = normalize_sim_parse_mode(body.parse_mode.as_deref());

    let media_kind = body.media_kind.to_ascii_lowercase();
    if !["photo", "video", "audio", "voice", "document", "sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        return Err(ApiError::bad_request("unsupported media_kind"));
    }

    let caption_text = body.caption.as_ref().and_then(value_to_optional_string);
    let (caption, caption_entities) = parse_optional_formatted_text(
        caption_text.as_deref(),
        body.parse_mode.as_deref(),
        None,
    );
    let caption_entities = if let Some(caption_text) = caption.as_deref() {
        merge_auto_message_entities(caption_text, caption_entities)
    } else {
        None
    };

    let media_input = if ["sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        parse_input_file_value(&body.media, &media_kind)?
    } else {
        body.media.clone()
    };
    let file = resolve_media_file(state, token, &media_input, &media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sticker_meta = if media_kind == "sticker" {
        load_sticker_meta(&mut conn, bot.id, &file.file_id)?
    } else {
        None
    };

    let media_value = match media_kind.as_str() {
        "photo" => serde_json::to_value(vec![PhotoSize {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            file_size: file.file_size,
        }])
        .map_err(ApiError::internal)?,
        "video" => serde_json::to_value(Video {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            duration: 0,
            thumbnail: None,
            cover: None,
            start_timestamp: None,
            qualities: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "audio" => serde_json::to_value(Audio {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            performer: None,
            title: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
            thumbnail: None,
        })
        .map_err(ApiError::internal)?,
        "voice" => serde_json::to_value(Voice {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "document" => serde_json::to_value(Document {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("document.bin").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "animation" => serde_json::to_value(Animation {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 512,
            height: 512,
            duration: 0,
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("animation.mp4").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "video_note" => serde_json::to_value(VideoNote {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            length: 384,
            duration: 0,
            thumbnail: None,
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "sticker" => {
            let format = sticker_meta
                .as_ref()
                .map(|m| m.format.as_str())
                .or_else(|| infer_sticker_format_from_file(&file))
                .unwrap_or("static");
            let is_animated = format == "animated";
            let is_video = format == "video";

            serde_json::to_value(Sticker {
                file_id: file.file_id.clone(),
                file_unique_id: file.file_unique_id.clone(),
                r#type: sticker_meta
                    .as_ref()
                    .map(|m| m.sticker_type.clone())
                    .unwrap_or_else(|| "regular".to_string()),
                width: 512,
                height: 512,
                is_animated,
                is_video,
                thumbnail: None,
                emoji: sticker_meta.as_ref().and_then(|m| m.emoji.clone()),
                set_name: sticker_meta.as_ref().and_then(|m| m.set_name.clone()),
                premium_animation: None,
                mask_position: sticker_meta
                    .as_ref()
                    .and_then(|m| m.mask_position_json.as_ref())
                    .and_then(|raw| serde_json::from_str::<MaskPosition>(raw).ok()),
                custom_emoji_id: sticker_meta.as_ref().and_then(|m| m.custom_emoji_id.clone()),
                needs_repainting: sticker_meta.as_ref().map(|m| m.needs_repainting),
                file_size: file.file_size,
            })
            .map_err(ApiError::internal)?
        }
        _ => return Err(ApiError::bad_request("unsupported media_kind")),
    };

    let chat_key = body.chat_id.to_string();

    let actor_user_id = current_request_actor_user_id()
        .ok_or_else(|| ApiError::bad_request("actor user is required for user media edit"))?;

    let owner_user_id: Option<i64> = conn
        .query_row(
            "SELECT from_user_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(owner_user_id) = owner_user_id else {
        return Err(ApiError::not_found("message to edit was not found"));
    };

    if owner_user_id != actor_user_id {
        return Err(ApiError::bad_request("message can't be edited"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, body.message_id)?;

    for key in ["photo", "video", "audio", "voice", "document", "animation", "video_note", "sticker"] {
        edited_message.as_object_mut().map(|obj| obj.remove(key));
    }

    edited_message[media_kind.as_str()] = media_value;
    if let Some(c) = caption {
        edited_message["caption"] = Value::String(c.clone());
        conn.execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![c, bot.id, chat_key, body.message_id],
        )
        .map_err(ApiError::internal)?;
    }
    if let Some(entities) = caption_entities {
        edited_message["caption_entities"] = entities;
    } else {
        edited_message.as_object_mut().map(|obj| obj.remove("caption_entities"));
    }
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));
    let is_channel_post = edited_message
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(Value::as_str)
        == Some("channel");

    let update_stub = Update {
        update_id: 0,
        message: None,
        edited_message: if is_channel_post {
            None
        } else {
            serde_json::from_value(edited_message.clone()).ok()
        },
        channel_post: None,
        edited_channel_post: if is_channel_post {
            serde_json::from_value(edited_message.clone()).ok()
        } else {
            None
        },
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
        update_value["edited_channel_post"] = edited_message.clone();
    } else {
        update_value["edited_message"] = edited_message.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("edited_channel_post").cloned() {
            edited_message = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);

    Ok(edited_message)
}

pub fn handle_sim_create_bot(state: &Data<AppState>, body: SimCreateBotRequest) -> ApiResult {
    let conn = lock_db(state)?;

    let token = generate_telegram_token();
    let now = Utc::now().timestamp();
    let suffix = token_suffix(&token);

    let first_name = body
        .first_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("LaraGram Bot {}", &suffix[..4]));

    let username = body
        .username
        .map(|v| sanitize_username(&v))
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("laragram_{}", suffix));

    conn.execute(
        "INSERT INTO bots (token, username, first_name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![token, username, first_name, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": conn.last_insert_rowid(),
        "token": token,
        "username": username,
        "first_name": first_name
    }))
}

pub fn handle_sim_update_bot(
    state: &Data<AppState>,
    token: &str,
    body: SimUpdateBotRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let first_name = body
        .first_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or(bot.first_name);

    let username = body
        .username
        .map(|v| sanitize_username(&v))
        .filter(|v| !v.is_empty())
        .unwrap_or(bot.username);

    conn.execute(
        "UPDATE bots SET first_name = ?1, username = ?2 WHERE id = ?3",
        params![first_name, username, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": bot.id,
        "token": token,
        "username": username,
        "first_name": first_name
    }))
}

pub fn handle_sim_upsert_user(state: &Data<AppState>, body: SimUpsertUserRequest) -> ApiResult {
    let conn = lock_db(state)?;

    struct ExistingSimUserProfile {
        first_name: String,
        username: Option<String>,
        last_name: Option<String>,
        phone_number: Option<String>,
        photo_url: Option<String>,
        bio: Option<String>,
        is_premium: bool,
        business_name: Option<String>,
        business_intro: Option<String>,
        business_location: Option<String>,
        gift_count: i64,
    }

    let normalize_optional_text = |input: Option<String>| {
        input
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };

    let id = body
        .id
        .unwrap_or_else(|| (Utc::now().timestamp_millis() % 9_000_000) + 10_000);

    let existing = conn
        .query_row(
            "SELECT first_name, username, last_name, phone_number, photo_url, bio, is_premium,
                    business_name, business_intro, business_location, gift_count
             FROM users
             WHERE id = ?1",
            params![id],
            |row| {
                Ok(ExistingSimUserProfile {
                    first_name: row.get(0)?,
                    username: row.get(1)?,
                    last_name: row.get(2)?,
                    phone_number: row.get(3)?,
                    photo_url: row.get(4)?,
                    bio: row.get(5)?,
                    is_premium: row.get::<_, i64>(6)? == 1,
                    business_name: row.get(7)?,
                    business_intro: row.get(8)?,
                    business_location: row.get(9)?,
                    gift_count: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let first_name = normalize_optional_text(body.first_name)
        .or_else(|| existing.as_ref().map(|profile| profile.first_name.clone()))
        .unwrap_or_else(|| format!("User {}", id));
    let username = body
        .username
        .as_deref()
        .map(sanitize_username)
        .filter(|value| !value.is_empty())
        .or_else(|| existing.as_ref().and_then(|profile| profile.username.clone()))
        .unwrap_or_else(|| format!("user_{}", id));
    let last_name = normalize_optional_text(body.last_name)
        .or_else(|| existing.as_ref().and_then(|profile| profile.last_name.clone()));
    let phone_number = normalize_optional_text(body.phone_number)
        .or_else(|| existing.as_ref().and_then(|profile| profile.phone_number.clone()));
    let photo_url = normalize_optional_text(body.photo_url)
        .or_else(|| existing.as_ref().and_then(|profile| profile.photo_url.clone()));
    let bio = normalize_optional_text(body.bio)
        .or_else(|| existing.as_ref().and_then(|profile| profile.bio.clone()));
    let business_name = normalize_optional_text(body.business_name)
        .or_else(|| existing.as_ref().and_then(|profile| profile.business_name.clone()));
    let business_intro = normalize_optional_text(body.business_intro)
        .or_else(|| existing.as_ref().and_then(|profile| profile.business_intro.clone()));
    let business_location = normalize_optional_text(body.business_location)
        .or_else(|| existing.as_ref().and_then(|profile| profile.business_location.clone()));
    let is_premium = body
        .is_premium
        .or_else(|| existing.as_ref().map(|profile| profile.is_premium))
        .unwrap_or(false);
    let gift_count = body
        .gift_count
        .or_else(|| existing.as_ref().map(|profile| profile.gift_count))
        .unwrap_or(0)
        .max(0);

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO users
         (id, username, first_name, last_name, phone_number, photo_url, bio, is_premium,
          business_name, business_intro, business_location, gift_count, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            phone_number = excluded.phone_number,
            photo_url = excluded.photo_url,
            bio = excluded.bio,
            is_premium = excluded.is_premium,
            business_name = excluded.business_name,
            business_intro = excluded.business_intro,
            business_location = excluded.business_location,
            gift_count = excluded.gift_count",
        params![
            id,
            username,
            first_name,
            last_name,
            phone_number,
            photo_url,
            bio,
            if is_premium { 1 } else { 0 },
            business_name,
            business_intro,
            business_location,
            gift_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "phone_number": phone_number,
        "photo_url": photo_url,
        "bio": bio,
        "is_premium": is_premium,
        "business_name": business_name,
        "business_intro": business_intro,
        "business_location": business_location,
        "gift_count": gift_count
    }))
}

pub fn handle_sim_delete_user(state: &Data<AppState>, body: SimDeleteUserRequest) -> ApiResult {
    if body.id <= 0 {
        return Err(ApiError::bad_request("user id is invalid"));
    }

    let conn = lock_db(state)?;
    let user_exists: Option<i64> = conn
        .query_row(
            "SELECT id FROM users WHERE id = ?1",
            params![body.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if user_exists.is_none() {
        return Err(ApiError::not_found("user not found"));
    }

    let user_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .map_err(ApiError::internal)?;
    if user_count <= 1 {
        return Err(ApiError::bad_request("at least one user must remain in simulator"));
    }

    conn.execute(
        "DELETE FROM sim_business_read_messages
         WHERE connection_id IN (
             SELECT connection_id
             FROM sim_business_connections
             WHERE user_id = ?1
         )",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM sim_business_connections WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_business_account_profiles WHERE user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM sim_chat_members WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_chat_join_requests WHERE user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_direct_message_topics WHERE user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM poll_votes WHERE voter_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM game_scores WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM star_transactions_ledger WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM star_subscriptions WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM callback_queries WHERE from_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM inline_queries WHERE from_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM shipping_queries WHERE from_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM pre_checkout_queries WHERE from_user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM users WHERE id = ?1", params![body.id])
        .map_err(ApiError::internal)?;

    Ok(json!({ "deleted": true, "id": body.id }))
}

pub fn handle_sim_set_user_profile_audio(
    state: &Data<AppState>,
    token: &str,
    body: SimSetUserProfileAudioRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let normalize_optional_text = |value: Option<String>| {
        value
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_sim_user_record(&mut conn, body.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let title = normalize_optional_text(body.title).unwrap_or_else(|| "Profile audio".to_string());
    let performer = normalize_optional_text(body.performer).or_else(|| Some(user.first_name.clone()));
    let file_name = normalize_optional_text(body.file_name).unwrap_or_else(|| "profile-audio.ogg".to_string());
    let mime_type = normalize_optional_text(body.mime_type).unwrap_or_else(|| "audio/ogg".to_string());
    let file_size = body.file_size.filter(|value| *value > 0);
    let duration = body.duration.unwrap_or(30).clamp(1, 3600);

    conn.execute(
        "UPDATE sim_user_profile_audios
         SET position = position + 1
         WHERE bot_id = ?1 AND user_id = ?2",
        params![bot.id, body.user_id],
    )
    .map_err(ApiError::internal)?;

    let file_id = generate_telegram_file_id("profile_audio");
    let file_unique_id = generate_telegram_file_unique_id();
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_user_profile_audios
         (bot_id, user_id, file_id, file_unique_id, duration, performer, title, file_name, mime_type, file_size, position, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11)",
        params![
            bot.id,
            body.user_id,
            &file_id,
            &file_unique_id,
            duration,
            performer.clone(),
            title.clone(),
            file_name.clone(),
            mime_type.clone(),
            file_size,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "user_id": body.user_id,
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "title": title,
        "file_name": file_name,
        "mime_type": mime_type,
        "file_size": file_size,
        "duration": duration,
    }))
}

pub fn handle_sim_upload_user_profile_audio(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SimUploadUserProfileAudioRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let stored = resolve_media_file(state, token, &request.audio, "audio")?;

    let normalize_optional_text = |value: Option<String>| {
        value
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let title = normalize_optional_text(request.title).unwrap_or_else(|| {
        request
            .file_name
            .as_deref()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| {
                if let Some((base, _)) = name.rsplit_once('.') {
                    base.trim().to_string()
                } else {
                    name.to_string()
                }
            })
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "Profile audio".to_string())
    });

    let performer = normalize_optional_text(request.performer).or_else(|| Some(user.first_name.clone()));
    let file_name = normalize_optional_text(request.file_name)
        .or_else(|| {
            stored
                .file_path
                .rsplit('/')
                .next()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "profile-audio.ogg".to_string());
    let mime_type = normalize_optional_text(request.mime_type)
        .or_else(|| stored.mime_type.clone())
        .unwrap_or_else(|| "audio/ogg".to_string());
    let duration = request.duration.unwrap_or(30).clamp(1, 3600);
    let now = Utc::now().timestamp();

    conn.execute(
        "UPDATE sim_user_profile_audios
         SET position = position + 1
         WHERE bot_id = ?1 AND user_id = ?2",
        params![bot.id, request.user_id],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_user_profile_audios
         (bot_id, user_id, file_id, file_unique_id, duration, performer, title, file_name, mime_type, file_size, position, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11)",
        params![
            bot.id,
            request.user_id,
            &stored.file_id,
            &stored.file_unique_id,
            duration,
            performer.clone(),
            &title,
            &file_name,
            &mime_type,
            stored.file_size,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "user_id": request.user_id,
        "file_id": stored.file_id,
        "file_unique_id": stored.file_unique_id,
        "file_path": stored.file_path,
        "title": title,
        "performer": performer,
        "file_name": file_name,
        "mime_type": mime_type,
        "file_size": stored.file_size,
        "duration": duration,
    }))
}

pub fn handle_sim_delete_user_profile_audio(
    state: &Data<AppState>,
    token: &str,
    body: SimDeleteUserProfileAudioRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let file_id = body.file_id.trim();
    if file_id.is_empty() {
        return Err(ApiError::bad_request("file_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_record(&mut conn, body.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let deleted = conn
        .execute(
            "DELETE FROM sim_user_profile_audios WHERE bot_id = ?1 AND user_id = ?2 AND file_id = ?3",
            params![bot.id, body.user_id, file_id],
        )
        .map_err(ApiError::internal)?;

    if deleted == 0 {
        return Err(ApiError::not_found("profile audio not found"));
    }

    Ok(json!({
        "deleted": true,
        "user_id": body.user_id,
        "file_id": file_id,
    }))
}

pub fn handle_sim_add_user_chat_boosts(
    state: &Data<AppState>,
    token: &str,
    body: SimAddUserChatBoostsRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let count = body.count.unwrap_or(1);
    if count <= 0 || count > 100 {
        return Err(ApiError::bad_request("count must be between 1 and 100"));
    }

    let duration_days = body.duration_days.unwrap_or(30);
    if duration_days <= 0 || duration_days > 3650 {
        return Err(ApiError::bad_request("duration_days must be between 1 and 3650"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_chat_boosts_storage(&mut conn)?;

    let chat_id_value = Value::from(body.chat_id);
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &chat_id_value)?;
    ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, body.user_id)?;

    let user = ensure_sim_user_record(&mut conn, body.user_id)?;
    if !user.is_premium {
        return Err(ApiError::bad_request("only premium users can boost chats"));
    }

    let source_json = serde_json::to_string(&json!({
        "source": "premium",
        "user": build_user_from_sim_record(&user, false),
    }))
    .map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();
    let mut added_boost_ids = Vec::<String>::with_capacity(count as usize);
    for index in 0..count {
        let boost_id = generate_telegram_numeric_id();
        let add_date = now - (index * 60);
        let expiration_date = add_date + (duration_days * 24 * 60 * 60);

        conn.execute(
            "INSERT INTO sim_user_chat_boosts
             (bot_id, chat_key, user_id, boost_id, add_date, expiration_date, source_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                bot.id,
                &chat_key,
                body.user_id,
                &boost_id,
                add_date,
                expiration_date,
                &source_json,
                now,
            ],
        )
        .map_err(ApiError::internal)?;

        added_boost_ids.push(boost_id);
    }

    Ok(json!({
        "added_count": added_boost_ids.len(),
        "boost_ids": added_boost_ids,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_remove_user_chat_boosts(
    state: &Data<AppState>,
    token: &str,
    body: SimRemoveUserChatBoostsRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_chat_boosts_storage(&mut conn)?;

    let chat_id_value = Value::from(body.chat_id);
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &chat_id_value)?;
    ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, body.user_id)?;

    ensure_sim_user_record(&mut conn, body.user_id)?;

    let mut stmt = conn
        .prepare(
            "SELECT boost_id
             FROM sim_user_chat_boosts
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3
             ORDER BY expiration_date DESC, add_date DESC, boost_id ASC",
        )
        .map_err(ApiError::internal)?;

    let existing_rows = stmt
        .query_map(params![bot.id, &chat_key, body.user_id], |row| row.get::<_, String>(0))
        .map_err(ApiError::internal)?;
    let existing_boost_ids = existing_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;

    if existing_boost_ids.is_empty() {
        return Ok(json!({
            "removed_count": 0,
            "boost_ids": Vec::<String>::new(),
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }

    let target_ids = if body.remove_all.unwrap_or(false) {
        existing_boost_ids.clone()
    } else if let Some(boost_ids) = body.boost_ids {
        let wanted = boost_ids
            .into_iter()
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect::<HashSet<_>>();

        existing_boost_ids
            .iter()
            .filter(|boost_id| wanted.contains(*boost_id))
            .cloned()
            .collect::<Vec<_>>()
    } else {
        vec![existing_boost_ids[0].clone()]
    };

    for boost_id in &target_ids {
        conn.execute(
            "DELETE FROM sim_user_chat_boosts WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3 AND boost_id = ?4",
            params![bot.id, &chat_key, body.user_id, boost_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!({
        "removed_count": target_ids.len(),
        "boost_ids": target_ids,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_create_group(
    state: &Data<AppState>,
    token: &str,
    body: SimCreateGroupRequest,
) -> ApiResult {
    let title = body.title.trim().to_string();
    if title.is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }

    let chat_type = body
        .chat_type
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "supergroup".to_string());
    if !matches!(chat_type.as_str(), "group" | "supergroup" | "channel") {
        return Err(ApiError::bad_request("chat_type must be group, supergroup, or channel"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let owner_record = ensure_user(
        &mut conn,
        body.owner_user_id,
        body.owner_first_name,
        body.owner_username,
    )?;

    let now = Utc::now().timestamp();
    let mut chat_id = if chat_type == "supergroup" || chat_type == "channel" {
        // Keep ids in -100xxxxxxxxxx range for Telegram supergroup/channel parity.
        -((Utc::now().timestamp_millis().abs() % 10_000_000_000) + 1_000_000_000_000)
    } else {
        -((Utc::now().timestamp_millis().abs() % 900_000_000_000) + 100_000)
    };
    loop {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT chat_id FROM sim_chats WHERE bot_id = ?1 AND chat_id = ?2",
                params![bot.id, chat_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            break;
        }
        chat_id -= 1;
    }

    let chat_key = chat_id.to_string();
    let description = body
        .description
        .as_deref()
        .map(str::trim)
        .map(str::to_string)
        .filter(|v| !v.is_empty());
    let username = body
        .username
        .as_deref()
        .map(sanitize_username)
        .filter(|v| !v.is_empty());
    let is_forum = if chat_type == "supergroup" {
        body.is_forum.unwrap_or(false)
    } else {
        false
    };
    let channel_show_author_signature = if chat_type == "channel" {
        body.show_author_signature.unwrap_or(false)
    } else {
        false
    };
    let message_history_visible = body.message_history_visible.unwrap_or(true);
    let slow_mode_delay = body.slow_mode_delay.unwrap_or(0).max(0);
    let permissions = default_group_permissions();
    let permissions_json = serde_json::to_string(&permissions).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO chats (chat_key, chat_type, title)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(chat_key)
         DO UPDATE SET chat_type = excluded.chat_type, title = excluded.title",
        params![chat_key, chat_type, title],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_chats
         (bot_id, chat_key, chat_id, chat_type, title, username, description, photo_file_id, is_forum, channel_show_author_signature, message_history_visible, slow_mode_delay, permissions_json, owner_user_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?14)",
        params![
            bot.id,
            chat_key,
            chat_id,
            chat_type,
            title,
            username,
            description,
            if is_forum { 1 } else { 0 },
            if channel_show_author_signature { 1 } else { 0 },
            if message_history_visible { 1 } else { 0 },
            slow_mode_delay,
            permissions_json,
            owner_record.id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
         VALUES (?1, ?2, ?3, 'owner', 'owner', ?4, ?4)",
        params![bot.id, chat_key, owner_record.id, now],
    )
    .map_err(ApiError::internal)?;

    let bot_admin_rights_json = if chat_type == "channel" {
        Some(serde_json::to_string(&channel_admin_rights_full_access()).map_err(ApiError::internal)?)
    } else {
        None
    };

    conn.execute(
        "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, admin_rights_json, joined_at, updated_at)
         VALUES (?1, ?2, ?3, 'admin', 'admin', ?4, ?5, ?5)",
        params![bot.id, chat_key, bot.id, bot_admin_rights_json, now],
    )
    .map_err(ApiError::internal)?;

    let mut member_users = Vec::<User>::new();
    let owner_user = build_user_from_sim_record(&owner_record, false);
    member_users.push(owner_user.clone());

    if let Some(member_ids) = body.initial_member_ids {
        for member_id in member_ids {
            if member_id == owner_record.id || member_id == bot.id {
                continue;
            }
            let member_record = ensure_user(
                &mut conn,
                Some(member_id),
                Some(format!("User {}", member_id)),
                None,
            )?;
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'member', 'member', ?4, ?4)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'member', role = 'member', joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
                params![bot.id, chat_key, member_record.id, now],
            )
            .map_err(ApiError::internal)?;
            member_users.push(build_user_from_sim_record(&member_record, false));
        }
    }

    let chat = Chat {
        id: chat_id,
        r#type: chat_type.clone(),
        title: conn
            .query_row(
                "SELECT title FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
                params![bot.id, chat_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten(),
        username: conn
            .query_row(
                "SELECT username FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
                params![bot.id, chat_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten(),
        first_name: None,
        last_name: None,
        is_forum: if is_forum { Some(true) } else { None },
        is_direct_messages: None,
    };

    let bot_user = build_bot_user(&bot);
    let old_left_bot = to_chat_member(ChatMemberLeft {
        status: "left".to_string(),
        user: bot_user.clone(),
    })?;
    let new_admin_bot = to_chat_member(ChatMemberAdministrator {
        status: "administrator".to_string(),
        user: bot_user,
        can_be_edited: false,
        is_anonymous: false,
        can_manage_chat: true,
        can_delete_messages: true,
        can_manage_video_chats: true,
        can_restrict_members: true,
        can_promote_members: false,
        can_change_info: true,
        can_invite_users: true,
        can_post_stories: true,
        can_edit_stories: true,
        can_delete_stories: true,
        can_post_messages: if chat_type == "channel" { Some(true) } else { None },
        can_edit_messages: if chat_type == "channel" { Some(true) } else { None },
        can_pin_messages: if chat_type == "channel" { None } else { Some(true) },
        can_manage_topics: if chat_type == "supergroup" { Some(is_forum) } else { None },
        can_manage_direct_messages: None,
        can_manage_tags: None,
        custom_title: None,
    })?;

    let my_chat_member_update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: owner_user.clone(),
            date: now,
            old_chat_member: old_left_bot,
            new_chat_member: new_admin_bot,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(my_chat_member_update).map_err(ApiError::internal)?,
    )?;

    let old_left_owner = to_chat_member(ChatMemberLeft {
        status: "left".to_string(),
        user: owner_user.clone(),
    })?;
    let new_owner = to_chat_member(ChatMemberOwner {
        status: "creator".to_string(),
        user: owner_user.clone(),
        is_anonymous: false,
        custom_title: None,
    })?;

    let chat_member_update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: owner_user.clone(),
            date: now,
            old_chat_member: old_left_owner,
            new_chat_member: new_owner,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(chat_member_update).map_err(ApiError::internal)?,
    )?;

    let mut service_fields = Map::<String, Value>::new();
    if chat.r#type == "channel" {
        service_fields.insert("channel_chat_created".to_string(), Value::Bool(true));
    } else if chat.r#type == "supergroup" {
        service_fields.insert("supergroup_chat_created".to_string(), Value::Bool(true));
    } else {
        service_fields.insert("group_chat_created".to_string(), Value::Bool(true));
    }

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &owner_user,
        now,
        service_text_chat_created(&owner_user, &chat.r#type),
        service_fields,
    )?;

    let response = SimCreateGroupResponse {
        chat,
        owner: owner_user,
        members: member_users,
        settings: SimGroupSettingsResponse {
            show_author_signature: channel_show_author_signature,
            paid_star_reactions_enabled: false,
            message_history_visible,
            slow_mode_delay,
            permissions,
        },
    };

    serde_json::to_value(response).map_err(ApiError::internal)
}

pub fn handle_sim_update_group(
    state: &Data<AppState>,
    token: &str,
    body: SimUpdateGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let existing: Option<(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        i64,
        i64,
        i64,
        Option<i64>,
        i64,
        i64,
        i64,
        i64,
        Option<String>,
    )> = conn
        .query_row(
            "SELECT chat_type, title, username, description, is_forum, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id, direct_messages_enabled, direct_messages_star_count, message_history_visible, slow_mode_delay, permissions_json
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((
        chat_type,
        current_title,
        current_username,
        current_description,
        current_is_forum,
        current_show_author_signature,
        current_paid_star_reactions_enabled,
        current_linked_discussion_chat_id,
        current_direct_messages_enabled,
        current_direct_messages_star_count,
        current_message_history_visible,
        current_slow_mode_delay,
        current_permissions_json,
    )) = existing else {
        return Err(ApiError::not_found("group not found"));
    };
    if chat_type == "private" {
        return Err(ApiError::bad_request("cannot edit private chat with group endpoint"));
    }

    let current_permissions = current_permissions_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
        .unwrap_or_else(default_group_permissions);

    let actor_id = body.user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = ensure_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, actor.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let can_change_by_status = matches!(actor_status.as_deref(), Some("owner") | Some("admin"));
    let can_change_by_permission = matches!(actor_status.as_deref(), Some("member"))
        && current_permissions.can_change_info.unwrap_or(false);
    if !(can_change_by_status || can_change_by_permission) {
        return Err(ApiError::bad_request(
            "only owner/admin or members with can_change_info can edit group",
        ));
    }

    let title = body
        .title
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| current_title.clone())
        .unwrap_or_else(|| format!("Group {}", body.chat_id));
    let title_changed = current_title.as_deref() != Some(title.as_str());

    let username = match body.username {
        Some(value) => {
            let normalized = sanitize_username(&value);
            if normalized.is_empty() {
                None
            } else {
                Some(normalized)
            }
        }
        None => current_username.clone(),
    };

    let description = match body.description {
        Some(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }
        None => current_description.clone(),
    };

    let is_forum = if chat_type == "supergroup" {
        body.is_forum.unwrap_or(current_is_forum == 1)
    } else {
        false
    };
    let show_author_signature = if chat_type == "channel" {
        body.show_author_signature
            .unwrap_or(current_show_author_signature == 1)
    } else {
        false
    };
    let paid_star_reactions_enabled = if chat_type == "channel" {
        body.paid_star_reactions_enabled
            .unwrap_or(current_paid_star_reactions_enabled == 1)
    } else {
        if body.paid_star_reactions_enabled.is_some() {
            return Err(ApiError::bad_request(
                "paid_star_reactions_enabled can only be set for channels",
            ));
        }
        false
    };

    let linked_discussion_chat_id = if chat_type == "channel" {
        if let Some(raw_linked_chat_id) = body.linked_chat_id {
            if raw_linked_chat_id == 0 {
                None
            } else {
                if raw_linked_chat_id == body.chat_id {
                    return Err(ApiError::bad_request("linked_chat_id must be different from channel chat_id"));
                }

                let linked_chat_key = raw_linked_chat_id.to_string();
                let linked_chat_type: Option<String> = conn
                    .query_row(
                        "SELECT chat_type
                         FROM sim_chats
                         WHERE bot_id = ?1 AND chat_key = ?2",
                        params![bot.id, &linked_chat_key],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ApiError::internal)?;

                match linked_chat_type.as_deref() {
                    Some("group") | Some("supergroup") => Some(raw_linked_chat_id),
                    Some("private") | Some("channel") => {
                        return Err(ApiError::bad_request("linked_chat_id must reference a group or supergroup"));
                    }
                    Some(_) => {
                        return Err(ApiError::bad_request("linked_chat_id has unsupported chat type"));
                    }
                    None => {
                        return Err(ApiError::not_found("linked chat not found"));
                    }
                }
            }
        } else {
            current_linked_discussion_chat_id
        }
    } else {
        if body.linked_chat_id.is_some() {
            return Err(ApiError::bad_request("linked_chat_id can only be set for channels"));
        }
        None
    };

    let direct_messages_enabled = if chat_type == "channel" {
        body.direct_messages_enabled
            .unwrap_or(current_direct_messages_enabled == 1)
    } else {
        if body.direct_messages_enabled.is_some() {
            return Err(ApiError::bad_request("direct_messages_enabled can only be set for channels"));
        }
        false
    };

    let direct_messages_star_count = if chat_type == "channel" {
        body.direct_messages_star_count
            .unwrap_or(current_direct_messages_star_count)
            .max(0)
    } else {
        if body.direct_messages_star_count.is_some() {
            return Err(ApiError::bad_request("direct_messages_star_count can only be set for channels"));
        }
        0
    };

    let message_history_visible = body
        .message_history_visible
        .unwrap_or(current_message_history_visible == 1);
    let slow_mode_delay = body
        .slow_mode_delay
        .unwrap_or(current_slow_mode_delay)
        .max(0);
    let permissions = body.permissions.unwrap_or_else(|| current_permissions.clone());
    let permissions_json = serde_json::to_string(&permissions).map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();

    if chat_type == "channel" {
        if let Some(linked_chat_id) = linked_discussion_chat_id {
            conn.execute(
                "UPDATE sim_chats
                 SET linked_discussion_chat_id = NULL,
                     updated_at = ?1
                 WHERE bot_id = ?2
                   AND chat_type = 'channel'
                   AND chat_key <> ?3
                   AND linked_discussion_chat_id = ?4",
                params![now, bot.id, &chat_key, linked_chat_id],
            )
            .map_err(ApiError::internal)?;
        }
    }

    conn.execute(
        "UPDATE chats SET title = ?1 WHERE chat_key = ?2",
        params![&title, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE sim_chats
         SET title = ?1,
             username = ?2,
             description = ?3,
             is_forum = ?4,
             channel_show_author_signature = ?5,
             channel_paid_reactions_enabled = ?6,
             linked_discussion_chat_id = ?7,
             direct_messages_enabled = ?8,
             direct_messages_star_count = ?9,
             message_history_visible = ?10,
             slow_mode_delay = ?11,
             permissions_json = ?12,
             updated_at = ?13
         WHERE bot_id = ?14 AND chat_key = ?15",
        params![
            title,
            username,
            description,
            if is_forum { 1 } else { 0 },
            if show_author_signature { 1 } else { 0 },
            if paid_star_reactions_enabled { 1 } else { 0 },
            linked_discussion_chat_id,
            if direct_messages_enabled { 1 } else { 0 },
            direct_messages_star_count,
            if message_history_visible { 1 } else { 0 },
            slow_mode_delay,
            permissions_json,
            now,
            bot.id,
            &chat_key,
        ],
    )
    .map_err(ApiError::internal)?;

    if title_changed {
        let chat = Chat {
            id: body.chat_id,
            r#type: chat_type.clone(),
            title: Some(title.clone()),
            username: username.clone(),
            first_name: None,
            last_name: None,
            is_forum: if is_forum { Some(true) } else { None },
            is_direct_messages: None,
        };

        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("new_chat_title".to_string(), Value::String(title.clone()));

        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            service_text_group_title_changed(&actor, &title),
            service_fields,
        )?;
    }

    Ok(json!({
        "chat": {
            "id": body.chat_id,
            "type": chat_type,
            "title": title,
            "username": username,
            "is_forum": if is_forum { Some(true) } else { None::<bool> }
        },
        "settings": {
            "description": description,
            "show_author_signature": show_author_signature,
            "paid_star_reactions_enabled": paid_star_reactions_enabled,
            "linked_chat_id": linked_discussion_chat_id,
            "direct_messages_enabled": direct_messages_enabled,
            "direct_messages_star_count": direct_messages_star_count,
            "message_history_visible": message_history_visible,
            "slow_mode_delay": slow_mode_delay,
            "permissions": permissions
        }
    }))
}

pub fn handle_sim_delete_group(
    state: &Data<AppState>,
    token: &str,
    body: SimDeleteGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("group not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot delete private chat with group endpoint"));
    }

    let Some(actor_id) = body.user_id else {
        return Err(ApiError::bad_request("owner user_id is required to delete group"));
    };
    let actor = get_or_create_user(
        &mut conn,
        Some(actor_id),
        body.actor_first_name,
        body.actor_username,
    )?;

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(is_group_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request("only group owner can delete this group"));
    }

    conn.execute(
        "DELETE FROM sim_chat_join_requests WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_chat_invite_links WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM message_reactions WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM chat_reply_keyboards WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM callback_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM inline_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM shipping_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM pre_checkout_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM invoices WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM inline_messages WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let poll_ids: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT id FROM polls WHERE bot_id = ?1 AND chat_key = ?2")
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![bot.id, &chat_key], |row| row.get::<_, String>(0))
            .map_err(ApiError::internal)?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row.map_err(ApiError::internal)?);
        }
        ids
    };
    for poll_id in poll_ids {
        conn.execute("DELETE FROM poll_votes WHERE poll_id = ?1", params![poll_id])
            .map_err(ApiError::internal)?;
        conn.execute("DELETE FROM poll_metadata WHERE poll_id = ?1", params![poll_id])
            .map_err(ApiError::internal)?;
    }
    conn.execute(
        "DELETE FROM polls WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM sim_message_drafts WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM messages WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let chat_fragment = format!("\"chat\":{{\"id\":{}", body.chat_id);
    conn.execute(
        "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2",
        params![bot.id, format!("%{}%", chat_fragment)],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM chats WHERE chat_key = ?1", params![&chat_key])
        .map_err(ApiError::internal)?;

    Ok(json!({
        "deleted": true,
        "chat_id": body.chat_id
    }))
}

pub fn handle_sim_set_bot_group_membership(
    state: &Data<AppState>,
    token: &str,
    body: SimSetBotGroupMembershipRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("group not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot update bot membership in private chat"));
    }

    let Some(target_status) = normalize_group_membership_status(&body.status) else {
        return Err(ApiError::bad_request(
            "status must be one of: member, admin, administrator, left, remove",
        ));
    };

    let actor_id = body.actor_user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "only owner or admin can update bot membership",
        ));
    }

    let old_status = load_chat_member_status(&mut conn, bot.id, &chat_key, bot.id)?
        .unwrap_or_else(|| "left".to_string());

    if old_status == target_status {
        return Ok(json!({
            "changed": false,
            "chat_id": body.chat_id,
            "status": target_status,
        }));
    }

    let now = Utc::now().timestamp();
    let channel_admin_rights_json = if sim_chat.chat_type == "channel" {
        Some(serde_json::to_string(&channel_admin_rights_full_access()).map_err(ApiError::internal)?)
    } else {
        None
    };
    match target_status {
        "admin" => {
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, admin_rights_json, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'admin', 'admin', ?4, ?5, ?5)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'admin', role = 'admin', admin_rights_json = excluded.admin_rights_json, joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
                params![bot.id, &chat_key, bot.id, channel_admin_rights_json, now],
            )
            .map_err(ApiError::internal)?;
        }
        "member" => {
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'member', 'member', ?4, ?4)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'member', role = 'member', admin_rights_json = NULL, joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
                params![bot.id, &chat_key, bot.id, now],
            )
            .map_err(ApiError::internal)?;
        }
        _ => {
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'left', 'left', NULL, ?4)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'left', role = 'left', admin_rights_json = NULL, updated_at = excluded.updated_at",
                params![bot.id, &chat_key, bot.id, now],
            )
            .map_err(ApiError::internal)?;
        }
    }

    let bot_user = build_bot_user(&bot);
    let chat = Chat {
        id: sim_chat.chat_id,
        r#type: sim_chat.chat_type.clone(),
        title: sim_chat.title.clone(),
        username: sim_chat.username.clone(),
        first_name: None,
        last_name: None,
        is_forum: if sim_chat.chat_type == "supergroup" {
            Some(sim_chat.is_forum)
        } else {
            None
        },
        is_direct_messages: None,
    };

    let old_chat_member = chat_member_from_status(&old_status, &bot_user)?;
    let new_chat_member = chat_member_from_status(target_status, &bot_user)?;
    let membership_update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: actor.clone(),
            date: now,
            old_chat_member,
            new_chat_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(membership_update).map_err(ApiError::internal)?,
    )?;

    let old_active = is_active_chat_member_status(&old_status);
    let new_active = is_active_chat_member_status(target_status);
    if old_active != new_active && sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        let text = if new_active {
            service_fields.insert(
                "new_chat_members".to_string(),
                serde_json::to_value(vec![bot_user.clone()]).map_err(ApiError::internal)?,
            );
            service_text_new_chat_members(&actor, std::slice::from_ref(&bot_user))
        } else {
            service_fields.insert(
                "left_chat_member".to_string(),
                serde_json::to_value(bot_user.clone()).map_err(ApiError::internal)?,
            );
            service_text_left_chat_member(&actor, &bot_user)
        };

        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            text,
            service_fields,
        )?;
    }

    Ok(json!({
        "changed": true,
        "chat_id": body.chat_id,
        "status": target_status,
    }))
}

pub fn handle_sim_create_group_invite_link(
    state: &Data<AppState>,
    token: &str,
    body: SimCreateGroupInviteLinkRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("group not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot create invite link for private chat"));
    }

    let actor_id = body.user_id.unwrap_or(bot.id);
    let actor_user = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor_user.id)?;
    if sim_chat.chat_type == "channel" {
        let can_manage = match actor_status.as_deref() {
            Some("owner") => true,
            Some("admin") => {
                let rights = load_chat_member_record(&mut conn, bot.id, &chat_key, actor_user.id)?
                    .map(|record| parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
                    .unwrap_or_else(channel_admin_rights_full_access);
                rights.can_manage_chat || rights.can_invite_users
            }
            _ => false,
        };

        if !can_manage {
            return Err(ApiError::bad_request(
                "only channel owner/admin with invite rights can create invite link",
            ));
        }
    } else {
        let can_invite_by_role = actor_status
            .as_deref()
            .map(is_group_admin_or_owner_status)
            .unwrap_or(false);
        let can_invite_by_permission = if actor_status.as_deref() == Some("member") {
            load_group_runtime_settings(&mut conn, bot.id, &chat_key)?
                .map(|settings| settings.permissions.can_invite_users.unwrap_or(true))
                .unwrap_or(false)
        } else {
            false
        };
        if !(can_invite_by_role || can_invite_by_permission) {
            return Err(ApiError::bad_request(
                "only owner/admin or members with can_invite_users can create invite link",
            ));
        }
    }

    let mut invite_link = generate_sim_invite_link();
    loop {
        let exists: Option<String> = conn
            .query_row(
                "SELECT invite_link FROM sim_chat_invite_links WHERE bot_id = ?1 AND invite_link = ?2",
                params![bot.id, &invite_link],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            break;
        }
        invite_link = generate_sim_invite_link();
    }

    let now = Utc::now().timestamp();
    let creates_join_request = body.creates_join_request.unwrap_or(false);
    let name = body
        .name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let expire_date = body.expire_date.filter(|v| *v > now);
    let member_limit = body.member_limit.filter(|v| *v > 0);

    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7, ?8, NULL, NULL, ?9, ?9)",
        params![
            bot.id,
            &chat_key,
            &invite_link,
            actor_user.id,
            if creates_join_request { 1 } else { 0 },
            name,
            expire_date,
            member_limit,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let record = SimChatInviteLinkRecord {
        invite_link,
        creator_user_id: actor_user.id,
        creates_join_request,
        is_primary: false,
        is_revoked: false,
        name,
        expire_date,
        member_limit,
        subscription_period: None,
        subscription_price: None,
    };

    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3 AND status = 'pending'",
            params![bot.id, &chat_key, &record.invite_link],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let invite = chat_invite_link_from_record(actor_user, &record, Some(pending_count));
    serde_json::to_value(invite).map_err(ApiError::internal)
}

pub fn handle_sim_join_group_by_invite_link(
    state: &Data<AppState>,
    token: &str,
    body: SimJoinGroupByInviteLinkRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let invite_link = body.invite_link.trim().to_string();
    if invite_link.is_empty() {
        return Err(ApiError::bad_request("invite_link is empty"));
    }

    let link_row: Option<(String, i64, i64, i64, Option<String>, Option<i64>, Option<i64>, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT chat_key, creator_user_id, creates_join_request, is_primary, name, expire_date, member_limit, subscription_period, subscription_price
             FROM sim_chat_invite_links
             WHERE bot_id = ?1 AND invite_link = ?2 AND is_revoked = 0",
            params![bot.id, &invite_link],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, creator_user_id, creates_join_request_raw, is_primary_raw, name, expire_date, member_limit, subscription_period, subscription_price)) = link_row else {
        return Err(ApiError::not_found("invite link not found"));
    };

    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot join private chat by invite link"));
    }

    let now = Utc::now().timestamp();
    if let Some(expire_at) = expire_date {
        if expire_at <= now {
            return Err(ApiError::bad_request("invite link expired"));
        }
    }

    let user = get_or_create_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, user.id)?;
    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(json!({
            "joined": false,
            "reason": "already_member",
            "chat_id": sim_chat.chat_id,
            "user_id": user.id,
        }));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    if let Some(limit) = member_limit {
        let active_members: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sim_chat_members
                 WHERE bot_id = ?1 AND chat_key = ?2 AND status IN ('owner','admin','member')",
                params![bot.id, &chat_key],
                |row| row.get(0),
            )
            .map_err(ApiError::internal)?;
        if active_members >= limit {
            return Err(ApiError::bad_request("invite link member limit reached"));
        }
    }

    let creator = if creator_user_id == bot.id {
        build_bot_user(&bot)
    } else if let Some(record) = load_sim_user_record(&mut conn, creator_user_id)? {
        build_user_from_sim_record(&record, false)
    } else {
        build_user_from_sim_record(
            &ensure_user(
                &mut conn,
                Some(creator_user_id),
                Some(format!("User {}", creator_user_id)),
                None,
            )?,
            false,
        )
    };

    let invite_record = SimChatInviteLinkRecord {
        invite_link: invite_link.clone(),
        creator_user_id,
        creates_join_request: creates_join_request_raw == 1,
        is_primary: is_primary_raw == 1,
        is_revoked: false,
        name,
        expire_date,
        member_limit,
        subscription_period,
        subscription_price,
    };
    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3 AND status = 'pending'",
            params![bot.id, &chat_key, &invite_link],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    let invite = chat_invite_link_from_record(creator, &invite_record, Some(pending_count));

    if invite.creates_join_request {
        conn.execute(
            "INSERT INTO sim_chat_join_requests
             (bot_id, chat_key, user_id, invite_link, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5)
             ON CONFLICT(bot_id, chat_key, user_id)
             DO UPDATE SET invite_link = excluded.invite_link, status = 'pending', updated_at = excluded.updated_at",
            params![bot.id, &chat_key, user.id, &invite_link, now],
        )
        .map_err(ApiError::internal)?;

        let from_user = build_user_from_sim_record(&user, false);
        let chat = chat_from_sim_record(&sim_chat, &user);

        let join_request_update = Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: Some(ChatJoinRequest {
                chat,
                from: from_user,
                user_chat_id: build_sim_user_chat_id(sim_chat.chat_id, user.id),
                date: now,
                bio: None,
                invite_link: Some(invite),
            }),
            chat_boost: None,
            removed_chat_boost: None,
        managed_bot: None,
        };
        persist_and_dispatch_update(
            state,
            &mut conn,
            token,
            bot.id,
            serde_json::to_value(join_request_update).map_err(ApiError::internal)?,
        )?;

        return Ok(json!({
            "joined": false,
            "pending": true,
            "chat_id": sim_chat.chat_id,
            "chat_type": sim_chat.chat_type,
            "user_id": user.id,
        }));
    }

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &user,
        current_status.as_deref(),
        Some(invite),
        None,
    )?;

    Ok(json!({
        "joined": true,
        "pending": false,
        "chat_id": sim_chat.chat_id,
        "chat_type": sim_chat.chat_type,
        "user_id": user.id,
    }))
}

pub fn handle_sim_mark_channel_message_view(
    state: &Data<AppState>,
    token: &str,
    body: SimMarkChannelMessageViewRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("chat is not a channel"));
    }

    let viewer = ensure_user(
        &mut conn,
        body.user_id,
        body.first_name,
        body.username,
    )?;
    ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, viewer.id)?;

    let message_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id
             FROM messages
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, body.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if message_exists.is_none() {
        return Err(ApiError::not_found("message not found"));
    }

    let (views, incremented) = mark_channel_post_view_for_user(
        &mut conn,
        bot.id,
        &chat_key,
        body.message_id,
        viewer.id,
    )?;

    Ok(json!({
        "chat_id": body.chat_id,
        "chat_type": sim_chat.chat_type,
        "message_id": body.message_id,
        "user_id": viewer.id,
        "views": views,
        "incremented": incremented,
        "window_seconds": CHANNEL_VIEW_WINDOW_SECONDS,
    }))
}

pub fn handle_sim_approve_join_request(
    state: &Data<AppState>,
    token: &str,
    body: SimResolveJoinRequestRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };

    let actor_id = body.actor_user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request("only owner or admin can approve join requests"));
    }
    if sim_chat.chat_type == "channel" {
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let request_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT invite_link, status
             FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, body.user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((invite_link, status)) = request_row else {
        return Err(ApiError::not_found("join request not found"));
    };
    if status != "pending" {
        return Ok(json!({
            "approved": false,
            "reason": "already_resolved",
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }

    let target_user = if let Some(record) = load_sim_user_record(&mut conn, body.user_id)? {
        record
    } else {
        ensure_user(
            &mut conn,
            Some(body.user_id),
            Some(format!("User {}", body.user_id)),
            None,
        )?
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'approved', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, body.user_id, now],
    )
    .map_err(ApiError::internal)?;

    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, target_user.id)?;
    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(json!({
            "approved": true,
            "joined": false,
            "reason": "already_member",
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    let invite = if let Some(raw_link) = invite_link {
        let record_row: Option<(i64, i64, i64, Option<String>, Option<i64>, Option<i64>, Option<i64>, Option<i64>)> = conn
            .query_row(
                "SELECT creator_user_id, creates_join_request, is_primary, name, expire_date, member_limit, subscription_period, subscription_price
                 FROM sim_chat_invite_links
                 WHERE bot_id = ?1 AND invite_link = ?2",
                params![bot.id, &raw_link],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some((creator_user_id, creates_join_request_raw, is_primary_raw, name, expire_date, member_limit, subscription_period, subscription_price)) = record_row {
            let creator = if creator_user_id == bot.id {
                build_bot_user(&bot)
            } else if let Some(record) = load_sim_user_record(&mut conn, creator_user_id)? {
                build_user_from_sim_record(&record, false)
            } else {
                build_user_from_sim_record(
                    &ensure_user(
                        &mut conn,
                        Some(creator_user_id),
                        Some(format!("User {}", creator_user_id)),
                        None,
                    )?,
                    false,
                )
            };
            Some(chat_invite_link_from_record(
                creator,
                &SimChatInviteLinkRecord {
                    invite_link: raw_link,
                    creator_user_id,
                    creates_join_request: creates_join_request_raw == 1,
                    is_primary: is_primary_raw == 1,
                    is_revoked: false,
                    name,
                    expire_date,
                    member_limit,
                    subscription_period,
                    subscription_price,
                },
                None,
            ))
        } else {
            None
        }
    } else {
        None
    };

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &target_user,
        current_status.as_deref(),
        invite,
        Some(true),
    )?;

    Ok(json!({
        "approved": true,
        "joined": true,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_decline_join_request(
    state: &Data<AppState>,
    token: &str,
    body: SimResolveJoinRequestRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };

    let actor_id = body.actor_user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request("only owner or admin can decline join requests"));
    }
    if sim_chat.chat_type == "channel" {
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_join_requests WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, body.user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(current_status) = status else {
        return Err(ApiError::not_found("join request not found"));
    };
    if current_status != "pending" {
        return Ok(json!({
            "declined": false,
            "reason": "already_resolved",
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'declined', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, body.user_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "declined": true,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

fn join_user_to_group(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    user: &SimUserRecord,
    old_status: Option<&str>,
    invite_link: Option<ChatInviteLink>,
    via_join_request: Option<bool>,
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
         VALUES (?1, ?2, ?3, 'member', 'member', ?4, ?4)
         ON CONFLICT(bot_id, chat_key, user_id)
         DO UPDATE SET status = 'member', role = 'member', joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
        params![bot_id, &sim_chat.chat_key, user.id, now],
    )
    .map_err(ApiError::internal)?;

    let from_user = build_user_from_sim_record(user, false);
    let chat = chat_from_sim_record(sim_chat, user);
    let old_member = chat_member_from_status(old_status.unwrap_or("left"), &from_user)?;
    let new_member = chat_member_from_status("member", &from_user)?;

    let update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: from_user.clone(),
            date: now,
            old_chat_member: old_member,
            new_chat_member: new_member,
            invite_link,
            via_join_request,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        conn,
        token,
        bot_id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )?;

    if sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "new_chat_members".to_string(),
            serde_json::to_value(vec![from_user.clone()]).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            conn,
            token,
            bot_id,
            &sim_chat.chat_key,
            &chat,
            &from_user,
            now,
            service_text_new_chat_members(&from_user, std::slice::from_ref(&from_user)),
            service_fields,
        )?;
    }

    Ok(())
}

pub fn handle_sim_join_group(
    state: &Data<AppState>,
    token: &str,
    body: SimJoinGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot join private chat"));
    }

    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, user.id)?;

    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(json!({
            "joined": false,
            "reason": "already_member",
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    let primary_invite_link: Option<String> = conn
        .query_row(
            "SELECT invite_link
             FROM sim_chat_invite_links
             WHERE bot_id = ?1 AND chat_key = ?2 AND is_primary = 1 AND is_revoked = 0
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, &chat_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let primary_invite = if let Some(raw_link) = primary_invite_link {
        if let Some(record) = load_invite_link_record(&mut conn, bot.id, &chat_key, &raw_link)? {
            Some(chat_invite_link_from_record(
                resolve_invite_creator_user(&mut conn, &bot, record.creator_user_id)?,
                &record,
                None,
            ))
        } else {
            None
        }
    } else {
        None
    };

    if primary_invite
        .as_ref()
        .map(|invite| invite.creates_join_request)
        .unwrap_or(false)
    {
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO sim_chat_join_requests
             (bot_id, chat_key, user_id, invite_link, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5)
             ON CONFLICT(bot_id, chat_key, user_id)
             DO UPDATE SET invite_link = excluded.invite_link, status = 'pending', updated_at = excluded.updated_at",
            params![
                bot.id,
                &chat_key,
                user.id,
                primary_invite.as_ref().map(|invite| invite.invite_link.clone()),
                now,
            ],
        )
        .map_err(ApiError::internal)?;

        let from_user = build_user_from_sim_record(&user, false);
        let chat = chat_from_sim_record(&sim_chat, &user);
        let join_request_update = Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: Some(ChatJoinRequest {
                chat,
                from: from_user,
                user_chat_id: build_sim_user_chat_id(sim_chat.chat_id, user.id),
                date: now,
                bio: None,
                invite_link: primary_invite,
            }),
            chat_boost: None,
            removed_chat_boost: None,
            managed_bot: None,
        };
        persist_and_dispatch_update(
            state,
            &mut conn,
            token,
            bot.id,
            serde_json::to_value(join_request_update).map_err(ApiError::internal)?,
        )?;

        return Ok(json!({
            "joined": false,
            "pending": true,
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    }

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &user,
        current_status.as_deref(),
        primary_invite,
        None,
    )?;

    Ok(json!({
        "joined": true,
        "pending": false,
        "chat_id": body.chat_id,
        "user_id": user.id
    }))
}

pub fn handle_sim_leave_group(
    state: &Data<AppState>,
    token: &str,
    body: SimLeaveGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot leave private chat"));
    }

    let current_status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, chat_key, user.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(status) = current_status.clone() else {
        return Ok(json!({
            "left": false,
            "reason": "not_member",
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    };
    if status == "owner" {
        return Err(ApiError::bad_request("owner cannot leave simulated group"));
    }
    if status == "left" {
        return Ok(json!({
            "left": false,
            "reason": "already_left",
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_members
         SET status = 'left', role = 'left', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, chat_key, user.id, now],
    )
    .map_err(ApiError::internal)?;

    let from_user = build_user_from_sim_record(&user, false);
    let chat = chat_from_sim_record(&sim_chat, &user);
    let old_member = chat_member_from_status(&status, &from_user)?;
    let new_member = chat_member_from_status("left", &from_user)?;

    let update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: from_user.clone(),
            date: now,
            old_chat_member: old_member,
            new_chat_member: new_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )?;

    if sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "left_chat_member".to_string(),
            serde_json::to_value(from_user.clone()).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &from_user,
            now,
            service_text_left_chat_member(&from_user, &from_user),
            service_fields,
        )?;
    }

    Ok(json!({
        "left": true,
        "chat_id": body.chat_id,
        "user_id": user.id
    }))
}

pub fn handle_sim_clear_history(
    state: &Data<AppState>,
    token: &str,
    body: SimClearHistoryRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat_id) =
        resolve_chat_key_and_id(&mut conn, bot.id, &Value::from(body.chat_id))?;

    let sim_chat = load_sim_chat_record(&mut conn, bot.id, &chat_key)?;
    let scoped_thread_id = body.message_thread_id;

    let message_ids: Vec<i64> = if let Some(message_thread_id) = scoped_thread_id {
        if message_thread_id <= 0 {
            return Err(ApiError::bad_request("message_thread_id is invalid"));
        }

        let sim_chat = sim_chat.as_ref().ok_or_else(|| {
            ApiError::bad_request(
                "message_thread_id is available only in forum supergroups and channel direct messages chats",
            )
        })?;

        if is_direct_messages_chat(sim_chat) {
            let topic_exists = load_direct_messages_topic_record(
                &mut conn,
                bot.id,
                &chat_key,
                message_thread_id,
            )?
            .is_some();
            if !topic_exists {
                return Err(ApiError::not_found("direct messages topic not found"));
            }
        } else if sim_chat.chat_type == "supergroup" && sim_chat.is_forum {
            if message_thread_id == 1 {
                let _ = ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
            } else if load_forum_topic(&mut conn, bot.id, &chat_key, message_thread_id)?.is_none() {
                return Err(ApiError::not_found("forum topic not found"));
            }
        } else {
            return Err(ApiError::bad_request(
                "message_thread_id is available only in forum supergroups and channel direct messages chats",
            ));
        }

        collect_message_ids_for_thread(&mut conn, bot.id, &chat_key, message_thread_id)?
    } else {
        let mut message_stmt = conn
            .prepare(
                "SELECT message_id
                 FROM messages
                 WHERE bot_id = ?1 AND chat_key = ?2
                 ORDER BY message_id ASC",
            )
            .map_err(ApiError::internal)?;
        let message_rows = message_stmt
            .query_map(params![bot.id, &chat_key], |row| row.get::<_, i64>(0))
            .map_err(ApiError::internal)?;
        let message_ids = message_rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(ApiError::internal)?;
        drop(message_stmt);
        message_ids
    };

    let mut deleted = 0usize;
    for chunk in message_ids.chunks(300) {
        deleted += delete_messages_with_dependencies(
            &mut conn,
            bot.id,
            &chat_key,
            chat_id,
            chunk,
        )?;
    }

    if let Some(message_thread_id) = scoped_thread_id {
        conn.execute(
            "DELETE FROM sim_message_drafts
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot.id, &chat_key, message_thread_id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "DELETE FROM sim_message_drafts WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
        )
        .map_err(ApiError::internal)?;

        let chat_fragment = format!("\"chat\":{{\"id\":{}", chat_id);
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2",
            params![bot.id, format!("%{}%", chat_fragment)],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "DELETE FROM invoices WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!({"deleted_count": deleted}))
}

fn handle_set_message_reaction(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMessageReactionRequest = parse_request(params)?;

    let reactions = normalize_reaction_values(request.reaction.as_ref().map(|r| {
        r.iter().map(|item| item.extra.clone()).collect::<Vec<Value>>()
    }))?;

    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chat_id_as_i64(&request.chat_id, &chat_key);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let actor = User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name.clone(),
        last_name: None,
        username: Some(bot.username.clone()),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: Some(true),
        can_read_all_group_messages: Some(false),
        supports_inline_queries: Some(false),
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    apply_message_reaction_change(
        state,
        &mut conn,
        &bot,
        token,
        &chat_key,
        chat_id,
        request.message_id,
        actor,
        reactions,
    )
}

pub fn handle_sim_set_user_reaction(
    state: &Data<AppState>,
    token: &str,
    body: SimSetUserReactionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let reactions = normalize_reaction_values(body.reaction)?;
    let chat_key = body.chat_id.to_string();

    let actor = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name,
        last_name: None,
        username: user.username,
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    apply_message_reaction_change(
        state,
        &mut conn,
        &bot,
        token,
        &chat_key,
        body.chat_id,
        body.message_id,
        actor,
        reactions,
    )
}

pub fn handle_sim_press_inline_button(
    state: &Data<AppState>,
    token: &str,
    body: SimPressInlineButtonRequest,
) -> ApiResult {
    if body.callback_data.trim().is_empty() {
        return Err(ApiError::bad_request("callback_data is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let mut message_value = load_message_value(&mut conn, &bot, body.message_id)?;
    enrich_message_with_linked_channel_context(
        &mut conn,
        bot.id,
        &chat_key,
        body.message_id,
        &mut message_value,
    )?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message not found"));
    }

    let callback_query_id = generate_telegram_numeric_id();
    let now = Utc::now().timestamp();

    let callback_from = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    let is_inline_origin = message_value
        .get("via_bot")
        .and_then(|v| v.get("id"))
        .and_then(Value::as_i64)
        == Some(bot.id);

    let inline_message_id = if is_inline_origin {
        let existing: Option<String> = conn
            .query_row(
                "SELECT inline_message_id FROM inline_messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
                params![bot.id, chat_key, body.message_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(existing_id) = existing {
            Some(existing_id)
        } else {
            let generated = generate_telegram_numeric_id();
            conn.execute(
                "INSERT OR REPLACE INTO inline_messages (inline_message_id, bot_id, chat_key, message_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![generated, bot.id, chat_key, body.message_id, now],
            )
            .map_err(ApiError::internal)?;
            Some(generated)
        }
    } else {
        None
    };

    let callback_message: Option<MaybeInaccessibleMessage> = if inline_message_id.is_some() {
        None
    } else {
        Some(serde_json::from_value(message_value).map_err(ApiError::internal)?)
    };

    let callback_query = CallbackQuery {
        id: callback_query_id.clone(),
        from: callback_from,
        message: callback_message,
        inline_message_id,
        chat_instance: generate_telegram_numeric_id(),
        data: Some(body.callback_data.clone()),
        game_short_name: None,
    };

    conn.execute(
        "INSERT INTO callback_queries (id, bot_id, chat_key, message_id, from_user_id, data, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![callback_query_id, bot.id, chat_key, body.message_id, user.id, body.callback_data, now],
    )
    .map_err(ApiError::internal)?;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: Some(callback_query),
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "ok": true,
        "callback_query_id": callback_query_id,
    }))
}

pub fn handle_sim_send_inline_query(
    state: &Data<AppState>,
    token: &str,
    body: SimSendInlineQueryRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let chat_key = chat_id.to_string();
    ensure_chat(&mut conn, &chat_key)?;

    let inline_query_id = generate_telegram_numeric_id();
    let now = Utc::now().timestamp();
    let query_text = body.query;
    let offset = body.offset.unwrap_or_default();

    let cached_answer_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT answer_json, expires_at
             FROM inline_query_cache
             WHERE bot_id = ?1 AND query = ?2 AND offset = ?3
                             AND (from_user_id = -1 OR from_user_id = ?4)
                         ORDER BY CASE WHEN from_user_id = ?4 THEN 0 ELSE 1 END
             LIMIT 1",
            params![bot.id, query_text, offset, user.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some((cached_answer_json, expires_at)) = cached_answer_row {
        if expires_at >= now {
            conn.execute(
                "INSERT INTO inline_queries (id, bot_id, chat_key, from_user_id, query, offset, created_at, answered_at, answer_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    inline_query_id,
                    bot.id,
                    chat_key,
                    user.id,
                    query_text,
                    offset,
                    now,
                    now,
                    cached_answer_json,
                ],
            )
            .map_err(ApiError::internal)?;

            return Ok(json!({
                "inline_query_id": inline_query_id,
                "cached": true,
            }));
        }
    }

    let inline_from = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    let inline_query = InlineQuery {
        id: inline_query_id.clone(),
        from: inline_from,
        query: query_text.clone(),
        offset: offset.clone(),
        chat_type: Some("private".to_string()),
        location: None,
    };

    conn.execute(
        "INSERT INTO inline_queries (id, bot_id, chat_key, from_user_id, query, offset, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            inline_query_id,
            bot.id,
            chat_key,
            user.id,
            query_text,
            offset,
            now
        ],
    )
    .map_err(ApiError::internal)?;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: Some(inline_query.clone()),
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "inline_query_id": inline_query_id,
        "cached": false,
    }))
}

pub fn handle_sim_get_inline_query_answer(
    state: &Data<AppState>,
    token: &str,
    inline_query_id: &str,
) -> ApiResult {
    if inline_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("inline_query_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT answer_json, answered_at FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((answer_json, answered_at)) = row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let parsed = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    Ok(json!({
        "inline_query_id": inline_query_id,
        "answered": answered_at.is_some(),
        "answered_at": answered_at,
        "answer": parsed,
    }))
}

pub fn handle_sim_get_callback_query_answer(
    state: &Data<AppState>,
    token: &str,
    callback_query_id: &str,
) -> ApiResult {
    if callback_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("callback_query_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT answer_json, answered_at FROM callback_queries WHERE id = ?1 AND bot_id = ?2",
            params![callback_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((answer_json, answered_at)) = row else {
        return Err(ApiError::not_found("callback query not found"));
    };

    let parsed = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    Ok(json!({
        "callback_query_id": callback_query_id,
        "answered": answered_at.is_some(),
        "answered_at": answered_at,
        "answer": parsed,
    }))
}

pub fn handle_sim_choose_inline_result(
    state: &Data<AppState>,
    token: &str,
    body: SimChooseInlineResultRequest,
) -> ApiResult {
    if body.inline_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("inline_query_id is required"));
    }
    if body.result_id.trim().is_empty() {
        return Err(ApiError::bad_request("result_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, i64, String, Option<String>)> = conn
        .query_row(
            "SELECT chat_key, from_user_id, query, answer_json FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![body.inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, from_user_id, query_text, answer_json)) = row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let answer_value: Value = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .ok_or_else(|| ApiError::bad_request("inline query has no answer yet"))?;

    let results = answer_value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad_request("inline query answer has no results"))?;

    let selected = results
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(body.result_id.as_str()))
        .or_else(|| results.first())
        .ok_or_else(|| ApiError::bad_request("inline query answer has empty results"))?;

    let message_text = selected
        .get("input_message_content")
        .and_then(|c| c.get("message_text"))
        .and_then(Value::as_str)
        .map(|v| v.to_string())
        .or_else(|| selected.get("title").and_then(Value::as_str).map(|v| v.to_string()))
        .or_else(|| selected.get("description").and_then(Value::as_str).map(|v| v.to_string()))
        .unwrap_or_else(|| "inline result".to_string());

    ensure_chat(&mut conn, &chat_key)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, from_user_id, message_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(&chat_key));

    let user_info: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT first_name, username FROM users WHERE id = ?1",
            params![from_user_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let (first_name, username) = user_info.unwrap_or_else(|| ("User".to_string(), None));

    let message_payload = json!({
        "message_id": message_id,
        "date": now,
        "chat": {
            "id": chat_id,
            "type": "private"
        },
        "from": {
            "id": from_user_id,
            "is_bot": false,
            "first_name": first_name,
            "username": username
        },
        "text": message_text,
        "via_bot": {
            "id": bot.id,
            "is_bot": true,
            "first_name": bot.first_name,
            "username": bot.username
        }
    });
    let message_for_update: Message = serde_json::from_value(message_payload).map_err(ApiError::internal)?;
    let message_update = serde_json::to_value(Update {
        update_id: 0,
        message: Some(message_for_update),
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, message_update)?;

    let chosen_from = User {
        id: from_user_id,
        is_bot: false,
        first_name: first_name.clone(),
        last_name: None,
        username: username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };
    let inline_message_id = generate_telegram_numeric_id();
    conn.execute(
        "INSERT OR REPLACE INTO inline_messages (inline_message_id, bot_id, chat_key, message_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![inline_message_id, bot.id, chat_key, message_id, now],
    )
    .map_err(ApiError::internal)?;

    let chosen_inline_result_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: Some(ChosenInlineResult {
            result_id: body.result_id.clone(),
            from: chosen_from,
            location: None,
            inline_message_id: Some(inline_message_id),
            query: query_text,
        }),
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, chosen_inline_result_update)?;

    Ok(json!({
        "message_id": message_id,
        "result_id": body.result_id,
    }))
}

fn handle_get_updates(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetUpdatesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let webhook_url: Option<String> = conn
        .query_row(
            "SELECT url FROM webhooks WHERE bot_id = ?1",
            params![bot.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if webhook_url.is_some() {
        return Err(ApiError::conflict(
            "can't use getUpdates method while webhook is active",
        ));
    }

    let offset = request.offset.unwrap_or(0);
    let mut limit = request.limit.unwrap_or(100);
    if limit <= 0 {
        limit = 1;
    }
    if limit > 100 {
        limit = 100;
    }

    if offset > 0 {
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND update_id < ?2",
            params![bot.id, offset],
        )
        .map_err(ApiError::internal)?;
    }

    let mut stmt = conn
        .prepare(
            "SELECT update_id, update_json FROM updates
             WHERE bot_id = ?1 AND update_id >= ?2 AND bot_visible = 1
             ORDER BY update_id ASC
             LIMIT ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, offset.max(1), limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ApiError::internal)?;

    let fetched_rows: Vec<(i64, String)> = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(stmt);

    let mut updates = Vec::new();
    let mut stale_update_ids = Vec::new();
    for (update_id, raw) in fetched_rows {
        let mut parsed: Value = serde_json::from_str(&raw).map_err(ApiError::internal)?;
        enrich_channel_post_payloads(&mut conn, bot.id, &mut parsed)?;

        if update_targets_deleted_message(&mut conn, bot.id, &parsed)? {
            stale_update_ids.push(update_id);
            continue;
        }

        updates.push(parsed);
    }

    if !stale_update_ids.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(stale_update_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "DELETE FROM updates WHERE bot_id = ? AND update_id IN ({})",
            placeholders
        );

        let mut bind_values = Vec::with_capacity(1 + stale_update_ids.len());
        bind_values.push(Value::from(bot.id));
        for id in stale_update_ids {
            bind_values.push(Value::from(id));
        }

        let mut delete_stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
        delete_stmt
            .execute(rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)))
            .map_err(ApiError::internal)?;
    }

    Ok(Value::Array(updates))
}

fn handle_get_webhook_info(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetWebhookInfoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT url, ip_address, max_connections FROM webhooks WHERE bot_id = ?1",
            params![bot.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let pending_update_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM updates WHERE bot_id = ?1 AND bot_visible = 1",
            params![bot.id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let (url, ip_address, max_connections) = if let Some((url, ip_address, max_connections)) = row {
        let normalized_ip = ip_address
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        (url, normalized_ip, max_connections)
    } else {
        (String::new(), None, None)
    };

    serde_json::to_value(WebhookInfo {
        url,
        has_custom_certificate: false,
        pending_update_count,
        ip_address,
        last_error_date: None,
        last_error_message: None,
        last_synchronization_error_date: None,
        max_connections,
        allowed_updates: None,
    })
    .map_err(ApiError::internal)
}

fn handle_log_out(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: LogOutRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    conn.execute("DELETE FROM webhooks WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM updates WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_close(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: CloseRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let _bot = ensure_bot(&mut conn, token)?;

    Ok(json!(true))
}

fn handle_verify_user(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: VerifyUserRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let custom_description =
        normalize_verification_custom_description(request.custom_description.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;
    ensure_sim_user_record(&mut conn, request.user_id)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_user_verifications
         (bot_id, user_id, custom_description, verified_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET
             custom_description = excluded.custom_description,
             verified_at = excluded.verified_at,
             updated_at = excluded.updated_at",
        params![bot.id, request.user_id, custom_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_verify_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: VerifyChatRequest = parse_request(params)?;
    let custom_description =
        normalize_verification_custom_description(request.custom_description.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;

    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if is_direct_messages_chat(&sim_chat) {
        return Err(ApiError::bad_request(
            "verification is not supported for channel direct messages chats",
        ));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_chat_verifications
         (bot_id, chat_key, custom_description, verified_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(bot_id, chat_key)
         DO UPDATE SET
             custom_description = excluded.custom_description,
             verified_at = excluded.verified_at,
             updated_at = excluded.updated_at",
        params![bot.id, chat_key, custom_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_remove_user_verification(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RemoveUserVerificationRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_user_verifications WHERE bot_id = ?1 AND user_id = ?2",
        params![bot.id, request.user_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_remove_chat_verification(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RemoveChatVerificationRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;

    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if is_direct_messages_chat(&sim_chat) {
        return Err(ApiError::bad_request(
            "verification is not supported for channel direct messages chats",
        ));
    }

    conn.execute(
        "DELETE FROM sim_chat_verifications WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn update_targets_deleted_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update: &Value,
) -> Result<bool, ApiError> {
    const MESSAGE_FIELDS: [&str; 6] = [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_message",
        "edited_business_message",
    ];

    for field in MESSAGE_FIELDS {
        let Some(message_value) = update.get(field) else {
            continue;
        };

        let Some(message_obj) = message_value.as_object() else {
            continue;
        };

        let Some(chat_value) = message_obj
            .get("chat")
            .and_then(Value::as_object)
            .and_then(|chat_obj| chat_obj.get("id"))
        else {
            continue;
        };

        let Some(message_id) = message_obj.get("message_id").and_then(Value::as_i64) else {
            continue;
        };

        let chat_key = value_to_chat_key(chat_value)?;
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
                params![bot_id, chat_key, message_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if exists.is_none() {
            return Ok(true);
        }
    }

    Ok(false)
}

fn resolve_edit_target(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: Option<Value>,
    message_id: Option<i64>,
    inline_message_id: Option<String>,
    method_name: &str,
) -> Result<(String, i64, bool), ApiError> {
    if let Some(inline_id) = inline_message_id {
        let trimmed = inline_id.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("inline_message_id is empty"));
        }

        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT chat_key, message_id FROM inline_messages WHERE inline_message_id = ?1 AND bot_id = ?2",
                params![trimmed, bot_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some((chat_key, resolved_message_id)) = row {
            return Ok((chat_key, resolved_message_id, true));
        }

        return Err(ApiError::not_found(format!(
            "{} inline_message_id not found",
            method_name
        )));
    }

    let Some(chat) = chat_id else {
        return Err(ApiError::bad_request("chat_id is required"));
    };
    let Some(msg_id) = message_id else {
        return Err(ApiError::bad_request("message_id is required"));
    };

    let (chat_key, _) = resolve_chat_key_and_id(conn, bot_id, &chat)?;
    Ok((chat_key, msg_id, false))
}

fn resolve_chat_key_and_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: &Value,
) -> Result<(String, i64), ApiError> {
    let requested_chat_key = value_to_chat_key(chat_id)?;
    let requested_chat_id = chat_id_as_i64(chat_id, &requested_chat_key);

    if let Some(sim_chat) = load_sim_chat_record(conn, bot_id, &requested_chat_key)? {
        return Ok((sim_chat.chat_key, sim_chat.chat_id));
    }

    if let Some(sim_chat) = load_sim_chat_record_by_chat_id(conn, bot_id, requested_chat_id)? {
        return Ok((sim_chat.chat_key, sim_chat.chat_id));
    }

    Ok((requested_chat_key, requested_chat_id))
}

fn publish_edited_message_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    edited_message: &Value,
) -> Result<(), ApiError> {
    let mut edited_with_timestamp = edited_message.clone();
    edited_with_timestamp["edit_date"] = Value::from(Utc::now().timestamp());
    let is_channel_post = edited_with_timestamp
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(Value::as_str)
        == Some("channel");
    let is_business_message = edited_with_timestamp
        .get("business_connection_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: if is_channel_post || is_business_message {
            None
        } else {
            Some(serde_json::from_value(edited_with_timestamp.clone()).map_err(ApiError::internal)?)
        },
        channel_post: None,
        edited_channel_post: if is_channel_post {
            Some(serde_json::from_value(edited_with_timestamp.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
        business_connection: None,
        business_message: None,
        edited_business_message: if is_business_message {
            Some(serde_json::from_value(edited_with_timestamp.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, conn, token, bot_id, update_value)
}

fn ensure_message_can_be_edited_by_bot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    via_inline_message: bool,
) -> Result<(), ApiError> {
    let owner_user_id: Option<i64> = conn
        .query_row(
            "SELECT from_user_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot_id, chat_key, message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(owner_user_id) = owner_user_id else {
        return Err(ApiError::not_found("message to edit was not found"));
    };

    if !via_inline_message {
        let actor_user_id = current_request_actor_user_id().unwrap_or(bot_id);
        if owner_user_id != actor_user_id {
            return Err(ApiError::bad_request("message can't be edited"));
        }

        if actor_user_id != bot_id {
            let chat_type = load_sim_chat_record(conn, bot_id, chat_key)?
                .map(|chat| chat.chat_type)
                .unwrap_or_else(|| "private".to_string());

            if chat_type != "private" {
                let actor_status = load_chat_member_status(conn, bot_id, chat_key, actor_user_id)?;
                if !actor_status
                    .as_deref()
                    .map(is_active_chat_member_status)
                    .unwrap_or(false)
                {
                    return Err(ApiError::bad_request("message can't be edited"));
                }
            }
        }
    }

    Ok(())
}

fn ensure_message_can_be_deleted_by_actor(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
) -> Result<(), ApiError> {
    let owner_user_id: Option<i64> = conn
        .query_row(
            "SELECT from_user_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot_id, chat_key, message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(owner_user_id) = owner_user_id else {
        return Err(ApiError::not_found("message to delete was not found"));
    };

    let actor_user_id = current_request_actor_user_id().unwrap_or(bot_id);
    if actor_user_id == bot_id || owner_user_id == actor_user_id {
        return Ok(());
    }

    let Some(sim_chat) = load_sim_chat_record(conn, bot_id, chat_key)? else {
        return Err(ApiError::bad_request("message can't be deleted"));
    };

    if sim_chat.chat_type == "private" {
        return Ok(());
    }

    if is_direct_messages_chat(&sim_chat) {
        return Ok(());
    }

    if sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup" {
        let actor_status = load_chat_member_status(conn, bot_id, chat_key, actor_user_id)?;
        if actor_status
            .as_deref()
            .map(is_group_admin_or_owner_status)
            .unwrap_or(false)
        {
            return Ok(());
        }

        return Err(ApiError::bad_request("message can't be deleted"));
    }

    if sim_chat.chat_type == "channel" {
        let Some(actor_record) = load_chat_member_record(conn, bot_id, chat_key, actor_user_id)? else {
            return Err(ApiError::bad_request("message can't be deleted"));
        };

        if actor_record.status == "owner" {
            return Ok(());
        }

        if actor_record.status == "admin" {
            let rights = parse_channel_admin_rights_json(actor_record.admin_rights_json.as_deref());
            if rights.can_manage_chat
                || rights.can_delete_messages
                || rights.can_post_messages
                || rights.can_edit_messages
            {
                return Ok(());
            }
        }

        return Err(ApiError::bad_request("message can't be deleted"));
    }

    Err(ApiError::bad_request("message can't be deleted"))
}

fn delete_messages_with_dependencies(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    chat_id: i64,
    message_ids: &[i64],
) -> Result<usize, ApiError> {
    if message_ids.is_empty() {
        return Ok(0);
    }

    let placeholders = std::iter::repeat("?")
        .take(message_ids.len())
        .collect::<Vec<_>>()
        .join(",");

    let drafts_sql = format!(
        "DELETE FROM sim_message_drafts WHERE bot_id = ? AND message_id IN ({})",
        placeholders,
    );
    let mut drafts_bind_values = Vec::with_capacity(1 + message_ids.len());
    drafts_bind_values.push(Value::from(bot_id));
    for id in message_ids {
        drafts_bind_values.push(Value::from(*id));
    }
    let mut drafts_stmt = conn.prepare(&drafts_sql).map_err(ApiError::internal)?;
    drafts_stmt
        .execute(rusqlite::params_from_iter(
            drafts_bind_values.iter().map(sql_value_to_rusqlite),
        ))
        .map_err(ApiError::internal)?;
    drop(drafts_stmt);

    let messages_sql = format!(
        "DELETE FROM messages WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
        placeholders,
    );
    let mut messages_bind_values = Vec::with_capacity(2 + message_ids.len());
    messages_bind_values.push(Value::from(bot_id));
    messages_bind_values.push(Value::from(chat_key.to_string()));
    for id in message_ids {
        messages_bind_values.push(Value::from(*id));
    }
    let mut messages_stmt = conn.prepare(&messages_sql).map_err(ApiError::internal)?;
    let deleted = messages_stmt
        .execute(rusqlite::params_from_iter(
            messages_bind_values.iter().map(sql_value_to_rusqlite),
        ))
        .map_err(ApiError::internal)?;
    drop(messages_stmt);

    if deleted > 0 {
        let chat_id_fragment = format!("\"chat\":{{\"id\":{}", chat_id);
        for message_id in message_ids {
            let message_id_fragment = format!("\"message_id\":{}", message_id);
            conn.execute(
                "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2 AND update_json LIKE ?3",
                params![
                    bot_id,
                    format!("%{}%", chat_id_fragment),
                    format!("%{}%", message_id_fragment),
                ],
            )
            .map_err(ApiError::internal)?;
        }

        let invoices_sql = format!(
            "DELETE FROM invoices WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
            placeholders,
        );
        let mut invoice_bind_values = Vec::with_capacity(2 + message_ids.len());
        invoice_bind_values.push(Value::from(bot_id));
        invoice_bind_values.push(Value::from(chat_key.to_string()));
        for id in message_ids {
            invoice_bind_values.push(Value::from(*id));
        }
        let mut invoice_stmt = conn.prepare(&invoices_sql).map_err(ApiError::internal)?;
        invoice_stmt
            .execute(rusqlite::params_from_iter(
                invoice_bind_values.iter().map(sql_value_to_rusqlite),
            ))
            .map_err(ApiError::internal)?;
        drop(invoice_stmt);

        ensure_sim_suggested_posts_storage(conn)?;
        let suggested_sql = format!(
            "DELETE FROM sim_suggested_posts WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
            placeholders,
        );
        let mut suggested_bind_values = Vec::with_capacity(2 + message_ids.len());
        suggested_bind_values.push(Value::from(bot_id));
        suggested_bind_values.push(Value::from(chat_key.to_string()));
        for id in message_ids {
            suggested_bind_values.push(Value::from(*id));
        }
        let mut suggested_stmt = conn.prepare(&suggested_sql).map_err(ApiError::internal)?;
        suggested_stmt
            .execute(rusqlite::params_from_iter(
                suggested_bind_values.iter().map(sql_value_to_rusqlite),
            ))
            .map_err(ApiError::internal)?;
        drop(suggested_stmt);
    }

    Ok(deleted)
}

fn collect_message_ids_for_thread(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_thread_id: i64,
) -> Result<Vec<i64>, ApiError> {
    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(chat_key));

    let value_as_i64 = |value: Option<&Value>| -> Option<i64> {
        value.and_then(|raw| {
            raw.as_i64()
                .or_else(|| raw.as_str().and_then(|text| text.trim().parse::<i64>().ok()))
        })
    };

    let mut thread_message_ids = HashSet::<i64>::new();
    let mut update_stmt = conn
        .prepare(
            "SELECT update_json
             FROM updates
             WHERE bot_id = ?1
             ORDER BY update_id DESC",
        )
        .map_err(ApiError::internal)?;
    let update_rows = update_stmt
        .query_map(params![bot_id], |row| row.get::<_, String>(0))
        .map_err(ApiError::internal)?;

    for row in update_rows {
        let raw = row.map_err(ApiError::internal)?;
        let Ok(update_value) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };

        for field in [
            "edited_business_message",
            "business_message",
            "edited_channel_post",
            "channel_post",
            "edited_message",
            "message",
        ] {
            let Some(message_obj) = update_value.get(field).and_then(Value::as_object) else {
                continue;
            };

            let belongs_to_chat = value_as_i64(
                message_obj
                    .get("chat")
                    .and_then(Value::as_object)
                    .and_then(|chat| chat.get("id")),
            ) == Some(chat_id);
            if !belongs_to_chat {
                continue;
            }

            let belongs_to_thread = value_as_i64(message_obj.get("message_thread_id"))
                == Some(message_thread_id)
                || value_as_i64(
                    message_obj
                        .get("direct_messages_topic")
                        .and_then(Value::as_object)
                        .and_then(|topic| topic.get("topic_id")),
                ) == Some(message_thread_id);
            if !belongs_to_thread {
                continue;
            }

            if let Some(message_id) = value_as_i64(message_obj.get("message_id")) {
                thread_message_ids.insert(message_id);
            }
        }
    }
    drop(update_stmt);

    if thread_message_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut stmt = conn
        .prepare(
            "SELECT message_id
             FROM messages
             WHERE bot_id = ?1 AND chat_key = ?2
             ORDER BY message_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot_id, chat_key], |row| row.get::<_, i64>(0))
        .map_err(ApiError::internal)?;

    let message_ids: Vec<i64> = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(stmt);

    let mut result = Vec::new();
    for message_id in message_ids {
        if thread_message_ids.contains(&message_id) {
            result.push(message_id);
        }
    }

    Ok(result)
}

fn handle_edit_message_text(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageTextRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());

    let explicit_entities = request
        .entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let should_auto_detect_entities = explicit_entities.is_none();
    let (parsed_text, parsed_entities) = parse_formatted_text(
        &request.text,
        request.parse_mode.as_deref(),
        explicit_entities,
    );
    let parsed_entities = if should_auto_detect_entities {
        merge_auto_message_entities(&parsed_text, parsed_entities)
    } else {
        parsed_entities
    };

    if parsed_text.trim().is_empty() {
        return Err(ApiError::bad_request("text is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageText",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let updated = conn
        .execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![parsed_text, bot.id, chat_key, message_id],
        )
        .map_err(ApiError::internal)?;

    if updated == 0 {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    if let Some(entities) = parsed_entities {
        edited_message["entities"] = entities;
    } else {
        edited_message.as_object_mut().map(|obj| obj.remove("entities"));
    }
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_edit_message_media(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageMediaRequest = parse_request(params)?;

    let media_obj = request
        .media
        .extra
        .as_object()
        .ok_or_else(|| ApiError::bad_request("media must be an object"))?;

    let media_type = media_obj
        .get("type")
        .and_then(Value::as_str)
        .map(|v| v.to_ascii_lowercase())
        .ok_or_else(|| ApiError::bad_request("media.type is required"))?;

    let media_ref = media_obj
        .get("media")
        .ok_or_else(|| ApiError::bad_request("media.media is required"))?;

    let explicit_caption_entities = media_obj.get("caption_entities").cloned();
    let should_auto_detect_entities = explicit_caption_entities.is_none();
    let parse_mode = media_obj.get("parse_mode").and_then(Value::as_str);
    let sim_parse_mode = normalize_sim_parse_mode(parse_mode);
    let (caption, caption_entities) = parse_optional_formatted_text(
        media_obj.get("caption").and_then(Value::as_str),
        parse_mode,
        explicit_caption_entities,
    );
    let caption_entities = if should_auto_detect_entities {
        if let Some(caption_text) = caption.as_deref() {
            merge_auto_message_entities(caption_text, caption_entities)
        } else {
            None
        }
    } else {
        caption_entities
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageMedia",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    let media_payload = match media_type.as_str() {
        "photo" => {
            let file = resolve_media_file(state, token, media_ref, "photo")?;
            json!([
                {
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "width": 1280,
                    "height": 720,
                    "file_size": file.file_size,
                }
            ])
        }
        "video" => {
            let file = resolve_media_file(state, token, media_ref, "video")?;
            json!({
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "width": media_obj.get("width").and_then(Value::as_i64).unwrap_or(1280),
                "height": media_obj.get("height").and_then(Value::as_i64).unwrap_or(720),
                "duration": media_obj.get("duration").and_then(Value::as_i64).unwrap_or(0),
                "mime_type": file.mime_type,
                "file_size": file.file_size,
            })
        }
        "audio" => {
            let file = resolve_media_file(state, token, media_ref, "audio")?;
            json!({
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "duration": media_obj.get("duration").and_then(Value::as_i64).unwrap_or(0),
                "performer": media_obj.get("performer").and_then(Value::as_str),
                "title": media_obj.get("title").and_then(Value::as_str),
                "mime_type": file.mime_type,
                "file_size": file.file_size,
            })
        }
        "document" => {
            let file = resolve_media_file(state, token, media_ref, "document")?;
            json!({
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
                "mime_type": file.mime_type,
                "file_size": file.file_size,
            })
        }
        _ => {
            return Err(ApiError::bad_request(
                "editMessageMedia supports only photo, video, audio, and document",
            ));
        }
    };

    for key in ["photo", "video", "audio", "voice", "document", "animation", "video_note"] {
        edited_message.as_object_mut().map(|obj| obj.remove(key));
    }

    edited_message[media_type.as_str()] = media_payload;
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    if let Some(c) = caption {
        edited_message["caption"] = Value::String(c.clone());
        conn.execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![c, bot.id, chat_key, message_id],
        )
        .map_err(ApiError::internal)?;
    }
    if let Some(entities) = caption_entities {
        edited_message["caption_entities"] = entities;
    } else {
        edited_message.as_object_mut().map(|obj| obj.remove("caption_entities"));
    }
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_edit_message_live_location(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageLiveLocationRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageLiveLocation",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;

    if edited_message.get("location").is_none() && edited_message.get("venue").is_none() {
        return Err(ApiError::bad_request("message has no live location to edit"));
    }

    let updated_location = Location {
        latitude: request.latitude,
        longitude: request.longitude,
        horizontal_accuracy: request.horizontal_accuracy,
        live_period: request.live_period,
        heading: request.heading,
        proximity_alert_radius: request.proximity_alert_radius,
    };

    if edited_message.get("venue").is_some() {
        if let Some(venue_obj) = edited_message.get_mut("venue").and_then(Value::as_object_mut) {
            venue_obj.insert("location".to_string(), serde_json::to_value(updated_location).map_err(ApiError::internal)?);
        }
    } else {
        edited_message["location"] = serde_json::to_value(updated_location).map_err(ApiError::internal)?;
    }

    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_stop_message_live_location(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: StopMessageLiveLocationRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "stopMessageLiveLocation",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    if let Some(location_obj) = edited_message.get_mut("location").and_then(Value::as_object_mut) {
        location_obj.remove("live_period");
        location_obj.remove("heading");
        location_obj.remove("proximity_alert_radius");
    }
    if let Some(venue_obj) = edited_message.get_mut("venue").and_then(Value::as_object_mut) {
        if let Some(location_obj) = venue_obj.get_mut("location").and_then(Value::as_object_mut) {
            location_obj.remove("live_period");
            location_obj.remove("heading");
            location_obj.remove("proximity_alert_radius");
        }
    }

    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn load_reply_message_for_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
    reply_message_id: i64,
) -> Result<Value, ApiError> {
    let reply_chat_key: Option<String> = conn
        .query_row(
            "SELECT chat_key FROM messages WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, reply_message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(reply_chat_key) = reply_chat_key else {
        return Err(ApiError::not_found("reply message not found"));
    };

    if reply_chat_key != chat_key {
        return Err(ApiError::bad_request("reply message not found in this chat"));
    }

    load_message_value(conn, bot, reply_message_id)
}

fn send_sim_user_payload_message(
    state: &Data<AppState>,
    token: &str,
    chat_id: Option<i64>,
    message_thread_id: Option<i64>,
    direct_messages_topic_id: Option<i64>,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
    sender_chat_id: Option<i64>,
    business_connection_id: Option<String>,
    payload: SimUserPayload,
    text: Option<String>,
    entities: Option<Value>,
    reply_to_message_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, user_id, first_name, username)?;
    let send_kind = match &payload {
        SimUserPayload::Dice(_) | SimUserPayload::Game(_) | SimUserPayload::Contact(_) | SimUserPayload::Location(_) | SimUserPayload::Venue(_) => ChatSendKind::Other,
    };

    let resolved_chat_id = chat_id.unwrap_or(user.id);
    let sim_chat = resolve_sim_chat_for_user_message(&mut conn, bot.id, resolved_chat_id, &user)?;
    let is_direct_messages = is_direct_messages_chat(&sim_chat);
    if !is_direct_messages && sim_chat.chat_type != "private" {
        ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &sim_chat.chat_key,
            user.id,
            send_kind,
        )?;
    }

    let mut sender_chat_json: Option<Value> = None;
    let resolved_thread_id: Option<i64>;
    let mut direct_messages_topic_json: Option<Value> = None;

    if is_direct_messages {
        if message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not supported in channel direct messages simulation",
            ));
        }
        if sender_chat_id.is_some() {
            return Err(ApiError::bad_request(
                "sender_chat_id is not supported in channel direct messages simulation",
            ));
        }

        let (topic_id, topic_value, forced_sender_chat) = resolve_direct_messages_topic_for_sender(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            direct_messages_topic_id,
        )?;
        resolved_thread_id = Some(topic_id);
        direct_messages_topic_json = Some(topic_value);
        if let Some(forced_sender_chat) = forced_sender_chat {
            sender_chat_json = Some(serde_json::to_value(forced_sender_chat).map_err(ApiError::internal)?);
        }
    } else {
        if direct_messages_topic_id.is_some() {
            return Err(ApiError::bad_request(
                "direct_messages_topic_id is available only in channel direct messages chats",
            ));
        }

        let sender_chat = resolve_sender_chat_for_sim_user_message(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            sender_chat_id,
            send_kind,
        )?;
        sender_chat_json = sender_chat
            .as_ref()
            .map(|chat| serde_json::to_value(chat).map_err(ApiError::internal))
            .transpose()?;
        resolved_thread_id = resolve_forum_message_thread_id(
            &mut conn,
            bot.id,
            &sim_chat,
            message_thread_id,
        )?;
    }

    let business_connection_record = normalize_business_connection_id(business_connection_id.as_deref())
        .map(|connection_id| load_business_connection_or_404(&mut conn, bot.id, &connection_id))
        .transpose()?;
    let business_connection_id = if let Some(record) = business_connection_record.as_ref() {
        if is_direct_messages {
            return Err(ApiError::bad_request(
                "business_connection_id is not supported in channel direct messages simulation",
            ));
        }
        if !record.is_enabled {
            return Err(ApiError::bad_request("business connection is disabled"));
        }
        if sim_chat.chat_type != "private" || sim_chat.chat_id != record.user_chat_id || user.id != record.user_id {
            return Err(ApiError::bad_request("business connection does not match chat/user"));
        }
        Some(record.connection_id.clone())
    } else {
        None
    };

    let chat_key = sim_chat.chat_key.clone();
    let direct_messages_star_count = if is_direct_messages {
        direct_messages_star_count_for_chat(&mut conn, bot.id, &sim_chat)?
    } else {
        0
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, text.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    if is_direct_messages {
        if let Some(topic_id) = resolved_thread_id {
            let topic_owner_user_id = load_direct_messages_topic_record(&mut conn, bot.id, &chat_key, topic_id)?
                .map(|record| record.user_id)
                .unwrap_or(user.id);
            upsert_direct_messages_topic(
                &mut conn,
                bot.id,
                &chat_key,
                topic_id,
                topic_owner_user_id,
                Some(message_id),
                Some(now),
            )?;
        }
    }

    let from = build_user_from_sim_record(&user, false);
    let chat = chat_from_sim_record(&sim_chat, &user);

    let mut message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
    });

    if let Some(sender_chat_value) = sender_chat_json {
        message_json["sender_chat"] = sender_chat_value;
    }

    match payload {
        SimUserPayload::Dice(dice) => {
            message_json["dice"] = serde_json::to_value(dice).map_err(ApiError::internal)?;
        }
        SimUserPayload::Game(game) => {
            message_json["game"] = serde_json::to_value(game).map_err(ApiError::internal)?;
        }
        SimUserPayload::Contact(contact) => {
            message_json["contact"] = serde_json::to_value(contact).map_err(ApiError::internal)?;
        }
        SimUserPayload::Location(location) => {
            message_json["location"] = serde_json::to_value(location).map_err(ApiError::internal)?;
        }
        SimUserPayload::Venue(venue) => {
            message_json["venue"] = serde_json::to_value(venue).map_err(ApiError::internal)?;
        }
    }
    if let Some(t) = text {
        message_json["text"] = Value::String(t);
    }
    if let Some(e) = entities {
        message_json["entities"] = e;
    }
    if let Some(thread_id) = resolved_thread_id {
        message_json["message_thread_id"] = Value::from(thread_id);
        message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(direct_messages_topic) = direct_messages_topic_json {
        message_json["direct_messages_topic"] = direct_messages_topic;
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if is_direct_messages && direct_messages_star_count > 0 {
        message_json["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if let Some(mode) = sim_parse_mode {
        message_json["sim_parse_mode"] = Value::String(mode);
    }
    if let Some(reply_id) = reply_to_message_id {
        let reply_value = load_reply_message_for_chat(&mut conn, &bot, &chat_key, reply_id)?;
        message_json["reply_to_message"] = reply_value;
        enrich_reply_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            reply_id,
            &mut message_json,
        )?;
    }

    let is_channel_post = sim_chat.chat_type == "channel";
    if !is_channel_post && !is_direct_messages {
        map_discussion_message_to_channel_post_if_needed(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            reply_to_message_id,
        )?;
        enrich_message_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            &mut message_json,
        )?;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;
    let is_business_message = business_connection_id.is_some();
    let mut bot_visible = if is_direct_messages || is_business_message {
        true
    } else {
        should_emit_user_generated_update_to_bot(
            &mut conn,
            &bot,
            &sim_chat.chat_type,
            user.id,
            &message,
        )?
    };
    if !bot_visible && !is_direct_messages && (sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup") {
        bot_visible = is_reply_to_linked_discussion_root_message(
            &mut conn,
            bot.id,
            &chat_key,
            reply_to_message_id,
        )?;
    }
    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post || is_business_message {
            None
        } else {
            Some(message.clone())
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(message.clone())
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: if is_business_message {
            Some(message.clone())
        } else {
            None
        },
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json, bot_visible) VALUES (?1, ?2, ?3)",
        params![
            bot.id,
            serde_json::to_string(&update_stub).map_err(ApiError::internal)?,
            if bot_visible { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if is_direct_messages && direct_messages_star_count > 0 {
        message_value["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if is_business_message {
        update_value["business_message"] = message_value.clone();
    } else if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    if bot_visible {
        dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);
    }

    if is_channel_post {
        forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

pub fn handle_sim_send_user_dice(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserDiceRequest,
) -> ApiResult {
    let emoji = body.emoji.unwrap_or_else(|| "🎲".to_string());
    let max_value = match emoji.as_str() {
        "🎯" | "🎲" | "🏀" | "🎳" => 6,
        "⚽" | "🏐" => 5,
        "🎰" => 64,
        _ => return Err(ApiError::bad_request("unsupported dice emoji")),
    };
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default().unsigned_abs();
    let value = (now_nanos % (max_value as u64)) as i64 + 1;

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Dice(Dice { emoji, value }),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_game(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserGameRequest,
) -> ApiResult {
    if body.game_short_name.trim().is_empty() {
        return Err(ApiError::bad_request("game_short_name is empty"));
    }

    let game = Game {
        title: body.game_short_name.clone(),
        description: format!("Game {}", body.game_short_name),
        photo: vec![PhotoSize {
            file_id: generate_telegram_file_id("game_photo"),
            file_unique_id: generate_telegram_file_unique_id(),
            width: 512,
            height: 512,
            file_size: None,
        }],
        text: None,
        text_entities: None,
        animation: None,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Game(game),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_contact(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserContactRequest,
) -> ApiResult {
    if body.phone_number.trim().is_empty() {
        return Err(ApiError::bad_request("phone_number is empty"));
    }
    if body.contact_first_name.trim().is_empty() {
        return Err(ApiError::bad_request("contact_first_name is empty"));
    }

    let contact = Contact {
        phone_number: body.phone_number,
        first_name: body.contact_first_name,
        last_name: body.contact_last_name,
        user_id: None,
        vcard: body.vcard,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Contact(contact),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_location(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserLocationRequest,
) -> ApiResult {
    let location = Location {
        latitude: body.latitude,
        longitude: body.longitude,
        horizontal_accuracy: body.horizontal_accuracy,
        live_period: body.live_period,
        heading: body.heading,
        proximity_alert_radius: body.proximity_alert_radius,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Location(location),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_venue(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserVenueRequest,
) -> ApiResult {
    if body.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if body.address.trim().is_empty() {
        return Err(ApiError::bad_request("address is empty"));
    }

    let venue = Venue {
        location: Location {
            latitude: body.latitude,
            longitude: body.longitude,
            horizontal_accuracy: None,
            live_period: None,
            heading: None,
            proximity_alert_radius: None,
        },
        title: body.title,
        address: body.address,
        foursquare_id: body.foursquare_id,
        foursquare_type: body.foursquare_type,
        google_place_id: body.google_place_id,
        google_place_type: body.google_place_type,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Venue(venue),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

enum SimUserPayload {
    Dice(Dice),
    Game(Game),
    Contact(Contact),
    Location(Location),
    Venue(Venue),
}

fn handle_edit_message_caption(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageCaptionRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());

    let explicit_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let should_auto_detect_entities = explicit_entities.is_none();
    let (parsed_caption, parsed_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_entities,
    );
    let parsed_entities = if should_auto_detect_entities {
        if let Some(caption_text) = parsed_caption.as_deref() {
            merge_auto_message_entities(caption_text, parsed_entities)
        } else {
            None
        }
    } else {
        parsed_entities
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageCaption",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    if !message_has_media(&edited_message) {
        return Err(ApiError::bad_request(
            "message has no media caption to edit; use editMessageText",
        ));
    }
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    let new_caption = parsed_caption.unwrap_or_default();
    conn.execute(
        "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
        params![new_caption, bot.id, chat_key, message_id],
    )
    .map_err(ApiError::internal)?;

    edited_message["caption"] = Value::String(new_caption);
    if let Some(entities) = parsed_entities {
        edited_message["caption_entities"] = entities;
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("caption_entities"));
    }
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_edit_message_checklist(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageChecklistRequest = parse_request_with_legacy_checklist(params)?;
    if request.business_connection_id.trim().is_empty() {
        return Err(ApiError::bad_request("business_connection_id is empty"));
    }

    let checklist = normalize_input_checklist(&request.checklist)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let connection = load_business_connection_or_404(
        &mut conn,
        bot.id,
        request.business_connection_id.trim(),
    )?;
    if !connection.is_enabled {
        return Err(ApiError::bad_request("business connection is disabled"));
    }
    if connection.user_chat_id != request.chat_id {
        return Err(ApiError::bad_request(
            "business connection does not match target private chat",
        ));
    }

    let business_connection = build_business_connection(&mut conn, bot.id, &connection)?;
    ensure_business_right(
        &business_connection,
        |rights| rights.can_reply,
        "not enough rights to edit business checklists",
    )?;

    let chat_id_value = Value::from(request.chat_id);
    let (chat_key, _) = resolve_chat_key_and_id(&mut conn, bot.id, &chat_id_value)?;

    let exists_in_chat: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, request.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if exists_in_chat.is_none() {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    ensure_message_can_be_edited_by_bot(&mut conn, bot.id, &chat_key, request.message_id, false)?;

    let mut edited_message = load_message_value(&mut conn, &bot, request.message_id)?;
    if edited_message.get("checklist").is_none() {
        return Err(ApiError::bad_request("message has no checklist to edit"));
    }

    let message_connection_id = edited_message
        .get("business_connection_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if message_connection_id.as_deref() != Some(connection.connection_id.as_str()) {
        return Err(ApiError::bad_request(
            "business connection does not match message",
        ));
    }

    conn.execute(
        "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
        params![checklist.title.clone(), bot.id, &chat_key, request.message_id],
    )
    .map_err(ApiError::internal)?;

    edited_message["checklist"] = serde_json::to_value(&checklist).map_err(ApiError::internal)?;
    edited_message["business_connection_id"] = Value::String(connection.connection_id);
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;
    Ok(edited_message)
}

fn handle_edit_message_reply_markup(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageReplyMarkupRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageReplyMarkup",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn apply_inline_reply_markup(target: &mut Value, reply_markup: Option<InlineKeyboardMarkup>) {
    if let Some(markup) = reply_markup {
        if let Ok(value) = serde_json::to_value(markup) {
            target["reply_markup"] = value;
        }
    } else {
        target.as_object_mut().map(|obj| obj.remove("reply_markup"));
    }
}

fn handle_delete_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMessageRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, chat_id) = resolve_chat_key_and_id(&mut conn, bot.id, &request.chat_id)?;
    let direct_messages_chat = load_sim_chat_record(&mut conn, bot.id, &chat_key)?
        .filter(|chat| is_direct_messages_chat(chat));

    match ensure_message_can_be_deleted_by_actor(&mut conn, bot.id, &chat_key, request.message_id) {
        Ok(()) => {}
        Err(err) if err.code == 404 => return Ok(Value::Bool(false)),
        Err(err) => return Err(err),
    }

    if let Some(chat) = direct_messages_chat.as_ref() {
        emit_suggested_post_refunded_updates_before_delete(
            state,
            &mut conn,
            token,
            &bot,
            chat,
            &[request.message_id],
        )?;
    }

    let deleted = delete_messages_with_dependencies(
        &mut conn,
        bot.id,
        &chat_key,
        chat_id,
        &[request.message_id],
    )?;

    Ok(Value::Bool(deleted > 0))
}

fn handle_delete_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMessagesRequest = parse_request(params)?;
    let message_ids = request.message_ids.clone();

    if message_ids.is_empty() {
        return Ok(Value::Bool(true));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, chat_id) = resolve_chat_key_and_id(&mut conn, bot.id, &request.chat_id)?;
    let direct_messages_chat = load_sim_chat_record(&mut conn, bot.id, &chat_key)?
        .filter(|chat| is_direct_messages_chat(chat));

    let placeholders = std::iter::repeat("?")
        .take(message_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let existing_sql = format!(
        "SELECT message_id
         FROM messages
         WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
        placeholders,
    );
    let mut existing_bind_values = Vec::with_capacity(2 + message_ids.len());
    existing_bind_values.push(Value::from(bot.id));
    existing_bind_values.push(Value::from(chat_key.clone()));
    for id in &message_ids {
        existing_bind_values.push(Value::from(*id));
    }

    let mut existing_stmt = conn.prepare(&existing_sql).map_err(ApiError::internal)?;
    let existing_rows = existing_stmt
        .query_map(
            rusqlite::params_from_iter(existing_bind_values.iter().map(sql_value_to_rusqlite)),
            |row| row.get::<_, i64>(0),
        )
        .map_err(ApiError::internal)?;

    let existing_ids: Vec<i64> = existing_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(existing_stmt);

    for message_id in &existing_ids {
        ensure_message_can_be_deleted_by_actor(&mut conn, bot.id, &chat_key, *message_id)?;
    }

    if let Some(chat) = direct_messages_chat.as_ref() {
        emit_suggested_post_refunded_updates_before_delete(
            state,
            &mut conn,
            token,
            &bot,
            chat,
            &existing_ids,
        )?;
    }

    let deleted = delete_messages_with_dependencies(
        &mut conn,
        bot.id,
        &chat_key,
        chat_id,
        &existing_ids,
    )?;

    Ok(Value::Bool(deleted > 0))
}

fn handle_set_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetWebhookRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if request.url.trim().is_empty() {
        return Err(ApiError::bad_request("bad webhook: URL is empty"));
    }

    let secret_token = request.secret_token.unwrap_or_default();
    let max_connections = request.max_connections.unwrap_or(40);
    let ip_address = request.ip_address.unwrap_or_default();

    conn.execute(
        "INSERT INTO webhooks (bot_id, url, secret_token, max_connections, ip_address)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id) DO UPDATE SET
            url = excluded.url,
            secret_token = excluded.secret_token,
            max_connections = excluded.max_connections,
            ip_address = excluded.ip_address",
        params![bot.id, request.url, secret_token, max_connections, ip_address],
    )
    .map_err(ApiError::internal)?;

    if request.drop_pending_updates.unwrap_or(false) {
        conn.execute("DELETE FROM updates WHERE bot_id = ?1", params![bot.id])
            .map_err(ApiError::internal)?;
    }

    Ok(Value::Bool(true))
}

fn handle_delete_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteWebhookRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    conn.execute("DELETE FROM webhooks WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;

    if request.drop_pending_updates.unwrap_or(false) {
        conn.execute("DELETE FROM updates WHERE bot_id = ?1", params![bot.id])
            .map_err(ApiError::internal)?;
    }

    Ok(Value::Bool(true))
}

fn parse_request<T: DeserializeOwned>(params: &HashMap<String, Value>) -> Result<T, ApiError> {
    let object = Map::from_iter(params.iter().map(|(k, v)| (k.clone(), v.clone())));
    decode_request_value(Value::Object(object))
}

fn parse_request_with_legacy_checklist<T: DeserializeOwned>(
    params: &HashMap<String, Value>,
) -> Result<T, ApiError> {
    let object = Map::from_iter(params.iter().map(|(k, v)| (k.clone(), v.clone())));
    let normalized = normalize_legacy_checklist_request_payload(Value::Object(object));
    decode_request_value(normalized)
}

fn normalize_legacy_checklist_request_payload(payload: Value) -> Value {
    match payload {
        Value::Object(mut root) => {
            if let Some(checklist_value) = root.get_mut("checklist") {
                normalize_legacy_checklist_value(checklist_value);
            }

            if !root.contains_key("checklist") {
                if let Some(items_value) = root.remove("items") {
                    let mut checklist = Map::new();
                    checklist.insert(
                        "title".to_string(),
                        root.remove("title")
                            .unwrap_or_else(|| Value::String("Checklist".to_string())),
                    );
                    checklist.insert("tasks".to_string(), normalize_legacy_checklist_tasks(items_value));

                    if let Some(value) = root.remove("others_can_add_tasks") {
                        checklist.insert("others_can_add_tasks".to_string(), value);
                    }
                    if let Some(value) = root.remove("others_can_mark_tasks_as_done") {
                        checklist.insert("others_can_mark_tasks_as_done".to_string(), value);
                    }

                    root.insert("checklist".to_string(), Value::Object(checklist));
                }
            }

            Value::Object(root)
        }
        other => other,
    }
}

fn normalize_legacy_checklist_value(value: &mut Value) {
    if let Value::Object(checklist) = value {
        if checklist.get("tasks").is_none() {
            if let Some(items_value) = checklist.remove("items") {
                checklist.insert(
                    "tasks".to_string(),
                    normalize_legacy_checklist_tasks(items_value),
                );
            }
        }
    }
}

fn normalize_legacy_checklist_tasks(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(index, item)| normalize_legacy_checklist_task(item, index + 1))
                .collect(),
        ),
        other => other,
    }
}

fn normalize_legacy_checklist_task(value: Value, fallback_id: usize) -> Value {
    match value {
        Value::Object(mut task) => {
            if task.get("text").is_none() {
                if let Some(title) = task.remove("title") {
                    task.insert("text".to_string(), title);
                } else if let Some(label) = task.remove("label") {
                    task.insert("text".to_string(), label);
                }
            }

            if task.get("id").is_none() {
                task.insert("id".to_string(), Value::from(fallback_id as i64));
            }

            if task.get("is_done").is_none() {
                if let Some(checked) = task.remove("is_checked") {
                    task.insert("is_done".to_string(), checked);
                } else if let Some(checked) = task.remove("checked") {
                    task.insert("is_done".to_string(), checked);
                }
            }

            Value::Object(task)
        }
        Value::String(text) => json!({
            "id": fallback_id as i64,
            "text": text,
        }),
        other => json!({
            "id": fallback_id as i64,
            "text": other.to_string(),
        }),
    }
}

fn decode_request_value<T: DeserializeOwned>(payload: Value) -> Result<T, ApiError> {
    match serde_json::from_value(payload.clone()) {
        Ok(decoded) => Ok(decoded),
        Err(strict_error) => {
            let variants = vec![
                coerce_text_like_fields(payload.clone()),
                coerce_text_like_fields(coerce_string_scalars(payload.clone())),
                coerce_string_scalars(payload.clone()),
                coerce_scalar_strings(payload.clone()),
                coerce_scalar_strings(coerce_string_scalars(payload)),
            ];

            for candidate in variants {
                if let Ok(decoded) = serde_json::from_value(candidate) {
                    return Ok(decoded);
                }
            }

            Err(ApiError::bad_request(normalize_request_decode_error(
                &strict_error.to_string(),
            )))
        }
    }
}

fn coerce_text_like_fields(value: Value) -> Value {
    coerce_text_like_fields_for_key(None, value)
}

fn coerce_text_like_fields_for_key(field_name: Option<&str>, value: Value) -> Value {
    match value {
        Value::Number(number) if field_name.map(is_text_like_field_name).unwrap_or(false) => {
            Value::String(number.to_string())
        }
        Value::Bool(flag) if field_name.map(is_text_like_field_name).unwrap_or(false) => {
            Value::String(flag.to_string())
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| coerce_text_like_fields_for_key(field_name, item))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| {
                    let coerced = coerce_text_like_fields_for_key(Some(key.as_str()), item);
                    (key, coerced)
                })
                .collect(),
        ),
        other => other,
    }
}

fn is_text_like_field_name(field_name: &str) -> bool {
    matches!(
        field_name,
        "text"
            | "caption"
            | "question"
            | "description"
            | "title"
            | "comment"
            | "payload"
            | "first_name"
            | "last_name"
            | "username"
            | "phone_number"
            | "emoji"
            | "name"
            | "url"
            | "currency"
            | "provider_token"
            | "invoice_payload"
            | "business_connection_id"
            | "gift_id"
            | "owned_gift_id"
    )
}

fn coerce_string_scalars(value: Value) -> Value {
    match value {
        Value::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.eq_ignore_ascii_case("true") {
                return Value::Bool(true);
            }
            if trimmed.eq_ignore_ascii_case("false") {
                return Value::Bool(false);
            }
            if trimmed.eq_ignore_ascii_case("null") {
                return Value::Null;
            }
            if let Ok(integer) = trimmed.parse::<i64>() {
                return Value::Number(integer.into());
            }
            if let Ok(float_number) = trimmed.parse::<f64>() {
                if let Some(number) = serde_json::Number::from_f64(float_number) {
                    return Value::Number(number);
                }
            }
            Value::String(raw)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(coerce_string_scalars).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, coerce_string_scalars(item)))
                .collect(),
        ),
        other => other,
    }
}

fn coerce_scalar_strings(value: Value) -> Value {
    match value {
        Value::Number(number) => Value::String(number.to_string()),
        Value::Bool(flag) => Value::String(flag.to_string()),
        Value::Array(items) => Value::Array(items.into_iter().map(coerce_scalar_strings).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, coerce_scalar_strings(item)))
                .collect(),
        ),
        other => other,
    }
}

fn parse_request_ignoring_prefixed_fields<T: DeserializeOwned>(
    params: &HashMap<String, Value>,
    ignored_prefixes: &[&str],
) -> Result<T, ApiError> {
    if ignored_prefixes.is_empty() {
        return parse_request(params);
    }

    let filtered = params
        .iter()
        .filter(|(key, _)| {
            !ignored_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<String, Value>>();

    parse_request(&filtered)
}

fn normalize_request_decode_error(message: &str) -> String {
    if message.contains("expected struct InputFile") {
        return "can't parse InputFile JSON object".to_string();
    }
    message.to_string()
}

fn value_to_optional_bool_loose(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(v) => Some(*v),
        Value::Number(n) => {
            if n.as_i64() == Some(1) {
                Some(true)
            } else if n.as_i64() == Some(0) {
                Some(false)
            } else {
                None
            }
        }
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" | "" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

fn value_to_optional_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => Some(value.to_string()),
    }
}

fn parse_optional_formatted_text(
    text: Option<&str>,
    parse_mode: Option<&str>,
    explicit_entities: Option<Value>,
) -> (Option<String>, Option<Value>) {
    match text {
        Some(raw) if !raw.is_empty() => {
            let (plain, entities) = parse_formatted_text(raw, parse_mode, explicit_entities);
            (Some(plain), entities)
        }
        _ => (None, None),
    }
}

fn parse_formatted_text(
    text: &str,
    parse_mode: Option<&str>,
    explicit_entities: Option<Value>,
) -> (String, Option<Value>) {
    if let Some(entities) = explicit_entities {
        return (text.to_string(), Some(entities));
    }

    match parse_mode.map(|v| v.to_ascii_lowercase()) {
        Some(mode) if mode == "html" => {
            let (clean, entities) = parse_html_entities(text);
            (clean, entities_value(entities))
        }
        Some(mode) if mode == "markdown" || mode == "markdownv2" => {
            let (clean, entities) = parse_markdown_entities(text, mode == "markdownv2");
            (clean, entities_value(entities))
        }
        _ => (text.to_string(), None),
    }
}

fn merge_auto_message_entities(text: &str, entities: Option<Value>) -> Option<Value> {
    let mut merged_entities = match entities {
        Some(Value::Array(items)) => items,
        Some(other) => vec![other],
        None => Vec::new(),
    };

    let mut occupied = collect_occupied_entity_ranges(&merged_entities);
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "bot_command",
        find_auto_bot_command_spans,
    );
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "mention",
        find_auto_mention_spans,
    );
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "hashtag",
        find_auto_hashtag_spans,
    );
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "cashtag",
        find_auto_cashtag_spans,
    );

    merged_entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    entities_value(merged_entities)
}

fn collect_occupied_entity_ranges(entities: &[Value]) -> Vec<(usize, usize)> {
    entities
        .iter()
        .filter_map(|entity| {
            let offset = entity.get("offset").and_then(Value::as_i64)?;
            let length = entity.get("length").and_then(Value::as_i64)?;
            if offset < 0 || length <= 0 {
                return None;
            }
            let start = offset as usize;
            Some((start, start + length as usize))
        })
        .collect()
}

fn range_is_free(occupied: &[(usize, usize)], start: usize, end: usize) -> bool {
    occupied
        .iter()
        .all(|(range_start, range_end)| end <= *range_start || start >= *range_end)
}

fn append_auto_entities(
    text: &str,
    entities: &mut Vec<Value>,
    occupied: &mut Vec<(usize, usize)>,
    entity_type: &str,
    detector: fn(&str) -> Vec<(usize, usize)>,
) {
    for (start_byte, end_byte) in detector(text) {
        let start = utf16_len(&text[..start_byte]);
        let length = utf16_len(&text[start_byte..end_byte]);
        if length == 0 {
            continue;
        }
        let end = start + length;
        if !range_is_free(occupied, start, end) {
            continue;
        }

        entities.push(json!({
            "type": entity_type,
            "offset": start,
            "length": length,
        }));
        occupied.push((start, end));
    }
}

fn find_auto_bot_command_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '/' {
            continue;
        }
        if let Some(end) = match_bot_command_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

fn find_auto_mention_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '@' {
            continue;
        }
        if let Some(end) = match_mention_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

fn find_auto_hashtag_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '#' {
            continue;
        }
        if let Some(end) = match_hashtag_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

fn find_auto_cashtag_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '$' {
            continue;
        }
        if let Some(end) = match_cashtag_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

fn match_bot_command_at(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start + 1;
    if cursor >= bytes.len() || !bytes[cursor].is_ascii_alphabetic() {
        return None;
    }

    cursor += 1;
    while cursor < bytes.len()
        && is_ascii_entity_word_byte(bytes[cursor])
        && (cursor - (start + 1)) < 32
    {
        cursor += 1;
    }

    let mut end = cursor;
    if cursor < bytes.len() && bytes[cursor] == b'@' {
        let mut username_cursor = cursor + 1;
        let mut username_len = 0usize;

        while username_cursor < bytes.len()
            && is_ascii_entity_word_byte(bytes[username_cursor])
            && username_len < 32
        {
            username_cursor += 1;
            username_len += 1;
        }

        if username_len >= 5 {
            end = username_cursor;
        }
    }

    Some(end)
}

fn match_mention_at(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start + 1;
    let mut len = 0usize;

    while cursor < bytes.len() && is_ascii_entity_word_byte(bytes[cursor]) && len < 32 {
        cursor += 1;
        len += 1;
    }

    if len == 0 {
        return None;
    }

    Some(cursor)
}

fn match_hashtag_at(text: &str, start: usize) -> Option<usize> {
    let mut count = 0usize;
    let mut end = start + 1;

    for (rel, ch) in text[start + 1..].char_indices() {
        if !is_hashtag_char(ch) || count >= 64 {
            break;
        }
        count += 1;
        end = start + 1 + rel + ch.len_utf8();
    }

    if count == 0 {
        return None;
    }

    Some(end)
}

fn match_cashtag_at(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start + 1;
    let mut left_len = 0usize;

    while cursor < bytes.len() && bytes[cursor].is_ascii_alphabetic() && left_len < 8 {
        cursor += 1;
        left_len += 1;
    }

    if left_len == 0 {
        return None;
    }

    let mut end = cursor;
    if cursor < bytes.len() && bytes[cursor] == b'_' {
        let mut right_cursor = cursor + 1;
        let mut right_len = 0usize;

        while right_cursor < bytes.len() && bytes[right_cursor].is_ascii_alphabetic() && right_len < 8
        {
            right_cursor += 1;
            right_len += 1;
        }

        if right_len > 0 {
            end = right_cursor;
        }
    }

    Some(end)
}

fn is_ascii_entity_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_hashtag_char(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}

fn utf16_span_to_byte_range(text: &str, offset: usize, length: usize) -> Option<(usize, usize)> {
    let target_end = offset.checked_add(length)?;
    let mut utf16_pos = 0usize;
    let mut start_byte = None;
    let mut end_byte = None;

    for (byte_idx, ch) in text.char_indices() {
        if start_byte.is_none() && utf16_pos == offset {
            start_byte = Some(byte_idx);
        }
        if utf16_pos == target_end {
            end_byte = Some(byte_idx);
            break;
        }

        utf16_pos += ch.len_utf16();
        if utf16_pos > target_end {
            return None;
        }
    }

    if start_byte.is_none() && utf16_pos == offset {
        start_byte = Some(text.len());
    }
    if end_byte.is_none() && utf16_pos == target_end {
        end_byte = Some(text.len());
    }

    match (start_byte, end_byte) {
        (Some(start), Some(end)) if start <= end => Some((start, end)),
        _ => None,
    }
}

fn entities_value(entities: Vec<Value>) -> Option<Value> {
    if entities.is_empty() {
        None
    } else {
        Some(Value::Array(entities))
    }
}

fn parse_html_entities(text: &str) -> (String, Vec<Value>) {
    let mut out = String::new();
    let mut entities = Vec::new();
    let mut stack: Vec<(String, usize, Option<String>, bool)> = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'<' {
            if let Some(end) = text[i..].find('>') {
                let end_idx = i + end;
                let raw_tag = &text[i + 1..end_idx];
                let tag = raw_tag.trim();

                let is_close = tag.starts_with('/');
                let lower = tag.to_ascii_lowercase();

                if is_close {
                    let name = lower.trim_start_matches('/').trim();
                    let wanted = match name {
                        "b" | "strong" => Some("bold"),
                        "i" | "em" => Some("italic"),
                        "u" | "ins" => Some("underline"),
                        "s" | "strike" | "del" => Some("strikethrough"),
                        "span" => Some("spoiler"),
                        "tg-spoiler" => Some("spoiler"),
                        "blockquote" => Some("blockquote"),
                        "tg-emoji" => Some("custom_emoji"),
                        "tg-time" => Some("date_time"),
                        "code" => Some("code"),
                        "pre" => Some("pre"),
                        "a" => Some("text_link"),
                        _ => None,
                    };

                    if let Some(target) = wanted {
                        if let Some(pos) = stack.iter().rposition(|(kind, _, _, _)| kind == target) {
                            let (_, start, extra, is_expandable) = stack.remove(pos);
                            let len = utf16_len(&out).saturating_sub(start);
                            if len > 0 {
                                let mut entity = json!({
                                    "type": if target == "blockquote" && is_expandable {
                                        "expandable_blockquote"
                                    } else {
                                        target
                                    },
                                    "offset": start,
                                    "length": len,
                                });
                                if let Some(extra) = extra {
                                    if target == "text_link" {
                                        entity["url"] = Value::String(extra);
                                    } else if target == "custom_emoji" {
                                        entity["custom_emoji_id"] = Value::String(extra);
                                    } else if target == "date_time" {
                                        let unix = extra
                                            .split(';')
                                            .find_map(|seg| seg.strip_prefix("unix:"))
                                            .and_then(|v| v.parse::<i64>().ok())
                                            .unwrap_or(0);
                                        entity["unix_time"] = Value::from(unix);
                                        if let Some(fmt) = extra
                                            .split(';')
                                            .find_map(|seg| seg.strip_prefix("format:"))
                                        {
                                            entity["date_time_format"] = Value::String(fmt.to_string());
                                        }
                                    } else if target == "pre" {
                                        if let Some(lang) = extra.strip_prefix("lang:") {
                                            entity["language"] = Value::String(lang.to_string());
                                        }
                                    }
                                }
                                entities.push(entity);
                            }
                        }
                    }
                } else {
                    let mut parts = lower.split_whitespace();
                    let name = parts.next().unwrap_or("");
                    let kind = match name {
                        "b" | "strong" => Some("bold"),
                        "i" | "em" => Some("italic"),
                        "u" | "ins" => Some("underline"),
                        "s" | "strike" | "del" => Some("strikethrough"),
                        "span" if has_css_class(tag, "tg-spoiler") => Some("spoiler"),
                        "tg-spoiler" => Some("spoiler"),
                        "blockquote" => Some("blockquote"),
                        "tg-emoji" => Some("custom_emoji"),
                        "tg-time" => Some("date_time"),
                        "code" => Some("code"),
                        "pre" => Some("pre"),
                        "a" => Some("text_link"),
                        _ => None,
                    };

                    if let Some(entity_type) = kind {
                        if entity_type == "code" {
                            if let Some(language) = extract_code_language(tag) {
                                if let Some((_, _, pre_extra, _)) = stack
                                    .iter_mut()
                                    .rev()
                                    .find(|(kind, _, _, _)| kind == "pre")
                                {
                                    *pre_extra = Some(format!("lang:{}", language));
                                    i = end_idx + 1;
                                    continue;
                                }
                            }
                        }

                        let start = utf16_len(&out);
                        let expandable = entity_type == "blockquote" && lower.contains("expandable");
                        let url = if entity_type == "text_link" { extract_href(tag) } else { None };
                        let extra = if entity_type == "custom_emoji" {
                            extract_attr(tag, "emoji-id").map(|v| format!("custom_emoji_id:{}", v))
                        } else if entity_type == "date_time" {
                            extract_attr(tag, "unix").map(|unix| {
                                let mut payload = format!("unix:{}", unix);
                                if let Some(fmt) = extract_attr(tag, "format") {
                                    payload.push_str(&format!(";format:{}", fmt));
                                }
                                payload
                            })
                        } else {
                            None
                        };
                        if let Some(payload) = extra {
                            let stored = if let Some(v) = payload.strip_prefix("custom_emoji_id:") {
                                v.to_string()
                            } else {
                                payload
                            };
                            stack.push((entity_type.to_string(), start, Some(stored), expandable));
                        } else {
                            stack.push((entity_type.to_string(), start, url, expandable));
                        }
                    }
                }

                i = end_idx + 1;
                continue;
            }
        }

        if bytes[i] == b'&' {
            if let Some(end) = text[i..].find(';') {
                let end_idx = i + end;
                let entity = &text[i..=end_idx];
                if let Some(decoded) = decode_html_entity(entity) {
                    out.push_str(decoded);
                    i = end_idx + 1;
                    continue;
                }
            }
        }

        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    (out, entities)
}

fn parse_markdown_entities(text: &str, markdown_v2: bool) -> (String, Vec<Value>) {
    let mut out = String::new();
    let mut entities = Vec::new();
    let mut stack: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut i = 0;
    let mut line_start = true;

    while i < text.len() {
        if text[i..].starts_with("```") {
            if let Some((advance, code_text, language)) = parse_markdown_pre_block(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&code_text);
                let len = utf16_len(&code_text);
                if len > 0 {
                    let mut entity = json!({
                        "type": "pre",
                        "offset": start,
                        "length": len,
                    });
                    if let Some(lang) = language {
                        entity["language"] = Value::String(lang);
                    }
                    entities.push(entity);
                }
                i += advance;
                continue;
            }
        }

        if markdown_v2 && text[i..].starts_with("![") {
            if let Some((advance, label, url)) = parse_markdown_media_link(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&label);
                let len = utf16_len(&label);
                if len > 0 {
                    if let Some(id) = extract_query_param(&url, "id") {
                        if url.starts_with("tg://emoji") {
                            entities.push(json!({
                                "type": "custom_emoji",
                                "offset": start,
                                "length": len,
                                "custom_emoji_id": id,
                            }));
                        } else if url.starts_with("tg://time") {
                            let mut entity = json!({
                                "type": "date_time",
                                "offset": start,
                                "length": len,
                                "unix_time": extract_query_param(&url, "unix")
                                    .and_then(|v| v.parse::<i64>().ok())
                                    .unwrap_or(0),
                            });
                            if let Some(fmt) = extract_query_param(&url, "format") {
                                entity["date_time_format"] = Value::String(fmt);
                            }
                            entities.push(entity);
                        }
                    } else if url.starts_with("tg://time") {
                        let mut entity = json!({
                            "type": "date_time",
                            "offset": start,
                            "length": len,
                            "unix_time": extract_query_param(&url, "unix")
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0),
                        });
                        if let Some(fmt) = extract_query_param(&url, "format") {
                            entity["date_time_format"] = Value::String(fmt);
                        }
                        entities.push(entity);
                    }
                }
                i += advance;
                continue;
            }
        }

        if markdown_v2 && text[i..].starts_with('\\') {
            let next_start = i + 1;
            if next_start < text.len() {
                if let Some(ch) = text[next_start..].chars().next() {
                    out.push(ch);
                    line_start = ch == '\n';
                    i = next_start + ch.len_utf8();
                    continue;
                }
            }
            i += 1;
            continue;
        }

        if markdown_v2 && line_start && (text[i..].starts_with('>') || text[i..].starts_with("**>")) {
            let mut start_shift = 1;
            let mut forced_expandable = false;
            if text[i..].starts_with("**>") {
                start_shift = 3;
                forced_expandable = true;
            }
            let line_end = text[i..].find('\n').map(|v| i + v).unwrap_or(text.len());
            let raw_line = &text[i + start_shift..line_end];
            let trimmed_line = raw_line.trim_start();
            let is_expandable = forced_expandable || trimmed_line.trim_end().ends_with("||");
            let content = if is_expandable {
                trimmed_line.trim_end().trim_end_matches("||").trim_end()
            } else {
                trimmed_line
            };

            let start = utf16_len(&out);
            out.push_str(content);
            let len = utf16_len(content);
            if len > 0 {
                entities.push(json!({
                    "type": if is_expandable { "expandable_blockquote" } else { "blockquote" },
                    "offset": start,
                    "length": len,
                }));
            }

            if line_end < text.len() {
                out.push('\n');
                i = line_end + 1;
                line_start = true;
            } else {
                i = line_end;
                line_start = false;
            }
            continue;
        }

        if text[i..].starts_with('[') {
            if let Some((advance, link_text, link_url)) = parse_markdown_link(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&link_text);
                let len = utf16_len(&link_text);
                if len > 0 {
                    entities.push(json!({
                        "type": "text_link",
                        "offset": start,
                        "length": len,
                        "url": link_url,
                    }));
                }
                i += advance;
                continue;
            }
        }

        let mut matched = false;
        for (token, entity_type) in markdown_tokens(markdown_v2) {
            if !text[i..].starts_with(token) {
                continue;
            }

            matched = true;
            let start = utf16_len(&out);
            let entry = stack.entry(token).or_default();
            if let Some(open_start) = entry.pop() {
                let len = start.saturating_sub(open_start);
                if len > 0 {
                    entities.push(json!({
                        "type": entity_type,
                        "offset": open_start,
                        "length": len,
                    }));
                }
            } else {
                entry.push(start);
            }

            i += token.len();
            break;
        }

        if matched {
            continue;
        }

        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            line_start = ch == '\n';
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    (out, entities)
}

fn parse_markdown_pre_block(input: &str) -> Option<(usize, String, Option<String>)> {
    if !input.starts_with("```") {
        return None;
    }

    let rest = &input[3..];
    let mut language = None;
    let mut content_start = 3;

    if let Some(line_end) = rest.find('\n') {
        let header = rest[..line_end].trim();
        if !header.is_empty() {
            language = Some(header.to_string());
        }
        content_start = 3 + line_end + 1;
    }

    let body = &input[content_start..];
    let close_rel = body.find("```")?;
    let close_abs = content_start + close_rel;
    let content = &input[content_start..close_abs];
    let advance = close_abs + 3;

    Some((advance, content.to_string(), language))
}

fn markdown_tokens(markdown_v2: bool) -> Vec<(&'static str, &'static str)> {
    if markdown_v2 {
        vec![
            ("||", "spoiler"),
            ("__", "underline"),
            ("*", "bold"),
            ("_", "italic"),
            ("~", "strikethrough"),
            ("`", "code"),
        ]
    } else {
        vec![("*", "bold"), ("_", "italic"), ("`", "code")]
    }
}

fn parse_markdown_link(input: &str) -> Option<(usize, String, String)> {
    let close_text = input.find(']')?;
    let rest = &input[close_text + 1..];
    if !rest.starts_with('(') {
        return None;
    }
    let close_url = rest.find(')')?;
    let text = &input[1..close_text];
    let url = &rest[1..close_url];
    let advance = close_text + 1 + close_url + 1;
    Some((advance, text.to_string(), url.to_string()))
}

fn parse_markdown_media_link(input: &str) -> Option<(usize, String, String)> {
    if !input.starts_with("![") {
        return None;
    }
    let close_label = input.find(']')?;
    let rest = &input[close_label + 1..];
    if !rest.starts_with('(') {
        return None;
    }
    let close_url = rest.find(')')?;
    let label = &input[2..close_label];
    let url = &rest[1..close_url];
    let advance = close_label + 1 + close_url + 1;
    Some((advance, label.to_string(), url.to_string()))
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn extract_href(tag: &str) -> Option<String> {
    extract_attr(tag, "href")
}

fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let needle = format!("{}=", attr.to_ascii_lowercase());
    let attr_pos = lower.find(&needle)?;
    let raw = &tag[attr_pos + needle.len()..].trim_start();
    if let Some(rest) = raw.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(rest[..end].to_string());
    }
    if let Some(rest) = raw.strip_prefix('\'') {
        let end = rest.find('\'')?;
        return Some(rest[..end].to_string());
    }

    let end = raw.find(char::is_whitespace).unwrap_or(raw.len());
    Some(raw[..end].to_string())
}

fn has_css_class(tag: &str, class_name: &str) -> bool {
    extract_attr(tag, "class")
        .map(|v| {
            v.split_whitespace()
                .any(|part| part.eq_ignore_ascii_case(class_name))
        })
        .unwrap_or(false)
}

fn extract_code_language(tag: &str) -> Option<String> {
    let class_attr = extract_attr(tag, "class")?;
    class_attr
        .split_whitespace()
        .find_map(|part| part.strip_prefix("language-"))
        .map(|v| v.to_string())
}

fn extract_query_param(url: &str, key: &str) -> Option<String> {
    let query = url.split('?').nth(1)?;
    for part in query.split('&') {
        let mut seg = part.splitn(2, '=');
        let k = seg.next()?.trim();
        let v = seg.next().unwrap_or("").trim();
        if k.eq_ignore_ascii_case(key) {
            return Some(v.to_string());
        }
    }
    None
}

fn decode_html_entity(entity: &str) -> Option<&'static str> {
    match entity {
        "&lt;" => Some("<"),
        "&gt;" => Some(">"),
        "&amp;" => Some("&"),
        "&quot;" => Some("\""),
        "&#39;" => Some("'"),
        "&apos;" => Some("'"),
        _ => None,
    }
}

fn normalize_reaction_values(raw: Option<Vec<Value>>) -> Result<Vec<Value>, ApiError> {
    let Some(items) = raw else {
        return Ok(Vec::new());
    };

    let mut normalized = Vec::<Value>::new();
    for item in items {
        let obj = item
            .as_object()
            .ok_or_else(|| ApiError::bad_request("reaction item must be an object"))?;

        let reaction_type = obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("emoji")
            .to_ascii_lowercase();

        if reaction_type == "paid" {
            normalized.push(json!({
                "type": "paid",
            }));
            continue;
        }

        let value = if reaction_type == "emoji" {
            let emoji = obj
                .get("emoji")
                .and_then(Value::as_str)
                .ok_or_else(|| ApiError::bad_request("reaction emoji is required"))?
                .trim()
                .to_string();

            if emoji.is_empty() {
                return Err(ApiError::bad_request("reaction emoji is empty"));
            }

            if !is_allowed_telegram_reaction_emoji(&emoji) {
                return Err(ApiError::bad_request("reaction emoji is not allowed"));
            }

            json!({
                "type": "emoji",
                "emoji": emoji,
            })
        } else {
            return Err(ApiError::bad_request(
                "only emoji and paid reactions are supported in simulator",
            ));
        };

        if !normalized.iter().any(|existing| existing == &value) {
            normalized.push(value);
        }
    }

    Ok(normalized)
}

fn is_allowed_telegram_reaction_emoji(emoji: &str) -> bool {
    const ALLOWED: &[&str] = &[
        "👍", "👎", "❤", "🔥", "🥰", "👏", "😁", "🤔", "🤯", "😱", "🤬", "😢",
        "🎉", "🤩", "🤮", "💩", "🙏", "👌", "🕊", "🤡", "🥱", "🥴", "😍", "🐳",
        "❤‍🔥", "🌚", "🌭", "💯", "🤣", "⚡", "🍌", "🏆", "💔", "🤨", "😐", "🍓",
        "🍾", "💋", "🖕", "😈", "😴", "😭", "🤓", "👻", "👨‍💻", "👀", "🎃", "🙈",
        "😇", "😨", "🤝", "✍", "🤗", "🫡", "🎅", "🎄", "☃", "💅", "🤪", "🗿",
        "🆒", "💘", "🙉", "🦄", "😘", "💊", "🙊", "😎", "👾", "🤷‍♂", "🤷", "😡",
    ];

    ALLOWED.contains(&emoji)
}

fn apply_message_reaction_change(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    token: &str,
    chat_key: &str,
    chat_id: i64,
    message_id: i64,
    actor: User,
    new_reaction: Vec<Value>,
) -> ApiResult {
    let message_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if message_exists.is_none() {
        return Err(ApiError::not_found("message to react was not found"));
    }

    let has_paid_reaction = new_reaction.iter().any(|reaction| {
        reaction
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| kind.eq_ignore_ascii_case("paid"))
            .unwrap_or(false)
    });

    if has_paid_reaction {
        let Some(sim_chat) = load_sim_chat_record(conn, bot.id, chat_key)? else {
            return Err(ApiError::bad_request(
                "paid reactions are available only in channels",
            ));
        };

        if sim_chat.chat_type != "channel" {
            return Err(ApiError::bad_request(
                "paid reactions are available only in channels",
            ));
        }

        if !sim_chat.channel_paid_reactions_enabled {
            return Err(ApiError::bad_request(
                "paid star reactions are disabled for this channel",
            ));
        }
    }

    let now = Utc::now().timestamp();
    let actor_is_bot = if actor.is_bot { 1 } else { 0 };

    let old_reaction_json: Option<String> = conn
        .query_row(
            "SELECT reactions_json FROM message_reactions
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND actor_user_id = ?4 AND actor_is_bot = ?5",
            params![bot.id, chat_key, message_id, actor.id, actor_is_bot],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let old_reaction: Vec<Value> = old_reaction_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<Value>>(raw).ok())
        .unwrap_or_default();

    if new_reaction.is_empty() {
        conn.execute(
            "DELETE FROM message_reactions
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND actor_user_id = ?4 AND actor_is_bot = ?5",
            params![bot.id, chat_key, message_id, actor.id, actor_is_bot],
        )
        .map_err(ApiError::internal)?;
    } else {
        let serialized = serde_json::to_string(&new_reaction).map_err(ApiError::internal)?;
        conn.execute(
            "INSERT INTO message_reactions (bot_id, chat_key, message_id, actor_user_id, actor_is_bot, reactions_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(bot_id, chat_key, message_id, actor_user_id, actor_is_bot)
             DO UPDATE SET reactions_json = excluded.reactions_json, updated_at = excluded.updated_at",
            params![bot.id, chat_key, message_id, actor.id, actor_is_bot, serialized, now],
        )
        .map_err(ApiError::internal)?;
    }

    let count_payload = {
        let mut counts: HashMap<String, (ReactionType, i64)> = HashMap::new();
        let mut stmt = conn
            .prepare(
                "SELECT reactions_json FROM message_reactions
                 WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            )
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![bot.id, chat_key, message_id], |row| row.get::<_, String>(0))
            .map_err(ApiError::internal)?;

        for row in rows {
            let raw = row.map_err(ApiError::internal)?;
            if let Ok(reactions) = serde_json::from_str::<Vec<Value>>(&raw) {
                for reaction in reactions {
                    let key = serde_json::to_string(&reaction).map_err(ApiError::internal)?;
                    let reaction_type: ReactionType =
                        serde_json::from_value(reaction).map_err(ApiError::internal)?;
                    let entry = counts.entry(key).or_insert((reaction_type, 0));
                    entry.1 += 1;
                }
            }
        }

        let mut payload = Vec::<ReactionCount>::new();
        for (_, (reaction_type, total_count)) in counts {
            payload.push(ReactionCount {
                r#type: reaction_type,
                total_count,
            });
        }
        payload
    };

    let chat = Chat {
        id: chat_id,
        r#type: "private".to_string(),
        title: None,
        username: None,
        first_name: None,
        last_name: None,
        is_forum: None,
        is_direct_messages: None,
    };
    let old_reaction_types: Vec<ReactionType> = old_reaction
        .into_iter()
        .map(|value| serde_json::from_value(value).map_err(ApiError::internal))
        .collect::<Result<Vec<_>, _>>()?;
    let new_reaction_types: Vec<ReactionType> = new_reaction
        .into_iter()
        .map(|value| serde_json::from_value(value).map_err(ApiError::internal))
        .collect::<Result<Vec<_>, _>>()?;

    let linked_context = load_linked_discussion_mapping_for_message(
        conn,
        bot.id,
        chat_key,
        message_id,
    )?;

    let mut reaction_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: Some(MessageReactionUpdated {
            chat: chat.clone(),
            message_id,
            user: Some(actor),
            actor_chat: None,
            date: now,
            old_reaction: old_reaction_types,
            new_reaction: new_reaction_types,
        }),
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    if let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = linked_context.as_ref() {
        if let Some(obj) = reaction_update
            .get_mut("message_reaction")
            .and_then(Value::as_object_mut)
        {
            obj.insert(
                "linked_channel_message_id".to_string(),
                Value::from(*channel_message_id),
            );
            obj.insert(
                "linked_discussion_root_message_id".to_string(),
                Value::from(*discussion_root_message_id),
            );
            obj.insert(
                "linked_channel_chat_id".to_string(),
                Value::String(channel_chat_key.clone()),
            );
        }
    }

    persist_and_dispatch_update(state, conn, token, bot.id, reaction_update)?;

    let mut reaction_count_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: Some(MessageReactionCountUpdated {
            chat,
            message_id,
            date: now,
            reactions: count_payload,
        }),
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    if let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = linked_context.as_ref() {
        if let Some(obj) = reaction_count_update
            .get_mut("message_reaction_count")
            .and_then(Value::as_object_mut)
        {
            obj.insert(
                "linked_channel_message_id".to_string(),
                Value::from(*channel_message_id),
            );
            obj.insert(
                "linked_discussion_root_message_id".to_string(),
                Value::from(*discussion_root_message_id),
            );
            obj.insert(
                "linked_channel_chat_id".to_string(),
                Value::String(channel_chat_key.clone()),
            );
        }
    }

    persist_and_dispatch_update(state, conn, token, bot.id, reaction_count_update)?;

    Ok(json!(true))
}

fn persist_and_dispatch_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    mut update_value: Value,
) -> Result<(), ApiError> {
    enrich_channel_post_payloads(conn, bot_id, &mut update_value)?;

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot_id, update_value.to_string()],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    update_value["update_id"] = json!(update_id);

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(state, conn, bot_id, clean_update.clone());

    if let Some(channel_post_value) = clean_update.get("channel_post") {
        let bot_record: Option<crate::database::BotInfoRecord> = conn
            .query_row(
                "SELECT id, first_name, username FROM bots WHERE id = ?1",
                params![bot_id],
                |row| {
                    Ok(crate::database::BotInfoRecord {
                        id: row.get(0)?,
                        first_name: row.get(1)?,
                        username: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(bot_record) = bot_record {
            if let Some(chat_id_value) = channel_post_value
                .get("chat")
                .and_then(Value::as_object)
                .and_then(|chat| chat.get("id"))
            {
                let channel_chat_key = value_to_chat_key(chat_id_value)?;
                forward_channel_post_to_linked_discussion_best_effort(
                    state,
                    conn,
                    token,
                    &bot_record,
                    &channel_chat_key,
                    channel_post_value,
                );
            }
        }
    }

    Ok(())
}

const CHANNEL_VIEW_WINDOW_SECONDS: i64 = 24 * 60 * 60;

fn load_channel_post_views(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
) -> Result<i64, ApiError> {
    let views: Option<i64> = conn
        .query_row(
            "SELECT views
             FROM sim_channel_post_stats
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot_id, chat_key, message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    Ok(views.unwrap_or(0).max(0))
}

fn store_channel_post_views(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    views: i64,
    updated_at: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_channel_post_stats (bot_id, chat_key, message_id, views, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, chat_key, message_id)
         DO UPDATE SET views = excluded.views, updated_at = excluded.updated_at",
        params![bot_id, chat_key, message_id, views.max(0), updated_at],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn mark_channel_post_view_for_user(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    viewer_user_id: i64,
) -> Result<(i64, bool), ApiError> {
    let now = Utc::now().timestamp();
    let last_viewed_at: Option<i64> = conn
        .query_row(
            "SELECT viewed_at
             FROM sim_channel_post_viewers
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND viewer_user_id = ?4",
            params![bot_id, chat_key, message_id, viewer_user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let should_increment = last_viewed_at
        .map(|seen_at| now.saturating_sub(seen_at) >= CHANNEL_VIEW_WINDOW_SECONDS)
        .unwrap_or(true);
    if !should_increment {
        return Ok((load_channel_post_views(conn, bot_id, chat_key, message_id)?, false));
    }

    conn.execute(
        "INSERT INTO sim_channel_post_viewers (bot_id, chat_key, message_id, viewer_user_id, viewed_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, chat_key, message_id, viewer_user_id)
         DO UPDATE SET viewed_at = excluded.viewed_at",
        params![bot_id, chat_key, message_id, viewer_user_id, now],
    )
    .map_err(ApiError::internal)?;

    let next_views = load_channel_post_views(conn, bot_id, chat_key, message_id)?
        .saturating_add(1);
    store_channel_post_views(conn, bot_id, chat_key, message_id, next_views, now)?;

    Ok((next_views, true))
}

fn channel_show_author_signature_enabled(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<bool, ApiError> {
    Ok(load_sim_chat_record(conn, bot_id, chat_key)?
        .map(|record| record.channel_show_author_signature)
        .unwrap_or(false))
}

fn derive_channel_author_signature(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_obj: &Map<String, Value>,
) -> Option<String> {
    if let Some(actor_user_id) = current_request_actor_user_id() {
        let actor_record = load_chat_member_record(conn, bot_id, chat_key, actor_user_id)
            .ok()
            .flatten();
        if let Some(record) = actor_record {
            let actor_can_publish = record.status == "owner"
                || (record.status == "admin"
                    && channel_admin_can_publish(&parse_channel_admin_rights_json(
                        record.admin_rights_json.as_deref(),
                    )));
            if actor_can_publish {
                if let Ok(Some(user)) = load_sim_user_record(conn, actor_user_id) {
                    if !user.first_name.trim().is_empty() {
                        return Some(user.first_name);
                    }
                }
            }
        }
    }

    if let Some(from_first_name) = message_obj
        .get("from")
        .and_then(Value::as_object)
        .and_then(|from| from.get("first_name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(from_first_name.to_string());
    }

    message_obj
        .get("sender_chat")
        .and_then(Value::as_object)
        .and_then(|sender_chat| sender_chat.get("title"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn enrich_channel_post_payloads(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update_value: &mut Value,
) -> Result<(), ApiError> {
    let Some(update_obj) = update_value.as_object_mut() else {
        return Ok(());
    };

    for field in ["channel_post", "edited_channel_post"] {
        let Some(message_value) = update_obj.get_mut(field) else {
            continue;
        };
        let Some(message_obj) = message_value.as_object_mut() else {
            continue;
        };

        let Some(chat_obj) = message_obj.get("chat").and_then(Value::as_object) else {
            continue;
        };
        let Some(chat_id_value) = chat_obj.get("id") else {
            continue;
        };
        let Ok(chat_key) = value_to_chat_key(chat_id_value) else {
            continue;
        };

        let is_channel_chat = chat_obj
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| kind == "channel")
            .unwrap_or(false);
        if !is_channel_chat {
            continue;
        }

        if !message_obj.contains_key("sender_chat") {
            message_obj.insert(
                "sender_chat".to_string(),
                Value::Object(chat_obj.clone()),
            );
        }

        let show_author_signature = channel_show_author_signature_enabled(conn, bot_id, &chat_key)?;

        let has_signature = message_obj
            .get("author_signature")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(|value| !value.is_empty())
            .unwrap_or(false);
        if show_author_signature && !has_signature {
            if let Some(signature) = derive_channel_author_signature(conn, bot_id, &chat_key, message_obj) {
                message_obj.insert("author_signature".to_string(), Value::String(signature));
            }
        } else if !show_author_signature {
            message_obj.remove("author_signature");
        }

        if let Some(message_id) = message_obj.get("message_id").and_then(Value::as_i64) {
            let views = load_channel_post_views(conn, bot_id, &chat_key, message_id)?;
            message_obj.insert("views".to_string(), Value::from(views));
        }
    }

    Ok(())
}

fn display_name_for_service_user(user: &User) -> String {
    if !user.first_name.trim().is_empty() {
        return user.first_name.clone();
    }

    if let Some(username) = user.username.as_ref() {
        if !username.trim().is_empty() {
            return format!("@{}", username);
        }
    }

    format!("user_{}", user.id)
}

fn service_text_new_chat_members(actor: &User, members: &[User]) -> String {
    let actor_name = display_name_for_service_user(actor);
    let member_names: Vec<String> = members
        .iter()
        .map(display_name_for_service_user)
        .collect();

    if members.len() == 1 && members[0].id == actor.id {
        format!("{} joined the group", actor_name)
    } else {
        format!("{} added {}", actor_name, member_names.join(", "))
    }
}

fn service_text_left_chat_member(actor: &User, left_member: &User) -> String {
    let actor_name = display_name_for_service_user(actor);
    let left_name = display_name_for_service_user(left_member);

    if actor.id == left_member.id {
        format!("{} left the group", left_name)
    } else {
        format!("{} removed {}", actor_name, left_name)
    }
}

fn service_text_group_title_changed(actor: &User, new_title: &str) -> String {
    format!(
        "{} changed the group name to \"{}\"",
        display_name_for_service_user(actor),
        new_title,
    )
}

fn service_text_chat_created(actor: &User, chat_type: &str) -> String {
    let actor_name = display_name_for_service_user(actor);
    match chat_type {
        "supergroup" => format!("{} created the supergroup", actor_name),
        "channel" => format!("{} created the channel", actor_name),
        _ => format!("{} created the group", actor_name),
    }
}

fn emit_service_message_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat_key: &str,
    chat: &Chat,
    from: &User,
    date: i64,
    text: String,
    service_fields: Map<String, Value>,
) -> Result<(), ApiError> {
    let persisted_text = text;
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot_id, chat_key, from.id, &persisted_text, date],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_json = json!({
        "message_id": message_id,
        "date": date,
        "chat": chat,
        "from": from,
        "text": persisted_text
    });
    for (key, value) in service_fields {
        message_json[key] = value;
    }

    let is_channel_post = chat.r#type == "channel";
    let update_value = if is_channel_post {
        json!({
            "update_id": 0,
            "channel_post": message_json,
        })
    } else {
        json!({
            "update_id": 0,
            "message": message_json,
        })
    };

    persist_and_dispatch_update(state, conn, token, bot_id, update_value)
}

fn load_message_value(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    message_id: i64,
) -> Result<Value, ApiError> {
    let row: Option<(String, i64, String, i64)> = conn
        .query_row(
            "SELECT chat_key, from_user_id, text, date FROM messages WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, from_user_id, text, date)) = row else {
        return Err(ApiError::not_found("message not found"));
    };

    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(&chat_key));
    let (is_bot, first_name, username) = if from_user_id == bot.id {
        (true, bot.first_name.clone(), Some(bot.username.clone()))
    } else {
        let user: Option<(String, Option<String>)> = conn
            .query_row(
                "SELECT first_name, username FROM users WHERE id = ?1",
                params![from_user_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;
        let (first, uname) = user.unwrap_or_else(|| ("User".to_string(), None));
        (false, first, uname)
    };

    let mut message = find_message_snapshot(conn, bot.id, message_id).unwrap_or_else(|| {
        json!({
            "message_id": message_id,
            "date": date,
            "from": {
                "id": from_user_id,
                "is_bot": is_bot,
                "first_name": first_name,
                "username": username
            }
        })
    });

    let chat = if let Some(sim_chat) = load_sim_chat_record(conn, bot.id, &chat_key).ok().flatten() {
        let sender = SimUserRecord {
            id: from_user_id,
            first_name: first_name.clone(),
            username: username.clone(),
            last_name: None,
            is_premium: false,
        };
        chat_from_sim_record(&sim_chat, &sender)
    } else {
        Chat {
            id: chat_id,
            r#type: "private".to_string(),
            title: None,
            username: username.clone(),
            first_name: Some(first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        }
    };

    message["message_id"] = Value::from(message_id);
    message["date"] = Value::from(date);
    message.as_object_mut().map(|obj| obj.remove("edit_date"));
    message["chat"] = serde_json::to_value(chat).map_err(ApiError::internal)?;
    message["from"] = json!({
        "id": from_user_id,
        "is_bot": is_bot,
        "first_name": first_name,
        "username": username
    });

    if message_has_media(&message) {
        message.as_object_mut().map(|obj| obj.remove("text"));
        message["caption"] = Value::String(text);
    } else {
        message.as_object_mut().map(|obj| obj.remove("caption"));
        message["text"] = Value::String(text);
    }

    Ok(message)
}

fn handle_reply_markup_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    reply_markup: Option<&Value>,
) -> Result<Option<Value>, ApiError> {
    let Some(markup_value) = reply_markup else {
        return Ok(None);
    };

    if markup_value.get("keyboard").is_some() {
        let normalized_markup_value = normalize_legacy_reply_keyboard_markup(markup_value);
        let parsed: ReplyKeyboardMarkup = serde_json::from_value(normalized_markup_value)
            .map_err(|_| ApiError::bad_request("reply_markup keyboard is invalid"))?;

        if parsed.keyboard.is_empty() {
            return Err(ApiError::bad_request("reply_markup keyboard must have at least one row"));
        }

        if parsed
            .keyboard
            .iter()
            .any(|row| row.is_empty() || row.iter().any(|button| button.text.trim().is_empty()))
        {
            return Err(ApiError::bad_request("keyboard rows/buttons must not be empty"));
        }

        let normalized = serde_json::to_value(parsed).map_err(ApiError::internal)?;
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO chat_reply_keyboards (bot_id, chat_key, markup_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(bot_id, chat_key)
             DO UPDATE SET markup_json = excluded.markup_json, updated_at = excluded.updated_at",
            params![bot_id, chat_key, normalized.to_string(), now],
        )
        .map_err(ApiError::internal)?;

        return Ok(Some(normalized));
    }

    if markup_value.get("remove_keyboard").is_some() {
        let parsed: ReplyKeyboardRemove = serde_json::from_value(markup_value.clone())
            .map_err(|_| ApiError::bad_request("reply_markup remove_keyboard is invalid"))?;

        if !parsed.remove_keyboard {
            return Err(ApiError::bad_request("remove_keyboard must be true"));
        }

        conn.execute(
            "DELETE FROM chat_reply_keyboards WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot_id, chat_key],
        )
        .map_err(ApiError::internal)?;

        let normalized = serde_json::to_value(parsed).map_err(ApiError::internal)?;
        return Ok(Some(normalized));
    }

    Ok(Some(markup_value.clone()))
}

fn normalize_legacy_reply_keyboard_markup(markup_value: &Value) -> Value {
    let mut normalized = markup_value.clone();
    let Some(rows) = normalized
        .get_mut("keyboard")
        .and_then(Value::as_array_mut)
    else {
        return normalized;
    };

    for row in rows {
        let Some(buttons) = row.as_array_mut() else {
            continue;
        };

        for button in buttons {
            let Some(button_obj) = button.as_object_mut() else {
                continue;
            };

            if button_obj.contains_key("request_users") {
                continue;
            }

            let Some(legacy_request_user) = button_obj.get("request_user") else {
                continue;
            };

            if let Some(request_users) = normalize_legacy_request_user_payload(legacy_request_user)
            {
                button_obj.insert("request_users".to_string(), request_users);
            }
        }
    }

    normalized
}

fn normalize_legacy_request_user_payload(legacy_request_user: &Value) -> Option<Value> {
    let legacy = legacy_request_user.as_object()?;
    let request_id = legacy.get("request_id").and_then(|raw| {
        raw.as_i64()
            .or_else(|| raw.as_str().and_then(|v| v.trim().parse::<i64>().ok()))
    })?;

    let mut normalized = Map::new();
    normalized.insert("request_id".to_string(), Value::from(request_id));
    normalized.insert("max_quantity".to_string(), Value::from(10));

    let mappings = [
        ("user_is_bot", "user_is_bot"),
        ("user_is_premium", "user_is_premium"),
        ("request_name", "request_name"),
        ("request_username", "request_username"),
        ("request_photo", "request_photo"),
    ];

    for (legacy_key, target_key) in mappings {
        if let Some(value) = legacy
            .get(legacy_key)
            .and_then(value_to_optional_bool_loose)
        {
            normalized.insert(target_key.to_string(), Value::Bool(value));
        }
    }

    Some(Value::Object(normalized))
}

fn find_message_snapshot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    message_id: i64,
) -> Option<Value> {
    let mut stmt = conn
        .prepare(
            "SELECT update_json FROM updates WHERE bot_id = ?1 ORDER BY update_id DESC LIMIT 5000",
        )
        .ok()?;

    let rows = stmt
        .query_map(params![bot_id], |row| row.get::<_, String>(0))
        .ok()?;

    for row in rows {
        let raw = row.ok()?;
        let update_value: Value = serde_json::from_str(&raw).ok()?;

        if let Some(msg) = update_value
            .get("edited_channel_post")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("channel_post")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("edited_message")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("message")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }
    }

    None
}

fn message_has_media(message: &Value) -> bool {
    ["photo", "video", "audio", "voice", "document", "animation", "video_note"]
        .iter()
        .any(|key| message.get(*key).is_some())
}

fn sql_value_to_rusqlite(v: &Value) -> rusqlite::types::Value {
    match v {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Null
            }
        }
        Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        _ => rusqlite::types::Value::Text(v.to_string()),
    }
}

fn value_to_chat_key(v: &Value) -> Result<String, ApiError> {
    match v {
        Value::String(s) if !s.trim().is_empty() => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(ApiError::bad_request("chat_id is empty or invalid")),
    }
}

fn chat_id_as_i64(chat_id: &Value, chat_key: &str) -> i64 {
    match chat_id {
        Value::Number(n) => n.as_i64().unwrap_or_else(|| fallback_chat_id(chat_key)),
        Value::String(s) => s
            .parse::<i64>()
            .unwrap_or_else(|_| fallback_chat_id(s)),
        _ => fallback_chat_id(chat_key),
    }
}

fn fallback_chat_id(input: &str) -> i64 {
    let mut acc: i64 = 0;
    for b in input.as_bytes() {
        acc = acc.wrapping_mul(31).wrapping_add(*b as i64);
    }
    -acc.abs().max(1)
}

fn resolve_bot_outbound_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id_value: &Value,
    send_kind: ChatSendKind,
) -> Result<(String, Chat), ApiError> {
    let requested_chat_key = value_to_chat_key(chat_id_value)?;
    ensure_chat(conn, &requested_chat_key)?;
    let chat_id = chat_id_as_i64(chat_id_value, &requested_chat_key);

    let sim_chat = load_sim_chat_record(conn, bot_id, &requested_chat_key)?
        .or(load_sim_chat_record_by_chat_id(conn, bot_id, chat_id)?);

    if let Some(sim_chat) = sim_chat {
        let sim_chat_key = sim_chat.chat_key.clone();
        if sim_chat.chat_type != "private" {
            let actor_user_id = current_request_actor_user_id().unwrap_or(bot_id);
            let outbound_sender_user_id = if sim_chat.chat_type == "channel" {
                actor_user_id
            } else {
                bot_id
            };

            if is_direct_messages_chat(&sim_chat) {
                let parent_channel_chat_id = sim_chat
                    .parent_channel_chat_id
                    .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
                ensure_channel_member_can_manage_direct_messages(
                    conn,
                    bot_id,
                    &parent_channel_chat_id.to_string(),
                    actor_user_id,
                )?;
                let _ = direct_messages_star_count_for_chat(conn, bot_id, &sim_chat)?;
            } else if sim_chat.chat_type == "channel" {
                if actor_user_id == bot_id {
                    ensure_bot_is_chat_admin_or_owner(conn, bot_id, &sim_chat_key)?;
                }
                ensure_request_actor_can_publish_to_channel(conn, bot_id, &sim_chat_key)?;
            }
            ensure_sender_can_send_in_chat(
                conn,
                bot_id,
                &sim_chat_key,
                outbound_sender_user_id,
                send_kind,
            )?;
            let is_supergroup = sim_chat.chat_type == "supergroup";
            return Ok((
                sim_chat_key,
                Chat {
                    id: sim_chat.chat_id,
                    r#type: sim_chat.chat_type,
                    title: sim_chat.title,
                    username: sim_chat.username,
                    first_name: None,
                    last_name: None,
                    is_forum: if is_supergroup && !sim_chat.is_direct_messages {
                        Some(sim_chat.is_forum)
                    } else {
                        None
                    },
                    is_direct_messages: if sim_chat.is_direct_messages {
                        Some(true)
                    } else {
                        None
                    },
                },
            ));
        }

        let recipient = load_sim_user_record(conn, sim_chat.chat_id)?;
        return Ok((
            sim_chat_key,
            Chat {
                id: sim_chat.chat_id,
                r#type: "private".to_string(),
                title: None,
                username: recipient.as_ref().and_then(|user| user.username.clone()),
                first_name: recipient.as_ref().map(|user| user.first_name.clone()),
                last_name: None,
                is_forum: None,
                is_direct_messages: None,
            },
        ));
    }

    let recipient = load_sim_user_record(conn, chat_id)?;
    Ok((
        requested_chat_key,
        Chat {
            id: chat_id,
            r#type: "private".to_string(),
            title: None,
            username: recipient.as_ref().and_then(|user| user.username.clone()),
            first_name: recipient.as_ref().map(|user| user.first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        },
    ))
}

#[derive(Debug)]
struct SimUserRecord {
    id: i64,
    first_name: String,
    username: Option<String>,
    last_name: Option<String>,
    is_premium: bool,
}

#[derive(Debug, Clone)]
struct SimChatRecord {
    chat_key: String,
    chat_id: i64,
    chat_type: String,
    title: Option<String>,
    username: Option<String>,
    is_forum: bool,
    is_direct_messages: bool,
    parent_channel_chat_id: Option<i64>,
    direct_messages_enabled: bool,
    direct_messages_star_count: i64,
    channel_show_author_signature: bool,
    channel_paid_reactions_enabled: bool,
    linked_discussion_chat_id: Option<i64>,
}

#[derive(Debug, Clone)]
struct SimChatMemberRecord {
    status: String,
    role: String,
    permissions_json: Option<String>,
    admin_rights_json: Option<String>,
    until_date: Option<i64>,
    custom_title: Option<String>,
    tag: Option<String>,
    joined_at: Option<i64>,
}

#[derive(Debug, Clone)]
struct SimBusinessConnectionRecord {
    connection_id: String,
    user_id: i64,
    user_chat_id: i64,
    rights_json: String,
    is_enabled: bool,
    gift_settings_show_button: bool,
    gift_settings_types_json: Option<String>,
    star_balance: i64,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone)]
struct SimBusinessProfileRecord {
    last_name: Option<String>,
    bio: Option<String>,
    profile_photo_file_id: Option<String>,
    public_profile_photo_file_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SimManagedBotRecord {
    owner_user_id: i64,
    managed_bot_id: i64,
    managed_token: String,
    managed_bot_username: String,
    managed_bot_first_name: String,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone)]
struct SimDirectMessagesTopicRecord {
    topic_id: i64,
    user_id: i64,
    created_at: i64,
    updated_at: i64,
    last_message_id: Option<i64>,
    last_message_date: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SimGroupSettingsResponse {
    show_author_signature: bool,
    paid_star_reactions_enabled: bool,
    message_history_visible: bool,
    slow_mode_delay: i64,
    permissions: ChatPermissions,
}

#[derive(Debug, Serialize)]
struct SimCreateGroupResponse {
    chat: Chat,
    owner: User,
    members: Vec<User>,
    settings: SimGroupSettingsResponse,
}

#[derive(Debug, Clone)]
struct SimChatInviteLinkRecord {
    invite_link: String,
    creator_user_id: i64,
    creates_join_request: bool,
    is_primary: bool,
    is_revoked: bool,
    name: Option<String>,
    expire_date: Option<i64>,
    member_limit: Option<i64>,
    subscription_period: Option<i64>,
    subscription_price: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
enum ChatSendKind {
    Text,
    Photo,
    Video,
    Audio,
    Voice,
    Document,
    VideoNote,
    Poll,
    Invoice,
    Other,
}

fn send_kind_from_media_field(media_field: &str) -> ChatSendKind {
    match media_field {
        "photo" => ChatSendKind::Photo,
        "video" => ChatSendKind::Video,
        "audio" => ChatSendKind::Audio,
        "voice" => ChatSendKind::Voice,
        "document" => ChatSendKind::Document,
        "video_note" => ChatSendKind::VideoNote,
        _ => ChatSendKind::Other,
    }
}

fn send_kind_from_payload_field(payload_field: &str) -> ChatSendKind {
    match payload_field {
        "poll" => ChatSendKind::Poll,
        "invoice" => ChatSendKind::Invoice,
        _ => ChatSendKind::Other,
    }
}

fn send_kind_from_sim_user_media_kind(media_kind: &str) -> ChatSendKind {
    match media_kind {
        "photo" => ChatSendKind::Photo,
        "video" => ChatSendKind::Video,
        "audio" => ChatSendKind::Audio,
        "voice" => ChatSendKind::Voice,
        "document" => ChatSendKind::Document,
        "video_note" => ChatSendKind::VideoNote,
        _ => ChatSendKind::Other,
    }
}

fn load_bot_privacy_mode_enabled(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
) -> Result<bool, ApiError> {
    let value: Option<i64> = conn
        .query_row(
            "SELECT privacy_mode_enabled FROM sim_bot_runtime_settings WHERE bot_id = ?1",
            params![bot_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    Ok(value.unwrap_or(1) == 1)
}

fn set_bot_privacy_mode_enabled(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    enabled: bool,
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_runtime_settings (bot_id, privacy_mode_enabled, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(bot_id)
         DO UPDATE SET
            privacy_mode_enabled = excluded.privacy_mode_enabled,
            updated_at = excluded.updated_at",
        params![bot_id, if enabled { 1 } else { 0 }, now],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn text_matches_privacy_command_or_mention(text: &str, bot_username: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let bot_username_lower = bot_username.to_ascii_lowercase();
    let mention = format!("@{}", bot_username_lower);
    let lowered = trimmed.to_ascii_lowercase();

    if lowered.contains(&mention) {
        return true;
    }

    let Some(first_token) = lowered.split_whitespace().next() else {
        return false;
    };
    if !first_token.starts_with('/') {
        return false;
    }

    if let Some((_, command_scope)) = first_token.split_once('@') {
        return command_scope == bot_username_lower;
    }

    true
}

fn message_targets_bot_via_entities(
    text: Option<&str>,
    entities: Option<&Vec<MessageEntity>>,
    bot_username: &str,
) -> bool {
    let Some(text) = text else {
        return false;
    };
    let Some(entities) = entities else {
        return false;
    };

    let bot_username_lower = bot_username.to_ascii_lowercase();
    let bot_mention = format!("@{}", bot_username_lower);
    let first_non_ws_byte = text
        .char_indices()
        .find_map(|(idx, ch)| if ch.is_whitespace() { None } else { Some(idx) })
        .unwrap_or(text.len());
    let first_non_ws_offset = utf16_len(&text[..first_non_ws_byte]);

    for entity in entities {
        if entity.offset < 0 || entity.length <= 0 {
            continue;
        }

        let Some(fragment) = entity_text_by_utf16_span(text, entity.offset as usize, entity.length as usize) else {
            continue;
        };

        if entity.r#type == "mention" {
            if fragment.to_ascii_lowercase() == bot_mention {
                return true;
            }
            continue;
        }

        if entity.r#type == "bot_command" {
            if entity.offset as usize != first_non_ws_offset {
                continue;
            }

            let normalized = fragment.to_ascii_lowercase();
            if let Some((_, command_scope)) = normalized.split_once('@') {
                if command_scope == bot_username_lower.as_str() {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    false
}

fn entity_text_by_utf16_span<'a>(text: &'a str, offset: usize, length: usize) -> Option<&'a str> {
    let (start, end) = utf16_span_to_byte_range(text, offset, length)?;
    text.get(start..end)
}

fn message_targets_bot_in_privacy_mode(message: &Message, bot: &crate::database::BotInfoRecord) -> bool {
    if let Some(reply) = message.reply_to_message.as_ref() {
        let reply_from_id = reply.from.as_ref().map(|from| from.id);
        if reply_from_id == Some(bot.id) {
            return true;
        }
    }

    if message_targets_bot_via_entities(
        message.text.as_deref(),
        message.entities.as_ref(),
        &bot.username,
    ) {
        return true;
    }

    if message_targets_bot_via_entities(
        message.caption.as_deref(),
        message.caption_entities.as_ref(),
        &bot.username,
    ) {
        return true;
    }

    if let Some(text) = message.text.as_deref() {
        if text_matches_privacy_command_or_mention(text, &bot.username) {
            return true;
        }
    }

    if let Some(caption) = message.caption.as_deref() {
        if text_matches_privacy_command_or_mention(caption, &bot.username) {
            return true;
        }
    }

    false
}

fn should_emit_user_generated_update_to_bot(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_type: &str,
    from_user_id: i64,
    message: &Message,
) -> Result<bool, ApiError> {
    if chat_type == "private" || from_user_id == bot.id {
        return Ok(true);
    }

    let privacy_mode_enabled = load_bot_privacy_mode_enabled(conn, bot.id)?;
    if !privacy_mode_enabled {
        return Ok(true);
    }

    Ok(message_targets_bot_in_privacy_mode(message, bot))
}

fn default_group_permissions() -> ChatPermissions {
    ChatPermissions {
        can_send_messages: Some(true),
        can_send_audios: Some(true),
        can_send_documents: Some(true),
        can_send_photos: Some(true),
        can_send_videos: Some(true),
        can_send_video_notes: Some(true),
        can_send_voice_notes: Some(true),
        can_send_polls: Some(true),
        can_send_other_messages: Some(true),
        can_add_web_page_previews: Some(true),
        can_change_info: Some(false),
        can_invite_users: Some(true),
        can_pin_messages: Some(false),
        can_manage_topics: Some(false),
        can_edit_tag: Some(false),
    }
}

fn chat_from_sim_record(record: &SimChatRecord, user: &SimUserRecord) -> Chat {
    if record.chat_type == "private" {
        Chat {
            id: record.chat_id,
            r#type: "private".to_string(),
            title: None,
            username: user.username.clone(),
            first_name: Some(user.first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: if record.is_direct_messages { Some(true) } else { None },
        }
    } else {
        Chat {
            id: record.chat_id,
            r#type: record.chat_type.clone(),
            title: record.title.clone(),
            username: record.username.clone(),
            first_name: None,
            last_name: None,
            is_forum: if record.chat_type == "supergroup" {
                Some(record.is_forum)
            } else {
                None
            },
            is_direct_messages: if record.is_direct_messages { Some(true) } else { None },
        }
    }
}

fn build_chat_from_group_record(record: &SimChatRecord) -> Chat {
    Chat {
        id: record.chat_id,
        r#type: record.chat_type.clone(),
        title: record.title.clone(),
        username: record.username.clone(),
        first_name: None,
        last_name: None,
        is_forum: if record.chat_type == "supergroup" {
            Some(record.is_forum)
        } else {
            None
        },
        is_direct_messages: if record.is_direct_messages { Some(true) } else { None },
    }
}

fn ensure_sim_user_record(
    conn: &mut rusqlite::Connection,
    user_id: i64,
) -> Result<SimUserRecord, ApiError> {
    if let Some(existing) = load_sim_user_record(conn, user_id)? {
        return Ok(existing);
    }

    ensure_user(
        conn,
        Some(user_id),
        Some(format!("User {}", user_id)),
        None,
    )
}

fn resolve_non_private_sim_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: &Value,
) -> Result<(String, SimChatRecord), ApiError> {
    let requested_chat_key = value_to_chat_key(chat_id)?;
    let requested_chat_id = chat_id_as_i64(chat_id, &requested_chat_key);
    let Some(sim_chat) = load_sim_chat_record(conn, bot_id, &requested_chat_key)?
        .or(load_sim_chat_record_by_chat_id(conn, bot_id, requested_chat_id)?) else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request(
            "chat must be a group, supergroup or channel",
        ));
    }

    Ok((sim_chat.chat_key.clone(), sim_chat))
}

fn ensure_bot_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(), ApiError> {
    let bot_status = load_chat_member_status(conn, bot_id, chat_key, bot_id)?;
    if !bot_status
        .as_deref()
        .map(is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "bot is not an administrator in this chat",
        ));
    }

    Ok(())
}

fn ensure_request_actor_can_publish_to_channel(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(), ApiError> {
    let Some(actor_user_id) = current_request_actor_user_id() else {
        return Ok(());
    };

    let Some(actor_record) = load_chat_member_record(conn, bot_id, chat_key, actor_user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin can publish messages",
        ));
    };

    if actor_record.status == "owner" {
        return Ok(());
    }

    if actor_record.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin can publish messages",
        ));
    }

    let rights = parse_channel_admin_rights_json(actor_record.admin_rights_json.as_deref());
    if !channel_admin_can_publish(&rights) {
        if actor_user_id == bot_id {
            return Err(ApiError::bad_request(
                "bot is not allowed to publish messages in this channel",
            ));
        }
        return Err(ApiError::bad_request(
            "not enough rights to publish messages in this channel",
        ));
    }

    Ok(())
}

fn ensure_channel_actor_can_manage_invite_links(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    actor_user_id: i64,
) -> Result<(), ApiError> {
    let Some(actor_record) = load_chat_member_record(conn, bot_id, chat_key, actor_user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin can manage invite links",
        ));
    };

    if actor_record.status == "owner" {
        return Ok(());
    }

    if actor_record.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin can manage invite links",
        ));
    }

    let rights = parse_channel_admin_rights_json(actor_record.admin_rights_json.as_deref());
    if rights.can_manage_chat || rights.can_invite_users {
        return Ok(());
    }

    if actor_user_id == bot_id {
        return Err(ApiError::bad_request(
            "bot is not allowed to manage invite links in this channel",
        ));
    }

    Err(ApiError::bad_request(
        "not enough rights to manage invite links in this channel",
    ))
}

fn resolve_chat_admin_actor(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
) -> Result<User, ApiError> {
    if let Some(actor_user_id) = current_request_actor_user_id() {
        if actor_user_id == bot.id {
            ensure_bot_is_chat_admin_or_owner(conn, bot.id, chat_key)?;
            return Ok(build_bot_user(bot));
        }

        let actor_status = load_chat_member_status(conn, bot.id, chat_key, actor_user_id)?;
        if !actor_status
            .as_deref()
            .map(is_group_admin_or_owner_status)
            .unwrap_or(false)
        {
            return Err(ApiError::bad_request("not enough rights to manage chat"));
        }

        let actor_record = ensure_sim_user_record(conn, actor_user_id)?;
        return Ok(build_user_from_sim_record(&actor_record, false));
    }

    ensure_bot_is_chat_admin_or_owner(conn, bot.id, chat_key)?;
    Ok(build_bot_user(bot))
}

fn ensure_request_actor_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
) -> Result<(), ApiError> {
    let _ = resolve_chat_admin_actor(conn, bot, chat_key)?;
    Ok(())
}

fn normalize_business_connection_id(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn default_business_connection_id(bot_id: i64, user_id: i64) -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("business:{}:{}", bot_id, user_id);
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AQAD{}", &hexed[..28])
}

fn default_business_accepted_gift_types() -> AcceptedGiftTypes {
    AcceptedGiftTypes {
        unlimited_gifts: true,
        limited_gifts: true,
        unique_gifts: true,
        premium_subscription: true,
        gifts_from_channels: true,
    }
}

fn parse_business_accepted_gift_types_json(raw: Option<&str>) -> AcceptedGiftTypes {
    raw.and_then(|value| serde_json::from_str::<AcceptedGiftTypes>(value).ok())
        .unwrap_or_else(default_business_accepted_gift_types)
}

fn default_business_bot_rights() -> BusinessBotRights {
    BusinessBotRights {
        can_reply: Some(true),
        can_read_messages: Some(true),
        can_delete_sent_messages: Some(true),
        can_delete_all_messages: Some(true),
        can_edit_name: Some(true),
        can_edit_bio: Some(true),
        can_edit_profile_photo: Some(true),
        can_edit_username: Some(true),
        can_change_gift_settings: Some(true),
        can_view_gifts_and_stars: Some(true),
        can_convert_gifts_to_stars: Some(true),
        can_transfer_and_upgrade_gifts: Some(true),
        can_transfer_stars: Some(true),
        can_manage_stories: Some(true),
    }
}

fn parse_business_bot_rights_json(raw: &str) -> BusinessBotRights {
    serde_json::from_str::<BusinessBotRights>(raw).unwrap_or_else(|_| default_business_bot_rights())
}

fn load_sim_business_connection_by_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    connection_id: &str,
) -> Result<Option<SimBusinessConnectionRecord>, ApiError> {
    conn.query_row(
        "SELECT connection_id, user_id, user_chat_id, rights_json, is_enabled,
                gift_settings_show_button, gift_settings_types_json, star_balance,
                created_at, updated_at
         FROM sim_business_connections
         WHERE bot_id = ?1 AND LOWER(connection_id) = LOWER(?2)",
        params![bot_id, connection_id],
        |row| {
            Ok(SimBusinessConnectionRecord {
                connection_id: row.get(0)?,
                user_id: row.get(1)?,
                user_chat_id: row.get(2)?,
                rights_json: row.get(3)?,
                is_enabled: row.get::<_, i64>(4)? == 1,
                gift_settings_show_button: row.get::<_, i64>(5)? == 1,
                gift_settings_types_json: row.get(6)?,
                star_balance: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn load_sim_business_connection_for_user(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    user_id: i64,
) -> Result<Option<SimBusinessConnectionRecord>, ApiError> {
    conn.query_row(
        "SELECT connection_id, user_id, user_chat_id, rights_json, is_enabled,
                gift_settings_show_button, gift_settings_types_json, star_balance,
                created_at, updated_at
         FROM sim_business_connections
         WHERE bot_id = ?1 AND user_id = ?2
         ORDER BY updated_at DESC
         LIMIT 1",
        params![bot_id, user_id],
        |row| {
            Ok(SimBusinessConnectionRecord {
                connection_id: row.get(0)?,
                user_id: row.get(1)?,
                user_chat_id: row.get(2)?,
                rights_json: row.get(3)?,
                is_enabled: row.get::<_, i64>(4)? == 1,
                gift_settings_show_button: row.get::<_, i64>(5)? == 1,
                gift_settings_types_json: row.get(6)?,
                star_balance: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn upsert_sim_business_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    connection_id: &str,
    user_id: i64,
    user_chat_id: i64,
    rights: &BusinessBotRights,
    is_enabled: bool,
) -> Result<SimBusinessConnectionRecord, ApiError> {
    let now = Utc::now().timestamp();
    let rights_json = serde_json::to_string(rights).map_err(ApiError::internal)?;

    if let Some(existing) = load_sim_business_connection_for_user(conn, bot_id, user_id)? {
        conn.execute(
            "UPDATE sim_business_connections
             SET connection_id = ?1,
                 user_chat_id = ?2,
                 rights_json = ?3,
                 is_enabled = ?4,
                 updated_at = ?5
             WHERE bot_id = ?6 AND connection_id = ?7",
            params![
                connection_id,
                user_chat_id,
                rights_json,
                if is_enabled { 1 } else { 0 },
                now,
                bot_id,
                existing.connection_id,
            ],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT INTO sim_business_connections
             (bot_id, connection_id, user_id, user_chat_id, rights_json, is_enabled, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
            params![
                bot_id,
                connection_id,
                user_id,
                user_chat_id,
                rights_json,
                if is_enabled { 1 } else { 0 },
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    load_sim_business_connection_by_id(conn, bot_id, connection_id)?
        .ok_or_else(|| ApiError::internal("failed to persist business connection"))
}

fn load_sim_business_profile(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    user_id: i64,
) -> Result<Option<SimBusinessProfileRecord>, ApiError> {
    conn.query_row(
        "SELECT last_name, bio, profile_photo_file_id, public_profile_photo_file_id
         FROM sim_business_account_profiles
         WHERE bot_id = ?1 AND user_id = ?2",
        params![bot_id, user_id],
        |row| {
            Ok(SimBusinessProfileRecord {
                last_name: row.get(0)?,
                bio: row.get(1)?,
                profile_photo_file_id: row.get(2)?,
                public_profile_photo_file_id: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn build_business_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    record: &SimBusinessConnectionRecord,
) -> Result<BusinessConnection, ApiError> {
    let user_record = ensure_sim_user_record(conn, record.user_id)?;
    let profile = load_sim_business_profile(conn, bot_id, record.user_id)?;

    let user = User {
        id: user_record.id,
        is_bot: false,
        first_name: user_record.first_name,
        last_name: profile
            .as_ref()
            .and_then(|item| item.last_name.clone())
            .or(user_record.last_name),
        username: user_record.username,
        language_code: None,
        is_premium: Some(user_record.is_premium),
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    Ok(BusinessConnection {
        id: record.connection_id.clone(),
        user,
        user_chat_id: record.user_chat_id,
        date: record.created_at,
        rights: Some(parse_business_bot_rights_json(&record.rights_json)),
        is_enabled: record.is_enabled,
    })
}

fn business_right_enabled(
    rights: &Option<BusinessBotRights>,
    resolver: impl Fn(&BusinessBotRights) -> Option<bool>,
) -> bool {
    rights
        .as_ref()
        .and_then(resolver)
        .unwrap_or(false)
}

fn ensure_business_right(
    connection: &BusinessConnection,
    resolver: impl Fn(&BusinessBotRights) -> Option<bool>,
    message: &str,
) -> Result<(), ApiError> {
    if business_right_enabled(&connection.rights, resolver) {
        return Ok(());
    }

    Err(ApiError::bad_request(message))
}

fn is_direct_messages_chat(sim_chat: &SimChatRecord) -> bool {
    sim_chat.is_direct_messages && sim_chat.parent_channel_chat_id.is_some()
}

fn direct_messages_star_count_for_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
) -> Result<i64, ApiError> {
    if !is_direct_messages_chat(sim_chat) {
        return Ok(0);
    }

    let parent_channel_chat_id = sim_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
    let parent_channel_chat = load_sim_chat_record(conn, bot_id, &parent_channel_chat_id.to_string())?
        .ok_or_else(|| ApiError::not_found("parent channel not found"))?;
    if !parent_channel_chat.direct_messages_enabled {
        return Err(ApiError::bad_request("channel direct messages are disabled"));
    }

    Ok(parent_channel_chat.direct_messages_star_count.max(0))
}

fn direct_messages_chat_key(channel_chat_id: i64) -> String {
    format!("dm:{}", channel_chat_id)
}

fn direct_messages_chat_id_for_channel(channel_chat_id: i64) -> i64 {
    let channel_abs = channel_chat_id.saturating_abs();
    -(1_000_000_000_000_000i64.saturating_add(channel_abs))
}

fn ensure_channel_member_can_manage_direct_messages(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    let Some(member) = load_chat_member_record(conn, bot_id, channel_chat_key, user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin with direct-messages rights can access channel direct messages",
        ));
    };

    if member.status == "owner" {
        return Ok(());
    }

    if member.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin with direct-messages rights can access channel direct messages",
        ));
    }

    if channel_admin_has_direct_messages_permission(member.admin_rights_json.as_deref()) {
        return Ok(());
    }

    Err(ApiError::bad_request(
        "not enough rights to manage channel direct messages",
    ))
}

fn ensure_channel_member_can_approve_suggested_posts(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    let Some(member) = load_chat_member_record(conn, bot_id, channel_chat_key, user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin with post rights can approve suggested posts",
        ));
    };

    if member.status == "owner" {
        return Ok(());
    }

    if member.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin with post rights can approve suggested posts",
        ));
    }

    let rights = parse_channel_admin_rights_json(member.admin_rights_json.as_deref());
    if rights.can_post_messages {
        return Ok(());
    }

    Err(ApiError::bad_request(
        "not enough rights to approve suggested posts",
    ))
}

fn ensure_channel_direct_messages_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat: &SimChatRecord,
) -> Result<SimChatRecord, ApiError> {
    if channel_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("channel direct messages are available only for channels"));
    }

    let chat_key = direct_messages_chat_key(channel_chat.chat_id);
    ensure_chat(conn, &chat_key)?;
    let now = Utc::now().timestamp();
    let chat_id = direct_messages_chat_id_for_channel(channel_chat.chat_id);
    let channel_title = channel_chat
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Channel");
    let dm_title = format!("{} Direct Messages", channel_title);

    conn.execute(
        "INSERT INTO sim_chats
         (bot_id, chat_key, chat_id, chat_type, title, username, description, photo_file_id,
          is_forum, is_direct_messages, parent_channel_chat_id, channel_show_author_signature,
          linked_discussion_chat_id, message_history_visible, slow_mode_delay, permissions_json,
          sticker_set_name, pinned_message_id, owner_user_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'supergroup', ?4, NULL, NULL, NULL,
             0, 1, ?5, 0,
                 NULL, 1, 0, NULL,
                 NULL, NULL, NULL, ?6, ?6)
         ON CONFLICT(bot_id, chat_key)
         DO UPDATE SET
                chat_id = excluded.chat_id,
            title = excluded.title,
            is_forum = 0,
            is_direct_messages = 1,
            parent_channel_chat_id = excluded.parent_channel_chat_id,
            updated_at = excluded.updated_at",
        params![bot_id, &chat_key, chat_id, dm_title, channel_chat.chat_id, now],
    )
    .map_err(ApiError::internal)?;

    upsert_chat_member_record(
        conn,
        bot_id,
        &chat_key,
        bot_id,
        "admin",
        "admin",
        Some(now),
        None,
        None,
        None,
        None,
        now,
    )?;

    load_sim_chat_record(conn, bot_id, &chat_key)?
        .ok_or_else(|| ApiError::internal("failed to create channel direct messages chat"))
}

fn load_direct_messages_topic_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    topic_id: i64,
) -> Result<Option<SimDirectMessagesTopicRecord>, ApiError> {
    conn.query_row(
        "SELECT topic_id, user_id, created_at, updated_at, last_message_id, last_message_date
         FROM sim_direct_message_topics
         WHERE bot_id = ?1 AND chat_key = ?2 AND topic_id = ?3",
        params![bot_id, chat_key, topic_id],
        |row| {
            Ok(SimDirectMessagesTopicRecord {
                topic_id: row.get(0)?,
                user_id: row.get(1)?,
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
                last_message_id: row.get(4)?,
                last_message_date: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn upsert_direct_messages_topic(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    topic_id: i64,
    user_id: i64,
    last_message_id: Option<i64>,
    last_message_date: Option<i64>,
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_direct_message_topics
         (bot_id, chat_key, topic_id, user_id, created_at, updated_at, last_message_id, last_message_date)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?7)
         ON CONFLICT(bot_id, chat_key, topic_id)
         DO UPDATE SET
            user_id = excluded.user_id,
            updated_at = excluded.updated_at,
            last_message_id = COALESCE(excluded.last_message_id, sim_direct_message_topics.last_message_id),
            last_message_date = COALESCE(excluded.last_message_date, sim_direct_message_topics.last_message_date)",
        params![
            bot_id,
            chat_key,
            topic_id,
            user_id,
            now,
            last_message_id,
            last_message_date,
        ],
    )
    .map_err(ApiError::internal)?;
    Ok(())
}

fn direct_messages_topic_value(
    conn: &mut rusqlite::Connection,
    user_id: i64,
    topic_id: i64,
) -> Result<Value, ApiError> {
    let user = load_sim_user_record(conn, user_id)?
        .map(|record| build_user_from_sim_record(&record, false));
    let topic = DirectMessagesTopic {
        topic_id,
        user,
    };
    serde_json::to_value(topic).map_err(ApiError::internal)
}

fn load_direct_messages_topics_for_chat_json(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Vec<Value>, ApiError> {
    let mut stmt = conn
        .prepare(
            "SELECT t.topic_id, t.user_id, t.updated_at, u.first_name, u.username
             FROM sim_direct_message_topics t
             LEFT JOIN users u ON u.id = t.user_id
             WHERE t.bot_id = ?1 AND t.chat_key = ?2
             ORDER BY t.updated_at DESC, t.topic_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot_id, chat_key], |row| {
            let topic_id: i64 = row.get(0)?;
            let user_id: i64 = row.get(1)?;
            let updated_at: i64 = row.get(2)?;
            let first_name: Option<String> = row.get(3)?;
            let username: Option<String> = row.get(4)?;
            let label = first_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    username
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(|value| format!("@{}", value))
                        .unwrap_or_else(|| format!("User {}", user_id))
                });

            Ok(json!({
                "topic_id": topic_id,
                "user_id": user_id,
                "name": label,
                "updated_at": updated_at,
            }))
        })
        .map_err(ApiError::internal)?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(ApiError::internal)?);
    }

    Ok(result)
}

fn resolve_direct_messages_topic_for_sender(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    sender: &SimUserRecord,
    requested_topic_id: Option<i64>,
) -> Result<(i64, Value, Option<Chat>), ApiError> {
    let parent_channel_chat_id = sim_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
    let parent_channel_key = parent_channel_chat_id.to_string();
    let parent_channel_chat = load_sim_chat_record(conn, bot_id, &parent_channel_key)?
        .ok_or_else(|| ApiError::not_found("parent channel not found"))?;

    let manager_allowed = load_chat_member_record(conn, bot_id, &parent_channel_key, sender.id)?
        .map(|record| {
            if record.status == "owner" {
                true
            } else if record.status == "admin" {
                channel_admin_has_direct_messages_permission(record.admin_rights_json.as_deref())
            } else {
                false
            }
        })
        .unwrap_or(false);

    if manager_allowed {
        let topic_id = requested_topic_id.unwrap_or(sender.id);
        if topic_id <= 0 {
            return Err(ApiError::bad_request("direct_messages_topic_id is invalid"));
        }

        let topic_record = load_direct_messages_topic_record(conn, bot_id, &sim_chat.chat_key, topic_id)?
            .ok_or_else(|| ApiError::not_found("direct messages topic not found"))?;
        if topic_record.user_id == sender.id {
            return Err(ApiError::bad_request(
                "direct-messages managers can't send messages to their own topic",
            ));
        }
        let topic_user_id = topic_record.user_id;

        let topic_value = direct_messages_topic_value(conn, topic_user_id, topic_id)?;
        return Ok((
            topic_id,
            topic_value,
            Some(build_chat_from_group_record(&parent_channel_chat)),
        ));
    }

    let topic_id = requested_topic_id.unwrap_or(sender.id);
    if topic_id != sender.id {
        return Err(ApiError::bad_request(
            "only channel admins with direct-messages rights can select direct_messages_topic_id",
        ));
    }

    let existing = load_direct_messages_topic_record(conn, bot_id, &sim_chat.chat_key, topic_id)?;
    if let Some(record) = existing {
        if record.user_id != sender.id {
            return Err(ApiError::bad_request("direct messages topic does not belong to sender"));
        }
    } else {
        upsert_direct_messages_topic(conn, bot_id, &sim_chat.chat_key, topic_id, sender.id, None, None)?;
    }

    let topic_value = direct_messages_topic_value(conn, sender.id, topic_id)?;
    Ok((topic_id, topic_value, None))
}

fn ensure_request_actor_can_manage_sender_chat_in_linked_context(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
    sim_chat: &SimChatRecord,
) -> Result<(), ApiError> {
    if ensure_request_actor_is_chat_admin_or_owner(conn, bot, chat_key).is_ok() {
        return Ok(());
    }

    if sim_chat.chat_type != "group" && sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request("not enough rights to manage chat"));
    }

    let linked_channel_chat_key: Option<String> = conn
        .query_row(
            "SELECT chat_key
             FROM sim_chats
             WHERE bot_id = ?1
               AND chat_type = 'channel'
               AND linked_discussion_chat_id = ?2
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, sim_chat.chat_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some(channel_chat_key) = linked_channel_chat_key {
        if ensure_request_actor_is_chat_admin_or_owner(conn, bot, &channel_chat_key).is_ok() {
            return Ok(());
        }
    }

    Err(ApiError::bad_request("not enough rights to manage chat"))
}

fn emit_chat_member_transition_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    target: &User,
    old_status: &str,
    new_status: &str,
    date: i64,
) -> Result<(), ApiError> {
    emit_chat_member_transition_update_with_records(
        state,
        conn,
        token,
        bot_id,
        chat,
        actor,
        target,
        old_status,
        new_status,
        None,
        None,
        date,
    )
}

fn emit_chat_member_transition_update_with_records(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    target: &User,
    old_status: &str,
    new_status: &str,
    old_record: Option<&SimChatMemberRecord>,
    new_record: Option<&SimChatMemberRecord>,
    date: i64,
) -> Result<(), ApiError> {
    let old_chat_member = if let Some(record) = old_record {
        chat_member_from_record(record, target, chat.r#type.as_str())?
    } else {
        chat_member_from_status(old_status, target)?
    };
    let new_chat_member = if let Some(record) = new_record {
        chat_member_from_record(record, target, chat.r#type.as_str())?
    } else {
        chat_member_from_status(new_status, target)?
    };

    let update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: actor.clone(),
            date,
            old_chat_member,
            new_chat_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    persist_and_dispatch_update(
        state,
        conn,
        token,
        bot_id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )
}

fn emit_my_chat_member_transition_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    old_status: &str,
    new_status: &str,
    date: i64,
) -> Result<(), ApiError> {
    emit_my_chat_member_transition_update_with_records(
        state,
        conn,
        token,
        bot_id,
        chat,
        actor,
        old_status,
        new_status,
        None,
        None,
        date,
    )
}

fn emit_my_chat_member_transition_update_with_records(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    old_status: &str,
    new_status: &str,
    old_record: Option<&SimChatMemberRecord>,
    new_record: Option<&SimChatMemberRecord>,
    date: i64,
) -> Result<(), ApiError> {
    let old_chat_member = if let Some(record) = old_record {
        chat_member_from_record(record, actor, chat.r#type.as_str())?
    } else {
        chat_member_from_status(old_status, actor)?
    };
    let new_chat_member = if let Some(record) = new_record {
        chat_member_from_record(record, actor, chat.r#type.as_str())?
    } else {
        chat_member_from_status(new_status, actor)?
    };

    let update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: actor.clone(),
            date,
            old_chat_member,
            new_chat_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    persist_and_dispatch_update(
        state,
        conn,
        token,
        bot_id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )
}

fn load_sim_chat_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<SimChatRecord>, ApiError> {
    conn.query_row(
        "SELECT chat_id, chat_type, title, username, is_forum, is_direct_messages, parent_channel_chat_id, direct_messages_enabled, direct_messages_star_count, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id
         FROM sim_chats
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| {
            Ok(SimChatRecord {
                chat_key: chat_key.to_string(),
                chat_id: row.get(0)?,
                chat_type: row.get(1)?,
                title: row.get(2)?,
                username: row.get(3)?,
                is_forum: row.get::<_, i64>(4)? == 1,
                is_direct_messages: row.get::<_, i64>(5)? == 1,
                parent_channel_chat_id: row.get(6)?,
                direct_messages_enabled: row.get::<_, i64>(7)? == 1,
                direct_messages_star_count: row.get::<_, i64>(8)?,
                channel_show_author_signature: row.get::<_, i64>(9)? == 1,
                channel_paid_reactions_enabled: row.get::<_, i64>(10)? == 1,
                linked_discussion_chat_id: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn load_sim_chat_record_by_chat_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
) -> Result<Option<SimChatRecord>, ApiError> {
    conn.query_row(
        "SELECT chat_key, chat_type, title, username, is_forum, is_direct_messages, parent_channel_chat_id, direct_messages_enabled, direct_messages_star_count, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id
         FROM sim_chats
         WHERE bot_id = ?1 AND chat_id = ?2
         ORDER BY updated_at DESC
         LIMIT 1",
        params![bot_id, chat_id],
        |row| {
            Ok(SimChatRecord {
                chat_key: row.get(0)?,
                chat_id,
                chat_type: row.get(1)?,
                title: row.get(2)?,
                username: row.get(3)?,
                is_forum: row.get::<_, i64>(4)? == 1,
                is_direct_messages: row.get::<_, i64>(5)? == 1,
                parent_channel_chat_id: row.get(6)?,
                direct_messages_enabled: row.get::<_, i64>(7)? == 1,
                direct_messages_star_count: row.get::<_, i64>(8)?,
                channel_show_author_signature: row.get::<_, i64>(9)? == 1,
                channel_paid_reactions_enabled: row.get::<_, i64>(10)? == 1,
                linked_discussion_chat_id: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn resolve_linked_chat_id_for_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
) -> Result<Option<i64>, ApiError> {
    if sim_chat.chat_type == "channel" {
        return Ok(sim_chat.linked_discussion_chat_id);
    }

    if sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup" {
        let linked_channel_id: Option<i64> = conn
            .query_row(
                "SELECT chat_id
                 FROM sim_chats
                 WHERE bot_id = ?1
                   AND chat_type = 'channel'
                   AND linked_discussion_chat_id = ?2
                 ORDER BY updated_at DESC
                 LIMIT 1",
                params![bot_id, sim_chat.chat_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        return Ok(linked_channel_id);
    }

    Ok(None)
}

fn ensure_private_sim_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
    user: &SimUserRecord,
) -> Result<SimChatRecord, ApiError> {
    let chat_key = chat_id.to_string();
    ensure_chat(conn, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_chats
         (bot_id, chat_key, chat_id, chat_type, title, username, description, photo_file_id, is_forum, is_direct_messages, parent_channel_chat_id, message_history_visible, slow_mode_delay, permissions_json, owner_user_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'private', NULL, ?4, NULL, NULL, 0, 0, NULL, 1, 0, NULL, NULL, ?5, ?5)
         ON CONFLICT(bot_id, chat_key) DO UPDATE SET updated_at = excluded.updated_at",
        params![bot_id, chat_key, chat_id, user.username, now],
    )
    .map_err(ApiError::internal)?;

    Ok(SimChatRecord {
        chat_key,
        chat_id,
        chat_type: "private".to_string(),
        title: None,
        username: user.username.clone(),
        is_forum: false,
        is_direct_messages: false,
        parent_channel_chat_id: None,
        direct_messages_enabled: false,
        direct_messages_star_count: 0,
        channel_show_author_signature: false,
        channel_paid_reactions_enabled: false,
        linked_discussion_chat_id: None,
    })
}

fn resolve_sim_chat_for_user_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
    user: &SimUserRecord,
) -> Result<SimChatRecord, ApiError> {
    let chat_key = chat_id.to_string();
    if let Some(record) = load_sim_chat_record(conn, bot_id, &chat_key)? {
        return Ok(record);
    }
    if let Some(record) = load_sim_chat_record_by_chat_id(conn, bot_id, chat_id)? {
        return Ok(record);
    }
    ensure_private_sim_chat(conn, bot_id, chat_id, user)
}

fn ensure_sender_chat_not_banned(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    sender_chat_id: i64,
) -> Result<(), ApiError> {
    let banned: Option<i64> = conn
        .query_row(
            "SELECT 1
             FROM sim_banned_sender_chats
             WHERE bot_id = ?1 AND chat_key = ?2 AND sender_chat_id = ?3",
            params![bot_id, chat_key, sender_chat_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if banned.is_some() {
        return Err(ApiError::bad_request(
            "this sender chat is banned in the destination chat",
        ));
    }

    Ok(())
}

fn ensure_user_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
    error_message: &'static str,
) -> Result<(), ApiError> {
    let status = load_chat_member_status(conn, bot_id, chat_key, user_id)?;
    if status
        .as_deref()
        .map(is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Ok(());
    }

    Err(ApiError::bad_request(error_message))
}

fn resolve_sender_chat_for_sim_user_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    sender_user: &SimUserRecord,
    sender_chat_id: Option<i64>,
    send_kind: ChatSendKind,
) -> Result<Option<Chat>, ApiError> {
    let Some(requested_sender_chat_id) = sender_chat_id else {
        return Ok(None);
    };

    if requested_sender_chat_id == 0 {
        return Err(ApiError::bad_request("sender_chat_id is invalid"));
    }

    if sim_chat.chat_type != "group" && sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request(
            "sender_chat_id can only be used in groups or supergroups",
        ));
    }

    if requested_sender_chat_id == sim_chat.chat_id {
        ensure_user_is_chat_admin_or_owner(
            conn,
            bot_id,
            &sim_chat.chat_key,
            sender_user.id,
            "only group owner/admin can send on behalf of this chat",
        )?;
        ensure_sender_chat_not_banned(conn, bot_id, &sim_chat.chat_key, requested_sender_chat_id)?;
        return Ok(Some(build_chat_from_group_record(sim_chat)));
    }

    let sender_chat_key = requested_sender_chat_id.to_string();
    let Some(sender_chat_record) = load_sim_chat_record(conn, bot_id, &sender_chat_key)? else {
        return Err(ApiError::bad_request("sender_chat_id chat not found"));
    };

    if sender_chat_record.chat_type != "channel" {
        return Err(ApiError::bad_request(
            "sender_chat_id must be the current group or its linked channel",
        ));
    }

    let linked_channel_chat_id = resolve_linked_chat_id_for_chat(conn, bot_id, sim_chat)?;
    if linked_channel_chat_id != Some(sender_chat_record.chat_id) {
        return Err(ApiError::bad_request(
            "sender_chat_id must match the linked channel for this discussion",
        ));
    }

    ensure_sender_can_send_in_chat(
        conn,
        bot_id,
        &sender_chat_key,
        sender_user.id,
        send_kind,
    )?;
    ensure_sender_chat_not_banned(conn, bot_id, &sim_chat.chat_key, requested_sender_chat_id)?;

    Ok(Some(build_chat_from_group_record(&sender_chat_record)))
}

fn is_active_chat_member_status(status: &str) -> bool {
    matches!(status, "owner" | "admin" | "member" | "restricted")
}

fn is_group_admin_or_owner_status(status: &str) -> bool {
    matches!(status, "owner" | "admin")
}

fn is_group_owner_status(status: &str) -> bool {
    status == "owner"
}

fn normalize_group_membership_status(raw_status: &str) -> Option<&'static str> {
    match raw_status.trim().to_ascii_lowercase().as_str() {
        "admin" | "administrator" => Some("admin"),
        "member" => Some("member"),
        "restricted" => Some("restricted"),
        "left" | "remove" | "removed" => Some("left"),
        _ => None,
    }
}

fn build_sim_user_chat_id(chat_id: i64, user_id: i64) -> i64 {
    let chat_part = chat_id.abs() % 10_000_000_000;
    let user_part = user_id.abs() % 100_000;
    ((chat_part * 100_000) + user_part).max(1)
}

fn load_sim_user_record(
    conn: &mut rusqlite::Connection,
    user_id: i64,
) -> Result<Option<SimUserRecord>, ApiError> {
    conn.query_row(
        "SELECT id, first_name, username, last_name, is_premium FROM users WHERE id = ?1",
        params![user_id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
                last_name: row.get(3)?,
                is_premium: row.get::<_, i64>(4)? == 1,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn get_or_create_user(
    conn: &mut rusqlite::Connection,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
) -> Result<SimUserRecord, ApiError> {
    if let Some(id) = user_id {
        if first_name.is_none() && username.is_none() {
            if let Some(existing) = load_sim_user_record(conn, id)? {
                return Ok(existing);
            }
        }
    }
    ensure_user(conn, user_id, first_name, username)
}

fn load_chat_member_status(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<Option<String>, ApiError> {
    conn.query_row(
        "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot_id, chat_key, user_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(ApiError::internal)
}

fn load_chat_member_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<Option<SimChatMemberRecord>, ApiError> {
    conn.query_row(
        "SELECT status, role, permissions_json, admin_rights_json, until_date, custom_title, tag, joined_at
         FROM sim_chat_members
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot_id, chat_key, user_id],
        |row| {
            Ok(SimChatMemberRecord {
                status: row.get(0)?,
                role: row.get(1)?,
                permissions_json: row.get(2)?,
                admin_rights_json: row.get(3)?,
                until_date: row.get(4)?,
                custom_title: row.get(5)?,
                tag: row.get(6)?,
                joined_at: row.get(7)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn upsert_chat_member_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
    status: &str,
    role: &str,
    joined_at: Option<i64>,
    permissions_json: Option<&str>,
    until_date: Option<i64>,
    custom_title: Option<&str>,
    tag: Option<&str>,
    updated_at: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_chat_members
         (bot_id, chat_key, user_id, status, role, permissions_json, until_date, custom_title, tag, joined_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(bot_id, chat_key, user_id)
         DO UPDATE SET
            status = excluded.status,
            role = excluded.role,
            permissions_json = excluded.permissions_json,
            until_date = excluded.until_date,
            custom_title = excluded.custom_title,
            tag = excluded.tag,
            joined_at = COALESCE(excluded.joined_at, sim_chat_members.joined_at),
            updated_at = excluded.updated_at",
        params![
            bot_id,
            chat_key,
            user_id,
            status,
            role,
            permissions_json,
            until_date,
            custom_title,
            tag,
            joined_at,
            updated_at,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn clear_chat_member_restrictions(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "UPDATE sim_chat_members
         SET permissions_json = NULL, until_date = NULL
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot_id, chat_key, user_id],
    )
    .map_err(ApiError::internal)?;
    Ok(())
}

fn chat_invite_link_from_record(
    creator: User,
    record: &SimChatInviteLinkRecord,
    pending_join_request_count: Option<i64>,
) -> ChatInviteLink {
    ChatInviteLink {
        invite_link: record.invite_link.clone(),
        creator,
        creates_join_request: record.creates_join_request,
        is_primary: record.is_primary,
        is_revoked: record.is_revoked,
        name: record.name.clone(),
        expire_date: record.expire_date,
        member_limit: record.member_limit,
        pending_join_request_count,
        subscription_period: record.subscription_period,
        subscription_price: record.subscription_price,
    }
}

fn generate_unique_invite_link_for_bot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
) -> Result<String, ApiError> {
    let mut invite_link = generate_sim_invite_link();
    loop {
        let exists: Option<String> = conn
            .query_row(
                "SELECT invite_link FROM sim_chat_invite_links WHERE bot_id = ?1 AND invite_link = ?2",
                params![bot_id, &invite_link],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            return Ok(invite_link);
        }
        invite_link = generate_sim_invite_link();
    }
}

fn pending_join_request_count_for_link(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    invite_link: &str,
) -> Result<i64, ApiError> {
    conn.query_row(
        "SELECT COUNT(*) FROM sim_chat_join_requests
         WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3 AND status = 'pending'",
        params![bot_id, chat_key, invite_link],
        |row| row.get(0),
    )
    .map_err(ApiError::internal)
}

fn load_invite_link_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    invite_link: &str,
) -> Result<Option<SimChatInviteLinkRecord>, ApiError> {
    conn.query_row(
        "SELECT creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price
         FROM sim_chat_invite_links
         WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3",
        params![bot_id, chat_key, invite_link],
        |row| {
            Ok(SimChatInviteLinkRecord {
                invite_link: invite_link.to_string(),
                creator_user_id: row.get(0)?,
                creates_join_request: row.get::<_, i64>(1)? == 1,
                is_primary: row.get::<_, i64>(2)? == 1,
                is_revoked: row.get::<_, i64>(3)? == 1,
                name: row.get(4)?,
                expire_date: row.get(5)?,
                member_limit: row.get(6)?,
                subscription_period: row.get(7)?,
                subscription_price: row.get(8)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

fn resolve_invite_creator_user(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    creator_user_id: i64,
) -> Result<User, ApiError> {
    if creator_user_id == bot.id {
        return Ok(build_bot_user(bot));
    }

    if let Some(record) = load_sim_user_record(conn, creator_user_id)? {
        return Ok(build_user_from_sim_record(&record, false));
    }

    let fallback = ensure_user(
        conn,
        Some(creator_user_id),
        Some(format!("User {}", creator_user_id)),
        None,
    )?;
    Ok(build_user_from_sim_record(&fallback, false))
}

#[derive(Debug)]
struct GroupRuntimeSettings {
    chat_type: String,
    slow_mode_delay: i64,
    permissions: ChatPermissions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
struct ChannelAdminRights {
    can_manage_chat: bool,
    can_post_messages: bool,
    can_edit_messages: bool,
    can_delete_messages: bool,
    can_invite_users: bool,
    can_change_info: bool,
    can_manage_direct_messages: bool,
}

impl Default for ChannelAdminRights {
    fn default() -> Self {
        channel_admin_rights_full_access()
    }
}

fn channel_admin_rights_full_access() -> ChannelAdminRights {
    ChannelAdminRights {
        can_manage_chat: true,
        can_post_messages: true,
        can_edit_messages: true,
        can_delete_messages: true,
        can_invite_users: true,
        can_change_info: true,
        can_manage_direct_messages: true,
    }
}

fn parse_channel_admin_rights_json(raw: Option<&str>) -> ChannelAdminRights {
    raw.and_then(|value| serde_json::from_str::<ChannelAdminRights>(value).ok())
        .unwrap_or_else(channel_admin_rights_full_access)
}

fn channel_admin_has_direct_messages_permission(raw: Option<&str>) -> bool {
    raw.and_then(|value| serde_json::from_str::<ChannelAdminRights>(value).ok())
        .map(|rights| rights.can_manage_direct_messages)
        .unwrap_or(false)
}

fn channel_admin_rights_from_promote_request(request: &PromoteChatMemberRequest) -> ChannelAdminRights {
    ChannelAdminRights {
        can_manage_chat: request.can_manage_chat.unwrap_or(false),
        can_post_messages: request.can_post_messages.unwrap_or(false),
        can_edit_messages: request.can_edit_messages.unwrap_or(false),
        can_delete_messages: request.can_delete_messages.unwrap_or(false),
        can_invite_users: request.can_invite_users.unwrap_or(false),
        can_change_info: request.can_change_info.unwrap_or(false),
        can_manage_direct_messages: request.can_manage_direct_messages.unwrap_or(false),
    }
}

fn channel_admin_can_publish(rights: &ChannelAdminRights) -> bool {
    rights.can_manage_chat || rights.can_post_messages
}

fn load_group_runtime_settings(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<GroupRuntimeSettings>, ApiError> {
    let row: Option<(String, i64, Option<String>)> = conn
        .query_row(
            "SELECT chat_type, slow_mode_delay, permissions_json
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot_id, chat_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_type, slow_mode_delay, permissions_raw)) = row else {
        return Ok(None);
    };
    if chat_type == "private" {
        return Ok(None);
    }

    let permissions = permissions_raw
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
        .unwrap_or_else(default_group_permissions);

    Ok(Some(GroupRuntimeSettings {
        chat_type,
        slow_mode_delay: slow_mode_delay.max(0),
        permissions,
    }))
}

fn permission_enabled(flag: Option<bool>, fallback: bool) -> bool {
    flag.unwrap_or(fallback)
}

fn is_send_kind_allowed_by_permissions(permissions: &ChatPermissions, send_kind: ChatSendKind) -> bool {
    if !permission_enabled(permissions.can_send_messages, true) {
        return false;
    }

    match send_kind {
        ChatSendKind::Text => true,
        ChatSendKind::Photo => permission_enabled(permissions.can_send_photos, true),
        ChatSendKind::Video => permission_enabled(permissions.can_send_videos, true),
        ChatSendKind::Audio => permission_enabled(permissions.can_send_audios, true),
        ChatSendKind::Voice => permission_enabled(permissions.can_send_voice_notes, true),
        ChatSendKind::Document => permission_enabled(permissions.can_send_documents, true),
        ChatSendKind::VideoNote => permission_enabled(permissions.can_send_video_notes, true),
        ChatSendKind::Poll => permission_enabled(permissions.can_send_polls, true),
        ChatSendKind::Invoice | ChatSendKind::Other => permission_enabled(permissions.can_send_other_messages, true),
    }
}

fn send_kind_label(send_kind: ChatSendKind) -> &'static str {
    match send_kind {
        ChatSendKind::Text => "messages",
        ChatSendKind::Photo => "photos",
        ChatSendKind::Video => "videos",
        ChatSendKind::Audio => "audio messages",
        ChatSendKind::Voice => "voice messages",
        ChatSendKind::Document => "documents",
        ChatSendKind::VideoNote => "video notes",
        ChatSendKind::Poll => "polls",
        ChatSendKind::Invoice | ChatSendKind::Other => "this type of messages",
    }
}

fn ensure_sender_can_send_in_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
    send_kind: ChatSendKind,
) -> Result<(), ApiError> {
    let Some(mut member_record) = load_chat_member_record(conn, bot_id, chat_key, user_id)? else {
        return Err(ApiError::bad_request("user is not a member of this chat"));
    };

    if member_record.status == "restricted" {
        let now = Utc::now().timestamp();
        if let Some(until_date) = member_record.until_date {
            if until_date > 0 && until_date <= now {
                upsert_chat_member_record(
                    conn,
                    bot_id,
                    chat_key,
                    user_id,
                    "member",
                    "member",
                    member_record.joined_at.or(Some(now)),
                    None,
                    None,
                    None,
                    member_record.tag.as_deref(),
                    now,
                )?;
                member_record.status = "member".to_string();
                member_record.role = "member".to_string();
                member_record.permissions_json = None;
                member_record.until_date = None;
                member_record.custom_title = None;
            }
        }
    }

    let member_status = member_record.status.as_str();

    if !is_active_chat_member_status(member_status) {
        return Err(ApiError::bad_request("user is not allowed to send messages in this chat"));
    }

    let Some(settings) = load_group_runtime_settings(conn, bot_id, chat_key)? else {
        return Ok(());
    };

    if settings.chat_type == "channel" {
        if member_status == "owner" {
            return Ok(());
        }

        if member_status == "admin" {
            let rights = parse_channel_admin_rights_json(member_record.admin_rights_json.as_deref());
            if channel_admin_can_publish(&rights) {
                return Ok(());
            }
            return Err(ApiError::bad_request(
                "not enough rights to publish messages in this channel",
            ));
        }

        return Err(ApiError::bad_request(
            "only channel owner/admin can publish messages",
        ));
    }

    if is_group_admin_or_owner_status(member_status) {
        return Ok(());
    }

    let effective_permissions = if member_status == "restricted" {
        member_record
            .permissions_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
            .unwrap_or_else(default_group_permissions)
    } else {
        settings.permissions.clone()
    };

    if !is_send_kind_allowed_by_permissions(&effective_permissions, send_kind) {
        return Err(ApiError::bad_request(format!(
            "not enough rights to send {} to the chat",
            send_kind_label(send_kind),
        )));
    }

    if settings.slow_mode_delay > 0 {
        let now = Utc::now().timestamp();
        let last_message_date: Option<i64> = conn
            .query_row(
                "SELECT date
                 FROM messages
                 WHERE bot_id = ?1 AND chat_key = ?2 AND from_user_id = ?3
                 ORDER BY date DESC, message_id DESC
                 LIMIT 1",
                params![bot_id, chat_key, user_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(last_date) = last_message_date {
            let remaining = (last_date + settings.slow_mode_delay) - now;
            if remaining > 0 {
                return Err(ApiError::bad_request(format!(
                    "Too Many Requests: retry after {}",
                    remaining,
                )));
            }
        }
    }

    Ok(())
}

fn ensure_sender_is_chat_member(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot_id, chat_key, user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(member_status) = status else {
        return Err(ApiError::bad_request("user is not a member of this chat"));
    };

    if !is_active_chat_member_status(member_status.as_str()) {
        return Err(ApiError::bad_request("user is not allowed to send messages in this chat"));
    }

    Ok(())
}

fn build_user_from_sim_record(record: &SimUserRecord, is_bot: bool) -> User {
    User {
        id: record.id,
        is_bot,
        first_name: record.first_name.clone(),
        last_name: record.last_name.clone(),
        username: record.username.clone(),
        language_code: None,
        is_premium: Some(record.is_premium),
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    }
}

fn build_bot_user(bot: &crate::database::BotInfoRecord) -> User {
    User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name.clone(),
        last_name: None,
        username: Some(bot.username.clone()),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    }
}

fn to_chat_member<T: Serialize>(member: T) -> Result<ChatMember, ApiError> {
    let value = serde_json::to_value(member).map_err(ApiError::internal)?;
    serde_json::from_value(value).map_err(ApiError::internal)
}

fn chat_member_from_status(status: &str, user: &User) -> Result<ChatMember, ApiError> {
    chat_member_from_status_with_details(status, user, None, None, None, None, None, None)
}

fn chat_member_from_status_with_details(
    status: &str,
    user: &User,
    tag: Option<String>,
    custom_title: Option<String>,
    permissions: Option<ChatPermissions>,
    until_date: Option<i64>,
    chat_type: Option<&str>,
    channel_admin_rights: Option<ChannelAdminRights>,
) -> Result<ChatMember, ApiError> {
    match status {
        "owner" => to_chat_member(ChatMemberOwner {
            status: "creator".to_string(),
            user: user.clone(),
            is_anonymous: false,
            custom_title,
        }),
        "admin" => {
            let is_channel = chat_type == Some("channel");
            let rights = channel_admin_rights.unwrap_or_else(channel_admin_rights_full_access);
            to_chat_member(ChatMemberAdministrator {
                status: "administrator".to_string(),
                user: user.clone(),
                can_be_edited: false,
                is_anonymous: false,
                can_manage_chat: if is_channel { rights.can_manage_chat } else { true },
                can_delete_messages: if is_channel { rights.can_delete_messages } else { true },
                can_manage_video_chats: true,
                can_restrict_members: if is_channel { false } else { true },
                can_promote_members: false,
                can_change_info: if is_channel { rights.can_change_info } else { true },
                can_invite_users: if is_channel { rights.can_invite_users } else { true },
                can_post_stories: true,
                can_edit_stories: true,
                can_delete_stories: true,
                can_post_messages: if is_channel { Some(rights.can_post_messages) } else { None },
                can_edit_messages: if is_channel { Some(rights.can_edit_messages) } else { None },
                can_pin_messages: if is_channel { None } else { Some(true) },
                can_manage_topics: if is_channel { None } else { Some(false) },
                can_manage_direct_messages: if is_channel {
                    Some(rights.can_manage_direct_messages)
                } else {
                    None
                },
                can_manage_tags: None,
                custom_title: if is_channel { None } else { custom_title },
            })
        }
        "member" => to_chat_member(ChatMemberMember {
            status: "member".to_string(),
            tag,
            user: user.clone(),
            until_date,
        }),
        "restricted" => {
            let effective_permissions = permissions.unwrap_or_else(default_group_permissions);
            let restricted_until = until_date.unwrap_or_else(|| Utc::now().timestamp() + 3600);
            to_chat_member(ChatMemberRestricted {
                status: "restricted".to_string(),
                tag,
                user: user.clone(),
                is_member: true,
                can_send_messages: permission_enabled(effective_permissions.can_send_messages, false),
                can_send_audios: permission_enabled(effective_permissions.can_send_audios, false),
                can_send_documents: permission_enabled(effective_permissions.can_send_documents, false),
                can_send_photos: permission_enabled(effective_permissions.can_send_photos, false),
                can_send_videos: permission_enabled(effective_permissions.can_send_videos, false),
                can_send_video_notes: permission_enabled(effective_permissions.can_send_video_notes, false),
                can_send_voice_notes: permission_enabled(effective_permissions.can_send_voice_notes, false),
                can_send_polls: permission_enabled(effective_permissions.can_send_polls, false),
                can_send_other_messages: permission_enabled(effective_permissions.can_send_other_messages, false),
                can_add_web_page_previews: permission_enabled(effective_permissions.can_add_web_page_previews, false),
                can_edit_tag: permission_enabled(effective_permissions.can_edit_tag, false),
                can_change_info: permission_enabled(effective_permissions.can_change_info, false),
                can_invite_users: permission_enabled(effective_permissions.can_invite_users, false),
                can_pin_messages: permission_enabled(effective_permissions.can_pin_messages, false),
                can_manage_topics: permission_enabled(effective_permissions.can_manage_topics, false),
                until_date: restricted_until,
            })
        }
        "banned" => to_chat_member(ChatMemberBanned {
            status: "kicked".to_string(),
            user: user.clone(),
            until_date: until_date.unwrap_or(0),
        }),
        _ => to_chat_member(ChatMemberLeft {
            status: "left".to_string(),
            user: user.clone(),
        }),
    }
}

fn chat_member_from_record(
    record: &SimChatMemberRecord,
    user: &User,
    chat_type: &str,
) -> Result<ChatMember, ApiError> {
    let permissions = record
        .permissions_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok());
    let is_channel = chat_type == "channel";
    let channel_admin_rights = if is_channel && record.status == "admin" {
        Some(parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
    } else {
        None
    };

    chat_member_from_status_with_details(
        record.status.as_str(),
        user,
        if is_channel { None } else { record.tag.clone() },
        if is_channel { None } else { record.custom_title.clone() },
        permissions,
        record.until_date,
        Some(chat_type),
        channel_admin_rights,
    )
}

fn ensure_default_user(conn: &mut rusqlite::Connection) -> Result<SimUserRecord, ApiError> {
    ensure_user(conn, Some(10001), Some("Test User".to_string()), Some("test_user".to_string()))
}

fn ensure_user(
    conn: &mut rusqlite::Connection,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
) -> Result<SimUserRecord, ApiError> {
    let id = user_id.unwrap_or(10001);
    let effective_first_name = first_name.unwrap_or_else(|| "Test User".to_string());
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO users (id, username, first_name, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            first_name = excluded.first_name",
        params![id, username, effective_first_name, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT id, first_name, username, last_name, is_premium FROM users WHERE id = ?1",
        params![id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
                last_name: row.get(3)?,
                is_premium: row.get::<_, i64>(4)? == 1,
            })
        },
    )
    .map_err(ApiError::internal)
}

fn dispatch_webhook_if_configured(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update: Value,
) {
    let webhook: Result<Option<(String, String)>, ApiError> = conn
        .query_row(
            "SELECT url, secret_token FROM webhooks WHERE bot_id = ?1",
            params![bot_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal);

    let Ok(Some((url, secret_token))) = webhook else {
        return;
    };

    let payload = strip_nulls(update);
    let state_for_log = state.clone();
    std::thread::spawn(move || {
        let started_at = Utc::now().timestamp_millis();
        let timer = std::time::Instant::now();
        let request_payload = json!({
            "url": url.clone(),
            "secret_token_set": !secret_token.is_empty(),
            "update": payload,
        });

        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                push_runtime_request_log(
                    &state_for_log,
                    RuntimeRequestLogEntry {
                        id: uuid::Uuid::new_v4().to_string(),
                        at: started_at,
                        method: "POST".to_string(),
                        path: "/webhook/dispatch".to_string(),
                        query: None,
                        status: 599,
                        duration_ms: timer.elapsed().as_millis() as u64,
                        remote_addr: None,
                        request: Some(request_payload),
                        response: Some(json!({
                            "ok": false,
                            "description": format!("webhook client build failed: {}", error),
                        })),
                    },
                );
                return;
            }
        };

        let webhook_update = request_payload
            .get("update")
            .cloned()
            .unwrap_or(Value::Null);
        let mut request = client.post(url).json(&webhook_update);
        if !secret_token.is_empty() {
            request = request.header("X-Telegram-Bot-Api-Secret-Token", secret_token);
        }

        let (status, response_payload) = match request.send() {
            Ok(response) => {
                let status = response.status().as_u16();
                let response_ok = response.status().is_success();
                let mut response_body_text = response.text().unwrap_or_default();
                let mut truncated = false;
                if response_body_text.chars().count() > 4000 {
                    response_body_text = response_body_text.chars().take(4000).collect::<String>();
                    truncated = true;
                }
                let response_body_value = if response_body_text.trim().is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str::<Value>(&response_body_text)
                        .unwrap_or_else(|_| Value::String(response_body_text))
                };
                (
                    status,
                    json!({
                        "ok": response_ok,
                        "status": status,
                        "body": response_body_value,
                        "truncated": truncated,
                    }),
                )
            }
            Err(error) => {
                let status = error.status().map(|value| value.as_u16()).unwrap_or(599);
                (
                    status,
                    json!({
                        "ok": false,
                        "description": error.to_string(),
                    }),
                )
            }
        };

        push_runtime_request_log(
            &state_for_log,
            RuntimeRequestLogEntry {
                id: uuid::Uuid::new_v4().to_string(),
                at: started_at,
                method: "POST".to_string(),
                path: "/webhook/dispatch".to_string(),
                query: None,
                status,
                duration_ms: timer.elapsed().as_millis() as u64,
                remote_addr: None,
                request: Some(request_payload),
                response: Some(response_payload),
            },
        );
    });
}

fn token_suffix(token: &str) -> String {
    token
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

fn generate_telegram_numeric_id() -> String {
    let ts = Utc::now().timestamp_micros().unsigned_abs();
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut mix: u64 = 0;
    for b in bytes.iter().take(8) {
        mix = (mix << 8) | u64::from(*b);
    }
    format!("{}{}", ts, mix % 1_000_000)
}

fn generate_telegram_file_id(kind: &str) -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("{}:{}:{}", kind, Utc::now().timestamp_nanos_opt().unwrap_or_default(), uuid::Uuid::new_v4());
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AgACAgQAAxk{}", &hexed[..48])
}

fn generate_telegram_file_unique_id() -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("{}:{}", Utc::now().timestamp_micros(), uuid::Uuid::new_v4());
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AQAD{}", &hexed[..24])
}

fn generate_telegram_token() -> String {
    let left = ((Utc::now().timestamp_millis().abs() as u64) % 900_000_000) + 100_000_000;
    let right = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    let compact = right.chars().take(35).collect::<String>();
    format!("{}:{}", left, compact)
}

fn generate_sim_invite_link() -> String {
    let code = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    let compact = code.chars().take(22).collect::<String>();
    format!("https://t.me/+{}", compact)
}

fn sanitize_username(input: &str) -> String {
    input
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_lowercase()
}

