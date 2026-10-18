//! `getChatAdministrators`

use simula_botapi_types::{methods, types as bot};
use simula_core::Peer;
use simula_tl::enums;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::members::{self, user_id_of};
use crate::resolve::{self, Access};

pub async fn handle(
    ctx: BotCtx,
    params: methods::GetChatAdministrators,
) -> BotResult<Vec<bot::ChatMember>> {
    match resolve::chat_for(&ctx, &params.chat_id, Access::Write).await? {
        Peer::User(_) => Err(BotError::bad_request(
            "there are no administrators in the private chat",
        )),
        Peer::Chat(chat) => {
            let group = members::group(&ctx, chat).await?;
            Ok(group
                .participants
                .iter()
                .filter(|p| !matches!(p, enums::ChatParticipant::ChatParticipant(_)))
                .map(|p| group.member(user_id_of(p)))
                .collect())
        }
        Peer::Channel(_) => Err(BotError::not_implemented("channels and supergroups")),
    }
}
