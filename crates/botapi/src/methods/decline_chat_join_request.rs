//! `declineChatJoinRequest` — https://core.telegram.org/bots/api#declinechatjoinrequest

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};
use crate::invites;

pub async fn handle(ctx: BotCtx, params: methods::DeclineChatJoinRequest) -> BotResult<bool> {
    let chat = invites::chat(&ctx, &params.chat_id).await?;
    ctx.call(
        functions::messages::HideChatJoinRequest {
            approved: false,
            peer: ctx.input_peer(chat),
            user_id: ctx.input_user(params.user_id),
        },
        ErrorContext::Plain,
    )
    .await?;
    Ok(true)
}
