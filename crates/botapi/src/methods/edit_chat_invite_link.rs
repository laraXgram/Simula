//! `editChatInviteLink` — https://core.telegram.org/bots/api#editchatinvitelink

use simula_botapi_types::{methods, types as bot};
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::invites;

pub async fn handle(
    ctx: BotCtx,
    params: methods::EditChatInviteLink,
) -> BotResult<bot::ChatInviteLink> {
    let request = params.creates_join_request == Some(true);
    invites::check(params.name.as_deref(), params.member_limit, request)?;
    let chat = invites::chat(&ctx, &params.chat_id).await?;
    invites::edit(
        &ctx,
        functions::messages::EditExportedChatInvite {
            peer: ctx.input_peer(chat),
            link: params.invite_link,
            // tdlib sends every field: what isn't given is reset.
            expire_date: Some(
                params
                    .expire_date
                    .unwrap_or(0)
                    .clamp(0, i64::from(i32::MAX)) as i32,
            ),
            usage_limit: Some(params.member_limit.unwrap_or(0) as i32),
            request_needed: Some(request),
            title: Some(params.name.unwrap_or_default()),
            ..Default::default()
        },
    )
    .await
}
