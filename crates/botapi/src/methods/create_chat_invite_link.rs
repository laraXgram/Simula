//! `createChatInviteLink` — https://core.telegram.org/bots/api#createchatinvitelink

use simula_botapi_types::{methods, types as bot};
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};
use crate::invites;

pub async fn handle(
    ctx: BotCtx,
    params: methods::CreateChatInviteLink,
) -> BotResult<bot::ChatInviteLink> {
    let request = params.creates_join_request == Some(true);
    invites::check(params.name.as_deref(), params.member_limit, request)?;
    let chat = invites::chat(&ctx, &params.chat_id).await?;
    let invite = ctx
        .call(
            functions::messages::ExportChatInvite {
                request_needed: request,
                peer: ctx.input_peer(chat),
                expire_date: params
                    .expire_date
                    .map(|d| d.clamp(0, i64::from(i32::MAX)) as i32),
                usage_limit: params.member_limit.map(|l| l as i32),
                title: params.name,
                ..Default::default()
            },
            ErrorContext::Plain,
        )
        .await?;
    invites::link(&ctx, &invite, &[]).await
}
