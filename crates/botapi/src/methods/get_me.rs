//! `getMe`

use simula_botapi_types::{methods, types as bot};
use simula_tl::{enums, functions};

use crate::convert;
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

pub async fn handle(ctx: BotCtx, _params: methods::GetMe) -> BotResult<bot::User> {
    let users = ctx
        .call(
            functions::users::GetUsers {
                id: vec![enums::InputUser::InputUserSelf],
            },
            ErrorContext::default(),
        )
        .await?;
    match users.first() {
        Some(enums::User::User(me)) => Ok(convert::user::user(me, true)),
        _ => Err(BotError::internal("users.getUsers returned no self user")),
    }
}
