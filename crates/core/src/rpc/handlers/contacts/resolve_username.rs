//! `contacts.resolveUsername`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::store;

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::contacts::ResolveUsername,
) -> RpcResult<enums::contacts::ResolvedPeer> {
    let viewer = ctx.caller()?.account;
    let name = req.username.trim_start_matches('@');
    let valid = (4..=32).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && name.as_bytes()[0].is_ascii_alphabetic();
    if !valid {
        return Err(RpcError::bad_request("USERNAME_INVALID"));
    }
    let account = store::accounts::by_username(ctx.db, name)?
        .ok_or_else(|| RpcError::bad_request("USERNAME_NOT_OCCUPIED"))?;
    let viewer_account = ctx.account(viewer)?;
    Ok(types::contacts::ResolvedPeer {
        peer: Peer::User(account.id).to_tl(),
        chats: vec![],
        users: vec![render::user(ctx, &viewer_account, &account)],
    }
    .into())
}
