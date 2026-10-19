//! `upload.saveBigFilePart`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::files;

pub fn handle(ctx: &mut Ctx<'_>, req: functions::upload::SaveBigFilePart) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    files::save_part(
        ctx,
        caller,
        req.file_id,
        req.file_part,
        Some(req.file_total_parts),
        &req.bytes,
    )?;
    Ok(true)
}
