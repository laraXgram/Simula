//! `upload.saveFilePart`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::files;

pub fn handle(ctx: &mut Ctx<'_>, req: functions::upload::SaveFilePart) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    files::save_part(ctx, caller, req.file_id, req.file_part, None, &req.bytes)?;
    Ok(true)
}
