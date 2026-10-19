//! `upload.getFile`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::files;

/// A chunk of a photo size, document or thumbnail.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::upload::GetFile,
) -> RpcResult<enums::upload::File> {
    ctx.caller()?;
    Ok(files::download(ctx, &req.location, req.offset, req.limit, req.precise)?.into())
}
