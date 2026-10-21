//! `getFile` — https://core.telegram.org/bots/api#getfile

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::files::download::{self, MAX_DOWNLOAD_SIZE};
use crate::files::id::FileId;

/// The file's size and a path to download it from (`/file/bot<token>/<file_path>`).
pub async fn handle(ctx: BotCtx, params: methods::GetFile) -> BotResult<bot::File> {
    if params.file_id.is_empty() {
        return Err(BotError::bad_request("file_id not specified"));
    }
    let invalid = || BotError::bad_request("invalid file_id");
    let id = FileId::decode(&params.file_id).map_err(|_| invalid())?;
    let (data, storage) = match download::read(&ctx, &id, MAX_DOWNLOAD_SIZE).await {
        Ok(Some(found)) => found,
        Ok(None) => return Err(BotError::bad_request("file is too big")),
        Err(_) => return Err(invalid()),
    };
    let extension = download::extension(&id, &storage);
    let path = ctx
        .state()
        .downloads
        .lock()
        .expect("downloads lock")
        .path_for(&id, extension);
    Ok(bot::File {
        file_id: params.file_id,
        file_unique_id: id.unique(),
        file_size: Some(data.len() as i64),
        file_path: Some(path),
    })
}
