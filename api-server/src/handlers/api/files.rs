use super::*;
use crate::generated::methods::GetFileRequest;

pub fn handle_get_file(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetFileRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, String, Option<i64>, String)> = conn
        .query_row(
            "SELECT file_id, file_unique_id, file_size, file_path FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.file_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((file_id, file_unique_id, file_size, file_path)) = row else {
        return Err(ApiError::not_found("file not found"));
    };

    Ok(json!({
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "file_path": file_path
    }))
}
