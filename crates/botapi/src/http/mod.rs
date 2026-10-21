//! HTTP surface: `/bot<token>/<method>` with GET or POST, like api.telegram.org.

mod decode;
mod legacy;
mod response;

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::{any, get};
use serde_json::Value;
use simula_botapi_types::registry;

use crate::ctx::BotCtx;
use crate::dispatch;
use crate::error::{BotError, BotResult};
use crate::state::Gateway;

pub use decode::{DecodedRequest, Upload};

pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/{token}/{method}", any(handle))
        .route("/file/{token}/{*path}", get(download))
        // Libraries compress request bodies (Telegraf under Bun sends `Content-Encoding: gzip`);
        // api.telegram.org accepts that, so we do too.
        .layer(tower_http::decompression::RequestDecompressionLayer::new())
        .with_state(gateway)
}

async fn handle(
    State(gateway): State<Arc<Gateway>>,
    Path((token, method)): Path<(String, String)>,
    request: Request,
) -> Response {
    let Some(token) = token.strip_prefix("bot") else {
        return response::error(&BotError::not_found());
    };
    let bot = match gateway.authenticate(token).await {
        Ok(bot) => bot,
        Err(error) => return response::error(&error),
    };
    let info = registry::METHODS
        .iter()
        .find(|m| m.name.eq_ignore_ascii_case(&method));
    let mut decoded = match decode::decode(request, info).await {
        Ok(decoded) => decoded,
        Err(error) => return response::error(&error),
    };
    let mut ctx = BotCtx::new(gateway, bot);
    ctx.uploads = Arc::new(std::mem::take(&mut decoded.files));
    let description = ctx.description_slot();
    match execute(ctx, &method, decoded).await {
        Ok(result) => response::ok(result, *description.lock().expect("description lock")),
        Err(error) => response::error(&error),
    }
}

/// Run a decoded method call as the bot.
async fn execute(ctx: BotCtx, method: &str, mut decoded: DecodedRequest) -> BotResult<Value> {
    if let Some(info) = info(method) {
        legacy::rewrite(info, &mut decoded.params);
    }
    dispatch::dispatch(ctx, method, decoded).await
}

/// Run a method call given as a JSON object, like the ones bots return in webhook
/// responses.
pub(crate) async fn execute_json(
    ctx: BotCtx,
    method: &str,
    params: serde_json::Map<String, Value>,
) -> BotResult<Value> {
    let decoded = DecodedRequest {
        params: decode::json_params(info(method), params),
        ..Default::default()
    };
    execute(ctx, method, decoded).await
}

fn info(method: &str) -> Option<&'static simula_botapi_types::MethodInfo> {
    registry::METHODS
        .iter()
        .find(|m| m.name.eq_ignore_ascii_case(method))
}

/// `/file/bot<token>/<file_path>`: a file the bot asked for with `getFile`.
async fn download(
    State(gateway): State<Arc<Gateway>>,
    Path((token, path)): Path<(String, String)>,
) -> Response {
    let Some(token) = token.strip_prefix("bot") else {
        return response::error(&BotError::not_found());
    };
    let bot = match gateway.authenticate(token).await {
        Ok(bot) => bot,
        Err(error) => return response::error(&error),
    };
    let ctx = BotCtx::new(gateway, bot);
    let Some(id) = ctx
        .state()
        .downloads
        .lock()
        .expect("downloads lock")
        .get(&path)
    else {
        return response::error(&BotError::not_found());
    };
    match crate::files::download::read(&ctx, &id, crate::files::download::MAX_DOWNLOAD_SIZE).await {
        Ok(Some((data, _))) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            data,
        )
            .into_response(),
        _ => response::error(&BotError::not_found()),
    }
}
