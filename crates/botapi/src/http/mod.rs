//! HTTP surface: `/bot<token>/<method>` with GET or POST, like api.telegram.org.

mod decode;
mod legacy;
mod response;

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::response::Response;
use axum::routing::any;
use serde_json::Value;
use simula_botapi_types::registry;

use crate::ctx::BotCtx;
use crate::dispatch;
use crate::error::{BotError, BotResult};
use crate::state::Gateway;

pub use decode::DecodedRequest;

pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/{token}/{method}", any(handle))
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
    let decoded = match decode::decode(request, info).await {
        Ok(decoded) => decoded,
        Err(error) => return response::error(&error),
    };
    let ctx = BotCtx::new(gateway, bot);
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
