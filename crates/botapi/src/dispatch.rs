//! Method name → handler. Names are case-insensitive, as on api.telegram.org. Documented
//! methods without a handler answer 501; unknown names answer 404.

use std::future::Future;

use serde_json::{Map, Value};
use simula_botapi_types::{Method, Rule, Violation, registry};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::http::DecodedRequest;
use crate::methods;

/// Whether documented limits are errors. Some methods clamp instead (getUpdates' limit).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Limits {
    Enforce,
    Clamp,
}

pub async fn dispatch(ctx: BotCtx, method: &str, request: DecodedRequest) -> BotResult<Value> {
    let params = request.params;
    match method.to_ascii_lowercase().as_str() {
        "deletewebhook" => run(ctx, params, Limits::Enforce, methods::delete_webhook::handle).await,
        "getme" => run(ctx, params, Limits::Enforce, methods::get_me::handle).await,
        "getupdates" => run(ctx, params, Limits::Clamp, methods::get_updates::handle).await,
        "getwebhookinfo" => run(ctx, params, Limits::Enforce, methods::get_webhook_info::handle).await,
        "sendmessage" => run(ctx, params, Limits::Enforce, methods::send_message::handle).await,
        lower => match registry::METHODS.iter().find(|m| m.name.to_ascii_lowercase() == lower) {
            Some(info) => Err(BotError::not_implemented(info.name)),
            None => Err(BotError::not_found()),
        },
    }
}

async fn run<M, F, Fut>(ctx: BotCtx, params: Map<String, Value>, limits: Limits, handler: F) -> BotResult<Value>
where
    M: Method,
    F: FnOnce(BotCtx, M) -> Fut,
    Fut: Future<Output = BotResult<M::Return>>,
{
    let params: M = parse(params)?;
    if limits == Limits::Enforce
        && let Err(violations) = params.validate()
    {
        return Err(violation(&violations[0]));
    }
    Ok(serde_json::to_value(handler(ctx, params).await?)?)
}

fn parse<M: Method>(params: Map<String, Value>) -> BotResult<M> {
    serde_path_to_error::deserialize(Value::Object(params)).map_err(|error| {
        let path = error.path().to_string();
        let inner = error.into_inner().to_string();
        match inner.strip_prefix("missing field `").and_then(|s| s.strip_suffix('`')) {
            // telegram-bot-api's own wording for the most common one.
            Some("text") => BotError::bad_request("message text is empty"),
            Some(field) => BotError::bad_request(format!("{field} is empty")),
            None => BotError::bad_request(format!("can't parse {path}: {inner}")),
        }
    })
}

fn violation(v: &Violation) -> BotError {
    let detail = match &v.rule {
        Rule::Chars { max: Some(max), actual, .. } if actual > max => format!("{} is too long", v.path),
        Rule::Bytes { max: Some(max), actual, .. } if actual > max => format!("{} is too long", v.path),
        Rule::Chars { .. } | Rule::Bytes { .. } => format!("{} is too short", v.path),
        Rule::Items { min: Some(min), actual, .. } if actual < min => format!("{} must have at least {min} items", v.path),
        Rule::Items { max: Some(max), .. } => format!("{} must have at most {max} items", v.path),
        Rule::Items { .. } => format!("{} has a wrong number of items", v.path),
        Rule::Range { .. } => format!("{} is out of range", v.path),
        Rule::OneOf { actual, .. } => format!("unsupported {} \"{actual}\"", v.path),
    };
    BotError::bad_request(detail)
}
