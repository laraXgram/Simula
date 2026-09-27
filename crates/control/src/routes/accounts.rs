//! Users and bots.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use serde_json::Value;
use simula_core::services::accounts::{self, NewBot, NewUser};
use simula_core::{RpcError, Sandbox, UserId};

use super::ok;
use crate::error::ControlResult;

pub async fn list(State(sandbox): State<Arc<Sandbox>>) -> ControlResult<Json<Value>> {
    Ok(ok(sandbox.accounts().await?))
}

pub async fn get(
    State(sandbox): State<Arc<Sandbox>>,
    Path(id): Path<i64>,
) -> ControlResult<Json<Value>> {
    let account = sandbox
        .account(UserId(id))
        .await?
        .ok_or_else(RpcError::user_id_invalid)?;
    Ok(ok(account))
}

pub async fn create_user(
    State(sandbox): State<Arc<Sandbox>>,
    Json(new): Json<NewUser>,
) -> ControlResult<Json<Value>> {
    Ok(ok(sandbox
        .admin(move |ctx| accounts::create_user(ctx, new))
        .await?))
}

pub async fn create_bot(
    State(sandbox): State<Arc<Sandbox>>,
    Json(new): Json<NewBot>,
) -> ControlResult<Json<Value>> {
    Ok(ok(sandbox
        .admin(move |ctx| accounts::create_bot(ctx, new))
        .await?))
}
