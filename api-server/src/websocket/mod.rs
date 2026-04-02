use actix::{Actor, ActorContext, AsyncContext, StreamHandler};
use actix_web::{
    get,
    web::{self, Data, Payload, Query},
    Error, HttpRequest, HttpResponse,
};
use actix_web_actors::ws;
use rusqlite::params;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::sync::broadcast;

use crate::database::{ensure_bot, lock_db, AppState};
use crate::types::strip_nulls;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);
const CHANNEL_CAPACITY: usize = 1024;

pub struct WebSocketHub {
    channels: Mutex<HashMap<String, broadcast::Sender<String>>>,
}

impl WebSocketHub {
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self, token: &str) -> broadcast::Receiver<String> {
        self.sender_for(token).subscribe()
    }

    pub fn publish_json(&self, token: &str, payload: &Value) {
        let sender = self.sender_for(token);
        let _ = sender.send(payload.to_string());
    }

    fn sender_for(&self, token: &str) -> broadcast::Sender<String> {
        if let Ok(mut channels) = self.channels.lock() {
            return channels
                .entry(token.to_string())
                .or_insert_with(|| {
                    let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
                    sender
                })
                .clone();
        }

        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        sender
    }
}

impl Default for WebSocketHub {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Deserialize)]
pub struct WsUpdatesQuery {
    pub last_update_id: Option<i64>,
}

#[get("/ws/bot{token}")]
pub async fn ws_bot_updates(
    req: HttpRequest,
    stream: Payload,
    state: Data<AppState>,
    path: web::Path<String>,
    query: Query<WsUpdatesQuery>,
) -> Result<HttpResponse, Error> {
    let token = path.into_inner();
    let since_id = query.last_update_id.unwrap_or(0);

    let initial_updates = {
        let mut conn = lock_db(&state).map_err(actix_web::error::ErrorInternalServerError)?;
        let bot = ensure_bot(&mut conn, &token).map_err(actix_web::error::ErrorInternalServerError)?;

        let mut stmt = conn
            .prepare(
                "SELECT update_json FROM updates
                 WHERE bot_id = ?1 AND update_id > ?2
                 ORDER BY update_id ASC
                 LIMIT 100",
            )
            .map_err(actix_web::error::ErrorInternalServerError)?;

        let rows = stmt
            .query_map(params![bot.id, since_id], |row| row.get::<_, String>(0))
            .map_err(actix_web::error::ErrorInternalServerError)?;

        let mut values = Vec::new();
        for row in rows {
            let raw = row.map_err(actix_web::error::ErrorInternalServerError)?;
            values.push(raw);
        }
        values
    };

    let receiver = state.ws_hub.subscribe(&token);
    ws::start(WsSession::new(receiver, initial_updates), &req, stream)
}

struct WsSession {
    rx: broadcast::Receiver<String>,
    initial_updates: Vec<String>,
    hb: Instant,
}

impl WsSession {
    fn new(rx: broadcast::Receiver<String>, initial_updates: Vec<String>) -> Self {
        Self {
            rx,
            initial_updates,
            hb: Instant::now(),
        }
    }

    fn setup_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }
            ctx.ping(b"ping");
        });
    }

    fn setup_update_pump(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_millis(150), |act, ctx| {
            loop {
                match act.rx.try_recv() {
                    Ok(message) => ctx.text(message),
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                    Err(broadcast::error::TryRecvError::Closed) => break,
                }
            }
        });
    }
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        for update in &self.initial_updates {
            if let Ok(parsed) = serde_json::from_str::<Value>(update) {
                ctx.text(strip_nulls(parsed).to_string());
            } else {
                ctx.text(update.clone());
            }
        }

        self.setup_heartbeat(ctx);
        self.setup_update_pump(ctx);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(ws::Message::Text(_)) | Ok(ws::Message::Binary(_)) | Ok(ws::Message::Continuation(_)) => {}
            Ok(ws::Message::Nop) => {}
            Err(_) => ctx.stop(),
        }
    }
}

