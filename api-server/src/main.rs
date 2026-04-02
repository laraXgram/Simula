mod database;
mod generated;
mod handlers;
mod routes;
mod types;
mod websocket;

use actix_cors::Cors;
use actix_web::{web::Data, App, HttpServer};
use rusqlite::Connection;
use std::{env, sync::Mutex};

use crate::database::{init_database, AppState};
use crate::routes::{
    bot_api_get, bot_api_post, file_download, health, sim_bootstrap, sim_clear_history, sim_create_bot,
    sim_edit_user_message_media, sim_send_user_message, sim_set_user_reaction, sim_update_bot, sim_upsert_user,
    sim_send_user_media,
};
use crate::websocket::{ws_bot_updates, WebSocketHub};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let host = env::var("API_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("API_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080);
    let db_path = env::var("DATABASE_URL").unwrap_or_else(|_| "laragram.db".to_string());

    let mut conn = Connection::open(db_path).map_err(std::io::Error::other)?;
    init_database(&mut conn).map_err(std::io::Error::other)?;

    let state = Data::new(AppState {
        db: Mutex::new(conn),
        ws_hub: WebSocketHub::new(),
    });

    println!("LaraGram API Server starting on http://{host}:{port}");

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(Cors::permissive())
            .service(health)
            .service(ws_bot_updates)
            .service(bot_api_get)
            .service(bot_api_post)
            .service(file_download)
            .service(sim_bootstrap)
            .service(sim_send_user_message)
                .service(sim_send_user_media)
            .service(sim_edit_user_message_media)
            .service(sim_set_user_reaction)
            .service(sim_create_bot)
            .service(sim_update_bot)
            .service(sim_upsert_user)
            .service(sim_clear_history)
    })
    .bind((host, port))?
    .run()
    .await
}
