mod database;
mod generated;
mod handlers;
mod routes;
mod types;
mod websocket;

use actix_files::{Files, NamedFile};
use actix_cors::Cors;
use actix_web::dev::Service;
use actix_web::{web, web::Data, App, HttpResponse, HttpServer};
use futures_util::FutureExt;
use rusqlite::Connection;
use serde_json::json;
use std::{collections::VecDeque, env, path::PathBuf, sync::Mutex};

use crate::database::{
    init_database, is_api_enabled, AppState,
};
use crate::routes::{
    bot_api_get, bot_api_post, file_download, health, runtime_env_get, runtime_env_set,
    runtime_info, runtime_logs, runtime_logs_clear, runtime_power_set, runtime_power_status,
    runtime_service_action, runtime_service_status,
    sim_bootstrap, sim_clear_history, sim_create_bot,
    sim_create_group, sim_create_group_invite_link,
    sim_delete_group,
    sim_delete_user,
    sim_delete_user_profile_audio,
    sim_upload_user_profile_audio,
    sim_add_user_chat_boosts,
    sim_remove_user_chat_boosts,
    sim_delete_owned_gift,
    sim_set_user_profile_audio,
    sim_get_privacy_mode,
    sim_open_channel_direct_messages,
    sim_mark_channel_message_view,
    sim_remove_business_connection,
    sim_set_business_connection,
    sim_set_bot_group_membership,
    sim_set_privacy_mode,
    sim_join_group, sim_join_group_by_invite_link, sim_leave_group,
    sim_approve_join_request, sim_decline_join_request,
    sim_update_group,
    sim_callback_query_answer, sim_choose_inline_result, sim_edit_user_message_media, sim_inline_query_answer, sim_press_inline_button, sim_send_inline_query, sim_send_user_message, sim_set_user_reaction, sim_update_bot, sim_upsert_user,
    sim_pay_invoice, sim_purchase_paid_media, sim_poll_voters, sim_vote_poll,
    sim_send_user_contact, sim_send_user_dice, sim_send_user_game, sim_send_user_location, sim_send_user_media, sim_send_user_venue,
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
        .unwrap_or(8081);
    let web_port: u16 = 8888;
    let db_path = env::var("DATABASE_URL").unwrap_or_else(|_| "simula.db".to_string());
    let web_dist_dir = match env::var("SIMULA_WEB_DIST_DIR") {
        Ok(path) => {
            let path = PathBuf::from(path);
            if path.exists() {
                Some(path)
            } else {
                log::warn!(
                    "SIMULA_WEB_DIST_DIR is set but path does not exist: {}",
                    path.display()
                );
                None
            }
        }
        Err(_) => None,
    };

    let mut conn = Connection::open(db_path).map_err(std::io::Error::other)?;
    init_database(&mut conn).map_err(std::io::Error::other)?;

    let state = Data::new(AppState {
        db: Mutex::new(conn),
        ws_hub: WebSocketHub::new(),
        runtime_logs: Mutex::new(VecDeque::new()),
        api_enabled: Mutex::new(true),
        runtime_transition: Mutex::new(None),
    });

    let auto_close_state = state.clone();
    actix_web::rt::spawn(async move {
        loop {
            if let Err(error) = crate::handlers::client::messages::handle_auto_close_due_polls(&auto_close_state) {
                log::warn!("auto-close poll sweep failed: {}", error.description);
            }
            if let Err(error) = crate::handlers::client::channels::handle_auto_publish_due_suggested_posts(&auto_close_state) {
                log::warn!("auto-publish suggested-post sweep failed: {}", error.description);
            }
            actix_web::rt::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    let webhook_retry_state = state.clone();
    actix_web::rt::spawn(async move {
        loop {
            let state_for_retry = webhook_retry_state.clone();
            let _ = actix_web::rt::task::spawn_blocking(move || {
                crate::handlers::client::webhook::retry_all_pending_webhooks(&state_for_retry, 200);
            })
            .await;

            actix_web::rt::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });

    println!("Simula API Server starting on http://{host}:{port}");

    let api_state = state.clone();
    let api_server = HttpServer::new(move || {
        App::new()
            .app_data(api_state.clone())
            .wrap(Cors::permissive())
            .wrap_fn(|req, srv| {
                let state = req.app_data::<Data<AppState>>().cloned();
                let path = req.path().to_string();

                if let Some(app_state) = state.clone() {
                    let bypass_runtime_power = path == "/health"
                        || path.starts_with("/client-api/runtime")
                        || path.starts_with("/ws/");
                    if !is_api_enabled(&app_state) && !bypass_runtime_power {
                        let response_payload = json!({
                            "ok": false,
                            "error_code": 503,
                            "description": "Service Unavailable: runtime power is off",
                        });
                        let response = HttpResponse::ServiceUnavailable()
                            .json(response_payload)
                            .map_into_right_body();

                        return async move { Ok(req.into_response(response)) }.boxed_local();
                    }
                }

                let fut = srv.call(req);
                async move {
                    let response = fut.await?.map_into_left_body();
                    Ok(response)
                }
                .boxed_local()
            })
            .service(health)
            .service(runtime_info)
            .service(runtime_logs)
            .service(runtime_logs_clear)
            .service(runtime_power_status)
            .service(runtime_power_set)
            .service(runtime_service_status)
            .service(runtime_service_action)
            .service(runtime_env_get)
            .service(runtime_env_set)
            .service(ws_bot_updates)
            .service(bot_api_get)
            .service(bot_api_post)
            .service(file_download)
            .service(sim_bootstrap)
            .service(sim_get_privacy_mode)
            .service(sim_set_privacy_mode)
            .service(sim_set_business_connection)
            .service(sim_remove_business_connection)
            .service(sim_open_channel_direct_messages)
            .service(sim_send_user_message)
            .service(sim_send_user_media)
            .service(sim_send_user_dice)
            .service(sim_send_user_game)
            .service(sim_send_user_contact)
            .service(sim_send_user_location)
            .service(sim_send_user_venue)
            .service(sim_edit_user_message_media)
            .service(sim_set_user_reaction)
            .service(sim_vote_poll)
            .service(sim_pay_invoice)
            .service(sim_purchase_paid_media)
            .service(sim_press_inline_button)
            .service(sim_send_inline_query)
            .service(sim_inline_query_answer)
            .service(sim_callback_query_answer)
            .service(sim_poll_voters)
            .service(sim_choose_inline_result)
            .service(sim_create_bot)
            .service(sim_create_group)
            .service(sim_create_group_invite_link)
            .service(sim_join_group_by_invite_link)
            .service(sim_approve_join_request)
            .service(sim_decline_join_request)
            .service(sim_delete_group)
            .service(sim_set_bot_group_membership)
            .service(sim_join_group)
            .service(sim_leave_group)
            .service(sim_mark_channel_message_view)
            .service(sim_update_group)
            .service(sim_update_bot)
            .service(sim_upsert_user)
            .service(sim_delete_user)
            .service(sim_set_user_profile_audio)
            .service(sim_upload_user_profile_audio)
            .service(sim_delete_user_profile_audio)
            .service(sim_add_user_chat_boosts)
            .service(sim_remove_user_chat_boosts)
            .service(sim_delete_owned_gift)
            .service(sim_clear_history)
    })
    .bind((host.clone(), port))?
    .run();

    if let Some(web_dist_dir) = web_dist_dir {
        let web_root = web_dist_dir.clone();
        let web_index = web_dist_dir.join("index.html");
        println!(
            "Simula Web Client serving on http://{}:{} (dist: {})",
            host,
            web_port,
            web_dist_dir.display()
        );

        let web_server = HttpServer::new(move || {
            let static_root = web_root.clone();
            let index_file = web_index.clone();
            App::new()
                .service(Files::new("/", static_root).index_file("index.html"))
                .default_service(web::to(move || {
                    let index_file = index_file.clone();
                    async move { NamedFile::open_async(index_file).await }
                }))
        })
        .bind((host, web_port))?
        .run();

        tokio::try_join!(api_server, web_server).map(|_| ())
    } else {
        api_server.await
    }
}
