#![cfg_attr(all(not(debug_assertions), target_os = "windows"), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Mutex;

use tauri::{api::path::resource_dir, AppHandle, Manager, RunEvent};

struct ApiProcess(Mutex<Option<Child>>);

fn server_binary_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "simula-api-server.exe"
    } else {
        "simula-api-server"
    }
}

fn resolve_server_binary(app: &AppHandle) -> Result<PathBuf, String> {
    let binary_name = server_binary_name();

    if let Some(resource_root) = resource_dir(&app.config()) {
        let resource_path = resource_root.join(binary_name);
        if resource_path.exists() {
            return Ok(resource_path);
        }
    }

    let current_dir = std::env::current_dir().map_err(|e| format!("failed to read current_dir: {e}"))?;
    let local_dev_path = current_dir
        .join("../api-server/target/release")
        .join(binary_name);

    if local_dev_path.exists() {
        return Ok(local_dev_path);
    }

    Err(format!(
        "api binary not found. expected '{}' in resources or ../api-server/target/release",
        binary_name
    ))
}

fn stop_api_process(app: &AppHandle) {
    if let Some(state) = app.try_state::<ApiProcess>() {
        if let Ok(mut guard) = state.0.lock() {
            if let Some(mut child) = guard.take() {
                let _ = child.kill();
            }
        }
    }
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let app_handle = app.handle();
            let server_binary = resolve_server_binary(&app_handle)?;

            let child = Command::new(server_binary)
                .env("API_HOST", "127.0.0.1")
                .env("API_PORT", "8081")
                .spawn()
                .map_err(|e| format!("failed to launch api server: {e}"))?;

            app.manage(ApiProcess(Mutex::new(Some(child))));
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(move |app_handle, event| {
            if let RunEvent::ExitRequested { .. } = event {
                stop_api_process(&app_handle);
            }
        });
}
