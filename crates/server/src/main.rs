//! `simula`: one process serving the Bot API (`/bot<token>/<method>`), the Control API
//! (`/_simula/v1/...`), client sessions (`/session`, JSON-TL over WebSocket) and, with
//! `--web-dir`, the built web client at `/` for a sandbox.

mod config;

use axum::Router;
use simula_botapi::Gateway;
use simula_core::{Sandbox, SandboxConfig};
use tower_http::services::{ServeDir, ServeFile};
use tracing_subscriber::EnvFilter;

use config::Config;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = match Config::from_args() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    if let Err(error) = run(config).await {
        eprintln!("simula: {error}");
        std::process::exit(1);
    }
}

async fn run(config: Config) -> std::io::Result<()> {
    let path = match &config.data_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            Some(dir.join("default.db"))
        }
        None => None,
    };
    let sandbox = Sandbox::open(SandboxConfig {
        id: "default".into(),
        path: path.clone(),
    })?;
    let gateway = Gateway::new(sandbox.clone());

    let mut app = Router::new()
        .nest("/_simula/v1", simula_control::router(sandbox.clone()))
        .merge(simula_session::router(sandbox.clone()))
        .merge(simula_botapi::router(gateway));
    // The client's assets live under `/_simula/web/` (see apps/web/vite.config.ts), clear
    // of the Bot API's `/<token>/<method>`; anything else unknown gets `index.html`.
    if let Some(dir) = &config.web_dir {
        if !dir.join("index.html").is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "no web client in {} (build it with `bun run --cwd apps/web build`)",
                    dir.display()
                ),
            ));
        }
        app = app
            .fallback_service(ServeDir::new(dir).fallback(ServeFile::new(dir.join("index.html"))));
    }

    let listener = tokio::net::TcpListener::bind((config.host, config.port)).await?;
    let address = listener.local_addr()?;
    tracing::info!(
        storage = %path.map_or_else(|| "memory".into(), |p| p.display().to_string()),
        web = %config.web_dir.as_ref().map_or_else(|| "off".into(), |d| format!("http://{address}/ from {}", d.display())),
        "Simula listening on http://{address}  (Bot API: /bot<token>/<method>, Control API: /_simula/v1, sessions: /session)"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
}

/// Ctrl-C, or SIGTERM (what `docker stop` sends; as PID 1 it would otherwise be ignored).
async fn shutdown() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
    tracing::info!("shutting down");
}
