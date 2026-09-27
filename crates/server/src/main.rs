//! `simula`: one process serving the Bot API (`/bot<token>/<method>`) and the Control API
//! (`/_simula/v1/...`) for a sandbox.

mod config;

use axum::Router;
use simula_botapi::Gateway;
use simula_core::{Sandbox, SandboxConfig};
use tracing_subscriber::EnvFilter;

use config::Config;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
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
    let sandbox = Sandbox::open(SandboxConfig { id: "default".into(), path: path.clone() })?;
    let gateway = Gateway::new(sandbox.clone());

    let app = Router::new()
        .nest("/_simula/v1", simula_control::router(sandbox.clone()))
        .merge(simula_botapi::router(gateway));

    let listener = tokio::net::TcpListener::bind((config.host, config.port)).await?;
    let address = listener.local_addr()?;
    tracing::info!(
        storage = %path.map_or_else(|| "memory".into(), |p| p.display().to_string()),
        "Simula listening on http://{address}  (Bot API: /bot<token>/<method>, Control API: /_simula/v1)"
    );
    axum::serve(listener, app).with_graceful_shutdown(shutdown()).await
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
