//! Settings from command-line flags, falling back to environment variables.

use std::net::IpAddr;
use std::path::PathBuf;

pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    /// Where sandbox databases live; `None` keeps everything in memory.
    pub data_dir: Option<PathBuf>,
    /// A built web client (`apps/web/dist`) to serve at `/`.
    pub web_dir: Option<PathBuf>,
}

const USAGE: &str =
    "usage: simula [--host ADDR] [--port PORT] [--data-dir DIR | --memory] [--web-dir DIR]

  --host ADDR       listen address (env SIMULA_HOST, default 127.0.0.1)
  --port PORT       listen port (env SIMULA_PORT, default 8081)
  --data-dir DIR    keep sandboxes in DIR (env SIMULA_DATA_DIR, default ./data)
  --memory          keep everything in memory (lost on exit)
  --web-dir DIR     serve the built web client from DIR at / (env SIMULA_WEB_DIR)";

impl Config {
    pub fn from_args() -> Result<Self, String> {
        let env = |key: &str| std::env::var(key).ok();
        let mut host = env("SIMULA_HOST").unwrap_or_else(|| "127.0.0.1".into());
        let mut port = env("SIMULA_PORT").unwrap_or_else(|| "8081".into());
        let mut data_dir = Some(PathBuf::from(
            env("SIMULA_DATA_DIR").unwrap_or_else(|| "data".into()),
        ));
        let mut web_dir = env("SIMULA_WEB_DIR").map(PathBuf::from);

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = || {
                args.next()
                    .ok_or_else(|| format!("{arg} needs a value\n\n{USAGE}"))
            };
            match arg.as_str() {
                "--host" => host = value()?,
                "--port" => port = value()?,
                "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
                "--memory" => data_dir = None,
                "--web-dir" => web_dir = Some(PathBuf::from(value()?)),
                "-h" | "--help" => return Err(USAGE.into()),
                other => return Err(format!("unknown argument {other}\n\n{USAGE}")),
            }
        }
        Ok(Self {
            host: host.parse().map_err(|_| format!("invalid host {host}"))?,
            port: port.parse().map_err(|_| format!("invalid port {port}"))?,
            data_dir,
            web_dir,
        })
    }
}
