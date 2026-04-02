# API Server

Rust-based Telegram Bot API server simulation with WebSocket support for real-time updates.

## Purpose

This server simulates the complete Telegram Bot API (`api.telegram.org`) locally, allowing developers to:
- Develop and test bots offline
- Debug bot behavior in real-time
- Avoid rate limits and API restrictions
- Test edge cases safely

## Features

- All Telegram Bot API methods
- WebHook support
- WebSocket for real-time updates
- SQLite database for persistence
- Multi-bot support
- Type-safe with generated Rust types

## API Endpoints

```
POST /bot<token>/<method>  - Execute bot API method
GET  /file/bot<token>/<file_path> - Download file
WS   /ws/bot<token>        - WebSocket connection
```

## Usage

```bash
# Install Rust (if not installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Run server
cargo run

# Run with auto-reload (development)
cargo watch -x run

# Build for production
cargo build --release
```

## Testing

```bash
# Test with curl
curl -X POST http://localhost:8080/bot<YOUR_TOKEN>/getMe

# Run tests
cargo test
```
