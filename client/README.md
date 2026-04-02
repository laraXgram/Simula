# Client

React-based Telegram-like client for testing and developing bots locally.

## Purpose

A web/desktop application that simulates the Telegram client experience, providing:
- Telegram-like chat interface
- Bot management (similar to BotFather)
- Multi-user simulation
- Real-time message updates via WebSocket
- Debug panel for inspecting requests/responses

## Features

- Telegram-inspired UI with TailwindCSS
- Chat interface (Private/Group/Channel)
- Real-time messaging via WebSocket
- Bot creation and management
- User switcher (simulate multiple users)
- Debug panel with request/response viewer
- Support for all message types (text, media, keyboards, etc.)

## Routes

- `/` - Home / Bot list
- `/bots` - Bot management
- `/chat` - Chat interface
- `/debug` - Debug panel
- `/settings` - Settings

## Usage

```bash
# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Building Desktop App

```bash
# Install Tauri CLI
npm install -D @tauri-apps/cli

# Run desktop app in dev mode
npm run tauri dev

# Build desktop app
npm run tauri build
```

## Environment Variables

Create a `.env` file:

```env
VITE_API_URL=http://localhost:8080
VITE_WS_URL=ws://localhost:8080/ws
```
