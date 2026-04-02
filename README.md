# Simula

**Offline Telegram Bot Development Environment**

LaraGram Simula is a complete simulation environment for developing Telegram bots without internet dependency or actual Telegram API. It provides a full-featured Bot API server and a Telegram-like client for testing and development.

---

## Features

- **100% Telegram Bot API Coverage** - All methods and types
- **Offline Development** - No internet required
- **Real-time Testing** - WebSocket-based instant updates
- **Multi-user Simulation** - Test with multiple fake users
- **Debug Panel** - Request/response viewer
- **Telegram-like UI** - Familiar interface for testing
- **Desktop Support** - Web + Desktop apps

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- (Optional) Rust 1.75+, Node.js 18+, Python 3.11+

### Installation

```bash
# Clone the repository
git clone https://github.com/laraxgram/Simula.git
cd Simula

# Start all services with Docker
docker-compose up -d

# Access the client
# Web: http://localhost:5173
# API: http://localhost:8080
```

---

## 🛠️ Development

### Scraper
```bash
cd scraper
pip install -r requirements.txt
python src/scraper.py
```

### API Server
```bash
cd api-server
cargo run
```

### Client
```bash
cd client
npm install
npm run dev
```
