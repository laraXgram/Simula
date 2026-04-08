# LaraGram Simula

Telegram Bot API simulator with a Simulated client, realtime updates, runtime controls, and traceable debug tooling.

## Quick Start (Release Profile)

```bash
git clone https://github.com/laraxgram/Simula.git
cd Simula

./scripts/release-up.sh
```

Available endpoints after startup:

- Client: http://127.0.0.1:8888
- API: http://127.0.0.1:8081
- Debug Console: http://127.0.0.1:8888/debug

## Docker Profiles

The root compose file supports dedicated environments:

- release: production-like api + client containers
- dev: hot-reload style api + vite client containers
- test: one-shot quality checks for api + client
- scraper: one-shot Telegram docs scrape + code generation
- ops: one-shot backup/restore containers

### Release

```bash
# Preferred (runs scraper+generator first)
./scripts/release-up.sh

# Manual equivalent
docker compose --profile release run --rm scraper
docker compose --profile release up -d --build api-server client

docker compose --profile release down
```

### Dev

```bash
docker compose --profile dev up --build
```

### Test

```bash
docker compose --profile test run --rm api-server-test
docker compose --profile test run --rm client-test
```

### Scraper

```bash
# Scrape Bot API docs and regenerate Rust/TypeScript generated files
docker compose --profile scraper run --rm scraper
```

## Data Persistence Policy (Volumes + Backup/Restore)

Runtime data lives on named Docker volumes:

- api-data: SQLite and runtime API data
- api-files: uploaded/media files

Backup artifacts are stored under backups/ on the host.

### Create Backup

```bash
./scripts/backup.sh
```

### Restore Backup

```bash
# Restore latest archive
./scripts/restore.sh

# Restore specific archive name or absolute path
./scripts/restore.sh simula_20260409_160000.tgz
```

Operational note:

- Stop release/dev services before restore to avoid concurrent file writes.

## Observability and Debug Workflow

Use /debug for end-to-end debugging:

- Realtime request/response logs from runtime endpoints
- Structured webhook dispatch viewer with URL/status filtering
- Realtime bot updates stream from websocket
- JSON inspector tabs: request, response, and request-vs-response diff
- Export trace bundles for bug reports
- Import trace bundles for offline investigation

Trace bundle format includes runtime logs, websocket updates, selected bot token, API base URL, and export timestamp.

## Optional Desktop Packaging (Tauri)

From the client directory:

```bash
cd client
npm install

# bootstrap tauri files once
npm run tauri:init

# desktop dev session
npm run tauri:dev

# desktop build artifacts
npm run tauri:build
```

This path is optional and kept isolated from the web build pipeline.

## Local Native Development (Without Docker)

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

### Scraper

```bash
cd scraper
pip install -r requirements.txt
python src/scraper.py
python src/generator.py
```

## Contributing

Thank you for considering contributing to the LaraGram Simula! The contribution guide can be found in the [LaraGram documentation](https://laraxgram.github.io/v3/contributions.html).

## Code of Conduct

In order to ensure that the LaraGram community is welcoming to all, please review and abide by the [Code of Conduct](https://laraxgram.github.io/v3/contributions.html#code-of-conduct).

## Security Vulnerabilities

If you discover a security vulnerability within LaraGram, please send an e-mail to LaraXGram via [laraxgram@gmail.com](mailto:laraxgram@gmail.com). All security vulnerabilities will be promptly addressed.

## License

The LaraGram Simula is open-sourced software licensed under the [MIT license](https://opensource.org/licenses/MIT).