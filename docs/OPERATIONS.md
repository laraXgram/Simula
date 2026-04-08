# Operations Guide

## Runtime Profiles

Use Docker Compose profiles to run a specific environment.

```bash
# Release stack (scraper+generator -> api + client)
./scripts/release-up.sh

# Manual equivalent
docker compose --profile release run --rm scraper
docker compose --profile release up -d --build api-server client

# Development stack (hot-reload oriented)
docker compose --profile dev up --build

# One-shot checks
docker compose --profile test run --rm api-server-test
docker compose --profile test run --rm client-test

# Scraper + generator job
docker compose --profile scraper run --rm scraper
```

## Persistence Layout

Persistent data is stored in named volumes:

- api-data: SQLite database and runtime data
- api-files: uploaded media/files

Backups are written to host folder backups/.

## Backup

```bash
./scripts/backup.sh
```

The backup archive follows this naming pattern:

- simula_YYYYMMDD_HHMMSS.tgz

## Restore

```bash
# Restore latest backup
./scripts/restore.sh

# Restore a specific archive
./scripts/restore.sh simula_20260409_160000.tgz
```

## Safe Restore Procedure

1. Stop running release/dev services.
2. Run restore script.
3. Start services again.
4. Validate with /health and a chat/webhook smoke test.

## Runtime Notes

- Runtime service control defaults to runtime-gate unless OS service manager is configured.
- Runtime logs are available through /client-api/runtime/logs.
- Clear runtime logs via /client-api/runtime/logs/clear.
