-- Sandbox bookkeeping: secret for access hashes, id sequences.
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Users and bots. Rarely-queried attributes live in `extra` (JSON) so new Telegram
-- features rarely need a migration.
CREATE TABLE accounts (
    id          INTEGER PRIMARY KEY,
    is_bot      INTEGER NOT NULL DEFAULT 0,
    first_name  TEXT    NOT NULL,
    last_name   TEXT,
    username    TEXT UNIQUE COLLATE NOCASE,
    phone       TEXT UNIQUE,
    lang_code   TEXT,
    is_premium  INTEGER NOT NULL DEFAULT 0,
    bot_token   TEXT UNIQUE,
    extra       TEXT    NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL
);

-- Per-account update state (the "common box").
CREATE TABLE account_state (
    account_id      INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    pts             INTEGER NOT NULL DEFAULT 0,
    qts             INTEGER NOT NULL DEFAULT 0,
    seq             INTEGER NOT NULL DEFAULT 0,
    next_message_id INTEGER NOT NULL DEFAULT 1
);

-- Messages in per-account boxes (private chats and basic groups): each participant has
-- its own copy with its own id, like Telegram. `global_id` links the copies.
CREATE TABLE messages (
    owner_id   INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    id         INTEGER NOT NULL,
    peer       INTEGER NOT NULL,   -- dialog, Bot API encoding
    from_id    INTEGER,            -- sender, Bot API encoding
    out        INTEGER NOT NULL,
    date       INTEGER NOT NULL,
    edit_date  INTEGER,
    global_id  INTEGER NOT NULL,
    random_id  INTEGER,
    body       TEXT    NOT NULL,   -- JSON-TL `message` as the owner sees it
    deleted    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (owner_id, id)
);
CREATE INDEX messages_by_peer   ON messages(owner_id, peer, id);
CREATE INDEX messages_by_global ON messages(global_id);

-- Every pts-carrying update per account, for getDifference and replays.
CREATE TABLE updates_log (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    pts        INTEGER NOT NULL,
    pts_count  INTEGER NOT NULL,
    date       INTEGER NOT NULL,
    body       TEXT    NOT NULL,   -- JSON-TL Update
    PRIMARY KEY (account_id, pts)
);
