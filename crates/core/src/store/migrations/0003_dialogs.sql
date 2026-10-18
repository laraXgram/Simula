-- Read state per dialog of a per-account box. Rows appear when something is read;
-- a missing row means nothing was read yet.
CREATE TABLE dialogs (
    owner_id           INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    peer               INTEGER NOT NULL,   -- Bot API encoding
    read_inbox_max_id  INTEGER NOT NULL DEFAULT 0,
    read_outbox_max_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (owner_id, peer)
);
