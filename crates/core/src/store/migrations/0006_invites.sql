-- Invite links of basic groups, supergroups and channels, and pending join requests.
CREATE TABLE invites (
    hash           TEXT PRIMARY KEY,
    peer           INTEGER NOT NULL,   -- the group or channel, Bot API encoding
    admin_id       INTEGER NOT NULL,   -- who created it
    date           INTEGER NOT NULL,
    expire_date    INTEGER,
    usage_limit    INTEGER,
    usage          INTEGER NOT NULL DEFAULT 0,
    request_needed INTEGER NOT NULL DEFAULT 0,
    revoked        INTEGER NOT NULL DEFAULT 0,
    permanent      INTEGER NOT NULL DEFAULT 0,  -- an admin's primary link
    title          TEXT
);
CREATE INDEX invites_by_peer ON invites(peer, admin_id);

CREATE TABLE join_requests (
    peer    INTEGER NOT NULL,
    user_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    date    INTEGER NOT NULL,
    invite  TEXT    NOT NULL,
    about   TEXT    NOT NULL DEFAULT '',
    PRIMARY KEY (peer, user_id)
);
