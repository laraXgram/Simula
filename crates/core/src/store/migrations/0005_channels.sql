-- Supergroups and channels. Unlike private chats and basic groups, each has one message
-- box of its own: a message is stored once, with one id every member sees, and the
-- channel has its own pts.
CREATE TABLE channels (
    id              INTEGER PRIMARY KEY,
    broadcast       INTEGER NOT NULL,              -- 1 channel, 0 supergroup
    title           TEXT    NOT NULL,
    username        TEXT UNIQUE COLLATE NOCASE,
    about           TEXT    NOT NULL DEFAULT '',
    creator_id      INTEGER NOT NULL,
    date            INTEGER NOT NULL,
    pts             INTEGER NOT NULL DEFAULT 0,
    next_message_id INTEGER NOT NULL DEFAULT 1,
    deleted         INTEGER NOT NULL DEFAULT 0,
    extra           TEXT    NOT NULL DEFAULT '{}'  -- default rights, pinned, last bot, …
);

-- Everyone with a status in the channel: members, admins, the restricted, the banned,
-- and those who left (kept so their history and status stay known).
CREATE TABLE channel_members (
    channel_id    INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id       INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    status        TEXT    NOT NULL,   -- creator | admin | member | restricted | banned | left
    inviter_id    INTEGER NOT NULL,
    date          INTEGER NOT NULL,
    promoted_by   INTEGER,
    kicked_by     INTEGER,
    admin_rights  TEXT,               -- JSON-TL chatAdminRights
    banned_rights TEXT,               -- JSON-TL chatBannedRights
    rank          TEXT,
    PRIMARY KEY (channel_id, user_id)
);
CREATE INDEX channel_members_by_user ON channel_members(user_id);

CREATE TABLE channel_messages (
    channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    id         INTEGER NOT NULL,
    author_id  INTEGER NOT NULL,   -- who sent it, even for posts that don't show it
    date       INTEGER NOT NULL,
    edit_date  INTEGER,
    random_id  INTEGER,
    body       TEXT    NOT NULL,   -- JSON-TL Message, as a non-author member sees it
    deleted    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (channel_id, id)
);
CREATE INDEX channel_messages_by_author ON channel_messages(author_id, channel_id);

-- Every pts-carrying update of a channel, for getChannelDifference.
CREATE TABLE channel_updates (
    channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    pts        INTEGER NOT NULL,
    pts_count  INTEGER NOT NULL,
    date       INTEGER NOT NULL,
    body       TEXT    NOT NULL,
    PRIMARY KEY (channel_id, pts)
);
