-- Basic groups and their members. (Supergroups and channels come with their own box.)
CREATE TABLE chats (
    id          INTEGER PRIMARY KEY,
    title       TEXT    NOT NULL,
    creator_id  INTEGER NOT NULL,
    date        INTEGER NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    deactivated INTEGER NOT NULL DEFAULT 0,
    extra       TEXT    NOT NULL DEFAULT '{}'   -- about, default rights, last bot, …
);

-- Current members. Who left or was removed has no row.
CREATE TABLE chat_members (
    chat_id    INTEGER NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    user_id    INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    role       TEXT    NOT NULL,   -- creator | admin | member
    inviter_id INTEGER NOT NULL,
    date       INTEGER NOT NULL,
    PRIMARY KEY (chat_id, user_id)
);
CREATE INDEX chat_members_by_user ON chat_members(user_id);
