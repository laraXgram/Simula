-- Forums and threads. A channel message's thread: in forums, its topic (1 = General);
-- in other supergroups, the message its reply chain starts from (none when not a reply).
ALTER TABLE channel_messages ADD COLUMN thread_id INTEGER;
CREATE INDEX channel_messages_by_thread ON channel_messages(channel_id, thread_id, id);

-- Topics of forums. A topic's id is the id of the service message that created it;
-- General is 1.
CREATE TABLE forum_topics (
    channel_id    INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    id            INTEGER NOT NULL,
    title         TEXT    NOT NULL,
    icon_color    INTEGER NOT NULL,
    icon_emoji_id INTEGER,
    creator_id    INTEGER NOT NULL,
    date          INTEGER NOT NULL,
    closed        INTEGER NOT NULL DEFAULT 0,
    hidden        INTEGER NOT NULL DEFAULT 0,  -- General only
    pinned        INTEGER,                     -- position among pinned topics
    PRIMARY KEY (channel_id, id)
);
