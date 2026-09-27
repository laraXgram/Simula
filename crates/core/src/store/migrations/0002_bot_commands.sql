-- A bot's command lists, one per (scope, language). `scope` is a key such as
-- "default", "users" or "peer:<id>" (see services/commands.rs); "" is every language.
CREATE TABLE bot_commands (
    bot_id    INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    scope     TEXT    NOT NULL,
    lang_code TEXT    NOT NULL,
    commands  TEXT    NOT NULL,   -- JSON-TL Vector<BotCommand>
    PRIMARY KEY (bot_id, scope, lang_code)
);
