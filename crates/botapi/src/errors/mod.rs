//! The two translation steps a real server applies to MTProto errors: tdlib turns them
//! into human messages, telegram-bot-api prefixes and formats them.

pub mod botapi;
pub mod tdlib;
