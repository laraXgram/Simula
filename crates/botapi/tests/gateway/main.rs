//! The Bot API over HTTP, in process: what a bot library would see. One test binary
//! (each links the whole TL library, so separate binaries cost disk and link time).

mod support;

mod bot_flow;
mod callbacks;
mod commands;
mod edit_delete;
mod groups;
mod send_pipeline;
mod webhooks;
