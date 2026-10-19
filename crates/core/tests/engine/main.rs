//! The engine at the MTProto level, through `Sandbox::call`. One test binary (each links
//! the whole TL library, so separate binaries cost disk and link time).

mod common;

mod callbacks;
mod channels;
mod dialogs;
mod edit_delete;
mod forums;
mod groups;
mod pins;
mod private_chat;
