//! The engine at the MTProto level, through `Sandbox::call`. One test binary (each links
//! the whole TL library, so separate binaries cost disk and link time).

mod common;

mod callbacks;
mod edit_delete;
mod private_chat;
