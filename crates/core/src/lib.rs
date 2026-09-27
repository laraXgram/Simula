//! Simula's engine at the MTProto level: accounts, peers, message boxes, updates and the
//! TL function handlers. Gateways (Bot API, client sessions, Control API) sit on top.

pub mod clock;
pub mod ctx;
pub mod error;
pub mod events;
pub mod ids;
pub mod outbox;
pub mod render;
pub mod rpc;
pub mod sandbox;
pub mod services;
pub mod store;
pub mod waits;

pub use ctx::{Caller, Ctx, SessionId};
pub use error::{RpcError, RpcResult};
pub use ids::{ChannelId, ChatId, Peer, UserId};
pub use outbox::AccountUpdates;
pub use sandbox::{Sandbox, SandboxConfig, UpdateListener};
pub use store::accounts::Account;
