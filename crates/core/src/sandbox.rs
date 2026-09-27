//! A sandbox is one isolated Telegram world. Its state lives on a dedicated thread that
//! runs jobs one at a time, each in its own transaction: no locks, deterministic order.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use simula_tl::{Function, RemoteCall, enums};
use tokio::sync::{broadcast, oneshot};

use crate::clock::Clock;
use crate::ctx::{Caller, Ctx, World};
use crate::error::{RpcError, RpcResult};
use crate::events::Event;
use crate::ids::{Peer, UserId, access_hash};
use crate::outbox::AccountUpdates;
use crate::rpc::{self, Reply};
use crate::services::peers;
use crate::store::{self, accounts::Account};
use crate::waits::{Deferred, Waits};

type Job = Box<dyn FnOnce(&mut World) + Send>;

/// Awaited for every published batch before the call that produced it returns, so an
/// action's effects are visible to gateways as soon as it completes. Listeners may call
/// back into the sandbox (the gateway fetches replied messages, as tdlib does).
pub type UpdateListener =
    Arc<dyn Fn(Arc<AccountUpdates>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

const CHANNEL_CAPACITY: usize = 4096;

/// How long a user's call waits for a bot's answer (callback queries) before
/// `BOT_RESPONSE_TIMEOUT`. An approximation of Telegram's server-side limit.
pub const DEFAULT_ANSWER_TIMEOUT: Duration = Duration::from_secs(15);

pub struct SandboxConfig {
    pub id: String,
    /// SQLite file; `None` keeps the sandbox in memory.
    pub path: Option<PathBuf>,
}

pub struct Sandbox {
    id: String,
    jobs: mpsc::Sender<Job>,
    updates: broadcast::Sender<Arc<AccountUpdates>>,
    events: broadcast::Sender<Arc<Event>>,
    listeners: std::sync::Mutex<Vec<UpdateListener>>,
    clock: Arc<Clock>,
    secret: [u8; 32],
    waits: Arc<Waits>,
    answer_timeout: std::sync::Mutex<Duration>,
}

impl Sandbox {
    pub fn open(config: SandboxConfig) -> std::io::Result<Arc<Self>> {
        let clock = Arc::new(Clock::default());
        let waits = Arc::new(Waits::default());
        let mut world = World::open(config.path.as_deref(), clock.clone(), waits.clone())
            .map_err(std::io::Error::other)?;
        let secret = world.secret();
        let (jobs, queue) = mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name(format!("sandbox-{}", config.id))
            .spawn(move || {
                for job in queue {
                    job(&mut world);
                }
            })?;
        Ok(Arc::new(Self {
            id: config.id,
            jobs,
            updates: broadcast::channel(CHANNEL_CAPACITY).0,
            events: broadcast::channel(CHANNEL_CAPACITY).0,
            listeners: std::sync::Mutex::default(),
            clock,
            secret,
            waits,
            answer_timeout: std::sync::Mutex::new(DEFAULT_ANSWER_TIMEOUT),
        }))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Run `f` on the sandbox thread.
    pub async fn run<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut World) -> R + Send + 'static,
    ) -> R {
        let (reply, result) = oneshot::channel();
        self.jobs
            .send(Box::new(move |world| {
                let _ = reply.send(f(world));
            }))
            .expect("sandbox thread stopped");
        result.await.expect("sandbox thread stopped")
    }

    /// Execute a TL function as `caller`.
    pub async fn invoke(&self, caller: Caller, call: Function) -> RpcResult<Box<dyn Reply>> {
        let method = call.name();
        let request = serde_json::to_value(&call).unwrap_or_default();
        let started = Instant::now();
        let outcome = self
            .run(move |world| {
                world.transact(Some(caller), |ctx| {
                    let reply = rpc::dispatch(ctx, call)?;
                    Ok((reply, ctx.take_deferred()))
                })
            })
            .await;

        let (result, deferred, updates) = match outcome {
            Ok(((reply, deferred), updates)) => (Ok(reply), deferred, updates),
            Err(error) => (Err(error), None, Vec::new()),
        };
        self.publish(updates).await;
        let result = match deferred {
            Some(deferred) => self.wait(deferred).await,
            None => result,
        };
        self.emit(Event::Rpc {
            at_ms: self.clock.now_ms(),
            account: caller.account,
            method,
            request,
            result: result
                .as_ref()
                .map(|reply| reply.to_json())
                .map_err(Clone::clone),
            duration_us: started.elapsed().as_micros() as u64,
        });
        result
    }

    /// The answer another account gives a deferred call, or its timeout error.
    async fn wait(&self, deferred: Deferred) -> RpcResult<Box<dyn Reply>> {
        let timeout = *self.answer_timeout.lock().expect("timeout lock");
        match tokio::time::timeout(timeout, deferred.receiver).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(_)) | Err(_) => {
                self.waits.cancel(deferred.key);
                Err(deferred.timeout_error)
            }
        }
    }

    /// How long calls wait for other accounts' answers (tests shorten it).
    pub fn set_answer_timeout(&self, timeout: Duration) {
        *self.answer_timeout.lock().expect("timeout lock") = timeout;
    }

    /// Typed [`Sandbox::invoke`].
    pub async fn call<F>(&self, caller: Caller, call: F) -> RpcResult<F::Return>
    where
        F: RemoteCall + Into<Function>,
        F::Return: 'static,
    {
        let reply = self.invoke(caller, call.into()).await?;
        reply
            .into_any()
            .downcast::<F::Return>()
            .map(|boxed| *boxed)
            .map_err(|_| RpcError::internal("reply type does not match the function's return type"))
    }

    /// Run an operation outside TL (Control API) in a transaction; its updates are delivered.
    pub async fn admin<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Ctx<'_>) -> RpcResult<R> + Send + 'static,
    ) -> RpcResult<R> {
        let (result, updates) = self.run(move |world| world.transact(None, f)).await?;
        self.publish(updates).await;
        Ok(result)
    }

    pub async fn account(&self, id: UserId) -> RpcResult<Option<Account>> {
        self.run(move |world| store::accounts::get(world.connection(), id))
            .await
            .map_err(Into::into)
    }

    pub async fn account_by_token(&self, token: String) -> RpcResult<Option<Account>> {
        self.run(move |world| store::accounts::by_token(world.connection(), &token))
            .await
            .map_err(Into::into)
    }

    pub async fn accounts(&self) -> RpcResult<Vec<Account>> {
        self.run(|world| store::accounts::list(world.connection()))
            .await
            .map_err(Into::into)
    }

    /// The `InputPeer` that `viewer` uses to address `peer` (access hash included).
    pub fn input_peer(&self, viewer: UserId, peer: Peer) -> enums::InputPeer {
        match peer {
            Peer::User(id) if id == viewer => enums::InputPeer::InputPeerSelf,
            _ => {
                let hash = access_hash(&self.secret, viewer, peer);
                peers::input_peer_with_hash(peer, hash)
            }
        }
    }

    pub fn add_listener(&self, listener: UpdateListener) {
        self.listeners
            .lock()
            .expect("listeners lock")
            .push(listener);
    }

    pub fn subscribe_updates(&self) -> broadcast::Receiver<Arc<AccountUpdates>> {
        self.updates.subscribe()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Arc<Event>> {
        self.events.subscribe()
    }

    pub fn emit(&self, event: Event) {
        let _ = self.events.send(Arc::new(event));
    }

    async fn publish(&self, updates: Vec<AccountUpdates>) {
        for batch in updates {
            self.emit(Event::Updates {
                at_ms: self.clock.now_ms(),
                account: batch.account,
                updates: serde_json::to_value(&batch.updates).unwrap_or_default(),
            });
            let batch = Arc::new(batch);
            let listeners = self.listeners.lock().expect("listeners lock").clone();
            for listener in &listeners {
                listener(batch.clone()).await;
            }
            let _ = self.updates.send(batch);
        }
    }
}
