//!
use anyhow::Result;
use async_trait::async_trait;
use core::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, oneshot};

pub mod asset;
pub mod audio;
pub mod project;

pub trait Actor: Send + Sync + Sized + 'static {
    type InitParams: Send;
    // type Envelope: Envelope<Self>;
    type Carrier: Carrier<Self>;
    type Data;

    fn new(params: Self::InitParams, loopback: Handle<Self>) -> Self;

    /// Run once before the first command is processed.
    fn on_start(&self) {}

    /// Run once after the mailbox closes and no more commands will come.
    fn on_stop(&self) {}
}

pub enum Delivery<A: Actor> {
    Read(BoxedReadEnvelope<A>),
    Write(BoxedWriteEnvelope<A>),
}

pub struct Query;
pub struct Modify;

pub trait Permission<A: Actor>: Sized + Send + 'static {
    type Guard: Send;

    /// Runs once per envelope, before `data`/`execute`. No-op for `Ref`;
    /// `Mutate`/`ActorRef` use it to commit the pre-mutation undo entry.
    fn pre_hook(_actor: &mut A) {}

    fn lock(actor: Arc<RwLock<A>>) -> impl Future<Output = Self::Guard>;

    fn delivery<E: Envelope<A, Self> + 'static>(env: E) -> Delivery<A>;
}

impl<A: Actor> Permission<A> for Query {
    type Guard = OwnedRwLockReadGuard<A>;

    fn lock(actor: Arc<RwLock<A>>) -> impl Future<Output = Self::Guard> {
        actor.read_owned()
    }

    fn delivery<E: Envelope<A, Self> + 'static>(env: E) -> Delivery<A> {
        Delivery::Read(Box::new(env))
    }
}
impl<A: Actor> Permission<A> for Modify {
    type Guard = OwnedRwLockWriteGuard<A>;

    fn lock(actor: Arc<RwLock<A>>) -> impl Future<Output = Self::Guard> {
        actor.write_owned()
    }

    fn delivery<E: Envelope<A, Self> + 'static>(env: E) -> Delivery<A> {
        Delivery::Write(Box::new(env))
    }
}

pub trait IntoEnvelope<P: Permission<Self::Actor>>: Command<P> {
    fn into_envelope<R>(self, reply: R) -> Delivery<Self::Actor>
    where
        R: ReplyPort<Self::Output> + 'static;
}

pub trait Command<P: Permission<Self::Actor>>: Send + 'static {
    type Output: Send;
    type Actor: Actor;

    fn execute(
        self,
        actor: <P as Permission<Self::Actor>>::Guard,
    ) -> impl Future<Output = Self::Output> + Send;
}

/// Every command still *executes* and still *produces* an `Output` — the
/// actor's behavior never changes. What varies is what happens to that
/// output afterward. `ReplyPort` is that axis, factored out as its own
/// trait so it applies identically to `Command` and `MutatingCommand`
/// instead of being duplicated (or half-supported) on each.
pub trait ReplyPort<O: Send>: Send {
    fn send(self, output: O);
}

/// Deliver the output back to a caller that is waiting for it.
pub struct Reply<O>(oneshot::Sender<O>);

impl<O: Send> ReplyPort<O> for Reply<O> {
    fn send(self, output: O) {
        // A dropped receiver just means the caller stopped waiting.
        let _ = self.0.send(output);
    }
}

/// Discard the output. The actor still computes it honestly; nobody is
/// listening. Zero runtime cost — `send` simply drops `output`.
pub struct NoReply;

impl<O: Send> ReplyPort<O> for NoReply {
    fn send(self, _output: O) {}
}

#[async_trait]
pub trait Envelope<A: Actor, P: Permission<A>>: Send {
    async fn engage(self: Box<Self>, handle: P::Guard);
}

pub type BoxedReadEnvelope<A> = Box<dyn Envelope<A, Query>>;
pub type BoxedWriteEnvelope<A> = Box<dyn Envelope<A, Modify>>;

pub type BoxedEnvelope<A, P> = Box<dyn Envelope<A, <P as Permission<A>>::Guard>>;

#[async_trait]
impl<A: Actor> Envelope<A, Query> for BoxedReadEnvelope<A> {
    async fn engage(self: Box<Self>, handle: <Query as Permission<A>>::Guard) {
        (*self).engage(handle).await;
    }
}

#[async_trait]
impl<A: Actor> Envelope<A, Modify> for BoxedWriteEnvelope<A> {
    async fn engage(self: Box<Self>, handle: <Modify as Permission<A>>::Guard) {
        (*self).engage(handle).await;
    }
}

struct StdEnvelope<A, C, R, P>
where
    C: Command<P, Actor = A>,
    R: ReplyPort<C::Output>,
    A: Actor,
    P: Permission<A>,
{
    command: C,
    reply: R,
    _actor: PhantomData<fn(P) -> A>,
}

impl<C, P> IntoEnvelope<P> for C
where
    C: Command<P>,
    P: Permission<C::Actor>,
    C::Actor: Actor,
{
    fn into_envelope<R>(self, reply: R) -> Delivery<Self::Actor>
    where
        R: ReplyPort<Self::Output> + 'static,
    {
        P::delivery(StdEnvelope {
            command: self,
            reply,
            _actor: PhantomData,
        })
    }
}

#[async_trait]
impl<A: Actor, C, R, P> Envelope<A, P> for StdEnvelope<A, C, R, P>
where
    P: Permission<A>,
    C: Command<P, Actor = A>,
    R: ReplyPort<C::Output>,
{
    async fn engage(self: Box<Self>, lock: P::Guard) {
        let Self { command, reply, .. } = *self;
        let output = command.execute(lock).await;
        reply.send(output);
    }
}

/// Abstracts over *how* envelopes travel from a `Handle` to the actor
/// task. `TokioMpsc` below is the stock implementation, but anything
/// that can move a `Box<dyn Envelope<A>>` from many producers to one
/// consumer qualifies: a priority queue, an unbounded channel, a
/// metrics-wrapped channel, etc.
#[async_trait]
pub trait Carrier<A: Actor>: Send {
    type Sender: Send + Clone + 'static;
    type Receiver: Send + 'static;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver);

    /// Send a message over the channel.
    ///
    /// # Errors
    /// - Implementation specific
    async fn send(sender: &Self::Sender, envelope: Delivery<A>) -> Result<()>;

    /// Awaits the next envelope, or `None` once the transport is closed.
    ///
    /// # Errors
    /// - Implementation Specific
    async fn recv(receiver: &Self::Receiver) -> Result<Delivery<A>>;
}

pub struct StdCarrier<A: Actor> {
    _p: PhantomData<A>,
}

#[async_trait]
impl<A: Actor> Carrier<A> for StdCarrier<A> {
    type Sender = flume::Sender<Delivery<A>>;
    type Receiver = flume::Receiver<Delivery<A>>;
    // type Sender = async_priority_channel::Sender<<A as Actor>::Envelope, PriorityLevel>;
    // type Receiver = async_priority_channel::Receiver<<A as Actor>::Envelope, PriorityLevel>;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    async fn send(sender: &Self::Sender, envelope: Delivery<A>) -> Result<()> {
        sender.send_async(envelope).await.expect("Failed to send");
        Ok(())
    }

    async fn recv(receiver: &Self::Receiver) -> Result<Delivery<A>> {
        Ok(receiver.recv_async().await?)
    }
}

/// A cloneable, `Send + Sync` handle to a running actor.
///
/// This is what the rest of the world holds and calls; it never sees the actor's
/// concrete state, only the commands it accepts. Each command type gets
/// two entry points — one that replies (`call*`), one that doesn't
/// (`notify` / `cast_mut`) — both funneling into the same `Envelope`
/// generic over `ReplyPort`.
pub struct Handle<A: Actor> {
    sender: <A::Carrier as Carrier<A>>::Sender,
}

pub trait HasHandle<A: Actor> {
    fn handle(&self) -> &Handle<A>;
}

impl<A: Actor> Clone for Handle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor> Handle<A> {
    async fn send_envelope<C, R, P>(&self, command: C, reply: R) -> Result<()>
    where
        P: Permission<C::Actor>,
        C: IntoEnvelope<P, Actor = A>,
        R: ReplyPort<C::Output> + 'static,
    {
        let envelope = command.into_envelope::<_>(reply);
        A::Carrier::send(&self.sender, envelope).await
    }

    /// Run a read-only `Command` and await its result.
    pub async fn call<C, P>(&self, command: C) -> C::Output
    where
        C: IntoEnvelope<P, Actor = A>,
        P: Permission<C::Actor>,
    {
        let (tx, rx) = oneshot::channel();
        self.send_envelope(command, Reply(tx))
            .await
            .expect("Send Failed");
        rx.await.expect("actor dropped")
    }

    /// Run a `Command` without waiting for (or even generating a
    /// channel for) its result. Useful for queries kept only for a side
    /// effect (logging, metrics) where the caller doesn't need the value.
    pub async fn notify<C, P>(&self, command: C) -> Result<()>
    where
        C: IntoEnvelope<P, Actor = A>,
        P: Permission<C::Actor>,
    {
        self.send_envelope(command, NoReply).await
    }
}

/// Owns the policy for *how* an `Actor` gets turned into a running task.
/// A generalized alternative implementation might add supervision /
/// restart-on-panic, metrics, tracing spans, backpressure policy, etc.,
/// all while keeping the same `spawn` signature.
pub trait Manager<A: Actor> {
    /// Spawn `actor` onto its own tokio task. Returns a cloneable
    /// `Handle` for sending it commands, and a `JoinHandle` that
    /// resolves to the actor's final state once its mailbox closes.
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> Handle<A>;
}

/// The stock `Manager`: runs the actor loop directly on the tokio
/// runtime, using whichever `Transport` is specified.
pub struct StdManager<A: Actor> {
    _p: PhantomData<A>,
}

impl<A> Manager<A> for StdManager<A>
where
    A: Actor,
{
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> Handle<A> {
        let (sender, receiver) = A::Carrier::pair(mailbox_capacity);
        let handle = Handle { sender };
        let loopback = handle.clone();
        let actor = A::new(params, loopback.clone());

        tokio::spawn(async move {
            let actor = Arc::new(RwLock::new(actor));
            while let Ok(delivery) = A::Carrier::recv(&receiver).await {
                let actor = Arc::clone(&actor);
                match delivery {
                    Delivery::Read(envelope) => {
                        let guard = Query::lock(actor).await;
                        envelope.engage(guard).await;
                    }
                    Delivery::Write(envelope) => {
                        let guard = Modify::lock(actor).await;
                        envelope.engage(guard).await;
                    }
                }
            }

            actor.write().await.on_stop();
            actor
        });

        handle
    }
}

/// Free-function helper so call sites can pick `A` and `M` explicitly
/// without needing fully-qualified trait syntax at every call site.
pub fn spawn_actor<A, M>(params: A::InitParams, mailbox_capacity: usize) -> Handle<A>
where
    A: Actor,
    M: Manager<A>,
{
    M::spawn(params, mailbox_capacity)
}

/// The stock `Manager`: runs the actor loop directly on the tokio
/// runtime, using whichever `Transport` is specified.
pub struct MultithreadManager<A: Actor>(PhantomData<A>);

impl<A> Manager<A> for MultithreadManager<A>
where
    A: Actor,
{
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> Handle<A> {
        let (sender, receiver) = A::Carrier::pair(mailbox_capacity);
        let handle = Handle { sender };
        let loopback = handle.clone();
        let actor = A::new(params, loopback.clone());

        tokio::spawn(async move {
            let actor = Arc::new(RwLock::new(actor));
            while let Ok(delivery) = A::Carrier::recv(&receiver).await {
                let actor = Arc::clone(&actor);
                match delivery {
                    Delivery::Read(envelope) => {
                        let guard = Query::lock(actor).await;
                        tokio::spawn(async move {
                            envelope.engage(guard).await;
                        });
                    }
                    Delivery::Write(envelope) => {
                        let guard = Modify::lock(actor).await;
                        tokio::spawn(async move {
                            envelope.engage(guard).await;
                        });
                    }
                }
            }

            actor.write().await.on_stop();
            actor
        });

        handle
    }
}
