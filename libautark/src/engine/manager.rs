use anyhow::Result;
use async_trait::async_trait;
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::oneshot;

pub mod asset;
pub mod audio;
pub mod project;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriorityLevel {
    Low = 0,      // Regular system traffic
    Standard = 1, // Normal WaitForAudioAsset commands
    High = 2,     // IO Completion notifications & Lifecycle events
}

// #[async_trait]
pub trait Actor: Send + Sync + Sized + 'static {
    type InitParams: Send;
    type Envelope: Envelope<Self>;
    type Carrier: Carrier<Self>;
    fn new(params: Self::InitParams, loopback: Handle<Self>) -> Self;

    /// Run once before the first command is processed.
    fn on_start(&self) {}

    /// Run once after the mailbox closes and no more commands will come.
    fn on_stop(&self) {}
}

pub trait Operate: Actor {
    type Data: Send;
    async fn mutate<O>(&self, f: impl AsyncFnOnce(&mut Self::Data) -> O) -> O;
    async fn query<O>(&self, f: impl AsyncFn(&Self::Data) -> O) -> O;
}

pub trait IntoEnvelope: Command {
    fn into_envelope<R>(self, reply: R) -> <Self::Actor as Actor>::Envelope
    where
        R: ReplyPort<Self::Output> + 'static;
}

pub trait Command: Send + 'static {
    type Output: Send + 'static;
    type Actor: Actor + Operate;

    fn execute(self, actor: &Self::Actor) -> impl Future<Output = Self::Output> + Send;
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

impl<O: Send + 'static> ReplyPort<O> for Reply<O> {
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
pub trait Envelope<A: Actor>: Send {
    async fn engage(self: Box<Self>, handle: Arc<A>);
}

pub type BoxedEnvelope<A> = Box<dyn Envelope<A>>;

#[async_trait]
impl<A: Actor> Envelope<A> for BoxedEnvelope<A> {
    async fn engage(self: Box<Self>, handle: Arc<A>) {
        (*self).engage(handle).await;
    }
}

struct StdEnvelope<A, C, R>
where
    C: Command,
    R: ReplyPort<C::Output>,
    A: Actor,
{
    command: C,
    reply: R,
    _actor: PhantomData<fn() -> A>,
}

impl<C> IntoEnvelope for C
where
    C: Command,
    C::Actor: Actor<Envelope = BoxedEnvelope<C::Actor>>,
{
    fn into_envelope<R>(self, reply: R) -> <C::Actor as Actor>::Envelope
    where
        R: ReplyPort<Self::Output> + 'static,
    {
        Box::new(StdEnvelope {
            command: self,
            reply,
            _actor: PhantomData,
        })
    }
}

#[async_trait]
impl<A: Actor, C, R> Envelope<A> for StdEnvelope<A, C, R>
where
    C: Command<Actor = A>,
    R: ReplyPort<C::Output>,
{
    async fn engage(self: Box<Self>, actor: Arc<A>) {
        let Self { command, reply, .. } = *self;
        let output = command.execute(&actor).await;
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
    async fn send(sender: &Self::Sender, envelope: A::Envelope) -> Result<()>;

    /// Awaits the next envelope, or `None` once the transport is closed.
    ///
    /// # Errors
    /// - Implementation Specific
    async fn recv(receiver: &Self::Receiver) -> Result<A::Envelope>;
}

pub struct StdCarrier<A: Actor> {
    _p: PhantomData<A>,
}

#[async_trait]
impl<A: Actor> Carrier<A> for StdCarrier<A> {
    type Sender = flume::Sender<<A as Actor>::Envelope>;
    type Receiver = flume::Receiver<<A as Actor>::Envelope>;
    // type Sender = async_priority_channel::Sender<<A as Actor>::Envelope, PriorityLevel>;
    // type Receiver = async_priority_channel::Receiver<<A as Actor>::Envelope, PriorityLevel>;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    async fn send(sender: &Self::Sender, envelope: <A as Actor>::Envelope) -> Result<()> {
        sender.send_async(envelope).await.expect("Failed to send");
        Ok(())
    }

    async fn recv(receiver: &Self::Receiver) -> Result<<A as Actor>::Envelope> {
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
    async fn send_envelope<C, R>(&self, command: C, reply: R) -> Result<()>
    where
        C: IntoEnvelope<Actor = A>,
        R: ReplyPort<C::Output> + 'static,
    {
        let envelope = command.into_envelope::<_>(reply);
        A::Carrier::send(&self.sender, envelope).await
    }

    /// Run a read-only `Command` and await its result.
    pub async fn call<C>(&self, command: C) -> C::Output
    where
        C: IntoEnvelope<Actor = A>,
    {
        let (tx, rx) = oneshot::channel();
        self.send_envelope(command, Reply(tx)).await.ok();
        rx.await.expect("actor dropped")
    }

    /// Run a `Command` without waiting for (or even generating a
    /// channel for) its result. Useful for queries kept only for a side
    /// effect (logging, metrics) where the caller doesn't need the value.
    pub async fn notify<C>(&self, command: C) -> Result<()>
    where
        C: IntoEnvelope<Actor = A>,
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
pub struct StdManager<A: Actor>(PhantomData<A>);

impl<A> Manager<A> for StdManager<A>
where
    A: Actor,
{
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> Handle<A> {
        let (sender, receiver) = A::Carrier::pair(mailbox_capacity);
        let handle = Handle { sender };
        let loopback = handle.clone();
        let actor = A::new(params, loopback.clone());

        let joiner = tokio::spawn(async move {
            let actor = Arc::new(actor);
            while let Ok(envelope) = A::Carrier::recv(&receiver).await {
                Box::new(envelope).engage(actor.clone()).await;
            }

            actor.on_stop();
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

        let joiner = tokio::spawn(async move {
            let actor = Arc::new(A::new(params, loopback.clone()));

            while let Ok(envelope) = A::Carrier::recv(&receiver).await {
                let actor_clone = Arc::clone(&actor);

                // Spawn a task to handle the individual message safely
                tokio::spawn(async move {
                    Box::new(envelope).engage(actor_clone).await;
                });
            }

            actor.on_stop();
        });
        drop(joiner);

        handle
    }
}
