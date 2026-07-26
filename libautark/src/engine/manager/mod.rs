use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio::{sync::oneshot, task::JoinHandle};

pub mod asset;
pub mod audio;
pub mod project;

#[async_trait]
pub trait Actor: Send + 'static {
    type Data;
    /// Run once before the first command is processed.
    async fn on_start(&mut self) {}

    /// Run once after the mailbox closes and no more commands will come.
    async fn on_stop(&mut self) {}

    fn pre_mutate(&mut self) {}

    fn post_mutate(&mut self) {}

    fn data(&self) -> &Self::Data;

    fn data_mut(&mut self) -> &mut Self::Data;
}

/// A read-only query executed against `&A`. Cannot mutate the actor.
#[async_trait]
pub trait Command<A: Actor>: Send + 'static {
    type Output: Send + 'static;

    async fn execute(self, actor: &A::Data) -> Self::Output;
}

/// A command executed against `&mut A`; may mutate its state. Takes
/// `&mut self` too, so it can carry accumulated info into its own result.
#[async_trait]
pub trait MutatingCommand<A: Actor>: Send + 'static {
    type Output: Send + 'static;

    async fn execute(self, actor: &mut A::Data) -> Self::Output;
}

/// Every command still *executes* and still *produces* an `Output` — the
/// actor's behavior never changes. What varies is what happens to that
/// output afterward. `ReplyPort` is that axis, factored out as its own
/// trait so it applies identically to `Command` and `MutatingCommand`
/// instead of being duplicated (or half-supported) on each.
pub trait ReplyPort<O: Send + 'static>: Send + 'static {
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

impl<O: Send + 'static> ReplyPort<O> for NoReply {
    fn send(self, _output: O) {}
}

// ---------------------------------------------------------------------
// 4. Envelopes — the object-safe bridge between heterogeneous commands
//    (and reply strategies) and a single homogeneous channel. This is
//    the only trait here that needs dynamic dispatch.
// ---------------------------------------------------------------------

#[async_trait]
pub trait Envelope<A: Actor>: Send {
    async fn handle(self: Box<Self>, actor: &mut A);
}

struct QueryEnvelope<A: Actor, C: Command<A>, R: ReplyPort<C::Output>> {
    command: C,
    reply: R,
    _actor: PhantomData<fn() -> A>,
}

#[async_trait]
impl<A, C, R> Envelope<A> for QueryEnvelope<A, C, R>
where
    A: Actor,
    C: Command<A>,
    R: ReplyPort<C::Output>,
{
    async fn handle(self: Box<Self>, actor: &mut A) {
        let QueryEnvelope { command, reply, .. } = *self;
        let output = command.execute(actor.data()).await;
        reply.send(output);
    }
}

struct MutatingEnvelope<A: Actor, C: MutatingCommand<A>, R: ReplyPort<C::Output>> {
    command: C,
    reply: R,
    _actor: PhantomData<fn() -> A>,
}

#[async_trait]
impl<A, C, R> Envelope<A> for MutatingEnvelope<A, C, R>
where
    A: Actor,
    C: MutatingCommand<A>,
    R: ReplyPort<C::Output>,
{
    async fn handle(self: Box<Self>, actor: &mut A) {
        let MutatingEnvelope {
            mut command, reply, ..
        } = *self;
        actor.pre_mutate();
        let output = command.execute(actor.data_mut()).await;
        actor.post_mutate();
        reply.send(output);
    }
}

/// Abstracts over *how* envelopes travel from a `Handle` to the actor
/// task. `TokioMpsc` below is the stock implementation, but anything
/// that can move a `Box<dyn Envelope<A>>` from many producers to one
/// consumer qualifies: a priority queue, an unbounded channel, a
/// metrics-wrapped channel, etc.
#[async_trait]
pub trait Transport<A: Actor>: Send + 'static {
    type Sender: Send + Sync + Clone + 'static;
    type Receiver: Send + 'static;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver);

    async fn send(sender: &Self::Sender, envelope: Box<dyn Envelope<A>>) -> Result<()>;

    /// Awaits the next envelope, or `None` once the transport is closed.
    async fn recv(receiver: &mut Self::Receiver) -> Option<Box<dyn Envelope<A>>>;
}

/// A cloneable, `Send + Sync` handle to a running actor. This is what
/// the rest of the world holds and calls; it never sees the actor's
/// concrete state, only the commands it accepts. Each command type gets
/// two entry points — one that replies (`call*`), one that doesn't
/// (`notify` / `cast_mut`) — both funneling into the same `Envelope`
/// generic over `ReplyPort`.
pub struct Handle<A: Actor, T: Transport<A>> {
    sender: T::Sender,
}

impl<A: Actor, T: Transport<A>> Clone for Handle<A, T> {
    fn clone(&self) -> Self {
        Handle {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor, T: Transport<A>> Handle<A, T> {
    /// Run a read-only `Command` and await its result.
    pub async fn call<C>(&self, command: C) -> Result<C::Output>
    where
        C: Command<A>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope: Box<dyn Envelope<A>> = Box::new(QueryEnvelope {
            command,
            reply: Reply(tx),
            _actor: PhantomData,
        });
        T::send(&self.sender, envelope).await?;
        Ok(rx.await?)
    }

    /// Run a `Command` without waiting for (or even generating a
    /// channel for) its result. Useful for queries kept only for a side
    /// effect (logging, metrics) where the caller doesn't need the value.
    pub async fn notify<C>(&self, command: C) -> Result<()>
    where
        C: Command<A>,
    {
        let envelope: Box<dyn Envelope<A>> = Box::new(QueryEnvelope {
            command,
            reply: NoReply,
            _actor: PhantomData,
        });
        T::send(&self.sender, envelope).await
    }

    /// Run a `MutatingCommand` and await its result.
    pub async fn call_mut<C>(&self, command: C) -> Result<C::Output>
    where
        C: MutatingCommand<A>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope: Box<dyn Envelope<A>> = Box::new(MutatingEnvelope {
            command,
            reply: Reply(tx),
            _actor: PhantomData,
        });
        T::send(&self.sender, envelope).await?;
        Ok(rx.await?)
    }

    /// Enqueue a `MutatingCommand` without waiting for its result
    /// ("cast" in classic actor-model terms — fire and forget).
    pub async fn cast_mut<C>(&self, command: C) -> Result<()>
    where
        C: MutatingCommand<A>,
    {
        let envelope: Box<dyn Envelope<A>> = Box::new(MutatingEnvelope {
            command,
            reply: NoReply,
            _actor: PhantomData,
        });
        T::send(&self.sender, envelope).await
    }
}

/// Owns the policy for *how* an `Actor` gets turned into a running task.
/// A generalized alternative implementation might add supervision /
/// restart-on-panic, metrics, tracing spans, backpressure policy, etc.,
/// all while keeping the same `spawn` signature.
pub trait Manager<A: Actor> {
    type Transport: Transport<A>;

    /// Spawn `actor` onto its own tokio task. Returns a cloneable
    /// `Handle` for sending it commands, and a `JoinHandle` that
    /// resolves to the actor's final state once its mailbox closes.
    fn spawn(actor: A, mailbox_capacity: usize) -> (Handle<A, Self::Transport>, JoinHandle<A>);
}

/// The stock `Manager`: runs the actor loop directly on the tokio
/// runtime, using whichever `Transport` is specified.
pub struct StdManager<T>(PhantomData<T>);

impl<A, T> Manager<A> for StdManager<T>
where
    A: Actor,
    T: Transport<A>,
{
    type Transport = T;

    fn spawn(mut actor: A, mailbox_capacity: usize) -> (Handle<A, T>, JoinHandle<A>) {
        let (sender, mut receiver) = T::pair(mailbox_capacity);

        let join = tokio::spawn(async move {
            actor.on_start().await;

            // Sequential execution guarantee: exactly one envelope is
            // ever "in flight" because `handle(...)` is fully awaited
            // before the loop asks the transport for the next one.
            while let Some(envelope) = T::recv(&mut receiver).await {
                envelope.handle(&mut actor).await;
            }

            actor.on_stop().await;
            actor
        });

        (Handle { sender }, join)
    }
}

/// Free-function helper so call sites can pick `A` and `M` explicitly
/// without needing fully-qualified trait syntax at every call site.
pub fn spawn_actor<A, M>(
    actor: A,
    mailbox_capacity: usize,
) -> (Handle<A, M::Transport>, JoinHandle<A>)
where
    A: Actor,
    M: Manager<A>,
{
    M::spawn(actor, mailbox_capacity)
}
