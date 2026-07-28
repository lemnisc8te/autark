use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio::{sync::oneshot, task::JoinHandle};

pub mod asset;
pub mod audio;
pub mod project;

#[async_trait]
pub trait Actor: Send + 'static + Sized {
    type InitParams;
    type Data;
    type Env: Envelope<Self>;
    fn new(p: Self::InitParams) -> Self;

    /// Run once before the first command is processed.
    async fn on_start(&mut self) {}

    /// Run once after the mailbox closes and no more commands will come.
    async fn on_stop(&mut self) {}

    fn pre_mutate(&mut self) {}

    fn post_mutate(&mut self) {}

    fn data(&self) -> &Self::Data;

    fn data_mut(&mut self) -> &mut Self::Data;
}

pub trait IntoEnvelope<A: Actor, O: Send + 'static> {
    fn into_envelope<T: Transport<A>, R: ReplyPort<O>>(self, reply: R) -> A::Env;
}

/// A read-only query executed against `&A`. Cannot mutate the actor.
#[async_trait]
pub trait Command<A: Actor>: IntoEnvelope<A, Self::Output> + Sized + Send + 'static {
    type Output: Send + 'static;
    async fn execute(self, actor: &A::Data) -> Self::Output;
}

/// A command executed against `&mut A`; may mutate its state.
#[async_trait]
pub trait MutatingCommand<A: Actor>:
    IntoEnvelope<A, Self::Output> + Sized + Send + 'static
{
    type Output: Send + 'static;
    async fn execute(self, actor: &mut A::Data) -> Self::Output;
}

#[async_trait]
impl<A: Actor<Env = BoxedEnvelope<A>>, U> IntoEnvelope<A, U::Output> for U
where
    U: Command<A>,
{
    fn into_envelope<T: Transport<A>, R: ReplyPort<U::Output>>(self, reply: R) -> BoxedEnvelope<A>
    where
        A: Actor<Env = BoxedEnvelope<A>>,
    {
        Box::new(QueryEnvelope {
            command: self,
            reply,
            _actor: PhantomData,
        })
    }
}

#[async_trait]
impl<A: Actor<Env = BoxedEnvelope<A>>, U> IntoEnvelope<A, U::Output> for U
where
    U: MutatingCommand<A>,
{
    fn into_envelope<T: Transport<A>, R: ReplyPort<U::Output>>(self, reply: R) -> BoxedEnvelope<A>
    where
        A: Actor<Env = BoxedEnvelope<A>>,
    {
        Box::new(MutatingEnvelope {
            command: self,
            reply,
            _actor: PhantomData,
        })
    }
}

#[async_trait]
impl<A: Actor<Env = BoxedEnvelope<A>>, U> IntoEnvelope<A, <U as MutatingCommand<A>>::Output> for U
where
    U: MutatingCommand<A> + Command<A>,
{
    fn into_envelope<T: Transport<A>, R: ReplyPort<<U as MutatingCommand<A>>::Output>>(
        self,
        reply: R,
    ) -> BoxedEnvelope<A>
    where
        A: Actor<Env = BoxedEnvelope<A>>,
    {
        panic!("Impossible")
    }
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

#[async_trait]
pub trait Envelope<A: Actor>: Send {
    async fn handle(self, actor: &mut A);
}

pub type BoxedEnvelope<A: Actor> = Box<dyn Envelope<A>>;

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
    async fn handle(self, actor: &mut A) {
        let QueryEnvelope { command, reply, .. } = self;
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
    async fn handle(self, actor: &mut A) {
        let MutatingEnvelope { command, reply, .. } = self;
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
    type Sender: Send + 'static;
    type Receiver: Send + 'static;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver);

    fn send(sender: &mut Self::Sender, envelope: A::Env) -> Result<()>;

    /// Awaits the next envelope, or `None` once the transport is closed.
    fn recv(receiver: &mut Self::Receiver) -> Result<A::Env>;
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

impl<A: Actor, T: Transport<A>> Clone for Handle<A, T>
where
    T::Sender: Clone,
{
    fn clone(&self) -> Self {
        Handle {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor, T: Transport<A>> Handle<A, T> {
    /// Run a read-only `Command` and await its result.
    pub async fn call<C>(&mut self, command: C) -> Result<C::Output>
    where
        C: Command<A>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = command.into_envelope::<T, _>(Reply(tx));
        T::send(&mut self.sender, envelope)?;
        Ok(rx.await?)
    }

    /// Run a `Command` without waiting for (or even generating a
    /// channel for) its result. Useful for queries kept only for a side
    /// effect (logging, metrics) where the caller doesn't need the value.
    pub async fn notify<C>(&mut self, command: C) -> Result<()>
    where
        C: Command<A>,
    {
        let envelope = command.into_envelope::<T, _>(NoReply);
        T::send(&mut self.sender, envelope)
    }

    /// Run a `MutatingCommand` and await its result.
    pub async fn call_mut<C>(&mut self, command: C) -> Result<C::Output>
    where
        C: MutatingCommand<A>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = command.into_envelope::<T, _>(Reply(tx));
        T::send(&mut self.sender, envelope);
        Ok(rx.await?)
    }

    /// Enqueue a `MutatingCommand` without waiting for its result
    /// ("cast" in classic actor-model terms — fire and forget).
    pub async fn cast_mut<C>(&mut self, command: C) -> Result<()>
    where
        C: MutatingCommand<A>,
    {
        let envelope = command.into_envelope::<T, _>(NoReply);
        T::send(&mut self.sender, envelope)
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
    fn spawn(
        params: A::InitParams,
        mailbox_capacity: usize,
    ) -> (Handle<A, Self::Transport>, JoinHandle<A>);
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

    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> (Handle<A, T>, JoinHandle<A>) {
        let mut actor = A::new(params);
        let (sender, mut receiver) = T::pair(mailbox_capacity);

        let join = tokio::spawn(async move {
            actor.on_start().await;

            // Sequential execution guarantee: exactly one envelope is
            // ever "in flight" because `handle(...)` is fully awaited
            // before the loop asks the transport for the next one.
            while let Ok(envelope) = T::recv(&mut receiver) {
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
    params: A::InitParams,
    mailbox_capacity: usize,
) -> (Handle<A, M::Transport>, JoinHandle<A>)
where
    A: Actor,
    M: Manager<A>,
{
    M::spawn(params, mailbox_capacity)
}
