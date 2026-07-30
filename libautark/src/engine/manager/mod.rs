use anyhow::Result;
use async_trait::async_trait;
use std::{marker::PhantomData, thread};
use tokio::{sync::oneshot, task::JoinHandle};

pub mod asset;
pub mod audio;
pub mod project;

#[async_trait]
pub trait Actor: Send + Sized + 'static {
    type InitParams;
    type Data;
    type Envelope: Envelope<Self>;
    fn new(params: Self::InitParams) -> Self;

    /// Run once before the first command is processed.
    fn on_start(&mut self) {}

    /// Run once after the mailbox closes and no more commands will come.
    fn on_stop(&mut self) {}

    fn pre_mutate(&mut self) {}

    fn post_mutate(&mut self) {}

    fn data(&self) -> &Self::Data;

    fn data_mut(&mut self) -> &mut Self::Data;
}

pub trait IntoEnvelope<A: Actor, P: Permission<A>>: Command<A, P> {
    fn into_envelope<T, R>(self, reply: R) -> A::Envelope
    where
        T: Carrier<A>,
        R: ReplyPort<Self::Output>;
}

pub struct Ref;
pub struct Mutate;

pub trait Permission<A: Actor> {
    type In<'a>;
    type Type<'a>;

    fn data<'a>(self, self_ref: Self::In<'a>) -> Self::Type<'a>;
}

impl<A: Actor> Permission<A> for Ref {
    type In<'a> = &'a A;
    type Type<'a> = &'a A::Data;

    fn data<'a>(self, self_ref: Self::In<'a>) -> Self::Type<'a> {
        self_ref.data()
    }
}
impl<A: Actor> Permission<A> for Mutate {
    type In<'a> = &'a mut A;
    type Type<'a> = &'a mut A::Data;

    fn data<'a>(self, self_ref: Self::In<'a>) -> Self::Type<'a> {
        self_ref.data_mut()
    }
}

// #[async_trait]
pub trait Command<A: Actor, P: Permission<A>>: Sized + Send + 'static {
    type Output: Send;

    fn execute(self, actor: P::Type<'_>) -> Self::Output;
}

/// Every command still *executes* and still *produces* an `Output` — the
/// actor's behavior never changes. What varies is what happens to that
/// output afterward. `ReplyPort` is that axis, factored out as its own
/// trait so it applies identically to `Command` and `MutatingCommand`
/// instead of being duplicated (or half-supported) on each.
pub trait ReplyPort<O: Send>: Send + 'static {
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

// #[async_trait]
pub trait Envelope<A: Actor>: Send {
    fn handle(self: Box<Self>, actor: &mut A);
}

pub type BoxedEnvelope<A: Actor> = Box<dyn Envelope<A>>;

impl<A: Actor> Envelope<A> for BoxedEnvelope<A> {
    fn handle(self: Box<Self>, actor: &mut A) {
        (*self).handle(actor);
    }
}

struct StdEnvelope<A: Actor, P: Permission<A>, C: Command<A, P>, R: ReplyPort<C::Output>> {
    command: C,
    reply: R,
    _actor: PhantomData<fn(P) -> A>,
    // _perm: PhantomData<P>,
}

impl<A, C> IntoEnvelope<A, Ref> for C
where
    C: Command<A, Ref>,
    A: Actor<Envelope = BoxedEnvelope<A>>,
{
    fn into_envelope<T, R>(self, reply: R) -> A::Envelope
    where
        A: Actor,
        T: Carrier<A>,
        R: ReplyPort<Self::Output>,
    {
        Box::new(StdEnvelope {
            command: self,
            reply,
            _actor: PhantomData,
            // _perm: PhantomData,
        })
    }
}

impl<A, C> IntoEnvelope<A, Mutate> for C
where
    C: Command<A, Mutate>,
    A: Actor<Envelope = BoxedEnvelope<A>>,
{
    fn into_envelope<T, R>(self, reply: R) -> A::Envelope
    where
        A: Actor,
        T: Carrier<A>,
        R: ReplyPort<Self::Output>,
    {
        Box::new(StdEnvelope {
            command: self,
            reply,
            _actor: PhantomData,
            // _perm: PhantomData,
        })
    }
}

impl<A: Actor, C, R> Envelope<A> for StdEnvelope<A, Ref, C, R>
where
    C: Command<A, Ref>,
    R: ReplyPort<C::Output>,
{
    fn handle(self: Box<Self>, actor: &mut A) {
        let Self { command, reply, .. } = *self;
        let output = command.execute(actor.data());
        reply.send(output);
    }
}

impl<A: Actor, C, R> Envelope<A> for StdEnvelope<A, Mutate, C, R>
where
    C: Command<A, Mutate>,
    R: ReplyPort<C::Output>,
{
    fn handle(self: Box<Self>, actor: &mut A) {
        let Self { command, reply, .. } = *self;
        actor.pre_mutate();
        let output = command.execute(actor.data_mut());
        actor.post_mutate();
        reply.send(output);
    }
}

// impl<A: Actor, P, C, R> Envelope<A> for StdEnvelope<A, P, C, R>
// where
//     P: Permission<A>,
//     C: Command<A, P>,
//     R: ReplyPort<C::Output>,
// {
//     fn handle(self: Box<Self>, actor: &mut A) {
//         panic!("Impossible")
//     }
// }

/// Abstracts over *how* envelopes travel from a `Handle` to the actor
/// task. `TokioMpsc` below is the stock implementation, but anything
/// that can move a `Box<dyn Envelope<A>>` from many producers to one
/// consumer qualifies: a priority queue, an unbounded channel, a
/// metrics-wrapped channel, etc.
pub trait Carrier<A: Actor>: Send + 'static {
    type Sender: Send + 'static;
    type Receiver: Send + 'static;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver);

    /// Send a message over the channel.
    ///
    /// # Errors
    /// - Implementation specific
    fn send(sender: &mut Self::Sender, envelope: A::Envelope) -> Result<()>;

    /// Awaits the next envelope, or `None` once the transport is closed.
    ///
    /// # Errors
    /// - Implementation Specific
    fn recv(receiver: &mut Self::Receiver) -> Result<A::Envelope>;
}

/// A cloneable, `Send + Sync` handle to a running actor.
///
/// This is what
/// the rest of the world holds and calls; it never sees the actor's
/// concrete state, only the commands it accepts. Each command type gets
/// two entry points — one that replies (`call*`), one that doesn't
/// (`notify` / `cast_mut`) — both funneling into the same `Envelope`
/// generic over `ReplyPort`.
pub struct Handle<A: Actor, T: Carrier<A>> {
    sender: T::Sender,
}

impl<A: Actor, T: Carrier<A>> Clone for Handle<A, T>
where
    T::Sender: Clone,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor, T: Carrier<A>> Handle<A, T> {
    /// Run a read-only `Command` and await its result.
    pub async fn call<C>(&mut self, command: C) -> Result<C::Output>
    where
        C: IntoEnvelope<A, Ref>,
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
        C: IntoEnvelope<A, Ref>,
    {
        let envelope = command.into_envelope::<T, _>(NoReply);
        T::send(&mut self.sender, envelope)
    }

    /// Run a `MutatingCommand` and await its result.
    pub async fn call_mut<C>(&mut self, command: C) -> Result<C::Output>
    where
        C: IntoEnvelope<A, Mutate>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = command.into_envelope::<T, _>(Reply(tx));
        T::send(&mut self.sender, envelope);
        Ok(rx.await?)
    }

    /// Enqueue a `MutatingCommand` without waiting for its result
    /// ("cast" in classic actor-model terms — fire and forget).
    pub async fn fire_mut<C>(&mut self, command: C) -> Result<()>
    where
        C: IntoEnvelope<A, Mutate>,
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
    type Carrier: Carrier<A>;
    /// Spawn `actor` onto its own tokio task. Returns a cloneable
    /// `Handle` for sending it commands, and a `JoinHandle` that
    /// resolves to the actor's final state once its mailbox closes.
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> Handle<A, Self::Carrier>;
}

/// The stock `Manager`: runs the actor loop directly on the tokio
/// runtime, using whichever `Transport` is specified.
pub struct StdManager<T>(PhantomData<T>);

impl<A, T> Manager<A> for StdManager<T>
where
    A: Actor,
    T: Carrier<A>,
{
    type Carrier = T;

    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> Handle<A, T> {
        let mut actor = A::new(params);
        let (sender, mut receiver) = T::pair(mailbox_capacity);

        thread::spawn(move || {
            actor.on_start();

            // Sequential execution guarantee: exactly one envelope is
            // ever "in flight" because `handle(...)` is fully awaited
            // before the loop asks the transport for the next one.
            while let Ok(envelope) = T::recv(&mut receiver) {
                Box::new(envelope).handle(&mut actor);
            }

            actor.on_stop();
            actor
        });

        Handle { sender }
    }
}

/// Free-function helper so call sites can pick `A` and `M` explicitly
/// without needing fully-qualified trait syntax at every call site.
pub fn spawn_actor<A, M>(params: A::InitParams, mailbox_capacity: usize) -> Handle<A, M::Carrier>
where
    A: Actor,
    M: Manager<A>,
{
    M::spawn(params, mailbox_capacity)
}
