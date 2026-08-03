use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio::{sync::oneshot, task::JoinHandle};

pub mod asset;
pub mod audio;
pub mod project;

#[async_trait]
pub trait Actor: HasHandle<Self> + Send + Sized + 'static {
    type InitParams;
    type Data;
    type Envelope: Envelope<Self>;
    type Carrier: Carrier<Self>;
    fn new(params: Self::InitParams, loopback: Handle<Self>) -> Self;

    /// Run once before the first command is processed.
    fn on_start(&mut self) {}

    /// Run once after the mailbox closes and no more commands will come.
    fn on_stop(&mut self) {}

    fn pre_mutate(&mut self) {}

    fn data(&self) -> &Self::Data;

    fn data_mut(&mut self) -> &mut Self::Data;
}

pub trait IntoEnvelope<P: Permission<Self::Actor>>: Command<P> {
    fn into_envelope<R>(self, reply: R) -> <Self::Actor as Actor>::Envelope
    where
        R: ReplyPort<Self::Output> + 'static;
}

pub struct Query;
pub struct Modify;
pub struct MetaQuery;
pub struct MetaMutate;

pub trait MutatePermission<A: Actor>: Permission<A> {}
pub trait RefPermission<A: Actor>: Permission<A> {}

impl<A: Actor> RefPermission<A> for Query {}
impl<A: Actor> RefPermission<A> for MetaQuery {}

impl<A: Actor> MutatePermission<A> for Modify {}
impl<A: Actor> MutatePermission<A> for MetaMutate {}

pub trait Permission<A: Actor>: Send + 'static {
    type In<'r>;
    type Type<'r>;

    /// Narrows the actor-thread's exclusive `&mut A` down to whatever
    /// this permission is allowed to see (`&A` for `Ref`/`ActorRef`,
    /// `&mut A` for `Mutate`).
    fn reborrow(actor: &mut A) -> Self::In<'_>;

    /// Runs once per envelope, before `data`/`execute`. No-op for `Ref`;
    /// `Mutate`/`ActorRef` use it to commit the pre-mutation undo entry.
    fn pre_hook(_actor: &mut A) {}

    fn data(self_ref: Self::In<'_>) -> Self::Type<'_>;
}

impl<A: Actor> Permission<A> for Query {
    type In<'r> = &'r A;
    type Type<'r> = &'r A::Data;

    fn reborrow(actor: &mut A) -> Self::In<'_> {
        &*actor
    }

    fn data(self_ref: Self::In<'_>) -> Self::Type<'_> {
        self_ref.data()
    }
}
impl<A: Actor> Permission<A> for Modify {
    type In<'r> = &'r mut A;
    type Type<'r> = &'r mut A::Data;

    fn reborrow(actor: &mut A) -> Self::In<'_> {
        actor
    }

    fn pre_hook(actor: &mut A) {
        actor.pre_mutate();
    }

    fn data(self_ref: Self::In<'_>) -> Self::Type<'_> {
        self_ref.data_mut()
    }
}

impl<A: Actor> Permission<A> for MetaQuery {
    type In<'r> = &'r A;
    type Type<'r> = &'r A;

    fn reborrow(actor: &mut A) -> Self::In<'_> {
        actor
    }

    fn data(self_ref: Self::In<'_>) -> Self::Type<'_> {
        self_ref
    }
}

impl<A: Actor> Permission<A> for MetaMutate {
    type In<'r> = &'r mut A;
    type Type<'r> = &'r mut A;

    fn reborrow(actor: &mut A) -> Self::In<'_> {
        actor
    }

    fn pre_hook(actor: &mut A) {
        actor.pre_mutate();
    }

    fn data(self_ref: Self::In<'_>) -> Self::Type<'_> {
        self_ref
    }
}

#[async_trait]
pub trait Command<P: Permission<Self::Actor>>: Send + 'static {
    type Output: Send;
    type Actor: Actor;

    async fn execute(self, actor: <P as Permission<Self::Actor>>::Type<'_>) -> Self::Output;
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
    async fn engage(self: Box<Self>, actor: &mut A);
}

pub type BoxedEnvelope<A> = Box<dyn Envelope<A>>;

#[async_trait]
impl<A: Actor> Envelope<A> for BoxedEnvelope<A> {
    async fn engage(self: Box<Self>, actor: &mut A) {
        (*self).engage(actor).await;
    }
}

struct StdEnvelope<A, P, C, R>
where
    P: Permission<C::Actor>,
    C: Command<P>,
    R: ReplyPort<C::Output>,
    A: Actor,
{
    command: C,
    reply: R,
    _actor: PhantomData<fn(P) -> A>,
}

impl<C, P> IntoEnvelope<P> for C
where
    P: Permission<C::Actor>,
    C: Command<P>,
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
impl<A: Actor, P: Permission<A>, C, R> Envelope<A> for StdEnvelope<A, P, C, R>
where
    C: Command<P, Actor = A>,
    R: ReplyPort<C::Output>,
{
    async fn engage(self: Box<Self>, actor: &mut A) {
        let Self { command, reply, .. } = *self;
        P::pre_hook(actor);
        let input = P::reborrow(actor);
        let output = command.execute(P::data(input)).await;
        reply.send(output);
    }
}

/// Abstracts over *how* envelopes travel from a `Handle` to the actor
/// task. `TokioMpsc` below is the stock implementation, but anything
/// that can move a `Box<dyn Envelope<A>>` from many producers to one
/// consumer qualifies: a priority queue, an unbounded channel, a
/// metrics-wrapped channel, etc.
pub trait Carrier<A: Actor>: Send {
    type Sender: Send + Clone + 'static;
    type Receiver: Send + 'static;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver);

    /// Send a message over the channel.
    ///
    /// # Errors
    /// - Implementation specific
    fn send(sender: &Self::Sender, envelope: A::Envelope) -> Result<()>;

    /// Awaits the next envelope, or `None` once the transport is closed.
    ///
    /// # Errors
    /// - Implementation Specific
    fn recv(receiver: &Self::Receiver) -> Result<A::Envelope>;
}

pub struct StdCarrier<A: Actor> {
    _p: PhantomData<A>,
}

#[async_trait]
impl<A: Actor> Carrier<A> for StdCarrier<A> {
    type Sender = flume::Sender<<A as Actor>::Envelope>;
    type Receiver = flume::Receiver<<A as Actor>::Envelope>;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    fn send(sender: &Self::Sender, envelope: <A as Actor>::Envelope) -> Result<()> {
        let _ = sender.send(envelope);
        Ok(())
    }

    fn recv(receiver: &Self::Receiver) -> Result<<A as Actor>::Envelope> {
        Ok(receiver.recv()?)
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
    fn send_envelope<C, R, P>(&self, command: C, reply: R) -> Result<()>
    where
        P: Permission<A>,
        C: IntoEnvelope<P, Actor = A>,
        R: ReplyPort<C::Output> + 'static,
    {
        let envelope = command.into_envelope::<_>(reply);
        A::Carrier::send(&self.sender, envelope) // no lock needed now
    }

    /// Run a read-only `Command` and await its result.
    pub async fn call<C, RP>(&self, command: C) -> C::Output
    where
        RP: RefPermission<A>,
        C: IntoEnvelope<RP, Actor = A>,
    {
        let (tx, rx) = oneshot::channel();
        self.send_envelope(command, Reply(tx)).ok();
        rx.await.expect("actor dropped")
    }

    pub fn call_blocking<C, RP>(&self, command: C) -> C::Output
    where
        RP: RefPermission<A>,
        C: IntoEnvelope<RP, Actor = A>,
    {
        let (tx, rx) = oneshot::channel();
        self.send_envelope(command, Reply(tx)).ok();
        rx.blocking_recv().expect("actor thread dropped")
    }

    /// Run a `Command` without waiting for (or even generating a
    /// channel for) its result. Useful for queries kept only for a side
    /// effect (logging, metrics) where the caller doesn't need the value.
    pub fn notify<C, RP>(&self, command: C) -> Result<()>
    where
        RP: RefPermission<A>,
        C: IntoEnvelope<RP, Actor = A>,
    {
        self.send_envelope(command, NoReply)
    }

    /// Run a `MutatingCommand` and await its result.
    pub async fn call_mut<C, MP>(&self, command: C) -> C::Output
    where
        MP: MutatePermission<A>,
        C: IntoEnvelope<MP, Actor = A>,
    {
        let (tx, rx) = oneshot::channel();
        self.send_envelope(command, Reply(tx)).ok();
        rx.await.unwrap()
    }

    /// Enqueue a `MutatingCommand` without waiting for its result
    /// ("cast" in classic actor-model terms — fire and forget).
    pub fn fire_mut<C, MP>(&self, command: C) -> Result<()>
    where
        MP: MutatePermission<A>,
        C: IntoEnvelope<MP, Actor = A>,
    {
        self.send_envelope(command, NoReply)
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
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> (Handle<A>, JoinHandle<A>);
}

/// The stock `Manager`: runs the actor loop directly on the tokio
/// runtime, using whichever `Transport` is specified.
pub struct StdManager<A: Actor>(PhantomData<A>);

impl<A> Manager<A> for StdManager<A>
where
    A: Actor,
{
    fn spawn(params: A::InitParams, mailbox_capacity: usize) -> (Handle<A>, JoinHandle<A>) {
        let (sender, receiver) = A::Carrier::pair(mailbox_capacity);
        let handle = Handle { sender };
        let loopback = handle.clone();
        let mut actor = A::new(params, loopback);

        let joiner = tokio::spawn(async move {
            // Sequential execution guarantee: exactly one envelope is
            // ever "in flight" because `handle(...)` is fully awaited
            // before the loop asks the transport for the next one.
            while let Ok(envelope) = A::Carrier::recv(&receiver) {
                Box::new(envelope).engage(&mut actor).await;
            }

            actor.on_stop();
            actor
        });

        (handle, joiner)
    }
}

/// Free-function helper so call sites can pick `A` and `M` explicitly
/// without needing fully-qualified trait syntax at every call site.
pub fn spawn_actor<A, M>(
    params: A::InitParams,
    mailbox_capacity: usize,
) -> (Handle<A>, JoinHandle<A>)
where
    A: Actor,
    M: Manager<A>,
{
    M::spawn(params, mailbox_capacity)
}
