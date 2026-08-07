//! Types and definitions for the [`Actor`] / [`Manager`] system used by the [`Engine`](crate::engine::Engine) to help organize logic

use anyhow::Result;
use async_trait::async_trait;
use core::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, oneshot};

pub mod asset;
pub mod audio;
pub mod project;

/// A data-containing object representing some state within the [`Engine`].
pub trait Actor: Send + Sync + Sized + 'static {
    /// The type of the parameter used to initialize an instance of this [`Actor`].
    type InitParam: Send;
    /// The type of the data this [`Actor`] is in charge of handling.
    type Data;

    /// Create a new instance of this [`Actor`]
    fn new(params: Self::InitParam, loopback: ActorRef<Self>) -> Self;

    /// Run once before the first command is processed.
    /// Optional behavior.
    fn on_start(&self) {}

    /// Run once after the mailbox closes and no more commands will come.
    /// Optional behavior.
    fn on_stop(&self) {}
}

pub enum Delivery<A: Actor> {
    Read(BoxedReadEnvelope<A>),
    Write(BoxedWriteEnvelope<A>),
}

pub struct Read;
pub struct Write;

pub trait Permission<A: Actor>: IntoDelivery<A> + Sized + Send + 'static {
    type Guard: Send;

    /// Runs once per envelope, before `data`/`execute`. No-op for `Ref`;
    /// `Mutate`/`ActorRef` use it to commit the pre-mutation undo entry.
    fn pre_hook(_actor: &mut A) {}

    /// Get the associated [`Guard`] from the [`Arc`]-d and [`RwLock`]-d [`Actor`] for this [`Permission`].
    fn lock(actor: Arc<RwLock<A>>) -> impl Future<Output = Self::Guard>;
}

impl<A: Actor> Permission<A> for Read {
    type Guard = OwnedRwLockReadGuard<A>;

    fn lock(actor: Arc<RwLock<A>>) -> impl Future<Output = Self::Guard> {
        actor.read_owned()
    }
}

impl<A: Actor> Permission<A> for Write {
    type Guard = OwnedRwLockWriteGuard<A>;

    fn lock(actor: Arc<RwLock<A>>) -> impl Future<Output = Self::Guard> {
        actor.write_owned()
    }
}

pub trait IntoDelivery<A>
where
    A: Actor,
{
    fn delivery<E: Envelope<A, Self> + 'static>(env: E) -> Delivery<A>
    where
        Self: Permission<A>;
}

impl<A> IntoDelivery<A> for Read
where
    A: Actor,
{
    fn delivery<E: Envelope<A, Self> + 'static>(env: E) -> Delivery<A> {
        Delivery::Read(Box::new(env))
    }
}

impl<A> IntoDelivery<A> for Write
where
    A: Actor,
{
    fn delivery<E: Envelope<A, Self> + 'static>(env: E) -> Delivery<A> {
        Delivery::Write(Box::new(env))
    }
}

pub trait Command<P: Permission<Self::Actor>>: Sized + Send + 'static {
    type Output: Send;
    type Actor: Actor;

    fn execute(
        self,
        actor: <P as Permission<Self::Actor>>::Guard,
    ) -> impl Future<Output = Self::Output> + Send;
}

trait IntoEnvelope<P: Permission<Self::Actor>>: Command<P> {
    fn into_envelope<R>(self, reply: R) -> Delivery<Self::Actor>
    where
        R: ReplyPort<Self::Output> + 'static;
}

impl<A, P, C> IntoEnvelope<P> for C
where
    A: Actor,
    P: Permission<A> + IntoDelivery<A>,
    C: Command<P, Actor = A>,
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

/// Every command still *executes* and still *produces* an `Output` — the
/// actor's behavior never changes. What varies is what happens to that
/// output afterward. `ReplyPort` is that axis, factored out as its own
/// trait so it applies identically to `Command` and `MutatingCommand`
/// instead of being duplicated (or half-supported) on each.
trait ReplyPort<O: Send>: Send {
    fn send(self, output: O);
}

/// Deliver the output back to a caller that is waiting for it.
struct Reply<O>(oneshot::Sender<O>);

impl<O: Send> ReplyPort<O> for Reply<O> {
    fn send(self, output: O) {
        // A dropped receiver just means the caller stopped waiting.
        let _ = self.0.send(output);
    }
}

/// Discard the output. The actor still computes it honestly; nobody is
/// listening. Zero runtime cost — `send` simply drops `output`.
struct NoReply;

impl<O: Send> ReplyPort<O> for NoReply {
    fn send(self, _output: O) {}
}

#[async_trait]
/// A wrapper for a [`Command`] that features a [`tokio::oneshot`] use to respond to the caller
pub trait Envelope<A: Actor, P: Permission<A>>: Send {
    async fn engage(self: Box<Self>, handle: P::Guard);
}

type BoxedReadEnvelope<A> = Box<dyn Envelope<A, Read>>;
type BoxedWriteEnvelope<A> = Box<dyn Envelope<A, Write>>;

#[async_trait]
impl<A: Actor> Envelope<A, Read> for BoxedReadEnvelope<A> {
    async fn engage(self: Box<Self>, handle: <Read as Permission<A>>::Guard) {
        (*self).engage(handle).await;
    }
}

#[async_trait]
impl<A: Actor> Envelope<A, Write> for BoxedWriteEnvelope<A> {
    async fn engage(self: Box<Self>, handle: <Write as Permission<A>>::Guard) {
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

struct Carrier<A: Actor> {
    _p: PhantomData<A>,
}

type Sender<A> = flume::Sender<Delivery<A>>;
type Receiver<A> = flume::Receiver<Delivery<A>>;

impl<A: Actor> Carrier<A> {
    fn pair(capacity: usize) -> (Sender<A>, Receiver<A>) {
        flume::bounded(capacity)
    }

    async fn send(sender: &Sender<A>, envelope: Delivery<A>) -> Result<()> {
        sender.send_async(envelope).await.expect("Failed to send");
        Ok(())
    }

    async fn recv(receiver: &Receiver<A>) -> Result<Delivery<A>> {
        Ok(receiver.recv_async().await?)
    }
}

/// A cloneable, `Send + Sync` handle to a running actor.
pub struct ActorRef<A: Actor> {
    sender: Sender<A>,
}

/// Defines whether has an [`ActorRef`] to an [`Actor`] of type [`A`](type@A).
pub trait HasActorRef<A: Actor> {
    #[expect(missing_docs)]
    fn get_ref(&self) -> &ActorRef<A>;
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor> ActorRef<A> {
    async fn send_envelope<C, R, P>(&self, command: C, reply: R) -> Result<()>
    where
        P: Permission<C::Actor>,
        C: IntoEnvelope<P, Actor = A>,
        R: ReplyPort<C::Output> + 'static,
    {
        let envelope = command.into_envelope::<_>(reply);
        Carrier::send(&self.sender, envelope).await
    }

    /// Run a read-only `Command` and await its result.
    pub async fn call<C, P>(&self, command: C) -> C::Output
    where
        C: Command<P, Actor = A>,
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
        C: Command<P, Actor = A>,
        P: Permission<C::Actor>,
    {
        self.send_envelope(command, NoReply).await
    }
}

/// Owns the policy for *how* an [`Actor`] gets turned into a running task.
/// A generalized alternative implementation might add supervision /
/// restart-on-panic, metrics, tracing spans, backpressure policy, etc.,
/// all while keeping the same `spawn` signature.i
pub trait Manager<A: Actor> {
    /// Spawn an instance of [`A`](type@A) onto its own [`tokio::task`].
    ///
    /// Returns a cloneable [`ActorRef`] for sending it commands.
    fn spawn(params: A::InitParam, mailbox_capacity: usize) -> ActorRef<A>;
}

/// The stock `Manager` impl.
/// Only one thread is used, so each message recieved must execute before any others can.
pub struct StdManager<A: Actor> {
    _p: PhantomData<A>,
}

impl<A> Manager<A> for StdManager<A>
where
    A: Actor,
{
    fn spawn(params: A::InitParam, mailbox_capacity: usize) -> ActorRef<A> {
        let (sender, receiver) = Carrier::pair(mailbox_capacity);
        let handle = ActorRef { sender };
        let loopback = handle.clone();
        let actor = A::new(params, loopback.clone());

        tokio::spawn(async move {
            let actor = Arc::new(RwLock::new(actor));
            while let Ok(delivery) = Carrier::recv(&receiver).await {
                let actor = Arc::clone(&actor);
                match delivery {
                    Delivery::Read(envelope) => {
                        let guard = Read::lock(actor).await;
                        envelope.engage(guard).await;
                    }
                    Delivery::Write(envelope) => {
                        let guard = Write::lock(actor).await;
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
pub fn spawn_actor<A, M>(params: A::InitParam, mailbox_capacity: usize) -> ActorRef<A>
where
    A: Actor,
    M: Manager<A>,
{
    M::spawn(params, mailbox_capacity)
}

/// A multithreaded impl of [`Manager`].
///
/// Each message received spawns a new thread to handle it
pub struct MultithreadManager<A: Actor>(PhantomData<A>);

impl<A> Manager<A> for MultithreadManager<A>
where
    A: Actor,
{
    fn spawn(params: A::InitParam, mailbox_capacity: usize) -> ActorRef<A> {
        let (sender, receiver) = Carrier::pair(mailbox_capacity);
        let handle = ActorRef { sender };
        let loopback = handle.clone();
        let actor = A::new(params, loopback.clone());

        tokio::spawn(async move {
            let actor = Arc::new(RwLock::new(actor));
            while let Ok(delivery) = Carrier::recv(&receiver).await {
                let actor = Arc::clone(&actor);
                match delivery {
                    Delivery::Read(envelope) => {
                        let guard = Read::lock(actor).await;
                        tokio::spawn(async move {
                            envelope.engage(guard).await;
                        });
                    }
                    Delivery::Write(envelope) => {
                        let guard = Write::lock(actor).await;
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
