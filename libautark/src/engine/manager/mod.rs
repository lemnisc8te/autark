pub(super) mod asset;
pub(super) mod audio;
pub(super) mod project;

use std::any::Any;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::oneshot;

pub trait Command: Send + 'static {
    type Object;
    type Output: Send + 'static;
    fn execute(self, obj: &mut Self::Object) -> impl Future<Output = Self::Output> + Send;
}

#[async_trait]
pub trait BoxedCommand: Send {
    // type Object;
    // The execution returns nothing because the output is sent down
    // a oneshot channel encapsulated inside the box.
    async fn execute_and_respond(self: Box<Self>, obj: &mut (dyn Any + Send));
}

// The generic magic wrapper that glues any AsyncCommand to its oneshot sender
struct CommandEnvelope<C: Command> {
    command: C,
    respond_tx: oneshot::Sender<C::Output>,
}

#[async_trait]
impl<C: Command> BoxedCommand for CommandEnvelope<C> {
    async fn execute_and_respond(self: Box<Self>, obj: &mut (dyn Any + Send)) {
        let obj = obj.downcast_mut::<C::Object>().unwrap();
        let output = self.command.execute(obj).await;
        // Ship the strongly-typed output back up to the specific caller
        let _ = self.respond_tx.send(output);
    }
}

pub trait ManToActTx {
    type In: Send;
    fn send(&self, input: Self::In) -> Result<()>;
}

pub type ManagerActorSender = flume::Sender<Box<dyn BoxedCommand>>;

impl ManToActTx for ManagerActorSender {
    type In = Box<dyn BoxedCommand>;

    fn send(&self, input: Self::In) -> Result<()> {
        let _ = self.send(input);
        Ok(())
    }
}

pub trait ManToActRx {
    type Out;
    fn recv(&self) -> Result<Self::Out>;
}

pub type ManagerActorReceiver = flume::Receiver<Box<dyn BoxedCommand>>;

impl ManToActRx for ManagerActorReceiver {
    type Out = Box<dyn BoxedCommand>;

    fn recv(&self) -> Result<Self::Out> {
        Ok(self.recv()?)
    }
}

#[async_trait]
pub trait Manager<C: Command>: Sized + Send + Sync + 'static
where
    C::Object: Send,
    C: Send + 'static,
{
    type MyActor: Actor<C>;
    type TxToActor: ManToActTx<In = Box<dyn BoxedCommand>>;

    fn new(tx: ManagerActorSender) -> Self;

    fn init(state: <Self::MyActor as Actor<C>>::State) -> Self {
        let (tx, rx) = flume::bounded(128);

        // 3. Construct your public Manager interface and private Actor runner
        let me = Self::new(tx);
        let actor = Self::MyActor::new(rx, state);
        // 4. Spawn the actor onto its dedicated background worker thread
        tokio::spawn(async move {
            actor.run();
        });
        me
    }

    fn tx(&self) -> Self::TxToActor;

    async fn submit(&self, cmd: C) -> Result<Handle<C::Output>> {
        let (respond_tx, rx) = oneshot::channel();

        let msg = CommandEnvelope {
            command: cmd,
            respond_tx,
        };
        let _ = self.tx().send(Box::new(msg));

        // Return the handle immediately; the thread is now processing it asynchronously
        Ok(Handle { rx })
    }
}

#[async_trait]
pub trait Actor<C: Command>: Sized + Send + 'static
where
    C::Object: Send,
    C: Send,
{
    type State;
    fn new(rx: ManagerActorReceiver, state: Self::State) -> Self;
    fn obj(&mut self) -> &mut <C as Command>::Object;
    fn rx(&self) -> ManagerActorReceiver;

    fn pre_command(&mut self) {}
    fn post_command(&mut self) {}
    // The primary background loop running on the thread
    async fn run(mut self) {
        let rx = self.rx();
        while let Ok(boxed_cmd) = rx.recv() {
            self.pre_command();
            boxed_cmd.execute_and_respond(self.obj()).await;
            self.post_command();
        }
    }
}

pub struct Handle<T> {
    rx: oneshot::Receiver<T>,
}
