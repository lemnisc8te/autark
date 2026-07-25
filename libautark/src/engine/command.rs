use std::{marker::PhantomData, sync::Arc};

use crate::{
    engine::{tick::Tick, token::Entity},
    model::{
        Kind, Stored,
        flow::{
            Node, NodeID,
            nodes::trackreader::TrackReader,
            socket::{Socket, SocketID},
        },
        project::ProjectData,
    },
};

use tokio::sync::{Notify, oneshot};

/// The object-safe trait used by the channel
pub trait ErasedCommand: Send {
    fn execute_and_reply(self: Box<Self>, project: Arc<ProjectData>);
}

/// The envelope bundles the command, its response channel,
/// AND its dependency gates (receivers it must wait on).
struct CommandEnvelope<C: Command> {
    command: C,
    reply_tx: oneshot::Sender<Result<C::Output>>,
    // Prerequisite gates this command must await before it runs
    prerequisites: Vec<Arc<Notify>>,
    // Gates that this command is responsible for unlocking once it finishes
    post_execution_triggers: Vec<Arc<Notify>>,
}

impl<C: Command + 'static> ErasedCommand for CommandEnvelope<C> {
    fn execute_and_reply(self: Box<Self>, project: Arc<ProjectData>) {
        // Spawn a lightweight task to handle execution out-of-order safely
        tokio::task::spawn(async move {
            // 1. Automatically wait for all parent dependencies to finish
            for gate in self.prerequisites {
                gate.notified().await;
            }

            // 2. Perform the actual work
            let response = self.command.execute(project);

            // 3. Automatically unblock any downstream child commands waiting on this
            for trigger in self.post_execution_triggers {
                trigger.notify_waiters();
            }

            // 4. Return the result back to the UI caller
            let _ = self.reply_tx.send(response);
        });
    }
}

use anyhow::Result;
pub trait Command: Send + Sync {
    type Output: Clone + Send + Sync;
    /// Defines the behavior of the [`Command`]
    ///
    /// # Errors
    ///
    /// Implementation specific
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output>;
}
pub struct AddTrack<K: Kind> {
    pub name: String,
    pub kind: K,
    pub channels: u16,
}

impl<K: Kind> Command for AddTrack<K>
where
    TrackReader<K>: Node,
{
    type Output = (<K::Track as Stored>::Id, NodeID);
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.add_track::<K>(self.name, self.channels)
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::Id);

impl<K: Kind> Command for RemoveTrack<K> {
    type Output = ();
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.remove_track::<K>(self.0)
    }
}

pub struct AddClip<K: Kind> {
    pub track: Entity<<K::Track as Stored>::Id>,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: Entity<<K::Asset as Stored>::Id>,
}

impl<K: Kind> Command for AddClip<K> {
    type Output = <K::Clip as Stored>::Id;
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.add_clip_to_track::<K>(self.track.inner, self.start, self.end, self.asset_id.inner)
    }
}

pub struct MoveClip<K: Kind> {
    pub track: Entity<<K::Track as Stored>::Id>,
    pub clip: Entity<<K::Clip as Stored>::Id>,
    pub new_start: Tick,
}

impl<K: Kind> Command for MoveClip<K> {
    type Output = ();
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.move_clip::<K>(self.track.inner, self.clip.inner, self.new_start)
    }
}

pub struct AddNode<N: Node> {
    pub node: N,
}

impl<N: Node> Command for AddNode<N> {
    type Output = NodeID;
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        let mut graph = project.graph.lock();
        Ok(graph.add_node(self.node))
    }
}

pub struct AddLink {
    pub from: Entity<SocketID>,
    pub to: Entity<SocketID>,
}

impl Command for AddLink {
    type Output = Option<SocketID>;
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.add_link(self.from.inner, self.to.inner)
    }
}

pub struct RemoveLink {
    pub from: SocketID,
    pub to: SocketID,
}

impl Command for RemoveLink {
    type Output = ();
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.remove_link(self.from, self.to)
    }
}

pub struct AddNodeInput<K: Kind> {
    pub node_id: NodeID,
    _p: PhantomData<K>,
}

impl<K: Kind> AddNodeInput<K> {
    #[must_use]
    pub const fn new(node_id: NodeID) -> Self {
        Self {
            node_id,
            _p: PhantomData,
        }
    }
}

impl<K: Kind> Command for AddNodeInput<K> {
    type Output = SocketID; // index of the newly created socket
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.add_socket_to_node(self.node_id, Socket::new(K::into_datakind(), "in", true))
    }
}

pub struct RemoveNodeInput {
    pub node_id: NodeID,
}

impl Command for RemoveNodeInput {
    type Output = ();
    fn execute(self, project: Arc<ProjectData>) -> Result<Self::Output> {
        project.remove_node_input(self.node_id)
    }
}
