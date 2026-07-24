use std::{any::Any, marker::PhantomData};

use crate::{
    engine::tick::Tick,
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

pub trait ErasedCommand {
    fn execute(self, project: &mut ProjectData) -> Result<Box<dyn Any>>;
}

impl<C: Command> ErasedCommand for C {
    fn execute(self, project: &mut ProjectData) -> Result<Box<dyn Any>>
    where
        <C as Command>::Output: 'static,
    {
        let res = self.execute(project)?;
        Ok(Box::new(res))
    }
}

use anyhow::Result;
pub trait Command {
    type Output;
    /// Defines the behavior of the [`Command`]
    ///
    /// # Errors
    ///
    /// Implementation specific
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output>;
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
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.add_track::<K>(self.name, self.channels)
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::Id);

impl<K: Kind> Command for RemoveTrack<K> {
    type Output = ();
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.remove_track::<K>(self.0)
    }
}

pub struct AddClip<K: Kind> {
    pub track: <K::Track as Stored>::Id,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: <K::Asset as Stored>::Id,
}

impl<K: Kind> Command for AddClip<K> {
    type Output = <K::Clip as Stored>::Id;
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.add_clip_to_track::<K>(self.track, self.start, self.end, self.asset_id)
    }
}

pub struct MoveClip<K: Kind> {
    pub track: <K::Track as Stored>::Id,
    pub clip: <K::Clip as Stored>::Id,
    pub new_start: Tick,
}

impl<K: Kind> Command for MoveClip<K> {
    type Output = ();
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.move_clip::<K>(self.track, self.clip, self.new_start)
    }
}

pub struct AddNode<N: Node> {
    pub node: N,
}

impl<N: Node> Command for AddNode<N> {
    type Output = NodeID;
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        Ok(project.graph.add_node(self.node))
    }
}

pub struct AddLink {
    pub from: SocketID,
    pub to: SocketID,
}

impl Command for AddLink {
    type Output = Option<SocketID>;
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.add_link(self.from, self.to)
    }
}

pub struct RemoveLink {
    pub from: SocketID,
    pub to: SocketID,
}

impl Command for RemoveLink {
    type Output = ();
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
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
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.add_socket_to_node(self.node_id, Socket::new(K::into_datakind(), "in", true))
    }
}

pub struct RemoveNodeInput {
    pub node_id: NodeID,
}

impl Command for RemoveNodeInput {
    type Output = ();
    fn execute(self, project: &mut ProjectData) -> Result<Self::Output> {
        project.remove_node_input(self.node_id)
    }
}
