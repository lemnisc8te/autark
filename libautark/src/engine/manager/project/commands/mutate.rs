use anyhow::{Result, anyhow};
use kameo::message::Message;
use std::marker::PhantomData;

use crate::{
    engine::{
        manager::project::{ProjectActor, commands::ProjectCommand},
        tick::Tick,
    },
    model::{
        Kind, Stored,
        arr::track::Track,
        flow::{
            Node, NodeID,
            nodes::trackreader::TrackReader,
            socket::{InputSocketID, OutputSocketID, Socket},
        },
        project::ProjectData,
    },
};

pub struct AddTrack<K: Kind> {
    pub name: String,
    pub kind: K,
    pub channels: u16,
}

impl<K: Kind> ProjectCommand for AddTrack<K> {}

impl<K: Kind> Message<AddTrack<K>> for ProjectActor
where
    TrackReader<K>: Node,
    K::Track: Stored<Data = ProjectData>,
    K::Clip: Stored<Data = ProjectData>,
{
    type Reply = (<K::Track as Stored>::ID, NodeID);

    async fn handle(
        &mut self,
        msg: AddTrack<K>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| proj.add_track::<K>(msg.name, msg.channels))
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::ID);

impl<K: Kind> ProjectCommand for RemoveTrack<K> {}

impl<K> Message<RemoveTrack<K>> for ProjectActor
where
    K: Kind,
    K::Track: Stored<Data = ProjectData>,
    K::Clip: Stored<Data = ProjectData>,
{
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: RemoveTrack<K>,
        _ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| {
            let track_id = msg.0;
            let track = <K as Kind>::Track::access_mut(proj)
                .remove(track_id)
                .ok_or(crate::engine::errors::EngineError::TrackNotFound)?;
            let linked_id = track
                .linked_node_id()
                .expect("Track was orphaned from node");
            proj.graph.purge(linked_id);
            for clip_id in track.clips().values() {
                <K as Kind>::Clip::access_mut(proj).remove(*clip_id);
            }
            Ok(())
        })
    }
}

pub struct AddClip<K: Kind> {
    pub track: <K::Track as Stored>::ID,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: <K::Asset as Stored>::ID,
}

impl<K: Kind> ProjectCommand for AddClip<K> {}

impl<K> Message<AddClip<K>> for ProjectActor
where
    K: Kind,
    K::Track: Stored<Data = ProjectData>,
    K::Clip: Stored<Data = ProjectData>,
{
    type Reply = Result<<K::Clip as Stored>::ID>;
    async fn handle(
        &mut self,
        msg: AddClip<K>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| proj.add_clip_to_track::<K>(msg.track, msg.start, msg.end, msg.asset_id))
    }
}

pub struct MoveClip<K: Kind> {
    pub track: <K::Track as Stored>::ID,
    pub clip: <K::Clip as Stored>::ID,
    pub new_start: Tick,
}

impl<K: Kind> ProjectCommand for MoveClip<K> {}

impl<K> Message<MoveClip<K>> for ProjectActor
where
    K: Kind,
    K::Track: Stored<Data = ProjectData>,
    K::Clip: Stored<Data = ProjectData>,
{
    type Reply = Result<()>;
    async fn handle(
        &mut self,
        msg: MoveClip<K>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| proj.move_clip::<K>(msg.track, msg.clip, msg.new_start))
    }
}

pub struct AddNode<N: Node> {
    pub node: N,
}

impl<N: Node> ProjectCommand for AddNode<N> {}

impl<N: Node> Message<AddNode<N>> for ProjectActor {
    type Reply = Result<NodeID>;
    async fn handle(
        &mut self,
        msg: AddNode<N>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.mutate(|proj| proj.graph.add_node(msg.node)))
    }
}

pub struct AddLink {
    pub from: OutputSocketID,
    pub to: InputSocketID,
}

impl ProjectCommand for AddLink {}

impl Message<AddLink> for ProjectActor {
    type Reply = Result<Option<OutputSocketID>>;

    async fn handle(
        &mut self,
        msg: AddLink,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| proj.add_link(msg.from, msg.to))
    }
}

pub struct RemoveLink {
    pub from: OutputSocketID,
    pub to: InputSocketID,
}

impl ProjectCommand for RemoveLink {}

impl Message<RemoveLink> for ProjectActor {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: RemoveLink,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| proj.remove_link(msg.from, msg.to))
    }
}

pub struct AddNodeInput<K: Kind> {
    pub node_id: NodeID,
    pub(crate) _p: PhantomData<K>,
}

impl<K: Kind> ProjectCommand for AddNodeInput<K> {}

impl<K: Kind> AddNodeInput<K> {
    #[must_use]
    pub const fn to(node_id: NodeID) -> Self {
        Self {
            node_id,
            _p: PhantomData,
        }
    }
}

impl<K: Kind> Message<AddNodeInput<K>> for ProjectActor {
    type Reply = Result<InputSocketID>; // index of the newly created socket

    async fn handle(
        &mut self,
        msg: AddNodeInput<K>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| {
            proj.add_input_socket_to_node(msg.node_id, Socket::new(K::into_datakind(), "in", true))
        })
    }
}

pub struct RemoveNodeInput {
    pub node_id: NodeID,
}

impl ProjectCommand for RemoveNodeInput {}

impl Message<RemoveNodeInput> for ProjectActor {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: RemoveNodeInput,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| proj.remove_node_input(msg.node_id))
    }
}

pub struct MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send,
    T: Send,
{
    pub func: F,
    pub id: <K::Track as Stored>::ID,
    pub(crate) _k: PhantomData<K>,
    pub(crate) _t: PhantomData<T>,
}

impl<K, F, T> ProjectCommand for MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send,
    T: Send,
{
}

impl<K, F, T> Message<MutateTrack<K, F, T>> for ProjectActor
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send + 'static,
    T: Send + 'static,
    K::Track: Stored<Data = ProjectData>,
{
    type Reply = Result<T>;

    async fn handle(
        &mut self,
        msg: MutateTrack<K, F, T>,
        _ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mutate(|proj| {
            let the_ref = K::Track::access_mut(proj)
                .get_mut(msg.id)
                .ok_or(anyhow!("Invalid Key: {:?}", msg.id))?;
            Ok((msg.func)(the_ref))
        })
    }
}
