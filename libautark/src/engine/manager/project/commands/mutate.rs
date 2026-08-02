use anyhow::{Result, anyhow};
use std::marker::PhantomData;

use crate::{
    engine::{
        manager::{
            Command, Mutate,
            project::{ProjectActor, commands::ProjectCommand},
        },
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

impl<K: Kind> Command<Mutate> for AddTrack<K>
where
    TrackReader<K>: Node,
    K::Track: Stored<Actor = ProjectActor>,
    K::Clip: Stored<Actor = ProjectActor>,
{
    type Output = (<K::Track as Stored>::ID, NodeID);
    type Actor = ProjectActor;

    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.add_track::<K>(self.name, self.channels)
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::ID);

impl<K: Kind> ProjectCommand for RemoveTrack<K> {}

impl<K> Command<Mutate> for RemoveTrack<K>
where
    K: Kind,
    K::Track: Stored<Actor = ProjectActor>,
    K::Clip: Stored<Actor = ProjectActor>,
{
    type Output = Result<()>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        {
            let this = &mut *actor;
            let track_id = self.0;
            let track = <K as Kind>::Track::access_mut(this)
                .remove(track_id)
                .ok_or(crate::engine::errors::EngineError::TrackNotFound)?;
            let linked_id = track
                .linked_node_id()
                .expect("Track was orphaned from node");
            this.graph.purge(linked_id);
            for clip_id in track.clips().values() {
                <K as Kind>::Clip::access_mut(this).remove(*clip_id);
            }
            Ok(())
        }
    }
}

pub struct AddClip<K: Kind> {
    pub track: <K::Track as Stored>::ID,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: <K::Asset as Stored>::ID,
}

impl<K: Kind> ProjectCommand for AddClip<K> {}

impl<K> Command<Mutate> for AddClip<K>
where
    K: Kind,
    K::Track: Stored<Actor = ProjectActor>,
    K::Clip: Stored<Actor = ProjectActor>,
{
    type Output = Result<<K::Clip as Stored>::ID>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.add_clip_to_track::<K>(self.track, self.start, self.end, self.asset_id)
    }
}

pub struct MoveClip<K: Kind> {
    pub track: <K::Track as Stored>::ID,
    pub clip: <K::Clip as Stored>::ID,
    pub new_start: Tick,
}

impl<K: Kind> ProjectCommand for MoveClip<K> {}

impl<K> Command<Mutate> for MoveClip<K>
where
    K: Kind,
    K::Track: Stored<Actor = ProjectActor>,
    K::Clip: Stored<Actor = ProjectActor>,
{
    type Output = Result<()>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.move_clip::<K>(self.track, self.clip, self.new_start)
    }
}

pub struct AddNode<N: Node> {
    pub node: N,
}

impl<N: Node> ProjectCommand for AddNode<N> {}

impl<N: Node> Command<Mutate> for AddNode<N> {
    type Output = NodeID;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.graph.add_node(self.node)
    }
}

pub struct AddLink {
    pub from: OutputSocketID,
    pub to: InputSocketID,
}

impl ProjectCommand for AddLink {}

impl Command<Mutate> for AddLink {
    type Output = Result<Option<OutputSocketID>>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.add_link(self.from, self.to)
    }
}

pub struct RemoveLink {
    pub from: OutputSocketID,
    pub to: InputSocketID,
}

impl ProjectCommand for RemoveLink {}

impl Command<Mutate> for RemoveLink {
    type Output = Result<()>;
    type Actor = ProjectActor;
    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.remove_link(self.from, self.to)
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

impl<K: Kind> Command<Mutate> for AddNodeInput<K> {
    type Output = Result<InputSocketID>; // index of the newly created socket
    type Actor = ProjectActor;
    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.add_input_socket_to_node(self.node_id, Socket::new(K::into_datakind(), "in", true))
    }
}

pub struct RemoveNodeInput {
    pub node_id: NodeID,
}

impl ProjectCommand for RemoveNodeInput {}

impl Command<Mutate> for RemoveNodeInput {
    type Output = Result<()>;
    type Actor = ProjectActor;
    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.remove_node_input(self.node_id)
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

impl<K, F, T> Command<Mutate> for MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send + 'static,
    T: Send + 'static,
    K::Track: Stored<Actor = ProjectActor>,
{
    type Output = Result<T>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        let the_ref = K::Track::access_mut(actor)
            .get_mut(self.id)
            .ok_or(anyhow!("Invalid Key: {:?}", self.id))?;
        Ok((self.func)(the_ref))
    }
}
