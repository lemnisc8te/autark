use anyhow::{Result, anyhow};
use core::marker::PhantomData;

use crate::{
    engine::{
        manager::{
            Command, Modify, Permission,
            project::{ProjectActor, commands::ProjectCommand},
        },
        tick::Tick,
    },
    model::{
        Kind, Stored,
        flow::{
            Node, NodeID,
            nodes::trackreader::TrackReader,
            socket::{InputSocketID, OutputSocketID, Socket},
        },
        project::{ProjectData, ProjectHistory},
    },
};

pub struct AddTrack<K: Kind> {
    pub name: String,
    pub kind: K,
    pub channels: u16,
}

impl<K: Kind> ProjectCommand for AddTrack<K> {}

impl<K: Kind> Command<Modify> for AddTrack<K>
where
    TrackReader<K>: Node,
    K::Track: Stored<Location = ProjectData>,
    K::Clip: Stored<Location = ProjectData>,
{
    type Output = (<K::Track as Stored>::ID, NodeID);
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| proj.project_mut().add_track::<K>(self.name, self.channels))
            .await
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::ID);

impl<K: Kind> ProjectCommand for RemoveTrack<K> {}

impl<K> Command<Modify> for RemoveTrack<K>
where
    K: Kind,
    K::Track: Stored<Location = ProjectData>,
    K::Clip: Stored<Location = ProjectData>,
{
    type Output = Result<()>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        {
            actor
                .mutate(async |proj| proj.project_mut().remove_track::<K>(self.0))
                .await
        }
    }
}

pub struct AddClip<K: Kind> {
    pub track_id: <K::Track as Stored>::ID,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: <K::Asset as Stored>::ID,
}

impl<K: Kind> ProjectCommand for AddClip<K> {}

impl<K> Command<Modify> for AddClip<K>
where
    K: Kind,
    K::Track: Stored<Location = ProjectData>,
    K::Clip: Stored<Location = ProjectData>,
{
    type Output = Result<<K::Clip as Stored>::ID>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| {
                proj.project_mut().add_clip_to_track::<K>(
                    self.track_id,
                    self.start,
                    self.end,
                    self.asset_id,
                )
            })
            .await
    }
}

pub struct MoveClip<K: Kind> {
    pub track: <K::Track as Stored>::ID,
    pub clip: <K::Clip as Stored>::ID,
    pub new_start: Tick,
}

impl<K: Kind> ProjectCommand for MoveClip<K> {}

impl<K> Command<Modify> for MoveClip<K>
where
    K: Kind,
    K::Track: Stored<Location = ProjectData>,
    K::Clip: Stored<Location = ProjectData>,
{
    type Output = Result<()>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| {
                proj.project_mut()
                    .move_clip::<K>(self.track, self.clip, self.new_start)
            })
            .await
    }
}

pub struct AddNode<N: Node> {
    pub node: N,
}

impl<N: Node> ProjectCommand for AddNode<N> {}

impl<N: Node> Command<Modify> for AddNode<N> {
    type Output = NodeID;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| proj.project_mut().graph.add_node(self.node))
            .await
    }
}

pub struct AddLink {
    pub from: OutputSocketID,
    pub to: InputSocketID,
}

impl ProjectCommand for AddLink {}

impl Command<Modify> for AddLink {
    type Output = Result<Option<OutputSocketID>>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| proj.project_mut().add_link(self.from, self.to))
            .await
    }
}

pub struct RemoveLink {
    pub from: OutputSocketID,
    pub to: InputSocketID,
}

impl ProjectCommand for RemoveLink {}

impl Command<Modify> for RemoveLink {
    type Output = ();
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| proj.project_mut().remove_link(self.from, self.to))
            .await;
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
impl<K: Kind> Command<Modify> for AddNodeInput<K> {
    type Output = Result<InputSocketID>; // index of the newly created socket
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| {
                proj.project_mut().add_input_socket_to_node(
                    self.node_id,
                    Socket::new(K::into_datakind(), "in", true),
                )
            })
            .await
    }
}

pub struct RemoveNodeInput {
    pub node_id: NodeID,
}

impl ProjectCommand for RemoveNodeInput {}

impl Command<Modify> for RemoveNodeInput {
    type Output = Result<()>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| proj.project_mut().remove_node_input(self.node_id))
            .await
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

impl<K, F, T> Command<Modify> for MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send + 'static,
    T: Send + 'static,
    K::Track: Stored<Location = ProjectHistory>,
{
    type Output = Result<T>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .mutate(async |proj| {
                let the_ref = K::Track::access_mut(proj)
                    .get_mut(self.id)
                    .ok_or(anyhow!("Invalid Key: {:?}", self.id))?;
                Ok((self.func)(the_ref))
            })
            .await
    }
}
