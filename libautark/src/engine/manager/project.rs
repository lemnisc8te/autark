use crate::engine::manager::BoxedEnvelope;
use crate::engine::manager::Carrier;
use crate::engine::manager::Command;
use crate::engine::manager::Mutate;
use crate::model::flow::socket::Socket;
use crate::model::flow::socket::SocketID;
use anyhow::Result;
use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    engine::{
        constants::{MAX_BUFFER_SLOTS, MAX_NODES},
        manager::Actor,
        state::GraphUpdate,
        tick::Tick,
    },
    model::{
        Kind, Stored,
        flow::{Node, NodeID, nodes::trackreader::TrackReader},
        project::ProjectData,
    },
};

use std::marker::PhantomData;

pub struct ProjectActor {
    pub(crate) current: ProjectData,
    pub(crate) undo_stack: Vec<ProjectData>,
    pub(crate) redo_stack: Vec<ProjectData>,
}

pub trait ProjectCommand {}

pub struct AddTrack<K: Kind> {
    pub name: String,
    pub kind: K,
    pub channels: u16,
}

impl<K: Kind> ProjectCommand for AddTrack<K> {}

impl<K: Kind> Command<ProjectActor, Mutate> for AddTrack<K>
where
    TrackReader<K>: Node,
{
    type Output = ();

    fn execute(self, project: &mut ProjectData) -> Self::Output {
        let _ = project.add_track::<K>(self.name, self.channels);
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::Id);

impl<K: Kind> ProjectCommand for RemoveTrack<K> {}

impl<K: Kind> Command<ProjectActor, Mutate> for RemoveTrack<K> {
    type Output = Result<()>;

    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.remove_track::<K>(self.0)
    }
}

pub struct AddClip<K: Kind> {
    pub track: <K::Track as Stored>::Id,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: <K::Asset as Stored>::Id,
}

impl<K: Kind> ProjectCommand for AddClip<K> {}

impl<K: Kind> Command<ProjectActor, Mutate> for AddClip<K> {
    type Output = Result<<K::Clip as Stored>::Id>;

    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.add_clip_to_track::<K>(self.track, self.start, self.end, self.asset_id)
    }
}

pub struct MoveClip<K: Kind> {
    pub track: <K::Track as Stored>::Id,
    pub clip: <K::Clip as Stored>::Id,
    pub new_start: Tick,
}

impl<K: Kind> ProjectCommand for MoveClip<K> {}

impl<K: Kind> Command<ProjectActor, Mutate> for MoveClip<K> {
    type Output = Result<()>;
    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.move_clip::<K>(self.track, self.clip, self.new_start)
    }
}

pub struct AddNode<N: Node> {
    pub node: N,
}
impl<N: Node> ProjectCommand for AddNode<N> {}

impl<N: Node> Command<ProjectActor, Mutate> for AddNode<N> {
    type Output = NodeID;
    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.graph.add_node(self.node)
    }
}

pub struct AddLink {
    pub from: SocketID,
    pub to: SocketID,
}

impl ProjectCommand for AddLink {}

impl Command<ProjectActor, Mutate> for AddLink {
    type Output = Result<Option<SocketID>>;
    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.add_link(self.from, self.to)
    }
}

pub struct RemoveLink {
    pub from: SocketID,
    pub to: SocketID,
}

impl ProjectCommand for RemoveLink {}

impl Command<ProjectActor, Mutate> for RemoveLink {
    type Output = Result<()>;
    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.remove_link(self.from, self.to)
    }
}

pub struct AddNodeInput<K: Kind> {
    pub node_id: NodeID,
    _p: PhantomData<K>,
}

impl<K: Kind> ProjectCommand for AddNodeInput<K> {}

impl<K: Kind> AddNodeInput<K> {
    #[must_use]
    pub const fn new(node_id: NodeID) -> Self {
        Self {
            node_id,
            _p: PhantomData,
        }
    }
}

impl<K: Kind> Command<ProjectActor, Mutate> for AddNodeInput<K> {
    type Output = Result<SocketID>; // index of the newly created socket
    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.add_socket_to_node(self.node_id, Socket::new(K::into_datakind(), "in", true))
    }
}

pub struct RemoveNodeInput {
    pub node_id: NodeID,
}

impl ProjectCommand for RemoveNodeInput {}

impl Command<ProjectActor, Mutate> for RemoveNodeInput {
    type Output = Result<()>;
    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.remove_node_input(self.node_id)
    }
}

pub struct MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send,
    T: Send,
{
    pub f: F,
    pub id: <K::Track as Stored>::Id,
    _k: PhantomData<K>,
    _t: PhantomData<T>,
}

impl<K, F, T> ProjectCommand for MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send,
    T: Send,
{
}

impl<K, F, T> Command<ProjectActor, Mutate> for MutateTrack<K, F, T>
where
    K: Kind,
    F: FnOnce(&mut K::Track) -> T + Send + 'static,
    T: Send + 'static,
{
    type Output = Result<T>;

    fn execute(self, project: &mut ProjectData) -> Self::Output {
        let the_ref = K::Track::access_mut(project)
            .get_mut(self.id)
            .ok_or(anyhow!("Invalid Key: {:?}", self.id))?;
        Ok((self.f)(the_ref))
    }
}

impl ProjectActor {
    pub const fn project(&self) -> &ProjectData {
        &self.current
    }

    pub fn undo(&mut self) {
        if let Some(prev) = self.undo_stack.pop() {
            self.redo_stack
                .push(std::mem::replace(&mut self.current, prev));
            self.publish_current();
        }
    }

    pub fn redo(&mut self) {
        if let Some(next) = self.redo_stack.pop() {
            self.undo_stack
                .push(std::mem::replace(&mut self.current, next));
            self.publish_current();
        }
    }

    pub fn commit(&mut self, next: ProjectData) {
        let previous_commit = std::mem::replace(&mut self.current, next);
        self.undo_stack.push(previous_commit);
        self.redo_stack.clear();
        self.publish_current();
    }

    /// Builds the next `GraphUpdate`
    fn publish_current(&self) -> Result<GraphUpdate> {
        let schedule = self.project().compile_graph()?;

        if schedule.buffer_count > MAX_BUFFER_SLOTS || self.project().graph.nodes.len() > MAX_NODES
        {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            anyhow::bail!("graph exceeds preallocated real-time budget; edit ignored")
        }

        let old_ids: std::collections::HashSet<NodeID> = self
            .undo_stack
            .last()
            .map(|proj| proj.graph.nodes.keys().collect())
            .unwrap_or_default();
        let new_ids: std::collections::HashSet<NodeID> =
            self.project().graph.nodes.keys().collect();

        let state_additions: Vec<_> = new_ids
            .difference(&old_ids)
            .map(|&id| (id, self.project().graph.nodes[id].spawn_state()))
            .collect();
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        Ok(GraphUpdate {
            project: self.project().clone().into(),
            schedule,
            state_additions,
            state_removals,
        })
    }
}

// #[async_trait]
impl Actor for ProjectActor {
    type Data = ProjectData;
    type InitParams = ProjectData;
    type Envelope = BoxedEnvelope<Self>;
    fn pre_mutate(&mut self) {
        let next = self.current.clone();
        self.commit(next);
    }

    fn data(&self) -> &Self::Data {
        &self.current
    }

    fn data_mut(&mut self) -> &mut Self::Data {
        &mut self.current
    }

    fn new(current: Self::InitParams) -> Self {
        Self {
            current,
            undo_stack: vec![],
            redo_stack: vec![],
        }
    }
}

pub struct ProjectTaskCarrier;

#[async_trait]
impl Carrier<ProjectActor> for ProjectTaskCarrier {
    type Sender = flume::Sender<<ProjectActor as Actor>::Envelope>;
    type Receiver = flume::Receiver<<ProjectActor as Actor>::Envelope>;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    fn send(sender: &mut Self::Sender, envelope: <ProjectActor as Actor>::Envelope) -> Result<()> {
        let _ = sender.send(envelope);
        Ok(())
    }

    fn recv(receiver: &mut Self::Receiver) -> Result<<ProjectActor as Actor>::Envelope> {
        Ok(receiver.recv()?)
    }
}
