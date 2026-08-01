use crate::{
    engine::manager::{
        ActorRef, BoxedEnvelope, Command, Mutate, Ref, StdHandle, asset::AssetActor,
    },
    model::{
        Audio,
        arr::{clip::ResolvedAudioClip, track::Track},
        flow::{
            nodes::trackreader::TrackReaderState,
            socket::{Socket, SocketDirection, SocketID},
        },
    },
};
use anyhow::Result;
use anyhow::anyhow;

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

use std::{any::Any, cell::RefCell, collections::HashSet, marker::PhantomData};

pub struct ProjectActor {
    pub(crate) current: ProjectData,
    pub(crate) undo_stack: Vec<ProjectData>,
    pub(crate) redo_stack: Vec<ProjectData>,
    pub(crate) known_node_ids: RefCell<HashSet<NodeID>>,
}

pub trait ProjectCommand {}

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
    type Output = (<K::Track as Stored>::Id, NodeID);
    type Actor = ProjectActor;

    fn execute(self, project: &mut ProjectData) -> Self::Output {
        project.add_track::<K>(self.name, self.channels)
    }
}

pub struct RemoveTrack<K: Kind>(pub <K::Track as Stored>::Id);

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
    pub track: <K::Track as Stored>::Id,
    pub start: Tick,
    pub end: Tick,
    pub asset_id: <K::Asset as Stored>::Id,
}

impl<K: Kind> ProjectCommand for AddClip<K> {}

impl<K> Command<Mutate> for AddClip<K>
where
    K: Kind,
    K::Track: Stored<Actor = ProjectActor>,
    K::Clip: Stored<Actor = ProjectActor>,
{
    type Output = Result<<K::Clip as Stored>::Id>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.add_clip_to_track::<K>(self.track, self.start, self.end, self.asset_id)
    }
}

pub struct MoveClip<K: Kind> {
    pub track: <K::Track as Stored>::Id,
    pub clip: <K::Clip as Stored>::Id,
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
    pub from: SocketID,
    pub to: SocketID,
}

impl ProjectCommand for AddLink {}

impl Command<Mutate> for AddLink {
    type Output = Result<Option<SocketID>>;
    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.add_link(self.from, self.to)
    }
}

pub struct RemoveLink {
    pub from: SocketID,
    pub to: SocketID,
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
    pub direction: SocketDirection,
    _p: PhantomData<K>,
}

impl<K: Kind> ProjectCommand for AddNodeInput<K> {}

impl<K: Kind> AddNodeInput<K> {
    #[must_use]
    pub const fn new(node_id: NodeID, direction: SocketDirection) -> Self {
        Self {
            node_id,
            direction,
            _p: PhantomData,
        }
    }
}

impl<K: Kind> Command<Mutate> for AddNodeInput<K> {
    type Output = Result<SocketID>; // index of the newly created socket
    type Actor = ProjectActor;
    fn execute(self, actor: &mut ProjectData) -> Self::Output {
        actor.add_socket_to_node(
            self.node_id,
            Socket::new(K::into_datakind(), "in", true),
            self.direction,
        )
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

// Ref Commands

pub struct GetMasterNodeId;

impl ProjectCommand for GetMasterNodeId {}

impl Command<Ref> for GetMasterNodeId {
    type Output = NodeID;

    type Actor = ProjectActor;

    fn execute(self, actor: <Ref as super::Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.master_node_id
    }
}

pub struct InputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for InputSocketOf {}

impl Command<Ref> for InputSocketOf {
    type Output = SocketID;

    type Actor = ProjectActor;

    fn execute(self, actor: <Ref as super::Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.graph.inputs_of(self.0)[self.1]
    }
}

pub struct OutputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for OutputSocketOf {}

impl Command<Ref> for OutputSocketOf {
    type Output = SocketID;

    type Actor = ProjectActor;

    fn execute(self, actor: <Ref as super::Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.graph.outputs_of(self.0)[self.1]
    }
}

// Actor Metacommands

pub struct Publish {
    pub asset_h: StdHandle<AssetActor>,
}

impl ProjectCommand for Publish {}

impl Command<ActorRef> for Publish {
    type Output = Result<GraphUpdate>;

    type Actor = ProjectActor;

    fn execute(self, actor: &ProjectActor) -> Self::Output {
        actor.publish_current(&self.asset_h)
    }
}

impl ProjectActor {
    pub const fn project(&self) -> &ProjectData {
        &self.current
    }

    pub const fn project_mut(&mut self) -> &mut ProjectData {
        &mut self.current
    }
    pub fn undo(&mut self) {
        if let Some(prev) = self.undo_stack.pop() {
            self.redo_stack
                .push(std::mem::replace(&mut self.current, prev));
        }
    }

    pub fn redo(&mut self) {
        if let Some(next) = self.redo_stack.pop() {
            self.undo_stack
                .push(std::mem::replace(&mut self.current, next));
        }
    }

    pub fn commit(&mut self, next: ProjectData) {
        let previous_commit = std::mem::replace(&mut self.current, next);
        self.undo_stack.push(previous_commit);
        self.redo_stack.clear();
    }

    /// Builds the next `GraphUpdate`
    pub fn publish_current(&self, asset_h: &StdHandle<AssetActor>) -> Result<GraphUpdate> {
        let schedule = self.project().compile_graph()?;

        if schedule.buffer_count > MAX_BUFFER_SLOTS || self.project().graph.nodes.len() > MAX_NODES
        {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            anyhow::bail!("graph exceeds preallocated real-time budget; edit ignored")
        }

        let old_ids: HashSet<NodeID> = self.known_node_ids.borrow().clone();
        let new_ids: HashSet<NodeID> = self.project().graph.nodes.keys().collect();

        let state_additions: Vec<_> = new_ids
            .difference(&old_ids)
            .map(|&id| {
                let node = self.project().graph.nodes[id].clone();
                if let Some(n) = node.as_any().downcast_ref::<TrackReader<Audio>>() {
                    let track_id = n.id;
                    let the_clips: std::collections::BTreeMap<Tick, ResolvedAudioClip> = self
                        .project()
                        .tracks[track_id]
                        .clips
                        .iter()
                        .map(|(tick, clipid)| {
                            let the_clip = self.project().clips[*clipid];
                            let resolved = ResolvedAudioClip::from_clip(the_clip, asset_h.clone());
                            (*tick, resolved)
                        })
                        .collect();

                    (
                        id,
                        Box::new(TrackReaderState { clips: the_clips }) as Box<dyn Any + Send>,
                    )
                } else {
                    (id, self.project().graph.nodes[id].spawn_state())
                }
            })
            .collect();
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        *self.known_node_ids.borrow_mut() = new_ids;
        Ok(GraphUpdate {
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
        self.project()
    }

    fn data_mut(&mut self) -> &mut Self::Data {
        self.project_mut()
    }

    fn new(current: Self::InitParams) -> Self {
        Self {
            current,
            undo_stack: vec![],
            redo_stack: vec![],
            known_node_ids: RefCell::new(HashSet::default()),
        }
    }
}
