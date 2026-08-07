//! Project-related types and definitions.

use core::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    engine::{
        ActorRef,
        asset::AssetActor,
        constants::{MAX_BUFFER_SLOTS, MAX_NODES},
        errors::EngineError,
        schedule::{CompiledSchedule, ScheduleStep, SlotIndex},
        state::GraphUpdate,
        tick::Tick,
    },
    model::{
        Audio, DataKind, Kind, Stored,
        arr::{
            clip::{AudioClip, AudioClipID, Clip, ResolvedAudioClip},
            track::{AudioTrack, AudioTrackID, Track},
        },
        flow::{
            Node, NodeID,
            graph::NodeGraph,
            nodes::trackreader::{TrackReader, TrackReaderState},
            socket::{InputSocketID, OutputSocketID, Socket, SocketMeta},
        },
    },
};
use anyhow::Result;
use futures::future::{join_all, try_join_all};
use slotmap::SlotMap;

#[derive(Debug, Clone)]
/// Project arrangement data.
///
/// See [`ProjectHistory`]
pub struct ProjectData {
    /// Audio track data storage
    pub tracks: SlotMap<AudioTrackID, AudioTrack>,
    /// Audio clip data storage
    pub clips: SlotMap<AudioClipID, AudioClip>,
    /// The `flow` `NodeGraph` for this project
    pub graph: NodeGraph,
}

impl ProjectData {
    #[must_use]
    /// Create a new `ProjectData`
    pub fn new() -> Self {
        let graph = NodeGraph::new();

        Self {
            tracks: SlotMap::with_key(),
            clips: SlotMap::with_key(),
            graph,
        }
    }

    /// Remove a link between `from` and `to`.
    ///
    pub fn remove_link(&mut self, from: OutputSocketID, to: InputSocketID) {
        self.graph.remove_link(from, to);
    }

    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn add_link(
        &mut self,
        from_id: OutputSocketID,
        to_id: InputSocketID,
    ) -> Result<Option<OutputSocketID>> {
        self.graph.add_link(from_id, to_id)
    }

    pub fn move_clip<K>(
        &mut self,
        track: <K::Track as Stored>::ID,
        clip: <K::Clip as Stored>::ID,
        new_start: Tick,
    ) -> Result<()>
    where
        K: Kind,
        K::Track: Stored<Location = Self>,
        K::Clip: Stored<Location = Self>,
    {
        let track = K::Track::access_mut(self)
            .get_mut(track)
            .ok_or(EngineError::TrackNotFound)?;
        track.clips_mut().retain(|_, &mut id| id != clip);
        track.clips_mut().insert(new_start, clip);
        if let Some(clip) = K::Clip::access_mut(self).get_mut(clip) {
            *clip.start_mut() = new_start;
        }
        Ok(())
    }

    pub fn add_clip_to_track<K>(
        &mut self,
        track: <K::Track as Stored>::ID,
        start: Tick,
        length: Tick,
        asset_id: <K::Asset as Stored>::ID,
    ) -> Result<<K::Clip as Stored>::ID>
    where
        K: Kind,
        K::Track: Stored<Location = Self>,
        K::Clip: Stored<Location = Self>,
    {
        let clip_id = K::Clip::access_mut(self).insert(K::Clip::new(start, length, asset_id));
        let track = K::Track::access_mut(self)
            .get_mut(track)
            .ok_or(EngineError::TrackNotFound)?;
        track.clips_mut().insert(start, clip_id);
        Ok(clip_id)
    }

    pub fn add_track<K: Kind>(
        &mut self,
        name: String,
        channels: u16,
    ) -> (<K::Track as Stored>::ID, NodeID)
    where
        TrackReader<K>: Node,
        K::Track: Stored<Location = Self>,
        K::Clip: Stored<Location = Self>,
    {
        let track_id = K::Track::access_mut(self).insert(K::Track::new(name));
        let reader_node = TrackReader::<K>::new(track_id, channels);
        let node_id = self.graph.add_node(reader_node);
        *K::Track::access_mut(self)[track_id].linked_node_id_mut() = Some(node_id);
        (track_id, node_id)
    }

    pub fn remove_track<K>(&mut self, track_id: <K::Track as Stored>::ID) -> Result<()>
    where
        K: Kind,
        K::Track: Stored<Location = Self>,
        K::Clip: Stored<Location = Self>,
    {
        let track = <K as Kind>::Track::access_mut(self)
            .remove(track_id)
            .ok_or(EngineError::TrackNotFound)?;
        let linked_id = track
            .linked_node_id()
            .expect("Track was orphaned from node");
        self.graph.purge(linked_id);
        for clip_id in track.clips().values() {
            <K as Kind>::Clip::access_mut(self).remove(*clip_id);
        }
        Ok(())
    }

    pub fn add_input_socket_to_node(
        &mut self,
        node_id: NodeID,
        socket: Socket,
    ) -> Result<InputSocketID> {
        let id = self.graph.input_sockets.insert(SocketMeta {
            owner: node_id,
            kind: socket.kind,
            name: socket.name,
            visible: socket.visible,
        });
        self.graph.node_input_sockets[node_id].push(id);
        Ok(id)
    }

    pub fn remove_node_input(&mut self, _node_id: NodeID) -> Result<()> {
        todo!()
    }

    pub fn socket_kind_of(&mut self, endpoint: InputSocketID) -> Result<DataKind> {
        self.graph
            .input_sockets
            .get(endpoint)
            .map(|s| s.kind)
            .ok_or(EngineError::SocketNotFound(endpoint).into())
    }

    /// Compile the graph.
    ///
    /// # Errors
    ///
    /// This function will return an error if
    /// 1. Topological sort fails
    /// 2. The `capture_id` is invalid
    pub fn compile_graph(
        &self,
        filter: Option<&[NodeID]>,
        capture_id: NodeID,
    ) -> Result<CompiledSchedule> {
        let order = self.graph.topo_sort(filter)?;

        let mut socket_slot: HashMap<OutputSocketID, SlotIndex> = HashMap::new();
        let mut buffer_count = 1usize; // slot 0 reserved for silence

        for &node_id in &order {
            for &out_id in self.graph.outputs_of(node_id) {
                socket_slot.insert(out_id, buffer_count);
                buffer_count += 1;
            }
        }

        let mut steps = Vec::with_capacity(order.len());
        for &node_id in &order {
            let input_ids = self.graph.inputs_of(node_id);
            let output_ids = self.graph.outputs_of(node_id);

            let input_slots: Vec<SlotIndex> = input_ids
                .iter()
                .map(|&in_id| {
                    self.graph
                        .links
                        .get(in_id)
                        .and_then(|src| socket_slot.get(src))
                        .copied()
                        .unwrap_or(0)
                })
                .collect();

            let output_slots: Vec<SlotIndex> = output_ids
                .iter()
                .map(|&out_id| socket_slot[&out_id])
                .collect();

            let node = self.graph.nodes[node_id].clone();

            steps.push(ScheduleStep {
                node,
                node_id,
                input_slots,
                output_slots,
            });
        }

        let capture_slot = self
            .graph
            .node_output_sockets
            .get(capture_id)
            .and_then(|outs| outs.first())
            .and_then(|&id| socket_slot.get(&id))
            .copied()
            .ok_or(EngineError::NodeNotFound(capture_id))?;

        Ok(CompiledSchedule {
            steps,
            buffer_count,
            capture_slot,
        })
    }
}

impl Default for ProjectData {
    fn default() -> Self {
        Self::new()
    }
}

/// A container for meta-operations on [`ProjectData`].
///
/// Because all mutations on [`ProjectData`] must be tracked by the undo/redo system, the only way to access the current project is via [`Self::project`]/ [`Self::project_mut`]
///
/// [`ProjectHistory`] also compiles the [`GraphUpdate`] passed to the audio thread.
pub struct ProjectHistory {
    current: ProjectData,
    undo_stack: Vec<ProjectData>,
    redo_stack: Vec<ProjectData>,
    /// Currently, this field keeps track of the `[NodeID]`s that the last update contained.
    known_node_ids: HashSet<NodeID>,
}

impl ProjectHistory {
    #[must_use]
    /// Create a new `ProjectHistory`
    pub fn new(current: ProjectData) -> Self {
        Self {
            current,
            undo_stack: vec![],
            redo_stack: vec![],
            known_node_ids: HashSet::default(),
        }
    }

    #[must_use]
    /// Get a shared reference to the current `ProjectData`
    pub const fn project(&self) -> &ProjectData {
        &self.current
    }

    /// Get an exclusive mutable reference to the current `ProjectData`
    pub const fn project_mut(&mut self) -> &mut ProjectData {
        &mut self.current
    }

    /// Revert the `current` `ProjectData` to the top of the `undo_stack`
    pub fn undo(&mut self) {
        if let Some(prev) = self.undo_stack.pop() {
            self.redo_stack
                .push(core::mem::replace(&mut self.current, prev));
        }
    }

    /// Revert the `current` `ProjectData` to the top of the `redo_stack`
    pub fn redo(&mut self) {
        if let Some(next) = self.redo_stack.pop() {
            self.undo_stack
                .push(core::mem::replace(&mut self.current, next));
        }
    }

    /// Add a new entry to the `undo_stack` and clear the `redo_stack`
    pub fn commit(&mut self, next: ProjectData) {
        let previous_commit = core::mem::replace(&mut self.current, next);
        self.undo_stack.push(previous_commit);
        self.redo_stack.clear();
    }

    /// Builds the next `GraphUpdate`
    ///
    /// # Errors
    /// This function will return an error if
    /// 1. Topological sort fails
    /// 2. The `master_node_id` is invalid
    /// 3. The graph is too large for the real-time budget
    pub async fn publish_current(
        &mut self,
        asset_h: &ActorRef<AssetActor>,
        filter: Option<&[NodeID]>,
    ) -> Result<GraphUpdate> {
        let schedule = self
            .project()
            .compile_graph(filter, self.current.graph.master_node_id)?;

        if schedule.buffer_count > MAX_BUFFER_SLOTS || self.project().graph.nodes.len() > MAX_NODES
        {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            anyhow::bail!("graph exceeds preallocated real-time budget; edit ignored")
        }

        let old_ids: HashSet<NodeID> = self.known_node_ids.clone();
        let new_ids: HashSet<NodeID> = self.project().graph.nodes.keys().collect();

        let state_additions = new_ids
            .difference(&old_ids)
            .map(|&id| self.create_node_state(asset_h, id));
        let state_additions = join_all(state_additions).await;
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        let _ = core::mem::replace(&mut self.known_node_ids, new_ids);
        Ok(GraphUpdate {
            schedule,
            state_additions,
            state_removals,
        })
    }

    async fn create_node_state(
        &self,
        asset_h: &ActorRef<AssetActor>,
        node_id: NodeID,
    ) -> (NodeID, Box<dyn Any + Send>) {
        let node = self.project().graph.nodes[node_id].clone();
        if let Some(n) = node.as_any().downcast_ref::<TrackReader<Audio>>() {
            let track_id = n.id;
            let (ticks, clips): (Vec<Tick>, Vec<_>) = self.project().tracks[track_id]
                .clips
                .iter()
                .map(|(tick, clipid)| {
                    let the_clip = self.project().clips[*clipid];
                    let resolved = ResolvedAudioClip::from_clip(the_clip, asset_h.clone());
                    (tick, resolved)
                })
                .unzip();
            let clips = try_join_all(clips).await.unwrap();
            let the_clips: BTreeMap<Tick, ResolvedAudioClip> =
                ticks.into_iter().zip(clips).collect();

            (
                node_id,
                Box::new(TrackReaderState { clips: the_clips }) as Box<dyn Any + Send>,
            )
        } else {
            (node_id, self.project().graph.nodes[node_id].spawn_state())
        }
    }

    // pub fn bounce_range(
    //     &self,
    //     asset_h: &Handle<AssetActor>,
    //     target: NodeID,
    //     range: Range<Tick>,
    //     block_size: usize,
    // ) -> Result<Vec<f32>> {
    //     use crate::engine::{
    //         manager::audio::AudioActor,
    //         state::{Garbage, NodeStatePool},
    //         util::abp::AudioBufferPool,
    //     };
    //     let ancestors: Vec<NodeID> = self
    //         .project()
    //         .graph
    //         .ancestors_of(target)
    //         .into_iter()
    //         .collect();
    //     let schedule = self.project().compile_graph(Some(&ancestors), target)?;

    //     let mut pool = AudioBufferPool::new(schedule.buffer_count, block_size);
    //     let mut state_pool = NodeStatePool::new();
    //     let (mut garbage_tx, _rx) = rtrb::RingBuffer::<Garbage>::new(1); // discarded, offline

    //     let state_addititions = ancestors
    //         .iter()
    //         .map(|&id| self.create_node_state(asset_h, id))
    //         .collect();
    //     let mut update = GraphUpdate {
    //         state_additions: ancestors
    //             .iter()
    //             .map(|&id| self.create_node_state(asset_h, id))
    //             .collect(),
    //         schedule,
    //         state_removals: vec![],
    //     };
    //     state_pool.apply(&mut update, &mut garbage_tx);

    //     let frames = usize::try_from((range.end - range.start).0)?;
    //     let mut out = Vec::with_capacity(frames);
    //     let mut cursor = range.start;
    //     while (cursor - range.start).0 < frames as u64 {
    //         let mixed =
    //             AudioActor::execute_block(&update.schedule, cursor, &mut pool, &mut state_pool);
    //         out.extend_from_slice(mixed);
    //         cursor = cursor + Tick(block_size as u64);
    //     }
    //     out.truncate(frames);
    //     Ok(out)
    // }
}
