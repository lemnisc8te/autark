use crate::{
    engine::manager::{BoxedEnvelope, Handle, HasHandle, StdCarrier, asset::AssetActor},
    model::{Audio, arr::clip::ResolvedAudioClip, flow::nodes::trackreader::TrackReaderState},
};
use anyhow::Result;
use futures::future::join_all;

use crate::{
    engine::{
        constants::{MAX_BUFFER_SLOTS, MAX_NODES},
        manager::Actor,
        state::GraphUpdate,
        tick::Tick,
    },
    model::{
        flow::{NodeID, nodes::trackreader::TrackReader},
        project::ProjectData,
    },
};

use std::{
    any::Any,
    collections::{BTreeMap, HashSet},
};

pub struct ProjectActor {
    pub(crate) current: ProjectData,
    pub(crate) undo_stack: Vec<ProjectData>,
    pub(crate) redo_stack: Vec<ProjectData>,
    pub(crate) known_node_ids: HashSet<NodeID>,
    loopback: Handle<Self>,
}

pub mod commands;

impl ProjectActor {
    #[must_use]
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
    pub async fn publish_current(
        &mut self,
        asset_h: &Handle<AssetActor>,
        filter: Option<&[NodeID]>,
    ) -> Result<GraphUpdate> {
        let schedule = self
            .project()
            .compile_graph(filter, self.current.master_node_id)?;

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

        let _ = std::mem::replace(&mut self.known_node_ids, new_ids);
        Ok(GraphUpdate {
            schedule,
            state_additions,
            state_removals,
        })
    }

    async fn create_node_state(
        &self,
        asset_h: &Handle<AssetActor>,
        node_id: NodeID,
    ) -> (NodeID, Box<dyn Any + Send>) {
        let node = self.project().graph.nodes[node_id].clone();
        if let Some(n) = node.as_any().downcast_ref::<TrackReader<Audio>>() {
            let track_id = n.id;
            let mut the_clips: BTreeMap<Tick, ResolvedAudioClip> = BTreeMap::new();
            for (tick, clipid) in &self.project().tracks[track_id].clips {
                let the_clip = self.project().clips[*clipid];
                let resolved = ResolvedAudioClip::from_clip(the_clip, asset_h.clone())
                    .await
                    .unwrap();
                the_clips.insert(*tick, resolved);
            }
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

impl HasHandle<Self> for ProjectActor {
    fn handle(&self) -> &Handle<Self> {
        &self.loopback
    }
}

// #[async_trait]
impl Actor for ProjectActor {
    type Data = ProjectData;
    type InitParams = ProjectData;
    type Envelope = BoxedEnvelope<Self>;
    type Carrier = StdCarrier<Self>;
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

    fn new(current: Self::InitParams, loopback: Handle<Self>) -> Self {
        Self {
            current,
            undo_stack: vec![],
            redo_stack: vec![],
            known_node_ids: HashSet::default(),
            loopback,
        }
    }
}
