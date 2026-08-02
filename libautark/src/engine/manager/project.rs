use crate::{
    engine::manager::{BoxedEnvelope, StdHandle, asset::AssetActor},
    model::{Audio, arr::clip::ResolvedAudioClip, flow::nodes::trackreader::TrackReaderState},
};
use anyhow::Result;

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

use std::{any::Any, collections::HashSet};

pub struct ProjectActor {
    pub(crate) current: ProjectData,
    pub(crate) undo_stack: Vec<ProjectData>,
    pub(crate) redo_stack: Vec<ProjectData>,
    pub(crate) known_node_ids: HashSet<NodeID>,
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
    pub fn publish_current(
        &mut self,
        asset_h: &StdHandle<AssetActor>,
        filter: Option<&[NodeID]>,
    ) -> Result<GraphUpdate> {
        let schedule = self.project().compile_graph(filter)?;

        if schedule.buffer_count > MAX_BUFFER_SLOTS || self.project().graph.nodes.len() > MAX_NODES
        {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            anyhow::bail!("graph exceeds preallocated real-time budget; edit ignored")
        }

        let old_ids: HashSet<NodeID> = self.known_node_ids.clone();
        let new_ids: HashSet<NodeID> = self.project().graph.nodes.keys().collect();

        let state_additions: Vec<_> = new_ids
            .difference(&old_ids)
            .map(|&id| self.create_node_state(asset_h, id))
            .collect();
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        let _ = std::mem::replace(&mut self.known_node_ids, new_ids);
        Ok(GraphUpdate {
            schedule,
            state_additions,
            state_removals,
        })
    }

    fn create_node_state(
        &self,
        asset_h: &super::Handle<AssetActor, super::StdCarrier<AssetActor>>,
        node_id: NodeID,
    ) -> (NodeID, Box<dyn Any + Send>) {
        let node = self.project().graph.nodes[node_id].clone();
        if let Some(n) = node.as_any().downcast_ref::<TrackReader<Audio>>() {
            let track_id = n.id;
            let the_clips: std::collections::BTreeMap<Tick, ResolvedAudioClip> =
                self.project().tracks[track_id]
                    .clips
                    .iter()
                    .map(|(tick, clipid)| {
                        let the_clip = self.project().clips[*clipid];
                        let resolved = ResolvedAudioClip::from_clip(the_clip, asset_h.clone());
                        (*tick, resolved)
                    })
                    .collect();

            (
                node_id,
                Box::new(TrackReaderState { clips: the_clips }) as Box<dyn Any + Send>,
            )
        } else {
            (node_id, self.project().graph.nodes[node_id].spawn_state())
        }
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
            known_node_ids: HashSet::default(),
        }
    }
}
