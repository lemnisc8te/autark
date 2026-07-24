//! The core audio engine. Used to manipulate `Project`s, hold the audio thread, and more.
pub mod assetserver;
pub mod audiomanager;
pub mod bbp;
pub mod command;
pub mod constants;
pub mod engineconfig;
pub mod errors;
pub mod state;
pub mod tick;
pub mod transport;

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};

use crate::engine::audiomanager::AudioManager;
use crate::engine::bbp::BlockBufferPool;
pub use crate::engine::command::*;
use crate::engine::constants::{MAX_BUFFER_SLOTS, MAX_NODES};
use crate::engine::state::{GraphUpdate, NodeStatePool};
use crate::engine::transport::Transport;
use crate::engine::{engineconfig::EngineConfig, tick::Tick};

use crate::model::{asset::AudioAssetID, flow::NodeID, project::ProjectData};

use anyhow::Result;

pub type SlotIndex = usize;

pub struct ScheduleStep {
    pub node_id: NodeID,
    pub input_slots: Vec<SlotIndex>,
    pub output_slots: Vec<SlotIndex>,
}

pub struct CompiledGraph {
    pub steps: Vec<ScheduleStep>,
    pub buffer_count: usize,
    pub master_output_slot: SlotIndex,
}

/// Runs the compiled schedule for one block and returns the master mix.
pub fn execute_block<'a>(
    schedule: &CompiledGraph,
    project: &ProjectData,
    block_start: Tick,
    pool: &'a mut BlockBufferPool,
    state_pool: &mut NodeStatePool,
) -> &'a [f32] {
    // assert_no_alloc(|| {

    // Clear the pool. Unless you want to summon demons.
    pool.clear();

    let mut executor = pool.executor();

    for i in 0..schedule.steps.len() {
        let step = &schedule.steps[i];
        let node = &project.graph.nodes[step.node_id];

        node.process_erased(
            &mut executor,
            state_pool.get_mut(step.node_id),
            project,
            block_start,
            &step.input_slots,
            &step.output_slots,
        );
    }

    executor.get_input(schedule.master_output_slot)
    // })
}

/// What the audio thread reads.
pub struct RenderState {
    pub project: Arc<ProjectData>,
    pub schedule: Arc<CompiledGraph>,
}

pub struct Engine {
    pub transport: Arc<Transport>,
    pub playhead: Arc<AtomicU64>,
    config: EngineConfig,
    current: Arc<ProjectData>,
    undo_stack: Vec<Arc<ProjectData>>,
    redo_stack: Vec<Arc<ProjectData>>,
    audio_manager: AudioManager,
}

impl Engine {
    /// .
    ///
    /// # Panics
    ///
    /// Panics if .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn new(project: Arc<ProjectData>) -> Result<Self> {
        let config = EngineConfig::create()?;
        let schedule = project.compile_graph()?;
        assert!(
            !(schedule.buffer_count > MAX_BUFFER_SLOTS || project.graph.nodes.len() > MAX_NODES),
            "Graph is too large"
        );

        // Initial state for every node already in the fresh graph.
        let state_additions: Vec<_> = project
            .graph
            .nodes
            .iter()
            .map(|(id, node)| (id, node.spawn_state()))
            .collect();

        let init_update = GraphUpdate {
            project: project.clone(),
            schedule: Arc::new(schedule),
            state_additions,
            state_removals: Vec::new(),
        };
        let transport = Arc::new(Transport::default());
        let playhead = Arc::new(AtomicU64::new(0));

        let audio_manager =
            AudioManager::new(init_update, &config, transport.clone(), playhead.clone())?;

        Ok(Self {
            config,
            playhead,
            transport,
            current: project,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            audio_manager,
        })
    }

    pub fn project(&self) -> &ProjectData {
        &self.current
    }

    pub const fn sample_rate(&self) -> u32 {
        self.config.config.sample_rate
    }

    pub const fn channels(&self) -> u16 {
        self.config.config.channels
    }

    /// Load an asset into the [`Engine`].
    ///
    /// Currently is blocking; will eventually create a dedicated off-threa asset server similar to the audio thread.
    ///
    /// # Errors
    ///
    /// This function will return an error if [`assetserver::load_audio_asset`] fails
    pub fn load_asset(&mut self, path: impl Into<PathBuf>) -> Result<AudioAssetID> {
        let asset = assetserver::load_audio_asset(path, self.sample_rate())?;
        let mut next = (*self.current).clone();
        let id = next.assets.insert(asset);
        self.commit(next);
        Ok(id)
    }

    /// Execute a command in the engine.
    ///
    /// # Errors
    ///
    /// This function will return an error if the command fails.
    pub fn apply<T>(&mut self, cmd: T) -> Result<T::Output>
    where
        T: Command,
    {
        let mut next = (*self.current).clone();
        let res = Self::apply_command(&mut next, cmd)?;
        self.commit(next);
        Ok(res)
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

    fn commit(&mut self, next: ProjectData) {
        self.undo_stack
            .push(std::mem::replace(&mut self.current, Arc::new(next)));
        self.redo_stack.clear();
        self.publish_current();
    }

    /// Builds the next `GraphUpdate` off the audio thread and pushes it
    /// through the ring. Allocation happens here, on the control thread —
    /// that's fine, this is not the real-time path.
    fn publish_current(&mut self) {
        let schedule = self
            .current
            .compile_graph()
            .expect("command validation prevents cycles");

        if schedule.buffer_count > MAX_BUFFER_SLOTS || self.current.graph.nodes.len() > MAX_NODES {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            eprintln!("graph exceeds preallocated real-time budget; edit ignored");
            return;
        }

        let old_ids: std::collections::HashSet<NodeID> = self
            .undo_stack
            .last()
            .map(|proj| proj.graph.nodes.keys().collect())
            .unwrap_or_default();
        let new_ids: std::collections::HashSet<NodeID> = self.current.graph.nodes.keys().collect();

        let state_additions: Vec<_> = new_ids
            .difference(&old_ids)
            .map(|&id| (id, self.current.graph.nodes[id].spawn_state()))
            .collect();
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        let update = GraphUpdate {
            project: self.current.clone(),
            schedule: Arc::new(schedule),
            state_additions,
            state_removals,
        };

        if self.audio_manager.update_tx.push(update).is_err() {
            eprintln!("update ring full — audio thread stalled or edits too rapid; dropping edit");
        }
    }

    fn apply_command<T>(project: &mut ProjectData, cmd: T) -> Result<T::Output>
    where
        T: Command,
    {
        cmd.execute(project)
    }

    pub fn move_playhead(&self, to: Tick) {
        self.playhead.swap(to.0, Ordering::Relaxed);
    }
}
