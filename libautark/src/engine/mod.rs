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
pub mod token;
pub mod transport;

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};

use crate::engine::audiomanager::AudioManager;
use crate::engine::bbp::BlockBufferPool;
pub use crate::engine::command::*;
use crate::engine::constants::{COMMAND_RING_CAPACITY, MAX_BUFFER_SLOTS, MAX_NODES};
use crate::engine::state::{GraphUpdate, NodeStatePool};
use crate::engine::transport::Transport;
use crate::engine::{engineconfig::EngineConfig, tick::Tick};

use crate::model::project::ProjectData;
use crate::model::{asset::AudioAssetID, flow::NodeID, project::RtProjectData};

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
    project: &RtProjectData,
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

pub struct Engine {
    cmd_rx: tokio::sync::mpsc::Receiver<Box<dyn ErasedCommand + Send>>,
    transport: Arc<Transport>,
    playhead: Arc<AtomicU64>,
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
    pub fn init(
        project: Arc<ProjectData>,
    ) -> Result<(
        Self,
        tokio::sync::mpsc::Sender<Box<dyn ErasedCommand + Send>>,
    )> {
        let config = EngineConfig::create()?;
        let schedule = project.compile_graph()?;
        assert!(
            !(schedule.buffer_count > MAX_BUFFER_SLOTS
                || project.graph.lock().nodes.len() > MAX_NODES),
            "Graph is too large"
        );

        // Initial state for every node already in the fresh graph.
        let state_additions: Vec<_> = project
            .graph
            .lock()
            .nodes
            .iter()
            .map(|(id, node)| (id, node.spawn_state()))
            .collect();

        let init_update = GraphUpdate {
            project: project.clone().into(),
            schedule: Arc::new(schedule),
            state_additions,
            state_removals: Vec::new(),
        };
        let transport = Arc::new(Transport::default());
        let playhead = Arc::new(AtomicU64::new(0));

        let audio_manager =
            AudioManager::new(init_update, &config, transport.clone(), playhead.clone())?;

        let (cmd_tx, cmd_rx): (tokio::sync::mpsc::Sender<_>, tokio::sync::mpsc::Receiver<_>) =
            tokio::sync::mpsc::channel(COMMAND_RING_CAPACITY);

        let me = Self {
            cmd_rx,
            config,
            playhead,
            transport,
            current: project,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            audio_manager,
        };

        Ok((me, cmd_tx))
    }

    pub async fn run_loop(&mut self) {
        while let Some(envelope) = self.cmd_rx.recv().await {
            // 2. Synchronize the backend changes over to the audio thread bridge
            self.apply(envelope);
        }
    }

    pub fn project(&self) -> Arc<ProjectData> {
        self.current.clone()
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
        let next = Arc::new((*self.current).clone());
        let id = next.assets.lock().insert(asset);
        self.commit(next.into());
        Ok(id)
    }

    /// Execute a command in the engine.
    ///
    /// # Errors
    ///
    /// This function will return an error if the command fails.
    pub fn apply(&mut self, cmd: Box<dyn ErasedCommand + Send>) {
        let next = Arc::new((*self.current).clone());
        cmd.execute_and_reply(next.clone());
        self.commit(next);
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

    fn commit(&mut self, next: Arc<ProjectData>) {
        self.undo_stack
            .push(std::mem::replace(&mut self.current, next));
        self.redo_stack.clear();
        self.publish_current();
    }

    /// Builds the next `GraphUpdate` off the audio thread and pushes it
    /// through the ring. Allocation happens here, on the control thread —
    /// that's fine, this is not the real-time path.
    fn publish_current(&mut self) {
        let graph = self.current.graph.lock();
        let schedule = self
            .current
            .compile_graph()
            .expect("command validation prevents cycles");

        if schedule.buffer_count > MAX_BUFFER_SLOTS || graph.nodes.len() > MAX_NODES {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            eprintln!("graph exceeds preallocated real-time budget; edit ignored");
            return;
        }

        let old_ids: std::collections::HashSet<NodeID> = self
            .undo_stack
            .last()
            .map(|proj| proj.graph.lock().nodes.keys().collect())
            .unwrap_or_default();
        let new_ids: std::collections::HashSet<NodeID> = graph.nodes.keys().collect();

        let state_additions: Vec<_> = new_ids
            .difference(&old_ids)
            .map(|&id| (id, graph.nodes[id].spawn_state()))
            .collect();
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        let update = GraphUpdate {
            project: self.current.clone().into(),
            schedule: Arc::new(schedule),
            state_additions,
            state_removals,
        };

        if self.audio_manager.update_tx.push(update).is_err() {
            eprintln!("update ring full — audio thread stalled or edits too rapid; dropping edit");
        }
    }

    pub fn move_playhead(&self, to: Tick) {
        self.playhead.swap(to.0, Ordering::Relaxed);
    }
}
