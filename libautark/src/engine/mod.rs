//! The core audio engine. Used to manipulate `Project`s, hold the audio thread, and more.
pub mod bbp;
pub mod command;
pub mod constants;
pub mod engineconfig;
pub mod errors;
pub mod manager;
pub mod state;
pub mod tick;
pub mod transport;

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};

use crate::engine::constants::{MAX_BUFFER_SLOTS, MAX_NODES};
use crate::engine::manager::{audio::AudioActor, project::ProjectActor};
use crate::engine::state::GraphUpdate;
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

#[derive(Default)]
pub struct CompiledGraph {
    pub steps: Vec<ScheduleStep>,
    pub buffer_count: usize,
    pub master_output_slot: SlotIndex,
}

pub struct Engine {
    pub transport: Arc<Transport>,
    pub playhead: Arc<AtomicU64>,
    config: EngineConfig,
    project_manager: Arc<ProjectActor>,
    audio_manager: AudioActor,
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
            AudioActor::init(init_update, &config, transport.clone(), playhead.clone())?;

        todo!();
        // Ok(Self {
        //     config,
        //     playhead,
        //     transport,
        //     project_manager: Arc::new(ProjectActor {
        //         current: project,
        //         undo_stack: Vec::new(),
        //         redo_stack: Vec::new(),
        //     }),
        //     audio_manager,
        // })
    }

    pub fn project(&self) -> &ProjectData {
        &self.project_manager.project()
    }

    pub const fn sample_rate(&self) -> u32 {
        self.config.config.sample_rate
    }

    pub const fn channels(&self) -> u16 {
        self.config.config.channels
    }

    pub fn move_playhead(&self, to: Tick) -> Result<()> {
        self.playhead.swap(to.0, Ordering::Relaxed);
        Ok(())
    }
}
