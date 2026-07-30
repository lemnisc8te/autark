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

use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};

use crate::engine::constants::{DEFAULT_MANAGER_CAPACITY, MAX_BUFFER_SLOTS, MAX_NODES};
use crate::engine::manager::asset::AssetTaskCarrier;
use crate::engine::manager::audio::{AudioManager, AudioTaskCarrier};
use crate::engine::manager::project::ProjectTaskCarrier;
use crate::engine::manager::{Actor, Manager, StdManager, spawn_actor};
use crate::engine::manager::{audio::AudioActor, project::ProjectActor};
use crate::engine::state::GraphUpdate;
use crate::engine::transport::Transport;
use crate::engine::{engineconfig::EngineConfig, tick::Tick};

use crate::model::{flow::NodeID, project::ProjectData};

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

        // let transport = Arc::new(Transport::default());
        let playhead = Arc::new(AtomicU64::new(0));

        let mut audio_h =
            StdManager::<AudioTaskCarrier>::spawn((config, playhead), DEFAULT_MANAGER_CAPACITY);

        let mut project_h = StdManager::<ProjectTaskCarrier>::spawn((), DEFAULT_MANAGER_CAPACITY);

        let mut asset_h = StdManager::<AssetTaskCarrier>::spawn((), DEFAULT_MANAGER_CAPACITY);

        todo!()
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
