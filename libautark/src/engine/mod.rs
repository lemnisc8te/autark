//! The core audio engine. Used to manipulate `Project`s, hold the audio thread, and more.
pub mod constants;
pub mod engineconfig;
pub mod errors;
pub mod manager;
pub mod state;
pub mod tick;
pub mod transport;
pub mod util;

use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};

use crate::engine::manager::HasHandle;
use crate::{
    engine::{
        constants::DEFAULT_MANAGER_CAPACITY,
        engineconfig::EngineConfig,
        manager::{
            Handle, IntoEnvelope, Manager, StdManager,
            asset::AssetActor,
            audio::{AudioActor, UpdateCmd},
            project::{ProjectActor, commands::meta::Publish},
        },
        tick::Tick,
    },
    model::{
        flow::{ErasedNode, NodeID},
        project::ProjectData,
    },
};

use anyhow::Result;

pub type SlotIndex = usize;

#[derive(Clone)]
pub struct ScheduleStep {
    pub node: Arc<dyn ErasedNode>,
    pub node_id: NodeID,
    pub input_slots: Vec<SlotIndex>,
    pub output_slots: Vec<SlotIndex>,
}

unsafe impl Send for ScheduleStep {}
unsafe impl Sync for ScheduleStep {}

#[derive(Default, Clone)]
pub struct CompiledGraph {
    pub steps: Vec<ScheduleStep>,
    pub buffer_count: usize,
    pub capture_slot: SlotIndex,
}

pub struct Engine {
    pub playhead: Arc<AtomicU64>,
    config: EngineConfig,
    asset_h: Handle<AssetActor>,
    project_h: Handle<ProjectActor>,
    audio_h: Handle<AudioActor>,
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
    pub fn new(project: ProjectData) -> Result<Self> {
        let config = EngineConfig::create()?;

        let playhead = Arc::new(AtomicU64::new(0));

        let audio_h = StdManager::<AudioActor>::spawn(
            (config.clone(), playhead.clone()),
            DEFAULT_MANAGER_CAPACITY,
        );

        let project_h = StdManager::<ProjectActor>::spawn(project, DEFAULT_MANAGER_CAPACITY);

        let asset_h = StdManager::<AssetActor>::spawn((), DEFAULT_MANAGER_CAPACITY);
        Ok(Self {
            playhead,
            config,
            asset_h,
            project_h,
            audio_h,
        })
    }

    pub async fn publish(&self, filter: Option<Vec<NodeID>>) {
        let update = self
            .project_h
            .call(Publish {
                asset_h: self.asset_h.clone(),
                filter,
            })
            .await
            .unwrap();
        self.audio_h.call(UpdateCmd(update)).await;
    }

    #[must_use]
    pub const fn sample_rate(&self) -> u32 {
        self.config.config.sample_rate
    }

    #[must_use]
    pub const fn channels(&self) -> u16 {
        self.config.config.channels
    }

    pub fn move_playhead(&self, to: Tick) {
        self.playhead.swap(to.0, Ordering::Relaxed);
    }
}

impl HasHandle<ProjectActor> for Engine {
    fn handle(&self) -> &Handle<ProjectActor> {
        dbg!("Getting project_h");
        &self.project_h
    }
}
impl HasHandle<AssetActor> for Engine {
    fn handle(&self) -> &Handle<AssetActor> {
        dbg!("Getting asset_h");
        &self.asset_h
    }
}

impl HasHandle<AudioActor> for Engine {
    fn handle(&self) -> &Handle<AudioActor> {
        dbg!("Getting audio_h");
        &self.audio_h
    }
}

impl Engine {
    pub async fn get<C>(&self, command: C) -> C::Output
    where
        C: IntoEnvelope,
        Self: HasHandle<C::Actor>,
    {
        HasHandle::<C::Actor>::handle(self).call(command).await
    }

    pub fn fire<C>(&self, command: C)
    where
        C: IntoEnvelope,
        Self: HasHandle<C::Actor>,
    {
        drop(HasHandle::<C::Actor>::handle(self).notify(command));
    }
}
