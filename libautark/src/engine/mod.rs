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

use crate::{
    engine::{
        constants::DEFAULT_MANAGER_CAPACITY,
        engineconfig::EngineConfig,
        manager::{
            Actor, Carrier, Command, Handle, IntoEnvelope, Manager, Mutate, Ref, StdManager,
            asset::AssetTaskCarrier, audio::AudioTaskCarrier, project::ProjectTaskCarrier,
        },
        tick::Tick,
    },
    model::{flow::NodeID, project::ProjectData},
};

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
    pub playhead: Arc<AtomicU64>,
    config: EngineConfig,
    asset_h: Handle<manager::asset::AssetActor, AssetTaskCarrier>,
    project_h: Handle<manager::project::ProjectActor, ProjectTaskCarrier>,
    audio_h: Handle<manager::audio::AudioActor, AudioTaskCarrier>,
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

        // let transport = Arc::new(Transport::default());
        let playhead = Arc::new(AtomicU64::new(0));

        let audio_h = StdManager::<AudioTaskCarrier>::spawn(
            (config.clone(), playhead.clone()),
            DEFAULT_MANAGER_CAPACITY,
        );

        let project_h = StdManager::<ProjectTaskCarrier>::spawn(project, DEFAULT_MANAGER_CAPACITY);

        let asset_h: Handle<manager::asset::AssetActor, AssetTaskCarrier> =
            StdManager::<AssetTaskCarrier>::spawn((), DEFAULT_MANAGER_CAPACITY);

        Ok(Self {
            playhead,
            config,
            asset_h,
            project_h,
            audio_h,
        })
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

// engine/mod.rs
pub trait HasHandle<A: Actor> {
    type Carrier: Carrier<A>;
    fn handle_mut(&mut self) -> &mut Handle<A, Self::Carrier>;
}

impl HasHandle<manager::project::ProjectActor> for Engine {
    type Carrier = ProjectTaskCarrier;
    fn handle_mut(&mut self) -> &mut Handle<manager::project::ProjectActor, ProjectTaskCarrier> {
        &mut self.project_h
    }
}
impl HasHandle<manager::asset::AssetActor> for Engine {
    type Carrier = AssetTaskCarrier;
    fn handle_mut(&mut self) -> &mut Handle<manager::asset::AssetActor, AssetTaskCarrier> {
        &mut self.asset_h
    }
}
impl HasHandle<manager::audio::AudioActor> for Engine {
    type Carrier = AudioTaskCarrier;
    fn handle_mut(&mut self) -> &mut Handle<manager::audio::AudioActor, AudioTaskCarrier> {
        &mut self.audio_h
    }
}

impl Engine {
    pub async fn call<C>(&mut self, command: C) -> Result<C::Output>
    where
        C: Command<Ref> + IntoEnvelope<Ref>,
        Self: HasHandle<C::Actor>,
    {
        Ok(HasHandle::<C::Actor>::handle_mut(self)
            .call(command)
            .await?
            .await)
    }

    pub async fn call_mut<C>(&mut self, command: C) -> Result<C::Output>
    where
        C: Command<Mutate> + IntoEnvelope<Mutate>,
        Self: HasHandle<C::Actor>,
    {
        HasHandle::<C::Actor>::handle_mut(self)
            .call_mut(command)
            .await
    }
}
