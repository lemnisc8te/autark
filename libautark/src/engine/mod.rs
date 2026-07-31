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

use crate::engine::manager::audio::UpdateCmd;
use crate::engine::manager::project::Publish;
use crate::model::flow::{ErasedNode, NodeID};
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
    model::project::ProjectData,
};

use anyhow::Result;

pub type SlotIndex = usize;

pub struct ScheduleStep {
    pub node: Arc<dyn ErasedNode>,
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

        let playhead = Arc::new(AtomicU64::new(0));

        let (audio_h, audio_j) = StdManager::<AudioTaskCarrier>::spawn(
            (config.clone(), playhead.clone()),
            DEFAULT_MANAGER_CAPACITY,
        );

        let (project_h, project_join) =
            StdManager::<ProjectTaskCarrier>::spawn(project, DEFAULT_MANAGER_CAPACITY);

        let (asset_h, asset_j) =
            StdManager::<AssetTaskCarrier>::spawn((), DEFAULT_MANAGER_CAPACITY);

        Ok(Self {
            playhead,
            config,
            asset_h,
            project_h,
            audio_h,
        })
    }

    pub async fn publish(&self) {
        let update = self
            .project_h
            .meta_call(Publish {
                asset_h: self.asset_h.clone(),
            })
            .await
            .unwrap();
        self.audio_h.fire_mut(UpdateCmd(update)).await.unwrap();
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

pub trait HasHandle<A: Actor> {
    type Carrier: Carrier<A>;
    fn handle(&self) -> &Handle<A, Self::Carrier>;
}

impl HasHandle<manager::project::ProjectActor> for Engine {
    type Carrier = ProjectTaskCarrier;
    fn handle(&self) -> &Handle<manager::project::ProjectActor, ProjectTaskCarrier> {
        &self.project_h
    }
}
impl HasHandle<manager::asset::AssetActor> for Engine {
    type Carrier = AssetTaskCarrier;
    fn handle(&self) -> &Handle<manager::asset::AssetActor, AssetTaskCarrier> {
        &self.asset_h
    }
}
impl HasHandle<manager::audio::AudioActor> for Engine {
    type Carrier = AudioTaskCarrier;
    fn handle(&self) -> &Handle<manager::audio::AudioActor, AudioTaskCarrier> {
        &self.audio_h
    }
}

impl Engine {
    pub async fn get<C>(&self, command: C) -> C::Output
    where
        C: Command<Ref> + IntoEnvelope<Ref>,
        Self: HasHandle<C::Actor>,
    {
        HasHandle::<C::Actor>::handle(self).call(command).await
    }

    pub async fn call_mut<C>(&self, command: C) -> C::Output
    where
        C: Command<Mutate> + IntoEnvelope<Mutate>,
        Self: HasHandle<C::Actor>,
    {
        HasHandle::<C::Actor>::handle(self).call_mut(command).await
    }

    pub async fn notify<C>(&self, command: C)
    where
        C: Command<Ref> + IntoEnvelope<Ref>,
        Self: HasHandle<C::Actor>,
    {
        let _ = HasHandle::<C::Actor>::handle(self).notify(command).await;
    }

    pub async fn fire_mut<C>(&self, command: C)
    where
        C: Command<Mutate> + IntoEnvelope<Mutate>,
        Self: HasHandle<C::Actor>,
    {
        let _ = HasHandle::<C::Actor>::handle(self).fire_mut(command).await;
    }
}
