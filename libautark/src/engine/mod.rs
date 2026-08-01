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

use crate::engine::manager::asset::AssetActor;
use crate::engine::manager::audio::{AudioActor, UpdateCmd};
use crate::engine::manager::project::{ProjectActor, Publish};
use crate::engine::manager::{StdCarrier, StdHandle};
use crate::model::flow::{ErasedNode, NodeID};
use crate::{
    engine::{
        constants::DEFAULT_MANAGER_CAPACITY,
        engineconfig::EngineConfig,
        manager::{
            Actor, Carrier, Command, Handle, IntoEnvelope, Manager, Mutate, Ref, StdManager,
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
    asset_h: StdHandle<AssetActor>,
    project_h: StdHandle<ProjectActor>,
    audio_h: StdHandle<AudioActor>,
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

        let (audio_h, _audio_j) = StdManager::<AudioActor>::spawn(
            (config.clone(), playhead.clone()),
            DEFAULT_MANAGER_CAPACITY,
        );

        let (project_h, _project_join) =
            StdManager::<ProjectActor>::spawn(project, DEFAULT_MANAGER_CAPACITY);

        let (asset_h, _asset_j) = StdManager::<AssetActor>::spawn((), DEFAULT_MANAGER_CAPACITY);

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
        self.audio_h.fire_mut(UpdateCmd(update)).unwrap();
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

pub trait HasHandle<A: Actor> {
    type Carrier: Carrier<A>;
    fn handle(&self) -> &Handle<A, Self::Carrier>;
}

impl HasHandle<ProjectActor> for Engine {
    type Carrier = StdCarrier<ProjectActor>;
    fn handle(&self) -> &StdHandle<ProjectActor> {
        &self.project_h
    }
}
impl HasHandle<AssetActor> for Engine {
    type Carrier = StdCarrier<AssetActor>;
    fn handle(&self) -> &StdHandle<AssetActor> {
        &self.asset_h
    }
}
impl HasHandle<AudioActor> for Engine {
    type Carrier = StdCarrier<AudioActor>;
    fn handle(&self) -> &StdHandle<AudioActor> {
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

    pub fn notify<C>(&self, command: C)
    where
        C: Command<Ref> + IntoEnvelope<Ref>,
        Self: HasHandle<C::Actor>,
    {
        let _ = HasHandle::<C::Actor>::handle(self).notify(command);
    }

    pub fn fire_mut<C>(&self, command: C)
    where
        C: Command<Mutate> + IntoEnvelope<Mutate>,
        Self: HasHandle<C::Actor>,
    {
        let _ = HasHandle::<C::Actor>::handle(self).fire_mut(command);
    }
}
