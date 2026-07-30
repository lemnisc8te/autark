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

use std::any::Any;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};
use std::thread;

use crate::engine::constants::{DEFAULT_MANAGER_CAPACITY, MAX_BUFFER_SLOTS, MAX_NODES};
use crate::engine::manager::asset::AssetTaskCarrier;
use crate::engine::manager::audio::{AudioManager, AudioTaskCarrier};
use crate::engine::manager::project::{ProjectCommand, ProjectTaskCarrier};
use crate::engine::manager::{
    Actor, BoxedEnvelope, Carrier, Command, Envelope, Handle, IntoEnvelope, Manager, Permission,
    Ref, StdManager, spawn_actor,
};
use crate::engine::manager::{audio::AudioActor, project::ProjectActor};
use crate::engine::state::GraphUpdate;
use crate::engine::transport::Transport;
use crate::engine::{engineconfig::EngineConfig, tick::Tick};

use crate::model::{flow::NodeID, project::ProjectData};

use anyhow::Result;
use tokio::sync::oneshot;

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

struct Message<A: Actor, C: IntoEnvelope<A, Ref>> {
    c: C,
    _a: PhantomData<A>,
}

// impl<A, C> Command<Engine, Ref> for Message<A,  C>
// where
//     A: Actor,
//     C: Command<A, Ref>,
// {
//     type Output = C::Output;

//     fn execute(self, engine: &Engine) -> Self::Output {
//         engine.
//     }
// }

impl Actor for Engine {
    type InitParams = ProjectData;

    type Data = Self;

    type Envelope = BoxedEnvelope<Self>;

    fn new(params: Self::InitParams) -> Self {
        todo!()
    }

    fn data(&self) -> &Self::Data {
        todo!()
    }

    fn data_mut(&mut self) -> &mut Self::Data {
        todo!()
    }
}
