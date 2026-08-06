//! The core audio engine. Used to manipulate `Project`s, hold the audio thread, and more.
pub mod constants;
pub mod errors;
pub mod manager;
pub mod schedule;
pub mod state;
pub mod tick;
pub mod transport;
pub mod util;

pub use tick::Tick;

use crate::engine::manager::{HasHandle, MultithreadManager, Permission};
use crate::{
    engine::{
        constants::DEFAULT_MANAGER_CAPACITY,
        manager::{
            Handle, IntoEnvelope, Manager, StdManager,
            asset::AssetActor,
            audio::{AudioActor, UpdateCmd},
            project::{ProjectActor, commands::meta::Publish},
        },
    },
    model::{flow::NodeID, project::ProjectData},
};

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use cpal::{Device, Host};
use cpal::{SampleFormat, StreamConfig};

#[derive(Clone)]
pub struct EngineConfig {
    host: Arc<Host>,
    device: Arc<Device>,
    config: StreamConfig,
    sample_format: SampleFormat,
}

impl EngineConfig {
    /// Create a new `EngineConfig`.
    ///
    /// # Errors
    ///
    /// This function will return an error if there is no audio output device, the device has been disconnected, the outptu device has no default configuration, or the device selected is not an output device.
    pub fn create() -> Result<Self> {
        use anyhow::Context;
        use cpal::traits::{DeviceTrait, HostTrait};

        let host = Arc::new(cpal::default_host());
        let device = Arc::new(host.default_output_device().context("no output device")?);
        let supported = device.default_output_config()?;
        let sample_format = supported.sample_format();
        let sample_rate = supported.sample_rate();
        let channels = supported.channels();
        let config: StreamConfig = supported.into();
        println!(
            "output device config: sr: {sample_rate} Hz, {channels} ch, format {sample_format:?}"
        );
        Ok(Self {
            host,
            device,
            config,
            sample_format,
        })
    }
}

/// The heart of it all, the `Engine`. Manages the [`Actor`](manager::Actor)s, the audio thread, garbage thread, and playhead.
pub struct Engine {
    /// The current location of the playhead. Is `Arc` so it can be shared with the audio thread.
    pub playhead: Arc<AtomicU64>,
    /// Engine Configuration
    config: EngineConfig,
    /// Handles to the actors
    asset_h: Handle<AssetActor>,
    /// Ditto
    project_h: Handle<ProjectActor>,
    /// Ditto
    audio_h: Handle<AudioActor>,
}

impl Engine {
    /// .
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

        let project_h =
            MultithreadManager::<ProjectActor>::spawn(project, DEFAULT_MANAGER_CAPACITY);

        let asset_h = MultithreadManager::<AssetActor>::spawn((), DEFAULT_MANAGER_CAPACITY);
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
        &self.project_h
    }
}
impl HasHandle<AssetActor> for Engine {
    fn handle(&self) -> &Handle<AssetActor> {
        &self.asset_h
    }
}

impl HasHandle<AudioActor> for Engine {
    fn handle(&self) -> &Handle<AudioActor> {
        &self.audio_h
    }
}

impl Engine {
    pub async fn get<C, P>(&self, command: C) -> C::Output
    where
        P: Permission<C::Actor>,
        C: IntoEnvelope<P>,
        Self: HasHandle<C::Actor>,
    {
        HasHandle::<C::Actor>::handle(self).call(command).await
    }

    pub async fn fire<C, P>(&self, command: C)
    where
        P: Permission<C::Actor>,
        C: IntoEnvelope<P>,
        Self: HasHandle<C::Actor>,
    {
        let _ = HasHandle::<C::Actor>::handle(self).notify(command).await;
    }
}
