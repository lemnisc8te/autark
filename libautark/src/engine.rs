//! The core audio engine. Used to manipulate `Project`s, hold the audio thread, and more.
pub mod constants;
pub mod errors;
pub mod manager;
pub mod schedule;
pub mod state;
pub mod tick;
pub mod transport;
pub mod util;

pub use errors::EngineError;
pub use manager::{
    Actor, ActorRef, HasActorRef, Manager, MultithreadManager, Permission, Read, StdManager, Write,
    asset, audio, project,
};
pub use tick::Tick;

/// Helper to group these "pub use"s as commands
pub mod commands {
    pub use super::manager::{Command, asset::commands::*, audio::*, project::commands::*};
}

use crate::{
    engine::{
        asset::AssetActor,
        audio::AudioActor,
        commands::{Publish, UpdateCmd},
        constants::DEFAULT_MANAGER_CAPACITY,
        manager::Command,
        project::ProjectActor,
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
    /// This function will return an error if there is no audio output device, the device has been disconnected, the output device has no default configuration, or the device selected is not an output device.
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

/// The heart of it all. Manages the [`Actor`]s, the audio thread, garbage thread, and playhead.
pub struct Engine {
    /// The current location of the playhead. Is `Arc` so it can be shared with the audio thread.
    pub playhead: Arc<AtomicU64>,
    /// Engine Configuration
    config: EngineConfig,
    /// Handles to the actors
    asset_h: ActorRef<AssetActor>,
    /// Ditto
    project_h: ActorRef<ProjectActor>,
    /// Ditto
    audio_h: ActorRef<AudioActor>,
}

impl Engine {
    /// Create a new [`Engine`].
    ///
    /// # Errors
    ///
    /// This function will return an error if creating the [`EngineConfig`] fails.
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

    pub(crate) async fn publish(&self, filter: Option<Vec<NodeID>>) {
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
    #[expect(missing_docs)]
    pub const fn sample_rate(&self) -> u32 {
        self.config.config.sample_rate
    }

    #[must_use]
    #[expect(missing_docs)]
    pub const fn channels(&self) -> u16 {
        self.config.config.channels
    }

    #[expect(missing_docs)]
    pub fn move_playhead(&self, to: Tick) {
        self.playhead.swap(to.0, Ordering::Relaxed);
    }
}

impl HasActorRef<ProjectActor> for Engine {
    fn get_ref(&self) -> &ActorRef<ProjectActor> {
        &self.project_h
    }
}
impl HasActorRef<AssetActor> for Engine {
    fn get_ref(&self) -> &ActorRef<AssetActor> {
        &self.asset_h
    }
}

impl HasActorRef<AudioActor> for Engine {
    fn get_ref(&self) -> &ActorRef<AudioActor> {
        &self.audio_h
    }
}

impl Engine {
    pub async fn get<C, P>(&self, command: C) -> C::Output
    where
        P: Permission<C::Actor>,
        C: Command<P>,
        Self: HasActorRef<C::Actor>,
    {
        HasActorRef::<C::Actor>::get_ref(self).call(command).await
    }

    pub async fn fire<C, P>(&self, command: C)
    where
        P: Permission<C::Actor>,
        C: Command<P>,
        Self: HasActorRef<C::Actor>,
    {
        let _ = HasActorRef::<C::Actor>::get_ref(self).notify(command).await;
    }
}
