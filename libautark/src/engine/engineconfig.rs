use std::sync::Arc;

use cpal::{Device, Host};
use cpal::{SampleFormat, StreamConfig};

#[derive(Clone)]
pub struct EngineConfig {
    pub host: Arc<Host>,
    pub device: Arc<Device>,
    pub config: StreamConfig,
    pub sample_format: SampleFormat,
}

impl EngineConfig {
    pub fn create() -> anyhow::Result<Self> {
        use anyhow::Context;
        use cpal::traits::{DeviceTrait, HostTrait};
        // --- 1. Host -> Device -> Config -----------------------------------

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
