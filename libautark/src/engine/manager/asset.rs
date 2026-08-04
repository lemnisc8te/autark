//! Asset loading — the one place symphonia is used. Fully in-memory decode;
//! streaming/paging would only change this function's internals.

use anyhow::Result;
use slotmap::SlotMap;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

use crate::{
    engine::{
        manager::{
            Actor, BoxedEnvelope, Handle, HasHandle, Operate, StdCarrier,
            asset::commands::LoadAudioAsset,
        },
        util::workerpool::WorkerPool,
    },
    model::{
        Audio, Kind, Location,
        asset::{AssetData, AudioAsset, AudioAssetID, AudioAssetPayload},
    },
};

use audioadapter_buffers::direct::InterleavedSlice;
use rubato::{
    Async, FixedAsync, Resampler, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};

use symphonia::core::{
    audio::sample::Sample,
    codecs::audio::AudioDecoderOptions,
    errors::Error,
    formats::{FormatOptions, TrackType, probe::Hint},
    io::MediaSourceStream,
    meta::MetadataOptions,
};

pub mod commands;

pub trait AssetSlot<K: Kind>: Sized
where
    K::Asset: Clone,
{
    fn new(data: AssetData<K::Asset>) -> Self;
    fn get_watch(&self) -> watch::Sender<AssetData<K::Asset>>;
}

#[derive(Debug, Clone)]
pub struct AudioAssetSlot {
    pub watch: watch::Sender<AssetData<AudioAsset>>,
}

impl AssetSlot<Audio> for AudioAssetSlot {
    fn new(data: AssetData<AudioAsset>) -> Self {
        Self {
            watch: watch::Sender::new(data),
        }
    }

    fn get_watch(&self) -> watch::Sender<AssetData<<Audio as Kind>::Asset>> {
        self.watch.clone()
    }
}

pub struct AssetRegistry {
    pub audio: SlotMap<AudioAssetID, AudioAssetSlot>,
    pub io_pool: WorkerPool,
}

impl AssetRegistry {
    fn new() -> Self {
        Self {
            audio: Default::default(),
            io_pool: WorkerPool::new(),
        }
    }

    fn create_audio_asset(
        LoadAudioAsset(path, target_sample_rate): LoadAudioAsset,
    ) -> Result<AudioAsset, anyhow::Error> {
        let file = Box::new(File::open(&path)?);
        let mss = MediaSourceStream::new(file, Default::default());
        let hint = Hint::new();
        let fmt_opts: FormatOptions = Default::default();
        let meta_opts: MetadataOptions = Default::default();
        let dec_opts: AudioDecoderOptions = Default::default();
        let mut format = symphonia::default::get_probe().probe(&hint, mss, fmt_opts, meta_opts)?;
        let track = format.default_track(TrackType::Audio).unwrap();
        let source_sample_rate = track
            .codec_params
            .as_ref()
            .unwrap()
            .audio()
            .unwrap()
            .sample_rate
            .unwrap_or(target_sample_rate);
        let mut decoder = symphonia::default::get_codecs().make_audio_decoder(
            track.codec_params.as_ref().unwrap().audio().unwrap(),
            &dec_opts,
        )?;
        let track_id = track.id;
        let channels = decoder
            .codec_params()
            .channels
            .as_ref()
            .map_or_else(|| 1u16, |channels| channels.count() as u16);
        let mut scratch: Vec<f32> = vec![];
        let mut samples: Vec<f32> = vec![];
        let mut total_sample_count = 0;
        while let Some(packet) = format.next_packet()? {
            // If the packet does not belong to the selected track, skip it.
            if packet.track_id != track_id {
                continue;
            }

            // Decode the packet into audio samples, ignoring any decode errors.
            match decoder.decode(&packet) {
                Ok(audio_buf) => {
                    scratch.resize(audio_buf.samples_interleaved(), f32::MID);

                    // Copy the audio samples from the generic audio buffer to the vector in interleaved
                    // order. The sample format to convert to is inferred from the type of the Vec.
                    // Sum up the total number of samples.
                    total_sample_count += scratch.len();
                    audio_buf.copy_to_slice_interleaved(&mut scratch);
                    samples.append(&mut scratch);
                    print!("\rDecoded {total_sample_count} samples");
                }
                Err(Error::DecodeError(_)) => (),
                Err(_) => break,
            }
        }
        println!();
        let resampled =
            Self::resample_rubato(&samples, channels, source_sample_rate, target_sample_rate);
        let len = resampled.len();
        let audio_asset = AudioAsset {
            payload: AudioAssetPayload::Resident(Arc::from(resampled)),
            channels,
            sample_rate: target_sample_rate,
            gain: 1.0,
            path,
            len,
        };
        Ok(audio_asset)
    }

    /// Sinc-interpolated resample of interleaved f32 samples, with proper
    /// anti-aliasing filtering. Import-time only — never called from the
    /// audio thread, so the allocations inside rubato's `process()` are fine.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn resample_rubato(
        interleaved: &[f32],
        channels: u16,
        from_rate: u32,
        to_rate: u32,
    ) -> Vec<f32> {
        println!("Converting sample rate {from_rate} Hz -> {to_rate} Hz");
        if from_rate == to_rate || interleaved.is_empty() {
            return interleaved.to_vec();
        }
        let channels = channels as usize;
        let frame_count = interleaved.len() / channels;
        let f_ratio = f64::from(to_rate) / f64::from(from_rate);

        let mut outdata: Vec<f32> =
            vec![0.0; 2 * channels * (frame_count as f64 * f_ratio).trunc() as usize];

        let outdata_capacity = outdata.len() / channels;

        let input_adapter = InterleavedSlice::new(interleaved, channels, frame_count).unwrap();
        let mut output_adapter =
            InterleavedSlice::new_mut(&mut outdata, channels, outdata_capacity).unwrap();

        let mut resampler = {
            let sinc_len = 128;
            let oversampling_factor = 256;
            let interpolation = SincInterpolationType::Quadratic;
            let window = WindowFunction::Blackman2;

            let params = SincInterpolationParameters::new(sinc_len, window)
                .oversampling_factor(oversampling_factor)
                .interpolation(interpolation);
            Async::<f32>::new_sinc(f_ratio, 2.0, &params, 1024, channels, FixedAsync::Output)
                .unwrap()
        };

        let (nbr_in, nbr_out) = resampler
            .process_all_into_buffer(&input_adapter, &mut output_adapter, frame_count, None)
            .unwrap();

        println!("Processed {nbr_in} input frames into {nbr_out} output frames");
        outdata
    }
}

pub struct AssetActor {
    reg: RwLock<AssetRegistry>,
    loopback: Handle<Self>,
}

impl Location for AssetActor {
    type Data = Self;
}

impl HasHandle<Self> for AssetActor {
    fn handle(&self) -> &Handle<Self> {
        &self.loopback
    }
}

impl Actor for AssetActor {
    type InitParams = ();

    type Envelope = BoxedEnvelope<Self>;

    type Carrier = StdCarrier<Self>;

    fn new((): Self::InitParams, loopback: Handle<Self>) -> Self {
        Self {
            reg: AssetRegistry::new().into(),
            loopback,
        }
    }
}

impl Operate for AssetActor {
    type Data = AssetRegistry;
    async fn mutate<O>(&self, f: impl AsyncFnOnce(&mut Self::Data) -> O) -> O {
        let mut lock = self.reg.write().await;
        f(&mut lock).await
    }

    async fn query<O>(&self, f: impl AsyncFn(&Self::Data) -> O) -> O {
        let lock = self.reg.read().await;
        f(&lock).await
    }
}
