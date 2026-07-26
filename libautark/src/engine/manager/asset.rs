//! Asset loading — the one place symphonia is used. Fully in-memory decode;
//! streaming/paging would only change this function's internals.

use anyhow::Result;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::model::asset::AudioAsset;

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

pub fn load_audio_asset(path: impl Into<PathBuf>, target_sample_rate: u32) -> Result<AudioAsset> {
    let path = path.into();
    // Create a media source. Note that the MediaSource trait is automatically implemented for File,
    // among other types.
    let file = Box::new(File::open(&path)?);

    // Create the media source stream using the boxed media source from above.
    let mss = MediaSourceStream::new(file, Default::default());

    // Create a hint to help the format registry guess what format reader is appropriate. In this
    // example we'll leave it empty.
    let hint = Hint::new();

    // Use the default options when reading and decoding.
    let fmt_opts: FormatOptions = Default::default();
    let meta_opts: MetadataOptions = Default::default();
    let dec_opts: AudioDecoderOptions = Default::default();

    // Probe the media source stream for a format.
    let mut format = symphonia::default::get_probe().probe(&hint, mss, fmt_opts, meta_opts)?;

    // Get the default audio track.
    let track = format.default_track(TrackType::Audio).unwrap();

    let source_sample_rate = track
        .codec_params
        .as_ref()
        .unwrap()
        .audio()
        .unwrap()
        .sample_rate
        .unwrap_or(target_sample_rate); // no rate in the stream -> assume it matches; can't do better
    // Create a decoder for the track.
    let mut decoder = symphonia::default::get_codecs().make_audio_decoder(
        track.codec_params.as_ref().unwrap().audio().unwrap(),
        &dec_opts,
    )?;

    // Store the track identifier, we'll use it to filter packets.
    let track_id = track.id;

    let channels = decoder
        .codec_params()
        .channels
        .as_ref()
        .map_or_else(|| 1u16, |channels| channels.count() as u16);
    let mut scratch: Vec<f32> = vec![];
    let mut samples: Vec<f32> = vec![]; // Vec::with_capacity(channels as usize * expected_sample_rate as usize * 2);
    let mut total_sample_count = 0;

    // Read and decode all packets from the format reader.
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
    let resampled = resample_rubato(&samples, channels, source_sample_rate, target_sample_rate);

    Ok(AudioAsset {
        samples: Arc::new(resampled),
        channels,
        sample_rate: target_sample_rate,
        gain: 1.0,
        path,
    })
}

/// Sinc-interpolated resample of interleaved f32 samples, with proper
/// anti-aliasing filtering. Import-time only — never called from the
/// audio thread, so the allocations inside rubato's `process()` are fine.
fn resample_rubato(interleaved: &[f32], channels: u16, from_rate: u32, to_rate: u32) -> Vec<f32> {
    println!("Converting sample rate {from_rate} Hz -> {to_rate} Hz");
    if from_rate == to_rate || interleaved.is_empty() {
        return interleaved.to_vec();
    }
    let channels = channels as usize;
    let frame_count = interleaved.len() / channels;
    let f_ratio = f64::from(to_rate) / f64::from(from_rate);

    let mut outdata: Vec<f32> = vec![0.0; 2 * channels * (frame_count as f64 * f_ratio) as usize];

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
        Async::<f32>::new_sinc(f_ratio, 2.0, &params, 1024, channels, FixedAsync::Output).unwrap()
    };

    let (nbr_in, nbr_out) = resampler
        .process_all_into_buffer(&input_adapter, &mut output_adapter, frame_count, None)
        .unwrap();

    println!("Processed {nbr_in} input frames into {nbr_out} output frames");
    outdata
}
