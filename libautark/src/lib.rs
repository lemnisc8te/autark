//! Autark is an experimental audio tool designed to create interesting and beautiful sounds.
//!
//! libautark is the library that provides backend functionality. It uses a [`Command`](engine::manager::Command)-based API to interface with an [`Engine`].
//! This API offers great flexibilty, including the ability to use `libautark` programmatically, within a UI, or something else entirely!
//!
//! The library is roughly split into two parts:
//! - [`engine`] defines the execution-related aspects of the library
//! - [`model`] defines the information-related aspects of the library
pub mod engine;
pub mod model;

use assert_no_alloc::AllocDisabler;

#[cfg(debug_assertions)] // required when disable_release is set (default)
#[global_allocator]
static ALLOC: AllocDisabler = AllocDisabler;

use crate::{
    engine::{
        commands::{
            AddClip, AddLink, AddNode, AddNodeInput, AddTrack, GetMasterNodeId, InputSocketOf,
            LoadAudioAsset, OutputSocketOf, Play, TransportCmd, WaitForAudioAsset,
        },
        transport::TransportState,
    },
    model::{
        Audio,
        flow::nodes::{biquad_filter::BiquadFilter, sum::Sum},
        project::ProjectData,
    },
};

use anyhow::Result;
use engine::Engine;
use futures::FutureExt;

#[doc(hidden)]
#[expect(clippy::too_many_lines)]
pub async fn demo() -> Result<()> {
    const CRATE_PATH: &str = env!("CARGO_MANIFEST_DIR");
    let engine = {
        let project = ProjectData::new();
        Engine::new(project).unwrap()
    };
    let master_node_id = engine.get(GetMasterNodeId).await;
    let master_in = engine.get(InputSocketOf(master_node_id, 0)).await;

    let song_asset = async {
        engine
            .get(LoadAudioAsset(
                format!("{CRATE_PATH}/assets/AUDIO_4892.mp3").into(),
                engine.sample_rate(),
            ))
            .await
            .unwrap()
    }
    .shared();

    let song_len = async {
        let asset = engine
            .get(WaitForAudioAsset(song_asset.clone().await))
            .await
            .unwrap();
        Ok::<u64, anyhow::Error>(asset.len as u64 / u64::from(asset.channels))
    };

    let filter1 = engine
        .get(AddNode {
            node: BiquadFilter::new(
                engine.channels(),
                model::flow::nodes::biquad_filter::FilterType::HighPass,
                engine.sample_rate(),
                1600.0,
                BiquadFilter::BUTTERWORTH_Q,
                0.0,
            ),
        })
        .await;

    let filter1_in = engine.get(InputSocketOf(filter1, 0)).await;
    let filter1_out = engine.get(OutputSocketOf(filter1, 0)).await;

    let master_sum = engine
        .get(AddNode {
            node: Sum::<Audio>::new(),
        })
        .await;
    let master_sum_in0 = engine
        .get(AddNodeInput::<Audio>::to(master_sum))
        .await
        .unwrap();

    let master_sum_out = engine.get(OutputSocketOf(master_sum, 0)).await;

    engine
        .get(AddLink {
            from: filter1_out,
            to: master_sum_in0,
        })
        .await?;

    engine
        .get(AddLink {
            from: master_sum_out,
            to: master_in,
        })
        .await?;

    let (song_track, song_node) = engine
        .get(AddTrack {
            name: "Song".to_string(),
            kind: Audio,
            channels: engine.channels(),
        })
        .await;

    let song_out = engine.get(OutputSocketOf(song_node, 0)).await;

    engine
        .get(AddLink {
            from: song_out,
            to: filter1_in,
        })
        .await?;

    let clap_asset = engine
        .get(LoadAudioAsset(
            format!("{CRATE_PATH}/assets/clap.mp3").into(),
            engine.sample_rate(),
        ))
        .await?;

    let clap_len = async {
        let asset = &engine.get(WaitForAudioAsset(clap_asset)).await.unwrap();
        Ok::<_, anyhow::Error>(asset.len as u64 / u64::from(asset.channels))
    };

    let (clap_track, clap_node) = engine
        .get(AddTrack {
            name: "Clap".to_string(),
            kind: Audio,
            channels: engine.channels(),
        })
        .await;

    let clap_out = engine.get(OutputSocketOf(clap_node, 0)).await;

    let master_sum_in1 = engine.get(AddNodeInput::<Audio>::to(master_sum)).await?;
    engine
        .fire(AddClip::<Audio> {
            track_id: song_track,
            start: engine::tick::Tick(0),
            end: engine::tick::Tick(song_len.await?),
            asset_id: song_asset.await,
        })
        .await;
    engine
        .fire(AddClip::<Audio> {
            track_id: clap_track,
            start: engine::tick::Tick(1000),
            end: engine::tick::Tick(clap_len.await?),
            asset_id: clap_asset,
        })
        .await;

    engine
        .fire(AddLink {
            from: clap_out,
            to: master_sum_in1,
        })
        .await;

    engine.publish(None).await;

    engine.move_playhead(engine::tick::Tick(0));
    engine.fire(Play).await;
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf).unwrap();
    engine.fire(TransportCmd(TransportState::Stopped)).await;
    Ok::<_, anyhow::Error>(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn it_works() {
        demo().await.unwrap();
    }
}
