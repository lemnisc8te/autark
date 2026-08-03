#[forbid(
    unused_unsafe,
    clippy::fallible_impl_from,
    clippy::used_underscore_items,
    clippy::undocumented_unsafe_blocks
)]
#[deny(
    unreachable_pub,
    unused_qualifications,
    clippy::pedantic,
    clippy::cargo,
    clippy::used_underscore_binding,
    clippy::perf,
    clippy::correctness,
    clippy::suspicious,
    clippy::complexity,
    clippy::style,
    clippy::branches_sharing_code,
    clippy::use_self,
    clippy::redundant_allocation,
    clippy::deref_by_slicing,
    clippy::cloned_instead_of_copied,
    unused_allocation,
    clippy::ptr_arg,
    clippy::needless_pass_by_ref_mut,
    clippy::needless_pass_by_value,
    clippy::min_ident_chars
)]
#[warn(
    // missing_docs,
    clippy::unwrap_in_result,
    clippy::large_stack_frames,
    clippy::panic,
    clippy::dbg_macro,
    // clippy::unwrap_used,
    // clippy::restriction
)]
#[allow(
    // warnings,
    // unused_variables,
    // clippy::must_use_candidate,
    clippy::default_trait_access,
    clippy::type_complexity,
    clippy::missing_panics_doc,
    unstable_name_collisions
)]
pub mod engine;
pub mod model;

use assert_no_alloc::*;

#[cfg(debug_assertions)] // required when disable_release is set (default)
#[global_allocator]
static A: AllocDisabler = AllocDisabler;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{
            manager::{
                asset::commands::{LoadAudioAsset, WaitForAudioAsset},
                audio::{Play, TransportCmd},
                project::commands::{
                    AddClip, AddLink, AddNode, AddNodeInput, AddTrack, GetMasterNodeId,
                    InputSocketOf, OutputSocketOf,
                },
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

    #[tokio::test]
    async fn it_works() {
        helper().await.unwrap();
    }

    async fn helper() -> Result<()> {
        let engine = {
            let project = ProjectData::new();
            Engine::new(project).unwrap()
        };
        let master_node_id = engine.get(GetMasterNodeId).await;
        dbg!("got: {master_node_id}");
        let master_in = engine.get(InputSocketOf(master_node_id, 0)).await;

        let song_asset = async {
            engine
                .load(LoadAudioAsset(
                    "./assets/AUDIO_4892.mp3".into(),
                    engine.sample_rate(),
                ))
                .await
                .unwrap()
        }
        .shared();

        dbg!("got asset id");
        let song_len = async {
            let asset = engine
                .get(WaitForAudioAsset(song_asset.clone().await))
                .await
                .unwrap();
            Ok::<u64, anyhow::Error>(asset.len as u64 / u64::from(asset.channels))
        };

        let filter1 = engine
            .call_mut(AddNode {
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
            .call_mut(AddNode {
                node: Sum::<Audio>::new(),
            })
            .await;
        let master_sum_in0 = engine
            .call_mut(AddNodeInput::<Audio>::to(master_sum))
            .await
            .unwrap();

        let master_sum_out = engine.get(OutputSocketOf(master_sum, 0)).await;

        engine
            .call_mut(AddLink {
                from: filter1_out,
                to: master_sum_in0,
            })
            .await?;

        engine
            .call_mut(AddLink {
                from: master_sum_out,
                to: master_in,
            })
            .await?;

        let (song_track, song_node) = engine
            .call_mut(AddTrack {
                name: "Song".to_string(),
                kind: Audio,
                channels: engine.channels(),
            })
            .await;

        let song_out = engine.get(OutputSocketOf(song_node, 0)).await;

        engine
            .call_mut(AddLink {
                from: song_out,
                to: filter1_in,
            })
            .await?;

        engine
            .call_mut(AddClip::<Audio> {
                track: song_track,
                start: engine::tick::Tick(0),
                end: engine::tick::Tick(song_len.await?),
                asset_id: song_asset.await,
            })
            .await?;

        let clap_asset = engine
            .load(LoadAudioAsset(
                "./assets/clap.mp3".into(),
                engine.sample_rate(),
            ))
            .await?;

        let clap_len = async {
            let asset = &engine.get(WaitForAudioAsset(clap_asset)).await.unwrap();
            Ok::<_, anyhow::Error>(asset.len as u64 / u64::from(asset.channels))
        };

        let (clap_track, clap_node) = engine
            .call_mut(AddTrack {
                name: "Clap".to_string(),
                kind: Audio,
                channels: engine.channels(),
            })
            .await;

        engine
            .call_mut(AddClip::<Audio> {
                track: clap_track,
                start: engine::tick::Tick(1000),
                end: engine::tick::Tick(clap_len.await?),
                asset_id: clap_asset,
            })
            .await?;

        let clap_out = engine.get(OutputSocketOf(clap_node, 0)).await;

        let master_sum_in1 = engine
            .call_mut(AddNodeInput::<Audio>::to(master_sum))
            .await?;

        engine
            .call_mut(AddLink {
                from: clap_out,
                to: master_sum_in1,
            })
            .await?;

        engine.publish(None).await;

        engine.move_playhead(engine::tick::Tick(0));
        engine.fire_mut(Play);
        println!("Playing... press enter to quit");
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf).unwrap();
        engine.fire_mut(TransportCmd(TransportState::Stopped));
        Ok::<_, anyhow::Error>(())
    }
}
