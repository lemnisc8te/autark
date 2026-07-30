#[forbid(
    unused_unsafe,
    clippy::fallible_impl_from,
    // clippy::used_underscore_binding,
    clippy::used_underscore_items,
    clippy::undocumented_unsafe_blocks
)]
#[deny(
    unreachable_pub,
    unused_qualifications,
    // clippy::pedantic,
    clippy::cargo,
    // clippy::nursery,
    clippy::perf,
    clippy::correctness,
    clippy::suspicious,
    clippy::complexity,
    // clippy::style,
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
    // clippy::panic,
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

// use assert_no_alloc::*;

// #[cfg(debug_assertions)] // required when disable_release is set (default)
// #[global_allocator]
// static A: AllocDisabler = AllocDisabler;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Audio;
    use crate::model::flow::nodes::biquad_filter::BiquadFilter;
    use crate::model::flow::nodes::sum::Sum;
    use crate::model::project::ProjectData;
    use anyhow::Result;

    use std::sync::Arc;

    use engine::Engine;
    #[test]
    fn it_works() {
        helper().unwrap();
    }

    fn helper() -> Result<()> {
        let mut engine = {
            let project = Arc::new(ProjectData::new());
            Engine::new(project)?
        };
        let master_node_id = engine.project().master_node_id;

        let master_in = engine.project().graph.inputs_of(master_node_id)[0];

        let song_asset = engine.load_asset("./assets/AUDIO_4892.mp3")?;

        let song_len = {
            let asset = &engine.project().assets[song_asset];
            asset.samples.len() as u64 / u64::from(asset.channels)
        };

        let filter1 = engine.apply(AddNode {
            node: BiquadFilter::new(
                engine.channels(),
                model::flow::nodes::biquad_filter::FilterType::HighPass,
                engine.sample_rate(),
                1600.0,
                BiquadFilter::BUTTERWORTH_Q,
                0.0,
            ),
        })?;

        let filter1_in = engine.project().graph.inputs_of(filter1)[0];
        let filter1_out = engine.project().graph.outputs_of(filter1)[0];

        let master_sum = engine.apply(AddNode {
            node: Sum::<Audio>::new(),
        })?;

        let master_sum_in0 = engine.apply(AddNodeInput::<Audio>::new(master_sum))?;

        let master_sum_out = engine.project().graph.outputs_of(master_sum)[0];

        engine.apply(AddLink {
            from: filter1_out,
            to: master_sum_in0,
        })?;

        engine.apply(AddLink {
            from: master_sum_out,
            to: master_in,
        })?;

        let (song_track, song_node) = engine.apply(AddTrack {
            name: "Song".to_string(),
            kind: Audio,
            channels: engine.channels(),
        })?;

        let song_out = engine.project().graph.outputs_of(song_node)[0];

        engine.apply(AddLink {
            from: song_out,
            to: filter1_in,
        })?;

        engine.apply(AddClip::<Audio> {
            track: song_track,
            start: engine::tick::Tick(0),
            end: engine::tick::Tick(song_len),
            asset_id: song_asset,
        })?;

        let clap_asset = engine.load_asset("./assets/clap.mp3")?;

        let clap_len = {
            let asset = &engine.project().assets[clap_asset];
            asset.samples.len() as u64 / u64::from(asset.channels)
        };

        let (clap_track, clap_node) = engine.apply(AddTrack {
            name: "Clap".to_string(),
            kind: Audio,
            channels: engine.channels(),
        })?;

        engine.apply(AddClip::<Audio> {
            track: clap_track,
            start: engine::tick::Tick(1000),
            end: engine::tick::Tick(clap_len),
            asset_id: clap_asset,
        })?;

        let clap_out = engine.project().graph.outputs_of(clap_node)[0];

        let master_sum_in1 = engine.apply(AddNodeInput::<Audio>::new(master_sum))?;

        engine.apply(AddLink {
            from: clap_out,
            to: master_sum_in1,
        })?;

        engine.move_playhead(engine::tick::Tick(0))?;

        engine.transport.play();
        println!("Playing... press enter to quit");
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        engine.transport.stop();
        Ok(())
    }
}
