use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use slotmap::new_key_type;

use crate::{
    engine::tick::Tick,
    model::{
        Audio, Kind, Renderable, Stored,
        arr::clip::AudioClipID,
        flow::NodeID,
        project::{ProjectData, RtProjectData},
    },
};

new_key_type! {
   pub struct AudioTrackID;
}

pub trait Track<K: Kind> {
    fn name(&self) -> &str;
    fn clips(&self) -> &BTreeMap<Tick, <K::Clip as Stored>::Id>;
    fn clips_mut(&mut self) -> &mut BTreeMap<Tick, <K::Clip as Stored>::Id>;
    fn linked_node_id(&self) -> Option<NodeID>;
    fn linked_node_id_mut(&mut self) -> &mut Option<NodeID>;
    fn new(name: impl Into<String>) -> Self;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioTrack {
    pub name: String,
    pub clips: BTreeMap<Tick, AudioClipID>,
    pub gain: f32,
    pub linked_node_id: Option<NodeID>,
}

impl Stored for AudioTrack {
    type Id = AudioTrackID;

    fn access(project: &ProjectData) -> Arc<Mutex<slotmap::SlotMap<Self::Id, Self>>> {
        project.tracks.clone()
    }
}

impl Track<Audio> for AudioTrack {
    fn name(&self) -> &str {
        &self.name
    }

    fn clips(&self) -> &BTreeMap<Tick, <<Audio as Kind>::Clip as Stored>::Id> {
        &self.clips
    }

    fn clips_mut(&mut self) -> &mut BTreeMap<Tick, <<Audio as Kind>::Clip as Stored>::Id> {
        &mut self.clips
    }

    fn linked_node_id(&self) -> Option<NodeID> {
        self.linked_node_id
    }

    fn linked_node_id_mut(&mut self) -> &mut Option<NodeID> {
        &mut self.linked_node_id
    }

    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            clips: Default::default(),
            gain: 1.0,
            linked_node_id: None,
        }
    }
}

impl Renderable for AudioTrack {
    fn render(&self, proj: &RtProjectData, buf: &mut [f32], block_start: Tick, channels: u16) {
        // Deinterleave
        let block_len: Tick = (buf.len() / channels as usize).into();
        let block_end = block_start + block_len;

        let lookback = self
            .clips
            .range(..block_start)
            .next_back()
            .map(|(_, id)| *id)
            .filter(|id| {
                proj.clips
                    .get(*id)
                    .is_some_and(|c| c.start + c.length > block_start)
            });
        let active = lookback
            .into_iter()
            .chain(self.clips.range(block_start..block_end).map(|(_, id)| *id));

        for clip_id in active {
            let Some(clip) = proj.clips.get(clip_id) else {
                panic!("Invalid clip");
            };
            clip.render(proj, buf, block_start, channels);
        }
    }
}
