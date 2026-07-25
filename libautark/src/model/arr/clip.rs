use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slotmap::{SlotMap, new_key_type};

use crate::{
    engine::tick::Tick,
    model::{
        Audio, Kind, Renderable, Stored,
        asset::AudioAssetID,
        project::{ProjectData, RtProjectData},
    },
};

new_key_type! {
    pub struct AudioClipID;
}

pub trait Clip<K: Kind>: Sized + Serialize + DeserializeOwned {
    fn new(start: Tick, length: Tick, asset_id: <K::Asset as Stored>::Id) -> Self;

    fn start_mut(&mut self) -> &mut Tick;

    fn parent(&self) -> Option<<K::Clip as Stored>::Id>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AudioClip {
    pub start: Tick,
    pub length: Tick,
    pub asset_id: AudioAssetID,
    pub parent: Option<AudioClipID>,
}

impl Stored for AudioClip {
    type Id = AudioClipID;

    fn access(project: &ProjectData) -> Arc<Mutex<SlotMap<Self::Id, Self>>> {
        project.clips.clone()
    }
}

impl Clip<Audio> for AudioClip {
    fn new(start: Tick, length: Tick, asset_id: <<Audio as Kind>::Asset as Stored>::Id) -> Self {
        Self {
            start,
            length,
            asset_id,
            parent: None,
        }
    }

    fn start_mut(&mut self) -> &mut Tick {
        &mut self.start
    }

    fn parent(&self) -> Option<<<Audio as Kind>::Clip as Stored>::Id> {
        self.parent
    }
}

impl Renderable for AudioClip {
    fn render(&self, proj: &RtProjectData, buf: &mut [f32], block_start: Tick, channels: u16) {
        let block_len: Tick = (buf.len() / channels as usize).into();

        let block_end = block_start + block_len;

        let Some(asset) = proj.assets.get(self.asset_id) else {
            panic!("invalid asset");
        };

        let clip_end = self.start + self.length;
        let overlap_start = block_start.max(self.start);
        let overlap_end = block_end.min(clip_end);
        if overlap_start >= overlap_end {
            panic!("eventually figure out what goes here");
        }
        for frame in (overlap_start.0)..overlap_end.0 {
            let src_idx = ((frame - self.start.0) as usize) * asset.channels as usize;
            let dst_idx = ((frame - block_start.0) as usize) * channels as usize;
            for ch in 0..channels as usize {
                let src_ch = ch.min(asset.channels as usize - 1);
                if let (Some(&sample), Some(dest)) = (
                    asset.samples.get(src_idx + src_ch),
                    buf.get_mut(dst_idx + ch),
                ) {
                    *dest += sample * asset.gain;
                }
            }
        }
    }
}
