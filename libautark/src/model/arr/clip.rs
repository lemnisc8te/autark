use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slotmap::new_key_type;
use tokio::runtime::Builder;

use crate::{
    engine::{
        manager::{
            Handle,
            asset::{AssetActor, AssetTaskCarrier, GetAudioAsset},
            project::ProjectActor,
        },
        tick::Tick,
    },
    model::{
        Audio, Kind, RenderBlock, Renderable, Stored,
        asset::{AudioAsset, AudioAssetID, AudioAssetPayload},
        project::ProjectData,
    },
};

new_key_type! {
    pub struct AudioClipID;
}

pub trait Clip<K: Kind>: Sized + Serialize + DeserializeOwned {
    fn new(start: Tick, length: Tick, asset_id: <K::Asset as Stored>::Id) -> Self;

    fn start_mut(&mut self) -> &mut Tick;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AudioClip {
    pub start: Tick,
    pub length: Tick,
    pub asset_id: AudioAssetID,
}

impl Stored for AudioClip {
    type Id = AudioClipID;
    type Actor = ProjectActor;

    fn access(project: &ProjectData) -> &slotmap::SlotMap<Self::Id, Self> {
        &project.clips
    }

    fn access_mut(project: &mut ProjectData) -> &mut slotmap::SlotMap<Self::Id, Self> {
        &mut project.clips
    }
}

impl Clip<Audio> for AudioClip {
    fn new(start: Tick, length: Tick, asset_id: <<Audio as Kind>::Asset as Stored>::Id) -> Self {
        Self {
            start,
            length,
            asset_id,
        }
    }

    fn start_mut(&mut self) -> &mut Tick {
        &mut self.start
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedAudioClip {
    pub start: Tick,
    pub length: Tick,
    asset: AudioAsset,
}

impl ResolvedAudioClip {
    pub fn from_clip(clip: AudioClip, asset_h: Handle<AssetActor, AssetTaskCarrier>) -> Self {
        let rt = Builder::new_current_thread().build().unwrap();

        // Block the main thread until the future completes
        let asset = rt
            .block_on(async { asset_h.call(GetAudioAsset(clip.asset_id)).await })
            .unwrap();
        ResolvedAudioClip {
            start: clip.start,
            length: clip.length,
            asset,
        }
    }
}

type Type = usize;

impl Renderable for ResolvedAudioClip {
    fn render(
        &self,
        RenderBlock {
            buf,
            block_start,
            channels,
        }: &mut RenderBlock,
    ) {
        let block_len: Tick = (buf.len() / *channels as usize).into();

        let block_end = *block_start + block_len;

        match &self.asset.payload {
            AudioAssetPayload::Empty => todo!(),
            AudioAssetPayload::Resident(samples) => {
                let clip_end = self.start + self.length;
                let overlap_start = (*block_start).max(self.start);
                let overlap_end = block_end.min(clip_end);
                if overlap_start >= overlap_end {
                    panic!("eventually figure out what goes here");
                }
                for frame in (overlap_start.0)..overlap_end.0 {
                    let src_idx = ((frame - self.start.0) as usize) * self.asset.channels as usize;
                    let dst_idx = ((frame - block_start.0) as usize) * self.asset.channels as Type;
                    for ch in 0..*channels as usize {
                        let src_ch = ch.min(self.asset.channels as usize - 1);
                        if let (Some(&sample), Some(dest)) =
                            (samples.get(src_idx + src_ch), buf.get_mut(dst_idx + ch))
                        {
                            *dest += sample * self.asset.gain;
                        }
                    }
                }
            }
            AudioAssetPayload::Streaming => todo!(),
        };
    }
}
