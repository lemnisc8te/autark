use crate::{
    engine::{
        manager::{
            Handle,
            asset::{
                AssetActor,
                commands::{SubscribeAudioAsset, WaitForAudioAsset},
            },
            project::ProjectActor,
        },
        tick::Tick,
    },
    model::{
        Audio, Kind, RenderBlock, Renderable, Stored,
        asset::{AssetData, AudioAsset, AudioAssetID, AudioAssetPayload},
        project::ProjectData,
    },
};
use anyhow::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slotmap::new_key_type;
use tokio::runtime::Builder;

new_key_type! {
    pub struct AudioClipID;
}

pub trait Clip<K: Kind>: Sized + Serialize + DeserializeOwned {
    fn new(start: Tick, length: Tick, asset_id: <K::Asset as Stored>::ID) -> Self;

    fn start_mut(&mut self) -> &mut Tick;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AudioClip {
    pub start: Tick,
    pub length: Tick,
    pub asset_id: AudioAssetID,
}

impl Stored for AudioClip {
    type ID = AudioClipID;
    type Actor = ProjectActor;
    type Storage = Self;

    fn access(project: &ProjectData) -> &slotmap::SlotMap<Self::ID, Self> {
        &project.clips
    }

    fn access_mut(project: &mut ProjectData) -> &mut slotmap::SlotMap<Self::ID, Self> {
        &mut project.clips
    }
}

impl Clip<Audio> for AudioClip {
    fn new(start: Tick, length: Tick, asset_id: <<Audio as Kind>::Asset as Stored>::ID) -> Self {
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
    pub async fn from_clip(clip: AudioClip, asset_h: Handle<AssetActor>) -> Result<Self> {
        let asset = asset_h.call(WaitForAudioAsset(clip.asset_id)).await?;
        // Extract the final value after the runtime finishes blocking
        Ok(Self {
            start: clip.start,
            length: clip.length,
            asset: asset.clone(), // Assumes your asset type implements Clone
        })
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
