use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use slotmap::new_key_type;

use crate::{
    engine::manager::asset::AssetActor,
    model::{Audio, Kind, Stored},
};

new_key_type! {
    pub struct AudioAssetID;
}

pub trait Asset<K: Kind> {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AssetData<Data> {
    Pending,
    Ready(Data),
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioAsset {
    #[serde(skip)]
    pub payload: AudioAssetPayload,
    pub gain: f32,
    pub sample_rate: u32,
    pub channels: u16,
    pub path: PathBuf,
    pub len: usize,
}

#[derive(Debug, Clone, Default)]
pub enum AudioAssetPayload {
    #[default]
    Empty,
    Resident(Arc<[f32]>),
    Streaming,
}

impl Stored for AudioAsset {
    type Id = AudioAssetID;
    type Actor = AssetActor;

    fn access(
        loc: &<Self::Actor as crate::engine::manager::Actor>::Data,
    ) -> &slotmap::SlotMap<Self::Id, Self> {
        &loc.audio
    }

    fn access_mut(
        loc: &mut <Self::Actor as crate::engine::manager::Actor>::Data,
    ) -> &mut slotmap::SlotMap<Self::Id, Self> {
        &mut loc.audio
    }
}

impl Asset<Audio> for AudioAsset {}
