use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use slotmap::new_key_type;

use crate::{
    engine::manager::asset::{AssetActor, AssetRegistry, AudioAssetSlot},
    model::{Audio, Kind, Stored},
};

new_key_type! {
    pub struct AudioAssetID;
}

pub trait Asset<K: Kind> {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AssetData<Data: Clone> {
    Pending,
    Ready(Data),
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioAsset {
    #[serde(skip)]
    pub payload: AudioAssetPayload,
    pub gain: f32,
    pub sample_rate: u32,
    pub channels: u16,
    pub path: PathBuf,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub enum AudioAssetPayload {
    Resident(Arc<[f32]>),
    Streaming,
}

impl Stored for AudioAsset {
    type ID = AudioAssetID;
    type Data = AssetRegistry;
    type Storage = AudioAssetSlot;

    fn access(loc: &AssetRegistry) -> &slotmap::SlotMap<Self::ID, Self::Storage> {
        &loc.audio
    }

    fn access_mut(loc: &mut AssetRegistry) -> &mut slotmap::SlotMap<Self::ID, Self::Storage> {
        &mut loc.audio
    }
}

impl Asset<Audio> for AudioAsset {}
