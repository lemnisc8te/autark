use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use slotmap::new_key_type;

use crate::{
    engine::manager::asset::AssetRegistry,
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
    pub samples: Arc<Vec<f32>>,
    pub gain: f32,
    pub sample_rate: u32,
    pub channels: u16,
    pub path: PathBuf,
}

impl Stored for AudioAsset {
    type Id = AudioAssetID;
    type Location = AssetRegistry;

    fn access(project: &Self::Location) -> &slotmap::SlotMap<Self::Id, Self> {
        &project.assets
    }

    fn access_mut(project: &mut Self::Location) -> &mut slotmap::SlotMap<Self::Id, Self> {
        &mut project.assets
    }
}

impl Asset<Audio> for AudioAsset {}
