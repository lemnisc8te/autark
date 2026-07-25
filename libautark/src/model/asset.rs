use std::{path::PathBuf, sync::Arc};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use slotmap::{SlotMap, new_key_type};

use crate::model::{Audio, Kind, Stored, project::ProjectData};

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

    fn access(project: &ProjectData) -> Arc<Mutex<SlotMap<Self::Id, Self>>> {
        project.assets.clone()
    }
}

impl Asset<Audio> for AudioAsset {}
