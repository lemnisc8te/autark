use super::AssetActor;
use anyhow::Result;
use std::path::PathBuf;

use crate::model::asset::AudioAsset;

use crate::engine::manager::{Command, Mutate, Permission, Query};

use crate::model::asset::AudioAssetID;

pub struct AudioAssetFromID(pub AudioAssetID);

impl Command<Query> for AudioAssetFromID {
    type Output = Option<AudioAsset>;

    type Actor = AssetActor;

    fn execute(self, actor: <Query as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.audio.get(self.0).cloned()
    }
}

pub struct LoadAudioAsset(pub PathBuf, pub u32);

impl Command<Mutate> for LoadAudioAsset {
    type Output = Result<AudioAssetID>;

    type Actor = AssetActor;

    fn execute(self, actor: <Mutate as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.load_audio_asset(self.0, self.1)
    }
}
