use super::AssetActor;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::watch;

use crate::{
    engine::manager::{
        Command, Modify, Permission, Query,
        asset::{AssetRegistry, AssetSlot},
    },
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

pub struct SubscribeAudioAsset(pub AudioAssetID);

impl Command<Query> for SubscribeAudioAsset {
    type Output = watch::Receiver<AssetData<AudioAsset>>;
    type Actor = AssetActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor.reg.audio.get(self.0).unwrap().watch.subscribe()
    }
}

pub struct WaitForAudioAsset(pub AudioAssetID);

impl Command<Query> for WaitForAudioAsset {
    type Output = Result<AudioAsset>;
    type Actor = AssetActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Guard) -> Self::Output {
        let mut rx = actor.reg.audio.get(self.0).unwrap().watch.subscribe();
        rx.wait_for(|data| match data {
            AssetData::Ready(_) | AssetData::Failed => true,
            AssetData::Pending => false,
        })
        .await?;

        // Extract the final value after the runtime finishes blocking
        match *rx.borrow() {
            AssetData::Ready(ref asset) => Ok(asset.clone()),
            AssetData::Failed => anyhow::bail!("asset {:?} failed to load", self.0),
            AssetData::Pending => unreachable!(),
        }
    }
}

pub struct LoadAudioAsset(pub PathBuf, pub u32);

impl Command<Modify> for LoadAudioAsset {
    type Output = Result<AudioAssetID>;
    type Actor = AssetActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        let new_key = actor.reg.audio.insert(AssetSlot::new(AssetData::Pending));

        let result =
            tokio::task::spawn_blocking(|| AssetRegistry::create_audio_asset(self)).await?;
        {
            let slot = actor.reg.audio.get_mut(new_key).unwrap();
            let data_status = match result {
                Ok(asset) => AssetData::Ready(asset),
                Err(_) => AssetData::Failed,
            };
            slot.watch.send_modify(|status| *status = data_status);
        };

        Ok(new_key)
    }
}
