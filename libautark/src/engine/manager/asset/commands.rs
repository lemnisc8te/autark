use super::AssetActor;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::watch;

use crate::{
    engine::manager::{
        Command, Operate,
        asset::{AssetRegistry, AssetSlot},
    },
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

pub struct SubscribeAudioAsset(pub AudioAssetID);

impl Command for SubscribeAudioAsset {
    type Output = watch::Receiver<AssetData<AudioAsset>>;

    type Actor = AssetActor;

    fn execute(self, actor: &AssetActor) -> impl Future<Output = Self::Output> + Send {
        actor.query(async move |reg| reg.audio.get(self.0).unwrap().watch.subscribe())
    }
}

pub struct WaitForAudioAsset(pub AudioAssetID);

impl Command for WaitForAudioAsset {
    type Output = Result<AudioAsset>;

    type Actor = AssetActor;

    fn execute(self, actor: &Self::Actor) -> impl Future<Output = Self::Output> {
        actor.query(async move |reg| {
            let mut rx = reg.audio.get(self.0).unwrap().watch.subscribe();
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
        })
    }
}

pub struct LoadAudioAsset(pub PathBuf, pub u32);

impl Command for LoadAudioAsset {
    type Output = Result<AudioAssetID>;

    type Actor = AssetActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        let new_key = actor
            .mutate(async |reg| reg.audio.insert(AssetSlot::new(AssetData::Pending)))
            .await;

        let result =
            tokio::task::spawn_blocking(|| AssetRegistry::create_audio_asset(self)).await?;
        actor
            .mutate(async |reg| {
                let slot = reg.audio.get_mut(new_key).unwrap();
                let data_status = match result {
                    Ok(asset) => AssetData::Ready(asset),
                    Err(_) => AssetData::Failed,
                };
                slot.watch.send_modify(|status| *status = data_status);
            })
            .await;

        Ok(new_key)
    }
}
