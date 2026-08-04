use super::AssetActor;
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::watch;

use crate::{
    engine::manager::{
        Command, Operate,
        asset::{AssetRegistry, AssetSlot},
    },
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

pub struct SubscribeAudioAsset(pub AudioAssetID);

// #[async_trait]
impl Command for SubscribeAudioAsset {
    type Output = watch::Receiver<AssetData<AudioAsset>>;

    type Actor = AssetActor;

    fn execute(self, actor: Arc<AssetActor>) -> impl Future<Output = Self::Output> + Send {
        dbg!("subscribing");
        actor.query(async move |reg| reg.audio.get(self.0).unwrap().watch.subscribe())
    }
}

pub struct WaitForAudioAsset(pub AudioAssetID);

impl Command for WaitForAudioAsset {
    type Output = Result<AudioAsset>;

    type Actor = AssetActor;

    fn execute(self, actor: Arc<Self::Actor>) -> impl Future<Output = Self::Output> {
        actor.query(async |reg| {
            let mut rx = reg.audio.get(self.0).unwrap().watch.subscribe();
            dbg!("waiting");
            rx.wait_for(|data| match data {
                AssetData::Ready(_) | AssetData::Failed => true,
                AssetData::Pending => false,
            })
            .await?;
            dbg!("updated");

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

// #[async_trait]
impl Command for LoadAudioAsset {
    type Output = Result<AudioAssetID>;

    type Actor = AssetActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        let new_key = actor
            .mutate(async |reg| reg.audio.insert(AssetSlot::new(AssetData::Pending)))
            .await;

        let result =
            tokio::task::spawn_blocking(async || AssetRegistry::create_audio_asset(self)).await?;
        actor.mutate(async |reg| {
            let slot = reg.audio.get_mut(new_key).unwrap();
            let data_status = match result.await {
                Ok(asset) => AssetData::Ready(asset),
                Err(_) => AssetData::Failed,
            };
            slot.watch.send(data_status);
            dbg!("Completed load");
        });
        Ok(new_key)
    }
}
