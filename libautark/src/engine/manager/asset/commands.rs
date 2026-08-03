use super::AssetActor;
use anyhow::Result;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::sync::watch;

use crate::{
    engine::manager::{
        Command, HasHandle, Meta, MetaMutate, Mutate, Permission, Query,
        asset::{AssetRegistry, AssetSlot},
    },
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

pub struct SubscribeAudioAsset(pub AudioAssetID);

#[async_trait]
impl Command<Query> for SubscribeAudioAsset {
    type Output = watch::Receiver<AssetData<AudioAsset>>;

    type Actor = AssetActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.audio.get(self.0).unwrap().watch.subscribe()
    }
}

pub struct WaitForAudioAsset(pub AudioAssetID);

#[async_trait]
impl Command<Meta> for WaitForAudioAsset {
    type Output = Result<AudioAsset>;

    type Actor = AssetActor;

    async fn execute(self, actor: <Meta as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        let mut rx = actor.loopback.call(SubscribeAudioAsset(self.0)).await;

        rx.wait_for(|data| match data {
            AssetData::Ready(_) => true,
            AssetData::Failed => true,
            AssetData::Pending => false,
        })
        .await;

        // Extract the final value after the runtime finishes blocking
        match *rx.borrow() {
            AssetData::Ready(ref asset) => Ok(asset.clone()),
            AssetData::Failed => anyhow::bail!("asset {:?} failed to load", self.0),
            AssetData::Pending => unreachable!(),
        }
    }
}

pub struct LoadAudioAsset(pub PathBuf, pub u32);

#[async_trait]
impl Command<MetaMutate> for LoadAudioAsset {
    type Output = Result<AudioAssetID>;

    type Actor = AssetActor;

    async fn execute(
        self,
        actor: <MetaMutate as Permission<Self::Actor>>::Type<'_>,
    ) -> Self::Output {
        let new_key = actor.reg.audio.insert(AssetSlot::new(AssetData::Pending));

        let task = async move || AssetRegistry::create_audio_asset(self);
        let result = actor.reg.io_pool.execute(task);
        let _ = actor.handle().fire_mut(CompleteAudioAssetLoad {
            id: new_key,
            result: result.await,
        });
        Ok(new_key)
    }
}

pub struct CompleteAudioAssetLoad {
    id: AudioAssetID,
    result: Result<AudioAsset>,
}
#[async_trait]
impl Command<Mutate> for CompleteAudioAssetLoad {
    type Output = ();

    type Actor = AssetActor;

    async fn execute(self, actor: <Mutate as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        let v = actor.audio.get_mut(self.id).unwrap();
        let data_status = match self.result {
            Ok(asset) => AssetData::Ready(asset),
            Err(_) => AssetData::Failed,
        };
        v.watch.send_modify(|f| *f = data_status);
    }
}
