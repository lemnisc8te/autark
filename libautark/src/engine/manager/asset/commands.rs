//! Implementors of [`Command`] operating on an [`AssetActor`]

use super::AssetActor;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::watch;

use crate::{
    engine::manager::{
        Command, Permission, Read, Write,
        asset::{AssetRegistry, AssetSlot},
    },
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

/// Subscribe to an [`AudioAsset`], receiving a `[tokio::sync::watch]` channel that can be awaited to recieve the asset once it has finished loading.
pub struct SubscribeAudioAsset(pub AudioAssetID);

impl Command<Read> for SubscribeAudioAsset {
    type Output = watch::Receiver<AssetData<AudioAsset>>;
    type Actor = AssetActor;

    async fn execute(self, actor: <Read as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor.reg.audio.get(self.0).unwrap().watch.subscribe()
    }
}

/// Asynchronously wait for an [`AudioAsset`] to finish loading.
pub struct WaitForAudioAsset(pub AudioAssetID);

impl Command<Read> for WaitForAudioAsset {
    type Output = Result<AudioAsset>;
    type Actor = AssetActor;

    async fn execute(self, actor: <Read as Permission<Self::Actor>>::Guard) -> Self::Output {
        let mut rx = actor.reg.audio.get(self.0).unwrap().watch.subscribe();
        rx.wait_for(|data| match data {
            AssetData::Ready(_) | AssetData::Failed(_) => true,
            AssetData::Pending => false,
        })
        .await?;

        // Extract the final value after the runtime finishes blocking
        match *rx.borrow() {
            AssetData::Ready(ref asset) => Ok(asset.clone()),
            AssetData::Failed(ref err) => anyhow::bail!("asset {:?} failed to load: {err}", self.0),
            AssetData::Pending => unreachable!(),
        }
    }
}

/// Spawn a blocking task to load an audio asset.
///
/// Immediately returns an [`AudioAssetID`] that can be used with [`WaitForAudioAsset`] to get the `[AudioAsset]` itself.
pub struct LoadAudioAsset(pub PathBuf, pub u32);

impl Command<Write> for LoadAudioAsset {
    type Output = Result<AudioAssetID>;
    type Actor = AssetActor;

    async fn execute(self, mut actor: <Write as Permission<Self::Actor>>::Guard) -> Self::Output {
        let new_key = actor.reg.audio.insert(AssetSlot::new(AssetData::Pending));

        let result =
            tokio::task::spawn_blocking(|| AssetRegistry::create_audio_asset(self)).await?;
        {
            let slot = actor.reg.audio.get_mut(new_key).unwrap();
            let data_status = match result {
                Ok(asset) => AssetData::Ready(asset),
                Err(err) => AssetData::Failed(err),
            };
            slot.watch.send_modify(|status| *status = data_status);
        };

        Ok(new_key)
    }
}
