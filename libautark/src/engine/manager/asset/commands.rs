use super::AssetActor;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::watch;

use crate::{
    engine::manager::{Command, Operate, asset::AssetSlot},
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

pub struct SubscribeAudioAsset(pub AudioAssetID);

// #[async_trait]
impl Command for SubscribeAudioAsset {
    type Output = watch::Receiver<AssetData<AudioAsset>>;

    type Actor = AssetActor;

    async fn execute(self, actor: &AssetActor) -> Self::Output {
        dbg!("subscribing");
        actor
            .query(async |reg| reg.audio.get(self.0).unwrap().watch.subscribe())
            .await
    }
}

pub struct WaitForAudioAsset(pub AudioAssetID);

// #[async_trait]
impl Command for WaitForAudioAsset {
    type Output = Result<AudioAsset>;

    type Actor = AssetActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        actor
            .query(async |reg| {
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
            .await
    }
}

pub struct LoadAudioAsset(pub PathBuf, pub u32);

// #[async_trait]
impl Command for LoadAudioAsset {
    type Output = Result<AudioAssetID>;

    type Actor = AssetActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        actor
            .mutate(async |reg| {
                let new_key = reg.audio.insert(AssetSlot::new(AssetData::Pending));
                // let handle = actor.handle().clone();
                // let key_clone = new_key;
                // let task = move || {
                //     dbg!("in task");
                //     let result = AssetRegistry::create_audio_asset(self);
                //     handle.fire_mut(CompleteAudioAssetLoad {
                //         id: key_clone,
                //         result,
                //     });
                //     dbg!("Sent completion update");
                // };
                // actor.reg.io_pool.execute(task);
                // dbg!("Executed task");
                Ok(new_key)
            })
            .await
    }
}

pub struct CompleteAudioAssetLoad {
    id: AudioAssetID,
    result: Result<AudioAsset>,
}
// #[async_trait]
impl Command for CompleteAudioAssetLoad {
    type Output = ();

    type Actor = AssetActor;

    async fn execute(self, actor: &AssetActor) -> Self::Output {
        dbg!("Completing load");
        actor
            .mutate(async |reg| {
                let slot = reg.audio.get_mut(self.id).unwrap();
                let data_status = match self.result {
                    Ok(asset) => AssetData::Ready(asset),
                    Err(_) => AssetData::Failed,
                };
                slot.watch.send(data_status);
                dbg!("Completed load");
            })
            .await
    }
}
