use super::AssetActor;
use anyhow::Result;
use kameo::message::{Context, Message};
use std::path::PathBuf;
use tokio::sync::watch;

use crate::{
    engine::manager::asset::{AssetRegistry, AssetSlot},
    model::asset::{AssetData, AudioAsset, AudioAssetID},
};

type WatchSlot = watch::Receiver<AssetData<AudioAsset>>;

pub struct SubscribeAudioAsset(pub AudioAssetID);

impl Message<SubscribeAudioAsset> for AssetActor {
    type Reply = WatchSlot;

    async fn handle(
        &mut self,
        msg: SubscribeAudioAsset,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.reg.audio.get(msg.0).unwrap().watch.subscribe()
    }
}

type AudioAssetResult = Result<AudioAsset>;

pub struct WaitForAudioAsset(pub AudioAssetID);

impl Message<WaitForAudioAsset> for AssetActor {
    type Reply = Result<AudioAsset>;
    async fn handle(
        &mut self,
        msg: WaitForAudioAsset,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut rx = self.reg.audio.get(msg.0).unwrap().watch.subscribe();
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
            AssetData::Failed => anyhow::bail!("asset {:?} failed to load", msg.0),
            AssetData::Pending => unreachable!(),
        }
    }
}

pub struct LoadAudioAsset(pub PathBuf, pub u32);

impl Message<LoadAudioAsset> for AssetActor {
    type Reply = Result<AudioAssetID>;

    async fn handle(
        &mut self,
        msg: LoadAudioAsset,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let new_key = self.reg.audio.insert(AssetSlot::new(AssetData::Pending));
        let loopback = self.loopback.clone();
        let key_clone = new_key;
        let task = move || {
            dbg!("in task");
            let result = AssetRegistry::create_audio_asset(msg);
            loopback.ask(CompleteAudioAssetLoad {
                id: key_clone,
                result,
            });
            dbg!("Sent completion update");
        };
        self.reg.io_pool.execute(task);
        dbg!("Executed task");
        Ok(new_key)
    }
}

pub struct CompleteAudioAssetLoad {
    id: AudioAssetID,
    result: Result<AudioAsset>,
}

impl Message<CompleteAudioAssetLoad> for AssetActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: CompleteAudioAssetLoad,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        dbg!("Completing load");
        let slot = self.reg.audio.get_mut(msg.id).unwrap();
        let data_status = match msg.result {
            Ok(asset) => AssetData::Ready(asset),
            Err(_) => AssetData::Failed,
        };
        slot.watch.send(data_status);
        dbg!("Completed load");
    }
}
