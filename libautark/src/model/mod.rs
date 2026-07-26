use std::hash::Hash;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slotmap::{Key, SlotMap};

use crate::{
    engine::tick::Tick,
    model::{
        arr::{
            clip::{AudioClip, Clip},
            track::{AudioTrack, Track},
        },
        asset::AudioAsset,
        project::ProjectData,
    },
};

pub mod arr;
pub mod asset;
pub mod flow;
pub mod project;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataKind {
    Audio,
    Midi,
    Cv,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Audio;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Midi;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Cv;

pub trait Kind:
    std::fmt::Debug
    + Clone
    + Copy
    + Default
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + Hash
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + 'static
{
    type Asset: Stored;
    type Clip: Clip<Self> + Stored;
    type Track: Track<Self> + Stored;

    fn into_datakind() -> DataKind;
}

impl Kind for Audio {
    type Asset = AudioAsset;
    type Clip = AudioClip;
    type Track = AudioTrack;

    fn into_datakind() -> DataKind {
        DataKind::Audio
    }
}

impl DataKind {
    #[must_use]
    pub fn can_connect_to(self, dest: Self) -> bool {
        self == dest || (self == Self::Audio && dest == Self::Cv)
    }
}

pub trait Renderable {
    fn render(&self, proj: &ProjectData, buf: &mut [f32], block_start: Tick, channels: u16);
}

pub trait Stored: Sized {
    type Id: Key + Serialize + DeserializeOwned + Send + Sync + 'static;
    fn access(project: &ProjectData) -> &SlotMap<Self::Id, Self>;
    fn access_mut(project: &mut ProjectData) -> &mut SlotMap<Self::Id, Self>;
}
