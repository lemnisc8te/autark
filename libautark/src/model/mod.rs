use std::hash::Hash;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slotmap::{Key, SlotMap};

use crate::{
    engine::{manager::Actor, tick::Tick},
    model::{
        arr::{
            clip::{AudioClip, Clip},
            track::{AudioTrack, Track},
        },
        asset::AudioAsset,
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

pub struct RenderBlock<'b> {
    pub buf: &'b mut [f32],
    pub block_start: Tick,
    pub channels: u16,
}

pub trait Renderable: Send {
    fn render(&self, block: &mut RenderBlock);
}

pub trait Stored: Sized {
    type Id: Key + Serialize + DeserializeOwned + Send + 'static;
    type Actor: Actor;
    // fn access<'a>(loc: <Ref as Permission<Self::Actor>>::Type<'a>) -> &'a SlotMap<Self::Id, Self>;

    fn access(loc: &<Self::Actor as Actor>::Data) -> &SlotMap<Self::Id, Self>;
    fn access_mut(loc: &mut <Self::Actor as Actor>::Data) -> &mut SlotMap<Self::Id, Self>;
    // fn access_mut<'a>(
    //     loc: <Mutate as Permission<Self::Actor>>::Type<'a>,
    // ) -> &'a mut SlotMap<Self::Id, Self>;
}
