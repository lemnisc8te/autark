//! Defines project data structure, [`Node`](flow::Node)s, and more.

use core::hash::Hash;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slotmap::{Key, SlotMap};

use crate::{
    engine::tick::Tick,
    model::{
        arr::{
            clip::{AudioClip, Clip},
            track::{AudioTrack, Track},
        },
        asset::{Asset, AudioAsset},
    },
};

pub mod arr;
pub mod asset;
pub mod flow;
pub mod project;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Defines the core 3 data formats.
pub enum DataKind {
    /// Audio data
    Audio,
    /// Midi data
    Midi,
    /// Control data
    /// Essentially `DataKind::Audio`, but isn't intended for listening/final output
    Cv,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
/// A `Kind`, used for audio data (see `DataKind::Audio`)
pub struct Audio;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
/// A `Kind`, used for MIDI data (see `DataKind::Midi`)
pub struct Midi;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
/// A `Kind`, used for control data (see `DataKind::Cv`)
pub struct Cv;

/// A trait parallel to the [`DataKind`] enum.
///
/// This is used to provide type-safe guarantees about the `Kind` of various objects.
pub trait Kind:
    core::fmt::Debug
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
    /// The `Asset` type of this `Kind`
    type Asset: Asset<Self> + Stored;
    /// The `Clip` type of this `Kind`
    type Clip: Clip<Self> + Stored<Storage = Self::Clip>;
    /// The `Track` type of this `Kind`
    type Track: Track<Self> + Stored<Storage = Self::Track>;

    /// Convert this `Kind` into its corresponding `DataKind` variant
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
    /// Defines permissible socket connections between Kinds
    pub fn can_connect_to(self, dest: Self) -> bool {
        self == dest || (self == Self::Audio && dest == Self::Cv)
    }
}

/// A block of audio.
pub struct RenderBlock<'b> {
    buf: &'b mut [f32],
    block_start: Tick,
    channels: u16,
}

/// A trait describing object that can be immediately rendered to audio.
pub(crate) trait Renderable: Send {
    /// Render this object into the `block`.
    fn render(&self, block: &mut RenderBlock);
}

/// [`Stored`] helps things (typically parameterized over [`Kind`]) held somewhere (typically an [`Actor`](crate::engine::Actor)) find out where they are. It makes it easier to get the corresponding type for each [`Kind`]'s [`Track`], [`Clip`], or [`Asset`]
pub trait Stored: Sized {
    /// The type of ID for this object. Must impl [`slotmap::Key`].
    type ID: Key + Serialize + DeserializeOwned + Send + Sync + 'static;
    /// The type of the location where this object's map is stored
    type Location;
    /// An optional wrapper around this object that is stored in the map
    type Storage;

    /// Get shared access to the [`SlotMap`] containing this object.
    fn access(loc: &Self::Location) -> &SlotMap<Self::ID, Self::Storage>;

    /// Get mutable access to the [`SlotMap`] containing this object.
    fn access_mut(loc: &mut Self::Location) -> &mut SlotMap<Self::ID, Self::Storage>;
}
