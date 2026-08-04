use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use slotmap::new_key_type;

use crate::{
    engine::{manager::project::ProjectActor, tick::Tick},
    model::{Audio, Kind, Stored, arr::clip::AudioClipID, flow::NodeID, project::ProjectData},
};

new_key_type! {
   pub struct AudioTrackID;
}

pub struct Subregion<K: Kind> {
    pub start: Tick,
    pub len: Tick,
    pub variations: Vec<BTreeMap<Tick, <K::Clip as Stored>::ID>>,
    pub current: usize,
}

pub trait Track<K: Kind> {
    fn name(&self) -> &str;
    fn clips(&self) -> &BTreeMap<Tick, <K::Clip as Stored>::ID>;
    fn clips_mut(&mut self) -> &mut BTreeMap<Tick, <K::Clip as Stored>::ID>;
    fn linked_node_id(&self) -> Option<NodeID>;
    fn linked_node_id_mut(&mut self) -> &mut Option<NodeID>;
    fn new(name: impl Into<String>) -> Self;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioTrack {
    pub name: String,
    pub clips: BTreeMap<Tick, AudioClipID>,
    pub gain: f32,
    pub linked_node_id: Option<NodeID>,
}

impl Stored for AudioTrack {
    type ID = AudioTrackID;
    type Data = ProjectData;
    type Storage = Self;

    fn access(loc: &ProjectData) -> &slotmap::SlotMap<Self::ID, Self> {
        &loc.tracks
    }

    fn access_mut(loc: &mut ProjectData) -> &mut slotmap::SlotMap<Self::ID, Self> {
        &mut loc.tracks
    }
}

impl Track<Audio> for AudioTrack {
    fn name(&self) -> &str {
        &self.name
    }

    fn clips(&self) -> &BTreeMap<Tick, <<Audio as Kind>::Clip as Stored>::ID> {
        &self.clips
    }

    fn clips_mut(&mut self) -> &mut BTreeMap<Tick, <<Audio as Kind>::Clip as Stored>::ID> {
        &mut self.clips
    }

    fn linked_node_id(&self) -> Option<NodeID> {
        self.linked_node_id
    }

    fn linked_node_id_mut(&mut self) -> &mut Option<NodeID> {
        &mut self.linked_node_id
    }

    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            clips: Default::default(),
            gain: 1.0,
            linked_node_id: None,
        }
    }
}
