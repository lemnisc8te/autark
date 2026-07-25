use std::{collections::HashMap, sync::Arc};

use crate::{
    engine::{CompiledGraph, ScheduleStep, SlotIndex, errors::EngineError, tick::Tick},
    model::{
        DataKind, Kind, Stored,
        arr::{
            clip::{AudioClip, AudioClipID, Clip},
            track::{AudioTrack, AudioTrackID, Track},
        },
        asset::{AudioAsset, AudioAssetID},
        flow::{
            Node, NodeID,
            graph::NodeGraph,
            nodes::{master::Master, trackreader::TrackReader},
            socket::{Socket, SocketDirection, SocketID, SocketMeta},
        },
    },
};
use anyhow::Result;

use parking_lot::Mutex;
use slotmap::SlotMap;

#[derive(Debug, Clone)]
pub struct ProjectData {
    pub tracks: Arc<Mutex<SlotMap<AudioTrackID, AudioTrack>>>,
    pub clips: Arc<Mutex<SlotMap<AudioClipID, AudioClip>>>,
    pub assets: Arc<Mutex<SlotMap<AudioAssetID, AudioAsset>>>,
    pub graph: Arc<Mutex<NodeGraph>>,
    pub master_node_id: NodeID,
}

#[derive(Debug, Clone)]
pub struct RtProjectData {
    pub tracks: SlotMap<AudioTrackID, AudioTrack>,
    pub clips: SlotMap<AudioClipID, AudioClip>,
    pub assets: SlotMap<AudioAssetID, AudioAsset>,
    pub graph: NodeGraph,
    pub master_node_id: NodeID,
}

impl From<Arc<ProjectData>> for RtProjectData {
    fn from(value: Arc<ProjectData>) -> Self {
        Self {
            tracks: {
                let guard = value.tracks.lock();
                (*guard).clone()
            },
            clips: {
                let guard = value.clips.lock();
                (*guard).clone()
            },
            assets: {
                let guard = value.assets.lock();
                (*guard).clone()
            },
            graph: {
                let guard = value.graph.lock();
                (*guard).clone()
            },
            master_node_id: value.master_node_id,
        }
    }
}

impl ProjectData {
    pub fn new() -> Self {
        let mut graph = NodeGraph::default();
        let master_node = Master;
        let master_node_id = graph.add_node(master_node);
        Self {
            tracks: Arc::new(Mutex::new(SlotMap::with_key())),
            clips: Arc::new(Mutex::new(SlotMap::with_key())),
            assets: Arc::new(Mutex::new(SlotMap::with_key())),
            graph: Arc::new(Mutex::new(graph)),
            master_node_id,
        }
    }

    pub fn remove_link(&self, from: SocketID, to: SocketID) -> Result<()> {
        let mut graph = self.graph.lock();
        graph.remove_link(from, to)
    }

    pub fn add_link(&self, from_id: SocketID, to_id: SocketID) -> Result<Option<SocketID>> {
        let mut graph = self.graph.lock();
        graph.add_link(from_id, to_id)
    }

    pub fn move_clip<K: Kind>(
        &self,
        track: <K::Track as Stored>::Id,
        clip: <K::Clip as Stored>::Id,
        new_start: Tick,
    ) -> Result<()> {
        K::Track::mutate(self, |tracks| {
            let track = tracks.get_mut(track).ok_or(EngineError::TrackNotFound)?;
            track.clips_mut().retain(|_, &mut id| id != clip);
            track.clips_mut().insert(new_start, clip);
            Ok(())
        })?;

        K::Clip::mutate(self, |clips| {
            if let Some(c) = clips.get_mut(clip) {
                *c.start_mut() = new_start;
            }
            Ok(())
        })
    }

    pub fn add_clip_to_track<K: Kind>(
        &self,
        track: <K::Track as Stored>::Id,
        start: Tick,
        length: Tick,
        asset_id: <K::Asset as Stored>::Id,
    ) -> Result<<K::Clip as Stored>::Id> {
        let clip_id = K::Clip::mutate(self, |clips| {
            Ok(clips.insert(K::Clip::new(start, length, asset_id)))
        })?;

        K::Track::mutate(self, |tracks| {
            let track = tracks.get_mut(track).ok_or(EngineError::TrackNotFound)?;
            track.clips_mut().insert(start, clip_id);
            Ok(clip_id)
        })
    }

    pub fn remove_track<K: Kind>(&self, track_id: <K::Track as Stored>::Id) -> Result<()> {
        let tracks = K::Track::access(self);
        let mut tracks = tracks.lock();
        let track = tracks.remove(track_id).ok_or(EngineError::TrackNotFound)?;
        let linked_id = track
            .linked_node_id()
            .expect("Track was orphaned from node");
        drop(tracks);

        let mut graph = self.graph.lock();
        graph.purge(linked_id);
        drop(graph);

        let clips = K::Clip::access(self);
        let mut clips = clips.lock();
        for clip_id in track.clips().values() {
            clips.remove(*clip_id);
        }
        Ok(())
    }

    pub fn add_track<K: Kind>(
        &self,
        name: String,
        channels: u16,
    ) -> Result<(<K::Track as Stored>::Id, NodeID)>
    where
        TrackReader<K>: Node,
    {
        let track_id = K::Track::mutate(self, |tracks| Ok(tracks.insert(K::Track::new(name))))?;
        let reader_node = TrackReader::<K>::new(track_id, channels);
        let node_id = self.graph.lock().add_node(reader_node);
        K::Track::mutate(self, |tracks| {
            let track = &mut tracks[track_id];
            *track.linked_node_id_mut() = Some(node_id);
            Ok((track_id, node_id))
        })
    }

    pub fn add_socket_to_node(&self, node_id: NodeID, socket: Socket) -> Result<SocketID> {
        let mut graph = self.graph.lock();
        let id = graph.sockets.insert(SocketMeta {
            owner: node_id,
            direction: SocketDirection::Input,
            kind: socket.kind,
            name: socket.name,
            visible: socket.visible,
        });
        graph.node_sockets[node_id].0.push(id);
        Ok(id)
    }

    pub fn remove_node_input(&self, node_id: NodeID) -> Result<()> {
        todo!()
    }

    pub fn socket_kind_of(&self, endpoint: SocketID) -> Result<DataKind> {
        let graph = self.graph.lock();
        graph
            .sockets
            .get(endpoint)
            .map(|s| s.kind)
            .ok_or(EngineError::SocketNotFound(endpoint).into())
    }

    pub fn compile_graph(&self) -> Result<CompiledGraph> {
        let graph = self.graph.lock();
        let order = graph.topo_sort()?;

        let mut socket_slot: HashMap<SocketID, SlotIndex> = HashMap::new();
        let mut buffer_count = 1usize; // slot 0 reserved for silence

        for &node_id in &order {
            for &out_id in graph.outputs_of(node_id) {
                socket_slot.insert(out_id, buffer_count);
                buffer_count += 1;
            }
        }

        let mut steps = Vec::with_capacity(order.len());
        for &node_id in &order {
            let input_ids = graph.inputs_of(node_id);
            let output_ids = graph.outputs_of(node_id);

            let input_slots: Vec<SlotIndex> = input_ids
                .iter()
                .map(|&in_id| {
                    graph
                        .links
                        .get(in_id) // O(1) lookup — no per-socket Vec to build anymore
                        .and_then(|src| socket_slot.get(src))
                        .copied()
                        .unwrap_or(0) // unconnected -> reserved silence slot
                })
                .collect();

            let output_slots: Vec<SlotIndex> = output_ids
                .iter()
                .map(|&out_id| socket_slot[&out_id])
                .collect();

            steps.push(ScheduleStep {
                node_id,
                input_slots,
                output_slots,
            });
        }

        let master_output_slot = graph
            .node_sockets
            .get(self.master_node_id)
            .and_then(|(_, outs)| outs.first())
            .and_then(|&id| socket_slot.get(&id))
            .copied()
            .ok_or(EngineError::NodeNotFound(self.master_node_id))?;

        Ok(CompiledGraph {
            steps,
            buffer_count,
            master_output_slot,
        })
    }
}

impl Default for ProjectData {
    fn default() -> Self {
        Self::new()
    }
}
