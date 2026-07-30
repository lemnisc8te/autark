use std::collections::HashMap;

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

use slotmap::SlotMap;

#[derive(Debug, Clone)]
pub struct ProjectData {
    pub tracks: SlotMap<AudioTrackID, AudioTrack>,
    pub clips: SlotMap<AudioClipID, AudioClip>,
    pub graph: NodeGraph,
    pub master_node_id: NodeID,
}

impl ProjectData {
    pub fn new() -> Self {
        let mut graph = NodeGraph::default();

        let master_node = Master;
        let master_node_id = graph.add_node(master_node);
        Self {
            tracks: SlotMap::with_key(),
            clips: SlotMap::with_key(),
            assets: SlotMap::with_key(),
            graph,
            master_node_id,
        }
    }

    pub fn remove_link(&mut self, from: SocketID, to: SocketID) -> Result<()> {
        self.graph.remove_link(from, to)
    }

    pub fn add_link(&mut self, from_id: SocketID, to_id: SocketID) -> Result<Option<SocketID>> {
        self.graph.add_link(from_id, to_id)
    }

    pub fn move_clip<K: Kind>(
        &mut self,
        track: <K::Track as Stored>::Id,
        clip: <K::Clip as Stored>::Id,
        new_start: Tick,
    ) -> Result<()> {
        let track = K::Track::access_mut(self)
            .get_mut(track)
            .ok_or(EngineError::TrackNotFound)?;
        track.clips_mut().retain(|_, &mut id| id != clip);
        track.clips_mut().insert(new_start, clip);
        if let Some(c) = K::Clip::access_mut(self).get_mut(clip) {
            *c.start_mut() = new_start;
        }
        Ok(())
    }

    pub fn add_clip_to_track<K: Kind>(
        &mut self,
        track: <K::Track as Stored>::Id,
        start: Tick,
        length: Tick,
        asset_id: <K::Asset as Stored>::Id,
    ) -> Result<<K::Clip as Stored>::Id> {
        let clip_id = K::Clip::access_mut(self).insert(K::Clip::new(start, length, asset_id));
        let track = K::Track::access_mut(self)
            .get_mut(track)
            .ok_or(EngineError::TrackNotFound)?;
        track.clips_mut().insert(start, clip_id);
        Ok(clip_id)
    }

    pub fn remove_track<K: Kind>(&mut self, track_id: <K::Track as Stored>::Id) -> Result<()> {
        let track = K::Track::access_mut(self)
            .remove(track_id)
            .ok_or(EngineError::TrackNotFound)?;
        let linked_id = track
            .linked_node_id()
            .expect("Track was orphaned from node");
        self.graph.purge(linked_id);
        for clip_id in track.clips().values() {
            K::Clip::access_mut(self).remove(*clip_id);
        }
        Ok(())
    }

    pub fn add_track<K: Kind>(
        &mut self,
        name: String,
        channels: u16,
    ) -> Result<(<K::Track as Stored>::Id, NodeID)>
    where
        TrackReader<K>: Node,
    {
        let track_id = K::Track::access_mut(self).insert(K::Track::new(name));
        let reader_node = TrackReader::<K>::new(track_id, channels);
        let node_id = self.graph.add_node(reader_node);
        *K::Track::access_mut(self)[track_id].linked_node_id_mut() = Some(node_id);
        Ok((track_id, node_id))
    }

    pub fn add_socket_to_node(&mut self, node_id: NodeID, socket: Socket) -> Result<SocketID> {
        let id = self.graph.sockets.insert(SocketMeta {
            owner: node_id,
            direction: SocketDirection::Input,
            kind: socket.kind,
            name: socket.name,
            visible: socket.visible,
        });
        self.graph.node_sockets[node_id].0.push(id);
        Ok(id)
    }

    pub fn remove_node_input(&mut self, node_id: NodeID) -> Result<()> {
        todo!()
    }

    pub fn socket_kind_of(&mut self, endpoint: SocketID) -> Result<DataKind> {
        self.graph
            .sockets
            .get(endpoint)
            .map(|s| s.kind)
            .ok_or(EngineError::SocketNotFound(endpoint).into())
    }

    pub fn compile_graph(&self) -> Result<CompiledGraph> {
        let order = self.graph.topo_sort()?;

        let mut socket_slot: HashMap<SocketID, SlotIndex> = HashMap::new();
        let mut buffer_count = 1usize; // slot 0 reserved for silence

        for &node_id in &order {
            for &out_id in self.graph.outputs_of(node_id) {
                socket_slot.insert(out_id, buffer_count);
                buffer_count += 1;
            }
        }

        let mut steps = Vec::with_capacity(order.len());
        for &node_id in &order {
            let input_ids = self.graph.inputs_of(node_id);
            let output_ids = self.graph.outputs_of(node_id);

            let input_slots: Vec<SlotIndex> = input_ids
                .iter()
                .map(|&in_id| {
                    self.graph
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

        let master_output_slot = self
            .graph
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
