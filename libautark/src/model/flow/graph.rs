use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use slotmap::{SecondaryMap, SlotMap};

use crate::{
    engine::errors::EngineError,
    model::flow::{
        ErasedNode, Node, NodeID,
        socket::{Socket, SocketDirection, SocketID, SocketMeta},
    },
};

/// A graph representing the signal flow between nodes.
#[derive(Debug, Default, Clone)]
pub struct NodeGraph {
    pub nodes: SlotMap<NodeID, Arc<dyn ErasedNode>>,
    pub sockets: SlotMap<SocketID, SocketMeta>,
    pub node_sockets: SecondaryMap<NodeID, (Vec<SocketID>, Vec<SocketID>)>, // ordered inputs, outputs
    // Map Incoming -> Outgoing sockets
    // essentially, maps sockets to where they get their value from
    pub links: SecondaryMap<SocketID, SocketID>,
}

impl NodeGraph {
    #[must_use]
    pub fn inputs_of(&self, node: NodeID) -> &[SocketID] {
        self.node_sockets.get(node).map_or(&[], |(ins, _)| ins)
    }
    #[must_use]
    pub fn outputs_of(&self, node: NodeID) -> &[SocketID] {
        self.node_sockets.get(node).map_or(&[], |(_, outs)| outs)
    }

    /// Robust against future reordering/insertion in a node's shape — looks
    /// up by the stable name in `SocketMeta` rather than position.
    #[must_use]
    pub fn socket_named(&self, node: NodeID, dir: SocketDirection, name: &str) -> Option<SocketID> {
        let (ins, outs) = self.node_sockets.get(node)?;
        let candidates = match dir {
            SocketDirection::Input => ins,
            SocketDirection::Output => outs,
        };
        candidates
            .iter()
            .copied()
            .find(|&id| self.sockets[id].name == name)
    }

    pub fn purge(&mut self, node_id: NodeID) {
        self.nodes.remove(node_id);
        if let Some((in_sockets, out_sockets)) = self.node_sockets.remove(node_id) {
            for socket in in_sockets.iter().chain(out_sockets.iter()) {
                self.sockets.remove(*socket).unwrap();
            }
        }
    }

    pub fn remove_link(&mut self, from: SocketID, to: SocketID) -> Result<()> {
        self.links
            .retain(|l_dest, l_source| !(*l_source == from && l_dest == to));
        Ok(())
    }

    pub fn add_node<N: Node>(&mut self, node: N) -> NodeID {
        let (inputs, outputs) = (node.spec_in(), node.spec_out());
        let node_id = self.nodes.insert(Arc::new(node));
        let register =
            |graph: &mut NodeGraph, socks: Vec<Socket>, dir: SocketDirection| -> Vec<SocketID> {
                socks
                    .into_iter()
                    .map(|s| {
                        graph.sockets.insert(SocketMeta {
                            owner: node_id,
                            direction: dir,
                            kind: s.kind,
                            name: s.name,
                            visible: s.visible,
                        })
                    })
                    .collect()
            };
        let input_ids = register(self, inputs, SocketDirection::Input);
        let output_ids = register(self, outputs, SocketDirection::Output);
        self.node_sockets.insert(node_id, (input_ids, output_ids));
        node_id
    }

    pub fn add_link(&mut self, from_id: SocketID, to_id: SocketID) -> Result<Option<SocketID>> {
        let from = &self.sockets[from_id];
        let to = &self.sockets[to_id];

        if from.direction != SocketDirection::Output || to.direction != SocketDirection::Input {
            anyhow::bail!(
                "Socket connection must be I->O, found {:?} -> {:?}",
                from.direction,
                to.direction
            );
        }

        if !from.kind.can_connect_to(to.kind) {
            anyhow::bail!("Invalid connection: {:?} -> {:?}", from.kind, to.kind)
        }

        let prev_link = self.links.insert(to_id, from_id);

        if self.topo_sort().is_err() {
            self.remove_link(from_id, to_id)?;
            return Err(EngineError::WouldCreateCycle.into());
        }
        Ok(prev_link)
    }

    /// Find the topological ordering of the nodes within the graph.
    /// This is used during schedule compilation
    pub fn topo_sort(&self) -> Result<Vec<NodeID>> {
        let mut in_degree: HashMap<NodeID, usize> = self.nodes.keys().map(|id| (id, 0)).collect();
        let mut successors: HashMap<NodeID, Vec<NodeID>> =
            self.nodes.keys().map(|id| (id, Vec::new())).collect();

        // liks: HashMap<SocketID /*input*/, SocketID /*source output*/>
        for (input_socket, &source_socket) in &self.links {
            let to_node = self.sockets[input_socket].owner;
            let from_node = self.sockets[source_socket].owner;
            *in_degree
                .get_mut(&to_node)
                .ok_or(EngineError::NodeNotFound(to_node))? += 1;
            successors
                .get_mut(&from_node)
                .ok_or(EngineError::NodeNotFound(from_node))?
                .push(to_node);
        }

        let mut queue: VecDeque<NodeID> = in_degree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(&id, _)| id)
            .collect();
        let mut order = Vec::with_capacity(self.nodes.len());

        while let Some(n) = queue.pop_front() {
            order.push(n);
            for &succ in &successors[&n] {
                let d = in_degree.get_mut(&succ).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push_back(succ);
                }
            }
        }
        if order.len() != self.nodes.len() {
            return Err(EngineError::WouldCreateCycle.into());
        }
        Ok(order)
    }
}
