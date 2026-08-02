use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use slotmap::{SecondaryMap, SlotMap};

use crate::{
    engine::errors::EngineError,
    model::flow::{
        ErasedNode, Node, NodeID,
        socket::{InputSocketID, OutputSocketID, Socket, SocketMeta},
    },
};

/// A graph representing the signal flow between nodes.
#[derive(Debug, Default, Clone)]
pub struct NodeGraph {
    pub nodes: SlotMap<NodeID, Arc<dyn ErasedNode>>,
    pub input_sockets: SlotMap<InputSocketID, SocketMeta>,
    pub output_sockets: SlotMap<OutputSocketID, SocketMeta>,
    pub node_input_sockets: SecondaryMap<NodeID, Vec<InputSocketID>>,
    pub node_output_sockets: SecondaryMap<NodeID, Vec<OutputSocketID>>,
    pub links: SecondaryMap<InputSocketID, OutputSocketID>,
}

impl NodeGraph {
    #[must_use]
    pub fn inputs_of(&self, node: NodeID) -> &[InputSocketID] {
        self.node_input_sockets.get(node).map_or(&[], |v| v)
    }
    #[must_use]
    pub fn outputs_of(&self, node: NodeID) -> &[OutputSocketID] {
        self.node_output_sockets.get(node).map_or(&[], |v| v)
    }

    /// Robust against future reordering/insertion in a node's shape — looks
    /// up by the stable name in `SocketMeta` rather than position.
    #[must_use]
    pub fn input_socket_named(&self, node: NodeID, name: &str) -> Option<InputSocketID> {
        let ins = self.node_input_sockets.get(node)?;

        ins.iter()
            .copied()
            .find(|&id| self.input_sockets[id].name == name)
    }

    pub fn output_socket_named(&self, node: NodeID, name: &str) -> Option<OutputSocketID> {
        let outs = self.node_output_sockets.get(node)?;
        outs.iter()
            .copied()
            .find(|&id| self.output_sockets[id].name == name)
    }

    pub fn purge(&mut self, node_id: NodeID) {
        self.nodes.remove(node_id);
        if let Some(in_sockets) = self.node_input_sockets.remove(node_id) {
            for socket in in_sockets.iter() {
                self.input_sockets.remove(*socket).unwrap();
            }
        }
        if let Some(out_sockets) = self.node_output_sockets.remove(node_id) {
            for socket in out_sockets.iter() {
                self.output_sockets.remove(*socket).unwrap();
            }
        }
    }

    pub fn remove_link(&mut self, from: OutputSocketID, to: InputSocketID) -> Result<()> {
        self.links
            .retain(|l_dest, l_source| !(*l_source == from && l_dest == to));
        Ok(())
    }

    pub fn add_node<N: Node>(&mut self, node: N) -> NodeID {
        let (inputs, outputs) = (node.spec_in(), node.spec_out());
        let node_id = self.nodes.insert(Arc::new(node));
        let register_inputs = |graph: &mut NodeGraph, socks: Vec<Socket>| -> Vec<InputSocketID> {
            socks
                .into_iter()
                .map(|s| {
                    graph.input_sockets.insert(SocketMeta {
                        owner: node_id,
                        kind: s.kind,
                        name: s.name,
                        visible: s.visible,
                    })
                })
                .collect()
        };
        let register_outputs = |graph: &mut NodeGraph, socks: Vec<Socket>| -> Vec<OutputSocketID> {
            socks
                .into_iter()
                .map(|s| {
                    graph.output_sockets.insert(SocketMeta {
                        owner: node_id,
                        kind: s.kind,
                        name: s.name,
                        visible: s.visible,
                    })
                })
                .collect()
        };
        let input_ids = register_inputs(self, inputs);
        let output_ids = register_outputs(self, outputs);
        self.node_input_sockets.insert(node_id, input_ids);
        self.node_output_sockets.insert(node_id, output_ids);
        node_id
    }

    pub fn add_link(
        &mut self,
        from_id: OutputSocketID,
        to_id: InputSocketID,
    ) -> Result<Option<OutputSocketID>> {
        let from = &self.output_sockets[from_id];
        let to = &self.input_sockets[to_id];

        if !from.kind.can_connect_to(to.kind) {
            anyhow::bail!("Invalid connection: {:?} -> {:?}", from.kind, to.kind)
        }

        let prev_link = self.links.insert(to_id, from_id);

        if self.topo_sort(None).is_err() {
            self.remove_link(from_id, to_id)?;
            return Err(EngineError::WouldCreateCycle.into());
        }
        Ok(prev_link)
    }

    /// Find the topological ordering of the nodes within the graph.
    /// This is used during schedule compilation
    /// `filter` determines the "branch" nodes that we want to focus on. This is used for soloing/muting
    pub fn topo_sort(&self, filter: Option<&[NodeID]>) -> Result<Vec<NodeID>> {
        // 1. Identify all reachable nodes if a filter is provided
        let mut nodes_to_sort: HashSet<NodeID> = HashSet::new();
        if let Some(seeds) = filter {
            let mut stack = VecDeque::from_iter(seeds.iter().cloned());
            while let Some(n) = stack.pop_front() {
                if nodes_to_sort.insert(n) {
                    // Add all nodes that 'n' points to
                    let outputs = self.outputs_of(n);
                    let links = self
                        .links
                        .iter()
                        .filter_map(|(i, o)| outputs.contains(o).then_some(i));
                    let succs = links.map(|i| self.input_sockets.get(i).unwrap().owner);
                    stack.extend(succs);
                }
            }
        } else {
            nodes_to_sort = self.nodes.keys().collect();
        }

        // 2. Build the subgraph structures
        let mut in_degree: HashMap<NodeID, usize> =
            nodes_to_sort.iter().map(|&id| (id, 0)).collect();
        let mut successors: HashMap<NodeID, Vec<NodeID>> =
            nodes_to_sort.iter().map(|&id| (id, Vec::new())).collect();

        for (input_socket, &source_socket) in &self.links {
            let to_node = self.input_sockets[input_socket].owner;
            let from_node = self.output_sockets[source_socket].owner;

            if nodes_to_sort.contains(&to_node) && nodes_to_sort.contains(&from_node) {
                *in_degree.get_mut(&to_node).unwrap() += 1;
                successors.get_mut(&from_node).unwrap().push(to_node);
            }
        }

        // 3. Kahn's Algorithm
        let mut queue: VecDeque<NodeID> = in_degree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut order = Vec::new();
        while let Some(n) = queue.pop_front() {
            order.push(n);
            if let Some(succs) = successors.get(&n) {
                for &succ in succs {
                    let d = in_degree.get_mut(&succ).unwrap();
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(succ);
                    }
                }
            }
        }

        if order.len() != nodes_to_sort.len() {
            return Err(EngineError::WouldCreateCycle.into());
        }
        Ok(order)
    }
}
