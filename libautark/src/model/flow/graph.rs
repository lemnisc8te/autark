//! NodeGraph-related dtypes and definitions

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
        nodes::master::Master,
        socket::{InputSocketID, OutputSocketID, Socket, SocketMeta},
    },
};

/// A graph representing the signal flow between nodes.
#[derive(Debug, Default, Clone)]
pub struct NodeGraph {
    /// A reference to the `Master` output node in the `graph`
    pub master_node_id: NodeID,
    pub nodes: SlotMap<NodeID, Arc<dyn ErasedNode>>,
    pub input_sockets: SlotMap<InputSocketID, SocketMeta>,
    pub output_sockets: SlotMap<OutputSocketID, SocketMeta>,
    pub node_input_sockets: SecondaryMap<NodeID, Vec<InputSocketID>>,
    pub node_output_sockets: SecondaryMap<NodeID, Vec<OutputSocketID>>,
    pub links: SecondaryMap<InputSocketID, OutputSocketID>,
}

impl NodeGraph {
    pub fn new() -> Self {
        let mut me = Self::default();

        let master_node = Master;
        let master_node_id = me.add_node(master_node);

        me.master_node_id = master_node_id;

        me
    }

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

    #[must_use]
    pub fn output_socket_named(&self, node: NodeID, name: &str) -> Option<OutputSocketID> {
        let outs = self.node_output_sockets.get(node)?;
        outs.iter()
            .copied()
            .find(|&id| self.output_sockets[id].name == name)
    }

    pub fn purge(&mut self, node_id: NodeID) {
        self.nodes.remove(node_id);
        if let Some(in_sockets) = self.node_input_sockets.remove(node_id) {
            for socket in &in_sockets {
                self.input_sockets.remove(*socket).unwrap();
                self.links.remove(*socket).unwrap();
            }
        }
        if let Some(out_sockets) = self.node_output_sockets.remove(node_id) {
            for socket in &out_sockets {
                self.output_sockets.remove(*socket).unwrap();
            }
        }
    }

    pub fn remove_link(&mut self, from: OutputSocketID, to: InputSocketID) {
        self.links
            .retain(|l_dest, l_source| !(*l_source == from && l_dest == to));
    }

    pub fn add_node<N: Node>(&mut self, node: N) -> NodeID {
        let (inputs, outputs) = (node.spec_in(), node.spec_out());
        let node_id = self.nodes.insert(Arc::new(node));
        let register_inputs = |graph: &mut Self, socks: Vec<Socket>| -> Vec<InputSocketID> {
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
        let register_outputs = |graph: &mut Self, socks: Vec<Socket>| -> Vec<OutputSocketID> {
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

    /// Add a link between `from_id` and `to_id`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the socket kinds cannot connect, or if the link would create a cycle.
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
            self.remove_link(from_id, to_id);
            return Err(EngineError::WouldCreateCycle.into());
        }
        Ok(prev_link)
    }

    /// All nodes that (transitively) feed `target`'s inputs, plus `target` itself.
    ///
    /// # Examples
    ///
    ///```
    /// # use libautark::model::flow::{graph::NodeGraph, nodes::utility::Utility};    ///
    /// let mut graph = NodeGraph::new();
    ///
    /// let utility_node_1 = graph.add_node(Utility);
    /// let utility_1_output = graph.outputs_of(utility_node_1)[0];
    ///
    /// let utility_node_2 = graph.add_node(Utility);
    /// let utility_2_input = graph.inputs_of(utility_node_2)[0];
    /// let utility_2_output = graph.outputs_of(utility_node_2)[0];
    ///
    /// graph.add_link(utility_1_output, utility_2_input);
    ///
    /// let ancestors = graph.ancestors_of(&[utility_node_2]);
    ///
    /// assert!(ancestors.contains(&utility_node_1));
    /// assert!(ancestors.contains(&utility_node_2));
    /// ```
    #[must_use]
    pub fn ancestors_of(&self, targets: &[NodeID]) -> HashSet<NodeID> {
        let mut seen = HashSet::new();
        let mut stack = VecDeque::from(targets.to_vec());
        while let Some(n) = stack.pop_front() {
            if seen.insert(n) {
                for &in_sock in self.inputs_of(n) {
                    if let Some(&src_sock) = self.links.get(in_sock) {
                        stack.push_back(self.output_sockets[src_sock].owner);
                    }
                }
            }
        }
        seen
    }

    /// All nodes that (transitively) feed `target`'s outputs, plus `target` itself.
    ///
    /// # Examples
    ///
    ///```
    /// # use libautark::model::flow::{graph::NodeGraph, nodes::utility::Utility};    ///
    /// let mut graph = NodeGraph::new();
    ///
    /// let utility_node_1 = graph.add_node(Utility);
    /// let utility_1_output = graph.outputs_of(utility_node_1)[0];
    ///
    /// let utility_node_2 = graph.add_node(Utility);
    /// let utility_2_input = graph.inputs_of(utility_node_2)[0];
    /// let utility_2_output = graph.outputs_of(utility_node_2)[0];
    ///
    /// graph.add_link(utility_1_output, utility_2_input);
    ///
    /// let succs = graph.successors_of(&[utility_node_1]);
    ///
    /// assert!(succs.contains(&utility_node_1));
    /// assert!(succs.contains(&utility_node_2));
    /// ```
    ///
    /// # Panics
    ///
    /// If there is an input socket entry in `self.links` that does not have a corresponding entry in `self.input_sockets`, then this function will panic.
    /// This should not happen.
    #[must_use]
    pub fn successors_of(&self, targets: &[NodeID]) -> HashSet<NodeID> {
        let mut seen = HashSet::new();
        let mut stack = VecDeque::from(targets.to_vec());
        while let Some(n) = stack.pop_front() {
            if seen.insert(n) {
                let outputs = self.outputs_of(n);
                let links = self
                    .links
                    .iter()
                    .filter_map(|(i, output)| outputs.contains(output).then_some(i));
                let succs = links.map(|i| {
                    self.input_sockets
                        .get(i)
                        .expect("Input socket was found in links,but had no corresponding entry")
                        .owner
                });
                stack.extend(succs);
            }
        }
        seen
    }

    /// Return the topological ordering of the nodes within the graph.
    ///
    /// This is mostly used during schedule compilation.
    ///
    /// `filter` determines the "branch" nodes that we want to focus on.
    /// This is used for soloing/muting.
    ///
    /// # Examples
    ///
    /// ```
    /// # use libautark::model::flow::{graph::NodeGraph, nodes::utility::Utility};
    /// let mut graph = NodeGraph::new();
    ///
    /// let utility_node_1 = graph.add_node(Utility);
    /// let utility_1_output = graph.outputs_of(utility_node_1)[0];
    ///
    /// let utility_node_2 = graph.add_node(Utility);
    /// let utility_2_input = graph.inputs_of(utility_node_2)[0];
    /// let utility_2_output = graph.outputs_of(utility_node_2)[0];
    ///
    /// let master_input = graph.inputs_of(graph.master_node_id)[0];
    ///
    /// graph.add_link(utility_1_output, utility_2_input);
    /// graph.add_link(utility_2_output, master_input);
    ///
    /// let order = graph.topo_sort(None).unwrap();
    ///
    /// assert_eq!(order, vec![utility_node_1,utility_node_2, graph.master_node_id])
    ///
    /// ```
    ///
    /// Filtering:
    /// ```
    /// # use libautark::model::flow::{graph::NodeGraph, nodes::utility::Utility};
    /// let mut graph = NodeGraph::new();
    ///
    /// let utility_node_1 = graph.add_node(Utility);
    /// let utility_1_output = graph.outputs_of(utility_node_1)[0];
    ///
    /// let utility_node_2 = graph.add_node(Utility);
    /// let utility_2_input = graph.inputs_of(utility_node_2)[0];
    /// let utility_2_output = graph.outputs_of(utility_node_2)[0];
    ///
    /// let master_input = graph.inputs_of(graph.master_node_id)[0];
    ///
    /// graph.add_link(utility_1_output, utility_2_input);
    /// graph.add_link(utility_2_output, master_input);
    ///
    /// let order = graph.topo_sort(Some(&[utility_node_2])).unwrap();
    ///
    /// assert_eq!(order, vec![utility_node_2, graph.master_node_id])
    ///
    /// ```
    ///
    ///
    /// # Panics
    ///
    /// This function will panic if it manages to find a successor node without calculating its in-degree
    ///
    /// # Errors
    ///
    /// This function will return a `Result::Err` if the current state of the graph is NOT acyclic (meaning there is a loop somewhere). This may be removed in the future to allow for greater expressiveness in graph construction
    pub fn topo_sort(&self, filter: Option<&[NodeID]>) -> Result<Vec<NodeID>> {
        // 1. Identify all reachable nodes if a filter is provided
        let nodes_to_sort: HashSet<NodeID> = if let Some(seeds) = filter {
            self.successors_of(seeds)
        } else {
            self.nodes.keys().collect()
        };

        // 2. Build the subgraph structures
        let mut in_degree: HashMap<NodeID, usize> =
            nodes_to_sort.iter().map(|&id| (id, 0)).collect();
        let mut successors: HashMap<NodeID, Vec<NodeID>> =
            nodes_to_sort.iter().map(|&id| (id, Vec::new())).collect();

        for (input_socket, &source_socket) in &self.links {
            let to_node = self.input_sockets[input_socket].owner;
            let from_node = self.output_sockets[source_socket].owner;

            if nodes_to_sort.contains(&to_node)
                && nodes_to_sort.contains(&from_node)
                && let Some(in_node) = in_degree.get_mut(&to_node)
                && let Some(successors) = successors.get_mut(&from_node)
            {
                *in_node += 1;
                successors.push(to_node);
            }
        }

        // 3. Kahn's Algorithm
        let mut queue: VecDeque<NodeID> = in_degree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut order = Vec::new();
        while let Some(n) = queue.pop_front() {
            order.push(n);
            if let Some(succs) = successors.get(&n) {
                for &succ in succs {
                    let degree = in_degree
                        .get_mut(&succ)
                        .expect("in_degree map did not contain the successor node");
                    *degree -= 1;
                    if *degree == 0 {
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn topo_sort_no_filter() -> Result<()> {
        use crate::model::flow::nodes::utility::Utility;
        let mut graph = NodeGraph::new();

        let utility_node_1 = graph.add_node(Utility);
        let utility_1_output = graph.outputs_of(utility_node_1)[0];

        let utility_node_2 = graph.add_node(Utility);
        let utility_2_input = graph.inputs_of(utility_node_2)[0];
        let utility_2_output = graph.outputs_of(utility_node_2)[0];

        let master_input = graph.inputs_of(graph.master_node_id)[0];

        graph.add_link(utility_1_output, utility_2_input)?;
        graph.add_link(utility_2_output, master_input)?;

        let order = graph.topo_sort(None)?;

        assert_eq!(order, vec![utility_node_2, graph.master_node_id]);
        Ok(())
    }

    #[test]
    fn topo_sort_with_filter() -> Result<()> {
        use crate::model::flow::nodes::utility::Utility;
        let mut graph = NodeGraph::new();

        let utility_node_1 = graph.add_node(Utility);
        let utility_1_output = graph.outputs_of(utility_node_1)[0];

        let utility_node_2 = graph.add_node(Utility);
        let utility_2_input = graph.inputs_of(utility_node_2)[0];
        let utility_2_output = graph.outputs_of(utility_node_2)[0];

        let master_input = graph.inputs_of(graph.master_node_id)[0];

        graph.add_link(utility_1_output, utility_2_input)?;
        graph.add_link(utility_2_output, master_input)?;

        let order = graph.topo_sort(Some(&[utility_node_2]))?;

        assert_eq!(order, vec![utility_node_2, graph.master_node_id]);
        Ok(())
    }
}
