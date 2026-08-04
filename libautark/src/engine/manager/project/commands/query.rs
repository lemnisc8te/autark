use kameo::message::Message;

use crate::{
    engine::manager::project::{ProjectActor, commands::ProjectCommand},
    model::flow::{
        NodeID,
        socket::{InputSocketID, OutputSocketID},
    },
};
use anyhow::Result;

pub struct GetMasterNodeId;

impl ProjectCommand for GetMasterNodeId {}

impl Message<GetMasterNodeId> for ProjectActor {
    type Reply = Result<NodeID>;

    async fn handle(
        &mut self,
        msg: GetMasterNodeId,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.project().master_node_id)
    }
}

pub struct InputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for InputSocketOf {}

impl Message<InputSocketOf> for ProjectActor {
    type Reply = Result<InputSocketID>;
    async fn handle(
        &mut self,
        msg: InputSocketOf,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.project()
            .graph
            .inputs_of(msg.0)
            .get(msg.1)
            .ok_or(anyhow::anyhow!("Invalid socket index"))
            .copied()
    }
}

pub struct OutputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for OutputSocketOf {}

impl Message<OutputSocketOf> for ProjectActor {
    type Reply = Result<OutputSocketID>;

    async fn handle(
        &mut self,
        msg: OutputSocketOf,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.project()
            .graph
            .outputs_of(msg.0)
            .get(msg.1)
            .ok_or(anyhow::anyhow!("Invalid output socket index"))
            .copied()
    }
}
