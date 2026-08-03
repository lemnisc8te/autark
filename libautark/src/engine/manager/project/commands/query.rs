use async_trait::async_trait;

use crate::{
    engine::manager::{
        Command, Permission, Query,
        project::{ProjectActor, commands::ProjectCommand},
    },
    model::flow::{
        NodeID,
        socket::{InputSocketID, OutputSocketID},
    },
};

pub struct GetMasterNodeId;

impl ProjectCommand for GetMasterNodeId {}

#[async_trait]
impl Command<Query> for GetMasterNodeId {
    type Output = NodeID;

    type Actor = ProjectActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.master_node_id
    }
}

pub struct InputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for InputSocketOf {}

#[async_trait]
impl Command<Query> for InputSocketOf {
    type Output = InputSocketID;

    type Actor = ProjectActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.graph.inputs_of(self.0)[self.1]
    }
}

pub struct OutputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for OutputSocketOf {}

#[async_trait]
impl Command<Query> for OutputSocketOf {
    type Output = OutputSocketID;

    type Actor = ProjectActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Type<'_>) -> Self::Output {
        actor.graph.outputs_of(self.0)[self.1]
    }
}
