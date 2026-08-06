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

impl Command<Query> for GetMasterNodeId {
    type Output = NodeID;
    type Actor = ProjectActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .query(async |proj| proj.project().graph.master_node_id)
            .await
    }
}

pub struct InputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for InputSocketOf {}

impl Command<Query> for InputSocketOf {
    type Output = InputSocketID;
    type Actor = ProjectActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .query(async move |proj| proj.project().graph.inputs_of(self.0)[self.1])
            .await
    }
}

pub struct OutputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for OutputSocketOf {}

impl Command<Query> for OutputSocketOf {
    type Output = OutputSocketID;
    type Actor = ProjectActor;

    async fn execute(self, actor: <Query as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .query(async move |proj| proj.project().graph.outputs_of(self.0)[self.1])
            .await
    }
}
