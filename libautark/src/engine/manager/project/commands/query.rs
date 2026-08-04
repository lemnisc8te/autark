use crate::{
    engine::manager::{
        Command, Operate,
        project::{ProjectActor, commands::ProjectCommand},
    },
    model::flow::{
        NodeID,
        socket::{InputSocketID, OutputSocketID},
    },
};

pub struct GetMasterNodeId;

impl ProjectCommand for GetMasterNodeId {}

impl Command for GetMasterNodeId {
    type Output = NodeID;

    type Actor = ProjectActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        actor.query(async |proj| proj.current.master_node_id).await
    }
}

pub struct InputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for InputSocketOf {}

impl Command for InputSocketOf {
    type Output = InputSocketID;

    type Actor = ProjectActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        actor
            .query(async |proj| proj.current.graph.inputs_of(self.0)[self.1])
            .await
    }
}

pub struct OutputSocketOf(pub NodeID, pub usize);

impl ProjectCommand for OutputSocketOf {}

impl Command for OutputSocketOf {
    type Output = OutputSocketID;

    type Actor = ProjectActor;

    async fn execute(self, actor: &Self::Actor) -> Self::Output {
        actor
            .query(async |proj| proj.current.graph.outputs_of(self.0)[self.1])
            .await
    }
}
