//! Read-only commands for the [`ProjectActor`]

use crate::{
    engine::manager::{Command, Permission, Read, project::ProjectActor},
    model::flow::{
        NodeID,
        socket::{InputSocketID, OutputSocketID},
    },
};

#[expect(missing_docs)]
pub struct GetMasterNodeId;

impl Command<Read> for GetMasterNodeId {
    type Output = NodeID;
    type Actor = ProjectActor;

    async fn execute(self, actor: <Read as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .read(async |proj| proj.project().graph.master_node_id)
            .await
    }
}

#[expect(missing_docs)]
pub struct InputSocketOf(pub NodeID, pub usize);

impl Command<Read> for InputSocketOf {
    type Output = InputSocketID;
    type Actor = ProjectActor;

    async fn execute(self, actor: <Read as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .read(async move |proj| proj.project().graph.inputs_of(self.0)[self.1])
            .await
    }
}

#[expect(missing_docs)]
pub struct OutputSocketOf(pub NodeID, pub usize);

impl Command<Read> for OutputSocketOf {
    type Output = OutputSocketID;
    type Actor = ProjectActor;

    async fn execute(self, actor: <Read as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .read(async move |proj| proj.project().graph.outputs_of(self.0)[self.1])
            .await
    }
}
