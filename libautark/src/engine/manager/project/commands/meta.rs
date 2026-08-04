use crate::{
    engine::{
        manager::{
            Command, Handle, Operate,
            asset::AssetActor,
            project::{ProjectActor, commands::ProjectCommand},
        },
        state::GraphUpdate,
    },
    model::flow::NodeID,
};
use anyhow::Result;

pub struct Publish {
    pub asset_h: Handle<AssetActor>,
    pub filter: Option<Vec<NodeID>>,
}

impl ProjectCommand for Publish {}

impl Command for Publish {
    type Output = Result<GraphUpdate>;

    type Actor = ProjectActor;

    async fn execute(self, actor: &ProjectActor) -> Self::Output {
        actor
            .mutate(async |proj| {
                proj.publish_current(&self.asset_h, self.filter.as_deref())
                    .await
            })
            .await
    }
}
