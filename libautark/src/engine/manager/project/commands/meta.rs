use crate::{
    engine::{
        manager::{
            Command, Handle, Modify, Permission,
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

impl Command<Modify> for Publish {
    type Output = Result<GraphUpdate>;
    type Actor = ProjectActor;

    async fn execute(self, mut actor: <Modify as Permission<Self::Actor>>::Guard) -> Self::Output {
        actor
            .data
            .publish_current(&self.asset_h, self.filter.as_deref())
            .await
    }
}
