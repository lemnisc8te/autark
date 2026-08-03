use crate::{
    engine::{
        manager::{
            Command, Handle, MetaMutate,
            asset::AssetActor,
            project::{ProjectActor, commands::ProjectCommand},
        },
        state::GraphUpdate,
    },
    model::flow::NodeID,
};
use anyhow::Result;
use async_trait::async_trait;

pub struct Publish {
    pub asset_h: Handle<AssetActor>,
    pub filter: Option<Vec<NodeID>>,
}

impl ProjectCommand for Publish {}
#[async_trait]
impl Command<MetaMutate> for Publish {
    type Output = Result<GraphUpdate>;

    type Actor = ProjectActor;

    async fn execute(self, actor: &mut ProjectActor) -> Self::Output {
        actor
            .publish_current(&self.asset_h, self.filter.as_deref())
            .await
    }
}
