use crate::{
    engine::{
        manager::{
            Command, Meta, StdHandle,
            asset::AssetActor,
            project::{ProjectActor, commands::ProjectCommand},
        },
        state::GraphUpdate,
    },
    model::flow::NodeID,
};
use anyhow::Result;

pub struct Publish {
    pub asset_h: StdHandle<AssetActor>,
    pub filter: Option<Vec<NodeID>>,
}

impl ProjectCommand for Publish {}

impl Command<Meta> for Publish {
    type Output = Result<GraphUpdate>;

    type Actor = ProjectActor;

    fn execute(self, actor: &mut ProjectActor) -> Self::Output {
        actor.publish_current(&self.asset_h, self.filter.as_deref())
    }
}
