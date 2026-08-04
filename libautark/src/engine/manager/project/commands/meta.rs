use crate::{
    engine::{
        manager::{
            asset::AssetActor,
            project::{ProjectActor, commands::ProjectCommand},
        },
        state::GraphUpdate,
    },
    model::flow::NodeID,
};
use anyhow::Result;
use kameo::{actor::ActorRef, message::Message};

pub struct Publish {
    pub asset_h: ActorRef<AssetActor>,
    pub filter: Option<Vec<NodeID>>,
}

impl ProjectCommand for Publish {}

impl Message<Publish> for ProjectActor {
    type Reply = Result<GraphUpdate>;

    async fn handle(
        &mut self,
        msg: Publish,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.publish_current(&msg.asset_h, msg.filter.as_deref())
            .await
    }
}
