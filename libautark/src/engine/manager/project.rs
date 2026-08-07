//! Actor for [`ProjectData`]-related operations

use crate::{
    engine::manager::{ActorRef, HasActorRef},
    model::project::{ProjectData, ProjectHistory},
};

use crate::engine::manager::Actor;

pub mod commands;

pub struct ProjectActor {
    pub data: ProjectHistory,
    pub loopback: ActorRef<Self>,
}

impl ProjectActor {
    async fn mutate<O>(&mut self, func: impl AsyncFnOnce(&mut ProjectHistory) -> O) -> O {
        let proj = self.data.project().clone();
        self.data.commit(proj);
        func(&mut self.data).await
    }

    async fn query<O>(&self, func: impl AsyncFn(&ProjectHistory) -> O) -> O {
        func(&self.data).await
    }
}

impl HasActorRef<Self> for ProjectActor {
    fn get_ref(&self) -> &ActorRef<Self> {
        &self.loopback
    }
}

impl Actor for ProjectActor {
    type InitParam = ProjectData;
    type Data = ProjectHistory;

    fn new(current: Self::InitParam, loopback: ActorRef<Self>) -> Self {
        Self {
            data: ProjectHistory::new(current),
            loopback,
        }
    }
}
