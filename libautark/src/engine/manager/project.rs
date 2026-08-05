use crate::{
    engine::manager::{Handle, HasHandle, StdCarrier},
    model::project::{ProjectData, ProjectHistory},
};

use crate::engine::manager::Actor;

pub struct ProjectActor {
    pub data: ProjectHistory,
    pub loopback: Handle<Self>,
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

pub mod commands;

impl HasHandle<Self> for ProjectActor {
    fn handle(&self) -> &Handle<Self> {
        &self.loopback
    }
}

impl Actor for ProjectActor {
    type InitParams = ProjectData;
    type Carrier = StdCarrier<Self>;
    type Data = ProjectHistory;

    fn new(current: Self::InitParams, loopback: Handle<Self>) -> Self {
        Self {
            data: ProjectHistory::new(current),
            loopback,
        }
    }
}
