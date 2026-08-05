use crate::{
    engine::manager::{Handle, HasHandle, StdCarrier},
    model::project::{ProjectData, ProjectMetaData},
};

use crate::engine::manager::Actor;

pub struct ProjectActor {
    pub data: ProjectMetaData,
    pub loopback: Handle<Self>,
}

impl ProjectActor {
    async fn mutate<O>(&mut self, f: impl AsyncFnOnce(&mut ProjectMetaData) -> O) -> O {
        let proj = self.data.project().clone();
        self.data.commit(proj);
        f(&mut self.data).await
    }

    async fn query<O>(&self, f: impl AsyncFn(&ProjectMetaData) -> O) -> O {
        f(&self.data).await
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
    type Data = ProjectMetaData;

    fn new(current: Self::InitParams, loopback: Handle<Self>) -> Self {
        Self {
            data: ProjectMetaData::new(current).into(),
            loopback,
        }
    }
}
