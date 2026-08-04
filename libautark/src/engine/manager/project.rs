use crate::{
    engine::manager::{BoxedEnvelope, Handle, HasHandle, Operate, StdCarrier},
    model::project::{ProjectData, ProjectMetaData},
};
use tokio::sync::RwLock;

use crate::engine::manager::Actor;

pub struct ProjectActor {
    pub data: RwLock<ProjectMetaData>,
    loopback: Handle<Self>,
}

pub mod commands;

impl HasHandle<Self> for ProjectActor {
    fn handle(&self) -> &Handle<Self> {
        &self.loopback
    }
}

impl Actor for ProjectActor {
    type InitParams = ProjectData;
    type Envelope = BoxedEnvelope<Self>;
    type Carrier = StdCarrier<Self>;

    fn new(current: Self::InitParams, loopback: Handle<Self>) -> Self {
        Self {
            data: ProjectMetaData::new(current).into(),
            loopback,
        }
    }
}
impl Operate for ProjectActor {
    type Data = ProjectMetaData;

    async fn mutate<O>(&self, f: impl AsyncFnOnce(&mut Self::Data) -> O) -> O {
        let mut lock = self.data.write().await;
        let proj = lock.project().clone();
        lock.commit(proj);
        f(&mut lock).await
    }

    async fn query<O>(&self, f: impl AsyncFn(&Self::Data) -> O) -> O {
        let lock = self.data.read().await;
        f(&lock).await
    }
}
