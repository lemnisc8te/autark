//! The core audio engine. Used to manipulate `Project`s, hold the audio thread, and more.
pub mod constants;
pub mod engineconfig;
pub mod errors;
pub mod manager;
pub mod state;
pub mod tick;
pub mod transport;
pub mod util;

use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};

use crate::{
    engine::{
        constants::DEFAULT_MANAGER_CAPACITY,
        engineconfig::EngineConfig,
        manager::{
            asset::AssetActor,
            audio::{AudioActor, UpdateCmd},
            project::{ProjectActor, commands::meta::Publish},
        },
        tick::Tick,
    },
    model::{
        flow::{ErasedNode, NodeID},
        project::ProjectData,
    },
};

use anyhow::Result;
use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::message::{DynMessage, Message};

pub type SlotIndex = usize;

pub struct ScheduleStep {
    pub node: Arc<dyn ErasedNode>,
    pub node_id: NodeID,
    pub input_slots: Vec<SlotIndex>,
    pub output_slots: Vec<SlotIndex>,
}

#[derive(Default)]
pub struct CompiledGraph {
    pub steps: Vec<ScheduleStep>,
    pub buffer_count: usize,
    pub capture_slot: SlotIndex,
}

pub struct Engine {
    pub playhead: Arc<AtomicU64>,
    config: EngineConfig,
    asset_h: ActorRef<AssetActor>,
    project_h: ActorRef<ProjectActor>,
    audio_h: ActorRef<AudioActor>,
}

impl Engine {
    /// .
    ///
    /// # Panics
    ///
    /// Panics if .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn new(project: ProjectData) -> Result<Self> {
        let config = EngineConfig::create()?;

        let playhead = Arc::new(AtomicU64::new(0));

        let audio_h = AudioActor::spawn((config.clone(), playhead.clone()));

        let project_h = ProjectActor::spawn(project);
        let asset_h = AssetActor::spawn(());
        Ok(Self {
            playhead,
            config,
            asset_h,
            project_h,
            audio_h,
        })
    }

    pub async fn publish(&self, filter: Option<Vec<NodeID>>) {
        let update = self
            .project_h
            .ask(Publish {
                asset_h: self.asset_h.clone(),
                filter,
            })
            .await
            .unwrap();
        self.audio_h.ask(UpdateCmd(update)).await.unwrap();
    }

    #[must_use]
    pub const fn sample_rate(&self) -> u32 {
        self.config.config.sample_rate
    }

    #[must_use]
    pub const fn channels(&self) -> u16 {
        self.config.config.channels
    }

    pub fn move_playhead(&self, to: Tick) {
        self.playhead.swap(to.0, Ordering::Relaxed);
    }
}

pub trait HasActorRef<A: Actor> {
    fn actor_ref(&self) -> &ActorRef<A>;
}

impl HasActorRef<ProjectActor> for Engine {
    fn actor_ref(&self) -> &ActorRef<ProjectActor> {
        dbg!("Getting project_h");
        &self.project_h
    }
}
impl HasActorRef<AssetActor> for Engine {
    fn actor_ref(&self) -> &ActorRef<AssetActor> {
        dbg!("Getting asset_h");
        &self.asset_h
    }
}

impl HasActorRef<AudioActor> for Engine {
    fn actor_ref(&self) -> &ActorRef<AudioActor> {
        dbg!("Getting audio_h");
        &self.audio_h
    }
}

impl Engine {
    pub async fn get<A, C>(&self, command: C) -> <<A as Message<C>>::Reply as kameo::Reply>::Ok
    where
        A: Message<C>,
        Self: HasActorRef<A>,
        C: DynMessage<A> + 'static,
    {
        let aref = HasActorRef::<A>::actor_ref(self);
        aref.ask(command).await.unwrap()
    }

    pub fn tell<A, C>(&self, command: C)
    where
        A: Message<C>,
        Self: HasActorRef<A>,
        C: Send + 'static,
    {
        HasActorRef::<A>::actor_ref(self).tell(command);
    }
}

impl Actor for Engine {
    type Args = ProjectData;

    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self> {
        Self::new(args)
    }
}

impl<T> Message<T> for Engine
where
    T: Send + 'static,
{
    type Reply = Result<Message<T>::Reply>;

    async fn handle(
        &mut self,
        msg: T,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        HasActorRef::<A>::actor_ref(self).ask(msg).await?
    }
}
