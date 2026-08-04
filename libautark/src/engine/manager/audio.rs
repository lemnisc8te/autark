use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use cpal::traits::StreamTrait;
use kameo::{Actor, actor::ActorRef, message::Message};

use crate::engine::{
    CompiledGraph,
    constants::{GARBAGE_RING_CAPACITY, MAX_BUFFER_SLOTS, UPDATE_RING_CAPACITY},
    engineconfig::EngineConfig,
    state::{Garbage, GraphUpdate, NodeStatePool},
    tick::Tick,
    transport::{Transport, TransportState},
    util::abp::AudioBufferPool,
};

pub struct AudioActor {
    update_tx: rtrb::Producer<GraphUpdate>,
    pub transport: Arc<Transport>,
    _stream: cpal::Stream,
    loopback: ActorRef<Self>,
}

impl AudioActor {
    pub fn init(
        config: &EngineConfig,
        playhead: Arc<AtomicU64>,
        loopback: ActorRef<Self>,
    ) -> Result<Self> {
        let transport = Arc::new(Transport::new());
        let init_update = GraphUpdate::default();

        let (mut update_tx, update_rx) = rtrb::RingBuffer::<GraphUpdate>::new(UPDATE_RING_CAPACITY);
        let (garbage_tx, mut garbage_rx) = rtrb::RingBuffer::<Garbage>::new(GARBAGE_RING_CAPACITY);

        // Seed the ring with the initial graph so the audio thread has
        // something to play from the very first callback.
        let _ = update_tx.push(init_update);

        // Background thread: the only place anything from the audio thread
        // actually gets dropped/deallocated.
        std::thread::spawn(move || {
            loop {
                while let Ok(garbage) = garbage_rx.pop() {
                    drop(garbage);
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });

        let stream =
            Self::build_stream::<f32>(config, transport.clone(), playhead, update_rx, garbage_tx)?;
        stream.play()?; // device stream runs continuously; transport gates output
        Ok(Self {
            transport,
            update_tx,
            _stream: stream,
            loopback,
        })
    }

    fn build_stream<T>(
        config: &EngineConfig,
        transport: Arc<Transport>,
        playhead: Arc<AtomicU64>,
        mut update_rx: rtrb::Consumer<GraphUpdate>,
        mut garbage_tx: rtrb::Producer<Garbage>,
    ) -> Result<cpal::Stream>
    where
        T: cpal::SizedSample + cpal::FromSample<f32>,
    {
        use cpal::traits::DeviceTrait;
        let channels = config.config.channels;
        let device = config.device.clone();
        let mut buffer_pool = AudioBufferPool::new(MAX_BUFFER_SLOTS, 1024);

        let mut state_pool = NodeStatePool::new();
        let mut current: Option<GraphUpdate> = None;
        let stream = device.build_output_stream(
            config.config,
            move |data: &mut [T], _info: &cpal::OutputCallbackInfo| {
                // assert_no_alloc::assert_no_alloc(|| {
                data.fill(T::from_sample(0.0));
                // Tier 1: drain any pending structural updates. Zero
                // allocation: everything was pre-built off-thread.
                while let Ok(mut update) = update_rx.pop() {
                    state_pool.apply(&mut update, &mut garbage_tx);
                    if let Some(old) = current.replace(update) {
                        let _ = garbage_tx.push(Garbage::Update(old));
                    }
                }

                if !transport.is_playing() {
                    return;
                }
                let frame_count = data.len() / channels as usize;
                let start = playhead.fetch_add(frame_count as u64, Ordering::Relaxed);

                let Some(GraphUpdate { schedule, .. }) = current.as_ref() else {
                    return;
                };

                let mixed =
                    Self::execute_block(schedule, Tick(start), &mut buffer_pool, &mut state_pool);

                for (dst, &src) in data.iter_mut().zip(mixed) {
                    *dst = T::from_sample(src);
                }
                // });
            },
            move |err| eprintln!("audio stream error: {err}"),
            None,
        )?;

        Ok(stream)
    }

    /// Runs the compiled schedule for one block and returns the master mix.
    pub fn execute_block<'a>(
        schedule: &CompiledGraph,
        block_start: Tick,
        pool: &'a mut AudioBufferPool,
        state_pool: &mut NodeStatePool,
    ) -> &'a [f32] {
        assert_no_alloc::assert_no_alloc(|| {
            // Clear the pool. Unless you want to summon demons.
            pool.clear();

            let mut executor = pool.executor();

            for i in 0..schedule.steps.len() {
                let step = &schedule.steps[i];
                let node = &step.node;

                node.process_erased(
                    &mut executor,
                    state_pool.get_mut(step.node_id),
                    block_start,
                    &step.input_slots,
                    &step.output_slots,
                );
            }

            executor.get_input(schedule.capture_slot)
        })
    }
}

pub struct TransportCmd(pub TransportState);

impl Message<TransportCmd> for AudioActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: TransportCmd,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.transport.transport(msg.0);
    }
}

pub struct Play;

impl Message<Play> for AudioActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: Play,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.transport.play();
    }
}

pub struct UpdateCmd(pub GraphUpdate);

impl Message<UpdateCmd> for AudioActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: UpdateCmd,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.update_tx.push(msg.0).is_err() {
            eprintln!("ring full, audio update dropped");
        }
    }
}

impl Actor for AudioActor {
    type Args = (EngineConfig, Arc<AtomicU64>);

    type Error = anyhow::Error;

    async fn on_start(
        (config, playhead): Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Self::init(&config, playhead, actor_ref)
    }
}
