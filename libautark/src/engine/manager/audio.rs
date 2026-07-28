use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use async_trait::async_trait;
use cpal::traits::StreamTrait;

use crate::{
    engine::{
        CompiledGraph,
        bbp::BlockBufferPool,
        constants::{GARBAGE_RING_CAPACITY, MAX_BUFFER_SLOTS, UPDATE_RING_CAPACITY},
        engineconfig::EngineConfig,
        manager::{self, Actor, Envelope, Manager, StdManager},
        state::{Garbage, GraphUpdate, NodeStatePool},
        tick::Tick,
        transport::Transport,
    },
    model::project::ProjectData,
};

pub struct AudioActor {
    pub update_tx: rtrb::Producer<GraphUpdate>,
    pub data: (),
    transport: Arc<Transport>,
    _stream: cpal::Stream,
}

impl AudioActor {
    pub fn init(config: &EngineConfig, playhead: Arc<AtomicU64>) -> Result<Self> {
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
            data: (),
            update_tx,
            _stream: stream,
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
        let mut buffer_pool = BlockBufferPool::new(MAX_BUFFER_SLOTS, 1024);

        let mut state_pool = NodeStatePool::new();
        let mut current: Option<GraphUpdate> = None;
        let stream = device.build_output_stream(
            config.config,
            move |data: &mut [T], _info: &cpal::OutputCallbackInfo| {
                assert_no_alloc::assert_no_alloc(|| {
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

                    let Some(GraphUpdate {
                        project, schedule, ..
                    }) = current.as_ref()
                    else {
                        return;
                    };

                    let mixed = Self::execute_block(
                        schedule,
                        project,
                        Tick(start),
                        &mut buffer_pool,
                        &mut state_pool,
                    );

                    for (dst, &src) in data.iter_mut().zip(mixed) {
                        *dst = T::from_sample(src);
                    }
                });
            },
            move |err| eprintln!("audio stream error: {err}"),
            None,
        )?;

        Ok(stream)
    }

    /// Runs the compiled schedule for one block and returns the master mix.
    pub fn execute_block<'a>(
        schedule: &CompiledGraph,
        project: &ProjectData,
        block_start: Tick,
        pool: &'a mut BlockBufferPool,
        state_pool: &mut NodeStatePool,
    ) -> &'a [f32] {
        // assert_no_alloc(|| {

        // Clear the pool. Unless you want to summon demons.
        pool.clear();

        let mut executor = pool.executor();

        for i in 0..schedule.steps.len() {
            let step = &schedule.steps[i];
            let node = &project.graph.nodes[step.node_id];

            node.process_erased(
                &mut executor,
                state_pool.get_mut(step.node_id),
                project,
                block_start,
                &step.input_slots,
                &step.output_slots,
            );
        }

        executor.get_input(schedule.master_output_slot)
        // })
    }
}

#[async_trait]
impl Envelope<AudioActor> for GraphUpdate {
    async fn handle(self, actor: &mut AudioActor) {
        actor.update_tx.push(self);
    }
}

#[async_trait]
impl Envelope<AudioActor> for Transport {
    async fn handle(self, actor: &mut AudioActor) {
        actor.transport.replace(self);
    }
}

#[async_trait]
impl Actor for AudioActor {
    type InitParams = (EngineConfig, Arc<AtomicU64>);
    /// The audio stream is inaccessible
    type Data = ();
    type Env = GraphUpdate;

    fn new((config, playhead): Self::InitParams) -> Self {
        Self::init(&config, playhead).unwrap()
    }

    fn data(&self) -> &Self::Data {
        &self.data
    }

    fn data_mut(&mut self) -> &mut Self::Data {
        &mut self.data
    }
}

pub struct AudioManager {}

pub struct AudioTaskTransport {}

#[async_trait]
impl manager::Transport<AudioActor> for AudioTaskTransport {
    type Sender = rtrb::Producer<<AudioActor as Actor>::Env>;
    type Receiver = rtrb::Consumer<<AudioActor as Actor>::Env>;

    fn pair(capacity: usize) -> (Self::Sender, Self::Receiver) {
        rtrb::RingBuffer::new(capacity)
    }

    fn send(sender: &mut Self::Sender, envelope: <AudioActor as Actor>::Env) -> Result<()> {
        let _ = sender.push(envelope);
        Ok(())
    }

    /// Awaits the next envelope, or `None` once the transport is closed.
    fn recv(receiver: &mut Self::Receiver) -> Result<<AudioActor as Actor>::Env> {
        Ok(receiver.pop()?)
    }
}

fn test(params: <AudioActor as Actor>::InitParams) {
    let m = StdManager::<AudioTaskTransport>::spawn(params, 0);
}
