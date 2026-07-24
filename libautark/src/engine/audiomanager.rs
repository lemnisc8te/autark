use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use cpal::traits::StreamTrait;

use crate::engine::{
    bbp::BlockBufferPool,
    constants::{GARBAGE_RING_CAPACITY, MAX_BUFFER_SLOTS, UPDATE_RING_CAPACITY},
    engineconfig::EngineConfig,
    execute_block,
    state::{Garbage, GraphUpdate, NodeStatePool},
    tick::Tick,
    transport::Transport,
};

pub struct AudioManager {
    pub update_tx: rtrb::Producer<GraphUpdate>,
    _stream: cpal::Stream,
}

impl AudioManager {
    /// Create a new [`AudioManager`]
    ///
    /// # Errors
    ///
    /// This function will return an error if creating or playing the audio stream fails.
    pub fn new(
        init_update: GraphUpdate,
        config: &EngineConfig,
        transport: Arc<Transport>,
        playhead: Arc<AtomicU64>,
    ) -> Result<Self> {
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

        let stream = Self::build_stream::<f32>(config, transport, playhead, update_rx, garbage_tx)?;
        stream.play()?; // device stream runs continuously; transport gates output
        Ok(Self {
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

                    let frame_count = data.len() / channels as usize;
                    let start = playhead.fetch_add(frame_count as u64, Ordering::Relaxed);

                    if !transport.is_playing() {
                        return;
                    }

                    let Some(GraphUpdate {
                        project, schedule, ..
                    }) = current.as_ref()
                    else {
                        return;
                    };

                    let mixed = execute_block(
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
}
