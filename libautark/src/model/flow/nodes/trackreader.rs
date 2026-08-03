use std::{collections::BTreeMap, marker::PhantomData};

use crate::{
    engine::{SlotIndex, tick::Tick, util::abp::PoolExecutor},
    model::{
        Audio, DataKind, Kind, RenderBlock, Renderable, Stored,
        arr::clip::ResolvedAudioClip,
        flow::{Node, Socket},
    },
};

#[derive(Debug, Clone)]
pub struct TrackReader<K: Kind> {
    channels: u16,
    kind: PhantomData<K>,
    pub id: <K::Track as Stored>::ID,
}

impl<K: Kind> TrackReader<K> {
    pub fn new(id: <K::Track as Stored>::ID, channels: u16) -> Self {
        Self {
            kind: PhantomData,
            id,
            channels,
        }
    }
}

pub struct TrackReaderState {
    pub clips: BTreeMap<Tick, ResolvedAudioClip>,
}

impl Node for TrackReader<Audio> {
    type State = TrackReaderState;

    fn init_state(&self) -> Self::State {
        TrackReaderState {
            clips: BTreeMap::default(),
        }
    }

    fn process(
        &self,
        pool: &mut PoolExecutor,
        state: &mut Self::State,
        block_start: Tick,
        _: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let output_buf = pool.get_output(outputs[0]);
        // Deinterleave
        let block_len: Tick = (output_buf.len() / self.channels as usize).into();
        let block_end = block_start + block_len;

        let lookback = state
            .clips
            .range(..block_start)
            .next_back()
            .map(|(_, c)| c)
            .filter(|c| c.start + c.length > block_start);

        let active = lookback
            .into_iter()
            .chain(state.clips.range(block_start..block_end).map(|(_, c)| c));

        let mut block = RenderBlock {
            buf: output_buf,
            block_start,
            channels: self.channels,
        };
        for clip in active {
            clip.render(&mut block);
        }
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "audio out", true)]
    }
}
