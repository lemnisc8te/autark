use std::marker::PhantomData;

use crate::{
    engine::{SlotIndex, bbp::PoolExecutor, tick::Tick},
    model::{
        Audio, DataKind, Kind, Renderable, Stored,
        flow::{Node, Socket},
        project::RtProjectData,
    },
};

#[derive(Debug, Clone)]
pub struct TrackReader<K: Kind> {
    channels: u16,
    kind: PhantomData<K>,
    id: <K::Track as Stored>::Id,
}

impl<K: Kind> TrackReader<K> {
    pub fn new(id: <K::Track as Stored>::Id, channels: u16) -> Self {
        Self {
            kind: PhantomData,
            id,
            channels,
        }
    }
}

pub struct TrackReaderState {
    // block_start: Tick,
}

impl Node for TrackReader<Audio> {
    type State = TrackReaderState;

    fn init_state(&self) -> Self::State {
        TrackReaderState {}
    }

    fn process(
        &self,
        pool: &mut PoolExecutor,
        _state: &mut Self::State,
        project: &RtProjectData,
        block_start: Tick,
        _: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        if let Some(track) = project.tracks.get(self.id) {
            let output_buf = pool.get_output(outputs[0]);
            track.render(project, output_buf, block_start, self.channels);
        }
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "audio out", true)]
    }
}
