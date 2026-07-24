use libautark::model::{Stored, arr::track::AudioTrack};
use vizia::{
    context::Context,
    view::{Handle, View},
    views::Label,
};

#[derive(Default)]
pub struct AudioTrackRenderer {
    track_id: <AudioTrack as Stored>::Id,
}

impl AudioTrackRenderer {
    fn new(cx: &mut Context, name: String) -> Handle<Self> {
        Self {
            ..Default::default()
        }
        .build(cx, |cx| {
            Label::new(cx, name);
        })
    }
}

impl View for AudioTrackRenderer {}
