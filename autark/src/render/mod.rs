use vizia::{
    context::{Context, EmitContext},
    modifiers::ActionModifiers,
    prelude::Signal,
    view::{Handle, View},
    views::{Button, Label, VStack},
};

use crate::render::track::AudioTrackRenderer;

pub mod track;
