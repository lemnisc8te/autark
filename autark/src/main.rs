#[forbid(
    unused_unsafe,
    clippy::fallible_impl_from,
    clippy::used_underscore_binding,
    clippy::used_underscore_items,
    clippy::undocumented_unsafe_blocks
)]
pub mod filesystem;
mod model;
pub mod render;

use libautark::engine::{Command, ErasedCommand};
use vizia::prelude::*;

use crate::{counter::CounterModifiers, model::track::TrackData};

// Define the application data model
pub struct AppData {
    engine_tx: rtrb::Producer<Box<dyn ErasedCommand>>,
    count: Signal<i32>,
    tracks: Signal<Vec<TrackData>>,
}

// Define events for mutating the application data
pub enum AppEvent {
    Increment,
    Decrement,
    AddTrack,
}

// Mutate application data in response to events
impl Model for AppData {
    fn event(&mut self, cx: &mut EventContext, event: &mut Event) {
        event.map(|app_event, meta| match app_event {
            AppEvent::Decrement => self.count.update(|count| *count -= 1),
            AppEvent::Increment => self.count.update(|count| *count += 1),AppEvent::AddTrack => self.tracks.update(|e|),
        });
    }
}

mod counter;

fn main() -> Result<(), ApplicationError> {
    Application::new(|cx| {
        cx.add_stylesheet(include_style!("src/style.css"))
            .expect("Failed to load stylesheet");
        let count = Signal::new(0);
        let tracks = Signal::new(vec![]);
        // Build model data into the application
        AppData { count, tracks }.build(cx);

        // Add the custom counter view and bind to the model data
        counter::Counter::new(cx, count)
            .on_increment(|cx| cx.emit(AppEvent::Increment))
            .on_decrement(|cx| cx.emit(AppEvent::Decrement));

        // Add the custom counter view and bind to the model data
        counter::Counter::new(cx, count)
            .on_increment(|cx| cx.emit(AppEvent::Increment))
            .on_decrement(|cx| cx.emit(AppEvent::Decrement));
    })
    .title("Counter")
    .inner_size((400, 150))
    .run()
}
