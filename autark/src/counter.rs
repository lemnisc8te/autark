use vizia::prelude::*;

// Define a custom view for the counter
pub struct Counter {
    pub(crate) on_increment: Option<Box<dyn Fn(&mut EventContext)>>,
    pub(crate) on_decrement: Option<Box<dyn Fn(&mut EventContext)>>,
}

impl Counter {
    pub fn new(cx: &mut Context, count: impl Res<i32> + Clone + 'static) -> Handle<Self> {
        Self {
            on_decrement: None,
            on_increment: None,
        }
        .build(cx, |cx| {
            HStack::new(cx, |cx| {
                Button::new(cx, |cx| Label::new(cx, Localized::new("dec")))
                    .on_press(|ex| ex.emit(CounterEvent::Decrement))
                    .class("dec");

                Button::new(cx, |cx| Label::new(cx, Localized::new("inc")))
                    .on_press(|ex| ex.emit(CounterEvent::Increment))
                    .class("inc");

                Label::new(cx, count).class("count").live(Live::Assertive);
            })
            .class("row");
        })
    }
}

// Internal events
pub enum CounterEvent {
    Decrement,
    Increment,
}

// Handle internal events
impl View for Counter {
    fn event(&mut self, cx: &mut EventContext, event: &mut Event) {
        event.map(|counter_event, meta| match counter_event {
            CounterEvent::Increment => {
                if let Some(callback) = &self.on_increment {
                    (callback)(cx);
                }
            }

            CounterEvent::Decrement => {
                if let Some(callback) = &self.on_decrement {
                    (callback)(cx);
                }
            }
        });
    }
}

// Custom modifiers
pub trait CounterModifiers {
    fn on_increment<F: Fn(&mut EventContext) + 'static>(self, callback: F) -> Self;
    fn on_decrement<F: Fn(&mut EventContext) + 'static>(self, callback: F) -> Self;
}

// Implement custom modifiers
impl<'a> CounterModifiers for Handle<'a, Counter> {
    fn on_increment<F: Fn(&mut EventContext) + 'static>(self, callback: F) -> Self {
        self.modify(|counter| counter.on_increment = Some(Box::new(callback)))
    }

    fn on_decrement<F: Fn(&mut EventContext) + 'static>(self, callback: F) -> Self {
        self.modify(|counter| counter.on_decrement = Some(Box::new(callback)))
    }
}
