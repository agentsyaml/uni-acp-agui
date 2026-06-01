//! Tracks lifecycle of a single AG-UI text message stream so the bridge emits
//! exactly one `TextMessageStart` before any content, exactly one
//! `TextMessageEnd` to close it, and never duplicates either boundary.

use agui_rs_core::{
    BaseEventFields, Event, TextMessageContentEvent, TextMessageEndEvent, TextMessageRole,
    TextMessageStartEvent,
};

#[derive(Debug, Clone)]
pub struct MessageState {
    message_id: String,
    open: bool,
}

impl MessageState {
    pub fn new(message_id: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            open: false,
        }
    }

    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Emit events for an incoming chunk: `[Start, Content]` on first chunk,
    /// `[Content]` thereafter. Empty chunks are skipped (returns empty vec)
    /// while preserving the `open` state.
    pub fn push_chunk(&mut self, delta: impl Into<String>) -> Vec<Event> {
        let delta = delta.into();
        if delta.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(2);
        if !self.open {
            self.open = true;
            out.push(Event::TextMessageStart(TextMessageStartEvent {
                message_id: self.message_id.clone(),
                role: TextMessageRole::Assistant,
                name: None,
                base: BaseEventFields::default(),
            }));
        }
        out.push(Event::TextMessageContent(TextMessageContentEvent {
            message_id: self.message_id.clone(),
            delta,
            base: BaseEventFields::default(),
        }));
        out
    }

    /// Emit `[TextMessageEnd]` if the message was open; otherwise `[]`.
    pub fn close(&mut self) -> Vec<Event> {
        if !self.open {
            return Vec::new();
        }
        self.open = false;
        vec![Event::TextMessageEnd(TextMessageEndEvent {
            message_id: self.message_id.clone(),
            base: BaseEventFields::default(),
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_chunk_emits_start_then_content() {
        let mut s = MessageState::new("m1");
        let evs = s.push_chunk("hello");
        assert_eq!(evs.len(), 2);
        assert!(matches!(evs[0], Event::TextMessageStart(_)));
        assert!(matches!(evs[1], Event::TextMessageContent(_)));
        assert!(s.is_open());
    }

    #[test]
    fn subsequent_chunk_emits_only_content() {
        let mut s = MessageState::new("m1");
        let _ = s.push_chunk("hello");
        let evs = s.push_chunk(" world");
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            Event::TextMessageContent(e) => assert_eq!(e.delta, " world"),
            other => panic!("expected content, got {other:?}"),
        }
    }

    #[test]
    fn close_when_open_emits_end() {
        let mut s = MessageState::new("m1");
        let _ = s.push_chunk("x");
        let evs = s.close();
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], Event::TextMessageEnd(_)));
        assert!(!s.is_open());
    }

    #[test]
    fn close_when_closed_is_noop() {
        let mut s = MessageState::new("m1");
        assert!(s.close().is_empty());
        let _ = s.push_chunk("x");
        let _ = s.close();
        assert!(s.close().is_empty());
    }
}
