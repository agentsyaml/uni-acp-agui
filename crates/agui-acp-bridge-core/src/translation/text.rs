use super::*;

impl Translator {
    pub(super) fn push_agent(
        &mut self,
        text: String,
        message_id: Option<&MessageId>,
    ) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        push_text_message(
            &mut self.agent,
            &mut self.agent_acp_message_id,
            text,
            message_id,
            TextMessageRole::Assistant,
        )
    }

    pub(super) fn push_user(&mut self, text: String, message_id: Option<&MessageId>) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        push_text_message(
            &mut self.user,
            &mut self.user_acp_message_id,
            text,
            message_id,
            TextMessageRole::User,
        )
    }

    pub(super) fn push_thought(
        &mut self,
        text: String,
        message_id: Option<&MessageId>,
    ) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        let incoming_id = message_id.map(|id| id.0.to_string());
        let needs_boundary = incoming_id.as_deref().is_some_and(|id| {
            self.thought.is_some() && self.thought_acp_message_id.as_deref() != Some(id)
        });
        let mut events = if needs_boundary {
            self.close_thought()
        } else {
            Vec::new()
        };
        let id = match &self.thought {
            Some(existing) => existing.clone(),
            None => {
                let new_id = incoming_id
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                events.push(Event::ReasoningMessageStart(ReasoningMessageStartEvent {
                    message_id: new_id.clone(),
                    role: ReasoningMessageRole::Reasoning,
                    base: BaseEventFields::default(),
                }));
                self.thought = Some(new_id.clone());
                self.thought_acp_message_id = incoming_id;
                new_id
            }
        };
        events.push(Event::ReasoningMessageContent(
            ReasoningMessageContentEvent {
                message_id: id,
                delta: text,
                base: BaseEventFields::default(),
            },
        ));
        events
    }

    pub(super) fn close_agent(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(mut state) = self.agent.take() {
            out.append(&mut close_text(&mut state));
        }
        self.agent_acp_message_id = None;
        out
    }

    pub(super) fn close_user(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(mut state) = self.user.take() {
            out.append(&mut close_text(&mut state));
        }
        self.user_acp_message_id = None;
        out
    }

    pub(super) fn close_thought(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(id) = self.thought.take() {
            out.push(Event::ReasoningMessageEnd(ReasoningMessageEndEvent {
                message_id: id,
                base: BaseEventFields::default(),
            }));
        }
        self.thought_acp_message_id = None;
        out
    }

    /// Close all open text messages and thought streams.
    /// Used when a tool call arrives (AG-UI requires messages to be closed first).
    pub(super) fn close_open_messages(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        out.append(&mut self.close_agent());
        out.append(&mut self.close_user());
        out.append(&mut self.close_thought());
        out
    }
}
