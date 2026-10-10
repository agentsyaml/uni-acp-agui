use super::*;

impl Translator {
    /// Configure a set of tool titles whose ACP-side `ToolCall` /
    /// `ToolCallUpdate` events should be suppressed in favour of the
    /// bridge's MCP-driven `FrontendToolCall` items.
    ///
    /// Pass both the short tool name AND any prefixed variant the agent
    /// might use (typically `<mcp-server-name>_<tool-name>`). Repeated
    /// calls *replace* the set, matching the per-run lifecycle of
    /// `RunAgentInput.tools`.
    pub fn set_suppressed_titles<I, S>(&mut self, titles: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.suppressed_titles = titles.into_iter().map(Into::into).collect();
    }

    /// Emit an AG-UI `TOOL_CALL_START` (and `TOOL_CALL_ARGS` if any) for a
    /// tool call the bridge is driving directly through its in-process
    /// MCP endpoint.
    ///
    /// Unlike [`Translator::translate`] for [`SessionUpdate::ToolCall`],
    /// this **bypasses the suppression filter**: the suppression set
    /// exists to drop *agent-side echoes* of these calls, so the canonical
    /// invocation is emitted directly as a complete AG-UI envelope. MCP
    /// `tools/call` only returns after the frontend responds; it must not
    /// delay `TOOL_CALL_END` while that execution is pending.
    pub fn translate_frontend_tool_call(
        &mut self,
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Option<&serde_json::Value>,
    ) -> Vec<Event> {
        let tool_call_id = tool_call_id.into();
        let tool_name = tool_name.into();

        let mut events = self.close_open_messages();

        events.push(Event::ToolCallStart(ToolCallStartEvent {
            tool_call_id: tool_call_id.clone(),
            tool_call_name: tool_name,
            parent_message_id: None,
            base: BaseEventFields::default(),
        }));

        if let Some(value) = arguments
            && let Some(event) = tool_call_args_event(&tool_call_id, value)
        {
            events.push(event);
        }

        events.push(Event::ToolCallEnd(ToolCallEndEvent {
            tool_call_id,
            base: BaseEventFields::default(),
        }));
        events
    }

    /// Track a newly open tool call, evicting the OLDEST open entry when the
    /// cap is exceeded. An evicted call is closed cleanly: its cached
    /// raw input/output are dropped with it and its `open_tool_calls` /
    /// order entries are removed together, and the returned events close the
    /// evicted call before the replacement call is started.
    fn insert_open_tool_call(&mut self, tool_call_id: String) -> Vec<Event> {
        let mut events = Vec::new();
        if self.open_tool_calls.insert(tool_call_id.clone()) {
            self.open_tool_call_order.push_back(tool_call_id.clone());
            while self.open_tool_call_order.len() > MAX_OPEN_TOOL_CALLS {
                let oldest = self
                    .open_tool_call_order
                    .pop_front()
                    .expect("len just checked above cap");
                if self.open_tool_calls.remove(&oldest) {
                    events.extend(self.close_tool_call(&oldest));
                }
            }
        }
        events
    }

    pub(super) fn close_tool_call(&mut self, tool_call_id: &str) -> Vec<Event> {
        let mut events = Vec::new();
        if let Some(input) = self.raw_tool_inputs.remove(tool_call_id)
            && let Some(event) = tool_call_args_event(tool_call_id, &input)
        {
            events.push(event);
        }
        events.push(Event::ToolCallEnd(ToolCallEndEvent {
            tool_call_id: tool_call_id.to_string(),
            base: BaseEventFields::default(),
        }));
        if let Some(output) = self.raw_tool_outputs.remove(tool_call_id) {
            events.push(tool_call_result_event(tool_call_id, &output));
        }
        events
    }

    fn remove_open_tool_call(&mut self, tool_call_id: &str) -> bool {
        if !self.open_tool_calls.remove(tool_call_id) {
            return false;
        }
        if let Some(position) = self
            .open_tool_call_order
            .iter()
            .position(|id| id == tool_call_id)
        {
            self.open_tool_call_order.remove(position);
        }
        true
    }

    /// Emit an AG-UI `TOOL_CALL_END` for legacy bridge-driven tool calls.
    /// Canonical frontend invocations are already complete and are not
    /// tracked here. Idempotent for unknown or already-closed ids.
    pub fn translate_frontend_tool_end(&mut self, tool_call_id: &str) -> Vec<Event> {
        if !self.remove_open_tool_call(tool_call_id) {
            self.raw_tool_inputs.remove(tool_call_id);
            self.raw_tool_outputs.remove(tool_call_id);
            return Vec::new();
        }
        self.close_tool_call(tool_call_id)
    }

    /// Handle a ToolCall session update.
    ///
    /// AG-UI rule: tool call arrival must close any open text message first.
    pub(super) fn handle_tool_call(
        &mut self,
        tc: &agent_client_protocol::schema::v1::ToolCall,
    ) -> Vec<Event> {
        let tool_name = tc.title.clone();
        let tool_call_id = tc.tool_call_id.0.to_string();

        // Suppress agent-side echoes of bridge-driven frontend tool calls
        // (see `suppressed_titles` doc on Translator). We still close any
        // open text message so the AG-UI rule about message boundaries is
        // preserved — the *next* event on the stream (ours, from the MCP
        // path) will then open the canonical TOOL_CALL_* envelope.
        if self.suppressed_titles.contains(&tool_name) {
            // ponytail: FIFO cap on suppressed ids; ids are also pruned on
            // their terminal update, so this only matters for calls that
            // never terminate.
            if !matches!(
                tc.status,
                ToolCallStatus::Completed | ToolCallStatus::Failed
            ) && !self.suppressed_ids.contains(&tool_call_id)
                && self.suppressed_ids.len() < MAX_SUPPRESSED_IDS
            {
                self.suppressed_ids.push_back(tool_call_id);
            }
            return self.close_open_messages();
        }

        let mut events = Vec::new();

        // Close open messages before starting a tool call (AG-UI protocol rule)
        events.append(&mut self.close_open_messages());

        let already_open = self.open_tool_calls.contains(&tool_call_id);
        let terminal = matches!(
            tc.status,
            ToolCallStatus::Completed | ToolCallStatus::Failed
        );
        if let Some(ref raw_input) = tc.raw_input {
            self.raw_tool_inputs
                .insert(tool_call_id.clone(), raw_input.clone());
        }
        if let Some(ref raw_output) = tc.raw_output {
            self.raw_tool_outputs
                .insert(tool_call_id.clone(), raw_output.clone());
        }
        if !already_open {
            if !terminal {
                events.extend(self.insert_open_tool_call(tool_call_id.clone()));
            }
            // Emit TOOL_CALL_START after any eviction closures.
            events.push(Event::ToolCallStart(ToolCallStartEvent {
                tool_call_id: tool_call_id.clone(),
                tool_call_name: tool_name,
                parent_message_id: None,
                base: BaseEventFields::default(),
            }));
        }
        if terminal {
            self.remove_open_tool_call(&tool_call_id);
            events.extend(self.close_tool_call(&tool_call_id));
        }
        events
    }

    /// Handle a ToolCallUpdate session update.
    pub(super) fn handle_tool_call_update(
        &mut self,
        update: &agent_client_protocol::schema::v1::ToolCallUpdate,
    ) -> Vec<Event> {
        let tool_call_id = update.tool_call_id.0.to_string();

        // Drop updates for ids that originated on a suppressed ToolCall.
        // The bridge's MCP path owns the lifecycle for those.
        if self.suppressed_ids.contains(&tool_call_id) {
            // If this update reports terminal status, also clear the id
            // so the suppression set doesn't grow unbounded over a long
            // session.
            if let Some(ref status) = update.fields.status
                && matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed)
            {
                self.suppressed_ids.retain(|id| id != &tool_call_id);
            }
            return Vec::new();
        }

        let terminal = matches!(
            update.fields.status,
            Some(ToolCallStatus::Completed | ToolCallStatus::Failed)
        );
        if !self.open_tool_calls.contains(&tool_call_id) {
            self.raw_tool_inputs.remove(&tool_call_id);
            self.raw_tool_outputs.remove(&tool_call_id);
            return Vec::new();
        }

        let mut events = Vec::new();
        if let Some(ref raw_input) = update.fields.raw_input {
            self.raw_tool_inputs
                .insert(tool_call_id.clone(), raw_input.clone());
        }
        if let Some(ref raw_output) = update.fields.raw_output {
            self.raw_tool_outputs
                .insert(tool_call_id.clone(), raw_output.clone());
        }

        if terminal {
            self.remove_open_tool_call(&tool_call_id);
            events.extend(self.close_tool_call(&tool_call_id));
        }

        events
    }
}
