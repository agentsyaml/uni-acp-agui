use super::*;

impl Translator {
    pub(super) fn handle_plan(
        &mut self,
        plan: &agent_client_protocol::schema::v1::Plan,
    ) -> Vec<Event> {
        let mut events = self.close_open_messages();
        let identity_safe = {
            let mut seen_names = HashSet::new();
            plan.entries
                .iter()
                .all(|entry| !entry.content.is_empty() && seen_names.insert(entry.content.as_str()))
        };
        if !identity_safe {
            events.extend(self.finish_open_plan_steps());
            events.push(raw_passthrough(&SessionUpdate::Plan(plan.clone())));
            self.plan_entries.clear();
            self.plan_meta = plan.meta.clone();
            self.plan_seen = true;
            return events;
        }

        let structure_same = self.plan_entries.len() == plan.entries.len()
            && self.plan_meta == plan.meta
            && self
                .plan_entries
                .iter()
                .zip(&plan.entries)
                .all(|(previous, current)| {
                    previous.entry.content == current.content
                        && previous.entry.priority == current.priority
                        && previous.entry.meta == current.meta
                });

        let current_names: HashSet<&str> = plan
            .entries
            .iter()
            .map(|entry| entry.content.as_str())
            .collect();
        let mut actions = Vec::new();
        for previous in &self.plan_entries {
            if previous.started && !current_names.contains(previous.entry.content.as_str()) {
                actions.push((previous.entry.content.clone(), false));
            }
        }

        let mut needs_raw = !self.plan_seen || !structure_same;
        let mut next_entries = Vec::with_capacity(plan.entries.len());
        for entry in &plan.entries {
            let previous = self
                .plan_entries
                .iter()
                .find(|previous| previous.entry.content == entry.content);
            let previous_status = previous.map(|previous| &previous.entry.status);
            let was_started = previous.is_some_and(|previous| previous.started);
            let mut started = was_started;

            match (previous_status, &entry.status) {
                (
                    Some(agent_client_protocol::schema::v1::PlanEntryStatus::Pending),
                    agent_client_protocol::schema::v1::PlanEntryStatus::InProgress,
                ) if !was_started => {
                    actions.push((entry.content.clone(), true));
                    started = true;
                }
                (None, agent_client_protocol::schema::v1::PlanEntryStatus::InProgress) => {
                    actions.push((entry.content.clone(), true));
                    started = true;
                }
                (_, agent_client_protocol::schema::v1::PlanEntryStatus::Completed)
                    if was_started =>
                {
                    actions.push((entry.content.clone(), false));
                    started = false;
                }
                (Some(previous), current) if previous == current => {}
                _ => {
                    needs_raw = true;
                    if matches!(
                        &entry.status,
                        agent_client_protocol::schema::v1::PlanEntryStatus::Completed
                    ) {
                        started = false;
                    }
                }
            }
            next_entries.push(PlanEntryState {
                entry: entry.clone(),
                started,
            });
        }

        actions.sort_by(|left, right| left.0.cmp(&right.0));
        for (name, start) in actions {
            if start {
                events.push(Event::StepStarted(agui_rs_core::events::StepStartedEvent {
                    step_name: name,
                    base: BaseEventFields::default(),
                }));
            } else {
                events.push(Event::StepFinished(
                    agui_rs_core::events::StepFinishedEvent {
                        step_name: name,
                        base: BaseEventFields::default(),
                    },
                ));
            }
        }
        if needs_raw {
            events.push(raw_passthrough(&SessionUpdate::Plan(plan.clone())));
        }

        self.plan_entries = next_entries;
        self.plan_meta = plan.meta.clone();
        self.plan_seen = true;
        events
    }

    pub(super) fn finish_open_plan_steps(&mut self) -> Vec<Event> {
        let mut names: Vec<String> = self
            .plan_entries
            .iter()
            .filter(|entry| entry.started)
            .map(|entry| entry.entry.content.clone())
            .collect();
        names.sort();
        names
            .into_iter()
            .map(|step_name| {
                Event::StepFinished(agui_rs_core::events::StepFinishedEvent {
                    step_name,
                    base: BaseEventFields::default(),
                })
            })
            .collect()
    }
}
