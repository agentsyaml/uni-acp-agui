use super::*;

#[test]
fn plan_with_in_progress_emits_step_started() {
    let pending = Plan::new(vec![
        PlanEntry::new(
            "step one",
            PlanEntryPriority::High,
            PlanEntryStatus::Pending,
        ),
        PlanEntry::new(
            "step two",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        ),
    ]);
    let mut t = Translator::new();
    assert!(matches!(
        t.translate(SessionUpdate::Plan(pending)).as_slice(),
        [Event::Raw(_)]
    ));

    let in_progress = Plan::new(vec![
        PlanEntry::new(
            "step one",
            PlanEntryPriority::High,
            PlanEntryStatus::InProgress,
        ),
        PlanEntry::new(
            "step two",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        ),
    ]);
    let evs = t.translate(SessionUpdate::Plan(in_progress));
    assert_eq!(evs.len(), 1);
    match &evs[0] {
        Event::StepStarted(s) => assert_eq!(s.step_name, "step one"),
        other => panic!("expected StepStarted, got {other:?}"),
    }
}

#[test]
fn repeated_plan_snapshots_emit_no_duplicate_step_events() {
    let mut t = Translator::new();
    let pending = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Pending,
    )]);
    let in_progress = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::InProgress,
    )]);
    let completed = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Completed,
    )]);

    let _ = t.translate(SessionUpdate::Plan(pending));
    assert!(matches!(
        t.translate(SessionUpdate::Plan(in_progress.clone()))
            .as_slice(),
        [Event::StepStarted(_)]
    ));
    assert!(
        t.translate(SessionUpdate::Plan(in_progress))
            .iter()
            .all(|event| !matches!(event, Event::StepStarted(_) | Event::StepFinished(_)))
    );
    assert!(matches!(
        t.translate(SessionUpdate::Plan(completed.clone()))
            .as_slice(),
        [Event::StepFinished(_)]
    ));
    assert!(
        t.translate(SessionUpdate::Plan(completed))
            .iter()
            .all(|event| !matches!(event, Event::StepStarted(_) | Event::StepFinished(_)))
    );
}

#[test]
fn first_completed_plan_entry_is_raw_but_in_progress_starts_and_finishes() {
    let mut completed_translator = Translator::new();
    let completed = Plan::new(vec![PlanEntry::new(
        "already done",
        PlanEntryPriority::High,
        PlanEntryStatus::Completed,
    )]);
    let completed_events = completed_translator.translate(SessionUpdate::Plan(completed));
    assert!(matches!(completed_events.as_slice(), [Event::Raw(_)]));
    assert!(
        !completed_events
            .iter()
            .any(|event| matches!(event, Event::StepFinished(_)))
    );

    let mut in_progress_translator = Translator::new();
    let in_progress = Plan::new(vec![PlanEntry::new(
        "already running",
        PlanEntryPriority::High,
        PlanEntryStatus::InProgress,
    )]);
    let in_progress_events = in_progress_translator.translate(SessionUpdate::Plan(in_progress));
    assert!(matches!(
        in_progress_events.as_slice(),
        [Event::StepStarted(_), Event::Raw(_)]
    ));

    let completed_after_unpaired = Plan::new(vec![PlanEntry::new(
        "already running",
        PlanEntryPriority::High,
        PlanEntryStatus::Completed,
    )]);
    let completion_events =
        in_progress_translator.translate(SessionUpdate::Plan(completed_after_unpaired));
    assert!(matches!(
        completion_events.as_slice(),
        [Event::StepFinished(_)]
    ));
}

#[test]
fn removed_open_plan_entry_finishes_before_snapshot_raw() {
    let mut t = Translator::new();
    let pending = Plan::new(vec![
        PlanEntry::new("keep", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
        PlanEntry::new(
            "removed",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        ),
    ]);
    let active = Plan::new(vec![
        PlanEntry::new(
            "keep",
            PlanEntryPriority::Medium,
            PlanEntryStatus::InProgress,
        ),
        PlanEntry::new(
            "removed",
            PlanEntryPriority::Medium,
            PlanEntryStatus::InProgress,
        ),
    ]);
    let _ = t.translate(SessionUpdate::Plan(pending));
    let _ = t.translate(SessionUpdate::Plan(active));

    let events = t.translate(SessionUpdate::Plan(Plan::new(vec![PlanEntry::new(
        "keep",
        PlanEntryPriority::Medium,
        PlanEntryStatus::InProgress,
    )])));
    assert!(matches!(
        events.as_slice(),
        [Event::StepFinished(_), Event::Raw(_)]
    ));
    match &events[0] {
        Event::StepFinished(step) => assert_eq!(step.step_name, "removed"),
        _ => unreachable!(),
    }
    assert!(matches!(
        t.translate(SessionUpdate::Plan(Plan::new(Vec::new())))
            .as_slice(),
        [Event::StepFinished(_), Event::Raw(_)]
    ));
    assert!(
        t.translate(SessionUpdate::Plan(Plan::new(Vec::new())))
            .is_empty()
    );
}

#[test]
fn duplicate_plan_identity_falls_back_without_steps() {
    let mut t = Translator::new();
    let duplicate = Plan::new(vec![
        PlanEntry::new("same", PlanEntryPriority::High, PlanEntryStatus::Pending),
        PlanEntry::new("same", PlanEntryPriority::Low, PlanEntryStatus::InProgress),
    ]);
    let events = t.translate(SessionUpdate::Plan(duplicate));
    assert!(matches!(events.as_slice(), [Event::Raw(_)]));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::StepStarted(_) | Event::StepFinished(_)))
    );
}

#[test]
fn plan_step_event_order_is_sorted_by_entry_name() {
    let mut t = Translator::new();
    let pending = Plan::new(vec![
        PlanEntry::new("zeta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
        PlanEntry::new("alpha", PlanEntryPriority::High, PlanEntryStatus::Pending),
    ]);
    let active = Plan::new(vec![
        PlanEntry::new("zeta", PlanEntryPriority::Low, PlanEntryStatus::InProgress),
        PlanEntry::new(
            "alpha",
            PlanEntryPriority::High,
            PlanEntryStatus::InProgress,
        ),
    ]);
    let _ = t.translate(SessionUpdate::Plan(pending));
    let events = t.translate(SessionUpdate::Plan(active));
    let names: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            Event::StepStarted(step) => Some(step.step_name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, ["alpha", "zeta"]);

    let completed = Plan::new(vec![
        PlanEntry::new("zeta", PlanEntryPriority::Low, PlanEntryStatus::Completed),
        PlanEntry::new("alpha", PlanEntryPriority::High, PlanEntryStatus::Completed),
    ]);
    let events = t.translate(SessionUpdate::Plan(completed));
    let finished_names: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            Event::StepFinished(step) => Some(step.step_name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(finished_names, ["alpha", "zeta"]);
}

#[test]
fn flush_resets_plan_state_between_runs() {
    let mut t = Translator::new();
    let pending = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Pending,
    )]);
    let in_progress = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::InProgress,
    )]);
    let completed = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Completed,
    )]);

    let _ = t.translate(SessionUpdate::Plan(pending));
    assert!(matches!(
        t.translate(SessionUpdate::Plan(in_progress)).as_slice(),
        [Event::StepStarted(_)]
    ));
    assert!(matches!(t.flush().as_slice(), [Event::StepFinished(_)]));
    assert!(t.flush().is_empty());

    let after_reset = t.translate(SessionUpdate::Plan(completed));
    assert!(matches!(after_reset.as_slice(), [Event::Raw(_)]));
    assert!(
        !after_reset
            .iter()
            .any(|event| matches!(event, Event::StepFinished(_)))
    );
}

#[test]
fn plan_snapshot_structure_changes_emit_full_raw_snapshot() {
    let mut t = Translator::new();
    let initial = Plan::new(vec![
        PlanEntry::new("alpha", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
        PlanEntry::new("beta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
    ]);
    let _ = t.translate(SessionUpdate::Plan(initial));

    let partial = Plan::new(vec![PlanEntry::new(
        "alpha",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Pending,
    )]);
    assert!(matches!(
        t.translate(SessionUpdate::Plan(partial)).as_slice(),
        [Event::Raw(_)]
    ));

    let priority_changed = Plan::new(vec![PlanEntry::new(
        "alpha",
        PlanEntryPriority::High,
        PlanEntryStatus::Pending,
    )]);
    assert!(matches!(
        t.translate(SessionUpdate::Plan(priority_changed))
            .as_slice(),
        [Event::Raw(_)]
    ));

    let mut meta = serde_json::Map::new();
    meta.insert("source".to_string(), serde_json::json!("changed"));
    let meta_changed = Plan::new(vec![
        PlanEntry::new("alpha", PlanEntryPriority::High, PlanEntryStatus::Pending).meta(meta),
    ]);
    assert!(matches!(
        t.translate(SessionUpdate::Plan(meta_changed)).as_slice(),
        [Event::Raw(_)]
    ));

    let mut reordered_translator = Translator::new();
    let ordered = Plan::new(vec![
        PlanEntry::new("alpha", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
        PlanEntry::new("beta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
    ]);
    let _ = reordered_translator.translate(SessionUpdate::Plan(ordered));
    let reordered = Plan::new(vec![
        PlanEntry::new("beta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
        PlanEntry::new("alpha", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
    ]);
    assert!(matches!(
        reordered_translator
            .translate(SessionUpdate::Plan(reordered))
            .as_slice(),
        [Event::Raw(_)]
    ));
}

#[test]
fn plan_top_level_meta_change_emits_raw_once() {
    let mut t = Translator::new();
    let entries = vec![PlanEntry::new(
        "same entry",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Pending,
    )];

    assert!(matches!(
        t.translate(SessionUpdate::Plan(Plan::new(entries.clone())))
            .as_slice(),
        [Event::Raw(_)]
    ));
    assert!(
        t.translate(SessionUpdate::Plan(Plan::new(entries.clone())))
            .is_empty()
    );

    let mut meta = serde_json::Map::new();
    meta.insert("source".to_string(), serde_json::json!("changed"));
    let changed = Plan::new(entries.clone()).meta(meta.clone());
    assert!(matches!(
        t.translate(SessionUpdate::Plan(changed)).as_slice(),
        [Event::Raw(_)]
    ));
    assert!(
        t.translate(SessionUpdate::Plan(Plan::new(entries).meta(meta)))
            .is_empty()
    );
}

#[test]
fn plan_all_pending_falls_through_as_raw() {
    let plan = Plan::new(vec![PlanEntry::new(
        "step",
        PlanEntryPriority::Medium,
        PlanEntryStatus::Pending,
    )]);
    let mut t = Translator::new();
    let evs = t.translate(SessionUpdate::Plan(plan));
    assert_eq!(evs.len(), 1);
    assert!(matches!(evs[0], Event::Raw(_)));
}
