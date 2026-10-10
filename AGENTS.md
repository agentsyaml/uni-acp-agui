# Project instructions

## Changes

- Preserve public APIs, protocol behavior, cancellation, resource ownership, and
  platform/feature gates unless a task explicitly changes them.
- Keep handwritten files at 500 lines or fewer. Target 450 lines to leave room
  for imports and formatting. Generated files, such as lockfiles, are exempt.
- Split files by responsibility. Do not use minification, arbitrary `include!`
  fragments, or duplicate implementations to meet the line limit.
- Use English in source code and comments. Preserve Unicode test data with
  equivalent escapes; localized documentation may remain in its own language.
- Limit each edit of new or rewritten code to roughly 500–2000 characters.
  Complete, unchanged functions or test groups may be moved as a unit.
- Repeat these size, language, and validation rules when delegating work.

## Verification

- Inspect dependency source when researching framework behavior; verify claims
  rather than relying only on documentation or search results.
- Do not test, build, lint, or format during implementation. Run consolidated
  validation after all edits are complete; use a single-file check only when
  specifically needed and authorized.
- Preserve existing test targets and assertions during structural refactors.
- Final checks include formatting, strict Clippy, locked feature-matrix tests,
  platform-specific behavior, and frontend checks when affected.
- Do not disable security checks or weaken assertions to obtain passing results.

## Workspace and communication

- Do not change system configuration or install global tools without consent.
- Do not commit or push unless explicitly requested. Review the complete diff
  and validation results before committing.
- Remove temporary files created by the task, not unrelated user files/caches.
- Write concise, concrete documentation; examples are useful for complex topics.
- Explain changes without internal task labels or assumed project expertise.
- Prefer working functionality over speculative constraints or abstractions;
  state any unresolved conflict or limitation explicitly.
