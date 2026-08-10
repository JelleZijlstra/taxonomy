---
name: generate-recommendations
description:
  Generate evidence-bearing, reviewable JSONL recommendations for taxonomy database
  changes, with guarded snapshots and a dry-run-first applicator. Use when Codex is
  asked to audit data and produce executable recommendations for fields, tags,
  Locations, type localities, merges, or another repeatable database correction without
  directly changing the database.
---

# Generate Recommendations

Research and generate recommendations; leave application to the user. Use
`scripts/apply_recommendations.py` as the single review, validation, and application
entry point.

## Workflow

1. Inspect relevant documentation in `docs/` for the topic and follow current
   conventions.
2. Inspect current database state and primary evidence. Separate source text from
   inference and retain uncertainty.
3. Write `recs/scripts/generate_<topic>_recommendations.py` when generation is repeated
   or database-dependent. Generators may read but must never write the database. This
   directory is intentionally untracked; keep durable applicators in `scripts/`.
4. Write a new topic- or round-specific JSONL under `recs/manifests/`, which is also
   intentionally untracked. Do not revise a manifest that the user has already applied;
   start another file.
5. Give every row `schema_version`, `action`, `confidence`, `reason`, evidence, and
   enough object IDs, labels, and old values to reject stale database state.
6. Run both views before handoff:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --dry-run
   ```

   When the applicator supports it, also lint the complete proposed object graph in
   memory before handoff:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl \
       --virtual-lint
   ```

   Virtual lint is advisory because database-wide queries cannot see newly proposed
   objects, but it catches ordinary field, tag, relationship, and model-lint problems
   before the user writes anything.

   To limit the compact review to particular actions, repeat `--review-action`:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review \
       --review-action manual_review
   ```

   For a manifest containing unresolved rows or actionable rows with important review
   caveats, inspect their complete notes with:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review-manual
   ```

   To make an explicit decision on every row, use the interactive review mode:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review-each
   ```

   It visits rows in manifest order. `yes` queues the row for application, `no` skips
   it, and `edit` opens the affected existing object and skips the automated row. Direct
   editing is unavailable for `create_object` rows. After the final choice, the selected
   rows are rebuilt and validated together before any automated write; this preserves
   object-reference dependencies and accounts for changes made in editors.

   Operation flags may be combined. Static `--review` and `--review-manual` output runs
   first, followed by requested virtual lint and `--dry-run`, then `--apply` or
   `--review-each`, and finally `--edit-manual`. For example,
   `--review-manual --review-each --edit-manual` prints the complete unresolved notes,
   reviews every row interactively, applies the accepted subset, and then opens all
   manual-review objects. Only `--apply` and `--review-each` are incompatible because
   they respectively mean applying every row and selecting individual rows.

   To work through unresolved rows interactively after validating the complete manifest,
   print each full manual-review note and open its database object editor:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --edit-manual
   ```

   To apply every actionable recommendation and then edit each unresolved manual-review
   object in the same run, use:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --apply --edit-manual
   ```

   The combined mode validates the complete manifest and resolves all manual editor
   targets before writing anything. It then applies the actionable recommendations and
   finally prints each complete manual-review note before opening that object's editor.

   This edits the explicit `object` on generic `manual_review` rows. For legacy
   type-locality `manual_review` rows, it edits the referenced Name. It does not open
   actionable rows that merely carry `review_note`. The interactive view gives each
   object a distinct header, shows the full evidence, and repeats the reason immediately
   before opening the editor so the recommended decision remains visible at the prompt.
   Edit-only mode warns and continues when a row has become stale, because it will not
   write that row. It resolves manual-review objects by stable model and ID, so a label
   change is reported but does not prevent editing; missing objects and invalid IDs or
   models still stop the run. Combined `--apply --edit-manual` mode remains strict about
   the complete manifest.

7. Do not use `--apply`. The user reviews and applies recommendations.

## Available actions

The unified dispatcher recognizes every action below. Prefer generic actions for simple
mutations and specialized actions when they provide important multi-object validation or
domain semantics.

### Generic actions, schema version 1

- `create_object`: create one model object from typed field values and assign it a
  manifest-local reference for subsequent actions.
- `set_field`: set one ordinary database field after checking its old value.
- `add_tag`: add one serialized ADT tag to a named tag field.
- `remove_tag`: remove one serialized ADT tag from a named tag field.
- `manual_review`: record a non-mutating, evidence-bearing review decision for any
  database object after validating its ID and label.

Existing-object mutation actions use:

```json
{
  "schema_version": 1,
  "action": "set_field",
  "confidence": "high",
  "reason": "Evidence-backed explanation.",
  "evidence": [{ "kind": "source", "text": "Exact evidence." }],
  "object": { "model": "Location", "id": 123, "label": "Old label" },
  "field": "latitude",
  "old_value": null,
  "new_value": "1°N-2°N"
}
```

Represent foreign keys as `{"model": "Region", "id": 1, "label": "Region"}` and enums as
`{"enum": "RegionKind", "name": "country"}`. `add_tag` and `remove_tag` replace
`old_value` and `new_value` with `tag`, whose value is the result of the ADT tag's
`serialize()` method. `set_field` intentionally rejects ADT fields.

Generic `manual_review` omits `field`, `old_value`, `new_value`, and `tag`. It uses the
same `object`, `confidence`, `reason`, and nonempty `evidence` fields, remains a no-op
under `--apply`, and is printed in full by `--review-manual`. Type-locality
`manual_review` retains its Name-specific schema below.

`create_object` uses an `object.ref` instead of an ID and provides a `values` mapping.
The values use the same typed representations as `set_field`; an ADT field is a list of
serialized tags. A later row may use the created object either as its target or as a
foreign-key value by giving
`{"model": "Location", "ref": "precise_site", "label": "Precise site"}`. References are
file-local, must be unique, and may only point backward to an earlier `create_object`
row. For example:

```json
{"schema_version":1,"action":"create_object","confidence":"high","reason":"Reviewed evidence.","evidence":[{"kind":"source","text":"Exact evidence."}],"object":{"model":"Location","ref":"precise_site","label":"Precise site"},"values":{"name":"Precise site","region":{"model":"Region","id":1,"label":"Region"},"tags":[]}}
{"schema_version":1,"action":"set_field","confidence":"high","reason":"Use the precise site.","evidence":[{"kind":"source","text":"Exact evidence."}],"object":{"model":"OccurrenceRecord","id":2,"label":"Source locality"},"field":"location","old_value":null,"new_value":{"model":"Location","ref":"precise_site","label":"Precise site"}}
```

Creation is restart-safe when the model's label identifies exactly one existing object
and every supplied field already has the recommended value. An existing same-label
object with different data is a validation error rather than an implicit reuse.

### Article actions, schema version 1

- `create_article`: create one electronic Article, optionally create its ordinary
  CitationGroup, create unchecked Persons for its authors, install a staged PDF, and run
  PDF text extraction and search indexing.

Use the dedicated `add-article` skill to research and generate this action. It owns a
checksum- and size-guarded file move, so do not emulate it with generic `create_object`.
Static review does not access the database or network; planning expands DOI metadata and
validates the database, CitationGroup, existing library folder, staged PDF, and final
destination. Explicit Article metadata overrides CrossRef. The user must authorize
`--apply`; generators and skills must not apply the row themselves.

### Location actions, schema version 1

- `edit_location`: atomically change multiple Location fields and/or tags.
- `rename_location`: rename one Location with collision checks.
- `merge_location`: merge a source Location into a target and reconcile compatible data;
  use `allow_temporal_context_conflicts` only after reviewing the conflict.

When a merge target also needs fields or tags changed, emit a separate `edit_location`
mutation for the target. The applicator validates the complete manifest and orders that
independent target edit before every dependent merge, so both mutations remain
separately guarded and idempotent.

Use the snapshot shape emitted by existing Location generators: Location and Region IDs
and names, minimum and maximum Period IDs and names, and stratigraphic-unit ID and name.
An `edit_location` row uses `changes` entries with `field`, `old_value`, and
`new_value`, plus optional serialized `add_tags` and `remove_tags`. Location actions may
also carry an optional top-level `review_note` for an important caveat that should be
printed in full by `--review-manual` without making the action non-executable.

### Type-locality actions, schema version 3

- `create_location`: create a fully described target Location and move a Name to it;
  identical repeated target definitions create one Location.
- `move_existing_location`: move a Name to a validated existing Location.
- `add_imprecise_locality`: add `TypeTag.ImpreciseLocality` without moving the Name.
- `manual_review`: retain an unresolved recommendation and its evidence.
- `no_action`: record why no type-locality change is recommended.

Any Location or type-locality action may carry an optional top-level `review_note`. Use
it when an otherwise executable recommendation has an important issue that the reviewer
should see separately from its ordinary rationale. `--review-manual` prints these
actionable notes alongside unresolved `manual_review` rows without preventing
application of the action.

Snapshot the Name ID and label and its current Location ID, name, and status tags. A
target includes Location and Region identity; coordinates and source note; temporal and
stratigraphic context; legacy `General` or `Unplaced` tags; and
`serialized_location_tags` for PLSS, coordinate provenance, external IDs, ignores, and
other tags. Preserve exact source wording in evidence.

## Extending the action set

This list describes the actions currently implemented, not a closed schema. If a task
does not fit them cleanly, add an action instead of forcing the change into a misleading
existing action:

1. implement a parser, stale-state validator/planner, review output, and dry-run/apply
   executor in an appropriate backend module under `taxonomy/applicator/`;
2. register its action name in `scripts/apply_recommendations.py`;
3. reject duplicate or interacting mutations that cannot be applied safely;
4. add focused parsing, stale-state, idempotence, dry-run, and application tests; and
5. add the new action to this list.

Keep specialized actions when they encode useful semantics or make a multi-record change
safer; generic actions are not a reason to discard those guardrails.

## Guardrails

- Treat `manual_review` as a legitimate result. Do not manufacture a target to maximize
  actionable rows.
- Preserve source wording and provenance. Put standardized interpretations on the
  database object without rewriting the quoted evidence.
- Require exact support for source-derived coordinates and include the corresponding
  provenance tag.
- Reuse an existing object only after checking all relevant context, not merely its
  label.
- Make the complete file validate before any write and keep all actions idempotent.
- Run focused tests, Ruff, formatting, and mypy before handoff.

## Standard of work

When asked to generate recommendations for a category of issues, do not stop halfway,
but review all issues in the category and attempt to generate recommendations. You have
the option to leave some issues for manual review, but use this option sparingly. If
there is a large number of issues for which you'd like to ask for manual review,
consider if there is any general policy that could resolve many of the issues, and ask
the user for a decision on that policy.
