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
   inference and retain uncertainty. Define the complete directly affected object set
   before writing rows. When a source or newly created object makes straightforward
   related changes evident, inspect those existing objects too—for example, connect
   Names to their original Article and add source-quoted locality or specimen details
   supplied by the same publication. Include the guarded companion rows unless the user
   explicitly narrows scope; do not stop at the motivating object while leaving obvious
   source-backed relationships unresolved.
3. Write `recs/scripts/generate_<topic>_recommendations.py` when generation is repeated
   or database-dependent. Generators may read but must never write the database. This
   directory is intentionally untracked; keep durable applicators in `scripts/`.
4. Write a new topic- or round-specific JSONL under `recs/manifests/`, which is also
   intentionally untracked. Do not revise a manifest that the user has already applied;
   start another file. When a prior manifest has been applied only partly, rebuild the
   follow-up from the current database and filesystem state so already-completed rows
   remain retry-safe and stale assumptions are rejected.
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
       --virtual-lint-issues-only
   ```

   Virtual lint is advisory because database-wide queries cannot see newly proposed
   objects, but it catches ordinary field, tag, relationship, and model-lint problems
   before the user writes anything. The recommended issues-only mode omits autofixable
   findings and prints only `VIRTUAL_LINT_ISSUES`; no output means none remain.

   Use `--virtual-lint` only when diagnosing the complete lint process. Its
   `VIRTUAL_LINT_AUTOFIXABLE`, `VIRTUAL_AUTOFIX_SIMULATED`, and
   `VIRTUAL_AUTOFIX_DEFERRED` findings are derived lint work and must not be duplicated
   as explicit manifest edits. Only `VIRTUAL_LINT_ISSUES` requires a manifest change, an
   explicit suppression, or manual review.

   Static review keeps one summary row per recommendation and adds indented, untruncated
   details: generic creation fields and match guards, schema-v2 guarded operations,
   Article and Taxon creation fields, Location and type-locality changes, coverage
   guards and members, and the evidence attached to every `manual_review` row. To limit
   the review to particular actions, repeat `--review-action`:

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
   `--review-each`, post-apply cleanup, and finally `--edit-manual`. For example,
   `--review-manual --review-each --edit-manual` prints the complete unresolved notes,
   reviews every row interactively, applies the accepted subset, and then opens all
   manual-review objects. Only `--apply` and `--review-each` are incompatible because
   they respectively mean applying every row and selecting individual rows.

   To work through unresolved rows interactively after validating the complete manifest,
   print each full manual-review note and open its database object editor:

   ```bash
   python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --edit-manual
   ```

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

Schema-version 2 `update_object` combines guarded scalar and ADT changes. Its ordinary
operations are `set`, `add`, and `remove`. Use `remove_raw` only to recover an ADT field
containing one exact malformed serialized tag that ordinary decoding cannot load. Supply
that exact list as `raw_value`; it must be the first change to that field, and the
remaining field must decode successfully. Follow it with an ordinary `add` when moving
the information to the correct tag field or ADT class.

Schema-version 2 `merge_collection` redirects one duplicate Collection to a canonical
target and atomically applies explicit guarded `object_updates`. Each nested update has
an existing `object` and one or more `set` changes with complete `old_value` and
`new_value` snapshots. Complete ADT-field snapshots are lists of serialized tags. The
planner checks indexed ordinary backreferences and rejects the merge unless each is
covered by an update whose new value no longer contains the source. It deliberately does
not scan serialized ADT fields; ordinary post-apply lint/autofix handles those
references. Include `type_specimen` text changes in the same action because Collection
lint enforces that they match the canonical label, and `Collection.merge()` does not
rewrite identifiers. If the target itself needs renaming or other metadata changes, put
a guarded `update_object` immediately before the merge.

Schema-version 2 `merge_person` combines two records for the same human identity. It
requires complete source and target Person-field snapshots plus guarded ID lists for
every Article, Name, Book, patronym, collector, involvement, and redirect reference. The
action moves all references to the canonical target, unions compatible tags and
biographical identifiers, clears transferred metadata from the source, and retains the
source name as a hard redirect. If the union contains multiple ORCIDs, it records a
reviewed `IgnoreLint("multiple_orcids")` tag on the canonical Person. Planning rejects
stale reference sets, redirect targets, and conflicting birth, death, biography, or Open
Library identifiers.

Schema-version 2 `reassign_person_references` handles an identity-safe subset of a
potential Person merge without redirecting the source. It requires complete source and
target Person-field snapshots plus guarded ID lists for every Article, Name, Book,
patronym, collector, and involvement reference. The action moves those current
references and one specified ORCID from the source to the target, but otherwise leaves
the source as an ordinary Person. Use it for ambiguous abbreviated forms such as
`C. Jones`: reviewed existing references may belong to `Craig M. Jones`, while a future
`C. Jones` reference may denote somebody else.

Schema-version 2 `merge_region` moves ordinary database references from one existing
Region to another and converts the source into a redirect whose `parent` is the target.
The planner rejects deleted sources, invalid targets, redirects to a different target,
and descendant cycles. A guarded `update_object` may rename or retag the target earlier
in the same manifest; use the target's proposed label in `merge_region`. Region tags on
the source are cleared because they describe the obsolete geographic identity.

Schema-version 2 `delete_region` marks one unreferenced existing Region as deleted and
clears its tags while retaining its database identity. Planning rejects redirects and
any Region that has a valid ordinary reference; execution repeats the same check through
`Region.remove()`. Its nonempty `guard` mapping snapshots identifying fields such as
kind, parent, comment, and tags before deletion. Use `merge_region` instead when the
obsolete name denotes the same geographic entity as a valid canonical Region.

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

### ItemFile actions, schema version 1

- `create_item_file`: create one ItemFile for an existing or inline new CitationGroup
  and install a staged PDF in the configured ItemFile directory without imposing Article
  filename conventions.

The action owns a checksum- and size-guarded file move, so do not combine generic
`create_object` with an unguarded filesystem operation. Its `item_file` object contains
`filename`, a `citation_group` ID/name snapshot or the same inline
`{name, type, region, tags}` definition used by `create_article`, optional scalar
`fields`, and serialized `tags`. Article and ItemFile rows may share identical inline
definitions; planning rejects conflicts and application creates each new group once. Its
`file` object contains a normalized `source_path` relative to `new_path`, `sha256`, and
`size`. Planning validates both source and destination and is restart-safe if the file
installation or database creation completed first. The user must authorize `--apply`;
generators and skills must not apply the row themselves.

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

### Type-locality actions, schema version 4

- `create_location`: create a fully described target Location and move a Name to it;
  identical repeated target definitions create one Location.
- `move_existing_location`: move a Name to a validated existing Location.
- `add_imprecise_locality`: add `TypeTag.ImpreciseLocality` without moving the Name.
- `set_partial_type_localities`: move a syntype or type-kind-unset Name to a Region-wide
  Recent or `<Region> fossil` Location (existing, newly created, or revived with
  `Location.get_or_create_general`) and replace its guarded `PartialTypeLocality` set
  with at least two existing or newly defined component Locations.
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
- Preserve source wording and provenance. The text of every `*Detail` tag is a direct
  quotation from its cited source, never a paraphrase or inference. Put standardized
  interpretations on structured fields or non-Detail comments without rewriting the
  quoted evidence.
- Treat completeness as part of correctness. For every created or newly connected
  source, enumerate the directly related existing objects reviewed and include all
  obvious evidence-backed companion changes (`original_citation`, `LocationDetail`,
  `SpecimenDetail`, and analogous fields or tags). If an applicator dependency prevents
  the changes from sharing one file, produce a named follow-up manifest after the
  dependency is applied or add an explicit `manual_review`; never omit them silently.
- If a recommendation campaign stages files in an intake directory, finish with a
  checksum-backed disposition for every campaign-created file. Creation actions must
  consume cataloged sources; rejected, duplicate, superseded, or extraction-only files
  must use `move_to_not_cataloged` when supported. Re-inventory after application so a
  later intake scan cannot rediscover leftovers.
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
