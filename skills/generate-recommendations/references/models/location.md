# Location recommendations

Read this file for Location edits, renames, merges, or geographic reconciliation.
Consult [docs/location.md](../../../../docs/location.md) for naming conventions and
model semantics. For changes to a Name's type locality, also read the
[Name reference](name.md#type-locality-actions-schema-version-4).

## Actions, schema version 1

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

## Geographic evidence and handoff

Define geographic campaign scope as described in the
[Region reference](region.md#geographic-campaign-scope). Require exact support for
source-derived coordinates and include the corresponding provenance tag. Match
geographic and temporal context before reusing a Location; check valid, alias, and
deleted records before proposing a new one.

Distinguish affected Names, action rows, and newly proposed Locations when reporting
counts. Give useful geographic breakdowns and state whether remaining counts describe
live state or the expected state after application.
