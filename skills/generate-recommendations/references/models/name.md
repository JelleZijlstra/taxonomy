# Name recommendations

Read this file for Name evidence, enrichment, and type-locality recommendations. Consult
[docs/name.md](../../../../docs/name.md) for model semantics; use the
[generic manifest format](../manifest-format.md) for ordinary field and tag changes.

## Evidence and companion changes

`InterpretedTypeTaxon` is restricted to genus- and family-group names and explains
type-species or type-genus selection. Do not use it for species identifications or
synonymies. Preserve those interpretations in NameComments or recommendation evidence.

For every created or newly connected source, enumerate the directly related existing
Names and include obvious source-backed companion changes such as `original_citation`,
`LocationDetail`, and `SpecimenDetail`. If a dependency prevents changes from sharing
one file, prepare a named follow-up after it is applied or record an explicit
`manual_review`. Do not silently omit companion work or predict deferred IDs.

## Type-locality research

For type-locality campaigns, read `docs/type-locality.md` and `docs/location.md`,
including the Location naming conventions. Start with stored `LocationDetail`,
`SpecimenDetail`, and type-designation evidence. Exact coordinates or identification
with a modern mapped place are not prerequisites for a useful recommendation: a
consistently named but unidentified locality may support an `Unplaced` Location in the
narrowest evidenced Region. Distinguish this from a known broad area (`General`) and a
type locality for which no meaningfully narrower evidence is available
(`ImpreciseLocality`). Check existing Locations, aliases, and deleted records before
proposing a new Location, and match temporal as well as geographic context. Preserve
source wording in evidence while reviewing proposed Location names separately for
naming-convention compliance.

## Type-locality actions, schema version 4

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

To restore a reviewed deleted named Location, use a fully described target with
`location_id: null` and `restore_deleted_location_id` set to its existing ID. The
applicator validates the ID, name, Region, temporal context, and supplied coordinates,
then restores that record instead of creating another. Aliases cannot be restored this
way. A matching record already restored is reused, so retries preserve its identity.
