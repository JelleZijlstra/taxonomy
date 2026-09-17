# Taxon recommendations

## Synonymize existing taxa

Use **`synonymize_taxon`**, schema version 2, to merge an existing Taxon into another.
Do not emulate it with separate Name moves and Taxon redirect-field changes. Application
calls `source.synonymize(target)`: it reparents children, moves valid Names and
occurrence records, handles legacy occurrence provenance, changes the source base Name
to synonym, preserves the target base Name's status, and leaves a Taxon redirect.

The target must be explicit. Interactive target selection is never invoked. This action
does not change other Names' statuses or their nomenclatural relationships.

Produce the guard from live read-only state:

```python
from taxonomy.applicator.taxon_synonymy import snapshot
from taxonomy.db.models import Taxon

source = Taxon(source_id)
target = Taxon(target_id)
row = {
    "schema_version": 2,
    "action": "synonymize_taxon",
    "confidence": "high",
    "reason": "Source-backed explanation of this synonymy.",
    "evidence": [{"kind": "source", "text": "Specific supporting evidence."}],
    "object": {"model": "Taxon", "id": source.id, "label": source.valid_name},
    "target": {"model": "Taxon", "id": target.id, "label": target.valid_name},
    "guard": snapshot(source, target),
}
```

`guard.source` and `guard.target` snapshot `age`, `rank`, `parent_id`, `base_name_id`,
`base_name_status`, and `data`. `guard.references` contains the exact source ID sets for
`names`, `children`, `occurrences`, and `occurrence_records`. Review the source's data
explicitly: the model method warns about it rather than transferring it to the target.

Planning rejects stale guards, self-merges, invalid targets, ancestry cycles, different
nomenclatural groups, duplicate rows, overlapping merges, and mutations of fields owned
by the merge. Keep companion evidence edits separate from taxon/status assignments; even
a no-op scalar guard on a merge-owned field conflicts, because generic updates write all
their supplied fields. New taxa cannot be created under a source being merged.

Completed merges are retry-safe after checking that the source redirects to the intended
target and every guarded reference has reached its expected destination. Duplicate
legacy Occurrences can remain on the redirect, as in `Taxon.synonymize`, provided the
target occurrence retains their provenance. Partial or subsequently altered states are
rejected.

Review, dry run, virtual lint, and the normal applicator approval flow are supported.
Virtual lint projects the affected Names, children, occurrence records, and legacy
occurrence comments; it never calls the mutating method. Only `--apply` executes the
merge.
