# Collection recommendations

Read this file for Collection merges and their dependent Name changes. For model
semantics, consult [docs/collection.md](../../../../docs/collection.md).

## Merge collections

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
