# Region recommendations

Read this file for Region merges, deletion, or geographic campaign scope. For model
semantics, consult [docs/region.md](../../../../docs/region.md).

## Merge or delete regions

Schema-version 2 `merge_region` moves ordinary database references from one existing
Region to another and converts the source into a redirect whose `parent` is the target.
The planner rejects deleted sources, invalid targets, redirects to a different target,
and descendant cycles. A guarded `update_object` may rename or retag the target earlier
in the same manifest; use the target's proposed label in `merge_region`. Region tags on
the source are retained as provenance for the obsolete geographic identity.

Schema-version 2 `delete_region` marks one unreferenced existing Region as deleted and
retains its tags and database identity. Planning rejects redirects and any Region that
has a valid ordinary reference; execution repeats the same check through
`Region.remove()`. Its nonempty `guard` mapping snapshots identifying fields such as
kind, parent, comment, and tags before deletion. Use `merge_region` instead when the
obsolete name denotes the same geographic entity as a valid canonical Region.

## Geographic campaign scope

State whether scope follows the database Region subtree or physical geography; use the
user's choice, including outlying descendants. Scope selects the records to inspect, not
necessarily the destinations of corrections. For Location redistribution, also read the
[Location reference](location.md).
