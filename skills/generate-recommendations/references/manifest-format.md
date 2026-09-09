# Generic recommendation format

Read this file when choosing generic actions or writing their fields, typed references,
and stale-state guards. Prefer specialized actions where they enforce required
multi-object validation; see the model index in [SKILL.md](../SKILL.md).

## Generic actions, schema version 1

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

## Values, references, and examples

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
`manual_review` uses the
[Name-specific schema](models/name.md#type-locality-actions-schema-version-4).

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
