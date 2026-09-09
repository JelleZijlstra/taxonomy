# Article recommendations

Read this file when generating Article recommendations or referencing an Article's
authors from other rows. General Article semantics remain in
[docs/article.md](../../../../docs/article.md).

## Creation

`create_article` (schema version 1) creates one electronic Article, optionally creates
its ordinary CitationGroup, creates unchecked Persons for its authors, installs a staged
PDF, and runs PDF text extraction and search indexing.

Use the [add-article skill](../../../add-article/SKILL.md) to research and generate this
action. It owns a checksum- and size-guarded file move, so do not emulate it with
generic `create_object`. Static review does not access the database or network; planning
expands DOI metadata and validates the database, CitationGroup, existing library folder,
staged PDF, and final destination. Explicit Article metadata overrides CrossRef. The
user must authorize `--apply`; generators and skills must not apply the row themselves.

## Author references

A `create_article` recommendation can declare a `ref` on an author, alongside either
name fields or an existing `person` ID/name guard:

```json
{ "family_name": "Yang", "given_names": "Rongsheng", "ref": "author:yang" }
```

Later rows can reference that Person as `{"model": "Person", "ref": "author:yang"}`.
Article intake owns the author's creation or reuse; do not also emit a `create_object`
Person row for that author. The ref identifies the same author during virtual review,
application, and reuse of an already installed Article. Article and author refs share
one namespace and must be unique within the manifest.

## Intake disposition

If a campaign stages files in an intake directory, finish with a checksum-backed
disposition for every campaign-created file. Creation actions must consume cataloged
sources; rejected, duplicate, superseded, or extraction-only files must use
`move_to_not_cataloged` when supported. Re-inventory after authorized application so a
later intake scan cannot rediscover leftovers. The add-article skill describes the
file-action schemas and intake audit.
