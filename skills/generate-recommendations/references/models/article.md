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

## Replacing an incomplete or corrupt PDF

`replace_article_file` (schema version 1) replaces the bytes of an existing electronic
PDF Article without creating a duplicate Article or changing its bibliographic fields.
Stage and visually verify the complete replacement in `new_path` or `downloads`; include
the original file's SHA-256 and byte size as stale-state guards:

```json
{
  "schema_version": 1,
  "action": "replace_article_file",
  "confidence": "high",
  "reason": "The installed PDF omits a source page; the replacement includes the complete paper.",
  "evidence": [
    {
      "kind": "source",
      "text": "Record acquisition URL, source pagination, and image verification here."
    }
  ],
  "article": { "id": 123, "name": "Example.pdf", "path": "Mammalia" },
  "old_file": { "sha256": "<old sha256>", "size": 12345 },
  "file": {
    "source_root": "new_path",
    "source_path": "Example-complete.pdf",
    "sha256": "<replacement sha256>",
    "size": 23456
  }
}
```

The Article's exact ID, filename, path, electronic-PDF kind, installed bytes and staged
bytes are checked before application. A source may be absent on a completed retry only
when the installed replacement and old-file backup both match. Paths must remain within
the configured roots, and conflicting file destinations or simultaneous Article field
mutations are rejected. Review and dry-run perform no filesystem or database writes.

On explicit application, the action preserves the old bytes in
`new_path/Not to be cataloged/replaced-article-<id>-<old sha256>.pdf`, installs the
verified replacement through a temporary file, and refreshes cached PDF text and search
indexing. Only after successful indexing is the intake source removed. If indexing
fails, the replacement, backup and intake source remain recoverable and the same row can
be retried. The backup is the disposition of the corrupt original; do not add a second
file-move row for it. Record every other campaign-created intake file's disposition as
usual.
