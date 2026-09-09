# ItemFile recommendations

Read this file when retaining a whole volume or issue with `create_item_file`. Use the
[add-article skill](../../../add-article/SKILL.md#whole-volume-and-issue-pdfs-itemfile)
for source intake, and follow the shared
[intake disposition rules](article.md#intake-disposition).

## Creation, schema version 1

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
