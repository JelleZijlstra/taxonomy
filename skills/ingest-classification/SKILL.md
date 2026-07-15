---
name: ingest-classification
description:
  Extract a taxonomic classification from a source into a reviewable JSONL CE file,
  validate its hierarchy and name mappings, and prepare it for a human-controlled
  database import. Use for ClassificationEntry ingestion, classification tables or
  checklists, CEDict transcription, CE-file validation, and dry-run or manual
  classification imports in the taxonomy repository.
---

# Ingest a classification

Keep source transcription, automated checking, and database mutation separate.

## 1. Create the CE file

Read the source closely. Render PDF pages when columns, tables, typography, or page
numbers matter. Preserve source spelling in `name`, including errors.

Treat `corrected_name` as a legacy name for `normalized_name`. Set it only when the
source representation does not follow the standard scientific-name format, for example
`R. rattus` instead of `Rattus rattus`. Do not use it to fix a misspelling, apply an
emendation, modernize a combination, or resolve a synonym. A normally formatted but
misspelled name such as `Rattus norvegecus` must remain unaltered and be reported by
validation.

Produce JSONL with one serialized `data_import.lib.CEDict` per line. Use enum member
names such as `species`, store the Article's exact `name` in `article`, order parents
before children, and include `parent` plus `parent_rank`. Use
`data_import.ce_file.write_ce_file()` when generating the file from Python. Put
source-specific extraction scripts in `data_import/` and generated files ending in
`.ce.jsonl` in `data_import/ce_files/` so they remain reviewable, versioned artifacts.

Never write to the database during extraction. Do not call
`add_classification_entries(..., dry_run=False)` from a source-specific script.

## 2. Validate and preview

Run:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --verbose
```

Without `--apply`, this command is read-only. It resolves the Article, checks JSON
fields and enums, runs `validate_ce_parents()`, reports exact, normalized, ambiguous,
and unrecognized matches against existing `Name` records, and previews the changes
through `add_classification_entries(..., dry_run=True)`. Treat every non-exact result as
a review item. Compare questionable spellings to the rendered source before editing.

Ambiguous and unrecognized mappings do not prevent a preview. They may be source
misspellings or genuinely absent database names and do not necessarily mean the
transcription is wrong.

## 3. Hand off the human import

Only a human may run the database-writing form:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --apply
```

The apply path is idempotent for the same CE file: existing entries are matched and
updated instead of inserted again. After the add/update pass, it calls
`format_ces_in_article()` to format and interactively run `edit_until_clean()` on every
CE in the article.

If the preview contains ambiguous or unrecognized mappings that the human has reviewed
and accepted, the writing command must explicitly acknowledge them:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --apply --allow-imperfect-matches
```

Never run `--apply` on the user's behalf unless they explicitly instruct you to perform
the database import.

## Required final response

After creating or updating a CE file, always end the response with both commands below,
replacing `PATH.ce.jsonl` with the actual repository-relative path:

```text
Preview and validate (read-only):
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --verbose

Import into the database (writes, then interactively lints all CEs in the article; run manually only):
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --apply
```

If accepted imperfect mappings require `--allow-imperfect-matches`, include that flag in
the returned import command and explain why it is required.
