---
name: ingest-classification
description:
  Transcribe a taxonomic classification from a source into a reviewable JSONL CE file,
  optionally including source-backed occurrence claims and Location proposals, and
  prepare it for a later human-controlled database import. Use for ClassificationEntry
  ingestion, classification tables or checklists, CEDict transcription, occurrence
  extraction, CE-file review, and dry-run or manual classification imports in the
  taxonomy repository.
---

# Ingest a classification

Keep source transcription, database reconciliation, and database mutation separate.

## 1. Choose direct JSONL or an extraction script

For a short, straightforward source with only one or a few entries, write the JSONL
artifact directly. Do not create a source-specific Python script merely to construct a
small fixed list of dictionaries.

Create an extraction script only when code materially helps parse or transform the
source—for example, a long classification, repeated records, multi-column text, a table
requiring normalization, or invariants that should be asserted across many entries. Put
such scripts in `data_import/`, use the newer `CEDict` abstraction, and write generated
files with `data_import.ce_file.write_ce_file()`.

Put CE files ending in `.ce.jsonl` in `data_import/ce_files/`. Each line is one JSON
object representing a `data_import.lib.CEDict`. For direct JSONL, use enum member names
such as `species` and serialize tags as objects with `kind` and `data` keys.

Never write to the database during transcription. Do not call
`add_classification_entries(..., dry_run=False)` from a source-specific script.

## 2. Transcribe the source

When the user supplies a brace-delimited source such as `{Agathaeromys nov.pdf}`,
resolve it from the repository root before reading it:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/article_info.py \
  '{Agathaeromys nov.pdf}'
```

Use `query_article.name` for the CE `article` field, even when `file.article_name` is a
parent volume. Read `file.path` or the existing `extracted_text.path`; rerun with
`--extract` only when cached text is needed. Do not call `get_path()` on a
non-electronic child Article.

Read the source closely. Render PDF pages when columns, tables, typography, or page
numbers matter. Preserve source spelling in `name`, including errors. Store the
Article's exact `name` in `article` when it is already known, but do not require an
Article, Name, Taxon, Region, or Location database lookup merely to transcribe a source.

Treat `corrected_name` as a legacy name for `normalized_name`. Set it only when the
source representation does not follow the standard scientific-name format, for example
`R. rattus` instead of `Rattus rattus`. Do not use it to fix a misspelling, apply an
emendation, modernize a combination, or resolve a synonym. A normally formatted but
misspelled name such as `Rattus norvegecus` must remain unaltered for later review.

Include the relevant classification hierarchy even for a short source: add the family
and genus entries needed to place a species, order parents before children, and include
both `parent` and `parent_rank`. Writing the JSONL directly changes only how the
artifact is produced; it does not reduce the classification represented in it.

### Add occurrence claims when the source provides them

Put each source claim under the relevant CEDict's optional `occurrences` list. The
required occurrence fields are:

- `locality`: the source's locality wording, transcribed faithfully;
- `basis`: one of `voucher`, `observation`, or `listing`.

Optional internal fields are `page`, `raw_data`, and source-evidence tags such as
`ObservationKind`, `MolecularData`, `SpecimenDetail`, or `CommentFromSource`. Optional
external interpretation uses `mapped_location`, containing the intended canonical
`Location.name`. Status tags such as `Vagrant`, `Introduced`, `Extirpated`,
`OccurrenceDubious`, `ClassificationDubious`, and `Rejected` are also external
interpretations. Do not put `TaxonomicSplitFrom` in a CE file; create split records
interactively after import.

For example:

```json
{
  "locality": "5 km N of Quito",
  "page": "42",
  "basis": "observation",
  "mapped_location": "5 km N of Quito, Pichincha, Ecuador",
  "tags": [{ "kind": "ObservationKind", "data": [1, 1] }]
}
```

In a justified extraction script, use `OccurrenceRecordTag` constructors and let
`write_ce_file()` serialize them. In direct JSONL, write the serialized tag form shown
above. Never replace the verbatim `locality` with a gazetteer result.

### Propose precise Locations separately

When a source supplies enough information to define a precise Location, optionally write
a sibling file named `PATH.locations.jsonl` next to `PATH.ce.jsonl`. A proposal requires
the intended `name`, `region`, and `period`; it may also include `latitude`,
`longitude`, `source`, `location_detail`, `comment`, and Location tags. These values are
strings naming the database objects that should be resolved later. Store gazetteer
identifiers, URLs, uncertainty, and method details in `location_detail` when they do not
fit a structured Location field.

Do not query the database first to decide whether to write a proposal. It is fine to
propose a Location optimistically: the database-aware preview will later reuse an exact
existing Location or report unresolved names and conflicts. Propose the most precise
level supported by the source and gazetteer, and define each proposed canonical Location
once even if many occurrences use it.

## 3. Review the source artifacts

For the normal source-processing workflow, review only what can be established from the
source and the files themselves:

- verify that every nonblank line is valid JSON and uses the expected field shapes;
- compare names, ranks, pages, locality wording, evidence basis, and quotations with the
  source;
- check source-internal hierarchy and repeated-entry invariants when applicable;
- ensure any Location proposal is supported by the source or cited gazetteer.

Do not require checks against the current database taxonomy or Location table at this
stage. In particular, do not run `scripts/import_ce_file.py` merely to finish a
transcription task. Taxon/name matching, Article resolution, and Location reuse are
questions for the later database-aware phase.

## 4. Preview against the database when requested

When the user explicitly asks to prepare or preview the actual database ingestion, run:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --verbose
```

Without `--apply`, this command is read-only. It resolves the Article; validates JSON
fields, enums, and source hierarchy; reports exact, normalized, ambiguous, and
unrecognized matches against existing `Name` records; validates occurrence claims; reads
the sibling Location file when present; and previews changes through
`add_classification_entries(..., dry_run=True)`. It reports Locations that already
exist, would be created, remain unresolved, or conflict.

Treat database match results as reconciliation items, not as reasons to alter a
source-faithful transcription automatically. Ambiguous and unrecognized mappings may be
source misspellings or genuinely absent database names.

## 5. Hand off the human import

Only a human may run the database-writing form:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --apply
```

The apply path is idempotent for the same CE file. It first creates reviewed missing
Locations, then matches or adds CEs, calls `format_ces_in_article()`, and finally
matches or adds OccurrenceRecords. It fills null external taxon and location mappings
but does not overwrite differing mappings or internal source data. An unresolved
`mapped_location` remains a `LocationHint` tag and a null location so the lint remains
visible. Existing Location conflicts block apply.

If the preview contains ambiguous or unrecognized mappings that the human has reviewed
and accepted, the writing command must explicitly acknowledge them:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --apply --allow-imperfect-matches
```

Never run `--apply` on the user's behalf unless they explicitly instruct you to perform
the database import.

## Required final response

After a source-only transcription, report the artifact paths, summarize what was
transcribed, and state that database matching was intentionally deferred. Do not claim
that the taxonomy or Locations were validated against the database.

When the user explicitly requests a database-aware ingestion handoff, include the actual
repository-relative path in the read-only preview command and the manual `--apply`
command. If accepted imperfect mappings require `--allow-imperfect-matches`, include
that flag and explain why.
