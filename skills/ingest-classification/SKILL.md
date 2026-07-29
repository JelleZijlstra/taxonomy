---
name: ingest-classification
description:
  Transcribe a taxonomic classification from one or more sources into a reviewable JSONL
  CE file, optionally including source-backed occurrence claims and Location proposals,
  and prepare it for a later human-controlled database import. Use for
  ClassificationEntry ingestion, classification tables or checklists, CEDict
  transcription, occurrence extraction, CE-file review, and dry-run or manual
  classification imports in the taxonomy repository.
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

A CE file may combine entries from multiple Articles. Keep each row's exact source in
its `article` field and include a complete source-local hierarchy for every Article;
parent validation, uncovered-entry reporting, and CE formatting are performed
independently for each Article. This is useful for a species-focused ingestion assembled
from several papers while retaining record-level provenance.

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
Article, Name, Taxon, or Location database lookup merely to transcribe a source. Region
names in Location proposals are the exception; validate them as described below.

Treat `corrected_name` as a legacy name for `normalized_name`. Set it only when the
source representation does not follow the standard scientific-name format, for example
`R. rattus` instead of `Rattus rattus`. Do not use it to fix a misspelling, apply an
emendation, modernize a combination, or resolve a synonym. A normally formatted but
misspelled name such as `Rattus norvegecus` must remain unaltered for later review.

Include the relevant classification hierarchy even for a short source: add the family
and genus entries needed to place a species, order parents before children, and include
both `parent` and `parent_rank`. Writing the JSONL directly changes only how the
artifact is produced; it does not reduce the classification represented in it.

Every ClassificationEntry `page` must be a single page number. Do not use a range,
comma-separated list, or compound value such as `755-756` or `755–756`. When an account
spans pages, use the single page on which the classification heading or entry begins;
keep evidence from later pages in occurrence-level provenance or comments as needed.

### Add occurrence claims when the source provides them

Put each source claim under the relevant CEDict's optional `occurrences` list. The
required occurrence fields are:

- `locality`: the source's locality wording, transcribed faithfully;
- `basis`: one of `voucher`, `observation`, or `listing`.

Optional internal fields are `page`, `raw_data`, and source-evidence tags such as
`ObservationKind`, `MolecularData`, `Voucher`, `SpecimenDetail`, or `CommentFromSource`.
Optional external interpretation uses `mapped_location`, containing the intended
canonical `Location.name`. Status tags such as `Vagrant`, `Introduced`, `Extirpated`,
`OccurrenceDubious`, `ClassificationDubious`, and `Rejected` are also external
interpretations. Do not put `TaxonomicSplitFrom` in a CE file; create split records
interactively after import.

When a source identifies catalogued specimens for a voucher occurrence, add one
`Voucher` tag per specimen. Preserve the catalogue string given by the source in the
tag's `text` field and resolve its repository to the corresponding database
`Collection`; do not combine multiple catalogue numbers into one tag. In an extraction
script, for example, use
`OccurrenceRecordTag.Voucher("MUSM 19358", models.Collection.by_label("MUSM"))` and let
`write_ce_file()` serialize it. The `Voucher` tag complements `basis: "voucher"`, which
records the evidence class but not the specimen identity. Use `SpecimenDetail` for
additional source-backed specimen information that is not captured by the catalogue
string and collection.

For an import-oriented ingestion, every extant occurrence must have a `mapped_location`.
Map it to the most precise Location justified by the source:

- use the specific collecting locality when the source provides one;
- use a named parish, province, island, or other geographic region when that is all the
  source supports;
- when an island-focused account says only “no specific locality,” “parish unknown,” or
  equivalent, map it to the island Location because the source still establishes the
  island;
- use the existing Location `Unknown (extant)` only when neither the occurrence wording
  nor the unambiguous source context supplies any geographic evidence.

Do not leave `mapped_location` null merely because the locality is imprecise, and do not
invent greater precision than the source provides. Preserve the source wording unchanged
in `locality`. Include each imprecise mapped name in the sibling Location plan so the
read-only preview can confirm that the intended existing Location is reused or that a
new imprecise Location is safe to create.

For example:

```json
{
  "locality": "5 km N of Quito",
  "page": "42",
  "basis": "observation",
  "mapped_location": "Quito: 5 km N",
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

Do not query the database first to decide whether to write a proposal. Draft proposals
from the source and gazetteer, then perform the mandatory read-only collision audit
below. Propose the most precise level supported by the source and gazetteer, and define
each proposed canonical Location once even if many occurrences use it.

Validate every proposed `region` against the database's Region vocabulary. Do not assume
that a political subdivision named by the source is a Region in the database. Either
query `models.Region` directly for the exact name or consult `docs/geography.md`. Use
the smallest valid Region that contains the locality; when a finer subdivision is not a
Region, retain it in `location_detail` and use the enclosing valid Region, often the
country. Include that subdivision in the Location `name` only when it is part of the
geographic name or is needed for disambiguation. This validation is separate from
checking whether the proposed Location itself already exists.

Follow the Location naming convention in `docs/location.md`:
`Name (disambiguator): modifier`. Keep the base `Name` as short and geographic as
possible. Do not append the Region merely to make the name globally unique: use
`Brimstone Hill`, not `Brimstone Hill, Saint Kitts`. Put the containing Region in
`region`, and put coordinates, elevation, source wording, and other identifying evidence
in their structured fields or `location_detail`. For a source locality defined by a
coordinate pair, the standardized coordinates may also be used as the modifier, for
example `Vilacota, Tacna, Peru: 17.145759°S 70.054278°W`; they must match the proposed
Location's structured coordinates. Add a parenthetical disambiguator such as
`Brimstone Hill (Saint Kitts)` only when an actual same-named Location makes it
necessary. Put relative distance, direction, or other distinguishing locality text after
a colon, as in `Quito: 5 km N` or `Foo River (California): mouth`. The occurrence's
`mapped_location` must exactly match the proposed canonical name.

When an exact-name Location already exists but is not the intended place because its
Region or period is incompatible, rename the proposal before handoff. Use a
parenthetical geographic disambiguator, normally the proposal's Region:
`La Vega (Dominican Republic)`, not `La Vega, Dominican Republic`. If that Region is too
broad to distinguish the places, use the smallest stable containing island or
administrative geography supported by the source and the database. A Period
disambiguator is appropriate only when it distinguishes Locations with different
assigned ages. Do not use source citations, coordinates, or specimen data as
disambiguators. Apply the renamed value consistently to the Location proposal and every
occurrence's `mapped_location`.

Parenthetical phrases should only be used for disambiguation. If a location is given
relative to a named place, use a location of the form `Quito: 5 km N`, not
`5 km N Quito` or `Quito (5 km N)`.

## 3. Review the source artifacts

For the normal source-processing workflow, review only what can be established from the
source and the files themselves:

- verify that every nonblank line is valid JSON and uses the expected field shapes;
- compare names, ranks, pages, locality wording, evidence basis, and quotations with the
  source;
- verify that every ClassificationEntry has one page number, never a page range or list;
- check source-internal hierarchy and repeated-entry invariants when applicable;
- ensure any Location proposal is supported by the source or cited gazetteer.
- ensure Location names follow the convention in `docs/location.md`;
- ensure Location names are minimal and do not repeat their Region unless the mandatory
  collision audit finds a real conflict requiring a parenthetical disambiguator;
- verify that every Location proposal uses a valid Region name, using the database or
  `docs/geography.md`.
- verify that every extant occurrence has a `mapped_location`, using an imprecise
  geographic Location or `Unknown (extant)` according to the rules above.

Other than validating Region names and auditing proposed Location names as above, do not
require reconciliation against the current database at this stage. Taxon/name matching
and Article reconciliation remain questions for the later database-aware phase.

## 4. Audit proposed Location names and preview when requested

Whenever a sibling `.locations.jsonl` is present, always run this read-only command
before finalizing the artifacts, even if the user requested only transcription:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --verbose
```

Without `--apply`, this command is read-only. It resolves the Article; validates JSON
fields, enums, and source hierarchy; reports exact, normalized, ambiguous, and
unrecognized matches against existing `Name` records; validates occurrence claims; reads
the sibling Location file when present; and previews changes through
`add_classification_entries(..., dry_run=True)`. It lists every mapped Location with its
status and reports Locations that already exist, would be restored or created, remain
unresolved, or conflict.

Treat every exact-name Location conflict as an artifact defect to resolve before
handoff. If the existing Location has an incompatible Region or period and is not the
same place, add the standard parenthetical geographic disambiguator described above,
update all matching `mapped_location` values, regenerate if applicable, and rerun the
preview. Do not finish while a proposed Location still has an avoidable exact-name
conflict. Do not rename a proposal when the exact existing Location is genuinely the
same place and can be reused.

Treat database match results as reconciliation items, not as reasons to alter a
source-faithful transcription automatically. Ambiguous and unrecognized mappings may be
source misspellings or genuinely absent database names.

## 5. Hand off the human import

Only a human may run the database-writing form:

```bash
/Users/jelle/py/venvs/taxonomy314/bin/python scripts/import_ce_file.py PATH.ce.jsonl --apply
```

The apply path is idempotent for the same CE file. It first creates reviewed missing
Locations, then matches or adds CEs independently for each Article, and finally matches
or adds OccurrenceRecords. It formats and runs the full model lint cycle on created or
updated Locations and OccurrenceRecords, and calls `format_ces_in_article()` for every
Article represented in the file. It fills null external taxon and location mappings but
does not overwrite differing mappings or internal source data. An unresolved
`mapped_location` remains a `LocationHint` tag and a null location so the lint remains
visible. Existing Location conflicts block apply.

Ambiguous and unrecognized Name matches remain visible in the validation report but do
not require a separate command-line acknowledgement. Review them before applying; unlike
imperfect Name matches, unresolved Location proposal conflicts still block apply.

Never run `--apply` on the user's behalf unless they explicitly instruct you to perform
the database import.

## Required final response

After a source-only transcription, report the artifact paths, summarize what was
transcribed, state that taxonomy matching was intentionally deferred, and report the
result of the mandatory Location collision audit when Location proposals were produced.
Do not imply that taxonomy matching was adjudicated merely because the read-only preview
was used for Location QA.

When the user explicitly requests a database-aware ingestion handoff, include the actual
repository-relative path in the read-only preview command and the manual `--apply`
command.
