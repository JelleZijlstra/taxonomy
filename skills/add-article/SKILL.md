---
name: add-article
description:
  Find and verify an Article file, choose its catalog filename and library folder,
  resolve bibliographic metadata, CitationGroup, and edited-volume parent dependencies,
  and write reviewed create_article or create_item_file recommendations without changing
  the database. Use when adding a new electronic Article, book, dissertation, book
  chapter, or parented supplementary file to the taxonomy library, or retaining a whole
  journal volume or issue as an ItemFile.
---

# Add an Article

Prepare source-faithful electronic files and executable `create_article`,
`create_item_file`, or `move_to_not_cataloged` recommendations. Leave each input file in
its current configured intake folder and leave application to the user.

## Scope

Use this workflow for ordinary electronic Articles backed by PDFs and parented
supplementary Articles in their original electronic format. It supports:

- DOI-driven CrossRef metadata with explicit corrections;
- fully explicit metadata when no DOI exists;
- existing CitationGroups;
- creation of one ordinary CitationGroup inline; and
- edited-volume chapters whose parent is an existing Article or a no-copy BOOK created
  by an earlier row in the same manifest; and
- supplementary PDFs, spreadsheets, documents, and other catalog-supported files whose
  parent is an existing or planned Article; and
- unchecked Person creation from the Article's author list.

Whole journal volumes and issues use the
[ItemFile workflow](#whole-volume-and-issue-pdfs-itemfile) below, including source
volumes downloaded to extract individual Articles.

The only supported Article without an electronic file is a BOOK needed as the parent of
a staged chapter. Non-PDF files must be parented `SUPPLEMENT` Articles. Stop and explain
the unsupported case instead of forcing an alternative version, redirect, or other work
without an electronic copy into this action. The applicator also requires an existing
destination folder for every file; proposing new library-folder taxonomy is a separate
task.

## Workflow

### 1. Load the catalog rules

Read the
[Article recommendation reference](../generate-recommendations/references/models/article.md)
for manifest mechanics, including author refs shared with dependent rows.

Read these repository documents before choosing values:

- `docs/article.md` for Article semantics;
- `docs/article-naming.md` for the filename grammar;
- `docs/citation-group.md` when choosing or creating a CitationGroup;
- `docs/person.md` when author identity needs interpretation; and
- `docs/finding-citations.md` when literature discovery is part of the request.

Keep general rules in these documents. This skill owns only the operational sequence.

### 2. Establish source identity

Find the publisher or repository landing page and a downloadable source file. Prefer the
publisher, institutional repository, Biodiversity Heritage Library, or another stable
primary host. Record both the landing-page URL and the actual acquisition URL in
evidence.

Download into `config.get_options().new_path` or leave a file already present directly
under `config.get_options().downloads_path`; never download directly into the catalog
library. Use a temporary descriptive intake name; it does not have to equal the final
Article name. If the file is already present, inspect that exact file rather than
substituting a similarly titled source. Do not copy a Downloads file into `new_path`:
set `file.source_root` to `"downloads"` so successful application can remove the one
intake copy that `check_new()` sees.

For PDFs, open or render the first page and inspect extracted text. For supplements,
inspect the native file and its internal title or table caption. Confirm the title,
authors, publication, year, pagination, and DOI against the landing page. For a chapter,
also verify the editors, parent title, publisher, publication city, parent extent,
chapter page range, and whether separately numbered plates follow the text. A mismatch
between the requested work, landing page, and staged file is a hard stop. Do not
silently switch sources.

Treat the publication itself as the authority for its author list. Transcribe every
printed author's most detailed attested form, including full given names when the PDF
prints them. Do not mechanically reuse the abbreviated Persons attached to the Names
that motivated the search, and do not reduce a printed full name to initials merely
because the Name authority is abbreviated. Reuse a guarded existing Person only after
checking that it represents the same person and preserves at least as much detail as the
publication. Otherwise supply the fuller author components so application can create an
unchecked Person; leave uncertain identities separate.

Transcribe the complete publication title, not a shortened search label. Apply the
database's Markdown title convention while preserving wording: italicize scientific
genus- and species-group names (and other source elements conventionally requiring
italics) with underscores. Title typography is metadata, not a reason to alter the
printed spelling or capitalization beyond the ordinary rules in `docs/article.md`.

### 3. Check current database state

Use `CLIRM_READONLY=1` for every inspection. Check for:

- an existing Article with the DOI;
- an existing Article with the proposed name;
- an existing parent Article by both ID and exact name;
- the exact CitationGroup and its type;
- similar Articles whose paths reveal the established taxonomic and geographic filing
  convention; and
- the Region to use if a CitationGroup must be created.

Do not create or reconcile Persons during research. The action uses
`Person.get_or_create_unchecked()` at application time, after the row is approved.

### 4. Choose target and validate the Article name

Use `create_article` for a PDF containing a single publication. For a whole journal
volume or issue, follow the ItemFile workflow below instead of Article-specific steps
4–7. When extracting an Article from a whole-volume download (for example from Google
Books or BHL), retain the complete downloaded PDF and prepare an ItemFile recommendation
for it as well, unless an appropriate ItemFile already exists.

If a paper appeared in separate installments, preserve the publication units as separate
PDFs and verify each printed page range. Record source-PDF page indices in evidence;
they are not necessarily the printed page numbers. Do not infer publication months from
issue numbers alone.

To name the Article, choose a concise but representative description of the complete
publication under `docs/article-naming.md`. The source title is not the filename, but
the filename must describe the work's overall contents rather than only the taxon,
specimen, or nomenclatural act that prompted acquisition. Use a `nov` core only when it
fairly represents the publication as a whole; use the normal taxonomic, geographic,
temporal, or topic form for broader faunas, expedition reports, reviews, catalogues, and
multi-taxon papers. Record the reasoning for any non-obvious content scope in evidence.
Electronic Article names must be printable ASCII and have an extension. Ordinary
electronic Articles must end in lowercase `.pdf`; a `SUPPLEMENT` keeps the source format
extension. A no-copy parent BOOK must be printable ASCII and have no extension.

Validate it with the catalog parser:

```bash
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python - <<'PY'
from taxonomy.db.models.article.name_parser import get_name_parser

filename = "REPLACE.pdf"  # Or an extensionless no-copy parent name.
errors = get_name_parser(filename).get_errors()
assert not errors, errors
PY
```

### 5. Choose the destination folder

For each staged file, select an existing folder relative to
`config.get_options().library_path`. Follow nearby Articles rather than inventing a
parallel hierarchy. Taxonomic placement is normally primary; use geography where that is
how comparable regional works are organized. For broad works, identify the narrowest
existing folder that accurately represents the main coverage without hiding important
scope.

The manifest records only the relative folder, never an absolute path. Verify that the
resolved folder exists. A no-copy parent has no `file` object and no destination. Do not
move any file yet.

### 6. Resolve metadata and dependencies

When a DOI exists, put it in `article.doi`. CrossRef supplies the default Article type,
title, authors, year, volume, issue, pages, publication-date tags, and journal name.
Still inspect the expanded result during dry run. Use `article.type`, `article.fields`,
`article.authors`, or `article.tags` only to correct or supplement it.

Without a DOI, provide at least `article.type`, the required fields for that type, and
`article.authors`. Preserve source spelling in bibliographic fields except for the
ordinary database formatting documented in `docs/`.

For an existing CitationGroup, snapshot its ID and exact name. For a new one, supply its
name, ArticleType, Region ID and name, and any serialized CitationGroup tags. Follow
`docs/citation-group.md`; a CrossRef container title is evidence, not automatic
authority to create a new journal.

For a chapter in an edited volume, declare stable `article.ref` values on both rows:

1. Create an explicit `BOOK` row for the volume, omitting `file`. Give it an
   extensionless name, the editors in `authors`, its book CitationGroup (normally the
   publication city), year, title, publisher, and numbered-page extent.
2. Create the PDF-backed `CHAPTER` row with a typed parent such as
   `parent: {"model": "Article", "ref": "volume-key", "label": "<parent name>"}`.
   Dependency planning permits this ref to point forward in manifest review order. Put
   the chapter authors and printed text page range on the child; do not put a
   CitationGroup on the child.

To use a parent already in the database instead, guard it with
`parent: {"id": 123, "name": "Exact parent name"}`. Do not rely on an unguarded database
name lookup. The older planned `{name}` form remains supported only for an earlier row;
new bundle workflows should use typed refs.

### 7. Review related Names and write companion recommendations

When the Article was acquired to resolve one or more Names, inspect every Name whose
original nomenclatural act is actually contained in the staged publication, not merely
the first search target. Prepare guarded companion recommendations for the obvious
source-backed work, including:

- setting `original_citation` to the new or newly applied Article;
- direct-quotation `TypeTag.LocationDetail` and `TypeTag.SpecimenDetail` tags when the
  publication states the type locality or type material; and
- other straightforward fields or tags directly established by the same pages.

Do not infer a type designation, locality, repository, specimen identity, or wording
that the source does not state. Detail-tag text must be a faithful quotation with only
transparent line-break or typographic normalization. If the Article and Name changes
cannot safely share one manifest because the Article is not yet addressable by a typed
manifest-local reference, create and validate a named follow-up manifest immediately
after the Article is applied; identify that dependency explicitly at handoff rather than
silently omitting the Name work.

For an already cataloged electronic Article, never change `Article.name` with generic
`set_field` or `update_object`: those actions do not move the library file. Use a
specialized action that calls the Article move semantics, or emit a guarded
`manual_review` with the exact parser-valid proposed name so the reviewer can rename it
through `Article.move()`.

### 8. Hash the staged file, write the rows, and close the intake set

Compute the exact byte size and SHA-256 after all file verification. Write a new JSONL
manifest under `recs/manifests/`; do not revise an already-applied manifest.

Existing CitationGroup example:

```json
{
  "schema_version": 1,
  "action": "create_article",
  "confidence": "high",
  "reason": "The publisher record and first page identify this work.",
  "evidence": [
    { "kind": "landing_page", "text": "https://doi.org/10.1234/example" },
    { "kind": "acquisition_url", "text": "https://example.org/article.pdf" },
    {
      "kind": "pdf_first_page",
      "text": "Title, authors, journal, year, and DOI verified on page 1."
    }
  ],
  "article": {
    "name": "Endodontidae Pacific.pdf",
    "doi": "10.1234/example",
    "citation_group": { "id": 123, "name": "Journal name" }
  },
  "file": {
    "source_root": "new_path",
    "source_path": "downloaded-source.pdf",
    "destination_folder": "Mollusca/Gastropoda",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "size": 123456
  }
}
```

`file.source_root` may be `"new_path"` (the default when omitted) or `"downloads"`.
`file.source_path` is always relative to that configured root. On an exact retry where
the catalog database row and destination bytes already exist, application verifies all
guards and removes a still-present source from either root. Dry run and review never
remove it.

Before handoff, inventory every file downloaded, generated, or copied during the
campaign in both configured intake roots. Each campaign file must be accounted for
exactly once by one of these outcomes:

- a `create_article` file object;
- a `create_item_file` file object;
- a checksum-guarded `move_to_not_cataloged` row; or
- an explicitly reported unresolved file that the user has asked to retain in intake.

Do not leave unreferenced whole-volume sources, alternate downloads, failed extracts, or
rejected reprints for a later `check_new()` run. When useful content has already been
retained elsewhere, use `move_to_not_cataloged`; when a whole volume is itself worth
retaining, use `create_item_file`. After application, re-inventory both roots and verify
that no campaign-created file remains. If application is partial, generate a new
retry-safe cleanup manifest from the live database and filesystem state.

Inline CitationGroup creation replaces `article.citation_group` with:

```json
{
  "name": "Journal name",
  "type": "JOURNAL",
  "region": { "id": 1, "name": "United States" },
  "tags": []
}
```

Edited-volume pair example (each object is one JSONL line):

```json
{"schema_version":1,"action":"create_article","confidence":"high","reason":"The title page identifies the edited volume.","evidence":[{"kind":"title_page","text":"Editors, title, city, publisher, year, and extent verified."}],"article":{"name":"Pacific islands-biodiversity (Editor et al. 2017) (3)","type":"BOOK","fields":{"year":"2017","title":"Biodiversity. Volume III","publisher":"Example Press","pages":"658"},"authors":[{"family_name":"Editor","given_names":"Ada"}],"citation_group":{"id":123,"name":"Riga"}}}
{"schema_version":1,"action":"create_article","confidence":"high","reason":"The PDF is a chapter of the preceding volume.","evidence":[{"kind":"pdf_first_page","text":"Chapter title, authors, and page 525 verified."}],"article":{"name":"Endodontidae 3nov.pdf","type":"CHAPTER","fields":{"year":"2017","title":"Chapter title","start_page":"525","end_page":"580"},"authors":[{"family_name":"Author","given_names":"A."}],"parent":{"name":"Pacific islands-biodiversity (Editor et al. 2017) (3)"}},"file":{"source_path":"chapter.pdf","destination_folder":"Mollusca/Gastropoda","sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","size":123456}}
```

Optional Article keys are:

- `type`: an `ArticleType` name such as `JOURNAL`, `BOOK`, or `THESIS`;
- `fields`: string or null values for `year`, `title`, `series`, `volume`, `issue`,
  `start_page`, `end_page`, `url`, `publisher`, `pages`, `misc_data`, and
  `article_number`;
- `authors`: ordered objects with `family_name` and optional `given_names`, `initials`,
  `tussenvoegsel`, and `suffix`, or a guarded existing Person as
  `{"person": {"id": 123, "name": "Exact family name"}}` (if the reference provides
  given names for the authors, use those rather than just the initials); and
- `tags`: serialized `ArticleTag` values; and
- `ref`: a stable bundle-local Article key when another row depends on this Article; and
- `parent`: an existing `{id, name}` snapshot, a typed `{model: "Article", ref, label}`
  dependency, or the legacy earlier planned `{name}` form, permitted only for `CHAPTER`
  and `PART`.

Explicit values override CrossRef. Do not copy CrossRef data into overrides merely to
make the row verbose.

### 9. Review and validate

Run all three views with the repository interpreter:

```bash
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py recs/manifests/<file>.jsonl \
  --virtual-lint-issues-only
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py recs/manifests/<file>.jsonl --dry-run
```

`--review` is database- and network-independent. Planning verifies the current database,
expands the DOI, resolves parents in manifest order, requires an existing destination
folder for every file, checks PDF magic bytes where applicable plus size and SHA-256 for
all formats, and blocks name, DOI, CitationGroup, parent, or destination conflicts.
Virtual lint is advisory; inspect every reported Article, CitationGroup, Person, and
companion Name issue. Resolve every `VIRTUAL_LINT_ISSUES` title-formatting finding,
including `must_have_italics`, rather than treating it as harmless advisory noise.

Do not use `--apply`. Tell the user where the staged file and manifest are, summarize
CrossRef overrides, fuller author identities, representative filename choices, any new
CitationGroup, the companion Name scope, and the intake-closure audit. Hand off the
exact validation results.

## Whole-volume and issue PDFs: ItemFile

Inspect `taxonomy/db/models/item_file.py` and `taxonomy/applicator/item_file.py` for the
current fields and action schema. Use `create_item_file`, not generic `create_object`
plus a separate file move: the dedicated action guards the file size and SHA-256.

- Preserve the complete downloaded PDF unchanged, including wrappers, contents,
  separately paginated sections, plates, and blank leaves. Check its actual contents and
  boundaries rather than trusting the host's volume label. Keep Article extracts as
  separate staged files; do not substitute an extract for the source ItemFile.
- Check existing ItemFiles by source URL/identifier, CitationGroup, series, volume,
  issue, and filename. A matching title or volume number alone does not establish the
  same source, particularly across series. Reuse an appropriate existing ItemFile; do
  not overwrite a different file or create a duplicate under another name.
- Stage the whole PDF under `config.get_options().new_path`. The destination is
  `config.get_options().item_file_path`, which must already exist. Choose a plain PDF
  filename with no directory components; Article filename grammar does not apply.
- Snapshot an existing CitationGroup's ID and exact name, or use the same inline
  `{name, type, region, tags}` definition supported by `create_article`. When a source
  volume and its extracts share a new journal, repeat its complete definition on each
  row. The combined plan rejects conflicting definitions, shares one virtual journal for
  lint, and creates it only once on application. Exact existing definitions are reused
  on retries. Follow its series/volume/issue conventions; propose any necessary
  CitationGroup tag correction separately, with source evidence, and validate the
  proposed objects together.
- Put supported, verified metadata in `item_file.fields`: `title`, `series`, `volume`,
  `issue`, `start_page`, `end_page`, and `url` (strings or null). Prefer a stable
  volume/item URL over an expiring download URL. There are no `year`, `authors`, or
  `pages` fields on ItemFile. Preserve the year, multiple pagination sequences, and
  other bibliographic qualifications in evidence or an `ItemFileTag.IFComment`; do not
  manufacture one continuous page range for separately paginated sections.
- Serialize ItemFile tags with their `serialize()` method. Record the full source URL,
  acquisition URL, verified extent, and extraction mapping in evidence so that each
  extract remains traceable to its source. An ItemFile is not an Article `parent`.

Write a new JSONL row with this shape (replace example values with verified data):

```json
{
  "schema_version": 1,
  "action": "create_item_file",
  "confidence": "high",
  "reason": "Retain the complete source volume used for Article extracts.",
  "evidence": [
    { "kind": "source", "text": "Verified source URL, title, year, and extent." }
  ],
  "item_file": {
    "filename": "Journal volume 12 (1900).pdf",
    "citation_group": { "id": 123, "name": "Journal name" },
    "fields": { "volume": "12", "url": "https://example.com/volume/12" },
    "tags": []
  },
  "file": {
    "source_root": "new_path",
    "source_path": "downloaded-whole-volume.pdf",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "size": 123456
  }
}
```

`file.source_path` is relative to the selected `source_root`, which defaults to
`new_path`; unlike `create_article`, this action has no `destination_folder`. Run the
same review, virtual lint, and dry run as step 8, including any companion Article rows.
Leave installation and database creation to the user; do not call interactive ItemFile
creation/check methods during preparation.

## Files that should not be cataloged

When a reviewed intake file is a duplicate, an unneeded whole source after its useful
extract has been cataloged, or otherwise should leave the `check_new()` queue, add a
checksum-guarded `move_to_not_cataloged` row. Do not move it while preparing the
manifest. The destination is always the `Not to be cataloged` directory under
`new_path`; use `destination_name` only when the source filename would collide with a
different file already there.

```json
{
  "schema_version": 1,
  "action": "move_to_not_cataloged",
  "confidence": "high",
  "reason": "The catalog already contains the useful Article extracted from this file.",
  "evidence": [
    { "kind": "duplicate_check", "text": "Existing Article and catalog file verified." }
  ],
  "file": {
    "source_root": "downloads",
    "source_path": "downloaded-source.pdf",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "size": 123456
  }
}
```

The action verifies the source and any pre-existing destination by size and SHA-256. It
is restart-safe: if the exact destination already exists, application removes only an
exact remaining intake duplicate; conflicting destination bytes stop validation.

## Application semantics

On explicit human application, `create_item_file` installs a checksum-verified copy in
`item_file_path`, creates the ItemFile, and only then removes the intake source. It
retains the complete PDF when Articles are later extracted with `ItemFile.burst()`.
Interrupted exact partial states can be retried; conflicting metadata or bytes are
rejected.

For Article actions, a no-copy parent creates or reuses its CitationGroup, creates
unchecked editor Persons and the BOOK, and records normal Article history without
touching the filesystem. A child then creates unchecked author Persons, points to that
exact parent, and installs the file through a verified temporary copy. PDFs are then
extracted into the configured text store and indexed for search; all formats receive the
normal Article history entries. Only after those steps succeed does it remove the intake
source, including a directly referenced Downloads source. An interrupted exact partial
state can be rerun; an exact completed database-and-library state cleans up a
still-present matching source, while any differing database value, parent, or file
checksum blocks the action.

The action deliberately does not call the interactive `edittitle()`,
`specify_authors()`, or `edit_until_clean()` loops used by the traditional shell flow.
Source inspection, explicit overrides, and advisory virtual lint replace those prompts.
Do not duplicate `VIRTUAL_LINT_AUTOFIXABLE` findings as Article overrides or generic
manifest edits; resolve only remaining `VIRTUAL_LINT_ISSUES` before the user applies the
row.
