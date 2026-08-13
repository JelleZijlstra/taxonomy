---
name: add-article
description:
  Find and verify an Article PDF, choose its catalog filename and library folder,
  resolve bibliographic metadata, CitationGroup, and edited-volume parent dependencies,
  and write reviewed create_article recommendations without changing the database. Use
  when adding a new electronic Article, book, dissertation, or book chapter to the
  taxonomy library from a PDF.
---

# Add an Article

Prepare source-faithful PDFs and executable `create_article` recommendations. Leave each
PDF in the configured staging folder and leave application to the user.

## Scope

Use this workflow for new electronic Articles backed by PDFs. It supports:

- DOI-driven CrossRef metadata with explicit corrections;
- fully explicit metadata when no DOI exists;
- existing CitationGroups;
- creation of one ordinary CitationGroup inline; and
- edited-volume chapters whose parent is an existing Article or a no-copy BOOK created
  by an earlier row in the same manifest; and
- unchecked Person creation from the Article's author list.

The only supported Article without a PDF is a BOOK needed as the parent of a staged
chapter. Stop and explain the unsupported case instead of forcing it into this action
for an alternative version, redirect, non-PDF file, or other work without an electronic
copy. The applicator also requires an existing destination folder for every PDF;
proposing new library-folder taxonomy is a separate task.

## Workflow

### 1. Load the catalog rules

Read these repository documents before choosing values:

- `docs/article.md` for Article semantics;
- `docs/article-naming.md` for the filename grammar;
- `docs/citation-group.md` when choosing or creating a CitationGroup;
- `docs/person.md` when author identity needs interpretation; and
- `docs/finding-citations.md` when literature discovery is part of the request.

Keep general rules in these documents. This skill owns only the operational sequence.

### 2. Establish source identity

Find the publisher or repository landing page and a downloadable PDF. Prefer the
publisher, institutional repository, Biodiversity Heritage Library, or another stable
primary host. Record both the landing-page URL and the actual acquisition URL in
evidence.

Download into `config.get_options().new_path`, never directly into the catalog library.
Use a temporary descriptive staging name; it does not have to equal the final Article
name. If the PDF is already present, inspect that exact file rather than substituting a
similarly titled paper.

Open or render the first page and inspect extracted text. Confirm the title, authors,
publication, year, pagination, and DOI against the landing page. For a chapter, also
verify the editors, parent title, publisher, publication city, parent extent, chapter
page range, and whether separately numbered plates follow the text. A mismatch between
the requested work, landing page, and PDF is a hard stop. Do not silently switch
sources.

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

### 4. Choose and validate the Article name

Choose a concise content description under `docs/article-naming.md`. The source title is
not the filename. Electronic Article names must be printable ASCII and end in lowercase
`.pdf`; a no-copy parent BOOK must be printable ASCII and have no extension.

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

For each PDF, select an existing folder relative to `config.get_options().library_path`.
Follow nearby Articles rather than inventing a parallel hierarchy. Taxonomic placement
is normally primary; use geography where that is how comparable regional works are
organized. For broad works, identify the narrowest existing folder that accurately
represents the main coverage without hiding important scope.

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

### 7. Hash the staged file and write the row

Compute the exact byte size and SHA-256 after all PDF verification. Write a new JSONL
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
    "source_path": "downloaded-source.pdf",
    "destination_folder": "Mollusca/Gastropoda",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "size": 123456
  }
}
```

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
  `{"person": {"id": 123, "name": "Exact family name"}}`; and
- `tags`: serialized `ArticleTag` values; and
- `ref`: a stable bundle-local Article key when another row depends on this Article; and
- `parent`: an existing `{id, name}` snapshot, a typed `{model: "Article", ref, label}`
  dependency, or the legacy earlier planned `{name}` form, permitted only for `CHAPTER`
  and `PART`.

Explicit values override CrossRef. Do not copy CrossRef data into overrides merely to
make the row verbose.

### 8. Review and validate

Run all three views with the repository interpreter:

```bash
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py recs/manifests/<file>.jsonl --virtual-lint
CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py recs/manifests/<file>.jsonl --dry-run
```

`--review` is database- and network-independent. Planning verifies the current database,
expands the DOI, resolves parents in manifest order, requires an existing destination
folder for every PDF, checks PDF magic bytes, size, and SHA-256, and blocks name, DOI,
CitationGroup, parent, or destination conflicts. Virtual lint is advisory; inspect every
reported Article, CitationGroup, and Person issue.

Do not use `--apply`. Tell the user where the staged PDF and manifest are, summarize
CrossRef overrides and any new CitationGroup, and hand off the exact validation results.

## Application semantics

On explicit human application, actions run in manifest order. A no-copy parent creates
or reuses its CitationGroup, creates unchecked editor Persons and the BOOK, and records
normal Article history without touching the filesystem. A child then creates unchecked
author Persons, points to that exact parent, installs the PDF through a verified
temporary copy, extracts its text into the configured text store, indexes its pages for
search, and adds the normal Article history entries. Only after those steps succeed does
it remove the staged source. An interrupted exact partial state can be rerun; any
differing database value, parent, or file checksum blocks the action.

The action deliberately does not call the interactive `edittitle()`,
`specify_authors()`, or `edit_until_clean()` loops used by the traditional shell flow.
Source inspection, explicit overrides, and advisory virtual lint replace those prompts.
Do not duplicate `VIRTUAL_LINT_AUTOFIXABLE` findings as Article overrides or generic
manifest edits; resolve only remaining `VIRTUAL_LINT_ISSUES` before the user applies the
row.
