# Chiroptera HMW Reference Matching Strategy

This document describes the stage-3 matching logic used by
`data_import/chiroptera_hmw_match_refs.py`.

The script reads the parsed stage-2 CSV and writes a new CSV that preserves all stage-2
columns while adding externally derived columns for four targets:

1. taxonomy Articles
2. DOIs
3. BatLit
4. Biodiversity Heritage Library (BHL)

## General principles

- Stage 1 and stage 2 are treated as source-derived data only.
- Stage 3 adds only external inferences in new columns.
- Matching is intentionally multi-stage:
  - generate candidates
  - score candidates using independent pieces of evidence
  - accept only candidates above threshold with enough separation from runner-up
- Ambiguous rows are preserved as such instead of forcing a low-confidence match.
- The pipeline is run in two rounds:
  - round 1 matches directly from parsed-reference and taxonomy data
  - round 2 learns conservative container-title to citation-group mappings from secure
    round-1 matches and reruns unresolved/weak rows with that extra normalization data

## Shared normalization and second-pass learning

Several parts of stage 3 depend on normalizing abbreviated journal titles and author
names.

Shared normalization includes:

- citation-group alias expansion from taxonomy
- fuzzy abbreviation-aware journal matching
- Romanized aliases for Cyrillic taxonomy author names
- extra first-author aliases for pinyin / Chinese / Vietnamese naming conventions

After round 1, the matcher learns only high-confidence journal/container mappings from
accepted matches. Those learned mappings are then used in round 2 as additional evidence
for:

- taxonomy candidate generation by citation group
- DOI Crossref lookups
- BatLit journal matching
- BHL lookup via stronger taxonomy matches

The learned mappings are additive rather than authoritative, so round 2 can improve
coverage without replacing strong round-1 results with weaker inferred ones.

## 1. Matching to taxonomy Articles

### Candidate generation

Candidates are gathered from several independent sources:

- described taxa:
  - `Name.corrected_original_name`
  - `resolve_variant()`
  - `original_citation`
- normalized URL matches
- year + first-author matches
- year + distinctive title-token matches
- citation group + year matches
- citation group + year + volume + start-page matches

For `scientific_description` rows, the raw reference is further decomposed into taxon
groups with associated page snippets, so the candidate set keeps track of which taxon
pointed to which page.

### Scoring

Article candidates are scored from:

- compatible reference/article type
- author agreement
- year agreement
- title agreement
- container / citation-group agreement
- volume / issue / page agreement
- described-taxon support

Special handling:

- Cyrillic family names in taxonomy author data are Romanized.
- pinyin / Chinese / Vietnamese naming conventions get additional aliases.
- `scientific_description` matches receive strong support when a candidate is the
  `original_citation` of one or more described taxa.
- additional score is given when a scientific-description page matches the Article start
  page or falls inside the Article page range.

### Classification

After scoring, near-duplicate candidates are collapsed by bibliographic signature so
BHL-backed or date-granularity duplicates do not create fake ambiguities.

Rows are then classified as:

- `matched`
- `ambiguous`
- `unmatched`

using score and score-gap thresholds, with slightly looser thresholds for books,
chapters, reports, and theses.

## 2. Matching DOIs

DOIs are filled from strongest to weakest source:

1. `Article.doi` on an accepted taxonomy match
2. Crossref inference from the matched taxonomy Article
3. Crossref inference directly from the parsed reference row

### Taxonomy-Article DOI inference

For matched Articles without a DOI, the script reuses the project Crossref candidate
logic from `taxonomy.db.models.article`.

### Parsed-reference DOI inference

The parsed-reference logic uses several Crossref query styles:

- exact OpenURL lookup by journal title + volume + start page
- exact OpenURL lookup by ISSN + volume + start page
- ISSN-restricted title search with a narrow year window
- OpenURL fallback queries assembled directly from parsed data:
  - article title
  - first author
  - year
  - journal title when available
  - volume / issue / start page when available

Journal resolution uses both:

- exact citation-group alias matching
- fuzzy abbreviation-aware citation-group matching
- learned round-2 container-title to citation-group mappings

Candidate DOI metadata is then checked against the parsed row using:

- title similarity
- prefix compatibility for truncated titles
- year agreement
- volume / issue agreement
- page agreement
- journal/container agreement
- author-family overlap

Only candidates with enough combined evidence are accepted.

## 3. Matching BatLit

BatLit links are filled from:

1. BatLit data already attached to an accepted taxonomy Article
2. direct BatLit search from the parsed row

The direct BatLit search uses:

- DOI exact match when available
- title similarity
- year agreement
- first-author and author-overlap agreement
- journal alias matching
- volume and start-page agreement

Rows are accepted only when the best BatLit candidate clears the score threshold and
beats the runner-up by a safe margin.

## 4. Matching BHL

BHL links are filled from:

1. BHL URLs already present on an accepted taxonomy Article
2. optional BHL inference using the project BHL logic

The script keeps DOI and BHL network behavior configurable:

- `off`
- `cached`
- `network`

so repeated runs can reuse the project cache instead of re-querying services. The
defaults are cache-friendly, and network-enabled runs populate the same cache for later
offline reuse.

## Output columns

The stage-3 CSV adds only external fields:

- taxonomy match status / score / reasons / candidate summaries
- accepted taxonomy Article identifiers and citation
- DOI and DOI source
- BatLit identifiers / citation / source
- BHL URL and source

This keeps the source-derived parsing output separate from cross-linked data.
