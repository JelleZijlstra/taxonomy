---
name: ingest-classification
description: >-
  Transcribe a taxonomic classification and source-backed occurrences into native,
  reviewable recommendation JSONL. Use for ClassificationEntry ingestion, classification
  tables or checklists, source-local hierarchy validation, reconciliation reports,
  occurrence extraction, and coverage assertions in the taxonomy repository.
---

# Ingest a classification

Keep transcription, reconciliation, validation, and database mutation separate. The
deliverable is native recommendation rows in one manifest, not a CE sidecar.

## Author from the source

For a small source, write schema-version 2 rows directly. For a repeated or complex
source, use the pure types in `taxonomy.applicator.classification`:
`ClassificationBundleBuilder`, `ClassificationDraft`, `OccurrenceDraft`,
`LocationDraft`, and `DraftEvidence`. Put one-off extractors in `recs/scripts/` and
manifests in `recs/manifests/`.

Resolve a repository Article and cached text with `scripts/article_info.py`. Render PDF
pages when columns, typography, tables, or printed pages matter. Use the exact Article
name/ref and preserve source spelling. Do not add unprinted higher taxa merely to make
the hierarchy complete.

Each ClassificationEntry must have:

- one source name and Rank;
- one printed page number, never a range or list;
- its Article ref;
- a typed source-local parent ref only when supported by the source;
- a source-only `raw_data` snapshot; and
- source-page evidence.

Source evidence is quotation-sensitive. Every tag whose name ends in `Detail` must be a
direct quotation from the cited source, with only transcription-level normalization. CE
`type_locality` and `TypeSpecimenData` must follow the same rule because materialization
copies them to Name `LocationDetail` and `SpecimenDetail` tags. In the builder, the
corresponding arguments are deliberately named `type_locality_quote`,
`type_specimen_data_quote`, and `etymology_detail_quote`. Put paraphrase or inference in
recommendation evidence, structured data, or a non-Detail comment instead.

Use `normalized_name` concepts only for abbreviations or nonstandard presentation, not
to correct spelling, modernize a combination, or choose synonymy. Database
reconciliation belongs in `mapped_name`, not the source snapshot.

Nested occurrences are convenient in the draft builder but lower to separate
OccurrenceRecord `create_object` rows. Preserve verbatim locality in `locality_text`.
Record one Voucher tag per cited specimen and use typed Collection refs. Every
occurrence must have either a justified Location ID/ref or a LocationHint that makes the
unresolved state explicit. Set its reconciled Taxon explicitly when the CE mapping
supports one; do not make the applicator infer a Taxon from source layout. Use
ObservationKind only with an observation basis and SpecimenDetail only with voucher
evidence.

## Native row model

- Use `create_object` with an explicit persisted-field `match` for each new CE,
  occurrence, Location, or ordinary Name.
- Use `update_object` with guarded `set`, `add`, and `remove` changes for an existing
  object that needs enrichment.
- Use typed `{model, ref}` values for proposed objects and guarded `{model, id, label}`
  values for persisted objects. Refs may appear in structured ADT tag arguments.
- For a largely uncovered group, add a `Materialize` tag to faithful CEs. Cleanup maps
  an unambiguous existing Name first; otherwise it creates a Taxon/base Name for an
  accepted entry or a synonym Name on the parent Taxon, then removes the tag. A root
  accepted CE must give the nearest existing ancestor as `parent_taxon_id`; descendants
  derive placement from their CE parents. Use explicit `create_taxon` only for an anchor
  or another Taxon graph that cannot be derived from the source classification.
- If a source-root CE belongs beneath a Taxon materialized from a different Article in
  the same manifest, use `MaterializeParent` (or the builder's `materialize_parent`)
  instead of inventing a cross-Article source parent. It is a reconciliation
  instruction: cleanup waits for the referenced CE's mapped Taxon, while both Articles
  retain their faithful source-local trees.
- Carry a directly quoted etymology in the CE `EtymologyDetail` tag. It is copied to the
  mapped Name with the CE Article as its source during cleanup.
- Mark a CE that is the original nomenclatural act with `OriginalCitation` (or pass
  `original_citation=True` to the builder). Materialization sets the CE Article as the
  new Name's original citation and uses the CE authority to select the exact act authors
  from the Article, including an explicitly attributed author subset.
- For an accepted later combination whose original Name is also absent, add
  `MaterializeBaseName` (or pass `materialize_base_name` to the builder) with the
  original spelling, original page, and optional original rank. If the original
  description Article exists, reference it and derive the authors and Code date from it.
  Otherwise leave `original_citation` blank and provide `verbatim_citation`,
  `citation_group`, and ordered `authors`; do not create a metadata-only Article merely
  to fill the field. The combination Name always inherits the CE Article's Code date.
  Cleanup atomically creates the accepted Taxon, the original base Name, and a separate
  name-combination Name sourced to the CE Article; the CE maps to the combination Name.
  Do not combine this instruction with `OriginalCitation`.
- `ClassificationBundleBuilder.article()` accepts either an Article ref produced by the
  manifest or a guarded `article_id` plus `article_label` for an existing Article.
- Use `check_coverage` to report complete/partial source scope. It never deletes.

The builder validates same-Article parents, cycles, duplicates, species/genus agreement,
one-page values, duplicate occurrence identities, and explicit Location resolution.
Treat failures as transcription problems, not invitations to weaken the checks. Lower
drafts deterministically: keep stable refs and source order, and require the same drafts
plus reconciliation snapshot to produce byte-identical JSONL. An existing exact match
with equivalent complete values is already applied; partial, conflicting, or ambiguous
matches require an explicit guarded update or manual review rather than implicit
enrichment.

## Review

Run without database writes:

```bash
CLIRM_READONLY=1 \
/Users/jelle/py/venvs/taxonomy314/bin/python \
  scripts/apply_recommendations.py MANIFEST \
  --review-classification --review-reconciliation \
  --virtual-lint-issues-only --dry-run
```

Database read-only mode does not imply offline mode. API lookups such as Crossref,
GeoNames, or Nominatim are allowed when useful and may populate `urlcache`; preserve
their returned provenance and never turn a fuzzy lookup into an unsupported mapping.

Verify every name, Rank, page, source parent, locality, basis, tag, and quote against
the rendered PDF, not OCR alone. In particular, search every emitted `*Detail`, CE type
locality, and `TypeSpecimenData` back to the source page before handoff. Review
ambiguous and unrecognized reconciliation candidates explicitly. Autofixable lint
findings may remain for the optional post-apply cleanup; unresolved non-autofixable
findings need correction or review.

Never run `--apply` without explicit authorization. If authorized, the generalized
`--edit-applied` option performs formatting and `edit_until_clean()` after the entire
selected plan succeeds. It is required for `Materialize` tags because persistent object
creation is deliberately a deferred, non-virtual-safe structured lint fix.
