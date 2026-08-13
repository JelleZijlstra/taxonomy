---
name: add-taxonomic-group
description: >-
  Build a source-faithful, dependency-aware bundle that extends taxonomy database
  coverage for a named taxonomic group. Use when asked to add a classification or
  database coverage for a family, order, genus, or other group, including discovering
  literature, downloading PDFs, recommending Articles and CitationGroups, creating taxa
  and base Names, transcribing ClassificationEntries and occurrences, and producing
  staged reviewable recommendation manifests.
---

# Add a taxonomic group

Produce staged source PDFs plus one native recommendation JSONL manifest per application
stage. A mostly covered group may need one manifest. A largely uncovered group normally
needs a bootstrap manifest, an authorized materialization/cleanup pass, and then an
enrichment manifest generated against the resulting Names. Do not mutate the database.

## Workflow

1. Inspect the requested clade and its existing database coverage read-only. Record the
   nearest existing ancestor, existing taxa/Names, and relevant Articles.
2. Make a source plan. Distinguish sources that establish the accepted classification
   from original descriptions needed for nomenclatural and type data. A paper proposing
   a new genus may recombine old species without supplying their original type evidence.
   Prefer primary classifications, revisions, catalogues, original descriptions, and
   later corrections. Record landing-page provenance for every download.
3. Use the `add-article` skill for each new work. Stage PDFs in configured `new_path`,
   verify source identity and SHA-256, and emit `create_article` rows with Article refs.
   Never substitute a different paper for a requested or identified source.
4. Use the `ingest-classification` skill and
   `taxonomy.applicator.classification.ClassificationBundleBuilder` to transcribe each
   Article. Keep source hierarchy separate from accepted Taxon placement. Treat the
   bootstrap transcription as final source evidence, not rough staging data:
   - Every tag whose name ends in `Detail` must contain a direct quotation from its
     cited source. Only transcription-level normalization such as joining line breaks,
     expanding a ligature, or repairing obvious OCR is allowed.
   - CE `type_locality` and `TypeSpecimenData` text must also be direct quotations,
     because materialization turns them into Name `LocationDetail` and `SpecimenDetail`
     tags. Use the builder's `type_locality_quote`, `type_specimen_data_quote`, and
     `etymology_detail_quote` parameters.
   - Put summaries and inferences in recommendation evidence, structured fields,
     `raw_data`, or non-Detail comments, never in a `*Detail` tag.

5. Reconcile explicitly. For a mostly uncovered source classification, mark CEs with
   `Materialize`: accepted entries become Taxon/base-Name pairs and synonym entries
   become Names on the accepted parent Taxon. Anchor only the source root to an existing
   Taxon ID; descendants follow the source-local hierarchy. When a later Article has a
   root CE whose accepted parent is materialized from an earlier Article in the same
   manifest, use `MaterializeParent` for that reconciliation-only relation. Do not add a
   cross-Article source `parent`. Existing unambiguous Names are mapped instead of
   duplicated. Use explicit `create_taxon` only for anchors or accepted structure not
   supported by a source CE. Leave uncertain candidates blocked and explain them in
   evidence or a `manual_review` row. Mark original nomenclatural acts with
   `OriginalCitation` so materialized Names retain the source Article and the exact
   act-author subset given by the CE authority. When an accepted CE is a later
   combination, use `MaterializeBaseName` to identify the original spelling, page, and
   optional rank. Reference the original Article only when it is actually available;
   otherwise provide a verbatim citation, CitationGroup, and ordered authors. Cleanup
   then creates the Taxon, original base Name, and a separate combination Name mapped to
   the CE.
6. Do not add `TypeSpeciesDetail` merely to support an uncomplicated original
   designation or monotypy. Set the structured `type` and `genus_type_kind` fields in
   enrichment. Use `TypeSpeciesDetail` only when a source's actual discussion is useful
   for a difficult or historical case, and quote that discussion directly.
7. Add non-mutating `check_coverage` rows where the source scope is demonstrably
   complete. Coverage assertions report or block on omissions; they never delete.
8. Emit the bootstrap rows below `recs/manifests/<group>/`. Keep one-off extraction code
   in `recs/scripts/`; do not create executable CE or Location sidecars. Never rewrite
   an applied manifest. If a source transcription was wrong, emit a new guarded repair
   manifest and separately fix its generator.
9. Validate without writes:

   ```bash
   CLIRM_READONLY=1 \
   /Users/jelle/py/venvs/taxonomy314/bin/python \
     scripts/apply_recommendations.py MANIFEST \
     --review --review-classification --review-reconciliation \
     --review-manual --virtual-lint-issues-only --dry-run
   ```

   Network-backed bibliographic and geographic lookups are allowed in this read-only
   phase and may populate `urlcache`. Database writes remain prohibited.

   Virtual lint is advisory for guarded replacements. When a manifest replaces a CE and
   updates its materialized Name, database-wide lint may temporarily see both the
   persisted and virtual CE, producing duplicate `IncludedSpecies`, multiple-original-
   locality, or mapped-name-candidate findings. Confirm that each finding consists only
   of the guarded old state and its proposed replacement; report it as a virtual-graph
   limitation rather than weakening the manifest's quotation or provenance rules.

10. Report the downloaded PDFs, manifest path, source gaps, reconciliation decisions,
    and validation results. Stop before `--apply` unless explicitly authorized.

## Enrichment stage

After the bootstrap manifest has been applied and `Materialize` cleanup has finished:

1. Re-read the persisted CEs, Taxa, and Names. Generate enrichment from current IDs and
   guarded old values rather than predicting deferred materialization IDs.
2. Inspect both `name_data_level()` and `fill_data_level()` for every created Name.
3. Use original descriptions for `original_citation`, type locality, type specimen,
   collection, type kinds, etymology, and derived tags. If an original description is
   unavailable, leave `original_citation` blank and retain a source-backed
   `verbatim_citation`; do not create a no-copy journal Article. It is valid to leave
   unsupported type fields blank.
4. Derive `Date`, `Organ`, `CollectedBy`, `Age`, `Gender`, `Coordinates`, `Altitude`,
   and similar structured tags only from quoted source evidence or a separately cited
   specimen record. Structured values may normalize the evidence; `*Detail` text may
   not.
5. Set genus type fields directly. Omit routine `TypeSpeciesDetail` tags.
6. If a PDF becomes available between stages, include its `create_article` row and all
   dependent Name updates in the enrichment manifest.
7. If bootstrap materialization copied faulty source text, repair both the CE evidence
   and the derived Name tags with exact guarded removals and additions.
8. Validate and hand off the enrichment manifest using the same read-only command.

If a missing original description becomes available only after enrichment was applied,
emit a third source-completion manifest. Install the verified Article, set
`original_citation`, reconcile the fallback `verbatim_citation` and CitationGroup with
guarded changes, and add newly supported type data. Never rewrite either applied stage.
If the fallback lacks the exact original spelling, page, citation, or authors required
for `MaterializeBaseName`, block that materialization for manual review rather than
guessing.

## Manifest rules

- Manifest order is review order; typed refs determine dependency execution order.
- A `create_article` row declares `article.ref`.
- A CE `Materialize` tag normally owns derived Taxon/Name creation. Root materialization
  uses `parent_taxon_id`; child materialization waits for its source parent. The tag is
  removed only after the CE has a mapped Name.
- A root CE may instead carry `MaterializeParent`, referencing an accepted CE from
  another Article in the same manifest. Its persistent fix waits for that CE's mapped
  Taxon and uses it only for database placement; the CE's source-local `parent` remains
  null. `MaterializeParent`, `parent_taxon_id`, and a source-local parent are mutually
  exclusive.
- A CE `OriginalCitation` tag declares that its Article is the original description.
  Materialization copies that Article and matches the CE authority against its authors;
  it fails instead of guessing when the authority cannot be reproduced exactly.
- `MaterializeBaseName` is mutually exclusive with `OriginalCitation`. It is for an
  accepted later combination whose base Name is absent. Materialization blocks if a
  candidate for the original spelling appears, so it cannot silently duplicate a Name.
- Do not create a no-copy journal Article merely to populate a Name's
  `original_citation`. A source-backed `verbatim_citation` is the faithful fallback.
- Whenever a Name has an `original_citation`, materialization takes the Name's date from
  that Article. The Article date must be the work's Code-relevant publication date.
- A `create_taxon` row remains available for an explicit accepted graph that cannot be
  derived from CEs and declares both refs.
- Each ClassificationEntry and OccurrenceRecord is its own schema-version 2
  `create_object` row with a persisted-field `match` block.
- Emit stable refs and deterministic source order. Given the same drafts and database
  reconciliation snapshot, rerunning a generator should produce byte-identical JSONL.
- Treat contextual matches strictly: an absent match creates, a completely equivalent
  match is already applied, and a partial, conflicting, or ambiguous match blocks until
  the generator emits a guarded `update_object` or a review decision.
- Use `update_object` for guarded enrichment of an existing object. Never rely on
  implicit null filling, fuzzy upsert, or deletion by omission.
- Treat initial classification materialization and detailed nomenclatural enrichment as
  separate application stages whenever enrichment depends on deferred Names. One
  manifest cannot safely guess the persistent IDs created by post-apply cleanup.
- Preserve source spelling, page, hierarchy, locality wording, and raw evidence. Do not
  infer an accepted Taxon parent from a source-local CE parent.
- Every occurrence has an explicit Location ref or a source-backed LocationHint.
- Autofixable virtual lint findings do not need duplicate manifest edits. If another row
  depends on the resulting object, create it explicitly.
