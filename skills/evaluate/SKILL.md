---
name: evaluate
description:
  Evaluate whether a taxonomic proposal is justified from primary literature and the
  taxonomy database. Use when asked to assess a proposed taxonomic change, synonymy,
  split, lump, revalidation, new species, name assignment, type-material linkage,
  genetic/morphological evidence, or whether putative taxa are correctly associated with
  available names.
---

# Evaluate

## Overview

Evaluate taxonomic proposals as evidentiary arguments, not just as paper summaries.
Determine both whether the proposed biological delimitation is supported and whether the
names applied to the delimited taxa are justified by type material, type localities,
original descriptions, and subsequent usage.

Use papers in the local taxonomy literature database and files under
`/Users/jelle/Dropbox/c` unless the user explicitly authorizes another source. Prefer
the database catalog and article paths over web search.

## Workflow

1. Identify the proposal.
   - Record exactly what change is proposed: new species, synonymy, revalidation,
     revised range, reassigned name, subspecies change, or clade label.
   - List all names involved, including synonyms, original combinations, type specimens,
     type localities, and current database status when available.
   - Separate two questions from the start: "Is this a real taxon?" and "Is this the
     correct name for it?"

2. Read the focal paper closely.
   - Extract the stated diagnosis, sampled specimens, vouchers, localities, markers,
     measurements, examined types, figures, tables, supplements, and GenBank or other
     accession numbers.
   - Inspect relevant figures or maps visually when layout matters; text extraction
     alone often misses whether a type sequence or locality is actually plotted.
   - Track whether claims in the discussion match the tables, trees, maps, and
     supplements.

3. Build the local literature context.
   - Follow the search procedure in "Finding Background Literature" before deciding
     whether the focal paper is convincing.
   - Read enough recent work to understand current practice for the group, not just the
     paper being evaluated and the original description.
   - Prefer primary sources: original descriptions, type catalogs, revisionary accounts,
     papers with voucher-level matrices, and papers that examined type material.

4. Evaluate biological delimitation.
   - Ask whether the evidence supports independently evolving lineages, not just labels
     in a tree.
   - Check sampling density near type localities, contact zones, range gaps, and
     outlying populations.
   - Compare mitochondrial, nuclear, morphological, morphometric, geographic,
     ecological, and karyotypic evidence for congruence.
   - Treat single-marker mtDNA splits, weakly sampled clades, discordant markers,
     missing vouchers, and unreported accessions as limitations.
   - Note whether diagnostic characters are explicit, repeatable, age/sex controlled,
     and compared against all plausible relatives.
   - Note sample sizes.

5. Evaluate nomenclatural and name-assignment support.
   - Anchor each available name to its type specimen and type locality.
   - Verify whether the genetic, morphological, or geographic material used for a
     putative taxon is directly linked to the type, topotypes, paratypes, or reliably
     identified comparative material.
   - If a name-bearing type was sequenced, check sequence length, marker coverage,
     contamination precautions, accession identity, and whether the sequence appears in
     the relevant analyses.
   - If the type was not sequenced, check whether topotypic material or diagnostic
     morphology bridges the focal population to the type.
   - Watch for asymmetric reasoning: accepting one part of a previous author's name
     assignment while rejecting another without new evidence.
   - Treat catalog-number errors, vague "near type locality" claims, missing voucher
     data, or unexamined types as evidence-quality issues, not mere trivia.

6. Compare the proposal against current database usage.
   - Query the taxonomy database for the current valid taxon, synonyms, original
     citations, type data, and classification history.
   - Use model-backed database objects and article records when available rather than
     string-only matching.
   - Identify exactly what database change would follow if the paper were accepted, and
     whether the paper supports that change at the required confidence.

7. Produce an assessment.
   - Focus on summarizing evidence rather than making a decision.
   - Separate "well supported", "plausible but under-demonstrated", and "unsupported or
     contradicted" points.
   - Cite concrete evidence: specimen numbers, accessions, localities, figures, tables,
     and exact paper sections.
   - State what additional evidence would resolve the issue, especially type/topotype
     sampling, broader morphology, or genomic data.

## Finding Background Literature

Use a database-first, snowballing search. The goal is to understand both the taxon's
recent research context and the nomenclatural anchors behind the names.

1. Start from the focal paper.
   - Extract every cited paper that is taxonomic, revisionary, phylogenetic, geographic,
     or nomenclatural.
   - Prioritize papers the focal paper relies on for current taxonomy, disagrees with,
     or uses for specimen identifications.
   - Search within the focal paper for terms such as `sensu`, `synonym`, `holotype`,
     `type`, `complex`, `group`, `clade`, `lineage`, `voucher`, `GenBank`, `diagnosis`,
     and `distribution`.

2. Query the taxonomy database.
   - Look up all involved current taxa, historical names, original combinations,
     synonyms, and misspellings.
   - Record original citations, type specimens, type localities, current valid names,
     and classification history.
   - Use Article records as the preferred catalog of local papers. Search titles,
     authors, years, paths, and linked names for the genus, subgenus, species epithets,
     species-group names, older combinations, and geographic keywords.
   - When possible, follow database links from `Name`, `Taxon`, and `Article` objects
     instead of relying on filename searches alone.

3. Search the local PDF library.
   - Search `/Users/jelle/Dropbox/c` by taxonomic folder, genus, subgenus, epithet,
     author/year, and geographic region.
   - Include adjacent folders when the group has moved among genera, subgenera, tribes,
     country folders, or "Inter-group" folders.
   - Use filename searches to discover candidates, then confirm relevance from the
     article title, abstract, methods, voucher tables, and references.

4. Read recent context before deep history.
   - Read recent revisions, checklists, species descriptions, molecular phylogenies,
     population-level papers, and regional faunal treatments for the focal group.
   - Read work on closely related or sympatric taxa when the focal paper's diagnosis
     depends on excluding them.
   - Pay special attention to recent papers with overlapping vouchers, GenBank
     accessions, localities, or specimen identifications.

5. Then read nomenclatural anchors.
   - Read original descriptions and type-catalog or revisionary accounts for every
     available name that could apply.
   - Check whether later authors corrected type localities, catalog numbers, synonymies,
     or misidentifications.
   - If a name assignment depends on a type sequence, topotype, or paratype, read the
     paper that established that link rather than assuming the focal paper inherited it
     correctly.

Stop searching when the remaining uncited or older papers are unlikely to change either
the biological assessment or the name assignment. Mention important papers that could
not be located in the database.

## Evidence Checklist

Use this checklist while reading:

- Proposal: What taxonomic act or recommendation is made?
- Scope: Which names, ranks, specimens, populations, and geographic areas are affected?
- Types: Which type specimens and type localities anchor the relevant names?
- Vouchers: Are all genetic and morphological samples voucher-backed and traceable?
- Type linkage: Is the focal material tied to the correct name by type sequence,
  topotypes, paratypes, or diagnostic comparison?
- Analysis inclusion: Are the name-bearing or bridging specimens actually included in
  the analyses used to justify the conclusion?
- Concordance: Do mitochondrial, nuclear, morphology, measurements, geography, and prior
  literature agree?
- Alternative names: Are older or competing names excluded with evidence?
- Contradictions: Do tables, supplements, figures, and discussion say the same thing?
- Practical outcome: What should the database do now, and what should remain flagged as
  uncertain?

## Output Shape

When the user asks for an evaluation, answer in compact prose unless they request a
formal report. A useful default structure is:

1. Bottom line.
2. What the focal paper shows. Are there any mistakes that potentially impact the
   conclusion?
3. What the broader literature shows.
4. Whether the name assignment is justified.
5. Recommended database treatment or caution.

Avoid treating a paper's conclusion as established merely because a clade is labeled
with a name. Say explicitly when a paper supports "a distinct clade/population" better
than it supports "this clade bears this name."
