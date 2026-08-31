---
name: inquiry-response
description:
  Research emails or messages concerning the taxonomy database or Mammal Diversity
  Database and produce evidence notes, staged source additions, and unapplied change
  recommendations. Do not draft or send replies or directly modify databases.
---

# Inquiry Response

## Overview

Respond to inquiries about matters related to the taxonomy database or the Mammal
Diversity Database (MDD).

## Workflow

1. Identify the inquiry. For example, search for emails or Slack messages. Read the
   complete shortlisted messages, related follow-ups, links, and attachments; identify
   them by date, subject, and sender. Prefer exact sender-address searches when
   available.

2. Gather evidence database-first.
   - Prefer the taxonomy database and local literature paths for published sources.
   - Use external links for museum catalogues, ledger images, GBIF records, or other
     primary online records.
   - Use the $add-article skill when you encounter a relevant source that is not yet
     represented by a usable electronic Article in the database and library.
   - When the inquiry concerns the MDD, compare the data available in the public MDD
     page as well as available local MDD exports. Determine whether a discrepancy is in
     the local taxonomy database, underlying MDD data, MDD prose, a stale public
     deployment, or frontend presentation such as maps.
   - Prefer primary literature and exact specimen/locality evidence. For ranges,
     distinguish vouchers and concrete records from modeled maps, inherited broad
     concepts, checklist assertions, and geographic plausibility.
   - For taxonomic proposals, separate biological delimitation from whether the proposed
     name is correctly linked to types or topotypic material.

3. Make an assessment based on the available evidence. If you think a change to the
   local taxonomy database is warranted, use the $generate-recommendations skill to
   propose a change manifest. If you think a change to the Mammal Diversity Database is
   warranted, say so. Finish every inquiry with a disposition such as "confirmed",
   "plausible but uncertain", "unsupported", or "already correct", plus the specific
   MDD/local action status. List any staged Article files and recommendation manifests,
   give their review and dry-run status, and state explicitly that they remain
   unapplied.

## Safeguards

- Never send emails or other messages directly.
- Never edit any database directly.
- Do not provide suggested draft responses, only evidence and notes.
- Run database-opening commands with `CLIRM_READONLY=1`.
- Do not mark messages read, archive them, change labels, or otherwise mutate the
  mailbox.
- Do not change MDD data or external systems.
- These safeguards do not prohibit staging source files or writing unapplied
  recommendation manifests through the referenced skills.
