---
name: write-research-note
description:
  Write or update brief source-faithful research notes in the taxonomy repository. Use
  when asked to create, revise, or prepare docs/research-notes/ pages about taxonomic,
  nomenclatural, bibliographic, specimen-provenance, locality, or literature questions
  that required deeper investigation.
---

# Write Research Note

## Overview

Create short notes that preserve the evidence behind resolved or partly resolved
research questions. Keep the prose factual, compact, and traceable to sources.

## Workflow

1. Identify the question and scope.
   - Use the note title as the research question when possible.
   - Separate published evidence, specimen records, catalogue metadata, and inference.
   - If the user asks only for research or discussion, do not edit files.

2. Gather evidence database-first.
   - Prefer the taxonomy database and local literature paths for published sources.
   - Use Article links in the form `[Author (year)](/a/<id>)`.
   - Use external links for museum catalogues, ledger images, GBIF records, or other
     primary online records.
   - Record exact specimen numbers, repositories, dates, localities, collectors, and
     relevant identification history.

3. Draft or update the note.
   - Put notes under `docs/research-notes/<slug>.md`.
   - Use this header format exactly:

     ```markdown
     # Does _Example_ occur somewhere?

     _Jelle S. Zijlstra, Month YYYY_
     ```

   - Start with a concise statement of the question and the main evidence.
   - Use short sections for each evidence set, such as type material, later museum
     records, unpublished records, and assessment.
   - Prefer careful factual wording over advocacy. Say "catalogued as", "reported by",
     "identified as", or "probably" only when that is what the evidence supports.
   - Keep editorial judgement in an `Assessment` section and distinguish it from the
     source summaries.

4. Maintain indexes and tests.
   - Add every new note to `docs/research-notes.md`.
   - Keep `docs/research-notes.md` linked from `docs/home.md`.
   - The repository test `taxonomy/test_docs.py` checks the note index and byline
     format; update the note rather than weakening the test.

5. Verify.
   - Run the focused docs tests after editing:

     ```bash
     python -m pytest -q taxonomy/test_docs.py
     ```

## Citation Style

- Use existing docs style: `[Author (year)](/a/<article-id>)` for database articles.
- Use plain Markdown links for external records.
- Add a `## Sources` section when it is useful to discuss the sources used.
- Avoid long quotations; summarize, and quote only short phrases needed for evidence.
