---
name: generate-recommendations
description:
  Generate evidence-bearing, reviewable JSONL recommendations for taxonomy database
  changes, with guarded snapshots and a dry-run-first applicator. Use when Codex is
  asked to audit data and produce executable recommendations for fields, tags,
  Locations, type localities, merges, or another repeatable database correction without
  directly changing the database.
---

# Generate Recommendations

Produce evidence-bearing JSONL for the user to review and apply. Use
`scripts/apply_recommendations.py` as the single review, validation, and application
entry point. Keep database access read-only with `CLIRM_READONLY=1`; do not run
`--apply` or interactive editing/application modes while generating recommendations.

## Load only relevant references

Read the model references for records being created, changed, or used as dependencies.
General model semantics and conventions remain in `docs/`.

| Model                                         | Read when                                                                |
| --------------------------------------------- | ------------------------------------------------------------------------ |
| [Article](references/models/article.md)       | Creating Articles, sharing author refs, or closing source intake.        |
| [Collection](references/models/collection.md) | Merging collections and updating dependent specimen identifiers.         |
| [Person](references/models/person.md)         | Merging people or selectively reassigning references.                    |
| [Region](references/models/region.md)         | Merging/deleting regions or defining geographic campaign scope.          |
| [ItemFile](references/models/item-file.md)    | Retaining whole journal volumes or issues.                               |
| [Location](references/models/location.md)     | Editing, renaming, merging, or geographically reconciling Locations.     |
| [Name](references/models/name.md)             | Enriching Names, retaining source evidence, or changing type localities. |

Read [manifest format](references/manifest-format.md) when writing generic actions,
values, refs, or guards. Read [review options](references/review-and-validation.md) for
filtered review and lint diagnostics, and
[extending actions](references/extending-actions.md) only when an applicator change is
needed. Prefer specialized actions when they provide required multi-object validation or
domain semantics.

Keep model-specific rules in `references/models/<model>.md`, with lowercase, hyphenated
filenames and a loading condition in this index. Keep shared mechanics in the shared
references; avoid duplicating them in model files.

## Workflow

1. Inventory the complete requested scope before extended research. Record selection
   criteria and object IDs, then inspect live state and relevant `docs/` conventions.
   Refresh snapshots for successive batches to incorporate the user's intervening work.
2. Start with stored evidence. Reopen sources when quotations are ambiguous, incomplete,
   conflicting, or lack necessary context. Research specific uncertainties that could
   change a recommendation. Include straightforward, source-backed companion changes
   within the user's scope; document dependencies or unresolved cases explicitly.
3. Review straightforward cases across the full inventory before spending substantial
   time on difficult ones. Maintain a coverage ledger: recommended change, reviewed with
   no change needed, reviewed but deferred, or not yet reviewed. Give each deferral its
   evidence, unresolved question, and useful next step. Resolve routine choices from
   existing instructions; ask only for a material policy decision that remains open.
4. Put one-off, database-dependent generators in `recs/scripts/` and new JSONL manifests
   in `recs/manifests/`. Both are intentionally untracked. Never rewrite an applied
   manifest. For partial application, build a new retry-safe follow-up from current
   database and filesystem state; retain the old manifest.
5. Give every row its schema version, action, confidence, reason, evidence, and IDs,
   labels, old values, or match guards sufficient to reject stale or ambiguous state.
   Reuse records only after checking relevant context, not just their labels. Preserve
   idempotence and validate the complete manifest before handoff. Account for every
   staged intake file as described in the Article reference.
6. Run review, dry run, and complete-proposal virtual lint using the repository's
   interpreter:

   ```bash
   CLIRM_READONLY=1 /Users/jelle/py/venvs/taxonomy314/bin/python \
     scripts/apply_recommendations.py recs/manifests/<file>.jsonl \
     --review --review-manual --virtual-lint-issues-only --dry-run
   ```

   `--review-manual` exposes unresolved rows and actionable review caveats. Virtual lint
   is advisory; report every outstanding `VIRTUAL_LINT_ISSUES` finding and any
   unavailable network/resource checks. Do not turn simulated or deferred autofixes into
   duplicate manifest edits. Run focused tests, Ruff, formatting, and mypy for
   implementation work.

7. Hand off the manifest, a compact review table, coverage counts, and the remaining
   queue with reasons. Distinguish affected records from action rows and created
   dependencies, and current counts from prospective counts. Report the exact review,
   dry-run, and virtual-lint results. Leave application to the user.

## Evidence rules

Every `*Detail` tag must quote its cited source directly. Put paraphrases, normalized
interpretations, and uncertainty in structured fields, non-Detail comments, or row
evidence. Do not invent identifiers, locations, coordinates, or source relationships to
increase completeness. `manual_review` is a valid outcome; an unreviewed record is not
evidence that a case is difficult. Continue through the authorized scope rather than
stopping after one batch or one difficult source.
