# Review and validation options

Read this file for filtered review, detailed lint diagnostics, or an explanation of the
user's interactive application workflow. Generation remains read-only; interactive
editing and application require the user's explicit authorization.

## Virtual lint diagnostics

Use `--virtual-lint` to inspect the full lint process. Its `VIRTUAL_LINT_AUTOFIXABLE`,
`VIRTUAL_AUTOFIX_SIMULATED`, and `VIRTUAL_AUTOFIX_DEFERRED` findings are derived lint
work and must not be duplicated as manifest edits. Only `VIRTUAL_LINT_ISSUES` requires a
manifest change, an explicit suppression, or manual review. Database queries may not see
newly proposed objects, so identify genuine graph limitations rather than weakening
source evidence.

## Review and application modes

Static review keeps one summary row per recommendation and adds indented, untruncated
details: generic creation fields and match guards, schema-v2 guarded operations, Article
and Taxon creation fields, Location and type-locality changes, coverage guards and
members, and the evidence attached to every `manual_review` row. To limit the review to
particular actions, repeat `--review-action`:

```bash
python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review \
    --review-action manual_review
```

For a manifest containing unresolved rows or actionable rows with important review
caveats, inspect their complete notes with:

```bash
python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review-manual
```

To make an explicit decision on every row, use the interactive review mode:

```bash
python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --review-each
```

It visits rows in manifest order. `yes` queues the row for application, `no` skips it,
and `edit` opens the affected existing object and skips the automated row. Direct
editing is unavailable for `create_object` rows. After the final choice, the selected
rows are rebuilt and validated together before any automated write; this preserves
object-reference dependencies and accounts for changes made in editors.

Operation flags may be combined. Static `--review` and `--review-manual` output runs
first, followed by requested virtual lint and `--dry-run`, then `--apply` or
`--review-each`, post-apply cleanup, and finally `--edit-manual`. For example,
`--review-manual --review-each --edit-manual` prints the complete unresolved notes,
reviews every row interactively, applies the accepted subset, and then opens all
manual-review objects. Only `--apply` and `--review-each` are incompatible because they
respectively mean applying every row and selecting individual rows.

To work through unresolved rows interactively after validating the complete manifest,
print each full manual-review note and open its database object editor:

```bash
python scripts/apply_recommendations.py recs/manifests/<file>.jsonl --edit-manual
```
