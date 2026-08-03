# Codex recommendations

This directory holds local, reviewable artifacts produced by Codex. One-off extraction
and recommendation generators belong in `scripts/`; generated JSONL recommendation, CE,
and Location manifests belong in `manifests/`. Both subdirectories are ignored by Git
and may be created as needed.

Durable import infrastructure and command-line entry points belong in `data_import/` and
`scripts/`. The recommendation applicator's implementation lives in
`taxonomy/applicator/`; its public entry point is `scripts/apply_recommendations.py`.
Recommendation manifests should be reviewed and dry-run there before any
human-authorized database write.
