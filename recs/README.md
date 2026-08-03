# Codex recommendations

This directory holds local, reviewable artifacts produced by Codex. One-off extraction
and recommendation generators belong in `scripts/`; generated JSONL recommendation, CE,
and Location manifests belong in `manifests/`. Both subdirectories are ignored by Git
and may be created as needed.

Durable import infrastructure, validators, and applicators belong in the repository's
`data_import/` and `scripts/` directories. Recommendation manifests should be reviewed
and dry-run there before any human-authorized database write.
