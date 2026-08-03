# Repository Guidelines

## Project Structure & Module Organization

- `taxonomy/`: Core library and CLI (`shell.py`); DB models under `taxonomy/db/`.
- `hsweb/`: Minimal web UI (run with `python -m hsweb`).
- `data_import/`: Durable import infrastructure and maintained importers.
- `recs/`: Local Codex-generated recommendation artifacts. One-off extractors and
  generators go in `recs/scripts/`; JSONL manifests go in `recs/manifests/`. Both
  subdirectories are intentionally untracked.
- `scripts/`: Durable reports, maintenance tools, and recommendation applicators
  (`python scripts/<name>.py`).
- `docs/`: User/developer docs.
- `mapper/`: Mapping helpers and inputs.
- Tests live next to modules (e.g., `taxonomy/db/models/name/test_*.py`).

## Setup, Build, and Dev Commands

- Use `/Users/jelle/py/venvs/taxonomy314/bin/python` to run Python commands.
- Install deps: `uv sync`.
- Run CLI shell: `python -m taxonomy.shell`.
- Run web app: `python -m hsweb`.
- Lint: `ruff check .` Format: `black .` Type-check: `mypy .`.
- Tests: `pytest -q` (single file: `pytest taxonomy/db/test_helpers.py`).

## Coding Style & Naming

- Python 3.14+, 4-space indent, UTF-8.
- Formatting: Black; Linting: Ruff (target py312).
- Prefer type hints; run `mypy` locally.
- Isolate I/O and network; keep core logic in `taxonomy/`.

## Testing Guidelines

- Framework: pytest. Name tests `test_*.py`; keep near code.
- Tests must be deterministic and offline; mock external APIs in `taxonomy/apis/*`.
- Keep fast unit tests; larger integration tests live beside modules.

## Commit & Pull Request Guidelines

- Commits: imperative subject ("Add X"), body explains why and scope.

## Security & Configuration Tips

- Do not commit secrets. Use env vars for credentials (Google Sheets, AWS, Zotero).
- Avoid committing large datasets; reference them in `docs/`.
- Keep local paths/tokens out of VCS.
- Agents must run commands that may open the taxonomy database with `CLIRM_READONLY=1`
  unless the user has explicitly authorized that exact command to write to the database.
  The project Codex configuration sets this automatically.
- Use `CLIRM_READONLY=0` only for an explicitly authorized database-writing command or
  for tests that operate exclusively on isolated temporary/in-memory databases. Keep
  ordinary tests and all read-only audits at the default value.

## Writing data import scripts

- Put one-off Codex-generated extraction scripts in `recs/scripts/` and their CE,
  Location, or recommendation JSONL output in `recs/manifests/`. Reserve `data_import/`
  for durable infrastructure and intentionally maintained importers.
- Accuracy is key. Make sure the output of your script matches the classification in the
  source exactly. Make sure page numbers are correct.
- Use the newer `CEDict` abstraction, as in `data_import/corbet_hill_1980.py`.
- Use assertions and helper functions to validate invariants, like making sure every
  species name matches the genus it's in.
- If the source book has text with multiple columns per page, use the split_lines()
  function to fix this.

## General guidelines

- Do not edit the database unless expressly intructed to do so. By default, leave
  database changes to humans.
- When evaluating a taxonomic question, prefer to use articles that are already in the
  database and library. If you encounter relevant references that are not yet in the
  database, download a PDF if possible and put it in new_path (as configured in
  taxonomy.ini); if you are unable to download a PDF, instead include a link to the
  article when you respond to the user.
