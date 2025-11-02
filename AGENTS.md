# Repository Guidelines

## Project Structure & Module Organization

- `taxonomy/`: Core library and CLI (`shell.py`); DB models under `taxonomy/db/`.
- `hsweb/`: Minimal web UI (run with `python -m hsweb`).
- `data_import/`: Importer scripts. These scripts parse data in an input data source
  into a format that is compatible with the database. When adding new scripts, use the
  newer `CEDict` abstraction, as in `data_import/corbet_hill_1980.py`.
- `scripts/`: Reports and maintenance scripts (`python scripts/<name>.py`).
- `docs/`: User/developer docs.
- `mapper/`: Mapping helpers and inputs.
- Tests live next to modules (e.g., `taxonomy/db/models/name/test_*.py`).

## Setup, Build, and Dev Commands

- Create env: `python3.14 -m venv .venv && source .venv/bin/activate`.
- Install deps: `uv sync`.
- Run CLI shell: `python -m taxonomy.shell`.
- Run web app: `python -m hsweb`.
- Lint: `ruff check .` Format: `black .` Type-check: `mypy .`.
- Tests: `pytest -q` (single file: `pytest taxonomy/db/test_helpers.py`).

## Coding Style & Naming

- Python 3.14+, 4-space indent, UTF-8.
- Formatting: Black; Linting: Ruff (target py312).
- Prefer type hints; run `mypy` locally.
- Names: modules/functions/vars snake_case; classes CamelCase; constants UPPER_SNAKE.
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
