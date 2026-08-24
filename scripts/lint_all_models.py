"""Autofix all model lints and inventory the issues that remain.

The output is an audit inventory, not an executable recommendation manifest. Each
JSONL row identifies one database object and groups all of its remaining lint issues.
A later Codex session can use the inventory as evidence when writing guarded
recommendations for ``scripts/apply_recommendations.py``.

This command intentionally writes autofixes to the taxonomy database. Run it in a
writable environment (for Codex, explicitly set ``CLIRM_READONLY=0``). The output is
written atomically so an interrupted run does not replace the previous complete file.
"""

import argparse
import json
import time
import traceback
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from taxonomy.db.models import BaseModel
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint_types import LintIssue, LintResult

DEFAULT_OUTPUT = (
    Path(__file__).parents[1] / "recs" / "manifests" / "model_lint_issues.jsonl"
)


@dataclass(frozen=True)
class LintRunSummary:
    objects_checked: int
    objects_with_issues: int
    issues: int
    structured_autofixes: int
    issues_by_model: Counter[str]
    issues_by_code: Counter[str]


def get_model_classes() -> tuple[type[BaseModel], ...]:
    """Return every concrete model in a deterministic order."""
    return tuple(sorted(BaseModel.__subclasses__(), key=lambda model: model.__name__))


def select_model_classes(names: Sequence[str]) -> tuple[type[BaseModel], ...]:
    models_by_name = {model.__name__: model for model in get_model_classes()}
    unknown = sorted(set(names) - models_by_name.keys())
    if unknown:
        available = ", ".join(sorted(models_by_name))
        raise ValueError(
            f"unknown model(s): {', '.join(unknown)}; available models: {available}"
        )
    return tuple(models_by_name[name] for name in dict.fromkeys(names))


def object_ref(obj: BaseModel, model: type[BaseModel] | None = None) -> dict[str, Any]:
    if model is None:
        model = type(obj)
    try:
        label = str(getattr(obj, model.label_field))
    except Exception as exc:
        label = f"<unavailable: {type(exc).__name__}: {exc}>"
    return {"model": model.__name__, "id": obj.id, "label": label}


def issue_data(issue: LintResult) -> dict[str, str | None]:
    return {
        "code": issue.code if isinstance(issue, LintIssue) else None,
        "message": str(issue),
    }


def lint_error(obj: BaseModel, model: type[BaseModel], exc: Exception) -> LintIssue:
    return LintIssue(
        f"{model.__name__} #{obj.id}: error running general lint: "
        f"{type(exc).__name__}: {exc}",
        code="lint_runner_error",
    )


def lint_models(
    model_classes: Iterable[type[BaseModel]],
    output_file: IO[str],
    *,
    progress_every: int = 1000,
) -> LintRunSummary:
    if progress_every < 1:
        raise ValueError("progress_every must be at least 1")

    objects_checked = 0
    objects_with_issues = 0
    issue_count = 0
    structured_autofixes = 0
    issues_by_model: Counter[str] = Counter()
    issues_by_code: Counter[str] = Counter()

    def record_fix(_issue: LintIssue) -> None:
        nonlocal structured_autofixes
        structured_autofixes += 1

    cfg = LintConfig(autofix=True, interactive=False, fix_callback=record_fix)

    for model in model_classes:
        query = model.select()
        total = query.count()
        started = time.monotonic()
        model_objects = 0
        model_objects_with_issues = 0
        model_issue_count = 0
        print(f"START model={model.__name__} objects={total}", flush=True)
        model.clear_lint_caches()
        try:
            for index, obj in enumerate(query, 1):
                objects_checked += 1
                model_objects += 1
                if index == 1 or index % progress_every == 0:
                    print(
                        f"PROGRESS model={model.__name__} objects={index}/{total}",
                        flush=True,
                    )
                try:
                    issues = list(obj.general_lint(cfg))
                except Exception as exc:
                    traceback.print_exc()
                    issues = [lint_error(obj, model, exc)]
                if not issues:
                    continue

                objects_with_issues += 1
                model_objects_with_issues += 1
                issue_count += len(issues)
                model_issue_count += len(issues)
                issues_by_model[model.__name__] += len(issues)
                for issue in issues:
                    code = (
                        issue.code
                        if isinstance(issue, LintIssue) and issue.code is not None
                        else "uncoded"
                    )
                    issues_by_code[code] += 1
                    print(
                        f"ISSUE model={model.__name__} id={obj.id} "
                        f"code={code}: {issue}",
                        flush=True,
                    )
                row = {
                    "schema_version": 1,
                    "object": object_ref(obj, model),
                    "issues": [issue_data(issue) for issue in issues],
                }
                output_file.write(
                    json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                )
                output_file.flush()
        finally:
            model.clear_lint_caches()
        elapsed = time.monotonic() - started
        print(
            f"DONE model={model.__name__} objects={model_objects} "
            f"objects_with_issues={model_objects_with_issues} "
            f"issues={model_issue_count} elapsed={elapsed:.1f}s",
            flush=True,
        )

    return LintRunSummary(
        objects_checked=objects_checked,
        objects_with_issues=objects_with_issues,
        issues=issue_count,
        structured_autofixes=structured_autofixes,
        issues_by_model=issues_by_model,
        issues_by_code=issues_by_code,
    )


def run(
    model_classes: Iterable[type[BaseModel]],
    output: Path,
    *,
    progress_every: int = 1000,
) -> LintRunSummary:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output.with_suffix(output.suffix + ".tmp")
    try:
        with temporary_output.open("w", encoding="utf-8") as output_file:
            summary = lint_models(
                model_classes, output_file, progress_every=progress_every
            )
        temporary_output.replace(output)
    except BaseException:
        print(
            f"Run did not complete; partial output remains at {temporary_output}",
            flush=True,
        )
        raise

    print(
        f"WROTE path={output} objects_checked={summary.objects_checked} "
        f"objects_with_issues={summary.objects_with_issues} "
        f"issues={summary.issues} "
        f"structured_autofixes={summary.structured_autofixes}",
        flush=True,
    )
    print(f"ISSUES_BY_MODEL {dict(summary.issues_by_model)}", flush=True)
    print(f"ISSUES_BY_CODE {dict(summary.issues_by_code)}", flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--models",
        nargs="+",
        metavar="MODEL",
        help="lint only these model classes (for example: Person IssueDate)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        metavar="N",
        help="print per-model progress every N objects (default: 1000)",
    )
    args = parser.parse_args()

    if BaseModel.clirm.is_read_only:
        parser.error(
            "autofix requires a writable database; explicitly run with "
            "CLIRM_READONLY=0"
        )
    try:
        model_classes = (
            get_model_classes()
            if args.models is None
            else select_model_classes(args.models)
        )
        run(model_classes, args.output, progress_every=args.progress_every)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
