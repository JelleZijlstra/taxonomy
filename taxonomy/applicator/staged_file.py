"""Planner and executor for disposing of an uncataloged intake file safely."""

from __future__ import annotations

import hashlib
import shutil
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from taxonomy import config

SCHEMA_VERSION = 1
MOVE_TO_NOT_CATALOGED = "move_to_not_cataloged"
ALLOWED_ACTIONS = {MOVE_TO_NOT_CATALOGED}
ALLOWED_CONFIDENCES = {"high", "medium", "low"}


class RecommendationError(Exception):
    pass


class OptionsLike(Protocol):
    @property
    def new_path(self) -> Path: ...

    @property
    def downloads_path(self) -> Path: ...


@dataclass(frozen=True, slots=True)
class Evidence:
    kind: str
    text: str


@dataclass(frozen=True, slots=True)
class FileSpec:
    source_path: str
    sha256: str
    size: int
    source_root: str = "new_path"
    destination_name: str | None = None


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[Evidence, ...]
    file: FileSpec


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    source_path: Path
    destination_path: Path
    already_applied: bool


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedAction, ...]
    action_counts: Counter[str]


def _required(data: Mapping[str, Any], key: str, line: int) -> Any:
    if key not in data:
        raise RecommendationError(f"line {line}: missing {key}")
    return data[key]


def _required_str(data: Mapping[str, Any], key: str, line: int) -> str:
    value = _required(data, key, line)
    if not isinstance(value, str) or not value:
        raise RecommendationError(f"line {line}: {key} must be a nonempty string")
    return value


def _required_int(data: Mapping[str, Any], key: str, line: int) -> int:
    value = _required(data, key, line)
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecommendationError(f"line {line}: {key} must be an integer")
    return value


def _object(value: Any, label: str, line: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RecommendationError(f"line {line}: {label} must be an object")
    return value


def _parse_evidence(value: Any, line: int) -> tuple[Evidence, ...]:
    if not isinstance(value, list) or not value:
        raise RecommendationError(f"line {line}: evidence must be a nonempty list")
    output: list[Evidence] = []
    for index, raw in enumerate(value, start=1):
        data = _object(raw, f"evidence[{index}]", line)
        if set(data) != {"kind", "text"}:
            raise RecommendationError(
                f"line {line}: evidence[{index}] accepts kind and text"
            )
        output.append(
            Evidence(
                _required_str(data, "kind", line), _required_str(data, "text", line)
            )
        )
    return tuple(output)


def _plain_filename(raw: str, *, label: str, line: int) -> str:
    if Path(raw).name != raw or raw in {".", ".."}:
        raise RecommendationError(f"line {line}: {label} must be a plain filename")
    return raw


def _safe_relative_path(raw: str, *, line: int) -> Path:
    path = Path(raw)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise RecommendationError(
            f"line {line}: file.source_path must be a normalized relative path"
        )
    return path


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    version = _required_int(data, "schema_version", line_number)
    if version != SCHEMA_VERSION:
        raise RecommendationError(
            f"line {line_number}: unsupported schema_version {version}"
        )
    action = _required_str(data, "action", line_number)
    if action != MOVE_TO_NOT_CATALOGED:
        raise RecommendationError(f"line {line_number}: unsupported action {action!r}")
    confidence = _required_str(data, "confidence", line_number)
    if confidence not in ALLOWED_CONFIDENCES:
        raise RecommendationError(
            f"line {line_number}: unsupported confidence {confidence!r}"
        )
    if set(data) != {
        "schema_version",
        "action",
        "confidence",
        "reason",
        "evidence",
        "file",
    }:
        unknown = set(data) - {
            "schema_version",
            "action",
            "confidence",
            "reason",
            "evidence",
            "file",
        }
        raise RecommendationError(
            f"line {line_number}: unsupported top-level field(s): "
            + ", ".join(sorted(unknown))
        )
    file_data = _object(_required(data, "file", line_number), "file", line_number)
    if set(file_data) - {
        "source_path",
        "source_root",
        "destination_name",
        "sha256",
        "size",
    }:
        raise RecommendationError(
            f"line {line_number}: file accepts source_path, source_root, "
            "destination_name, sha256, and size"
        )
    sha256 = _required_str(file_data, "sha256", line_number).lower()
    if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
        raise RecommendationError(
            f"line {line_number}: file.sha256 must be 64 hexadecimal characters"
        )
    size = _required_int(file_data, "size", line_number)
    if size <= 0:
        raise RecommendationError(f"line {line_number}: file.size must be positive")
    source_root = file_data.get("source_root", "new_path")
    if source_root not in {"new_path", "downloads"}:
        raise RecommendationError(
            f"line {line_number}: file.source_root must be 'new_path' or 'downloads'"
        )
    destination_name = file_data.get("destination_name")
    if destination_name is not None:
        if not isinstance(destination_name, str) or not destination_name:
            raise RecommendationError(
                f"line {line_number}: file.destination_name must be a nonempty string"
            )
        _plain_filename(
            destination_name, label="file.destination_name", line=line_number
        )
    return Recommendation(
        line_number,
        action,
        confidence,
        _required_str(data, "reason", line_number),
        _parse_evidence(data.get("evidence"), line_number),
        FileSpec(
            _required_str(file_data, "source_path", line_number),
            sha256,
            size,
            source_root,
            destination_name,
        ),
    )


def _file_digest(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _check_file(path: Path, spec: FileSpec, *, line: int, label: str) -> None:
    try:
        size, sha256 = _file_digest(path)
    except OSError as exc:
        raise RecommendationError(
            f"line {line}: could not read {label} {path}: {exc}"
        ) from exc
    if size != spec.size:
        raise RecommendationError(
            f"line {line}: {label} size changed: expected {spec.size}, found {size}"
        )
    if sha256 != spec.sha256:
        raise RecommendationError(
            f"line {line}: {label} checksum changed: expected {spec.sha256}, "
            f"found {sha256}"
        )


def build_plan(
    recommendations: Iterable[Recommendation], *, options: OptionsLike | None = None
) -> RecommendationPlan:
    resolved_options: OptionsLike = options or config.get_options()
    destination_root = resolved_options.new_path / "Not to be cataloged"
    actions: list[PlannedAction] = []
    seen_sources: set[Path] = set()
    seen_destinations: set[Path] = set()
    for recommendation in recommendations:
        line = recommendation.line_number
        source_rel = _safe_relative_path(recommendation.file.source_path, line=line)
        root = (
            resolved_options.new_path
            if recommendation.file.source_root == "new_path"
            else resolved_options.downloads_path
        )
        source = root / source_rel
        resolved_source = source.resolve(strict=False)
        if not resolved_source.is_relative_to(root.resolve()):
            raise RecommendationError(
                f"line {line}: staged file resolves outside "
                f"{recommendation.file.source_root}: {source}"
            )
        destination_name = recommendation.file.destination_name or source.name
        _plain_filename(destination_name, label="destination filename", line=line)
        destination = destination_root / destination_name
        resolved_destination = destination.resolve(strict=False)
        if not resolved_destination.is_relative_to(destination_root.resolve()):
            raise RecommendationError(
                f"line {line}: destination resolves outside Not to be cataloged: "
                f"{destination}"
            )
        if resolved_source == resolved_destination:
            raise RecommendationError(
                f"line {line}: source is already in Not to be cataloged"
            )
        if resolved_source in seen_sources:
            raise RecommendationError(
                f"line {line}: staged file is used by more than one recommendation: "
                f"{source}"
            )
        if resolved_destination in seen_destinations:
            raise RecommendationError(
                f"line {line}: destination is used by more than one recommendation: "
                f"{destination}"
            )
        seen_sources.add(resolved_source)
        seen_destinations.add(resolved_destination)
        if source.is_symlink() or destination.is_symlink():
            raise RecommendationError(f"line {line}: symlinked files are not supported")
        if source.exists():
            _check_file(source, recommendation.file, line=line, label="source file")
        if destination.exists():
            _check_file(
                destination, recommendation.file, line=line, label="destination file"
            )
        if not source.exists() and not destination.exists():
            raise RecommendationError(
                f"line {line}: neither source nor destination file exists"
            )
        action = PlannedAction(
            recommendation, source, destination, already_applied=False
        )
        if destination.exists():
            action = replace(action, already_applied=True)
        actions.append(action)
    return RecommendationPlan(
        tuple(actions), Counter(row.recommendation.action for row in actions)
    )


def print_review_table(rows: Iterable[Recommendation]) -> None:
    for row in rows:
        print(
            f"line={row.line_number} action={row.action} "
            f"confidence={row.confidence} source_root={row.file.source_root!r} "
            f"source={row.file.source_path!r} "
            f"destination={row.file.destination_name or Path(row.file.source_path).name!r}"
        )


def execute_plan(plan: RecommendationPlan, *, apply: bool) -> None:
    for action in plan.actions:
        status = (
            "ALREADY_APPLIED"
            if action.already_applied
            else ("APPLY" if apply else "WOULD_MOVE")
        )
        print(
            f"{status} action={MOVE_TO_NOT_CATALOGED} "
            f"source={str(action.source_path)!r} "
            f"destination={str(action.destination_path)!r} "
            f"sha256={action.recommendation.file.sha256}"
        )
        if not apply:
            continue
        action.destination_path.parent.mkdir(parents=True, exist_ok=True)
        if not action.destination_path.exists():
            shutil.move(action.source_path, action.destination_path)
        elif action.source_path.exists():
            action.source_path.unlink()
    mode = "Applied" if apply else "Dry run"
    print(f"{mode}: {len(plan.actions)} {MOVE_TO_NOT_CATALOGED} recommendation(s).")
    if not apply:
        print("No database or filesystem changes made. Pass --apply to execute.")
