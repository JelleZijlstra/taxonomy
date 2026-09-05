"""Planner and executor for cataloging a staged PDF as an ItemFile.

``create_item_file`` validates the complete database record and a checksum-guarded
move from ``new_path`` into ``item_file_path`` as one restartable action. The public
CLI is :mod:`scripts.apply_recommendations`.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from taxonomy import config
from taxonomy.applicator import citation_group as citation_group_recommendations
from taxonomy.applicator import generic as generic_recommendations
from taxonomy.applicator.citation_group import CitationGroupSpec, PlannedCitationGroup
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.models import CitationGroup, ItemFile, Region
from taxonomy.db.models.item_file import ItemFileTag

SCHEMA_VERSION = 1
CREATE_ITEM_FILE = "create_item_file"
ALLOWED_ACTIONS = {CREATE_ITEM_FILE}
ALLOWED_CONFIDENCES = {"high", "medium", "low"}
SCALAR_FIELDS = {"title", "series", "volume", "issue", "start_page", "end_page", "url"}


class RecommendationError(Exception):
    pass


class OptionsLike(Protocol):
    @property
    def new_path(self) -> Path: ...

    @property
    def item_file_path(self) -> Path: ...

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


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[Evidence, ...]
    filename: str
    fields: Mapping[str, str | None]
    serialized_tags: tuple[Any, ...]
    citation_group: CitationGroupSpec
    file: FileSpec


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    source_path: Path
    destination_path: Path
    citation_group: PlannedCitationGroup
    fields: Mapping[str, str | None]
    tags: tuple[ItemFileTag, ...]
    item_file: ItemFile | None
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


def _parse_citation_group(value: Any, line: int) -> CitationGroupSpec:
    try:
        return citation_group_recommendations.parse_spec(
            value, line=line, context="item_file.citation_group"
        )
    except citation_group_recommendations.RecommendationError as exc:
        raise RecommendationError(str(exc)) from exc


def _parse_file(value: Any, line: int) -> FileSpec:
    data = _object(value, "file", line)
    if set(data) - {"source_path", "sha256", "size", "source_root"}:
        raise RecommendationError(
            f"line {line}: file accepts source_path, sha256, size, and source_root"
        )
    sha256 = _required_str(data, "sha256", line).lower()
    if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
        raise RecommendationError(
            f"line {line}: file.sha256 must be 64 hexadecimal characters"
        )
    size = _required_int(data, "size", line)
    if size <= 0:
        raise RecommendationError(f"line {line}: file.size must be positive")
    source_root = data.get("source_root", "new_path")
    if source_root not in {"new_path", "downloads"}:
        raise RecommendationError(
            f"line {line}: file.source_root must be 'new_path' or 'downloads'"
        )
    return FileSpec(_required_str(data, "source_path", line), sha256, size, source_root)


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    version = _required_int(data, "schema_version", line_number)
    if version != SCHEMA_VERSION:
        raise RecommendationError(
            f"line {line_number}: unsupported schema_version {version}"
        )
    action = _required_str(data, "action", line_number)
    if action != CREATE_ITEM_FILE:
        raise RecommendationError(f"line {line_number}: unsupported action {action!r}")
    confidence = _required_str(data, "confidence", line_number)
    if confidence not in ALLOWED_CONFIDENCES:
        raise RecommendationError(
            f"line {line_number}: unsupported confidence {confidence!r}"
        )
    item_file = _object(
        _required(data, "item_file", line_number), "item_file", line_number
    )
    filename = _required_str(item_file, "filename", line_number)
    if Path(filename).name != filename or filename in {".", ".."}:
        raise RecommendationError(
            f"line {line_number}: item_file.filename must be a plain filename"
        )
    fields_data = item_file.get("fields", {})
    if not isinstance(fields_data, dict):
        raise RecommendationError(
            f"line {line_number}: item_file.fields must be an object"
        )
    if unknown_fields := set(fields_data) - SCALAR_FIELDS:
        raise RecommendationError(
            f"line {line_number}: unsupported ItemFile field(s): "
            + ", ".join(sorted(unknown_fields))
        )
    fields: dict[str, str | None] = {}
    for field, value in fields_data.items():
        if value is not None and not isinstance(value, str):
            raise RecommendationError(
                f"line {line_number}: item_file.fields.{field} must be a string or null"
            )
        fields[field] = value
    tags = item_file.get("tags", [])
    if not isinstance(tags, list):
        raise RecommendationError(f"line {line_number}: item_file.tags must be a list")
    if unknown_item_keys := set(item_file) - {
        "filename",
        "fields",
        "tags",
        "citation_group",
    }:
        raise RecommendationError(
            f"line {line_number}: unsupported item_file key(s): "
            + ", ".join(sorted(unknown_item_keys))
        )
    allowed_top = {
        "schema_version",
        "action",
        "confidence",
        "reason",
        "evidence",
        "item_file",
        "file",
    }
    if unknown_top := set(data) - allowed_top:
        raise RecommendationError(
            f"line {line_number}: unsupported top-level key(s): "
            + ", ".join(sorted(unknown_top))
        )
    return Recommendation(
        line_number=line_number,
        action=action,
        confidence=confidence,
        reason=_required_str(data, "reason", line_number),
        evidence=_parse_evidence(data.get("evidence"), line_number),
        filename=filename,
        fields=fields,
        serialized_tags=tuple(tags),
        citation_group=_parse_citation_group(
            _required(item_file, "citation_group", line_number), line_number
        ),
        file=_parse_file(_required(data, "file", line_number), line_number),
    )


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


def _file_digest(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _check_pdf(path: Path, spec: FileSpec, *, line: int, label: str) -> None:
    try:
        actual_size, digest = _file_digest(path)
        with path.open("rb") as file:
            header = file.read(1024)
    except OSError as exc:
        raise RecommendationError(
            f"line {line}: could not read {label} {path}: {exc}"
        ) from exc
    if b"%PDF-" not in header:
        raise RecommendationError(f"line {line}: {label} {path} is not a PDF")
    if actual_size != spec.size:
        raise RecommendationError(
            f"line {line}: {label} size changed: expected {spec.size}, "
            f"found {actual_size}"
        )
    if digest != spec.sha256:
        raise RecommendationError(
            f"line {line}: {label} checksum changed: expected {spec.sha256}, "
            f"found {digest}"
        )


def _get_citation_group(object_id: int) -> CitationGroup:
    try:
        return CitationGroup.get(id=object_id)
    except CitationGroup.DoesNotExist as exc:
        raise RecommendationError(f"CitationGroup {object_id} does not exist") from exc


def _get_item_file(filename: str) -> ItemFile | None:
    try:
        return ItemFile.get(filename=filename)
    except ItemFile.DoesNotExist:
        return None


def _decode_tags(serialized: tuple[Any, ...], *, line: int) -> tuple[ItemFileTag, ...]:
    try:
        return tuple(ItemFileTag.unserialize(tag) for tag in serialized)
    except Exception as exc:
        raise RecommendationError(
            f"line {line} item_file.tags: invalid serialized tag: {exc}"
        ) from exc


def _expected_fields(fields: Mapping[str, str | None]) -> dict[str, str | None]:
    return {field: fields.get(field) for field in SCALAR_FIELDS}


def _check_field_snapshot(item_file: ItemFile, action: PlannedAction) -> None:
    expected = _expected_fields(action.fields)
    mismatches = [
        f"{field}={getattr(item_file, field)!r} (expected {value!r})"
        for field, value in expected.items()
        if getattr(item_file, field) != value
    ]
    expected_cg = action.citation_group.citation_group
    if expected_cg is None or item_file.citation_group.id != expected_cg.id:
        mismatches.append(
            f"citation_group={item_file.citation_group!r} "
            f"(expected {expected_cg!r})"
        )
    if frozenset(item_file.tags or ()) != frozenset(action.tags):
        mismatches.append("tags differ")
    if mismatches:
        raise RecommendationError(
            f"line {action.recommendation.line_number}: existing ItemFile "
            f"{item_file.filename!r} conflicts with recommendation: "
            + "; ".join(mismatches)
        )


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    options: OptionsLike | None = None,
    get_citation_group: Callable[[int], CitationGroup] = _get_citation_group,
    get_item_file: Callable[[str], ItemFile | None] = _get_item_file,
    citation_groups_named: Callable[[str], Iterable[CitationGroup]] = (
        citation_group_recommendations.citation_groups_named
    ),
    get_region: Callable[[int], Region] = citation_group_recommendations.get_region,
    initial_citation_groups: Iterable[PlannedCitationGroup] = (),
) -> RecommendationPlan:
    resolved_options: OptionsLike = options or config.get_options()
    rows = list(recommendations)
    if not rows:
        return RecommendationPlan((), Counter())
    item_file_root = resolved_options.item_file_path
    if not item_file_root.is_dir():
        raise RecommendationError(
            f"ItemFile destination directory does not exist: {item_file_root}"
        )
    destination_root = item_file_root.resolve()
    actions: list[PlannedAction] = []
    seen_filenames: set[str] = set()
    seen_sources: set[Path] = set()
    planned_new_citation_groups = {
        planned.spec.name: planned
        for planned in initial_citation_groups
        if planned.citation_group is None
    }
    for recommendation in rows:
        line = recommendation.line_number
        if recommendation.filename in seen_filenames:
            raise RecommendationError(
                f"line {line}: duplicate create_item_file filename "
                f"{recommendation.filename!r}"
            )
        seen_filenames.add(recommendation.filename)
        source_rel = _safe_relative_path(recommendation.file.source_path, line=line)
        source_root = (
            resolved_options.new_path
            if recommendation.file.source_root == "new_path"
            else resolved_options.downloads_path
        )
        source = source_root / source_rel
        resolved_source = source.resolve(strict=False)
        if not resolved_source.is_relative_to(source_root.resolve()):
            raise RecommendationError(
                f"line {line}: staged file resolves outside "
                f"{recommendation.file.source_root}: {source}"
            )
        if resolved_source in seen_sources:
            raise RecommendationError(
                f"line {line}: staged file is used by more than one recommendation: "
                f"{source}"
            )
        seen_sources.add(resolved_source)
        destination = item_file_root / recommendation.filename
        if not destination.resolve(strict=False).is_relative_to(destination_root):
            raise RecommendationError(
                f"line {line}: destination resolves outside item_file_path: "
                f"{destination}"
            )
        if resolved_source == destination.resolve(strict=False):
            raise RecommendationError(
                f"line {line}: source and destination must be different paths"
            )
        if source.is_symlink():
            raise RecommendationError(f"line {line}: staged file may not be a symlink")
        if destination.is_symlink():
            raise RecommendationError(f"line {line}: destination may not be a symlink")

        try:
            citation_group = citation_group_recommendations.plan_citation_group(
                recommendation.citation_group,
                line=line,
                planned_new=planned_new_citation_groups,
                get_citation_group=get_citation_group,
                citation_groups_named=citation_groups_named,
                get_region=get_region,
            )
        except citation_group_recommendations.RecommendationError as exc:
            raise RecommendationError(str(exc)) from exc
        tags = _decode_tags(recommendation.serialized_tags, line=line)
        item_file = get_item_file(recommendation.filename)
        action = PlannedAction(
            recommendation=recommendation,
            source_path=source,
            destination_path=destination,
            citation_group=citation_group,
            fields=dict(recommendation.fields),
            tags=tags,
            item_file=item_file,
            already_applied=False,
        )
        if item_file is not None:
            _check_field_snapshot(item_file, action)
        if source.exists():
            _check_pdf(source, recommendation.file, line=line, label="staged file")
        if destination.exists():
            _check_pdf(
                destination, recommendation.file, line=line, label="destination file"
            )
        if not source.exists() and not destination.exists():
            raise RecommendationError(
                f"line {line}: neither staged nor destination file exists"
            )
        if item_file is not None and destination.exists():
            action = replace(action, already_applied=True)
        actions.append(action)
    return RecommendationPlan(
        tuple(actions), Counter(action.recommendation.action for action in actions)
    )


def print_review_table(rows: Iterable[Recommendation]) -> None:
    for row in rows:
        print(
            f"line={row.line_number} action={row.action} "
            f"confidence={row.confidence} filename={row.filename!r} "
            f"citation_group={row.citation_group.name!r} "
            f"source={row.file.source_path!r}"
        )
        generic_recommendations._print_review_detail(
            "citation group", repr(row.citation_group)
        )
        for field_name, value in row.fields.items():
            generic_recommendations._print_review_detail(
                "field", f"{field_name}={value!r}"
            )
        for tag in row.serialized_tags:
            generic_recommendations._print_review_detail(
                "tag",
                generic_recommendations._format_serialized_tag(ItemFile.tags, tag),
            )
        generic_recommendations._print_review_detail("file", repr(row.file))


def add_virtual_models(
    plan: RecommendationPlan,
    builder: ProposalBuilder,
    *,
    new_citation_groups: dict[str, CitationGroup] | None = None,
) -> None:
    if new_citation_groups is None:
        new_citation_groups = {}
    for action in plan.actions:
        context = f"line {action.recommendation.line_number} {CREATE_ITEM_FILE}"
        if action.item_file is not None:
            builder.copy(action.item_file, context=context)
            continue
        builder.create(
            ItemFile,
            context=context,
            filename=action.recommendation.filename,
            citation_group=citation_group_recommendations.add_virtual_model(
                action.citation_group,
                builder,
                context=context,
                new_citation_groups=new_citation_groups,
            ),
            tags=action.tags,
            title=action.fields.get("title"),
            series=action.fields.get("series"),
            volume=action.fields.get("volume"),
            issue=action.fields.get("issue"),
            start_page=action.fields.get("start_page"),
            end_page=action.fields.get("end_page"),
            url=action.fields.get("url"),
        )


def _create_item_file(values: Mapping[str, Any]) -> ItemFile:
    return ItemFile.create(**values)


def _install_pdf(source: Path, destination: Path, expected_sha256: str) -> None:
    if destination.exists():
        return
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            with source.open("rb") as input_file:
                shutil.copyfileobj(input_file, output)
            output.flush()
            os.fsync(output.fileno())
        if _file_digest(temporary)[1] != expected_sha256:
            raise RecommendationError(f"copied PDF checksum mismatch for {destination}")
        temporary.replace(destination)
        temporary = None
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def execute_plan(
    plan: RecommendationPlan,
    *,
    apply: bool,
    create_item_file: Callable[[Mapping[str, Any]], ItemFile] = _create_item_file,
    install_pdf: Callable[[Path, Path, str], None] = _install_pdf,
    create_citation_group: Callable[[PlannedCitationGroup], CitationGroup] = (
        citation_group_recommendations.create_citation_group
    ),
    created_citation_groups: dict[str, CitationGroup] | None = None,
) -> tuple[ItemFile, ...]:
    resolved: list[ItemFile] = []
    if created_citation_groups is None:
        created_citation_groups = {}
    for action in plan.actions:
        status = (
            "ALREADY_APPLIED"
            if action.already_applied
            else ("APPLY" if apply else "WOULD_CREATE")
        )
        print(
            f"{status} action={CREATE_ITEM_FILE} "
            f"filename={action.recommendation.filename!r} "
            f"citation_group={action.citation_group.spec.name!r} "
            f"source={str(action.source_path)!r} "
            f"destination={str(action.destination_path)!r} "
            f"sha256={action.recommendation.file.sha256}"
        )
        if not apply:
            continue
        if not action.destination_path.exists():
            install_pdf(
                action.source_path,
                action.destination_path,
                action.recommendation.file.sha256,
            )
        item_file = action.item_file
        if item_file is None:
            cg = action.citation_group.citation_group
            if cg is None:
                cg = created_citation_groups.get(action.citation_group.spec.name)
                if cg is None:
                    cg = create_citation_group(action.citation_group)
                    created_citation_groups[action.citation_group.spec.name] = cg
            item_file = create_item_file(
                {
                    "filename": action.recommendation.filename,
                    "citation_group": cg,
                    "tags": action.tags,
                    **_expected_fields(action.fields),
                }
            )
        resolved.append(item_file)
        if action.source_path.exists():
            action.source_path.unlink()
    mode = "Applied" if apply else "Dry run"
    print(f"{mode}: {len(plan.actions)} {CREATE_ITEM_FILE} recommendation(s).")
    if not apply:
        print("No database or filesystem changes made. Pass --apply to execute.")
    return tuple(resolved)
