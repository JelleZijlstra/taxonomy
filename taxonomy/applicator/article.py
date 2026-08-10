"""Planner and executor for adding a cataloged Article and optional staged PDF.

``create_article`` is intentionally specialized: it validates bibliographic metadata,
an optional CitationGroup creation, an optional parent Article, and the filesystem move
as one restartable plan.
The public CLI is :mod:`scripts.apply_recommendations`.
"""

from __future__ import annotations

import datetime
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
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import ArticleKind, ArticleType, NamingConvention, PersonType
from taxonomy.db.helpers import trimdoi
from taxonomy.db.models import Article, CitationGroup, Person, Region
from taxonomy.db.models.article.api_data import expand_doi_json
from taxonomy.db.models.article.article import ArticleTag
from taxonomy.db.models.article.lint import is_valid_doi
from taxonomy.db.models.article.name_parser import get_name_parser
from taxonomy.db.models.citation_group import CitationGroupTag
from taxonomy.db.models.citation_group.cg import CitationGroupStatus
from taxonomy.db.models.person import AuthorTag, VirtualPerson

SCHEMA_VERSION = 1
CREATE_ARTICLE = "create_article"
ALLOWED_ACTIONS = {CREATE_ARTICLE}
ALLOWED_CONFIDENCES = {"high", "medium", "low"}
SUPPORTED_ARTICLE_TYPES = {
    ArticleType.JOURNAL,
    ArticleType.CHAPTER,
    ArticleType.BOOK,
    ArticleType.THESIS,
    ArticleType.WEB,
    ArticleType.MISCELLANEOUS,
    ArticleType.PART,
}

SCALAR_FIELDS = {
    "year",
    "title",
    "series",
    "volume",
    "issue",
    "start_page",
    "end_page",
    "url",
    "publisher",
    "pages",
    "misc_data",
    "article_number",
}
AUTHOR_FIELDS = {"family_name", "given_names", "initials", "tussenvoegsel", "suffix"}


class RecommendationError(Exception):
    pass


class OptionsLike(Protocol):
    @property
    def new_path(self) -> Path: ...

    @property
    def library_path(self) -> Path: ...


@dataclass(frozen=True, slots=True)
class Evidence:
    kind: str
    text: str


@dataclass(frozen=True, slots=True)
class PersonSpec:
    family_name: str
    given_names: str | None = None
    initials: str | None = None
    tussenvoegsel: str | None = None
    suffix: str | None = None

    @classmethod
    def from_virtual_person(cls, person: VirtualPerson) -> PersonSpec:
        return cls(
            family_name=person.family_name,
            given_names=person.given_names,
            initials=person.initials,
            tussenvoegsel=person.tussenvoegsel,
            suffix=person.suffix,
        )

    def as_kwargs(self) -> dict[str, str | None]:
        return {
            "family_name": self.family_name,
            "given_names": self.given_names,
            "initials": self.initials,
            "tussenvoegsel": self.tussenvoegsel,
            "suffix": self.suffix,
        }


@dataclass(frozen=True, slots=True)
class RegionSpec:
    region_id: int
    region_name: str


@dataclass(frozen=True, slots=True)
class CitationGroupSpec:
    citation_group_id: int | None
    name: str
    article_type: ArticleType | None
    region: RegionSpec | None
    serialized_tags: tuple[Any, ...]

    @property
    def requests_creation(self) -> bool:
        return self.citation_group_id is None


@dataclass(frozen=True, slots=True)
class FileSpec:
    source_path: str
    destination_folder: str
    sha256: str
    size: int


@dataclass(frozen=True, slots=True)
class ParentSpec:
    article_id: int | None
    name: str

    @property
    def is_planned(self) -> bool:
        return self.article_id is None


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[Evidence, ...]
    name: str
    doi: str | None
    article_type: ArticleType | None
    fields: Mapping[str, str | None]
    authors: tuple[PersonSpec, ...] | None
    serialized_tags: tuple[Any, ...] | None
    citation_group: CitationGroupSpec | None
    parent: ParentSpec | None
    file: FileSpec | None


@dataclass(frozen=True, slots=True)
class PlannedCitationGroup:
    spec: CitationGroupSpec
    region: Region | None
    citation_group: CitationGroup | None
    tags: tuple[CitationGroupTag, ...]


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    source_path: Path | None
    destination_path: Path | None
    article: Article | None
    citation_group: PlannedCitationGroup | None
    parent_article: Article | None
    planned_parent_name: str | None
    kind: ArticleKind
    article_type: ArticleType
    fields: Mapping[str, str | None]
    authors: tuple[PersonSpec, ...]
    tags: tuple[ArticleTag, ...]
    crossref_journal: str | None
    already_applied: bool


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedAction, ...]
    action_counts: Counter[str]


def _required(data: Mapping[str, Any], key: str, line: int) -> Any:
    if key not in data or data[key] is None or data[key] == "":
        raise RecommendationError(f"line {line}: {key!r} is required")
    return data[key]


def _required_str(data: Mapping[str, Any], key: str, line: int) -> str:
    value = _required(data, key, line)
    if not isinstance(value, str):
        raise RecommendationError(f"line {line}: {key!r} must be a string")
    return value


def _required_int(data: Mapping[str, Any], key: str, line: int) -> int:
    value = _required(data, key, line)
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecommendationError(f"line {line}: {key!r} must be an integer")
    return value


def _object(value: Any, label: str, line: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RecommendationError(f"line {line}: {label} must be an object")
    return value


def _parse_type(value: Any, label: str, line: int) -> ArticleType:
    if not isinstance(value, str):
        raise RecommendationError(f"line {line}: {label} must be an ArticleType name")
    try:
        return ArticleType[value.upper()]
    except KeyError as exc:
        raise RecommendationError(
            f"line {line}: unknown ArticleType {value!r} in {label}"
        ) from exc


def _parse_evidence(value: Any, line: int) -> tuple[Evidence, ...]:
    if not isinstance(value, list) or not value:
        raise RecommendationError(f"line {line}: evidence must be a nonempty list")
    output = []
    for item in value:
        data = _object(item, "evidence item", line)
        output.append(
            Evidence(
                _required_str(data, "kind", line), _required_str(data, "text", line)
            )
        )
    return tuple(output)


def _parse_authors(value: Any, line: int) -> tuple[PersonSpec, ...]:
    if not isinstance(value, list) or not value:
        raise RecommendationError(
            f"line {line}: article.authors must be a nonempty list"
        )
    output = []
    for item in value:
        data = _object(item, "article author", line)
        unknown = set(data) - AUTHOR_FIELDS
        if unknown:
            raise RecommendationError(
                f"line {line}: unsupported author field(s): {', '.join(sorted(unknown))}"
            )
        kwargs: dict[str, str | None] = {}
        for field in AUTHOR_FIELDS:
            field_value = data.get(field)
            if field_value is not None and not isinstance(field_value, str):
                raise RecommendationError(
                    f"line {line}: author {field} must be a string or null"
                )
            kwargs[field] = field_value
        family_name = kwargs.pop("family_name")
        if not family_name:
            raise RecommendationError(f"line {line}: author family_name is required")
        output.append(PersonSpec(family_name=family_name, **kwargs))
    return tuple(output)


def _parse_citation_group(value: Any, line: int) -> CitationGroupSpec:
    data = _object(value, "article.citation_group", line)
    name = _required_str(data, "name", line)
    object_id = data.get("id")
    if object_id is not None:
        if not isinstance(object_id, int) or isinstance(object_id, bool):
            raise RecommendationError(
                f"line {line}: citation_group.id must be an integer"
            )
        if set(data) != {"id", "name"}:
            raise RecommendationError(
                f"line {line}: an existing citation_group accepts only id and name"
            )
        return CitationGroupSpec(object_id, name, None, None, ())
    article_type = _parse_type(
        _required(data, "type", line), "citation_group.type", line
    )
    region_data = _object(
        _required(data, "region", line), "citation_group.region", line
    )
    region = RegionSpec(
        _required_int(region_data, "id", line), _required_str(region_data, "name", line)
    )
    tags = data.get("tags", [])
    if not isinstance(tags, list):
        raise RecommendationError(f"line {line}: citation_group.tags must be a list")
    unknown = set(data) - {"name", "type", "region", "tags"}
    if unknown:
        raise RecommendationError(
            f"line {line}: unsupported citation_group field(s): {', '.join(sorted(unknown))}"
        )
    return CitationGroupSpec(None, name, article_type, region, tuple(tags))


def _parse_parent(value: Any, line: int) -> ParentSpec:
    data = _object(value, "article.parent", line)
    name = _required_str(data, "name", line)
    object_id = data.get("id")
    if object_id is None:
        if set(data) != {"name"}:
            raise RecommendationError(
                f"line {line}: a planned parent accepts only name"
            )
        return ParentSpec(None, name)
    if not isinstance(object_id, int) or isinstance(object_id, bool):
        raise RecommendationError(f"line {line}: article.parent.id must be an integer")
    if set(data) != {"id", "name"}:
        raise RecommendationError(
            f"line {line}: an existing parent accepts only id and name"
        )
    return ParentSpec(object_id, name)


def _parse_file(value: Any, line: int) -> FileSpec:
    file_data = _object(value, "file", line)
    source_path = _required_str(file_data, "source_path", line)
    destination_folder = _required_str(file_data, "destination_folder", line)
    sha256 = _required_str(file_data, "sha256", line).lower()
    if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
        raise RecommendationError(
            f"line {line}: file.sha256 must be 64 hexadecimal characters"
        )
    size = _required_int(file_data, "size", line)
    if size <= 0:
        raise RecommendationError(f"line {line}: file.size must be positive")
    if set(file_data) != {"source_path", "destination_folder", "sha256", "size"}:
        raise RecommendationError(
            f"line {line}: file accepts source_path, destination_folder, sha256, and size"
        )
    return FileSpec(source_path, destination_folder, sha256, size)


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    version = _required_int(data, "schema_version", line_number)
    if version != SCHEMA_VERSION:
        raise RecommendationError(
            f"line {line_number}: unsupported schema_version {version}"
        )
    action = _required_str(data, "action", line_number)
    if action != CREATE_ARTICLE:
        raise RecommendationError(f"line {line_number}: unsupported action {action!r}")
    confidence = _required_str(data, "confidence", line_number)
    if confidence not in ALLOWED_CONFIDENCES:
        raise RecommendationError(
            f"line {line_number}: unsupported confidence {confidence!r}"
        )
    article = _object(_required(data, "article", line_number), "article", line_number)
    name = _required_str(article, "name", line_number)
    file = _parse_file(data["file"], line_number) if "file" in data else None
    parser = get_name_parser(name)
    if errors := parser.get_errors():
        raise RecommendationError(
            f"line {line_number}: invalid Article name {name!r}: {'; '.join(errors)}"
        )
    if file is not None and parser.extension != "pdf":
        raise RecommendationError(
            f"line {line_number}: Article name must have lowercase .pdf extension"
        )
    if file is None and parser.extension:
        raise RecommendationError(
            f"line {line_number}: non-electronic Article name must not have an extension"
        )
    doi_value = article.get("doi")
    if doi_value is not None and not isinstance(doi_value, str):
        raise RecommendationError(f"line {line_number}: article.doi must be a string")
    doi = trimdoi(doi_value) if doi_value else None
    if doi is not None and not is_valid_doi(doi):
        raise RecommendationError(f"line {line_number}: invalid DOI {doi!r}")
    type_value = article.get("type")
    article_type = (
        _parse_type(type_value, "article.type", line_number)
        if type_value is not None
        else None
    )
    fields_data = article.get("fields", {})
    if not isinstance(fields_data, dict):
        raise RecommendationError(
            f"line {line_number}: article.fields must be an object"
        )
    unknown_fields = set(fields_data) - SCALAR_FIELDS
    if unknown_fields:
        raise RecommendationError(
            f"line {line_number}: unsupported Article field(s): {', '.join(sorted(unknown_fields))}"
        )
    fields: dict[str, str | None] = {}
    for field, value in fields_data.items():
        if value is not None and not isinstance(value, str):
            raise RecommendationError(
                f"line {line_number}: article.fields.{field} must be a string or null"
            )
        fields[field] = value
    authors = (
        _parse_authors(article["authors"], line_number)
        if "authors" in article
        else None
    )
    tags_value = article.get("tags")
    if tags_value is not None and not isinstance(tags_value, list):
        raise RecommendationError(f"line {line_number}: article.tags must be a list")
    citation_group = (
        _parse_citation_group(article["citation_group"], line_number)
        if "citation_group" in article
        else None
    )
    parent = (
        _parse_parent(article["parent"], line_number) if "parent" in article else None
    )
    unknown_article = set(article) - {
        "name",
        "doi",
        "type",
        "fields",
        "authors",
        "tags",
        "citation_group",
        "parent",
    }
    if unknown_article:
        raise RecommendationError(
            f"line {line_number}: unsupported article key(s): {', '.join(sorted(unknown_article))}"
        )
    if doi is None and article_type is None:
        raise RecommendationError(
            f"line {line_number}: article requires a DOI or an explicit type"
        )
    allowed_top = {
        "schema_version",
        "action",
        "confidence",
        "reason",
        "evidence",
        "article",
        "file",
    }
    if unknown_top := set(data) - allowed_top:
        raise RecommendationError(
            f"line {line_number}: unsupported top-level key(s): {', '.join(sorted(unknown_top))}"
        )
    return Recommendation(
        line_number=line_number,
        action=action,
        confidence=confidence,
        reason=_required_str(data, "reason", line_number),
        evidence=_parse_evidence(data.get("evidence"), line_number),
        name=name,
        doi=doi,
        article_type=article_type,
        fields=fields,
        authors=authors,
        serialized_tags=tuple(tags_value) if tags_value is not None else None,
        citation_group=citation_group,
        parent=parent,
        file=file,
    )


def _safe_relative_path(raw: str, *, label: str, line: int) -> Path:
    path = Path(raw)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise RecommendationError(
            f"line {line}: {label} must be a normalized relative path"
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
            f"line {line}: {label} size changed: expected {spec.size}, found {actual_size}"
        )
    if digest != spec.sha256:
        raise RecommendationError(
            f"line {line}: {label} checksum changed: expected {spec.sha256}, found {digest}"
        )


def _get_article(name: str) -> Article | None:
    try:
        return Article.get(name=name)
    except Article.DoesNotExist:
        return None


def _get_article_by_id(object_id: int) -> Article:
    try:
        return Article.get(id=object_id)
    except Article.DoesNotExist as exc:
        raise RecommendationError(f"Article {object_id} does not exist") from exc


def _articles_with_doi(doi: str) -> Iterable[Article]:
    return Article.select_valid().filter(Article.doi == doi)


def _is_catalog_folder(path: str) -> bool:
    return any(Article.select_valid().filter(Article.path == path))


def _get_citation_group(object_id: int) -> CitationGroup:
    try:
        return CitationGroup.get(id=object_id)
    except CitationGroup.DoesNotExist as exc:
        raise RecommendationError(f"CitationGroup {object_id} does not exist") from exc


def _citation_groups_named(name: str) -> Iterable[CitationGroup]:
    return CitationGroup.select().filter(CitationGroup.name == name)


def _get_region(object_id: int) -> Region:
    try:
        return Region.get(id=object_id)
    except Region.DoesNotExist as exc:
        raise RecommendationError(f"Region {object_id} does not exist") from exc


def _decode_tags(
    serialized: tuple[Any, ...], adt_type: Any, *, context: str
) -> tuple[Any, ...]:
    try:
        return tuple(adt_type.unserialize(tag) for tag in serialized)
    except Exception as exc:
        raise RecommendationError(f"{context}: invalid serialized tag: {exc}") from exc


def _person_key(
    person: Person | PersonSpec,
) -> tuple[str, str | None, str | None, str | None, str | None]:
    return (
        person.family_name,
        person.given_names,
        person.initials,
        person.tussenvoegsel,
        person.suffix,
    )


def _check_field_snapshot(article: Article, action: PlannedAction) -> None:
    expected = {
        "kind": action.kind,
        "path": (
            action.recommendation.file.destination_folder
            if action.recommendation.file is not None
            else None
        ),
        "doi": action.recommendation.doi,
        "type": action.article_type,
        **action.fields,
    }
    mismatches = [
        f"{field}={getattr(article, field)!r} (expected {value!r})"
        for field, value in expected.items()
        if getattr(article, field) != value
    ]
    expected_cg = (
        action.citation_group.citation_group if action.citation_group else None
    )
    actual_cg_id = (
        article.citation_group.id if article.citation_group is not None else None
    )
    expected_cg_id = expected_cg.id if expected_cg is not None else None
    if actual_cg_id != expected_cg_id:
        mismatches.append(
            f"citation_group={article.citation_group!r} (expected {expected_cg!r})"
        )
    actual_parent_id = article.parent.id if article.parent is not None else None
    expected_parent_id = (
        action.parent_article.id if action.parent_article is not None else None
    )
    if actual_parent_id != expected_parent_id:
        mismatches.append(
            f"parent={article.parent!r} (expected {action.parent_article!r})"
        )
    if tuple(_person_key(person) for person in article.get_authors()) != tuple(
        _person_key(person) for person in action.authors
    ):
        mismatches.append("authors differ")
    if frozenset(article.tags or ()) != frozenset(action.tags):
        mismatches.append("tags differ")
    if mismatches:
        raise RecommendationError(
            f"line {action.recommendation.line_number}: existing Article "
            f"{article.name!r} conflicts with recommendation: {'; '.join(mismatches)}"
        )


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    options: OptionsLike | None = None,
    get_article: Callable[[str], Article | None] = _get_article,
    get_article_by_id: Callable[[int], Article] = _get_article_by_id,
    articles_with_doi: Callable[[str], Iterable[Article]] = _articles_with_doi,
    is_catalog_folder: Callable[[str], bool] = _is_catalog_folder,
    get_citation_group: Callable[[int], CitationGroup] = _get_citation_group,
    citation_groups_named: Callable[
        [str], Iterable[CitationGroup]
    ] = _citation_groups_named,
    get_region: Callable[[int], Region] = _get_region,
    expand_doi: Callable[[str], Mapping[str, Any]] = expand_doi_json,
) -> RecommendationPlan:
    resolved_options: OptionsLike = options or config.get_options()
    actions: list[PlannedAction] = []
    seen_names: set[str] = set()
    seen_dois: set[str] = set()
    seen_sources: set[Path] = set()
    planned_new_citation_groups: dict[str, PlannedCitationGroup] = {}
    planned_actions_by_name: dict[str, PlannedAction] = {}
    for recommendation in recommendations:
        line = recommendation.line_number
        if recommendation.name in seen_names:
            raise RecommendationError(
                f"line {line}: duplicate create_article name {recommendation.name!r}"
            )
        seen_names.add(recommendation.name)
        if recommendation.doi is not None:
            if recommendation.doi in seen_dois:
                raise RecommendationError(
                    f"line {line}: duplicate create_article DOI {recommendation.doi!r}"
                )
            seen_dois.add(recommendation.doi)
        source: Path | None = None
        destination: Path | None = None
        if recommendation.file is not None:
            source_rel = _safe_relative_path(
                recommendation.file.source_path, label="file.source_path", line=line
            )
            folder_rel = _safe_relative_path(
                recommendation.file.destination_folder,
                label="file.destination_folder",
                line=line,
            )
            source = resolved_options.new_path / source_rel
            destination_dir = resolved_options.library_path / folder_rel
            destination = destination_dir / recommendation.name
            new_root = resolved_options.new_path.resolve()
            library_root = resolved_options.library_path.resolve()
            if not source.resolve(strict=False).is_relative_to(new_root):
                raise RecommendationError(
                    f"line {line}: staged file resolves outside new_path: {source}"
                )
            resolved_source = source.resolve(strict=False)
            if resolved_source in seen_sources:
                raise RecommendationError(
                    f"line {line}: staged file is used by more than one recommendation: "
                    f"{source}"
                )
            seen_sources.add(resolved_source)
            if not destination_dir.is_dir():
                raise RecommendationError(
                    f"line {line}: destination folder does not exist: {destination_dir}"
                )
            if not is_catalog_folder(recommendation.file.destination_folder):
                raise RecommendationError(
                    f"line {line}: destination is not an existing catalog folder: "
                    f"{recommendation.file.destination_folder!r}"
                )
            if not destination_dir.resolve().is_relative_to(library_root):
                raise RecommendationError(
                    f"line {line}: destination folder resolves outside library_path: "
                    f"{destination_dir}"
                )
            if source.is_symlink():
                raise RecommendationError(
                    f"line {line}: staged file may not be a symlink"
                )
            if destination.is_symlink():
                raise RecommendationError(
                    f"line {line}: destination may not be a symlink"
                )

        raw: Mapping[str, Any] = {}
        if recommendation.doi is not None:
            raw = expand_doi(recommendation.doi)
            if not raw:
                raise RecommendationError(
                    f"line {line}: CrossRef returned no metadata for DOI {recommendation.doi}"
                )
        raw_type = raw.get("type")
        article_type = recommendation.article_type or (
            raw_type if isinstance(raw_type, ArticleType) else None
        )
        if article_type is None or article_type is ArticleType.ERROR:
            raise RecommendationError(
                f"line {line}: DOI metadata did not yield a usable Article type; set article.type"
            )
        if article_type not in SUPPORTED_ARTICLE_TYPES:
            raise RecommendationError(
                f"line {line}: create_article does not support {article_type.name}"
            )
        kind = (
            ArticleKind.electronic
            if recommendation.file is not None
            else ArticleKind.no_copy
        )
        if kind is ArticleKind.no_copy and article_type is not ArticleType.BOOK:
            raise RecommendationError(f"line {line}: only BOOK Articles may omit file")
        if article_type in {ArticleType.CHAPTER, ArticleType.PART}:
            if recommendation.parent is None:
                raise RecommendationError(
                    f"line {line}: {article_type.name} Article requires article.parent"
                )
            if recommendation.citation_group is not None:
                raise RecommendationError(
                    f"line {line}: {article_type.name} Article inherits its "
                    "CitationGroup from its parent"
                )
        elif recommendation.parent is not None:
            raise RecommendationError(
                f"line {line}: article.parent is supported only for CHAPTER and PART"
            )
        fields = {
            field: value
            for field, value in raw.items()
            if field in SCALAR_FIELDS and (value is None or isinstance(value, str))
        }
        fields.update(recommendation.fields)
        raw_authors = raw.get("author_tags")
        if recommendation.authors is not None:
            authors = recommendation.authors
        elif isinstance(raw_authors, list) and all(
            isinstance(author, VirtualPerson) for author in raw_authors
        ):
            authors = tuple(
                PersonSpec.from_virtual_person(author) for author in raw_authors
            )
        else:
            authors = ()
        if recommendation.serialized_tags is not None:
            tags = _decode_tags(
                recommendation.serialized_tags,
                ArticleTag,
                context=f"line {line} article.tags",
            )
        else:
            raw_tags = raw.get("tags", ())
            tags = tuple(raw_tags) if isinstance(raw_tags, list) else ()
            if not all(isinstance(tag, ArticleTag) for tag in tags):
                raise RecommendationError(
                    f"line {line}: DOI expansion returned invalid Article tags"
                )
        tag_set = set(tags)
        if recommendation.doi is not None and recommendation.doi.startswith("10.2307/"):
            jstor_id = recommendation.doi.removeprefix("10.2307/")
            if jstor_id.isnumeric():
                tag_set.add(ArticleTag.JSTOR(jstor_id))
        tags = tuple(sorted(tag_set))

        planned_cg: PlannedCitationGroup | None = None
        if spec := recommendation.citation_group:
            if spec.citation_group_id is not None:
                cg = get_citation_group(spec.citation_group_id)
                if (
                    cg.id != spec.citation_group_id
                    or cg.name != spec.name
                    or cg.is_invalid()
                ):
                    raise RecommendationError(
                        f"line {line}: CitationGroup {spec.citation_group_id} is not valid with name {spec.name!r}"
                    )
                planned_cg = PlannedCitationGroup(spec, None, cg, tuple(cg.tags or ()))
            else:
                assert spec.region is not None and spec.article_type is not None
                region = get_region(spec.region.region_id)
                if (
                    region.id != spec.region.region_id
                    or region.name != spec.region.region_name
                ):
                    raise RecommendationError(
                        f"line {line}: Region {region.id} changed from {spec.region.region_name!r} to {region.name!r}"
                    )
                cg_tags = _decode_tags(
                    spec.serialized_tags,
                    CitationGroupTag,
                    context=f"line {line} citation_group.tags",
                )
                matches = list(citation_groups_named(spec.name))
                if len(matches) > 1:
                    raise RecommendationError(
                        f"line {line}: multiple CitationGroups are named {spec.name!r}"
                    )
                existing_cg = matches[0] if matches else None
                if existing_cg is not None and (
                    existing_cg.status is not CitationGroupStatus.normal
                    or existing_cg.type is not spec.article_type
                    or existing_cg.region is None
                    or existing_cg.region.id != region.id
                    or frozenset(existing_cg.tags or ()) != frozenset(cg_tags)
                ):
                    raise RecommendationError(
                        f"line {line}: existing CitationGroup {spec.name!r} conflicts with requested creation"
                    )
                planned_cg = PlannedCitationGroup(spec, region, existing_cg, cg_tags)
                if existing_cg is None:
                    earlier = planned_new_citation_groups.get(spec.name)
                    if earlier is not None:
                        assert earlier.region is not None
                        if earlier.spec != spec or earlier.region.id != region.id:
                            raise RecommendationError(
                                f"line {line}: conflicting definitions for new "
                                f"CitationGroup {spec.name!r}"
                            )
                        planned_cg = earlier
                    else:
                        planned_new_citation_groups[spec.name] = planned_cg
        if (
            article_type in {ArticleType.JOURNAL, ArticleType.THESIS}
            and planned_cg is None
        ):
            raise RecommendationError(
                f"line {line}: {article_type.name} Article requires article.citation_group"
            )
        if planned_cg is not None:
            cg_type = (
                planned_cg.citation_group.type
                if planned_cg.citation_group
                else planned_cg.spec.article_type
            )
            if (
                article_type
                in {ArticleType.JOURNAL, ArticleType.BOOK, ArticleType.THESIS}
                and cg_type is not article_type
            ):
                raise RecommendationError(
                    f"line {line}: CitationGroup type {cg_type!r} does not match Article type {article_type.name}"
                )

        parent_article: Article | None = None
        planned_parent_name: str | None = None
        if parent_spec := recommendation.parent:
            if parent_spec.article_id is not None:
                parent_article = get_article_by_id(parent_spec.article_id)
                if (
                    parent_article.id != parent_spec.article_id
                    or parent_article.name != parent_spec.name
                    or parent_article.is_invalid()
                ):
                    raise RecommendationError(
                        f"line {line}: parent Article {parent_spec.article_id} is not "
                        f"valid with name {parent_spec.name!r}"
                    )
                parent_type = parent_article.type
            else:
                earlier_action = planned_actions_by_name.get(parent_spec.name)
                if earlier_action is None:
                    raise RecommendationError(
                        f"line {line}: planned parent {parent_spec.name!r} must be an "
                        "earlier create_article recommendation"
                    )
                parent_article = earlier_action.article
                parent_type = earlier_action.article_type
                planned_parent_name = parent_spec.name
            if parent_type is not ArticleType.BOOK:
                raise RecommendationError(
                    f"line {line}: {article_type.name} parent must be a BOOK Article"
                )

        existing = get_article(recommendation.name)
        if recommendation.doi is not None:
            collisions = [
                article
                for article in articles_with_doi(recommendation.doi)
                if existing is None or article.id != existing.id
            ]
            if collisions:
                raise RecommendationError(
                    f"line {line}: DOI {recommendation.doi} is already used by {collisions[0]}"
                )
        action = PlannedAction(
            recommendation=recommendation,
            source_path=source,
            destination_path=destination,
            article=existing,
            citation_group=planned_cg,
            parent_article=parent_article,
            planned_parent_name=planned_parent_name,
            kind=kind,
            article_type=article_type,
            fields=fields,
            authors=authors,
            tags=tags,
            crossref_journal=(
                raw.get("journal") if isinstance(raw.get("journal"), str) else None
            ),
            already_applied=False,
        )
        if existing is not None:
            _check_field_snapshot(existing, action)
        if recommendation.file is None:
            if existing is not None:
                action = replace(action, already_applied=True)
        else:
            assert source is not None and destination is not None
            if source.exists():
                _check_pdf(source, recommendation.file, line=line, label="staged file")
            elif existing is None or not destination.exists():
                raise RecommendationError(
                    f"line {line}: staged file does not exist: {source}"
                )
            if destination.exists():
                _check_pdf(
                    destination,
                    recommendation.file,
                    line=line,
                    label="destination file",
                )
                if existing is None:
                    raise RecommendationError(
                        f"line {line}: destination exists without the recommended "
                        f"Article: {destination}"
                    )
                action = replace(action, already_applied=True)
        actions.append(action)
        planned_actions_by_name[recommendation.name] = action
    return RecommendationPlan(
        tuple(actions), Counter(action.recommendation.action for action in actions)
    )


def print_review_table(rows: Iterable[Recommendation]) -> None:
    for row in rows:
        cg = row.citation_group.name if row.citation_group else "-"
        parent = row.parent.name if row.parent else "-"
        destination = row.file.destination_folder if row.file else "-"
        print(
            f"line={row.line_number} action={row.action} confidence={row.confidence} "
            f"name={row.name!r} doi={row.doi!r} citation_group={cg!r} "
            f"parent={parent!r} destination={destination!r}"
        )


def add_virtual_models(plan: RecommendationPlan, builder: ProposalBuilder) -> None:
    now = datetime.datetime.now(tz=datetime.UTC)
    new_citation_groups: dict[str, CitationGroup] = {}
    planned_articles: dict[str, Article] = {}
    for action in plan.actions:
        context = f"line {action.recommendation.line_number} create_article"
        if action.article is not None:
            article = builder.copy(action.article, context=context)
            planned_articles[action.recommendation.name] = article
            continue
        cg: CitationGroup | None = None
        if action.citation_group is not None:
            planned = action.citation_group
            if planned.citation_group is not None:
                cg = builder.copy(planned.citation_group, context=context)
            else:
                assert (
                    planned.region is not None and planned.spec.article_type is not None
                )
                cg = new_citation_groups.get(planned.spec.name)
                if cg is None:
                    cg = builder.create(
                        CitationGroup,
                        context=context,
                        name=planned.spec.name,
                        type=planned.spec.article_type,
                        region=planned.region,
                        status=CitationGroupStatus.normal,
                        tags=planned.tags,
                    )
                    new_citation_groups[planned.spec.name] = cg
        virtual_people = [
            builder.create(
                Person,
                context=context,
                family_name=person.family_name,
                given_names=person.given_names,
                initials=person.initials,
                tussenvoegsel=person.tussenvoegsel,
                suffix=person.suffix,
                type=PersonType.unchecked,
                naming_convention=NamingConvention.unspecified,
            )
            for person in action.authors
        ]
        parent: Article | None = None
        if action.planned_parent_name is not None:
            parent = planned_articles[action.planned_parent_name]
        elif action.parent_article is not None:
            parent = builder.copy(action.parent_article, context=context)
        path = (
            action.recommendation.file.destination_folder
            if action.recommendation.file is not None
            else None
        )
        article = builder.create(
            Article,
            context=context,
            name=action.recommendation.name,
            kind=action.kind,
            path=path,
            doi=action.recommendation.doi,
            type=action.article_type,
            citation_group=cg,
            parent=parent,
            author_tags=tuple(
                AuthorTag.Author(person=person) for person in virtual_people
            ),
            tags=action.tags,
            _location="None",
            misc_data="None",
            addmonth=str(now.month),
            addday=str(now.day),
            addyear=str(now.year),
        )
        for field, value in action.fields.items():
            setattr(article, field, value)
        planned_articles[action.recommendation.name] = article


def _create_citation_group(planned: PlannedCitationGroup) -> CitationGroup:
    assert planned.region is not None and planned.spec.article_type is not None
    return CitationGroup.create(
        name=planned.spec.name,
        type=planned.spec.article_type,
        region=planned.region,
        status=CitationGroupStatus.normal,
        tags=planned.tags,
    )


def _create_article(name: str, values: Mapping[str, Any]) -> Article:
    return Article.make(name, **values)


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


def _run_auxiliary(article: Article, *, add_history: bool) -> None:
    article.store_pdf_content()
    article.index_pdf_for_search()
    if add_history:
        article.add_to_history()
        article.add_to_history("name")


def _add_article_history(article: Article) -> None:
    article.add_to_history()
    article.add_to_history("name")


def execute_plan(
    plan: RecommendationPlan,
    *,
    apply: bool,
    create_citation_group: Callable[
        [PlannedCitationGroup], CitationGroup
    ] = _create_citation_group,
    get_or_create_person: Callable[..., Person] = Person.get_or_create_unchecked,
    create_article: Callable[[str, Mapping[str, Any]], Article] = _create_article,
    install_pdf: Callable[[Path, Path, str], None] = _install_pdf,
    run_auxiliary: Callable[..., None] = _run_auxiliary,
    add_article_history: Callable[[Article], None] = _add_article_history,
) -> None:
    created_citation_groups: dict[str, CitationGroup] = {}
    resolved_articles: dict[str, Article] = {}
    for action in plan.actions:
        status = (
            "ALREADY_APPLIED"
            if action.already_applied
            else ("APPLY" if apply else "WOULD_CREATE")
        )
        cg_name = action.citation_group.spec.name if action.citation_group else None
        parent_name = (
            action.recommendation.parent.name if action.recommendation.parent else None
        )
        file = action.recommendation.file
        print(
            f"{status} action=create_article name={action.recommendation.name!r} "
            f"doi={action.recommendation.doi!r} type={action.article_type.name} "
            f"kind={action.kind.name} citation_group={cg_name!r} "
            f"parent={parent_name!r} source={str(action.source_path) if action.source_path else None!r} "
            f"destination={str(action.destination_path) if action.destination_path else None!r} "
            f"sha256={file.sha256 if file else None}"
        )
        if action.crossref_journal is not None:
            print(f"  CrossRef journal: {action.crossref_journal}")
        if not apply:
            continue
        cg = action.citation_group.citation_group if action.citation_group else None
        if action.citation_group is not None and cg is None:
            cg = created_citation_groups.get(action.citation_group.spec.name)
            if cg is None:
                cg = create_citation_group(action.citation_group)
                created_citation_groups[action.citation_group.spec.name] = cg
        parent = action.parent_article
        if action.planned_parent_name is not None:
            parent = resolved_articles[action.planned_parent_name]
        article = action.article
        if article is None:
            people = [
                get_or_create_person(**person.as_kwargs()) for person in action.authors
            ]
            values: dict[str, Any] = {
                "kind": action.kind,
                "path": file.destination_folder if file is not None else None,
                "doi": action.recommendation.doi,
                "type": action.article_type,
                "citation_group": cg,
                "parent": parent,
                "author_tags": tuple(
                    AuthorTag.Author(person=person) for person in people
                ),
                "tags": action.tags,
                **action.fields,
            }
            article = create_article(action.recommendation.name, values)
        resolved_articles[action.recommendation.name] = article
        if file is None:
            if action.article is None:
                add_article_history(article)
            continue
        assert action.source_path is not None and action.destination_path is not None
        if not action.destination_path.exists():
            install_pdf(action.source_path, action.destination_path, file.sha256)
        run_auxiliary(article, add_history=True)
        if action.source_path.exists():
            action.source_path.unlink()
    mode = "Applied" if apply else "Dry run"
    print(f"{mode}: {len(plan.actions)} create_article recommendation(s).")
    if not apply:
        print("No database or filesystem changes made. Pass --apply to execute.")
