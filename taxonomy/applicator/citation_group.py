"""Shared CitationGroup dependencies for Article and ItemFile recommendations."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import ArticleType
from taxonomy.db.models import CitationGroup, Region
from taxonomy.db.models.citation_group import CitationGroupTag
from taxonomy.db.models.citation_group.cg import CitationGroupStatus


class RecommendationError(Exception):
    pass


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
class PlannedCitationGroup:
    spec: CitationGroupSpec
    region: Region | None
    citation_group: CitationGroup | None
    tags: tuple[CitationGroupTag, ...]


def parse_spec(value: Any, *, line: int, context: str) -> CitationGroupSpec:
    def required(data: dict[str, Any], key: str, typ: type) -> Any:
        result = data.get(key)
        if not isinstance(result, typ) or isinstance(result, bool) or result == "":
            raise RecommendationError(
                f"line {line}: {context}.{key} must be a nonempty {typ.__name__}"
            )
        return result

    if not isinstance(value, dict):
        raise RecommendationError(f"line {line}: {context} must be an object")
    name = required(value, "name", str)
    if "id" in value:
        object_id = required(value, "id", int)
        if set(value) != {"id", "name"}:
            raise RecommendationError(
                f"line {line}: an existing citation_group accepts only id and name"
            )
        return CitationGroupSpec(object_id, name, None, None, ())
    type_name = required(value, "type", str)
    try:
        article_type = ArticleType[type_name.upper()]
    except KeyError as exc:
        raise RecommendationError(
            f"line {line}: unknown ArticleType {type_name!r} in {context}.type"
        ) from exc
    region_data = required(value, "region", dict)
    region = RegionSpec(
        required(region_data, "id", int), required(region_data, "name", str)
    )
    if set(region_data) != {"id", "name"}:
        raise RecommendationError(
            f"line {line}: {context}.region accepts only id and name"
        )
    tags = value.get("tags", [])
    if not isinstance(tags, list):
        raise RecommendationError(f"line {line}: citation_group.tags must be a list")
    if unknown := set(value) - {"name", "type", "region", "tags"}:
        raise RecommendationError(
            f"line {line}: unsupported citation_group field(s): {', '.join(sorted(unknown))}"
        )
    return CitationGroupSpec(None, name, article_type, region, tuple(tags))


def get_citation_group(object_id: int) -> CitationGroup:
    try:
        return CitationGroup.get(id=object_id)
    except CitationGroup.DoesNotExist as exc:
        raise RecommendationError(f"CitationGroup {object_id} does not exist") from exc


def citation_groups_named(name: str) -> Iterable[CitationGroup]:
    return CitationGroup.select().filter(CitationGroup.name == name)


def get_region(object_id: int) -> Region:
    try:
        return Region.get(id=object_id)
    except Region.DoesNotExist as exc:
        raise RecommendationError(f"Region {object_id} does not exist") from exc


def plan_citation_group(
    spec: CitationGroupSpec,
    *,
    line: int,
    planned_new: dict[str, PlannedCitationGroup],
    get_citation_group: Callable[[int], CitationGroup] = get_citation_group,
    citation_groups_named: Callable[
        [str], Iterable[CitationGroup]
    ] = citation_groups_named,
    get_region: Callable[[int], Region] = get_region,
) -> PlannedCitationGroup:
    if spec.citation_group_id is not None:
        cg = get_citation_group(spec.citation_group_id)
        if cg.id != spec.citation_group_id or cg.name != spec.name or cg.is_invalid():
            raise RecommendationError(
                f"line {line}: CitationGroup {spec.citation_group_id} is not valid with name {spec.name!r}"
            )
        return PlannedCitationGroup(spec, None, cg, tuple(cg.tags or ()))
    assert spec.region is not None and spec.article_type is not None
    region = get_region(spec.region.region_id)
    if region.id != spec.region.region_id or region.name != spec.region.region_name:
        raise RecommendationError(
            f"line {line}: Region {region.id} changed from {spec.region.region_name!r} to {region.name!r}"
        )
    try:
        tags = tuple(CitationGroupTag.unserialize(tag) for tag in spec.serialized_tags)
    except Exception as exc:
        raise RecommendationError(
            f"line {line} citation_group.tags: invalid serialized tag: {exc}"
        ) from exc
    matches = list(citation_groups_named(spec.name))
    if len(matches) > 1:
        raise RecommendationError(
            f"line {line}: multiple CitationGroups are named {spec.name!r}"
        )
    existing = matches[0] if matches else None
    if existing is not None and (
        existing.status is not CitationGroupStatus.normal
        or existing.type is not spec.article_type
        or existing.region is None
        or existing.region.id != region.id
        or frozenset(existing.tags or ()) != frozenset(tags)
    ):
        raise RecommendationError(
            f"line {line}: existing CitationGroup {spec.name!r} conflicts with requested creation"
        )
    planned = PlannedCitationGroup(spec, region, existing, tags)
    if existing is None:
        if earlier := planned_new.get(spec.name):
            assert earlier.region is not None
            if (
                earlier.spec.article_type is not spec.article_type
                or earlier.region.id != region.id
                or frozenset(earlier.tags) != frozenset(tags)
            ):
                raise RecommendationError(
                    f"line {line}: conflicting definitions for new CitationGroup {spec.name!r}"
                )
            return earlier
        planned_new[spec.name] = planned
    return planned


def add_virtual_model(
    planned: PlannedCitationGroup,
    builder: ProposalBuilder,
    *,
    context: str,
    new_citation_groups: dict[str, CitationGroup],
) -> CitationGroup:
    if planned.citation_group is not None:
        return builder.copy(planned.citation_group, context=context)
    assert planned.region is not None and planned.spec.article_type is not None
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
    return cg


def create_citation_group(planned: PlannedCitationGroup) -> CitationGroup:
    assert planned.region is not None and planned.spec.article_type is not None
    return CitationGroup.create(
        name=planned.spec.name,
        type=planned.spec.article_type,
        region=planned.region,
        status=CitationGroupStatus.normal,
        tags=planned.tags,
    )
