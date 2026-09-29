from collections.abc import Iterable
from typing import Any

from taxonomy.apis import plss
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import (
    AgeClass,
    Group,
    OccurrenceValidity,
    SpeciesGroupType,
    SpecimenOrgan,
    Status,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import (
    add_tag_issue,
    create_related_object_issue,
    remove_tag_issue,
)
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.location.age import (
    is_non_recent_location,
    is_pre_pleistocene_location,
    is_recent_location,
)
from taxonomy.db.models.name.name import ExperimentalLintCondition as Exp
from taxonomy.db.models.name.name import Name, TypeTag

from .base import LINT
from .helpers import is_valid_mammal

_RECENT_TAXON_AGES = frozenset({AgeClass.extant, AgeClass.recently_extinct})


@LINT.add("unique_type_locality")
def check_unique_type_locality(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_locality is None or nam.original_citation is None:
        return
    if nam.status is not Status.valid:
        return
    if nam.has_type_tag(TypeTag.InterpretedTypeLocality):
        return
    if nam.taxon.age not in _RECENT_TAXON_AGES:
        return
    original_localities = dict.fromkeys(
        tag.text
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.LocationDetail)
        and tag.source == nam.original_citation
    )
    if len(original_localities) > 1:
        message = f"multiple original localities for {nam}:\n"
        for text in original_localities:
            message += f"  {text}\n"
        yield message


@LINT.add("general_type_locality")
def check_general_type_locality(nam: Name, cfg: LintConfig) -> Iterable[str]:
    location = nam.type_locality
    if location is None or location.name != location.region.name:
        return
    if nam.has_type_tag(TypeTag.ImpreciseLocality) or nam.has_type_tag(
        TypeTag.PartialTypeLocality
    ):
        return
    tagged_region = next(
        (
            region
            for region in (location.region, *location.region.all_parents())
            if region.has_tag(models.RegionTag.MustHavePreciseTypeLocality)
        ),
        None,
    )
    if tagged_region is None:
        return
    yield (
        f"type locality {location.name!r} matches its Region name, and Region "
        f"{tagged_region.name!r} is tagged MustHavePreciseTypeLocality; use a more "
        "precise Location or add the ImpreciseLocality tag"
    )


def _partial_type_locality_container_kind(location: models.Location) -> str | None:
    if location.name == location.region.name and is_recent_location(location):
        return "Recent"
    if location.name == f"{location.region.name} fossil" and is_non_recent_location(
        location
    ):
        return "fossil"
    return None


def _region_is_within(region: models.Region, container: models.Region) -> bool:
    current: models.Region | None = region
    while current is not None:
        if current == container:
            return True
        current = current.parent
    return False


@LINT.add("partial_type_locality")
def check_partial_type_locality(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    tags = list(nam.get_tags(nam.type_tags, TypeTag.PartialTypeLocality))
    if not tags:
        return
    if nam.species_type_kind not in (
        None,
        SpeciesGroupType.syntypes,
        SpeciesGroupType.nonexistent,
    ):
        yield (
            "has PartialTypeLocality tags, but species_type_kind is "
            f"{nam.species_type_kind.name}, not syntypes or unset"
        )
    unique_location_ids = {tag.location.id for tag in tags}
    if len(unique_location_ids) < 2:
        yield (
            "must have more than one distinct PartialTypeLocality; got "
            f"{len(unique_location_ids)}"
        )
    if nam.type_locality is None:
        yield "has PartialTypeLocality tags but no type locality"
        return
    container_kind = _partial_type_locality_container_kind(nam.type_locality)
    if container_kind is None:
        yield (
            "type locality for PartialTypeLocality tags must be either the Recent "
            "Location named for its Region or the '<Region> fossil' Location"
        )
        return
    outside_region = False
    for tag in tags:
        if not _region_is_within(tag.location.region, nam.type_locality.region):
            outside_region = True
            yield (
                f"PartialTypeLocality {tag.location.name!r} is outside type-locality "
                f"Region {nam.type_locality.region.name!r}"
            )
    if (
        outside_region
        or len(unique_location_ids) < 2
        or nam.species_type_kind not in (None, SpeciesGroupType.syntypes)
    ):
        return
    region = tags[0].location.region
    while not all(_region_is_within(tag.location.region, region) for tag in tags):
        if region.parent is None:
            return
        region = region.parent
    if region == nam.type_locality.region:
        return

    def get_container() -> models.Location:
        period = models.Period.get(
            name="Recent" if container_kind == "Recent" else "Phanerozoic"
        )
        return models.Location.get_or_create_general(region, period)

    yield create_related_object_issue(
        f"type-locality Region {nam.type_locality.region.name!r} is not the "
        f"smallest enclosing Region {region.name!r} for PartialTypeLocality tags",
        nam,
        "type_locality",
        get_container,
    )


@LINT.add("type_locality_validity")
def check_type_locality_validity(nam: Name, cfg: LintConfig) -> Iterable[str]:
    tags = list(nam.get_tags(nam.type_tags, TypeTag.TypeLocalityValidity))
    for tag in set(tags):
        if tags.count(tag) > 1:
            yield f"has duplicate {tag}"
    allowed = {
        OccurrenceValidity.occurrence_dubious,
        OccurrenceValidity.classification_dubious,
        OccurrenceValidity.incidental,
    }
    for tag in tags:
        if tag.validity not in allowed:
            yield (
                "TypeLocalityValidity only allows occurrence_dubious or "
                f"classification_dubious, not {tag.validity.name}"
            )


@LINT.add("type_locality_distribution_rules")
def check_type_locality_distribution_rules(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_locality is None:
        return
    for tag in nam.taxon.get_tags(
        nam.taxon.tags, models.tags.TaxonTag.RedirectOccurrences
    ):
        if models.tags.is_region_within(nam.type_locality.region, tag.region):
            yield (
                f"type locality {nam.type_locality} is within {tag.region}, "
                f"where RedirectOccurrences says {nam.taxon} should be "
                f"{tag.target} in light of {tag.source}"
            )


@LINT.add("coordinates")
def check_coordinates(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    coordinate_tags = list(nam.get_tags(nam.type_tags, TypeTag.Coordinates))
    if nam.type_locality is None:
        for tag in coordinate_tags:
            yield f"{tag} is present, but type locality is not set"
        return
    location = nam.type_locality
    for tag in coordinate_tags:
        if (tag.latitude, tag.longitude) == (location.latitude, location.longitude):
            message = (
                f"remove redundant {tag}: coordinates exactly match Location "
                f"{location}"
            )
            yield remove_tag_issue(message, nam, tag, field="type_tags")
            continue

        extent = coordinate_lint.make_extent(tag.latitude, tag.longitude)
        if extent is None:
            continue  # reported elsewhere

        yield from coordinate_lint.check_extent_in_region(extent, location.region)
        if location.latitude is None or location.longitude is None:
            continue
        location_extent = coordinate_lint.make_extent(
            location.latitude, location.longitude
        )
        if location_extent is None:
            continue  # reported on the Location
        distance = coordinate_lint.extent_distance_km(extent, location_extent)
        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
            yield (
                f"type-locality coordinates {tag.latitude}, {tag.longitude} are "
                f"{distance:.1f} km from Location {location} coordinates "
                f"{location.latitude}, {location.longitude}"
            )


@LINT.add("type_locality_age")
def check_type_locality_age(nam: Name, cfg: LintConfig) -> Iterable[str]:
    location = nam.type_locality
    if location is None:
        return

    if is_recent_location(location):
        if nam.taxon.age not in _RECENT_TAXON_AGES:
            yield (
                f"type locality {location} is Recent, but taxon {nam.taxon} "
                f"has age {nam.taxon.age.name.replace('_', ' ')}"
            )
        return

    if nam.taxon.age not in _RECENT_TAXON_AGES or not is_non_recent_location(location):
        return

    recent_organs = sorted(
        {
            tag.organ.name.replace("_", " ")
            for tag in nam.type_tags
            if isinstance(tag, TypeTag.Organ)
            and tag.organ in {SpecimenOrgan.skin, SpecimenOrgan.in_alcohol}
        }
    )
    if recent_organs:
        yield (
            f"type locality {location} is non-Recent, but Organ tag(s) "
            f"{', '.join(recent_organs)} indicate a Recent type specimen"
        )
    if is_pre_pleistocene_location(location):
        yield (
            f"type locality {location} is pre-Pleistocene, but the name belongs "
            f"to {nam.taxon.age.name.replace('_', ' ')} taxon {nam.taxon}"
        )


@LINT.add("must_have_type_locality")
def check_must_have_type_locality(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.group is not Group.species
        or nam.type_locality is not None
        or "type_locality" not in nam.get_required_fields()
    ):
        return
    if nam.has_type_tag(TypeTag.LocationDetail):
        yield "missing type locality, but has LocationDetail tag"
    elif nam.original_citation is not None and nam.taxon.age is AgeClass.extant:
        yield "missing type locality, but has originali citation and is extant"


@LINT.add("type_locality_strict")
def check_type_locality_strict(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.species or not is_valid_mammal(nam):
        return
    if nam.has_type_tag(TypeTag.InterpretedTypeLocality):
        return
    if "type_locality" not in nam.get_required_fields():
        return
    # will have to deal with this separately
    if nam.original_citation is None:
        return
    if nam.type_locality is None:
        yield "missing type locality"
        return
    if not Exp().should_apply(nam, cfg):
        return
    original_localities = [
        tag
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.LocationDetail)
        and tag.source == nam.original_citation
    ]
    if not original_localities:
        yield "missing original locality detail"


@LINT.add("location_detail_coordinates")
def check_location_detail_coordinates(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    extracted: list[  # type: ignore[name-defined]
        tuple[TypeTag.LocationDetail, str, str, coordinate_lint.CoordinateExtent]
    ] = []
    for tag in models.location.lint._get_applicable_location_detail_tags(nam):
        coordinates = helpers.extract_coordinates(tag.text)
        if coordinates is None:
            continue
        parsed = coordinate_lint.standardize_coordinate_pair(*coordinates)
        if parsed is not None:
            extracted.append((tag, *parsed))
    if not extracted:
        return

    location = nam.type_locality
    if location is not None and (
        location.latitude is not None or location.longitude is not None
    ):
        if location.latitude is None or location.longitude is None:
            return
        location_extent = coordinate_lint.make_extent(
            location.latitude, location.longitude
        )
        if location_extent is None:
            return
        for tag, latitude, longitude, extent in extracted:
            distance = coordinate_lint.extent_distance_km(extent, location_extent)
            if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
                yield (
                    f"coordinates {latitude}, {longitude} extracted from "
                    f"LocationDetail {tag.text!r} are {distance:.1f} km from "
                    f"Location {location} coordinates {location.latitude}, "
                    f"{location.longitude}"
                )
        return

    coordinate_tags: list[  # type: ignore[name-defined]
        tuple[TypeTag.Coordinates, coordinate_lint.CoordinateExtent]
    ] = []
    for tag in nam.get_tags(nam.type_tags, TypeTag.Coordinates):
        maybe_extent = coordinate_lint.make_extent(tag.latitude, tag.longitude)
        if maybe_extent is not None:
            coordinate_tags.append((tag, maybe_extent))
    if coordinate_tags:
        for detail, latitude, longitude, extent in extracted:
            if all(
                coordinate_lint.extent_distance_km(extent, coordinate_extent)
                > coordinate_lint.COORDINATE_TOLERANCE_KM
                for _, coordinate_extent in coordinate_tags
            ):
                yield (
                    f"coordinates {latitude}, {longitude} extracted from "
                    f"LocationDetail {detail.text!r} conflict with all Coordinates "
                    f"tags: {[tag for tag, _ in coordinate_tags]}"
                )
        return

    for index, (tag, latitude, longitude, extent) in enumerate(extracted):
        for other_tag, other_latitude, other_longitude, other_extent in extracted[
            index + 1 :
        ]:
            distance = coordinate_lint.extent_distance_km(extent, other_extent)
            if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
                yield (
                    f"cannot infer Coordinates tag because {latitude}, {longitude} "
                    f"from LocationDetail {tag.text!r} and {other_latitude}, "
                    f"{other_longitude} from LocationDetail {other_tag.text!r} "
                    f"differ by {distance:.1f} km"
                )
                return

    distinct_extents = {
        (
            extent.latitude.minimum,
            extent.latitude.maximum,
            extent.longitude.minimum,
            extent.longitude.maximum,
        )
        for _, _, _, extent in extracted
    }
    if len(distinct_extents) > 1:
        coordinate_text = ", ".join(
            f"{latitude}, {longitude}" for _, latitude, longitude, _ in extracted
        )
        yield (
            "cannot infer a single Coordinates tag because LocationDetail tags "
            f"contain multiple coordinate pairs within "
            f"{coordinate_lint.COORDINATE_TOLERANCE_KM} km: {coordinate_text}"
        )
        return

    if nam.type_locality is None:
        return

    _, latitude, longitude, _ = extracted[0]
    expected = TypeTag.Coordinates(latitude, longitude)
    message = f"add {expected} inferred from LocationDetail"
    yield add_tag_issue(message, nam, expected, field="type_tags")


@LINT.add("location_detail_plss")
def check_location_detail_plss(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Report conflicts within one Name or with its reviewed Location PLSS tag."""
    descriptions: list[tuple[Any, plss.PLSSDescription]] = []
    for tag in models.location.lint._get_applicable_location_detail_tags(nam):
        for extracted in plss.extract_plss(tag.text):
            description = extracted.description
            if extracted.has_alternative_section:
                # "Sections 30-31" supports the shared township/range but neither
                # individual section.  This mirrors the Location lint's handling
                # and allows a reviewed Location in either cited section.
                description = plss.PLSSDescription(
                    description.township,
                    description.township_fraction,
                    description.township_direction,
                    description.range,
                    description.range_fraction,
                    description.range_direction,
                    meridian=description.meridian,
                )
            descriptions.append((tag, description))
    if not descriptions:
        return
    if len(descriptions) >= 2:
        first_tag, first = descriptions[0]
        for tag, description in descriptions[1:]:
            if not first.is_compatible_with(description):
                yield (
                    "LocationDetail tags contain incompatible PLSS descriptions:\n"
                    f"- {first.canonical_text!r} from {first_tag.text!r}\n"
                    f"- {description.canonical_text!r} from {tag.text!r}"
                )
                return

    if nam.type_locality is None:
        return

    location_tags = list(
        nam.type_locality.get_tags(nam.type_locality.tags, models.tags.LocationTag.PLSS)
    )
    if len(location_tags) != 1:
        return
    accepted = plss.parse_canonical(location_tags[0].text)
    if accepted is None:
        return
    conflict = next(
        (
            (tag, description)
            for tag, description in descriptions
            if not accepted.is_compatible_with(description)
        ),
        None,
    )
    if conflict is not None:
        tag, description = conflict
        yield (
            f"LocationDetail PLSS description conflicts with type locality:\n"
            f"- {description.canonical_text!r} from {tag.text!r}\n"
            f"- {accepted.canonical_text!r} PLSS tag from type locality {nam.type_locality.name!r}"
        )
