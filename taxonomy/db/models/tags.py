"""Tag classes that need to defined after all models for import cycle reasons."""

from collections.abc import Iterable
from typing import Any, NotRequired

from taxonomy import adt
from taxonomy.db.constants import (
    URL,
    DistributionOrigin,
    DistributionPresence,
    Managed,
    Markdown,
)

from . import Article, Collection, Name, Region, Taxon


class PersonTag(adt.ADT):
    Wiki(text=URL, tag=1)  # type: ignore[name-defined]
    Institution(institution=Collection, tag=2)  # type: ignore[name-defined]
    ActiveRegion(region=Region, tag=3)  # type: ignore[name-defined]
    Biography(article=Article, tag=4)  # type: ignore[name-defined]
    TransliteratedFamilyName(text=Managed, tag=5)  # type: ignore[name-defined]
    ORCID(text=Managed, tag=6)  # type: ignore[name-defined]
    OnlineBio(text=URL, tag=7)  # type: ignore[name-defined]


class LocationTag(adt.ADT):
    # General locality; should be simplified if possible.
    General(tag=1)  # type: ignore[name-defined]

    # Locality identifiers in other databases

    # Paleobiology Database
    PBDB(id=Managed, tag=2)  # type: ignore[name-defined]

    # {North America Tertiary-localities.pdf}, appendix to
    # Evolution of Tertiary Mammals of North America
    ETMNA(id=Managed, tag=3)  # type: ignore[name-defined]

    # Neogene of the Old World database
    NOW(id=Managed, tag=4)  # type: ignore[name-defined]

    IgnoreLintLocation(  # type: ignore[name-defined]
        label=Managed, comment=NotRequired[Markdown], tag=5
    )

    # Indicate that after some research, it is unclear where this place is
    Unplaced(comment=NotRequired[Markdown], tag=6)  # type: ignore[name-defined]

    # Region that is nearby and used as a base for a disambiguator
    NearbyRegion(region=Region, tag=7)  # type: ignore[name-defined]

    # Reviewed Public Land Survey System description. The text is a canonical,
    # human-readable land description; plss_id is the township-level CadNSDI
    # PLSSID and therefore also records the resolved principal meridian.
    PLSS(  # type: ignore[name-defined]
        text=Managed, plss_id=Managed, comment=NotRequired[Markdown], tag=8
    )

    # Evidence supporting the Location's latitude and longitude fields. External
    # identifiers are snapshots of the object used, not cached coordinate values.
    CoordinatesFromPLSS(plss_id=Managed, tag=9)  # type: ignore[name-defined]
    CoordinatesFromGeoNames(geoname_id=Managed, tag=10)  # type: ignore[name-defined]
    CoordinatesFromNominatim(  # type: ignore[name-defined]
        osm_type=Managed,
        osm_id=Managed,
        category=Managed,
        use_bounding_box=Managed,
        tag=11,
    )
    CoordinatesFromName(  # type: ignore[name-defined]
        name=Name, text=NotRequired[Markdown], tag=12
    )
    CoordinatesFromOccurrenceRecord(  # type: ignore[name-defined]
        occurrence_record_id=Managed, tag=13
    )
    CoordinatesFromLocationName(tag=14)  # type: ignore[name-defined]
    CoordinatesManual(comment=Markdown, tag=15)  # type: ignore[name-defined]


COORDINATE_PROVENANCE_TAG_TYPES = (
    LocationTag.CoordinatesFromPLSS,
    LocationTag.CoordinatesFromGeoNames,
    LocationTag.CoordinatesFromNominatim,
    LocationTag.CoordinatesFromName,
    LocationTag.CoordinatesFromOccurrenceRecord,
    LocationTag.CoordinatesManual,
)


def is_coordinate_provenance_tag(tag: adt.ADT) -> bool:
    return tag is LocationTag.CoordinatesFromLocationName or isinstance(
        tag, COORDINATE_PROVENANCE_TAG_TYPES
    )


class TaxonTag(adt.ADT):
    NominalGenus(genus=Name, tag=1)  # type: ignore[name-defined]
    MDD(id=Managed, tag=2)  # type: ignore[name-defined]
    KeyReference(article=Article, tag=3)  # type: ignore[name-defined]
    EnglishCommonName(name=Managed, tag=4)  # type: ignore[name-defined]
    IncertaeSedis(comment=NotRequired[Markdown], tag=5)  # type: ignore[name-defined]
    Basal(comment=NotRequired[Markdown], tag=6)  # type: ignore[name-defined]
    IgnoreLintTaxon(label=Managed, comment=NotRequired[Markdown], tag=7)  # type: ignore[name-defined]

    # These statuses apply in the named Region and all its descendants. A tag on
    # a more specific Region overrides a broader tag on the same axis.
    RegionalOrigin(  # type: ignore[name-defined]
        region=Region,
        origin=DistributionOrigin,
        source=Article,
        comment=NotRequired[Markdown],
        tag=8,
    )
    RegionalPresence(  # type: ignore[name-defined]
        region=Region,
        presence=DistributionPresence,
        source=Article,
        comment=NotRequired[Markdown],
        tag=9,
    )

    # cutoff_year defaults to the source's publication year. Reassessment applies
    # through that year; redirects automatically move only records from before it.
    ReassessOccurrences(  # type: ignore[name-defined]
        region=Region,
        source=Article,
        cutoff_year=NotRequired[int],
        comment=NotRequired[Markdown],
        tag=10,
    )
    RedirectOccurrences(  # type: ignore[name-defined]
        region=Region,
        target=Taxon,
        source=Article,
        cutoff_year=NotRequired[int],
        comment=NotRequired[Markdown],
        tag=11,
    )


def region_distance(region: Region, ancestor: Region) -> int | None:
    """Return the number of parent links from region to ancestor."""
    if region == ancestor:
        return 0
    for distance, parent in enumerate(region.all_parents(), start=1):
        if parent == ancestor:
            return distance
    return None


def is_region_within(region: Region, ancestor: Region) -> bool:
    return region_distance(region, ancestor) is not None


def get_matching_taxon_tags(
    taxon: Taxon, region: Region, tag_type: type[Any]
) -> list[Any]:
    return [
        tag
        for tag in taxon.tags
        if isinstance(tag, tag_type)
        if is_region_within(region, tag.region)
    ]


def get_effective_regional_tag(
    taxon: Taxon, region: Region, tag_type: type[Any]
) -> Any | None:
    """Return the most specific unambiguous regional tag for a taxon."""
    matches_with_distance = [
        (distance, tag)
        for tag in get_matching_taxon_tags(taxon, region, tag_type)
        if (distance := region_distance(region, tag.region)) is not None
    ]
    if not matches_with_distance:
        return None
    closest_distance = min(distance for distance, _ in matches_with_distance)
    closest = [
        tag for distance, tag in matches_with_distance if distance == closest_distance
    ]
    status_attribute = "origin" if tag_type is TaxonTag.RegionalOrigin else "presence"
    statuses = {getattr(tag, status_attribute) for tag in closest}
    if len(statuses) != 1:
        return None
    return closest[0]


def iter_overlapping_region_pairs(
    tags: Iterable[TaxonTag],
) -> Iterable[tuple[TaxonTag, TaxonTag]]:
    tags = list(tags)
    for index, first in enumerate(tags):
        for second in tags[index + 1 :]:
            if is_region_within(first.region, second.region) or is_region_within(
                second.region, first.region
            ):
                yield first, second
