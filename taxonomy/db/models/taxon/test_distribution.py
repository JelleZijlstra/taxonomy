from taxonomy.db.constants import DistributionOrigin, DistributionPresence, Rank
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.region import Region
from taxonomy.db.models.tags import TaxonTag, get_effective_regional_tag

from .lint import check_regional_distribution_tags
from .taxon import Taxon


def _region(name: str, parent: Region | None = None) -> Region:
    return Region.virtual(name=name, parent=parent)


def _taxon(
    tags: tuple[object, ...], *, rank: Rank = Rank.species, parent: Taxon | None = None
) -> Taxon:
    return Taxon.virtual(tags=tags, rank=rank, parent=parent)


def _source() -> Article:
    return Article.virtual(name="distribution source")


def test_regional_status_applies_to_child_region() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    source = _source()
    taxon = _taxon(
        (TaxonTag.RegionalOrigin(continent, DistributionOrigin.introduced, source),)
    )

    tag = get_effective_regional_tag(taxon, country, TaxonTag.RegionalOrigin)

    assert tag is not None
    assert tag.origin is DistributionOrigin.introduced


def test_more_specific_regional_status_overrides_broad_status() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    source = _source()
    taxon = _taxon(
        (
            TaxonTag.RegionalPresence(
                continent, DistributionPresence.extirpated, source
            ),
            TaxonTag.RegionalPresence(country, DistributionPresence.resident, source),
        )
    )

    tag = get_effective_regional_tag(taxon, country, TaxonTag.RegionalPresence)

    assert tag is not None
    assert tag.presence is DistributionPresence.resident


def test_infraspecific_taxon_inherits_species_regional_status() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    source = _source()
    species = _taxon(
        (TaxonTag.RegionalOrigin(continent, DistributionOrigin.introduced, source),)
    )
    subspecies = _taxon((), rank=Rank.subspecies, parent=species)

    tag = get_effective_regional_tag(subspecies, country, TaxonTag.RegionalOrigin)

    assert tag is not None
    assert tag.origin is DistributionOrigin.introduced


def test_infraspecific_regional_status_overrides_species_status() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    source = _source()
    species = _taxon(
        (TaxonTag.RegionalOrigin(continent, DistributionOrigin.introduced, source),)
    )
    subspecies = _taxon(
        (TaxonTag.RegionalOrigin(country, DistributionOrigin.native, source),),
        rank=Rank.subspecies,
        parent=species,
    )

    tag = get_effective_regional_tag(subspecies, country, TaxonTag.RegionalOrigin)

    assert tag is not None
    assert tag.origin is DistributionOrigin.native


def test_conflicting_statuses_for_same_region_are_linted() -> None:
    region = _region("Canada")
    source = _source()
    taxon = _taxon(
        (
            TaxonTag.RegionalOrigin(region, DistributionOrigin.native, source),
            TaxonTag.RegionalOrigin(region, DistributionOrigin.introduced, source),
        )
    )

    messages = list(check_regional_distribution_tags(taxon, LintConfig()))

    assert len(messages) == 1
    assert "conflicting RegionalOrigin values" in str(messages[0])
