from typing import cast

from taxonomy.db.constants import DistributionOrigin, DistributionPresence
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.region import Region
from taxonomy.db.models.tags import TaxonTag, get_effective_regional_tag

from .lint import check_regional_distribution_tags
from .taxon import Taxon


class _RegionStub:
    def __init__(self, name: str, parent: _RegionStub | None = None) -> None:
        self.name = name
        self.parent = parent

    def all_parents(self) -> object:
        current = self.parent
        while current is not None:
            yield current
            current = current.parent

    def __str__(self) -> str:
        return self.name


def _region(name: str, parent: Region | None = None) -> Region:
    return cast(Region, _RegionStub(name, cast(_RegionStub | None, parent)))


def _taxon(tags: tuple[object, ...]) -> Taxon:
    class _TaxonStub:
        def __init__(self) -> None:
            self.tags = tags

        def get_tags(self, values: object, tag_type: type[object]) -> object:
            return (tag for tag in self.tags if isinstance(tag, tag_type))

    return cast(Taxon, _TaxonStub())


def test_regional_status_applies_to_child_region() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    source = cast(Article, object())
    taxon = _taxon(
        (TaxonTag.RegionalOrigin(continent, DistributionOrigin.introduced, source),)
    )

    tag = get_effective_regional_tag(taxon, country, TaxonTag.RegionalOrigin)

    assert tag is not None
    assert tag.origin is DistributionOrigin.introduced


def test_more_specific_regional_status_overrides_broad_status() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    source = cast(Article, object())
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


def test_conflicting_statuses_for_same_region_are_linted() -> None:
    region = _region("Canada")
    source = cast(Article, object())
    taxon = _taxon(
        (
            TaxonTag.RegionalOrigin(region, DistributionOrigin.native, source),
            TaxonTag.RegionalOrigin(region, DistributionOrigin.introduced, source),
        )
    )

    messages = list(check_regional_distribution_tags(taxon, LintConfig()))

    assert len(messages) == 1
    assert "conflicting RegionalOrigin values" in str(messages[0])
