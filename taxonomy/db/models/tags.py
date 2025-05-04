"""Tag classes that need to defined after all models for import cycle reasons."""

from typing import NotRequired

from taxonomy import adt
from taxonomy.db.constants import URL, Managed, Markdown

from . import Article, Collection, Name, Region


class PersonTag(adt.ADT):
    Wiki(text=URL, tag=1)  # type: ignore[name-defined]
    Institution(institution=Collection, tag=2)  # type: ignore[name-defined]
    ActiveRegion(region=Region, tag=3)  # type: ignore[name-defined]
    Biography(article=Article, tag=4)  # type: ignore[name-defined]
    TransliteratedFamilyName(text=Managed, tag=5)  # type: ignore[name-defined]
    ORCID(text=Managed, tag=6)  # type: ignore[name-defined]
    OnlineBio(text=URL, tag=7)  # type: ignore[name-defined]


class TaxonTag(adt.ADT):
    NominalGenus(genus=Name, tag=1)  # type: ignore[name-defined]
    MDD(id=Managed, tag=2)  # type: ignore[name-defined]
    KeyReference(article=Article, tag=3)  # type: ignore[name-defined]
    EnglishCommonName(name=Managed, tag=4)  # type: ignore[name-defined]
    IncertaeSedis(comment=NotRequired[Markdown], tag=5)  # type: ignore[name-defined]
    Basal(comment=NotRequired[Markdown], tag=6)  # type: ignore[name-defined]
    IgnoreLintTaxon(label=Managed, comment=NotRequired[Markdown], tag=7)  # type: ignore[name-defined]
