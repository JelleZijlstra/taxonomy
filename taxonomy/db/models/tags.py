"""Tag classes that need to defined after all models for import cycle reasons."""

from typing import NotRequired

from taxonomy import adt

from . import Article, Collection, Name, Region


class PersonTag(adt.ADT):
    Wiki(text=str, tag=1)  # type: ignore[name-defined]
    Institution(institution=Collection, tag=2)  # type: ignore[name-defined]
    ActiveRegion(region=Region, tag=3)  # type: ignore[name-defined]
    Biography(article=Article, tag=4)  # type: ignore[name-defined]
    TransliteratedFamilyName(text=str, tag=5)  # type: ignore[name-defined]
    ORCID(text=str, tag=6)  # type: ignore[name-defined]
    OnlineBio(text=str, tag=7)  # type: ignore[name-defined]


class TaxonTag(adt.ADT):
    NominalGenus(genus=Name, tag=1)  # type: ignore[name-defined]
    MDD(id=str, tag=2)  # type: ignore[name-defined]
    KeyReference(article=Article, tag=3)  # type: ignore[name-defined]
    EnglishCommonName(name=str, tag=4)  # type: ignore[name-defined]
    IncertaeSedis(comment=NotRequired[str], tag=5)  # type: ignore[name-defined]
    Basal(comment=NotRequired[str], tag=6)  # type: ignore[name-defined]
