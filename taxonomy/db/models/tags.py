from __future__ import annotations

"""

Tag classes that need to defined after all models for import cycle reasons.

"""
from typing_extensions import NotRequired

from ... import adt
from . import Article, Collection, Name, Region


class PersonTag(adt.ADT):
    Wiki(text=str, tag=1)  # type: ignore
    Institution(institution=Collection, tag=2)  # type: ignore
    ActiveRegion(region=Region, tag=3)  # type: ignore
    Biography(article=Article, tag=4)  # type: ignore
    TransliteratedFamilyName(text=str, tag=5)  # type: ignore


class TaxonTag(adt.ADT):
    NominalGenus(genus=Name, tag=1)  # type: ignore
    MDD(id=str, tag=2)  # type: ignore
    KeyReference(article=Article, tag=3)  # type: ignore
    EnglishCommonName(name=str, tag=4)  # type: ignore
    IncertaeSedis(comment=NotRequired[str], tag=5)  # type: ignore
    Basal(comment=NotRequired[str], tag=6)  # type: ignore
