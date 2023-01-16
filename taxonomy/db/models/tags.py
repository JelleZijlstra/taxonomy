from __future__ import annotations

"""

Tag classes that need to defined after all models for import cycle reasons.

"""
from ... import adt
from . import Collection
from . import Region
from . import Article


class PersonTag(adt.ADT):
    Wiki(text=str, tag=1)  # type: ignore
    Institution(institution=Collection, tag=2)  # type: ignore
    ActiveRegion(region=Region, tag=3)  # type: ignore
    Biography(article=Article, tag=4)  # type: ignore
