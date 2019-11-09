"""

Definitions of various models.

"""
from .base import BaseModel as BaseModel, database as database  # noqa: F401
from .article import Article as Article, ArticleComment as ArticleComment  # noqa: F401
from . import citations as citations  # noqa: F401
from .collection import Collection as Collection  # noqa: F401
from .location import Location as Location  # noqa: F401
from .citation_group import (  # noqa: F401
    CitationGroup as CitationGroup,
    CitationGroupPattern as CitationGroupPattern,
    CitationGroupTag as CitationGroupTag,
)
from .name import (  # noqa: F401
    Name as Name,
    NameComment as NameComment,
    TypeTag as TypeTag,
    Tag as Tag,
    STATUS_TO_TAG as STATUS_TO_TAG,
    has_data_from_original as has_data_from_original,
)
from .name_complex import (  # noqa: F401
    NameComplex as NameComplex,
    SpeciesNameComplex as SpeciesNameComplex,
    NameEnding as NameEnding,
    SpeciesNameEnding as SpeciesNameEnding,
)
from .occurrence import Occurrence as Occurrence  # noqa: F401
from .period import Period as Period  # noqa: F401
from .region import Region as Region  # noqa: F401
from .taxon import Taxon as Taxon  # noqa: F401
