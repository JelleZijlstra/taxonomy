"""

Definitions of various models.

"""
from .base import *

from .article import Article
from .collection import Collection
from .location import Location
from .name import Name, NameComment, TypeTag, Tag, STATUS_TO_TAG, has_data_from_original
from .name_complex import NameComplex, SpeciesNameComplex, NameEnding, SpeciesNameEnding
from .occurrence import Occurrence 
from .period import Period 
from .region import Region
from .taxon import Taxon
