"""Definitions of various models."""

__all__ = [
    "Article",
    "ArticleComment",
    "BaseModel",
    "Book",
    "CitationGroup",
    "CitationGroupPattern",
    "CitationGroupTag",
    "ClassificationEntry",
    "Collection",
    "IgnoredDoi",
    "IssueDate",
    "ItemFile",
    "Location",
    "Name",
    "NameComment",
    "NameComplex",
    "NameEnding",
    "NameTag",
    "Occurrence",
    "Period",
    "Person",
    "Region",
    "SpeciesNameComplex",
    "SpeciesNameEnding",
    "Specimen",
    "StratigraphicUnit",
    "Taxon",
    "TypeTag",
    "fill_data",
    "has_data_from_original",
    "tags",
]
from .base import BaseModel as BaseModel
from .article import Article as Article, ArticleComment as ArticleComment
from .collection import Collection as Collection
from .location import Location as Location
from .citation_group import (
    CitationGroup as CitationGroup,
    CitationGroupPattern as CitationGroupPattern,
    CitationGroupTag as CitationGroupTag,
)
from .classification_entry import ClassificationEntry as ClassificationEntry
from .ignored_doi import IgnoredDoi as IgnoredDoi
from .issue_date import IssueDate as IssueDate
from .item_file import ItemFile as ItemFile
from .name.name import (
    Name as Name,
    NameComment as NameComment,
    TypeTag as TypeTag,
    NameTag as NameTag,
    has_data_from_original as has_data_from_original,
)
from .name_complex import (
    NameComplex as NameComplex,
    SpeciesNameComplex as SpeciesNameComplex,
    NameEnding as NameEnding,
    SpeciesNameEnding as SpeciesNameEnding,
)
from .occurrence import Occurrence as Occurrence
from .period import Period as Period
from .region import Region as Region
from .stratigraphic_unit import StratigraphicUnit as StratigraphicUnit
from .taxon import Taxon as Taxon
from .person import Person as Person
from .book import Book as Book
from .specimen import Specimen as Specimen
from . import fill_data as fill_data, name as name, tags as tags
