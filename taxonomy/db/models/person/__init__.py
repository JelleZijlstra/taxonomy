"""Package for Person and related code."""

__all__ = [
    "AuthorTag",
    "Person",
    "PersonLevel",
    "VirtualPerson",
    "get_initials",
    "get_new_authors_list",
    "is_more_specific_than",
    "is_valid_orcid",
    "normalize_orcid",
]

from .person import AuthorTag as AuthorTag
from .person import Person as Person
from .person import PersonLevel as PersonLevel
from .person import VirtualPerson as VirtualPerson
from .person import get_initials as get_initials
from .person import get_new_authors_list as get_new_authors_list
from .person import is_more_specific_than as is_more_specific_than
from .person import is_valid_orcid as is_valid_orcid
from .person import normalize_orcid as normalize_orcid
