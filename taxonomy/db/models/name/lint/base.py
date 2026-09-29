"""Lint steps for Names.

TODOs:
- For species-group names, disallow "other" and "unranked" as original ranks
- Require original_rank for species-group names with an original name

"""

from collections.abc import Iterable

from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.name.name import Name, TypeTag


def get_ignores(nam: Name) -> Iterable[IgnoreLint]:
    return nam.get_tags(nam.type_tags, TypeTag.IgnoreLint)


def add_ignore(nam: Name, label: str, comment: str) -> None:
    nam.add_type_tag(TypeTag.IgnoreLint(label, comment=comment))


LINT = Lint(Name, get_ignores, add_ignore, ignore_field="type_tags")
