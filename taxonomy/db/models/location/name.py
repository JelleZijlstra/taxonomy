"""Parsing and rendering for structured Location names."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParsedLocationName:
    """The three structural parts of a Location name."""

    base_name: str
    disambiguator: str | None = None
    modifier: str | None = None

    @classmethod
    def parse(cls, name: str) -> ParsedLocationName:
        modifier: str | None
        prefix, separator, modifier = name.partition(":")
        prefix = prefix.strip()
        modifier = modifier.strip() if separator else None
        if modifier == "":
            modifier = None

        split = split_trailing_parenthetical(prefix)
        if split is None:
            base_name = prefix
            disambiguator = None
        else:
            base_name, disambiguator = split
            disambiguator = disambiguator.strip() or None
        return cls(base_name.strip(), disambiguator, modifier)

    def render(self) -> str:
        name = self.base_name
        if self.disambiguator is not None:
            name += f" ({self.disambiguator})"
        if self.modifier is not None:
            name += f": {self.modifier}"
        return name


def split_trailing_parenthetical(name: str) -> tuple[str, str] | None:
    """Split a whitespace-delimited final parenthetical, including nested ones."""
    if not name.endswith(")"):
        return None
    depth = 0
    for index in range(len(name) - 1, -1, -1):
        character = name[index]
        if character == ")":
            depth += 1
        elif character == "(":
            depth -= 1
            if depth == 0:
                if index == 0 or not name[index - 1].isspace():
                    return None
                return name[:index].rstrip(), name[index + 1 : -1]
    return None
