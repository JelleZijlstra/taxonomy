"""Code to encode phylogenetic definitions.

Generally follows Article 9 of the PhyloCode: http://www.ohio.edu/phylocode/art9.html

"""

import enum
import json
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from .models.taxon import Taxon

_Taxon = Union[int, "Taxon"]

# to work around circular imports
taxon_cls: Any = None


class DefinitionType(enum.Enum):
    branch = 0
    node = 1
    apomorphy = 2
    other = 3


class Definition:
    def __init__(self, typ: DefinitionType, arguments: Iterable[str | _Taxon]) -> None:
        self.type = typ
        self.arguments: list[Any] = list(arguments)

    def serialize(self) -> str:
        arguments = [
            element.id if hasattr(element, "id") else element
            for element in self.arguments
        ]
        return json.dumps([self.type.value, arguments])

    @classmethod
    def unserialize(cls, serialized_str: str) -> "Definition":
        typ, arguments = json.loads(serialized_str)
        return _cls_of_type[DefinitionType(typ)](*arguments)

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__, ", ".join(map(str, self.arguments))
        )


class Node(Definition):
    """The most recent common ancestor of the argument taxa."""

    def __init__(self, *raw_anchors: _Taxon) -> None:
        anchors = list(map(_make_anchor, raw_anchors))
        assert len(anchors) >= 2, (
            "Node-based definitions need at least two anchors (got %s)." % anchors
        )
        super().__init__(DefinitionType.node, anchors)

    def __str__(self) -> str:
        return "<" + "&".join(taxon.valid_name for taxon in self.arguments)


class Branch(Definition):
    """Taxa more closely related to taxon A than to taxa X, Y, Z."""

    def __init__(self, anchor: _Taxon, *excluded: _Taxon) -> None:
        self.anchor = _make_anchor(anchor)
        self.excluded = list(map(_make_anchor, excluded))
        assert len(self.excluded) >= 1, (
            "Brancho-based defitions need at least one excluded taxon (got %s)."
            % self.excluded
        )
        super().__init__(DefinitionType.branch, [self.anchor] + self.excluded)

    def __str__(self) -> str:
        return ">{}~{}".format(
            self.anchor.valid_name,
            "∨".join(taxon.valid_name for taxon in self.excluded),
        )


class Apomorphy(Definition):
    """Taxa having a synapomorphy homologous with the state in the anchor taxon."""

    def __init__(self, apomorphy: str, anchor: _Taxon) -> None:
        self.anchor = _make_anchor(anchor)
        self.apomorphy = apomorphy
        super().__init__(DefinitionType.apomorphy, [self.apomorphy, self.anchor])

    def __str__(self) -> str:
        return f">{self.apomorphy}({self.anchor})"


class Other(Definition):
    """Other definitions."""

    def __init__(self, definition: str) -> None:
        self.definition = definition
        super().__init__(DefinitionType.other, [definition])

    def __str__(self) -> str:
        return self.definition


_cls_of_type = {
    DefinitionType.branch: Branch,
    DefinitionType.node: Node,
    DefinitionType.apomorphy: Apomorphy,
    DefinitionType.other: Other,
}


def _make_anchor(argument: _Taxon) -> "Taxon":
    if isinstance(argument, int):
        argument = taxon_cls.get(taxon_cls.id == argument)
    assert isinstance(argument, taxon_cls), "Expected a Taxon but got %s" % argument
    return argument
