import enum

from .adt import ADT

LEAF = 1
NODE = 2
TAG = 3


class SomeEnum(enum.IntEnum):
    foo = 1
    bar = 2


class Tree(ADT):
    Leaf(tag=LEAF)  # type: ignore[name-defined]
    Node(left=Tree, right=Tree, tag=NODE)  # type: ignore[name-defined]
    Tag(val=SomeEnum, tag=TAG)  # type: ignore[name-defined]

    def __repr__(self) -> str:
        if self is Tree.Leaf:
            return "Leaf"
        elif isinstance(self, Tree.Node):
            return f"Node({self.left}, {self.right})"
        elif isinstance(self, Tree.Tag):
            return f"Tag(SomeEnum({self.val.value}))"
        else:
            assert False, f"incorrect node {self}"


def test_repr() -> None:
    assert repr(Tree.Leaf) == "Leaf"
    assert repr(Tree.Node(Tree.Leaf, Tree.Leaf)) == "Node(Leaf, Leaf)"
    assert repr(Tree.Tag(SomeEnum.foo)) == "Tag(SomeEnum(1))"


def test_serialize() -> None:
    assert Tree.unserialize(Tree.Leaf.serialize()) is Tree.Leaf
    assert Tree.unserialize(Tree.Node(Tree.Leaf, Tree.Leaf).serialize()) == Tree.Node(
        Tree.Leaf, Tree.Leaf
    )
    assert Tree.unserialize(Tree.Tag(SomeEnum.foo).serialize()) == Tree.Tag(
        SomeEnum.foo
    )
