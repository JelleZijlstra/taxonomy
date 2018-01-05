import unittest

from .adt import ADT


LEAF = 1
NODE = 2


class Tree(ADT):
    Leaf(tag=LEAF)
    Node(left=Tree, right=Tree, tag=NODE)

    def __repr__(self) -> None:
        if self is Tree.Leaf:
            return 'Leaf'
        elif isinstance(self, Tree.Node):
            return f'Node({self.left}, {self.right})'
        else:
            assert False, f'incorrect node {self}'


class TestADT(unittest.TestCase):
	def test_repr(self) -> None:
		assert repr(Tree.Leaf) == 'Leaf'
		assert repr(Tree.Node(Tree.Leaf, Tree.Leaf)) == 'Node(Leaf, Leaf)'

	def test_serialize(self) -> None:
		assert Tree.unserialize(Tree.Leaf.serialize()) is Tree.Leaf
		assert Tree.unserialize(Tree.Node(Tree.Leaf, Tree.Leaf).serialize()) == Tree.Node(Tree.Leaf, Tree.Leaf)