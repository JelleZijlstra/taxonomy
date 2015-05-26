__doc__ = """

Code to encode phylogenetic definitions.

Generally follows Article 9 of the PhyloCode: http://www.ohio.edu/phylocode/art9.html

"""

import json
import enum

# to work around circular imports
taxon_cls = None


class DefinitionType(enum.Enum):
	branch = 0
	node = 1
	apomorphy = 2
	other = 3


class Definition(object):
	def __init__(self, typ, arguments):
		self.type = typ
		self.arguments = list(arguments)

	def serialize(self):
		arguments = [
			element.id if hasattr(element, 'id') else element
			for element in self.arguments
		]
		return json.dumps([self.type.value, arguments])

	@classmethod
	def unserialize(self, serialized_str):
		typ, arguments = json.loads(serialized_str)
		return _cls_of_type[DefinitionType(typ)](*arguments)

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, ', '.join(map(str, self.arguments)))


class Node(Definition):
	"""The most recent common ancestor of the argument taxa."""
	def __init__(self, *anchors):
		anchors = list(map(_make_anchor, anchors))
		assert len(anchors) >= 2, \
			"Node-based definitions need at least two anchors (got %s)." % anchors
		super().__init__(DefinitionType.node, anchors)

	def __str__(self):
		return '<' + '&'.join(taxon.valid_name for taxon in self.arguments)


class Branch(Definition):
	"""Taxa more closely related to taxon A than to taxa X, Y, Z."""
	def __init__(self, anchor, *excluded):
		self.anchor = _make_anchor(anchor)
		self.excluded = list(map(_make_anchor, excluded))
		assert len(self.excluded) >= 1, \
			"Brancho-based defitions need at least one excluded taxon (got %s)." % self.excluded
		super().__init__(DefinitionType.branch, [self.anchor] + self.excluded)

	def __str__(self):
		return '>%s~%s' % (self.anchor.valid_name, '∨'.join(taxon.valid_name for taxon in self.excluded))


class Apomorphy(Definition):
	"""Taxa having a synapomorphy homologous with the state in the anchor taxon."""
	def __init__(self, apomorphy, anchor):
		self.anchor = _make_anchor(anchor)
		self.apomorphy = apomorphy
		super().__init__(DefinitionType.apomorphy, [self.apomorphy, self.anchor])

	def __str__(self):
		return '>%s(%s)' % (self.apomorphy, self.anchor)


class Other(Definition):
	"""Other definitions."""
	def __init__(self, definition):
		self.definition = definition
		super().__init__(DefinitionType.other, [definition])

	def __str__(self):
		return self.definition


_cls_of_type = {
	DefinitionType.branch: Branch,
	DefinitionType.node: Node,
	DefinitionType.apomorphy: Apomorphy,
	DefinitionType.other: Other,
}


def _make_anchor(argument):
	if isinstance(argument, int):
		argument = taxon_cls.get(taxon_cls.id == argument)
	assert isinstance(argument, taxon_cls), "Expected a Taxon but got %s" % argument
	return argument
