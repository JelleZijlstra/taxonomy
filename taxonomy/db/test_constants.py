import itertools
import unittest

from .constants import NomenclatureStatus


class TestNomenclatureStatus(unittest.TestCase):
	def test_hierarchy(self) -> None:
		all_statuses = set(NomenclatureStatus)  # type: ignore
		in_hierarchy = set(itertools.chain.from_iterable(NomenclatureStatus.hierarchy()))
		difference = all_statuses - in_hierarchy
		assert not difference, f'{", ".join(v.name for v in difference)} are not in the hierarchy'
