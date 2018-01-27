import itertools

from .constants import NomenclatureStatus


def test_hierarchy() -> None:
    all_statuses = set(NomenclatureStatus)  # type: ignore
    in_hierarchy = set(itertools.chain.from_iterable(NomenclatureStatus.hierarchy()))
    difference = all_statuses - in_hierarchy
    assert not difference, f'{", ".join(v.name for v in difference)} are not in the hierarchy'
