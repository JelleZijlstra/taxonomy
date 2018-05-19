"""

Module for high-level analysis of the database.

"""

import collections
from typing import Dict, Iterable, Optional, Sequence

from . import constants, models


class _SuffixTree(object):

    def __init__(self, names: Iterable[str] = ()) -> None:
        self.dictionary: Dict[str, _SuffixTree] = collections.defaultdict(_SuffixTree)
        self.count = 0
        for name in names:
            self._add(list(reversed(name)))

    def _add(self, name: Sequence[str]) -> None:
        self.count += 1
        if not name:
            return
        last, rest = name[0], name[1:]
        self.dictionary[last]._add(rest)

    def display(
        self,
        max_depth: int = 1,
        depth: int = 0,
        most_common: Optional[int] = None,
        min_count: Optional[int] = None,
    ) -> None:
        print(self.count)
        if max_depth == 0:
            return
        if most_common is None:
            subtrees = sorted(self.dictionary.items())
        else:
            all_subtrees = sorted(
                self.dictionary.items(), key=lambda pair: pair[1].count, reverse=True
            )
            subtrees = list(all_subtrees)[:most_common]
        for key, subtree in subtrees:
            if min_count is not None and subtree.count < min_count:
                continue
            print("%s%s: " % (" " * (4 * depth), key), end="")
            subtree.display(
                max_depth=max_depth - 1,
                depth=depth + 1,
                most_common=most_common,
                min_count=min_count,
            )

    def __getattr__(self, attr: str) -> "_SuffixTree":
        if attr in self.dictionary:
            return self.dictionary[attr]
        else:
            raise AttributeError(attr)


def genus_suffix_tree(no_complex_only: bool = False) -> _SuffixTree:
    query = models.Name.select(models.Name.root_name).where(
        models.Name.group == constants.Group.genus
    )
    if no_complex_only:
        query = query.where(models.Name._name_complex_id >> None)
    return _SuffixTree(name.root_name for name in query)
