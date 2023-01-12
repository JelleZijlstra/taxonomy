"""Data structure that maintains a tree of folders, for the purpose of placement suggestions."""

from collections import defaultdict
import re
from typing import (
    Callable,
    Counter,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from typing_extensions import Protocol

_ArticlePath = Tuple[str, ...]
_IGNORED_FIRST_WORDS = {
    "MS",
    "Mammalia",
    "Animalia",
    "Tetrapoda",
    "Reptilia",
    "Herpetofauna",
    "Micromammalia",
    "Macromammalia",
}


class Entry(Protocol):
    @property
    def name(self) -> str:
        pass

    def path_list(self) -> List[str]:
        pass


class TreeSizeData(NamedTuple):
    total_count: int
    max_path_length: int


def _get_path_key(article: Entry) -> _ArticlePath:
    return tuple(article.path_list())


def _get_name_key(article: Entry) -> str:
    # get the key to the suggestions array
    key = article.name
    if " " in key:
        first_word, rest = key.split(" ", maxsplit=1)
        if first_word in _IGNORED_FIRST_WORDS:
            key = rest
    parts = re.split(r"[\s\-,]", key)
    return parts[0]


class CountTreeNode:
    entries: Set[str]  # maintain the names to deduplicate
    children: Dict[str, "CountTreeNode"]
    tree_size_data: TreeSizeData

    def __init__(self) -> None:
        self.entries: Set[str] = set()
        self.children = defaultdict(CountTreeNode)

    def get_tree(self, path: Sequence[str]) -> "CountTreeNode":
        if not path:
            return self
        head, *rest = path
        return self.children[head].get_tree(rest)

    def walk(
        self,
        *,
        path: Tuple[str, ...] = (),
        callback: Callable[["CountTreeNode", Tuple[str, ...]], None],
    ) -> None:
        callback(self, path)
        for name, child in sorted(self.children.items()):
            child.walk(path=path + (name,), callback=callback)

    def display(
        self,
        min_size: Optional[int] = None,
        should_include: Optional[
            Callable[["CountTreeNode", Sequence[str]], bool]
        ] = None,
    ) -> None:
        self.collect_tree_size_data()

        def callback(node: CountTreeNode, path: Sequence[str]) -> None:
            if min_size is not None and node.tree_size_data.total_count < min_size:
                return
            if should_include is not None and not should_include(node, path):
                return
            spaces = " " * (len(path) * 4)
            label = path[-1] if path else "root"
            num_children = len(node.entries) + len(node.children)
            print(
                f"{spaces}{label}: {num_children}/{node.tree_size_data.total_count}/{node.tree_size_data.max_path_length}"
            )

        self.walk(callback=callback)

    def display_with_unnecessary_children(self, limit: int = 100) -> None:
        self.collect_tree_size_data()

        def callback(node: CountTreeNode, path: Sequence[str]) -> None:
            # The < here means that if a folder is at exactly the limit, we don't have
            # an opinion on whether or not it should have subdirectories. This was
            # probably not originally intentional, but seems like a reasonable idea.
            if node.tree_size_data.total_count < limit and node.children:
                path_str = "/".join(path)
                print(
                    f"{path_str}: {node.tree_size_data.total_count}, {len(node.children)} children"
                )

        self.walk(callback=callback)

    def print_if_too_big(self, limit: int = 100) -> List[Sequence[str]]:
        oversized = []

        def callback(node: CountTreeNode, path: Sequence[str]) -> None:
            count = len(node.entries) + len(node.children)
            if count > limit:
                print(f'{"/".join(path)}: {count}')
                oversized.append(path)

        self.walk(callback=callback)
        self.display_with_unnecessary_children(limit=limit)
        return oversized

    def collect_tree_size_data(self) -> TreeSizeData:
        child_data = [
            child.collect_tree_size_data() for child in self.children.values()
        ]
        count = len(self.entries) + sum(c.total_count for c in child_data)
        max_path_length = 1 + max((c.max_path_length for c in child_data), default=0)
        self.tree_size_data = TreeSizeData(count, max_path_length)
        return self.tree_size_data

    def get_all_dirs(self) -> Dict[str, List[_ArticlePath]]:
        result: Dict[str, List[_ArticlePath]] = defaultdict(list)
        for child, child_tree in self.children.items():
            result[child].append((child,))
            child_dirs = child_tree.get_all_dirs()
            for key, paths in child_dirs.items():
                for path in paths:
                    result[key].append((child, *path))
        return result


class FolderTree:
    full_path_suggestions: Dict[str, Dict[_ArticlePath, Set[str]]]
    folder_name_occurrences: Dict[str, Set[_ArticlePath]]
    count_tree: CountTreeNode

    def __init__(self, articles: Iterable[Entry] = ()) -> None:
        self.reset()

        for article in articles:
            self.add(article)

    def reset(self) -> None:
        self.full_path_suggestions = defaultdict(lambda: defaultdict(set))
        self.count_tree = CountTreeNode()
        self.folder_name_occurrences = defaultdict(set)

    def add(self, article: Entry) -> None:
        name_key = _get_name_key(article)
        path_key = _get_path_key(article)
        if not any(path_key) or path_key[0] == "NOFILE":
            # probably the path hasn't been set
            return
        self.full_path_suggestions[name_key][path_key].add(article.name)
        tree = self.count_tree
        for i, part in enumerate(path_key + ("",)):
            if not part:
                tree.entries.add(article.name)
                break
            else:
                self.folder_name_occurrences[part].add(path_key[: i + 1])
                tree = tree.children[part]

    def get_full_path_suggestions(self, article: Entry) -> Iterable[_ArticlePath]:
        suggs = self.full_path_suggestions[_get_name_key(article)]
        ctr = Counter({key: len(names) for key, names in suggs.items()})
        return [sugg for sugg, _ in ctr.most_common()]
