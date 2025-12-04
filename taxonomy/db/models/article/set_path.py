"""Setting the path on a newly added file."""

from collections.abc import Sequence
from itertools import islice

from taxonomy import getinput, uitools

from .article import Article


def determine_path(art: Article) -> bool:
    # short-circuiting
    return full_path_suggestions(art) or folder_suggestions(art)


def full_path_suggestions(art: Article) -> bool:
    def _temp_opener(cmd: str, data: object) -> bool:
        art.openf(place="temp")
        return True

    for sugg in islice(Article.get_foldertree().get_full_path_suggestions(art), 5):
        print(f'Suggested placement: {" -> ".join(sugg)}')
        cmd, _ = uitools.menu(
            options={
                "y": "this suggestion is correct",
                "n": "this suggestion is not correct",
                "s": "stop suggestions",
                "o": "open the file",
            },
            process={"o": _temp_opener},
        )
        if cmd == "y":
            path = build_path(art, sugg)
            set_path(art, path)
            return True
        elif cmd == "n":
            continue
        elif cmd == "s":
            return False
    return False


def get_folder_interactively(callbacks: getinput.CallbackMap = {}) -> Sequence[str]:
    """Build a folder path, using full path names from anywhere."""
    occurrences = Article.get_foldertree().folder_name_occurrences
    chosen = getinput.get_with_completion(
        message="folder> ",
        options=occurrences,
        disallow_other=True,
        history_key=build_path_with_name,
        callbacks=callbacks,
    )
    path: Sequence[str]
    if not chosen:
        path = ()
    else:
        options = occurrences[chosen]
        if len(options) == 1:
            path = next(iter(options))
        else:
            option_strings = sorted("/".join(option) for option in options)
            print("Multiple possibilities:")
            for option_string in option_strings:
                print(f"- {option_string}")
            inner_chosen = getinput.get_with_completion(
                options=option_strings,
                message=f"{chosen}> ",
                disallow_other=True,
                history_key=(build_path_with_name, chosen),
                callbacks=callbacks,
            )
            if inner_chosen:
                path = inner_chosen.split("/")
            else:
                path = ()
    return path


def build_path_with_name(art: Article, *, allow_skip: bool = False) -> list[str]:
    """Build a folder path, using full path names from anywhere."""
    path = get_folder_interactively(art.get_wrapped_adt_callbacks())
    if allow_skip and not path:
        return []
    return build_path(art, path)


def build_path(art: Article, starting_path: Sequence[str] = ()) -> list[str]:
    tree = Article.get_foldertree().count_tree.get_tree(starting_path)
    path = list(starting_path)
    while tree.children:
        print("Child folders:")
        for child in sorted(tree.children):
            print(f" - {child}")
        chosen = getinput.get_with_completion(
            options=tree.children,
            disallow_other=True,
            history_key=tuple(path),
            callbacks=art.get_wrapped_adt_callbacks(),
        )
        if not chosen:
            if path:
                return path
            else:
                print("Path must be nonempty")
        else:
            path.append(chosen)
            tree = tree.children[chosen]
    return path


def folder_suggestions(art: Article, *, allow_skip: bool = False) -> bool:
    path = build_path_with_name(art, allow_skip=allow_skip)
    if not path:
        return False
    set_path(art, path)
    return True


def set_path(art: Article, folder_array: Sequence[str]) -> None:
    if not folder_array:
        raise ValueError(f"invalid folder array {folder_array}")
    art.path = "/".join(folder_array)
