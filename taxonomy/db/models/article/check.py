"""Checking for new files."""

import os
import re
import shutil
import subprocess
import time
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

from taxonomy import config, getinput, uitools
from taxonomy.command_set import CommandSet
from taxonomy.db.constants import ArticleKind
from taxonomy.db.models.base import get_static_callbacks

from .add_data import add_data_for_new_file
from .article import Article
from .name_parser import get_name_parser
from .set_path import determine_path, get_folder_interactively

CS = CommandSet("check", "Related to checking for new files")
FOLDER_SIZE_LIMIT = 32

_options = config.get_options()


class LsFile(NamedTuple):
    name: str
    raw_path: Sequence[str] = ()

    def path_list(self) -> Sequence[str]:
        return self.raw_path

    @property
    def path(self) -> str:
        return "/".join(self.raw_path)


FileList = dict[str, Article]
LsFileList = dict[str, LsFile]


def build_lslist() -> LsFileList:
    # Gets list of files into self.lslist, an array of results (Article form).
    lslist: LsFileList = {}
    print("acquiring list of files... ", end="", flush=True)
    library = _options.library_path
    for dirpath, _, filenames in os.walk(library):
        path = Path(dirpath).relative_to(library)
        for filename in filenames:
            ext = Path(filename).suffix
            if not ext or not ext[1:].isalpha():
                continue
            parts = [part for part in path.parts if part]
            file = LsFile(filename, parts)
            if filename in lslist:
                print(f"---duplicate {filename}---")
                print(file.path)
                print(lslist[filename].path)
            lslist[filename] = file
    print(f"processed ({len(lslist)} found)")
    return lslist


def build_newlist(path: Path | None = None) -> LsFileList:
    out: LsFileList = {}
    if path is None:
        path = _options.new_path
    print("acquiring list of new files... ", end="", flush=True)
    for entry in os.scandir(path):
        if entry.is_file() and entry.name and not Path(entry.name).name.startswith("."):
            out[entry.name] = LsFile(entry.name)
    out = dict(sorted(out.items(), key=lambda kv: kv[0]))

    if not out:
        print("no new files found")
    else:
        print(f"done ({len(out)} found)")
    return out


def build_csvlist() -> FileList:
    print("acquiring database list... ", end="", flush=True)
    csvlist = {
        f.name: f
        for f in Article.select_valid().filter(
            Article.kind.is_in(
                (ArticleKind.electronic, ArticleKind.alternative_version)
            )
        )
    }
    print(f"done ({len(csvlist)} found)")
    return csvlist


_has_run_full_check = False


@CS.register
def check_new() -> None:
    """Check only for new files.

    On the first run, automatically invokes full check(), which fills up caches
    for file paths. On later calls, run full check() to update file paths.

    """
    if not _has_run_full_check:
        check()
        return
    try:
        newcheck()
        burstcheck()
        oversized_folders()
    except uitools.EndOfInput as e:
        print(f"Exiting from check ({e!r})")


@CS.register
def check(*, dry_run: bool = False) -> None:
    """Checks the catalog for things to be changed:
    - Checks whether there are any files in the catalog that are not in the
        library
    - Checks whether there are any files in the library that are not in the
        catalog
    - Checks whether there are new files in temporary storage that need to be
        added to the library
    """
    # always get new ls list, since changes may have occurred since previous check()
    lslist = build_lslist()
    if not lslist:
        print("found no files in lslist")
        return
    csvlist = build_csvlist()
    try:
        # check whether all files in the actual library are in the catalog
        lscheck(lslist, csvlist, dry_run=dry_run)
        # check whether all files in the catalog are in the actual library
        csvcheck(lslist, csvlist, dry_run=dry_run)
        # check whether there are any new files to be added
        newcheck(dry_run=dry_run)
        # check whether there are any files to be burst
        burstcheck(dry_run=dry_run)
        # check if folders are too large
        oversized_folders()
        global _has_run_full_check
        _has_run_full_check = True
    except uitools.EndOfInput as e:
        print(f"Exiting from check ({e!r})")


def setpath(art: Article, fromfile: LsFile, *, verbose: bool = True) -> None:
    if art.path != fromfile.path:
        if verbose:
            print(f"Updating folders for file {art.name}")
            print(f"Stored path: {art.path}")
            print(f"New path: {fromfile.path}")

        art.path = fromfile.path


def csvcheck(lslist: LsFileList, csvlist: FileList, *, dry_run: bool = False) -> bool:
    # check CSV list for problems
    # - detect articles in catalog that are not in the actual library
    # - correct filepaths
    print("checking whether cataloged articles are in library... ", end="")
    Article.folder_tree.reset()
    for name, file in csvlist.items():
        # if file already exists in right place
        if name in lslist:
            # update path
            setpath(file, lslist[name])
        else:
            print()
            header = f"Could not find file {name}"
            if dry_run:
                print(header)
                continue
            cmd, _ = uitools.menu(
                head=header,
                options={
                    "i": "give information about this file",
                    "r": "remove this file from the catalog",
                    "m": "move to the next component",
                    "s": "skip this file",
                    "q": "quit the program",
                    "e": "edit the file",
                    "red": "redirect the file to another file",
                },
                process={
                    "i": uitools.make_callback(file.full_data),
                    "e": uitools.make_callback(file.edit),
                    "q": uitools.stop_callback("csvcheck"),
                    "m": lambda *args: False,
                },
            )
            if cmd == "r":
                file.remove(force=True)
            elif cmd == "red":
                target = Article.getter(None).get_one(
                    "Please enter the redirect target: ", allow_empty=False
                )
                file.merge(target)
            elif cmd == "s":
                break
            elif cmd == "m":
                return False
        Article.folder_tree.add(file)
    print("done")
    return True


def _lscheck_name(
    name: str, lsfile: LsFile, csvlist: FileList, *, dry_run: bool = False
) -> bool:
    """Return whether lscheck() should return immediately."""
    if name in csvlist:
        return False
    print()
    header = f"Could not find file {name} in catalog"
    if dry_run:
        print(header)
        return False

    def add(cmd: str, data: object) -> bool:
        existing = Article.maybe_get(name)
        if existing is not None and existing.isredirect():
            print("There is already a redirect here.")
            return True
        file = Article.make(name=name, path=lsfile.path, kind=ArticleKind.electronic)
        add_data_for_new_file(file)
        return False

    def remover(cmd: str, data: object) -> bool:
        path = _options.library_path / lsfile.path / lsfile.name
        path.unlink()
        return False

    def opener(cmd: str, data: object) -> bool:
        path = _options.library_path / lsfile.path / lsfile.name
        subprocess.check_call(["open", path])
        return True

    def dir_opener(cmd: str, data: object) -> bool:
        path = _options.library_path / lsfile.path
        subprocess.check_call(["open", path])
        return True

    cmd, _ = uitools.menu(
        head=header,
        options={
            "a": "add the file to the catalog",
            "s": "skip this file",
            "q": "quit the program",
            "m": "move to the next component of the catalog",
            "r": "remove this file",
            "o": "open this file",
            "d": "open this directory",
        },
        process={
            "q": uitools.stop_callback("lscheck"),
            "r": remover,
            "o": opener,
            "d": dir_opener,
            "a": add,
        },
    )
    return cmd == "m"


def lscheck(lslist: LsFileList, csvlist: FileList, *, dry_run: bool = False) -> bool:
    # check LS list for errors
    # - Detect articles in the library that are not in the catalog.
    print("checking whether articles in library are in catalog... ")
    for name, lsfile in lslist.items():
        if _lscheck_name(name, lsfile, csvlist, dry_run=dry_run):
            return True
    print("done")
    return True


def burstcheck(*, dry_run: bool = False) -> bool:
    print("checking for files to be bursted... ", end="")
    burstlist = build_newlist(_options.burst_path)
    for file in burstlist.values():
        if dry_run:
            print(file.name)
        else:
            burst(file)
    print("done")
    return True


def newcheck(*, dry_run: bool = False) -> bool:
    # look for new files
    print("checking for newly added articles... ", end="")
    newlist = build_newlist()
    for file in newlist.values():
        if dry_run:
            print(file.name)
        else:
            add_new_file(file)
    print("done")
    return True


def add_new_file(file: LsFile) -> bool:
    def rename_function() -> None:
        nonlocal file
        oldname = file.name
        newname = Article.getter("name").get_one_key(
            prompt="New name: ", default=oldname
        )
        if newname is None:
            return
        # allow renaming to existing name, for example to replace in-press files, but warn
        if Article.has(newname):
            print("Warning: file already exists")
        file = LsFile(newname)
        shutil.move(str(_options.new_path / oldname), str(_options.new_path / newname))

    def quitter() -> bool:
        raise uitools.EndOfInput("newadd")

    parser = get_name_parser(file.name)
    if parser.error_occurred():
        parser.print_errors()
        cmd = getinput.yes_no(
            "This filename could not be parsed. Do you want to rename it? "
        )
        if cmd:
            open_new(file)
            rename_function()

    getinput.add_to_clipboard(file.name)

    print(f"Adding file {file.name!r}")
    selection = getinput.get_line(
        "> ",
        validate=lambda x: x in ("", "n", "s"),
        help={
            "o": "open this file",
            "q": "quit",
            "s": "skip this file",
            "n": 'move this file to "Not to be cataloged"',
            "r": "rename this file",
            "open_dir": "open a directory",
            "": "add this file to the catalog",
        },
        callbacks={
            **get_static_callbacks(),
            "o": lambda: open_new(file),
            "q": quitter,
            "r": rename_function,
            "open_dir": open_dir,
        },
    )
    match selection:
        case "n":
            new_path = _options.new_path
            shutil.move(
                str(new_path / file.name),
                str(new_path / "Not to be cataloged" / file.name),
            )
            return False
        case "s":
            return False
        case "":
            pass
        case _:
            raise NotImplementedError(f"Unknown selection: {selection!r}")

    new_name = check_for_existing_file(file)
    if new_name is None:
        return False

    # now it's time to actually make a CommonArticle
    article = Article.make(name=new_name, kind=ArticleKind.electronic)
    if not determine_path(article):
        print("Unable to determine folder")
        return False
    subprocess.check_call(
        ["mv", "-n", str(_options.new_path / file.name), str(article.get_path())]
    )
    add_data_for_new_file(article)
    return True


def open_new(lsfile: LsFile) -> None:
    path = _options.new_path / lsfile.name
    subprocess.check_call(["open", str(path)])


def check_for_existing_file(lsfile: LsFile) -> str | None:
    """During processing of a new file, check for existing files with the same name.

    Returns None if the new file should be skipped, else the name that should be used for
    the new file.

    """
    maybe_existing = Article.maybe_get(lsfile.name)
    name = lsfile.name
    if maybe_existing is None:
        return name
    existing = maybe_existing
    options = {
        "r": "move over the existing file",
        "d": "delete the new file",
        "o": "open the new and existing files",
        "s": "choose a new name",
        "w": "overwrite a redirect",
    }

    def processcommand(cmd: str) -> tuple[str, object]:
        if cmd not in options:
            return ("s", cmd)
        else:
            return (cmd, None)

    def opener(cmd: str, data: object) -> bool:
        open_new(lsfile)
        assert existing is not None
        existing.openf()
        return True

    def replacer(cmd: str, data: object) -> bool:
        assert existing is not None
        file = existing.resolve_redirect()
        shutil.move(str(_options.new_path / lsfile.name), str(file.get_path()))
        file.store_pdf_content(force=True)
        file.edit()
        return False

    def deleter(cmd: str, data: object) -> bool:
        (_options.new_path / lsfile.name).unlink()
        return False

    def renamer(cmd: str, data: str) -> bool:
        if Article.has(data):
            print("This filename already exists")
            return True
        else:
            nonlocal name
            name = data
            return False

    def overwrite_redirect(cmd: str, data: object) -> bool:
        if not existing.isredirect():
            print(f"{existing} is not a redirect")
            return True
        existing.name = f"{existing.name}-{existing.id}"
        existing.save()
        return False

    existing.display_names()
    if existing.isredirect():
        print(f"The existing file is a redirect to {existing.parent}.")
    cmd, _ = uitools.menu(
        head="A file with this name already exists. Please enter a new filename",
        options=options,
        validfunction=lambda name, options: name.endswith(".pdf"),
        processcommand=processcommand,
        process={
            "o": opener,
            "r": replacer,
            "d": deleter,
            "s": renamer,
            "w": overwrite_redirect,
        },
    )
    if cmd in ("s", "w"):
        return name
    else:
        return None


def burst(lsfile: LsFile) -> bool:
    # bursts a PDF file into several files
    print(f"Bursting file {lsfile.name!r}. Opening file.")
    full_path = _options.burst_path / lsfile.name
    subprocess.check_call(["open", str(full_path)])

    while True:
        line = Article.getter("name").get_one_key("File name: ", allow_empty=False)
        match line:
            case "q" | "quit":
                raise uitools.EndOfInput("burst")
            case "c" | "continue":
                if full_path.exists():
                    subprocess.check_call(
                        [
                            "mv",
                            "-n",
                            str(full_path),
                            str(_options.burst_path / "Old" / lsfile.name),
                        ]
                    )
                    if full_path.exists():
                        print(f"File still exists: {full_path}")
                        target = (
                            _options.burst_path / "Old" / f"{time.time()}{lsfile.name}"
                        )
                        subprocess.check_call(["mv", "-n", str(full_path), str(target)])
                break
            case "s" | "skip":
                break
            case "o" | "open":
                subprocess.check_call(["open", str(full_path)])
            case "" | None:
                continue
            case _ if line.endswith(".pdf"):
                if Article.has(line):
                    if not getinput.yes_no(
                        "A file with this name already exists. Do you want to continue"
                        " anyway?"
                    ):
                        continue
                page_range = getinput.get_line(
                    prompt="Page range: ",
                    validate=lambda page_range: bool(
                        re.match(r"^\d+-\d+$", page_range)
                    ),
                    history_key=str(full_path),
                )
                if not page_range:
                    continue
                start, end = page_range.split("-")
                output_path = _options.new_path / line
                subprocess.check_call(
                    [
                        # brew install qpdf
                        "qpdf",
                        "--empty",
                        "--pages",
                        str(full_path),
                        f"{start}-{end}",
                        "--",
                        str(output_path),
                    ]
                )
                print(f"Split off file {line}")
                add_new_file(LsFile(line))
            case _:
                print(f"Invalid input: {line}")

    return True


@CS.register
def oversized_folders(
    limit: int = FOLDER_SIZE_LIMIT, *, should_open: bool = False
) -> None:
    oversized = Article.get_foldertree().count_tree.print_if_too_big(limit=limit)
    if should_open:
        for path in oversized:
            full_path = _options.library_path / "/".join(path)
            subprocess.check_call(["open", full_path])


@CS.register
def open_dir() -> None:
    path = get_folder_interactively()
    if path:
        full_path = _options.library_path / "/".join(path)
        subprocess.check_call(["open", full_path])


@CS.register
def rename_regex(pattern: str, replacement: str, *, force: bool = False) -> bool:
    """Rename all files matching the given regex."""
    rgx = re.compile(pattern)

    for e in Article.select_valid():
        if rgx.search(e.name):
            new_title = rgx.sub(replacement, e.name)
            if force or getinput.yes_no(f"Rename {e.name!r} to {new_title!r}?"):
                e.move(new_title)

    return True
