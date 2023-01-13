"""

Checking for new files.

"""
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import NamedTuple, NoReturn

from .... import config, getinput, uitools
from ....command_set import CommandSet
from ...constants import ArticleKind
from .article import Article
from .add_data import add_data_for_new_file
from .name_parser import get_name_parser
from .set_path import determine_path, get_folder_interactively

CS = CommandSet("check", "Related to checking for new files")
FOLDER_SIZE_LIMIT = 32

_options = config.get_options()


class LsFile(NamedTuple):
    name: str
    raw_path: list[str] = []

    def path_list(self) -> list[str]:
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
    out = {key: value for key, value in sorted(out.items(), key=lambda kv: kv[0])}

    if not out:
        print("no new files found")
    else:
        print(f"done ({len(out)} found)")
    return out


@CS.register
def check(dry_run: bool = False) -> None:
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
    print("acquiring database list... ", end="", flush=True)
    csvlist = {
        f.name: f
        for f in Article.select_valid().filter(Article.kind == ArticleKind.electronic)
    }
    print(f"done ({len(csvlist)} found)")
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
    except uitools.EndOfInput as e:
        print(f"Exiting from check ({e!r})")


def setpath(art: Article, fromfile: LsFile, verbose: bool = True) -> None:
    if art.path != fromfile.path:
        if verbose:
            print(f"Updating folders for file {art.name}")
            print(f"Stored path: {art.path}")
            print(f"New path: {fromfile.path}")

        art.path = fromfile.path


def csvcheck(lslist: LsFileList, csvlist: FileList, dry_run: bool = False) -> bool:
    # check CSV list for problems
    # - detect articles in catalog that are not in the actual library
    # - correct filepaths
    print("checking whether cataloged articles are in library... ", end="")
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
    print("done")
    return True


def lscheck(lslist: LsFileList, csvlist: FileList, dry_run: bool = False) -> bool:
    # check LS list for errors
    # - Detect articles in the library that are not in the catalog.
    print("checking whether articles in library are in catalog... ")
    for name, lsfile in lslist.items():
        if name not in csvlist:
            print()
            header = f"Could not find file {name} in catalog"
            if dry_run:
                print(header)
                continue

            def add(cmd: str, data: object) -> bool:
                existing = Article.maybe_get(name)
                if existing is not None and existing.isredirect():
                    print("There is already a redirect here.")
                    return True
                file = Article.make(
                    name=name, path=lsfile.path, kind=ArticleKind.electronic
                )
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

            cmd, _ = uitools.menu(
                head=header,
                options={
                    "a": "add the file to the catalog",
                    "s": "skip this file",
                    "q": "quit the program",
                    "m": "move to the next component of the catalog",
                    "r": "remove this file",
                    "o": "open this file",
                },
                process={
                    "q": uitools.stop_callback("lscheck"),
                    "r": remover,
                    "o": opener,
                    "a": add,
                },
            )
            if cmd == "m":
                return True
    print("done")
    return True


def burstcheck(dry_run: bool = False) -> bool:
    print("checking for files to be bursted... ", end="")
    burstlist = build_newlist(_options.burst_path)
    for file in burstlist.values():
        if dry_run:
            print(file.name)
        else:
            burst(file)
    print("done")
    return True


def newcheck(dry_run: bool = False) -> bool:
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
    def renameFunction(*args: object) -> bool:
        nonlocal file
        oldname = file.name
        newname = Article.getter("name").get_one_key(
            prompt="New name: ", default=oldname
        )
        if newname is None:
            return True
        # allow renaming to existing name, for example to replace in-press files, but warn
        if Article.has(newname):
            print("Warning: file already exists")
        file = LsFile(newname)
        shutil.move(
            str(_options.new_path / oldname),
            str(_options.new_path / newname),
        )
        return True

    def opener(cmd: str, data: object) -> bool:
        open_new(file)
        return True

    def archiver(cmd: str, data: object) -> bool:
        new_path = _options.new_path
        shutil.move(
            str(new_path / file.name),
            str(new_path / "Not to be cataloged" / file.name),
        )
        return False

    def quitter(cmd: str, data: object) -> bool:
        raise uitools.EndOfInput("newadd")

    def open_dir_cb(cmd: str, data: object) -> bool:
        open_dir()
        return True

    parser = get_name_parser(file.name)
    if parser.errorOccurred():
        parser.printErrors()
        cmd = getinput.yes_no(
            "This filename could not be parsed. Do you want to rename it? ",
            callbacks={"o": lambda: open_new(file)},
        )
        if cmd:
            renameFunction()

    getinput.add_to_clipboard(file.name)

    selection, _ = uitools.menu(
        head="Adding file " + file.name,
        options={
            "o": "open this file",
            "q": "quit",
            "s": "skip this file",
            "n": 'move this file to "Not to be cataloged"',
            "r": "rename this file",
            "open_dir": "open a directory",
            "": "add this file to the catalog",
        },
        process={
            "o": opener,
            "r": renameFunction,
            "n": archiver,
            "q": quitter,
            "open_dir": open_dir_cb,
        },
    )
    if selection in ("n", "s"):
        return False

    new_name = check_for_existing_file(file)
    if new_name is None:
        return False

    # now it's time to actually make a CommonArticle
    article = Article.make(name=new_name, kind=ArticleKind.electronic)
    if not determine_path(article):
        print("Unable to determine folder")
        return False
    subprocess.check_call(
        [
            "mv",
            "-n",
            str(_options.new_path / file.name),
            str(article.get_path()),
        ]
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
        file.edit()
        return False

    def deleter(cmd: str, data: object) -> bool:
        os.unlink(_options.new_path / lsfile.name)
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
    if cmd == "s" or cmd == "w":
        return name
    else:
        return None


def burst(lsfile: LsFile) -> bool:
    # bursts a PDF file into several files
    print(f'Bursting file "{lsfile.name}". Opening file.')
    full_path = _options.burst_path / lsfile.name
    subprocess.check_call(["open", str(full_path)])

    def processcommand(cmd: str) -> tuple[str | None, object]:
        if cmd in ("c", "s", "q"):
            return cmd, None
        else:
            if Article.has(cmd):
                if not getinput.yes_no(
                    "A file with this name already exists. Do you want to continue anyway?"
                ):
                    return "i", cmd
            return "a", cmd

    def quitter(cmd: str, data: object) -> NoReturn:
        raise uitools.EndOfInput("burst")

    def continuer(cmd: str, data: object) -> bool:
        subprocess.check_call(
            [
                "mv",
                "-n",
                str(full_path),
                str(_options.burst_path / "Old" / lsfile.name),
            ]
        )
        if full_path.exists():
            print("File still exists: oldPath")
            target = _options.burst_path / "Old" / f"{time.time()}{lsfile.name}"
            subprocess.check_call(["mv", "-n", str(full_path), str(target)])
        return False

    def adder(cmd: str, filename: str) -> bool:
        page_range, _ = uitools.menu(
            prompt="Page range: ",
            validfunction=lambda page_range, _: bool(
                re.match(r"^\d+-\d+$", page_range)
            ),
        )
        start, end = page_range.split("-")
        output_path = _options.new_path / filename
        subprocess.check_call(
            [
                "gs",
                "-dBATCH",
                "-dNOPAUSE",
                "-q",
                "-sDEVICE=pdfwrite",
                f"-dFirstPage={start}",
                f"-dLastPage={end}",
                f"-sOUTPUTFILE={output_path}",
                str(full_path),
            ]
        )
        print(f"Split off file {filename}")
        add_new_file(LsFile(filename))
        return True

    uitools.menu(
        head="Enter file names and page ranges",
        prompt="File name: ",
        options={
            "c": "continue with the next file",
            "q": "quit",
            "s": "skip this file",
            # fake commands, used internally by processcommand/process
            # a => add this file
            # i => ignore
        },
        processcommand=processcommand,
        validfunction=lambda *args: True,
        process={
            "q": quitter,
            "i": lambda *args: True,
            "c": continuer,
            "s": lambda *args: False,
            "a": adder,
        },
    )
    return True


@CS.register
def oversized_folders(
    limit: int = FOLDER_SIZE_LIMIT, should_open: bool = False
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