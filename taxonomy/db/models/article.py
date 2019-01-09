from ..constants import ArticleType

from peewee import CharField, IntegerField, TextField
import prompt_toolkit
from typing import Iterable, List, NamedTuple

from .base import BaseModel, EnumField

_TYPE_TO_FIELDS = {
    ArticleType.JOURNAL: [
        "authors",
        "year",
        "title",
        "journal",
        "volume",
        "issue",
        "start_page",
        "end_page",
        "url",
    ],
    ArticleType.CHAPTER: [
        "authors",
        "year",
        "title",
        "start_page",
        "end_page",
        "parent",
        "url",
    ],
    ArticleType.BOOK: ["authors", "year", "title", "pages", "publisher", "isbn"],
    ArticleType.THESIS: ["authors", "year", "title", "pages", "publisher", "series"],
    ArticleType.SUPPLEMENT: ["title", "parent"],
    ArticleType.WEB: ["authors", "year", "title", "url"],
    ArticleType.MISCELLANEOUS: ["authors", "year", "title", "url"],
}


class LsFile(NamedTuple):
    name: str
    path: List[str]


class Article(BaseModel):
    addmonth = CharField()
    addday = CharField()
    addyear = CharField()
    path = CharField()
    name = CharField()
    authors = CharField()
    year = CharField()
    title = CharField()
    journal = CharField()
    series = CharField()
    volume = CharField()
    issue = CharField()
    start_page = CharField()
    end_page = CharField()
    url = CharField()
    doi = CharField()
    typ = EnumField(ArticleType, db_column="type")
    publisher = CharField()
    location = CharField()
    pages = CharField()
    ids = TextField()
    bools = TextField()
    parent = CharField()
    misc_data = TextField()

    class Meta:
        db_table = "article"

    def get_required_fields(self) -> Iterable[str]:
        yield "type"
        yield "addmonth"
        yield "addday"
        yield "addyear"
        yield "path"
        yield "name"
        yield from _TYPE_TO_FIELDS[self.type]

#     @classmethod
#     def check(cls, dry_run: bool = False) -> bool:
#         """Checks the catalog for things to be changed:
#         - Checks whether there are any files in the catalog that are not in the
#           library
#         - Checks whether there are any files in the library that are not in the
#           catalog
#         - Checks whether there are new files in temporary storage that need to be
#           added to the library
#         """
#         lslist = build_lslist()
#         if not lslist:
#             print("found no files in lslist")
#             return False
#         try:
#             # check whether all files in the catalog are in the actual library
#             cls.csvcheck(lslist, dry_run=dry_run)
#             # check whether all files in the actual library are in the catalog
#             cls.lscheck(lslist, dry_run=dry_run)
#             # check whether there are any files to be burst
#             cls.burstcheck(dry_run=dry_run)
#             # check whether there are any new files to be added
#             cls.newcheck(lslist, dry_run=dry_run)
#             # check if folders are too large
#             cls.oversized_folders()
#         except prompt_toolkit.EOFError as e:
#             print(f"Exiting from check ({e!r})")
#             return False
#         return True


# def build_lslist(self) -> FileList:
#     # Gets list of files into self.lslist, an array of results (Article form).
#     lslist: FileList = {}
#     print("acquiring list of files... ", end="", flush=True)
#     self.folder_tree.reset()
#     library = self.options.library_path
#     for dirpath, _, filenames in os.walk(library):
#         path = Path(dirpath).relative_to(library)
#         for filename in filenames:
#             ext = Path(filename).suffix
#             if not ext or not ext[1:].isalpha():
#                 continue
#             parts = [part for part in path.parts if part]
#             data = {"name": filename, "path": parts}
#             file = self.child_class(data, parent=self)
#             if filename in lslist:
#                 print(f"---duplicate {filename}---")
#                 print(file.path)
#                 print(lslist[filename].path)
#             lslist[filename] = file
#             self.folder_tree.add(file)
#     print("processed")
#     return lslist
