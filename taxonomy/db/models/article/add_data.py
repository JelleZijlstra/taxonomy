"""

Adding data to (usually new) files.

"""
from bs4 import BeautifulSoup
from functools import lru_cache
from pathlib import Path
import re
import requests
import traceback
from typing import Any, Sequence
import urllib.parse

from .article import Article
from .lint import is_valid_doi
from ..citation_group import CitationGroup
from ..person import AuthorTag, Person
from ...constants import ArticleType, ArticleKind
from ...helpers import clean_string, trimdoi, clean_strings_recursively
from .... import config, parsing, getinput, uitools

_options = config.get_options()


RawData = dict[str, Any]


@lru_cache()
def get_doi_information(doi: str) -> BeautifulSoup | None:
    """Retrieves information for this DOI from the API."""
    response = requests.get(
        "http://www.crossref.org/openurl/",
        {"pid": _options.crossrefid, "id": f"doi:{doi}", "noredirect": "true"},
    )
    if response.ok:
        soup = BeautifulSoup(response.text, "xml")
        if soup.query_result:
            query = soup.query_result.body.query
            if query["status"] != "resolved":
                print(f"Could not resolve DOI {doi}")
                return None
            print(f"Retrieved data for DOI {doi}")
            return query
    print(f"Could not retrieve data for DOI {doi}")
    return None


def expand_doi(doi: str) -> RawData:
    result = get_doi_information(doi)
    if not result:
        return {}
    data: RawData = {"doi": doi}

    doiType = result.doi["type"]
    # values from http:#www.crossref.org/schema/queryResultSchema/crossref_query_output2.0.xsd
    doi_type_to_article_type = {
        "journal_title": ArticleType.JOURNAL,
        "journal_issue": ArticleType.JOURNAL,
        "journal_volume": ArticleType.JOURNAL,
        "journal_article": ArticleType.JOURNAL,
        "conference_paper": ArticleType.CHAPTER,
        "component": ArticleType.CHAPTER,
        "book_content": ArticleType.CHAPTER,
        "dissertation": ArticleType.THESIS,
        "conference_title": ArticleType.BOOK,
        "conference_series": ArticleType.BOOK,
        "book_title": ArticleType.BOOK,
        "book_series": ArticleType.BOOK,
        "report-paper_title": ArticleType.MISCELLANEOUS,
        "report-paper_series": ArticleType.MISCELLANEOUS,
        "report-paper_content": ArticleType.MISCELLANEOUS,
        "standard_title": ArticleType.MISCELLANEOUS,
        "standard_series": ArticleType.MISCELLANEOUS,
        "standard_content": ArticleType.MISCELLANEOUS,
    }
    if doiType not in doi_type_to_article_type:
        return {}
    data["type"] = doi_type_to_article_type[doiType]
    # kill leading zeroes
    if result.volume is not None:
        data["volume"] = re.sub(r"^0", "", result.volume.text)
    if result.issue is not None:
        data["issue"] = re.sub(r"^0", "", result.issue.text)
    if result.first_page is not None:
        data["start_page"] = re.sub(r"^0", "", result.first_page.text)
    if result.last_page is not None:
        data["end_page"] = re.sub(r"^0", "", result.last_page.text)
    if result.year is not None:
        data["year"] = result.year.text
    if result.article_title is not None:
        title = result.article_title.text
        if title.upper() == title:
            # all uppercase title; let's clean it up a bit
            # this won't give correct punctuation, but it'll be better than all-uppercase
            title = title[0] + title[1:].lower()
        data["title"] = clean_string(title)
    if result.journal_title is not None:
        data["journal"] = result.journal_title.text
    if result.isbn is not None:
        data["isbn"] = result.isbn.text
    if result.contributors is not None:
        authors = []
        for author in result.contributors.children:
            info = {"family_name": clean_string(author.surname.text)}
            if author.given_name:
                given_names = clean_string(author.given_name.text.title())
                if given_names[-1].isupper():
                    given_names = given_names + "."
                if parsing.matches_grammar(
                    given_names.replace(" ", ""), parsing.initials_pattern
                ):
                    info["initials"] = given_names.replace(" ", "")
                else:
                    info["given_names"] = re.sub(r"\. ([A-Z]\.)", r".\1", given_names)
            authors.append(info)
        data["author_tags"] = authors
    if result.volume_title is not None:
        booktitle = result.volume_title.text
        if data["type"] == ArticleType.BOOK:
            data["title"] = booktitle
        else:  # chapter
            data["parent_info"] = {"title": booktitle, "isbn": data["isbn"]}
            if result.article_title is not None:
                data["title"] = result.article_title.text
    return data


def extract_doi(art: Article) -> str | None:
    pdfcontent = art.getpdfcontent()
    matches = re.findall(
        r"(doi|DOI)\s*(\/((full|abs|pdf)\/)?|:|\.org\/)?\s*(?!URL:)([^\s]*?)(,?\s|â|$)",
        pdfcontent,
        re.DOTALL,
    )
    if not matches:
        return None
    print("Detected possible DOI.")
    # reverse the list because some chapters first put the book DOI and then the chapter DOI
    for match in reversed(matches):
        doi = trimdoi(match[4])
        # PNAS tends to return this
        if re.search(r"^10.\d{4}\/?$", doi):
            doi = re.sub(r".*?10\.(\d{4})\/? ([^\s]+).*", r"10.\1/\2", pdfcontent)
        # Elsevier accepted manuscripts
        if doi in ("Reference:", "Accepted Manuscript"):
            match = re.search(r"Accepted date: [^\s]+ ([^\s]+)", pdfcontent, re.DOTALL)
            if match:
                doi = match.group(1)
            else:
                print("Could not find DOI")
                return None
        # get rid of false positive DOIs containing only letters or numbers, or containing line breaks
        if doi and not re.search(r"^([a-z\(\)]*|\d*)$", doi) and "\n" not in doi:
            # remove final period
            doi = doi.rstrip(".")
            # get rid of urlencoded stuff
            doi = urllib.parse.unquote(doi)
            print("Found DOI: " + doi)
            return doi
        else:
            print(f"Could not find DOI: {doi}.")
    return None


def get_doi_data(art: Article) -> RawData:
    doi = extract_doi(art)
    if doi is not None:
        return expand_doi(doi)
    return {}


def get_jstor_data(art: Article) -> RawData:
    pdfcontent = art.getpdfcontent()
    if not re.search(
        r"(Stable URL: https?:\/\/www\.jstor\.org\/stable\/| Accessed: )",
        pdfcontent,
    ):
        return {}
    print("Detected JSTOR file; extracting data.")
    head_text = (
        pdfcontent.split(
            "\nYour use of the JSTOR archive indicates your acceptance of JSTOR's Terms and Conditions of Use"
        )[0]
        .split("\nJSTOR is a not-for-profit service that helps scholars")[0]
        .strip()
    )
    # get rid of occasional text above relevant info
    head_text = re.sub(r"^.*\n\n", "", head_text)
    # bail out
    if "Review by:" in head_text:
        print("Unable to process data")
        return {}

    # split into fields
    head = re.split(
        r"(\s*Author\(s\): |\s*(Reviewed work\(s\):.*)?\s*Source: |\s*Published by: |\s*Stable URL: |( \.)?\s*Accessed: )",
        head_text,
    )

    data: RawData = {}
    # handle the easy ones
    data["title"] = head[0]
    # multiplied by 4 because capturing groups also go into the output of re.split
    url = head[4 * 4]
    data["doi"] = "10.2307/" + url.split()[0].split("/")[-1]
    # problem sometimes
    if not re.search(r"(, Vol\. |, No\. | \(|\), pp?\. )", head[2 * 4]):
        print("Unable to process data")
        return {}
    # Process "source" field
    source = re.split(
        r",\s+(Vol|No|Bd|H)\.\s+|(?<=\d)\s+\(|\),\s+pp?\.\s+", head[2 * 4]
    )
    journal = source[0]
    if journal[-4:].isnumeric() and journal.count(", ") == 2:
        journal = journal.split(", ")[0].strip()
    data["journal"] = journal
    if data["journal"] == "Mammalian Species":
        source_field = head[2 * 4].strip()
        match = re.match(
            r"^Mammalian Species, (Vol|No)\. (?P<volume>\d+)"
            r"(, No\. (?P<issue>\d+)|, [A-Za-z ]+)? "
            r"\(.+ (?P<year>\d{4})\), pp\. (?P<pages>[\d-]+)$",
            source_field,
        )
        assert match is not None, f"failed to match {source_field}"
        data["volume"] = match.group("volume")
        if match.group("issue"):
            data["issue"] = match.group("issue")
        year = match.group("year")
        pages = match.group("pages")
    else:
        num_splits = (len(source) + 1) // 2
        if num_splits < 3:
            return {}
        try:
            data["volume"] = source[1 * 2]
            # issue may have been omitted
            if num_splits > 4:
                data["issue"] = source[2 * 2]
            # year
            year = source[3 * 2] if num_splits > 4 else source[2 * 2]
            # start and end pages
            pages = source[4 * 2] if num_splits > 4 else source[3 * 2]
        except IndexError:
            print("unable to process data")
            return {}
    data["year"] = re.sub(r"^.*,\s", "", year)
    first_last = pages.split("-")
    data["start_page"] = first_last[0]
    data["end_page"] = first_last[1] if len(first_last) > 1 else first_last[0]
    # Process authors field
    # Will fail with various variants, including double surnames
    authors = re.split(r"(, | and )", head[1 * 4])
    # array for correctly formatted authors
    fmtauth = []
    for i, author_str in enumerate(authors):
        if i % 2 == 1:
            continue
        author = author_str.split()
        lastname = author[-1]
        fmtauth.append(clean_up_author(lastname, author[:-1]))
    data["author_tags"] = fmtauth
    # if it isn't, this code fails miserably anyway
    data["type"] = ArticleType.JOURNAL
    return data


def get_zootaxa_data(art: Article) -> RawData:
    pdfcontent = art.getpdfcontent()
    if "ZOOTAXA" not in pdfcontent:
        return {}
    print("Detected Zootaxa file")
    zootaxa_rgx = re.compile(
        r"""
            \s*Zootaxa\s+
            (?P<volume>\d+):\s+(?P<start_page>\d+)[-–](?P<end_page>\d+)\s+\((?P<year>\d+)\)
            \n.*ISSN\s1175-5334\s\(online\sedition\)\s\s(ZOOTAXA\n\n)?
            (?P<title>.+)
            \n(?P<authors>[A-Z][^\n]+[A-Z]\d+)\n
        """,
        re.DOTALL | re.VERBOSE,
    )
    match = zootaxa_rgx.match(pdfcontent)
    data: RawData = {}
    if not match:
        print("failed to find match")
        return data
    data["type"] = ArticleType.JOURNAL
    data["title"] = match.group("title")
    data["year"] = match.group("year")
    data["journal"] = "Zootaxa"
    data["volume"] = match.group("volume")
    data["start_page"] = match.group("start_page")
    data["end_page"] = match.group("end_page")
    authors_str = re.sub(r"\d+(, ?\d+)*", "", match.group("authors"))
    authors = []
    for author in re.split(r", ?|\s?& ?", authors_str):
        first_names, last_name = author.rsplit(maxsplit=1)
        authors.append({"family_name": last_name.title(), "given_names": first_names})
    data["author_tags"] = authors
    return data


def clean_up_author(family_name: str, names: Sequence[str]) -> dict[str, str]:
    if all(name.endswith(".") for name in names):
        return {"family_name": family_name, "initials": "".join(names)}
    else:
        given_names = unspace_initials(" ".join(names))
        return {"family_name": family_name, "given_names": given_names}


def unspace_initials(authority: str) -> str:
    return re.sub(r"([A-Z]\.) (?=[A-Z]\.)", r"\1", authority).strip()


_REUSE_NOFILE_EXCLUDED = ("addmonth", "addday", "addyear", "adddate", "name", "path")


def reuse_nofile(art: Article) -> bool:
    while True:
        nofile = Article.getter(None).get_one("Citation handle to reuse: ")
        if nofile is None:
            return False
        if nofile.isfile():
            print(f"{nofile} is a file")
            continue
        break
    data = {
        field: getattr(nofile, field)
        for field in Article.fields()
        if field not in _REUSE_NOFILE_EXCLUDED
    }
    set_multi(art, data)
    nofile.merge(art)
    print("Data copied.")
    art.edit_until_clean(initial_edit=True)
    return True


def set_multi(art: Article, data: RawData, *, only_new: bool = True) -> None:
    for attr, value in clean_strings_recursively(data).items():
        if attr == "author_tags":
            set_author_tags_from_raw(art, value, only_new=only_new)
        elif attr == "journal":
            if art.citation_group is not None and only_new:
                print(f"{art}: ignoring journal {value}")
                continue
            print(f"{art}: set citation group to {value}")
            art.citation_group = CitationGroup.get_or_create(value)
        elif attr in Article.fields():
            current = getattr(art, attr)
            if current and only_new:
                print(f"{art}: ignore field {attr} (new: {value}; existing: {current})")
                continue
            print(f"{art}: set {attr} to {value}")
            setattr(art, attr, value)
    # Somehow this doesn't always autosave
    art.save()


def set_author_tags_from_raw(
    art: Article, value: Any, *, only_new: bool = True, interactive: bool = False
) -> None:
    if art.author_tags and only_new:
        print(f"{art}: dropping authors {value} (existing: {art.author_tags})")
        return
    for params in value:
        if params["family_name"].isupper():
            params["family_name"] = params["family_name"].title()
    new_tags = [
        AuthorTag.Author(person=Person.get_or_create_unchecked(**params))
        for params in value
    ]
    if art.author_tags is not None:
        if len(art.author_tags) == len(new_tags):
            new_tags = [
                existing if existing.person.is_more_specific_than(new.person) else new
                for existing, new in zip(art.author_tags, new_tags)
            ]
        getinput.print_diff(art.author_tags, new_tags)
    if interactive:
        if not getinput.yes_no("Replace authors? "):
            art.fill_field("author_tags")
            return
    art.author_tags = new_tags  # type: ignore


def doi_input(art: Article) -> bool:
    def processcommand(cmd: str) -> tuple[str | None, object]:
        if cmd in ("o", "r"):
            return cmd, None
        try:
            typ = _string_to_type[cmd]
        except KeyError:
            pass
        else:
            if typ:
                return "t", typ
        cmd = trimdoi(cmd)
        if is_valid_doi(cmd):
            return "d", cmd
        return None, None

    def reuse(cmd: str, data: object) -> bool:
        return not reuse_nofile(art)

    def set_type(cmd: str, data: object) -> bool:
        art.type = data  # type: ignore
        return False

    def set_doi(cmd: str, data: object) -> bool:
        art.doi = data
        return not art.expand_doi(set_fields=True)

    def opener(cmd: str, data: object) -> bool:
        art.openf()
        return True

    result, _ = uitools.menu(
        head="If this file has a DOI or AMNH handle, please enter it. Otherwise, enter the type of the file.",
        helpinfo=(
            "In addition to the regular commands, the following synonyms are accepted for the several types:\n"
            + _get_type_synonyms_as_string()
        ),
        options={
            "o": "open the file",
            "r": "re-use a citation from a NOFILE entry",
            "p": "print PDF content",
            # fake commands:
            # 't': 'set type',
            # 'd': 'enter doi',
        },
        processcommand=processcommand,
        validfunction=lambda *args: True,
        process={"o": opener, "r": reuse, "t": set_type, "d": set_doi},
    )
    return result in ("r", "h", "d")


_string_to_type: dict[str, ArticleType] = {
    "misc": ArticleType.MISCELLANEOUS,
    **{t.name[0].lower(): t for t in ArticleType},
    **{t.name.lower(): t for t in ArticleType},
    **{t.name: t for t in ArticleType},
}


def _get_type_synonyms_as_string() -> str:
    arr: dict[ArticleType, list[str]] = {}
    for key, value in _string_to_type.items():
        if isinstance(key, str):
            arr.setdefault(value, []).append(key)
    out = ""
    for typ, aliases in arr.items():
        out += f'{typ.name.lower().title()}: {", ".join(aliases)}\n'
    return out


AUTO_ADDERS = [get_jstor_data, get_zootaxa_data, get_doi_data]


def add_data_for_new_file(art: Article) -> bool:
    if art.kind is None:
        art.fill_field("kind")
    if art.kind is ArticleKind.redirect:
        return True
    successful = False
    if Path(art.name).suffix in (".pdf", ".PDF"):
        for adder in AUTO_ADDERS:
            try:
                data = adder(art)
            except Exception:
                traceback.print_exc()
                print(f"Failed to automatically extract data using {adder}")
            else:
                set_multi(art, data)
                successful = True
                break
    if not successful:
        successful = doi_input(art)
    if not successful:
        successful = art.trymanual()
    art.lint_wrapper()
    art.save()
    getinput.add_to_clipboard(art.name)
    art.edittitle()
    return successful
