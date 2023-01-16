"""

Adding data to (usually new) files.

"""
import enum
from functools import lru_cache
import json
from pathlib import Path
import re
import requests
import traceback
from typing import Any
from collections.abc import Sequence
import urllib.parse

from .article import Article, ArticleTag
from .lint import is_valid_doi
from ..citation_group import CitationGroup, CitationGroupTag
from ..person import AuthorTag, Person
from ...constants import ArticleType, ArticleKind
from ...helpers import clean_string, trimdoi, clean_strings_recursively
from ...url_cache import cached, CacheDomain
from .... import config, parsing, getinput, uitools, command_set

_options = config.get_options()

CS = command_set.CommandSet("add_data", "Commands for adding data to articles")

RawData = dict[str, Any]


@lru_cache
def get_doi_json(doi: str) -> dict[str, Any] | None:
    try:
        return json.loads(get_doi_json_cached(doi))
    except Exception:
        traceback.print_exc()
        print(f"Could not resolve DOI {doi}")
        return None


@cached(CacheDomain.doi)
def get_doi_json_cached(doi: str) -> str:
    # "Good manners" section in https://api.crossref.org/swagger-ui/index.html
    url = f"https://api.crossref.org/works/{urllib.parse.quote(doi)}?mailto=jelle.zijlstra@gmail.com"
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def is_doi_valid(doi: str) -> bool:
    try:
        get_doi_json_cached(doi)
    except requests.exceptions.HTTPError as e:
        if "404 Client Error: Not Found" not in str(e):
            traceback.print_exc()
            print("ignoring unexpected error")
            return True
        return False
    return True


# values from http://www.crossref.org/schema/queryResultSchema/crossref_query_output2.0.xsd
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
for _key, _value in list(doi_type_to_article_type.items()):
    # usage seems to be inconsistent, let's just use both
    doi_type_to_article_type[_key.replace("-", "_")] = _value
    doi_type_to_article_type[_key.replace("_", "-")] = _value


def expand_doi_json(doi: str) -> RawData:
    result = get_doi_json(doi)
    if result is None:
        return {}
    work = result["message"]
    data: RawData = {"doi": doi}
    typ = ArticleType.ERROR
    if work["type"] in doi_type_to_article_type:
        data["type"] = typ = doi_type_to_article_type[work["type"]]

    if titles := work.get("title"):
        title = titles[0]
        if title.isupper():
            # all uppercase title; let's clean it up a bit
            # this won't give correct capitalization, but it'll be better than all-uppercase
            title = title[0] + title[1:].lower()
        data["title"] = clean_string(title)

    if author_raw := work.get("author"):
        authors = []
        for author in author_raw:
            info = {"family_name": clean_string(author["family"])}
            if given := author["given"]:
                given_names = clean_string(given.title())
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

    if volume := work.get("volume"):
        data["volume"] = volume.removeprefix("0")
    if issue := work.get("issue"):
        data["issue"] = issue.removeprefix("0")

    if page := work.get("page"):
        if typ in (ArticleType.JOURNAL, ArticleType.CHAPTER):
            if match := re.fullmatch(r"^(\d+)-(\d+)", page):
                data["start_page"] = match.group(1)
                data["end_page"] = match.group(2)
            elif page.isnumeric():
                data["start_page"] = data["end_page"] = page
            else:
                data["start_page"] = page
        else:
            data["pages"] = page
    if date := work.get("published", {}).get("date-parts"):
        data["year"] = str(date[0][0])

    if isbns := work.get("ISBN"):
        isbn = isbns[0]
    else:
        isbn = None

    if typ is ArticleType.BOOK:
        data["isbn"] = isbn

    if container_title := get_container_title(work):
        if typ is ArticleType.JOURNAL:
            data["journal"] = container_title
        elif typ is ArticleType.CHAPTER:
            data["parent_info"] = {"title": container_title, "isbn": isbn}

    return data


def get_container_title(work: dict[str, Any]) -> str | None:
    if container_title := work.get("container-title"):
        title = container_title[0]
        return title.replace("&amp;", "&").replace("’", "'")
    return None


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
        return expand_doi_json(doi)
    return {}


class ISSNKind(enum.Enum):
    print = 1
    electronic = 2
    other = 3


def get_issns(
    doi: str, verbose: bool = False
) -> tuple[str, list[tuple[ISSNKind, str]]] | None:
    data = get_doi_json(doi)
    if data is None:
        if verbose:
            print(f"{doi}: found no information")
        return None
    work = data["message"]
    journal = get_container_title(work)
    if journal is None:
        if verbose:
            print(f"{doi}: no container title")
        return None
    raw_issns = work.get("ISSN", [])
    typed_issns = work.get("issn-type", [])

    pairs = []
    seen_issns = set()
    for typed in typed_issns:
        issn = typed["value"]
        if typed["type"] == "print":
            pairs.append((ISSNKind.print, issn))
        elif typed["type"] == "electronic":
            pairs.append((ISSNKind.electronic, issn))
        else:
            print("unexpected ISSN type:", typed["type"])
            pairs.append((ISSNKind.other, issn))
        seen_issns.add(issn)
    for issn in raw_issns:
        if issn not in seen_issns:
            pairs.append((ISSNKind.other, issn))
    return journal, pairs


def get_cg_by_name(name: str) -> CitationGroup | None:
    try:
        cg = CitationGroup.select().filter(CitationGroup.name == name).get()
    except CitationGroup.DoesNotExist:
        return None
    if target := cg.get_redirect_target():
        return target
    return cg


@CS.register
def fill_issns(limit: int | None = None, verbose: bool = False) -> None:
    for art in (
        Article.select_valid()
        .filter(
            Article.type == ArticleType.JOURNAL,
            Article.doi != None,
            Article.citation_group != None,
        )
        .limit(limit)
    ):
        cg = art.citation_group
        existing_issn = cg.get_tag(CitationGroupTag.ISSN)
        existing_issn_online = cg.get_tag(CitationGroupTag.ISSNOnline)
        if existing_issn or existing_issn_online:
            if verbose:
                print(f"{cg}: ignoring because it has an ISSN")
            continue
        issns = get_issns(art.doi, verbose=verbose)
        if issns is None:
            art.maybe_remove_corrupt_doi()
            if verbose:
                print(f"{art}: ignoring because there was no ISSN information")
            continue
        if verbose:
            print(f"{art}: got ISSN information {issns}")
        journal_name, pairs = issns
        if not pairs:
            if verbose:
                print(f"{art}: no ISSNs")
            continue
        found_cg = get_cg_by_name(journal_name)
        if found_cg is None:
            print(
                f"{art} ({cg.name}): Ignoring ISSNs {pairs} because {journal_name} is"
                " not a known citation group"
            )
            continue
        if found_cg != cg:
            print(f"{art}: Ignoring ISSNs {pairs} because {journal_name} != {cg.name}")
            continue
        for kind, issn in pairs:
            if kind is ISSNKind.electronic:
                tag = CitationGroupTag.ISSNOnline(issn)
            else:
                tag = CitationGroupTag.ISSN(issn)
            if journal_name != cg.name:
                extra = f" (using name {journal_name})"
            else:
                extra = ""
            print(f"{cg}{extra}: adding tag {tag}")
            cg.add_tag(tag)


def get_jstor_data(art: Article) -> RawData:
    pdfcontent = art.getpdfcontent()
    if not re.search(
        r"(Stable URL: https?:\/\/www\.jstor\.org\/stable\/| Accessed: )", pdfcontent
    ):
        return {}
    print("Detected JSTOR file; extracting data.")
    head_text = (
        pdfcontent.split(
            "\nYour use of the JSTOR archive indicates your acceptance of JSTOR's Terms"
            " and Conditions of Use"
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
        (
            r"(\s*Author\(s\): |\s*(Reviewed work\(s\):.*)?\s*Source: |\s*Published by:"
            r" |\s*Stable URL: |( \.)?\s*Accessed: )"
        ),
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
        elif attr == "isbn":
            existing = art.getIdentifier(ArticleTag.ISBN)
            if existing:
                if existing == value:
                    continue
                if only_new:
                    print(f"{art}: ignoring ISBN {value}")
                    continue
            art.add_tag(ArticleTag.ISBN(text=value))
        elif attr in Article.fields():
            current = getattr(art, attr)
            if current and only_new:
                if current != value:
                    print(
                        f"{art}: ignore field {attr} (new: {value}; existing:"
                        f" {current})"
                    )
                continue
            print(f"{art}: set {attr} to {value}")
            setattr(art, attr, value)
        else:
            print(f"warning: ignoring field {attr}: {value}")
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
                for existing, new in zip(art.author_tags, new_tags, strict=True)
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
        head=(
            "If this file has a DOI or AMNH handle, please enter it. Otherwise, enter"
            " the type of the file."
        ),
        helpinfo=(
            "In addition to the regular commands, the following synonyms are accepted"
            " for the several types:\n"
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


def add_data_for_new_file(art: Article) -> None:
    if art.kind is None:
        art.fill_field("kind")
    if art.kind is ArticleKind.redirect:
        return
    successful = False
    if Path(art.name).suffix in (".pdf", ".PDF"):
        for adder in AUTO_ADDERS:
            try:
                data = adder(art)
            except Exception:
                traceback.print_exc()
                print(f"Failed to automatically extract data using {adder}")
            else:
                if data:
                    set_multi(art, data)
                    successful = True
                    break
    if not successful:
        successful = doi_input(art)
    if not successful:
        art.trymanual()
    art.format()
    art.save()
    art.add_to_history()
    art.add_to_history("name")
    getinput.add_to_clipboard(art.name)
    art.edittitle()
    art.edit_until_clean()
    print("Added to catalog!")
