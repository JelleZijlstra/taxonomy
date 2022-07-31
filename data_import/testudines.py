from dataclasses import dataclass, field
import enum
import itertools
import textwrap
import traceback

from collections.abc import Sequence

from functools import cached_property
from data_import.lib import Source, clean_string, get_text, extract_pages, split_lines
import re
from taxonomy.db.constants import Rank
from taxonomy.db import models
from typing_extensions import Self, assert_never

from taxonomy.db.models.name import TypeTag

SOURCE = Source("expt/recolumnized.txt", "Testudines (TTWG 2021).pdf")
REFS = Source("expt/refs.txt", "Testudines (TTWG 2021).pdf")
RefKey = tuple[tuple[str, ...], str]
DRY_RUN = True
VERBOSE = True


class LineKind(enum.Enum):
    taxon = 1
    synonym = 2
    type_species = 3
    type_locality = 4
    vernacular_name = 5
    type_specimen = 6
    comment = 7


@dataclass
class Line:
    text: str
    kind: LineKind


IGNORED_WORDS = {"and", "†"}

OVERRIDES = {
    ("Paolillo",): ("Paolillo", "O."),
    ("Kuyl", "Ballasina", "Dekker", "Maas", "Willemsen", "Goudsmit"): (
        "van",
        "der",
        "Kuyl",
        "Ballasina",
        "Dekker",
        "Maas",
        "Willemsen",
        "Goudsmit",
    ),
    ("Turtle", "Taxonomy", "Working", "Group"): ("TTWG",),
    ("Carvalho",): ("Carvalho", "de", "M."),
    ("Blainville",): ("Blainville", "de"),
}


def _make_key(authors: Sequence[str], year_key: str):
    authors = tuple(authors)
    return OVERRIDES.get(authors, authors), year_key


@dataclass
class NameDetails:
    original_name: str
    authority: list[str]
    year: str
    ref: str
    page: str | None
    comment: str | None = None

    @property
    def root_name(self) -> str:
        return self.original_name.split()[-1]

    @cached_property
    def author_people(self) -> list[models.Person]:
        return [models.Person.get_or_create_unchecked(name) for name in self.authority]

    @property
    def author_tags(self) -> tuple[models.person.AuthorTag, ...]:
        return tuple(models.person.AuthorTag.Author(p) for p in self.author_people)

    @classmethod
    def parse(cls, stripped: str, refs_dict: dict[RefKey, str]) -> Self:
        pieces = stripped.split()
        name_bits = [pieces[0]]
        idx = 1
        while (
            pieces[idx][0].islower() or pieces[idx][0] == "(" or pieces[idx] == "Japon"
        ):
            name_bits.append(pieces[idx])
            idx += 1
        author_bits: list[str] = []
        seen_in = False
        while not pieces[idx][0].isdigit():
            if pieces[idx] == "in":
                seen_in = True
                idx += 1
                break
            elif pieces[idx] not in IGNORED_WORDS:
                author_bits.append(pieces[idx].replace(",", ""))
            idx += 1
        in_author_bits: list[str] = []
        if seen_in:
            while not pieces[idx][0].isdigit():
                if pieces[idx] not in IGNORED_WORDS:
                    in_author_bits.append(pieces[idx].replace(",", ""))
                idx += 1

        year_plus = pieces[idx]
        idx += 1
        if idx < len(pieces):
            comment = " ".join(pieces[idx:])
        else:
            comment = None

        if ":" in year_plus:
            year_key, page = year_plus.strip(",").split(":", maxsplit=1)
        else:
            year_key = year_plus
            page = None
        year = year_key[:4]

        key = _make_key(author_bits, year_key)
        try:
            ref = refs_dict[key]
        except KeyError:
            if in_author_bits:
                key = _make_key(in_author_bits, year_key)
                ref = refs_dict.get(key)
            else:
                ref = None
        if ref is None:
            print("Failed to extract author for", stripped)

        return cls(" ".join(name_bits), author_bits, year, ref, page, comment)


@dataclass
class Name:
    name_line: str
    type_locality: str | None = None
    type_specimen: str | None = None
    type_species: str | None = None
    comment: str | None = None
    details: NameDetails | None = None


@dataclass
class Taxon:
    name_line: str
    rank: Rank
    name: str
    authority: list[str]
    vernacular_name: str | None = None
    names: list[Name] = field(default_factory=list)
    parent: "Taxon | None" = field(default=None, repr=False)
    models_taxon: models.Taxon | None = None

    def get_models_taxon(self) -> models.Taxon | None:
        if self.models_taxon is not None:
            return self.models_taxon
        name = self.name
        if self.rank in (Rank.species, Rank.subspecies):
            name = re.sub(r" \([^\)]+\)", "", name)
        candidates = list(
            models.Taxon.filter(
                models.Taxon.valid_name == name, models.Taxon.rank == self.rank
            )
        )
        if len(candidates) > 1:
            raise RuntimeError(f"multiple candidates for {self}: {candidates}")
        if not candidates:
            return None
        self.models_taxon = candidates[0]
        return candidates[0]


def parse_refs() -> dict[RefKey, str]:
    refs: dict[RefKey, str] = {}
    current_key: RefKey | None = None
    current_lines: list[str] = []
    text = get_text(REFS)
    pages = extract_pages(text)
    lines = []
    for i, page_lines in pages:
        page_lines = textwrap.dedent("\n".join(page_lines)).splitlines()
        lines += split_lines(page_lines, i, dedent_right=False)
    lines = itertools.dropwhile(
        lambda l: l.strip() != "• IUCN Red List Assessments", lines
    )

    ref_list = []
    for line in lines:
        stripped = line.strip()
        if stripped == "CBFTT ACCOUNTS":
            break
        if stripped == "• IUCN Red List Assessments":
            continue
        if not stripped:
            continue
        if line.startswith(" "):
            ref_list[-1].append(line)
        else:
            ref_list.append([line])

    for ref in ref_list:
        ref_text = (
            "\t".join([line.strip() for line in ref])
            .replace("-\t", "-")
            .replace("/\t", "/")
            .replace("\t", " ")
        )

        m = re.search(
            r"^(.*?\.(?: de| \(Eds\.\))?) \b((15|16|17|18|19|20)\d\d[a-z]?)( \[[^\]]+\])?\.",
            ref_text,
        )
        assert m is not None, ref_text
        authors = m.group(1)
        year = m.group(2)
        authors = authors.replace(",", ", ")
        authors = re.sub(r"\s+", " ", authors)
        authors = re.sub(r" \[[^\]]+\]\.", "", authors)
        authors = re.sub(r" (de|von|zu|da Silva|van|da|\(Ed\.\))\.", "", authors)
        authors = re.sub(r" (de|von|zu|da Silva)\,", ",", authors)
        authors = re.sub(r", [JS]r\.", "", authors)
        authors = re.sub(r", ([A-Z]\.-?)+", "", authors)
        authors = re.sub(r",? and ", ", ", authors)
        authors = authors.replace("Boeadi.", "Boeadi")
        current_key = tuple(" ".join(authors.split(", ")).split()), year
        refs[current_key] = clean_string(" ".join(current_lines))

    with open("data_import/data/expt/ref_keys.txt", "w") as f:
        for key in sorted(refs):
            print(key, file=f)
    return refs


def indentation_of(line: str) -> int:
    return len(line) - len(line.lstrip())


def is_sentence_end(line: str) -> bool:
    if line.endswith(".”"):
        return True
    return line.endswith(".") and not line.endswith(" et al.")


@dataclass
class TaxaParser:
    lines: list[Line] = field(default_factory=list)
    taxa: list[Taxon] = field(default_factory=list)
    refs: dict[RefKey, str] = field(default_factory=dict)
    in_synonymy: bool = False

    def run(self) -> None:
        self.refs = parse_refs()

        lines = get_text(SOURCE)
        for line in lines:
            try:
                self.parse_line(line)
            except Exception as e:
                traceback.print_exc()
                print("Failed to parse:", line, "due to", repr(e))

    def parse_line(self, line: str) -> None:
        line = line.rstrip().replace("\t", "    ")
        stripped = line.strip()
        if not stripped:
            return
        leading_spaces = len(line) - len(stripped)
        if leading_spaces == 0:
            kind = LineKind.taxon
            # For genus names, there's no "Synonymy" header
            first_char = re.sub(r" \([A-Za-z]+\)", "", line).split()[1][0]
            self.in_synonymy = first_char.isupper()
        else:
            if stripped == "Synonymy:":
                self.in_synonymy = True
                return
            if stripped.startswith("Type species:"):
                assert self.in_synonymy, line
                kind = LineKind.type_species
            elif stripped.startswith("Type locality:"):
                assert self.in_synonymy, line
                kind = LineKind.type_locality
            elif stripped.startswith("Comment:"):
                assert self.in_synonymy, line
                kind = LineKind.comment
            elif stripped.startswith("Type specimen"):
                assert self.in_synonymy, line
                kind = LineKind.type_specimen
            elif stripped.startswith("Geologic age:"):
                return
            elif not self.in_synonymy:
                return
            elif re.search(r"\d", line):
                kind = LineKind.synonym
            else:
                return
        self.lines.append(Line(line, kind))

        match kind:
            case LineKind.type_locality:
                assert self.taxa[-1].names[-1].type_locality is None, (
                    repr(self.taxa[-1].names),
                    line,
                )
                self.taxa[-1].names[-1].type_locality = stripped.removeprefix(
                    "Type locality:"
                ).strip()
            case LineKind.type_specimen:
                assert self.taxa[-1].names[-1].type_specimen is None, (
                    repr(self.taxa[-1].names),
                    line,
                )
                self.taxa[-1].names[-1].type_specimen = stripped.strip()
            case LineKind.comment:
                assert self.taxa[-1].names[-1].comment is None, (
                    repr(self.taxa[-1].names),
                    line,
                )
                self.taxa[-1].names[-1].comment = stripped.removeprefix(
                    "Comment:"
                ).strip()
            case LineKind.type_species:
                assert self.taxa[-1].names[-1].type_species is None, (
                    repr(self.taxa[-1].names),
                    line,
                )
                self.taxa[-1].names[-1].type_species = stripped.removeprefix(
                    "Type species:"
                ).strip()
            case LineKind.vernacular_name:
                pass
            case LineKind.synonym:
                try:
                    details = NameDetails.parse(stripped, self.refs)
                except Exception as e:
                    traceback.print_exc()
                    print(f"Failed to parse {stripped}: {e!r}")
                    details = None
                self.taxa[-1].names.append(Name(stripped, details=details))
            case LineKind.taxon:
                words = line.split()
                assert len(words) >= 3, line
                rank: Rank | None = None
                name_words = [words[0]]
                index = 1
                while True:
                    if words[index][0] == "(" and rank is None:
                        rank = Rank.subgenus
                        name_words.append(words[index])
                    elif words[index][0].islower():
                        if rank is Rank.species:
                            rank = Rank.subspecies
                        else:
                            rank = Rank.species
                        name_words.append(words[index])
                    else:
                        break
                    index += 1
                authority = words[index:]
                if rank is None:
                    name = words[0]
                    if name.endswith("inae"):
                        rank = Rank.subfamily
                    elif name.endswith("idae"):
                        rank = Rank.family
                    elif name.endswith("oidea"):
                        rank = Rank.superfamily
                    elif name.endswith("odira"):
                        rank = Rank.suborder
                    elif name == "Testudines":
                        rank = Rank.order
                    else:
                        rank = Rank.genus
                if self.taxa:
                    parent = self.taxa[-1]
                else:
                    parent = None
                while parent is not None and parent.rank <= rank:
                    parent = parent.parent
                name = " ".join(name_words)
                taxon = Taxon(line, rank, name, authority, parent=parent)
                self.taxa.append(taxon)
            case _:
                assert_never(kind)


def get_taxa() -> list[Taxon]:
    parser = TaxaParser()
    parser.run()
    return parser.taxa


def maybe_add(nam: models.Name, attr: str, value: object) -> None:
    current = getattr(nam, attr)
    if not current:
        if VERBOSE:
            print(f"{nam}: set {attr} to {value}")
        if not DRY_RUN:
            setattr(nam, attr, value)
    elif current != value:
        if attr == "author_tags":
            left = [t.person.family_name for t in current]
            right = [t.person.family_name for t in value]
            if left == right:
                return
        if attr == "verbatim_citation":
            if value is None:
                return
            if value in current:
                return
            new_value = f"{current} [From {{{SOURCE.source}}}: {value}]"
            if VERBOSE:
                print(f"{nam}: set verbatim_citation to {new_value}")
            if not DRY_RUN:
                nam.verbatim_citation = new_value
            return
        print(f"{nam}: {attr}: {current} != {value}")


def fill_name(nam: models.Name, name: Name) -> None:
    art = SOURCE.get_source()
    if name.type_locality is not None:
        tag = TypeTag.LocationDetail(name.type_locality, art)
        if VERBOSE:
            print(f"{nam}: add tag {tag}")
        if not DRY_RUN:
            nam.add_type_tag(tag)
    if name.type_species is not None:
        tag = TypeTag.TypeSpeciesDetail(name.type_species, art)
        if VERBOSE:
            print(f"{nam}: add tag {tag}")
        if not DRY_RUN:
            nam.add_type_tag(tag)
    if name.details is not None:
        maybe_add(nam, "original_name", name.details.original_name)
        maybe_add(nam, "author_tags", name.details.author_tags)
        maybe_add(nam, "year", name.details.year)
        maybe_add(nam, "verbatim_citation", name.details.ref)
        if name.details.page is not None:
            maybe_add(nam, "page_described", name.details.page)
        if name.details.comment is not None and name.details.comment.strip().startswith(
            "("
        ):
            tag = TypeTag.NomenclatureDetail(name.details.comment, art)
            if VERBOSE:
                print(f"{nam}: add tag {tag}")
            if not DRY_RUN:
                nam.add_type_tag(tag)


def handle_taxon(taxon: Taxon) -> None:
    models_taxon = taxon.get_models_taxon()
    if models_taxon is None:
        assert taxon.parent is not None, f"{taxon} has no parent"
        parent_model = taxon.parent.get_models_taxon()
        is_nominate_ssp = taxon.rank is Rank.subspecies and re.search(
            r"( [a-z]+)\1$", taxon.name
        )
        print(f"Add {taxon.rank.name} {taxon.name} to {parent_model}")
        if is_nominate_ssp and VERBOSE:
            print("Note: Add as nominate subspecies")
        if not DRY_RUN:
            assert (
                parent_model is not None
            ), f"{taxon.parent} is not associated with a taxon"
            if is_nominate_ssp:
                models_taxon = parent_model.add_nominate()
            else:
                models_taxon = parent_model.add_static(taxon.rank, taxon.name)
    elif VERBOSE:
        print(f"Taxon: Associate {taxon.rank.name} {taxon.name} with {models_taxon}")
    if models_taxon is None or not taxon.names:
        return
    fill_name(models_taxon.base_name, taxon.names[0])
    for name in taxon.names[1:]:
        assert name.details is not None, repr(name)
        nam = models_taxon.syn(
            name.details.root_name,
            year=name.details.year,
            author_tags=name.details.author_tags,
        )
        if nam is None:
            print(f"Add new name for {name}")
            if not DRY_RUN:
                nam = models_taxon.add_syn(name.details.root_name, interactive=False)
        elif VERBOSE:
            print(f"Name: Associate {nam} with {name}")
        if nam is not None:
            fill_name(nam, name)


def main() -> None:
    taxa = get_taxa()
    for taxon in taxa:
        break
        handle_taxon(taxon)


if __name__ == "__main__":
    main()
