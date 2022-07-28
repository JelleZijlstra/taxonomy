from dataclasses import dataclass, field
import enum

from collections.abc import Sequence

from functools import cached_property
from data_import.lib import Source, clean_string, get_text
import re
from taxonomy.db.constants import Rank
from taxonomy.db import models
from typing_extensions import Self, assert_never

from taxonomy.db.models.name import TypeTag

SOURCE = Source("testudines.txt", "Testudines (TTWG 2017).pdf")
REFS = Source("refs.txt", "Testudines (TTWG 2017).pdf")
RefKey = tuple[tuple[str, ...], str]
DRY_RUN = True
VERBOSE = False


class LineKind(enum.Enum):
    taxon = 1
    synonym = 2
    type_species = 3
    type_locality = 4
    vernacular_name = 5


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
            year_key, page = year_plus.strip(",").split(":")
        else:
            year_key = year_plus
            page = None
        year = year_key[:4]

        key = _make_key(author_bits, year_key)
        try:
            ref = refs_dict[key]
        except KeyError:
            if not in_author_bits:
                raise
            key = _make_key(in_author_bits, year_key)
            ref = refs_dict[key]

        return cls(" ".join(name_bits), author_bits, year, ref, page, comment)


@dataclass
class Name:
    name_line: str
    type_locality: str | None = None
    type_species: str | None = None
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
    for line in get_text(REFS):
        if not line.strip():
            continue
        if (
            (line[0].isupper() or line.startswith("van der"))
            and "," in line
            and (
                m := re.search(
                    r"^(.*?\.) \b((17|18|19|20)\d\d[a-z]?)( \[[^\]]+\])?\.", line
                )
            )
        ):
            if current_lines:
                assert current_key is not None
                refs[current_key] = clean_string(" ".join(current_lines))
                current_lines = []
            current_lines = [line]
            authors = m.group(1)
            year = m.group(2)
            authors = re.sub(r" \[[^\]]+\]\.", "", authors)
            authors = re.sub(r" (de|von|zu|da Silva|van|da|\(Ed\.\))\.", "", authors)
            authors = re.sub(r" (de|von|zu|da Silva)\,", ",", authors)
            authors = re.sub(r", [JS]r\.", "", authors)
            authors = re.sub(r", ([A-Z]\.-?)+", "", authors)
            authors = re.sub(r",? and ", ", ", authors)
            authors = authors.replace("Boeadi.", "Boeadi")
            current_key = tuple(" ".join(authors.split(", ")).split()), year

        else:
            current_lines.append(line)

    if current_lines:
        assert current_key is not None
        refs[current_key] = clean_string(" ".join(current_lines))
    return refs


def get_taxa() -> list[Taxon]:
    refs = parse_refs()
    lines: list[Line] = []
    taxa: list[Taxon] = []
    for line in get_text(SOURCE):
        line = line.rstrip().replace("\t", "    ")
        stripped = line.strip()
        if not stripped:
            continue
        leading_spaces = len(line) - len(stripped)
        if leading_spaces == 0:
            kind = LineKind.taxon
        else:
            if stripped.startswith("Type species:"):
                kind = LineKind.type_species
            elif stripped.startswith("Type locality:"):
                kind = LineKind.type_locality
            elif lines[-1].kind is LineKind.taxon and not any(
                c.isdigit() for c in line
            ):
                kind = LineKind.vernacular_name
            else:
                kind = LineKind.synonym
        lines.append(Line(line, kind))

        match kind:
            case LineKind.type_locality:
                assert taxa[-1].names[-1].type_locality is None, (
                    repr(taxa[-1].names),
                    line,
                )
                taxa[-1].names[-1].type_locality = stripped.removeprefix(
                    "Type locality:"
                ).strip()
            case LineKind.type_species:
                assert taxa[-1].names[-1].type_species is None, (
                    repr(taxa[-1].names),
                    line,
                )
                taxa[-1].names[-1].type_species = stripped.removeprefix(
                    "Type locality:"
                ).strip()
            case LineKind.vernacular_name:
                assert taxa[-1].vernacular_name is None, repr(taxa[-1])
                taxa[-1].vernacular_name = stripped
            case LineKind.synonym:
                try:
                    details = NameDetails.parse(stripped, refs)
                except Exception as e:
                    print(f"Failed to parse {stripped}: {e}")
                    details = None
                taxa[-1].names.append(Name(stripped, details=details))
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
                if taxa:
                    parent = taxa[-1]
                else:
                    parent = None
                while parent is not None and parent.rank <= rank:
                    parent = parent.parent
                taxon = Taxon(
                    line, rank, " ".join(name_words), authority, parent=parent
                )
                taxa.append(taxon)
            case _:
                assert_never(kind)
    return taxa


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
            if value in current:
                return
            new_value = f"{current} [From {{{SOURCE.inputfile}}}: {value}]"
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
        handle_taxon(taxon)


if __name__ == "__main__":
    main()
