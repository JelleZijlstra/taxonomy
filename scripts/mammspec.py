import argparse
import re
from dataclasses import dataclass

from taxonomy.db import models
from taxonomy.db.constants import Group, NomenclatureStatus, Rank, RegionKind
from taxonomy.db.models.article.article import Article
from taxonomy.db.models.name.name import Name, NameTag, TypeTag
from taxonomy.db.models.person import Person
from taxonomy.db.models.taxon.taxon import Taxon


@dataclass(frozen=True)
class SynonymyEntry:
    name: str
    authors: list[Person] | None
    citation: Article | None
    colon: bool
    page: str
    post_text: str
    references: list[Article]

    def stringify(self, art_to_ref: dict[Article, tuple[str, str]]) -> str:
        if self.citation is None:
            citation = "**WARNING: Missing original citation**"
        else:
            author, year = art_to_ref[self.citation]
            citation = f"{author}, {year}"
        if self.authors is not None:
            citation = f"{stringify_author_list(self.authors)} in {citation}"

        def replacer(match: re.Match[str]) -> str:
            art_id = int(match.group(1))
            art = Article(art_id)
            author, year = art_to_ref[art]
            return f"{author}, {year}"

        post_text = re.sub(r"{/a/(\d+)}", replacer, self.post_text)

        return f"_{self.name}_{':' if self.colon else ''} {citation}:{self.page}. {post_text}"


def get_original_type_locality(nam: Name) -> str | None:
    if nam.type_locality is None:
        return None
    for tag in nam.type_tags:
        if (
            isinstance(tag, TypeTag.LocationDetail)
            and tag.source == nam.original_citation
        ):
            return tag.text
    return None


def make_entry(nam: Name, taxon: Taxon) -> SynonymyEntry | None:
    statuses = list(
        models.name.lint.get_applicable_nomenclature_statuses_from_tags(nam)
    )
    colon = False
    references = []
    if NomenclatureStatus.incorrect_subsequent_spelling in statuses:
        tag_target = nam.get_tag_target(NameTag.IncorrectSubsequentSpellingOf)
        if tag_target is None or tag_target.original_citation is None:
            post_text = "**WARNING: Missing IncorrectSubsequentSpellingOf tag**"
        elif nam.corrected_original_name is None:
            post_text = "**WARNING: Missing corrected original name**"
        else:
            expected_con = re.sub(
                rf"{nam.root_name}$", tag_target.root_name, nam.corrected_original_name
            )
            if expected_con == tag_target.corrected_original_name:
                intro = "Incorrect subsequent spelling of"
            else:
                intro = "Name combination and incorrect subsequent spelling of"
            post_text = f"{intro} _{tag_target.root_name}_ {{/a/{tag_target.original_citation.id}}}."
    elif NomenclatureStatus.unjustified_emendation in statuses:
        tag_target = nam.get_tag_target(NameTag.UnjustifiedEmendationOf)
        if tag_target is None or tag_target.original_citation is None:
            post_text = "**WARNING: Missing UnjustifiedEmendationOf tag**"
        else:
            post_text = f"Unjustified emendation of _{tag_target.root_name}_ {{/a/{tag_target.original_citation.id}}}."
    elif NomenclatureStatus.nomen_novum in statuses:
        tag_target = nam.get_tag_target(NameTag.NomenNovumFor)
        if tag_target is None or tag_target.original_citation is None:
            post_text = "**WARNING: Missing NomenNovumFor tag**"
        else:
            post_text = f"Nomen novum for _{tag_target.root_name}_ {{/a/{tag_target.original_citation.id}}}."
    elif NomenclatureStatus.name_combination in statuses:
        colon = True
        if nam.corrected_original_name == taxon.valid_name:
            post_text = "First use of current name combination."
        else:
            post_text = "Name combination."
    elif nam.group is Group.genus and nam.type is not None:
        if nam.taxon != taxon:
            prefix = "Part. "
        else:
            prefix = ""
        type_kind = (
            nam.genus_type_kind.name.replace("_", " ")
            if nam.genus_type_kind is not None
            else "**WARNING: Type kind not known.**"
        )
        if nam.type.original_citation is None:
            aut = "**WARNING: Original citation unknown**"
        else:
            aut = f"{{/a/{nam.type.original_citation.id}}}"
            references.append(nam.type.original_citation)
        post_text = f"{prefix}Type species _{nam.type.corrected_original_name}_ {aut}, by {type_kind}."
    elif (tl_text := get_original_type_locality(nam)) and nam.type_locality is not None:
        country = nam.type_locality.region.parent_of_kind(RegionKind.country)
        post_text = f'Type locality "{tl_text}'
        if country is not None:
            post_text += f'," {country.name}.'
        else:
            post_text += '."'
    elif nam.nomenclature_status is NomenclatureStatus.available:
        post_text = "**WARNING: Type data missing.**"
    else:
        post_text = "**WARNING: Unknown name type**"
    if nam.original_citation is None:
        post_text += " **WARNING: Missing original citation**"
        authors = nam.get_authors()
    else:
        references.append(nam.original_citation)
        if nam.get_authors() == nam.original_citation.get_authors():
            authors = None
        else:
            authors = nam.get_authors()
    return SynonymyEntry(
        name=nam.original_name or "**WARNING: Missing original name**",
        authors=authors,
        citation=nam.original_citation,
        page=nam.page_described or "**WARNING: Missing page described**",
        post_text=post_text,
        references=references,
        colon=colon,
    )


def synonymy_for_taxon(taxon: Taxon) -> list[SynonymyEntry]:
    nams_set = {nam for nam in taxon.all_names() if nam.group is taxon.base_name.group}
    if taxon.rank in (Rank.genus, Rank.subgenus):
        for species_nam in taxon.all_names():
            if species_nam.original_parent is not None:
                nams_set.add(species_nam.original_parent)
    nams = sorted(
        nams_set,
        key=lambda nam: (
            nam.numeric_year(),
            nam.taxonomic_authority(),
            nam.numeric_page_described(),
            nam.original_rank or Rank.subspecies,
            nam.root_name,
        ),
    )
    entries = [make_entry(nam, taxon) for nam in nams]
    return [entry for entry in entries if entry is not None]


def get_reference_key(art: Article) -> tuple[str, str]:
    authors = list(art.get_authors())
    return stringify_author_list(authors), get_year(art.year)


def get_year(year: str | None) -> str:
    return year[:4] if year else "Unknown"


def stringify_author_list(authors: list[Person]) -> str:
    if len(authors) <= 2:
        return " and ".join(auth.family_name for auth in authors)
    return f"{authors[0].family_name} et al."


def stringify_synonymies(syns: list[tuple[Taxon, list[SynonymyEntry]]]) -> list[str]:
    references = {
        ref for _, entries in syns for entry in entries for ref in entry.references
    }
    refs_by_key: dict[tuple[str, str], list[Article]] = {}
    for ref in references:
        key = get_reference_key(ref)
        refs_by_key.setdefault(key, []).append(ref)
    ref_lines: list[str] = []
    art_to_ref: dict[Article, tuple[str, str]] = {}
    for (author, year), arts in sorted(refs_by_key.items()):
        num = len(arts)
        for i, art in enumerate(
            sorted(arts, key=lambda art: (art.get_date_object(), art.id))
        ):
            if num == 1:
                suffix = ""
            else:
                suffix = chr(i + ord("a"))
            citation = models.article.citations.cite_mammspec(art, year_suffix=suffix)
            ref_lines.append(citation)
            art_to_ref[art] = (author, year + suffix)

    lines = []
    for taxon, synonymy in syns:
        if taxon.base_name.original_citation is None:
            authority = taxon.base_name.taxonomic_authority()
            year = get_year(taxon.base_name.year)
        else:
            authority, year = art_to_ref[taxon.base_name.original_citation]
        parens = taxon.base_name.should_parenthesize_authority()
        line = f"**_{taxon.valid_name}_ {'(' if parens else ''}{authority}, {year}{')' if parens else ''}**"
        lines.append(line)

        for syn in synonymy:
            lines.append("* " + syn.stringify(art_to_ref))

    lines.append("\n")
    lines.append("**References**")
    lines.extend(f"* {line}" for line in ref_lines)
    return lines


def make_synonymy(taxa: list[Taxon]) -> str:
    syns = [(taxon, synonymy_for_taxon(taxon)) for taxon in taxa]
    return "".join(line + "\n\n" for line in stringify_synonymies(syns))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("taxa", type=str, nargs="+")
    parser.add_argument("--output", type=str)
    args = parser.parse_args()
    taxa = [
        Taxon.select_valid().filter(Taxon.valid_name == taxon).get()
        for taxon in args.taxa
    ]
    syns = make_synonymy(taxa)
    print(syns)
    if args.output is not None:
        import pypandoc

        pypandoc.convert_text(syns, to="docx", format="md", outputfile=args.output)
