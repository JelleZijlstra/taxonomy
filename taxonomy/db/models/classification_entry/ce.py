"""Entries in a published classification."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, NotRequired, Self

from clirm import Field

from taxonomy import command_set, events, getinput
from taxonomy.adt import ADT
from taxonomy.db import helpers, models
from taxonomy.db.constants import Group, NomenclatureStatus, Rank
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig, TextOrNullField


class ClassificationEntryTag(ADT):
    CommentClassificationEntry(text=str, tag=1)  # type: ignore[name-defined]
    TextualRank(text=str, tag=2)  # type: ignore[name-defined]
    CorrectedName(text=str, tag=3)  # type: ignore[name-defined]
    PageLink(url=str, page=str, tag=4)  # type: ignore[name-defined]
    TypeSpecimenData(text=str, tag=5)  # type: ignore[name-defined]
    OriginalCombination(text=str, tag=6)  # type: ignore[name-defined]
    OriginalPageDescribed(text=str, tag=7)  # type: ignore[name-defined]
    IgnoreLintClassificationEntry(label=str, comment=NotRequired[str], tag=8)  # type: ignore[name-defined]


class ClassificationEntry(BaseModel):
    creation_event = events.Event["ClassificationEntry"]()
    save_event = events.Event["ClassificationEntry"]()
    call_sign = "CE"
    label_field = "name"
    clirm_table_name = "classification_entry"

    article = Field[Article]("article_id", related_name="classification_entries")
    name = Field[str]()
    rank = Field[Rank]()
    parent = Field[Self | None]("parent_id", related_name="children")
    page = Field[str | None]()
    mapped_name = Field["models.Name | None"](
        "mapped_name_id", related_name="classification_entries"
    )
    authority = Field[str | None]()
    year = Field[str | None]()
    citation = Field[str | None]()
    type_locality = Field[str | None]()
    raw_data = TextOrNullField()
    tags = ADTField[ClassificationEntryTag](is_ordered=False)

    def edit(self) -> None:
        self.fill_field("tags")

    def get_corrected_name(self) -> str | None:
        for tag in self.get_tags(self.tags, ClassificationEntryTag.CorrectedName):
            return tag.text
        if corrected_name := self.get_corrected_name_without_tags():
            return corrected_name
        return None

    def get_corrected_name_without_tags(self) -> str | None:
        group = self.get_group()
        if group is Group.family:
            return self.name.replace("æ", "ae")
        name = self.name
        if self.rank is Rank.synonym:
            if self.name.isascii() and re.fullmatch(r"[A-Za-z\-]+", self.name):
                return self.name.replace("-", "")
            if "[" in name:
                name = re.sub(r"\[(?!sic)([^\]]+)\]", r"\1", name)
        corrected_name = models.name.name.infer_corrected_original_name(name, group)
        if corrected_name is not None:
            return corrected_name
        return None

    def get_group(self) -> Group:
        if self.rank is Rank.synonym:
            if self.parent is not None:
                return self.parent.get_group()
        return helpers.group_of_rank(self.rank)

    def is_synonym_without_full_name(self) -> bool:
        if self.rank is not Rank.synonym:
            return False
        if self.get_group() is not Group.species:
            return False
        name = self.get_corrected_name()
        if name is None:
            return True
        return " " not in name

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        yield from models.classification_entry.lint.LINT.run(self, cfg)

    def has_tag(self, tag_cls: ClassificationEntry._Constructors) -> bool:  # type: ignore[name-defined]
        tag_id = tag_cls._tag
        return any(tag[0] == tag_id for tag in self.get_raw_tags_field("tags"))

    def add_tag(self, tag: ClassificationEntryTag) -> None:
        self.tags = (*self.tags, tag)  # type: ignore[assignment]

    @classmethod
    def create_for_article(
        cls, art: Article, fields: Sequence[Field[Any]], *, format_each: bool = False
    ) -> list[ClassificationEntry]:
        entries: list[ClassificationEntry] = []
        next_page = ""
        next_name = ""
        while True:
            entry = cls.create_one(
                art, fields, defaults={"page": next_page, "name": next_name}
            )
            if entry is None:
                break
            if format_each:
                entry.format()
            entry.edit()
            if entry.page is not None:
                next_page = entry.page
            if entry.rank is Rank.genus:
                next_name = entry.name
            elif entry.rank > Rank.genus:
                next_name = ""
            entries.append(entry)
        return entries

    @classmethod
    def create_one(
        cls,
        art: Article,
        fields: Sequence[Field[Any]],
        defaults: Mapping[str, str] = MappingProxyType({}),
    ) -> ClassificationEntry | None:
        fields = [
            ClassificationEntry.name,
            ClassificationEntry.rank,
            ClassificationEntry.parent,
            ClassificationEntry.page,
            *fields,
        ]
        values: dict[str, Any] = {"article": art}
        ce_name: str | None = None
        for field in fields:
            name = field.name.removesuffix("_id")
            if name == "rank" and ce_name is not None:
                if (rank := _infer_rank_from_name(ce_name)) is not None:
                    print(f"Inferred rank as {rank.name}.")
                    values[name] = rank
                    continue
            if name == "parent":
                if ce_name is not None:
                    if values["rank"] == Rank.species:
                        genus_name = ce_name.split()[0]
                        if parent_ce := _get_genus_ce(genus_name, art):
                            print(f"Inferred parent as genus {parent_ce}.")
                            values["parent"] = parent_ce
                            continue
                    elif values["rank"] == Rank.subspecies:
                        species_name = " ".join(ce_name.split()[:2])
                        if parent_ce := _get_species_ce(species_name, art):
                            print(f"Inferred parent as species {parent_ce}.")
                            values["parent"] = parent_ce
                            continue
                parent = cls.get_parent_completion(art)
                values["parent"] = parent
            else:
                try:
                    value = cls.get_value_for_field_on_class(
                        name, default=defaults.get(name, "")
                    )
                    if value is None and not field.allow_none:
                        return None
                    values[name] = value
                    if field.name == "name":
                        ce_name = value
                except getinput.StopException:
                    return None
        return cls.create(**values)

    def __str__(self) -> str:
        parts = [f"{self.name} ({self.rank.name})"]
        if self.authority is not None:
            parts.append(f" {self.authority}")
            if self.year is not None:
                parts.append(f", {self.year}")
        parts.append(f" ({self.article}")
        if self.page is not None:
            parts.append(f": {self.page}")
        parts.append(")")
        parts.append(f" (#{self.id})")
        return "".join(parts)

    def __repr__(self) -> str:
        base = str(self)
        if self.mapped_name is not None:
            return f"{base} -> {self.mapped_name}"
        return base

    def display(
        self, *, full: bool = False, depth: int = 0, max_depth: int = 2
    ) -> None:
        print("  " * depth + str(self))
        if not full:
            max_depth -= 1
        if max_depth <= 0:
            return
        for child in self.children:
            child.display(full=full, depth=depth + 4, max_depth=max_depth)

    def add_incorrect_subsequent_spelling_for_genus(self) -> models.Name | None:
        name = self.get_corrected_name()
        if name is None:
            return None
        genus_name, *_ = name.split()
        print(f"Adding incorrect subsequent spelling for genus {genus_name!r}...")
        target = models.Name.getter(None).get_one("genus> ")
        if target is None:
            return None
        nam = target.add_variant(
            genus_name,
            status=NomenclatureStatus.incorrect_subsequent_spelling,
            paper=self.article,
            page_described=self.page,
            original_name=genus_name,
            interactive=False,
        )
        if nam is None:
            return None
        nam.original_rank = Rank.genus
        nam.format()
        nam.edit_until_clean()
        return nam

    def add_incorrect_subsequent_spelling(
        self, target: models.Name | None = None
    ) -> models.Name | None:
        print(f"Adding incorrect subsequent spelling for {self.name!r}...")
        name = self.get_corrected_name()
        if name is None:
            return None
        if target is None:
            target = models.Name.getter(None).get_one("name> ")
        if target is None:
            return None
        nam = target.add_variant(
            name.split()[-1],
            status=NomenclatureStatus.incorrect_subsequent_spelling,
            paper=self.article,
            page_described=self.page,
            original_name=self.name,
            interactive=False,
        )
        if nam is None:
            return None
        nam.original_rank = self.rank
        nam.corrected_original_name = name
        self.mapped_name = nam
        nam.format()
        nam.edit_until_clean()
        return nam

    def add_family_group_synonym(
        self, type: models.Name | None = None
    ) -> models.Name | None:
        print(f"Adding family-group synonym for {self.name!r}...")
        if type is None:
            type = models.Name.getter(None).get_one("type> ")
        if type is None:
            return None
        stem = type.get_stem()
        if stem is None:
            stem = getinput.get_line("stem> ")
        taxon = type.taxon.parent_of_rank(Rank.genus).parent
        if taxon is None:
            print("No taxon found.")
            return None
        nam = taxon.add_syn(
            root_name=stem,
            year=self.article.year,
            original_name=self.name,
            original_citation=self.article,
            page_described=self.page,
            interactive=False,
        )
        if nam is None:
            return None
        nam.author_tags = self.article.author_tags
        self.mapped_name = nam
        nam.format()
        nam.edit_until_clean()
        return nam

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "add_incorrect_subsequent_spelling": self.add_incorrect_subsequent_spelling,
            "add_incorrect_subsequent_spelling_for_genus": self.add_incorrect_subsequent_spelling_for_genus,
            "add_family_group_synonym": self.add_family_group_synonym,
        }

    @classmethod
    def get_parent_completion(
        cls, art: Article, callbacks: getinput.CallbackMap = {}
    ) -> Self | None:
        siblings = list(cls.select_valid().filter(cls.article == art))
        return getinput.choose_one_by_name(
            siblings,
            callbacks=callbacks,
            message="parent> ",
            print_choices=False,
            history_key=("parent", art.id),
        )

    def should_exempt_from_string_cleaning(self, field: str) -> bool:
        return field == "raw_data"

    def get_rank_string(self) -> str:
        if self.rank is Rank.other:
            for tag in self.tags:
                if isinstance(tag, ClassificationEntryTag.TextualRank):
                    return tag.text
        return self.rank.name

    def get_children_of_rank(self, rank: Rank) -> Iterable[ClassificationEntry]:
        for child in self.children:
            if child.rank is rank:
                yield child
            yield from child.get_children_of_rank(rank)

    def parent_of_rank(self, rank: Rank) -> ClassificationEntry | None:
        if self.rank is rank:
            return self
        if self.parent is None:
            return None
        return self.parent.parent_of_rank(rank)

    def format(
        self,
        *,
        quiet: bool = False,
        autofix: bool = True,
        interactive: bool = True,
        verbose: bool = False,
        manual_mode: bool = False,
    ) -> bool:
        result = super().format(
            quiet=quiet,
            autofix=autofix,
            interactive=interactive,
            verbose=verbose,
            manual_mode=manual_mode,
        )
        if self.mapped_name is not None:
            self.mapped_name.format(
                quiet=quiet,
                autofix=autofix,
                interactive=interactive,
                verbose=verbose,
                manual_mode=manual_mode,
            )
            return super().format(
                quiet=quiet,
                autofix=autofix,
                interactive=interactive,
                verbose=verbose,
                manual_mode=manual_mode,
            )
        return result


def _infer_rank_from_name(ce_name: str) -> Rank | None:
    if re.fullmatch(r"[A-ZÆ][a-zæüö]+ [a-zæüö]+", ce_name):
        return Rank.species
    elif re.fullmatch(r"[A-ZÆ][a-zæüö]+ [a-zæüö]+ [a-zæüö]+", ce_name):
        return Rank.subspecies
    if " " not in ce_name:
        if ce_name.endswith("idae"):
            return Rank.family
        elif ce_name.endswith("inae"):
            return Rank.subfamily
        elif ce_name.endswith("ini"):
            return Rank.tribe
    return None


def _get_genus_ce(genus: str, art: Article) -> ClassificationEntry | None:
    ces = list(
        ClassificationEntry.select_valid().filter(
            ClassificationEntry.article == art,
            ClassificationEntry.rank == Rank.genus,
            ClassificationEntry.name == genus,
        )
    )
    if len(ces) != 1:
        return None
    ce = ces[0]
    # If there are subgenera, they should probably be the parent instead
    if any(child.rank is Rank.subgenus for child in ce.children):
        return None
    return ce


def _get_species_ce(species: str, art: Article) -> ClassificationEntry | None:
    ces = list(
        ClassificationEntry.select_valid().filter(
            ClassificationEntry.article == art,
            ClassificationEntry.rank == Rank.species,
            ClassificationEntry.name == species,
        )
    )
    if len(ces) != 1:
        return None
    return ces[0]


CS = command_set.CommandSet("ce", "Commands related to classification entries.")


@CS.register
def classification_entries_for_article() -> None:
    art = Article.getter(None).get_one("article> ")
    if art is None:
        return
    extra_fields: list[Field[Any]] = []
    for field in (
        ClassificationEntry.authority,
        ClassificationEntry.year,
        ClassificationEntry.citation,
        ClassificationEntry.type_locality,
    ):
        if getinput.yes_no(f"Add {field.name}?"):
            extra_fields.append(field)
    format_each = getinput.yes_no("Format each entry?")
    entries = ClassificationEntry.create_for_article(
        art, extra_fields, format_each=format_each
    )
    for entry in entries:
        entry.format()
        entry.edit_until_clean()
