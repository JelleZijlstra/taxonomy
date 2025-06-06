"""Entries in a published classification."""

from __future__ import annotations

import datetime
import enum
import re
from collections.abc import Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, ClassVar, NotRequired, Self

from clirm import DoesNotExist, Field, Query

from taxonomy import command_set, events, getinput
from taxonomy.adt import ADT
from taxonomy.db import helpers, models
from taxonomy.db.constants import (
    URL,
    AgeClass,
    Group,
    Managed,
    Markdown,
    NomenclatureStatus,
    Rank,
    SourceLanguage,
)
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig, TextOrNullField


class ClassificationEntryStatus(enum.Enum):
    valid = 0
    removed = 6
    redirect = 7


class ClassificationEntry(BaseModel):
    creation_event = events.Event["ClassificationEntry"]()
    save_event = events.Event["ClassificationEntry"]()
    call_sign = "CE"
    label_field = "name"
    clirm_table_name = "classification_entry"
    fields_without_completers: ClassVar[set[str]] = {"raw_data"}

    article = Field[Article]("article_id", related_name="classification_entries")
    name = Field[str]()
    rank = Field[Rank]()
    status = Field[ClassificationEntryStatus](default=ClassificationEntryStatus.valid)
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
    tags = ADTField["ClassificationEntryTag"](is_ordered=False)

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(
            ClassificationEntry.status != ClassificationEntryStatus.removed,
            ClassificationEntry.status != ClassificationEntryStatus.redirect,
        )

    def get_redirect_target(self) -> ClassificationEntry | None:
        if self.status is not ClassificationEntryStatus.redirect:
            return None
        return self.parent

    def is_invalid(self) -> bool:
        return self.status in (
            ClassificationEntryStatus.removed,
            ClassificationEntryStatus.redirect,
        )

    def should_skip(self) -> bool:
        return self.status in (
            ClassificationEntryStatus.removed,
            ClassificationEntryStatus.redirect,
        )

    def edit(self) -> None:
        self.fill_field("tags")

    def merge(self, other: ClassificationEntry | None = None) -> None:
        if other is None:
            other = ClassificationEntry.getter(None).get_one("target> ")
            if other is None:
                return
        if self == other:
            print("Cannot merge with self.")
            return
        self.parent = other
        self.status = ClassificationEntryStatus.redirect

    def numeric_page(self) -> int:
        return helpers.to_int(self.page)

    def set_page(self, page: str | None = None) -> None:
        existing = self.page
        if page is None:
            page = self.getter("page").get_one_key("page> ", default=existing or "")
            if page is None:
                return
        if existing is not None:

            def adjust_tag(tag: ADT) -> ADT:
                if (
                    isinstance(tag, ClassificationEntryTag.PageLink)
                    and tag.page == existing
                ):
                    print(f"adjust page in {tag}")
                    return ClassificationEntryTag.PageLink(url=tag.url, page=page)
                return tag

            self.map_tags_field(ClassificationEntry.tags, adjust_tag)
        self.page = page
        print(f"{self}: set page {existing} -> {page}")

    def get_corrected_name(self) -> str | None:
        for tag in self.get_tags(self.tags, ClassificationEntryTag.CorrectedName):
            return tag.text
        if corrected_name := self.get_corrected_name_without_tags():
            return corrected_name
        return None

    def get_corrected_name_without_tags(self) -> str | None:
        group = self.get_group()
        if group is Group.family:
            return self.name.replace("æ", "ae").replace("œ", "oe")
        name = self.name
        if self.rank.is_synonym:
            if self.name.isascii() and re.fullmatch(r"[A-Za-z\-]+", self.name):
                return self.name.replace("-", "")
            if "[" in name:
                name = re.sub(r"\[(?!sic)([^\]]+)\]", r"\1", name)
        elif self.rank is Rank.division and re.match(r"[A-Z][a-z]+ Division", name):
            return name
        corrected_name = models.name.name.infer_corrected_original_name(name, group)
        if corrected_name is not None:
            return corrected_name
        return None

    def get_group(self) -> Group:
        if self.rank is Rank.synonym:
            if self.parent is not None:
                return self.parent.get_group()
        return helpers.group_of_rank(self.rank)

    def get_date_object(self) -> datetime.date:
        return self.article.get_date_object()

    def get_name_to_use_as_normalized_original_name(
        self, *, enhanced: bool = False
    ) -> str | None:
        if self.is_synonym_without_full_name():
            root = self.get_corrected_name()
            parent = self.parent_of_rank(Rank.genus)
            if parent is None:
                return None
            if enhanced:
                return f"[{parent.name}] {root}"
            return f"{parent.name} {root}"
        else:
            return self.get_corrected_name()

    def is_synonym_without_full_name(self) -> bool:
        if self.rank is not Rank.synonym_species:
            return False
        name = self.get_corrected_name()
        if name is None:
            return True
        return " " not in name

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        yield from models.classification_entry.lint.LINT.run(self, cfg)

    @classmethod
    def clear_lint_caches(cls) -> None:
        models.classification_entry.lint.LINT.clear_caches()

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
        _parent_stack: list[ClassificationEntry] = []
        while True:
            entry = cls.create_one(
                art,
                fields,
                defaults={"page": next_page, "name": next_name},
                _parent_stack=_parent_stack,
            )
            if entry is None:
                break
            if format_each:
                entry.format(format_mapped=False)
            entry.edit()
            if format_each:
                entry.format(format_mapped=True)
            if entry.page is not None:
                next_page = entry.page
            if entry.rank is Rank.genus:
                next_name = entry.name
            elif entry.rank in (Rank.species, Rank.subspecies):
                next_name = entry.name.split()[0]
            elif entry.rank > Rank.genus:
                genus = entry.parent_of_rank(Rank.genus)
                if genus is not None:
                    next_name = genus.name
                else:
                    next_name = ""
            entries.append(entry)
            if (
                not entry.rank.is_synonym
                and helpers.group_of_rank(entry.rank) is not Group.species
            ):
                _parent_stack.append(entry)
        return entries

    @classmethod
    def create_one(
        cls,
        art: Article,
        fields: Sequence[Field[Any]],
        defaults: Mapping[str, str] = MappingProxyType({}),
        _parent_stack: list[ClassificationEntry] = [],
        prefilled_values: Mapping[str, Any] = MappingProxyType({}),
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
            if name in prefilled_values:
                values[name] = prefilled_values[name]
                continue
            if name == "rank" and ce_name is not None:
                if (rank := _infer_rank_from_name(ce_name)) is not None:
                    print(f"Inferred rank as {rank.name}.")
                    values[name] = rank
                    continue
            if name == "parent":
                while _parent_stack and _parent_stack[-1].rank <= values["rank"]:
                    _parent_stack.pop()
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
                parent: ClassificationEntry | None = None
                if _parent_stack:
                    print(f"Parent is {_parent_stack[-1]}")
                    parent = _parent_stack[-1]
                else:
                    parent = cls.get_parent_completion(art)
                values["parent"] = parent
            else:
                try:
                    value = cls.get_value_for_field_on_class(
                        name,
                        default=defaults.get(name, ""),
                        callbacks={":clear": _parent_stack.clear},
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
        try:
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
        except DoesNotExist:
            return f"<invalid CE #{self.id}>"

    def __repr__(self) -> str:
        base = str(self)
        if self.mapped_name is not None:
            return f"{base} -> {self.mapped_name}"
        return base

    def display(
        self,
        *,
        full: bool = False,
        depth: int = 0,
        max_depth: int = 2,
        show_parent: bool = True,
    ) -> None:
        line = str(self)
        if show_parent and self.parent is not None:
            line += f" (parent: {self.parent})"
        print("  " * depth + line)
        if not full:
            max_depth -= 1
        if max_depth <= 0:
            return
        for child in sorted(self.get_children(), key=lambda ce: ce.numeric_page()):
            child.display(
                full=full, depth=depth + 4, max_depth=max_depth, show_parent=False
            )

    def get_children(self) -> Query[Self]:
        return self.add_validity_check(self.children)

    def ensure_page_set(self) -> None:
        if self.page is None:
            self.display()
            self.page = getinput.get_line("page> ", callbacks=self.get_adt_callbacks())

    def add_incorrect_subsequent_spelling_for_genus(self) -> models.Name | None:
        name = self.get_corrected_name()
        if name is None:
            return None
        genus_name, *_ = name.split()
        print(f"Adding incorrect subsequent spelling for genus {genus_name!r}...")
        target = models.Name.getter(None).get_one("genus> ")
        if target is None:
            return None
        self.ensure_page_set()
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
        self.ensure_page_set()
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
        nam.corrected_original_name = self.get_name_to_use_as_normalized_original_name()
        self.mapped_name = nam
        nam.format()
        nam.edit_until_clean()
        return nam

    def syn_from_paper(self) -> models.Name | None:
        print(f"Adding synonym for {self.name!r}...")
        taxon = models.Taxon.getter(None).get_one("taxon> ")
        if taxon is None:
            return None
        self.ensure_page_set()
        group = self.get_group()
        corrected_name = self.get_corrected_name()
        if corrected_name is None:
            root_name = None
        elif group is Group.species:
            root_name = corrected_name.split()[-1]
        else:
            root_name = corrected_name
        nam = taxon.syn_from_paper(
            paper=self.article,
            page_described=self.page,
            group=group,
            original_name=self.name,
            corrected_original_name=self.get_name_to_use_as_normalized_original_name(),
            original_rank=self.rank,
            root_name=root_name,
        )
        if nam is None:
            return None
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
        self.ensure_page_set()
        corrected_name = self.get_corrected_name()
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
            type=type,
            corrected_original_name=corrected_name,
        )
        if nam is None:
            return None
        nam.author_tags = self.article.author_tags
        self.mapped_name = nam
        nam.format()
        nam.edit_until_clean()
        return nam

    def add_syns(self) -> None:
        defaults = {"page": self.page or ""}
        prefilled_values = {
            "rank": helpers.GROUP_TO_SYNONYM_RANK[self.get_group()],
            "parent": self,
        }
        while True:
            entry = self.create_one(
                self.article, [], defaults=defaults, prefilled_values=prefilled_values
            )
            if entry is None:
                break
            defaults["page"] = entry.page or ""
            entry.format(format_mapped=False)
            entry.edit()
            entry.format()

    def add_taxon(self) -> None:
        taxon = models.Taxon.getter(None).get_one("taxon> ")
        if taxon is None:
            return
        self.ensure_page_set()
        taxon.from_paper(
            self.rank,
            name=self.name,
            paper=self.article,
            page_described=self.page,
            original_rank=self.rank,
        )

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        article_callbacks = self.article.get_shareable_adt_callbacks()
        return {
            **super().get_adt_callbacks(),
            **article_callbacks,
            "add_incorrect_subsequent_spelling": self.add_incorrect_subsequent_spelling,
            "add_family_group_synonym": self.add_family_group_synonym,
            "syn_from_paper": self.syn_from_paper,
            "edit_parent": self.edit_parent,
            "take_over_mapped_name": self.take_over_mapped_name,
            "add_syns": self.add_syns,
            "add_taxon": self.add_taxon,
            "set_page": self.set_page,
            "merge": self.merge,
            "kerr_subgeneric": self._kerr_subgeneric,
        }

    def _kerr_subgeneric(self) -> None:
        words = self.name.split()
        if len(words) == 3:
            self.add_tag(
                ClassificationEntryTag.CorrectedName(f"{words[0]} {words[2].lower()}")
            )
        else:
            print("Cannot add Kerr subgeneric tag to this name.")

    def take_over_mapped_name(self) -> None:
        if self.mapped_name is None:
            print("No mapped name.")
            return
        self.mapped_name.display()
        mapped_ce = self.mapped_name.get_mapped_classification_entry()
        if mapped_ce is not None:
            mapped_ce.display()
        else:
            print("No mapped classification entry.")
        if not getinput.yes_no("Take over mapped name?"):
            return
        models.name.lint.take_over_name(
            self.mapped_name, self, LintConfig(interactive=True)
        )

    @classmethod
    def get_parent_completion(
        cls,
        art: Article,
        *,
        callbacks: getinput.CallbackMap = {},
        default: Self | None = None,
        prompt: str = "parent> ",
    ) -> Self | None:
        siblings = list(cls.select_valid().filter(cls.article == art))
        return getinput.choose_one_by_name(
            siblings,
            callbacks=callbacks,
            message=prompt,
            print_choices=False,
            history_key=("parent", art.id),
            default=default,
            decode_fn=cls.get_from_key,
        )

    def edit_parent(self) -> None:
        print(f"Current parent: {self.parent}")
        parent = self.get_parent_completion(
            self.article, callbacks=self.get_adt_callbacks()
        )
        if parent is not None:
            self.parent = parent

    def should_exempt_from_string_cleaning(self, field: str) -> bool:
        return field == "raw_data"

    def get_rank_string(self) -> str:
        if self.rank.needs_textual_rank:
            for tag in self.tags:
                if isinstance(tag, ClassificationEntryTag.TextualRank):
                    return tag.text
        return self.rank.display_name

    def get_children_of_rank(self, rank: Rank) -> Iterable[ClassificationEntry]:
        for child in self.get_children():
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
        format_mapped: bool = True,
    ) -> bool:
        result = super().format(
            quiet=quiet,
            autofix=autofix,
            interactive=interactive,
            verbose=verbose,
            manual_mode=manual_mode,
        )
        if format_mapped and self.mapped_name is not None:
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


class ClassificationEntryTag(ADT):
    CommentFromSource(text=Markdown, tag=1)  # type: ignore[name-defined]
    TextualRank(text=Managed, tag=2)  # type: ignore[name-defined]
    CorrectedName(text=Managed, tag=3)  # type: ignore[name-defined]
    PageLink(url=URL, page=Managed, tag=4)  # type: ignore[name-defined]
    TypeSpecimenData(text=Markdown, tag=5)  # type: ignore[name-defined]
    OriginalCombination(text=Managed, tag=6)  # type: ignore[name-defined]
    OriginalPageDescribed(text=Managed, tag=7)  # type: ignore[name-defined]
    IgnoreLintClassificationEntry(label=Managed, comment=NotRequired[Markdown], tag=8)  # type: ignore[name-defined]
    AgeClassCE(age=AgeClass, tag=9)  # type: ignore[name-defined]
    CommonName(name=Managed, language=SourceLanguage, tag=10)  # type: ignore[name-defined]
    # Indicates we should not look for a mapped name
    Informal(tag=11)  # type: ignore[name-defined]
    CommentFromDatabase(text=Markdown, tag=12)  # type: ignore[name-defined]
    # Should not be included in species counts
    TreatedAsDubious(tag=13)  # type: ignore[name-defined]
    CECondition(tag=14, status=NomenclatureStatus, comment=NotRequired[Markdown])  # type: ignore[name-defined]

    # Structured data from the source
    StructuredData(label=Managed, text=Markdown, tag=15)  # type: ignore[name-defined]

    ReferencedUsage(ce=ClassificationEntry, comment=NotRequired[Markdown], tag=16)  # type: ignore[name-defined]
    LSIDCE(text=Managed, tag=17)  # type: ignore[name-defined]


_NAME_CHARS = r"[a-zæüöïœ]+"


def _infer_rank_from_name(ce_name: str) -> Rank | None:
    if " " in ce_name and ce_name[0].islower():
        ce_name = ce_name[0].upper() + ce_name[1:]
    ce_name = models.name.name.clean_original_name(ce_name)
    if re.fullmatch(rf"[A-ZÆ]{_NAME_CHARS} [A-Z]?{_NAME_CHARS}", ce_name):
        return Rank.species
    elif re.fullmatch(rf"[A-ZÆ]{_NAME_CHARS} {_NAME_CHARS} {_NAME_CHARS}", ce_name):
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
    if any(child.rank is Rank.subgenus for child in ce.get_children()):
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
def classification_entries_for_article(art: Article | None = None) -> None:
    if art is None:
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
    create_for_article(art, extra_fields=extra_fields, format_each=format_each)


def create_for_article(
    art: Article, *, extra_fields: Sequence[Field[Any]] = [], format_each: bool = True
) -> None:
    art.display_classification_entries()
    entries = ClassificationEntry.create_for_article(
        art, extra_fields, format_each=format_each
    )
    for entry in entries:
        entry.format()
        entry.edit_until_clean()
