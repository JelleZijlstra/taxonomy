"""Entries in a published classification."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Self

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
            return self.name
        corrected_name = models.name.name.infer_corrected_original_name(
            self.name, group
        )
        if corrected_name is not None:
            return corrected_name
        return None

    def get_group(self) -> Group:
        return helpers.group_of_rank(self.rank)

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
        while True:
            entry = cls.create_one(art, fields)
            if entry is None:
                break
            if format_each:
                entry.format()
            entry.edit()
            entries.append(entry)
        return entries

    @classmethod
    def create_one(
        cls, art: Article, fields: Sequence[Field[Any]]
    ) -> ClassificationEntry | None:
        fields = [
            ClassificationEntry.name,
            ClassificationEntry.rank,
            ClassificationEntry.parent,
            ClassificationEntry.page,
            *fields,
        ]
        values: dict[str, Any] = {"article": art}
        for field in fields:
            name = field.name.removesuffix("_id")
            if name == "parent":
                parent = cls.get_parent_completion(art)
                values["parent"] = parent
            else:
                try:
                    value = cls.get_value_for_field_on_class(name)
                    if value is None and not field.allow_none:
                        return None
                    values[name] = value
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
        nam.corrected_original_name = name
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
