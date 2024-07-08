"""Entries in a published classification."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Self

from clirm import Field

from taxonomy import events, getinput
from taxonomy.adt import ADT
from taxonomy.db import models
from taxonomy.db.constants import NomenclatureStatus, Rank
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig


class ClassificationEntryTag(ADT):
    CommentClassificationEntry(text=str, tag=1)  # type: ignore[name-defined]


class ClassificationEntry(BaseModel):
    creation_event = events.Event["ClassificationEntry"]()
    save_event = events.Event["ClassificationEntry"]()
    call_sign = "CE"
    label_field = "id"
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
    tags = ADTField[ClassificationEntryTag](is_ordered=False)

    def edit(self) -> None:
        self.fill_field("tags")

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        yield from models.classification_entry.lint.LINT.run(self, cfg)

    @classmethod
    def create_for_article(cls, art: Article, fields: Sequence[Field[Any]]) -> None:
        while True:
            entry = cls.create_one(art, fields)
            if entry is None:
                break
            entry.edit()

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

    def display(self, *, full: bool = False, depth: int = 0) -> None:
        print("  " * depth + str(self))
        for child in self.children:
            child.display(full=full, depth=depth + 4)

    def add_incorrect_subsequent_spelling_for_genus(self) -> None:
        genus_name, *_ = self.name.split()
        print(f"Adding incorrect subsequent spelling for genus {genus_name!r}...")
        target = models.Name.getter(None).get_one("genus> ")
        if target is None:
            return
        nam = target.add_variant(
            genus_name,
            status=NomenclatureStatus.incorrect_subsequent_spelling,
            paper=self.article,
            page_described=self.page,
            original_name=genus_name,
            interactive=False,
        )
        if nam is None:
            return
        nam.original_rank = Rank.genus
        nam.format()
        nam.edit_until_clean()

    def add_incorrect_subsequent_spelling(self) -> None:
        print(f"Adding incorrect subsequent spelling for {self.name!r}...")
        target = models.Name.getter(None).get_one("name> ")
        if target is None:
            return
        nam = target.add_variant(
            self.name.split()[-1],
            status=NomenclatureStatus.incorrect_subsequent_spelling,
            paper=self.article,
            page_described=self.page,
            original_name=self.name,
            interactive=False,
        )
        if nam is None:
            return
        nam.format()
        nam.edit_until_clean()

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "add_incorrect_subsequent_spelling": self.add_incorrect_subsequent_spelling,
            "add_incorrect_subsequent_spelling_for_genus": self.add_incorrect_subsequent_spelling_for_genus,
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
