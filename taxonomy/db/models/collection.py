from typing import Any, List, Optional, Tuple, Type

from peewee import BooleanField, CharField, ForeignKeyField

from ... import events, getinput
from .. import models

from .base import BaseModel, ModelT
from .region import Region


class Collection(BaseModel):
    creation_event = events.Event["Collection"]()
    save_event = events.Event["Collection"]()
    label_field = "label"
    grouping_field = "city"
    call_sign = "C"

    label = CharField()
    name = CharField()
    location = ForeignKeyField(
        Region, related_name="collections", db_column="location_id"
    )
    comment = CharField(null=True)
    city = CharField(null=True)
    removed = BooleanField(default=False)

    def __repr__(self) -> str:
        city = f", {self.city}" if self.city else ""
        return f"{self.name}{city} ({self.label})"

    @classmethod
    def by_label(cls, label: str) -> "Collection":
        colls = list(cls.filter(cls.label == label))
        if len(colls) == 1:
            return colls[0]
        else:
            raise ValueError(f"found {colls} with label {label}")

    @classmethod
    def get_or_create(
        cls, label: str, name: str, location: Region, comment: Optional[str] = None
    ) -> "Collection":
        try:
            return cls.by_label(label)
        except ValueError:
            return cls.create(
                label=label, name=name, location=location, comment=comment
            )

    @classmethod
    def create_interactively(
        cls: Type[ModelT],
        label: Optional[str] = None,
        name: Optional[str] = None,
        location: Optional[Region] = None,
        **kwargs: Any,
    ) -> ModelT:
        if label is None:
            label = getinput.get_line("label> ")
        if name is None:
            name = getinput.get_line("name> ")
        if location is None:
            location = cls.get_value_for_foreign_key_field_on_class("location")
        obj = cls.create(label=label, name=name, location=location)
        obj.fill_required_fields()
        return obj

    def display(
        self, full: bool = True, depth: int = 0, organized: bool = False
    ) -> None:
        city = f", {self.city}" if self.city else ""
        print(" " * depth + f"{self!r}{city}, {self.location}")
        if self.comment:
            print(" " * (depth + 4) + f"Comment: {self.comment}")
        if full:
            if organized:
                models.taxon.display_organized(
                    [
                        (str(f"{nam} (type: {nam.type_specimen})"), nam.taxon)
                        for nam in self.type_specimens
                    ],
                    depth=depth,
                )
            else:
                for nam in sorted(
                    self.type_specimens, key=lambda nam: nam.taxon.valid_name
                ):
                    print(" " * (depth + 4) + f"{nam} (type: {nam.type_specimen})")
                    for tag in nam.type_tags or ():
                        if isinstance(tag, models.name.TypeTag.CollectionDetail):
                            print(" " * (depth + 8) + str(tag))

    def get_partial(
        self, display: bool = False
    ) -> Tuple[List["models.name.Name"], List["models.name.Name"]]:
        multiple = []
        probable_repo = []
        for nam in models.Name.with_tag_of_type(models.name.TypeTag.Repository):
            for tag in nam.get_tags(nam.type_tags, models.name.TypeTag.Repository):
                if tag.repository == self:
                    multiple.append(nam)
                    if display:
                        print(tag)
                        nam.display()
                    break

        for nam in models.Name.with_tag_of_type(models.name.TypeTag.ProbableRepository):
            for tag in nam.get_tags(
                nam.type_tags, models.name.TypeTag.ProbableRepository
            ):
                if tag.repository == self:
                    probable_repo.append(nam)
                    if display:
                        print(tag)
                        nam.display()
                    break
        return multiple, probable_repo

    def merge(self, other: "Collection") -> None:
        for nam in self.type_specimens:
            nam.collection = other
            nam.save()
        self.delete_instance()
