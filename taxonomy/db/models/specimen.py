from __future__ import annotations
from peewee import CharField, ForeignKeyField, IntegerField, TextField
import time
import datetime

from .base import BaseModel
from ... import events, getinput

from .taxon import Taxon
from .region import Region


class Specimen(BaseModel):
    creation_event = events.Event["Specimen"]()
    save_event = events.Event["Specimen"]()
    call_sign = "JZ"
    label_field = "id"

    taxon = ForeignKeyField(Taxon)
    region = ForeignKeyField(Region)
    taxon_text = CharField()
    location_text = CharField()
    date = CharField()
    description = CharField()
    link = CharField()

    class Meta:
        db_table = "specimen"

    @classmethod
    def create_interactively(cls, **kwargs: object) -> Specimen | None:
        taxon_text = Specimen.getter("taxon_text").get_one_key("taxon_text> ")
        if taxon_text is None:
            return None
        taxon = Taxon.getter(None).get_one("taxon> ")
        if taxon is None:
            return None
        location_text = Specimen.getter("location_text").get_one_key("location_text> ")
        if location_text is None:
            return None
        region = Region.getter(None).get_one("region> ")
        if region is None:
            return None
        description = Specimen.getter("description").get_one_key("description> ")
        if description is None:
            return None
        date = Specimen.getter("date").get_one_key("date> ")
        if date is None:
            return None
        link = Specimen.getter("link").get_one_key("link> ")
        return cls.create(
            taxon_text=taxon_text,
            taxon=taxon,
            location_text=location_text,
            region=region,
            date=date,
            description=description,
            link=link,
        )

    def add_comment(self, text: str | None = None) -> SpecimenComment | None:
        return SpecimenComment.create_interactively(specimen=self, text=text)

    def display(self, full: bool = False) -> None:
        super().display(full=full)
        for comment in SpecimenComment.select_valid().filter(
            SpecimenComment.specimen == self
        ):
            print("---")
            print(comment.text)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        return {**callbacks, "add_comment": self.add_comment}


class SpecimenComment(BaseModel):
    call_sign = "JZCO"

    specimen = ForeignKeyField(
        Specimen, related_name="comments", db_column="specimen_id"
    )
    date = IntegerField()
    text = TextField()

    class Meta:
        db_table = "specimen_comment"

    @classmethod
    def make(cls, specimen: Specimen, text: str) -> SpecimenComment:
        return cls.create(specimen=specimen, text=text, date=int(time.time()))

    @classmethod
    def create_interactively(
        cls, specimen: Specimen | None = None, text: str | None = None, **kwargs: object
    ) -> SpecimenComment | None:
        if specimen is None:
            specimen = cls.get_value_for_foreign_key_field_on_class(
                "specimen", allow_none=False
            )
            if specimen is None:
                return None
        if text is None:
            text = getinput.get_line(prompt="text> ")
            if text is None:
                return None
        return cls.make(specimen=specimen, text=text)

    def get_description(self) -> str:
        components = [
            self.kind.name,
            datetime.datetime.fromtimestamp(self.date).strftime("%b %d, %Y %H:%M:%S"),
        ]
        return f'{self.text} ({"; ".join(components)})'
