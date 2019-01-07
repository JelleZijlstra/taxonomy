from peewee import CharField, ForeignKeyField

from ..constants import OccurrenceStatus

from .base import BaseModel, EnumField
from .taxon import Taxon
from .location import Location


class Occurrence(BaseModel):
    taxon = ForeignKeyField(Taxon, related_name="occurrences", db_column="taxon_id")
    location = ForeignKeyField(Location, related_name="taxa", db_column="location_id")
    comment = CharField()
    status = EnumField(OccurrenceStatus, default=OccurrenceStatus.valid)
    source = CharField()

    def add_comment(self, new_comment: str) -> None:
        if self.comment is None:
            self.comment = new_comment
        else:
            self.comment += " " + new_comment
        self.save()

    def __repr__(self) -> str:
        out = "{} in {} ({}{})".format(
            self.taxon,
            self.location,
            self.source,
            "; " + self.comment if self.comment else "",
        )
        if self.status != OccurrenceStatus.valid:
            out = "[{}] {}".format(self.status.name.upper(), out)
        return out
