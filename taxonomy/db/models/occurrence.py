from clirm import Field

from ..constants import OccurrenceStatus
from .article import Article
from .base import BaseModel
from .location import Location
from .taxon import Taxon


class Occurrence(BaseModel):
    taxon = Field[Taxon]("taxon_id", related_name="occurrences")
    location = Field[Location]("location_id", related_name="taxa")
    comment = Field[str]()
    status = Field[OccurrenceStatus](default=OccurrenceStatus.valid)
    source = Field[Article | None]("source_id", related_name="occurrences")
    call_sign = "O"
    clirm_table_name = "occurrence"
    # The taxon field can become invalid when a taxon is merged into another. Allowing this
    # is not ideal because we can also get an Occurrence from a location. Alternatively,
    # we could reassign the occurrence to the other taxon, but that may cause duplicate
    # occurrences.
    fields_may_be_invalid = {"taxon"}

    def add_comment(self, new_comment: str) -> None:
        if self.comment is None:
            self.comment = new_comment
        else:
            self.comment += " " + new_comment

    def __repr__(self) -> str:
        out = "{} in {} ({}{})".format(
            self.taxon,
            self.location,
            self.source.name if self.source else "no source",
            "; " + self.comment if self.comment else "",
        )
        if self.status != OccurrenceStatus.valid:
            out = f"[{self.status.name.upper()}] {out}"
        return out
