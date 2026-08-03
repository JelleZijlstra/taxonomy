import enum
import subprocess
from collections.abc import Iterable
from typing import Any, ClassVar, NotRequired

from clirm import Field

from taxonomy import events, getinput
from taxonomy.adt import ADT
from taxonomy.db import coordinate_lint, models
from taxonomy.db.constants import (
    AltitudeUnit,
    DistributionOrigin,
    DistributionPresence,
    Managed,
    Markdown,
    OccurrenceBasis,
    OccurrenceValidity,
)
from taxonomy.db.constants import ObservationKind as ObservationKindEnum
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig, TextOrNullField
from taxonomy.db.models.classification_entry import ClassificationEntry
from taxonomy.db.models.collection import Collection
from taxonomy.db.models.location import Location
from taxonomy.db.models.taxon import Taxon


class OccurrenceRecordStatus(enum.IntEnum):
    valid = 0
    deleted = 1
    alias = 2


class OccurrenceRecord(BaseModel):
    creation_event = events.Event["OccurrenceRecord"]()
    save_event = events.Event["OccurrenceRecord"]()
    call_sign = "OR"
    label_field = "locality_text"
    call_sign_field = None
    clirm_table_name = "occurrence_record"
    fields_without_completers: ClassVar[set[str]] = {"raw_data"}

    classification_entry = Field[ClassificationEntry](
        "classification_entry_id", related_name="occurrence_records"
    )
    locality_text = Field[str]()
    page = Field[str | None]()
    basis = Field[OccurrenceBasis]()
    raw_data = TextOrNullField()
    taxon = Field[Taxon | None]("taxon_id", related_name="occurrence_records")
    location = Field[Location | None]("location_id", related_name="occurrence_records")
    tags = ADTField["OccurrenceRecordTag"](is_ordered=False)
    status = Field[OccurrenceRecordStatus](default=OccurrenceRecordStatus.valid)

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(OccurrenceRecord.status == OccurrenceRecordStatus.valid)

    def is_invalid(self) -> bool:
        return self.status is not OccurrenceRecordStatus.valid

    def should_skip(self) -> bool:
        return self.status is not OccurrenceRecordStatus.valid

    def __repr__(self) -> str:
        taxon = self.taxon or self.classification_entry.name
        location = self.location or self.locality_text
        return f"{taxon} at {location} ({self.basis.name}; {self.classification_entry})"

    def __str__(self) -> str:
        return self.__repr__()

    def get_page(self) -> str | None:
        return self.page or self.classification_entry.page

    def edit(self) -> None:
        self.fill_field("tags")

    def has_tag(self, tag_cls: OccurrenceRecord._Constructors) -> bool:  # type: ignore[name-defined]
        tag_id = tag_cls._tag
        return any(tag[0] == tag_id for tag in self.get_raw_tags_field("tags"))

    def add_tag(self, tag: OccurrenceRecordTag) -> None:
        self.tags = (*self.tags, tag)  # type: ignore[assignment]

    def remove_tags(self, tag_cls: type[ADT]) -> None:
        self.tags = tuple(tag for tag in self.tags if not isinstance(tag, tag_cls))  # type: ignore[assignment]

    def get_canonical_record(self) -> OccurrenceRecord:
        split_tags = list(
            self.get_tags(self.tags, OccurrenceRecordTag.TaxonomicSplitFrom)
        )
        if split_tags:
            return split_tags[0].record
        return self

    def split_for_taxon(self, taxon: Taxon | None = None) -> OccurrenceRecord | None:
        if taxon is None:
            taxon = Taxon.getter(None).get_one("taxon> ")
        if taxon is None:
            return None
        canonical = self.get_canonical_record()
        new_tags = (
            *(
                tag
                for tag in canonical.tags
                if not isinstance(tag, OccurrenceRecordTag.TaxonomicSplitFrom)
            ),
            OccurrenceRecordTag.TaxonomicSplitFrom(canonical),
        )
        return OccurrenceRecord.create(
            classification_entry=canonical.classification_entry,
            locality_text=canonical.locality_text,
            page=canonical.page,
            basis=canonical.basis,
            raw_data=canonical.raw_data,
            taxon=taxon,
            location=canonical.location,
            tags=new_tags,
        )

    def open_coordinates(self) -> None:
        has_source_coordinates = False
        for tag in self.get_tags(self.tags, OccurrenceRecordTag.Coordinates):
            extent = coordinate_lint.make_extent(tag.latitude, tag.longitude)
            if extent is not None:
                has_source_coordinates = True
                subprocess.check_call(["open", *extent.openstreetmap_urls])
        if not has_source_coordinates and self.location is not None:
            self.location.open_coordinates()

    def display_location(self) -> None:
        if self.location is None:
            print("No mapped Location")
        else:
            self.location.display()

    def edit_classification_entry(self) -> None:
        self.classification_entry.display()
        self.classification_entry.edit()

    @classmethod
    def create_interactively(
        cls, classification_entry: ClassificationEntry | None = None, **kwargs: Any
    ) -> OccurrenceRecord | None:
        if classification_entry is None:
            classification_entry = ClassificationEntry.getter(None).get_one(
                "classification entry> "
            )
        if classification_entry is None:
            return None
        locality_text = getinput.get_line("locality from source> ")
        if not locality_text:
            return None
        page = getinput.get_line("page> ", default=classification_entry.page or "")
        basis = getinput.get_enum_member(OccurrenceBasis, prompt="basis> ")
        if basis is None:
            return None
        record = cls.create(
            classification_entry=classification_entry,
            locality_text=locality_text,
            page=page or None,
            basis=basis,
            taxon=None,
            location=None,
            raw_data=None,
            tags=(),
            **kwargs,
        )
        record.format(quiet=True)
        return record

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        article_callbacks = (
            self.classification_entry.article.get_shareable_adt_callbacks()
        )
        return {
            **super().get_adt_callbacks(),
            **article_callbacks,
            "display_location": self.display_location,
            "edit_classification_entry": self.edit_classification_entry,
            "open_coordinates": self.open_coordinates,
            "split_for_taxon": self.split_for_taxon,
        }

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        yield from models.occurrence_record.lint.LINT.run(self, cfg)

    @classmethod
    def clear_lint_caches(cls) -> None:
        models.occurrence_record.lint.LINT.clear_caches()


class OccurrenceRecordTag(ADT):
    ObservationKind(kind=ObservationKindEnum, tag=1)  # type: ignore[name-defined]
    MolecularData(tag=2)  # type: ignore[name-defined]
    SpecimenDetail(text=Markdown, tag=3)  # type: ignore[name-defined]
    CommentFromSource(text=Markdown, tag=4)  # type: ignore[name-defined]

    CommentFromDatabase(text=Markdown, tag=11)  # type: ignore[name-defined]
    TaxonomicSplitFrom(record=OccurrenceRecord, tag=12)  # type: ignore[name-defined]
    LocationHint(name=Managed, tag=13)  # type: ignore[name-defined]

    # Verbatim tags preserve source text. Their normalized counterparts are also
    # source-derived, but are stored in a form that can be queried and compared with
    # the mapped Location. Lints infer normalized tags where possible.
    VerbatimCoordinates(text=Managed, tag=16)  # type: ignore[name-defined]
    Coordinates(latitude=Managed, longitude=Managed, tag=17)  # type: ignore[name-defined]
    VerbatimElevation(text=Managed, tag=18)  # type: ignore[name-defined]
    Elevation(elevation=Managed, unit=AltitudeUnit, tag=19)  # type: ignore[name-defined]
    CoordinateUncertaintyFromSource(text=Managed, tag=20)  # type: ignore[name-defined]
    VerbatimDate(text=Managed, tag=21)  # type: ignore[name-defined]
    Date(date=Managed, tag=22)  # type: ignore[name-defined]
    IgnoreLintOccurrenceRecord(  # type: ignore[name-defined]
        label=Managed, comment=NotRequired[Markdown], tag=23
    )

    # Source tags preserve what the publication said. ValidityAssessment records
    # the database's judgment; database origin and presence live on Taxon.
    ValidityFromSource(  # type: ignore[name-defined]
        validity=OccurrenceValidity, comment=NotRequired[Markdown], tag=24
    )
    ValidityAssessment(  # type: ignore[name-defined]
        validity=OccurrenceValidity, comment=NotRequired[Markdown], tag=25
    )
    OriginFromSource(  # type: ignore[name-defined]
        origin=DistributionOrigin, comment=NotRequired[Markdown], tag=26
    )
    PresenceFromSource(  # type: ignore[name-defined]
        presence=DistributionPresence, comment=NotRequired[Markdown], tag=27
    )
    # Including the reviewed Taxon prevents a review from silently surviving a
    # later reassignment of the occurrence record.
    ReviewedInLightOf(  # type: ignore[name-defined]
        article=Article, taxon=Taxon, comment=Markdown, tag=28
    )
    Voucher(text=Managed, collection=Collection, tag=29)  # type: ignore[name-defined]
