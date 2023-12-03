from __future__ import annotations

import datetime
import enum
import time
from collections import defaultdict
from collections.abc import Iterable

from peewee import CharField, ForeignKeyField, IntegerField, TextField

from ... import adt, events, getinput
from .base import ADTField, BaseModel, LintConfig
from .location import Location
from .region import Region
from .taxon import Taxon


class Specimen(BaseModel):
    creation_event = events.Event["Specimen"]()
    save_event = events.Event["Specimen"]()
    call_sign = "JZ"
    label_field = "id"

    taxon = ForeignKeyField(Taxon)
    region = ForeignKeyField(Region)
    location = ForeignKeyField(Location, null=True)
    taxon_text = CharField()
    location_text = CharField()
    date = CharField()
    description = CharField()
    link = CharField()
    tags = ADTField(lambda: SpecimenTag, null=True)

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
        location = Location.getter(None).get_one("location> ")
        if location is None:
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
            location=location,
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

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        if not any(isinstance(tag, SpecimenTag.TaxonCount) for tag in self.tags):
            yield f"{self}: missing TaxonCount tag"
        if not any(isinstance(tag, SpecimenTag.FindKind) for tag in self.tags):
            yield f"{self}: missing FindKind tag"
        for tag in self.tags:
            if isinstance(tag, SpecimenTag.TaxonCount):
                if " " in tag.taxon:
                    yield f"{self}: TaxonCount taxon should be genus only"

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        return {**callbacks, "add_comment": self.add_comment}

    def edit(self) -> None:
        self.fill_field("tags")

    def total_num_specimens(self) -> int:
        if not self.tags:
            return 0
        return sum(t.count for t in self.tags if isinstance(t, SpecimenTag.TaxonCount))

    def get_kind(self) -> KindOfFind | None:
        if not self.tags:
            return None
        for tag in self.tags:
            if isinstance(tag, SpecimenTag.FindKind):
                return tag.kind
        return None

    def get_taxa(self) -> set[str]:
        if not self.tags:
            return set()
        return {t.taxon for t in self.tags if isinstance(t, SpecimenTag.TaxonCount)}

    @classmethod
    def taxon_report(cls) -> None:
        counts: dict[str, int] = defaultdict(int)
        for spec in cls.select_valid():
            if not spec.tags:
                continue
            for tag in spec.tags:
                if isinstance(tag, SpecimenTag.TaxonCount):
                    counts[tag.taxon] += tag.count
        for taxon, count in sorted(counts.items()):
            print(f"{count} {taxon}")

    @classmethod
    def grouped_taxon_report(cls, group_by: str) -> None:
        counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for spec in cls.select_valid():
            if not spec.tags:
                continue
            group_label = str(getattr(spec, group_by))
            for tag in spec.tags:
                if isinstance(tag, SpecimenTag.TaxonCount):
                    counts[group_label][tag.taxon] += tag.count
        for label, group in sorted(counts.items()):
            print(f"=== {label} ===")
            for taxon, count in sorted(group.items()):
                print(f"{count} {taxon}")

    @classmethod
    def location_report(cls) -> None:
        by_loc: dict[str, list[Specimen]] = defaultdict(list)
        for spec in cls.select_valid():
            by_loc[spec.location].append(spec)
        for loc, specs in sorted(by_loc.items(), key=lambda p: len(p[1])):
            counts: dict[str, int] = defaultdict(int)
            for spec in specs:
                for tag in spec.tags:
                    if isinstance(tag, SpecimenTag.TaxonCount):
                        counts[tag.taxon] += tag.count
            total = sum(counts.values())
            if total < 5:
                continue
            getinput.print_header(f"{loc} ({len(specs)} lots, {total} individuals)")
            for taxon, count in sorted(counts.items()):
                if count < 3:
                    continue
                print(f"{count} {taxon} ({count/total*100:.02f}%)")

    def __str__(self) -> str:
        return f"JSZ#{self.id}"


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


class KindOfFind(enum.IntEnum):
    bottle = 1
    picked_up = 2
    professional = 3


class BoxType(enum.IntEnum):
    default = 1
    separate = 2


class SpecimenTag(adt.ADT):
    TaxonCount(count=int, taxon=str, tag=1)  # type: ignore
    FindKind(kind=KindOfFind, tag=2)  # type: ignore
    Box(type=BoxType, taxon=str, description=str, tag=3)  # type: ignore
