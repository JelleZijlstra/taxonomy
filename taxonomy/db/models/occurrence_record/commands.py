from __future__ import annotations

from taxonomy import command_set
from taxonomy.db.models.classification_entry import ClassificationEntry
from taxonomy.db.models.taxon import Taxon

from .model import OccurrenceRecord

CS = command_set.CommandSet("occurrence_record", "Occurrence-record commands.")


@CS.register
def add_occurrence_record(
    ce: ClassificationEntry | None = None,
) -> OccurrenceRecord | None:
    return OccurrenceRecord.create_interactively(classification_entry=ce)


@CS.register
def occurrence_records_for_ce(
    ce: ClassificationEntry | None = None,
) -> list[OccurrenceRecord]:
    if ce is None:
        ce = ClassificationEntry.getter(None).get_one("classification entry> ")
    if ce is None:
        return []
    records = sorted(ce.occurrence_records, key=lambda record: record.locality_text)
    for record in records:
        print(record)
    return records


@CS.register
def occurrence_records_for_taxon(taxon: Taxon | None = None) -> list[OccurrenceRecord]:
    if taxon is None:
        taxon = Taxon.getter(None).get_one("taxon> ")
    if taxon is None:
        return []
    records = sorted(taxon.occurrence_records, key=lambda record: record.locality_text)
    for record in records:
        print(record)
    return records


@CS.register
def edit_occurrence_record(
    record: OccurrenceRecord | None = None,
) -> OccurrenceRecord | None:
    if record is None:
        record = OccurrenceRecord.getter(None).get_one("occurrence record> ")
    if record is None:
        return None
    record.edit()
    record.format(quiet=True)
    return record
