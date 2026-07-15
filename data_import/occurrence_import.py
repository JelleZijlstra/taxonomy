from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping

from data_import import lib
from taxonomy.db import models
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag


def iter_occurrences(
    entries: Iterable[lib.CEDict],
) -> Iterable[tuple[lib.CEDict, lib.CEOccurrenceDict]]:
    for entry in entries:
        for occurrence in entry.get("occurrences", []):
            yield entry, occurrence


def print_occurrence_report(
    entries: Iterable[lib.CEDict], locations: Mapping[str, models.Location | None]
) -> None:
    occurrences = list(iter_occurrences(entries))
    bases = Counter(occurrence["basis"] for _, occurrence in occurrences)
    mapped_locations = sum(
        occurrence.get("mapped_location") in locations
        and locations[occurrence["mapped_location"]] is not None
        for _, occurrence in occurrences
        if occurrence.get("mapped_location") is not None
    )
    unresolved_locations = sum(
        occurrence.get("mapped_location") is not None
        and locations.get(occurrence["mapped_location"]) is None
        for _, occurrence in occurrences
    )
    print(f"Occurrence records: {len(occurrences)}")
    if bases:
        print(
            "Occurrence basis:",
            ", ".join(
                f"{basis.name}={count}"
                for basis, count in sorted(
                    bases.items(), key=lambda item: item[0].value
                )
            ),
        )
    print(
        "Occurrence location mappings now:",
        f"resolved={mapped_locations}, not_resolved={unresolved_locations}",
    )


def _location_for_occurrence(
    occurrence: lib.CEOccurrenceDict, locations: Mapping[str, models.Location | None]
) -> models.Location | None:
    mapped_name = occurrence.get("mapped_location")
    if mapped_name is None:
        return None
    return locations.get(mapped_name)


def _tags_for_occurrence(
    occurrence: lib.CEOccurrenceDict, location: models.Location | None
) -> list[OccurrenceRecordTag]:
    tags = list(occurrence.get("tags", []))
    mapped_name = occurrence.get("mapped_location")
    if (
        mapped_name is not None
        and location is None
        and not any(
            isinstance(tag, OccurrenceRecordTag.LocationHint)
            and tag.name == mapped_name
            for tag in tags
        )
    ):
        tags.append(OccurrenceRecordTag.LocationHint(mapped_name))
    return tags


def _find_existing_record(
    ce: models.ClassificationEntry, occurrence: lib.CEOccurrenceDict
) -> models.OccurrenceRecord | None:
    candidates = models.OccurrenceRecord.select().filter(
        models.OccurrenceRecord.classification_entry == ce,
        models.OccurrenceRecord.locality_text == occurrence["locality"],
        models.OccurrenceRecord.page == occurrence.get("page"),
        models.OccurrenceRecord.basis == occurrence["basis"],
    )
    primary = [
        candidate
        for candidate in candidates
        if not candidate.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom)
    ]
    if len(primary) > 1:
        raise ValueError(
            f"multiple primary OccurrenceRecords match {ce} / {occurrence!r}"
        )
    return primary[0] if primary else None


def _merge_tags(
    record: models.OccurrenceRecord, tags: Iterable[OccurrenceRecordTag]
) -> None:
    existing = list(record.tags)
    for tag in tags:
        if tag not in existing:
            record.add_tag(tag)
            existing.append(tag)


def add_occurrence_records(
    entries: Iterable[lib.CEDict], locations: Mapping[str, models.Location | None]
) -> list[models.OccurrenceRecord]:
    records = []
    for entry, occurrence in iter_occurrences(entries):
        ce = lib.get_existing(entry, strict=True)
        if ce is None:
            raise ValueError(f"ClassificationEntry was not imported: {entry!r}")
        taxon = ce.mapped_name.taxon.resolve_redirect() if ce.mapped_name else None
        location = _location_for_occurrence(occurrence, locations)
        tags = _tags_for_occurrence(occurrence, location)
        record = _find_existing_record(ce, occurrence)
        if record is None:
            record = models.OccurrenceRecord.create(
                classification_entry=ce,
                locality_text=occurrence["locality"],
                page=occurrence.get("page"),
                basis=occurrence["basis"],
                raw_data=occurrence.get("raw_data"),
                taxon=taxon,
                location=location,
                tags=tags,
            )
            print(f"created OccurrenceRecord: {record}")
        else:
            if record.raw_data is None and occurrence.get("raw_data") is not None:
                record.raw_data = occurrence["raw_data"]
            elif (
                occurrence.get("raw_data") is not None
                and record.raw_data != occurrence["raw_data"]
            ):
                print(f"warning: not overwriting differing raw_data on {record}")
            if record.taxon is None:
                record.taxon = taxon
            elif taxon is not None and record.taxon != taxon:
                print(f"warning: not overwriting differing taxon on {record}")
            if record.location is None:
                record.location = location
            elif location is not None and record.location != location:
                print(f"warning: not overwriting differing location on {record}")
                mapped_name = occurrence.get("mapped_location")
                if mapped_name is not None:
                    tags.append(OccurrenceRecordTag.LocationHint(mapped_name))
            _merge_tags(record, tags)
            print(f"already exists: {record}")
        record.format(quiet=True)
        records.append(record)
    return records
