from __future__ import annotations

from collections.abc import Iterable

from taxonomy.db import models
from taxonomy.db.constants import OccurrenceBasis
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location, LocationStatus
from taxonomy.db.models.taxon import Taxon

from .model import OccurrenceRecord, OccurrenceRecordTag


def get_inferred_taxon(record: OccurrenceRecord) -> Taxon | None:
    mapped_name = record.classification_entry.mapped_name
    if mapped_name is None:
        return None
    return mapped_name.taxon.resolve_redirect()


def _get_location_by_name(name: str) -> Location | None:
    candidates = list(Location.select().filter(Location.name == name))
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    if candidate.deleted is LocationStatus.alias:
        return candidate.parent
    if candidate.deleted is LocationStatus.valid:
        return candidate
    return None


def get_inferred_location(record: OccurrenceRecord) -> Location | None:
    hint_names = [
        tag.name
        for tag in record.get_tags(record.tags, OccurrenceRecordTag.LocationHint)
    ]
    hinted_candidates = {
        candidate
        for name in hint_names
        if (candidate := _get_location_by_name(name)) is not None
    }
    if len(hinted_candidates) == 1:
        return next(iter(hinted_candidates))
    if hint_names:
        return None
    if candidate := _get_location_by_name(record.locality_text):
        return candidate
    try:
        region = models.Region.get(models.Region.name == record.locality_text)
        return region.get_location()
    except models.Region.DoesNotExist, models.Location.DoesNotExist:
        return None


def check_taxon(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    inferred = get_inferred_taxon(record)
    if record.taxon is None:
        if inferred is None:
            yield f"{record}: cannot infer taxon from classification entry [missing_taxon]"
        else:
            message = f"{record}: taxon should be {inferred} [missing_taxon]"
            if cfg.autofix:
                print(message)
                record.taxon = inferred
            else:
                yield message
        return
    if inferred is None or record.taxon == inferred:
        return
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom) or record.has_tag(
        OccurrenceRecordTag.CommentFromDatabase
    ):
        return
    yield (
        f"{record}: taxon {record.taxon} differs from classification-entry mapping "
        f"{inferred} without an explanation [taxon_mapping]"
    )


def check_location(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    hints = list(record.get_tags(record.tags, OccurrenceRecordTag.LocationHint))
    if record.location is None:
        inferred = get_inferred_location(record)
        if inferred is None:
            yield f"{record}: cannot infer location [missing_location]"
        else:
            message = f"{record}: location should be {inferred} [missing_location]"
            if cfg.autofix:
                print(message)
                record.location = inferred
                record.remove_tags(OccurrenceRecordTag.LocationHint)
            else:
                yield message
        return
    inferred = get_inferred_location(record)
    if hints and inferred == record.location:
        message = f"{record}: remove resolved LocationHint [location_hint]"
        if cfg.autofix:
            print(message)
            record.remove_tags(OccurrenceRecordTag.LocationHint)
        else:
            yield message
        return
    if hints and inferred is not None and inferred != record.location:
        yield (
            f"{record}: location {record.location} differs from LocationHint mapping "
            f"{inferred} [location_mapping]"
        )


def check_basis_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.basis is not OccurrenceBasis.observation and record.has_tag(
        OccurrenceRecordTag.ObservationKind
    ):
        yield f"{record}: ObservationKind requires observation basis [basis_tags]"
    if record.basis is not OccurrenceBasis.voucher and record.has_tag(
        OccurrenceRecordTag.SpecimenDetail
    ):
        yield f"{record}: SpecimenDetail requires voucher basis [basis_tags]"


def check_split(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    tags = list(record.get_tags(record.tags, OccurrenceRecordTag.TaxonomicSplitFrom))
    if len(tags) > 1:
        yield f"{record}: has multiple TaxonomicSplitFrom tags [taxonomic_split]"
        return
    if not tags:
        return
    canonical = tags[0].record
    if canonical == record:
        yield f"{record}: TaxonomicSplitFrom points to itself [taxonomic_split]"
        return
    if canonical.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom):
        yield f"{record}: TaxonomicSplitFrom points to a derived record [taxonomic_split]"
    for field in ("classification_entry", "locality_text", "page", "basis"):
        if getattr(record, field) != getattr(canonical, field):
            yield (
                f"{record}: {field} differs from canonical split record {canonical} "
                "[taxonomic_split]"
            )
    if record.taxon is not None and record.taxon == canonical.taxon:
        yield f"{record}: split record has the same taxon as {canonical} [taxonomic_split]"


def check_duplicate(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom):
        return
    candidates = OccurrenceRecord.select().filter(
        OccurrenceRecord.classification_entry == record.classification_entry,
        OccurrenceRecord.locality_text == record.locality_text,
        OccurrenceRecord.page == record.page,
        OccurrenceRecord.basis == record.basis,
    )
    for candidate in candidates:
        if candidate != record and not candidate.has_tag(
            OccurrenceRecordTag.TaxonomicSplitFrom
        ):
            yield f"{record}: duplicates primary record {candidate} [duplicate]"
