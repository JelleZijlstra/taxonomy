"""Pure authoring helpers for native classification recommendation manifests.

The draft objects in this module never load or mutate database objects.  They lower a
compact, source-oriented transcription to ordinary schema-version 2 recommendation
rows that the shared applicator can validate and execute.
"""

from __future__ import annotations

import json
import re
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from taxonomy.db.constants import OccurrenceBasis, Rank


class ClassificationDraftError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DraftEvidence:
    kind: str
    text: str

    def as_json(self) -> dict[str, str]:
        return {"kind": self.kind, "text": self.text}


@dataclass(slots=True)
class OccurrenceDraft:
    locality: str
    basis: OccurrenceBasis
    page: str | None = None
    raw_data: str | None = None
    location: dict[str, Any] | None = None
    taxon: dict[str, Any] | None = None
    tags: list[dict[str, Any] | list[Any]] = field(default_factory=list)
    evidence: list[DraftEvidence] = field(default_factory=list)
    key: str | None = None


@dataclass(slots=True)
class ClassificationDraft:
    source: ArticleDraft
    key: str
    name: str
    rank: Rank
    page: str
    parent_key: str | None = None
    authority: str | None = None
    year: str | None = None
    citation: str | None = None
    type_locality: str | None = None
    raw_data: str | None = None
    mapped_name: dict[str, Any] | None = None
    tags: list[dict[str, Any] | list[Any]] = field(default_factory=list)
    evidence: list[DraftEvidence] = field(default_factory=list)
    scratch: dict[str, Any] = field(default_factory=dict)
    occurrences: list[OccurrenceDraft] = field(default_factory=list)

    @property
    def ref(self) -> str:
        return f"ce:{self.source.key}:{self.key}"

    def add_occurrence(
        self,
        locality: str,
        basis: OccurrenceBasis,
        *,
        page: str | None = None,
        raw_data: str | None = None,
        location: dict[str, Any] | None = None,
        taxon: dict[str, Any] | None = None,
        tags: list[dict[str, Any] | list[Any]] | None = None,
        evidence: list[DraftEvidence] | None = None,
        key: str | None = None,
    ) -> OccurrenceDraft:
        occurrence = OccurrenceDraft(
            locality=locality,
            basis=basis,
            page=page,
            raw_data=raw_data,
            location=location,
            taxon=taxon,
            tags=list(tags or ()),
            evidence=list(evidence or ()),
            key=key,
        )
        self.occurrences.append(occurrence)
        return occurrence


@dataclass(slots=True)
class LocationDraft:
    key: str
    label: str
    match: dict[str, Any]
    values: dict[str, Any]
    reason: str
    evidence: list[DraftEvidence]
    confidence: str = "high"

    @property
    def ref(self) -> str:
        return f"location:{self.key}"


@dataclass(slots=True)
class CoverageDraft:
    article_key: str
    parent_key: str | None
    covered_keys: tuple[str, ...]
    completeness: str
    unexpected: str
    reason: str
    evidence: list[DraftEvidence]


class ArticleDraft(AbstractContextManager["ArticleDraft"]):
    def __init__(
        self,
        bundle: ClassificationBundleBuilder,
        key: str,
        *,
        article_ref: str | None,
        article_id: int | None,
        article_label: str | None,
        evidence: list[DraftEvidence],
    ) -> None:
        self.bundle = bundle
        self.key = key
        if article_ref is not None and article_id is not None:
            raise ClassificationDraftError(
                "Article draft accepts only one of article_ref and article_id"
            )
        if article_id is not None and not article_label:
            raise ClassificationDraftError("article_label is required with article_id")
        if article_id is None and article_label is not None:
            raise ClassificationDraftError(
                "article_label is only allowed with article_id"
            )
        self.article_ref = article_ref or key
        self.article_id = article_id
        self.article_label = article_label
        self.evidence = evidence
        self.entries: list[ClassificationDraft] = []

    @property
    def article_value(self) -> dict[str, Any]:
        if self.article_id is not None:
            assert self.article_label is not None
            return {
                "model": "Article",
                "id": self.article_id,
                "label": self.article_label,
            }
        return {"model": "Article", "ref": self.article_ref}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def add_entry(
        self,
        name: str,
        rank: Rank,
        *,
        page: str,
        key: str | None = None,
        parent: ClassificationDraft | str | None = None,
        authority: str | None = None,
        year: str | None = None,
        citation: str | None = None,
        type_locality_quote: str | None = None,
        raw_data: str | None = None,
        mapped_name: dict[str, Any] | None = None,
        tags: list[dict[str, Any] | list[Any]] | None = None,
        evidence: list[DraftEvidence] | None = None,
        scratch: dict[str, Any] | None = None,
        type_specimen_data_quote: str | None = None,
        original_combination: str | None = None,
        comment_from_source: str | None = None,
        original_page_described: str | None = None,
        textual_rank: str | None = None,
        age_class: str | None = None,
        normalized_name: str | None = None,
        materialize: bool = False,
        materialize_parent_taxon_id: int | None = None,
        materialize_parent: ClassificationDraft | None = None,
        etymology_detail_quote: str | None = None,
        original_citation: bool = False,
        materialize_base_name: dict[str, Any] | None = None,
        structured_data: dict[str, str] | None = None,
    ) -> ClassificationDraft:
        """Add a source entry.

        The ``*_quote`` arguments are copied into Name detail evidence during
        materialization. Their values must therefore be direct source quotations, not
        summaries or reconciled database wording.
        """
        resolved_key = key or self.bundle.make_key(name)
        parent_key = parent.key if isinstance(parent, ClassificationDraft) else parent
        lowered_tags = list(tags or ())

        def add_tag(tag: str, **arguments: str) -> None:
            lowered_tags.append({"tag": tag, "arguments": arguments})

        if type_specimen_data_quote is not None:
            add_tag("TypeSpecimenData", text=type_specimen_data_quote)
        if original_combination is not None:
            add_tag("OriginalCombination", text=original_combination)
        if comment_from_source is not None:
            add_tag("CommentFromSource", text=comment_from_source)
        if original_page_described is not None:
            add_tag("OriginalPageDescribed", text=original_page_described)
        if textual_rank is not None:
            add_tag("TextualRank", text=textual_rank)
        if age_class is not None:
            add_tag("AgeClassCE", age=age_class)
        if normalized_name is not None:
            add_tag("CorrectedName", text=normalized_name)
        if materialize_parent_taxon_id is not None and not materialize:
            raise ClassificationDraftError(
                "materialize_parent_taxon_id requires materialize=True"
            )
        if materialize_parent is not None and not materialize:
            raise ClassificationDraftError(
                "materialize_parent requires materialize=True"
            )
        if materialize_parent_taxon_id is not None and materialize_parent is not None:
            raise ClassificationDraftError(
                "materialize_parent and materialize_parent_taxon_id are mutually exclusive"
            )
        if materialize_parent is not None:
            if parent is not None:
                raise ClassificationDraftError(
                    "materialize_parent cannot be combined with a source-local parent"
                )
            if materialize_parent.source.bundle is not self.bundle:
                raise ClassificationDraftError(
                    "materialize_parent must reference a CE in the same bundle"
                )
            if materialize_parent.rank.is_synonym:
                raise ClassificationDraftError(
                    "materialize_parent must reference an accepted CE"
                )
        if materialize:
            arguments: dict[str, Any] = {}
            if materialize_parent_taxon_id is not None:
                arguments["parent_taxon_id"] = materialize_parent_taxon_id
            lowered_tags.append({"tag": "Materialize", "arguments": arguments})
        if materialize_parent is not None:
            lowered_tags.append(
                {
                    "tag": "MaterializeParent",
                    "arguments": {
                        "ce": {
                            "model": "ClassificationEntry",
                            "ref": materialize_parent.ref,
                            "label": materialize_parent.name,
                        }
                    },
                }
            )
        if etymology_detail_quote is not None:
            add_tag("EtymologyDetail", text=etymology_detail_quote)
        if original_citation:
            lowered_tags.append({"tag": "OriginalCitation", "arguments": {}})
        if materialize_base_name is not None:
            if not materialize:
                raise ClassificationDraftError(
                    "materialize_base_name requires materialize=True"
                )
            base_name_arguments = dict(materialize_base_name)
            authors = base_name_arguments.pop("authors", None)
            lowered_tags.append(
                {"tag": "MaterializeBaseName", "arguments": base_name_arguments}
            )
            if authors is not None:
                if not isinstance(authors, list) or not authors:
                    raise ClassificationDraftError(
                        "materialize_base_name.authors must be a nonempty list"
                    )
                for order, person in enumerate(authors, start=1):
                    lowered_tags.append(
                        {
                            "tag": "MaterializeBaseNameAuthor",
                            "arguments": {"person": person, "order": order},
                        }
                    )
        for label, text in sorted((structured_data or {}).items()):
            add_tag("StructuredData", label=label, text=text)
        draft = ClassificationDraft(
            source=self,
            key=resolved_key,
            name=name,
            rank=rank,
            page=page,
            parent_key=parent_key,
            authority=authority,
            year=year,
            citation=citation,
            type_locality=type_locality_quote,
            raw_data=raw_data,
            mapped_name=mapped_name,
            tags=lowered_tags,
            evidence=list(evidence or ()),
            scratch=dict(scratch or {}),
        )
        self.entries.append(draft)
        return draft


class ClassificationBundleBuilder:
    """Build deterministic native recommendation rows from source drafts."""

    def __init__(self) -> None:
        self.articles: list[ArticleDraft] = []
        self.locations: dict[str, LocationDraft] = {}
        self.coverage: list[CoverageDraft] = []

    @staticmethod
    def make_key(text: str) -> str:
        key = re.sub(r"[^a-z0-9]+", "-", text.casefold()).strip("-")
        if not key:
            raise ClassificationDraftError(f"cannot derive a key from {text!r}")
        return key

    def article(
        self,
        key: str,
        *,
        article_ref: str | None = None,
        article_id: int | None = None,
        article_label: str | None = None,
        evidence: list[DraftEvidence] | None = None,
    ) -> ArticleDraft:
        if any(article.key == key for article in self.articles):
            raise ClassificationDraftError(f"duplicate Article draft key {key!r}")
        article = ArticleDraft(
            self,
            key,
            article_ref=article_ref,
            article_id=article_id,
            article_label=article_label,
            evidence=list(evidence or ()),
        )
        self.articles.append(article)
        return article

    def add_location(self, draft: LocationDraft) -> None:
        if draft.key in self.locations:
            raise ClassificationDraftError(
                f"duplicate Location draft key {draft.key!r}"
            )
        self.locations[draft.key] = draft

    def assert_coverage(
        self,
        article_key: str,
        covered: list[ClassificationDraft],
        *,
        parent: ClassificationDraft | None = None,
        completeness: str = "complete",
        unexpected: str = "report",
        reason: str,
        evidence: list[DraftEvidence],
    ) -> None:
        self.coverage.append(
            CoverageDraft(
                article_key,
                parent.key if parent else None,
                tuple(item.key for item in covered),
                completeness,
                unexpected,
                reason,
                evidence,
            )
        )

    def validate(self) -> None:
        article_keys = {article.key for article in self.articles}
        if len(article_keys) != len(self.articles):
            raise ClassificationDraftError("duplicate Article keys")
        for article in self.articles:
            entries = {entry.key: entry for entry in article.entries}
            if len(entries) != len(article.entries):
                raise ClassificationDraftError(
                    f"Article {article.key!r} has duplicate entry keys"
                )
            seen_identity: set[tuple[str, Rank, str, str | None]] = set()
            for entry in article.entries:
                if not article.evidence and not entry.evidence:
                    raise ClassificationDraftError(
                        f"{article.key}:{entry.key} requires source evidence"
                    )
                if not re.fullmatch(r"[1-9][0-9]*", entry.page):
                    raise ClassificationDraftError(
                        f"{article.key}:{entry.key} page must be one page number"
                    )
                identity = (entry.name, entry.rank, entry.page, entry.parent_key)
                if identity in seen_identity and not entry.rank.is_synonym:
                    raise ClassificationDraftError(
                        f"duplicate source entry {entry.name!r} in {article.key}"
                    )
                seen_identity.add(identity)
                if entry.parent_key is not None and entry.parent_key not in entries:
                    raise ClassificationDraftError(
                        f"{article.key}:{entry.key} has unknown parent {entry.parent_key!r}"
                    )
                visited = {entry.key}
                parent_key = entry.parent_key
                while parent_key is not None:
                    if parent_key in visited:
                        raise ClassificationDraftError(
                            f"parent cycle in Article {article.key!r} at {entry.key!r}"
                        )
                    visited.add(parent_key)
                    parent_key = entries[parent_key].parent_key
                if entry.rank is Rank.species and " " in entry.name:
                    genus = entry.name.split()[0]
                    ancestor = entry
                    while ancestor.parent_key is not None:
                        ancestor = entries[ancestor.parent_key]
                        if ancestor.rank is Rank.genus:
                            if ancestor.name != genus:
                                raise ClassificationDraftError(
                                    f"species {entry.name!r} conflicts with source genus {ancestor.name!r}"
                                )
                            break
                occurrence_ids: set[tuple[str, str | None, OccurrenceBasis]] = set()
                for occurrence in entry.occurrences:
                    occurrence_id = (
                        occurrence.locality,
                        occurrence.page,
                        occurrence.basis,
                    )
                    if occurrence_id in occurrence_ids:
                        raise ClassificationDraftError(
                            f"duplicate occurrence for {article.key}:{entry.key}: {occurrence.locality!r}"
                        )
                    occurrence_ids.add(occurrence_id)
                    if occurrence.location is None and not any(
                        isinstance(tag, dict) and tag.get("tag") == "LocationHint"
                        for tag in occurrence.tags
                    ):
                        raise ClassificationDraftError(
                            f"occurrence {article.key}:{entry.key}:{occurrence.locality} must have a Location ref or LocationHint"
                        )
                    tag_names = {
                        tag.get("tag")
                        for tag in occurrence.tags
                        if isinstance(tag, dict)
                    }
                    if (
                        "ObservationKind" in tag_names
                        and occurrence.basis is not OccurrenceBasis.observation
                    ):
                        raise ClassificationDraftError(
                            "ObservationKind requires observation basis"
                        )
                    if tag_names & {"Voucher", "SpecimenDetail"} and (
                        occurrence.basis is not OccurrenceBasis.voucher
                    ):
                        raise ClassificationDraftError(
                            "Voucher and SpecimenDetail require voucher basis"
                        )
        for location in self.locations.values():
            if not location.evidence:
                raise ClassificationDraftError(
                    f"Location {location.key!r} requires source evidence"
                )
            latitude = location.values.get("latitude")
            longitude = location.values.get("longitude")
            if (latitude is None) != (longitude is None):
                raise ClassificationDraftError(
                    f"Location {location.key!r} must provide latitude and longitude together"
                )
        for coverage in self.coverage:
            if coverage.article_key not in article_keys:
                raise ClassificationDraftError(
                    f"coverage references unknown Article {coverage.article_key!r}"
                )
            if coverage.completeness not in {"partial", "complete"}:
                raise ClassificationDraftError(
                    "coverage completeness must be partial or complete"
                )
            if coverage.unexpected not in {"report", "error"}:
                raise ClassificationDraftError(
                    "coverage unexpected must be report or error"
                )
            if not coverage.evidence:
                raise ClassificationDraftError("coverage assertions require evidence")

    @staticmethod
    def _source_snapshot(entry: ClassificationDraft) -> str:
        source = {
            key: value
            for key, value in {
                "name": entry.name,
                "rank": entry.rank.name,
                "page": entry.page,
                "authority": entry.authority,
                "year": entry.year,
                "citation": entry.citation,
                "type_locality": entry.type_locality,
                "parent": entry.parent_key,
                "tags": entry.tags,
            }.items()
            if value not in (None, [], {})
        }
        return json.dumps(
            source, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    def to_rows(self) -> list[dict[str, Any]]:
        self.validate()
        rows: list[dict[str, Any]] = []
        for location in self.locations.values():
            rows.append(
                {
                    "schema_version": 2,
                    "action": "create_object",
                    "object": {
                        "model": "Location",
                        "ref": location.ref,
                        "label": location.label,
                    },
                    "match": location.match,
                    "values": location.values,
                    "confidence": location.confidence,
                    "reason": location.reason,
                    "evidence": [item.as_json() for item in location.evidence],
                }
            )
        entries_by_article: dict[str, dict[str, ClassificationDraft]] = {}
        for article in self.articles:
            entries_by_article[article.key] = {
                entry.key: entry for entry in article.entries
            }
            for entry in article.entries:
                article_value = article.article_value
                parent_value = (
                    {
                        "model": "ClassificationEntry",
                        "ref": f"ce:{article.key}:{entry.parent_key}",
                    }
                    if entry.parent_key is not None
                    else None
                )
                values: dict[str, Any] = {
                    "article": article_value,
                    "name": entry.name,
                    "rank": entry.rank.name,
                    "status": "valid",
                    "parent": parent_value,
                    "page": entry.page,
                    "mapped_name": entry.mapped_name,
                    "authority": entry.authority,
                    "year": entry.year,
                    "citation": entry.citation,
                    "type_locality": entry.type_locality,
                    "raw_data": entry.raw_data or self._source_snapshot(entry),
                    "tags": entry.tags,
                }
                match = {
                    "article": article_value,
                    "name": entry.name,
                    "rank": entry.rank.name,
                    "page": entry.page,
                    "parent": parent_value,
                }
                rows.append(
                    {
                        "schema_version": 2,
                        "action": "create_object",
                        "object": {
                            "model": "ClassificationEntry",
                            "ref": entry.ref,
                            "label": entry.name,
                        },
                        "match": match,
                        "values": values,
                        "confidence": "high",
                        "reason": "Transcribed from the source classification.",
                        "evidence": [
                            item.as_json()
                            for item in (*article.evidence, *entry.evidence)
                        ],
                    }
                )
                for index, occurrence in enumerate(entry.occurrences, start=1):
                    occurrence_key = occurrence.key or f"{entry.key}:{index}"
                    page = occurrence.page or entry.page
                    ce_value = {"model": "ClassificationEntry", "ref": entry.ref}
                    occurrence_values = {
                        "classification_entry": ce_value,
                        "locality_text": occurrence.locality,
                        "page": page,
                        "basis": occurrence.basis.name,
                        "raw_data": occurrence.raw_data,
                        "taxon": occurrence.taxon,
                        "location": occurrence.location,
                        "tags": occurrence.tags,
                        "status": "valid",
                    }
                    rows.append(
                        {
                            "schema_version": 2,
                            "action": "create_object",
                            "object": {
                                "model": "OccurrenceRecord",
                                "ref": f"occurrence:{article.key}:{occurrence_key}",
                                "label": occurrence.locality,
                            },
                            "match": {
                                "classification_entry": ce_value,
                                "locality_text": occurrence.locality,
                                "page": page,
                                "basis": occurrence.basis.name,
                            },
                            "values": occurrence_values,
                            "confidence": "high",
                            "reason": "Occurrence explicitly recorded by the source.",
                            "evidence": [
                                item.as_json()
                                for item in (
                                    *article.evidence,
                                    *entry.evidence,
                                    *occurrence.evidence,
                                )
                            ],
                        }
                    )
        for coverage in self.coverage:
            article = next(
                item for item in self.articles if item.key == coverage.article_key
            )
            where: dict[str, Any] = {"article": article.article_value}
            if coverage.parent_key is not None:
                where["parent"] = {
                    "model": "ClassificationEntry",
                    "ref": f"ce:{coverage.article_key}:{coverage.parent_key}",
                }
            rows.append(
                {
                    "schema_version": 2,
                    "action": "check_coverage",
                    "scope": {"model": "ClassificationEntry", "where": where},
                    "completeness": coverage.completeness,
                    "covered_objects": [
                        {
                            "model": "ClassificationEntry",
                            "ref": f"ce:{coverage.article_key}:{key}",
                        }
                        for key in coverage.covered_keys
                    ],
                    "unexpected": coverage.unexpected,
                    "confidence": "high",
                    "reason": coverage.reason,
                    "evidence": [item.as_json() for item in coverage.evidence],
                }
            )
        return rows

    def write_jsonl(
        self, path: Path, *, prefix_rows: list[dict[str, Any]] | None = None
    ) -> None:
        rows = [*(prefix_rows or ()), *self.to_rows()]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(
                json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                for row in rows
            )
        )
