"""Build a reviewable bat-literature acquisition list.

The input inventory combines literature implicated by Names below Chiroptera with the
parsed/matched HMW bat bibliography.  Known local holdings, BatLit records, and strong
online-availability evidence are retained in the full audit but omitted from the
publishable most-wanted outputs.

This script is read-only.  Its optional citation manifest contains guarded suggestions
for missing StructuredVerbatimCitation tags; it never applies them.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from taxonomy.db.constants import ArticleKind
from taxonomy.db.models import Article, Name, Taxon
from taxonomy.db.models.name import TypeTag, parse_citations
from taxonomy.refmatch import matcher

DEFAULT_HMW_CSV = Path("refmatch/output/chiroptera-hmw-refs-taxonomy-matches.csv")
DEFAULT_OUTPUT = Path("recs/reports/bat_literature_most_wanted.csv")
DEFAULT_MARKDOWN = Path("recs/reports/bat_literature_most_wanted.md")
DEFAULT_ALL_OUTPUT = Path("recs/reports/bat_literature_inventory.csv")
INPUT_SOURCE_ARTICLE_NAMES = frozenset({"Chiroptera (HMW)"})

OUTPUT_FIELDS = (
    "candidate_id",
    "status",
    "priority_score",
    "source_count",
    "taxonomy_name_count",
    "hmw_reference_count",
    "sources",
    "reference",
    "authors",
    "year",
    "title",
    "container_title",
    "series",
    "volume",
    "issue",
    "pages",
    "publisher",
    "place",
    "reference_type",
    "doi",
    "url",
    "taxonomy_article_id",
    "taxonomy_article_kind",
    "batlit_id",
    "batlit_url",
    "batlit_zenodo_doi",
    "bhl_url",
    "taxonomy_name_ids",
    "taxonomy_names",
    "hmw_source_rows",
    "hmw_sections",
    "availability_basis",
    "review_note",
    "raw_references",
)

OVERRIDE_STATUSES = {"wanted", "available_online", "needs_review", "ignore"}


def clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalized(value: str) -> str:
    return matcher.normalize_text(value)


def split_pages(value: str) -> tuple[str, str]:
    return matcher.first_page(value), matcher.last_page(value)


def first_author(authors: str) -> str:
    families = matcher.split_author_families(authors)
    return families[0] if families else ""


def markdown_reference(value: str) -> str:
    # Positioned-PDF extraction may split one italic word into adjacent runs.
    value = re.sub(r"</i>(\s*)<i>", r"\1", value)
    value = value.replace("<i>", "*").replace("</i>", "*")
    value = re.sub(r"<[^>]+>", "", value)
    return html.unescape(clean(value))


def readily_online_url(url: str, reference_type: str) -> bool:
    """Return whether a URL is strong evidence that the work itself is online.

    A DOI/publisher landing page alone is deliberately not treated as availability:
    it may only expose a paywall.  Such cases can be resolved through overrides.
    """
    lower = url.casefold()
    return bool(
        url
        and (
            "biodiversitylibrary.org/" in lower
            or reference_type == "web"
            or re.search(r"\.pdf(?:[?#]|$)", lower)
        )
    )


@dataclass
class SourceRecord:
    source: str
    authors: str = ""
    year: str = ""
    title: str = ""
    container_title: str = ""
    series: str = ""
    volume: str = ""
    issue: str = ""
    pages: str = ""
    publisher: str = ""
    place: str = ""
    reference_type: str = ""
    raw_reference: str = ""
    formatted_reference: str = ""
    doi: str = ""
    url: str = ""
    taxonomy_article_id: str = ""
    taxonomy_article_kind: str = ""
    batlit_id: str = ""
    batlit_url: str = ""
    batlit_zenodo_doi: str = ""
    bhl_url: str = ""
    taxonomy_name_id: str = ""
    taxonomy_name: str = ""
    hmw_source_row: str = ""
    hmw_section: str = ""
    taxonomy_match_status: str = ""
    taxonomy_top_candidates: str = ""

    def identity_keys(self) -> set[str]:
        keys = set()
        if self.taxonomy_article_id:
            keys.add(f"article:{self.taxonomy_article_id}")
        if self.doi:
            keys.add(f"doi:{matcher.normalize_doi(self.doi)}")
        if self.batlit_id:
            keys.add(f"batlit:{matcher.batlit_key(self.batlit_id)}")
        raw_key = normalized(self.raw_reference)
        if len(raw_key) >= 30 and raw_key not in {"loc cit", "ibid"}:
            keys.add(f"citation:{raw_key}")
        page, _ = split_pages(self.pages)
        author = first_author(self.authors)
        year = matcher.numeric_year(self.year)
        container = normalized(self.container_title)
        title = normalized(self.title)
        if (
            author
            and year is not None
            and (title or (container and self.volume and page))
        ):
            keys.add(
                "bib:"
                + "|".join(
                    (author, str(year), title, container, self.volume.casefold(), page)
                )
            )
        return keys

    def is_local(self) -> bool:
        return self.taxonomy_article_kind in {
            ArticleKind.electronic.name,
            ArticleKind.physical.name,
        }

    def has_online_evidence(self) -> bool:
        return bool(
            self.bhl_url
            or self.taxonomy_article_kind == ArticleKind.reference.name
            or readily_online_url(self.url, self.reference_type)
        )


@dataclass(frozen=True)
class AvailabilityOverride:
    match_key: str
    status: str
    url: str
    note: str


class DisjointSet:
    def __init__(self, size: int) -> None:
        self.parents = list(range(size))

    def find(self, value: int) -> int:
        while self.parents[value] != value:
            self.parents[value] = self.parents[self.parents[value]]
            value = self.parents[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parents[right_root] = left_root


def article_citation(article: Article) -> str:
    try:
        return article.cite()
    except Exception as error:
        return f"[Article {article.id} could not be cited: {error}]"


def article_authors(article: Article) -> str:
    return " & ".join(person.taxonomic_authority() for person in article.get_authors())


def article_record(article: Article, name: Name) -> SourceRecord:
    effective_kind = article.get_effective_kind()
    batlit_id = batlit_url = batlit_zenodo_doi = ""
    bhl_url = matcher.get_article_bhl_url(article)
    if effective_kind not in (ArticleKind.electronic, ArticleKind.physical):
        batlit_id, batlit_url, batlit_zenodo_doi, _ = matcher.get_article_batlit(
            article
        )
    pages = article.start_page or ""
    if article.end_page:
        pages += f"-{article.end_page}" if pages else article.end_page
    if not pages:
        pages = article.pages or ""
    return SourceRecord(
        source="taxonomy_original_citation",
        authors=article_authors(article),
        year=article.year or "",
        title=article.title or "",
        container_title=(
            article.citation_group.get_citable_name()
            if article.citation_group is not None
            else ""
        ),
        series=article.series or "",
        volume=article.volume or "",
        issue=article.issue or "",
        pages=pages,
        publisher=article.publisher or "",
        reference_type=article.type.name.casefold() if article.type else "",
        raw_reference=article_citation(article),
        doi=article.doi or "",
        url=article.url or "",
        taxonomy_article_id=str(article.id),
        taxonomy_article_kind=effective_kind.name,
        batlit_id=batlit_id,
        batlit_url=batlit_url,
        batlit_zenodo_doi=batlit_zenodo_doi,
        bhl_url=bhl_url,
        taxonomy_name_id=str(name.id),
        taxonomy_name=str(name),
    )


def verbatim_record(name: Name) -> SourceRecord:
    structured = name.get_type_tag(TypeTag.StructuredVerbatimCitation)
    citation_group = name.get_citation_group()
    pages = ""
    if structured is not None and structured.start_page:
        pages = structured.start_page
        if structured.end_page:
            pages += f"-{structured.end_page}"
    return SourceRecord(
        source="taxonomy_verbatim_citation",
        authors=" & ".join(
            person.taxonomic_authority() for person in name.get_authors()
        ),
        year=name.year or "",
        container_title=(
            citation_group.get_citable_name() if citation_group is not None else ""
        ),
        series=(structured.series or "") if structured is not None else "",
        volume=(structured.volume or "") if structured is not None else "",
        issue=(structured.issue or "") if structured is not None else "",
        pages=pages,
        raw_reference=name.verbatim_citation or "",
        taxonomy_name_id=str(name.id),
        taxonomy_name=str(name),
    )


def taxonomy_records(taxon_name: str) -> Iterable[SourceRecord]:
    root = Taxon.getter("valid_name")(taxon_name)
    if root is None:
        raise RuntimeError(f"Could not find Taxon {taxon_name!r}")
    for name in root.all_names_lazy():
        if name.original_citation is not None:
            if name.original_citation.name in INPUT_SOURCE_ARTICLE_NAMES:
                continue
            yield article_record(name.original_citation, name)
        elif name.verbatim_citation:
            yield verbatim_record(name)


def hmw_records(path: Path) -> Iterable[SourceRecord]:
    with path.open(newline="") as file:
        reader = csv.DictReader(file)
        required = {"raw_reference", "taxonomy_article_id", "batlit_id", "bhl_url"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise RuntimeError(f"HMW CSV lacks fields: {', '.join(sorted(missing))}")
        article_cache: dict[str, Article] = {}
        for row in reader:
            article_id = row["taxonomy_article_id"]
            article_kind = ""
            if article_id:
                article = article_cache.get(article_id)
                if article is None:
                    article = Article.get(id=int(article_id))
                    article_cache[article_id] = article
                article_kind = article.get_effective_kind().name
                row_year = matcher.numeric_year(row["year"])
                article_year = article.valid_numeric_year()
                if (
                    row_year is not None
                    and article_year is not None
                    and abs(row_year - article_year) > 5
                ):
                    row["taxonomy_match_status"] = "ambiguous"
                    row["taxonomy_top_candidates"] = (
                        f"Rejected stage-3 Article {article_id}: reference year "
                        f"{row_year}, Article year {article_year}"
                    )
                    article_id = ""
                    article_kind = ""
            yield SourceRecord(
                source="hmw_bats_references",
                authors=row["authors"],
                year=row["year"],
                title=row["title"],
                container_title=row["container_title"] or row["book_title"],
                series=row["series"],
                volume=row["volume"],
                issue=row["issue"],
                pages=row["pages"],
                publisher=row["publisher"],
                place=row["place"],
                reference_type=row["reference_type"],
                raw_reference=row["raw_reference"],
                formatted_reference=row["formatted_reference"],
                doi=row["doi"],
                url=row["url"],
                taxonomy_article_id=article_id,
                taxonomy_article_kind=article_kind,
                batlit_id=row["batlit_id"],
                batlit_url=row["batlit_url"],
                batlit_zenodo_doi=row["batlit_zenodo_doi"],
                bhl_url=row["bhl_url"],
                hmw_source_row=row["source_row"],
                hmw_section=row["section"],
                taxonomy_match_status=row["taxonomy_match_status"],
                taxonomy_top_candidates=row["taxonomy_top_candidates"],
            )


def group_records(records: Sequence[SourceRecord]) -> list[list[SourceRecord]]:
    disjoint = DisjointSet(len(records))
    key_owner: dict[str, int] = {}
    for index, record in enumerate(records):
        for key in record.identity_keys():
            previous = key_owner.setdefault(key, index)
            disjoint.union(previous, index)
    groups: dict[int, list[SourceRecord]] = {}
    for index, record in enumerate(records):
        groups.setdefault(disjoint.find(index), []).append(record)
    return list(groups.values())


def unique_values(records: Sequence[SourceRecord], field_name: str) -> list[str]:
    return list(
        dict.fromkeys(
            clean(getattr(record, field_name))
            for record in records
            if clean(getattr(record, field_name))
        )
    )


def preferred(records: Sequence[SourceRecord], field_name: str) -> str:
    values = unique_values(records, field_name)
    return max(values, key=len, default="")


def candidate_keys(records: Sequence[SourceRecord]) -> set[str]:
    return {key for record in records for key in record.identity_keys()}


def candidate_id(records: Sequence[SourceRecord]) -> str:
    keys = sorted(candidate_keys(records))
    seed = keys[0] if keys else preferred(records, "raw_reference")
    digest = hashlib.blake2s(seed.encode(), digest_size=6).hexdigest()
    return f"bat-{digest}"


def load_overrides(path: Path | None) -> dict[str, AvailabilityOverride]:
    if path is None:
        return {}
    output = {}
    with path.open(newline="") as file:
        for row in csv.DictReader(file):
            status = clean(row.get("status"))
            match_key = clean(row.get("match_key"))
            if not match_key or status not in OVERRIDE_STATUSES:
                raise RuntimeError(
                    f"Invalid override match_key/status: {match_key!r}/{status!r}"
                )
            output[match_key] = AvailabilityOverride(
                match_key, status, clean(row.get("url")), clean(row.get("note"))
            )
    return output


def find_override(
    records: Sequence[SourceRecord], overrides: Mapping[str, AvailabilityOverride]
) -> AvailabilityOverride | None:
    for key in [candidate_id(records), *sorted(candidate_keys(records))]:
        if key in overrides:
            return overrides[key]
    return None


def classify(
    records: Sequence[SourceRecord], override: AvailabilityOverride | None
) -> tuple[str, str, str, str]:
    local = [record for record in records if record.is_local()]
    if local:
        kinds = ", ".join(sorted({record.taxonomy_article_kind for record in local}))
        return "excluded_local", f"local catalogue has {kinds} copy", "", ""
    batlit_ids = unique_values(records, "batlit_id")
    if batlit_ids:
        return (
            "excluded_batlit",
            f"matched BatLit item(s): {', '.join(batlit_ids)}",
            "",
            "",
        )
    online = [record for record in records if record.has_online_evidence()]
    if online:
        urls = unique_values(online, "bhl_url") or unique_values(online, "url")
        return (
            "excluded_online",
            "work-level online link recorded",
            " | ".join(urls),
            "",
        )
    if override is not None:
        if override.status == "available_online":
            return (
                "excluded_online",
                "availability override",
                override.url,
                override.note,
            )
        if override.status == "ignore":
            return "excluded_other", "ignored by override", override.url, override.note
        return override.status, "availability override", override.url, override.note
    ambiguous = [
        record
        for record in records
        if record.taxonomy_match_status == "ambiguous"
        and record.taxonomy_top_candidates
    ]
    if ambiguous:
        note = "Possible local taxonomy match: " + preferred(
            ambiguous, "taxonomy_top_candidates"
        )
        return "needs_review", "ambiguous taxonomy match", "", note
    return (
        "wanted",
        "not found in local holdings, BatLit, BHL, or a recorded work-level URL",
        "",
        "Online availability remains an absence-of-evidence result until reviewed.",
    )


def aggregate(
    records: Sequence[SourceRecord], overrides: Mapping[str, AvailabilityOverride]
) -> dict[str, str]:
    override = find_override(records, overrides)
    status, basis, override_url, review_note = classify(records, override)
    sources = unique_values(records, "source")
    name_ids = unique_values(records, "taxonomy_name_id")
    hmw_rows = unique_values(records, "hmw_source_row")
    scientific_count = sum(
        record.hmw_section == "Scientific Descriptions" for record in records
    )
    priority_score = len(name_ids) * 3 + len(hmw_rows) + scientific_count * 4
    formatted = preferred(records, "formatted_reference")
    raw = preferred(records, "raw_reference")
    url = override_url or preferred(records, "url")
    row = {
        "candidate_id": candidate_id(records),
        "status": status,
        "priority_score": str(priority_score),
        "source_count": str(len(sources)),
        "taxonomy_name_count": str(len(name_ids)),
        "hmw_reference_count": str(len(hmw_rows)),
        "sources": " | ".join(sources),
        "reference": markdown_reference(formatted or raw),
        "authors": preferred(records, "authors"),
        "year": preferred(records, "year"),
        "title": preferred(records, "title"),
        "container_title": preferred(records, "container_title"),
        "series": preferred(records, "series"),
        "volume": preferred(records, "volume"),
        "issue": preferred(records, "issue"),
        "pages": preferred(records, "pages"),
        "publisher": preferred(records, "publisher"),
        "place": preferred(records, "place"),
        "reference_type": " | ".join(unique_values(records, "reference_type")),
        "doi": preferred(records, "doi"),
        "url": url,
        "taxonomy_article_id": " | ".join(
            unique_values(records, "taxonomy_article_id")
        ),
        "taxonomy_article_kind": " | ".join(
            unique_values(records, "taxonomy_article_kind")
        ),
        "batlit_id": " | ".join(unique_values(records, "batlit_id")),
        "batlit_url": preferred(records, "batlit_url"),
        "batlit_zenodo_doi": preferred(records, "batlit_zenodo_doi"),
        "bhl_url": preferred(records, "bhl_url"),
        "taxonomy_name_ids": " | ".join(name_ids),
        "taxonomy_names": " | ".join(unique_values(records, "taxonomy_name")),
        "hmw_source_rows": " | ".join(hmw_rows),
        "hmw_sections": " | ".join(unique_values(records, "hmw_section")),
        "availability_basis": basis,
        "review_note": review_note,
        "raw_references": " | ".join(unique_values(records, "raw_reference")),
    }
    return row


def row_sort_key(row: Mapping[str, str]) -> tuple[Any, ...]:
    return (
        -int(row["priority_score"]),
        matcher.numeric_year(row["year"]) or 9999,
        normalized(row["reference"]),
    )


def write_csv(path: Path, rows: Iterable[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, rows: Sequence[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Bat literature most wanted",
        "",
        (
            "These references were not found in the local taxonomy library, BatLit, "
            "BHL, or recorded work-level URLs. Online availability should still be "
            "reviewed before publication."
        ),
        "",
    ]
    for index, row in enumerate(rows, start=1):
        reference = row["reference"] or "[Incomplete citation]"
        lines.append(f"{index}. {reference}")
        evidence = []
        if row["taxonomy_name_count"] != "0":
            evidence.append(f"{row['taxonomy_name_count']} taxonomy Name record(s)")
        if row["hmw_reference_count"] != "0":
            evidence.append(f"{row['hmw_reference_count']} HMW occurrence(s)")
        if evidence:
            lines.append(f"   - Demand evidence: {', '.join(evidence)}.")
        identifiers = []
        if row["doi"]:
            identifiers.append(f"DOI `{row['doi']}`")
        if row["taxonomy_article_id"]:
            identifiers.append(f"taxonomy Article {row['taxonomy_article_id']}")
        if identifiers:
            lines.append(f"   - Known identifiers: {', '.join(identifiers)}.")
        if row["review_note"]:
            lines.append(f"   - Review note: {row['review_note']}")
        lines.append("")
    path.write_text("\n".join(lines))


def citation_audit_rows(
    taxon_name: str,
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    root = Taxon.getter("valid_name")(taxon_name)
    if root is None:
        raise RuntimeError(f"Could not find Taxon {taxon_name!r}")
    names = [
        name
        for name in root.all_names_lazy()
        if name.original_citation is None and name.verbatim_citation
    ]
    reuse = Counter(name.verbatim_citation for name in names)
    audit = []
    manifest = []
    for name in names:
        assert name.verbatim_citation is not None
        parsed = parse_citations.parse_citation(name.verbatim_citation)
        existing = name.get_type_tag(TypeTag.StructuredVerbatimCitation)
        proposed = TypeTag.StructuredVerbatimCitation(
            series=parsed.series,
            volume=parsed.volume,
            issue=parsed.issue,
            start_page=parsed.start_page,
            end_page=parsed.end_page,
        )
        has_proposed_fields = any(
            value is not None
            for value in (
                parsed.series,
                parsed.volume,
                parsed.issue,
                parsed.start_page,
                parsed.end_page,
            )
        )
        status = (
            "structured"
            if existing is not None
            else "proposed_structured" if has_proposed_fields else "manual_review"
        )
        citation_group = name.get_citation_group()
        audit.append(
            {
                "name_id": str(name.id),
                "name": str(name),
                "status": status,
                "reuse_count": str(reuse[name.verbatim_citation]),
                "citation_group": str(citation_group or ""),
                "page_described": name.page_described or "",
                "series": parsed.series or "",
                "volume": parsed.volume or "",
                "issue": parsed.issue or "",
                "start_page": parsed.start_page or "",
                "end_page": parsed.end_page or "",
                "existing_structured": repr(existing) if existing is not None else "",
                "verbatim_citation": name.verbatim_citation,
            }
        )
        if existing is None and has_proposed_fields:
            manifest.append(
                {
                    "schema_version": 1,
                    "action": "add_tag",
                    "confidence": "high",
                    "reason": (
                        "Parse explicit series, volume, issue, or page fields from the "
                        "preserved verbatim citation without altering its wording."
                    ),
                    "evidence": [
                        {"kind": "verbatim_citation", "text": name.verbatim_citation}
                    ],
                    "object": {
                        "model": "Name",
                        "id": name.id,
                        "label": name.corrected_original_name,
                    },
                    "field": "type_tags",
                    "tag": proposed.serialize(),
                }
            )
    return audit, manifest


def write_citation_audit(
    path: Path, manifest_path: Path | None, taxon_name: str
) -> None:
    rows, manifest = citation_audit_rows(taxon_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, rows[0].keys() if rows else ())
        writer.writeheader()
        writer.writerows(rows)
    if manifest_path is not None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w") as file:
            for row in manifest:
                file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    print(
        f"Citation audit: {len(rows)} Names; {len(manifest)} guarded tag suggestions.",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taxon", default="Chiroptera")
    parser.add_argument("--hmw-csv", type=Path, default=DEFAULT_HMW_CSV)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--all-output", type=Path, default=DEFAULT_ALL_OUTPUT)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument(
        "--availability-overrides",
        type=Path,
        help="CSV with match_key,status,url,note; status is wanted, available_online, needs_review, or ignore.",
    )
    parser.add_argument("--skip-hmw", action="store_true")
    parser.add_argument("--skip-taxonomy", action="store_true")
    parser.add_argument(
        "--minimum-priority",
        type=int,
        default=1,
        help="Minimum priority_score for the publishable outputs (default: 1).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum publishable rows after priority sorting; full audit is unaffected.",
    )
    parser.add_argument("--citation-audit", type=Path)
    parser.add_argument("--citation-manifest", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.citation_manifest is not None and args.citation_audit is None:
        raise SystemExit("--citation-manifest requires --citation-audit")
    records: list[SourceRecord] = []
    if not args.skip_taxonomy:
        print(f"Reading original-citation data below {args.taxon}...", flush=True)
        records.extend(taxonomy_records(args.taxon))
    if not args.skip_hmw:
        print(f"Reading HMW references from {args.hmw_csv}...", flush=True)
        records.extend(hmw_records(args.hmw_csv))
    overrides = load_overrides(args.availability_overrides)
    rows = [aggregate(group, overrides) for group in group_records(records)]
    rows.sort(key=row_sort_key)
    wanted = [
        row
        for row in rows
        if row["status"] == "wanted"
        and int(row["priority_score"]) >= args.minimum_priority
    ]
    if args.limit is not None:
        wanted = wanted[: args.limit]
    review = [row for row in rows if row["status"] == "needs_review"]
    write_csv(args.output, wanted)
    write_csv(args.all_output, rows)
    write_markdown(args.markdown_output, wanted)
    if args.citation_audit is not None:
        write_citation_audit(args.citation_audit, args.citation_manifest, args.taxon)
    counts = Counter(row["status"] for row in rows)
    print(f"Input records: {len(records)}; deduplicated works: {len(rows)}")
    print(
        "Status counts: "
        + ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
    )
    print(f"Wrote {len(wanted)} wanted rows to {args.output}")
    print(f"Wrote {len(review)} review rows in the full audit at {args.all_output}")


if __name__ == "__main__":
    main()
