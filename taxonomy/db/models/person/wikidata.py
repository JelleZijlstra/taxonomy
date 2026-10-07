"""Read-only Wikidata identity evidence and comparisons for Person records."""

from __future__ import annotations

import calendar
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any

import httpx

from taxonomy.apis import wikidata as api
from taxonomy.db import models
from taxonomy.db.constants import NamingConvention, PersonType

from .person import Person, is_valid_orcid, normalize_orcid

IDENTITY_TYPES = {PersonType.alias, PersonType.hard_redirect, PersonType.soft_redirect}
DATE_PROPERTIES = {"birth": "P569", "death": "P570"}
DISCOVERY_PROPERTIES = {"bhl_creator_id": "P4081", "bionomia_id": "P6944"}
GREGORIAN = "http://www.wikidata.org/entity/Q1985727"


def canonical_person(person: Person) -> Person:
    seen: set[int] = set()
    while person.type in IDENTITY_TYPES:
        if person.id in seen or person.target is None:
            raise ValueError(f"Broken or cyclic Person target chain at {person.id}")
        seen.add(person.id)
        person = person.target
    if person.type is PersonType.deleted:
        raise ValueError(f"Person target {person.id} is deleted")
    return person


def stored_qids(person: Person) -> set[str]:
    return {
        tag.text for tag in person.get_tags(person.tags, models.tags.PersonTag.Wikidata)
    }


def effective_qids(person: Person) -> set[str]:
    return stored_qids(canonical_person(person))


def statements(entity: dict[str, Any], prop: str) -> list[dict[str, Any]]:
    return [
        statement
        for statement in entity.get("claims", {}).get(prop, [])
        if statement.get("rank") != "deprecated"
    ]


def statement_value(statement: dict[str, Any]) -> Any:
    snak = statement.get("mainsnak", {})
    if snak.get("snaktype") != "value":
        return None
    return snak.get("datavalue", {}).get("value")


def is_human(entity: dict[str, Any]) -> bool:
    instances = [statement_value(s) for s in statements(entity, "P31")]
    # Additional classifications, such as national hero, do not negate human.
    return any(
        isinstance(value, dict) and value.get("id") == "Q5" for value in instances
    )


def requires_human_item(person: Person) -> bool:
    return person.naming_convention is not NamingConvention.organization


def can_use_item(person: Person, entity: dict[str, Any]) -> bool:
    """Respect a reviewed type exception without suppressing the other checks."""
    return (
        not requires_human_item(person)
        or is_human(entity)
        or any(
            tag.label == "wikidata_entity_type"
            for tag in person.get_tags(person.tags, models.tags.PersonTag.IgnoreLint)
        )
    )


# These sourcing qualifiers describe calendar notation, not uncertainty in the
# year. In particular Brandt's birth uses Q27055388. Keep other qualifiers out of
# automatic imports, while still showing/comparing their stated years.
CALENDAR_QUALIFIERS = {"Q27055388", "Q26877143", "Q18195782"}
JULIAN = "http://www.wikidata.org/entity/Q1985786"


def _statement_year(statement: dict[str, Any]) -> str | None:
    value = statement_value(statement)
    if not isinstance(value, dict) or value.get("precision") not in (9, 10, 11):
        return None
    if value.get("calendarmodel") not in (GREGORIAN, JULIAN):
        return None
    time = value.get("time", "")
    match = (
        re.fullmatch(r"([+-][0-9]{4,})-([0-9]{2})-([0-9]{2})T00:00:00Z", time)
        if isinstance(time, str)
        else None
    )
    if match is None:
        return None
    year, month, day = map(int, match.groups())
    if year == 0:
        return None
    precision = value["precision"]
    if precision >= 10 and not 1 <= month <= 12:
        return None
    if precision == 11:
        leap = (
            year % 4 == 0 if value["calendarmodel"] == JULIAN else calendar.isleap(year)
        )
        days = (31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
        if not 1 <= day <= days[month - 1]:
            return None
    return str(year)


def _year_is_unambiguous(statement: dict[str, Any]) -> bool:
    if _statement_year(statement) is None:
        return False
    value = statement_value(statement)
    if value.get("before", 0) != 0 or value.get("after", 0) != 0:
        return False
    for prop, snaks in (statement.get("qualifiers") or {}).items():
        # Age, timezone, time of day, source context and ranking provenance do
        # not make the stated year uncertain.
        if prop in {"P3629", "P421", "P4241", "P805", "P7452"}:
            continue
        if prop != "P1480" or any(
            snak.get("datavalue", {}).get("value", {}).get("id")
            not in CALENDAR_QUALIFIERS
            for snak in snaks
        ):
            return False
    return True


def _describe_time(value: Any) -> str:
    if not isinstance(value, dict):
        return f"unrecognized date value {value!r}"
    time = value.get("time")
    precision = value.get("precision")
    precision_label = {
        6: "millennium",
        7: "century",
        8: "ten-year",
        9: "year",
        10: "month",
        11: "day",
    }.get(precision if isinstance(precision, int) else -1)
    details = [
        (
            f"{precision_label} precision"
            if precision_label
            else f"precision {precision!r}"
        )
    ]
    if isinstance(time, str) and re.fullmatch(
        r"[+-][0-9]{4,}-[0-9]{2}-[0-9]{2}T00:00:00Z", time
    ):
        time = time.removeprefix("+").removesuffix("T00:00:00Z")
        if precision in (6, 7, 8, 9):
            time = time.rsplit("-", 2)[0]
        elif precision == 10:
            time = time.rsplit("-", 1)[0]
    calendar_model = value.get("calendarmodel")
    if calendar_model == JULIAN:
        details.append("Julian calendar")
    elif calendar_model != GREGORIAN:
        details.append(f"unsupported calendar {calendar_model!r}")
    if value.get("before", 0) or value.get("after", 0):
        details.append(
            f"uncertainty: before={value.get('before', 0)}, after={value.get('after', 0)}"
        )
    return f"{time!s} ({'; '.join(details)})"


def describe_date_statement(statement: dict[str, Any]) -> str:
    """Describe the source date even when it cannot supply an exact year."""
    value = statement_value(statement)
    if value is None:
        return "no known date"
    parts = [_describe_time(value)]
    if _statement_year(statement) is None:
        parts.append("cannot extract an exact year")
    qualifiers = statement.get("qualifiers") or {}
    for prop, label in (("P1319", "earliest date"), ("P1326", "latest date")):
        for snak in qualifiers.get(prop, []):
            parts.append(
                f"{label}: {_describe_time(snak.get('datavalue', {}).get('value'))}"
            )
    other_qualifiers = sorted(set(qualifiers) - {"P1319", "P1326"})
    if other_qualifiers:
        parts.append(f"qualifiers: {', '.join(other_qualifiers)}")
    return "; ".join(parts)


def compare_date(local: str | None, claims: list[dict[str, Any]]) -> dict[str, Any]:
    # Unknown/no-value statements are absence of evidence, not disagreements.
    dated_claims = []
    for claim in claims:
        value = statement_value(claim)
        if value is None:
            continue
        if isinstance(value, dict) and isinstance(value.get("precision"), int):
            if 0 <= value["precision"] < 9:
                continue  # A century or decade alone cannot supply a birth/death year.
        dated_claims.append(claim)
    values = [_statement_year(statement) for statement in dated_claims]
    years = {value for value in values if value is not None}
    unambiguous = bool(dated_claims) and all(
        _year_is_unambiguous(claim) for claim in dated_claims
    )
    result: dict[str, Any] = {
        "local": local,
        "external": values,
        "statements": claims,
        "unambiguous": unambiguous,
        "suggested": None,
    }
    if not dated_claims:
        status = "no_external_value"
    elif len(years) != 1 or None in values:
        status = "review"
    else:
        year = next(iter(years))
        local_match = re.fullmatch(r"(-?[0-9]+)(?:-[0-9]{2}-[0-9]{2})?", local or "")
        if local_match and str(int(local_match[1])) == year:
            status = "match"
        elif not unambiguous:
            status = "review"
        elif not local:
            status = "missing_local"
            result["suggested"] = year
        elif local_match:
            status = "conflict"
        else:
            status = "review"
    result["status"] = status
    return result


def compare_identifier(local: set[str], claims: list[dict[str, Any]]) -> dict[str, Any]:
    values = [statement_value(statement) for statement in claims]
    if not claims:
        status = "no_external_value"
    elif any(not isinstance(value, str) or not value for value in values):
        status = "review"
    elif local == set(values):
        status = "match"
    elif len(set(values)) != 1 or len(local) > 1:
        status = "review"
    elif any(statement.get("qualifiers") for statement in claims):
        status = "review"
    elif not local:
        status = "missing_local"
    else:
        status = "conflict"
    return {
        "status": status,
        "local": sorted(local),
        "external": values,
        "statements": claims,
    }


def compare_openlibrary(person: Person, claims: list[dict[str, Any]]) -> dict[str, Any]:
    local = person.get_ol_ids()
    values = [statement_value(statement) for statement in claims]
    valid = {
        value
        for value in values
        if isinstance(value, str) and re.fullmatch(r"OL[1-9][0-9]*A", value)
    }
    all_ids = local | valid
    result: dict[str, Any] = {
        "local": sorted(local),
        "primary": person.ol_id,
        "external": values,
        "statements": claims,
        "suggested": None,
        "missing": sorted(valid - local),
        "links": [f"https://openlibrary.org/authors/{oid}" for oid in sorted(all_ids)],
    }
    if not claims:
        status = "no_external_value"
    elif (
        any(value not in valid for value in values if isinstance(value, str))
        or any(not isinstance(value, str) for value in values)
        # P1810 (database name) and P3740 (work count) are descriptive
        # metadata, not limits on the identifier's applicability.
        or any(
            prop not in {"P1810", "P3740"}
            for statement in claims
            for prop in statement.get("qualifiers") or {}
        )
    ):
        status = "review"
    elif not person.ol_id:
        if len(all_ids) == 1:
            status = "missing_local"
            result["suggested"] = next(iter(all_ids))
        else:
            status = "choose_primary"
    elif valid - local:
        status = "missing_additional"
    else:
        status = "match"
    result["status"] = status
    return result


def compare_person(person: Person, entity: dict[str, Any]) -> dict[str, Any]:
    comparisons = {
        field: compare_date(getattr(person, field), statements(entity, prop))
        for field, prop in DATE_PROPERTIES.items()
    }
    comparisons["orcid"] = compare_identifier(
        {
            normalize_orcid(tag.text)
            for tag in person.get_tags(person.tags, models.tags.PersonTag.ORCID)
        },
        statements(entity, "P496"),
    )
    comparisons["ol_id"] = compare_openlibrary(person, statements(entity, "P648"))
    for value in comparisons["orcid"]["external"]:
        if not isinstance(value, str) or not is_valid_orcid(value):
            comparisons["orcid"]["status"] = "review"
    for field, comparison in comparisons.items():
        if has_review(person, entity["id"], field, comparison):
            comparison["status"] = "reviewed"
    return comparisons


def statement_hash(claims: list[dict[str, Any]]) -> str:
    """Fingerprint reviewed evidence independently of statement order."""
    serialized = sorted(json.dumps(claim, sort_keys=True) for claim in claims)
    return hashlib.sha256(json.dumps(serialized).encode()).hexdigest()


def review_value(local: str | list[str] | None) -> str:
    return ",".join(sorted(local)) if isinstance(local, list) else local or ""


def has_review(
    person: Person, qid: str, field: str, comparison: dict[str, Any]
) -> bool:
    return any(
        tag.field == field
        and tag.local_value == review_value(comparison["local"])
        and tag.wikidata_id == qid
        and tag.statement_hash == statement_hash(comparison["statements"])
        and bool(tag.comment.strip())
        for tag in person.get_tags(person.tags, models.tags.PersonTag.WikidataReview)
    )


def local_snapshot(person: Person) -> dict[str, Any]:
    return {
        "id": person.id,
        "name": person.get_full_name(),
        "type": person.type.name,
        "target_id": person.target.id if person.target is not None else None,
        "birth": person.birth,
        "death": person.death,
        "ol_id": person.ol_id,
        "tags": [tag.serialize() for tag in person.tags],
    }


def report_person(person: Person, sources: Iterable[Person]) -> dict[str, Any]:
    sources = list(sources)
    row: dict[str, Any] = {
        "person": local_snapshot(person),
        "sources": [local_snapshot(source) for source in sources],
        "wikipedia": [],
        "items": [],
        "issues": [],
        "errors": [],
        "candidate_qid": None,
    }
    qids = set().union(*(stored_qids(source) for source in sources))
    for source in sources:
        if source.id != person.id and stored_qids(source):
            row["issues"].append(
                f"Wikidata tag is on alias/redirect Person {source.id}"
            )
    urls = sorted(
        {
            tag.text
            for source in sources
            for tag in source.get_tags(source.tags, models.tags.PersonTag.Wiki)
        }
    )
    for url in urls:
        try:
            link = api.resolve_wikipedia(url)
            row["wikipedia"].append(
                {
                    "url": url,
                    "qid": link.qid,
                    "needs_review": link.needs_review,
                    "snapshot": asdict(link.snapshot),
                }
            )
            if link.qid is None:
                row["issues"].append(f"No Wikidata item for {url}")
            else:
                qids.add(link.qid)
            if link.needs_review:
                row["issues"].append(f"Wikipedia page needs identity review: {url}")
        except (httpx.HTTPError, ValueError) as exc:
            row["errors"].append(f"{url}: {exc}")
    canonical_qids: set[str] = set()
    for qid in sorted(qids):
        try:
            item = api.get_item(qid)
            evidence: dict[str, Any] = {
                "requested_qid": qid,
                "snapshot": asdict(item.snapshot),
            }
            row["items"].append(evidence)
            entity = item.entity
            if entity is None:
                row["issues"].append(f"Missing Wikidata item: {qid}")
                continue
            resolved_qid = entity["id"]
            canonical_qids.add(resolved_qid)
            if qid != resolved_qid:
                row["issues"].append(f"Wikidata {qid} redirects to {resolved_qid}")
            evidence["qid"] = resolved_qid
            evidence["revision_url"] = (
                f"https://www.wikidata.org/w/index.php?oldid={entity['lastrevid']}"
            )
            evidence["human"] = is_human(entity)
            if not can_use_item(person, entity):
                row["issues"].append(
                    f"{resolved_qid} is not unambiguously an individual human"
                )
                continue
            evidence["comparisons"] = compare_person(person, entity)
            evidence["discovery"] = {
                field: statements(entity, prop)
                for field, prop in DISCOVERY_PROPERTIES.items()
            }
            for field, comparison in evidence["comparisons"].items():
                if comparison["status"] in {"conflict", "review", "choose_primary"}:
                    row["issues"].append(
                        f"{resolved_qid}: {field} {comparison['status']}"
                    )
        except (httpx.HTTPError, ValueError) as exc:
            row["errors"].append(f"{qid}: {exc}")
    if len(canonical_qids) > 1:
        row["issues"].append("Links resolve to multiple Wikidata identities")
    if len(canonical_qids) == 1 and not row["issues"] and not row["errors"]:
        row["candidate_qid"] = next(iter(canonical_qids))
    row["status"] = (
        "error" if row["errors"] else "review" if row["issues"] else "compared"
    )
    return row


def report_people(
    people: Iterable[Person],
    *,
    person_ids: set[int] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Group aliases with canonical people; retain every selected failure as a row.

    Duplicate detection includes all stored QIDs and all identities resolved in
    this run. Wikipedia links outside a limited run have not been resolved.
    """
    groups: dict[int, list[Person]] = defaultdict(list)
    canonical: dict[int, Person] = {}
    owners: dict[str, set[int]] = defaultdict(set)
    rows: list[dict[str, Any]] = []
    found_ids: set[int] = set()
    for person in people:
        found_ids.add(person.id)
        try:
            identity = canonical_person(person)
        except ValueError as exc:
            if (person_ids is not None and person.id in person_ids) or (
                person_ids is None
                and (
                    stored_qids(person)
                    or any(person.get_tags(person.tags, models.tags.PersonTag.Wiki))
                )
            ):
                rows.append(
                    {
                        "person": local_snapshot(person),
                        "status": "error",
                        "errors": [str(exc)],
                        "issues": [],
                        "items": [],
                        "candidate_qid": None,
                    }
                )
            continue
        groups[identity.id].append(person)
        canonical[identity.id] = identity
        for qid in stored_qids(person):
            owners[qid].add(identity.id)
    if person_ids is not None and (missing := person_ids - found_ids):
        raise ValueError(f"Unknown or deleted Person IDs: {sorted(missing)}")
    selected = 0
    for person_id, sources in sorted(groups.items()):
        if person_ids is not None and not any(
            source.id in person_ids for source in sources
        ):
            continue
        if not any(
            stored_qids(source)
            or any(source.get_tags(source.tags, models.tags.PersonTag.Wiki))
            for source in sources
        ):
            if person_ids is not None:
                rows.append(
                    {
                        "person": local_snapshot(canonical[person_id]),
                        "status": "no_links",
                        "errors": [],
                        "issues": [],
                        "items": [],
                        "candidate_qid": None,
                    }
                )
            continue
        if limit is not None and selected >= limit:
            break
        selected += 1
        row = report_person(canonical[person_id], sources)
        rows.append(row)
        for item in row["items"]:
            if "qid" in item:
                owners[item["qid"]].add(person_id)
    for row in rows:
        for item in row["items"]:
            if "qid" in item and len(owners[item["qid"]]) > 1:
                row["issues"].append(
                    f"{item['qid']} is linked to canonical Persons {sorted(owners[item['qid']])}"
                )
                row["candidate_qid"] = None
                if row["status"] != "error":
                    row["status"] = "review"
    return rows
