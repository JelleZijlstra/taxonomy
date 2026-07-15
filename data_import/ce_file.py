from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any, cast

from data_import import lib
from taxonomy.db import constants, helpers, models
from taxonomy.db.models.classification_entry.ce import ClassificationEntryTag


class CEFileError(ValueError):
    pass


_ENUM_FIELDS: Mapping[str, type[constants.Rank | constants.AgeClass]] = {
    "rank": constants.Rank,
    "parent_rank": constants.Rank,
    "age_class": constants.AgeClass,
}
_REQUIRED_FIELDS = frozenset({"article", "page", "name", "rank"})
_KNOWN_FIELDS = frozenset(lib.CEDict.__annotations__)


def _serialize_value(key: str, value: Any) -> Any:
    if key in _ENUM_FIELDS and value is not None:
        return value.name
    if key == "article":
        return value.name
    if key == "tags":
        return [{"kind": type(tag).__name__, "data": tag.serialize()} for tag in value]
    return value


def serialize_ce(ce: lib.CEDict) -> dict[str, Any]:
    """Convert a CEDict to a stable, human-readable JSON object."""
    return {key: _serialize_value(key, value) for key, value in ce.items()}


def write_ce_file(path: Path, ces: Iterable[lib.CEDict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for ce in ces:
            json.dump(serialize_ce(ce), file, ensure_ascii=False, sort_keys=True)
            file.write("\n")


def _deserialize_enum(
    enum_type: type[constants.Rank | constants.AgeClass], value: Any, *, field: str
) -> constants.Rank | constants.AgeClass:
    if not isinstance(value, str):
        raise CEFileError(f"{field} must be an enum name, got {value!r}")
    try:
        return enum_type[value]
    except KeyError:
        choices = ", ".join(member.name for member in enum_type)
        raise CEFileError(
            f"unknown {field} {value!r}; expected one of: {choices}"
        ) from None


def deserialize_ce(data: object, *, line_number: int) -> lib.CEDict:
    if not isinstance(data, dict):
        raise CEFileError(f"line {line_number}: expected a JSON object")
    unknown = set(data) - _KNOWN_FIELDS
    if unknown:
        raise CEFileError(f"line {line_number}: unknown fields: {sorted(unknown)}")
    missing = _REQUIRED_FIELDS - set(data)
    if missing:
        raise CEFileError(f"line {line_number}: missing fields: {sorted(missing)}")

    output = dict(data)
    for key, enum_type in _ENUM_FIELDS.items():
        if key in output and output[key] is not None:
            output[key] = _deserialize_enum(enum_type, output[key], field=key)
    article_name = output["article"]
    if not isinstance(article_name, str):
        raise CEFileError(f"line {line_number}: article must be a string")
    try:
        output["article"] = models.Article.get(name=article_name)
    except models.Article.DoesNotExist:
        raise CEFileError(
            f"line {line_number}: no Article named {article_name!r}"
        ) from None

    if "tags" in output:
        raw_tags = output["tags"]
        if not isinstance(raw_tags, list):
            raise CEFileError(f"line {line_number}: tags must be a list")
        tags = []
        for raw_tag in raw_tags:
            if not isinstance(raw_tag, dict) or set(raw_tag) != {"kind", "data"}:
                raise CEFileError(f"line {line_number}: invalid tag {raw_tag!r}")
            tag = ClassificationEntryTag.unserialize(raw_tag["data"])
            if type(tag).__name__ != raw_tag["kind"]:
                raise CEFileError(
                    f"line {line_number}: tag kind does not match serialized data"
                )
            tags.append(tag)
        output["tags"] = tags
    return cast(lib.CEDict, output)


def read_ce_file(path: Path) -> list[lib.CEDict]:
    ces = []
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                raise CEFileError(f"line {line_number}: {exc.msg}") from exc
            ces.append(deserialize_ce(data, line_number=line_number))
    if not ces:
        raise CEFileError("CE file is empty")
    article_ids = {ce["article"].id for ce in ces}
    if len(article_ids) != 1:
        raise CEFileError("all entries in a CE file must use the same article")
    return ces


def validate_structure(ces: Iterable[lib.CEDict]) -> list[lib.CEDict]:
    entries = list(ces)
    return list(lib.validate_ce_parents(entries))


def _matching_names(ce: lib.CEDict, name: str) -> list[models.Name]:
    group = helpers.group_of_rank(ce["rank"])
    return list(
        models.Name.select_valid().filter(
            models.Name.corrected_original_name == name, models.Name.group == group
        )
    )


def iter_name_match_results(
    ces: Iterable[lib.CEDict],
) -> Iterator[tuple[str, lib.CEDict, list[models.Name]]]:
    for ce in ces:
        exact = _matching_names(ce, ce["name"])
        if exact:
            yield ("exact" if len(exact) == 1 else "ambiguous", ce, exact)
            continue
        normalized_name = ce.get("corrected_name")
        if normalized_name is not None:
            normalized = _matching_names(ce, normalized_name)
            if normalized:
                yield (
                    "normalized" if len(normalized) == 1 else "ambiguous",
                    ce,
                    normalized,
                )
                continue
        yield "unrecognized", ce, []


def print_validation_report(ces: Iterable[lib.CEDict]) -> bool:
    entries = list(ces)
    results = list(iter_name_match_results(entries))
    counts = Counter(status for status, _ce, _matches in results)
    print(f"Validated {len(entries)} classification entries")
    print(
        "Ranks:",
        ", ".join(
            f"{rank.name}={count}"
            for rank, count in sorted(
                Counter(ce["rank"] for ce in entries).items(),
                key=lambda item: item[0].value,
            )
        ),
    )
    print(
        "Name mapping:",
        ", ".join(
            f"{status}={counts[status]}"
            for status in ("exact", "normalized", "ambiguous", "unrecognized")
        ),
    )
    for status, ce, matches in results:
        if status == "exact":
            continue
        suffix = ""
        if status == "normalized":
            suffix = f" -> {ce['corrected_name']}"
        elif matches:
            suffix = " -> " + ", ".join(str(name) for name in matches)
        print(f"[{status}] {ce['rank'].name} {ce['name']}{suffix} (page {ce['page']})")
    return counts["ambiguous"] == 0 and counts["unrecognized"] == 0
