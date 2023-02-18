import json
from collections.abc import Iterable
from typing import Any

from taxonomy.apis.cloud_search import SearchField
from taxonomy.db.models.base import BaseModel

BATCH_LENGTH_LIMIT = 5 * 1024 * 1024  # 5 MB
LIMIT_WITH_BUFFER = 0.95 * BATCH_LENGTH_LIMIT


def generate_indexing_requests(limit: int | None = None) -> Iterable[dict[str, Any]]:
    for cls in BaseModel.__subclasses__():
        if not cls.search_fields:
            continue
        for obj in cls.select_valid().limit(limit):
            default_id = f"{cls.call_sign.lower()}/{obj.id}"
            for raw_dict in obj.get_search_dicts():
                # Filter out null, empty strings, empty lists, etc.
                data_dict = {k: v for k, v in raw_dict.items() if v}
                data_dict.setdefault("call_sign", cls.call_sign)
                full_dict = {
                    "type": "add",
                    "id": data_dict.pop("id", default_id),
                    "fields": data_dict,
                }
                yield full_dict


def generate_field_configs() -> Iterable[dict[str, Any]]:
    name_to_field: dict[str, tuple[SearchField, type[BaseModel]]] = {}
    for cls in BaseModel.__subclasses__():
        for field in cls.search_fields:
            if field.name in name_to_field:
                existing_field, existing_cls = name_to_field[field.name]
                if existing_field != field:
                    raise ValueError(
                        f"Duplicate field name {field.name!r} in"
                        f" {existing_cls.__name__!r} and {cls.__name__!r}"
                    )
                continue
            name_to_field[field.name] = field, cls
            yield field.to_json()


def compact_dump(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), indent=None)


def generate_batches(limit: int | None = None) -> Iterable[str]:
    current_batch: list[str] = []
    current_batch_length = 0
    for request in generate_indexing_requests(limit):
        dumped = compact_dump(request)
        if current_batch_length + len(dumped) > LIMIT_WITH_BUFFER:
            yield "[" + ",".join(current_batch) + "]"
            current_batch = [dumped]
            current_batch_length = len(dumped)
        else:
            current_batch.append(dumped)
            current_batch_length += len(dumped)

    if current_batch:
        yield "[" + ",".join(current_batch) + "]"
