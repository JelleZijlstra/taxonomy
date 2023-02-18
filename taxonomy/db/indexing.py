import json
import re
import time
from collections.abc import Iterable
from typing import Any

import boto3
from botocore.config import Config

from taxonomy import getinput
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.config import get_options
from taxonomy.db.models.base import BaseModel

BATCH_LENGTH_LIMIT = 5 * 1024 * 1024  # 5 MB
LIMIT_WITH_BUFFER = 0.95 * BATCH_LENGTH_LIMIT


def _clean_string(v: object) -> object:
    if isinstance(v, str):
        return re.sub(r"[^\u0009\u000a\u000d\u0020-\uD7FF\uE000-\uFFFD]", "", v)
    elif isinstance(v, list):
        return [_clean_string(elt) for elt in v]
    else:
        return v


def generate_indexing_requests(limit: int | None = None) -> Iterable[dict[str, Any]]:
    for cls in BaseModel.__subclasses__():
        if not cls.search_fields:
            continue
        for obj in getinput.print_every_n(
            cls.select_valid().limit(limit), label=cls.__name__
        ):
            default_id = f"{cls.call_sign.lower()}/{obj.id}"
            for raw_dict in obj.get_search_dicts():
                # Filter out null, empty strings, empty lists, etc.
                data_dict = {k: _clean_string(v) for k, v in raw_dict.items() if v}
                data_dict.setdefault("call_sign", cls.call_sign)
                full_dict = {
                    "type": "add",
                    "id": data_dict.pop("id", default_id),
                    "fields": data_dict,
                }
                yield full_dict


def get_all_fields() -> Iterable[SearchField]:
    call_sign = SearchField(SearchFieldType.literal, "call_sign")
    name_to_field: dict[str, tuple[SearchField, type[BaseModel]]] = {
        "call_sign": (call_sign, BaseModel)
    }
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
            yield field


def generate_field_configs() -> Iterable[dict[str, Any]]:
    for field in get_all_fields():
        yield field.to_json()


def compact_dump(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), indent=None).encode("utf-8")


def generate_batches(limit: int | None = None) -> Iterable[bytes]:
    current_batch: list[bytes] = []
    current_batch_length = 0
    for request in generate_indexing_requests(limit):
        dumped = compact_dump(request)
        if current_batch_length + len(dumped) > LIMIT_WITH_BUFFER:
            yield b"[" + b",".join(current_batch) + b"]"
            current_batch = [dumped]
            current_batch_length = len(dumped)
        else:
            current_batch.append(dumped)
            current_batch_length += len(dumped)

    if current_batch:
        yield b"[" + b",".join(current_batch) + b"]"


def _get_client(service: str = "cloudsearch", **kwargs: object) -> Any:
    options = get_options()
    return boto3.client(
        service,
        config=Config(region_name="us-east-1"),
        aws_access_key_id=options.aws_key,
        aws_secret_access_key=options.aws_secret_key,
        **kwargs,
    )


def create_index_fields() -> None:
    options = get_options()
    client = _get_client("cloudsearch")
    for field in generate_field_configs():
        print("Define field", field)
        result = client.define_index_field(
            DomainName=options.aws_cloudsearch_domain, IndexField=field
        )
        print(result)


def run_indexing(limit: int | None = None, batch_offset: int | None = None) -> None:
    options = get_options()
    client = _get_client(
        "cloudsearchdomain", endpoint_url=options.aws_cloudsearch_document_endpoint
    )
    for i, batch in enumerate(
        getinput.print_every_n(generate_batches(limit=limit), n=1, label="batches")
    ):
        if batch_offset is not None and i < batch_offset:
            continue
        response = client.upload_documents(
            documents=batch, contentType="application/json"
        )
        for warning in response.get("warnings", []):
            print(warning)
        time.sleep(10)
