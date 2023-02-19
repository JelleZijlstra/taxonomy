import json
from functools import cache
from typing import Any

import boto3
from botocore.config import Config

from taxonomy.apis.cloud_search import SearchFieldType
from taxonomy.config import get_options
from taxonomy.db.indexing import get_all_fields

HIGHLIGHT_CONFIG = {
    "max_phrases": 3,
    "format": "text",
    "pre_tag": "**",
    "post_tag": "**",
}


@cache
def get_client() -> Any:
    options = get_options()
    return boto3.client(
        "cloudsearchdomain",
        endpoint_url=options.aws_cloudsearch_search_endpoint,
        config=Config(region_name="us-east-1"),
        aws_access_key_id=options.aws_key,
        aws_secret_access_key=options.aws_secret_key,
    )


@cache
def get_highlight_param() -> str:
    highlights = {
        field.name: HIGHLIGHT_CONFIG
        for field in get_all_fields()
        if field.get_highlight_enabled()
    }
    return json.dumps(highlights, indent=None, separators=(",", ":"))


@cache
def get_options_param() -> str:
    options = {
        "fields": [
            f"{field.name}^{field.get_weight()}"
            for field in get_all_fields()
            if field.field_type in (SearchFieldType.text, SearchFieldType.text_array)
        ]
    }
    return json.dumps(options, indent=None, separators=(",", ":"))


def run_query(query: str, size: int = 10, start: int = 0) -> dict[str, Any]:
    client = get_client()
    response = client.search(
        query=query,
        queryParser="simple",
        highlight=get_highlight_param(),
        queryOptions=get_options_param(),
        size=size,
        start=start,
    )
    return response["hits"]
