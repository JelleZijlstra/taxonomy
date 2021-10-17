from functools import lru_cache
import pprint
from typing import Any, Dict
import requests

DOMAIN = "http://openlibrary.org/"


@lru_cache()
def get_json(api: str, identifier: str) -> Dict[str, Any]:
    url = f"{DOMAIN}{api}/{identifier}.json"
    response = requests.get(url)
    try:
        data = response.json()
    except Exception as e:
        raise RuntimeError(response.status_code) from e
    else:
        pprint.pprint(data)
        return data


def get_author(identifier: str) -> Dict[str, Any]:
    result = get_json("authors", identifier)
    if result.get("type", {}).get("key") == "/type/redirect":
        ol_id = result["location"].split("/")[2]
        return get_author(ol_id)
    return result


def get_from_isbn(isbn: str) -> Dict[str, Any]:
    return get_json("isbn", isbn)
