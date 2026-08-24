import json
from unittest.mock import Mock

import httpx
import pytest

from taxonomy.apis import openstreetmap


def test_lookup_element(monkeypatch: pytest.MonkeyPatch) -> None:
    get_data = Mock(
        return_value=json.dumps(
            {
                "elements": [
                    {
                        "type": "relation",
                        "id": 2554044,
                        "tags": {"boundary": "historic", "name": "New London County"},
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(openstreetmap, "get_openstreetmap_data", get_data)

    assert openstreetmap.lookup_element("relation", 2554044) == openstreetmap.Element(
        osm_type="relation",
        osm_id=2554044,
        tags={"boundary": "historic", "name": "New London County"},
    )
    get_data.assert_called_once_with(
        "https://api.openstreetmap.org/api/0.6/relation/2554044.json"
    )


def test_lookup_missing_element(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        openstreetmap, "get_openstreetmap_data", Mock(return_value="null")
    )

    assert openstreetmap.lookup_element("relation", 1234) is None


def test_get_openstreetmap_data_caches_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = Mock(status_code=404)
    monkeypatch.setattr(httpx, "get", Mock(return_value=response))
    undecorated = openstreetmap.get_openstreetmap_data.__wrapped__  # type: ignore[attr-defined]

    assert undecorated("https://example.com/relation/1.json") == "null"
    response.raise_for_status.assert_not_called()
