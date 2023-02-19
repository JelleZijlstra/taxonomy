"""

Interacting with AWS CloudSearch.

"""

import enum
from dataclasses import dataclass
from typing import Any


class SearchFieldType(enum.Enum):
    date = 1
    date_array = 2
    double = 3
    double_array = 4
    int = 5
    int_array = 6
    latlon = 7
    literal = 8
    literal_array = 9
    text = 10
    text_array = 11

    def supports_facet(self) -> bool:
        return self in {
            SearchFieldType.int,
            SearchFieldType.double,
            SearchFieldType.literal,
            SearchFieldType.date,
            SearchFieldType.latlon,
            SearchFieldType.int_array,
            SearchFieldType.int_array,
            SearchFieldType.double_array,
            SearchFieldType.literal_array,
            SearchFieldType.date_array,
        }

    def supports_sort(self) -> bool:
        return self in {
            SearchFieldType.int,
            SearchFieldType.double,
            SearchFieldType.literal,
            SearchFieldType.text,
            SearchFieldType.date,
            SearchFieldType.latlon,
        }

    def supports_highlight(self) -> bool:
        return self in {SearchFieldType.text, SearchFieldType.text_array}

    def supports_search(self) -> bool:
        return self not in {SearchFieldType.text, SearchFieldType.text_array}


@dataclass
class SearchField:
    field_type: SearchFieldType
    name: str
    facet_enabled: bool = False
    highlight_enabled: bool | None = None
    return_enabled: bool | None = None
    search_enabled: bool = True
    sort_enabled: bool | None = None

    def get_weight(self) -> int:
        if self.name == "name":
            return 100
        else:
            return 1

    def get_highlight_enabled(self) -> bool | None:
        if not self.field_type.supports_highlight():
            return None
        if self.highlight_enabled is not None:
            return self.highlight_enabled
        return True

    def get_return_enabled(self) -> bool | None:
        if self.return_enabled is not None:
            return self.return_enabled
        return not self.field_type.supports_highlight()

    def get_search_enabled(self) -> bool | None:
        if not self.field_type.supports_search():
            return None
        return self.search_enabled

    def get_facet_enabled(self) -> bool | None:
        if not self.field_type.supports_facet():
            return None
        return self.facet_enabled

    def get_sort_enabled(self) -> bool | None:
        if not self.field_type.supports_sort():
            return None
        if self.sort_enabled is None:
            return self.field_type in {SearchFieldType.int, SearchFieldType.literal}
        return self.sort_enabled

    def to_json(self) -> dict[str, Any]:
        # see https://docs.aws.amazon.com/cloudsearch/latest/developerguide/API_IndexField.html
        data: dict[str, Any] = {
            "IndexFieldName": self.name,
            "IndexFieldType": self.field_type.name.replace("_", "-"),
        }
        options = {
            "ReturnEnabled": self.get_return_enabled(),
            "HighlightEnabled": self.get_highlight_enabled(),
            "SearchEnabled": self.get_search_enabled(),
            "FacetEnabled": self.get_facet_enabled(),
            "SortEnabled": self.get_sort_enabled(),
        }
        camel_case = self.field_type.name.replace("_", " ").title().replace(" ", "")
        if camel_case == "Latlon":
            camel_case = "LatLon"
        data[f"{camel_case}Options"] = {
            k: v for k, v in options.items() if v is not None
        }
        return data
