"""

Interacting with AWS CloudSearch.

"""

from dataclasses import dataclass
import enum
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


@dataclass
class SearchField:
    field_type: SearchFieldType
    name: str
    facet_enabled: bool = False
    highlight_enabled: bool = False
    return_enabled: bool = False
    search_enabled: bool = True
    sort_enabled: bool | None = None

    def to_json(self) -> dict[str, Any]:
        # see https://docs.aws.amazon.com/cloudsearch/latest/developerguide/API_IndexField.html
        data: dict[str, Any] = {
            "IndexFieldName": self.name,
            "IndexFieldType": self.field_type.name.replace("_", "-"),
        }
        options = {
            "FacetEnabled": self.facet_enabled,
            "ReturnEnabled": self.return_enabled,
            "SearchEnabled": self.search_enabled,
            "HighlightEnabled": self.highlight_enabled,
        }
        sort_enabled = self.sort_enabled
        if sort_enabled is None:
            sort_enabled = self.field_type in {
                SearchFieldType.int,
                SearchFieldType.literal,
            }
        options["SortEnabled"] = sort_enabled
        camel_case = self.field_type.name.replace("_", " ").title().replace(" ", "")
        data[f"{camel_case}Options"] = options
        return data
