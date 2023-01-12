"""

Importing data from the NOW database.

Needs an import file "nowlocs.tsv", to be downloaded from
http://pantodon.science.helsinki.fi/now/locality_list.php

"""
import csv
from pathlib import Path
from typing import Dict, List
from taxonomy.db.models import Period
from taxonomy.db.models.location import Location, LocationTag

ROOT = Path(__file__).parent.parent


def load_locs() -> List[Dict[str, str]]:
    with (ROOT / "data_import/data/nowlocs.tsv").open("r") as f:
        rows = csv.DictReader(f, delimiter="\t")
        return [dict(row) for row in rows]


def link_locs(dry_run: bool = True) -> None:
    """Finds locations with matching names in NOW and links them.

    TODO look also at aliases in NOW.

    """
    recent = Period.get(name="Recent")
    for row in load_locs():
        name = row["NAME"]
        now_id = row["LIDNUM"]
        try:
            loc = Location.get(name=name)
        except Location.DoesNotExist:
            continue
        else:
            if loc.stratigraphic_unit == recent:
                print(f"{loc}: ignoring link to {now_id} {name}")
                continue
            tags = list(loc.get_tags(loc.tags, LocationTag.NOW))
            if not tags:
                print(f"{loc}: linked to {now_id} {name}")
                if not dry_run:
                    loc.add_tag(LocationTag.NOW(id=now_id))
                    loc.save()
            elif any(tag.id == now_id for tag in tags):
                pass
            else:
                print(f"{loc}: has now_ids {tags} but matches {now_id} {name}")
