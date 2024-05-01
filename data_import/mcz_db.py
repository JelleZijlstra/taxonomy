import csv
import sys
from collections.abc import Iterable

from taxonomy.db.models import Collection, Name, TypeTag

from .lib import DATA_DIR, get_type_specimens


def get_hesp_data(*, fossil: bool) -> dict[str, list[Name]]:
    mcz = Collection.getter("label")("MCZ")
    assert mcz is not None
    return get_type_specimens(mcz)


def get_mcz_db(*, fossil: bool) -> Iterable[dict[str, str]]:
    path = "mcz-vp.csv" if fossil else "mcz-mammals.csv"
    with (DATA_DIR / path).open() as f:
        rows = csv.DictReader(f)
        yield from sorted(rows, key=lambda row: row["TYPESTATUS"])


def main(*, dry_run: bool = True, fossil: bool = False) -> None:
    hesp_data = get_hesp_data(fossil=fossil)
    for row in get_mcz_db(fossil=fossil):
        if row["TOPTYPESTATUS"] in ("Paratype", "Topotype", "Paralectotype"):
            continue
        cat_num = f"MCZ {row['CAT_NUM']}"
        if "VPF-" in cat_num:  # fish
            continue
        url = f"https://mczbase.mcz.harvard.edu/guid/{row['GUID']}"
        if cat_num not in hesp_data:
            print(f"Cannot find: {cat_num}: {row['TYPESTATUS']}, {url}")
            continue
        for nam in hesp_data[cat_num]:
            tag = TypeTag.TypeSpecimenLink(url)
            if tag in nam.type_tags:
                continue
            print(f"{nam}: add tag {tag}")
            if not dry_run:
                nam.add_type_tag(tag)


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv, fossil="--fossil" in sys.argv)
