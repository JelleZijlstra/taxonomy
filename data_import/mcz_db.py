import csv
import sys
from collections import defaultdict
from collections.abc import Iterable

from taxonomy.db.models import Collection, Name, TypeTag, name_lint

from .lib import DATA_DIR


def get_hesp_data(fossil: bool) -> dict[str, list[Name]]:
    mcz_mamm = Collection.getter("label")(
        "MCZ (Vertebrate Paleontology)" if fossil else "MCZ (Mammalogy)"
    )
    assert mcz_mamm is not None
    output = defaultdict(list)
    for nam in mcz_mamm.type_specimens:
        if nam.type_specimen is None:
            continue
        for spec in name_lint.parse_type_specimen(nam.type_specimen):
            if isinstance(spec, name_lint.Specimen):
                output[spec.text].append(nam)
    multiple = Collection.getter("label")("multiple")
    assert multiple is not None
    for nam in multiple.type_specimens:
        if nam.type_specimen is None:
            continue
        for spec in name_lint.parse_type_specimen(nam.type_specimen):
            if isinstance(spec, name_lint.Specimen) and spec.text.startswith("MCZ "):
                output[spec.text].append(nam)
    return output


def get_mcz_db(fossil: bool) -> Iterable[dict[str, str]]:
    path = "mcz-vp.csv" if fossil else "mcz-mammals.csv"
    with (DATA_DIR / path).open() as f:
        rows = csv.DictReader(f)
        yield from sorted(rows, key=lambda row: row["TYPESTATUS"])


def main(dry_run: bool = True, fossil: bool = False) -> None:
    hesp_data = get_hesp_data(fossil)
    for row in get_mcz_db(fossil):
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
