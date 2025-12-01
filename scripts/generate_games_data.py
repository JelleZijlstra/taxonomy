"""
Generate game data files for Hesperomys frontend.

Currently generates a genus→family mapping for extant mammal genera.

Usage:
  /Users/jelle/py/venvs/taxonomy314/bin/python scripts/generate_games_data.py

Outputs:
  hsweb/game_data/genus_family.json
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from taxonomy.db.constants import AgeClass, Rank, Status
from taxonomy.db.models.taxon.taxon import Taxon


def generate_genus_family_data() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for tx in Taxon.select_valid().filter(
        Taxon.rank == Rank.genus,
        Taxon.age.is_in([AgeClass.extant, AgeClass.recently_extinct]),
    ):
        if tx.base_name.status is not Status.valid:
            continue
        cls = tx.get_derived_field("class_")
        if cls.valid_name != "Mammalia":
            continue
        family = tx.get_derived_field("family")
        genus = tx.valid_name
        fam = family.valid_name
        rows.append({"genus": genus, "family": fam})
    rows.sort(key=lambda r: r["genus"].casefold())
    return rows


def generate_family_genera_data() -> list[dict[str, object]]:
    by_family: dict[str, list[str]] = {}
    for tx in Taxon.select_valid().filter(
        Taxon.rank == Rank.genus,
        Taxon.age.is_in([AgeClass.extant, AgeClass.recently_extinct]),
    ):
        if tx.base_name.status is not Status.valid:
            continue
        cls = tx.get_derived_field("class_")
        if cls.valid_name != "Mammalia":
            continue
        family = tx.get_derived_field("family")
        genus = tx.valid_name
        fam = family.valid_name
        by_family.setdefault(fam, []).append(genus)
    out: list[dict[str, object]] = []
    for fam, genera in by_family.items():
        out.append({"family": fam, "genera": sorted(set(genera), key=str.casefold)})
    out.sort(key=lambda r: str(r["family"]).casefold())
    return out


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    out = Path("hsweb/game_data/genus_family.json")
    data = generate_genus_family_data()
    write_json(out, data)
    print(f"Wrote {len(data)} rows to {out}")
    out2 = Path("hsweb/game_data/family_genera.json")
    data2 = generate_family_genera_data()
    write_json(out2, data2)
    print(f"Wrote {len(data2)} families to {out2}")


if __name__ == "__main__":
    main()
