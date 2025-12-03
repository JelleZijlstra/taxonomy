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
    out2b = Path("hsweb/game_data/family_genera_grouped.json")
    data2b = generate_family_genera_grouped_data()
    write_json(out2b, data2b)
    print(f"Wrote {len(data2b)} grouped families to {out2b}")
    out3 = Path("hsweb/game_data/genus_species.json")
    data3 = generate_species_by_genus_data()
    write_json(out3, data3)
    print(f"Wrote {len(data3)} genera to {out3}")


def _get_parent_genus(tx: Taxon) -> Taxon | None:
    cur: Taxon | None = tx.parent
    while cur is not None:
        if cur.rank == Rank.genus:
            return cur
        cur = cur.parent
    return None


def generate_species_by_genus_data() -> list[dict[str, object]]:
    by_genus: dict[str, set[str]] = {}
    for tx in Taxon.select_valid().filter(
        Taxon.rank == Rank.species,
        Taxon.age.is_in([AgeClass.extant, AgeClass.recently_extinct]),
    ):
        if tx.base_name.status is not Status.valid:
            continue
        cls = tx.get_derived_field("class_")
        if cls.valid_name != "Mammalia":
            continue
        genus = _get_parent_genus(tx)
        if genus is None:
            continue
        genus_name = genus.valid_name
        epithet = tx.base_name.root_name or ""
        if not genus_name or not epithet:
            continue
        by_genus.setdefault(genus_name, set()).add(epithet)
    out: list[dict[str, object]] = []
    for genus_name, species in by_genus.items():
        out.append({"genus": genus_name, "species": sorted(species, key=str.casefold)})
    out.sort(key=lambda r: str(r["genus"]).casefold())
    return out


def _ancestor_with_rank(tx: Taxon, rank: Rank) -> Taxon | None:
    cur = tx
    while cur is not None:
        if cur.rank == rank:
            return cur
        cur = cur.parent
    return None


def generate_family_genera_grouped_data() -> list[dict[str, object]]:
    """Group genera within a family by subfamily and tribe.

    Structure per family:
      {
        "family": str,
        "groups": [
          {
            "name": str | null,  # subfamily name
            "tribes": [ { "name": str | null, "genera": [str, ...] }, ... ],
            "unplaced_genera": [str, ...],  # genera in subfamily not in any tribe
          },
          # ... a group with name = null contains genera without subfamily
        ]
      }
    """
    fam_map: dict[str, dict[str | None, dict[str | None, set[str]]]] = (
        {}
    )  # family -> subfamily -> tribe -> set(genus)
    for tx in Taxon.select_valid().filter(
        Taxon.rank == Rank.genus,
        Taxon.age.is_in([AgeClass.extant, AgeClass.recently_extinct]),
    ):
        if tx.base_name.status is not Status.valid:
            continue
        cls = tx.get_derived_field("class_")
        if cls.valid_name != "Mammalia":
            continue
        family_tx = tx.get_derived_field("family")
        family = family_tx.valid_name
        subfam_tx = _ancestor_with_rank(tx, Rank.subfamily)
        subfam = subfam_tx.valid_name if subfam_tx is not None else None
        tribe_tx = _ancestor_with_rank(tx, Rank.tribe)
        tribe = tribe_tx.valid_name if tribe_tx is not None else None
        genus = tx.valid_name
        fam_map.setdefault(family, {}).setdefault(subfam, {}).setdefault(
            tribe, set()
        ).add(genus)

    out = []
    for family, subfam_map in fam_map.items():
        groups = []
        for subfam, tribe_map in subfam_map.items():
            tribes_list = []
            unplaced = set()
            for tribe, genera_set in tribe_map.items():
                if tribe is None:
                    unplaced |= genera_set
                else:
                    tribes_list.append(
                        {"name": tribe, "genera": sorted(genera_set, key=str.casefold)}
                    )
            group = {
                "name": subfam,
                "tribes": sorted(tribes_list, key=lambda d: str(d["name"]).casefold()),
                "unplaced_genera": sorted(unplaced, key=str.casefold),
            }
            groups.append(group)
        out.append(
            {
                "family": family,
                "groups": sorted(
                    groups,
                    key=lambda g: (
                        "" if g["name"] is None else str(g["name"]).casefold()
                    ),
                ),
            }
        )
    out.sort(key=lambda r: str(r["family"]).casefold())
    return out


if __name__ == "__main__":
    main()
