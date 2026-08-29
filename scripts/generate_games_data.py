"""
Generate game data files for Hesperomys frontend.

Generate classification quizzes for living and recently extinct mammals.
Optionally join an MDD species CSV by existing IDs for continent filters.

Usage:
  /Users/jelle/py/venvs/taxonomy314/bin/python scripts/generate_games_data.py

Outputs:
  hsweb/game_data/genus_family.json
"""

import argparse
import csv
import datetime
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from taxonomy.db.constants import AgeClass, Rank, Status
from taxonomy.db.models.tags import TaxonTag
from taxonomy.db.models.taxon.taxon import Taxon

CONTINENTS = {
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania (Continent)",
    "South America",
}


def game_taxa(rank: Rank) -> Iterable[Taxon]:
    for tx in Taxon.select_valid().filter(
        Taxon.rank == rank,
        Taxon.age.is_in([AgeClass.extant, AgeClass.recently_extinct]),
    ):
        if tx.base_name.status is not Status.valid:
            continue
        cls = tx.get_derived_field("class_")
        if cls is not None and cls.valid_name == "Mammalia":
            yield tx


def generate_family_order_data(taxa: Iterable[Taxon]) -> list[dict[str, str]]:
    rows = []
    for family in taxa:
        order = _ancestor_with_rank(family, Rank.order)
        if order is not None:
            rows.append({"family": family.valid_name, "order": order.valid_name})
    return sorted(rows, key=lambda row: row["family"].casefold())


def generate_order_families_data(
    families: Iterable[dict[str, str]],
) -> list[dict[str, object]]:
    by_order: dict[str, set[str]] = defaultdict(set)
    for row in families:
        by_order[row["order"]].add(row["family"])
    return [
        {"order": order, "families": sorted(names, key=str.casefold)}
        for order, names in sorted(
            by_order.items(), key=lambda item: item[0].casefold()
        )
    ]


def parse_continents(value: str) -> list[str]:
    """Keep only explicit, unqualified MDD continent records.

    Unknown labels are errors, not guessed ranges. NA and Domesticated are not
    geographic ranges, and a trailing question mark is not positive evidence.
    """
    continents = set()
    for part in value.split("|"):
        part = part.strip()
        if not part or part in {"NA", "Domesticated"}:
            continue
        if part.removesuffix("?") not in CONTINENTS:
            raise ValueError(f"Unknown MDD continent: {part!r}")
        if not part.endswith("?"):
            continents.add(part)
    return sorted(continents)


def generate_geography_data(
    mdd_rows: Iterable[dict[str, str]], taxa: Iterable[Taxon]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Join ranges by unambiguous existing MDD IDs; keep local classification.

    Ambiguous or missing IDs never trigger a scientific-name fallback. Report
    every local species so omitted ranges can be reviewed without DB writes.
    """
    by_id: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in mdd_rows:
        if row["id"]:
            by_id[row["id"]].append(row)
    taxa = list(taxa)
    ids_by_taxon = {
        tx.id: {tag.id for tag in tx.get_tags(tx.tags, TaxonTag.MDD)} for tx in taxa
    }
    id_counts = Counter(mid for ids in ids_by_taxon.values() for mid in ids)
    species: dict[str, dict[str, list[str]]] = defaultdict(dict)
    genera: dict[str, set[str]] = defaultdict(set)
    families: dict[str, set[str]] = defaultdict(set)
    records: list[dict[str, Any]] = []
    for tx in taxa:
        ids = ids_by_taxon[tx.id]
        record: dict[str, Any] = {
            "taxon_id": tx.id,
            "name": tx.valid_name,
            "mdd_ids": sorted(ids),
        }
        records.append(record)
        if len(ids) != 1:
            record["status"] = "missing_id" if not ids else "multiple_ids"
            continue
        (mid,) = ids
        if id_counts[mid] != 1:
            record["status"] = "shared_local_id"
            continue
        matches = by_id.get(mid, [])
        if len(matches) != 1:
            record["status"] = "missing_mdd_row" if not matches else "duplicate_mdd_id"
            continue
        row = matches[0]
        record["mdd_name"] = row["sciName"]
        record["continentDistribution"] = row["continentDistribution"]
        continents = parse_continents(row["continentDistribution"])
        if not continents:
            record["status"] = "no_confirmed_continent"
            continue
        genus = _get_parent_genus(tx)
        family = _ancestor_with_rank(tx, Rank.family)
        if genus is None or family is None or not tx.base_name.root_name:
            record["status"] = "incomplete_classification"
            continue
        epithet = tx.base_name.root_name
        species[genus.valid_name][epithet] = continents
        genera[genus.valid_name].update(continents)
        families[family.valid_name].update(continents)
        record["status"] = "matched"
    coverage = dict(sorted(Counter(row["status"] for row in records).items()))
    data = {
        "version": 1,
        "continents": sorted(CONTINENTS),
        "species": dict(sorted(species.items())),
        "genera": {key: sorted(value) for key, value in sorted(genera.items())},
        "families": {key: sorted(value) for key, value in sorted(families.items())},
        "coverage": coverage,
    }
    report = {
        "coverage": coverage,
        "records": sorted(records, key=lambda r: r["taxon_id"]),
    }
    return data, report


def generate_genus_family_data() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for tx in game_taxa(Rank.genus):
        family = tx.get_derived_field("family")
        genus = tx.valid_name
        fam = family.valid_name
        rows.append({"genus": genus, "family": fam})
    rows.sort(key=lambda r: r["genus"].casefold())
    return rows


def generate_family_genera_data() -> list[dict[str, object]]:
    by_family: dict[str, list[str]] = {}
    for tx in game_taxa(Rank.genus):
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mdd-csv", type=Path, help="MDD species CSV with continentDistribution"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("hsweb/game_data"))
    args = parser.parse_args()
    if args.mdd_csv:
        with args.mdd_csv.open(encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            required = {"id", "sciName", "continentDistribution"}
            if not required.issubset(reader.fieldnames or []):
                parser.error(f"MDD CSV must contain {', '.join(sorted(required))}")
            geography, report = generate_geography_data(reader, game_taxa(Rank.species))
        source = {
            "name": "Mammal Diversity Database",
            "file": args.mdd_csv.name,
            "sha256": hashlib.sha256(args.mdd_csv.read_bytes()).hexdigest(),
            "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        }
        geography["source"] = report["source"] = source
        write_json(args.output_dir / "geography.json", geography)
        write_json(args.output_dir / "geography_report.json", report)
        print(f"Geography coverage: {geography['coverage']}")
    else:
        print("No --mdd-csv: geography files are not regenerated.")
    out = args.output_dir / "genus_family.json"
    data = generate_genus_family_data()
    write_json(out, data)
    print(f"Wrote {len(data)} rows to {out}")
    out2 = args.output_dir / "family_genera.json"
    data2 = generate_family_genera_data()
    write_json(out2, data2)
    print(f"Wrote {len(data2)} families to {out2}")
    out2b = args.output_dir / "family_genera_grouped.json"
    data2b = generate_family_genera_grouped_data()
    write_json(out2b, data2b)
    print(f"Wrote {len(data2b)} grouped families to {out2b}")
    out3 = args.output_dir / "genus_species.json"
    data3 = generate_species_by_genus_data()
    write_json(out3, data3)
    print(f"Wrote {len(data3)} genera to {out3}")
    families = generate_family_order_data(game_taxa(Rank.family))
    write_json(args.output_dir / "family_order.json", families)
    print(f"Wrote {len(families)} families to family_order.json")
    orders = generate_order_families_data(families)
    write_json(args.output_dir / "order_families.json", orders)
    print(f"Wrote {len(orders)} orders to order_families.json")


def _get_parent_genus(tx: Taxon) -> Taxon | None:
    cur: Taxon | None = tx.parent
    while cur is not None:
        if cur.rank == Rank.genus:
            return cur
        cur = cur.parent
    return None


def generate_species_by_genus_data() -> list[dict[str, object]]:
    by_genus: dict[str, set[str]] = {}
    for tx in game_taxa(Rank.species):
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
    cur: Taxon | None = tx
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
    for tx in game_taxa(Rank.genus):
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
                        "" if g["name"] is None else str(g["name"]).casefold()  # type: ignore[index]
                    ),
                ),
            }
        )
    out.sort(key=lambda r: str(r["family"]).casefold())
    return out  # type: ignore[return-value]


if __name__ == "__main__":
    main()
