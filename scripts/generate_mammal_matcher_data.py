from __future__ import annotations

import argparse
import csv
import datetime
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from taxonomy.db.constants import AgeClass, Rank, Status
from taxonomy.db.models import Article, Name, Taxon
from taxonomy.db.nomenclature_book import get_taxa

DEFAULT_MDD_ARTICLE_NAME = "Mammalia-MDD 2_4.csv"
DEFAULT_OUTPUT = Path("mammal_names.json")
COVERED_AGES = {AgeClass.extant, AgeClass.recently_extinct}
BINOMIAL_RE = re.compile(r"^[A-Z][A-Za-z-]+ [a-z][a-z-]+$")


def get_mdd_path(article_name: str) -> Path:
    return Article.get(name=article_name).get_path()


def clean_name(text: str) -> str:
    text = text.replace("_", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def is_binomial(text: str) -> bool:
    return bool(BINOMIAL_RE.fullmatch(clean_name(text)))


def split_common_names(text: str) -> list[str]:
    names = [name.strip() for name in text.split("|") if name.strip()]
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            out.append(name)
    return out


def get_common_names(mdd_path: Path) -> dict[str, list[str]]:
    common_names: dict[str, list[str]] = {}
    with mdd_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sci_name = clean_name(row["sciName"])
            names = split_common_names(row.get("mainCommonName", ""))
            names.extend(split_common_names(row.get("otherCommonNames", "")))
            if names:
                common_names[sci_name] = split_common_names("|".join(names))
    return common_names


def get_current_species(root: Taxon) -> list[Taxon]:
    taxa = [
        taxon
        for taxon in get_taxa(root)
        if taxon.rank is Rank.species
        and taxon.age in COVERED_AGES
        and taxon.base_name.status is Status.valid
    ]
    return sorted(taxa, key=lambda taxon: taxon.valid_name)


def get_species_for_name(name: Name) -> Taxon | None:
    try:
        species = name.taxon.parent_of_rank(Rank.species)
    except ValueError:
        return None
    if species.base_name.status is not Status.valid or species.age not in COVERED_AGES:
        return None
    return species


def iter_synonym_names(species: Taxon) -> Iterable[Name]:
    yield from species.all_names_lazy()


def possible_binomials(name: Name, species: Taxon) -> set[str]:
    out: set[str] = set()
    for candidate in (
        name.corrected_original_name,
        name.original_name,
        name.get_default_valid_name(),
    ):
        if candidate and is_binomial(candidate):
            out.add(clean_name(candidate))
    if name.root_name and " " in species.valid_name:
        genus = species.valid_name.split()[0]
        out.add(f"{genus} {name.root_name}")
    return out


def get_higher_name(taxon: Taxon, rank: Rank) -> str:
    value = taxon.get_derived_field(rank.name.strip("_"))
    if value is not None and value.rank is rank:
        return value.valid_name
    return ""


def build_data(root: Taxon, mdd_path: Path) -> dict[str, Any]:
    common_names = get_common_names(mdd_path)
    species = get_current_species(root)
    valid_names: dict[str, dict[str, Any]] = {}
    synonyms: dict[str, str] = {}
    synonym_targets: dict[str, set[str]] = defaultdict(set)

    for taxon in species:
        names = common_names.get(taxon.valid_name, [])
        base_name = taxon.base_name
        valid_names[taxon.valid_name] = {
            "order": get_higher_name(taxon, Rank.order),
            "family": get_higher_name(taxon, Rank.family),
            "author": base_name.get_full_authority(),
            "year": base_name.year[:4] if base_name.year else "",
            "common_name_en": names[0] if names else "",
            "common_names": names,
            "taxon_id": taxon.id,
            "name_id": base_name.id,
        }

        for name in iter_synonym_names(taxon):
            species_for_name = get_species_for_name(name)
            if species_for_name is None or species_for_name != taxon:
                continue
            for synonym in possible_binomials(name, taxon):
                if synonym == taxon.valid_name or synonym in valid_names:
                    continue
                synonym_targets[synonym].add(taxon.valid_name)

    used_synonyms: set[str] = set()
    for synonym, targets in synonym_targets.items():
        if len(targets) == 1:
            lower_synonym = synonym.casefold()
            if lower_synonym in used_synonyms:
                continue
            used_synonyms.add(lower_synonym)
            synonyms[synonym] = next(iter(targets))

    genera = sorted({name.split()[0] for name in valid_names})
    return {
        "metadata": {
            "name": "MammalMatcher",
            "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "source": "Hesperomys taxonomy database",
            "source_url": "https://hesperomys.com",
            "common_name_source": mdd_path.name,
            "common_name_source_url": "https://www.mammaldiversity.org",
            "generator_url": (
                "https://github.com/JelleZijlstra/taxonomy/blob/master/"
                "scripts/generate_mammal_matcher_data.py"
            ),
            "species_count": len(valid_names),
            "synonym_count": len(synonyms),
            "references": [
                {
                    "citation": (
                        "Zijlstra, J.S. 2025. Hesperomys Project "
                        "(Version 25.12.0) [Data set]. Zenodo."
                    ),
                    "doi": "10.5281/zenodo.17806114",
                },
                {
                    "citation": (
                        "Mammal Diversity Database. 2026. Mammal Diversity "
                        "Database. Version 2.4."
                    ),
                    "doi": "10.5281/zenodo.18135819",
                },
            ],
        },
        "valid_names": valid_names,
        "synonyms": dict(sorted(synonyms.items())),
        "genera": genera,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate MammalMatcher's mammal_names.json data file."
    )
    parser.add_argument("--taxon", default="Mammalia")
    parser.add_argument("--mdd-article-name", default=DEFAULT_MDD_ARTICLE_NAME)
    parser.add_argument("--mdd-csv", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    root = Taxon.getter("valid_name")(args.taxon)
    if root is None:
        raise SystemExit(f"Unknown taxon: {args.taxon}")

    mdd_path = args.mdd_csv or get_mdd_path(args.mdd_article_name)
    data = build_data(root, mdd_path)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {args.output} with "
        f"{data['metadata']['species_count']} valid species and "
        f"{data['metadata']['synonym_count']} synonyms"
    )


if __name__ == "__main__":
    main()
