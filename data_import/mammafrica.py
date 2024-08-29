import argparse
import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCES = {
    "carnivora": lib.Source(
        "carnivora-perissodactyla-pholidota-africa.txt",
        "Carnivora, Perissodactyla, Pholidota Africa.pdf",
    ),
    "glires": lib.Source("glires-africa.txt", "Glires Africa.pdf"),
    "chiroptera": lib.Source(
        "chiroptera-lipotyphla-africa.txt", "Chiroptera, Lipotyphla Africa.pdf"
    ),
    "primates": lib.Source(
        "primates-africa-butynski-et-al-.txt",
        "Primates Africa (Butynski et al. 2013).pdf",
    ),
}


def extract_names(pages: lib.PagesT, source: lib.Source) -> Iterable[lib.CEDict]:
    art = source.get_source()
    rank_stack: list[tuple[Rank, str]] = []
    for _, lines in pages:
        buffer = []
        for line in lines:
            if line.strip() == "Contents" or line.strip().isnumeric():
                continue
            if "primates" in source.inputfile:
                words = line.split()
                if not words:
                    continue
                if not words[-1].isnumeric():
                    buffer.append(line)
                    continue
            elif "\b" not in line:
                buffer.append(line)
                continue
            line = "".join([*buffer, line]).strip()
            if line.startswith(("BIBLIOGRAPHY", "Appendix", "Glossary")):
                break
            buffer = []

            words = line.split()
            page = words[-1].split("\b")[-1]
            parent_rank: Rank | None
            if words[0].isupper() or words[0] in (
                "Order",
                "Family",
                "Subfamily",
                "Genus",
            ):
                rank_text = words[0].lower()
                if rank_text == "hyporder":
                    rank_text = "infraorder"
                rank = Rank[rank_text]
                name = words[1].title()
                assert re.fullmatch(r"[A-Z][a-z]+", name), (name, repr(line))
                while rank_stack and rank_stack[-1][0] <= rank:
                    rank_stack.pop()
                if rank_stack:
                    parent_rank, parent = rank_stack[-1]
                else:
                    parent_rank = parent = None
                rank_stack.append((rank, name))
            else:
                genus_name = words[0]
                if genus_name == "Series":
                    continue
                species_name = words[1]
                if species_name == "cf.":
                    continue
                if "Group," in line:
                    continue
                name = f"{genus_name} {species_name}"
                assert re.fullmatch(r"[A-Z][a-z]+ [a-z]+", name), repr(line)
                if name in ("Acknowledgements for", "The Mammals", "Mammals of"):
                    continue
                rank = Rank.species
                try:
                    parent_rank, parent = rank_stack[-1]
                except IndexError:
                    raise ValueError(f"no rank stack for {line!r}") from None
                assert parent_rank in (Rank.genus, Rank.subgenus), rank_stack
            assert page.isnumeric(), repr(line)
            yield {
                "article": art,
                "parent_rank": parent_rank,
                "parent": parent,
                "name": name,
                "rank": rank,
                "page": page,
            }


def main(source: lib.Source, *, dry_run: bool = True) -> None:
    text = lib.get_text(source)
    pages = lib.extract_pages(text, permissive=True, ignore_page_numbers=True)
    pages = lib.align_columns(pages, ignore_close_to_end=True)
    names = extract_names(pages, source)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=dry_run)
    lib.print_ce_summary(names)
    lib.format_ces(source)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("volume", choices=list(SOURCES))
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    main(SOURCES[args.volume], dry_run=args.dry_run)
