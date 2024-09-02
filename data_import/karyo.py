import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source("mammalia-karyo.txt", "Mammalia-karyo.pdf")


class KaryoDict(lib.CEDict):
    raw_text: str


def extract_names(text: Iterable[str]) -> Iterable[KaryoDict]:
    current_order: str | None = None
    current_family: str | None = None
    lines = list(text)
    art = SOURCE.get_source()
    for i, line in enumerate(lines):
        parent: str | None
        parent_rank: Rank | None
        line = line.replace("-cytotype", "         ")
        if line.startswith(("Order ", "Family ")):
            rank_text, name, *_ = line.split()
            rank = Rank[rank_text.lower()]
            page = None
            for index in range(i, i + 3):
                words = lines[index].split()
                if not words:
                    continue
                last_word = words[-1]
                if last_word.isdigit():
                    page = last_word
            assert page is not None
            if rank is Rank.order:
                current_order = name
                current_family = None
                parent = parent_rank = None
            else:
                assert rank is Rank.family
                current_family = name
                parent = current_order
                parent_rank = Rank.order
        elif (
            re.fullmatch(
                r" {2,4}[A-Z][a-z]+( \([A-Za-z]+\))? [a-z’]+ .* \d+", line.rstrip()
            )
            is not None
        ):
            line = line.strip()
            page = line.split()[-1]
            assert page.isdigit()
            parent = current_family
            parent_rank = Rank.family
            name = line.split("   ")[0].removesuffix("Ideogram").strip()
            if name.count(" ") == 2:
                rank = Rank.subspecies
            elif name.count(" ") == 1:
                rank = Rank.species
            else:
                if " sp " in name:
                    continue
                assert False, name
        else:
            words = line.split()
            if not words:
                continue
            last_word = words[-1]
            assert (
                len(words) <= 2
                or not last_word.isdigit()
                or " sp." in line
                or " sp " in line
            ), line
            continue
        assert page is not None, line
        yield {
            "raw_text": line,
            "rank": rank,
            "name": name,
            "page": page,
            "parent": parent,
            "parent_rank": parent_rank,
            "article": art,
        }


def main() -> None:
    data = lib.get_text(SOURCE)
    names = extract_names(data)
    names2 = lib.validate_ce_parents(names, drop_duplicates=True)
    names2 = lib.add_classification_entries(names2, dry_run=False)
    lib.print_ce_summary(names2)


if __name__ == "__main__":
    main()
