import re
from collections import Counter
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source(
    "mammalia-corbet-hill-1991.txt", "Mammalia (Corbet & Hill 1991).pdf"
)


def clear_images(pages: lib.PagesT) -> lib.PagesT:
    for page_no, lines in pages:
        leading_ws_counts: Counter[int] = Counter()
        for line in lines:
            if not line.rstrip():
                continue
            leading_ws_counts[len(line) - len(line.lstrip())] += 1
        ((leading_ws, count),) = leading_ws_counts.most_common(1)
        if leading_ws != 0:
            prop = count / sum(leading_ws_counts.values())
            if prop < 0.3:
                raise ValueError(
                    f"Leading whitespace on page {page_no} is not consistent: {leading_ws_counts}"
                )
            lines = [line[leading_ws:] for line in lines]
        yield page_no, lines


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()
    last_genus: str | None = None
    rank: Rank | None = None
    for page_no, lines in pages:
        it = lib.PeekingIterator(lines)
        for line in it:
            if match := re.search(r"^ {5,}(ORDER|SUBORDER) ([A-Z]+)", line):
                rank = Rank[match.group(1).lower()]
                name = match.group(2).title()
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
            elif match := re.search(
                r"^ *(Family|Subfamily|Subgenus) ([A-Z][a-z]+)", line
            ):
                rank = Rank[match.group(1).lower()]
                name = match.group(2)
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
            elif (
                match := re.search(r"^([A-Z][a-z]+) ?($|;|'|,)(?! (c\. )?\d+ sp)", line)
            ) and not re.search(r"; (c\. )?\d+ sp", line):
                name = match.group(1)
                last_genus = name
                rank = Rank.genus
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
            elif match := re.search(r"^(?:\? )?([A-Z])\. ([a-z]+)", line):
                initial, epithet = match.groups()
                rank = Rank.species
                assert last_genus
                if last_genus[0] != initial:
                    print(last_genus, line)
                    continue
                yield {
                    "name": f"{last_genus} {epithet}",
                    "rank": rank,
                    "page": str(page_no),
                    "article": art,
                }
            elif rank is Rank.species:
                if (
                    (not line.strip())
                    or line.startswith("   ")
                    or line.strip().startswith("(")
                ):
                    continue
                else:
                    print(repr(line))


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    pages = clear_images(pages)
    names = extract_names(pages)
    names = lib.add_parents(names)
    names = lib.no_childless_ces(names)
    # names = lib.count_by_rank(names, Rank.order)
    # names = lib.count_by_rank(names, Rank.family)
    names = lib.print_unrecognized_genera(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)

    lib.print_ce_summary(names)
    lib.format_ces(SOURCE, format_name=False)


if __name__ == "__main__":
    main()
