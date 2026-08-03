import re
from collections.abc import Iterable
from pathlib import Path

from data_import import lib
from data_import.ce_file import write_ce_file
from taxonomy.db import constants, models

ARTICLE_NAME = "Glyptodontidae (Fernicola 2008).pdf"
OUTPUT_PATH = Path("recs/manifests/fernicola_2008_glyptodonts.ce.jsonl")

# depth | rank | source text. The two printed columns have been placed in
# hierarchical reading order, with the left column followed by the right.
SOURCE = """
0|magnorder|Magnorden XENARTHRA Cope, 1889
1|order|Orden CINGULATA Illiger 1811
2|suborder|Suborden GLYPTODONTIA Ameghino,1889 [Nodo A]
3|genus|†Asymmetrura Fariña, 1981
3|genus|†Berthawyleria Castellanos, 1940
3|genus|†Chacus Zurita, 2002
3|genus|†Coscinocercus Cabrera, 1939
3|genus|†Eonaucum Scillato-Yané y Carlini, 1998
3|genus|†Eosclerophorus Castellanos, 1948
3|genus|†Hoplophorus Lund, 1839
3|genus|†Isolinia Castellanos, 1951b
3|genus|†Parahoplophorus, Castellanos, 1931
3|genus|†Phlyctaenopyga Cabrera, 1944
3|genus|†Plohophoroides Castellanos, 1928
3|genus|†Plohophorops Castellanos, 1935
3|genus|†Stromaphoropsis Kraglievich, 1932
3|genus|†Teisseiria Kraglievich, 1932
3|genus|†Trabalia Kraglievich, 1932
3|genus|†Trachycalyptus, Ameghino, 1908
3|genus|†Uruguayurus Mones, 1987
3|genus|†Zaphilus Ameghino, 1889
3|tribe|Tribu NEOTHORACOPHORINI Castellanos, 1951
4|genus|†Pseudoneothoracophorus Castellanos, 1951
4|genus|†Neothoracophorus Ameghino, 1889
3|tribe|Tribu PALAEHOPLOPHORINI Hoffstetter, 1958
4|genus|†Palaehoplophoroides Scillato-Yané y Carlini, 1998
4|genus|†Palaehoplophorus Ameghino, 1883
4|genus|†Protoglyptodon Ameghino, 1885
4|genus|†Aspidocalyptus Cabrera, 1939
4|genus|†Pseudoeuryurus Ameghino, 1889
4|genus|†Chlamyphractus Castellanos, 1940
3|tribe|Tribu NEURYURINI Hoffstetter, 1958
4|genus|†Neuryurus Ameghino, 1889
3|family|Familia GLYPTATELIDAE Castellanos, 1932
4|genus|†Glyptatelus Ameghino, 1897
4|genus|†Clypeotherium Scillato-Yané 1977
4|genus|†Neoglyptatelus Carlini, Vizcaíno y Scillato-Yané,1997
4|genus|†Lomaphorelus Ameghino, 1902
4|genus|†Pachyarmatherium Downing y White, 1995
3|infraorder|Infraorden PROPALAEHOPLOPHOROINEI Ameghino, 1891a [Nodo B]
4|superfamily|Superfamilia PROPALAEHOPLOPHOROIDEA Ameghino, 1891a
5|family|Familia PROPALAEHOPLOPHORIDAE Ameghino, 1891a
6|genus|†Propalaehoplophorus Ameghino, 1887a
6|genus|†Eucinepeltus Ameghino, 1891
6|genus|†Asterostemma Ameghino, 1889
6|genus|†Metopotoxus Ameghino, 1898
6|genus|†Cochlops Ameghino, 1889
3|infraorder|Infraorden GLYPTODONTOINEI Gray, 1869 [Nodo C]
4|genus|†Eosclerocalyptus Ameghino, 1919
4|genus|†Hoplophractus Cabrera, 1939
4|genus|†Stromaphorus Castellanos, 1926
4|genus|†Pseudoplohophorus Castellanos, 1926
4|superfamily|Superfamilia GLYPTODONTOIDEA Gray, 1869 [Nodo G]
5|tribe|Tribu LOMAPHORINI Hoffstetter, 1958
6|genus|†Urotherium Castellanos, 1926
6|genus|†Lomaphorops Castellanos, 1931
6|genus|†Lomaphorus Ameghino, 1889
6|genus|†Peiranoa Castellanos, 1946
6|genus|†Thachycalyptoides Saint-André, 1996
5|family|Familia PANOCHTHIDAE Castellanos 1927 [Nodo J]
6|subfamily|Subfamilia PANOCHTHINAE Castellanos, 1927
7|tribe|Tribu NEOSCLEROCALYPTINI Paula Couto,1957
8|genus|†Neosclerocalyptus Paula Couto, 1957
7|tribe|Tribu PANOCHTINI Castellanos, 1927
8|genus|†Panochthus Burmeister, 1866
8|genus|†Nopachtus Ameghino, 1888
8|genus|†Propanochthus Castellanos, 1925
5|family|Familia GLYPTODONTIDAE Gray, 1869 [Nodo H]
6|subfamily|Subfamilia PLOHOPHORINAE Castellanos, 1932
7|tribe|Tribu PLOHOPHORINI Castellanos, 1932
8|genus|†Plohophorus Ameghino, 1887b
6|subfamily|Subfamilia GLYPTODONTINAE Gray, 1869 [Nodo I]
7|tribe|Tribu DOEDICURINI Ameghino, 1889
8|genus|†Doedicurus Burmeister, 1874
8|genus|†Comaphorus Ameghino, 1886
8|genus|†Eleutherocercus Koken, 1888
8|genus|†Palaeodaedicurus Castellanos, 1927
8|genus|†Prodaedicurus Castellanos, 1927
8|genus|†Castellanosia Kraglievich, 1932
8|genus|†Xiphuroides Castellanos, 1927
8|genus|†Plaxaplous Ameghino, 1884b
8|genus|†Doedicuroides Castellanos, 1940
7|tribe|Tribu GLYPTODONTINI Gray, 1869
8|genus|†Glyptodon Owen, 1839
8|genus|†Glyptodontidium Cabrera, 1944
8|genus|†Stromatherium Castellanos, 1953
8|genus|†Paraglyptodon, Castellanos, 1932
8|genus|†Glyptostracon Castellanos, 1953
8|genus|†Heteroglyptodon Roselli, 1976
8|genus|†Glyptotherium Osborn, 1903
"""


def _parse_source_text(
    raw: str, rank: constants.Rank
) -> tuple[str, str | None, str | None]:
    text = re.sub(
        r"^(?:Magnorden|Orden|Suborden|Infraorden|Superfamilia|Familia|Subfamilia|Tribu)\s+",
        "",
        raw,
    )
    text = text.removeprefix("†")
    text = re.sub(r"\s*\[Nodo [A-Z]\]$", "", text)
    match = re.fullmatch(r"([^\s,]+),?\s+(.+?)(?:,\s*|\s+)(\d{4}[a-z]?)", text)
    assert match is not None, (raw, rank)
    name = match.group(1)
    if rank is not constants.Rank.genus:
        name = name.title()
    return name, match.group(2), match.group(3)


def extract(article: models.Article) -> Iterable[lib.CEDict]:
    stack: dict[int, lib.CEDict] = {}
    entries = []
    for line in SOURCE.strip().splitlines():
        depth_text, rank_text, raw = line.split("|", 2)
        depth = int(depth_text)
        rank = constants.Rank[rank_text]
        name, authority, year = _parse_source_text(raw, rank)
        ce: lib.CEDict = {
            "article": article,
            "page": "22",
            "rank": rank,
            "name": name,
            "authority": authority,  # type: ignore[typeddict-item]
            "year": year,  # type: ignore[typeddict-item]
            "raw_data": raw,
        }
        if "†" in raw:
            ce["age_class"] = constants.AgeClass.fossil
        if depth:
            parent = stack[depth - 1]
            ce["parent"] = parent["name"]
            ce["parent_rank"] = parent["rank"]
        stack = {
            old_depth: old_ce
            for old_depth, old_ce in stack.items()
            if old_depth < depth
        }
        stack[depth] = ce
        entries.append(ce)
    yield from lib.validate_ce_parents(entries)


def main() -> None:
    article = models.Article.get(name=ARTICLE_NAME)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_ce_file(OUTPUT_PATH, extract(article))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
