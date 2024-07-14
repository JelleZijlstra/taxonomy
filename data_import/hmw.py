import csv
import pprint
import re
import subprocess
from collections.abc import Iterable
from functools import cache
from pathlib import Path
from typing import NotRequired

from data_import.lib import (
    CEDict,
    add_classification_entries,
    print_field_counts,
    validate_ce_parents,
)
from taxonomy.db.constants import Rank
from taxonomy.db.models.article.article import Article

REPO_LOCATION = Path("~/py/hmw").expanduser()
HMW_CSV = REPO_LOCATION / "hmw.csv"


VOLUME_TO_ARTICLE = {
    "Handbook of the Mammals of the World – Volume 6 Lagomorphs and Rodents I, Barcelona: Lynx Edicions": (
        "Glires (HMW)"
    ),
    "Handbook of the Mammals of the World – Volume 9 Bats, Barcelona: Lynx Edicions": (
        "Chiroptera (HMW)"
    ),
    "Handbook of the Mammals of the World – Volume 5 Monotremes and Marsupials, Barcelona: Lynx Edicions": (
        "Marsupialia, Monotremata (HMW)"
    ),
    "Handbook of the Mammals of the World – Volume 8 Insectivores, Sloths and Colugos, Barcelona: Lynx Edicions": (
        "Placentalia-HMW 8"
    ),
    "Handbook of the Mammals of the World – Volume 3 Primates, Barcelona: Lynx Edicions": (
        "Primates (HMW)"
    ),
    "Handbook of the Mammals of the World – Volume 7 Rodents II, Barcelona: Lynx Edicions": (
        "Rodentia (HMW7)"
    ),
    "Handbook of the Mammals of the World – Volume 4 Sea Mammals, Barcelona: Lynx Edicions": (
        "Mammalia-marine (HMW)"
    ),
    "Handbook of the Mammals of the World – Volume 2 Hoofed Mammals, Barcelona: Lynx Edicions": (
        "Ungulata (HMW)"
    ),
    "Handbook of the Mammals of the World – Volume 1 Carnivores, Barcelona: Lynx Edicions": (
        "Carnivora (HMW)"
    ),
}


@cache
def get_article(article_name: str) -> Article:
    return Article.get(name=VOLUME_TO_ARTICLE[article_name])


def load_csv() -> list[dict[str, str]]:
    if not REPO_LOCATION.exists():
        subprocess.check_call(
            ["git", "clone", "https://github.com/jhpoelen/hmw.git", str(REPO_LOCATION)]
        )
    with HMW_CSV.open() as f:
        reader = csv.DictReader(f)
        return list(reader)


NAME_FIXES = {"Dactylopsila tater": "Dactylopsila tatei"}


def fix_up_row(row: dict[str, str]) -> dict[str, str]:
    if row["docId"] == "038A1613FFBCFF96830BF7D5C0A9FD66":
        row = {
            **row,
            "name": "Dactylopsila megalura",
            "interpretedGenus": "Dactylopsila",
            "interpretedSpecies": "megalura",
            "interpretedAuthorityName": "Rothschild & Dollman",
            "interpretedAuthorityYear": "1932",
            "taxonomy": (
                "Dactylopsila megalura Rothschild & Dollman, 1932 , "
                "“ The Gebroeders [Mountains] , Weyland Range , Dutch New "
                "Guinea .” This species is monotypic."
            ),
            "subspeciesAndDistribution": (
                "Weyland "
                "Mts, and in Snow (= Surdiman) and Star (= Jayawijaya) Mts, "
                "in W & C New Guinea Central Range."
            ),
        }
    elif row["docId"] == "03993828FFE20F46FFF1F663CAB2FC49":
        row = {
            **row,
            "name": "Dendromus insignis",
            "interpretedGenus": "Dendromus",
            "interpretedSpecies": "insignis",
            "interpretedAuthorityName": "Thomas",
            "interpretedAuthorityYear": "1903",
            "taxonomy": (
                "Dendromys insignis Thomas , "
                "1903, “ Nandi , British East Africa [= Kenya ].” This "
                "species is monotypic."
            ),
            "subspeciesAndDistribution": (
                "Montane areas of E "
                "Africa, in East DR Congo, W Uganda, Rwanda, S Kenya, and "
                "Eastern Arc Mts of Tanzania."
            ),
        }
    elif row["docId"] == "03AD87FAFFF7F61E8C673317FDCBFBD2":
        row = {**row, "interpretedSpecies": "anderseni"}
    elif row["docId"] == "03C5A071FFDCFFEBFA17547B5EE6FD5C":
        row = {
            **row,
            "name": "Phyllomys nigrispinus",
            "interpretedGenus": "Phyllomys",
            "interpretedSpecies": "nigrispinus",
            "interpretedAuthorityName": "Wagner",
            "interpretedAuthorityYear": "1842",
            "taxonomy": (
                "Loncheres nigrispina "
                "Wagner, 1842 , “Ypanema.” Restricted by L. H. Emmons and "
                "colleagues in 2002 to “Floresta Nacional de Ipanema, 20 km "
                "NW Sorocaba, Sao Paulo, Brazil, 23°26’S47°37°'W,elev. "
                "550-970 m.”"
            ),
        }
    elif row["docId"] == "03F06D13FF82204B0D5B129309B5F3F6":
        row = {**row, "interpretedSpecies": "liangshanensis"}
    elif row["docId"] == "03F06D13FF9A2053084815810E66F8AA":
        row = {**row, "interpretedSpecies": "gromovi"}
    elif row["docId"] == "064D0660FF93ED6EFF6CF6A2F6D0F241":
        row = {
            **row,
            "name": "Urocitellus elegans",
            "interpretedGenus": "Urocitellus",
            "interpretedSpecies": "elegans",
            "interpretedAuthorityName": "Kennicott",
            "interpretedAuthorityYear": "1863",
            "taxonomy": (
                "Spermophilus elegans "
                "Kennicott, 1863 , “Fort Bridger,” Uinta Co., Wyoming, USA. "
                "Three subspecies are recognized. "
            ),
            "subspeciesAndDistribution": (
                "U.e.elegansKennicott,1863—SWyoming,SEIdaho,NEUtah,N&WColorado,andextremeWNebraska(USA)."
                " U.e.aureusDavis,1939—CIdahotoSWMontana(USA). U. e. nevadensis A. H."
                " Howell, 1928 — SE Oregon, SW Idaho, and N Nevada (USA)."
            ),
        }
    elif row["docId"] == "064D0660FFF4ED09FFDBF67DF6F9F539":
        row = {
            **row,
            "name": "Hylopetes sipora",
            "interpretedGenus": "Hylopetes",
            "interpretedSpecies": "sipora",
            "interpretedAuthorityName": "Chasen",
            "interpretedAuthorityYear": "1940",
            "taxonomy": (
                "Hylopetes "
                "sagitta sipora Chasen, 1940 , “Sipora Island, Mentawi "
                "Islands, West Sumatra,” Indonesia. Hylopetes siporawas "
                "described from an imma- ture specimen and was originally "
                "included in H. sagitta; an adult specimen is needed to "
                "clarify its taxonomic status. Monotypic."
            ),
            "subspeciesAndDistribution": "Sipora I, Mentawi Is (off W umatra). ",
        }
    elif row["docId"] == "123187A5FFFCFFA5FFBFF48BF85436C6":
        row = {
            **row,
            "name": "Cuscomys oblativus",
            "interpretedGenus": "Cuscomys",
            "interpretedSpecies": "oblativus",
            "interpretedAuthorityName": "Eaton",
            "interpretedAuthorityYear": "1916",
            "taxonomy": (
                "Abrocoma oblativa Eaton, "
                "1916 , “Machu Picchu,” Cusco, Peru . Cuscomys oblativus was "
                "described from skeletal material of two individuals found in "
                "Inca burials near Machu Picchu. It was thought to be "
                "extinct, but in 2009, a Cuscomys was captured alive, "
                "photographed, and released at Winay Wayna (2650 m), only 3 "
                "km from Machu Picchu,the type locality. Significant "
                "geographical barriers from the type locality of C. oblativus "
                "separate it from C. ashaninka . Monotypic."
            ),
            "subspeciesAndDistribution": (
                "SE Peru (Cusco), in the vicinity of the Inca ruins at Machu Picchu."
            ),
        }
    elif row["docId"] == "143F87B3FFC9FF8CFF5191BCFB55FCC0":
        row = {**row, "interpretedSpecies": "ichneumon"}
    elif row["docId"] == "143F87B3FFCEFF88FF0A9AE6F6C0F907":
        row = {
            **row,
            "name": "Atilax paludinosus",
            "interpretedGenus": "Atilax",
            "interpretedSpecies": "paludinosus",
            "interpretedAuthorityName": "Cuvier",
            "interpretedAuthorityYear": "1829",
            "taxonomy": (
                "Herpestes "
                "paludinosus Cuvier, 1829, Cape of Good Hope, South Africa . "
                "Ten subspecies are recognized."
            ),
            "subspeciesAndDistribution": (
                "A. p. paludinosus Cuvier, 1829 — S South Africa . A. p. "
                "macrodon JA. Allen, 1924 — Central African Republic through "
                "Congo republics to Rwanda and Burundi . A. p. mutis Thomas, "
                "1902 — Ethiopia . A. p. mordax Thomas, 1912 — S Tanzania . "
                "A. p. pluto Temminck, 1853 — Senegal to Nigeria . A. p. "
                "robustus Gray, 1865 — Chad and Sudan . A. p. rubellus Thomas "
                "& Wroughton, 1908 — Malawi , Mozambique , and Zimbabwe . A. "
                "p. rubescens Hollister, 1912 — N Tanzania , Kenya , and "
                "Uganda . A. p. spadiceus Cabrera, 1921 — Cameroon to Gabon . "
                "A. p. transvaalensis Roberts, 1933 — N South Africa to "
                "Angola and Zambia ."
            ),
        }
    elif row["docId"] == "1E30E2753496FF27E16924227E378281":
        row = {**row, "interpretedSpecies": "nolthenii"}
    elif row["docId"] == "1E30E2753536FE87E45F2A2D7EF28E83":
        row = {
            **row,
            "name": "Halmaheramys bokimekot",
            "interpretedGenus": "Halmaheramys",
            "interpretedSpecies": "bokimekot",
            "interpretedAuthorityName": "Fabre et al.",
            "interpretedAuthorityYear": "2013",
            "taxonomy": (
                "Halmaheramys bokimekot "
                "Fabre et al., 2013 , “15 km north-west of Sagea village "
                "(central Halmahera, Halmahera Island, North Moluccas, "
                'Indonesia), at 723 m a.s.l. Coordinates: 00°36°42-60" N, '
                "128°2°49-00’E .” Halmaheramys was found to be basal to a "
                "clade including Taeromys , Paruromys , and Bunomys . "
                "Subfossil records are known from Morotai Island, north of "
                "Halmahera Island, Indonesia. Another species of Halmaheramys "
                "is currently under description from Obi and Bisa Islands by "
                "P. H. Fabre and colleagues. Monotypic. "
            ),
        }
    elif row["docId"] == "3D474A54A012877FFAF6A1BC1664FC1E":
        row = {**row, "interpretedSpecies": "hoffmanni"}
    elif row["docId"] == "3D474A54A03A8756FFF7ADC61325F3CD":
        row = {**row, "interpretedSpecies": "ibarrai"}
    elif row["docId"] == "6A61FC4EFFAF01481CF1F54A65D6DC41":
        row = {
            **row,
            "name": "Bassaricyon gabbii",
            "interpretedGenus": "Bassaricyon",
            "interpretedSpecies": "gabbii",
            "interpretedAuthorityName": "J. A. Allen",
            "interpretedAuthorityYear": "1876",
            "taxonomy": (
                "Bassaricyon gabbii J. A. Allen, 1876 , Talamanca, "
                "Costa Rica . Four subspecies recognized. "
            ),
            "subspeciesAndDistribution": (
                "B. g. gabbii J. A. Allen, 1876 — Costa Rica "
                "(Talamanca Mts). B. g. lasius Harris, 1932 — N Costa Rica . "
                "B. g. pauli Enders, 1936 — Panama ( Chiriqui Mts). B. g. "
                "richardsoni J. A. Allen, 1908 — Nicaragua , possibly "
                "Guatemala and Honduras ."
            ),
        }
    elif row["docId"] == "D344591F533507062319FDD11EA4F7A7":
        row = {**row, "interpretedSpecies": "pelengensis"}
    elif row["docId"] == "D51587EFFFEC9A36F5FB1B67F605F7CC":
        row = {
            **row,
            "name": "Galidia elegans",
            "interpretedGenus": "Galidia",
            "interpretedSpecies": "elegans",
            "interpretedAuthorityName": "Geoffroy Saint-Hilaire",
            "interpretedAuthorityYear": "1837",
            "taxonomy": (
                "Galidia elegans Geoffroy Saint-Hilaire, 1837 , "
                "Madagascar , subsequently restricted to the region of "
                "“Tamatave” [= Toamasina ]. The relationships between the "
                "subspecies have not been examined in a modern "
                "phylogeographic sense and in certain cases the characters "
                "used to separate them are ambiguous. The subspecific status "
                "of certain geographically intermediate populations remains "
                "unresolved (e.g. Sambirano Basin, lake region to the west of "
                "Bemaraha, and the forests near Daraina). Captive hybrids "
                "between these different geographical forms produce fertile "
                "young. Three subspecies recognized."
            ),
            "subspeciesAndDistribution": (
                "G. e. elegans Geoffroy Saint-Hilaire, 1837 — E "
                "Madagascar (from the region surrounding the Andapa Basin S "
                "to Tolagnaro). G. e. dambrensis Tate & Rand, 1941 — N "
                "Madagascar (originally described from Montagne d’Ambre, but "
                "animals at Ankarana are also referable to this form). G. e. "
                "occidentalis Albignac, 1971 — CW Madagascar (limestone "
                "regions of Bemaraha, Namoroka & Kelifely)."
            ),
        }
    elif row["docId"] == "E84887F9FFDBD6550FF1FE1C183A3F09":
        row = {**row, "interpretedSpecies": "arenarius"}
    elif row["docId"] == "03F06D13FF4C2084089B1E090CC4FB70":
        row = {**row, "interpretedSpecies": "petersoni"}  # incorrectly has fossor
    elif row["docId"] == "3D474A54A0AE87C2FFFFAA6613D3F88D":
        row = {**row, "interpretedSpecies": "nimbasilvanus"}  # incorrectly has goliath
    elif row["docId"] == "4C3D87E8FFB56A0AFA5A97FE1406B6B8":
        row = {**row, "interpretedGenus": "Ia", "interpretedSpecies": "io"}
    elif row["docId"] == "4C3D87E8FFE26A5DFF919E4D18B8BFBC":
        row = {**row, "interpretedSpecies": "westralis"}
    elif row["docId"] == "EA7087C1FF81246CFFCAF73F06180816":
        row = {**row, "interpretedSpecies": "pirata"}
    return row


class HMWDict(CEDict):
    docId: str
    taxonomy: NotRequired[str]
    subspeciesAndDistribution: NotRequired[str]
    text: NotRequired[str]


def parse_rows(rows: list[dict[str, str]]) -> Iterable[HMWDict]:
    seen_doc_ids = set()
    for row in rows:
        try:
            if row["docId"] in seen_doc_ids:
                if row["docId"] == "D51587EFFFE89A33F1551E29FA7CFAB1":
                    continue
                raise ValueError(f"Duplicate docId {row['docId']}")
            # Duplicated entries for Phyllomys
            if row["docId"] in {
                "03C5A071FFDFFFEBFAD35C2D5248F449",  # medius
                "03C5A071FFDFFFEBFFC85B185EE8F6F3",  # lundi
                "03C5A071FFDFFFEBFFCA51B95DDEFC48",  # kerri
                "03C5A071FFDFFFEAFACE542C5E36F848",  # thomasi, = 03C5A071FFDDFFE8FACE542C5E36F84E
                "03C5A071FFDCFFEBFA17547B5EE6FD5C",  # nigrispinus, = 03C5A071FFDEFFEDFA17547D53E1FBB6
                "03C5A071FFDEFFEAFF0D502953C7FC83",  # dasythrix, = 03C5A071FFDCFFE8FF0D501653C6FC82
                "03C5A071FFDEFFEAFA115B695305F3BF",  # sulinus, = 03C5A071FFDCFFE8FA115B565305F3BE
            }:
                continue
            seen_doc_ids.add(row["docId"])
            row = fix_up_row(row)
            family = (
                row["docName"]
                .split("_")[2]
                .split(".")[0]
                .replace("Emballorunidae", "Emballonuridae")
                .replace("Phascolarctida", "Phascolarctidae")
                .replace("Orycetropodidae", "Orycteropodidae")
                .replace("Prionodonotidae", "Prionodontidae")
                .replace("Phitheciidae", "Pitheciidae")
                .replace("Craesononycteridae", "Craseonycteridae")
            )
            authority = row["interpretedAuthorityName"]
            year = row["interpretedAuthorityYear"]
            parent: str | None
            parent_rank: Rank | None
            if row["name"]:
                # has fewer OCR mistakes than row["hame"]
                name = f"{row['interpretedGenus']} {row['interpretedSpecies']}"
                name = NAME_FIXES.get(name, name)
                rank = Rank.species
                parent = family
                parent_rank = Rank.family
            else:
                match = re.match(r"^Family ([A-Z]+)", row["verbatimText"])
                assert match, pprint.pformat(row, sort_dicts=False)
                name = match.group(1).title()
                rank = Rank.family
                parent = parent_rank = None
            yield {
                "docId": row["docId"],
                "article": get_article(row["docOrigin"]),
                "page": row["docPageNumber"],
                "name": name,
                "rank": rank,
                "parent": parent,
                "parent_rank": parent_rank,
                "authority": authority,
                "year": year,
                "taxonomy": row["taxonomy"],
                "subspeciesAndDistribution": row["subspeciesAndDistribution"],
            }
            if row["subspeciesAndDistribution"]:
                genus_initial = row["interpretedGenus"][0]
                species_initial = row["interpretedSpecies"][0]
                rgx = re.compile(
                    rf"{genus_initial}\. *{species_initial}\."
                    r" *(?P<subspecies>[a-z]+)(?=[ [A-Za-z])(?P<authority>[^\d:]+),"
                    r" *(?P<year>\d{4})"
                )
                for match in rgx.finditer(row["subspeciesAndDistribution"]):
                    yield {
                        "docId": row["docId"],
                        "article": get_article(row["docOrigin"]),
                        "page": row["docPageNumber"],
                        "name": f"{name} {match.group('subspecies')}",
                        "rank": Rank.subspecies,
                        "parent": name,
                        "parent_rank": rank,
                        "authority": match.group("authority"),
                        "year": match.group("year"),
                        "text": row["subspeciesAndDistribution"],
                    }
        except Exception as e:
            print(f"Failed to parse {pprint.pformat(row, sort_dicts=False)}: {e}")
            raise


def main() -> None:
    rows = load_csv()
    data = parse_rows(rows)
    data = sorted(data, key=lambda row: -row["rank"])
    hmw_data: Iterable[CEDict] = list(
        validate_ce_parents(
            data,
            skip_missing_parents={
                "Heterocephalidae",
                "Myrmecophagidae",
                "Hylobatidae",
                "Odobenidae",
                "Microbiotheriidae",
            },
        )
    )
    hmw_data = add_classification_entries(hmw_data, dry_run=False)
    print_field_counts(dict(d) for d in hmw_data)


if __name__ == "__main__":
    main()
