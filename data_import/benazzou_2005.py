from collections.abc import Iterable
from pathlib import Path

from data_import import ce_file, lib
from taxonomy.db.constants import Rank
from taxonomy.db.models import Article

ARTICLE = "Mammalia Morocco (Benazzou 2005).pdf"
OUTPUT = Path("data_import/ce_files/benazzou_2005.ce.jsonl")

# Source spellings from the table on pages 7-8.
CLASSIFICATION = {
    "Rongeurs": {
        "Sciuridae": {"Atlantoxerus": ["getulus"], "Xerus": ["erythropus"]},
        "Gerbillidae": {
            "Gerbillus": [
                "campestris",
                "pyramidum",
                "hesperinus",
                "hoogstrali",
                "occiduus",
                "gerbillus",
                "henleyi",
                "riggenbachi",
                "nanus",
            ],
            "Dipodillus": ["maghrebi", "somini"],
            "Pachyuromys": ["duprasi"],
            "Meriones": ["shawi", "libycus", "crassus"],
            "Psammomys": ["obesus"],
        },
        "Muridae": {
            "Apodemus": ["sylvaticus"],
            "Lemniscomys": ["barbarus"],
            "Rattus": ["rattus", "norvegecus"],
            "Mus": ["spretus", "musculus"],
            "Mastomys": ["erythrileucus"],
            "Acomys": ["cahirinus"],
        },
        "Gliridae": {"Eliomys": ["quercinus"]},
        "Dipodidae": {"Jaculus": ["jaculus", "orientalis"]},
        "Hystricidae": {"Hystrix": ["cristata"]},
        "Ctenodactylidae": {"Ctenodactylus": ["vali", "goundi"]},
    },
    "Insectivores": {
        "Erinaceidae": {"Erinaceus": ["algirus"], "Paraechinus": ["aethiopicus"]},
        "Soricidae": {
            "Crocidura": [
                "russula",
                "whitakeri",
                "tarfayensis",
                "lusitania",
                "bolivari",
            ],
            "Suncus": ["estruscus"],
        },
    },
    "Macroscélides": {"Macroscelidae": {"Elephantulus": ["rozeti"]}},
    "Chiroptères": {
        "Rhinopomatidae": {"Rhinopoma": ["hardwickei", "microphyllum"]},
        "Nycteridae": {"Nycteris": ["thebaica"]},
        "Rhinolophidae": {
            "Rhinopholus": [
                "ferrumequinum",
                "euryale",
                "hipposideros",
                "mehelyi",
                "blasii",
            ],
            "Aselia": ["tridens"],
            "Hipposideros": ["caffer"],
        },
        "Vespertilionidae": {
            "Myotis": [
                "mystacinus",
                "emarginatus",
                "nattereri",
                "capaccinii",
                "biythi",
            ],
            "Pipestrellus": ["pipistrellus", "kuhli", "savii", "rueppelli"],
            "Nyctalus": ["lasiopterus"],
            "Eptesicus": ["serotinus"],
            "Otonycteris": ["hemprichi"],
            "Barbastella": ["barbastrellus"],
            "Plecotus": ["austriacus"],
            "Miniopterus": ["schereibersi"],
        },
        "Molossidae": {"Tadarida": ["teniotis"]},
    },
    "Lagomorphes": {"Leporidae": {"Lepus": ["capensis"], "Oryctolagus": ["cuniculus"]}},
    "Carnivores": {
        "Canidae": {
            "Canis": ["aureus"],
            "Vulpes": ["vulpes", "rueppelli"],
            "Fennecus": ["zerda"],
        },
        "Mustelidae": {
            "Mustela": ["nivalis", "putorius"],
            "Poecilictis": ["libyca"],
            "Mellivora": ["capensis"],
            "Lutra": ["lutra"],
        },
        "Viverridae": {"Genetta": ["genetta"], "Herpestes": ["ichneumon"]},
        "Hyaenidae": {"Hyaena": ["hyaena"]},
        "Felidae": {
            "Felis": ["libyca", "margarita", "caracal"],
            "Panthera": ["pardus"],
            "Acinonyx": ["jabatus"],
        },
    },
    "Artiodactyles": {
        "Suidae": {"Sus": ["scrofa"]},
        "Bovidae": {
            "Oryx": ["dammah"],
            "Addax": ["nasomaculatus"],
            "Gazella": ["dorcas", "cuvieri", "dama"],
            "Ammotragus": ["lervia"],
        },
        "Cervidae": {"Cervus": ["elaphus", "dama", "nippon"]},
    },
    "Primates": {"Cercopithecidae": {"Macaca": ["sylavanus"]}},
}


def extract_names() -> Iterable[lib.CEDict]:
    article = Article.get(name=ARTICLE)
    page_7_orders = {"Rongeurs", "Insectivores", "Macroscélides", "Chiroptères"}
    for order, families in CLASSIFICATION.items():
        page = "7" if order in page_7_orders else "8"
        yield {"article": article, "page": page, "name": order, "rank": Rank.order}
        for family, genera in families.items():
            yield {
                "article": article,
                "page": page,
                "name": family,
                "rank": Rank.family,
                "parent": order,
                "parent_rank": Rank.order,
            }
            for genus, epithets in genera.items():
                yield {
                    "article": article,
                    "page": page,
                    "name": genus,
                    "rank": Rank.genus,
                    "parent": family,
                    "parent_rank": Rank.family,
                }
                for epithet in epithets:
                    name = f"{genus} {epithet}"
                    yield {
                        "article": article,
                        "page": page,
                        "name": name,
                        "rank": Rank.species,
                        "parent": genus,
                        "parent_rank": Rank.genus,
                    }


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    ce_file.write_ce_file(OUTPUT, extract_names())
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
