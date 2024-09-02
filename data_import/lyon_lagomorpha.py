import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

TEXT = """
334
Lepus (Lepus) arcticus Ross.
Lepus (Lepus) arcticus bangsii Rhoads.
Lepus (Lepus) arcticus canus Preble.
Lepus (Lepus) campestris Bachman.
Lepus (Lepus) corsicanus de Winton.
Lepus (Lepus) creticus Barrett-Hamilton.
Lepus (Lepus) cyprinus Barrett-Hamilton.
Lepus (Lepus) europæus Pallas.
Lepus (Lepus) gichiganus Allen.
Lepus (Lepus) groenlandicus Rhoads.
Lepus (Lepus) labradorius Miller.
Lepus (Lepus) lilfordi de Winton.
Lepus (Lepus) othus Merriam.
Lepus (Lepus) parnassius Miller.
Lepus (Lepus) poadromus Merriam.
Lepus (Lepus) timidus Linnseus.
Lepus (Lepus) timidus ainu Barrett-Hamilton.
Lepus (Lepus) timidus lutescens Barrett-Hamilton.
Lepus (Lepus) transylvanicus Matschie.
Lepus (Lepus) varronis Miller.
Lepus (Pcecilolagus) americanus Erxleben.
335
Lepus (Poecilolagus) americanus bairdii (Hayden).
Lepus (Poecilolagus) americanus columbiensis Rhoads.
Lepus (Poecilolagus) americanus dalli Merriam.
Lepus (Poecilolagus) americanus macfarlani Merriam.
Lepus (Poecilolagus) americanus phæonotus Allen.
Lepus (Poecilolagus) americanus struthopus BangS.
Lepus (Poecilolagus) americanus virginianus (Harlan).
Lepus (Poecilolagus) bishopi Allen.
Lepus (Poecilolagus) klamathensis Merriam.
Lepus (Poecilolagus) saliens Osgood.
Lepus (Poecilolagus) washingtonii Baird.
Lepus (Macrotolagus) alleni Mearns.
Lepus (Macrotolagus) alleni palitans Bangs.
Lepus (Macrotolagus) asellus Miller.
Lepus (Macrotolagus) californicus Gray.
Lepus (Macrotolagus) californicus xanti Thomas.
Lepus (Macrotolagus) callotis Wagler.
Lepus (Macrotolagus) gaillardi Mearns.
Lepus (Macrotolagus) gaillardi battyi Allen.
Lepus (Macrotolagus) insularis Bryant.
Lepus (Macrotolagus) martirensis Stowell.
Lepus (Macrotolagus) melanotis Alearns.
Lepus (Macrotolagus) merriami Mearns.
Lepus (Macrotolagus) texianus Waterhouse.
Lepus (Macrotolagus) texianus deserticola Mearns.
Lepus (Macrotolagus) texianus eremicus Allen.
Lepus (Macrotolagus) texianus griseus Mearns.
Lepus (Macrotolagus) texianus micropus Allen.
Lepus ægyptius Audouin and Geoffroy.
Lepus arabicus Hemprich and Ehrenberg.
Lepus atlanticus de Winton.
Lepus berberanus Heuglin.
Lepus brachyurus Temminck.
Lepus capensis Linnseus.
Lepus capensis centralis Thomas.
Lepus capensis ochropus Wagner.
Lepus crawshayi de Winton.
Lepus dayanus Blandford.
Lepus etruscus Bosco.
Lepus fagani Thomas.
Lepus hainanus Swinhoe.
Lepus harterti Thomas.
Lepus hawkeri Thomas.
Lepus hypsibius Blanford.
Lepus judeæ Gray.
Lepus kabylicus de Winton.
Lepus mandschuricus Radde.
Lepus mediterraneus Wagner.
Lepus microtis Heuglin.
Lepus monticularis Thomas.
Lepus nigricollis F. Cuvier.
Lepus oiostolus Hodgson.
Lepus omanensis Thomas.
Lepus pallidor Barrett-Hamilton.
Lepus pallipes Hodgson.
Lepus peguensis Blyth.
Lepus ruficaudatus Is. Geoffroy.
Lepus salae Jentink.
Lepus saxatilis F. Cuvier.
Lepus schlumbergeri St. Loup.
Lepus sechuensis de Winton.
Lepus siamensis Bonhote.
336
Lepus sinaiticus Hemprich and Ehrenberg.
Lepus sinensis Gray.
Lepus somalensis Heuglin.
Lepus swinhoei Thomas.
Lepus syriacus Hemprich and Ehrenberg.
Lepus tigrensis Blanford.
Lepus tolai Pallas.
Lepus tunetæ de Winton.
Lepus victoriæ Thomas.
Lepus whitakeri Thomas.
Lepus whytei Thomas.
Lepus yarkandensis Gihither.
Lepus zechi Matschie.
Oryctolagus cuniculus (Linnaeus).
Sylvilagus (Sylvilagus) andinus (Thomas).
Sylvilagus (Sylvilagus) arizonæ (Allen).
Sylvilagus (Sylvilagus) arizonæ confinis (Allen).
Sylvilagus (Sylvilagus) arizonæ major (Alearns).
Sylvilagus (Sylvilagus) arizonæ minor (Mearns).
Sylvilagus (Sylvilagus) baileyi (Merriam).
Sylvilagus (Sylvilagus) braziliensis (Linngeus).
Sylvilagus (Sylvilagus) cumanicus (Thomas).
Sylvilagus (Sylvilagus) defilippii (Cornalia).
Sylvilagus (Sylvilagus) durangæ (Allen).
Sylvilagus (Sylvilagus) floridanus (Allen).
Sylvilagus (Sylvilagus) floridanus alacer (Bangs).
Sylvilagus (Sylvilagus) floridanus audubonii (Baird).
Sylvilagus (Sylvilagus) floridanus aztecus (Allen).
Sylvilagus (Sylvilagus) floridanus caniclunis (Miller).
Sylvilagus (Sylvilagus) floridanus chapmani (Allen).
Sylvilagus (Sylvilagus) floridanus holzneri (Mearns).
Sylvilagus (Sylvilagus) floridanus mallurus (Thomas).
Sylvilagus (Sylvilagus) floridanus mearnsi (Allen).
Sylvilagus (Sylvilagus) floridanus persultator (Elliot).
Sylvilagus (Sylvilagus) floridanus pinetis (Allen).
Sylvilagus (Sylvilagus) floridanus rigidus (Mearns).
Sylvilagus (Sylvilagus) floridanus sanctidiegi (Miller).
Sylvilagus (Sylvilagus) floridanus subcinctus (Miller).
Sylvilagus (Sylvilagus) floridanus transitionalis (Bangs).
Sylvilagus (Sylvilagus) floridanus yucatanicus (Miller).
Sylvilagus (Sylvilagus) gabbi (Allen).
Sylvilagus (Sylvilagus) grangeri (Allen).
Sylvilagus (Sylvilagus) graysoni (Allen).
Sylvilagus (Sylvilagus) incitatus (Bangs).
Sylvilagus (Sylvilagus) insolitus (Allen).
Sylvilagus (Sylvilagus) laticinctus (Elliot).
Sylvilagus (Sylvilagus) laticinctus rufipes (Elliot).
Sylvilagus (Sylvilagus) margaritæ (Miller).
Sylvilagus (Sylvilagus) minensis Thomas.
Sylvilagus (Sylvilagus) nigronuchalis (Hartert).
Sylvilagus (Sylvilagus) nuttallii (Bachman).
Sylvilagus (Sylvilagus) orinoci Thomas.
Sylvilagus (Sylvilagus) orizabæ (Merriam).
Sylvilagus (Sylvilagus) paraguensis Thomas.
Sylvilagus (Sylvilagus) parvulus (Allen).
Sylvilagus (Sylvilagus) russatus (Allen).
Sylvilagus (Sylvilagus) simplicanus (Miller).
Sylvilagus (Sylvilagus) superciliaris (Allen).
Sylvilagus (Syilvilagus) surdaster Thomas.
Sylvilagus (Sylvilagus) truei (Allen).
Sylvilagus (Sylvilagus) veræcrucis (Thomas).
Sylvilagus (Microlagus) bachmani (Waterhouse).
337
Sylvilagus (Microlagus) bachmani ubericolor (Miller).
Sylvilagus (Microlagus) cerrosensis (Allen).
Sylvilagus (Microlagus) cinerascens (Allen).
Sylvilagus (Microlagus) peninsularis (Allen).
Limnolagus aquaticus (Bachman).
Limnolagus aquaticus attwateri (Allen).
Limnolagus palustris (Bachman).
Limnolagus palustris paludicola (Miller and Bangs).
Limnolagus telmalemonus (Elliot).
Brachylagus idahoensis (Merriam).
Caprolagus hispidus (Pearson).
Pronolagus crassicaudatus (Is. Geoffroy).
Pronolagus crassicaudatus curryi (Thomas).
Pronolagus crassicaudatus nyikæ (Thomas).
Romerolagus nelsoni Merriam.
Nesolagus netscheri (Jentink).
Pentalagus furnessi (Stone).
Ochotona (Ochotona) curzoniæ (Hodgson).
Ochotona (Ochotona) daurica (Pallas).
Ochotona (Ochotona) koslowi Büchner.
Ochotona (Ochotona) ladacensis Günther.
Ochotona (Ochotona) melanostoma Büchner.
Ochotona (Pika) alpina (Pallas).
Ochotona (Pika) collaris (Nelson).
Ochotona (Pika) cuppes Bangs.
Ochotona (Pika) hyperboreus (Pallas).
Ochotona (Pika) kolymensis Allen.
Ochotona (Pika) littoralis (Peters).
Ochotona (Pika) princeps (Richardson).
Ochotona (Pika) pusilla (Pallas).
Ochotona (Pika) saxatilis Bangs.
Ochotona (Pika) schisticeps (Merriam).
Ochotona (Conothoa) erythrotis Biichner.
Ochotona (Conothoa) roylei (Ogilby).
Ochotona rufescens (Gray).
Ochotona rutila (Severzow).
"""

SOURCE = lib.Source("lyon-lagomorpha.txt", "Lagomorpha (Lyon 1904).pdf")


def extract_names() -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()
    current_page = None
    for line in TEXT.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.isdigit():
            current_page = line
            continue
        match = re.fullmatch(
            r"(?P<name>[A-Z][a-z]+( \([A-Z][a-z]+\))? [a-zæ]+( (?!de )[a-zæ]+)?) (?P<author>.*?)\.",
            line,
        )
        assert match is not None, line
        name = match.group("name")
        author = match.group("author")
        words = name.split()
        num_words = len(words)
        if any(word.startswith("(") for word in words):
            num_words -= 1
        if num_words == 2:
            rank = Rank.species
        elif num_words == 3:
            rank = Rank.subspecies
        else:
            assert False, line
        assert current_page is not None
        yield {
            "name": name,
            "authority": author,
            "page": current_page,
            "article": art,
            "rank": rank,
        }


def main() -> None:
    names = extract_names()
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
