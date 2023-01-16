from typing import Any, Dict, List

from data_import.lib import NameConfig, associate_names
from taxonomy.db.constants import CommentKind

raw_data = """Alectops ater Gray    1866    BMNH
Anoura wiedii   Peters  1869    MNHN
Arctibeus leucomus  Gray    1848    BMNH
Atalapha cineria brasiliensis   Pira    1905
Celaeno brocksiana  Leach   1821    D. Brookes Museum
Centuria mexicanus  Saussure    1860    MNHN
Chilonycteris osburnii  Tomes   1861    BMNH
Choeronycteris mexicana Tschudi 1844    ZMB
Choeronycteris peruana  Tschudi 1844    ZMB
Depanycteris isabella   Thomas  1920    BMNH
Dermanura cineria   Gervais 1855    MNHN
Diphylla ecaudata   Spix    1823    ZSM
Desmodus fuscus Burmeister  1854    ZMB
Desmodus mordax Burmeister  1879    ZMB
Dysopes amplexicaudatus Wagner  1850    NMW
Dysopes caecus  Rengger 1830
Dysopes holosericeus    Wagner  1843    NMW
Dysopes leucopleura Wagner  1843    NMW
Dysopes olivaceofuscus  Wagner  1850    NMW
Dysopes rufocastaneus   Schinz  1844    ZMB
Dysopes thyropterus Schinz  1844    ZMB
Emballonura brunnea Gervais 1855    MNHN
Furia horrens   F. Cuvier   1828    MNHN
Furipterus caerulescens Tomes   1856    Tomes Collection
Glossophaga ecaudata    É. Geoffroy Saint-Hilaire   1818    MNHN
Hyonycteris albiventer  Tomes   1856    Tomes Collection
Ischnoglossa nivalis    Saussure    1860    MNHN
Lasiurus borealis salinae   Thomas  1902    BMNH
Lasiurus grayi  Tomes   1857    BMNH
Lophostoma silvicolum   d'Orbigny   1835    MNHN
Macrotus mexicanus  Saussure    1860    MNHN
Molossops aequatorianus Cabrera 1917    MNCN
Molossus acuticaudatus  Desmarest   1820    MNHN
Molossus aztecus    Saussure    1860    MNHN
Molossus (Molossops) brachymeles    Peters  1865    ZSM
Molossus crassicaudatus É. Geoffroy Saint-Hilaire   1805
Molossus fumarius   Spix    1823    ZSM
Molossus laticaudatus   É. Geoffroy Saint-Hilaire   1805
Molossus moxensis   d'Orbigny   1837    MNHN
Molossus myosurus   Tschudi 1844    ZSM
Molossus ursinus    Spix    1823    ZSM
Myotis nigricans osculatii  Cabrera 1917    MNCN
Myotis thomasi  Cabrera 1901    MNCN
Noctilio affinis    d'Orbigny   1837    MNHN
Noctilio dorsatus   Desmarest   1818    BMNH
Noctilio rufipes    d'Orbigny   1835    MNHN
Noctilio unicolor   Desmarest   1818
Noctilio vittaus    Schinz  1821    ZMB
Nycticejus ega  Gervais 1856    MNHN
Nycticejus varius   Poppig  1835
Nyctiplanus rotundatus  Gray    1849
Peropteryx leucoptera   Peters  1867    ZMB
Phyllodia parnellii Gray    1843    BMNH
Phyllostoma angusticeps Gervais 1855    MNHN
Phyllostoma bennettii   Gray    1838    BMNH
Phyllostoma bernicaudum Schinz  1821
Phyllostoma brachyotum  Schinz  1821
Phyllostoma chrysocomos Wagner  1855
Phyllostoma erythromos  Tschudi 1844
Phyllostoma innominatum Tschudi 1844
Phyllostoma longifolium Wagner  1843    NMW
Phyllostoma macrophyllum    Schinz  1821    ZMB
Phyllostoma maximus Wied-Neuwied    1821
Phyllostoma obscurum    Schinz  1821    ZMB
Phyllostoma rotundum    É. Geoffroy Saint-Hilaire   1810    MNHN
Phyllostoma spiculatum  Lichtenstein    1825    ZMB
Phyllostoma superciliatum   Schinz  1821    ZMB
Proboscidea rivalis Spix    1823    ZSM
Promops bonariensis Peters  1874
Rhinolophus ecaudatus   Schinz  1821    ZMB
Schizostoma minutum Gervais 1855    MNHN
Spectrellum macrurum    Gervais 1856    MNHN
Stenoderma tolteca  Saussure    1860    MNHN
Sturnira spectrum   Gray    1842    BMNH
Thyroptera tricolor Spix    1823    ZSM
Trachops fuliginosus    Gray    1857
Tylostoma mexicana  Saussure    1850    MNHN
Vampyrus auricularis    Saussure    1860    MNHN
Vampyrus cirrhosus  Spix    1823    ZSM
Vespertilio albescens   É. Geoffroy Saint-Hilaire   1806    MNHN
Vespertilio auripendulus    Shaw    1800
Vespertilio borealis    Müller  1776
Vespertilio brasiliensis    Desmarest   1819
Vespertilio brasiliensis    Spix    1823    ZSM
Vespertilio caninus Schinz  1821    ZMB
Vespertilio cinnamomeus Wagner  1855    ZSM
Vespertilio derasus Burmeister  1854    ZMB
Vespertilio espadae Cabrera 1901    MNCN
Vespertilio furinalis   d'Orbigny & Gervais 1847    MNHN
Vespertilio guianensis  Lacépède    1789
Vespertilio hastatus    Pallas  1767
Vespertilio hypothrix   d'Orbigny & Gervais 1847    MNHN
Vespertilio innoxius    Gervais 1841    MNHN
Vespertilio (Myotis) kinnamon   Gervais 1856    MNHN
Vespertilio labialis    Kerr    1792
Vespertilio leptura Schreber    1774
Vespertilio maximiliani Fischer 1829
Vespertilio maximus É. Geoffroy Saint-Hilaire   1806    MNHN
Vespertilio minor   Fermin  1765
Vespertilio nasutus Shaw    1800
Vespertilio nigricans   Schinz  1821    ZMB
Vespertilio nitens  Wagner  1855
Vespertilio noveboracensis  Erxleben    1777
Vespertilio oxyotus Peters  1866
Vespertilio ruber   É. Geoffroy Saint-Hilaire   1806    MNHN
Vespertilio soricinus   Pallas  1766
Vespertilio spectrum    Linnaeus    1758
Vespertilio spixii  Fischer 1829
Vespertilio splendidus  Wagner  1845    ZSM
Vespertilio subflavus   F. Cuvier   1832    MNHN
Vespertilio villosissimus   É. Geoffroy Saint-Hilaire   1806    MNHN
"""

if __name__ == "__main__":
    data = [line.split("\t") for line in raw_data.splitlines()]

    raw_names: list[dict[str, Any]] = []
    for line in data:
        name: dict[str, Any] = {
            "original_name": line[0],
            "authority": line[1],
            "year": line[2],
            "raw_text": ", ".join(line),
        }
        if len(line) > 3:
            name["museum"] = line[3]
        raw_names.append(name)

    names = associate_names(
        raw_names,
        NameConfig(
            {
                "Anoura wiedii": "Anura wiedii",
                "Choeronycteris peruana": "Glossophaga (Choeronycteris) peruana",
                "Nycticejus varius": "Nysticeius varius",
            },
            {"Poppig": "Poeppig"},
            {("Myotis nigricans osculatii", "Cabrera")},
        ),
    )

    for name in names:
        if not name.get("name_obj"):
            continue
        comment = "Type specimen could not be located."
        if "museum" in name:
            comment += f' It is most likely in the {name["museum"]}.'
        name["name_obj"].add_comment(
            CommentKind.type_specimen,
            comment,
            "Chiroptera Neotropis-European types.pdf",
            "appendix 6",
        )
