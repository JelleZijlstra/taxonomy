import csv
import enum
import html
import json
import re
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, TypeVar

from taxonomy.db import helpers
from taxonomy.db.constants import Rank
from taxonomy.db.models.classification_entry import (
    ClassificationEntry,
    ClassificationEntryTag,
)

from . import lib
from .lib import PeekingIterator

SOURCE = lib.Source("old/msw3-all.csv", "Mammalia-review (MSW3)")

RANK_COLUMNS = [
    "Order",
    "Suborder",
    "Infraorder",
    "Superfamily",
    "Family",
    "Subfamily",
    "Tribe",
    "Genus",
    "Subgenus",
    "Species",
    "Subspecies",
]


class TokenType(enum.Enum):
    OPEN_TAG = 1
    CLOSE_TAG = 2
    COMMA = 3
    SEMICOLON = 4
    TEXT = 5
    DATE = 6
    SQUARE_BRACKETS = 7
    PERIOD = 8
    PARENTHESES = 9
    OPEN_PAREN = 10
    CLOSE_PAREN = 11


@dataclass(frozen=True)
class Token:
    type: TokenType
    value: str


T = TypeVar("T")


REPLACEMENTS = {
    "; see Cabrera (1954)<i>.</i>": ".",
    "orii</i>,</b> Kuroda": "orii</i></b> Kuroda",
    "<u>Insular</u>:": "",
    "<u>Mainland</u>:": "",
    "<i>. </i><u>Not": "; <u>Not",
    ". <u>Remaining names (most from small islands), not allocated to subspecies</u>:": (
        "; <u>Unallocated:</u>"
    ),
    "<b><i>; ": "; <b><i>",
    "<b><i>.</i></b>": ".",
    "<b> (</b>": " (",
    "<i>.</i>": ".",
    "<i> </i>": " ",
    "<b> </b>": " ",
    ";.": ";",
    ". <u>Not": "; <u>Not",
    "<i>Chlamydophorus</i> Wagler, 1830, is an unjustified emendation of <i>Chlamyphorus</i> Harlan; the names <i>Chlamiphorus</i> Contreras, 1973; <i>Chlamydephorus</i> Lenz, 1831, and <i>Chlamydiphorus</i> Bonaparte, 1831, are incorrect subsequent spellings of <i>Chlamyphorus</i> Harlan.": (
        "<i>Chlamydophorus</i> Wagler, 1830; <i>Chlamiphorus</i> Contreras, 1973; <i>Chlamydephorus</i> Lenz, 1831; <i>Chlamydiphorus</i> Bonaparte, 1831."
    ),
    "</i></b><i>s</i>": "s</i></b>",
    "persicus auctorum ignotus": "persicus",
    ">of sundry authors but not <i>Kangurus veterum </i>Lesson and Garnot (see below)": (
        "> [of sundry authors but not <i>Kangurus veterum </i>Lesson and Garnot (see below)]"
    ),
    "<i>,</i>": ",",
    "subspecies</u>:": "subspecies:</u>",
    "Lamotte and Petter (1981);": "Lamotte and Petter, 1981;",
    "(Matschie), 1895": "(Matschie, 1895)",
    ". See McKenna and Bell (1997).": "[See McKenna and Bell, 1997].",
    "; see Daams and de Bruijn (1995); Rossolimo et al. (2001).": ".",
    "1876),<i> ": "1876); <i>",
    "<i>golzmajeri</i> 1960": "<i>golzmajeri</i> Anonymous, 1960",
    ". The following names belong to this species or another species of the same species group, but seem impossible to allocate to a subspecies, according to Groves (2001<i>c</i>):": (
        "; <b>unallocated:</b> "
    ),
    "1812: plate only, not text)": "1812) [plate only, not text]",
    " , various authors": " [various authors]",
    "(Wroughton, 1908;": "(Wroughton, 1908);",
    "Layard, in Blyth": "Layard in Blyth",
    "</b>1916;": "</b> Anonymous, 1916;",
    ", un-necessary renaming": "",
    ", unnecessary naming": "",
    "Aspalacina, Gray, 1825": "Aspalacina Gray, 1825",
    "Spalacidae Gray 1821": "Spalacidae Gray, 1821",
    "Baiomyini new tribe (see <i>Baiomys </i> account); ": "",
    "Ochrotomyini new tribe (see <i>Ochrotomys </i>account); ": "",
    "Neotominae Merriam 1894": "Neotominae Merriam, 1894",
    "Hershkovitz, 1966<i>b</i>; Reithrodontomyini": (
        "Hershkovitz, 1966; Reithrodontomyini"
    ),
    "Hesperomyini, Simpson, 1945": "Hesperomyini Simpson, 1945",
    "Reithrodontina, Steppan, 1995": "Reithrodontina Steppan, 1995",
    " (Rhombomyini Pavlinov and Rossolimo, 1987; Rhombomyina Pavlinov, Dubrovskii, Rossollimo, and Potapova, 1990)": (
        "; Rhombomyini Pavlinov and Rossolimo, 1987; Rhombomyina Pavlinov, Dubrovskii, Rossollimo, and Potapova, 1990"
    ),
    "1872, not Ellobiinae Adams, 1858": "1872",
    ", justified emendation by Pavlinov et al., 1995<i>a</i>": "",
    "<i>longior</i>, <i>media</i>": "<i>longior</i> (Pallas, 1779); <i>media</i>",
    " (in Wang et al., 1996)": "",
    "1828], <i>Steneocranius": "1828]; <i>Steneocranius",
    "Fatio, 1869, not Millet, 1828)": "Fatio, 1869) [not Millet, 1828]",
    "Schrank, 1798, not Linnaeus, 1758)": "Schrank, 1798) [not Linnaeus, 1758]",
    "Kostenko [date unknown": "Kostenko, 1-1 [date unknown",
    "1953<i> nigrescens": "1953; <i>nigrescens",
    "Martin; 1984;": "Martin, 1984;",
    "1941:<i>": "1941; <i>",
    " (See Zagordnyuk, 1992<i>b</i>, for comments regarding scientific names applied to Ukranian samples).": (
        ""
    ),
    "1941], <i>pecchioli": "1941]; <i>pecchioli",
    "Gould, 1853 <i>fulvoventer": "Gould, 1853; <i>fulvoventer",
    "Miller, 1902s": "Miller, 1902",
    "; both names are <i>nomina nuda</i> (Miller, 1902:759).": ".",
    "1867) <i>wroughtoni": "1867); <i>wroughtoni",
    "Allen1915": "Allen, 1915",
    "] [": "; ",
    "Allozyme studies revealed 4-5 main groups of populations in this species (Hafner and Sullivan, 1995). Synonyms listed here follow these groupings: ": (
        ""
    ),
    "Leporidae Gray, 1821, Leporinorum Fischer, 1817, Oryctolaginae Gureev, 1948, Pentalaginae Gureev, 1948": (
        "Leporidae Gray, 1821; Leporinorum Fischer, 1817; Oryctolaginae Gureev, 1948; Pentalaginae Gureev, 1948"
    ),
    ".<u> East Africa</u>:<u> </u>": "; ",
    ". <u>Arabia and Near East</u>:": "; ",
    ". <u>Northwest Africa (Mahgreb)</u>:": "; ",
    ". <u>South of Isthmus of Panama</u>:": "; ",
    ".<u> North of Isthmus of Panama</u>:": "; ",
    ".<u> Mexico and Central America</u>:": "; ",
    "; <i>pinetis robustus </i>(Bailey, 1905).": ".",
    " as <i>C. thomasi </i>and <i>C. avia</i>.": ".",
    "1888, an insect).": "1888, an insect].",
    " in Kitchener et al., 1993<i>a</i>": "",
    "<b>s </b>": "s ",
    "Temminck (ex Kittlitz),": "Temminck ex Kittlitz,",
    "Kerr 1792:99": "Kerr, 1792",
    "Harrison, 1958 <b><i>sudani": "Harrison, 1958; <b><i>sudani",
    "[not Gervais, 1855 or Gervais, 1856)": "[not Gervais, 1855 or Gervais, 1856]",
    "Handley and Ferris (1972)": "Handley and Ferris, 1972",
    "Goldman 1915<b>;": "Goldman, 1915; <b>",
    "Iredale (ex MacGillivray)": "Iredale ex MacGillivray",
    "1829:117": "1829",
    "<b>; </b>": "; ",
    " (in Kitchener et al., 1995<i>b</i>).": " [in Kitchener et al., 1995<i>b</i>].",
    "<u> </u>": " ",
    "author unknown, date 1797 or 1800": "Anonymous, 1797-1800",
    "<u></u>": " ",
    "; 1924": ", 1924",
    "<sup>": "",
    "</sup>": "",
    "</i>a ": "a</i> ",
    "Horsfield (Hodgson, 1855 MS.)": "Horsfield, 1855 [Hodgson, 1855 MS.]",
    ". Including:": "; ",
    "Flower, 1869, Galidiinae Gill, 1872": "Flower, 1869; Galidiinae Gill, 1872",
    "s<i>triata </i>": "<i>striata</i> ",
    "(Gray, 1847, not Gray, 1836)": "(Gray, 1847) [not Gray, 1836]",
    "; .": ".",
    "</i>[sic]": "</i>",
    "G.[Baron] Cuvier": "G. Cuvier",
    "G. [Baron] Cuvier": "G. Cuvier",
    "d’Orbigny (<i>in</i> Gray 1865)": "d'Orbigny in Gray, 1865",
    "Gloger, 1842 (<i>in</i> Gray, 1865)": "Gloger, 1842 [in Gray, 1865]",
    "Gmelin (<i>in</i> Gray, 1837)": "Gmelin in Gray, 1837",
    "(Humbolt, <i>in</i> Coues, 1877)": "Humboldt in Coues, 1877",
    "<i>edmondi, </i>": "<i>edmondi</i> Anonymous, ",
    "1896),<i> ": "1896); <i>",
    "<b>(</b>": "(",
    "<u>names based on hybrids between <i>lelwel </i>and <i>cokii, swaynei </i>or <i>tora</i></u>:": (
        "<u>unallocated:</u>"
    ),
    "(</b>": "</b>(",
    '"Œegosceridae" (Aegocerotidae)': "Aegocerotidae",
    "1910,<i> canescens": "1910; <i>canescens",
}
REGEX_REPLACEMENTS = {
    r"\. <u>The following taxa are from [A-Za-z, \-]+</u>:": "; ",
    r"<u>The following taxa are from [A-Za-z, \-]+</u>:": "",
    r". (<u>)?The following (taxa )?(are|is) from [A-Za-z ,]+(</u>)?:": "; ",
    r"(\d{4}\)?), (?=<b>|\*?<i>)": r"\1; ",
    r" \(([A-Z][a-z]+) Chaline, Mein, and F\. Petter, 1977\)": (
        r"; \1 Chaline, Mein, and F. Petter, 1977"
    ),
    r"; see [^;\]]+\.$": ".",
    r"\. See [^;]+\.$": ".",
    r"\. Some synonyms listed and much of the southern.*": ".",
    r"\. Ellerman and Morrison-Scott \(1951:608\) and J\. T\. Marshall, Jr\. \(1998\) listed .*": (
        "."
    ),
    r"\d{4} ?\[(\d{4})\]": r"\1",
    r"<u>\(\d\) [^<]+</u>:": "",
    r"^<u>(South Africa|South of Isthmus of Panama|North of Mexico)</u>:": "",
    r"\s+": " ",
}


def clean_text(text: str) -> str:
    # comments ended up in synonyms
    if text.startswith(
        (
            "The largest, darkest, and longest-tailed species of <i>Calomyscus</i>",
            "Subgenus <i>Microtus</i> , <i>socialis</i> species group",
            "Subgenus <i>Lophuromys</i> , <i>L. aquilus</i> species group",
            "<i>Sylvaemus</i>  group. A close relative of <i>A. mystacinus</i>",
            "2n = 62, FNa = 74-76 (Volobouev et al., 2002<i>a</i> ).",
            "Member of the <i>D. incomtus</i>  species complex",
            "Samples from the range of <i>D. cabrali</i>  were originally",
            "This species occurs with <i>M. natalensis</i>  and <i>M",
            "Although usually listed as a subspecies or synonym of <i>M</i> . ",
            "Subgenus <i>Mus</i> . Known by 21 specimens collected in dry",
            "A very distinctive species restricted to montane forest",
            "A distinctive species with golden brown upperparts, white",
            "Recognized as a species by Emmons and Feer (1990) but not by",
            "IUCN – Lower Risk (nt)",
            "Reviewed by Reeves and Leatherwood",
        )
    ):
        return ""
    if (
        text
        == "As proposed by Smeenk and Kaneko (2000), the International Commission on Zoological Nomenclature (2001<i>a</i> ) has ruled (Opinion 1978) that the following synonyms are invalid: <i>elegans </i>(Temminck, 1844) [not of Ogilby, 1838], <i>javanicus </i>(Schinz, 1845), and <i>lasiotis </i>(Thomas, 1880); see also Ellerman and Morrison-Scott (1951) and Corbet (1978<i>c</i>)."
    ):
        return "<i>elegans<i> (Temminck, 1844) [not of Ogilby, 1838]; <i>javanicus</i> (Schinz, 1845); <i>lasiotis</i> (Thomas, 1880)."
    text = html.unescape(text)
    if "Calibella" in text:
        text = "<i>Anthopithecus </i>F. Cuvier, 1829; <i>Arctopithecus </i>G. Cuvier, 1819; <i>Hapale </i>Illiger, 1811; <i>Hapales </i>F. Cuvier, 1829; <i>Harpale </i>Gray, 1821; <i>Iacchus </i>Spix, 1823; <i>Jacchus </i>É. Geoffroy, 1812; <i>Midas</i> É. Geoffroy, 1828 [not of Latreille, 1796]; <i>Ouistitis</i> Burnett, 1826; <i>Sagoin </i>Desmarest, 1804; <i>Sagoinus </i>Kerr, 1792; <i>Sagouin </i>Lacépède, 1799; <i>Saguin </i>Fischer, 1803; <i>Mico </i>Lesson, 1840; <i>Liocephalus </i>Wagner, 1840; <i>Micoella </i>Gray, 1870; <i>Cebuella </i>Gray, 1866; <i>Calibella </i>van Roosmalen and van Roosmalen, 2003."

    # a bunch of names at the end lack bolding. Added it manually from the physical book
    if "which is preoccupied by <i>occipitalis</i> Dice, 1925" in text:
        text = "*<i>altivallis</i>  Rhoads, 1895; *<i>angularis</i> Merriam, 1897; *<i>argusensis</i> Huey, 1931; *<i>diaboli</i> Grinnell, 1914; *<i>infrapallidus</i> Grinnell, 1914; <i>lorenzi</i> Huey, 1940; *<i>neglectus</i> Bailey, 1914; <i>pallescens</i> Rhoads, 1895; <i>perpes</i> Merriam, 1901; <i>piutensis</i> Grinnell and Hill, 1936; <i>sanctidiegi</i> Huey, 1945; *<i>scapterus</i> Elliot, 1904; <b><i>abbotti</i></b> Huey, 1928; <b><i>abstrusus</i></b> Hall and Davis, 1935; <b><i>actuosus</i></b> Kelson, 1951; *<b><i>albatus</i></b> Grinnell, 1912; <i>aderrans</i> Huey, 1939; <i>boregoensis</i> Huey, 1939; <i>crassus</i> Chattin, 1941; <i>flavidus</i> Goldman, 1931; *<i>harquahalae</i> Grinnell, 1936; <i>patulus</i> Goldman, 1938; <b><i>albicaudatus</i></b> Hall, 1930; *<b><i>alexandrae</i></b> Goldman, 1933; *<b><i>alpinus</i></b> Merriam, 1897; <b><i>alticolus</i></b> J. A. Allen, 1899; <b><i>analogus</i></b> Goldman, 1938; <b><i>angustidens</i></b> Baker, 1953; <b><i>anitae</i></b> J. A. Allen, 1898; *<i>apache</i> Bailey, 1910; *<b><i>aphrastus</i></b> Elliot, 1903; <b><i>aureiventris</i></b> Hall, 1930; *<b><i>aureus</i></b> J. A. Allen, 1893; *<i>latirostris</i> Merriam, 1901;<b><i> awahnee</i></b> Merriam, 1908; *<b><i>baileyi</i></b> Merriam, 1901; <b><i>basilicae</i></b> Benson and Tillotson, 1940 [a renaming of <i>occipitalis</i> Benson and Tillotson, 1939, which is preoccupied by <i>occipitalis</i> Dice, 1925, a fossil <i>Thomomys</i> from Rancho La Brea, California]; <b><i>birdseyei</i></b> Goldman, 1937; <b><i>bonnevillei</i></b> Durant, 1946; <b><i>borjasensis</i></b> Huey, 1945; <b><i>brazierhowelli</i></b> Huey, 1960; <b><i>brevidens</i></b> Hall, 1932; <b><i>cactophilus</i></b> Huey, 1929; <b><i>camoae</i></b> Burt, 1937; *<b><i>canus</i></b> Bailey, 1910; <b><i>catalinae</i></b> Goldman, 1931; <i>hueyi</i> Goldman, 1938; <i>parvulus</i> Goldman, 1938; <b><i>catavinensis</i></b> Huey, 1931; <b><i>centralis</i></b> Hall, 1930; *<b><i>cervinus</i> </b>J. A. Allen, 1895; *<b><i>chrysonotus</i></b> Grinnell, 1912; <b><i>cinereus</i></b> Hall, 1932; <b><i>collis</i></b> Hooper, 1940; <b><i>concisor</i></b> Hall and Davis, 1935; <b><i>confinalis</i></b> Goldman, 1936; <b><i>connectens</i></b> Hall, 1936; <b><i>contractus</i></b> Durrant, 1946; <b><i>convergens</i></b> Nelson and Goldman, 1934; <b><i>convexus</i></b> Durrant, 1939; <b><i>cultellus</i></b> Kelson, 1951; <b><i>cunicularius</i></b> Huey, 1945; <b><i>curtatus</i></b> Hall, 1932; <b><i>depressus</i></b> Hall, 1932; *<b><i>desertorum</i></b> Merriam, 1901; <i>cedrinus</i> Huey, 1955; <i>desitus</i> Goldman, 1936; <i>hualpaiensis</i> Goldman, 1936; *<i>muralis</i> Goldman, 1936; <i>suboles</i> Goldman, 1928; <b><i>detumidus</i></b> Grinnell, 1935; <b><i>dissimilis</i></b> Goldman, 1931; <b><i>divergens</i></b> Nelson and Goldman, 1934; <b><i>estanciae</i></b> Benson and Tillotson, 1939; *<b><i>fulvus</i></b> Woodhouse, 1852; <i>mutabilis</i> Goldman, 1933; <i>nasutus</i> Hall, 1932; <i>operosus</i> Hatfield, 1942; <b><i>fumosus</i></b> Hall, 1932; <b><i>guadalupensis</i></b> Goldman, 1936; <b><i>homorus</i></b> Huey, 1949; <b><i>howelli</i></b> Goldman, 1936; <b><i>humilis</i></b> Baker, 1953; <b><i>imitabilis</i></b> Goldman, 1939; <b><i>incomptus</i></b> Goldman, 1939; <b><i>internatus</i></b> Goldman, 1936; <b><i>jojobae</i></b> Huey, 1945; <b><i>juarezensis</i></b> Huey, 1945; <b><i>lachuguilla</i></b> Bailey, 1902; <b><i>lacrymalis</i></b> Hall, 1932; *<b><i>laticeps</i></b> Baird, 1855; <i>minor</i> Bailey, 1914; <i>silvifugus</i> Grinnell, 1935; <b><i>latus</i></b> Hall and Davis, 1935; <b><i>lenis</i></b> Goldman, 1942; *<b><i>leucodon</i></b> Merriam, 1897; <b><i>levidensis</i></b> Goldman, 1942; <b><i>limitaris</i></b> Goldman, 1936; <b><i>limpiae</i></b> Blair, 1939; <b><i>litoris</i></b> Burt, 1940; <b><i>lucidus</i></b> Hall, 1932; <b><i>lucrificus</i></b> Hall and Durham, 1938; *<b><i>magdalenae</i></b> Nelson and Goldman, 1909; <b><i>martirensis</i></b> J. A. Allen, 1898; *<b><i>mearnsi</i></b> Bailey, 1914; <i>alienus</i> Goldman, 1938; <i>caneloensis</i> Lange, 1959; <i>carri</i> Lange, 1959; <i>chiricahuae</i> Nelson and Goldman, 1934; <i>collinus</i> Goldman, 1931; <i>extenuatus</i> Goldman, 1935; <i>grahamensis</i> Goldman, 1931; *<b><i>mewa</i></b> Merriam, 1908; <b><i>minimus</i></b> Durrant, 1939; <b><i>modicus</i></b> Goldman, 1931; <i>proximus</i> Burt and Campbell, 1934; <b><i>morulus</i></b> Hooper, 1940; <b><i>nanus</i></b> Hall, 1932; <b><i>navus</i></b> Merriam, 1901; <i>acrirostratus</i> Grinnell, 1935; <i>agricolaris</i> Grinnell, 1935; *<b><i>neglectus</i></b> Bailey, 1914; <b><i>nesophilus</i></b> Durrant, 1936; <b><i>nigricans</i></b> Rhoads, 1895; affinis Huey, 1945; *cabezonae Merriam, 1901; *jacinteus Grinnell and Swarth, 1914; puertae Grinnell, 1914; *<b>operarius</b> Merriam, 1897; <b>optabilis</b> Goldman, 1936; <b>opulentus</b> Goldman, 1935; <b>osgoodi</b> Goldman, 1931; <b>paguatae</b> Hooper, 1940; <b>pascalis</b> Merriam, 1901; ingens Grinnell, 1932; *<b>pectoralis</b> Goldman, 1936; <b>peramplus</b> Goldman, 1931; rufidulus Hoffmeister, 1955; <b>perditus</b> Merriam, 1901; <b>perpallidus</b> Merriam, 1886; amargosae Grinnell, 1921; *melanotis Grinnell, 1918; mohavensis Grinnell, 1918; *oreoecus Burt, 1932; *providentialis Grinnell, 1931; <b>pervagus</b> Merriam, 1901; <b>pervarius</b> Goldman, 1938; *<b>phelleoecus</b> Burt, 1933; <b>pinalensis</b> Goldman, 1938; <b>planirostris</b> Burt, 1931; absonus Goldman, 1931; boreorarius Durham, 1952; nicholi Goldman, 1938; trumbullensis Hall and Davis, 1934; virgineus Goldman, 1937; <b>planorum</b> Hooper, 1940; <b>powelli</b> Durrant, 1955; <b>proximarinus</b> Huey, 1945; <b>pusillus</b> Goldman, 1931; aridicola Huey, 1937; comobabiensis Huey, 1937; depauperatus Grinnell and Hill, 1936; growlerensis Huey, 1937; phasma Goldman, 1933; <b>retractus</b> Baker, 1953; <b>rhizophagus</b> Huey, 1949; <b>riparius</b> Grinnell and Hill, 1936; <b>robustus</b> Durrant, 1946; <b>rubidus</b> Youngman, 1958; <b>ruidosae</b> Hall, 1932; <b>rupestris</b> Chattin, 1941; <b>ruricola</b> Huey, 1949; <b>russeolus</b> Nelson and Goldman, 1909; <b>saxatilis</b> Grinnell, 1934; <b>scotophilus</b> Davis, 1940; <b>sevieri</b> Durrant, 1946; <b>siccovallis</b> Huey, 1945; *<b>simulus</b> Nelson and Goldman, 1934; *<b>sinaloae</b> Merriam, 1901; *<b>solitarius</b> Grinnell, 1926; <b>spatiosus</b> Goldman, 1938; <b>stansburyi</b> Durrant, 1946; *<b>sturgisi</b> Goldman, 1938; <b>subsimilis</b> Goldman, 1933; <b>texensis</b> Bailey, 1902; <b>tivius</b> Durrant, 1937; *<b>toltecus</b> J.A. Allen, 1893; <b>tularosae</b> Hall, 1932; <b>vanrosseni</b> Huey, 1934; <b>varus</b> Hall and Long, 1960; <b>vescus</b> Hall and Davis, 1935; <b>villai</b> Baker, 1953; <b>wahwahensis</b> Durrant, 1937; <b>winthropi</b> Nelson and Goldman, 1934; <b>xerophilus</b> Huey, 1945."
    if "references in J. T. Marshall, Jr., 1998, and ICZN, 1990" in text:
        text = "<i>albicans</i>  Billberg, 1827; <i>amurensis</i> Argyropulo, 1933;<i> arenarius </i>Migulin, 1938;<i> bicolor</i> Tichomirow and Kortchagin, 1889; <i>borealis</i> Ognev, 1924;<i> decolor</i> Argyropulo, 1932;<i> funereus</i> Ognev, 1924; <i>gansuensis</i> Satunin, 1902;<i> germanicus</i> Noack, 1918; <i>gilvus</i> Petényi, 1882; <i>hanuma</i> Ognev, 1948; <i>hapsaliensis</i> Reinwaldt, 1927;<i> helvolus</i> Fitzinger, 1867 [<i>nomen nudum</i>];<i> heroldii</i> Krausse, 1922; <i>hortulanus</i> Nordmann, 1840; <i>kambei</i> Kishida and Mori, 1931 [<i>nomen nudum</i>];<i> kuro</i> Kuroda, 1940 [see Kaneko and Maeda, 2002]; <i>longicauda </i>Mori, 1939 [see Kaneko and Maeda, 2002];<i> manchu</i> Thomas, 1909;<i> mongolium</i> Thomas, 1908; <i>niveus</i> Billberg, 1827; <i>nogaiorum</i> Heptner, 1934;<i> orii </i>Kuroda, 1924 [see Kaneko and Maeda, 2002]; <i>oxyrrhinus</i> Kashkarov, 1922;<i> pachycercus</i> Blanford, 1875;<i> polonicus</i> Niezabitowsky, 1934;<i> raddei</i> Kastschenko, 1910; <i>rotans</i> Fortuyn, 1912; <i>rufiventris</i> Argyropulo, 1932;<i> sareptanicus</i> Hilzheimer, 1911; <i>severtzovi</i> Kashkarov, 1922; <i>solymarensis </i>Kretzoi, <i>in</i> Jánossy, 1986 [<i>nomen nudum</i> according to Kowalski, 2001];<i> striatus</i> Billberg, 1827;<i> synanthropus </i>Kretzoi, 1965; <i>takagii</i> Kishida and Mori, 1931 [<i>nomen nudum</i>];<i> takayamai</i> Kuroda, 1938 [see Kaneko and Maeda, 2002];<i> tomensis</i> Kastschenko, 1899; <i>utsuryonis</i> Mori, 1938 [see Kaneko and Maeda, 2002]; <i>variabilis</i> Argyropulo, 1933;<i> vinogradovi</i> Argyropulo, 1933; <i>wagneri</i> Eversmann, 1848; <i>yamashinai</i> Kuroda, 1934 [see Kaneko and Maeda, 2002]; <i>yesonis</i> Kuroda, 1928 [see Kaneko and Maeda, 2002]; <b><i>bactrianus</i> </b>Blyth, 1846; <b><i>castaneus </i></b>Waterhouse, 1843; <i>albertisii </i>Peters and Doria, 1881; <i>bieni</i> Young, 1934; <i>canacorum</i> Revilloid, 1914; <i>commissarius</i> Mearns, 1905; <i>dubius</i> Hodgson, 1845 [not Fischer, 1829]; <i>dunckeri </i>Mohr, 1923; <i>fredericae</i> Sody, 1933; <i>manei</i> Gray, 1843 [<i>nomen nudum</i>]; <i>manei</i> Kelaart, 1852; <i>mohri </i>Ellerman, 1941; <i>momiyamai </i>Kuroda, 1920 [see Kaneko and Maeda, 2002]; <i>mystacinus</i> Mohr, 1923 [not Danford and Alston, 1877]; <i>nipalensis</i> Hodgson, 1841 [<i>nomen nudum</i>];<i> rama</i> Blyth, 1865; <i>sinicus</i> Cabrera, 1922; <i>taitensis </i>Zelebor, 1869 [probably <i>nomen nudum</i>];<i> taiwanus</i> Horikawa, 1929 [not Tokuda, 1941; see Kaneko and Maeda, 2002]; <i>tytleri</i> Blyth, 1859; <i>urbanus</i> Hodgson, 1845; <i>viculorum</i> Anderson, 1879; <b><i>domesticus </i></b>Schwarz and Schwarz, 1943 [not Rutty, 1772, a <i>nomen nudum</i>, but conserved as <i>domesticus</i> Schwarz and Schwarz, 1943; see explanation and references in J. T. Marshall, Jr., 1998, and ICZN, 1990]; <i>abbotti</i> Waterhouse, 1837; <i>adelaidensis</i> Gray, 1841; <i>airolensis</i> Burg, 1921; <i>albidiventris</i> (Burg, 1923) [not Blyth, 1852]; <i>albinus</i> Minà Palumbo, 1868; <i>albus</i> Bechstein, 1801; <i>ater</i> Fraipont, 1907 [<i>nomen nudum</i>; not Millais, 1905];<i> azoricus</i> Schinz, 1845; <i>brevirostris </i>Waterhouse, 1837; <i>deserti </i>(Loche, 1867) [see Cockrum and Setzer, 1976]; <i>candidus</i> Laurent, 1937 [not Bechstein, 1796]; <i>caudatus</i> Martino, 1934; <i>corsicus </i>Kratochvil, 1986; <i>faeroensis</i> Clarke, 1904; <i>far</i> Cabrera, 1921; <i>flavescens</i> Fischer, 1872 [not Elliot, 1839, or Waterhouse, 1837]; <i>flavus</i> Bechstein, 1801 [not Kerr, 1792]; <i>formosovi</i> Heptner, 1930; <i>gentilis</i> Brants, 1827; <i>gerbillinus</i> Blyth, 1853; <i>helgolandicus </i>Zimmerman, 1953;<i> helviticus</i> Burg, 1923; <i>homourus</i> Hodgson, 1845; <i>indianus</i> Wied, 1862; <i>jalapae</i> J. A. Allen and Chapman, 1897; <i>jamesoni</i> Krausse, 1921; <i>kalehpeninsularis</i> Goodwin, 1940; <i>lundii </i>Fitzinger, 1867 [<i>nomen nudum</i>];<i> makovensis </i>Orlov, Nadjafova, and Bulatova, 1992; <i>maculatus</i> Bechstein, 1801; <i>major</i> Severtzov, 1873 [not Brants, 1827, or Pallas, 1779]; <i>melanogaster</i> Minà Palumbo, 1868; <i>microdontoides </i>Noack, 1889; <i>modestus</i> Wagner, 1842;<i> muralis</i> Barrett-Hamilton, 1899; <i>mykinessiensis</i> Degerbol, 1940; <i>nattereri </i>Fitzinger, 1867 [<i>nomen nudum</i>];<i> niger</i> Bechstein, 1801 [not Bechstein, 1796]; nudoplicatus Gaskoin, 1856; pallescens Heuglin, 1877; parvulus Tschudi, 1844 [not Hermann, 1804, or Mosanský, 1994]; percnonotus Moulthrop, 1942; peruvianus Peale, 1848; poschiavinus Fatio, 1869; praetextus Brants, 1827; rubicundus Minà Palumbo, 1868; simsoni Higgins and Petterd, 1883; subcaeruleus Fritsche, 1928 [not Lesson, 1842]; subterraneus Montessus, 1899; tataricus Satunin, 1908; theobaldi Blyth, 1853; orientalis Cretzschmar, 1826 [not Desmarest, 1819]; vignaudii Des Murs and Prévost, 1850; <b>gentilulus</b> Thomas, 1919; <u>Not allocated to subspecies:</u> albula Kishida, 1924 [Japan; see Kaneko and Maeda, 2002]; cinereomaculatus Fitzinger, 1867 [Europe, nomen nudum]; molossinus Temminck, 1844 [Japan; holotype is hybrid between castaneus and musculus; J. T. Marshall, Jr., 1998]; nordmanni Keyserling and Blasius, 1840 [nomen nudum]; reboudi Loche, 1867 [Lataste, 1883a, and Cabrera, 1923, identified this as a house mouse; J. T. Marshall, Jr. treated it as a synonym of domesticus but noted from the original description that the tail is too long for M. spretus and the eye was gerbil-like; Kowalski and Rzebik-Kowalska, 1991, claimed the holotype to be lost and the name should be treated as nomen dubium]; tantillus G. M. Allen, 1927 [holotype is a hybrid between musculus and castaneus; J. T. Marshall, Jr., 1998]; varius Fitzinger, 1867 [not Bechstein, 1796; Europe, nomen nudum]; yonakuni Kuroda, 1924 [S Ryukyu Isls; description seems to indicate hybrid between castaneus and musculus and such a phenetic mixture is reflected in specimens from Okinawa identified by J. T. Marshall, Jr.; see also Kaneko and Maeda, 2002]."
    if "suppressed, ICZN, O. 451" in text:
        text = "<i>altaicus</i>  (Noack, 1911); <i>argunensis</i> Dybowski, 1922; <i>canus</i> de Sélys Longchamps, 1839; <i>communis</i> Dwigubski, 1804; <i>deitanus</i> Cabrera, 1907; <i>desertorum</i> Bogdanov, 1882; <i>flavus</i> Kerr, 1792; <i>fulvus</i> de Sélys Longchamps, 1839; <i>italicus</i> Altobello, 1921; <i>kurjak</i> Bolkay, 1925; <i>lycaon</i> Trouessart, 1910; <i>major</i> Ogérien, 1863; <i>minor</i> Ogerien, 1863, <i>niger</i> Hermann, 1804; <i>orientalis</i> (Wagner, 1841); <i>orientalis</i> Dybowski, 1922; <i>signatus </i>Cabrera, 1907; <b><i>albus</i></b> Kerr, 1792; <i>dybowskii</i> Domaniewski, 1926; <i>kamtschaticus</i> Dybowski, 1922; <i>turuchanensis</i> Ognev, 1923; <b><i>alces</i></b> Goldman, 1941; <b><i>arabs</i></b> Pocock, 1934; <b><i>arctos</i></b> Pocock, 1935; <b><i>baileyi</i></b> Nelson and Goldman, 1929; <b><i>beothucus</i></b> G. M. Allen and Barbour, 1937; <b><i>bernardi</i> </b>Anderson, 1943; <i>banksianus</i> Anderson, 1943; <b><i>campestris</i></b> Dwigubski, 1804; <i>bactrianus</i> Laptev, 1929; <i>cubanenesis</i> Ognev, 1923; <i>desertorum</i> Bogdanov, 1882; <b><i>chanco</i></b> Gray, 1863; <i>coreanus</i> Abe, 1923; <i>dorogostaiskii</i> Skalon, 1936; <i>ekloni</i> Przewalski, 1883; <i>filchneri</i> (Matschie, 1907); <i>karanorensis</i> (Matschie, 1907); <i>laniger</i> (Hodgson, 1847) [preoccupied]; <i>niger</i> Sclater, 1874; <i>tschiliensis</i> (Matschie, 1907); <b><i>columbianus</i></b> Goldman, 1941; <b><i>crassodon</i></b> Hall, 1932; <b><i>dingo</i></b> Meyer, 1793 [domestic dog]; <i>antarcticus </i>Kerr, 1792[suppressed, ICZN, O. 451]; <i>australasiae</i> Desmarest, 1820; <i>australiae</i> Gray, 1826; <i>dingoides</i>, Matschie, 1915; <i>macdonnellensis</i> Matschie, 1915; <i>novaehollandiae</i> Voigt, 1831; <i>papuensis</i> Ramsay, 1879; <i>tenggerana</i> Kohlbrugge, 1896; <i>harappensis</i> Prashad, 1936; <i>hallstromi</i> Troughton, 1957; <b><i>familiaris</i></b> Linnaeus, 1758 [domestic dog]; <i>aegyptius </i>Linnaeus, 1758; <i>alco </i>C. E. H. Smith, 1839; <i>americanus </i>Gmelin, 1792; <i>anglicus </i>Gmelin, 1792; <i>antarcticus </i>Gmelin, 1792; <i>aprinus </i>Gmelin, 1792; <i>aquaticus </i>Linnaeus, 1758; <i>aquatilis </i>Gmelin, 1792; <i>avicularis </i>Gmelin, 1792; <i>borealis</i> C. E. H. Smith, 1839; <i>brevipilis </i>Gmelin, 1792; <i>cursorius </i>Gmelin, 1792; <i>domesticus </i>Linnaeus, 1758; <i>extrarius </i>Gmelin, 1792; <i>ferus</i> C. E. H. Smith, 1839; <i>fricator </i>Gmelin, 1792; <i>fricatrix </i>Linnaeus, 1758; <i>fuillus </i>Gmelin, 1792; <i>gallicus </i>Gmelin, 1792; <i>glaucus </i>C. E. H. Smith, 1839; <i>graius </i>Linnaeus, 1758; <i>grajus </i>Gmelin, 1792; <i>hagenbecki</i> Krumbiegel, 1950; <i>haitensis </i>C. E. H. Smith, 1839; <i>hibernicus </i>Gmelin, 1792; <i>hirsutus </i>Gmelin, 1792; <i>hybridus </i>Gmelin, 1792; <i>islandicus </i>Gmelin, 1792; <i>italicus </i>Gmelin, 1792; <i>laniarius </i>Gmelin, 1792; <i>leoninus </i>Gmelin, 1792; <i>leporarius</i> C. E. H. Smith, 1839; <i>major </i>Gmelin, 1792; <i>major </i>Gmelin, 1792; <i>mastinus </i>Linnaeus, 1758; <i>melitacus </i>Gmelin, 1792; <i>melitaeus </i>Linnaeus, 1758; <i>minor </i>Gmelin, 1792; <i>molossus </i>Gmelin, 1792; <i>mustelinus </i>Linnaeus, 1758; <i>obesus </i>Gmelin, 1792; <i>orientalis </i>Gmelin, 1792; <i>pacificus </i>C. E. H. Smith, 1839; <i>plancus </i>Gmelin, 1792; <i>pomeranus </i>Gmelin, 1792; <i>sagaces</i> C. E. H. Smith, 1839; <i>sanguinarius</i> C. E. H. Smith, 1839; <i>sagax </i>Linnaeus, 1758; <i>scoticus </i>Gmelin, 1792; <i>sibiricus </i>Gmelin, 1792; <i>suillus </i>C. E. H. Smith, 1839; <i>terraenovae </i>C. E. H. Smith, 1839; <i>terrarius</i> C. E. H. Smith, 1839; <i>turcicus </i>Gmelin, 1792; <i>urcani</i> C. E. H. Smith, 1839; <i>variegatus </i>Gmelin, 1792; <i>venaticus </i>Gmelin, 1792; <i>vertegus </i>Gmelin, 1792; <b><i>floridanus</i></b> Miller, 1912; <b><i>fuscus</i></b> Richardson, 1839; <i>gigas</i> (Townsend, 1850); <b><i>gregoryi</i></b> Goldman, 1937; <b><i>griseoalbus</i></b> Baird, 1858; <i>knightii</i> Anderson, 1945; <b><i>hattai</i></b> Kishida, 1931; <i>rex</i> Pocock, 1935; <b><i>hodophilax</i></b> Temminck, 1839; <i>hodopylax</i> Temminck, 1844; <i>japonicus</i> Nehring, 1885; <b><i>hudsonicus</i></b> Goldman, 1941; <b><i>irremotus</i></b> Goldman, 1937; <b><i>labradorius</i></b> Goldman, 1937; <b>ligoni</b> Goldman, 1937; <b>lycaon</b> Schreber, 1775; canadensis de Blainville, 1843; ungavensis Comeau, 1940; <b>mackenzii</b> Anderson, 1943; <b>manningi</b> Anderson, 1943; <b>mogollonensis</b> Goldman, 1937; <b>monstrabilis</b> Goldman, 1937; niger Bartram, 1791; <b>nubilus</b> Say, 1823; variabilis Wied-Neuwied, 1841; <b>occidentalis</b> Richardson, 1829; sticte Richardson, 1829; ater Richardson, 1829; <b>orion</b> Pocock, 1935; <b>pallipes</b> Sykes, 1831; <b>pambasileus</b> Elliot, 1905; <b>rufus</b> Audubon and Bachman, 1851; <b>tundrarum</b> Miller, 1912; <b>youngi</b> Goldman, 1937."
    if "<b><i>dalli</i></b><i> </i>Merriam, 1896" in text:
        text = "<i>albus </i> Gmelin, 1788; <i>alpinus </i>G. Fischer, 1814; <i>annulatus </i>Billberg, 1827; <i>argenteus </i>Billberg, 1827; <i>aureus </i>Fitzinger, 1855; <i>badius </i>Schrank, 1798; <i>brunneus </i>Billberg, 1827; <i>cadaverinus </i>Eversmann, 1840; <i>euryrhinus </i>Nilsson, 1847; <i>eversmanni </i>(Gray, 1864); <i>falciger </i>Reichenbach, 1836; <i>formicarius </i>Billberg, 1828; <i>fuscus </i>Gmelin, 1788; <i>grandis </i>J. E. Gray, 1864; <i>griseus </i>Kerr, 1792; <i>gobiensis</i> Sokolov and Orlov, 1992; <i>longirostris </i>Eversmann, 1840; <i>major </i>Nilsson, 1820; <i>marsicanus </i>Altobello, 1921; <i>minor </i>Nilsson, 1820; <i>myrmephagus </i>Billberg, 1827; <i>niger </i>Gmelin, 1788; <i>normalis </i>Gray, 1864; <i>norvegicus </i>J. B. Fischer, 1829; <i>polonicus </i>J. E. Gray, 1864; <i>pyrenaicus </i>J. B. Fischer, 1829; <i>rossicus </i>J. E. Gray, 1864; <i>rufus </i>Borkhausen, 1797; <i>scandinavicus </i>Gray, 1864; <i>stenorostris </i>Gray, 1864; <i>ursus </i>Boddaert, 1772; <b><i>alascensis</i></b><i> </i>Merriam, 1896; <i>alexandrae </i>Merriam, 1914; <i>cressonus </i>Merriam, 1916; <i>eximius </i>Merriam, 1916; <i>holzworthi </i>Merriam, 1929; <i>innuitus </i>Merriam, 1914; <i>internationalis </i>Merriam, 1914; <i>kenaiensis </i>Merriam, 1904; <i>kidderi </i>Merriam, 1902; <i>nuchek </i>Merriam, 1916; <i>phaeonyx </i>Merriam, 1904; <i>sheldoni </i>Merriam, 1910; <i>toklat </i>Merriam, 1914; <i>tundrensis </i>Merriam, 1914; <b><i>beringianus</i></b><i> </i>Middendorff, 1851; <i>kolymensis </i>Ognev, 1924; <i>mandchuricus </i>Heude, 1898; <i>piscator </i>Pucheran, 1855; <b><i>californicus</i></b><i> </i>Merriam, 1896; <i>colusus </i>Merriam, 1914; <i>henshawi </i>Merriam, 1914; <i>klamathensis </i>Merriam, 1914; <i>magister </i>Merriam, 1914; <i>mendocinensis </i>Merriam, 1916; <i>tularensis </i>Merriam, 1914; <b><i>collaris</i></b><i> </i>F. G. Cuvier, 1824; <i>jeniseensis </i>Ognev, 1924; <i>sibiricus </i>J. E. Gray, 1864; <b><i>crowtheri</i></b><i> </i>Schinz, 1844; <b><i>dalli</i></b><i> </i>Merriam, 1896; <i>nortoni </i>Merriam, 1914; <i>orgiloides </i>Merriam, 1918; <i>townsendi </i>Merriam, 1916; <b><i>gyas</i></b><i> </i>Merriam, 1902; <i>merriami </i>J. A. Allen, 1902; <b><i>horribilis</i></b><i> </i>Ord, 1815; <i>absarokus </i>Merriam, 1914; <i>andersoni </i>Merriam, 1918; <i>apache </i>Merriam, 1916; <i>arizonae </i>Merriam, 1916; <i>bairdi </i>Merriam, 1914; <i>bisonophagus </i>Merriam, 1918; <i>canadensis </i>Merriam, 1914; <i>candescens </i>C. E. H. Smith, 1827; <i>cinereus </i>Desmarest, 1820; <i>crassus </i>Merriam, 1918; <i>dusorgus </i>Merriam, 1918; <i>ereunetes </i>Merriam, 1918; <i>griseus </i>Choris, 1822; <i>horriaeus </i>Baird, 1858; <i>hylodromus </i>Elliot, 1904; <i>idahoensis </i>Merriam, 1918; <i>imperator </i>Merriam, 1914; <i>impiger </i>Merriam, 1918; <i>inopinatus </i>Merriam, 1918; <i>kennerleyi </i>Merriam, 1914; <i>kluane </i>Merriam, 1916; <i>latifrons </i>Merriam, 1914; <i>macfarlani </i>Merriam, 1918; <i>macrodon </i>Merriam, 1918; <i>mirus </i>Merriam, 1918; <i>navaho </i>Merriam, 1914; <i>nelsoni </i>Merriam, 1914; <i>ophrus </i>Merriam, 1916; <i>oribasus </i>Merriam, 1918; <i>pallasi </i>Merriam, 1916; <i>pellyensis </i>Merriam, 1918; <i>perturbans </i>Merriam, 1918; <i>planiceps </i>Merriam, 1918; <i>pulchellus </i>Merriam, 1918; <i>richardsoni </i>Swainson, 1838; <i>rogersi </i>Merriam, 1918; <i>rungiusi </i>Merriam, 1918; <i>russelli </i>Merriam, 1914; <i>sagittalis </i>Merriam, 1918; <i>selkirki </i>Merriam, 1916; <i>shoshone </i>Merriam, 1914; <i>texensis </i>Merriam, 1914; <i>utahensis </i>Merriam, 1914; <i>washake </i>Merriam, 1916; <b><i>isabellinus </i></b>Horsfield, 1826; <i>leuconyx </i>Severtzov, 1873; <i>pamirensis </i>Ognev, 1924; <b><i>lasiotus</i></b><i> </i>Gray, 1867; <i>baikalensis </i>Ognev, 1924; <i>cavifrons </i>(Heude, 1901); <i>ferox </i>Temminck, 1844 [preoccupied]; <i>macneilli</i> Lydekker, 1909; <i>melanarctos </i>Heude, 1898; <i>yesoensis </i>Lydekker, 1897; <b><i>middendorffi</i></b><i> </i>Merriam, 1896; <i>kadiaki </i>Kleinschmidt, 1911; <b>pruinosus</b> Blyth, 1854; lagomyiarius Przewalski, 1883; <b>sitkensis</b> Merriam, 1896; caurinus Merriam, 1914; eltonclarki Merriam, 1914; eulophus Merriam, 1904; insularis Merriam, 1916; mirabilis Merriam, 1916; neglectus Merriam, 1916; orgilos Merriam, 1914; shirasi Merriam, 1914; <b>stikeenensis</b> Merriam, 1914; atnarko Merriam, 1918; chelan Merriam, 1916; chelidonias Merriam, 1918; crassodon Merriam, 1918; hoots Merriam, 1916; kwakiutl Merriam, 1916; pervagor Merriam, 1914; tahltanicus Merriam, 1914; warburtoni Merriam 1916; <b>syriacus</b> Hemprich and Ehrenberg, 1828; caucasicus Smirnov, 1919; dinniki Smirnov, 1919; lasistanicus Satunin, 1913; meridionalis Middendorff, 1851; persicus Lönnberg, 1925; schmitzi Matschie, 1917; smirnovi Lönnberg, 1925."
    if (
        "names based on populations possibly originating from scrofa/celebensis hybrids"
        in text
    ):
        text = "<i>anglicus</i> Reichenbach, 1846;<i> aper </i>Erxleben, 1777;<i> asiaticus</i> Sanson, 1878; <i>bavaricus </i>Reichenbach, 1846;<i> campanogallicus </i>Reichenbach, 1846;<i> capensis </i>Reichenbach, 1846;<i> castilianus </i>Thomas, 1911;<i> celticus </i>Sanson, 1878;<i> chinensis </i>Linnaeus, 1758; <i>crispus </i>Fitzinger, 1858; <i>deliciosus </i>Reichenbach, 1846;<i> domesticus</i> Erxleben, 1777;<i> europaeus </i>Pallas, 1811;<i> fasciatus </i>von Schreber, 1790;<i> ferox </i>Moore, 1870; <i>ferus </i>Gmelin, 1788; <i>gambianus </i>Gray, 1847 [<i>nomen nudum</i>]; <i>hispidus </i>von Schreber, 1790; <i>hungaricus </i>Reichenbach, 1846;<i> ibericus </i>Sanson, 1878; <i>italicus </i>Reichenbach, 1846;<i> juticus </i>Fitzinger, 1858; <i>lusitanicus </i>Reichenbach, 1846;<i> macrotis </i>Fitzinger, 1858; <i>monungulus </i>G. Fischer [von Waldheim], 1814; <i>moravicus </i>Reichenbach, 1846;<b> </b><i>nanus </i>Nehring, 1884; <i>palustris </i>Rütimeyer, 1862; <i>pliciceps </i>Gray, 1862; <i>polonicus </i>Reichenbach, 1846; <i>sardous </i>Reichenbach, 1846;<i> scropha </i>Gray, 1827; <i>sennaarensis</i> Fitzinger, 1858 [<i>nomen nudum</i>]; <i>sennaarensis </i>Gray, 1868; <i>sennaariensis </i>Fitzinger, 1860; <i>setosus </i>Boddaert, 1785;<i> siamensis </i>von Schreber, 1790;<i> sinensis </i>Erxleben, 1777; <i>suevicus </i>Reichenbach, 1846;<i> syrmiensis </i>Reichenbach, 1846;<i> turcicus </i>Reichenbach, 1846;<i> variegatus </i>Reichenbach, 1846;<i> vulgaris </i>(S. D. W., 1836); <i>wittei </i>Reichenbach, 1846; <b><i>algira </i></b>Loche, 1867;<i> barbarus </i>Sclater, 1860 [<i>nomen nudum</i>]; <i>sahariensis </i>Heim de Balzac, 1937; <b><i>attila </i></b>Thomas, 1912;<i> falzfeini </i>Matschie, 1918; <b><i>cristatus </i></b>Wagner, 1839; <i>affinis </i>Gray, 1847 [<i>nomen nudum</i>]; <i>aipomus </i>Gray, 1868; <i>aipomus </i>Hodgson, 1842 [<i>nomen nudum</i>]; <i>bengalensis </i>Blyth, 1860;<i> indicus </i>Gray, 1843 [<i>nomen nudum</i>]; <i>isonotus</i> Gray, 1868;<i> isonotus </i>Hodgson, 1842 [<i>nomen nudum</i>]; <i>jubatus </i>Miller, 1906;<i> typicus </i>Lydekker, 1900; <i>zeylonensis<b> </b></i>Blyth, 1851;<i> <b>davidi </b></i>Groves, 1981; <b><i>leucomystax </i></b>Temminck, 1842; <i>japonica </i>Nehring, 1885;<i> nipponicus </i>Heude, 1899; <b><i>libycus</i></b> Gray, 1868; <i>lybicus<b> </b></i>Groves, 1981;<i> mediterraneus </i>Ulmansky, 1911;<i> reiseri </i>Bolkay, 1925; <b><i>majori</i></b><i> </i>De Beaux and Festa, 1927;<i> <b>meridionalis </b></i>Forsyth Major, 1882;<i> baeticus </i>Thomas, 1912;<i> sardous </i>Ströbel, 1882; <b><i>moupinensis </i></b>Milne-Edwards, 1871;<i> acrocranius</i> Heude, 1892;<i> chirodontus </i>Heude, 1888; <i>chirodonticus</i> Heude, 1899;<i> collinus</i> Heude, 1892;<i> curtidens </i>Heude, 1892;<i> dicrurus </i>Heude, 1888;<i> flavescens </i>Heude, 1899;<i> frontosus </i>Heude, 1892;<i> laticeps</i> Heude, 1892;<i> leucorhinus </i>Heude, 1888;<i> melas</i> Heude, 1892;<i> microdontus </i>Heude, 1892;<i> oxyodontus </i>Heude, 1888;<i> paludosus</i> Heude, 1892;<i> palustris </i>Heude, 1888;<i> planiceps</i> Heude, 1892;<i> scrofoides </i>Heude, 1892;<i> spatharius </i>Heude, 1892;<i> taininensis </i>Heude, 1888; <b><i>nigripes</i></b> Blanford, 1875;<i> <b>riukiuanus</b></i> Kuroda, 1924;<i> <b>sibiricus </b></i>Staffe, 1922;<i> raddeanus </i>Adlerberg, 1930;<i> <b>taivanus </b></i>(Swinhoe, 1863); <b><i>ussuricus </i></b>Heude, 1888;<i> canescens </i>Heude, 1888;<i> continentalis </i>Nehring, 1889;<i> coreanus </i>Heude, 1897;<i> gigas </i>Heude, 1892;<i> mandchuricus </i>Heude, 1897;<i> songaricus </i>Heude, 1897; <b><i>vittatus </i></b>Boie, 1828;<i> andersoni </i>Thomas and Wroughton, 1909;<i> jubatulus </i>Miller, 1906;<i> milleri </i>Jentink, 1905; <i>pallidiloris </i>Mees, 1957;<i> peninsularis </i>Miller, 1906;<i> rhionis </i>Miller, 1906; <i>typicus </i>Heude, 1899; <u>Unallocated:</u> <i>andamanensis </i>Blyth, 1858;<i> babi </i>Miller, 1906;<i> enganus</i> Lyon, 1916; floresianus Jentink, 1905; natunensis Miller, 1901; nicobaricus Miller, 1902; tuancus Lyon, 1916; aruensis Rosenberg, 1878; ceramensis Rosenberg, 1878; goramensis De Beaux, 1924; niger Finsch, 1886; papuensis Lesson and Garnot, 1826; ternatensis Rolleston, 1877."

    while True:
        original_text = text
        inner_name = (
            r"[A-Z][a-z]+ [A-Z][a-zA-Z \-\.]+(?:, [A-Za-z, ]+? and [A-Z][a-z]+)?, \d{4}"
        )
        text = re.sub(
            rf"(, \d{{4}});? \(({inner_name}(?:; {inner_name})*)\)(?=;|\.)",
            r"\1; \2",
            text,
        )
        for pattern, sub in REGEX_REPLACEMENTS.items():
            text = re.sub(pattern, sub, text)
        for pattern, sub in REPLACEMENTS.items():
            text = text.replace(pattern, sub)
        text = text.strip()
        if text == original_text:
            break

    return text


def tokenize(text: str) -> Iterable[Token]:
    text = clean_text(text)
    it = PeekingIterator(text)
    while True:
        try:
            token = next(it)
        except StopIteration:
            break
        match token:
            case " ":
                continue
            case ",":
                yield Token(TokenType.COMMA, ",")
            case ";":
                yield Token(TokenType.SEMICOLON, ";")
            case ".":
                yield Token(TokenType.PERIOD, ".")
            case "[":
                pieces = ["[", *it.advance_until("]")]
                yield Token(TokenType.SQUARE_BRACKETS, "".join(pieces))
            case "(":
                pieces = ["(", *it.advance_until(")")]
                token_text = "".join(pieces)
                if (
                    "sensu" in token_text
                    or token_text.startswith(("(see", "(not", "(preoccupied by"))
                    or "October" in token_text
                    or not any(c.isdigit() for c in token_text)
                ):
                    yield Token(TokenType.PARENTHESES, token_text)
                else:
                    yield Token(TokenType.OPEN_PAREN, "(")
                    yield from tokenize(token_text[1:-1])
                    yield Token(TokenType.CLOSE_PAREN, ")")
            case "<":
                pieces = ["<"]
                next_char = it.advance()
                if next_char == "/":
                    pieces.append("/")
                    token_type = TokenType.CLOSE_TAG
                else:
                    pieces.append(next_char)
                    token_type = TokenType.OPEN_TAG
                pieces += it.advance_until(">")
                yield Token(token_type, "".join(pieces))
            case _ if token.isdigit():
                pieces = [token]
                while True:
                    maybe_next_char = it.peek()
                    if maybe_next_char is None:
                        break
                    if maybe_next_char.isdigit() or maybe_next_char == "-":
                        pieces.append(maybe_next_char)
                        next(it)
                    else:
                        break
                yield Token(TokenType.DATE, "".join(pieces))
            case _ if token.isalpha():
                pieces = [token]
                while True:
                    maybe_next_char = it.peek()
                    if maybe_next_char is None:
                        break
                    if maybe_next_char not in ("<", ",", ";", "[", "(", " "):
                        pieces.append(maybe_next_char)
                        next(it)
                    else:
                        break
                yield Token(TokenType.TEXT, "".join(pieces).strip())


def _expect_token(it: PeekingIterator[Token], typ: TokenType) -> Token:
    return it.expect(lambda t: t.type is typ)


def parse_single_name(it: PeekingIterator[Token]) -> dict[str, Any] | str:
    data: dict[str, Any] = {}
    next_char = it.peek()
    if next_char is None:
        raise RuntimeError("Unexpected end of sequence")
    if next_char.type is TokenType.OPEN_TAG:
        is_subspecies = True
        it.advance()
    else:
        is_subspecies = False
    data["is_subspecies"] = is_subspecies
    name = it.advance().value
    if is_subspecies:
        name_pieces = [name]
        while it.next_is(lambda t: t.type is TokenType.TEXT):
            name_pieces.append(it.advance().value)
        name = " ".join(name_pieces)
        _expect_token(it, TokenType.CLOSE_TAG)
    if ":" in name:
        return name
    data["name"] = name
    if it.next_is(lambda t: t.type is TokenType.OPEN_TAG):
        it.advance()
        _expect_token(it, TokenType.CLOSE_TAG)  # random <b></b>
    elif it.next_is(lambda t: t.type is TokenType.COMMA):
        it.advance()  # random comma
    if it.next_is(lambda t: t.type is TokenType.OPEN_PAREN):
        it.advance()
        is_parenthesized = True
    else:
        is_parenthesized = False
    data["is_parenthesized"] = is_parenthesized
    if it.next_is(lambda t: t.type is TokenType.SQUARE_BRACKETS):
        data["comment"] = it.advance().value
        data["author"] = None
        data["year"] = None
        return data
    author = [_expect_token(it, TokenType.TEXT).value]
    while it.next_is(lambda t: t.type in (TokenType.TEXT, TokenType.COMMA)):
        token = it.advance()
        author.append(token.value)
        author.append(" ")
    if it.next_is(lambda t: t.type is TokenType.SQUARE_BRACKETS):
        author.append(it.advance().value)
        _expect_token(it, TokenType.COMMA)
    data["author"] = re.sub(
        r"\s+", " ", " ".join(author).strip().rstrip(",").strip()
    ).replace(" , ", ", ")
    data["year"] = _expect_token(it, TokenType.DATE).value
    if is_parenthesized:
        _expect_token(it, TokenType.CLOSE_PAREN)
    if it.next_is(lambda t: t.type is TokenType.SQUARE_BRACKETS):
        data["comment"] = it.advance().value
    elif it.next_is(lambda t: t.type is TokenType.PARENTHESES):
        data["comment"] = it.advance().value
    return data


def _parse_standard(tokens: list[Token]) -> list[dict[str, Any]]:
    it = PeekingIterator(tokens)
    if it.peek() is None:
        return []
    dicts = []
    special = False
    while True:
        name = parse_single_name(it)
        if isinstance(name, str):
            special = True
            continue
        name["special"] = special
        dicts.append(name)
        try:
            token = next(it)
        except StopIteration:
            break
        if token.type is TokenType.PERIOD:
            it.assert_done()
            break
        elif token.type is TokenType.SEMICOLON:
            continue
        else:
            raise RuntimeError(f"Unexpected token: {token}")
    return dicts


def _parse_alt1(tokens: list[Token]) -> list[dict[str, Any]]:
    it = PeekingIterator(tokens)
    dicts = []
    while True:
        name = it.expect(lambda t: t.type is TokenType.TEXT).value
        if it.next_is(
            lambda t: t.type in (TokenType.PARENTHESES, TokenType.SQUARE_BRACKETS)
        ):
            comment = it.advance().value
        else:
            comment = None
        data_dict = {
            "name": name,
            "author": None,
            "year": None,
            "comment": comment,
            "is_subspecies": False,
            "is_parenthesized": False,
            "special": False,
        }
        dicts.append(data_dict)
        try:
            token = next(it)
        except StopIteration:
            break
        else:
            if token.type not in (TokenType.COMMA, TokenType.SEMICOLON):
                raise RuntimeError(f"Unexpected token: {token}")
    return dicts


def parse(text: str) -> list[dict[str, Any]]:
    try:
        tokenizer = tokenize(text)
        tokens = [
            token
            for token in tokenizer
            if not (
                token.type in (TokenType.OPEN_TAG, TokenType.CLOSE_TAG)
                and "i" in token.value
            )
        ]
        try:
            return _parse_standard(tokens)
        except RuntimeError:
            return _parse_alt1(tokens)
    except RuntimeError:
        print("Failed to parse:", text)
        raise


def parse_synonyms(text: str, parent: dict[str, Any]) -> Iterable[dict[str, Any]]:
    if not text:
        return
    dicts = parse(text)
    has_any_subspecies = any(d["is_subspecies"] for d in dicts)
    if has_any_subspecies:
        current_subspecies = parent["name"].split()[-1]
    else:
        current_subspecies = None
    for data_dict in dicts:
        if data_dict["is_subspecies"]:
            current_subspecies = data_dict["name"]
            continue
        if current_subspecies is not None:
            parent_name = f"{parent['name']} {current_subspecies}"
            parent_rank = Rank.subspecies
        else:
            parent_name = parent["name"]
            parent_rank = parent["rank"]
        yield {
            "rank": Rank.synonym,
            "name": data_dict["name"],
            "authority": (
                helpers.clean_string(data_dict["author"])
                if data_dict["author"]
                else None
            ),
            "year": data_dict["year"],
            "comment": data_dict.get("comment"),
            "raw_data": json.dumps(data_dict),
            "parent": parent_name,
            "parent_rank": parent_rank,
        }


def is_valid_parent(text: str) -> bool:
    return (
        text
        not in (
            "",
            "unnamed subgenus, see comments",
            "??See comments",
            "?",
            "[incertae sedis]",
            "Gray",
        )
        and "?" not in text
        and "comment" not in text
    )


def translate_row(row: dict[str, str]) -> Iterable[dict[str, Any]]:
    rank_text = row["TaxonLevel"].lower()
    rank = Rank[rank_text]
    parent: str | None
    parent_rank: Rank | None
    if rank is Rank.subspecies:
        name = f"{row['Genus']} {row['Species']} {row['Subspecies']}"
        parent = f"{row['Genus']} {row['Species']}"
        parent_rank = Rank.species
    elif rank is Rank.species:
        name = f"{row['Genus']} {row['Species']}"
        if is_valid_parent(row["Subgenus"]):
            parent = row["Subgenus"]
            parent_rank = Rank.subgenus
        else:
            parent = row["Genus"]
            parent_rank = Rank.genus
    else:
        parents = [
            (column, row[column])
            for column in reversed(RANK_COLUMNS)
            if is_valid_parent(row[column])
        ]
        name = parents[0][1].title()
        if len(parents) > 1:
            parent = parents[1][1].title()
            parent_rank = Rank[parents[1][0].lower()]
        else:
            parent = parent_rank = None
    citation = row["CitationName"] + ", "
    if row["CitationVolume"]:
        citation += row["CitationVolume"]
        if row["CitationIssue"]:
            citation += f"({row['CitationIssue']})"
        citation += ":"
    citation += row["CitationPages"]
    if row["ActualDate"]:
        citation += f" [{row['ActualDate']}]"
    if row["CitationType"]:
        citation += f" {row['CitationType']}"
    if citation.strip() == ",":
        final_citation = None
    else:
        final_citation = citation
    name = helpers.clean_string(
        name.replace("variegates", "variegatus")
        .replace("maculats", "maculatus")
        .replace(" princes", " princeps")
        .replace(" gentiles", " gentilis")
        .strip()
    )
    new_row = {
        "name": name,
        "rank": rank,
        "parent": parent.strip() if parent is not None else None,
        "parent_rank": parent_rank,
        "authority": helpers.clean_string(row["Author"]),
        "year": row["ActualDate"] or row["Date"],
        "citation": final_citation,
        "type_locality": row["TypeLocality"],
        "raw_data": json.dumps(row),
    }
    yield new_row
    if row["Status"].startswith("<i>"):
        syns = row["Status"]
    else:
        syns = row["Synonyms"]
    yield from parse_synonyms(syns, new_row)


def main(argv: list[str]) -> None:
    lines = lib.get_text(SOURCE)
    reader = csv.DictReader(lines)
    rows = [row for line in reader for row in translate_row(line)]
    rank_to_name_to_row: dict[Rank, dict[str, dict[str, Any]]] = defaultdict(dict)
    final_rows = []
    for row in rows:
        if row["rank"] is Rank.synonym:
            final_rows.append(row)
            continue
        if row["name"] in rank_to_name_to_row[row["rank"]]:
            print("duplicate", row, rank_to_name_to_row[row["rank"]][row["name"]])
        else:
            final_rows.append(row)
        rank_to_name_to_row[row["rank"]][row["name"]] = row
    rows = final_rows

    for row in rows:
        if row["rank"] is Rank.order:
            continue
        parent_row = rank_to_name_to_row.get(row["parent_rank"], {}).get(row["parent"])
        if parent_row is None:
            print(row)

    art = SOURCE.get_source()
    ces = list(art.get_classification_entries())
    for child in art.article_set:
        ces += child.get_classification_entries()
    valid_name_to_ce: dict[tuple[str, Rank], ClassificationEntry] = {}
    for ce in ces:
        if ce.rank is Rank.synonym:
            continue
        if (ce.name, ce.rank) in valid_name_to_ce:
            print("Duplicate", ce, valid_name_to_ce[(ce.name, ce.rank)])
        valid_name_to_ce[(ce.name, ce.rank)] = ce
    name_to_ces: dict[str, list[ClassificationEntry]] = defaultdict(list)
    for ce in ces:
        assert ce.raw_data is not None
        name_to_ces[ce.name].append(ce)

    remaining_ces = set(ces)
    for row in rows:
        if row.get("parent") is None:
            parent = None
        else:
            parent = valid_name_to_ce[(row["parent"], row["parent_rank"])]
        possible_names = name_to_ces[row["name"]]
        if row.get("comment"):
            comment = helpers.clean_string(row["comment"])
            tags = [ClassificationEntryTag.CommentClassificationEntry(comment)]
        else:
            tags = []
        possible_names = [
            name
            for name in possible_names
            if name.rank is row["rank"]
            and name.authority == row["authority"]
            and name.year == row["year"]
            and all(tag in name.tags for tag in tags)
        ]
        if parent is not None:
            possible_names = [
                name
                for name in possible_names
                if name.parent == parent
                or (
                    name.parent is not None
                    and name.parent.rank is Rank.species
                    and name.parent == parent.parent
                )
            ]
        if not possible_names:
            print(
                "Create CE",
                row["rank"].name,
                row["name"],
                row["authority"],
                row["year"],
                row["parent"],
                row["parent_rank"],
            )
            new_ce = ClassificationEntry.create(
                article=art,
                name=row["name"],
                rank=row["rank"],
                parent=parent,
                authority=row["authority"],
                year=row["year"],
                citation=row.get("citation"),
                type_locality=row.get("type_locality"),
                raw_data=row["raw_data"],
                tags=tags,
            )
            print(new_ce)
        else:
            if len(possible_names) > 1:
                possible_names = [
                    name for name in possible_names if name.tags == tuple(tags)
                ]
            if len(possible_names) > 1:
                print("Multiple names", possible_names, row)
            ce = possible_names[0]
            remaining_ces.discard(ce)
            if row["raw_data"] != ce.raw_data:
                print("Set raw data for", ce)
                ce.raw_data = row["raw_data"]
            if parent != ce.parent:
                print(f"Change parent for {ce} from {ce.parent} to {parent}", row)
                ce.parent = parent
    for ce in remaining_ces:
        print("Delete", ce)


if __name__ == "__main__":
    main(sys.argv)
