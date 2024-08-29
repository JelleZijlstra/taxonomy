import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source(
    "arvicolinae-palearctic-krystufek-shenbrot-.txt",
    "Arvicolinae Palearctic (Krystufek & Shenbrot 2022).pdf",
)

TOC = """
Alexandromys alpinus Lissovsky, Yatsentyuk, Petrova & Abramson, 2017 - Khangai Grass Vole
Alexandromys oeconomus (Pallas, 1776) -Tundra Grass Vole, Root Vole...
Alexandromys evoronensis (Kovalskaja & Sokolov, 1980) - Evoron Grass Vole.
Alexandromys fortis (Büchner, 1889) - Reed Vole..
Alexandromys fortis calamorum (Thomas, 1902).
Alexandromys fortis dolichocephalus (Mori, 1930)...
Alexandromys fortis fortis (Büchner, 1889) ...
Alexandromys fortis fujianensis (Hong, 1981)..
Alexandromys fortis michnoi (Kastchenko, 1910).
Alexandromys fortis uliginosus (Jones & Jonson, 1955) ...
Alexandromys kikuchii (Kuroda, 1920) - Taiwan Grass Vole
Alexandromys limnophilus (Büchner, 1889) - Lacustrine Grass Vole
Alexandromys limnophilus limnophilus (Büchner, 1889) ....
Alexandromys limnophilus malygini (Courant et al., 1999)...
Alexandromys maximowiczii maximowiczii (Schrenck, 1859)..
Alexandromys maximowiczii (Schrenck, 1859) - Maximowicz's Grass Vole.
Alexandromys maximowiczii ungurensis (Kastschenko, 1913)..
Alexandromys middendorffii hyperboreus (Vinogradov, 1933)....
Alexandromys middendorffii middendorffii (Poljakov, 1881)..
Alexandromys middendorffii (Polyakov, 1881) - Middendorfs Grass Vole
Alexandromys middendorffii ryphaeus (Heptner, 1948) ...
Alexandromys mongolicus (Radde, 1861) - Mongolian Grass Vole.
Alexandromys montebelli (A. Milne Edwards, 1872) - Japanese Grass Vole
Alexandromys mujanensis (Orlov & Kovalskaya, 1978) - Muya Grass Vole
Alexandromys sachalinensis (Vasin, 1955) - Sakhalin Grass Vole...
Alexandromys shantaricus (Ognev 1929) - Gromov's Grass Vole
Alexandromys shantaricus gromovi Vorontsov, Boeskorov, Lyapunova & Revin, 1988..
Alexandromys shantaricus shantaricus Ognev, 1929...
Alticola albicauda (True, 1894) - White-tailed Mountain Vole.
Alticola argentatus (Severtzov, 1879) - Silver Mountain Vole
Alticola argentatus argentatus (Severtzov, 187))
Alticola argentatus blanfordi (Scully, 1880)...
Alticola argentatus phasma Miller, 1912......
Alticola argentatus severtzowi (Tikhomirov & Korchagin, 1889)
Alticola argentatus subluteus Thomas, 1914...
Alticola argentatus tarasovi Rossolimo & Pavlinov, 1992...
Alticola argentatus worthingtoni Miller, 1906
Alticola barakshin Bannikov, 1947 - Gobi Altai Mountain Vole..
Alticola kohistanicus new species - Kohistan Mountain Vole..
Alticola lemminus (Miller, 1899) - Lemming Vole...
Alticola macrotis (Radde, 1861) - Large-eared Vole..
Alticola macrotis fetisovi (Galkina & Jepifantseva, 1988) ..
Alticola macrotis macrotis (Radde, 1861)...
Alticola montosus (True, 1894) - Kashmir Mountain Vole..
Alticola olchonensis Litvinov, 1960 - Baikal Mountain Vole.
Alticola parvidens Schlitter & Setzer, 1973 - Hindu Kush Mountain Vole
Alticola roylei (Gray, 1842) - Royle's Mountain Vole.......
Alticola semicanus (G. M. Allen, 1924) - Khangay (Mongolian) Mountain Vole...
Alticola semicanus alleni Argyropulo, 1933
Alticola semicanus semicanus (G. M. Allen, 1924) ....
Alticola stoliczkanus (Blanford, 1875) - Stoliczka's Mountain Vole..
Alticola stoliczkanus bhatnagari Biswas & Khajuria, 1955..
Alticola stoliczkanus lama (Barrett-Hamilton, 1900)...
Alticola stoliczkanus stoliczkanus (Blanford, 1875) ......
Alticola strelzovi (Kastschenko, 1899) - Strelzov's Mountain Vole...
Alticola strelzovi desertorum (Kastschenko, 1901) ...
Alticola strelzovi strelzovi (Kastschenko, 1899)..
Alticola tuvinicus khubsugulensis Litvinov, 1973...
Alticola tuvinicus Ognev, 1950 - Tuva Mountain Vole
Alticola tuvinicus tuvinicus Ognev, 1950 ....
Anteliomys hintoni luojishanensis (Liu, 2018)
Anteliomys chinensis (Thomas, 1891) - Long-tailed Chinese Vole.
Anteliomys custos (Thomas, 1912) - Mountain Chinese Vole.
Anteliomys custos changsanensis (Wang & Yang, 2000)...
Anteliomys custos custos (Thomas, 1912)...
Anteliomys custos ninglangensis (Wang & Li, 2000)...
Anteliomys custos rubellus (G. M. Allen, 1924).
Anteliomys hintoni hintoni (Osgood, 1932).
Anteliomys hintoni jinyangensis (Liu, 2018)...
Anteliomys hintoni meiguensis (Liu, 2018)..
Anteliomys hintoni Osgood, 1932 - Hinton's Chinese Vole..
Anteliomys olitor (Thomas, 1911) - Dwarf Chinese Vole...
Anteliomys olitor hypolitor (Wang & Li, 2000)
Anteliomys olitor olitor (Thomas, 1911) .....
Anteliomys proditor (Hinton, 1923) - Yulungshan Chinese Vole.
Anteliomys tarquinius (Thomas, 1912) - Sichuan Chinese Vole.
Anteliomys wardi (Thomas, 1912) - Ward's Chinese Vole....
Arvicola amphibius (Linnæus, 1758) - Eurasian Water Vole.
Arvicola italicus destructor Savi, 1838
Arvicola italicus italicus Savi, 1838..
Arvicola italicus Savi, 1838 - Italian Water Vole.
Arvicola persicus Filippi, 1865 - Persian Water Vole...
Arvicola sapidus Miller, 1908 - Iberian Water Vole..
Arvicola sapidus sapidus Miller, 1908.
Arvicola sapidus tenebricus Miller, 1908
Bramus fuscocapillus (Blyth, 1843) - Afghan Mole Vole
Bramus lutescens (Thomas, 1897) - Transcaucasian Mole Vole..
Caryomys eva (Thomas, 1911) - Long-tailed Brownish Vole..
Caryomys eva alcinous (Thomas, 1911)
Caryomys eva eva (Thomas, 1911)...
Caryomys inez (Thomas, 1908) - Short-tailed Brownish Vole ...
Caryomys inez inez (Thomas, 1908).
Caryomys inez nux (Thomas, 1910)
Chionomys gud (Satunin, 1909) - Gudaur Snow Vole..
Chionomys lasistanius (Neuhäuser, 1936) - Lazistan Snow Vole.
Chionomys nivalis (Martins 1842) - European Snow Vole..
Chionomys roberti (T'homas, 1906) - Robert's Snow Vole...
Clethrionomys centralis (Miller, 1906) - Tien Shan Red-backed Vole..
Clethrionomys glareolus (Schreber, 1780) - Bank Vole....
Clethrionomys rutilus (Pallas, 1779) - Siberian Red-backed Vole...
Craseomys regulus Thomas, 1907 - Korean Grey-sided Vole...
Craseomys rex (Imaizumi, 1971) - Dark Grey-sided Vole..
Craseomys rufocanus bedfordiae (Thomas, 1905)..
Craseomys rufocanus rufocanus (Sundevall, 1840)..
Craseomys rufocanus shanseius Thomas, 1908..
Craseomys smithii (Thomas, 1905) - Smith's Grey-sided Vole .....
Dicrostonyx groenlandicus (T'raill, 1823) - Nearctic Collared Lemming...
Dicrostonyx groenlandicus vinogradovi Ognev, 1948 - Vinogradov's Collard Lemming..
Dicrostonyx torquatus (Pallas, 1779) - Siberian (Palaearctic) Collared Lemming...
Dicrostonyx torquatus pallidus (Middendorff, 1852)
Dicrostonyx torquatus torquatus (Pallas, 1779)
Dicrostonyx torquatus ungulatus (Baer, 1841)...
Dinaromys bogdanovi (V. Martino & E. Martino, 1922) - Martino's Dinaric Vole..
Dinaromys bogdanovi bogdanovi (V. Martino & E. Martino, 1922)
Dinaromys bogdanovi grebenscikovi (V. Martino, 1934) .....
Dinaromys longipedis (Dulic & Vidinic, 1967) - Western Dinaric Vole
Ellobius talpinus (Pallas, 1770) - Common Mole Vole
Ellobius talpinus talpinus (Pallas, 1770).
Ellobius talpinus orientalis G. M. Allen, 1924.
Ellobius talpinus rufescens (Eversmann, 1870)
Ellobius talpinus kashtchenkoi Thomas, 1912.
Ellobius talpinus transcaspiae Thomas, 1912..
Ellobius tancrei alaicus Vorontsov, Liapunova, Zakarjan & Ivanov, 1969..
Ellobius tancrei albicatus Thomas, 1912 Ellobius tancrei coenosus Thomas, 1912.
Ellobius tancrei fuscipes Thomas, 1909...
Ellobius tancrei larvatus G. M. Allen, 1924
Ellobius tancrei tancrei W. Blasius, 1884.
Ellobius tancrei W. Blasius, 1884 - Eastern Mole Vole...
Eolagurus luteus (Eversmann, 1840) - Yellow Desert Lemming.....
Eolagurus przewalskii (Büchner, 1889) - Przewalski's Desert Lemming.
Eothenomys colurnus colurnus (Thomas, 1911).
Eothenomys colurnus kanoi Tokuda, 1937..
Eothenomys colurnus (Thomas, 1911) - Fujian Oriental Vole...
Eothenomys eleusis (T'homas, 1911) - Yunnan Oriental Vole...
Eothenomys eleusis cachinus (Thomas, 1921)
Eothenomys eleusis eleusis (Thomas, 1911)..
Eothenomys eleusis fidelis Hinton, 1923.
Eothenomys eleusis miletus (Thomas, 1914)
Eothenomys eleusis shimianensis Liu, 2018..
Eothenomys melanogaster (A. Milne Edwards, 1871) - Père David's Oriental Vole
GENUS: Alexandromys Ognev, 1914 - Grass Voles.
GENUS: Alticola Blanford, 1881 - Mountain Voles...
GENUS: Anteliomys Miller, 1896 - Chinese Voles...
GENUS: Arvicola Lacépède, 1799 - Water Voles..
GENUS: Bramus Pomel, 1892 - Southern Mole Voles.
GENUS: Caryomys Thomas, 1911 - Brownish Voles...
GENUS: Chionomys Miller, 1908 - Snow Voles ....
GENUS: Clethrionomys Tilesius, 1850 - Red-backed Voles.
GENUS: Craseomys Miller, 1900 - Grey-sided Voles
GENUS: Dicrostonyx Gloger, 1841 - Collared (Varying) Lemming...
GENUS: Dinaromys Kretzoi, 1955 - Dinaric Voles...
GENUS: Ellobius Fischer, 1814 - Northern Mole Voles ..
GENUS: Eolagurus Argyropulo, 1946 - Desert Lemmings..
GENUS: Eothenomys Miller, 1900 - Oriental Voles.
GENUS: Hyperacrius Miller, 1896 - Kashmir Voles.
GENUS: Lagurus Gloger, 1841 - Steppe Lemmings.
GENUS: Lasiopodomys Lataste, 1887 - Hairy-footed Voles.
GENUS: Lemmus Link, 1795 - Brown Lemmings ...
GENUS: Microtus Schrank, 1798 - Grey Voles.
GENUS: Mictomicrotus, new genus - Liangshan Voles...
GENUS: Myopus Miller, 1910 - Wood Lemmings...
GENUS: Neodon Hodgson, 1849 - Scrub Voles...
GENUS: Proedromys Thomas, 1911 -Groove-toothed Voles....
GENUS: Prometheomys Satunin, 1901 - Long-clawed Mole Voles........
GENUS: Stenocranius Kashchenko, 1901 - Narrow-headed Voles..
GENUS: Volemys Zagorodnjuk, 1990 - Sichuan Voles...
Craseomys andersoni (Thomas, 1905) - Anderson's Grey-sided Vole.
Craseomys rufocanus (Sundevall, 1846) - Siberian Grey-sided Vole.
Hyperacrius fertilis (True, 1894) - True's Kashmir Vole..
Hyperacrius fertilis brachelix (Miller, 1899)..
Hyperacrius fertilis fertilis (True, 1894) ...
Hyperacrius fertilis zygomaticus Phillips, 1969..
Hyperacrius wynnei (Blanford, 1881) - Murree Kashmir Vole
Hyperacrius wynnei wynnei (Blanford, 1881)
Hyperacrius wynnei traubi Phillips, 1969
Lagurus lagurus (Pallas, 1773) - Steppe Lemming...
Lagurus lagurus abacanicus Serebrennikov, 1929...
Lagurus lagurus aggressus Serebrennikov, 1929
Lagurus lagurus altorum Thomas, 1912...
Lagurus lagurus lagurus (Pallas, 1773).
Lasiopodomys brandtii (Radde, 1861) - Yellow (Brandt's) Hairy-footed Vole..
Lasiopodomys brandtii brandtii (Radde, 1861)
Lasiopodomys brandtii hangaicus (Bannikov, 1948)..
Lasiopodomys mandarinus (A. Milne Edwards, 1871) - Mandarin Hairy-footed Vole...
Lasiopodomys mandarinus faeceus (G. Allen, I924)
Lasiopodomys mandarinus johannes (Thomas, 1910)..
Lasiopodomys mandarinus kishidai (Mori, 1930).
Lasiopodomys mandarinus mandarinus (A. Milne-Edwards, 1871)..
Lasiopodomys mandarinus vinogradovi (Fetisov, 1936)..
Lemmus lemmus (Linnæus, 1758) - Palaearctic Brown Lemming
Lemmus lemmus amurensis Vinogradov, 1924.
Lemmus lemmus chernovi Spitsyn, Bolotov & Kondakov, 2021
Lemmus lemmus kamchaticus new subspecies..
Lemmus lemmus lemmus (Linnæus, 1758)..
Lemmus lemmus novosibiricus Vinogradov, 1924
Lemmus lemmus ognevi Vinogradov, 1933.
Lemmus lemmus portenkoi Tchernyavsky, 1967
Lemmus lemmus sibiricus (Kerr, 1792) ....
Lemmus nigripes (True, 1894) - Beringian Brown Lemming...
Microtus afghanus afghanus Thomas, 1912.....
Microtus afghanus balchanensis Heptner & Shukurov, 1950..
Microtus afghanus dangarinensis Golenishchev & Sablina, 1991..
Microtus afghanus Thomas, 1912 - Afghan Vole.
Microtus agrestis (Linnæus, 1761) - Common Field Vole......
Microtus anatolicus Krystufek & Kefelioglu, 2001 - Anatolian Social Vole
Microtus arvalis (Pallas, 1779) - Common Grey Vole (Common Vole) .
Microtus brachycercus (Lehmann, 1961) - Short-tailed Savi's Vole..
Microtus brachycercus brachycercus (Lehmann, 1961)...
Microtus brachycercus niethammericus Contoli, 2003 .....
Microtus bucharensis bucharensis Vinogradov, 1930....
Microtus bucharensis davydovi Golenishchev & Sablina, 1991
Microtus bucharensis Vinogradov, 1930 - Bucharian Vole..
Microtus cabrerae Thomas, 1906 - Cabrera's Vole..
Microtus daghestanicus (Shidlovskiy, 1919) - Dagestan Pine Vole...
Microtus dogramacii Kefelioglu & Krystufek, 1999 - Dogramaci's Social Vole
Microtus duodecimcostatus (Sélys, 1839) - Mediterranean Pine Vole..
Microtus felteni (Malec & Storch, 1963) - Balkan Pine Vole.
Microtus fingeri (Neuhäuse, 1936) - Anatolian Pine Vole
Microtus guentheri (Danford & Alston, 1880) - Levant (Guenther's) Social Vole
Microtus hartingi Barrett-Hamilton, 1903 - Harting's Social Vole.
Microtus ilaeus igromovi Meyer & Golenishchev, 1996...
Microtus ilaeus ilaeus Thomas, 1912
Microtus ilaeus Thomas, 1912 - Kyrgyz Grey Vole...
Microtus irani bateae Kretzoi, 1962.
Microtus irani irani Thomas, 1921...
Microtus irani karamani Krystufek, Vohralik, Zima, Koubínová & Buzan, 2010..
Microtus irani schidlovskii Argyropulo, 1933..
Microtus irani Thomas, 1921 - Iranian Social Vole...
Microtus yuldaschi (Severtsov, 1879) - Juniper Vole...
Microtus yuldaschi carruthersi Thomas, 1909..
Microtus yuldaschi yuldaschi (Severtsov, 1879)
Microtus kermanensis Roguin, 1988 - Kerman Grey Vole
Microtus lavernedii (Crespon, 1844) - Mediterranean Field Vole..
Microtus liechtensteini (Wettstein, 1927) - Liechtenstein's Pine Vole
Microtus lusitanicus Gerbe, 1879 - Lusitanian Pine Vole
Microtus majori (Thomas, 1906) - Major's Pine Vole...
Microtus multiplex (Fatio, 1905) - Alpine Pine Vole...
Microtus mustersi Hinton, 1926 - Muster's Social Vole
Microtus mystacinus (Filippi, 1865) - Caspian Grey Vole...
Microtus nebrodensis (Minà-Palumbo, 1868) - Sicilian Savi's Vole..
Microtus obscurus (Eversmann, 1841) - Altai Grey Vole
Microtus paradoxus (Ogneff & Heptner, 1920) - Khorasan (Kopetdag) Social Vole..
Microtus pyrenaicus (Sélys, 1847) - Pyrenean Pine Vole
Microtus rossiaemeridionalis Ognev, 1924 - East-European Grey Vole..
Microtus rozianus (Bocage, 1865) - Portugese Field Vole
Microtus savii tolfetanus Contoli, 2003 ...
Microtus savii (Selys, 1838) - Common Savi's Vole...
Microtus savii savii (Selys, 1838)....
Microtus schelkovnikovi Satunin, 1907 - Schelkovnikov's Vole.
Microtus socialis (Pallas, 1773) - Common Social Vole
Microtus socialis aristovi Golenishchev, 2002
Microtus socialis bogdoensis Wang & Ma, 1981
Microtus socialis gravesi Goodwin, 1934
Microtus socialis nikolajevi Ognev, 1950..
Microtus socialis parvus Satunin, 1901.
Microtus socialis satunini (Ognev, 1924) ..
Microtus socialis socialis (Pallas, 1773) ..
Microtus socialis zaitsevi Golenishchev, 2002.
Microtus subterraneus (Sélys, 1836) - European Pine Vole....
Microtus tatricus (Kratochvíl, 1952) - Carpathian Pine Vole..
Microtus tatricus tatricus (Kratochvíl, 1952) ...
Microtus tatricus zykovi (Zagorodnyuk, 1989)...
Microtus thomasi (Barrett-Hamilton, 1903) - Thomas' Pine Vole..
Microtus transcaspicus Satunin, 1905 - Transcaspian Grey Vole.
Mictomicrotus liangshanensis (Liu, Sun, Zeng & Zhao, 2007) - Liangshan Vole...
Myopus schisticolor (Lilljeborg, 1844) - Wood Lemming...
Neodon clarkei (Hinton, 1923) - Clarke's Scrub Vole..
Neodon forresti Hinton, 1923 - Forrest's Scrub Vole.
Neodon fuscus (Büchner 1889) - Büchner's Scrub Vole...
Neodon irene (Thomas, 1911) - Chinese Scrub Vole.
Neodon irene irene (Thomas, 1911) ....
Neodon irene oniscus (Thomas, 1911)....
Neodon leucurus (Blyth, 1862) - Blyth's Scrub Vole...
Neodon linzhiensis Liu, Sun, Liu, Wang, Guo & Murphy, 2012 - Linzhi Scrub Vole.
Neodon medogensis Liu, Jin, Liu et al., 2017 - Medog Scrub Vole...
Neodon nepalensis Pradhan, Sharma, Sherchan et al., 2019 - Nepalese Scrub Vole.
Neodon nyalamensis Liu, Jin, Liu et al., 2017 - Nyalam Scrub Vole...
Proedromys bedfordi Thomas, 1911 - Groove-toothed Vole.
Prometheomys schaposchnikowi (Satunin, 1901) - Long-clawed Mole Vole.
Stenocranius gregalis (Pallas, 1779) - Common Narrow-headed Vole...
Stenocranius raddei (Polyakov, 1881) - Radde's Narrow-headed Vole..
SUBGENUS: Alexandromys Ognev, 1914.......
SUBGENUS: Alticola Blanford, 1881
SUBGENUS: Anteliomys Miller, 1896.
SUBGENUS: Aschizomys Miller, 1899...
SUBGENUS: Blanfordimys Argyropulo, 1933...
SUBGENUS: Chionomys Miller, 1908.......
SUBGENUS: Craseomys Miller, 1900 ....
SUBGENUS: Dicrostonyx Gloger, 1841.....
SUBGENUS: Ermites S. Liu, Y. Liu, Guo et al., 2012...
SUBGENUS: Euarvicola Acloque, 1900 - Field Voles..
SUBGENUS: Iberomys Chaline, 1972 - Cabrera's Voles
SUBGENUS: Lasiopodomys Lataste, 1887..
SUBGENUS: Lemmimicrotus Tokuda, 1941
SUBGENUS: Microtus Schrank, 1798 - Grey Voles...
SUBGENUS: Myolemmus Pomel, 1852...
SUBGENUS: Nedon Hodgson, 1849 - Himalayan Scrub Voles
Neodon sikimensis Hodgson, 1849 - Sikkim Scrub Vole..
SUBGENUS: Oecomicrotus Rabeder, 1981............
SUBGENUS: Phaiomys Blyth, 1862 - Thibetan Scrub Voles....
SUBGENUS: Phaulomys Thomas, 1905.....
SUBGENUS: Protochionomys new subgenus...
SUBGENUS: Terricola Fatio, 1867 - Pine voles...
SUBGENUS: Yushanomys new subgenus....
SUBTRIBE: Arvicolina Gray, 1821.
SUBTRIBE: Bramina Miller & Gidley, 1918.....
SUBTRIBE: Clethrionomyina Hooper & Hart, 1962......
SUBTRIBE: Eothenomyina - New Subtribe ...
SUBTRIBE: Hyperacrina New Subtribe.....
SUBTRIBE: Lagurina Kretzoi, 1955 ...
SUBTRIBE: Microtina Rhoads, 1895........
SUBTRIBE: Pliomyina Kretzoi, 1969
TRIBE: Arvicolini Gray, 1821..
TRIBE: Clethrionomyini Hooper & Hart, 1962 .....
TRIBE: Dicrostonychini Kretzoi, 1955 ..
TRIBE: Lemmini Miller, 1896..
TRIBE: Prometheomyini Kretzoi, 1955..
Volemys millicens (Thomas, 1911) - Common Sichuan Vole..
Volemys musseri (Lawrence, 1982) - Marie's Sichuan Vole.
"""


def parse_toc() -> Iterable[tuple[Rank, str]]:
    for line in TOC.splitlines():
        if not line:
            continue
        if line.startswith(("GENUS", "SUBGENUS", "TRIBE", "SUBTRIBE")):
            rank = Rank[line.split(":")[0].lower()]
            name = line.split()[1].strip(",")
            yield rank, name
        else:
            words = line.split()
            if words[2][0].isalpha() and words[2][0].islower() and words[2] != "new":
                yield Rank.subspecies, " ".join(words[:3])
            else:
                yield Rank.species, " ".join(words[:2])


def is_split_score(lines: list[str], index: int) -> int:
    count = 0
    for line in lines:
        if len(line) < index + 1:
            continue
        if line[index] != " ":
            continue
        if line[index + 1] == " ":
            continue
        count += 1
    return count


def format_page(page_no: int, lines: list[str]) -> list[str]:
    lines = lines[2:]
    scores = sorted(
        [(i, is_split_score(lines, i)) for i in range(120)],
        key=lambda pair: pair[1],
        reverse=True,
    )
    index, score = scores[0]
    if not (50 <= index <= 100):
        # probably only figures
        assert score < 10, page_no
        return []
    first_column = [line[: index + 1] for line in lines]
    second_column = [line[index + 1 :] for line in lines]
    skip_lines: set[int] = set()
    for i, line in enumerate(first_column):
        if second_column[i] and not line.endswith("  "):
            skip_lines.add(i)
    first_column = [
        line.rstrip() for i, line in enumerate(first_column) if i not in skip_lines
    ]
    second_column = [
        line.rstrip() for i, line in enumerate(second_column) if i not in skip_lines
    ]

    skip_lines = set()
    for i, line in enumerate(first_column):
        if (
            i > 0
            and not line
            and first_column[i - 1]
            and second_column[i]
            and not second_column[i - 1]
        ):
            skip_lines.add(i)
    first_column = [line for i, line in enumerate(first_column) if i not in skip_lines]
    second_column = [
        line for i, line in enumerate(second_column) if (i + 1) not in skip_lines
    ]
    return lib.merge_lines(first_column + second_column)


def extract_pages(lines: Iterable[str]) -> lib.PagesT:
    """Split the text into pages."""
    current_lines: list[str] = []
    page_no = 24
    for line in lines:
        line = line.replace(" ", " ")
        if line.startswith("\x0c"):
            yield page_no, format_page(page_no, current_lines)
            current_lines = []
            page_no += 1
        current_lines.append(line)
    yield page_no, format_page(page_no, current_lines)


def _parse_synonyms(lines: list[str], page_no: int) -> Iterable[lib.CEDict]:
    text = " ".join(lines).removeprefix("Synonyms.").strip().removesuffix(".").strip()
    text = re.sub(r"\s+", " ", text)
    for line in text.split(";"):
        synonym = line.strip()
        if match := re.search(r" \[(.*)\]$", synonym):
            comment = match.group(1)
            start, _ = match.span()
            synonym = synonym[:start].strip()
        else:
            comment = ""
        match = re.fullmatch(
            r"(?P<name>[^|]+) \| (?P<author>[A-ZÉ][^\d]+),? (?P<year>\d{4})", synonym
        )
        if match is None:
            name_regex = re.compile(
                r"""(?P<name>\[?[A-Z][a-z\.?\[\]]+(\s\([A-Z][a-züžſæ\-\[\]]+\))?(\s[a-züžſæčœ\-\[\]]+|\s[Vv]ar\.)*)
                \s(?P<author>[A-ZÉ][^\d]+)
                ,?\s(?P<year>\d{4})
                """,
                re.VERBOSE,
            )
            match = name_regex.fullmatch(synonym)
        if not match:
            if synonym == "Ellobius talpinus tanaiticus Zubko":
                yield {
                    "name": "Ellobius talpinus tanaiticus",
                    "rank": Rank.synonym,
                    "authority": "Zubko",
                    "article": SOURCE.get_source(),
                    "page": str(page_no),
                    "comment": comment,
                }
            else:
                assert False, line
        else:
            yield {
                "name": match.group("name"),
                "rank": Rank.synonym,
                "authority": match.group("author").strip().strip(","),
                "year": match.group("year"),
                "article": SOURCE.get_source(),
                "page": str(page_no),
                "comment": comment,
            }


def _parse_original_name(lines: list[str], page_no: int) -> Iterable[lib.CEDict]:
    text = " ".join(lines).strip()
    for section in IGNORED_SECTIONS:
        if section in text:
            text = text.split(section)[0].strip()

    if "|" in text:
        name_regex = re.compile(
            r"""(?P<name>[^|]+)
            \|\s*(?P<author>[A-ZÉĐ][^\d]+)
            ,?\s(?P<year>\d{4}):\s?(?P<page_no>\d[^\.]*)\.(?P<comment>.*)
            """,
            re.VERBOSE,
        )
    else:
        name_regex = re.compile(
            r"""(?P<name>\[?[A-Z][a-zŏæ\.?\[\]]+(\s\([A-Z][a-züžſæ\-\[\]]+\))?(\s[a-züžſæčœ\-\[\]]+|\s[Vv]ar\.)*)
            \s(?P<author>[A-ZÉ][^\d]+)
            ,?\s(?P<year>\d{4}[a-z]?)(?P<page_no>:\s?\d[^\.]*|\s\([^)]+\))\.(?P<comment>.*)
            """,
            re.VERBOSE,
        )
    match = name_regex.fullmatch(text)
    if not match:
        print("CANNOT MATCH 476", text)
        return
    assert match, lines
    comment = match.group("comment").strip()
    page_described = (
        match.group("page_no").strip().strip(":").strip("(").strip(")").strip()
    )
    data: lib.CEDict = {
        "name": match.group("name").strip(),
        "authority": match.group("author").strip().strip(","),
        "year": match.group("year"),
        "article": SOURCE.get_source(),
        "page": str(page_no),
        "rank": Rank.synonym,
        "raw_data": text,
        "page_described": page_described,
    }
    if "type locality" in comment.lower():
        data["type_locality"] = comment
    else:
        data["comment"] = comment
    yield data


IGNORED_SECTIONS = (
    "Taxonomy.",
    "Distribution.",
    "Characteristics.",
    "Distribution (",
    "Taxonomy and ",
    "Nomenclature.",
)


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()
    # so it can serve as the parent
    yield {"rank": Rank.subfamily, "name": "Arvicolinae", "article": art, "page": "23"}

    pending_synonym_lines: list[str] = []
    pending_original_name_lines: list[str] = []
    started_taxon = False
    for page_no, lines in pages:
        for line in lines:
            line = line.lstrip()
            higher_match = re.search(
                r"^\s*(?P<rank>TRIBE|SUBTRIBE|GENUS|SUBGENUS): (?P<name>[A-Z][a-z]{2,20}) (– New (Subtribe|Subgenus|Genus)|(?P<author>[A-Z][^\d]{3,30}), (?P<year>\d{4}))( –)?$",
                line,
            )
            if higher_match:
                assert not pending_synonym_lines, pending_synonym_lines
                yield {
                    "name": higher_match.group("name"),
                    "rank": Rank[higher_match.group("rank").lower()],
                    "authority": higher_match.group("author"),
                    "year": higher_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                }
                started_taxon = True
                continue
            if line.strip().startswith(("TRIBE", "SUB", "GENUS")):
                print("BAD LINE 520", repr(line))
            species_match = re.search(
                r"^ *(?P<name>[A-Z][a-z]{3,30} [a-z]{3,20}( [a-z]{3,20})?) (new (sub)?species|(?P<author>(de |von )?[A-ZĐ][^\d]{2,70}), (?P<year>\d{4}))( –|$)",
                line,
            )
            if species_match is None:
                species_match = re.search(
                    r"^ *(?P<name>[A-Z][a-z]{3,30} [a-z]{3,20}( [a-z]{3,20})?) \((?P<author>(de |von )?[A-ZĐ][^\d]{2,50}), (?P<year>\d{4})\)( –|$)",
                    line,
                )
            if species_match:
                assert not pending_synonym_lines, pending_synonym_lines
                name = species_match.group("name")
                rank = Rank.species if name.count(" ") == 1 else Rank.subspecies
                data: lib.CEDict = {
                    "name": name,
                    "rank": rank,
                    "authority": species_match.group("author"),
                    "year": species_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                }
                if rank is Rank.subspecies:
                    data["parent_rank"] = Rank.species
                    data["parent"] = " ".join(name.split(" ")[:2])
                yield data
                started_taxon = True
                continue
            if line.startswith(IGNORED_SECTIONS):
                if pending_synonym_lines:
                    yield from _parse_synonyms(pending_synonym_lines, page_no)
                    pending_synonym_lines = []
                if pending_original_name_lines:
                    yield from _parse_original_name(
                        pending_original_name_lines, page_no
                    )
                    pending_original_name_lines = []
                started_taxon = False
            if started_taxon and (
                pending_original_name_lines or re.search(r"\d{4}:", line)
            ):
                if "Synonyms." in line:
                    line, starting_synonyms = line.split("Synonyms.")
                else:
                    starting_synonyms = None
                pending_original_name_lines.append(line)
                if starting_synonyms:
                    yield from _parse_original_name(
                        pending_original_name_lines, page_no
                    )
                    pending_original_name_lines = []
                    pending_synonym_lines.append(starting_synonyms)
                    continue
            if line.startswith("Synonyms.") or pending_synonym_lines:
                started_taxon = False
                if pending_original_name_lines:
                    yield from _parse_original_name(
                        pending_original_name_lines, page_no
                    )
                    pending_original_name_lines = []
                for section in IGNORED_SECTIONS:
                    if section in line:
                        line = line.split(section)[0]
                        pending_synonym_lines.append(line)
                        yield from _parse_synonyms(pending_synonym_lines, page_no)
                        pending_synonym_lines = []
                        break
                else:
                    pending_synonym_lines.append(line)


def validate_taxa(nams: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    toc_taxa = set(parse_toc())
    for nam in nams:
        if nam["rank"] is Rank.synonym or nam["rank"] is Rank.subfamily:
            yield nam
            continue
        key = (nam["rank"], nam["name"])
        if key not in toc_taxa:
            print("MISSING FROM TOC", key)
        else:
            toc_taxa.remove(key)
        yield nam
    for not_found in sorted(toc_taxa):
        print(not_found)


def main() -> None:
    text = lib.get_text(SOURCE)
    pages = extract_pages(text)
    names = extract_names(pages)
    names = lib.add_parents(names)
    names = lib.validate_ce_parents(names)
    names = validate_taxa(names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
