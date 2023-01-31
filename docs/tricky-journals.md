# Tricky journals

Some journals have tricky histories: they were renamed repeatedly, divided into
different series (and sometimes undivided again), or were known under different names.
This page discusses the tools I use to make sense of these of such journals, and lists
evidence for a few particular cases.

# Treatment in the database

I use the following tags on [citation group](/docs/citation-group) objects to help with
tricky cases:

- _YearRange_: Gives the first and last years when a journal was published. A
  maintenance script enforces that all articles and names in the citation group fall
  within the range.
- _Predecessor_: Reference to an older journal that this journal is a renaming of.
- _MustHaveSeries_: Enforce that all articles in this citation group must have the
  "series" field set.

# Specific cases

Below "v." stands for "volume".

## University of California Geology

The University of California's geological series has gone through a confusing series of
renames:

- [Bulletin of the Department of Geology of the University of California](/cg/3965) (v.
  1-2, 1896–1902)
- [University of California Publications, Bulletin of the Department of Geology](/cg/1253)
  (v. 3-6, 1902–1911)
- [University of California Publications in Geology](/cg/23) (v. 7-12, 1912–1921)
- [University of California Publications, Bulletin of the Department of Geological Sciences](/cg/850)
  (v. 13-28, 1921–1951)
- [University of California Publications in Geological Sciences](/cg/1251) (v.29-,
  1951-)

Main source: https://searchworks.stanford.edu/view/355199

BHL splits it up differently:

- [Bulletin of the Department of Geology of University of California](https://www.biodiversitylibrary.org/bibliography/77485)
  (v. 1-2)
- [Bulletin of the Department of Geology](https://www.biodiversitylibrary.org/bibliography/69850)
  (v. 3–12)
- [University of California Publications in Geological Sciences](https://www.biodiversitylibrary.org/bibliography/77953)
  (v. 13)

## Beiträge zur Paläontologie

The Austrian journal _Beiträge zur Paläontologie_ had various more ornate names in the
past.

- [Beiträge zur Paläontologie Österreich-Ungarns und des Orients](/cg/1702) v. 1-8
  (1882-1890/91)
  - Stanford catalog claims that v. 1 was titled "Beiträge zur Paläontologie von
    Österreich-Ungarn und den angrenzenden Gebeiten \[sic\]" but see
    [BHL](https://www.biodiversitylibrary.org/item/50650#page/11/mode/1up)
- [Beiträge zur Paläontologie und Geologie Österreich-Ungarns und des Orients](/cg/1192)
  v. 9-27 (1894/95-1914/15)
- [Beiträge zur Paläontologie von Österreich](/cg/2016) v. 1-17 (1976-1992)
- [Beiträge zur Paläontologie](/cg/1055) v. 18-32 (1994-2011)

Sources:

- https://searchworks.stanford.edu/view/353310 (BzPÖUO)
- https://searchworks.stanford.edu/view/408412 (BzPvÖ)
- https://searchworks.stanford.edu/view/2998509 (BzP)
- https://www.biodiversitylibrary.org/bibliography/14585#/summary
- https://www.biodiversitylibrary.org/bibliography/14602#/summary

## Neues Jahrbuch

The venerable German journal the _Neues Jahrbuch für Geologie und Paläontologie_ has
gone through an exceptionally confusing set of names. It is divided into _Abhandlungen_
and _Monatshefte_, each of which has had its own set of renamings.

- Monatshefte
  - [Taschenbuch für die gesammte Mineralogie](/cg/4261) (Frankfurt, 1807-1829)
  - [Jahrbuch für Mineralogie, Geognosie, Geologie und Petrefakten-Kunde](/cg/2748)
    (Heidelberg, 1830-1832)
  - [Neues Jahrbuch für Mineralogie, Geognosie, Geologie und Petrefaktenkunde](/cg/1577)
    (1833-1862)
  - [Neues Jahrbuch für Mineralogie, Geologie und Paläontologie](/cg/810) (1863-1942)
    - Divided into Abt. A, B (1925-1927)
    - Divided into Abt. 1, 2, 3 (1928-1942)
  - [Neues Jahrbuch für Mineralogie, Geologie und Paläontologie. Monatshefte](/cg/4262)
    (1943-1949)
    - Divided into Abt. A, B
  - [Neues Jahrbuch für Geologie und Paläontologie. Monatshefte](/cg/786) (1950-2006)
- Abhandlungen
  - Neues Jahrbuch für Mineralogie, Geologie und Paläontologie. Beilage-band (1881-1942,
    included in [CG#810](/cg/810))
  - [Neues Jahrbuch für Mineralogie, Geologie und Paläontologie. Abhandlungen](/cg/1635)
    (1943-1950)
  - [Neues Jahrbuch für Geologie und Paläontologie. Abhandlungen](/cg/711)
    (1950-present)
- Post-1950 there is also a separate Neues Jahrbuch für Mineralogie.

Sources:

- https://searchworks.stanford.edu/view/398259
- https://searchworks.stanford.edu/view/371179 (most detail)
- https://searchworks.stanford.edu/view/371180
- https://searchworks.stanford.edu/view/487499
- https://searchworks.stanford.edu/view/497423
- https://www.biodiversitylibrary.org/bibliography/51831#/summary

## Comptes Rendus

The proceedings of the Paris Academy are another venerable journal. Their most confusing
aspect is the proliferation of series in the late 20th century. Series that are relevant
to the database are _italicized_.

- The Stanford catalog notes that before 1835 there was a "Proces-verbaux des seances de
  l'Academie", but I have not encountered this name in my research
- [Comptes rendus hebdomadaires des séances de l'Académie des sciences](/cg/1598)
  (1835–1978)
  - No series (v. 1–261, 1835–1965)
  - Same name (and therefore same citation group), but split into series (v. 262-287,
    1966-1978)
    - Série A - Sciences mathématiques et Série B - Sciences physiques (apparently
      combined and sometimes split)
    - Série C - Sciences chimiques
    - _Série D_ - Sciences naturelles
- [Comptes rendus des séances de l'Académie des sciences](/cg/4698) (1979–1983)
  - Continuing series A–D as before (v. 288–291, 1979–1980)
  - New set of series (v. 292–297, 1981–1983)
    - Série I, Mathématique
    - Série II, Mécanique, physique, chimie, sciences de la terre, sciences de l'univers
    - _Série III_, Sciences de la vie
- [Comptes rendus de l'Académie des sciences](/cg/47) (1984–2001)
  - Série I, Mathématique (v. 298–333, 1984–2001)
  - _Série II_, Mécanique, physique, chimie, sciences de l'univers, sciences de la terre
    (v. 298–317, 1984–1993)
  - _Série II_, Sciences de la terre et des planètes (v. 318–333, 1994–2001)
    - Sometimes called "IIa", which I'll stick with since there were two Série II
      sequences
  - Série II, Mécanique, physique, chimie, astronomie (v. 318–325, 1994–1997) (properly
    IIb)
  - Série IIb, Mécanique, physique, astronomie (v. 326–327, 1998–1999)
  - Série IIc, Chimie (v. 1–4, 1998–2001)
  - _Série III_, Sciences de la vie (v. 298–324, 1984–2001)
  - Série IV, Physique, astrophysique (v. 1–2, 2000–2001)
  - I think a series for "Mécanique" also showed up at some point but I lost track
- Split into different journals (2002–present)
  - _[Comptes Rendus Biologies](/cg/1322)_ (v. 325-, 2002-)
  - Comptes Rendus Chimie (v. 5–, 2002–)
  - Comptes Rendus Géoscience (v. 334–, 2002–)
  - Comptes Rendus Mathématique (v. 334–, 2002–)
  - _[Comptes Rendus Palevol](/cg/730)_ (v. 1–, 2002–)
  - Comptes Rendus Physique (v. 3–, 2002–)

Sources:

- https://www.academie-sciences.fr/fr/Transmettre-les-connaissances/comptes-rendus-de-l-academie-des-sciences-numerisees-sur-le-site-de-la-bibliotheque-nationale-de-france.html
  (from the Académie itself, but the list is an oversimplification)
- https://searchworks.stanford.edu/view/13433202

## Muséum national d'histoire naturelle

The Paris Museum has published and frequently renamed several journals. They seem to
fall in three groups.

- The "old" group, frequently renamed and now defunct
  - [Annales du Muséum d'histoire naturelle](/cg/1365) (v. 1–21, 1802–1813,
    [Stanford](https://searchworks.stanford.edu/view/373356))
  - [Mémoires du Muséum d'histoire naturelle](/cg/1683) (v. 1–20, 1815–1832,
    [Stanford](https://searchworks.stanford.edu/view/373360))
  - [Nouvelles annales du Muséum d'histoire naturelle](/cg/1345) (v. 1–4, 1832–1835,
    [Stanford](https://searchworks.stanford.edu/view/373363))
  - [Archives du Muséum d'histoire naturelle](/cg/342) (v. 1–10, 1836–1861,
    [Stanford](https://searchworks.stanford.edu/view/5791298))
  - [Nouvelles archives du Muséum d'histoire naturelle](/cg/1952) (1865–1914,
    [Stanford](https://searchworks.stanford.edu/view/9318862))
    - Ser. 1 (v. 1–10, 1865–1874)
    - Ser. 2 (v. 1–10, 1878-1888)
    - Ser. 3 (v. 1–10, 1889–1898)
    - Ser. 4 (v. 1–10, 1899–1908)
    - Ser. 5 (v. 1–6, 1909–1914)
  - [Archives du Muséum national d'histoire naturelle](/cg/2226) (1926–1970,
    [Stanford](https://searchworks.stanford.edu/view/373357),
    [BHL](https://www.biodiversitylibrary.org/bibliography/168894))
    - Ser. 6 (v. 1–19, 1926–1942)
    - Ser. 7 (v. 1–10, 1952–1970)
- The "Bulletin" group, now Zoosystema and Geodiversitas
  - [Bulletin du Muséum d'histoire naturelle](/cg/4701) (v. 1-12, 1895–1906,
    [BHL](https://www.biodiversitylibrary.org/bibliography/68686))
  - [Bulletin du Muséum national d'histoire naturelle](/cg/740)
    ([BHL](https://www.biodiversitylibrary.org/bibliography/5943))
    - Series 1 (v. 13-34, 1907–1928)
    - Series 2 (v. 1-42, 1929–1970)
    - Series 3 (1971–1978) was divided into named sections
      - Zoologie ([BHL](https://www.biodiversitylibrary.org/bibliography/149559))
      - Sciences de la terre
        ([BHL](https://www.biodiversitylibrary.org/bibliography/145272))
      - Botanique, Écologie générale, Sciences de l'homme, Sciences physico-chimiques
        (not of concern here)
    - Series 4 (v. 1–18, 1979–1996) was divided into lettered sections
      - Section A Zoologie, biologie et écologie animales (v. 1–18, 1979–1996,
        [BHL](https://www.biodiversitylibrary.org/bibliography/158834))
      - Section B was named "Botanique, biologie et écologie végétales, phytochimie" in
        1979–80 ([BHL](https://www.biodiversitylibrary.org/bibliography/14109)) and then
        "Adansonia" in 1981–1996
        ([BHL](https://www.biodiversitylibrary.org/bibliography/13855))
      - Section C Sciences de la terre, paléontologie, géologie, minéralogie (v. 1–18,
        1979–1996, [BHL](https://www.biodiversitylibrary.org/bibliography/146268))
  - Then split into different journals (1997–present)
    - [Zoosystema](/cg/1075) (succeeding section A)
    - Adansonia (succeeding section B)
    - [Geodiversitas](/cg/468) (succeeding section C)
- The "Mémoires" group, for longer papers, always named
  [Mémoires du Muséum national d'histoire naturelle](/cg/892)
  - The "nouvelle série" of 1936–1950
    ([MNHN](https://sciencepress.mnhn.fr/fr/collections/memoires-du-museum-national-d-histoire-naturelle-nouvelle-serie-1935-1950))
  - Three thematic series (1950–1992)
    - Série A Zoologie (v. 1–154, 1950–1992,
      [BHL](https://www.biodiversitylibrary.org/bibliography/155000))
    - Série B Botanique (v. 1–32, 1950–1990,
      [BHL](https://www.biodiversitylibrary.org/bibliography/158815))
    - Série C Sciences de la terre (v. 1–56, 1950–1990,
      [BHL](https://www.biodiversitylibrary.org/bibliography/160082), one volume was
      "Géologie" instead)
  - Reunified into a single series (v. 155–, 1993–,
    [BHL](https://www.biodiversitylibrary.org/bibliography/162187))

Note that there are also some similarly named publications by the Lyon museum (e.g.
[Nouvelles archives du Muséum d'histoire naturelle de Lyon](/cg/1733)).

## Proceedings of the Zoological Society of London

The [_Proceedings_](/cg/1) were one of the most important venues for publishing new
species in 19th-century Britain.

For most of its lifetime, the journal lacked explicit volume numbers; each volume
contained the papers presented in the Society's meetings during a particular year.
However, the publication of the printed volume often happened considerably later. A
useful overview of the early history of the journal is [Duncan (1937)](/a/14498), with
additions by [Cowan (1973)](/a/59416) and [Dickinson (2005)](/a/14497). These sources
provide precise dates of publication for all volumes from 1861 to 1925; before 1861 the
data is fuzzier.

The precise title varied, but the journal is usually (including in this database)
referred to as _Proceedings of the Zoological Society of London_ throughout its history:

- _Proceedings of the Committee of Science and Correspondence of the Zoological Society
  of London_ (1831–1832; [Stanford](https://searchworks.stanford.edu/view/10156443))
  - Parts 1 and 2
- _Proceedings of the Zoological Society of London_ (1833–1860;
  [Stanford](https://searchworks.stanford.edu/view/10156446))
  - Yearly volumes were numbered as parts 1–28. I believe they are more commonly
    referred to by their years.
- _The proceedings of the scientific meetings of the Zoological Society of London_
  (1861–1890; [Stanford](https://searchworks.stanford.edu/view/10156448))
  - Up to 1873 volumes were issued in three parts, afterwards in four
- _Proceedings of the general meetings for scientific business of the Zoological Society
  of London_ (1891–1936; [Stanford](https://searchworks.stanford.edu/view/10156452))
  - During the years 1901–1905, two volumes were published each year with independent
    numbering. I refer to these as "1901-I" and "1901-II".
  - During 1917–1920 fewer than four parts were published, as some parts were combined.
- Split into series A and B (1937–1944), now with volume numbers (107–113) instead of
  just years
  - _Proceedings of the Zoological Society of London. Series A, General and
    experimental_ ([Stanford](https://searchworks.stanford.edu/view/10156470))
  - _Proceedings of the Zoological Society of London. Series B, Systematic and
    morphological_ ([Stanford](https://searchworks.stanford.edu/view/10156473))
  - According to the Stanford catalog series A and B start with vol. 107, pt. 2. I do
    not know what became of pt. 1.
- _Proceedings of the Zoological Society of London_ (1944–1965, volumes 114–145,
  [Stanford](https://searchworks.stanford.edu/view/10156483))
- [_Journal of Zoology, London_](/cg/693) (starting 1965, vol. 146,
  [Stanford](https://searchworks.stanford.edu/view/401297))
  - Later again temporarily split into series A and B (1985–1987)

Separate [_Abstracts of the Proceedings of the Zoological Society of London_](/cg/38)
were issued for some time, at least 1897 to 1933, often providing an earlier date of
publication for names more fully described in the _Proceedings_. I have often found it
difficult to locate these publications.
