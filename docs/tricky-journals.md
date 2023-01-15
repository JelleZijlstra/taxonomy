# Tricky journals

Some journals have tricky histories: they were renamed repeatedly,
divided into different series (and sometimes undivided again), or
were known under different names. This page discusses the tools
I use to make sense of these of such journals, and lists evidence
for a few particular cases.

# Treatment in the database

I use the following tags on [citation group](/docs/citation-group)
objects to help with tricky cases:

- _YearRange_: Gives the first and last years when a journal was
  published. A maintenance script enforces that all articles and
  names in the citation group fall within the range.
- _Predecessor_: Reference to an older journal that this journal
  is a renaming of.
- _MustHaveSeries_: Enforce that all articles in this citation group
  must have the "series" field set.

# Specific cases

## University of California Geology

The University of California's geological series has gone through a confusing
series of renames:

- [Bulletin of the Department of Geology of the University of California](/cg/3965) (v. 1-2, 1896–1902)
- [University of California Publications, Bulletin of the Department of Geology](/cg/1253) (v. 3-6, 1902–1911)
- [University of California Publications in Geology](/cg/23) (v. 7-12, 1912–1921)
- [University of California Publications, Bulletin of the Department of Geological Sciences](/cg/850) (v. 13-28, 1921–1951)
- [University of California Publications in Geological Sciences](/cg/1251) (v.29-, 1951-)

Main source: https://searchworks.stanford.edu/view/355199

BHL splits it up differently:
- [Bulletin of the Department of Geology of University of California](https://www.biodiversitylibrary.org/bibliography/77485) (v. 1-2)
- [Bulletin of the Department of Geology](https://www.biodiversitylibrary.org/bibliography/69850) (v. 3–12)
- [University of California Publications in Geological Sciences](https://www.biodiversitylibrary.org/bibliography/77953) (v. 13)

## Beiträge zur Paläontologie

The Austrian journal _Beiträge zur Paläontologie_ had various more ornate names in the past.

- [Beiträge zur Paläontologie Österreich-Ungarns und des Orients](/cg/1702) v. 1-8 (1882-1890/91)
    - Stanford catalog claims that v. 1 was titled "Beiträge zur Paläontologie von Österreich-Ungarn und den angrenzenden Gebeiten \[sic\]" but see [BHL](https://www.biodiversitylibrary.org/item/50650#page/11/mode/1up)
- [Beiträge zur Paläontologie und Geologie Österreich-Ungarns und des Orients](/cg/1192) v. 9-27 (1894/95-1914/15)
- [Beiträge zur Paläontologie von Österreich](/cg/2016) v. 1-17 (1976-1992)
- [Beiträge zur Paläontologie](/cg/1055) v. 18-32 (1994-2011)

Sources:
- https://searchworks.stanford.edu/view/353310 (BzPÖUO)
- https://searchworks.stanford.edu/view/408412 (BzPvÖ)
- https://searchworks.stanford.edu/view/2998509 (BzP)
- https://www.biodiversitylibrary.org/bibliography/14585#/summary
- https://www.biodiversitylibrary.org/bibliography/14602#/summary

## Neues Jahrbuch

The venerable German journal the _Neues Jahrbuch für Geologie und Paläontologie_
has gone through an exceptionally confusing set of names. It is divided into
_Abhandlungen_ and _Monatshefte_, each of which has had its own set of renamings.

- Monatshefte
    - [Taschenbuch für die gesammte Mineralogie](/cg/4261) (Frankfurt, 1807-1829)
    - [Jahrbuch für Mineralogie, Geognosie, Geologie und Petrefakten-Kunde](/cg/2748) (Heidelberg, 1830-1832)
    - [Neues Jahrbuch für Mineralogie, Geognosie, Geologie und Petrefaktenkunde](/cg/1577) (1833-1862)
    - [Neues Jahrbuch für Mineralogie, Geologie und Paläontologie](/cg/810) (1863-1942)
        - Divided into Abt. A, B (1925-1927)
        - Divided into Abt. 1, 2, 3 (1928-1942)
    - [Neues Jahrbuch für Mineralogie, Geologie und Paläontologie. Monatshefte](/cg/4262) (1943-1949)
        - Divided into Abt. A, B
    - [Neues Jahrbuch für Geologie und Paläontologie. Monatshefte](/cg/786) (1950-2006)
- Abhandlungen
    - Neues Jahrbuch für Mineralogie, Geologie und Paläontologie. Beilage-band (1881-1942, included in [CG#810](/cg/810))
    - [Neues Jahrbuch für Mineralogie, Geologie und Paläontologie. Abhandlungen](/cg/1635) (1943-1950, CG#1635)
    - [Neues Jahrbuch für Geologie und Paläontologie. Abhandlungen](/cg/711) (1950-present)
- Post-1950 there is also a separate Neues Jahrbuch für Mineralogie.

Sources:
- https://searchworks.stanford.edu/view/398259
- https://searchworks.stanford.edu/view/371179 (most detail)
- https://searchworks.stanford.edu/view/371180
- https://searchworks.stanford.edu/view/487499
- https://searchworks.stanford.edu/view/497423
- https://www.biodiversitylibrary.org/bibliography/51831#/summary