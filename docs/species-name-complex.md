# Species name complex

A species name complex is an entity that describes a group of species names that share the same etymology
and grammatical behavior. For example, all species names based on the Latin adjective _palustris_ form the
[_palustris_ species name complex](/sc/palustris).

Species name complexes are primarily useful for
determining whether and how species names need to be declined for gender, because species names that are
Latin adjectives agree in grammatical gender with the genus name. Species name complexes also make it easier to
understand the etymologies of names.

## Fields

Species name complexes have the following fields:

- _kind_: the kind of name complex, such as an adjective or patronym. The options are discussed more
  [below](#kinds-of-name-complexes).
- _label_: a unique label that identifies the complex. For complexes based on Latin words, the label
  is the Latin word; for others it might be a brief description (e.g., [non_latin](/sc/1)).
- _stem_: for complexes based on Latin words, all names in the complexes end with this stem. For example,
  the stem for the [_palustris_](/sc/palustris) complex is "_palustris_", and the complex includes names
  that either are exactly _palustris_ (e.g., [_Oryzomys palustris_](/n/59371)) or _palustris_ with some prefix
  (e.g., [_Sorex megapalustris_](/n/15061)).
- _masculine ending_: the ending the name takes in the masculine. This may be an empty string, but if it
  is not, the stem will always be suffixed with this ending, because the masculine form of an adjective
  is the dictionary form. For _palustris_, the masculine ending is _-is_. Many name complexes do not
  change depending on gender, in which case all three ending fields are left blank (e.g., [_vagrans_](/sc/vagrans))
- _feminine ending_: to find the feminine form of a name, we remove the masculine ending from the stem,
  then add the feminine ending. For _palustris_, the feminine ending is also _-is_, so the feminine and
  masculine forms are the same.
- _neuter ending_: like the feminine ending, but for the neuter form. The neuter ending of _palustris_ is _-e_,
  so if a _palustris_ name is combined with a neuter genus name, the species name becomes _palustre_,
  as with [_Archaeotherium palustre_](/n/71468).
- _comment_: A text comment on the name complex. Often this will give an etymology.

## Kinds of name complexes

Under Article 11.9.1, species names fall in one of five groups: (1) Latin adjectives, (2) Latin nouns
in the nominative case, (3) Latin or Latinized nouns in the genitive case, (4) Latin adjectives in the
genitive case, and (5) non-Latin words.

### Latin adjectives

Latin and Latinized adjectives are a very common category of species names. Names in this group are
the only ones that change form depending on the grammatical gender of the genus they are assigned to.
Zoological nomenclature declines only Latin and Latinized adjectives; adjectives in other languages,
even Greek, are treated as indeclinable (Art. 31.2.3).

There are two main groups of Latin adjectives, and several smaller ones:

- First and second declension adjectives in _-us_, _-a_, _-um_ (for example, [_rufus_](/sc/rufus) "red").
- Third declension adjectives in _-is_, _-is_, _-e_ (for example, _agilis_ "agile"). This includes names in
  [_-ensis_](/sc/ensis), which are commonly used with geographic names. These adjectives are identical in the masculine
  and feminine forms, but the neuter form uses the _-e_ ending.
- Third declension adjectives in _-ns_, which are identical in all genders in the nominative singular and
  can therefore be treated as indeclinable for the purposes of nomenclature. This includes names like
  [_elegans_](/sc/elegans) "elegant" and [_rufescens_](/sc/rufescens) "reddish".
- First and second declension adjectives in _-(e)r_, _-ra_, _-rum_ (for example, [_ater_](/sc/ater) "black"). These are
  fairly rare, but a few names in this category are common.
- Third declension adjectives in _-or_, _-or_, _-us_. These are frequently comparatives, including [_major_](/sc/major)
  "larger" and [_minor_](/sc/minor) "smaller".

Latin has many other groups of irregular adjectives, including some that are used in scientific names,
such as [_alius_, _alia_, _aliud_](/sc/alius) and [_celer_, _celeris_, _celere_](/sc/celer).

There is a special group of names of the form adjective-i-noun, where the noun is often a body part,
like [_breviceps_](/sc/ceps) "short-headed". I believe that these names are adjectives, although many are
indeclinable.

### Nouns in apposition

Latin nouns in the nominative case are fairly common as scientific names. For example, _typus_ "type"
is common. The Code specifies (Art. 31.2.2) that names that could be either a noun or an
adjective are to be treated as nouns unless there is evidence that the author intended them to be
adjectives. This affects many names in _-fer_. These names are in [the "noun in apposition" complex](/sc/4).

### Genitive nouns

Latin nouns in the genitive case are often geographic names (e.g. _italiae_ "of Italy"), which are included
in [the "genitive" complex](/sc/genitive). However, by
far the most common group are patronyms, names formed from personal names. Patronyms in turn fall
into two subgroups. Latin and Latinized names use the proper Latin genitive form, which will
frequently be a suffix of _-is_ or _-i_ (["patronym latin"](/sc/9)).
Modern names are turned into patronyms by adding [_-i_](/sc/2) (for a man),
[_-ae_](/sc/6) (for a woman), [_-orum_](/sc/8) (for multiple men or a mixed group),
or [_-arum_](/sc/7) (for multiple women).

A few practical issues that arise are:

- Taxonomists regularly make mistakes with patronymic endings, like using _-i_ when naming a species
  after a group of people or using _-i_ with a woman's name. However, such errors may not be corrected
  by changing the name (see Art. 31.1.3 and 32.5).
- There is frequently variation between names in _-ii_ and _-i_. The latter is the standard non-Latin
  masculine patronymic ending, and the former is the result of implicitly Latinizing a name by adding the
  ending _-ius_, then taking the genitive. In general, the original spelling should be followed.
  The Code has special prohibitions that stipulate that variation between _-i_ and _-ii_ is always
  an incorrect subsequent spelling, not an emendation, and that _-i_ and _-ii_ names are homonyms
  even if spelled differently (Art. 58).

### Genitive adjectives

Although the Code allows for species names formed from the genitive forms of Latin adjectives,
the provision appears to be mostly intended for parasites. I have never encountered such names
in tetrapods, but the few examples in the database are included in [the "genitive adjective" complex](/sc/5).

### Non-Latin words

A large proportion of species names are not based on Latin or Latinized words. These names are
always indeclinable. They are in [the "non-Latin" complex](/sc/1).
