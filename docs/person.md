# Person

People are included in the database when they do something that affects the database's
taxonomic and biological data. They can currently do so in four ways:

- Writing [articles](/docs/article)
- Introducing new [names](/docs/name)
- Collecting type specimens
- Having things named after them

All of these relationships are reflected in the database.

I only added people to the database as a distinct concept relatively recently (October
2020), and it is difficult to sort out, for example, all the people named
[Smith](/h/Smith) in the database. Therefore, most people are in an "unchecked" state,
where they are aggregated just by name, and I am not confident that all references
actually point to the same person. For some other names, I have sorted out the people
involved more precisely, and marked some people as "checked". For example, I cleaned up
all references to myself and aggregated them at [Zijlstra, Jelle Sjoerd](/h/5331).

## Fields

Persons have the following fields:

- _type_: By default this is set to _unchecked_, which means I have not manually
  reviewed the data associated with the person to verify all references are to the same
  person. After I verify a name, I set the type to _checked_. A _hard redirect_ marks an
  obsolete duplicate whose references should all point to its canonical target. A _name
  alias_ is a valid publication identity whose references intentionally retain the
  alternate displayed name while also being aggregated under a canonical Person. The
  former _soft redirect_ state is deprecated and automatically converted to a hard
  redirect by Person lint. Last, a person can be _deleted_. Unchecked persons that no
  longer have any references to them are automatically marked as deleted.
- _naming convention_: Different cultures have different conventions for forming and
  treating personal names, and this field lets us indicate what convention to use. For
  example, this may be set to _chinese_ or _dutch_. Special cases include _organization_
  (which occasionally are listed as the authors of articles or have species named after
  them), and _ancient_ for ancient people, who usually only have a single name, like
  [Philip II of Macedon](/h/1).
- _family name_: The person's family name. This field is always set and used to find
  persons, so it's also set for organizations and for people who technically do not have
  a family name, like Philip II.
- _given names_: The person's given names, in full, like "Jelle Sjoerd".
- _initials_: The person's initials, like "J.S.".
- _suffix_: Suffixes like "Jr." and "III".
- _tussenvoegsel_: This is a convention-dependent intermediate name component. In
  Vietnamese names, it contains the middle name or names. In Dutch names, the
  [tussenvoegsel](https://en.wikipedia.org/wiki/Tussenvoegsel) consists of words like
  "de" or "van den" placed between the given and family names. The field is also used
  for some German and French names, like "von Meyer", that are not always included in
  the person's name. I am not clear on how exactly such words are supposed to be treated
  in some European languages—perhaps in some cases they should actually be treated as
  part of the family name.
- _birth_: The person's year of birth.
- _death_: The person's year of death.
- _bio_: A short description that helps identify the person, like "American
  paleontologist".
- _tags_: Several references that give more information about the person:
  - _Wiki_: A link to a Wikipedia article
  - _Institution_: Reference to a [collection](/docs/collection) the person was or is
    associated with.
  - _ActiveRegion_: Reference to a [region](/docs/region) that the person was or is
    active in.
  - _Biography_: Reference to an [article](/docs/article) containing biographical
    details about the person, such as an obituary.
  - _ORCID_: The person's public [ORCID](https://orcid.org/) identifier.
  - _IgnoreORCIDWork_: A reviewed exception for one DOI that an ORCID profile claims but
    whose deposited author list identifies different people. The tag records both the
    ORCID and DOI so it does not suppress checks of the person's other works.

## Naming conventions

Different cultures use different conventions for forming and treating personal names,
and in this database I try to reflect these conventions. The database currently supports
marking persons with about 15 naming conventions; more may be added as necessary. The
naming conventions can also be used to check for mistakes; for example, Cyrillic letters
can only appear in names fron languages that use Cyrillic, such as Russian.

More coding work is also needed to make sure naming conventions are used correctly when
displaying names on this website.

- _unspecified_: Default naming convention for unchecked names. These are generally
  treated similarly to Western names.
- _general_: Catch-all naming convention for names from most European and some other
  languages. These generally have a family name (consisting usually of one word, but
  possibly of multiple words, separated by spaces or dashes) and one or more given
  names. There are separate naming conventions for several other European languages that
  have certain peculiarities, but usually these peculiarities are only relevant in rare
  cases. Names from these languages that lack these peculiarities may be placed in the
  _general_ convention.
- _ancient_: This naming convention is used for ancient people without family names, and
  in practice also for some not-so-ancient people without a family name like
  [Queen Victoria](/h/49746).
- _organization_: For organizations, which occasionally are listed as the authors of
  articles or have things named after them. We also have a person named
  "[Anonymous](/h/35349)" for anonymously authored works.

Languages written in the Latin alphabet:

- _dutch_: Dutch has very specific rules for treating names with _tussenvoegsels_ such
  as "van den", but Dutch names are otherwise similar to other Western names. These
  rules apply only to Dutch people from the Netherlands; in Belgium these
  _tussenvoegsels_ are simply treated as part of the family name without special
  treatment.
- _english_: English names allow suffixes like "Jr.", "III".
- _english_peer_: For English peers, the suffix is the noble title (e.g., "3rd Earl of
  Cranbrook").
- _french_: Nothing too special, but the conventions around particles (e.g., in "de
  Blainville") need further study.
- _german_: German names may use particles like "von".
- _hungarian_: Hungarian names conventionally put the family name first.
- _italian_: Nothing too special.
- _portuguese_: I need to understand Portuguese naming conventions better, but Brazilian
  people sometimes seem to have a lot of names and it is not always clear which ones are
  family names.
- _spanish_: Spanish-speaking people have two family names, one deriving from the
  father's name and the other from the mother's. In practice the second family name is
  often omitted or abbreviated (e.g., "Albuja-V."). There is also variation in whether
  the two names are written as separate words or joined with a hyphen. My preference is
  to join them with a hyphen (e.g., [Elvira Martín-Suárez](/h/44465)), unless a
  different spelling is consistently used for this person. In some cases, the second
  family name is usually omitted; in that case, it may be placed in the _suffix_ field.
  Some Spanish-speaking people have a particle like "de" before their family name. For
  some, like [Miguel de Cervantes](/h/71522), this particle is usually omitted when
  referring to the person by their family name ("Cervantes"); for others, like
  [Luis de la Torre](/h/73188), the particle is included when citing the family name
  ("de la Torre").
- _turkish_: Turkish names are separated because forms of the letter I are capitalized
  differently: I ı forms a separate pair from İ i.
- _vietnamese_: A Vietnamese name commonly has the native order family name, middle name
  or names, given name. International publications also commonly use the order given
  name, middle name or names, family name. Store these components according to their
  meaning: the inherited family name in _family_name_, the personal given name in
  _given_names_, and the middle name or names in _tussenvoegsel_. For example, Nguyễn
  Trường Sơn is stored with `family_name="Nguyễn"`, `tussenvoegsel="Trường"`, and
  `given_names="Sơn"`; the ordinary Western-order display is "Sơn Trường Nguyễn", and
  the family-first bibliographic display is "Nguyễn, Sơn Trường". Taxonomic authors are
  commonly cited by their given name, so this person's taxonomic authority is "Sơn" and
  a citation with initials uses "Nguyễn, S.T.". A compound inherited family name stays
  together in _family_name_; it is not split merely because it contains a space.
  Preserve a different native-order or publication form as a name alias when references
  need to retain that exact displayed identity. Do not infer omitted diacritics or the
  boundary between middle and given names without evidence from the person's
  publications, ORCID profile, institutional page, or another reliable source.

Non-Latin writing systems:

- _pinyin_: Chinese names transliterated using Hanyu Pinyin. If the given name consists
  of two syllables, a hyphen is used to join them and the second syllable is written in
  lowercase (example [Yang Zhong-jian](/h/47669)). The hyphen is the canonical stored
  form because it preserves the syllable boundary: a renderer can always produce
  "Zhongjian" or the initials "Z.-j." from "Zhong-jian", but it cannot reliably recover
  the boundary from "Zhongjian". The joined form is more common in general-purpose
  display and may be used there without changing the stored name. This distinction can
  occasionally matter (as with [Ji Shu-an](/h/48904)). The Chinese surname 吕 Lü is
  sometimes transliterated as "Lv" or "Lyu" when it is difficult to use the umlaut.
  These names should be normalized to the standard pinyin "Lü". Pinyin names are
  displayed in family-name-first order, without a comma; the explicit bibliographic
  family-first form uses a comma.
- _chinese_: Chinese name transliterated using a system other than Pinyin. This usually
  involves people from places like Taiwan, Hongkong, or Malaysia. In these names, the
  second part of a compound name following a hyphen is capitalized.
- _korean_.
- _japanese_.
- _burmese_: A Burmese personal name generally does not contain an inherited family
  name, so store the complete attested personal name as one atomic value in
  _family_name_ and leave _given_names_, _initials_, _tussenvoegsel_, and _suffix_
  empty. Preserve its attested word order, capitalization, and use of spaces or hyphens;
  do not treat the final word as a Western surname or normalize hyphens to spaces.
  Honorifics such as "U" and "Daw" are not part of the canonical personal name. Remove
  them when the evidence establishes that they are titles, retaining the exact
  publication form as an alias when useful. Do not mechanically remove elements such as
  "Sai", "Saw", or "Naw", which may be integral to a person's name or identity.
  Bibliographic matching should tolerate external services splitting the final word into
  a family-name field, but that service-side split is not evidence for changing the
  Person record.
- _russian_: Russian names may be entered in the database in either Cyrillic or the
  Latin alphabet, but Cyrillic is preferred. If written in the Latin alphabet, initials
  may contain multiple letters if they reflect Cyrillic letters that cannot be
  transliterated to Latin one-to-one (e.g., "Yu" for Ю).
- _ukrainian_: Ukrainian names may be entered in Cyrillic or the Latin alphabet, but
  Cyrillic is preferred. Derived Latin forms use
  [Ukraine's official national romanization system](https://zakon.rada.gov.ua/laws/show/55-2010-%D0%BF?lang=en#Text)
  (Cabinet of Ministers Resolution No. 55), not Russian BGN/PCGN romanization. In
  particular, Ukrainian Г becomes _H_, Ґ becomes _G_, and И becomes _Y_; several letters
  have different forms at the beginning of a word. A `TransliteratedFamilyName` tag
  records an attested or preferred Latin family name and overrides the mechanically
  derived form.

A catch-all:

- _other_: Used for names in rare languages with unusual requirements. Names using this
  convention are exempt from most checks that restrict what characters may appear in a
  name.

## Which name to use

Generally, use the most precise name possible, so "Jelle Sjoerd Zijlstra" instead of
variants like "Jelle Zijlstra", "Jelle S. Zijlstra", "J.S. Zijlstra". Generally use full
names over abbreviations ("Michael" instead of "Mike") and use diacritic marks if they
are present in the person's native language, even if they are sometimes dropped in
source material. However, if the person normally uses an abbreviated or simplified name,
use it.

Sometimes people use different names over the course of their career:

- [Yang Zhong-jian](/h/47669), a Chinese paleontologist, started his long career before
  Pinyin became the standard transliteration system for Chinese, and he was long known
  as "C.C. Young". For consistency with other mainland Chinese people, I use the modern
  transliteration "Yang Zhong-jian" for him.
- [Gudrun Daxner-Höck](/h/9135) published a few papers as "Gudrun Daxner" before her
  marriage to [Volker Höck](/h/25245). She is better known by her married name, so that
  is the name used in the database.
- [The 5th Earl of Cranbrook](/h/35739) was known as "Lord Medway" before he inherited
  [his father's](/h/36015) peerage; now he is usually credited as "Earl of Cranbrook".
  In this database, he is currently listed under his personal name, Gathorne
  Gathorne-Hardy. This is unfortunate because this name is virtually never used in the
  scientific literature, but it is more consistent (Gathorne-Hardy is, after all, his
  family name) and helps prevent confusion with his father, the 4th Earl of Cranbrook,
  who as an explorer also makes several appearances in the database.

Many other variations appear for names from different cultures, as already touched upon
under "Naming conventions" above. Sometimes diacritics are dropped, or second and later
given names omitted. In the database the general goal is to use names as they are
written in the person's native language, with all diacritics included. Similarly, all
given names should generally be included, although occasionally some rarely used names
can be omitted, especially for continental European names.

In cases where using a single canonical name could lead to confusion, a _name alias_ can
be used, which points to the canonical name but uses a different form. For example,
Cranbrook's early work was published under the name [Lord Medway](/h/82298), so these
articles are given under that alias.

An initial-only or otherwise ambiguous Person is not automatically an alias of a fuller
name. If publication evidence identifies the existing references to "C. Jones" with
"Craig M. Jones", move those references to the fuller Person but leave "C. Jones" as a
separate non-redirect Person. A future publication using the same abbreviation may refer
to someone else.

Names are currently written in the Latin alphabet in the database, except for a few
Russian names written in Cyrillic. It would perhaps be more consistent with the general
commitment to use names in their native forms to list names in other non-Latin writing
systems, such as Chinese. However, the English-language scientific literature usually
lists names only in their Latin forms, and personally I cannot read or write in writing
systems other than Latin, Cyrillic, and Greek. There is very little taxonomic literature
in Greek, so for now only Latin and Cyrillic are allowed in the database. In the future,
it would be useful to add non-Latin transcriptions of more names.
