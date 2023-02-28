# Person

People are only interesting when they do something that affects the names in the
database. They can currently do so in four ways:

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
  person. After I verify a name, I set the type to _checked_. I can also alias a name to
  another name for the same person, making it either a _soft redirect_ (indicating that
  references should still be verified before pointing them to the target person) or a
  _hard redirect_ (all references should be updated to point to the target person).
  Last, a person can be _deleted_. Unchecked persons that no longer have any references
  to them are automatically marked as deleted.
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
- _tussenvoegsel_: In Dutch names, the
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
  languages. This should perhaps be split up further; for example, Brazilians sometimes
  have long lists of names where it is not clear which names are parts of the family
  name and which are given names.
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
- _spanish_: Spanish-speaking people have two family names, one deriving from the
  father's name and the other from the mother's. In practice the second family name is
  often omitted or abbreviated (e.g., "Albuja-V."). There is also variation in whether
  the two names are written as separate words or joined with a hyphen. My preference is
  to join them with a hyphen (e.g., [Elvira Martín-Suárez](/h/44465)), unless a
  different spelling is consistently used for this person.
- _german_: German names may use particles like "von".
- _hungarian_: Hungarian names conventionally put the family name first.
- _turkish_: Turkish names are separated because forms of the letter I are capitalized
  differently: I ı forms a separate pair from İ i.
- _vietnamese_: Vietnamese names also write the family name first. Unlike most other
  East Asian-style names, Vietnamese names usually also include a middle name. Written
  Vietnamese uses a large set of unusual diacritics, but these seem to be usually
  omitted when Vietnamese scientists are listed as authors. Modes of citation for
  Vietnamese names vary; for example, [Nguyen Truong Son](/h/43537) is often cited as
  "Son", but also as "S.T. Nguyen". In the database, the family name field should
  contain the family name, and the given names field should contain the given name
  followed by the middle name. Note that this reverses the normal name order in
  Vietnamese.

Non-Latin writing systems:

- _pinyin_: Chinese names transliterated using Hanyu Pinyin. If the given name consists
  of two syllables, a hyphen is used to join them and the second syllable is written in
  lowercase (example [Yang Zhong-jian](/h/47669)). It is more common to join the two
  syllables together without a hyphen, but this can occasionally lead to ambiguity (as
  with [Ji Shu-an](/h/48904)), and it is easier to remove the hyphen when it is not
  desired than to add it when it is.
- _chinese_: Chinese name transliterated using a system other than Pinyin. This usually
  involves people from places like Taiwan, Hongkong, or Malaysia. In these names, the
  second part of a compound name following a hyphen is capitalized.
- _korean_.
- _japanese_.
- _burmese_: Burmese names appear to consist of two to four single-syllable portions,
  which are either written as separate words or joined with hyphens. For Burmese names,
  I currently put the whole name in the family name, written as separate words and not
  with hyphens.
- _russian_: Russian names may be entered in the database in either Cyrillic or the
  Latin alphabet. If written in the Latin alphabet, initials may contain multiple
  letters if they reflect Cyrillic letters that cannot be transliterated to Latin
  one-to-one (e.g., "Yu" for Ю).
- _ukrainian_: The Ukrainian language uses a slightly different alphabet than Russian.

A catch-all:

- _other_: Used for names in rare languages with unusual requirements. Names using this
  convention are exempt from most checks that restrict what characters may appear in a
  name.

## Which name to use

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

Names are currently written in the Latin alphabet in the database, except for a few
Russian names written in Cyrillic. It would perhaps be more consistent with the general
commitment to use names in their native forms to list names in other non-Latin writing
systems, such as Chinese. However, the English-language scientific literature usually
lists names only in their Latin forms, and personally I cannot read or write in writing
systems other than Latin, Cyrillic, and Greek. There is very little taxonomic literature
in Greek, so for now only Latin and Cyrillic are allowed in the database. In the future,
it would be useful to add non-Latin transcriptions of more names.
