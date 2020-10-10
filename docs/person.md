# Person

People are only interesting when they do something that affects the names
in the database. They can currently do so in four ways:

- Writing [articles](/docs/article)
- Introducing new [names](/docs/name)
- Collecting type specimens
- Having things named after them

All of these relationships are reflected in the database.

I only added people to the database as a distinct concept relatively
recently (October 2020), and it is difficult to sort out, for example,
all the people named [Smith](/h/Smith) in the database. Therefore,
most people are in an "unchecked" state, where they are aggregated
just by name, and I am not confident that all references actually
point to the same person. For some other names, I have sorted out
the people involved more precisely, and marked some people as
"checked". For example, I cleaned up all references to myself and
aggregated them at [Zijlstra, Jelle Sjoerd](/h/5331).

## Fields

Persons have the following fields:

- _type_: By default this is set to *unchecked*, which means I have not
  manually reviewed the data associated with the person to verify all
  references are to the same person. After I verify a name, I set the
  type to *checked*. I can also alias a name to another name for the
  same person, making it either a *soft redirect* (indicating that
  references should still be verified before pointing them to the target
  person) or a *hard redirect* (all references should be updated to point
  to the target person). Last, a person can be *deleted*. Unchecked persons
  that no longer have any references to them are automatically marked as
  deleted.
- _naming convention_: Different cultures have different conventions for
  forming and treating personal names, and this field lets us indicate
  what convention to use. For example, this may be set to *chinese* or
  *dutch*. Special cases include *organization* (which occasionally are
  listed as the authors of articles or have species named after them),
  and *ancient* for ancient people, who usually only have a single name,
  like [Philip II of Macedon](/h/1).
- _family name_: The person's family name. This field is always set and
  used to find persons, so it's also set for organizations and for
  people who technically do not have a family name, like Philip II.
- _given names_: The person's given names, in full, like "Jelle Sjoerd".
- _initials_: The person's initials, like "J.S.".
- _suffix_: Suffixes like "Jr." and "III".
- _tussenvoegsel_: In Dutch names, the [tussenvoegsel](https://en.wikipedia.org/wiki/Tussenvoegsel)
  consists of words like "de" or "van den" placed between the given and
  family names. The field is also used for some German and French names,
  like "von Meyer", that are not always included in the person's name. I am
  not clear on how exactly such words are supposed to be treated in some
  European languages—perhaps in some cases they should actually be treated
  as part of the family name.
- _birth_: The person's year of birth.
- _death_: The person's year of death.
- _bio_: A short description that helps identify the person, like
  "American paleontologist".
- _tags_: Several references that give more information about the person:
  - _Wiki_: A link to a Wikipedia article
  - _Institution_: Reference to a [collection](/docs/collection) the person
    was or is associated with.
  - _ActiveRegion_: Reference to a [region](/docs/region) that the person
    was or is active in.
  - _Biography_: Reference to an [article](/docs/article) containing
    biographical details about the person, such as an obituary.
