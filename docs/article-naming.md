# Article filename naming convention

Article filenames are short, structured descriptions of their contents. The parser in
[`name_parser.py`](../taxonomy/db/models/article/name_parser.py) recognizes the forms
below. Treat a filename as valid only if `NameParser` reports no errors.

## Overall form

```text
<core> [(<authorship>)] [(<free modifier>)][.<extension>]
```

- The extension, when present, consists of lowercase ASCII letters (normally `pdf`).
- At most two trailing parenthesized modifiers are allowed.
- A modifier ending in a four-digit year is authorship. Accepted forms include `(2016)`,
  `(Verzi 2016)`, `(Verzi & Montalvo 2016)`, and `(Verzi et al. 2016)`.
- One arbitrary, non-nested free modifier may follow the authorship, for example
  `(supplement)`, `(plates)`, or `(2-1)`. When both are present, authorship must come
  first: `(Smith 2020) (supplement)`. When both the final version of an Article and its
  preliminary PDF are present, either use the modifier `(in press)` for the latter
  (preferred) or `(final)` for the former (if the unmodified name is taken).
- Article lint additionally restricts names to printable ASCII. Electronic Articles must
  have an extension; non-electronic Articles (those without a backing file) must not.

## Normal core

Most filenames use this left-to-right form:

```text
[<name list>] [<geography>] [<time>] [-<topic list>]
```

Examples:

```text
Rhagomys longilingua Peru.pdf
Micromammalia Slovenia Pleistocene.pdf
Chiroptera, Lipotyphla, Marsupialia Austria Korneuburg Miocene.pdf
Gomphotheriidae Mongolia Valley of Lakes E-M Miocene.pdf
Animalia-index (Sherborn 1922) (2-1).pdf
```

### Scientific names

A name has this general shape:

```text
[Cf. |Aff. ]Genus [(Subgenus)] [[cf. |aff. ]species [subspecies]]
[ Division| group| complex]
```

The genus and subgenus start with a capital; species-group epithets are lowercase.
Question marks are allowed in the genus and species-group words. A name may instead be a
letters-and-spaces phrase ending in ` virus`.

Separate multiple names with comma-space. An all-lowercase item inherits the genus of
the preceding name:

```text
Murina beelzebub, cinerea, walstoni
```

This expands conceptually to `Murina beelzebub`, `Murina cinerea`, and
`Murina walstoni`.

### Geography

Geography has the form:

```text
[<compass modifier> ]<major area>[ <specific locality>]
```

Major areas must exactly match an entry in
[`geography.txt`](../taxonomy/db/models/article/parserdata/geography.txt). Compass
modifiers must come from
[`geography_modifiers.txt`](../taxonomy/db/models/article/parserdata/geography_modifiers.txt)
(for example, `N`, `SW`, or `C`). Text after the major area and before the time or topic
is the more specific locality, such as `Mongolia Valley of Lakes`.

Comma-space separates multiple geographic entries. If a later entry has no recognized
major area, it inherits the preceding one.

### Time

A time unit is a period, optionally preceded by a modifier. Use the exact vocabulary in
[`periods.txt`](../taxonomy/db/models/article/parserdata/periods.txt) and
[`period_modifiers.txt`](../taxonomy/db/models/article/parserdata/period_modifiers.txt).
Hyphens express ranges and comma-space separates multiple units:

```text
Pleistocene
E Miocene
E-M Miocene
Eocene-Oligocene
MN7-8
```

In a shared-period range such as `E-M Miocene`, the period applies to both modifiers.

### Topics

A hyphen introduces the topic section; comma-space separates multiple topics:

```text
Mammalia-review
Rodentia-types, bibliography
```

Topic text is otherwise free-form. The parser specifically permits a nonstandard leading
name when a topic contains one of: `review`, `types`, `biography`, `obituary`,
`bibliography`, `catalog`, `catalogue`, `festschrift`, `publication`, `meeting`, or
`collection`.

## Special cores

The parser also recognizes these forms:

| Purpose                     | Form                                              | Example                                        |
| --------------------------- | ------------------------------------------------- | ---------------------------------------------- |
| New taxa                    | `<name list> nov`                                 | `Agathaeromys nov.pdf`                         |
| Counted new taxa            | `<group> <number>nov`                             | `Oryzomyini 10nov.pdf`                         |
| New taxa plus context       | `<nov phrase>, <normal core>`                     | `Agathaeromys nov, Rodentia Spain Miocene.pdf` |
| Replacement name            | `<replacement> for <preoccupied epithet or name>` | `Neurotrichus skoczeni for minor.pdf`          |
| _Mammalian Species_ account | `MS <name list>`                                  | `MS Peromyscus maniculatus.pdf`                |
| Complete journal issue      | `<journal> <volume-or-issue>`                     | `Lemur News 12.pdf`                            |

For a list of new species, later lowercase items inherit the first genus, as in
`Murina beelzebub, cinerea, walstoni nov.pdf`. A complete-issue number may have an
immediately adjacent parenthesized subnumber, for example `Lemur News 12(3).pdf`.

One parasite-specific normal form is deliberately not interpreted as a new-taxon phrase
even though it ends in `nov`:

```text
Oryzomys palustris-Hoplopleura oryzomydis nov.pdf
```

## Validation

Do not rely on visual inspection alone. Validate the final filename with the same parser
used by Article lint:

```python
from taxonomy.db.models.article.name_parser import get_name_parser

parser = get_name_parser(filename)
assert not parser.get_errors(), parser.get_errors()
```

The parser is the authority on syntax. It does not determine whether the chosen taxa,
places, periods, topics, authorship, or modifier accurately describe the source.
