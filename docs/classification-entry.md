# Classification entry

A classification entry in a name used in some previous classification. It refers to a
name as it appears in the source; a classification entry like [#31](/ce/31) represents
the information that a source (here, [Orr, 1949](/a/57164)) contains the name _Taxidea
taxus_ on page 53.

Information about classification entries falls in two groups:

- Internal information, which appears in the original source. At its most basic, it is
  the name itself, but a classification entry may also list other nomenclatural
  information that the source provides, such as the author and year.
- External information, which the database provides to put the classification entry in
  context. The most important is the _mapped name_, the [name](/docs/name) in the
  database that corresponds to the classification entry.

## Scope

Classifications are a relatively recent addition to the database, and they have not been
added in all places where they would be useful. There is an ongoing effort to expand
classifications to more sources.

## Internal information

Internal information includes the following fields:

- _article_: The [article](/docs/article) that provides the classification entry.
- _name_: The name as it appears in the source.
- _rank_: The taxonomic rank of the name in the source (which may be "unranked" or
  "informal").
- _parent_: The parent classification entry of the name in the source's classification.
- _page_: The page on which the name appears in the source.
- _authority_: The author given for the name in the source.
- _year_: The year of publication given for the name in the source.
- _citation_: The citation provided for the name in the source.
- _type_locality_: The type locality given for the name in the source.

And the following tags:

- _CommentFromSource_: A comment provided for the name in the source, often a brief
  description like "nomen nudum".
- _TextualRank_: If _rank_ is set to "other", this tag holds the rank as given in the
  source.
- _TypeSpecimenData_: Data given about the name's type specimen in the source.
- _OriginalCombination_: The original combination for the name, as given in the source.
- _OriginalPageDescribed_: The page on which the name was coined, according to the
  source.
- _AgeClassCE_: May be used if the name is marked as extinct in a source mostly dealing
  with extant animals.
- _CommonName_: Common name provided by the source.
- _Informal_: This name represents an informal grouping in the source, which does not
  correspond to a name in the database.
- _TreatedAsDubious_: The source treats the name as dubious or otherwise does not
  include it in its main classification. The intended use case is to allow species
  counts.

## External information

External information includes one field:

- _mapped_name_: The [name](/docs/name) that corresponds to this classification entry.
  The database enforces that this field is always set (except for some unusual cases).

And the following tags:

- _CorrectedName_: The name normalized to a standard format, e.g. clearing up diacritics
  and unusual casing. This is used to find the mapped name.
- _CommentFromDatabase_: A comment with some information provided by the database editor
  about this name.
