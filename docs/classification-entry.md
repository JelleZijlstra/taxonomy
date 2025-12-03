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

## Name matching

After a classification has been digitized, we want to connect it to names that are
already in the database, so we can match each name with the taxon it refers to. This is
referred to internally as "mapping": every classification entry is supposed to have a
_mapped name_, a [name](/docs/name) that corresponds to the entry. The matching
procedure is implemented in the `check_missing_mapped_name` function in
[the code](https://github.com/JelleZijlstra/taxonomy/blob/e4bf2630f5b62585ec1d002b598b571c4314941b/taxonomy/db/models/classification_entry/lint.py#L169),
and summarized here.

The procedure aims to automate name matching as much as possible, but leaves difficult
cases for a human to manually resolve. There are four stages: candidate generation,
scoring, decision making, and validation.

Candidate generation aims to produce a list of names that could match the classification
entry. It works differently for names belonging to different groups. For species-group
names, the procedure is most complex. First, we look for exact matches: names with the
same normalized original name as the classification entry, or taxa with the same
currently valid name. If this does not produce any results, we do a broader fuzzy
search. This looks for names with a matching root name (the last element of the binomen
or trinomen) in a genus that shares a species with the classification entry. For
example, if we were to encounter the name "_Vampyressa brockii_", we would search for
names assigned to the genus _Vampyressa_, but also for names in any genera that include
species-group names that were originally placed in _Vampyressa_, such as _Vampyriscus_,
which includes the name _Vampyressa bidens_ as a synonym of one of its species. In
addition to names where the root name matches exactly, we include names with spelling
variations that would be considered homonyms under Article 58 of the ICZN (as well as a
few other categories of similar names). For example, names ending in _-i_ and _-ii_ are
equivalent under Article 58, so for our example "_Vampyressa brockii_", we would accept
a name like "_Vampyriscus brocki_" as a candidate: it is in a genus that shares a
species name with _Vampyressa_, and it has a root name that matches _brockii_, despite
the spelling difference. If this fuzzy match does not produce any candidates, we broaden
the search to sister genera of the genera we were previously using. In this example, the
genus _Vampyressa_ is in the subtribe Vampyressina, so we would consider all other
genera in that subtribe, for instance _Chiroderma_. If there was a species named
"_brocki_" or similar in _Chiroderma_, we would then produce it as a candidate. The
candidate generation step is simpler for other names (genus-group, family-group, and
high-group), largely based on exact matches. However, for family-group names we produce
candidates with different name endings.

Once we have candidates, we score them. The idea is that a perfect match would have the
lowest score, and any point where the candidate differs from what we expect loses
points. The number of points assigned to various situation is arbitrary and based on
what seemed to work best. A number of factors are used, some tied to fairly specific
scenario. For example, names that match the classification entry's name more precisely
are preferred; if the year is given on the classification entry, we prefer names with a
matching year; available names are preferred over unavailable names; names that postdate
the publication of the classification entry are preferred.

The next stage is decision-making. We sort all candidates by score. If there is a single
candidate with the lowest score, we pick it. Otherwise—if there are no candidates, or
multiple candidates are tied—we leave the choice to a human to decide.

Even after a mapped name has been decided, we run validation to make sure that the
choice is correct. For example, we flag many cases where the year of publication of the
mapped name differs from that given for the classification entry. We also check that the
genus matches: for the classification entry, we find the parent entry of genus rank, and
fo the name, we check the _original_parent_ field. The two should map to the same name.
