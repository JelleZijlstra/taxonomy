# Name

This document describes the contents of the Name table and its columns.

The Name table is intended to include all available names within the scope of
the database, as well as all unavailable names that are relevant to
nomenclature (e.g., those that have been used in the past as if they were
available). A core concept of the database is the separation between
_names_ and _taxa_. Names are labels regulated by the International Code of
Zoological Nomenclature, and their attributes are (mostly) defined by objective
rules. Taxa are groups of animals recognized by the science of taxonomy and
their limits are subjective in nature.

## Basic data

- _group_: The group (as determined by the Code) under which the name falls.
  The Code regulates the family group (roughly, superfamilies through
  subtribes), genus group (genera and subgenera), and species group (species
  and subspecies). Names above the family group are largely unregulated; in
  this database they are labeled as the _high group_. This group also includes
  unranked taxa interpolated between regulated ranks (for example, the
  unranked taxon [Oryzomyalia](/t/Oryzomyalia) between the subfamily Sigmodontinae and its
  constituent tribes).
- _root name_: The name to be used as a root to form a valid taxon name from
  this name. For high- and genus-group names, this is simply the name. For
  family-group names, it is the stem without any rank ending (e.g., "Sigmodont"
  for [Sigmodontinae](/n/Sigmodontinae)). For species-group names, it is the
  specific name (e.g., "palustris" for [_Oryzomys palustris_](/n/59371)).
- _status_: The taxonomic status of the name. The status may be _valid_ (the
  name is the base name for a valid taxon), _synonym_ (the name denotes a
  synonym of a valid taxon), _dubious_ (the name is treated as a synonym, but
  the status is unclear), _nomen dubium_ (the name is treated as the base name
  of a taxon, but that taxon is not known to represent a biological reality), or
  _species inquirenda_ (there is insufficient information to determine whether
  the name's taxon is biologically real). The "dubious" status is being
  deprecated, mostly in favor of "nomen dubium". The use of the terms "nomen
  dubium" and "species inquirenda" may not match that in other work. For the
  purposes of this database, a nomen dubium is a name that cannot be
  unambiguously allocated to a biologically real taxon, for example because
  its type specimen is lost or uninformative. A species inquirenda is a name
  (not necessarily a species name) that has not been shown to be valid, a
  synonym, or a nomen dubium, but for which I believe there
  is insufficient evidence that it represents a real taxon. Usually, this
  situation arises with names published long ago that have been ignored in
  subsequent studies. For example, Kretzoi (1941) named a species
  [_Gazelloportax andreei_](/n/75172). The name is available, but as far as I know no
  subsequent author has commented on it, even though several authors have
  treated the taxonomy of the group of which it is part. The name cannot be
  treated as a synonym of another name, it is probably not valid (if so,
  some author would have used the name), and it cannot be treated as a nomen
  dubium (taxonomic revision may well show that it is a synonym of some other
  species). The database does not currently have a way to distinguish objective
  from subjective synonyms; this should be changed.
- _taxon_: The [taxon](/docs/taxon) to which the name is allocated.
- _original name_: The form of the name used by the original description. This
  may be an incorrect original spelling. If there are multiple original
  spellings, they should all be listed as separate names.
- _corrected original name_: Like the original name, but without adornments
  like diacritics, subgenus names, and obsolete family-group suffixes.
- _nomenclature status_: The status of the name under the Code, either
  available or unavailable for some specific reason (e.g., the name is a nomen
  nudum; the name was not properly published). This field has not been
  consistently applied to all names, and it may require more detailed treatment
  (for example, not all statuses are mutually exclusive).

## Citation and authority

- _author tags_: Reference to the [person(s)](/docs/person) who created this
  name.
- _original citation_: Reference to the publication (in the [Article](/docs/article) table) in
  which the name was first made available. This is usually set only if I own a
  copy (e.g., a PDF) of the publication.
- _page described_: The place in the original citation where the name was made
  available. Normally this is a page reference, but it may be a reference to a
  plate or figure. Where relevant, the page should be the location of a heading
  like "_Aus bus_, sp. nov.", not necessarily the first mention of the name. If
  there is no such heading, the page should be the first page on which the name
  appears. Special cases:
    - Some articles have multiple paginations (e.g., one for the volume and
      one for the individual work). In such cases, use the higher page number,
      because it is less likely to be ambiguous.
    - Sometimes page numbers are misprinted. In that case, specify the page
      number like "2 [as 1]", where "2" is the true page number that would be
      used if the pagination of the work was correct, and "1" is the page number
      that actually appears on the page.
- _verbatim citation_: A free-form text specifying the original place of
  publication of the name, used when I do not have the full paper (so that
  _original citation_ is not set). This is a place for rough notes that will
  eventually help me find the full citation; the format is not standardized.
- _citation group_: The [citation group](/docs/citation-group) that the original
  citation of the name belongs to. Set if and only if _verbatim citation_ is set.
- _year_: The year in which the name was established. If the exact year is not
  known, this may be of the form "1899-1900". The Code recommends a citation of
  the form '1900 ["1899"]' if the actual year of publication differs from that
  in the work itself. The database does not currently provide a way to handle
  this circumstance, but it should be handled as a property of the publication,
  not of the name. The Code also stipulates citation as "1940 (1870)" if a
  family-group name takes the priority of an earlier name under certain
  circumstances; this is also not currently supported by the database.

## Gender and stem

- _name complex_ and _species name complex_: A group of names of the same
  derivation. Used to help
  determine gender and stem for genus-group names and gender endings for
  species-group names. There are separate name complexes for the genus and
  species groups. Family-group and high-group names do not have name
  complexes. A name complex may encompass names based on a specific Latin
  or Greek root word, or names whose treatment is stipulated by a specific
  article in the Code (for example, "names whose gender is explicitly specified
  as masculine"). See the [name complex](/docs/name-complex) and
  [species name complex](/docs/species-name-complex) documentation for
  more.

## Types

- _type_: For the family and genus group only, a reference to the name treated
  as the type of the name.
- _verbatim type_: Textual description of the type. Should be replaced by data
  in a column with a more specific format (e.g., type or type_specimen).
- _type locality_: Reference to the locality object that encompasses the type
  locality of the name. This tends to be a specific fossil site for fossils and
  a larger political region for extant names.
- _type specimen_: The type specimen of the name. Normally, this should be in
  the form of a standard specimen reference, e.g. "AMNH 108371". Otherwise, it
  should be whatever information is known that will unambiguously indicate the
  specimen involved.
- _collection_: Reference to the collection in which the type specimen is
  located (in the [Collection](/docs/collection) table).
- _genus type kind_: For genus-group taxa, the way the type species was
  designated (original designation, original monotypy, tautonymy, Linnaean
  tautonymy, subsequent designation).
- _species type kind_: For species-group taxa, the kind of type specimen
  (holotype, syntypes, lectotype, neotype).

## Tags

There two fields that contain lists of tags that record various pieces of
information about the names. The _type tags_ field mostly records additional
information about the type specimen; the _tags_ field mostly has data about
the nomenclatural status of the name.

Tags include the following:

- _PreoccupiedBy_: The name is preoccupied by another name.
- _UnjustifiedEmendationOf_: The name is an unjustified emendation of another
  name.
- _IncorrectSubsequentSpellingOf_: The name is an incorrect subsequent spelling
  of another name.
- _NomenNovumFor_: The name is a nomen novum (replacement name) for another
  name. In modern-day nomenclature a nomen novum is usually created only if
  the earlier name is preoccupied, but in the past some authors replaced
  names for flimsier reasons.
- _VariantOf_: The name is either an unjustified emendation or an incorrect
  subsequent spelling of another name, but it is not clear which. The
  difference between the two hinges on whether the change in spelling was
  intentional or accidental, and without seeing the original description
  it is often not possible to figure this out. Marking such names as
  "variants" helps me because it signals that the name does not require its
  own type locality and similar data, regardless of what its precise status
  turns out to be.
- _PartiallySuppressedBy_: The name was suppressed by the Commission for
  purposes of priority but not homonymy.
- _FullySuppressedBy_: The name was suppressed by the Commission for purposes
  of both priority and homonymy.
- _TakesPriorityOf_: This name takes the priority of another name, a situation
  that the Code sometimes calls for in family-group names.
- _NomenOblitum_: The name has been formally identified as a _nomen oblitum_
  (forgotten name), relative to another name.
- _MandatoryChangeOf_: The name is a mandatory change (e.g., a gender correction)
  of another name. Mandatory changes are usually not covered in the database.
- _Conserved_: The name was placed on an Official List of names in zoology.
- _IncorrectOriginalSpellingOf_: The name is an incorrect original spelling of
  another name. See the "Justified emendations" section below for related
  discussion.
- _SelectionOfSpelling_: For names with multiple original spellings, a reference
  to the author who formally selected the correct original spelling.
- _SubsequentUsageOf_: The name is a subsequent usage of another name, without
  its own availability. Such names are included in the database if they are
  listed as synonyms in previous compilations.
- _SelectionOfPriority_: The name was selected to have priority over another,
  simultaneously published name.
- _ReversalOfPriority_: The Commission reversed the priority of this name
  relative to another.
- _Rejected_: The name was placed on one of the Official Indices by the Commission,
  without being explicitly suppressed.
- _JustifiedEmendationOf_: The name is a justified emendation of another name.
  See below for more detail on how justified emendations are treated.

Type tags are more commonly used; ideally every species-group name and many
genus-group names should have at least one. They fall into several groups:

- Sourced quotations
  - _SpecimenDetail_: Sourced quotation with information about the material on
    which a species was based. Every type specimen should be supported by a
    _SpecimenDetail_ field that confirms the identity of the type specimen.
  - _LocationDetail_: Sourced quotation with information about the type locality.
    Every type locality should be supported by a _LocationDetail_ field that
    confirms the placement of the type locality.
  - _CollectionDetail_: Sourced quotation with information about the
    [collection](/docs/collection) that the type material is located in. This
    may include the explanation of an obscure abbreviation.
  - _CitationDetail_: Sourced quotation of the original citation of a name.
    Usually this should just go into the "verbatim citation" field, but an
    explicitly referenced _CitationDetail_ tag is useful if the citation is
    obscure or controversial.
  - _DefinitionDetail_: Sourced quotation of a phylogenetic definition of the
    name.
  - _EtymologyDetail_: Sourced quotation about the origin or grammatical treatment
    of the name. This may support the [name complex](/docs/name-complex) or
    [species name complex](/docs/species-name-complex) that the name is assigned
    to, but an _EtymologyDetail_ tag is not essential for assigning a name to a
    complex, because many original descriptions do not specify an etymology.
- Structured information about the type
  - _CollectedBy_: The [person](/docs/person) who collected the type specimen.
  - _Involved_: A [person](/docs/person) who was involved
    in the history of the type material, but did not
    collect it. This can include the preparator, the
    sponsor of the collector, or the owner of the
    specimen.
  - _Collector_: Similar to _CollectedBy_, but as a string. This tag is being phased
    out.
  - _Date_: Date when the type specimen was collected. Ideally this should be the
    day, but it can be a month or year if no more precise data is available. If no collection date
    is known, but a subsequent date is (e.g., the date
    the specimen was registered in a collection), that
    date should be used with "<" prefixed to it (e.g.,
    "<1893" or "<7 December 1893"). If the specimen was
    kept in captivity, the date should be the date it
    was captured.
  - _Gender_: The biological gender of the type specimen.
  - _Age_: The approximate age of the type specimen, such as "adult", "subadult",
    or "juvenile".
  - _Organ_: A preserved part of the specimen, such as "skin", "skull", or "tooth".
    There may be (and often are) multiple _Organ_ tags. The tag has fields for detail
    (such as tooth position) and condition of the specimen.
  - _Altitude_: Altitude, in meters or feet, at which the type specimen was
    collected.
  - _Coordinates_: Geographical coordinates (latitude and longitude) at which the
    type specimen was collected.
  - _Repository_: Reference to a [collection](/docs/collection) that holds some
    of the type material. Should be used if and only if the "collection" field
    is set to the special [multiple](/c/multiple) collection. This usually
    appears when the species has syntypes, but there are a few cases in which
    a holotype specimen is distributed among several collections.
  - _ProbableRepository_: Used if there is some evidence that the type material
    may be in a particular collection, but no clear statement in the sources. For
    example, the description may be an author who usually worked with material from
    a particular collection. The intended use of this tag is that it can help
    give clues to researchers looking for the type material and help generate lists
    of possible type material for those compiling catalogues of particular
    collections.
- Nomenclatural actions
  - _TypeDesignation_: The designation of the type species of a genus-group name.
    Includes references to the source and to the designated type species.
  - _CommissionTypeDesignation_: Like _TypeDesignation_, but the designation is by
    the Commission and therefore overrides any other designation.
  - _LectotypeDesignation_: Designation of a specimen as the lectotype of a
    species-group name.
  - _NeotypeDesignation_: Designation of a specimen as the neotype of a
    species-group name.
- Miscellaneous fields
  - _NamedAfter_: Reference to the [person](/docs/person) this name was named
    after. This field was added recently, so it is not yet used in all cases
    where it should be used.
  - _IncludedSpecies_: For genus-group names, reference to a species-group name that
    was one of the originally included species in the genus. Usually this is given
    only for species without an originally designated type, in which case the
    included species are the species eligible for designation as the type.
  - _GenusCoelebs_: Indicates that a genus-group name was originally proposed
    without any included species.
  - _TypeLocality_, _StratigraphyDetail_, _Habitat_: Deprecated tags with detail
    about the type locality; _LocationDetail_ should be used instead.
  - _Host_: Name of the type host of a symbiont.

## Miscellaneous data

- _data_: Dictionary in JSON form with miscellaneous data. This is mostly data
  from previous versions of this database that is now no longer relevant, such
  as vernacular Dutch and English names.
- _definition_: Phylogenetic definition of the name, where applicable.

## Justified emendations

Although the Code generally mandates that original spellings are to be
maintained, on rare occasions a spelling other than the original is valid (see
Arts. 32.5 and 33.2.3.1). In such cases, three names should be entered into the
database:

- The name as corrected, with its original author and date and with `nomenclature_status`
  set to `as_emended`. The page_described field should be set to the page where the
  original spelling was used. This name should have other standard nomenclatural
  information, like the type locality and type specimen. If its `nomenclature_status`
  cannot be `as_emended` (for example because it is a `nomen_novum`), the _AsEmended_
  tag should be added instead, pointing to the justified emendation name.
- The name as originally spelled, with its original author and date, but no other
  data (such as `type_tags`). It should have `nomenclature_status` set to
  `incorrect_original_spelling` and an _IncorrectOriginalSpellingOf_ tag pointing to
  the emended name.
- The name as corrected, with the author and date who first performed the correction.
  This name should have `nomenclature_status` set to `justified_emendation` and a
  _JustifiedEmendationOf_ tag pointing to the original spelling.

Emendations under Arts. 32.5.2 (removal of diacritics and other marks) and 32.5.3
(correction of family-group names) are generally unambiguous and mechanical, so
the database will usually make these corrections silently. If desired, a name
with status `justified_emendation` can be entered with as its authority the first
author to use the corrected name.

## Mandatory changes

There are two categories of mandatory changes (Art. 34):

- For family-group names, the ending must be changed if the rank changes.
- For species-group names, the specific name must be changed to agree in gender
  with the generic name.

These can both be expressed with the _MandatoryChangeOf_ tag in the database, but
this is rarely necessary.
