# Name

This document describes the contents of the Name table and its columns.

The Name table is intended to include all available names within the scope of the
database, as well as all unavailable names that are relevant to nomenclature (e.g.,
those that have been used in the past as if they were available). A core concept of the
database is the separation between _names_ and _taxa_. Names are labels regulated by the
International Code of Zoological Nomenclature, and their attributes are (mostly) defined
by objective rules. Taxa are groups of animals recognized by the science of taxonomy and
their limits are subjective in nature.

## Basic data

- _group_: The group (as determined by the Code) under which the name falls. The Code
  regulates the family group (roughly, superfamilies through subtribes), genus group
  (genera and subgenera), and species group (species and subspecies). Names above the
  family group are largely unregulated; in this database they are labeled as the _high
  group_. This group also includes unranked taxa interpolated between regulated ranks
  (for example, the unranked taxon [Oryzomyalia](/t/Oryzomyalia) between the subfamily
  Sigmodontinae and its constituent tribes).
- _root name_: The name to be used as a root to form a valid taxon name from this name.
  For high- and genus-group names, this is simply the name. For family-group names, it
  is the stem without any rank ending (e.g., "Sigmodont" for
  [Sigmodontinae](/n/Sigmodontinae)). For species-group names, it is the specific name
  (e.g., "palustris" for [_Oryzomys palustris_](/n/59371)).
- _status_: The taxonomic status of the name. The status may be _valid_ (the name is the
  base name for a valid taxon), _synonym_ (the name denotes a synonym of a valid taxon),
  _dubious_ (the name is treated as a synonym, but the status is unclear), _nomen
  dubium_ (the name is treated as the base name of a taxon, but that taxon is not known
  to represent a biological reality), or _species inquirenda_ (there is insufficient
  information to determine whether the name's taxon is biologically real). The "dubious"
  status is being deprecated, mostly in favor of "nomen dubium". The use of the terms
  "nomen dubium" and "species inquirenda" may not match that in other work. For the
  purposes of this database, a nomen dubium is a name that cannot be unambiguously
  allocated to a biologically real taxon, for example because its type specimen is lost
  or uninformative. A species inquirenda is a name (not necessarily a species name) that
  has not been shown to be valid, a synonym, or a nomen dubium, but for which I believe
  there is insufficient evidence that it represents a real taxon. Usually, this
  situation arises with names published long ago that have been ignored in subsequent
  studies. For example, Kretzoi (1941) named a species
  [_Gazelloportax andreei_](/n/75172). The name is available, but as far as I know no
  subsequent author has commented on it, even though several authors have treated the
  taxonomy of the group of which it is part. The name cannot be treated as a synonym of
  another name, it is probably not valid (if so, some author would have used the name),
  and it cannot be treated as a nomen dubium (taxonomic revision may well show that it
  is a synonym of some other species). The database does not currently have a way to
  distinguish objective from subjective synonyms; this should be changed.
- _taxon_: The [taxon](/docs/taxon) to which the name is allocated.
- _original name_: The form of the name used by the original description. This may be an
  incorrect original spelling. If there are multiple original spellings, they should all
  be listed as separate names.
- _corrected original name_: Like the original name, but without adornments like
  diacritics, subgenus names, and obsolete family-group suffixes.
- _nomenclature status_: The status of the name under the Code, either available or
  unavailable for some specific reason (e.g., the name is a nomen nudum; the name was
  not properly published). This field has not been consistently applied to all names,
  and it may require more detailed treatment (for example, not all statuses are mutually
  exclusive).

## Citation and authority

- _author tags_: Reference to the [person(s)](/docs/person) who created this name.
- _original citation_: Reference to the publication (in the [Article](/docs/article)
  table) in which the name was first made available. This is usually set only if I own a
  copy (e.g., a PDF) of the publication.
- _page described_: The place in the original citation where the name was made
  available. See below for more detail.
- _verbatim citation_: A free-form text specifying the original place of publication of
  the name, used when I do not have the full paper (so that _original citation_ is not
  set). This is a place for rough notes that will eventually help me find the full
  citation; the format is not standardized.
- _citation group_: The [citation group](/docs/citation-group) that the original
  citation of the name belongs to. Set if and only if _verbatim citation_ is set.
- _year_: The year in which the name was established. If the exact year is not known,
  this may be of the form "1899-1900". The Code recommends a citation of the form '1900
  ["1899"]' if the actual year of publication differs from that in the work itself. The
  database does not currently provide a way to handle this circumstance, but it should
  be handled as a property of the publication, not of the name. The Code also stipulates
  citation as "1940 (1870)" if a family-group name takes the priority of an earlier name
  under certain circumstances; this is also not currently supported by the database.

### _page_described_

The _page_described_ field describes where in the original citation the name was
created. It serves two purposes:

- Make it easy to verify data about the name by reading the original description and
  locating the correct page. In particular, it should be easy to find the information
  that makes the name available for nomenclatural purposes.
- Enable automated tools to consume the data, for example by verifying that the page
  number is within the page range of the citation, or potentially by linking directly to
  the right page on websites like Biodiversity Heritage Library.

In simple cases, the original citation is an article that starts at say page 100 and
ends at page 110, and on page 105 there is a heading "_Aus bus_, sp. nov.". In that
case, the _page_described_ field should be "105".

Unfortunately, there are many complications. Here are guidelines for dealing with
various edge cases that I have encountered:

- If there is no header that unambiguously introduces the description of the new name,
  use the place where it is most prominently discussed. This is not necessarily the
  first occurrence of the name, as that may be a passing mention in an abstract or
  figure legend. However, if there are multiple places in the paper that collectively
  satisfy the conditions that make the name available (e.g., the description is far from
  the actual scientific name), multiple page numbers can be used. A range ("105-106")
  can be used if the statement that makes the name available extends over multiple
  printed pages.
- If the name was introduced on a plate, use "pl. N", where N is the plate number in
  Arabic numerals (not Roman numerals even if that is what the work uses).
- If the name was introduced in a footnote, add "(footnote)" after the page number, e.g.
  "105 (footnote)". If the footnotes are numbered, use the numbering, e.g. "105
  (footnote 3)" if the name was introduced in footnote 3. Similarly, if the name was
  introduced in a table or figure, "(figure 1)" or "(table 1)" can be added. However,
  this is not necessary if the figure or table spans the whole page.
- Some works have discontinuous pagination or even no page numbers. In such cases, use
  whatever is needed to make the reference reasonably clear. For example:
  - In short articles, you can just use "unnumbered"
  - For unnumbered pages associated with a plate, a possible approach is "pl. 3
    (unnumbered p. 4)" for the 4th unnumbered page associated with plate 3.
  - If there are multiple discontinuous page sequences in an article, often they can be
    split out into separate [Article](/docs/article) objects, which can carry more
    precise citation data to make them easier to identify.
- Some articles have multiple paginations (e.g., one for the volume and one for the
  individual work). In such cases, use the higher page number, because it is less likely
  to be ambiguous.
- Sometimes page numbers are misprinted. In that case, specify the page number like "2
  [as 1]", where "2" is the true page number that would be used if the pagination of the
  work was correct, and "1" is the page number that actually appears on the page.

If the citation is otherwise hard to find, I often add a comment to the name specifying
the page number in the PDF version of the reference where the name can be found.

The database enforces a consistent format for this field, but only for names for which
the original citation is known. An informal overview of what kind of texts are allowed:

- Any number of pages, separated by commas. Each comma-separated part should be a valid
  page.
- Every page may be followed by arbitrary text enclosed in parentheses.
- Every page may also be followed by "[as N]", where N is a number.
- A page may be a single number, a pair of two separated by a hyphen, or "pl." followed
  by a number.

Some statistics for this field (as of April 13, 2023, after I finished making all names
with original citations follow this format):

- 66849 names with both an original citation and a page described
- 3029 unique values in the _page_described_ field
- The most common value is "2" with 1062 occurrences. The top 10 most common values are
  the numbers from 1 to 10, mostly in order except that "1" is between "4" and "5".
- 65114 values (97.4%) are simple numbers
- Other relatively common categories, some of which may overlap:
  - 71 (0.1%) are Roman numerals (e.g. "xvii")
  - 442 (0.7%) contain a plate number (e.g., "pl. 248C")
  - 441 (0.7%) contain multiple plain numeric page numbers (e.g., "337, 356")
  - 298 (0.4%) contain mention of a footnote (e.g., "59 (footnote)")
  - 64 (0.1%) are simple page ranges (e.g., "64-65")

## Gender and stem

- _name complex_ and _species name complex_: A group of names of the same derivation.
  Used to help determine gender and stem for genus-group names and gender endings for
  species-group names. There are separate name complexes for the genus and species
  groups. Family-group and high-group names do not have name complexes. A name complex
  may encompass names based on a specific Latin or Greek root word, or names whose
  treatment is stipulated by a specific article in the Code (for example, "names whose
  gender is explicitly specified as masculine"). See the
  [name complex](/docs/name-complex) and
  [species name complex](/docs/species-name-complex) documentation for more.

## Types

- _type_: For the family and genus group only, a reference to the name treated as the
  type of the name.
- _verbatim type_: Textual description of the type. Should be replaced by data in a
  column with a more specific format (e.g., type or type_specimen).
- _type locality_: Reference to the locality object that encompasses the type locality
  of the name. This tends to be a specific fossil site for fossils and a larger
  political region for extant names.
- _type specimen_: The type specimen of the name. Normally, this should be in the form
  of a standard specimen reference, e.g. "AMNH 108371". Otherwise, it should be whatever
  information is known that will unambiguously indicate the specimen involved. See below
  for more details on the format.
- _collection_: Reference to the collection in which the type specimen is located (in
  the [Collection](/docs/collection) table).
- _genus type kind_: For genus-group taxa, the way the type species was designated
  (original designation, original monotypy, tautonymy, Linnaean tautonymy, subsequent
  designation).
- _species type kind_: For species-group taxa, the kind of type specimen (holotype,
  syntypes, lectotype, neotype).

### _type_specimen_

The _type_specimen_ field normally contains the catalog number of a type specimen. If
there are multiple type specimens with different numbers, or if parts of a single
specimen are cataloged under different numbers, these are separated with commas (for
example, "USNM 120, MCZ 4759" for two syntypes of _Neotamias dorsalis_).

Each entry in the list is a single specimen, possibly followed by some parenthesized
alternative numbers and comments. Formats for museum catalog numbers vary widely from
one collection to another, and even numbers from the same collection are often presented
differently in different sources. A standardized format should be defined for each
collection, ideally mirroring what the collection itself uses. As of this writing, the
database only defines such a format for a few collections.

Parenthesized phrases after them main catalog number may include:

- Former catalog numbers in parentheses prefixed with "= ". For example, "AMNH 12345 (=
  USNM 54321) (= MCZ 1234)" would indicate that the specimen is currently in the AMNH as
  AMNH 12345, but was previously cataloged as USNM 54321 and MCZ 1234. A field number or
  informal number may be added with quotes, e.g. 'RGM.1332450 (= "Trinil 2")' for the
  type of _Homo erectus_. The repository should be listed in a _FormerRepository_ tag.
- Extra catalog numbers may be added in parentheses prefixed with "+ ". This format is
  used when the type specimen is primarily in one collection, but some secondary
  material is in another. For example, the skin and skull may be in the primary
  collection, but a tissue sample in another. This would be expressed as e.g. "INPA 2550
  (+ MVZ:Mamm:195429)". The repository should be listed in a _ExtraRepository_ tag.
- Future catalog numbers may be added in parentheses prefixed with "=> ". Sometimes new
  species descriptions contain a statement that the type specimen is to be transferred
  to some other institution. Until that transfer actually occurs, the original number
  should be listed as primary, e.g. "MSB:Mamm:12345 (=> CBF 12345)". The repository
  should be listed in a _FutureRepository_ tag.
- A comment in parentheses ending with an exclamation mark, usually for cases where the
  catalog number is not enough to uniquely identify the specimen. For example, if AMNH
  12345 contains a skin and a skull, but only the skin is a type, the entry should read
  "AMNH 12345 (skin!)".

Many collections have different catalogues for different taxonomic groups and for fossil
and extant specimens. In such cases, a unique format should be used, usually with some
collection identifier between the institution code and the number.

The following special forms are always allowed:

- "BMNH (unnumbered)": a specimen in the BMNH that lacks a catalogue number
- "BMNH (no number given)" (or "numbers"): there is a type (or multiple types) in the
  BMNH, but the source does not record the catalog number. This is most frequently
  useful with syntypes, where a source might say that one syntype is ZMB 12345 and
  another syntype is in the BMNH. If there is only a single type, simply set the
  _collection_ field to "BMNH" and leave _type_specimen_ blank.
- "BMNH (lost)": the type specimen used to be in the BMNH but is currently considered
  lost.

## Tags

There are two fields that contain lists of tags that record various pieces of
information about the names. The _type tags_ field mostly records additional information
about the type specimen; the _tags_ field mostly has data about the nomenclatural status
of the name.

Tags include the following:

- _PreoccupiedBy_: The name is preoccupied by another name.
- _UnjustifiedEmendationOf_: The name is an unjustified emendation of another name.
- _IncorrectSubsequentSpellingOf_: The name is an incorrect subsequent spelling of
  another name.
- _NomenNovumFor_: The name is a nomen novum (replacement name) for another name. In
  modern-day nomenclature a nomen novum is usually created only if the earlier name is
  preoccupied, but in the past some authors replaced names for flimsier reasons.
- _VariantOf_: The name is either an unjustified emendation or an incorrect subsequent
  spelling of another name, but it is not clear which. The difference between the two
  hinges on whether the change in spelling was intentional or accidental, and without
  seeing the original description it is often not possible to figure this out. Marking
  such names as "variants" helps me because it signals that the name does not require
  its own type locality and similar data, regardless of what its precise status turns
  out to be.
- _PartiallySuppressedBy_: The name was suppressed by the Commission for purposes of
  priority but not homonymy.
- _FullySuppressedBy_: The name was suppressed by the Commission for purposes of both
  priority and homonymy.
- _TakesPriorityOf_: This name takes the priority of another name, a situation that the
  Code sometimes calls for in family-group names.
- _NomenOblitum_: The name has been formally identified as a _nomen oblitum_ (forgotten
  name), relative to another name.
- _MandatoryChangeOf_: The name is a mandatory change (e.g., a gender correction) of
  another name. Mandatory changes are usually not covered in the database.
- _Conserved_: The name was placed on an Official List of names in zoology.
- _IncorrectOriginalSpellingOf_: The name is an incorrect original spelling of another
  name. See the "Justified emendations" section below for related discussion.
- _SelectionOfSpelling_: For names with multiple original spellings, a reference to the
  author who formally selected the correct original spelling.
- _SubsequentUsageOf_: The name is a subsequent usage of another name, without its own
  availability. Such names are included in the database if they are listed as synonyms
  in previous compilations, usually if it represents a misidentification.
- _NameCombinationOf_: The name is a new name combination (e.g., reassignment to a
  different genus) of a previous name. Name combinations are currently only rarely
  listed.
- _SelectionOfPriority_: The name was selected to have priority over another,
  simultaneously published name.
- _ReversalOfPriority_: The Commission reversed the priority of this name relative to
  another.
- _Rejected_: The name was placed on one of the Official Indices by the Commission,
  without being explicitly suppressed.
- _JustifiedEmendationOf_: The name is a justified emendation of another name. See below
  for more detail on how justified emendations are treated.

Type tags are more commonly used; ideally every species-group name and many genus-group
names should have at least one. They fall into several groups:

- Sourced quotations
  - _SpecimenDetail_: Sourced quotation with information about the material on which a
    species was based. Every type specimen should be supported by a _SpecimenDetail_
    field that confirms the identity of the type specimen.
  - _LocationDetail_: Sourced quotation with information about the type locality. Every
    type locality should be supported by a _LocationDetail_ field that confirms the
    placement of the type locality.
  - _CollectionDetail_: Sourced quotation with information about the
    [collection](/docs/collection) that the type material is located in. This may
    include the explanation of an obscure abbreviation.
  - _CitationDetail_: Sourced quotation of the original citation of a name. Usually this
    should just go into the "verbatim citation" field, but an explicitly referenced
    _CitationDetail_ tag is useful if the citation is obscure or controversial.
  - _DefinitionDetail_: Sourced quotation of a phylogenetic definition of the name.
  - _EtymologyDetail_: Sourced quotation about the origin or grammatical treatment of
    the name. This may support the [name complex](/docs/name-complex) or
    [species name complex](/docs/species-name-complex) that the name is assigned to, but
    an _EtymologyDetail_ tag is not essential for assigning a name to a complex, because
    many original descriptions do not specify an etymology.
- Structured information about the type
  - _CollectedBy_: The [person](/docs/person) who collected the type specimen.
  - _Involved_: A [person](/docs/person) who was involved in the history of the type
    material, but did not collect it. This can include the preparator, the sponsor of
    the collector, or the owner of the specimen.
  - _Collector_: Similar to _CollectedBy_, but as a string. This tag is being phased
    out.
  - _Date_: Date when the type specimen was collected. Ideally this should be the day,
    but it can be a month or year if no more precise data is available. If no collection
    date is known, but a subsequent date is (e.g., the date the specimen was registered
    in a collection), that date should be used with "<" prefixed to it (e.g., "<1893" or
    "<7 December 1893"). If the specimen was kept in captivity, the date should be the
    date it was captured.
  - _Gender_: The biological gender of the type specimen.
  - _Age_: The approximate age of the type specimen, such as "adult", "subadult", or
    "juvenile".
  - _Organ_: A preserved part of the specimen, such as "skin", "skull", or "tooth".
    There may be (and often are) multiple _Organ_ tags. The tag has fields for detail
    (such as tooth position) and comments about the specimen. See below for more detail
    on how _Organ_ tags work.
  - _Altitude_: Altitude, in meters or feet, at which the type specimen was collected.
  - _Coordinates_: Geographical coordinates (latitude and longitude) at which the type
    specimen was collected.
  - _Repository_: Reference to a [collection](/docs/collection) that holds some of the
    type material. Should be used if and only if the "collection" field is set to the
    special [multiple](/c/multiple) collection. This usually appears when the species
    has syntypes, but there are a few cases in which a holotype specimen is distributed
    among several collections.
  - _ProbableRepository_: Used if there is some evidence that the type material may be
    in a particular collection, but no clear statement in the sources. For example, the
    description may be an author who usually worked with material from a particular
    collection. The intended use of this tag is that it can help give clues to
    researchers looking for the type material and help generate lists of possible type
    material for those compiling catalogues of particular collections.
  - _FormerRepository_, _ExtraRepository_, and _FutureRepository_: Used for several
    cases where type material is associated with multiple collections; see above.
- Nomenclatural actions
  - _TypeDesignation_: The designation of the type species of a genus-group name.
    Includes references to the source and to the designated type species.
  - _CommissionTypeDesignation_: Like _TypeDesignation_, but the designation is by the
    Commission and therefore overrides any other designation.
  - _LectotypeDesignation_: Designation of a specimen as the lectotype of a
    species-group name.
  - _NeotypeDesignation_: Designation of a specimen as the neotype of a species-group
    name.
- Miscellaneous fields
  - _NamedAfter_: Reference to the [person](/docs/person) this name was named after.
    This field was added recently, so it is not yet used in all cases where it should be
    used.
  - _IncludedSpecies_: For genus-group names, reference to a species-group name that was
    one of the originally included species in the genus. Usually this is given only for
    species without an originally designated type, in which case the included species
    are the species eligible for designation as the type.
  - _GenusCoelebs_: Indicates that a genus-group name was originally proposed without
    any included species.
  - _TypeLocality_, _StratigraphyDetail_, _Habitat_: Deprecated tags with detail about
    the type locality; _LocationDetail_ should be used instead.
  - _Host_: Name of the type host of a symbiont.

## Miscellaneous data

- _data_: Dictionary in JSON form with miscellaneous data. This is mostly data from
  previous versions of this database that is now no longer relevant, such as vernacular
  Dutch and English names.
- _definition_: Phylogenetic definition of the name, where applicable.

## _Organ_ tags

The _Organ_ tag indicates the preserved parts of the type specimen. For extant mammals,
this is usually something like "skin and skull"; for fossils, it can be a long list of
bones. The tag has three fields:

- _organ_, one member of a long but fixed list of organs, e.g. "skin"
- _detail_, text describing the organ in more detail. For most organs, the precise text
  is tightly restricted to facilitate comparisons; for some others this has not yet been
  done. All allow adding "?" in front of the text (to indicate uncertainty) and "part"
  or "parts" after the text (to indicate that only part of the organ is present). As
  appropriate, many organs allow a count, possibly prefixed with ">" or "~" (e.g., ">1",
  "~30"), the word "proximal" or "distal", the word "shaft", or the letter "L" or "R"
  for left or right.
- _comment_, free text. Often this is the collection number of part of the type
  specimen, or further text describing its physical condition.

The options for the _organ_ field are as follows:

- Non-bones
  - _skin_, preserved skin
  - _in_alcohol_ or other fluid, a whole animal or carcass
  - _whole_animal_, usually in non-mammals, the entire animal preserved
  - _tissue_sample_ in extant animals, e.g. a liver sample
- Skull bones
  - _skull_. In extant mammals this tends to implicitly include the mandible, but in
    fossils the mandible should be implicitly included. In mammals, if teeth are
    present, those should be recorded in the standard notation. For example, "part, LC,
    RM1-3" means a partial skull holding the left canine and right first through third
    upper molars. The detail field may hold short arbitrary text as well as tooth
    positions. The word "edentulous" may be used to indicate no teeth are present.
  - _mandible_, the entirety of the lower jaw. In fossils, the tooth present should be
    recorded in the detail field. In mammals, _mandible_ should only be used if both
    branches are still articulated. If the two are separate, use _dentary_ instead. In
    nonmammals, the mandible includes more bones than just the dentary. As with the
    skull, the detail field may hold short arbitrary text.
  - _dentary_, the left or right tooth-bearing part of the mandible. The detail text
    mostly just refers to the teeth; for example, "Lm1-2, Rp4, Rm2-3" means a left
    dentary with the first and second molars and a right dentary with the fourth
    premolar and second and third molars.
  - _palate_, both upper jaws together, with the teeth present.
  - _maxilla_, one of the main tooth-bearing upper jaws.
  - _premaxilla_, holding the incisors and canine.
  - _tooth_, one or more isolated teeth. In mammals, the text follows a standard formula
    like "RX1", where "R" (or "L") indicates left or right, "X" is the category of tooth
    and "1" is its position in the series. The category is uppercase in the upper jaw
    and lowercase in the lower jaw. The standard categories are "I" (incisor), "C"
    (canine), "P" (premolar), and "M" (molar). No number should be added to "C", or to
    the incisor in rodents, because there is only ever one. Other categories allowed
    include "DI", "DC", and "DP" for deciduous teeth; "A" for antemolars (in shrews);
    and "IF", "PMF", and "MF" for incisiform, premolariform, and molariform teeth.
    Ranges of teeth may be indicated like "Lp4-m1" or "Lm1-3". If there is uncertainty
    about the exact tooth, separate the options with slashes: "M1/2" is a first or
    second upper molar, "C/c" is an upper or lower canine. In nonmammals (and some
    mammals), the teeth are not so precisely identified, and usually only a count is
    specified, or the word "maxillary" or "mandibular" for upper and lower teeth.
  - _frontlet_, in ruminants, part of the skull with horns or antlers
  - _predentary_, in ornithischians
- Paired bones
  - _scapula_
  - _coracoid_
  - _scapulocoracoid_, the fused scapula and coracoid
  - _clavicle_
  - _humerus_
  - _ulna_
  - _radius_
  - _pelvis_, the entire bone (also called innominate or os coxae) when fused together
    (as is normally the case in mammsl)
  - _ilium_, when separate from the pelvis
  - _ischium_, when separate from the pelvis
  - _pubis_, when separate from the pelvis
  - _prepubis_, in some archosaurs
  - _femur_
  - _patella_
  - _tibia_
  - _fibula_
  - _astragalus_
  - _calcaneum_
  - _petrosal_
  - _antler_, in deer
  - _horn_core_, in bovids
  - _tibiotarsus_
  - _carpometacarpal_, in birds
  - _tarsometatarsus_, in birds
- Bones of the hands and feet
  - _metacarpal_
    - The position can be indicated as "McI" for the first metacarpal
  - _metatarsal_
    - Detail field is e.g. "MtII" for the second metatarsal
  - _metapodial_
    - Either a metacarpal or metatarsal
  - _phalanx_manus_
    - "IV-1" means the proximal (first) phalanx of the fourth digit.
  - _phalanx_pes_
    - As for the manus
  - _phalanx_
    - A phalanx of either the manus or the pes
  - _carpal_
  - _navicular_
- Other bones
  - _vertebra_, including the sacrum and associated bones such as chevrons
  - _rib_
  - _caudal_tube_, in cingulates
  - _osteoderm_, in cingulates and various archosaurs, among others
  - _hyoid_
  - _baculum_
  - _furcula_
  - _sternum_
  - _shell_, usually in turtles and cingulates
  - _interclavicle_
  - _gastralia_
- Multiple bones
  - _postcranial_skeleton_. In extant mammals this often indicates the entire
    postcranial skeleton; in fossils it is used (usually with the detail text "part") if
    the source is not precise about the bones preserved.
  - _skeleton_. In fossils, used if the entire skeleton is preserved.
  - _limb_. Used (with "fore" or "hind" in the detail field) if the source is imprecise
    about the exact limb bones preserved.
  - _girdle_, with "pectoral" or "pelvic" in the detail field, also used when the source
    is not precise about the bones preserved.
  - _manus_, for the entire manus. It is preferred to list the individual bones
    separately.
  - _pes_, similar to the manus.
- Miscellaneous
  - _other_, used to tag organs that have yet to be sorted out and may need new entries
    in the list of allowed organs
  - _egg_

## Justified emendations

Although the Code generally mandates that original spellings are to be maintained, on
rare occasions a spelling other than the original is valid (see Arts. 32.5 and
33.2.3.1). In such cases, two names should be entered into the database:

- The name as originally spelled, with its original author and date and with
  `nomenclature_status` set to `as_emended`. The _root_name_ field should have the
  corrected spelling. The _page_described_ field should be set to the page where the
  original spelling was used. This name should have other standard nomenclatural
  information, like the type locality and type specimen. If its `nomenclature_status`
  cannot be `as_emended` (for example because it is a `nomen_novum`), the _AsEmendedBy_
  tag indicates its status.
- The name as corrected, with the author and date who first performed the correction.
  This name should have `nomenclature_status` set to `justified_emendation` and a
  _JustifiedEmendationOf_ tag pointing to the original spelling.

Emendations under Arts. 32.5.2 (removal of diacritics and other marks) and 32.5.3
(correction of family-group names) are generally unambiguous and mechanical, so the
database will usually make these corrections silently. If desired, a name with status
`justified_emendation` can be entered with as its authority the first author to use the
corrected name.

## Mandatory changes

There are two categories of mandatory changes (Art. 34):

- For family-group names, the ending must be changed if the rank changes.
- For species-group names, the specific name must be changed to agree in gender with the
  generic name.

These can both be expressed with the _MandatoryChangeOf_ tag in the database, but this
is rarely necessary.
