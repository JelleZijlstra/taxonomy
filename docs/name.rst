**************
The Name table
**************

This document describes the contents of the Name table and its columns.

The Name table is intended to include all available names within the scope of
the database, as well as all unavailable names that are relevant to
nomenclature (e.g., those that have been used in the past as if they were
available). A core concept of the database is the separation between
*names* and *taxa*. Names are labels regulated by the International Code of
Zoological Nomenclature, and their attributes are (mostly) defined by objective
rules. Taxa are groups of animals recognized by the science of taxonomy and
their limits are subjective in nature.

Basic data
----------

- _group_: The group (as determined by the Code) under which the name falls.
  The code regulates the family group (roughly, superfamilies through
  subtribes), genus group (genera and subgenera), and species group (species
  and subspecies). Names above the family group are largely unregulated; in
  this database they are labeled as the *high group*. This group also includes
  unranked taxa interpolated between regulated ranks (for example, the
  unranked taxon Oryzomyalia between the subfamily Sigmodontinae and its
  constituent tribes).
- _root name_: The name to be used as a root to form a valid taxon name from
  this name. For high- and genus-group names, this is simply the name. For
  family-group names, it is the stem without any rank ending (e.g., "Sigmodont"
  for Sigmodontinae). For species-group names, it is the specific name (e.g.,
  "palustris" for *Oryzomys palustris*).
- _status_: The taxonomic status of the name. The status may be *valid* (the
  name is the base name for a valid taxon), *synonym* (the name denotes a
  synonym of a valid taxon), *dubious* (the name is treated as a synonym, but
  the status is unclear), *nomen dubium* (the name is treated as the base name
  of a taxon, but that taxon is not known to represent a biological reality), or
  *species inquirenda* (there is insufficient information to determine whether
  the name's taxon is biologically real). The "dubious" status is being
  deprecated, mostly in favor of "nomen dubium". The use of the terms "nomen
  dubium" and "species inquirenda" may not match that in other work. For the
  purposes of this database, a nomen dubium is a name that cannot be
  unambiguously allocated to a biologically real taxon, for example because
  its type specimen is lost or uninformative. A species inquirenda is a name
  (not necessarily a species name) that has not been shown to be valid, a
  synonym, or a nomen dubium, but which, for whatever reason, I believe there
  is insufficient evidence that it represents a real taxon. Usually, this
  situation arises with names published long ago that have been ignored in
  subsequent studies. For example, Kretzoi (1941) named a species
  *Gazelloportax andreei*. The name is available, but as far as I know no
  subsequent author has commented on it, even though several authors have
  treated the taxonomy of the group of which it is part. The name cannot be
  treated as a synonym of another name, it is probably not valid (if so,
  some author would have used the name), and it cannot be treated as a nomen
  dubium (taxonomic revision may well show that it is a synonym of some other
  species). The database does not currently have a way to distinguish objective
  from subjective synonyms; this should be changed.
- _taxon_: The taxon to which the name is allocated.
- _original name_: The form of the name used by the original description. This
  may be an incorrect original spelling. If there are multiple original
  spellings, they should all be listed as separate names.
- _nomenclature status_: The status of the name under the Code, either
  available or unavailable for some specific reason (e.g., the name is a nomen
  nudum; the name was not properly published). This field has not been
  consistently applied to all names, and it may require more detailed treatment
  (for example, not all statuses are mutually exclusive).

Citation and authority
----------------------

- _authority_: The author or authors who established the name. This should be
  of the form "Smith", "Smith & Jones", or "Smith, Jones & Dupont". Initials
  (e.g., "J.A. Allen") may be added to prevent ambiguity. There are a few
  authority of the form "Smith et al." in the database, but this form is
  deprecated. If the name was established in a work by other authors, the
  authority field should still just have the name of the author of the name
  (not "Smith in Jones" or similar).
- _original citation_: Reference to the publication (in the Article table) in
  which the name was first made available. This is usually set only if I own a
  copy (e.g., a PDF) of the publication.
- _page described_: The place in the original citation where the name was made
  available. Normally this is a page reference, but it may be a reference to a
  plate or figure. Where relevant, the page should be the location of a heading
  like "*Aus bus*, sp. nov.", not necessarily the first mention of the name. If
  there is no such heading, the page should be the first page on which the name
  appears.
- _verbatim citation_: A free-form text specifying the original place of
  publication of the name, used when I do not have the full paper (so that
  _original citation_ is not set). This is a place for rough notes that will
  eventually help me find the full citation; the format is not standardized.
- _year_: The year in which the name was established. If the exact year is not
  known, this may be of the form "1899-1900". The Code recommends a citation of
  the form '1900 ["1899"]' if the actual year of publication differs from that
  in the work itself. The database does not currently provide a way to handle
  this circumstance, but it should be handled as a property of the publication,
  not of the name. The Code also stipulates citation as "1940 (1870)" if a
  family-group name takes the priority of an earlier name under certain
  circumstances; this is also not currently supported by the database.

Gender and stem
---------------

- _stem_: For genus-group names only, the stem to be used to form a
   family-group name from this name.
- _gender_: For genus-group names only, the grammatical gender of the name
   (masculine, feminine, or neuter).
- _name complex_: A group of names of the same derivation. Used to help
  determine gender and stem for genus-group names and gender endings for
  species-group names. There are separate name complexes for the genus and
  species groups. Family-group and high-group names do not have name
  complexes. A name complex may encompass names based on a specific Latin
  or Greek root word, or names whose treatment is stipulated by a specific
  article in the Code (for example, "names whose gender is explicitly specified
  as masculine").

Types
-----

- _type_: For the family and genus group only, a reference to the name treated
  as the type of the name.
- _verbatim type_: Textual description of the type. Should be replaced by data
  in a column with a more specific format (e.g., type or type_specimen).
- _type locality_: Reference to the locality object that encompasses the type
  locality of the name. This tends to be a specific fossil site for fossils and
  a larger political region for extant names. I may eventually switch to a
  different system for recording type localities precisely and consistently.
- _type locality description_: Textual description of the exact type locality.
  May also contain discussion of how the type locality was determined.
- _type specimen_: The type specimen of the name. Normally, this should be in
  the form of a standard specimen reference, e.g. "AMNH 108371". Otherwise, it
  should be whatever information is known that will unambiguously indicate the
  specimen involved.
- _collection_: Reference to the collection in which the type specimen is
  located (in the Collection table).
- _type description_: Textual description of the type specimen (e.g., "skin
  and skull", "adult male", "right M1"). This should perhaps be more formal
  (perhaps with separate tags for sex, age, organ, or other data).
- _type specimen source_: Reference to the publication in which information
  about the type specimen was found.
- _genus type kind_: For genus-group taxa, the way the type species was
  designated (original designation, original monotypy, tautonymy, Linnaean
  tautonymy, subsequent designation).
- _species type kind_: For species-group taxa, the kind of type specimen
  (holotype, syntypes, lectotype, neotype).

Miscellaneous data
------------------

- _data_: Dictionary in JSON form with miscellaneous data. This is mostly data
  from previous versions of this database that is now no longer relevant, such
  as vernacular Dutch and English names.
- _nomenclature comments_: Comments about the nomenclature of the name (e.g.,
  whether and why it is unavailable).
- _other comments_: Miscellaneous comments, which should be reallocated to
  either nomenclature or taxonomy comments.
- _taxonomy comments_: Comments about the taxonomy of the name (e.g., why it is
  considered a synonym).
- _definitiion_: Phylogenetic definition of the name, where applicable.
- _tags_: Various more unusual nomenclatural attributes in a fixed form (for
  example, the name this name is preoccupied by or is an incorrect subsequent
  spelling of).

Justified emendations
---------------------
Although the Code generally mandates that original spellings are to be
maintained, on rare occasions a spelling other than the original is valid (see
Arts. 32.5 and 33.2.3.1). In such cases, three names should be entered into the
database:

- The name as corrected, with its original author and date and with nomenclature_status
  set to as_emended. The page_described field should be omitted.
- The name as originally spelled, with its original author and date, but no other
  data (such as type_tags). It should have nomenclature_status set to
  incorrect_original_spelling and an IncorrectOriginalSpellingOf tag pointing to
  the emended name.
- The name as corrected, with the author and date who first performed the correction.
  This name should have nomenclature_status set to justified_emendation and a
  JustifiedEmendationOf tag pointing to the original spelling.

Emendations under Arts. 32.5.2 (removal of diacritics and other marks) and 32.5.3
(correction of family-group names) are generally unambiguous and mechanical, so
the database will usually make these corrections silently. If desired, a name
with status justified_emendation can be entered with as its authority the first
author to use the corrected name.

Mandatory changes
-----------------
There are two categories of mandatory changes (Art. 34):
- For family-group names, the ending must be changed if the rank changes.
- For species-group names, the specific name must be changed to agree in gender
  with the generic name.
These can both be expressed with the MandatoryChangeOf tag in the database, but
this is rarely necessary.
