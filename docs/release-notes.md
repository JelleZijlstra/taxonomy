This page lists major changes in the code and data for the _Hesperomys_ database.
Version numbers correspond to Git tags in the
[frontend](https://github.com/JelleZijlstra/hesperomys/) and
[backend](https://github.com/JelleZijlstra/taxonomy/) repositories, and to database
exports released on Zenodo.

# 25.3.0 (March 15, 2025)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15033739.svg)](https://doi.org/10.5281/zenodo.15033739)

- Database
  - Add classifications from [Koopman (1994)](/a/58061),
    [Wilson & Reeder (1993)](/a/68307), [Jackson & Groves (2015)](/a/34474), and many
    other sources.
  - Add verified citations for a large number of additional names, including over 99% of
    currently recognized valid mammal species.
  - Update classification with some recent taxonomic revisions.
- Backend
  - Add new status "unavailable" for a taxon that is likely valid but lacks an available
    name.
  - Various optimizations to reduce memory usage.
  - Check that the same page number in an article is consistently linked to the same BHL
    page.
  - Automatically merge equivalent classification entries.
  - Add support for associating classification entries with a specific earlier entry,
    for the case where a previous synonymy refers to a usage that this database does not
    list as an independent name.
  - Infer page links for non-BHL pages.
  - Check that classification entries have valid page numbers.
  - Check that subspecies classification entries are listed under species.
  - Add a way for unavailable names to point to an equivalent available name.
  - Make the root name of name combinations agree in gender with the genus name.
  - Add code to find when a valid taxon has a senior synonym.
  - Redo convention for how to represent page numbers.
  - Parse and standardize Gallica URLs.
- Frontend
  - Add bibliographical notes describing
    [Griffith's _General and Particular Description_](/docs/biblio/gen-part-desc),
    [Blainville's _Ostéographie_](/docs/biblio/osteographie), and
    [Ehrenberg's _Symbolae physicae_](/docs/biblio/symbolae-physicae).

Thanks to Connor J. Burgin, Virginia Hayssen, and Rudolf Haslauer for supplying
literature.

# 24.9.1 (September 21, 2024)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.13824506.svg)](https://doi.org/10.5281/zenodo.13824506)

- Database
  - Always treat emendations from _-ii_ to _-i_ and similar as incorrect subsequent
    spellings rather than unjustified emendations (ICZN Art. 33.4)
  - Mark more family-group names as subsequent usages of earlier identical names
  - Enforce that classification entries mapping to unavailable names are also marked as
    unavailable
  - Add a more precise set of ranks, mostly for infrasubspecific names
  - Enforce that classification entries always map to a name of the same group (i.e.,
    family-group names are not mapped to names ranked above the family group)
  - Adjust attribution of names that were originally proposed as family-group names but
    are now used at a higher rank. For example, Marsupialia is now attributed to Lesson
    (1827), with Marsupialia Illiger, 1811, an unavailable family-group name, treated as
    a synonym.
  - Re-attribute a number of family-group taxa to an older authority
  - Remove some "subsequent usage" names as redundant
  - Add some new taxa and verify more citations
- Frontend
  - Add toggle to control how name lists are sorted

# 24.9.0 (September 2, 2024)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.13630864.svg)](https://doi.org/10.5281/zenodo.13630864)

This release adds a new category of data to the Hesperomys database: classifications. A
classification lists all the taxa included in a literature reference in an organized
way, and each name in the classification is associated with a name in the Hesperomys
database. As an example, [this page](https://hesperomys.com/ce/9291) shows the
classification of mammals as it appeared in Wilson and Reeder (2005), and
[this](/a/57429) shows the classification of Linnaeus (1758). The database now includes
more than 140,000 entries from published classifications. The `ce.csv` file (available
on Zenodo) exports all of them.

Classifications enabled me to add many more name combinations to the database; the goal
there is that eventually, all scientific names that have been used for mammals in the
past have an entry in the database and can be mapped to a currently recognized species.
The previous release only included 1028 name combinations; this release includes 19578.
I added classifications for several major recent listings of mammal species, including
Corbet & Hill (1980), Honacki et al. (1982), Corbet & Hill (1991), Wilson & Reeder
(2005), and several release of the Mammal Diversity Databases, and from many other
sources.

There are several other use cases for classifications. They can help show how
classifications have changed over time, for example by highlighting which authors have
considered a taxon as valid in the past. It is possible to compare two classifications,
showing how they differ. Adding the entire classification from a source also helps
ensure that no names are missed. I added a number of previously missed available names
that I encountered while entering names from various old sources.

- Database
  - Add numerous name combinations and misspelled names from the literature
  - Apply some recent taxonomic changes
  - Add verified citations for more names
  - Add classifications
  - Remove some duplicate species name complexes and add more species name complexes
- Backend
  - Separate misidentifications from subsequent usages
  - Separate _SourceDetail_ (detailing where the original author got a name from) from
    _CitationDetail_ (detailing where the name was first published) tags
  - Support marking citation groups as children of others; this is used for book series
- Frontend
  - Show data for classification entries
  - Rename "cleaned" to "normalized" original name
  - Correctly italicize taxon names on taxon pages
  - Hide empty sections on name pages
  - Show classification entries
  - Show misidentifications in a separate section
  - Show source details
  - Hide name identities when they are redundant
  - Fix rendering of references for some articles

Thanks to Virginia Hayssen, Rudolf Haslauer, and Connor Burgin for supplying literature.

# 24.6.0 (June 17, 2024)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.12047416.svg)](https://doi.org/10.5281/zenodo.12047416)

- Database
  - Add an algorithm that infers the most likely location of the type specimen in cases
    where the database does not record the location of a type specimen. Type specimens
    inferred by this algorithm are shown in a separate section on collection pages.
  - Add more _CitationDetail_ tags containing the way a citation was quoted in a source.
  - New species and recent taxonomic changes, mostly for extant mammals
  - Add more nomenclatural data, such as original type localities, mostly for extant
    mammals.
- Backend
  - Improvements to logic for finding Biodiversity Heritage Library links
  - Detect more duplicate articles
  - Clean up outdated code and move more checks into the lint system

Thanks to Rudolf Haslauer and Connor Burgin for supplying literature.

# 24.4.0 (April 13, 2024)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10969300.svg)](https://doi.org/10.5281/zenodo.10969300)

This release focuses on compatibility with the MDD and improved automatic data
extraction. More than a third of names are now linked directly to pages in the
Biodiversity Heritage Library.

- Database
  - Fix species assignment for many extant mammal names based on comparisons with data
    from the MDD. The database's classification for extant mammals now exactly matches
    the MDD (except for new changes that have not been published in the MDD's latest
    release yet).
  - Set more precise page numbers instead of page ranges for a few dozen names.
  - Add coverage of Cynognathia, a group of Triassic cynodonts closely related to
    mammals.
  - Add full authority citations for many more names, notably including many published
    in the publications of the Geological Survey of India and many with type specimens
    in the ZMMU.
  - Set the _page_described_ field for several thousand more names.
  - Move to a more consistent format for type specimens. All type specimen references
    now start with the institution code.
  - Mark numerous names as junior homonyms.
  - Add links to many more citations (primarily in the Biodiversity Heritage Library).
  - Add direct page links to original citations in over 30,000 names.
  - Add a few hundred more type locality coordinates and correct more than a hundred
    incorrect coordinates.
- Backend
  - Add support for ORCID identifiers (though they are currently set for very few
    people)
  - Enforce that collection labels consist only of letters and that type specimen
    references start with the corresponding collection label
  - Automatically detect species-group homonyms. Distinguish between primary and
    secondary homonyms.
  - Revamp system for computing the nomenclatural status of names.
  - Fill the citation group column for names with a checked original citation in the
    exported data.
  - Add support for linking names directly to an online resource (primarily the
    Biodiversity Heritage Library).
  - Check that type locality coordinates are in the right country.
- Frontend
  - Add option to show names in a taxon that are missing a field.
  - Add tool for finding homonyms in the species group.
  - Add frontend support for new information (authority page links and PhyloCode numbers
    on names; bibliographic notes and alternative URLs on articles; comments on citation
    groups).
  - List basal, incertae sedis, and dubious child taxa separately.
- Exported files
  - Add page links and type specimen links to Name export files.

Thanks to Rudolf Haslauer, Severin Uebbing, and especially Connor Burgin for supplying
information and literature.

# 24.1.0 (January 10, 2024)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10481655.svg)](https://doi.org/10.5281/zenodo.10481655)

- Database
  - Add many newly named taxa, though some recent literature may still be missing.
  - Many small corrections and additions, some based on literature I did not previously
    have access to.
  - Add coverage for several archosauromorph groups, including ornithischians,
    "rauisuchians", aetosaurs, phytosaurs, and basal archosauromorphs. All
    archosauromorphs except crown-group birds are now reasonably well covered.
  - Improve "organ" tags for the material included in type specimens. Replace many
    organs labeled with the legacy "other" or "postcranial_skeleton" organs and start
    standardizing the format of the "details" field.
  - Start adding name combinations to the database, mostly for newly covered taxa. Some
    names previously classed as "subsequent usage" are now marked as "name combination".
- Backend
  - Add many new bones to the enumeration of organs
  - Add script to infer the publication date of articles based on that of other articles
    in the same issue
  - Add ability to add the PhyloCode number for a name
  - Support for name combinations
  - Make terminal autocompletions more efficient
  - Taxa that are placed in a parent taxon that also contains sibling taxa of higher
    ranks are now marked as either "basal" (current evidence indicates they are unlikely
    to belong in any particular subtaxon) or "incertae sedis" (they may belong in some
    subtaxon). (This is not yet reflected in the frontend.)

Thanks to Rudolf Haslauer and Connor Burgin for supplying literature.

# 23.8.1 (August 29, 2023)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8298623.svg)](https://doi.org/10.5281/zenodo.8298623)

- Database
  - Add numerous additional links to type specimen catalog entries, primarily for US
    collections found in [VertNet](http://www.vertnet.org/index.html). About 37% of
    names in the database with type specimens now have a link to an online collection
    database.
  - Add and update some additional type specimens.
- Backend
  - Add support for "extra" and "future" catalog numbers on type specimens.
  - Require that type specimen links from a particular collection follow a fixed prefix
  - Support marking fields on tags as required or not. Fixes a bug where some name pages
    (those that have lectotype/neotype designations for which no source has been
    recorded) failed to render.
- Frontend
  - Add total counts to all lists.

# 23.8.0 (August 17, 2023)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8260038.svg)](https://doi.org/10.5281/zenodo.8260038)

- Database
  - Add issue numbers to several hundred articles (mostly in _Annals and Magazine of
    Natural History_ and _Proceedings of the Zoological Society of London_). Correct a
    few publication dates and issue numbers.
  - Add original citations for all names described in the _Journal of the Bombay Natural
    History Society_.
  - Add a few dozen missing type localities for synonyms of extant mammal species.
  - Add numerous additional type specimens for extant mammals, based mostly on online
    collection databases.
  - Add thousands of links to type specimen catalog entries, mostly in
    [BMNH](</c/BMNH_(mammals)>), [MNHN](</c/MNHN_(ZM)>), and
    [MCZ](</c/MCZ_(Mammalogy)>).
  - Numerous new taxonomic changes and newly examined old literature.
- Backend
  - Add capability to mark the former repository of a type specimen (see e.g.
    [FMSM](/c/FMSM)).
  - Enforce a consistent format for the type specimen field.
  - Add the capability to enforce a more precise format for individual collections.
  - Enforce that names have only one value for e.g. the "age" tag for type specimens.
  - Use the "IssueDate" mechanism to infer what issue papers were published in.
- Frontend
  - Add new page listing the newest additions to the database (e.g.,
    [new names](/new/n)).

# 23.6.0 (June 16, 2023)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8049254.svg)](https://doi.org/10.5281/zenodo.8049254)

- Database
  - Add numerous new names and recently published articles. Since the previous release,
    1226 additional names and 1450 additional articles have been added.
  - Add given names or initials for a number of name authors that were given only as
    family names.
  - Remove about 200 duplicate articles
  - Remove some duplicate names, mostly in the turtles
  - Remove uses of the legacy "dubious" status
  - Fix some incorrect parent taxa
  - Add numerous new fossil taxa based on the recent literature
  - Further taxonomic changes for extant mammals for alignment with the MDD
  - Simplify the treatment of justified emendations, using two names instead of three
  - Change many \_page_described_fields to follow a more consistent format
- Backend
  - Further checks for publication dates. Distinguish between "parts" (separately
    published portions of a larger work) and "chapters" (simultaneously published
    portions of a larger work, usually with different authorship). Enforce that chapters
    have the same publication date as their enclosing work. Allow ranges of years (e.g.,
    "1848-1852") only for works composed of parts, not for names or for other kinds of
    works.
  - More sophisticated processing and validation of LSIDs in order to display correct
    publication dates.
  - Enforce that the _page_described_ and _original_rank_ fields are set for all names
    with original citations.
  - Add new nomenclature statuses to distinguish between unpublished names, separating
    out "unpublished_thesis" (unpublished because named in a thesis),
    "unpublished_electronic" (named in an electronic work that does not fulfill the
    ICZN's criteria for publication), "unpublished_supplement" (named in electronic
    supplementary material only), and "unpublished_pending" (not yet available, but
    expected to be made available by print publication).
- Frontend
  - Better ordering for various lists of names and articles (e.g., ordering by page)

# 23.3.0 (March 13, 2023)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7730954.svg)](https://doi.org/10.5281/zenodo.7730954)

- Database
  - Incorporate some data from the African Chiroptera Database (thanks to Victor Van
    Cakenberghe).
  - Incorporate many missed names from the Mammal Diversity Database (with more to come)
  - Add [SMF](/c/SMF) mammalian type specimens from a type catalog I located.
  - Various new data, including some additional original citations and a few new
    species.
- Backend
  - Add the ability to associate type catalogs and collection databases with Collection
    objects (e.g., [MNHN](/c/MNHN)).
  - Add the ability to link to collection database entries for type specimens.
  - Support name aliases, in order to provide more familiar citation forms for some
    personal names. This feature is not yet widely used.
- Frontend
  - Add bibliographic notes on
    [_Zoology of the Erebus and Terror_](/docs/biblio/erebus-terror) and
    [_Histoire naturelle des Mammifères_](/docs/biblio/histnatmammiferes).
  - Add new pages on [data sources](/docs/data-sources) and [scores](/docs/scores).

# 23.2.0 (February 19, 2023)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7654755.svg)](https://doi.org/10.5281/zenodo.7654755)

This is the first tagged release. It comes with a number of major improvements:

- I started to collaborate with the
  [Mammal Diversity Database](https://www.mammaldiversity.org/) to improve our
  respective databases. So far, this collaboration has led to a major update of the
  taxonomy used for extant mammals in this database. Thanks to Nate Upham for reaching
  out to start this collaboration and to Connor Burgin for reviewing numerous
  differences between the databases with me.
- Much of the database is now available on Zenodo as an immutable CSV export
  ([doi:10.5281/zenodo.7654755](https://doi.org/10.5281/zenodo.7654755)). Thanks to
  Jorrit Poelen for advice in this area.
- The database now supports a full-text search function that also searches my reference
  database.
- The database now has better support for tracking and checking publication dates of
  references and names. Many publication dates (about 20%) are now specified to the
  month or day.

This version may be cited as:

- Zijlstra, J.S. 2023. Hesperomys Project (Version 23.2.0) [Data set]. Zenodo.
  [doi:10.5281/zenodo.7654755](https://doi.org/10.5281/zenodo.7654755)

More detailed list of changes:

- Database
  - Start harmonizing the taxonomy for living mammals with the
    [Mammal Diversity Database](https://www.mammaldiversity.org/), fixing numerous cases
    where my classification was out of date. A few discrepancies remain.
  - Clean up about 300 duplicate articles with the same DOI
  - Clean up a few dozen duplicate citation groups
  - Clean up some unresolvable DOIs
  - Add ISSN to over 600 journals
  - Fix numerous incorrect page fields on names and articles
  - Clean up many journal citations (adding missing end pages, correcting incorrect
    pages, standardizing formats, standardizing italics)
  - Add many new taxa and articles
  - Clean up unprintable characters in strings
  - Add geographic coordinates for more type localities
  - For species not currently assigned to a genus, display the nominal genus name in
    quotes (e.g., "_Microsciurus_" _flaviventris_).
  - Correct numerous specific epithets to agree in gender with their genus
- Frontend
  - Add full-text search which also searches in my library of references
  - Display links to various identifiers (ISSN, ISBN, etc.) on article and citation
    group pages
  - Redirect merged entities into their target entity
  - Add [Future work](/docs/future-work) document
  - Improve display of lists to make it more intuitive
  - Display a transliteration for Cyrillic names (e.g. "Несов (Nesov)")
  - Add [Taxonomy](/docs/taxonomy) document
  - Support [SSL](https://hesperomys.com/)
- Backend
  - Support redirecting Taxon and Name entities to others
  - Add support for marking the valid year range on journals (documented in
    [Citation group](/docs/citation-group))
  - Check that the _page_described_ field for names is within the page range of their
    original citation
  - Validate the _series_, _volume_, _issue_, _start_page_, and _end_page_ fields on
    journal articles. Documented in [Citation group](/docs/citation-group)
  - Check and restrict the use of italics in article titles
  - Add new "recently_extinct" age class
  - Support adding ZooBank links (LSIDs) to articles and names
  - Support publication dates granular to the month or day level
  - Add support for storing when each volume of a journal was published, and use this
    information to correct many dates for
    [_Proceedings of the Zoological Society of London_](/cg/1).
  - New [Dating](/docs/dating) page, and numerous improvements to the treatment of
    publication dates. A significant proportion of names and articles now have dates
    granular to the month or day, and many incorrect publication dates have been
    corrected.

# Previous

Before January 2023 I did not track release notes, but updated the database at irregular
intervals.
