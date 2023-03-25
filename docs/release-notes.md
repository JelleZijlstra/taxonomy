This page lists major changes in the code and data for the _Hesperomys_ database.
Version numbers correspond to Git tags in the
[frontend](https://github.com/JelleZijlstra/hesperomys/) and
[backend](https://github.com/JelleZijlstra/taxonomy/) repositories, and to database
exports released on Zenodo.

# Unreleased

- Database
  - Add given names or initials for a number of name authors that were given only as
    family names.
  - Remove about 200 duplicate articles
  - Remove some duplicate names, mostly in the turtles
  - Remove uses of the legacy "dubious" status
  - Fix some incorrect parent taxa
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
- Frontend

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
