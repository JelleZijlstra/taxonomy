This page lists major changes in the code and data for the _Hesperomys_ database.
Version numbers correspond to Git tags in the
[frontend](https://github.com/JelleZijlstra/hesperomys/) and
[backend](https://github.com/JelleZijlstra/taxonomy/) repositories. In the future,
they will also apply to the database itself.

# Unreleased

- Database
  - Clean up about 300 duplicate articles with the same DOI
  - Clean up a few dozen duplicate citation groups
  - Clean up some unresolvable DOIs
  - Add ISSN to over 600 journals
  - Fix numerous incorrect page fields on names and articles
  - Clean up many journal citations (adding missing end pages,
    correcting incorrect pages, standardizing formats)
  - Add a few new taxa and articles
- Frontend
  - Display links to various identifiers (ISSN, ISBN, etc.) on
    article and citation group pages
  - Redirect merged entities into their target entity
- Backend
  - Support redirecting Taxon and Name entities to others
  - Add support for marking the valid year range on journals
    (documented in [Tricky journals](/docs/tricky-journals))
  - Check that the _page_described_ field for names is within
    the page range of their original citation
  - Validate the _series_, _volume_, _issue_, _start_page_,
    and _end_page_ fields on journal articles. Documented
    in [Citation group](/docs/citation-group)

# Previous

Before January 2023 I did not track release notes.