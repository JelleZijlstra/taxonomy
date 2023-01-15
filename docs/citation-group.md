# Citation group

"Citation group" are an organizing tool used in the database to group
similar [articles](/docs/article). There are three kinds of citation groups:

- Journal articles are grouped in journals
- Books are grouped by city of publication
- Dissertations are grouped by their university

For [names](/docs/name) that lack precise citation data, we try to include
the citation group in which they were originally published. That way, if I
gain access to the archive of a journal like [_Palaeontographica_](/cg/Palaeontographica),
it is easy to find all the articles in the journal that contain data of interest.

## Fields

Citation groups have the following fields:

- _type_: Journal, book, dissertation, or alias
- _name_: Name of the journal, city of publication, or university of publication
- _region_: [Region](/docs/region) that the citation group is published in
- _target_: Reference to another citation group; used if the citation group is an
  alias for another
- _archive_: Name of the archive in which articles from a journal can be found (e.g., "JSTOR")

## Journals

The following are a few guidelines for organizing journal citation groups:

- Prefer using the "series" field over using different journal names for variants of the same journal. For example, these should be [the same journal](/cg/47):
  - "Comptes Rendus de l'Académie des Sciences - Series IIA - Earth and Planetary Science"
  - "Comptes Rendus de l'Académie des Sciences - Series III - Sciences de la Vie"
  - "Comptes Rendus de l'Académie des Sciences de Paris"
- Omit "The" in journal names where possible ("[Texas Journal of Science](/cg/Texas_Journal_of_Science)", not "The Texas Journal of Science"); but "The" may be included in one-word journal names like "The Auk" and "The Condor".
- For journal names in the Latin alphabet, use the spelling and capitalization of the original language (this tends to mean much less capitalization in French and German than in English).
- If a journal was renamed, use the historical name of the journal at the time each individual article was published. Minor variations may be ignored (e.g., "Zoölogy" versus "Zoology"); if so, standardize on the more recent name. Perhaps eventually we'll have a system to merge renamed journals into the same internal journal, and automatically select the right name depending on time period.
- For journal names not in the Latin alphabet, I don't have a well-developed policy. If there is an established Latin-alphabet name (e.g., "[Vertebrata PalAsiatica](/cg/Vertebrata_PalAsiatica)"), use it. Otherwise, we should use the native name, with redirects for common Latin-alphabet renderings. (In practice this mostly affects Russian and some Japanese journals. Chinese journals tend to have well-established English names.)
- For the citation_group field on names, it's OK to guess at the journal name or use an abbreviation. We'll clean up the names eventually.

See [Tricky journals](/docs/tricky-journals) for some tricky cases.
