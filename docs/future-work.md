# Future work

This page discusses some possible future work on the database. No guarantees on when
these will happen, if ever.

If something on this list would be especially useful to you or if you have suggestions,
please [let me know](mailto:jelle.zijlstra@gmail.com).

## Improving coverage

- Fill in the _verbatim_citation_ field. This field should arguably be the bare minimum
  for validating that a name is real. Previously I worked to set this field for almost
  all post-1950 names. Tools:
  - _recent_names_without_verbatim_: names published after some year missing the field
  - _most_common_authors_without_verbatim_citation_: missing citations sorted by author
    name. I find it useful to tackle all names from one author, because I'll frequently
    find multiple of them at once.
- Fill in the _year_ and _species_type_kind_ fields (and enforce in lint that they are
  always filled in). These are even more bare minimum, and there are very few names that
  are missing them.
- Compare with other databases to find missing data and taxa:
  - Mammal Diversity Database
  - PaleoBioDB
  - Wikipedia new species lists
- Add more outbound links to articles (e.g., figure out more DOIs and article URLs)
- Add more geographic coordinates for type localities
- Work to improve publication dates
  - Figure out dates for "Monatsberichte": is _Ptenochirus_ actually from 1862? Jackson
    & Groves cite Bauer AM, Gunther R, Klipfel M (1995) The herpetological contributions
    of Wilhelm C.H. Peters (1815–1883). Society for the Study of Amphibians and Reptiles
    in association with the Deutsche fur Herpetologie und Terrarienkunde.
  - "see Schwarz, E., Sitzb. Ges. Naturf. Freunde, Berlin, for 1926, p. 29, 1927" might
    have dates for _Sitzungsberichte_ publications (found in {Placentalia Africa (Allen
    1939.pdf)}) p. 154
  - Jackson & Groves (2015: 493-494) have a useful list of references for dates

## Frontend

- Improve speed (server-side rendering? caching?)
  - Sketch: An additional SQLite database on the server box that gets cleared on deploy
    and stores (GQL query, args) -> result.
    - Could be pre-filled if we want to
    - Need to worry about filling up the disk
- UI indication that names belong to a group with high-quality vs. low-quality coverage
  (so users don't expect insect families to list all genera)

## Backend

- Track journal publishers
  - Why would it help? This could allow me to group journals with the same publisher so
    I can find citations published in them.
  - Could use CrossRef's DOI information to jumpstart
  - Maybe add a new citation group type for publishers
  - Or just a CitationGroupTag for the publisher that we can put on journals
  - Complications: Journals change publishers sometimes. "JSTOR" is a useful category
    but is not technically a publisher. For books, need to be able to record both the
    city of publication and the publisher.
- Find more duplicate articles (those without DOIs)
- Detect more duplicate persons:
  - Unchecked names that are almost the same as redirects
  - Cases where we have both "Firstname M. Lastname" and "Firstname Middlename Lastname"
- Make the editor not allow leaving required (non-null) fields empty (especially enums)
- Clean up duplicate tags (e.g. _Echinothrix_ Flower & Lydekker has the same UnjustifiedEmendation tag twice)

## New or disabled checks

- `check_expected_base_name`: check that the base name for each taxon is the oldest
  available one.
  - Why would it help? Automatically find nomenclatural issues where there is a senior
    synonym.
  - Complications: Lots of special cases where the oldest name is not the one that
    should be used.
- Standardize and check the format of the _page_described_ field.
  - Why would it help? The check that matches up _page_described_ to the article's pages
    will be more powerful.
- `check_type_designations_present`: check that lectotypes and neotypes have a type
  designation field.
  - Complications: Often people just say "lectotype" without specifying who designated
    it. Or I don't have access to the source that did the designation.
  - Idea: Add a new tag to names that encompasses a verbatim_citation and
    citation_group, so we can track needed citations here.
- Check that taxa don't have parents of the same rank (with some restrictions)
  - Noticed that _Sminthopsis aitkeni_ was placed within _S. fuliginosa_ but not ranked
    as a subspecies.

## New data not currently included

This lists kinds of data that is not currently included in the database at all, but that
is potentially useful.

- _ORCID_ IDs for Persons
  - Why would it help? Meant to be a standard identifier for researchers.
  - Complication: CrossRef API doesn't give me these IDs. Is there any API I can use to
    connect ORCID IDs to articles or people?
- Track new _name combinations_ (e.g., generic reassignments)
  - Why would it help? Make it possible to look up any name you see in a source in the
    database.
  - Complications: It's a lot more work.

## New taxonomic groups

I want to avoid adding too many additional taxonomic groups because just keeping the
mammals up to date is plenty of work, but here are some groups that may get more
comprehensive coverage in the future:

- Phorusrhacid birds
- Fossil lissamphibians
- Ornithischian dinosaurs
- Sauropterygians
- Early archosauromorphs

Extant vertebrates of other classes would be an obvious expansion but there are usually
already good resources available. For extant amphibians, Darrel Frost's
[Amphbians of the World](https://amphibiansoftheworld.amnh.org/) is unequaled, and any
coverage in this database would be duplicative. For extant reptiles the
[EMBL Reptile Database](http://www.reptile-database.org/) is also quite good but not as
rigorous in terms of nomenclature. For fish
[FishBase](https://www.fishbase.se/search.php) is great. For birds
[Zoonomen](http://zoonomen.net/) is useful but more bare-bones. Still, I will
occasionally add some extant taxa of these classes.
