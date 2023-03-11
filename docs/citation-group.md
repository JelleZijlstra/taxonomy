# Citation group

"Citation group" are an organizing tool used in the database to group similar
[articles](/docs/article). There are three kinds of citation groups:

- Journal articles are grouped in journals
- Books are grouped by city of publication
- Dissertations are grouped by their university

For [names](/docs/name) that lack precise citation data, we try to include the citation
group in which they were originally published. That way, if I gain access to the archive
of a journal like [_Palaeontographica_](/cg/Palaeontographica), it is easy to find all
the articles in the journal that contain data of interest.

## Fields

Citation groups have the following fields:

- _type_: Journal, book, dissertation, or alias
- _name_: Name of the journal, city of publication, or university of publication
- _region_: [Region](/docs/region) that the citation group is published in
- _target_: Reference to another citation group; used if the citation group is an alias
  for another
- _archive_: Name of the archive in which articles from a journal can be found (e.g.,
  "JSTOR")

## Journals

The following are a few guidelines for organizing journal citation groups.

### Splitting and lumping

- Prefer using the "series" field over using different journal names for variants of the
  same journal. For example, these should be [the same journal](/cg/47):
  - "Comptes Rendus de l'Académie des Sciences - Series IIA - Earth and Planetary
    Science"
  - "Comptes Rendus de l'Académie des Sciences - Series III - Sciences de la Vie"
  - "Comptes Rendus de l'Académie des Sciences de Paris"
- If a journal was renamed, use the historical name of the journal at the time each
  individual article was published. Minor variations may be ignored (e.g., "Zoölogy"
  versus "Zoology"); if so, standardize on the more recent name. Perhaps eventually
  we'll have a system to merge renamed journals into the same internal journal, and
  automatically select the right name depending on time period.

### Names

- Omit "The" in journal names where possible
  ("[Texas Journal of Science](/cg/Texas_Journal_of_Science)", not "The Texas Journal of
  Science"); but "The" may be included in one-word journal names like "The Auk" and "The
  Condor".
- For journal names in the Latin alphabet, use the spelling and capitalization of the
  original language (this tends to mean much less capitalization in French and German
  than in English).
- For journal names not in the Latin alphabet, I don't have a well-developed policy. If
  there is an established Latin-alphabet name (e.g.,
  "[Vertebrata PalAsiatica](/cg/Vertebrata_PalAsiatica)"), use it. Otherwise, we should
  use the native name, with redirects for common Latin-alphabet renderings. (In practice
  this mostly affects Russian and some Japanese journals. Chinese journals tend to have
  well-established English names.)
- For the citation_group field on names, it's OK to guess at the journal name or use an
  abbreviation. We'll clean up the names eventually.

### Series, volume, issue, pages

The five fields _series_, _volume_, _issue_, _start_page_, and _end_page_ on
[articles](/docs/article) indicate where in the journal the article is to be found.

#### Series

The _series_ is used for only some journals. It is used in two somewhat distinct cases:

- Some journals (e.g. [Annals and Magazine of Natural History](/cg/33)) periodically
  reset their volume numbers to 1 and start over. In this case, the series is usually a
  number written in Arabic numerals. Sometimes the numbering is simply reset without an
  explicit series number. In this case, the abbreviation "n.s." (for "new series") or
  its German equivalent "N.F." ("Neue Folge") may be placed in the series field.
- Some journals are divided into subjournals covering distinct subject matter (e.g.,
  [Comptes rendus de l'Académie des sciences](/cg/47)). Usually the volume numbers are
  kept the same across each subjournal. Most frequently the subjournals are referred to
  by capital letters (A, B, ...), but there are also journals that use Roman numerals
  (I, II, ...) or simply names (e.g., "Biology").

What both cases have in common is that a single volume number may refer to two different
physical volumes. In a few journals, both kinds of series exist at the same time. In
this case, the two series markers are placed together in the _series_ field, separated
by a semicolon (;).

Journal articles are only allowed to set the _series_ field if their citation group has
the _SeriesRegex_ tag. This tag provides a regular expression that all series fields
must match. If the _MustHaveSeries_ tag is also present, all articles in the citation
group must have the field set.

#### Volume

The _volume_ must be set for all journal articles, except for "in press" articles that
have not yet been assigned to a volume. Most frequently, the volume is a single number
that is continually increasing, often with a new volume number every year. However,
there are many variations. Common ones include:

- Sometimes two volumes are combined into one. If what was supposed to be volumes 8 and
  9 is instead published as one volume, it may be labeled "8-9".
- Supplementary volumes may be published in addition to the continuous series. These are
  labeled e.g. "Suppl. 8".

By default, all journal volume numbers should match the regular expression
`(Suppl\. )?\d{1,4}`: 1 to 4 digits, optionally preceded by "Suppl.". Citation groups
can set the _VolumeRegex_ tag to override this default and allow a different set of
volume numbers.

#### Issue

The _issue_ is optional. Normally, a volume is divided into issues, each of which is
published as a single physical journal. As with volumes, issues are normally written as
single numbers. Often, multiple issues are combined and the combined issue is then named
e.g. "3-4". Supplementary issues ("Suppl. 3") are also common.

The classical concept of the issue is of course tied to print journals, which are
published physically in issues. However, online publication is now the primary venue for
many journals, and the concept of the issue no longer cleanly applies. Some journals
have co-opted the issue concept for new purposes.

By default, issues must match the regular expression
`\d{1,3}|\d{1,2}-\d{1,2}|Suppl\. \d{1,2}`: a 1 to 3-digit number; two 1 or 2-digit
numbers around a hyphen; or "Suppl." followed by a 1 or 2-digit numbers. Citation groups
can set the _IssueRegex_ tag to allow a different set of issue numbers.

#### Start and end page

Classically, the page numbering in a journal is continuous through the whole volume, and
the start and end page are each numbers, the end page greater than or equal to the start
page. These two numbers are placed in the _start_page_ and _end_page_ field in the
database. For pre-publication "in press" articles, the _start_page_ is set to "in press"
and the _end_page_ is left blank.

With online publication, this is no longer the case: often each article has independent
page numbers. Articles are still labeled with numbers corresponding to the volume and
issue. Some examples of how these are treated in the database:

- In [Biology Letters](/cg/748), each article has three numbers (e.g., "17", "12" and
  "20210533"). The first one is the volume, the second one is the issue, and the third
  is placed in the _start_page_, with the _end_page_ left blank.
- In [The Science of Nature](/cg/1374), each article has two numbers (e.g., "108" and
  "23"), the volume and issue. The _start_page_ is set to 1 and the _end_page_ to the
  article's total page count.
- In [Palaeontologia Electronica](/cg/736), in the past articles were labeled e.g.
  "21.2.28A 1-12" on the title page. We therefore set _volume_ to 21, _issue_ to
  "2;28A", _start_page_ to 1, and _end_page_ to 12. More recently the title page now
  says "24(2):a24", which we treat similarly to _Biology Letters_, setting the
  _start_page_ to "a24" and leaving the _end_page_ blank.

By default, the database requires that the start and end page are both numeric and that
the end page is greater than or equal to the start page, and that both are at most four
digits. The _PageRegex_ tag can be used to customize this behavior. It has three fields:

- _start_page_regex_: If this is set, the _end_page_ may be omitted; if so, the
  _start_page_ must match this regex.
- _pages_regex_: If this is set, the start and end page must match this regex.
- _allow_standard_: If this flag is on, the start and end page are allowed if they
  follow the standard rules (both numeric), regardless of the values of the other two
  fields.

### Tricky cases

Some journals have tricky histories: they were renamed repeatedly, divided into
different series (and sometimes undivided again), or were known under different names.
Often citations in the literature do not precisely reflect this history, making it
difficult to provide accurate citations in the database. I use the following tags on
citation groups to help keep track of such cases:

- _YearRange_: Gives the first and last years when a journal was published. A
  maintenance script enforces that all articles and names in the citation group fall
  within the range.
- _Predecessor_: Reference to an older journal that this journal is a renaming of.
- _MustHaveSeries_: Enforce that all articles in this citation group must have the
  "series" field set.
- _DatingTools_: Brief discussion of resources to help determine the publication date
  for members of this citation group.

### Issue dates

For some journals, precise publication dates are available for each issue. I track these
in the database where possible and enforce that all articles in the issue use the
correct publication date. See [_Annals of Natural History_](/cg/106) for an example.

## Links

- [Dating](/docs/dating) has more information on publication dates

Short notes on several tricky areas of bibliography:

- [_Beiträge zur Paläontologie_](/docs/biblio/beitraege)
- [_Comptes Rendus_](/docs/biblio/comptes-rendus)
- [Zoology of the _Erebus_ and _Terror_](/docs/biblio/erebus-terror)
- [_Histoire naturelle des mammifères_](/docs/biblio/histnatmammiferes)
- [Publications of the Muséum national d'Histoire naturelle](/docs/biblio/mnhn)
- [_Neues Jahrbuch_](/docs/biblio/neues-jahrbuch)
- [_Proceedings of the Zoological Society of London_](/docs/biblio/pzsl)
- [University of California geological publications](/docs/biblio/uc-geology)
