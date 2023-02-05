# Publication dates

The Principle of Priority is central to zoological nomenclature: if two names are
considered synonyms, the oldest one prevails. Thus, it is important to determine when
names and other nomenclatural actions were published. This page discusses some common
issues and pitfalls around dates:

Historically, precise dating has not been a priority for the database. I am now trying
to improve the situation, but there are still many dubious dates. In the past I recorded
dates only up to the year, but because priority can depend on not just the year but also
the month and day of publication, I now record precise dates where possible.

## Electronic publication

Historically, the Code has required print publication as a criterion for availability.
However, in recent years publishing has moved largely online, and the Code has moved to
allow electronic publishing ([ICZN, 2012](/a/59568)). Electronic-only publications, and
electronic-only preprints, can now (after 2011) make a name available, but only if the
name is registered. In practice, this means the publication must contain an LSID, or
ZooBank identifier, in the text.

My procedure for determining publication dates for recently published names is usually
as follows:

- Determine the DOI (digital object identifier) for the article
- Use the [CrossRef API](https://api.crossref.org/swagger-ui/index.html) to determine
  the print and online publication dates for the article
- Try to find an LSID for the article. My current procedure is to search for the LSID
  for any names published in the artice on the [ZooBank API](https://zoobank.org/Api),
  and use the name's ZooBank data to find the LSID for the article.
- If there is an LSID, use the online publication date; else use the print publication
  date.

Common problems with this procedure include:

- Sometimes an article has an LSID, but it is not present in the article itself, meaning
  it cannot help make the name available.
- Some articles publishing new names do have LSIDs but my scripts do not find them.
- Sometimes the CrossRef API dates are incorrect or incomplete; for example, for
  [_Acta Palaeontologica Polonica_](/cg/675) articles the print publication date is
  usually not recorded.

If there is evidence that the dates published by the standard procedure are not correct,
I will adjust them.

## Pitfalls

_Publication in parts_. Many old (19th-century) books were published in installments,
often over a period of several years. The breaks are not always obvious in the work
itself, and may be in the middle of a continuous text, even in the middle of a sentence.
Often external sources have to be consulted to determine the correct pagination and
publication dates for each part. The title page of the book will usually give only the
publication date of the last installment.

_Incorrect printed dates_. Most works include at least a year of publication somewhere
in their text, and by default, this date is presumed to be the correct publication date.
However, sometimes it is known from other evidence that the printed publication date is
incorrect.

_Date of reading vs. date of publication_. Several historically prominent journals, such
as the [_Proceedings of the Zoological Society of London_](/cg/1) (_PZSL_), originated
as a record of the meetings of a learned society. Thus, the date when a paper is read is
often recorded in the journal. However, what makes a name available is its publication
in print, not the reading of a paper at a society, so these dates are not to be used as
publication dates. Often the actual date of publication was long after the date the
paper was read. This is especially jarring for _PZSL_ because the year of reading is the
volume number; it is common for a name to appear in e.g. the 1860 volume, but be
published only in 1861.

_Multiple publication_. Today there is a strong norm that when a paper is published, the
research in it is new and not published elsewhere. In the past, this wasn't the case,
and nearly identical pieces were regularly published in multiple places. To determine
the original publication, it is then important to determine exactly when each was
published.

## Sources

The bibliography of natural history has been a subject of study since the great
[Charles Davies Sherborn](/h/53933) (1861–1942), who produced many papers determining
the dates of publication of important systematic works. Since his time, many other
useful sources have been published determining dates of publication.

These include:

- [Jackson & Groves (2015)](/a/34474), who list precise publication dates for all
  references where possible and provide an invaluable list of resources used for
  determining publication dates.
- [Sherborn (1922)](/a/59291), which includes a bibliography of many important
  19th-century books.
- [Duncan (1937)](/a/14498), providing precise publication dates for _PZSL_, one of the
  most prolific and trickiest to date zoological journals.
- [Woodward (1903)](/a/48772) (and subsequent entries in the series), a complete and
  thorough catalog of books in the British Museum's library.
