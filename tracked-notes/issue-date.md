# IssueDate

An IssueDate records the publication date of a journal issue (or a separately paginated
portion of it). Its CitationGroup, series, volume, issue, and page range identify the
publication unit. Keep bibliographic evidence in
`IssueDateTag.Comment(text, optional_source=article)`, including the source page or
table, the printed date, and any interpretation needed to select publication over other
events such as signing to press. A printing or receipt observation is not necessarily a
publication date.

## Calendars and precision

`date` preserves the source calendar and precision. Gregorian and Julian dates use
`YYYY`, `YYYY-MM`, or `YYYY-MM-DD`. Gregorian is the default.
`IssueDateTag.Calendar(Calendar.julian)` marks Julian dates; an explicit Gregorian tag
is also accepted. Conflicting Calendar tags are invalid. The enum can be extended when
another calendar is implemented; unsupported calendars must fail validation and
conversion explicitly. `Calendar.french_republican` supports the historical years I–XIV.
Store Republican years without leading zeros (`1` through `14`), followed optionally by
a month name and day: `12`, `12-Brumaire`, `12-Brumaire-1`. Day numbers also omit
leading zeros. Use the names Vendémiaire, Brumaire, Frimaire, Nivôse, Pluviôse, Ventôse,
Germinal, Floréal, Prairial, Messidor, Thermidor, and Fructidor. Month names are
case-insensitive, with the accented spellings shown here. Use `Complémentaires` for the
five complementary days (six in years III, VII, and XI). Thus `12-Brumaire` converts to
the adopted date `1803-11-22`, while `12-Brumaire-1` is `1803-10-24`;
`3-Complémentaires-6` is 22 September 1795. Padded years/days and numeric Republican
months are not accepted. Preserve the original spelling and Roman year in the source
comment. A source date range spanning months still requires review; do not silently
discard its starting month.

This is the historical concordance described by
[IMCCE](https://promenade.imcce.fr/fr/pages2/277.html), starting on 22 September 1792.
No arithmetic/proleptic extension or 1871 revival is assumed: years outside I–XIV fail
explicitly. Year XIV can be converted as a complete calendar year, even though civil use
ended during it on 1 January 1806; an imprint in year XIV is not itself proof of the
work's actual publication date.

Article `PublicationDate` tags use the same enum through their optional `calendar`
attribute. Omitted values are Gregorian, and old serialized tags need no migration. Keep
PublicationDate comments concise: source quotations and page/source references. Keep
conversion calculations and curation explanations in recommendation evidence, rather
than in the permanent tag comment. A Gregorian date printed alongside the Republican
date is still source evidence and may be retained in the quote. Date comparisons use
Gregorian bounds when calendars differ: compatible observations are intersected before
selecting the latest possible day. For example, a dual-dated imprint `an XIV / 1805`
yields 31 December 1805, within both years; disjoint observations still require review.
Article `year` always remains Gregorian. The PDF-date extractor does not identify source
calendars, so its automatic mismatch check skips custom-calendar tags instead of
comparing them to unlabelled numeric dates.

`get_gregorian_date()` returns the date used by Article year inference. Gregorian input
retains its precision. Julian month-only and year-only dates are resolved to the last
day of that month or year **in the Julian calendar**, then converted to Gregorian. The
same source-calendar last-day convention applies to Republican months and years. Thus
Julian `1910-09` becomes the adopted date `1910-10-13`, and `1913-12` becomes
`1914-01-13`. These are nomenclatural defaults, not observed exact dates. The source
record remains unchanged. A converted date is not stored redundantly.

Validation applies the source calendar's leap-year rules; e.g. Julian 1900-02-29 is
valid. Conversion handles the changing calendar offset rather than adding a fixed number
of days. Date ranges and nonnumeric dates require review; they are not supported
IssueDate dates. Adding IssueDates does not itself write Article or Name dates; normal
Article lint picks up the derived date later. Existing date precedence, including
separate-publication exceptions, is unchanged.

## Pagination convention

Store page numbers as strings. Matching uses the numeric value **and** the page
sequence, never the value alone:

| Form         | Meaning                                                       | Examples                        |
| ------------ | ------------------------------------------------------------- | ------------------------------- |
| Arabic       | Ordinary main-text pagination                                 | `1`, `100`                      |
| Leading zero | A distinct printed sequence with a leading zero               | `01`, `0100`                    |
| Roman        | Roman-numbered sequence, case-insensitive                     | `i`, `XVI`                      |
| Qualified    | An explicitly identified independent section                  | `compte-rendu:1`, `appendix:iv` |
| Bis          | The existing second occurrence of an Arabic-numbered sequence | `160bis`, `0160bis`             |

A qualifier is a stable lowercase ASCII identifier made of letters, digits and hyphens,
starting with a letter. Use the section's established name, consistently within the
journal; for Ezhegodnik's independently Arabic-numbered reports use `compte-rendu`.
Qualifiers are curation metadata, not claimed printed prefixes. Quote the printed range
and section heading in the evidence. Main text can remain unqualified when other
colliding sequences are explicitly qualified.

`01` and `1` never match; neither do `i` and `1`, qualified and unqualified pages, or
ordinary and `bis` pages. Roman upper/lower case is equivalent. Extra leading zeroes do
not create further sequences: `001` and `01` both denote page 1 of the leading-zero
sequence. When two sections use identical printed notation, use qualifiers rather than
inventing more zeroes or putting section names in `series` or `issue`.

Both endpoints must use the same sequence and must be in ascending order. An Article
must use the same convention to match an IssueDate. Store one IssueDate per contiguous
range/sequence when an issue includes multiple sequences; repeat the issue identity and
date and retain the source citation on each record. Both endpoints may be absent for a
whole unit without a known range; this permits fallback only when that is the sole
candidate for its volume and series.

Distinct issues with overlapping ranges remain ambiguous unless the Article's issue
disambiguates them. Identical IssueDate records count as equivalent only when their
calendars also agree.
