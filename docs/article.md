# Article

The term "article" is used for all references used in the database, including books,
dissertations, and websites. Most references are in my physical or electronic library,
but a few are of important works that I do not have a persistent copy of.

## Fields

Articles have the following fields:

- _name_: Unique name for the citation. This matches the file name for files in my
  electronic library, but mostly does not appear in the web version of the database. See
  [article naming](/docs/article-naming) for the rules governing the name.
- _author_tags_: The authors of the article, as references to [persons](/docs/person).
- _year_: Year the article was published. Ideally this should be derived from
  _PublicationDate_ tags.
- _title_: Title of the article. This is in sentence case, except for English book
  titles in title case.
- _citation group_: Reference to the [citation group](/docs/citation-group) the article
  belongs to: the journal for journal articles, city of publication for books, and
  university for dissertations.
- _series_: Series of publication, used with some journals. Also used for the name of
  the degree (e.g., "PhD") for dissertations. See
  [the citation group documentation](/docs/citation-group) for more detail on how to use
  this and the next few fields.
- _volume_: Volume of publication for journals
- _issue_: Issue of publication for journals
- _start page_: First page of a journal article
- _end page_: Last page of a journal article
- _page number_: Page number (or other identifier) for journals that do not use
  contiguous page numbering systems (often modern online-only journals).
- _url_: URL at which the article can be found
- _doi_: Digital Object Identifier of the article
- _publisher_: Name of the publisher (for books) or university (for dissertations)
- _pages_: Total number of pages (for books and dissertations)
- _parent_: Article that this article belongs to; used for book chapters (the parent is
  the book), supplementary materials, and aliases
- _tags_: Arbitrary information about the article. Important ones include:
  - _PublicationDate_: evidence for when the article was published. Ideally articles
    should have a tag with the "internal" kind indicating internal evidence for when the
    article was published. Its optional `calendar` attribute defaults to Gregorian; use
    `Calendar.julian` or `Calendar.french_republican` to preserve a source date in
    another calendar. Inference converts these dates to Gregorian for `year`. Preserve
    the printed wording in the comment. See
    [calendar conventions](issue-date.md#calendars-and-precision). Prefix the date with
    `<` for evidence that publication preceded the date, or `>` for evidence that it
    followed the date. See [publication bounds](#publication-bounds).
  - _PublishedBefore_: evidence that this article was published before another Article.
    Its `article` field references that work; its `comment` explains the evidence and
    its scope (for example, a citation to an already published installment).
  - _InitialsOnly_: indicating that the article's authors are given only with initials,
    not full names.
  - _IgnoreORCIDProfile_: a reviewed exception for one ORCID profile that claims the
    Article's DOI even though that profile belongs to none of the Article's authors.
    Unlike a general lint ignore, this does not suppress other ORCID profiles that may
    later provide valid author evidence for the same Article.

## Publication bounds

`PublicationDate(external, "<1799-04-14", comment)` records a terminus ante quem;
`PublicationDate(external, ">1798-07", comment)` records a terminus post quem. The
prefix remains in the source tag. The `year` field stores the adopted date without a
prefix. Inference uses the latest possible date consistent with the selected evidence:

| Evidence                     | Inferred `year`                           |
| ---------------------------- | ----------------------------------------- |
| `<1799-04-14`                | `1799-04-14`                              |
| `<1799-04-14`, `<1799-10-06` | `1799-04-14`                              |
| `>1798-07`, `<1799-04-14`    | `1799-04-14`                              |
| `>1798-07` alone             | No inferred date; there is no upper bound |
| `>1800`, `<1799`             | Lint error: contradictory evidence        |

Inference adopts the cutoff itself, rather than manufacturing a preceding day. Dates
have no time of day: a publication and its witness may occur on the same calendar day.
Partial dates retain their precision and are compared using their possible calendar
intervals. For example, `<1799-04` adopts `1799-04`. The optional source calendar also
applies to bounds; conversion happens before comparison.

The existing source precedence still selects ordinary date evidence (decisions, external
evidence, internal evidence, then eligible bibliographic metadata). An external upper
bound can therefore supersede a later title-page year. Explicit `<` and `>` constraints
from all sources are checked together. Lower bounds alone do not supply an adopted date
or hide an upper date from the next eligible source. An incompatible stored `year` is
reported by lint rather than being replaced with a lower bound.

Prefer `PublishedBefore(other_article, comment)` when the witness is another cataloged
work. For example, an Audebert installment cited by Bechstein can reference the
Bechstein Article; the installment need not repeat the evidence for Bechstein's date.
Inference follows that Article's publication evidence, including further
`PublishedBefore` links, and uses its `year` only when there is no date evidence to
infer from. The resulting upper bound has external-evidence priority. A witness with
only a lower bound cannot provide an upper date. Circular references and contradictory
bounds produce lint errors. Chapter and supplement inference preserves the parent's
actual bounds when checking local constraints. References are resolved afresh, so a
witness's corrected evidence changes the inferred date without first requiring its
stored `year` to be updated.
