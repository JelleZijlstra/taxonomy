# Article

The term "article" is used for all references used in the database, including books,
dissertations, and websites. Most references are in my physical or electronic
library, but a few are of important works that I do not have a persistent copy of.

## Fields

Articles have the following fields:

- _name_: Unique name for the citation. This matches the file name for files in my
  electronic library, but mostly does not appear in the web version of the database.
- _author_tags_: The authors of the article, as references to [persons](/docs/person).
- _year_: Year the article was published
- _title_: Title of the article
- _citation group_: Reference to the [citation group](/docs/citation-group) the article
  belongs to: the journal for journal articles, city of publication for books, and university
  for dissertations.
- _series_: Series of publication, used with some journals. Also used for the name of the
  degree (e.g., "PhD") for dissertations.
- _volume_: Volume of publication for journals
- _issue_: Issue of publication for journals
- _start page_: First page of a journal article
- _end page_: Last page of a journal article
- _url_: URL at which the article can be found
- _doi_: Digital Object Identifier of the article
- _publisher_: Name of the publisher (for books) or university (for dissertations)
- _pages_: Total number of pages (for books and dissertations)
- _parent_: Article that this article belongs to; used for book chapters (the parent
  is the book), supplementary materials, and aliases
