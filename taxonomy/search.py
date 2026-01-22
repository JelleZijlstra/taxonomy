"""
Helpers for interacting with the full-text search database.

Schema for the database:

CREATE TABLE pages (
  article_id INTEGER NOT NULL,
  page_num   INTEGER NOT NULL,
  year       INTEGER,
  text       TEXT,
  PRIMARY KEY (article_id, page_num)
);

CREATE VIRTUAL TABLE pages_fts USING fts5(
  text,
  content='pages',
  content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);

-- Keep FTS in sync (required for content='pages')
CREATE TRIGGER pages_ai AFTER INSERT ON pages BEGIN
  INSERT INTO pages_fts(rowid, text) VALUES (new.rowid, new.text);
END;

CREATE TRIGGER pages_ad AFTER DELETE ON pages BEGIN
  INSERT INTO pages_fts(pages_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
END;

CREATE TRIGGER pages_au AFTER UPDATE OF text ON pages BEGIN
  -- delete old index entry
  INSERT INTO pages_fts(pages_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
  -- add new index entry
  INSERT INTO pages_fts(rowid, text) VALUES (new.rowid, new.text);
END;

-- Helpful for year filters
CREATE INDEX idx_pages_year ON pages(year);

PRAGMA optimize;

"""

import functools
import re
import sqlite3
import traceback
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from .config import get_options


@functools.cache
def get_database() -> sqlite3.Connection:
    """Return a cached sqlite3 connection to the search database.

    The file path is configured via `Options.search_db_filename`.
    """
    option = get_options()
    # static analysis: ignore[internal_error]
    return sqlite3.connect(option.search_db_filename)


def run_query(sql: str, args: tuple[object, ...] = ()) -> list[tuple[Any, ...]]:
    """Execute a SQL statement with parameters and return all rows.

    Commits the transaction implicitly using the connection context manager.
    """
    db = get_database()
    with db:
        cur = db.execute(sql, args)
        return cur.fetchall()


def run_many(sql: str, seq_of_args: Iterable[tuple[object, ...]]) -> None:
    """Execute an executemany() with implicit commit."""
    db = get_database()
    with db:
        db.executemany(sql, seq_of_args)


@dataclass(frozen=True)
class PageRecord:
    article_id: int
    page_num: int
    year: int | None
    text: str


def replace_article_pages(
    article_id: int, *, pages: Sequence[str], year: int | None
) -> None:
    """Replace all indexed pages for an article.

    - Deletes existing rows in `pages` for the given `article_id`.
    - Inserts new rows for each page in `pages` using 1-based `page_num`.
    The FTS index is kept in sync via triggers defined in the DB schema.
    """
    # Remove old pages first.
    remove_article_pages(article_id)

    if not pages:
        return

    # Insert new pages in a batch for efficiency.
    rows = (
        (article_id, i, year, preprocess_page_text(text))
        for i, text in enumerate(pages, start=1)
    )
    run_many(
        "INSERT INTO pages(article_id, page_num, year, text) VALUES (?, ?, ?, ?)", rows
    )


def remove_article_pages(article_id: int) -> None:
    """Delete all indexed pages for an article."""
    run_query("DELETE FROM pages WHERE article_id = ?", (article_id,))


@dataclass(frozen=True)
class SearchHit:
    article_id: int
    page_num: int
    year: int | None
    snippet: str


def search(
    query: str,
    *,
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[SearchHit]:
    """Run an FTS search against `pages_fts` and return highlighted snippets.

    - `query`: FTS5 query string (unicode61, diacritics-insensitive as configured).
    - `year_min`/`year_max`: optional inclusive year bounds.
    - `limit`: maximum number of results to return.
    """
    conditions = ["pages_fts MATCH ?"]
    args: list[object] = [query]
    if year_min is not None:
        conditions.append("pages.year >= ?")
        args.append(year_min)
    if year_max is not None:
        conditions.append("pages.year <= ?")
        args.append(year_max)

    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT pages.article_id, pages.page_num, pages.year,
               snippet(pages_fts, -1, '<b>', '</b>', '…', 10) as snippet
        FROM pages_fts
        JOIN pages ON pages_fts.rowid = pages.rowid
        WHERE {where_clause}
        ORDER BY bm25(pages_fts)
        LIMIT ? OFFSET ?
    """
    args.extend([limit, offset])
    try:
        rows = run_query(sql, tuple(args))
    except sqlite3.OperationalError as e:
        print("Error during search query:", e)
        traceback.print_exc()
        return []
    return [
        SearchHit(int(a), int(p), int(y) if y is not None else None, str(s))
        for a, p, y, s in rows
    ]


def get_article_pages(article_id: int) -> list[PageRecord]:
    """Fetch stored pages for an article (for inspection or debugging)."""
    rows = run_query(
        "SELECT article_id, page_num, year, text FROM pages WHERE article_id = ? ORDER BY page_num",
        (article_id,),
    )
    return [
        PageRecord(int(a), int(p), int(y) if y is not None else None, str(t))
        for a, p, y, t in rows
    ]


_EOL_HYPHEN_RE = re.compile(r"(?<=\w)-\s*\n\s*(?=\w)")


def preprocess_page_text(text: str) -> str:
    r"""Normalize PDF text for indexing.

    - Unicode normalize to NFKC
    - Remove soft hyphens (U+00AD)
    - Join end-of-line hyphenations: ``taxo-\nnomy`` -> ``taxonomy``
    """
    t = unicodedata.normalize("NFKC", text)
    # Remove discretionary hyphen
    t = t.replace("\u00ad", "")
    # Join hyphenated line breaks conservatively: letter-\nletter
    t = _EOL_HYPHEN_RE.sub("", t)
    return t
