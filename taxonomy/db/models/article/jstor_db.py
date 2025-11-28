"""Helpers to query the local JSTOR metadata SQLite database.

Relies on get_options().jstor_db_filename pointing to the database created by
scripts/import_jstor_jsonl_to_sqlite.py.

Note this database is incomplete: it contains only those articles that are part
of JSTOR's full-text analysis program:
https://support.jstor.org/hc/en-us/articles/32479181127575-JSTOR-Text-Analysis-Support-Getting-Started
About 64% of JSTOR identifiers in the database have entries in this local DB
(as of November 2025).

"""

import functools
import sqlite3
from dataclasses import dataclass
from typing import Any

from taxonomy.config import get_options
from taxonomy.db import helpers


@functools.cache
def _get_conn() -> sqlite3.Connection | None:
    path = get_options().jstor_db_filename
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _query(sql: str, args: tuple[Any, ...]) -> list[sqlite3.Row]:
    conn = _get_conn()
    assert conn is not None, "JSTOR database not configured"
    cur = conn.execute(sql, args)
    return list(cur.fetchall())


def get_by_ithaka_doi(doi: str) -> dict[str, Any] | None:
    rows = _query("SELECT * FROM jstor WHERE ithaka_doi = ?", (doi,))
    if not rows:
        return None
    return dict(rows[0])


def get_candidates_by_journal_and_volume(
    journal_name: str, volume: str | None
) -> list[dict[str, Any]]:
    # Match on exact journal name (case-insensitive, with optional trailing period)
    j1 = journal_name
    j2 = journal_name.rstrip(".")
    if volume:
        rows = _query(
            """
            SELECT * FROM jstor
            WHERE (is_part_of = ? OR is_part_of = ?)
              AND issue_volume = ?
            """,
            (j1, j2, volume),
        )
    else:
        rows = _query(
            "SELECT * FROM jstor WHERE (is_part_of = ? OR is_part_of = ?)", (j1, j2)
        )
    return [dict(r) for r in rows]


def title_similarity(a: str, b: str) -> float:
    a_s = helpers.simplify_string(a)
    b_s = helpers.simplify_string(b)
    if not a_s and not b_s:
        return 1.0

    # Jaccard-like similarity using character bigrams as a simple, fast proxy
    def bigrams(s: str) -> set[str]:
        return {s[i : i + 2] for i in range(max(0, len(s) - 1))}

    a_bigrams = bigrams(a_s)
    b_bigrams = bigrams(b_s)
    if not a_bigrams or not b_bigrams:
        return 0.0
    return len(a_bigrams & b_bigrams) / len(a_bigrams | b_bigrams)


@dataclass(frozen=True)
class JSTORCandidate:
    row: dict[str, Any]
    similarity: float


def find_best_jstor_match(
    *, journal_name: str, volume: str | None, title: str | None
) -> JSTORCandidate | None:
    if not title:
        return None
    candidates = get_candidates_by_journal_and_volume(journal_name, volume)
    if not candidates:
        return None
    best: JSTORCandidate | None = None
    for row in candidates:
        row_title = (row.get("title") or "").strip()
        if not row_title:
            continue
        sim = title_similarity(title, row_title)
        cand = JSTORCandidate(row=row, similarity=sim)
        if best is None or cand.similarity > best.similarity:
            best = cand
    return best
