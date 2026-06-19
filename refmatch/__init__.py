"""

refmatch is a set of tools for parsing reference lists and matching them to other
bibliographies.

We generally organize the pipeline in three stages:

- Stage 1: Find the list of references. The output is essentially a flat list of strings, one
  string per reference.
- Stage 2: Parse the reference strings into structured data. The output is a CSV with one row
  per reference, and columns for the author, year, title, etc.
- Stage 3: Match the parsed references to a bibliography of known articles, such as that
  in the taxonomy database. The `taxonomy.refmatch` package contains a matcher module for
  matching to the data in the taxonomy database, and also looking for DOIs, BatLit matches, and
  Biodiversity Heritage Library matches.

"""
