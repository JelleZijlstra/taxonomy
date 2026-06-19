# Reference Matching Scripts

Run these commands from the repository root with the taxonomy Python environment active:

```bash
python refmatch/<script>.py
```

The blessed output directory is `refmatch/output/`. The scripts create it as needed, and
generated CSVs in that directory are intentionally ignored by Git.

The normal workflow has three stages:

1. Extract source references into a raw CSV.
2. Parse the raw references into a structured CSV.
3. Match the parsed references against taxonomy Articles and cached external
   identifiers.

The source-specific extraction scripts perform stages 1 and 2 in one run. The
source-specific matcher scripts perform stage 3.

## MSW3

Blessed workflow:

```bash
python refmatch/msw3_refs.py
python refmatch/msw3_match_refs.py
```

Outputs:

- `refmatch/output/msw3-refs.csv`
- `refmatch/output/msw3-refs-parsed.csv`
- `refmatch/output/msw3-refs-taxonomy-matches.csv`

`msw3_refs.py` reads the local MSW3 Word document from its default path and uses macOS
`textutil` to extract text. `msw3_match_refs.py` reads the parsed CSV from
`refmatch/output/` and writes the matched CSV there too.

## HMW Chiroptera

Blessed workflow:

```bash
python refmatch/chiroptera_hmw_refs.py
python refmatch/chiroptera_hmw_match_refs.py
```

Outputs:

- `refmatch/output/chiroptera-hmw-refs.csv`
- `refmatch/output/chiroptera-hmw-refs-parsed.csv`
- `refmatch/output/chiroptera-hmw-refs-taxonomy-matches.csv`

`chiroptera_hmw_refs.py` reads `data_import/data/chiroptera-hmw-refs.pdf` by default and
uses Poppler's `pdftotext` and `pdftohtml`. The matcher reads and writes under
`refmatch/output/`.

## Lookup Policy

By default, both matcher scripts use cached DOI and BHL lookups:

- `--doi-mode=cached`
- `--bhl-mode=cached`

That is the preferred mode for routine runs because it is deterministic and does not
make fresh network calls. Use `--help` to see the escape hatches for custom inputs,
outputs, or network lookups, but avoid documenting alternate invocations as part of the
normal workflow.
