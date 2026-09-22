# Taxonomy games

The Hesperomys frontend has five games:

- **Order by Family**: type the order containing a family, with autocomplete
  suggestions.
- **Families by Order**: enter every family in an order.
- **Family by Genus**: enter a genus's family.
- **Species by Genus**: enter any accepted species epithet or binomial.
- **Genera by Family**: enter every genus in a family.

All games use Hesperomys classification and include living and recently extinct mammals
with valid base names. A continent selection restricts both prompts and answers. For
example, Species by Genus only accepts species recorded on that continent, even when the
genus has species elsewhere. Genera by Family also filters its subfamily/tribe hints and
applies hard mode's size threshold to the remaining genera. Families by Order similarly
filters the required families and offers hard mode for orders with more than 10
remaining families. Both listing games offer letter hints and retry skipped or imperfect
prompts until completed flawlessly. Progress is separate for each game, continent, and
(where applicable) difficulty; existing worldwide saves are retained.

## Regenerating the data

Export the MDD species sheet as CSV, including `id`, `sciName`, and
`continentDistribution`. The generator reads the CSV and database without modifying
either:

```sh
CLIRM_READONLY=1 python -m scripts.generate_games_data --mdd-csv /path/to/mdd-species.csv
```

The output directory defaults to `hsweb/game_data/` and can be changed with
`--output-dir`. The classification files are `family_order.json`, `order_families.json`,
`genus_family.json`, `genus_species.json`, `family_genera.json`, and
`family_genera_grouped.json`. `geography.json` supplies the continent indexes.
`geography_report.json` records the matching result for every eligible local species,
including the MDD ID, source name, and verbatim continent field. Both geography files
record the source CSV filename, SHA-256, generation time, and coverage counts. Keep the
source CSV with the report for reproducibility.

Without `--mdd-csv`, only classification files are regenerated; existing geography files
are left untouched. Always supply a current MDD CSV when refreshing data for deployment.
Generated files are ignored by git; they must be deployed with the frontend build. No
GraphQL/schema changes are required.

## Geography semantics

Ranges are joined only through existing `TaxonTag.MDD` IDs, with one local species and
one MDD row per ID. There is no scientific-name or type-locality fallback. Missing,
multiple, duplicate, or shared IDs are reported and excluded from continent quizzes.
Hesperomys determines each matched species's current genus, family, and order; MDD
supplies its range, not its classification.

Only unqualified values in `continentDistribution` count as positive evidence.
Question-marked ranges are excluded; `NA` and `Domesticated` are not continent records.
Unexpected continent labels fail generation instead of being silently interpreted. MDD's
`Oceania (Continent)` is displayed as **Oceania**. No claim is made that every included
species is native: these filters follow the MDD continent field, not an origin/presence
assessment.

Unmatched species and species without confirmed continent data remain playable
worldwide. Continent quizzes are therefore source-dependent subsets, not complete
regional checklists. Missing geography data leaves worldwide play available but never
silently replaces a selected continent with worldwide play. Empty selections show an
explicit message rather than a completed game.

## Validation

```sh
CLIRM_READONLY=1 python -m pytest -q scripts/test_generate_games_data.py
```

In the Hesperomys frontend checkout:

```sh
CI=true npm test -- --watchAll=false --runInBand src/games/games.test.tsx
npm run build
```
