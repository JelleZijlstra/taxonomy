# Release process

Reserve the Zenodo DOI before the release commit. Generate the exports after that
commit, deploy the website, then upload and publish the same version on Zenodo. Each
Zenodo stage is a separate command so an upload retry does not repeat Git tags, database
exports, or the EC2 deployment.

## Setup

Use the project's Python environment. Create a
[Zenodo personal access token](https://zenodo.org/account/settings/applications/tokens/new/)
and set it in the `ZENODO_TOKEN` environment variable. It needs `deposit:write` for
metadata and file uploads, and `deposit:actions` to create a new version and publish it.
Do not put the token in a tracked file or command-line argument.

The commands below use a sample version; choose the actual release version without a `v`
prefix:

```bash
release_version=26.10.0
```

## 1. Prepare the database, draft, DOI, and documentation

- Make sure lint is reasonably clean (`run_maintenance(skip_slow=False)`).
- Update caches and derived fields through the usual maintenance workflow:
  `compute_derived_fields(); warm_all_caches(); write_derived_data()`.
- Run `generate_summary_paragraph()` and update the "Size" section in `docs/home.md`.
- Review the release notes in `docs/release-notes.md`.

Create the new Zenodo version and reserve its DOI:

```bash
CLIRM_READONLY=1 python -m scripts.zenodo_release prepare "$release_version"
```

This uses the published version cited in `docs/home.md` as the preceding version. Use
`--previous RECORD_ID` to override it; this must be a specific published version's ID,
not the concept DOI identifying all versions. Use `--date YYYY-MM-DD` if preparing the
draft before its intended publication date; the default is today's UTC date.

Preparation inherits the previous record's descriptive metadata, updates the version and
date, and asks Zenodo to reserve the new DOI. It writes:

- `releases/VERSION.json`: the version, draft ID, reserved DOI, publication date,
  previous record ID, and API environment. This is a tracked release record, with no
  credentials or machine-specific paths.
- `docs/home.md`: the current project citation.
- `docs/release-notes.md`: the target version's heading and DOI badge. Existing notes
  and previous releases are retained.

Review those edits and the draft's metadata using the printed Zenodo URL. The reserved
DOI will resolve after publication. Rerunning `prepare` uses the saved draft and date;
it does not reserve another DOI. If interrupted before saving the release record,
Zenodo's new-version action reuses the existing unpublished version.

Commit the release record and documentation, together with the rest of the release
changes. Commit any frontend changes too. Both repositories must be clean before running
`aws/deploy.py`; that script creates the Git tags and pushes them.

## 2. Generate the CSV bundle

```bash
CLIRM_READONLY=1 python -m scripts.zenodo_release export "$release_version"
```

This runs the existing public exporters with database writes disabled:

| File             | Exporter             |
| ---------------- | -------------------- |
| `collection.csv` | `export_collections` |
| `name.csv`       | `export_names`       |
| `taxon.csv`      | `export_taxa`        |
| `ce.csv`         | `export_all_ces`     |
| `references.csv` | `export_articles`    |

The completed files and a checksum manifest go in `outputs/releases/VERSION/`, which is
ignored by Git. Files become available there only when all five exports succeed.
Rerunning `export` verifies and reuses that bundle. It refuses to overwrite a changed or
incomplete existing bundle; use `--files-dir NEW_DIRECTORY` to regenerate elsewhere.

Keep the database and derived data unchanged from export through deployment so the
website and CSV release agree. Export detects changes to those input files during
generation and aborts, but it does not lock the database against another process.

## 3. Cut the Git release and deploy to EC2

```bash
CLIRM_READONLY=1 python aws/deploy.py deploy "$release_version"
```

This uses the existing deployment workflow: validate the version and clean checkouts,
tag and push the repositories, save the local database backup, upload to EC2, and
restart the website. Verify that hesperomys.com is working before publishing Zenodo.

## 4. Upload and verify Zenodo files

```bash
CLIRM_READONLY=1 python -m scripts.zenodo_release upload "$release_version"
```

The command checks the saved release identity and local export manifest, skips remote
files with matching size and MD5 checksum, replaces changed files in the draft, and
streams each upload with progress reports. It retries connection failures, timeouts,
rate limits (honoring `Retry-After`), and transient server errors up to five attempts.
Use `--attempts N` **before** the subcommand to change the limit.

After an uncertain response, it checks whether Zenodo already has the complete file
before sending it again. An interrupted file upload otherwise restarts that file from
the beginning; the bucket API used here does not implement rsync-style resumption. Files
that have already verified are skipped when you rerun `upload`.

If the new version inherited obsolete files, the command reports them. Rerun with
`upload VERSION --prune` to delete unselected files from the draft after the selected
files verify. This flag treats the selected file list as the complete release.

To use a different generated bundle, pass `--files-dir DIRECTORY` to both `upload` and
`publish`. To upload previously exported files without a generated manifest, pass their
paths explicitly to both commands, for example:

```bash
CLIRM_READONLY=1 python -m scripts.zenodo_release upload "$release_version" /path/to/name.csv /path/to/taxon.csv
```

## 5. Review and publish

```bash
CLIRM_READONLY=1 python -m scripts.zenodo_release status "$release_version"
CLIRM_READONLY=1 python -m scripts.zenodo_release publish "$release_version"
```

`publish` checks the version, DOI, publication date, and exact file set against the
local release. It refuses missing files, extra files, or size/checksum differences. It
then requests publication and polls for confirmation. An uncertain or pending result is
reported as such; rerunning the command checks the current state before issuing any
publication request. An already published matching release succeeds without publishing
again.

Neither upload nor publication edits tracked files, so the DOI-bearing release commit
remains the one tagged and deployed in step 3. These commands do not create Git commits
or tags or perform an EC2 deployment themselves.

## Sandbox testing

The [Zenodo sandbox](https://sandbox.zenodo.org/) uses a separate account/token and test
DOIs. Set `ZENODO_SANDBOX_TOKEN`, and pass `--sandbox` before the subcommand:

```bash
CLIRM_READONLY=1 python -m scripts.zenodo_release --sandbox prepare "$release_version" --previous SANDBOX_RECORD_ID
CLIRM_READONLY=1 python -m scripts.zenodo_release --sandbox upload "$release_version"
```

Preparation needs a published predecessor in that sandbox account. Sandbox release
records go in ignored `outputs/zenodo-sandbox/`; production citations and release
records are never changed. The API and workflow tests run offline using simulated Zenodo
responses. See the [Zenodo API documentation](https://developers.zenodo.org/) for the
deposition, bucket-upload, and versioning endpoints.
