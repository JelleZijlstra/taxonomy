# Release process

- Make sure lint is reasonably clean (`run_maintenance(skip_slow=False)`)
- Update caches and derived fields
  (`compute_derived_fields(); warm_all_caches(); write_derived_data()`)
- Run `generate_summary_paragraph()` and update the "Size" section in `docs/home.md`
- Review the release notes in `docs/release-notes.md`
- Commit your changes. Make sure there are no untracked files, as `deploy.py` will fail
  if there are any.
- Generate export files
  (`export_collections("collection.csv"); export_names("name.csv"); export_taxa("taxon.csv"); export_all_ces("ce.csv")`)
- Create a new Zenodo release
- Add the new DOI to `home.md` and `release-notes.md`
- Run `aws/deploy.py deploy <version>` (where `version` is e.g. "23.8.0", not
  "v23.8.0"). This will tag the release in Git and push to GitHub, as well as to
  hesperomys.com.
- Verify that the website is still working.
