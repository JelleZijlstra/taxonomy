from pathlib import Path

from taxonomy import config


def test_parse_orcid_credentials(tmp_path: Path) -> None:
    filename = tmp_path / "taxonomy.ini"
    filename.write_text("""\
[taxonomy]
use_sqlite = true
orcid_client_id = APP-EXAMPLE
orcid_client_secret = example-secret
""")

    options = config.parse_config_file(filename)

    assert options.orcid_client_id == "APP-EXAMPLE"
    assert options.orcid_client_secret == "example-secret"  # noqa: S105
