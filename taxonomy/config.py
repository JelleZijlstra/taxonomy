import configparser
import functools
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import NamedTuple


class Options(NamedTuple):
    new_path: Path = Path()
    library_path: Path = Path()
    data_path: Path = Path()
    parserdata_path: Path = Path()
    db_filename: Path = Path()
    urlcache_filename: Path = Path()
    derived_data_filename: Path = Path()
    photos_path: Path = Path()
    pdf_text_path: Path = Path()

    db_server: str = ""
    db_username: str = ""
    db_password: str = ""
    db_name: str = ""
    use_sqlite: bool = True

    googlekey: str = ""
    googlecus: str = ""
    crossrefid: str = ""

    paleobiodb_cookie: Mapping[str, str] = {}

    taxonomy_repo: Path = Path()
    hesperomys_repo: Path = Path()
    pem_file: Path = Path()
    hesperomys_host: str = ""

    aws_key: str = ""
    aws_secret_key: str = ""
    aws_cloudsearch_domain: str = ""
    aws_cloudsearch_document_endpoint: str = ""
    aws_cloudsearch_search_endpoint: str = ""

    mdd_sheet: str = ""
    mdd_worksheet_gid: int = 0
    mdd_higher_worksheet_gid: int = 0
    mdd_species_worksheet_gid: int = 0
    mdd_journals_worksheet_gid: int = 0
    book_sheet: str = ""
    book_sheet_gid: int = 0

    bhl_api_key: str = ""
    zotero_key: str = ""
    openai_key: str = ""

    geojson_path: Path = Path()

    @property
    def burst_path(self) -> Path:
        return self.new_path / "Burst"


def error(message: str) -> None:
    print(message, file=sys.stderr)


def parse_path(section: Mapping[str, str], key: str, base_path: Path) -> Path:
    if key not in section:
        return base_path
    else:
        raw_path = section[key]
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = base_path / path
        return path


_network_available: bool | None = None


def set_network_available(*, value: bool) -> None:
    global _network_available
    _network_available = value


def is_network_available() -> bool:
    if _network_available is not None:
        return _network_available
    return _is_network_available_from_env()


@functools.cache
def _is_network_available_from_env() -> bool:
    return not bool(os.environ.get("TAXONOMY_NO_NETWORK"))


@functools.cache
def parse_config_file(filename: Path) -> Options:
    parser = configparser.ConfigParser()
    parser.read(filename)
    try:
        section = parser["taxonomy"]
    except KeyError:
        error(f'config file {filename} missing required section "taxonomy"')
        return Options()
    else:
        base_path = filename.parent
        db_filename = parse_path(section, "db_filename", base_path)
        if "TAXONOMY_DB_FILENAME" in os.environ:
            db_filename = db_filename.parent / os.environ["TAXONOMY_DB_FILENAME"]
        return Options(
            new_path=parse_path(section, "new_path", base_path),
            library_path=parse_path(section, "library_path", base_path),
            data_path=parse_path(section, "data_path", base_path),
            parserdata_path=parse_path(section, "parserdata_path", base_path),
            derived_data_filename=parse_path(
                section, "derived_data_filename", base_path
            ),
            photos_path=parse_path(section, "photos_path", base_path),
            pdf_text_path=parse_path(section, "pdf_text_path", base_path),
            db_filename=db_filename,
            urlcache_filename=parse_path(section, "urlcache_filename", base_path),
            db_server=section.get("db_server", ""),
            db_username=section.get("db_username", ""),
            db_password=section.get("db_password", ""),
            db_name=section.get("db_name", ""),
            use_sqlite=section.getboolean("use_sqlite") or False,
            googlekey=section.get("googlekey", ""),
            googlecus=section.get("googlecus", ""),
            crossrefid=section.get("crossrefid", ""),
            paleobiodb_cookie=(
                json.loads(section["paleobiodb_cookie"])
                if "paleobiodb_cookie" in section
                else {}
            ),
            taxonomy_repo=parse_path(section, "taxonomy_repo", base_path),
            hesperomys_repo=parse_path(section, "hesperomys_repo", base_path),
            pem_file=parse_path(section, "pem_file", base_path),
            hesperomys_host=section.get("hesperomys_host", ""),
            aws_key=section.get("aws_key", ""),
            aws_secret_key=section.get("aws_secret_key", ""),
            aws_cloudsearch_domain=section.get("aws_cloudsearch_domain", ""),
            aws_cloudsearch_search_endpoint=section.get(
                "aws_cloudsearch_search_endpoint", ""
            ),
            aws_cloudsearch_document_endpoint=section.get(
                "aws_cloudsearch_document_endpoint", ""
            ),
            mdd_sheet=section.get("mdd_sheet", ""),
            mdd_worksheet_gid=int(section.get("mdd_worksheet_gid", "0")),
            mdd_journals_worksheet_gid=int(
                section.get("mdd_journals_worksheet_gid", "0")
            ),
            mdd_species_worksheet_gid=int(
                section.get("mdd_species_worksheet_gid", "0")
            ),
            mdd_higher_worksheet_gid=int(section.get("mdd_higher_worksheet_gid", "0")),
            bhl_api_key=section.get("bhl_api_key", ""),
            zotero_key=section.get("zotero_key", ""),
            geojson_path=parse_path(section, "geojson_path", base_path),
            openai_key=section.get("openai_key", ""),
            book_sheet=section.get("book_sheet", ""),
            book_sheet_gid=int(section.get("book_sheet_gid", "0")),
        )


def get_options() -> Options:
    if "TAXONOMY_CONFIG_FILE" in os.environ:
        config_file = Path(os.environ["TAXONOMY_CONFIG_FILE"])
    else:
        config_file = Path(__file__).parent.parent / "taxonomy.ini"
    return parse_config_file(config_file)
