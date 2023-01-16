import configparser
import functools
import json
import os
import socket
import sys
from pathlib import Path
from typing import NamedTuple
from collections.abc import Mapping


class Options(NamedTuple):
    new_path: Path = Path()
    library_path: Path = Path()
    data_path: Path = Path()
    parserdata_path: Path = Path()
    db_filename: Path = Path()
    derived_data_filename: Path = Path()
    cached_data_filename: Path = Path()
    photos_path: Path = Path()

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
        # Hack for non-essential computer.
        if "zijlistra" in socket.gethostname().lower():
            db_filename = db_filename.parent / "taxonomy2.db"
        return Options(
            new_path=parse_path(section, "new_path", base_path),
            library_path=parse_path(section, "library_path", base_path),
            data_path=parse_path(section, "data_path", base_path),
            parserdata_path=parse_path(section, "parserdata_path", base_path),
            derived_data_filename=parse_path(
                section, "derived_data_filename", base_path
            ),
            cached_data_filename=parse_path(section, "cached_data_filename", base_path),
            photos_path=parse_path(section, "photos_path", base_path),
            db_filename=db_filename,
            db_server=section.get("db_server", ""),
            db_username=section.get("db_username", ""),
            db_password=section.get("db_password", ""),
            db_name=section.get("db_name", ""),
            use_sqlite=section.getboolean("use_sqlite"),
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
        )


def get_options() -> Options:
    if "TAXONOMY_CONFIG_FILE" in os.environ:
        config_file = Path(os.environ["TAXONOMY_CONFIG_FILE"])
    else:
        config_file = Path(__file__).parent.parent / "taxonomy.ini"
    return parse_config_file(config_file)
