"""Prepare a DOI-bearing release commit, then upload and publish its exports."""

import datetime
import json
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from taxonomy.apis.zenodo import (
    PRODUCTION_URL,
    SANDBOX_URL,
    UploadFile,
    ZenodoClient,
    ZenodoError,
    files_by_name,
    is_published,
    require_draft,
)

# Preserve the public filenames and exporters from RELEASE.md.
STANDARD_EXPORTS = {
    "collection.csv": "export_collections",
    "name.csv": "export_names",
    "taxon.csv": "export_taxa",
    "ce.csv": "export_all_ces",
    "references.csv": "export_articles",
}


@dataclass(frozen=True)
class Release:
    version: str
    deposition_id: int
    doi: str
    publication_date: str
    previous_deposition_id: int
    api_url: str

    @property
    def draft_url(self) -> str:
        return f"{self.api_url.removesuffix('/api')}/uploads/{self.deposition_id}"

    def validate(self, deposition: dict[str, Any]) -> None:
        metadata = deposition["metadata"]
        if (
            deposition["id"] != self.deposition_id
            or metadata.get("version") != self.version
            or metadata.get("publication_date") != self.publication_date
            or get_doi(deposition) != self.doi
        ):
            raise ZenodoError(
                "Zenodo's ID, DOI, version or publication date differs from the release record"
            )


def validate_version(version: str) -> None:
    if not re.fullmatch(r"\d{2}\.(?:[1-9]|1[0-2])\.\d+", version):
        raise ZenodoError("Expected a release version such as 26.10.0")


def release_path(repo: Path, version: str, api_url: str) -> Path:
    validate_version(version)
    if api_url == PRODUCTION_URL:
        return repo / "releases" / f"{version}.json"
    if api_url == SANDBOX_URL:
        return repo / "outputs" / "zenodo-sandbox" / f"{version}.json"
    raise ZenodoError(f"Unsupported API URL: {api_url}")


def load_release(repo: Path, version: str, api_url: str) -> Release:
    path = release_path(repo, version, api_url)
    if not path.exists():
        raise ZenodoError(f"No release record at {path}; run prepare first")
    release = Release(**json.loads(path.read_text()))
    if release.version != version or release.api_url != api_url:
        raise ZenodoError(f"Wrong version or Zenodo environment in {path}")
    return release


def get_doi(deposition: dict[str, Any]) -> str:
    metadata = deposition["metadata"]
    reservation = metadata.get("prereserve_doi")
    doi = reservation.get("doi") if isinstance(reservation, dict) else None
    doi = doi or metadata.get("doi") or deposition.get("doi")
    if not isinstance(doi, str) or not doi.startswith("10."):
        raise ZenodoError(
            "Zenodo did not return a reserved DOI; refusing to invent one"
        )
    return doi


def previous_record_id(repo: Path) -> int:
    match = re.search(
        r"https://doi\.org/10\.5281/zenodo\.(\d+)", (repo / "docs/home.md").read_text()
    )
    if match is None:
        raise ZenodoError(
            "Cannot find the previous Zenodo record in docs/home.md; supply --previous"
        )
    return int(match[1])


def documentation_edits(repo: Path, release: Release) -> dict[Path, str]:
    """Only update the current citation and the target release's heading/DOI badge."""
    home_path = repo / "docs/home.md"
    home = home_path.read_text()
    citation_pattern = re.compile(
        r"(Zijlstra, J\.S\. )\d{4}(\. Hesperomys Project \(Version )"
        r"(?P<version>[^)]+)(\) \[Data set\]\. Zenodo\.\n\s*)"
        r"\[doi:[^\]]+\]\(https://doi\.org/[^)]+\)"
    )
    match = citation_pattern.search(home)
    if match is None:
        raise ZenodoError("Cannot locate the project citation in docs/home.md")
    if tuple(map(int, match["version"].split("."))) > tuple(
        map(int, release.version.split("."))
    ):
        raise ZenodoError(
            "Refusing to replace the home page citation with an older release"
        )
    date = datetime.date.fromisoformat(release.publication_date)
    citation = (
        f"{match[1]}{date.year}{match[2]}{release.version}{match[4]}"
        f"[doi:{release.doi}](https://doi.org/{release.doi})"
    )
    home = home[: match.start()] + citation + home[match.end() :]

    notes_path = repo / "docs/release-notes.md"
    notes = notes_path.read_text()
    heading = f"# {release.version} ({date.strftime('%B')} {date.day}, {date.year})"
    badge = f"[![DOI](https://zenodo.org/badge/DOI/{release.doi}.svg)](https://doi.org/{release.doi})"
    sections = list(
        re.finditer(r"^# (\d{2}\.\d{1,2}\.\d+)\b[^\n]*", notes, flags=re.MULTILINE)
    )
    if not sections:
        raise ZenodoError("Cannot locate release sections in docs/release-notes.md")
    for index, section in enumerate(sections):
        if section[1] != release.version:
            continue
        end = sections[index + 1].start() if index + 1 < len(sections) else len(notes)
        body = notes[section.end() : end]
        badge_pattern = r"\[!\[DOI\]\(https://zenodo\.org/badge/DOI/[^)]+\)\]\(https://doi\.org/[^)]+\)"
        if re.search(badge_pattern, body):
            body = re.sub(badge_pattern, lambda _: badge, body, count=1)
        else:
            body = f"\n\n{badge}\n" + body
        notes = notes[: section.start()] + heading + body + notes[end:]
        break
    else:
        start = sections[0].start()
        notes = notes[:start] + f"{heading}\n\n{badge}\n\n" + notes[start:]
    return {home_path: home, notes_path: notes}


def prepare(
    client: ZenodoClient,
    repo: Path,
    version: str,
    *,
    previous_id: int | None = None,
    publication_date: datetime.date | None = None,
) -> Release:
    path = release_path(repo, version, client.base_url)
    if path.exists():
        release = load_release(repo, version, client.base_url)
        if previous_id is not None and previous_id != release.previous_deposition_id:
            raise ZenodoError("--previous differs from the existing release record")
        if (
            publication_date is not None
            and publication_date.isoformat() != release.publication_date
        ):
            raise ZenodoError("--date differs from the existing release record")
        deposition = client.deposition(release.deposition_id)
        release.validate(deposition)
        if is_published(deposition):
            return release
        require_draft(deposition)
    else:
        if previous_id is None:
            if client.base_url == SANDBOX_URL:
                raise ZenodoError(
                    "Sandbox preparation requires --previous with a published sandbox record"
                )
            previous_id = previous_record_id(repo)
        previous = client.deposition(previous_id)
        if previous["metadata"].get("version") == version:
            raise ZenodoError(
                "The previous record already has this version; choose a new version"
            )
        draft = client.new_version(previous_id)
        metadata = dict(draft["metadata"])
        if metadata.get("version") not in {
            None,
            "",
            version,
            previous["metadata"].get("version"),
        }:
            raise ZenodoError(
                "The existing Zenodo draft belongs to a different release"
            )
        # A new version inherits metadata. Never submit the preceding version's DOI
        # as an externally assigned DOI, or reuse its publication date/version.
        metadata.pop("doi", None)
        metadata.pop("relations", None)
        metadata.update(
            version=version,
            publication_date=(
                publication_date or datetime.datetime.now(datetime.UTC).date()
            ).isoformat(),
            prereserve_doi=True,
        )
        metadata["title"] = re.sub(
            r"\(Version [^)]+\)", f"(Version {version})", metadata["title"]
        )
        updated = client.request_json(
            "PUT", f"deposit/depositions/{draft['id']}", json={"metadata": metadata}
        )
        release = Release(
            version=version,
            deposition_id=draft["id"],
            doi=get_doi(updated),
            publication_date=metadata["publication_date"],
            previous_deposition_id=previous_id,
            api_url=client.base_url,
        )
        release.validate(updated)
        # Save the identity before editing documentation so a rerun recovers from
        # a formatting error or interruption without creating another draft.
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(asdict(release), indent=2) + "\n")
        temporary.replace(path)
    if client.base_url == PRODUCTION_URL:
        for doc_path, contents in documentation_edits(repo, release).items():
            doc_path.write_text(contents)
    return release


def export_directory(repo: Path, version: str) -> Path:
    validate_version(version)
    return repo / "outputs" / "releases" / version


def export_inputs() -> dict[str, tuple[int, int] | None]:
    from taxonomy.config import get_options

    options = get_options()
    paths = {
        "database": options.db_filename,
        "wal": Path(f"{options.db_filename}-wal"),
        "journal": Path(f"{options.db_filename}-journal"),
        "derived_data": options.derived_data_filename,
    }
    result: dict[str, tuple[int, int] | None] = {}
    for key, path in paths.items():
        if path.is_file():
            stat = path.stat()
            result[key] = stat.st_size, stat.st_mtime_ns
        else:
            result[key] = None
    if result["database"] is None:
        raise ZenodoError("The configured taxonomy database does not exist")
    return result


def bundle_paths(directory: Path, version: str) -> list[Path]:
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        raise ZenodoError(
            f"No export manifest at {manifest_path}; run export first or supply explicit files"
        )
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("version") != version or set(manifest["files"]) != set(
        STANDARD_EXPORTS
    ):
        raise ZenodoError("The export bundle has the wrong version or file set")
    paths = [directory / name for name in STANDARD_EXPORTS]
    for file in read_upload_files(paths):
        if not file.matches(manifest["files"][file.name]):
            raise ZenodoError(f"Export changed since generation: {file.path}")
    return paths


def export_release(
    repo: Path, version: str, *, directory: Path | None = None
) -> list[Path]:
    """Generate the existing public exports, leaving partial output unpublished.

    Run in a fresh CLI process after updating derived data. Database editing must
    stop until deployment; input file changes during export cause this step to fail.
    """
    from taxonomy.db import export
    from taxonomy.db.models.base import BaseModel

    validate_version(version)
    directory = directory or export_directory(repo, version)
    if directory.exists():
        paths = bundle_paths(directory, version)
        print(f"Reusing verified export bundle: {directory}")
        return paths
    before = export_inputs()
    directory.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{version}-", dir=directory.parent
    ) as temporary:
        staging = Path(temporary) / "exports"
        staging.mkdir()
        with BaseModel.clirm.readonly():
            for name, function in STANDARD_EXPORTS.items():
                print(f"Generating {name}", flush=True)
                getattr(export, function)(str(staging / name))
        if export_inputs() != before:
            raise ZenodoError(
                "Database or derived data changed during export; stop editing and rerun export"
            )
        files = read_upload_files([staging / name for name in STANDARD_EXPORTS])
        manifest = {
            "version": version,
            "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "files": {
                file.name: {"size": file.size, "checksum": f"md5:{file.checksum}"}
                for file in files
            },
        }
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        staging.rename(directory)
    return [directory / name for name in STANDARD_EXPORTS]


def read_upload_files(paths: list[Path]) -> list[UploadFile]:
    if not paths:
        raise ZenodoError("Supply the complete set of release files")
    if len({path.name for path in paths}) != len(paths):
        raise ZenodoError(
            "Upload filenames must be unique, even when in different directories"
        )
    return [UploadFile.read(path) for path in paths]


def verify_files(
    deposition: dict[str, Any], files: list[UploadFile], *, allow_extra: bool = False
) -> None:
    remote = files_by_name(deposition)
    expected = {file.name for file in files}
    extra = set(remote) - expected
    if extra and not allow_extra:
        raise ZenodoError(
            f"Unexpected remote files: {', '.join(sorted(extra))}; supply all files or upload with --prune"
        )
    for file in files:
        file.check_unchanged()
        if file.name not in remote or not file.matches(remote[file.name]):
            raise ZenodoError(f"Missing file or checksum/size mismatch: {file.name}")


def upload(
    client: ZenodoClient, release: Release, paths: list[Path], *, prune: bool = False
) -> None:
    files = read_upload_files(paths)
    deposition = client.deposition(release.deposition_id)
    release.validate(deposition)
    require_draft(deposition)
    for file in files:
        client.upload(release.deposition_id, file)
    deposition = client.deposition(release.deposition_id)
    release.validate(deposition)
    require_draft(deposition)
    verify_files(deposition, files, allow_extra=True)
    if prune:
        expected = {file.name for file in files}
        for name, remote in files_by_name(deposition).items():
            if name not in expected:
                print(f"Removing inherited/unselected draft file: {name}")
                client.request_json(
                    "DELETE",
                    f"deposit/depositions/{release.deposition_id}/files/{remote['id']}",
                )
        deposition = client.deposition(release.deposition_id)
    verify_files(deposition, files)


def publish(
    client: ZenodoClient, release: Release, paths: list[Path]
) -> dict[str, Any]:
    files = read_upload_files(paths)
    deposition = client.deposition(release.deposition_id)
    release.validate(deposition)
    verify_files(deposition, files)
    result = client.publish(release.deposition_id)
    release.validate(result)
    verify_files(result, files)
    return result
