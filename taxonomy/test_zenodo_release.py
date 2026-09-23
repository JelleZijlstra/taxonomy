import copy
import datetime
import hashlib
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
import pytest

from taxonomy import zenodo_release as release_module
from taxonomy.apis.zenodo import (
    PRODUCTION_URL,
    SANDBOX_URL,
    UploadFile,
    ZenodoClient,
    ZenodoError,
)
from taxonomy.zenodo_release import Release

VERSION = "26.10.0"
DATE = datetime.date(2026, 10, 2)


def digest(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


class FakeZenodo:
    """In-memory API with the documented old-resource newversion response."""

    def __init__(self) -> None:
        self.previous: dict[str, Any] = {
            "id": 10,
            "state": "done",
            "submitted": True,
            "metadata": {
                "title": "Hesperomys Project (Version 26.9.0)",
                "version": "26.9.0",
                "publication_date": "2026-09-21",
                "doi": "10.5281/zenodo.10",
                "creators": [{"name": "Zijlstra, Jelle"}],
                "description": "Existing dataset description",
                "license": "cc-by-4.0",
            },
            "files": [],
            "links": {"latest_draft": f"{PRODUCTION_URL}/deposit/depositions/20"},
        }
        self.draft: dict[str, Any] | None = None
        self.calls: list[tuple[str, str, bytes]] = []
        self.publish_state = "done"
        self.hook: Callable[[httpx.Request], httpx.Response | None] | None = None

    def add_file(self, name: str, data: bytes) -> None:
        assert self.draft is not None
        self.draft["files"] = [
            file for file in self.draft["files"] if file["filename"] != name
        ]
        self.draft["files"].append(
            {
                "id": f"id-{name}",
                "filename": name,
                "filesize": len(data),
                "checksum": digest(data),
            }
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = unquote(request.url.path)
        self.calls.append((request.method, path, request.content))
        if self.hook is not None:
            result = self.hook(request)
            if result is not None:
                return result
        if path == "/api/deposit/depositions/10" and request.method == "GET":
            return httpx.Response(200, json=self.previous)
        if path == "/api/deposit/depositions/10/actions/newversion":
            if self.draft is None:
                self.draft = copy.deepcopy(self.previous)
                self.draft.update(
                    id=20,
                    state="unsubmitted",
                    submitted=False,
                    links={"bucket": f"{PRODUCTION_URL}/files/bucket"},
                )
            return httpx.Response(201, json=self.previous)
        assert self.draft is not None
        if path == "/api/deposit/depositions/20":
            if request.method == "PUT":
                self.draft["metadata"] = json.loads(request.content)["metadata"]
                self.draft["metadata"]["prereserve_doi"] = {
                    "doi": "10.5281/zenodo.20",
                    "recid": 20,
                }
            return httpx.Response(200, json=self.draft)
        if request.method == "PUT" and path.startswith("/api/files/bucket/"):
            name = path.removeprefix("/api/files/bucket/")
            assert request.headers["content-length"] == str(len(request.content))
            self.add_file(name, request.content)
            return httpx.Response(
                200,
                json={
                    "size": len(request.content),
                    "checksum": f"md5:{digest(request.content)}",
                },
            )
        if request.method == "DELETE" and path.startswith(
            "/api/deposit/depositions/20/files/"
        ):
            file_id = path.rsplit("/", 1)[1]
            self.draft["files"] = [
                file for file in self.draft["files"] if file["id"] != file_id
            ]
            return httpx.Response(204)
        if (
            path == "/api/deposit/depositions/20/actions/publish"
            and request.method == "POST"
        ):
            self.draft.update(state=self.publish_state, submitted=True)
            return httpx.Response(202, json=self.draft)
        raise AssertionError(f"Unexpected request: {request.method} {path}")


@pytest.fixture
def api() -> Iterator[tuple[FakeZenodo, ZenodoClient]]:
    fake = FakeZenodo()
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        yield fake, ZenodoClient(http, attempts=3, sleep=lambda _: None)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/home.md").write_text(
        "Welcome.\n\n- Zijlstra, J.S. 2026. Hesperomys Project (Version 26.9.0) [Data set]. Zenodo.\n"
        "  [doi:10.5281/zenodo.10](https://doi.org/10.5281/zenodo.10)\n\n# Size\nUnchanged statistics\n"
    )
    (tmp_path / "docs/release-notes.md").write_text(
        "Release notes\n\n# 26.9.0 (September 21, 2026)\n\n"
        "[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10.svg)](https://doi.org/10.5281/zenodo.10)\n\n"
        "- Earlier changes.\n"
    )
    return tmp_path


def prepared(api: tuple[FakeZenodo, ZenodoClient], repo: Path) -> Release:
    return release_module.prepare(api[1], repo, VERSION, publication_date=DATE)


def test_prepare_reserves_new_doi_and_preserves_old_history(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, _ = api
    original_notes = (repo / "docs/release-notes.md").read_text()
    release = prepared(api, repo)
    assert release.deposition_id == 20
    assert release.doi == "10.5281/zenodo.20"
    assert release.publication_date == "2026-10-02"
    payload = next(
        json.loads(body)["metadata"]
        for method, _, body in fake.calls
        if method == "PUT"
    )
    assert "doi" not in payload
    assert payload["prereserve_doi"] is True
    assert payload["creators"] == fake.previous["metadata"]["creators"]
    assert payload["license"] == "cc-by-4.0"
    assert payload["description"] == "Existing dataset description"
    assert payload["title"] == "Hesperomys Project (Version 26.10.0)"
    notes = (repo / "docs/release-notes.md").read_text()
    assert "# 26.10.0 (October 2, 2026)" in notes
    assert original_notes.removeprefix("Release notes\n\n") in notes
    home = (repo / "docs/home.md").read_text()
    assert "Version 26.10.0" in home
    assert "Unchanged statistics" in home
    assert "10.5281/zenodo.10" not in home
    state = json.loads((repo / "releases/26.10.0.json").read_text())
    assert set(state) == {
        "version",
        "deposition_id",
        "doi",
        "publication_date",
        "previous_deposition_id",
        "api_url",
    }


def test_prepare_is_repeatable_without_creating_or_changing_a_draft(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    first = prepared(api, repo)
    notes = (repo / "docs/release-notes.md").read_text()
    fake.calls.clear()
    assert release_module.prepare(client, repo, VERSION) == first
    assert all(method == "GET" for method, _, _ in fake.calls)
    assert (repo / "docs/release-notes.md").read_text() == notes


def test_prepare_updates_existing_release_section_without_losing_notes(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    notes = repo / "docs/release-notes.md"
    notes.write_text(
        "# 26.10.0 (October 1, 2026)\n\n- Written release notes.\n\n"
        + notes.read_text()
    )
    prepared(api, repo)
    assert notes.read_text().count("# 26.10.0") == 1
    assert "- Written release notes." in notes.read_text()


def test_prepare_recovers_after_documentation_error(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    home = repo / "docs/home.md"
    original = home.read_text()
    home.write_text("invalid citation")
    with pytest.raises(ZenodoError, match="locate the project citation"):
        release_module.prepare(
            client, repo, VERSION, previous_id=10, publication_date=DATE
        )
    assert (repo / "releases/26.10.0.json").exists()
    home.write_text(original)
    fake.calls.clear()
    release_module.prepare(client, repo, VERSION)
    assert all(method == "GET" for method, _, _ in fake.calls)


def test_prepare_refuses_another_versions_draft(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    client.new_version(10)
    assert fake.draft is not None
    fake.draft["metadata"]["version"] = "27.1.0"
    with pytest.raises(ZenodoError, match="different release"):
        prepared(api, repo)
    assert not (repo / "releases/26.10.0.json").exists()
    assert not any(method == "PUT" for method, _, _ in fake.calls)


def test_prepare_recovers_after_newversion_response_is_lost(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, _ = api
    failed = False

    def hook(request: httpx.Request) -> httpx.Response | None:
        nonlocal failed
        if request.url.path.endswith("/actions/newversion") and not failed:
            failed = True
            fake.hook = None
            fake(request)
            raise httpx.ReadTimeout("response lost")
        return None

    fake.hook = hook
    assert prepared(api, repo).deposition_id == 20
    assert fake.draft is not None


def test_upload_replaces_old_files_then_skips_verified_files(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"new names")
    fake.add_file(file.name, b"old names")
    release_module.upload(client, release, [file])
    fake.calls.clear()
    release_module.upload(client, release, [file])
    assert all(method == "GET" for method, _, _ in fake.calls)


def test_upload_rewinds_entire_file_after_failure(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    contents = b"some names\n" * 200_000
    file.write_bytes(contents)
    uploads: list[bytes] = []

    def hook(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.startswith("/api/files/"):
            uploads.append(request.content)
            if len(uploads) == 1:
                raise httpx.WriteError("connection dropped")
        return None

    fake.hook = hook
    release_module.upload(client, release, [file])
    assert uploads == [contents, contents]


def test_upload_does_not_repeat_an_upload_with_a_lost_success_response(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")

    def hook(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.startswith("/api/files/"):
            fake.add_file(file.name, request.content)
            raise httpx.ReadTimeout("response lost after commit")
        return None

    fake.hook = hook
    release_module.upload(client, release, [file])
    assert (
        sum(
            method == "PUT" and path.startswith("/api/files/")
            for method, path, _ in fake.calls
        )
        == 1
    )


def test_upload_stops_after_checksum_failures(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")

    def hook(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.startswith("/api/files/"):
            return httpx.Response(
                200, json={"size": file.stat().st_size, "checksum": "md5:bad"}
            )
        return None

    fake.hook = hook
    with pytest.raises(ZenodoError, match="checksum or size mismatch"):
        release_module.upload(client, release, [file])
    assert (
        sum(path.startswith("/api/files/") for _, path, _ in fake.calls)
        == client.attempts
    )


def test_upload_detects_source_changes(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")

    def hook(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.startswith("/api/files/"):
            file.write_bytes(b"changed during upload")
        return None

    fake.hook = hook
    with pytest.raises(ZenodoError, match="File changed"):
        release_module.upload(client, release, [file])


def test_upload_only_prunes_after_selected_files_verify(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")
    fake.add_file("stale.csv", b"stale")
    with pytest.raises(ZenodoError, match="Unexpected remote files"):
        release_module.upload(client, release, [file])
    assert fake.draft is not None
    assert len(fake.draft["files"]) == 2
    release_module.upload(client, release, [file], prune=True)
    assert [entry["filename"] for entry in fake.draft["files"]] == ["name.csv"]


@pytest.mark.parametrize("problem", ["missing", "extra", "checksum", "version"])
def test_publish_requires_matching_metadata_and_complete_file_set(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path, problem: str
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")
    if problem != "missing":
        fake.add_file(file.name, b"names" if problem != "checksum" else b"wrong")
    if problem == "extra":
        fake.add_file("stale.csv", b"stale")
    if problem == "version":
        assert fake.draft is not None
        fake.draft["metadata"]["version"] = "26.11.0"
    with pytest.raises(ZenodoError):
        release_module.publish(client, release, [file])
    assert not any(path.endswith("/actions/publish") for _, path, _ in fake.calls)


def test_publish_is_confirmed_and_repeatable(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")
    fake.add_file(file.name, b"names")
    assert release_module.publish(client, release, [file])["state"] == "done"
    assert release_module.publish(client, release, [file])["state"] == "done"
    assert sum(path.endswith("/actions/publish") for _, path, _ in fake.calls) == 1
    with pytest.raises(ZenodoError, match="published"):
        release_module.upload(client, release, [file])


def test_publish_reconciles_a_lost_response(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")
    fake.add_file(file.name, b"names")

    def hook(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.endswith("/actions/publish"):
            assert fake.draft is not None
            fake.draft.update(state="done", submitted=True)
            raise httpx.ReadTimeout("published but response lost")
        return None

    fake.hook = hook
    assert release_module.publish(client, release, [file])["state"] == "done"
    assert sum(path.endswith("/actions/publish") for _, path, _ in fake.calls) == 1


def test_pending_publication_is_not_reported_as_success_or_reposted(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")
    fake.add_file(file.name, b"names")
    fake.publish_state = "inprogress"
    for _ in range(2):
        with pytest.raises(ZenodoError, match="not yet confirmed"):
            release_module.publish(client, release, [file])
    assert sum(path.endswith("/actions/publish") for _, path, _ in fake.calls) == 1


@pytest.mark.parametrize("status", [401, 403, 400])
def test_permanent_errors_are_not_retried(
    api: tuple[FakeZenodo, ZenodoClient], status: int
) -> None:
    fake, client = api
    fake.hook = lambda _: httpx.Response(status, json={"message": "permanent error"})
    with pytest.raises(ZenodoError, match=f"HTTP {status}"):
        client.deposition(10)
    assert len(fake.calls) == 1


def test_rate_limit_uses_retry_after(api: tuple[FakeZenodo, ZenodoClient]) -> None:
    fake, client = api
    sleeps: list[float] = []
    client.sleep = sleeps.append

    def hook(request: httpx.Request) -> httpx.Response | None:
        if len(fake.calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "12"})
        return None

    fake.hook = hook
    client.deposition(10)
    assert sleeps == [12]


@pytest.mark.parametrize(
    "url",
    ["https://other.example/api/files/bucket", "http://zenodo.org/api/files/bucket"],
)
def test_unexpected_api_links_are_rejected(
    api: tuple[FakeZenodo, ZenodoClient], url: str
) -> None:
    fake, client = api
    with pytest.raises(ZenodoError, match="Unexpected API link"):
        client.request_json("GET", url)
    assert not fake.calls


def test_upload_checksums_accept_both_api_representations(tmp_path: Path) -> None:
    path = tmp_path / "file.csv"
    path.write_bytes(b"data")
    file = UploadFile.read(path)
    assert file.matches({"size": 4, "checksum": f"md5:{digest(b'data')}"})
    assert file.matches({"filesize": "4", "checksum": digest(b"data")})


def test_duplicate_upload_basenames_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ZenodoError, match="unique"):
        release_module.read_upload_files(
            [tmp_path / "a/file.csv", tmp_path / "b/file.csv"]
        )


def test_sandbox_prepare_keeps_production_docs_and_state_separate(repo: Path) -> None:
    fake = FakeZenodo()
    fake.previous["links"]["latest_draft"] = f"{SANDBOX_URL}/deposit/depositions/20"
    home = (repo / "docs/home.md").read_text()
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        client = ZenodoClient(http, base_url=SANDBOX_URL, sleep=lambda _: None)
        release = release_module.prepare(
            client, repo, VERSION, previous_id=10, publication_date=DATE
        )
    assert release.api_url == SANDBOX_URL
    assert (repo / "outputs/zenodo-sandbox/26.10.0.json").exists()
    assert not (repo / "releases").exists()
    assert (repo / "docs/home.md").read_text() == home


@pytest.fixture
def exporters(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from taxonomy.db import export
    from taxonomy.db.models.base import BaseModel

    called: list[str] = []

    def generate(filename: str) -> None:
        assert BaseModel.clirm.is_read_only
        called.append(Path(filename).name)
        Path(filename).write_text("id,name\n1,Example\n")

    for function in release_module.STANDARD_EXPORTS.values():
        monkeypatch.setattr(export, function, generate)
    monkeypatch.setattr(
        release_module, "export_inputs", lambda: {"database": (100, 200)}
    )
    return called


def test_export_creates_complete_bundle_and_reuses_it(
    repo: Path, exporters: list[str]
) -> None:
    paths = release_module.export_release(repo, VERSION)
    assert set(exporters) == set(release_module.STANDARD_EXPORTS)
    assert len(paths) == 5
    directory = paths[0].parent
    assert release_module.bundle_paths(directory, VERSION) == paths
    exporters.clear()
    assert release_module.export_release(repo, VERSION) == paths
    assert not exporters


def test_export_refuses_modified_bundle(repo: Path, exporters: list[str]) -> None:
    paths = release_module.export_release(repo, VERSION)
    paths[0].write_text("changed")
    with pytest.raises(ZenodoError, match="changed since generation"):
        release_module.bundle_paths(paths[0].parent, VERSION)


def test_failed_export_does_not_leave_a_completed_bundle(
    repo: Path, exporters: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from taxonomy.db import export

    def fail(filename: str) -> None:
        Path(filename).write_text("partial")
        raise ValueError("export interrupted")

    monkeypatch.setattr(export, "export_names", fail)
    with pytest.raises(ValueError, match="export interrupted"):
        release_module.export_release(repo, VERSION)
    assert not release_module.export_directory(repo, VERSION).exists()
    assert not list((repo / "outputs/releases").iterdir())


def test_export_detects_database_changes(
    repo: Path, exporters: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshots = iter([{"database": (100, 200)}, {"database": (101, 300)}])
    monkeypatch.setattr(release_module, "export_inputs", lambda: next(snapshots))
    with pytest.raises(ZenodoError, match="changed during export"):
        release_module.export_release(repo, VERSION)
    assert not release_module.export_directory(repo, VERSION).exists()


def test_export_cli_needs_no_token_or_network(
    repo: Path, exporters: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import zenodo_release

    monkeypatch.setattr(zenodo_release, "REPO_ROOT", repo)
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)
    zenodo_release.main(["export", VERSION])
    assert len(exporters) == 5


def test_full_cli_release_sequence(
    api: tuple[FakeZenodo, ZenodoClient],
    repo: Path,
    exporters: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from scripts import zenodo_release

    monkeypatch.setattr(zenodo_release, "REPO_ROOT", repo)
    monkeypatch.setenv("ZENODO_TOKEN", "test-token-not-to-be-printed")
    monkeypatch.setattr(
        zenodo_release, "ZenodoClient", lambda *_args, **_kwargs: api[1]
    )
    zenodo_release.main(["prepare", VERSION, "--date", DATE.isoformat()])
    tracked = [
        repo / "releases/26.10.0.json",
        repo / "docs/home.md",
        repo / "docs/release-notes.md",
    ]
    prepared_contents = [path.read_bytes() for path in tracked]
    zenodo_release.main(["export", VERSION])
    zenodo_release.main(["upload", VERSION])
    zenodo_release.main(["upload", VERSION])
    zenodo_release.main(["status", VERSION])
    zenodo_release.main(["publish", VERSION])
    assert prepared_contents == [path.read_bytes() for path in tracked]
    fake = api[0]
    assert (
        sum(
            method == "PUT" and path.startswith("/api/files/")
            for method, path, _ in fake.calls
        )
        == 5
    )
    output = capsys.readouterr().out
    assert "Publication confirmed." in output
    assert "test-token-not-to-be-printed" not in output


def test_uncertain_unaccepted_publish_is_not_automatically_retried(
    api: tuple[FakeZenodo, ZenodoClient], repo: Path
) -> None:
    fake, client = api
    release = prepared(api, repo)
    file = repo / "name.csv"
    file.write_bytes(b"names")
    fake.add_file(file.name, b"names")

    def hook(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.endswith("/actions/publish"):
            raise httpx.ReadTimeout("unknown outcome")
        return None

    fake.hook = hook
    with pytest.raises(ZenodoError, match="outcome is uncertain"):
        release_module.publish(client, release, [file])
    assert sum(path.endswith("/actions/publish") for _, path, _ in fake.calls) == 1
