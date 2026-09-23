"""Zenodo's deposition API, including retryable, streamed bucket uploads.

API reference: https://developers.zenodo.org/
"""

import datetime
import email.utils
import hashlib
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

PRODUCTION_URL = "https://zenodo.org/api"
SANDBOX_URL = "https://sandbox.zenodo.org/api"
RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}
CHUNK_SIZE = 1024 * 1024


class ZenodoError(RuntimeError):
    pass


class RetryableZenodoError(ZenodoError):
    def __init__(self, message: str, retry_after: float = 0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True)
class UploadFile:
    path: Path
    size: int
    checksum: str
    mtime_ns: int

    @classmethod
    def read(cls, path: Path) -> UploadFile:
        before = path.stat()
        if not path.is_file():
            raise ZenodoError(f"Not a regular file: {path}")
        with path.open("rb") as stream:
            checksum = hashlib.file_digest(
                stream, lambda: hashlib.md5(usedforsecurity=False)
            ).hexdigest()
        result = cls(path, before.st_size, checksum, before.st_mtime_ns)
        result.check_unchanged()
        return result

    @property
    def name(self) -> str:
        return self.path.name

    def check_unchanged(self) -> None:
        current = self.path.stat()
        if (current.st_size, current.st_mtime_ns) != (self.size, self.mtime_ns):
            raise ZenodoError(f"File changed during upload preparation: {self.path}")

    def matches(self, remote: dict[str, Any]) -> bool:
        checksum = str(remote.get("checksum", "")).removeprefix("md5:").lower()
        size = remote.get("filesize", remote.get("size"))
        return size is not None and int(size) == self.size and checksum == self.checksum

    def chunks(self) -> Iterator[bytes]:
        self.check_unchanged()
        started = last_report = time.monotonic()
        sent = 0
        with self.path.open("rb") as stream:
            while chunk := stream.read(CHUNK_SIZE):
                yield chunk
                sent += len(chunk)
                now = time.monotonic()
                if now - last_report >= 2:
                    rate = sent / max(now - started, 0.001) / CHUNK_SIZE
                    print(
                        f"  {self.name}: {sent / CHUNK_SIZE:.1f}/{self.size / CHUNK_SIZE:.1f}"
                        f" MiB sent ({rate:.1f} MiB/s)",
                        flush=True,
                    )
                    last_report = now
        self.check_unchanged()


def files_by_name(deposition: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {file["filename"]: file for file in deposition["files"]}


def require_draft(deposition: dict[str, Any]) -> None:
    if deposition.get("submitted") or deposition.get("state") != "unsubmitted":
        raise ZenodoError(
            "This deposition is published or being published; files cannot be changed"
        )


def is_published(deposition: dict[str, Any]) -> bool:
    return deposition.get("state") == "done" and bool(deposition.get("submitted"))


def retry_after_seconds(value: str) -> float:
    try:
        return max(0, float(value))
    except ValueError:
        try:
            date = email.utils.parsedate_to_datetime(value)
            return max(0, (date - datetime.datetime.now(datetime.UTC)).total_seconds())
        except TypeError, ValueError, OverflowError:
            return 0


class ZenodoClient:
    def __init__(
        self,
        client: httpx.Client,
        *,
        base_url: str = PRODUCTION_URL,
        attempts: int = 5,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if base_url not in {PRODUCTION_URL, SANDBOX_URL}:
            raise ValueError(f"Unsupported Zenodo API: {base_url}")
        if attempts < 1:
            raise ValueError("At least one attempt is required")
        self.client = client
        self.base_url = base_url
        self.attempts = attempts
        self.sleep = sleep

    def url(self, path: str) -> str:
        url = path if "://" in path else f"{self.base_url}/{path.lstrip('/')}"
        parsed = urlsplit(url)
        expected = urlsplit(self.base_url)
        if (
            parsed.scheme != expected.scheme
            or parsed.netloc != expected.netloc
            or not parsed.path.startswith("/api/")
            or parsed.query
            or parsed.fragment
        ):
            raise ZenodoError(f"Unexpected API link: {url}")
        return url

    def request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        file: UploadFile | None = None,
    ) -> httpx.Response:
        url = self.url(path)
        try:
            if file is None:
                response = self.client.request(
                    method, url, json=json, follow_redirects=False
                )
            else:
                response = self.client.request(
                    method,
                    url,
                    content=file.chunks(),
                    headers={
                        "Content-Length": str(file.size),
                        "Content-Type": "application/octet-stream",
                    },
                    follow_redirects=False,
                )
        except httpx.TransportError as exc:
            raise RetryableZenodoError(f"{method} {url}: {exc}") from exc
        if response.status_code in RETRYABLE_STATUSES:
            raise RetryableZenodoError(
                f"{method} {url}: HTTP {response.status_code}",
                retry_after_seconds(response.headers.get("Retry-After", "")),
            )
        # A repeated DELETE may encounter a file removed by the previous attempt.
        if method == "DELETE" and response.status_code == 404:
            return response
        if not response.is_success:
            raise ZenodoError(
                f"{method} {url}: HTTP {response.status_code}: {response.text[:1000]}"
            )
        return response

    def wait_to_retry(self, error: RetryableZenodoError, attempt: int) -> None:
        if attempt == self.attempts:
            raise error
        delay = max(min(2**attempt, 60), error.retry_after)
        print(
            f"{error}; retrying in {delay:g}s ({attempt}/{self.attempts})", flush=True
        )
        self.sleep(delay)

    def request_json(
        self, method: str, path: str, *, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        # Publishing is intentionally excluded; an uncertain POST must be reconciled
        # with the deposition state instead of blindly repeated.
        if method == "POST" and not path.endswith("/actions/newversion"):
            raise ValueError(
                "Only the idempotent newversion action may be retried here"
            )
        for attempt in range(1, self.attempts + 1):
            try:
                response = self.request(method, path, json=json)
            except RetryableZenodoError as exc:
                self.wait_to_retry(exc, attempt)
            else:
                if method == "DELETE":
                    return {}
                result = response.json()
                if not isinstance(result, dict):
                    raise ZenodoError(f"Expected a JSON object from {path}")
                return result
        raise AssertionError("unreachable")

    def deposition(self, deposition_id: int) -> dict[str, Any]:
        return self.request_json("GET", f"deposit/depositions/{deposition_id}")

    def new_version(self, previous_id: int) -> dict[str, Any]:
        previous = self.deposition(previous_id)
        if not is_published(previous):
            raise ZenodoError(
                "--previous must identify a published version, not a draft or concept DOI"
            )
        original = self.request_json(
            "POST", f"deposit/depositions/{previous_id}/actions/newversion"
        )
        # The response describes the OLD version. Always follow latest_draft.
        draft = self.request_json("GET", original["links"]["latest_draft"])
        require_draft(draft)
        return draft

    def upload(self, deposition_id: int, file: UploadFile) -> None:
        for attempt in range(1, self.attempts + 1):
            try:
                file.check_unchanged()
                deposition = self.deposition(deposition_id)
                require_draft(deposition)
                existing = files_by_name(deposition).get(file.name)
                if existing is not None:
                    if file.matches(existing):
                        print(f"Verified; skipping {file.name}")
                        return
                    # This also replaces files inherited from the previous version.
                    self.request_json(
                        "DELETE",
                        f"deposit/depositions/{deposition_id}/files/{quote(existing['id'], safe='')}",
                    )
                print(
                    f"Uploading {file.name} ({file.size / CHUNK_SIZE:.1f} MiB), attempt {attempt}/{self.attempts}",
                    flush=True,
                )
                response = self.request(
                    "PUT",
                    f"{deposition['links']['bucket']}/{quote(file.name, safe='')}",
                    file=file,
                )
                file.check_unchanged()
                if not file.matches(response.json()):
                    raise RetryableZenodoError(
                        f"Upload checksum or size mismatch for {file.name}"
                    )
                remote = files_by_name(self.deposition(deposition_id)).get(file.name)
                if remote is None or not file.matches(remote):
                    raise RetryableZenodoError(
                        f"Uploaded file is not yet verified: {file.name}"
                    )
            except RetryableZenodoError as exc:
                # Re-read the file list before retrying: a lost response does not
                # necessarily mean the upload failed. Each PUT opens the file afresh.
                self.wait_to_retry(exc, attempt)
            else:
                print(f"Verified {file.name}")
                return

    def publish(self, deposition_id: int) -> dict[str, Any]:
        deposition = self.deposition(deposition_id)
        if is_published(deposition):
            return deposition
        if deposition.get("state") == "unsubmitted":
            require_draft(deposition)
            try:
                self.request(
                    "POST", f"deposit/depositions/{deposition_id}/actions/publish"
                )
            except RetryableZenodoError:
                deposition = self.deposition(deposition_id)
                if deposition.get("state") == "unsubmitted":
                    raise ZenodoError(
                        "Publication outcome is uncertain. Run status, then rerun publish if still a draft"
                    ) from None
        elif deposition.get("state") != "inprogress":
            raise ZenodoError(
                f"Unexpected publication state: {deposition.get('state')}"
            )
        for _ in range(30):
            deposition = self.deposition(deposition_id)
            if is_published(deposition):
                return deposition
            self.sleep(2)
        raise ZenodoError(
            "Publication is not yet confirmed. Run status; rerunning publish will check its state"
        )
