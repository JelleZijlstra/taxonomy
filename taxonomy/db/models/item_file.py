"""Raw files downloaded from external sources like BHL or Google Books."""

import json
import re
import shutil
import subprocess
import tempfile
import traceback
from pathlib import Path
from typing import Literal, NotRequired, Self, TypedDict

import fitz
import httpx
from clirm import Field

from taxonomy import getinput
from taxonomy.adt import ADT
from taxonomy.apis import bhl
from taxonomy.config import get_options, is_network_available
from taxonomy.db import constants as db_constants
from taxonomy.db.constants import Managed, Markdown
from taxonomy.db.url_cache import CacheDomain, run_query
from taxonomy.getinput import CallbackMap

from .base import ADTField, BaseModel
from .citation_group import CitationGroup


class ItemFileTag(ADT):
    IFComment(text=Markdown, tag=1)  # type: ignore[name-defined]
    IFAlternativeUrl(url=Managed, tag=2)  # type: ignore[name-defined]


class ItemFile(BaseModel):
    call_sign = "IF"
    label_field = "filename"
    clirm_table_name = "item_file"

    filename = Field[str]()
    title = Field[str | None](default=None)
    citation_group = Field[CitationGroup]("citation_group_id")
    series = Field[str | None](default=None)
    volume = Field[str | None](default=None)
    issue = Field[str | None](default=None)
    start_page = Field[str | None](default=None)
    end_page = Field[str | None](default=None)
    url = Field[str | None](default=None)
    tags = ADTField[ItemFileTag](is_ordered=False)

    def open(self) -> None:
        subprocess.check_call(["open", str(self.get_path())])

    def get_path(self) -> Path:
        options = get_options()
        item_file_path = options.item_file_path
        return item_file_path / self.filename

    def open_url(self) -> None:
        if self.url is not None:
            subprocess.check_call(["open", self.url])
        else:
            print(f"No URL for {self}")

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "open": self.open,
            "o": self.open,
            "open_url": self.open_url,
        }

    def edit(self) -> None:
        self.fill_field("tags")

    def detect_url(self) -> None:
        if self.url is not None:
            return
        link = extract_first_page_link(self.get_path())
        if link is not None and link.startswith("https://books.google.com/"):
            print(f"Detected Google Books link: {link}")
            self.url = link
        elif m := re.fullmatch(r"(\d+)\.pdf", self.filename):
            bhl_item_id = int(m.group(1))
            bhl_url = f"https://www.biodiversitylibrary.org/item/{bhl_item_id}"
            item_metadata = bhl.get_item_metadata(bhl_item_id)
            if item_metadata is not None:
                print(f"Detected BHL link: {bhl_url}")
                self.url = bhl_url

    @classmethod
    def create_from_filename(cls, filename: str) -> Self | None:
        cg = CitationGroup.getter(None).get_one(
            prompt="Citation group> ",
            callbacks=_make_cb_map(get_options().item_file_path / filename),
        )
        if cg is None:
            return None
        itf = cls.create(filename=filename, citation_group=cg)
        for field in ("volume", "issue", "url"):
            itf.fill_field(field)
        itf.detect_url()
        itf.edit()
        return itf

    @classmethod
    def check(cls) -> None:
        options = get_options()
        existing_on_disk = {
            f.name for f in options.item_file_path.iterdir() if f.is_file()
        }
        existing_in_db = {
            item_file.filename: item_file for item_file in ItemFile.select_valid()
        }

        for filename in sorted(existing_on_disk - existing_in_db.keys()):
            print(f"Found item file on disk that is not in DB: {filename!r}")
            if getinput.yes_no("Create entry in DB? "):
                itf = cls.create_from_filename(filename)
                if itf is not None:
                    print(f"Created {itf}")
        for filename in sorted(existing_in_db.keys() - existing_on_disk):
            print(f"Item file in DB but missing on disk: {filename!r}")
            existing_in_db[filename].edit()

        cls.check_new()

    @classmethod
    def check_new(cls, *, autonomous: bool = False) -> None:
        options = get_options()
        newpath = options.burst_path / "Old"
        for f in sorted(newpath.iterdir(), key=lambda f: f.name):
            if f.is_file() and f.name != ".DS_Store":
                full_path = newpath / f.name
                print(f"Adding item file: {f.name!r}")
                if autonomous:
                    # Autonomous mode: do not prompt; try LLM auto-create only.
                    try:
                        itf = cls._auto_create_from_pdf(
                            full_path, allow_interactive_cg=False
                        )
                    except Exception as e:
                        print(f"LLM auto-create failed: {e!r}; skipping")
                        itf = None
                    if itf is not None:
                        print(f"Created {itf}")
                    else:
                        print("Skipped (no existing CitationGroup or unknown type)")
                elif getinput.yes_no("Add? ", callbacks=_make_cb_map(full_path)):
                    # First, try LLM-based auto classification and creation.
                    itf = None
                    try:
                        itf = cls._auto_create_from_pdf(full_path)
                    except Exception as e:
                        print(
                            f"LLM auto-create failed: {e!r}; falling back to manual input"
                        )
                    if itf is None:
                        # Fallback to manual flow
                        shutil.move(full_path, options.item_file_path / f.name)
                        itf = cls.create_from_filename(f.name)
                    if itf is not None:
                        print(f"Created {itf}")

    @classmethod
    def _auto_create_from_pdf(
        cls, full_path: Path, *, allow_interactive_cg: bool = True
    ) -> Self | None:
        """Attempt to classify the PDF using GPT 5.2 and auto-create ItemFile.

        Returns the created ItemFile on success, or None if classification is
        unavailable or inconclusive.
        """
        options = get_options()
        if not is_network_available():
            return None
        if not options.openai_key:
            return None

        filename = full_path.name
        # Try cache first
        cached_json = _urlcache_get(CacheDomain.gpt_item_file_verdict, filename)
        verdict: _Verdict | None
        if cached_json is not None:
            verdict = _parse_verdict_json(cached_json)
        else:
            print("Trying GPT 5.2 to classify PDF…")
            verdict = _classify_pdf_with_gpt(full_path, api_key=options.openai_key)
            if verdict is None:
                return None
            # Store in cache
            _urlcache_set(
                CacheDomain.gpt_item_file_verdict, filename, json.dumps(verdict)
            )
        if verdict is None or verdict["type"] == "unknown":
            return None

        # Move file into the item_file_path before creating DB object.
        dest = options.item_file_path / filename
        if full_path != dest:
            shutil.move(full_path, dest)

        match verdict["type"]:
            case "journal":
                journal_name = verdict["journal_name"].strip()  # type: ignore[typeddict-item]
                series = verdict.get("series") or None
                volume = verdict.get("volume") or None
                issue = verdict.get("issue") or None
                if allow_interactive_cg:
                    cg = CitationGroup.get_or_create(journal_name)
                else:
                    cg = CitationGroup.select_one(name=journal_name)
                if cg is None:
                    return None
                itf = cls.create(
                    filename=filename,
                    citation_group=cg,
                    series=series,
                    volume=volume,
                    issue=issue,
                )
                itf.detect_url()
                return itf
            case "book":
                city = verdict["city"].strip()  # type: ignore[typeddict-item]
                title = verdict.get("title") or None
                if allow_interactive_cg:
                    cg = CitationGroup.get_or_create_city(city)
                else:
                    # Non-interactive: only use an existing BOOK CitationGroup with this city name
                    cg = CitationGroup.select_one(
                        name=city, type=db_constants.ArticleType.BOOK
                    )
                if cg is None:
                    return None
                itf = cls.create(filename=filename, citation_group=cg, title=title)
                itf.detect_url()
                return itf
            case _:
                return None


def _make_cb_map(full_path: Path) -> CallbackMap:
    return {
        "open": lambda: subprocess.check_call(["open", str(full_path)]),
        "o": lambda: subprocess.check_call(["open", str(full_path)]),
    }


def extract_first_page_link(pdf_path: Path) -> str | None:
    doc = fitz.open(pdf_path)
    page = doc[0]

    links = page.get_links()
    # Each link dict can include 'uri', 'page', 'from', etc.
    uris = [link.get("uri") for link in links if link.get("uri")]

    if not uris:
        return None
    if len(uris) > 1:
        return None

    return uris[0]


class _JournalVerdict(TypedDict):
    type: Literal["journal"]
    journal_name: str
    series: NotRequired[str | None]
    volume: NotRequired[str | None]
    issue: NotRequired[str | None]


class _BookVerdict(TypedDict):
    type: Literal["book"]
    city: str
    title: NotRequired[str | None]


class _UnknownVerdict(TypedDict):
    type: Literal["unknown"]


_Verdict = _JournalVerdict | _BookVerdict | _UnknownVerdict


def _classify_pdf_with_gpt(pdf_path: Path, *, api_key: str) -> _Verdict | None:
    """Call GPT 5.2 with the PDF to classify as journal/book/unknown.

    Uses OpenAI Responses API if available, falling back to sending extracted
    first-page text when file upload fails.
    """
    try:
        return _classify_pdf_with_gpt_via_responses(pdf_path, api_key=api_key)
    except Exception as e:
        traceback.print_exc()
        print(f"Responses API failed: {e!r}; trying text fallback")
        # Fallback: extract first 2 pages' text and use chat completion
        try:
            text = _extract_pdf_text(pdf_path, max_pages=2)
            if not text.strip():
                return None
            return _classify_text_with_gpt(text, api_key=api_key)
        except Exception:
            traceback.print_exc()
            print("Text fallback failed")
            return None


def _classify_pdf_with_gpt_via_responses(pdf_path: Path, *, api_key: str) -> _Verdict:
    """Upload a compact preview PDF and call Responses API with model 'gpt-5.2'."""
    headers = {"Authorization": f"Bearer {api_key}"}
    # 1) Create and upload a preview selecting the first few informative pages
    preview_path = _make_informative_preview_pdf(
        pdf_path, target_pages=5, max_scan=40, min_chars=150
    )
    try:
        with preview_path.open("rb") as f:
            files = {"file": (preview_path.name, f, "application/pdf")}
            data = {"purpose": "assistants"}
            r = httpx.post(
                "https://api.openai.com/v1/files",
                headers=headers,
                files=files,
                data=data,
                timeout=60.0,
            )
        r.raise_for_status()
        file_id = r.json()["id"]
    finally:
        try:
            preview_path.unlink(missing_ok=True)
        except Exception:
            pass

    # 2) Call Responses API
    system_instructions = (
        "You are a bibliographic classifier for zoological references. "
        "Given a PDF file, decide if it is a journal volume or a book. "
        "If journal: return JSON with keys type='journal', journal_name, series (if any), volume, issue. "
        "If book: return JSON with keys type='book', city, title. "
        "If unsure: return JSON with type='unknown'. "
        "Return ONLY JSON, no commentary."
    )
    body = {
        "model": "gpt-5.2",
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": system_instructions},
                    {"type": "input_file", "file_id": file_id},
                ],
            }
        ],
        "max_output_tokens": 500,
    }
    r2 = httpx.post(
        "https://api.openai.com/v1/responses",
        headers={**headers, "Content-Type": "application/json"},
        content=json.dumps(body),
        timeout=120.0,
    )
    r2.raise_for_status()
    data = r2.json()
    print(f"Responses API raw output: {data!r}")
    # Responses API returns output_text in top-level sometimes; otherwise parse content
    text = data.get("output_text")
    if not text:
        # Try choices-like structure
        out: list[object] | str = data.get("output", []) or data.get("choices", [])
        if out and isinstance(out, list):
            # Heuristic extraction
            maybe_text = out[0]
            if isinstance(maybe_text, dict):
                text = maybe_text["content"][0]["text"]
    if not text:
        raise RuntimeError("No response text from Responses API")
    assert isinstance(text, str), repr(text)
    return _parse_verdict_json(text)


def _classify_text_with_gpt(text: str, *, api_key: str) -> _Verdict | None:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    prompt = (
        "You are a bibliographic classifier for zoological references.\n"
        "Decide if the text is from a journal volume or a book.\n"
        "If journal: return JSON with keys type='journal', journal_name, series (if any), volume, issue.\n"
        "If book: return JSON with keys type='book', city, title.\n"
        "If unsure: return JSON with type='unknown'.\n"
        "Return ONLY JSON, no commentary.\n\n"
        f"Text:\n{text[:20000]}"
    )
    body = {
        "model": "gpt-5.2",
        "messages": [
            {"role": "system", "content": "Return only strict JSON."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }
    r = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=body,
        timeout=60.0,
    )
    r.raise_for_status()
    data = r.json()
    text_out = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    if not text_out:
        return None
    assert isinstance(text_out, str), repr(text_out)
    return _parse_verdict_json(text_out)


def _parse_verdict_json(text: str) -> _Verdict:
    # Extract JSON from text (strip code fences if present)
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`\n")
        # Remove potential language hint like json\n
        first_nl = s.find("\n")
        if first_nl != -1 and s[:first_nl].lower() in {"json", "javascript"}:
            s = s[first_nl + 1 :]
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        # Try to find JSON substring
        m = re.search(r"\{[\s\S]*\}", s)
        if not m:
            return {"type": "unknown"}
        try:
            obj = json.loads(m.group(0))
        except Exception:
            return {"type": "unknown"}

    t = (obj.get("type") or "").strip().lower()
    if t == "journal":
        jname = str(obj.get("journal_name") or "").strip()
        if not jname:
            return {"type": "unknown"}
        return {
            "type": "journal",
            "journal_name": jname,
            "series": obj.get("series") or None,
            "volume": obj.get("volume") or None,
            "issue": obj.get("issue") or None,
        }
    if t == "book":
        city = str(obj.get("city") or "").strip()
        if not city:
            return {"type": "unknown"}
        return {"type": "book", "city": city, "title": obj.get("title") or None}
    return {"type": "unknown"}


def _extract_pdf_text(pdf_path: Path, *, max_pages: int = 2) -> str:
    doc = fitz.open(pdf_path)
    texts: list[str] = []
    pages = min(len(doc), max_pages)
    for i in range(pages):
        page = doc[i]
        texts.append(page.get_text("text"))
    return "\n\n".join(texts)


def _urlcache_get(domain: CacheDomain, key: str) -> str | None:
    rows = run_query(
        """
        SELECT content FROM url_cache WHERE domain = ? AND key = ?
        """,
        (domain.value, key),
    )
    if len(rows) == 1:
        return rows[0][0]
    return None


def _urlcache_set(domain: CacheDomain, key: str, content: str) -> None:
    run_query(
        """
        INSERT INTO url_cache(domain, key, content) VALUES(?, ?, ?)
        """,
        (domain.value, key, content),
    )


def _make_informative_preview_pdf(
    pdf_path: Path, *, target_pages: int = 5, max_scan: int = 40, min_chars: int = 150
) -> Path:
    """Create a temporary PDF using the first informative pages.

    Select up to target_pages among the first max_scan pages that have at least
    min_chars characters in their extracted text. If none are found, fall back
    to the first target_pages pages.
    """
    src = fitz.open(pdf_path)
    try:
        total = len(src)
        scan_upto = min(total, max_scan)
        indices: list[int] = []
        for i in range(scan_upto):
            page = src[i]
            txt = page.get_text("text")
            if len("".join(txt.split())) >= min_chars:
                indices.append(i)
                if len(indices) >= target_pages:
                    break
        if not indices:
            indices = list(range(min(total, target_pages)))

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf3:
            tmp_path = Path(tf3.name)
        dst = fitz.open()
        try:
            for idx in indices:
                dst.insert_pdf(src, from_page=idx, to_page=idx)
            dst.save(tmp_path)
        finally:
            dst.close()
        return tmp_path
    finally:
        src.close()
