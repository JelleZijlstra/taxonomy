import re
from pathlib import Path

import taxonomy

RESEARCH_NOTE_BYLINE_RE = re.compile(
    r"^_Jelle S\. Zijlstra, "
    r"(January|February|March|April|May|June|July|August|September|October|November|December) "
    r"\d{4}_$"
)


def _docs_root() -> Path:
    assert taxonomy.__file__ is not None
    return Path(taxonomy.__file__).parent.parent / "docs"


def _docs_link(path: Path, docs_root: Path) -> str:
    return f"/docs/{path.relative_to(docs_root).with_suffix('')}"


def _assert_markdown_files_linked_from(
    directory: Path, source: Path, docs_root: Path
) -> None:
    source_text = source.read_text()
    missing_links = {
        _docs_link(path, docs_root)
        for path in sorted(directory.glob("*.md"))
        if _docs_link(path, docs_root) not in source_text
    }
    assert not missing_links, f"Missing links from {source}: {missing_links}"


def test_biblio_notes_are_linked_from_citation_group_docs() -> None:
    docs_root = _docs_root()
    _assert_markdown_files_linked_from(
        docs_root / "biblio", docs_root / "citation-group.md", docs_root
    )


def test_research_notes_are_linked_from_research_notes_docs() -> None:
    docs_root = _docs_root()
    _assert_markdown_files_linked_from(
        docs_root / "research-notes", docs_root / "research-notes.md", docs_root
    )


def test_research_notes_index_is_linked_from_home_docs() -> None:
    docs_root = _docs_root()
    home_docs = (docs_root / "home.md").read_text()
    assert "/docs/research-notes" in home_docs


def test_research_notes_have_standard_byline() -> None:
    docs_root = _docs_root()
    missing_bylines: dict[Path, str | None] = {}
    for path in sorted((docs_root / "research-notes").glob("*.md")):
        lines = path.read_text().splitlines()
        byline = lines[2] if len(lines) > 2 else None
        if byline is None or RESEARCH_NOTE_BYLINE_RE.fullmatch(byline) is None:
            missing_bylines[path] = byline
    assert (
        not missing_bylines
    ), f"Missing standard research note bylines: {missing_bylines}"
