import hashlib
from pathlib import Path
from typing import Any

import pytest

from taxonomy.applicator import staged_file as recommendations
from taxonomy.config import Options


def _row(content: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "move_to_not_cataloged",
        "confidence": "high",
        "reason": "The useful extract has already been cataloged.",
        "evidence": [{"kind": "duplicate_check", "text": "Exact bytes checked."}],
        "file": {
            "source_root": "downloads",
            "source_path": "source.pdf",
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
        },
    }


def _options(tmp_path: Path) -> Options:
    new_path = tmp_path / "new"
    downloads_path = tmp_path / "downloads"
    new_path.mkdir()
    downloads_path.mkdir()
    return Options(new_path=new_path, downloads_path=downloads_path)


def test_moves_download_to_not_cataloged(tmp_path: Path) -> None:
    content = b"%PDF-1.4\nsource\n"
    options = _options(tmp_path)
    source = options.downloads_path / "source.pdf"
    source.write_bytes(content)
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(_row(content), 1),), options=options
    )

    recommendations.execute_plan(plan, apply=True)

    assert not source.exists()
    assert (
        options.new_path / "Not to be cataloged" / "source.pdf"
    ).read_bytes() == content


def test_exact_completed_move_cleans_duplicate_source(tmp_path: Path) -> None:
    content = b"%PDF-1.4\nsource\n"
    options = _options(tmp_path)
    source = options.downloads_path / "source.pdf"
    source.write_bytes(content)
    destination = options.new_path / "Not to be cataloged" / "source.pdf"
    destination.parent.mkdir()
    destination.write_bytes(content)
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(_row(content), 1),), options=options
    )

    recommendations.execute_plan(plan, apply=True)

    assert plan.actions[0].already_applied
    assert not source.exists()
    assert destination.read_bytes() == content


def test_conflicting_destination_is_rejected(tmp_path: Path) -> None:
    content = b"%PDF-1.4\nsource\n"
    options = _options(tmp_path)
    (options.downloads_path / "source.pdf").write_bytes(content)
    destination = options.new_path / "Not to be cataloged" / "source.pdf"
    destination.parent.mkdir()
    destination.write_bytes(b"different")

    with pytest.raises(recommendations.RecommendationError, match="size changed"):
        recommendations.build_plan(
            (recommendations.parse_recommendation(_row(content), 1),), options=options
        )


def test_dry_run_does_not_move(tmp_path: Path) -> None:
    content = b"%PDF-1.4\nsource\n"
    options = _options(tmp_path)
    source = options.downloads_path / "source.pdf"
    source.write_bytes(content)
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(_row(content), 1),), options=options
    )

    recommendations.execute_plan(plan, apply=False)

    assert source.exists()
    assert not (options.new_path / "Not to be cataloged" / "source.pdf").exists()
