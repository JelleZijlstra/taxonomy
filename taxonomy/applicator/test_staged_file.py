import hashlib
from pathlib import Path
from typing import Any, Never

import pytest

from taxonomy.applicator import staged_file as recommendations
from taxonomy.config import Options
from taxonomy.db.models import Article


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


def _replacement(
    tmp_path: Path,
) -> tuple[Options, Article, bytes, bytes, dict[str, Any]]:
    from taxonomy.db.constants import ArticleKind

    options = _options(tmp_path)._replace(library_path=tmp_path / "library")
    options.library_path.mkdir()
    old, new = b"%PDF-1.4\nincomplete\n", b"%PDF-1.4\ncomplete replacement\n"
    article = Article.virtual(name="Source.pdf", path=None, kind=ArticleKind.electronic)
    (options.library_path / article.name).write_bytes(old)
    (options.downloads_path / "source.pdf").write_bytes(new)
    row = _row(new)
    row.update(
        action="replace_article_file",
        article={"id": 1, "name": article.name, "path": None},
        old_file={"sha256": hashlib.sha256(old).hexdigest(), "size": len(old)},
    )
    return options, article, old, new, row


def test_replace_article_preserves_old_and_retries(tmp_path: Path) -> None:
    options, article, old, new, row = _replacement(tmp_path)
    recommendation = recommendations.parse_recommendation(row, 1)
    plan = recommendations.build_plan(
        (recommendation,), options=options, get_article=lambda _: article
    )
    action = plan.actions[0]
    recommendations.execute_plan(plan, apply=False)
    assert action.destination_path.read_bytes() == old
    assert action.source_path.read_bytes() == new
    assert action.backup_path is not None and not action.backup_path.exists()

    def fail_index(_article: Article) -> Never:
        raise RuntimeError("index unavailable")

    with pytest.raises(RuntimeError, match="index unavailable"):
        recommendations.execute_plan(plan, apply=True, refresh_article=fail_index)
    assert action.destination_path.read_bytes() == new
    assert action.backup_path.read_bytes() == old
    assert action.source_path.exists()
    retried = recommendations.build_plan(
        (recommendation,), options=options, get_article=lambda _: article
    )
    assert retried.actions[0].already_applied
    refreshed: list[Article] = []
    recommendations.execute_plan(retried, apply=True, refresh_article=refreshed.append)
    assert refreshed == [article]
    assert not action.source_path.exists()
    assert action.backup_path.read_bytes() == old
    completed = recommendations.build_plan(
        (recommendation,), options=options, get_article=lambda _: article
    )
    recommendations.execute_plan(completed, apply=True, refresh_article=lambda _: None)
    assert action.destination_path.read_bytes() == new


@pytest.mark.parametrize(
    "failure",
    [
        "old_bytes",
        "new_bytes",
        "name",
        "path",
        "kind",
        "backup",
        "symlink",
        "missing_source",
    ],
)
def test_replace_article_rejects_stale_state(tmp_path: Path, failure: str) -> None:
    from taxonomy.db.constants import ArticleKind

    options, article, _old, _new, row = _replacement(tmp_path)
    recommendation = recommendations.parse_recommendation(row, 1)
    if failure == "old_bytes":
        (options.library_path / article.name).write_bytes(b"changed")
    elif failure == "new_bytes":
        (options.downloads_path / "source.pdf").write_bytes(b"changed")
    elif failure == "name":
        article.name = "Other.pdf"
    elif failure == "path":
        article.path = "Elsewhere"
    elif failure == "kind":
        article.kind = ArticleKind.no_copy
    elif failure == "backup":
        backup = (
            options.new_path
            / "Not to be cataloged"
            / f"replaced-article-1-{row['old_file']['sha256']}.pdf"
        )
        backup.parent.mkdir()
        backup.write_bytes(b"unrelated")
    elif failure == "symlink":
        source = options.downloads_path / "source.pdf"
        moved = options.downloads_path / "real.pdf"
        source.rename(moved)
        source.symlink_to(moved)
    elif failure == "missing_source":
        (options.downloads_path / "source.pdf").unlink()
    with pytest.raises(recommendations.RecommendationError):
        recommendations.build_plan(
            (recommendation,), options=options, get_article=lambda _: article
        )


def test_replace_article_rechecks_bytes_after_planning(tmp_path: Path) -> None:
    options, article, _old, _new, row = _replacement(tmp_path)
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(row, 1),),
        options=options,
        get_article=lambda _: article,
    )
    (options.library_path / article.name).write_bytes(b"changed meanwhile")
    with pytest.raises(recommendations.RecommendationError):
        recommendations.execute_plan(plan, apply=True, refresh_article=lambda _: None)
    assert (options.library_path / article.name).read_bytes() == b"changed meanwhile"
    assert plan.actions[0].source_path.exists()


def test_replacement_duplicate_destination_and_parser_guards(tmp_path: Path) -> None:
    options, article, _old, new, row = _replacement(tmp_path)
    rec = recommendations.parse_recommendation(row, 1)
    with pytest.raises(
        recommendations.RecommendationError, match="duplicate replacement"
    ):
        recommendations.build_plan(
            (rec, rec), options=options, get_article=lambda _: article
        )
    row["file"]["destination_name"] = "different.pdf"
    with pytest.raises(
        recommendations.RecommendationError, match="destination comes from"
    ):
        recommendations.parse_recommendation(row, 1)
    del row["file"]["destination_name"]
    row["old_file"] = {"sha256": hashlib.sha256(new).hexdigest(), "size": len(new)}
    with pytest.raises(recommendations.RecommendationError, match="must differ"):
        recommendations.parse_recommendation(row, 1)
