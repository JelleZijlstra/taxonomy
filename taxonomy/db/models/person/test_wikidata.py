import copy
from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from taxonomy.apis import wikidata as api
from taxonomy.db import models
from taxonomy.db.constants import NamingConvention, PersonType
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint_types import LintIssue
from taxonomy.db.models.person import Person, lint
from taxonomy.db.models.person import wikidata as evidence


def person(**kwargs: Any) -> Person:
    return Person.virtual(
        **{
            "family_name": "Smith",
            "given_names": "Jane",
            "initials": None,
            "suffix": None,
            "tussenvoegsel": None,
            "birth": None,
            "death": None,
            "type": PersonType.checked,
            "target": None,
            "bio": None,
            "ol_id": None,
            "naming_convention": NamingConvention.english,
            "tags": (),
            **kwargs,
        }
    )


def claim(value: Any, **kwargs: Any) -> dict[str, Any]:
    return {
        "id": "Q42$statement",
        "rank": "normal",
        "mainsnak": {"snaktype": "value", "datavalue": {"value": value}},
        "references": [
            {
                "hash": "source",
                "snaks": {
                    "P854": [{"datavalue": {"value": "https://example.org/biography"}}]
                },
            }
        ],
        **kwargs,
    }


def date_claim(
    time: str = "+1900-01-01T00:00:00Z", precision: int = 9, **kwargs: Any
) -> dict[str, Any]:
    return claim(
        {
            "time": time,
            "precision": precision,
            "calendarmodel": evidence.GREGORIAN,
            **kwargs,
        }
    )


def item(qid: str = "Q42", claims: dict[str, Any] | None = None) -> api.Item:
    return api.Item(
        qid,
        api.Snapshot(
            "2026-01-01T00:00:00+00:00",
            {
                "entities": {
                    qid: {
                        "id": qid,
                        "type": "item",
                        "lastrevid": 123,
                        "claims": {"P31": [claim({"id": "Q5"})], **(claims or {})},
                    }
                }
            },
        ),
    )


def link(qid: str = "Q42") -> api.WikipediaLink:
    return api.WikipediaLink(
        "https://en.wikipedia.org/wiki/Smith",
        api.Snapshot(
            "2026-01-01T00:00:00+00:00",
            {
                "query": {
                    "pages": [
                        {"title": "Smith", "ns": 0, "pageprops": {"wikibase_item": qid}}
                    ]
                }
            },
        ),
    )


@pytest.mark.parametrize(
    ("local", "time", "precision", "status", "external"),
    [
        (None, "+1900-01-01T00:00:00Z", 9, "missing_local", "1900"),
        ("1900", "+1900-00-00T00:00:00Z", 9, "match", "1900"),
        ("1900", "+1900-05-03T00:00:00Z", 11, "match", "1900"),
        ("1900-05-03", "+1900-00-00T00:00:00Z", 9, "match", "1900"),
        ("1901", "+1900-00-00T00:00:00Z", 9, "conflict", "1900"),
        ("c. 1900", "+1900-00-00T00:00:00Z", 9, "review", "1900"),
        (None, "+1900-00-00T00:00:00Z", 8, "no_external_value", None),
        (None, "-0300-00-00T00:00:00Z", 9, "missing_local", "-300"),
        (None, "+1900-02-30T00:00:00Z", 11, "review", None),
    ],
)
def test_date_precision(
    local: str | None, time: str, precision: int, status: str, external: str | None
) -> None:
    comparison = evidence.compare_date(local, [date_claim(time, precision)])
    assert comparison["status"] == status
    assert comparison["external"] == ([] if precision < 9 else [external])


@pytest.mark.parametrize(
    "claims",
    [
        [date_claim(before=1)],
        [
            date_claim()
            | {"qualifiers": {"P1480": [{"datavalue": {"value": {"id": "Q5727902"}}}]}}
        ],
        [date_claim(), date_claim("+1901-00-00T00:00:00Z")],
    ],
)
def test_ambiguous_dates_retain_evidence(claims: list[dict[str, Any]]) -> None:
    before = copy.deepcopy(claims)
    comparison = evidence.compare_date(None, claims)
    assert comparison["status"] == "review"
    assert comparison["statements"] == before == claims


def test_deprecated_dates_do_not_compete_with_current_statements() -> None:
    data = item(
        claims={
            "P569": [
                date_claim(),
                date_claim("+1901-01-01T00:00:00Z") | {"rank": "deprecated"},
            ]
        }
    )
    assert data.entity is not None
    comparison = evidence.compare_person(person(birth="1900"), data.entity)
    assert comparison["birth"]["status"] == "match"
    assert len(data.entity["claims"]["P569"]) == 2


@pytest.mark.parametrize(
    ("local", "claims", "status"),
    [
        (set(), [claim("OL123A")], "missing_local"),
        ({"OL123A"}, [claim("OL123A")], "match"),
        ({"OL1A"}, [claim("OL123A")], "conflict"),
        ({"OL1A"}, [claim("OL1A"), claim("OL123A")], "review"),
        (set(), [claim(None, mainsnak={"snaktype": "somevalue"})], "review"),
        (set(), [], "no_external_value"),
    ],
)
def test_identifier_comparisons(
    local: set[str], claims: list[dict[str, Any]], status: str
) -> None:
    assert evidence.compare_identifier(local, claims)["status"] == status


@pytest.mark.parametrize(
    ("local", "external", "status"),
    [
        ({"OL123A"}, ["OL123A"], "match"),
        (set(), ["OL123A"], "review"),
        ({"OL1A"}, ["OL123A"], "review"),
        ({"OL123A"}, ["OL123A", "OL456A"], "review"),
        ({"OL123A", "OL456A"}, ["OL123A", "OL456A"], "match"),
    ],
)
def test_qualified_identifier_comparisons(
    local: set[str], external: list[str], status: str
) -> None:
    claims = [
        claim(value, qualifiers={"P1810": [{"datavalue": {"value": "Smith"}}]})
        for value in external
    ]
    before = copy.deepcopy(claims)
    comparison = evidence.compare_identifier(local, claims)
    assert comparison["status"] == status
    assert comparison["statements"] == before == claims


def test_wikidata_profile_matching_orcid_with_name_qualifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orcid = "0000-0002-4797-609X"
    p = person(
        tags=(models.tags.PersonTag.ORCID(orcid), models.tags.PersonTag.Wikidata("Q42"))
    )
    data = item(
        claims={
            "P496": [
                claim(
                    orcid,
                    qualifiers={"P1810": [{"datavalue": {"value": "Thomas Ziegler"}}]},
                )
            ]
        }
    )
    monkeypatch.setattr(api, "get_item", Mock(return_value=data))
    assert not list(lint.wikidata_profile.linter(p, _wiki_lint_config()))


def test_wikidata_profile_autofills_missing_orcid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orcid_id = "0000-0001-7707-0013"
    p = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    data = item(claims={"P496": [claim(orcid_id), claim(orcid_id)]})
    before = copy.deepcopy(data.snapshot.response)
    monkeypatch.setattr(api, "get_item", Mock(return_value=data))
    issues = list(lint.wikidata_profile(p, _wiki_lint_config()))
    assert len(issues) == 1 and issues[0].fix is not None
    assert f"https://orcid.org/{orcid_id}" in str(issues[0])
    assert not list(p.get_tags(p.tags, models.tags.PersonTag.ORCID))
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    assert list(p.get_tags(p.tags, models.tags.PersonTag.ORCID)) == [
        models.tags.PersonTag.ORCID(orcid_id)
    ]
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    assert data.snapshot.response == before


@pytest.mark.parametrize(
    ("local", "claims"),
    [
        ("0000-0002-4797-609X", [claim("0000-0001-7707-0013")]),
        (None, [claim("0000-0001-7707-0013"), claim("0000-0002-4797-609X")]),
        (None, [claim("0000-0000-0000-0000")]),
        (None, [claim(None, mainsnak={"snaktype": "somevalue"})]),
        (
            None,
            [
                claim(
                    "0000-0001-7707-0013",
                    qualifiers={
                        "P1480": [{"datavalue": {"value": {"id": "Q5727902"}}}]
                    },
                )
            ],
        ),
    ],
)
def test_wikidata_profile_does_not_autofill_uncertain_or_conflicting_orcid(
    monkeypatch: pytest.MonkeyPatch, local: str | None, claims: list[dict[str, Any]]
) -> None:
    tags = (models.tags.PersonTag.Wikidata("Q42"),)
    p = person(tags=(*tags, models.tags.PersonTag.ORCID(local)) if local else tags)
    before = p.tags
    monkeypatch.setattr(
        api, "get_item", Mock(return_value=item(claims={"P496": claims}))
    )
    issues = list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1 and issues[0].fix is None
    assert p.tags == before


def test_aliases_use_canonical_identifier_and_report_one_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    alias = person(
        type=PersonType.alias,
        target=canonical,
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),),
    )
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link()))
    monkeypatch.setattr(api, "get_item", Mock(return_value=item()))
    assert evidence.effective_qids(alias) == {"Q42"}
    rows = evidence.report_people([canonical, alias], person_ids={alias.id})
    assert len(rows) == 1
    assert rows[0]["person"]["id"] == canonical.id
    assert rows[0]["candidate_qid"] == "Q42"
    assert len(rows[0]["sources"]) == 2
    assert canonical.tags == (models.tags.PersonTag.Wikidata("Q42"),)
    assert alias.type is PersonType.alias


def test_alias_tag_is_reported_for_review(monkeypatch: pytest.MonkeyPatch) -> None:
    canonical = person()
    alias = person(
        type=PersonType.alias,
        target=canonical,
        tags=(models.tags.PersonTag.Wikidata("Q42"),),
    )
    monkeypatch.setattr(api, "get_item", Mock(return_value=item()))
    row = evidence.report_people([canonical, alias])[0]
    assert row["candidate_qid"] is None
    assert "alias/redirect" in row["issues"][0]


def test_duplicate_resolved_ids_block_both_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    people = [
        person(
            tags=(
                models.tags.PersonTag.Wiki(f"https://en.wikipedia.org/wiki/Smith_{i}"),
            )
        )
        for i in (1, 2)
    ]
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link()))
    monkeypatch.setattr(api, "get_item", Mock(return_value=item()))
    rows = evidence.report_people(people)
    assert len(rows) == 2
    assert all(
        row["status"] == "review" and row["candidate_qid"] is None for row in rows
    )
    assert all(
        f"canonical Persons {sorted(p.id for p in people)}" in row["issues"][0]
        for row in rows
    )


def test_duplicate_outside_limited_scope_is_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    people = [
        person(
            tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),)
        ),
        person(tags=(models.tags.PersonTag.Wikidata("Q42"),)),
    ]
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link()))
    monkeypatch.setattr(api, "get_item", Mock(return_value=item()))
    rows = evidence.report_people(people, person_ids={people[0].id}, limit=1)
    assert len(rows) == 1
    assert rows[0]["status"] == "review"


def test_network_errors_are_not_missing_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        api, "resolve_wikipedia", Mock(side_effect=httpx.ConnectError("offline"))
    )
    row = evidence.report_people(
        [
            person(
                tags=(
                    models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),
                )
            )
        ]
    )[0]
    assert row["status"] == "error"
    assert row["candidate_qid"] is None
    assert "offline" in row["errors"][0]


@pytest.mark.parametrize(
    ("claims", "expected"),
    [
        ([], False),
        ([claim({"id": "Q5"})], True),
        ([claim({"id": "Q5"}), claim({"id": "Q148057"})], True),
        ([claim({"id": "Q148057"})], False),
        ([claim({"id": "Q5"}, rank="deprecated"), claim({"id": "Q148057"})], False),
        ([claim(None, mainsnak={"snaktype": "somevalue"})], False),
    ],
)
def test_human_classification(claims: list[dict[str, Any]], *, expected: bool) -> None:
    data = item(claims={"P31": claims})
    assert data.entity is not None
    assert evidence.is_human(data.entity) is expected


def test_human_with_additional_classification_can_link_and_fill_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),)
    )
    data = item(
        claims={
            "P31": [claim({"id": "Q5"}), claim({"id": "Q148057"})],
            "P569": [date_claim()],
        }
    )
    monkeypatch.setattr(api, "get_item", Mock(return_value=data))
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link()))
    cfg = _wiki_lint_config(autofix=True)
    assert list(lint.wikidata_entity_type(p, cfg)) == []
    assert list(lint.wikidata_from_wikipedia(p, cfg)) == []
    assert evidence.stored_qids(p) == {"Q42"}
    assert list(lint.wikidata_profile(p, cfg)) == []
    assert p.birth == "1900"


def test_nonhuman_item_does_not_propose_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        api,
        "get_item",
        Mock(
            return_value=item(
                claims={"P31": [claim({"id": "Q4167410"})], "P569": [date_claim()]}
            )
        ),
    )
    row = evidence.report_people(
        [person(tags=(models.tags.PersonTag.Wikidata("Q42"),))]
    )[0]
    assert row["status"] == "review"
    assert row["candidate_qid"] is None
    assert "comparisons" not in row["items"][0]
    assert row["items"][0]["snapshot"]["response"]["entities"]["Q42"]["claims"]["P569"]


def test_conflicting_links_preserve_both_items(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "get_item", Mock(side_effect=item))
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link("Q43")))
    row = evidence.report_people(
        [
            person(
                tags=(
                    models.tags.PersonTag.Wikidata("Q42"),
                    models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),
                )
            )
        ]
    )[0]
    assert row["status"] == "review"
    assert len(row["items"]) == 2
    assert row["candidate_qid"] is None


def test_broken_target_is_an_error_row() -> None:
    broken = person(
        type=PersonType.alias,
        target=None,
        tags=(models.tags.PersonTag.Wikidata("Q42"),),
    )
    row = evidence.report_people([broken])[0]
    assert row["status"] == "error"
    assert "Broken" in row["errors"][0]


def test_unknown_person_id_fails_explicitly() -> None:
    with pytest.raises(ValueError, match="Unknown or deleted"):
        evidence.report_people([person()], person_ids={2})


def test_unchecked_person_can_have_wikidata_tag() -> None:
    p = person(type=PersonType.unchecked, tags=(models.tags.PersonTag.Wikidata("Q42"),))
    issues = list(p.lint(LintConfig(autofix=False, interactive=False)))
    assert not any("unchecked but tags set" in str(issue) for issue in issues)
    p.tags = (models.tags.PersonTag.Wikidata("q42"),)  # type: ignore[assignment]
    assert any(
        "invalid Wikidata ID" in str(issue)
        for issue in p.lint(LintConfig(autofix=False, interactive=False))
    )


def test_profile_lint_requires_network_and_preserves_conflicting_local_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(birth="1901", tags=(models.tags.PersonTag.Wikidata("Q42"),))
    before = evidence.local_snapshot(p)
    monkeypatch.setattr(
        api, "get_item", Mock(return_value=item(claims={"P569": [date_claim()]}))
    )
    assert lint.wikidata_profile.required_resources == {
        LintResource.NETWORK,
        LintResource.SLOW,
    }
    issues = list(
        lint.wikidata_profile.linter(p, LintConfig(autofix=True, interactive=False))
    )
    assert any("birth: conflict" in str(issue) for issue in issues)
    assert evidence.local_snapshot(p) == before


@pytest.mark.parametrize(
    ("prop", "value"), [("P496", "0000-0000-0000-0000"), ("P648", "OL123W")]
)
def test_invalid_external_identifiers_require_review(prop: str, value: str) -> None:
    data = item(claims={prop: [claim(value)]})
    assert data.entity is not None
    comparisons = evidence.compare_person(person(), data.entity)
    assert comparisons["orcid" if prop == "P496" else "ol_id"]["status"] == "review"


def test_offline_duplicate_lint_ignores_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    second = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    alias = person(
        type=PersonType.alias,
        target=first,
        tags=(models.tags.PersonTag.Wikidata("Q42"),),
    )
    monkeypatch.setattr(
        Person, "select_valid", Mock(return_value=[first, second, alias])
    )
    Person.clear_lint_caches()
    cfg = LintConfig(autofix=False, interactive=False)
    try:
        issues = [
            issue
            for p in (first, second)
            for issue in lint.duplicate_wikidata.linter(p, cfg)
        ]
        assert len(issues) == 1
        assert "Q42" in str(issues[0])
        assert not list(lint.duplicate_wikidata.linter(alias, cfg))
        assert any(
            "canonical Person" in str(issue)
            for issue in lint.wikidata_tags.linter(alias, cfg)
        )
    finally:
        Person.clear_lint_caches()


@pytest.mark.parametrize("first_primary", [False, True])
@pytest.mark.parametrize("second_primary", [False, True])
def test_duplicate_openlibrary_checks_primary_and_extra_ids(
    monkeypatch: pytest.MonkeyPatch, *, first_primary: bool, second_primary: bool
) -> None:
    people = [
        person(
            ol_id="OL123A" if primary else "OL456A",
            tags=() if primary else (models.tags.PersonTag.OpenLibrary("OL123A"),),
        )
        for primary in (first_primary, second_primary)
    ]
    # Give nonshared primaries distinct IDs when both shared IDs are extra tags.
    if not second_primary:
        people[1].ol_id = "OL789A"
    monkeypatch.setattr(Person, "select_valid", Mock(return_value=people))
    before = [(p.ol_id, p.tags, p.type, p.target) for p in people]
    Person.clear_lint_caches()
    try:
        issues = [
            issue
            for p in people
            for issue in lint.duplicate_openlibrary.linter(
                p, LintConfig(autofix=True, interactive=False)
            )
        ]
        assert len(issues) == 1
        assert "https://openlibrary.org/authors/OL123A" in str(issues[0])
        assert [(p.ol_id, p.tags, p.type, p.target) for p in people] == before
    finally:
        Person.clear_lint_caches()


def test_duplicate_openlibrary_ignores_aliases_and_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = person(
        ol_id="OL123A", tags=(models.tags.PersonTag.OpenLibrary("OL123A"),)
    )
    people = [canonical] + [
        person(
            type=kind,
            target=canonical,
            ol_id="OL123A",
            tags=(models.tags.PersonTag.OpenLibrary("OL123A"),),
        )
        for kind in (
            PersonType.alias,
            PersonType.hard_redirect,
            PersonType.soft_redirect,
            PersonType.deleted,
        )
    ]
    monkeypatch.setattr(Person, "select_valid", Mock(return_value=people))
    Person.clear_lint_caches()
    try:
        for p in people:
            assert not list(
                lint.duplicate_openlibrary.linter(
                    p, LintConfig(autofix=False, interactive=False)
                )
            )
    finally:
        Person.clear_lint_caches()


def test_multiple_wikidata_ids_require_review() -> None:
    p = person(
        tags=(
            models.tags.PersonTag.Wikidata("Q42"),
            models.tags.PersonTag.Wikidata("Q43"),
        )
    )
    issues = list(
        lint.wikidata_tags.linter(p, LintConfig(autofix=False, interactive=False))
    )
    assert any("multiple Wikidata IDs" in str(issue) for issue in issues)


def test_selected_person_without_links_gets_a_row() -> None:
    p = person()
    assert evidence.report_people([p], person_ids={p.id})[0]["status"] == "no_links"


def test_selected_broken_alias_without_links_gets_an_error_row() -> None:
    p = person(type=PersonType.alias)
    assert evidence.report_people([p], person_ids={p.id})[0]["status"] == "error"


def _wiki_lint_config(*, autofix: bool = False) -> LintConfig:
    return LintConfig(
        autofix=autofix,
        interactive=False,
        available_resources=frozenset({LintResource.NETWORK, LintResource.SLOW}),
    )


def test_wikipedia_lint_proposes_then_applies_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiki_tag = models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith")
    p = person(birth="1901", tags=(wiki_tag,))
    resolve = Mock(return_value=link())
    monkeypatch.setattr(api, "resolve_wikipedia", resolve)
    # A biography discrepancy is distinct from the existing identity link.
    monkeypatch.setattr(
        api, "get_item", Mock(return_value=item(claims={"P569": [date_claim()]}))
    )
    issues = list(lint.wikidata_from_wikipedia(p, _wiki_lint_config()))
    assert len(issues) == 1 and issues[0].fix is not None
    assert issues[0].code == "wikidata_from_wikipedia"
    assert p.tags == (wiki_tag,)
    assert list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True))) == []
    assert list(p.tags) == [wiki_tag, models.tags.PersonTag.Wikidata("Q42")]
    assert p.birth == "1901"
    resolve.reset_mock()
    assert list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True))) == []
    resolve.assert_not_called()


@pytest.mark.parametrize(
    "kind",
    [
        PersonType.alias,
        PersonType.hard_redirect,
        PersonType.soft_redirect,
        PersonType.deleted,
    ],
)
def test_wikipedia_lint_skips_noncanonical_people(
    monkeypatch: pytest.MonkeyPatch, kind: PersonType
) -> None:
    resolve = Mock()
    monkeypatch.setattr(api, "resolve_wikipedia", resolve)
    p = person(
        type=kind,
        target=person(),
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),),
    )
    assert list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True))) == []
    resolve.assert_not_called()


@pytest.mark.parametrize(
    "tags",
    [
        (),
        (
            models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),
            models.tags.PersonTag.Wikidata("Q43"),
        ),
    ],
)
def test_wikipedia_lint_skips_missing_links_and_existing_ids(
    monkeypatch: pytest.MonkeyPatch, tags: tuple[Any, ...]
) -> None:
    resolve = Mock()
    monkeypatch.setattr(api, "resolve_wikipedia", resolve)
    p = person(tags=tags)
    assert list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True))) == []
    assert p.tags == tags
    resolve.assert_not_called()


@pytest.mark.parametrize(
    "failure", ["ambiguous_page", "missing_qid", "missing_item", "nonhuman", "network"]
)
def test_wikipedia_lint_does_not_autofix_unresolved_links(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    resolved = link()
    data = item()
    if failure == "ambiguous_page":
        resolved.snapshot.response["query"]["pages"][0]["pageprops"][
            "disambiguation"
        ] = ""
    elif failure == "missing_qid":
        resolved.snapshot.response["query"]["pages"][0]["pageprops"] = {}
    elif failure == "missing_item":
        data.snapshot.response["entities"] = {"Q42": {"missing": True}}
    elif failure == "nonhuman":
        data = item(claims={"P31": [claim({"id": "Q4167410"})]})
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=resolved))
    fetch = Mock(return_value=data)
    if failure == "network":
        fetch.side_effect = httpx.ConnectError("offline")
    monkeypatch.setattr(api, "get_item", fetch)
    p = person(
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),)
    )
    before = p.tags
    issues = list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True)))
    if failure == "nonhuman":
        assert issues == []
        issues = list(lint.wikidata_entity_type(p, _wiki_lint_config(autofix=True)))
        assert issues[0].code == "wikidata_entity_type"
    assert len(issues) == 1 and issues[0].fix is None
    assert p.tags == before


@pytest.mark.parametrize("same_identity", [True, False])
def test_wikipedia_lint_requires_all_links_to_agree_after_redirects(
    monkeypatch: pytest.MonkeyPatch, *, same_identity: bool
) -> None:
    p = person(
        tags=(
            models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),
            models.tags.PersonTag.Wiki("https://fr.wikipedia.org/wiki/Smith"),
        )
    )
    monkeypatch.setattr(
        api, "resolve_wikipedia", Mock(side_effect=[link("Q42"), link("Q43")])
    )
    monkeypatch.setattr(
        api,
        "get_item",
        Mock(side_effect=[item("Q42"), item("Q42" if same_identity else "Q43")]),
    )
    issues = list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True)))
    if same_identity:
        assert issues == []
        assert evidence.stored_qids(p) == {"Q42"}
    else:
        assert len(issues) == 1 and issues[0].fix is None
        assert "different Wikidata identities" in str(issues[0])
        assert not evidence.stored_qids(p)


def test_wikipedia_lint_requires_network_and_slow_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolve = Mock()
    monkeypatch.setattr(api, "resolve_wikipedia", resolve)
    p = person(
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),)
    )
    for resources in (
        frozenset(),
        frozenset({LintResource.NETWORK}),
        frozenset({LintResource.SLOW}),
    ):
        assert (
            list(
                lint.wikidata_from_wikipedia(
                    p,
                    LintConfig(
                        autofix=True, interactive=False, available_resources=resources
                    ),
                )
            )
            == []
        )
    resolve.assert_not_called()


def test_same_year_at_different_precisions_is_one_value() -> None:
    claims = [
        date_claim("+1877-01-01T00:00:00Z", 9),
        date_claim("+1877-03-28T00:00:00Z", 11),
    ]
    assert evidence.compare_date("1877", claims)["status"] == "match"
    missing = evidence.compare_date(None, claims)
    assert missing["status"] == "missing_local"
    assert missing["suggested"] == "1877"
    assert missing["external"] == ["1877", "1877"]


def test_brandt_calendar_qualifier_does_not_hide_year() -> None:
    birth = date_claim("+1802-05-25T00:00:00Z", 11)
    birth["qualifiers"] = {
        "P1480": [{"snaktype": "value", "datavalue": {"value": {"id": "Q27055388"}}}]
    }
    assert evidence.compare_date("1802", [birth])["status"] == "match"
    assert evidence.compare_date(None, [birth])["suggested"] == "1802"
    death = date_claim("+1879-07-03T00:00:00Z", 11, calendarmodel=evidence.JULIAN)
    death["qualifiers"] = {
        "P1480": [
            {"datavalue": {"value": {"id": qid}}} for qid in ("Q26877143", "Q18195782")
        ]
    }
    claims = [death, date_claim("+1879-07-15T00:00:00Z", 11)]
    assert evidence.compare_date("1879", claims)["status"] == "match"
    assert evidence.compare_date(None, claims)["suggested"] == "1879"


@pytest.mark.parametrize("snaktype", ["somevalue", "novalue"])
def test_absent_wikidata_date_does_not_conflict_with_local_year(snaktype: str) -> None:
    claims = [claim(None, mainsnak={"snaktype": snaktype})]
    for local in (None, "1802"):
        assert evidence.compare_date(local, claims)["status"] == "no_external_value"
    assert evidence.compare_date("1802", [])["status"] == "no_external_value"


def test_uncertain_date_can_match_but_cannot_fill_a_year() -> None:
    birth = date_claim("+1802-05-25T00:00:00Z", 11)
    birth["qualifiers"] = {"P1480": [{"datavalue": {"value": {"id": "Q5727902"}}}]}
    assert evidence.compare_date("1802", [birth])["status"] == "match"
    assert evidence.compare_date(None, [birth])["status"] == "review"
    assert evidence.compare_date("1801", [birth])["status"] == "review"


def test_profile_autofills_years_and_single_openlibrary_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    monkeypatch.setattr(
        api,
        "get_item",
        Mock(
            return_value=item(
                claims={
                    "P569": [
                        date_claim("+1877-01-01T00:00:00Z", 9),
                        date_claim("+1877-03-28T00:00:00Z", 11),
                    ],
                    "P570": [date_claim("+1949-08-19T00:00:00Z", 11)],
                    "P648": [claim("OL123A"), claim("OL123A")],
                }
            )
        ),
    )
    issues = list(lint.wikidata_profile(p, _wiki_lint_config()))
    assert len(issues) == 3 and all(issue.fix for issue in issues)
    assert [p.birth, p.death, p.ol_id] == [None, None, None]
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    assert (p.birth, p.death, p.ol_id) == ("1877", "1949", "OL123A")
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []


def test_multiple_openlibrary_ids_require_primary_choice_then_add_extras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    monkeypatch.setattr(
        api,
        "get_item",
        Mock(return_value=item(claims={"P648": [claim("OL123A"), claim("OL456A")]})),
    )
    issues = list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1 and issues[0].fix is None
    assert "most data" in str(issues[0])
    assert "https://openlibrary.org/authors/OL123A" in str(issues[0])
    assert "https://openlibrary.org/authors/OL456A" in str(issues[0])
    assert p.ol_id is None and len(p.tags) == 1
    p.ol_id = "OL456A"
    issues = list(lint.wikidata_profile(p, _wiki_lint_config()))
    assert len(issues) == 1 and issues[0].fix is not None
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    assert p.ol_id == "OL456A"
    assert p.get_ol_ids() == {"OL123A", "OL456A"}
    assert models.tags.PersonTag.OpenLibrary("OL123A") in p.tags
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []


def test_primary_not_in_wikidata_is_preserved_and_new_id_is_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(ol_id="OL456A", tags=(models.tags.PersonTag.Wikidata("Q42"),))
    monkeypatch.setattr(
        api, "get_item", Mock(return_value=item(claims={"P648": [claim("OL123A")]}))
    )
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    assert p.ol_id == "OL456A"
    assert p.get_ol_ids() == {"OL123A", "OL456A"}
    assert list(lint.wikidata_profile(p, _wiki_lint_config())) == []


def test_known_extras_also_count_when_choosing_primary() -> None:
    p = person(tags=(models.tags.PersonTag.OpenLibrary("OL456A"),))
    assert (
        evidence.compare_openlibrary(p, [claim("OL123A")])["status"] == "choose_primary"
    )


def test_redundant_extra_tag_is_removed_after_primary_changes() -> None:
    p = person(
        ol_id="OL123A",
        tags=(
            models.tags.PersonTag.OpenLibrary("OL123A"),
            models.tags.PersonTag.OpenLibrary("OL456A"),
        ),
    )
    assert list(lint.openlibrary_tags(p, _wiki_lint_config(autofix=True))) == []
    assert p.tags == (models.tags.PersonTag.OpenLibrary("OL456A"),)
    assert p.ol_id == "OL123A"


@pytest.mark.parametrize("primary", [None, "OL1872868A", "OL123A"])
def test_openlibrary_name_qualifier_allows_autofill(
    monkeypatch: pytest.MonkeyPatch, primary: str | None
) -> None:
    p = person(ol_id=primary, tags=(models.tags.PersonTag.Wikidata("Q42"),))
    data = item(
        claims={
            "P648": [
                claim(
                    "OL1872868A",
                    qualifiers={
                        "P1810": [{"datavalue": {"value": "Roy Chapman Andrews"}}]
                    },
                )
            ]
        }
    )
    before = copy.deepcopy(data.snapshot.response)
    monkeypatch.setattr(api, "get_item", Mock(return_value=data))
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    assert p.ol_id == (primary or "OL1872868A")
    assert p.get_ol_ids() == {primary or "OL1872868A", "OL1872868A"}
    assert data.snapshot.response == before
    assert list(lint.wikidata_profile(p, _wiki_lint_config())) == []


@pytest.mark.parametrize(
    "statement",
    [
        claim("OL123W"),
        claim(
            "OL123A",
            qualifiers={"P1480": [{"datavalue": {"value": {"id": "Q5727902"}}}]},
        ),
        claim(
            "OL123A",
            qualifiers={
                "P1810": [{"datavalue": {"value": "Roy Chapman Andrews"}}],
                "P1480": [{"datavalue": {"value": {"id": "Q5727902"}}}],
            },
        ),
    ],
)
def test_qualified_or_invalid_openlibrary_id_is_not_autofilled(
    monkeypatch: pytest.MonkeyPatch, statement: dict[str, Any]
) -> None:
    p = person(tags=(models.tags.PersonTag.Wikidata("Q42"),))
    monkeypatch.setattr(
        api, "get_item", Mock(return_value=item(claims={"P648": [statement]}))
    )
    issues = list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1 and issues[0].fix is None
    assert p.ol_id is None


def test_multiple_wikidata_identities_cannot_autofill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(
        tags=(
            models.tags.PersonTag.Wikidata("Q42"),
            models.tags.PersonTag.Wikidata("Q43"),
        )
    )
    fetch = Mock(return_value=item(claims={"P569": [date_claim()]}))
    monkeypatch.setattr(api, "get_item", fetch)
    assert list(lint.wikidata_profile(p, _wiki_lint_config(autofix=True))) == []
    fetch.assert_not_called()
    assert p.birth is None


def test_openlibrary_extra_is_an_identifier_tag() -> None:
    p = person(
        type=PersonType.unchecked, tags=(models.tags.PersonTag.OpenLibrary("OL123A"),)
    )
    issues = list(
        p.lint(
            LintConfig(
                autofix=False, interactive=False, available_resources=frozenset()
            )
        )
    )
    assert not any(
        "unchecked but tags" in str(issue) or "invalid Open Library" in str(issue)
        for issue in issues
    )


def test_openlibrary_lookup_uses_extra_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(ol_id="OL456A", tags=(models.tags.PersonTag.OpenLibrary("OL123A"),))
    monkeypatch.setattr(Person, "select_one", Mock(return_value=None))
    query = Mock()
    query.filter.return_value = [p]
    monkeypatch.setattr(Person, "select_valid", Mock(return_value=query))
    from taxonomy.db.models.person import person as person_module

    fetch = Mock()
    monkeypatch.setattr(person_module, "get_author", fetch)
    assert Person.get_or_create_from_ol_id("OL123A") is p
    fetch.assert_not_called()
    assert p.ol_id == "OL456A"


def test_organization_can_link_to_nonhuman_item(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(
        family_name="California State University, Fullerton",
        given_names=None,
        naming_convention=NamingConvention.organization,
        tags=(
            models.tags.PersonTag.Wiki(
                "https://en.wikipedia.org/wiki/California_State_University,_Fullerton"
            ),
        ),
    )
    data = item("Q1026925", claims={"P31": [claim({"id": "Q3918"})]})
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link("Q1026925")))
    monkeypatch.setattr(api, "get_item", Mock(return_value=data))
    assert list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True))) == []
    assert evidence.stored_qids(p) == {"Q1026925"}
    assert list(lint.wikidata_profile(p, _wiki_lint_config())) == []
    row = evidence.report_people([p])[0]
    assert row["status"] == "compared"
    assert row["candidate_qid"] == "Q1026925"
    assert row["items"][0]["human"] is False


def test_organization_still_rejects_ambiguous_wikipedia_links(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(
        naming_convention=NamingConvention.organization,
        given_names=None,
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),),
    )
    resolved = link()
    resolved.snapshot.response["query"]["pages"][0]["pageprops"]["disambiguation"] = ""
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=resolved))
    issues = list(lint.wikidata_from_wikipedia(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1 and issues[0].fix is None
    assert not evidence.stored_qids(p)


def test_mononym_convention_does_not_imply_nonhuman_identity() -> None:
    assert evidence.requires_human_item(
        person(naming_convention=NamingConvention.mononym)
    )


def _only_wikidata_lints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        lint.LINT,
        "linters",
        [
            lint.wikidata_entity_type,
            lint.wikidata_from_wikipedia,
            lint.wikidata_profile,
        ],
    )


def test_type_exception_allows_linking_and_keeps_profile_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ignore = models.tags.PersonTag.IgnoreLint(
        "wikidata_entity_type", comment="Hindu deity"
    )
    p = person(
        naming_convention=NamingConvention.mononym,
        given_names=None,
        birth="1901",
        tags=(
            models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),
            ignore,
        ),
    )
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link()))
    monkeypatch.setattr(
        api,
        "get_item",
        Mock(
            return_value=item(
                claims={
                    "P31": [claim({"id": "Q178885"})],
                    "P569": [date_claim()],
                    "P648": [claim("OL123A")],
                }
            )
        ),
    )
    _only_wikidata_lints(monkeypatch)
    for _ in range(2):
        issues = list(lint.LINT.run(p, _wiki_lint_config(autofix=True)))
        assert len(issues) == 1
        assert "birth: conflict" in str(issues[0])
        assert ignore in p.tags
        assert evidence.stored_qids(p) == {"Q42"}
        assert p.birth == "1901"
        assert p.ol_id == "OL123A"
    # Reports share the exception but retain the actual entity type as evidence.
    row = evidence.report_people([p])[0]
    assert row["items"][0]["human"] is False
    assert "comparisons" in row["items"][0]
    assert not any("individual human" in issue for issue in row["issues"])
    assert any("birth conflict" in issue for issue in row["issues"])


def test_nonhuman_is_reported_only_by_dedicated_lint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = person(
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"),)
    )
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=link()))
    monkeypatch.setattr(
        api,
        "get_item",
        Mock(return_value=item(claims={"P31": [claim({"id": "Q178885"})]})),
    )
    _only_wikidata_lints(monkeypatch)
    issues = list(lint.LINT.run(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].code == "wikidata_entity_type"
    assert not evidence.stored_qids(p)
    p.tags = (models.tags.PersonTag.Wikidata("Q42"),)  # type: ignore[assignment]
    issues = list(lint.LINT.run(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].code == "wikidata_entity_type"


@pytest.mark.parametrize(
    "failure", ["offline", "network_error", "missing_item", "unresolved_page"]
)
def test_type_exception_survives_unavailable_evidence(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    ignore = models.tags.PersonTag.IgnoreLint(
        "wikidata_entity_type", comment="Mythological figure"
    )
    p = person(
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"), ignore)
    )
    resolved = link()
    data = item()
    if failure == "unresolved_page":
        resolved.snapshot.response["query"]["pages"][0]["pageprops"] = {}
    elif failure == "missing_item":
        data.snapshot.response["entities"] = {"Q42": {"missing": True}}
    fetch = Mock(return_value=data)
    if failure == "network_error":
        fetch.side_effect = httpx.ConnectError("offline")
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=resolved))
    monkeypatch.setattr(api, "get_item", fetch)
    _only_wikidata_lints(monkeypatch)
    cfg = _wiki_lint_config(autofix=True)
    if failure == "offline":
        cfg = LintConfig(
            autofix=True, interactive=False, available_resources=frozenset()
        )
    issues = list(lint.LINT.run(p, cfg))
    assert ignore in p.tags
    assert not any("unused IgnoreLint" in str(issue) for issue in issues)
    assert not evidence.stored_qids(p)


def test_type_exception_does_not_bypass_ambiguous_links(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ignore = models.tags.PersonTag.IgnoreLint(
        "wikidata_entity_type", comment="Fictional character"
    )
    p = person(
        tags=(models.tags.PersonTag.Wiki("https://en.wikipedia.org/wiki/Smith"), ignore)
    )
    resolved = link()
    resolved.snapshot.response["query"]["pages"][0]["pageprops"]["disambiguation"] = ""
    monkeypatch.setattr(api, "resolve_wikipedia", Mock(return_value=resolved))
    _only_wikidata_lints(monkeypatch)
    issues = list(lint.LINT.run(p, _wiki_lint_config(autofix=True)))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].code == "wikidata_from_wikipedia"
    assert not evidence.stored_qids(p)
    assert ignore in p.tags


def test_other_ignores_do_not_waive_entity_type_check() -> None:
    p = person(tags=(models.tags.PersonTag.IgnoreLint("wikidata_profile"),))
    entity = item(claims={"P31": [claim({"id": "Q178885"})]}).entity
    assert entity is not None
    assert not evidence.can_use_item(p, entity)


@pytest.mark.parametrize("precision", [0, 6, 7, 8])
def test_coarse_dates_do_not_request_an_exact_year(precision: int) -> None:
    coarse = date_claim("+1900-00-00T00:00:00Z", precision)
    for local in (None, "1905"):
        comparison = evidence.compare_date(local, [coarse])
        assert comparison["status"] == "no_external_value"
        assert comparison["statements"] == [coarse]
        assert comparison["suggested"] is None
    assert (
        evidence.compare_date("1905", [coarse, date_claim("+1905-00-00T00:00:00Z")])[
            "status"
        ]
        == "match"
    )


@pytest.mark.parametrize("prop", ["P3629", "P421", "P4241", "P805", "P7452"])
def test_date_metadata_does_not_prevent_year_autofill(prop: str) -> None:
    statement = date_claim()
    statement["qualifiers"] = {prop: [{"datavalue": {"value": "metadata"}}]}
    assert evidence.compare_date(None, [statement])["suggested"] == "1900"


def test_openlibrary_work_count_is_descriptive_metadata() -> None:
    statement = claim(
        "OL123A", qualifiers={"P3740": [{"datavalue": {"value": {"amount": "+5"}}}]}
    )
    result = evidence.compare_openlibrary(person(), [statement])
    assert result["status"] == "missing_local"
    assert result["suggested"] == "OL123A"


def test_wikidata_review_is_scoped_to_value_item_field_and_evidence() -> None:
    claims = [date_claim(), date_claim("+1901-00-00T00:00:00Z")]
    entity = item(claims={"P569": claims, "P570": claims}).entity
    assert entity is not None
    review = models.tags.PersonTag.WikidataReview(
        "birth",
        "1900",
        "Q42",
        evidence.statement_hash(claims),
        "Institutional biography supports 1900.",
    )
    p = person(birth="1900", death="1900", tags=(review,))
    comparison = evidence.compare_person(p, entity)
    assert comparison["birth"]["status"] == "reviewed"
    assert comparison["death"]["status"] == "review"
    entity["claims"]["P569"] = list(reversed(claims))
    assert evidence.compare_person(p, entity)["birth"]["status"] == "reviewed"
    p.birth = "1901"
    assert evidence.compare_person(p, entity)["birth"]["status"] == "review"
    p.birth = "1900"
    entity["id"] = "Q43"
    assert evidence.compare_person(p, entity)["birth"]["status"] == "review"
    entity["id"] = "Q42"
    entity["claims"]["P569"][0]["references"] = []
    assert evidence.compare_person(p, entity)["birth"]["status"] == "review"


def test_reviewed_wrong_orcid_does_not_hide_other_profile_conflicts() -> None:
    claims = [claim("0000-0003-3215-9633")]
    review = models.tags.PersonTag.WikidataReview(
        "orcid",
        "0000-0003-3066-0870",
        "Q42",
        evidence.statement_hash(claims),
        "Publisher affiliations establish that the external ORCID belongs to a different person.",
    )
    p = person(
        birth="1901", tags=(models.tags.PersonTag.ORCID("0000-0003-3066-0870"), review)
    )
    entity = item(claims={"P496": claims, "P569": [date_claim()]}).entity
    assert entity is not None
    result = evidence.compare_person(p, entity)
    assert result["orcid"]["status"] == "reviewed"
    assert result["birth"]["status"] == "conflict"
    p.tags = (models.tags.PersonTag.ORCID("0000-0002-4797-609X"), review)  # type: ignore[assignment]
    assert evidence.compare_person(p, entity)["orcid"]["status"] == "conflict"
