"""Named lint steps for Persons."""

import functools
import re
from collections.abc import Iterable
from typing import Any

from taxonomy.apis import orcid
from taxonomy.db import helpers, models
from taxonomy.db.constants import PersonType
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint import IgnoreLint, Lint, field_issue
from taxonomy.db.models.lint_types import LintResult

from .name_matching import (
    external_identity_matches_person,
    format_external_identity,
    public_identity_matches_person,
)
from .person import Person, VirtualPerson, is_more_specific_than, normalize_orcid


def get_ignores(person: Person) -> Iterable[IgnoreLint]:
    return person.get_tags(person.tags, models.tags.PersonTag.IgnoreLint)


def add_ignore(person: Person, label: str, comment: str) -> None:
    person.add_tag(models.tags.PersonTag.IgnoreLint(label, comment=comment))


LINT = Lint(Person, get_ignores, add_ignore)


def get_orcid_tags(person: Person) -> list[Any]:
    return list(person.get_tags(person.tags, models.tags.PersonTag.ORCID))


@LINT.add("multiple_orcids")
def multiple_orcids(person: Person, cfg: LintConfig) -> Iterable[str]:
    tags = get_orcid_tags(person)
    if len(tags) > 1:
        yield f"has multiple ORCID tags: {tags}"


@LINT.add_multi_duplicate_finder("duplicate_orcid")
def duplicate_orcid(person: Person) -> Iterable[str]:
    # Use every ORCID so a reviewed, legitimate multi-profile Person still
    # participates in duplicate detection for each identifier.
    return {normalize_orcid(tag.text) for tag in get_orcid_tags(person)}


@LINT.add("soft_redirect")
def soft_redirect(person: Person, cfg: LintConfig) -> Iterable[LintResult]:
    """Replace the deprecated soft-redirect state with a hard redirect."""
    if person.type is PersonType.soft_redirect:
        yield field_issue(
            "converting deprecated soft redirect to hard redirect",
            person,
            "type",
            PersonType.hard_redirect,
        )


def _virtual_name(person: Person) -> VirtualPerson:
    return VirtualPerson(
        family_name=person.family_name,
        given_names=person.given_names,
        initials=person.initials,
        tussenvoegsel=person.tussenvoegsel,
        suffix=person.suffix,
        naming_convention=person.naming_convention,
    )


def _normalized_display_name(person: Person) -> str:
    simplified = helpers.simplify_string(person.get_full_name(), clean_words=False)
    return re.sub(r"\W+", "", simplified)


@LINT.add("redirect_to_less_specific")
def redirect_to_less_specific(person: Person, cfg: LintConfig) -> Iterable[str]:
    """Warn when a redirect discards compatible name detail.

    This is deliberately diagnostic rather than an autofix: the canonical row may
    need a fuller name, but a title, nickname, duplicated mononym, or malformed
    source spelling may instead be a legitimate less-canonical redirect.
    """
    if person.type not in (PersonType.hard_redirect, PersonType.soft_redirect):
        return
    if person.target is None:
        return
    if _normalized_display_name(person) == _normalized_display_name(person.target):
        return
    if is_more_specific_than(_virtual_name(person), _virtual_name(person.target)):
        yield (
            f"redirect name {person.get_full_name()!r} is more specific than "
            f"target {person.target.get_full_name()!r}"
        )


def _identity_person(person: Person) -> Person:
    seen: set[int] = set()
    while person.target is not None and person.type in (
        PersonType.alias,
        PersonType.hard_redirect,
        PersonType.soft_redirect,
    ):
        if person.id in seen:
            break
        seen.add(person.id)
        person = person.target
    return person


def _person_orcids(person: Person) -> set[str]:
    return {normalize_orcid(tag.text) for tag in get_orcid_tags(person)}


def _format_author_suggestion(
    profile: orcid.OrcidProfile, stored_orcid: str, authors: list[Person]
) -> str:
    same_orcid = [
        author for author in authors if stored_orcid in _person_orcids(author)
    ]
    if same_orcid:
        names = sorted({author.get_full_name() for author in same_orcid})
        return f"; article author with the same ORCID: {', '.join(map(repr, names))}"

    public_name_matches = [
        author
        for author in authors
        if external_identity_matches_person(
            given_names=profile.given_names,
            family_names=profile.family_names,
            credit_name=profile.credit_name,
            other_names=profile.other_names,
            person=author,
        )
    ]
    if public_name_matches:
        names = sorted({author.get_full_name() for author in public_name_matches})
        return (
            f"; ORCID public name matches article author: {', '.join(map(repr, names))}"
        )
    return ""


@LINT.add("orcid_profile", required_resources={LintResource.NETWORK, LintResource.SLOW})
def orcid_profile(person: Person, cfg: LintConfig) -> Iterable[str]:
    """Check stored identifiers against their public ORCID identity records."""
    identity = _identity_person(person)
    for tag in get_orcid_tags(person):
        stored_orcid = normalize_orcid(tag.text)
        profile = orcid.get_orcid_profile(stored_orcid)
        if profile is None:
            yield f"ORCID {stored_orcid} has no public record"
            continue
        if profile.orcid != stored_orcid:
            yield (
                f"ORCID {stored_orcid} redirects to public record {profile.orcid}; "
                "review the stored identifier"
            )
        has_public_name = any(
            (
                profile.given_names,
                profile.family_names,
                profile.credit_name,
                profile.other_names,
            )
        )
        if has_public_name and not public_identity_matches_person(
            given_names=profile.given_names,
            family_names=profile.family_names,
            credit_name=profile.credit_name,
            other_names=profile.other_names,
            person=identity,
        ):
            public_name = format_external_identity(
                given_names=profile.given_names,
                family_names=profile.family_names,
                credit_name=profile.credit_name,
            )
            yield (
                f"ORCID {stored_orcid} public name {public_name!r} does not match "
                f"Person {identity.get_full_name()!r}"
            )


@functools.cache
def _articles_by_doi() -> dict[str, tuple[models.Article, ...]]:
    articles: dict[str, list[models.Article]] = {}
    for article in models.Article.select_valid():
        if article.doi is not None and not article.has_tag(
            models.article.ArticleTag.GeneralDOI
        ):
            articles.setdefault(orcid.normalize_doi(article.doi), []).append(article)
    return {doi: tuple(matches) for doi, matches in articles.items()}


@LINT.add(
    "orcid_works",
    required_resources={LintResource.NETWORK, LintResource.SLOW},
    clear_caches=_articles_by_doi.cache_clear,
)
def orcid_works(person: Person, cfg: LintConfig) -> Iterable[str]:
    """Check known DOI works against the Article author list."""
    identity = _identity_person(person)
    ignored_works = {
        (normalize_orcid(tag.orcid), orcid.normalize_doi(tag.doi))
        for tag in person.get_tags(person.tags, models.tags.PersonTag.IgnoreORCIDWork)
    }
    checked_articles: set[int] = set()
    for tag in get_orcid_tags(person):
        stored_orcid = normalize_orcid(tag.text)
        profile = orcid.get_orcid_profile(stored_orcid)
        if profile is None:
            continue
        for work in profile.works:
            if (stored_orcid, work.doi) in ignored_works:
                continue
            for article in _articles_by_doi().get(work.doi, ()):
                if article.id in checked_articles:
                    continue
                checked_articles.add(article.id)
                author_identities = list(
                    dict.fromkeys(
                        _identity_person(author) for author in article.get_authors()
                    )
                )
                if identity not in author_identities:
                    suggestion = _format_author_suggestion(
                        profile, stored_orcid, author_identities
                    )
                    yield (
                        f"ORCID {stored_orcid} lists DOI {work.doi}, but "
                        f"{identity.get_full_name()!r} is not an author of "
                        f"Article {article.name!r}{suggestion}"
                    )
