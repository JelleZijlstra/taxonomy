"""Named lint steps for Persons."""

import functools
from collections.abc import Iterable
from typing import Any

from taxonomy.apis import orcid
from taxonomy.db import models
from taxonomy.db.constants import PersonType
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint import IgnoreLint, Lint

from .name_matching import external_identity_matches_person, format_external_identity
from .person import Person, normalize_orcid


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
        if has_public_name and not external_identity_matches_person(
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
    checked_articles: set[int] = set()
    for tag in get_orcid_tags(person):
        stored_orcid = normalize_orcid(tag.text)
        profile = orcid.get_orcid_profile(stored_orcid)
        if profile is None:
            continue
        for work in profile.works:
            for article in _articles_by_doi().get(work.doi, ()):
                if article.id in checked_articles:
                    continue
                checked_articles.add(article.id)
                author_identities = {
                    _identity_person(author) for author in article.get_authors()
                }
                if identity not in author_identities:
                    yield (
                        f"ORCID {stored_orcid} lists DOI {work.doi}, but "
                        f"{identity.get_full_name()!r} is not an author of "
                        f"Article {article.name!r}"
                    )
