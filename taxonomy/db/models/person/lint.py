"""Named lint steps for Persons."""

import functools
import re
from collections.abc import Iterable
from typing import Any

from taxonomy.apis import orcid, wikidata
from taxonomy.db import helpers, models
from taxonomy.db.constants import NamingConvention, PersonType
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint import (
    IgnoreLint,
    Lint,
    add_tag_issue,
    field_issue,
    remove_tag_issue,
)
from taxonomy.db.models.lint_types import LintResult

from . import wikidata as wikidata_evidence
from .name_matching import (
    external_identity_matches_person,
    format_external_identity,
    public_identity_matches_person,
)
from .person import (
    Person,
    PersonLevel,
    VirtualPerson,
    is_more_specific_than,
    normalize_orcid,
)


def get_ignores(person: Person) -> Iterable[IgnoreLint]:
    return person.get_tags(person.tags, models.tags.PersonTag.IgnoreLint)


def add_ignore(person: Person, label: str, comment: str) -> None:
    person.add_tag(models.tags.PersonTag.IgnoreLint(label, comment=comment))


LINT = Lint(Person, get_ignores, add_ignore)

ENFORCED_NAMING_CONVENTIONS = (
    NamingConvention.vietnamese,
    NamingConvention.pinyin,
    NamingConvention.chinese,
    NamingConvention.korean,
    NamingConvention.japanese,
    NamingConvention.german,
    NamingConvention.french,
    NamingConvention.dutch,
    NamingConvention.italian,
    NamingConvention.hungarian,
    NamingConvention.russian,
    NamingConvention.ukrainian,
    NamingConvention.turkish,
    # TODO: Maybe add Spanish and Portuguese, but they add hundreds more names to clean up
)


@functools.cache
def _family_name_to_naming_conventions() -> dict[str, set[NamingConvention]]:
    conventions: dict[str, set[NamingConvention]] = {}
    for person in Person.select_valid().filter(
        Person.naming_convention.is_in(ENFORCED_NAMING_CONVENTIONS)
    ):
        if not person.is_invalid():
            conventions.setdefault(person.family_name, set()).add(
                person.naming_convention
            )
    return conventions


@LINT.add(
    "naming_convention", clear_caches=_family_name_to_naming_conventions.cache_clear
)
def naming_convention(person: Person, cfg: LintConfig) -> Iterable[str]:
    """Suggest conventions for review based on exact family-name matches."""
    if (
        person.naming_convention is not NamingConvention.unspecified
        or person.is_invalid()
        or person.get_level() < PersonLevel.has_given_name
    ):
        return
    conventions = _family_name_to_naming_conventions().get(person.family_name)
    if conventions:
        names = ", ".join(sorted(convention.name for convention in conventions))
        yield (
            f"naming convention is unspecified, but family name {person.family_name!r} "
            f"is also used with naming conventions: {names}"
        )


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


@LINT.add_multi_duplicate_finder("duplicate_openlibrary")
def duplicate_openlibrary(person: Person) -> Iterable[str]:
    if person.is_invalid() or person.type is PersonType.alias:
        return ()
    return {
        f"https://openlibrary.org/authors/{ol_id}"
        for ol_id in person.get_ol_ids()
        if re.fullmatch(r"OL[1-9][0-9]*A", ol_id)
    }


@LINT.add("wikidata_tags")
def wikidata_tags(person: Person, cfg: LintConfig) -> Iterable[str]:
    qids = wikidata_evidence.stored_qids(person)
    if len(qids) > 1:
        yield f"has multiple Wikidata IDs: {sorted(qids)}"
    if qids and person.type in wikidata_evidence.IDENTITY_TYPES:
        yield "Wikidata identifiers belong on the canonical Person; review target and tags"


@LINT.add_multi_duplicate_finder("duplicate_wikidata")
def duplicate_wikidata(person: Person) -> Iterable[str]:
    if person.is_invalid() or person.type is PersonType.alias:
        return ()
    return {
        qid
        for qid in wikidata_evidence.stored_qids(person)
        if wikidata.is_valid_qid(qid)
    }


@LINT.add(
    "wikidata_entity_type", required_resources={LintResource.NETWORK, LintResource.SLOW}
)
def wikidata_entity_type(person: Person, cfg: LintConfig) -> Iterable[str]:
    """Check the entity type independently of linking and profile comparisons."""
    if (
        person.is_invalid()
        or person.type is PersonType.alias
        or not wikidata_evidence.requires_human_item(person)
    ):
        return
    qids = wikidata_evidence.stored_qids(person)
    if not qids:
        for url in sorted(
            {
                tag.text
                for tag in person.get_tags(person.tags, models.tags.PersonTag.Wiki)
            }
        ):
            link = wikidata.resolve_wikipedia(url)
            if not link.needs_review and link.qid is not None:
                qids.add(link.qid)
            else:
                yield f"cannot check Wikidata entity type for unresolved Wikipedia link: {url}"
    for qid in sorted(qids):
        if not wikidata.is_valid_qid(qid):
            continue
        entity = wikidata.get_item(qid).entity
        if entity is None:
            # Type evaluation is indeterminate; preserve an existing IgnoreLint.
            yield f"cannot check Wikidata entity type: {qid} has no public item"
        elif not wikidata_evidence.is_human(entity):
            # Do not call can_use_item here: the wrapper must see the issue even
            # when ignored, so it does not remove the exception as unused.
            yield (
                f"Wikidata {qid} is not unambiguously an individual human; "
                f"review https://www.wikidata.org/wiki/{entity['id']}"
            )


@LINT.add(
    "wikidata_from_wikipedia",
    required_resources={LintResource.NETWORK, LintResource.SLOW},
)
def wikidata_from_wikipedia(person: Person, cfg: LintConfig) -> Iterable[LintResult]:
    """Add a missing identity link from the Person's existing Wikipedia links."""
    if (
        person.is_invalid()
        or person.type is PersonType.alias
        or wikidata_evidence.stored_qids(person)
    ):
        return
    urls = sorted(
        {tag.text for tag in person.get_tags(person.tags, models.tags.PersonTag.Wiki)}
    )
    if not urls:
        return
    qids: set[str] = set()
    for url in urls:
        link = wikidata.resolve_wikipedia(url)
        if link.needs_review:
            yield f"Wikipedia link needs identity review before adding Wikidata: {url}"
            return
        if link.qid is None:
            yield f"No Wikidata item for Wikipedia link: {url}"
            return
        qids.add(link.qid)
    resolved_qids: set[str] = set()
    for qid in sorted(qids):
        entity = wikidata.get_item(qid).entity
        if entity is None:
            yield f"Wikidata {qid} has no public item"
            return
        if not wikidata_evidence.can_use_item(person, entity):
            # Reported separately by wikidata_entity_type, which can be ignored
            # without disabling link inference.
            return
        resolved_qids.add(entity["id"])
    if len(resolved_qids) != 1:
        yield f"Wikipedia links resolve to different Wikidata identities: {sorted(resolved_qids)}"
        return
    qid = next(iter(resolved_qids))
    yield add_tag_issue(
        f"add Wikidata {qid} from Wikipedia: {', '.join(urls)}",
        person,
        models.tags.PersonTag.Wikidata(qid),
    )


@LINT.add(
    "wikidata_profile", required_resources={LintResource.NETWORK, LintResource.SLOW}
)
def wikidata_profile(person: Person, cfg: LintConfig) -> Iterable[LintResult]:
    """Fill unambiguous missing years and identifiers; report disagreements."""
    if person.is_invalid() or person.type is PersonType.alias:
        return
    qids = wikidata_evidence.stored_qids(person)
    if len(qids) > 1:
        return  # Resolve the wikidata_tags identity conflict before importing facts.
    for qid in sorted(qids):
        if not wikidata.is_valid_qid(qid):
            continue  # Reported by the model's offline lint.
        item = wikidata.get_item(qid)
        entity = item.entity
        if entity is None:
            yield f"Wikidata {qid} has no public item"
            continue
        if entity["id"] != qid:
            yield f"Wikidata {qid} redirects to {entity['id']}; review the identifier"
            continue
        if not wikidata_evidence.can_use_item(person, entity):
            continue
        for field, comparison in wikidata_evidence.compare_person(
            person, entity
        ).items():
            source = f"https://www.wikidata.org/w/index.php?oldid={entity['lastrevid']}"
            if field == "ol_id":
                yield from _wikidata_openlibrary(person, qid, comparison, source)
                continue
            if field == "orcid" and comparison["status"] == "missing_local":
                orcid_id = comparison["external"][0]
                yield add_tag_issue(
                    f"add ORCID from Wikidata {qid}: https://orcid.org/{orcid_id}; "
                    f"{source}",
                    person,
                    models.tags.PersonTag.ORCID(orcid_id),
                )
                continue
            if field in ("birth", "death") and comparison["status"] == "missing_local":
                year = comparison["suggested"]
                yield field_issue(
                    f"fill {field} year {year} from Wikidata {qid}: {source}",
                    person,
                    field,
                    year,
                )
                continue
            if comparison["status"] in {"conflict", "review", "missing_local"}:
                if field in ("birth", "death"):
                    external = " | ".join(
                        wikidata_evidence.describe_date_statement(statement)
                        for statement in comparison["statements"]
                    )
                else:
                    external = repr(comparison["external"])
                yield (
                    f"Wikidata {qid} {field}: {comparison['status']}; "
                    f"local {comparison['local']!r}, external {external}; "
                    f"review statements at {source}"
                )


def _wikidata_openlibrary(
    person: Person, qid: str, comparison: dict[str, Any], source: str
) -> Iterable[LintResult]:
    status = comparison["status"]
    links = ", ".join(comparison["links"])
    if status == "missing_local":
        yield field_issue(
            f"fill primary Open Library ID from Wikidata {qid}: {links}; {source}",
            person,
            "ol_id",
            comparison["suggested"],
        )
    elif status == "missing_additional":
        for oid in comparison["missing"]:
            yield add_tag_issue(
                f"add extra Open Library ID from Wikidata {qid}: "
                f"https://openlibrary.org/authors/{oid}; "
                f"primary https://openlibrary.org/authors/{person.ol_id}; {source}",
                person,
                models.tags.PersonTag.OpenLibrary(oid),
            )
    elif status == "choose_primary":
        yield (
            f"Wikidata {qid}: choose the Open Library author record with the most data "
            f"as ol_id; retain the others as OpenLibrary tags: {links}; {source}"
        )
    elif status == "review":
        yield (
            f"Wikidata {qid} ol_id: review; local {comparison['local']!r}, "
            f"external {comparison['external']!r}; {links}; {source}"
        )


@LINT.add("openlibrary_tags")
def openlibrary_tags(person: Person, cfg: LintConfig) -> Iterable[LintResult]:
    for tag in person.get_tags(person.tags, models.tags.PersonTag.OpenLibrary):
        if tag.text == person.ol_id:
            yield remove_tag_issue(
                f"remove extra OpenLibrary tag duplicating primary: "
                f"https://openlibrary.org/authors/{tag.text}",
                person,
                tag,
            )


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
        if profile.orcid != stored_orcid and not public_identity_matches_person(
            given_names=profile.given_names,
            family_names=profile.family_names,
            credit_name=profile.credit_name,
            other_names=profile.other_names,
            person=identity,
        ):
            # orcid_profile reports this unresolved identity conflict. Works on
            # the redirect destination cannot safely be attributed to this Person.
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
