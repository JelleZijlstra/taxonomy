"""Conservative matching between external author names and Persons."""

import re
from difflib import SequenceMatcher

from taxonomy.db import helpers, models
from taxonomy.db.constants import NamingConvention

from .person import Person, VirtualPerson, get_initials


def _normalized_name_part(text: str) -> str:
    return (
        helpers.simplify_string(text, clean_words=False)
        .replace("-", "")
        .replace("'", "")
        .replace(".", "")
        .replace(",", "")
    )


_GIVEN_NAME_PARTICLES = {
    "al",
    "da",
    "das",
    "de",
    "del",
    "della",
    "den",
    "der",
    "di",
    "do",
    "dos",
    "du",
    "e",
    "el",
    "la",
    "las",
    "le",
    "los",
    "van",
    "von",
    "y",
}

_EXTERNAL_NAME_TITLES = {"dr", "mr", "mrs", "ms", "prof", "professor"}


def _family_name_variants(person: Person) -> set[str]:
    names = {person.family_name}
    if (
        person.tussenvoegsel
        and person.naming_convention is not NamingConvention.vietnamese
    ):
        names.add(f"{person.tussenvoegsel} {person.family_name}")
    if person.suffix:
        names.add(f"{person.family_name} {person.suffix}")
        if (
            person.tussenvoegsel
            and person.naming_convention is not NamingConvention.vietnamese
        ):
            names.add(f"{person.tussenvoegsel} {person.family_name} {person.suffix}")
    names.update(
        tag.text
        for tag in person.get_tags(
            person.tags, models.tags.PersonTag.TransliteratedFamilyName
        )
    )
    if person.naming_convention in (
        NamingConvention.russian,
        NamingConvention.ukrainian,
    ):
        names.add(person.get_transliterated_family_name())
    variants = {_normalized_name_part(name) for name in names}
    if person.naming_convention is NamingConvention.pinyin and "lu" in variants:
        # External sources commonly retain input-method spellings Lyu or Lv for Lü.
        variants.update({"lyu", "lv"})
    return variants


def _initial_variants(person: Person | VirtualPerson) -> set[str]:
    variants: set[str] = set()
    if initials := get_initials(person):
        variants.add(_normalized_name_part(initials).replace(".", ""))
    if person.given_names:
        significant_names = " ".join(
            part
            for part in person.given_names.split()
            if part.strip(".,").casefold() not in _GIVEN_NAME_PARTICLES
        )
        if significant_names:
            without_particles = VirtualPerson(
                family_name=person.family_name,
                given_names=significant_names,
                naming_convention=person.naming_convention,
            )
            if initials := get_initials(without_particles):
                variants.add(_normalized_name_part(initials).replace(".", ""))
        split_initials = "".join(
            part[0]
            for part in re.split(r"[.\s-]+", person.given_names)
            if part and part.casefold() not in _GIVEN_NAME_PARTICLES
        )
        if split_initials:
            variants.add(_normalized_name_part(split_initials))
    return variants


def _full_name_tokens(person: Person | VirtualPerson) -> list[str]:
    text = " ".join(
        part
        for part in (
            person.given_names or person.initials,
            person.tussenvoegsel,
            person.family_name,
            person.suffix,
        )
        if part
    )
    return [_normalized_name_part(part) for part in re.split(r"[\s-]+", text) if part]


def _raw_full_name(person: Person | VirtualPerson) -> str:
    return " ".join(
        part
        for part in (
            person.given_names or person.initials,
            person.tussenvoegsel,
            person.family_name,
            person.suffix,
        )
        if part
    )


def _significant_full_name_tokens(person: Person | VirtualPerson) -> list[str]:
    return [
        token
        for token in _full_name_tokens(person)
        if token.casefold() not in _GIVEN_NAME_PARTICLES
    ]


def _full_name_initials(person: Person | VirtualPerson) -> list[str]:
    return sorted(
        _normalized_name_part(part)[0]
        for part in re.split(r"[.\s-]+", _raw_full_name(person))
        if part
        and part.strip(",").casefold() not in _GIVEN_NAME_PARTICLES
        and _normalized_name_part(part)
    )


def _has_abbreviated_name_part(person: Person | VirtualPerson) -> bool:
    return person.initials is not None or "." in _raw_full_name(person)


def _given_name_tokens(person: Person | VirtualPerson) -> list[str]:
    if not person.given_names:
        return []
    return [
        _normalized_name_part(part)
        for part in re.split(r"[\s-]+", person.given_names)
        if part and part.strip(".,").casefold() not in _GIVEN_NAME_PARTICLES
    ]


def _full_first_given_name(person: Person | VirtualPerson) -> str | None:
    return next((token for token in _given_name_tokens(person) if len(token) > 1), None)


def _full_first_given_names_conflict(external: VirtualPerson, person: Person) -> bool:
    external_first = _full_first_given_name(external)
    person_first = _full_first_given_name(person)
    if external_first is None or person_first is None:
        return False
    if external_first == person_first:
        return False
    shorter, longer = sorted((external_first, person_first), key=len)
    return len(shorter) < 3 or not longer.startswith(shorter)


def _given_names_match_as_subset(external: VirtualPerson, person: Person) -> bool:
    external_names = _given_name_tokens(external)
    person_names = _given_name_tokens(person)
    if (
        not external_names
        or not person_names
        or len(external_names) == len(person_names)
    ):
        return False
    shorter, longer = sorted((external_names, person_names), key=len)
    return shorter == longer[: len(shorter)] or shorter == longer[-len(shorter) :]


def _family_names_match(external: VirtualPerson, person: Person) -> bool:
    external_family = _normalized_name_part(external.family_name)
    variants = _family_name_variants(person)
    if external_family in variants:
        return True
    # External services inconsistently move later given names and second family
    # names across the given/family boundary. Four characters avoids treating short
    # East Asian family names as substrings of unrelated names.
    return any(
        len(shorter) >= 4 and (longer.startswith(shorter) or longer.endswith(shorter))
        for variant in variants
        for shorter, longer in ((external_family, variant), (variant, external_family))
    )


def author_name_matches_person(external: VirtualPerson, person: Person) -> bool:
    """Match a split external author name using reviewed Person conventions."""
    external_tokens = _full_name_tokens(external)
    person_tokens = _full_name_tokens(person)
    if person.naming_convention is not NamingConvention.unspecified and sorted(
        external_tokens
    ) == sorted(person_tokens):
        return True
    external_significant = _significant_full_name_tokens(external)
    person_significant = _significant_full_name_tokens(person)
    if sorted(external_significant) == sorted(person_significant) and sorted(
        external_tokens
    ) != sorted(person_tokens):
        return True
    if (
        _has_abbreviated_name_part(external) or _has_abbreviated_name_part(person)
    ) and _full_name_initials(external) == _full_name_initials(person):
        shared_full_tokens = {
            token for token in external_significant if len(token) > 1
        } & {token for token in person_significant if len(token) > 1}
        if shared_full_tokens:
            return True
    if person.suffix and sorted(_full_name_tokens(external)) == sorted(
        _full_name_tokens(person)
    ):
        return True
    if person.given_names is None and (
        _full_name_tokens(external) == _full_name_tokens(person)
        or "".join(_full_name_tokens(external)) == "".join(_full_name_tokens(person))
    ):
        return True
    if person.naming_convention is NamingConvention.pinyin:
        # Pinyin given names are stored with syllable-separating hyphens, while
        # ORCID and other bibliographic services commonly concatenate them. Do
        # this comparison on the complete fields so ``De-yan`` and ``Deyan``
        # match without making ordinary multi-part given names order-insensitive.
        if (
            _family_names_match(external, person)
            and external.given_names
            and person.given_names
            and _normalized_name_part(external.given_names)
            == _normalized_name_part(person.given_names)
        ):
            return True
        if sorted(_full_name_tokens(external)) == sorted(_full_name_tokens(person)):
            return True
        if (
            external.given_names
            and person.given_names
            and _normalized_name_part(external.family_name)
            == _normalized_name_part(person.given_names)
            and _normalized_name_part(external.given_names)
            == _normalized_name_part(person.family_name)
        ):
            return True
    elif person.naming_convention is NamingConvention.burmese:
        if _full_name_tokens(external) == _full_name_tokens(person):
            return True
        person_tokens = _full_name_tokens(person)
        if (
            person.given_names is None
            and person_tokens
            and _normalized_name_part(external.family_name) == person_tokens[-1]
            and external.initials
        ):
            external_initial_text = _normalized_name_part(external.initials).replace(
                ".", ""
            )
            person_initial_text = "".join(token[0] for token in person_tokens[:-1])
            if external_initial_text == person_initial_text:
                return True
    elif person.naming_convention is NamingConvention.vietnamese:
        if sorted(_full_name_tokens(external)) == sorted(_full_name_tokens(person)):
            return True
    if not _family_names_match(external, person):
        return False
    if _given_names_match_as_subset(external, person):
        return True
    external_initials = _initial_variants(external)
    person_initials = _initial_variants(person)
    if not external_initials or not person_initials:
        return True
    return any(
        normalized_external.startswith(normalized_person)
        or normalized_person.startswith(normalized_external)
        for normalized_external in external_initials
        for normalized_person in person_initials
    )


def _unsplit_name_matches_person(name: str, person: Person) -> bool:
    external_tokens = [
        _normalized_name_part(part)
        for part in re.split(r"[\s,.-]+", name)
        if _normalized_name_part(part)
        and part.casefold() not in _GIVEN_NAME_PARTICLES | _EXTERNAL_NAME_TITLES
    ]
    person_tokens = _significant_full_name_tokens(person)
    if not external_tokens or not person_tokens:
        return False
    if external_tokens == person_tokens or sorted(external_tokens) == sorted(
        person_tokens
    ):
        return True
    if "".join(external_tokens) == "".join(person_tokens):
        return True
    shorter, longer = sorted((external_tokens, person_tokens), key=len)
    if len(shorter) >= 2:
        if person.naming_convention in {
            NamingConvention.pinyin,
            NamingConvention.vietnamese,
        } and set(shorter) <= set(longer):
            return True
        remaining = iter(longer)
        if all(any(token == candidate for candidate in remaining) for token in shorter):
            return True
    external_initials = sorted(token[0] for token in external_tokens)
    person_initials = sorted(token[0] for token in person_tokens)
    shared_full_tokens = {token for token in external_tokens if len(token) > 1} & {
        token for token in person_tokens if len(token) > 1
    }
    return external_initials == person_initials and bool(shared_full_tokens)


def external_identity_matches_person(
    *,
    given_names: str | None,
    family_names: str | None,
    credit_name: str | None,
    other_names: tuple[str, ...],
    person: Person,
) -> bool:
    """Match the public names returned for one external identity."""
    if given_names and family_names:
        external = VirtualPerson(family_name=family_names, given_names=given_names)
        if (
            person.naming_convention is NamingConvention.pinyin
            and author_name_matches_person(external, person)
        ):
            return True
        if not _full_first_given_names_conflict(
            external, person
        ) and author_name_matches_person(external, person):
            return True
        # ORCID profiles sometimes reverse the given- and family-name fields. Treat
        # an exact match of the complete token set as equivalent without making the
        # ordinary split-name matcher generally order-insensitive.
        if _unsplit_name_matches_person(f"{given_names} {family_names}", person):
            return True
    unsplit_names = [credit_name, *other_names]
    if bool(given_names) != bool(family_names):
        # Some ORCID records put the record holder's complete name into just one
        # of the two structured fields. In that case it is still an unsplit public
        # name, not evidence that the Person has no family or given name.
        unsplit_names.append(given_names or family_names)
    return any(
        _unsplit_name_matches_person(name, person) for name in unsplit_names if name
    )


def _fuzzy_public_tokens_match(name: str, person: Person) -> bool:
    external_tokens = [
        _normalized_name_part(part)
        for part in re.split(r"[\s,.-]+", name)
        if _normalized_name_part(part)
        and part.casefold() not in _GIVEN_NAME_PARTICLES | _EXTERNAL_NAME_TITLES
    ]
    person_tokens = _significant_full_name_tokens(person)
    if len(external_tokens) < 2 or len(person_tokens) < 2:
        if len(external_tokens) != 1:
            return False
        external = external_tokens[0]
        if person.naming_convention is NamingConvention.pinyin:
            if external in person_tokens:
                return True
            external = external_tokens[0]
            if any(
                external == "".join((*person_tokens[index:], *person_tokens[:index]))
                for index in range(len(person_tokens))
            ):
                return True
            return any(
                external == "".join(person_tokens[start:end])
                for start in range(len(person_tokens))
                for end in range(start + 1, len(person_tokens) + 1)
            )
        return any(
            (external == token and len(external) >= 3)
            or (
                min(len(external), len(token)) >= 4
                and SequenceMatcher(a=external, b=token).ratio() >= 0.82
            )
            for token in person_tokens
        )
    shorter, longer = sorted((external_tokens, person_tokens), key=len)

    def compatible(left: str, right: str) -> bool:
        if left == right:
            return True
        first, second = sorted((left, right), key=len)
        return len(first) >= 4 and (
            second.startswith(first)
            or second.endswith(first)
            or SequenceMatcher(a=first, b=second).ratio() >= 0.82
        )

    def match_remaining(index: int, available: tuple[str, ...]) -> bool:
        if index == len(shorter):
            return True
        return any(
            compatible(shorter[index], candidate)
            and match_remaining(
                index + 1,
                (*available[:candidate_index], *available[candidate_index + 1 :]),
            )
            for candidate_index, candidate in enumerate(available)
        )

    return match_remaining(0, tuple(longer))


def public_identity_matches_person(
    *,
    given_names: str | None,
    family_names: str | None,
    credit_name: str | None,
    other_names: tuple[str, ...],
    person: Person,
) -> bool:
    """Match a public identity profile, including reviewed formatting defects.

    Unlike article-author inference, this may accept reordered complete names and
    minor spelling variants. It still requires at least two compatible name tokens,
    except for a concatenated pinyin name whose complete token sequence matches.
    """
    if external_identity_matches_person(
        given_names=given_names,
        family_names=family_names,
        credit_name=credit_name,
        other_names=other_names,
        person=person,
    ):
        return True
    names = [credit_name, *other_names]
    if given_names or family_names:
        names.append(" ".join(part for part in (given_names, family_names) if part))
    return any(_fuzzy_public_tokens_match(name, person) for name in names if name)


def format_external_identity(
    *, given_names: str | None, family_names: str | None, credit_name: str | None
) -> str:
    if given_names and family_names:
        return f"{given_names} {family_names}"
    return credit_name or family_names or given_names or "<no public name>"
