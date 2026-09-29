import enum
import re
from collections.abc import Iterable

from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.collection import (
    BMNH_COLLECTION,
    MULTIPLE_COLLECTION,
    Collection,
)
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import Name, TypeTag
from taxonomy.db.models.name.type_specimen import (
    BaseSpecimen,
    InformalSpecimen,
    InformalWithoutInstitution,
    SimpleSpecimen,
    SpecialSpecimen,
    Specimen,
    SpecimenRange,
    TripletSpecimen,
    parse_type_specimen,
    stringify_specimen_list,
)

from .base import LINT
from .helpers import fix_type_specimen_link


class RepositoryKind(enum.Enum):
    repository = 1
    former = 2
    extra = 3
    future = 4


def _get_repositories(nam: Name) -> set[tuple[RepositoryKind, Collection]]:
    repos = set()
    if nam.collection is not None and nam.collection.id != MULTIPLE_COLLECTION:
        repos.add((RepositoryKind.repository, nam.collection))
    repo: Collection
    for tag in nam.type_tags:
        match tag:
            case TypeTag.Repository(repo):
                repos.add((RepositoryKind.repository, repo))
            case TypeTag.FormerRepository(repo):
                repos.add((RepositoryKind.former, repo))
            case TypeTag.ExtraRepository(repo):
                repos.add((RepositoryKind.extra, repo))
            case TypeTag.FutureRepository(repo):
                repos.add((RepositoryKind.future, repo))
    return repos


@LINT.add("type_specimen_order")
def check_type_specimen_order(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # reported elsewhere
    expected_text = stringify_specimen_list(specs)
    if nam.type_specimen == expected_text:
        return
    message = (
        f"Incorrectly formatted type specimen: got {nam.type_specimen!r}, expected"
        f" {expected_text!r}"
    )
    yield field_issue(message, nam, "type_specimen", expected_text)


@LINT.add("type_specimen")
def check_type_specimen(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError as e:
        yield f"cannot parse type specimen string {nam.type_specimen}: {e}"
        return
    for spec in specs:
        if isinstance(spec, SpecimenRange):
            yield from _check_specimen(spec.start, nam)
            yield from _check_specimen(spec.end, nam)
        else:
            yield from _check_specimen(spec, nam)


def _validate_specimen(
    spec: BaseSpecimen,
    possible_repos: set[tuple[RepositoryKind, Collection]],
    allowed_kinds: set[RepositoryKind],
) -> str | None:
    messages = []
    for kind, repo in possible_repos:
        if kind not in allowed_kinds:
            continue
        message = repo.validate_specimen(spec)
        if message is None:
            return None
        messages.append(message)
    return "; ".join(messages)


def _check_specimen(spec: Specimen, nam: Name) -> Iterable[str]:
    repos = _get_repositories(nam)
    if message := _validate_specimen(spec.base, repos, {RepositoryKind.repository}):
        yield f"{spec.base.stringify()!r} does not match: {message}"
    for former in spec.former_texts:
        if isinstance(former, InformalWithoutInstitution):
            continue
        if message := _validate_specimen(
            former, repos, {RepositoryKind.repository, RepositoryKind.former}
        ):
            yield (
                f"former specimen reference {former!r} does not match any collection"
                f" ({message})"
            )
    for future in spec.future_texts:
        if message := _validate_specimen(
            future, repos, {RepositoryKind.repository, RepositoryKind.future}
        ):
            yield (
                f"future specimen reference {future!r} does not match any collection"
                f" ({message})"
            )
    for extra in spec.extra_texts:
        if message := _validate_specimen(
            extra, repos, {RepositoryKind.repository, RepositoryKind.extra}
        ):
            yield (
                f"extra specimen reference {extra!r} does not match any collection"
                f" ({message})"
            )


_BMNH_REGEXES = [
    (r"^BMNH ([MR])[\- \.]*(\d+[a-z]?)$", r"BMNH \1 \2"),  # M1234 -> M 1234
    (r"^BMNH (\d)(\.\d+\.\d+\.\d+)$", r"BMNH 190\1\2"),  # 2.1.1.1 -> 1902.1.1.1
    (r"^BMNH ([4-9]\d)(\.\d+\.\d+\.\d+)$", r"BMNH 18\1\2"),  # 55.1.1.1 -> 1855.1.1.1
    (r"^BMNH ([1-2]\d)(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 11.1.1.1 -> 1911.1.1.1
    (r"^BMNH (\d{4,5})$", r"BMNH OR \1"),  # BMNH 12345 -> BMNH OR 12345
    (r"^BMNH (3[0-5])(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 33.1.1.1 -> 1933.1.1.1
    (r"^BMNH (3[7-9])(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 38.1.1.1 -> 1838.1.1.1
]


@LINT.add("bmnh_types")
def check_bmnh_type_specimens(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    """Fix some simple issues with BMNH type specimens."""
    if nam.type_specimen is None:
        return
    if nam.collection is None or nam.collection.id != BMNH_COLLECTION:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain
    for spec in specs:
        if not isinstance(spec, Specimen) or not isinstance(spec.base, SimpleSpecimen):
            continue
        text = spec.base.text
        if not text.startswith("BMNH"):
            continue
        new_spec = clean_up_bmnh_type(text)
        if new_spec != text:
            message = f"replace {text!r} with {new_spec!r}"
            yield field_issue(
                message, nam, "type_specimen", nam.type_specimen.replace(text, new_spec)
            )


def get_all_type_specimen_texts(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        if isinstance(spec, Specimen):
            yield from _get_all_type_specimen_texts_from_specimen(spec)
        elif isinstance(spec, SpecimenRange):
            yield from _get_all_type_specimen_texts_from_specimen(spec.start)
            yield from _get_all_type_specimen_texts_from_specimen(spec.end)


def _get_all_type_specimen_texts_from_specimen(spec: Specimen) -> Iterable[str]:
    if isinstance(spec.base, SimpleSpecimen):
        yield spec.base.text
    elif isinstance(spec.base, TripletSpecimen):
        yield spec.base.stringify()


@LINT.add("type_specimen_link")
def check_must_have_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # TODO: cover ExtraRepository, FormerRepository here
    # After replacing all TypeSpecimenLink tags. Then we should be able to associate every TypeSpecimenLinkFor tag with some part of the type_specimen text.
    if nam.collection is None or not nam.collection.must_have_specimen_links(nam):
        return
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain

    if nam.collection.id == MULTIPLE_COLLECTION:
        collections = [
            tag.repository for tag in nam.get_tags(nam.type_tags, TypeTag.Repository)
        ]
    else:
        collections = [nam.collection]

    actual_specimens = {
        tag.specimen
        for tag in nam.get_tags(nam.type_tags, TypeTag.TypeSpecimenLinkFor)
        if any(coll.is_valid_specimen_link(tag.url) for coll in collections)
    }

    for spec in specs:
        if isinstance(spec, SpecimenRange):
            yield f"type specimen range is not supported for links: {spec}"
            continue
        if isinstance(spec.base, (SpecialSpecimen, InformalSpecimen)):
            continue
        text = spec.base.stringify()
        if text not in actual_specimens:
            yield (
                f"missing type specimen link for {text!r} in {nam.type_specimen!r}. "
                "Add a TypeSpecimenLinkFor tag with the correct URL."
            )


@LINT.add("duplicate_type_specimen_links")
def check_duplicate_type_specimen_links(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    tags_with_specimens = {
        tag.url for tag in nam.type_tags if isinstance(tag, TypeTag.TypeSpecimenLinkFor)
    }
    if not tags_with_specimens:
        return
    new_tags = tuple(
        tag
        for tag in nam.type_tags
        if not isinstance(tag, TypeTag.TypeSpecimenLink)
        or tag.url not in tags_with_specimens
    )
    if nam.type_tags != new_tags:
        removed = set(nam.type_tags) - set(new_tags)
        message = f"remove redundant tags: {removed}"
        yield field_issue(message, nam, "type_tags", new_tags)


@LINT.add("replace_simple_type_specimen_link")
def replace_simple_type_specimen_link(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if nam.type_specimen is None:
        return
    if "," in nam.type_specimen or "(" in nam.type_specimen:
        return
    num_ts_links = sum(
        isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags
    )
    if num_ts_links != 1:
        return
    new_tags = tuple(
        (
            TypeTag.TypeSpecimenLinkFor(tag.url, nam.type_specimen)
            if isinstance(tag, TypeTag.TypeSpecimenLink)
            and nam.type_specimen is not None
            else tag
        )
        for tag in nam.type_tags
    )
    message = "replace TypeSpecimenLink tag with TypeSpecimenLinkFor"
    yield field_issue(message, nam, "type_tags", new_tags)


def _parse_specimen_detail(text: str) -> dict[str, str] | None:
    match = re.fullmatch(r"(.*) \[at (https?://.*)\]", text)
    if match is None:
        return None
    rest = match.group(1)
    url = match.group(2)
    out = {"at": url}
    for piece in rest.split(" ... "):
        match = re.fullmatch(r"\[([^\]]+)\] (.*)", piece)
        if match:
            out[match.group(1)] = match.group(2)
    return out


@LINT.add("replace_type_specimen_link")
def replace_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.type_specimen is None:
        return
    if not any(isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags):
        return
    from_spec_details: dict[str, str] = {}
    for tag in nam.type_tags:
        if not isinstance(tag, TypeTag.SpecimenDetail):
            continue
        parsed = _parse_specimen_detail(tag.text)
        if not parsed:
            continue
        url = fix_type_specimen_link(parsed["at"])
        if "data.nhm.ac.uk" in url:
            catno = parsed["catalogNumber"]
            catno = (
                "BMNH "
                + catno.replace("NHMUK", "")
                .replace("ZD", "")
                .replace("GMCM", "")
                .replace("GERM", "")
                .strip()
            )
            catno = clean_up_bmnh_type(catno).replace("BMNH ", "BMNH Mammals ")
            catno = re.sub(r"\.\[[a-zA-Z]\]$", "", catno)
            catno = re.sub(r"\.([a-z])$", r"\1", catno)
        elif "coldb.mnhn.fr" in url:
            code = url.split("/")[-2].upper()
            catno = f"MNHN-{code}-{parsed['catalogNumber']}"
        elif "institutioncode" in parsed and "catalognumber" in parsed:
            catno = f"{parsed['institutioncode']} {parsed['catalognumber']}"
            catno = (
                catno.replace("UCMP ", "UCMP:V:")
                .replace("FMNH ", "FMNH Mammals ")
                .replace("UCLA ", "UCLA Mammals ")
                .replace("UF ", "UF:VP:")
            )
            catno = re.sub(r"^([A-Z]+) \1", r"\1", catno)
        else:
            continue
        from_spec_details[url] = catno

    new_tags = []
    messages = []
    possible_types = set(get_possible_type_specimens(nam))
    for tag in nam.type_tags:
        if not isinstance(tag, TypeTag.TypeSpecimenLink):
            new_tags.append(tag)
            continue
        specimen = None
        if tag.url in from_spec_details:
            specimen = from_spec_details[tag.url]
        elif tag.url.startswith("https://mczbase.mcz.harvard.edu/guid/"):
            specimen = tag.url.removeprefix("https://mczbase.mcz.harvard.edu/guid/")
        elif match := re.fullmatch(
            r"https?://portal\.vertnet\.org/o/ucla/mammals\?id=urn-catalog-ucla-mammals-(\d+)",
            tag.url,
        ):
            specimen = f"UCLA Mammals {match.group(1)}"
        elif match := re.fullmatch(
            r"http://coldb\.mnhn\.fr/catalognumber/mnhn/([a-z]+)/([a-z\-]+\d+)([a-z])",
            tag.url,
        ):
            specimen = f"MNHN-{match.group(1).upper()}-{match.group(2).upper()}{match.group(3)}"
        elif match := re.fullmatch(
            r"http://portal\.vertnet\.org/o/amnh/mammals\?id=urn-catalog-amnh-mammals-([a-z\d\-]+)",
            tag.url,
        ):
            specimen = f"AMNH {match.group(1).upper()}"

        if specimen is not None and specimen in possible_types:
            new_tag = TypeTag.TypeSpecimenLinkFor(tag.url, specimen)
            messages.append(f"{tag} -> {new_tag}")
        else:
            if specimen is not None:
                print(
                    f"Reject {specimen!r} (not in {possible_types} for {nam}, URL"
                    f" {tag.url})"
                )
            yield f"replace TypeSpecimenLink tag: {tag}"
            new_tag = tag
        new_tags.append(new_tag)

    if not messages:
        return
    message = (
        f"replace TypeSpecimenLink tag with TypeSpecimenLinkFor: {', '.join(messages)}"
    )
    yield field_issue(message, nam, "type_tags", new_tags)


def get_possible_type_specimens(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        match spec:
            case SpecimenRange(start=start, end=end):
                yield from _get_possible_type_specimens_from_specimen(start)
                yield from _get_possible_type_specimens_from_specimen(end)
            case Specimen():
                yield from _get_possible_type_specimens_from_specimen(spec)


def _get_possible_type_specimens_from_specimen(spec: Specimen) -> Iterable[str]:
    yield spec.base.stringify()
    for former in spec.former_texts:
        yield former.stringify()
    for future in spec.future_texts:
        yield future.stringify()
    for extra in spec.extra_texts:
        yield extra.stringify()


def clean_up_bmnh_type(text: str) -> str:
    for rgx, replacement in _BMNH_REGEXES:
        text = re.sub(rgx, replacement, text)
    return text
