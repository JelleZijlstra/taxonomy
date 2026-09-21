import pytest

from scripts import mdd_diff
from scripts.mdd_names import (
    _mdd_coordinates_match,
    get_name_for_type_locality,
    get_names_for_type_data,
    get_type_locality_coordinates,
    get_type_locality_country_and_subregion,
)
from taxonomy.db.constants import (
    ArticleType,
    EmendationJustification,
    NamingConvention,
    NomenclatureStatus,
    PersonType,
    RegionKind,
)
from taxonomy.db.models import Article, Person, Region
from taxonomy.db.models.location import Location
from taxonomy.db.models.name import Name, NameTag, TypeTag
from taxonomy.db.models.person import AuthorTag


@pytest.mark.parametrize(
    "tag_type",
    [
        NameTag.NomenNovumFor,
        NameTag.JustifiedEmendationOf,
        NameTag.UnjustifiedEmendationOf,
        NameTag.IncorrectOriginalSpellingOf,
        NameTag.IncorrectSubsequentSpellingOf,
        NameTag.NameCombinationOf,
        NameTag.MandatoryChangeOf,
        NameTag.SubsequentUsageOf,
        NameTag.RerankingOf,
    ],
)
def test_replacement_type_data_follows_multiple_links(tag_type: type[NameTag]) -> None:
    country = Region.virtual(name="Sweden", kind=RegionKind.country)
    original = Name.virtual(
        tags=(),
        type_tags=(),
        type_locality=Location.virtual(
            region=country, latitude="59°N", longitude="18°E"
        ),
    )
    if tag_type is NameTag.JustifiedEmendationOf:
        tag = NameTag.JustifiedEmendationOf(
            original, justification=EmendationJustification.inadvertent_error
        )
    else:
        tag = tag_type(original)
    intermediate = Name.virtual(tags=(tag,), type_tags=())
    replacement = Name.virtual(
        nomenclature_status=NomenclatureStatus.nomen_novum,
        tags=(NameTag.NomenNovumFor(intermediate),),
        type_tags=(),
    )

    names = get_names_for_type_data(replacement)

    assert names == [replacement, intermediate, original]
    assert get_type_locality_country_and_subregion(names[-1]) == ("Sweden", "", "")
    assert get_type_locality_coordinates(names, names[-1]) == ("59", "18")


@pytest.mark.parametrize(
    "tag_type", [NameTag.MisidentificationOf, NameTag.UnavailableVersionOf]
)
def test_replacement_type_data_does_not_cross_unrelated_links(
    tag_type: type[NameTag],
) -> None:
    unrelated = _name(location_coordinates=("1", "2"))
    target = Name.virtual(tags=(tag_type(unrelated),), type_tags=(), type_locality=None)
    replacement = Name.virtual(
        nomenclature_status=NomenclatureStatus.nomen_novum,
        tags=(NameTag.NomenNovumFor(target),),
        type_tags=(),
    )

    names = get_names_for_type_data(replacement)

    assert names == [replacement, target]
    assert get_type_locality_coordinates(names, names[-1]) == ("", "")


def test_replacement_type_data_retains_intermediate_coordinates() -> None:
    original = Name.virtual(tags=(), type_tags=(), type_locality=None)
    intermediate = Name.virtual(
        tags=(NameTag.NameCombinationOf(original),),
        type_tags=(TypeTag.Coordinates("3°N", "4°E"),),
    )
    replacement = Name.virtual(
        nomenclature_status=NomenclatureStatus.nomen_novum,
        tags=(NameTag.NomenNovumFor(intermediate),),
        type_tags=(),
    )

    names = get_names_for_type_data(replacement)

    assert get_type_locality_coordinates(names, names[-1]) == ("3", "4")


def test_replacement_type_data_rejects_cycles() -> None:
    replacement = Name.virtual(
        nomenclature_status=NomenclatureStatus.nomen_novum, tags=()
    )
    intermediate = Name.virtual(tags=(NameTag.NameCombinationOf(replacement),))
    replacement.tags = (NameTag.NomenNovumFor(intermediate),)

    with pytest.raises(ValueError, match="cycle in type-data links"):
        get_names_for_type_data(replacement)


def test_replacement_type_data_follows_long_acyclic_chains() -> None:
    original = Name.virtual(tags=())
    target = original
    for _ in range(12):
        target = Name.virtual(
            nomenclature_status=NomenclatureStatus.nomen_novum,
            tags=(NameTag.NomenNovumFor(target),),
        )

    names = get_names_for_type_data(target)

    assert len(names) == 13
    assert names[-1] == original


def test_type_locality_resolution_preserves_existing_locality() -> None:
    replacement = _name()
    immediate = _name(location_coordinates=("1", "2"))
    original = _name(location_coordinates=("3", "4"))

    assert get_name_for_type_locality([replacement, immediate, original]) == immediate


def test_type_locality_resolution_prefers_original_over_intermediate_locality() -> None:
    replacement = _name()
    immediate = _name()
    intermediate = _name(location_coordinates=("1", "2"))
    original = _name(location_coordinates=("3", "4"))

    assert (
        get_name_for_type_locality([replacement, immediate, intermediate, original])
        == original
    )


def test_type_locality_resolution_retains_intermediate_if_original_is_missing() -> None:
    replacement = _name()
    immediate = _name()
    intermediate = _name(location_coordinates=("1", "2"))
    original = _name()

    assert (
        get_name_for_type_locality([replacement, immediate, intermediate, original])
        == intermediate
    )


def test_replacement_type_data_without_target_uses_own_data() -> None:
    replacement = Name.virtual(
        nomenclature_status=NomenclatureStatus.nomen_novum, tags=()
    )

    assert get_names_for_type_data(replacement) == [replacement]


def test_non_replacement_type_data_uses_own_data() -> None:
    original = Name.virtual(tags=())
    variant = Name.virtual(
        nomenclature_status=NomenclatureStatus.unjustified_emendation,
        tags=(NameTag.UnjustifiedEmendationOf(original),),
    )

    assert get_names_for_type_data(variant) == [variant]


def _name(
    *,
    coordinates: tuple[str, str] | None = None,
    location_coordinates: tuple[str | None, str | None] | None = None,
    partial_location_coordinates: tuple[tuple[int, str | None, str | None], ...] = (),
) -> Name:
    tags: tuple[TypeTag, ...] = (
        () if coordinates is None else (TypeTag.Coordinates(*coordinates),)
    )
    tags += tuple(
        TypeTag.PartialTypeLocality(
            Location.virtual(latitude=latitude, longitude=longitude)
        )
        for _, latitude, longitude in partial_location_coordinates
    )
    if location_coordinates is None:
        type_locality = None
    else:
        type_locality = Location.virtual(
            latitude=location_coordinates[0], longitude=location_coordinates[1]
        )
    return Name.virtual(type_tags=tags, type_locality=type_locality)


def test_type_locality_coordinates_fall_back_to_location() -> None:
    name = _name(location_coordinates=("37.9", "-122.1"))

    assert get_type_locality_coordinates([name], name) == ("37.9", "-122.1")


def test_name_coordinates_take_precedence_over_location() -> None:
    name = _name(coordinates=("38°N", "123°W"), location_coordinates=("37.9", "-122.1"))

    assert get_type_locality_coordinates([name], name) == ("38", "-123")


def test_invalid_name_coordinates_fall_back_to_location() -> None:
    name = _name(
        coordinates=("not a latitude", "not a longitude"),
        location_coordinates=("37.9°N", "122.1°W"),
    )

    assert get_type_locality_coordinates([name], name) == ("37.9", "-122.1")


def test_type_locality_coordinate_ranges_are_exported_for_mdd() -> None:
    name = _name(coordinates=("12°S-10.5°S", "99°10'E-99°40'E"))

    assert get_type_locality_coordinates([name], name) == (
        "(-12 to -10.5)",
        "(99.166667 to 99.666667)",
    )


def test_partial_type_locality_coordinates_are_unioned_for_mdd() -> None:
    name = _name(
        partial_location_coordinates=(
            (1, "5.2586951°N-5.3699421°N", "162.9007493°E-163.0356302°E"),
            (2, "6.784067°N-6.9898922°N", "158.1112084°E-158.3379619°E"),
        )
    )

    assert get_type_locality_coordinates([name], name) == (
        "(5.258695 to 6.989892)",
        "(158.111208 to 163.03563)",
    )


def test_partial_type_locality_union_requires_every_component() -> None:
    name = _name(
        location_coordinates=("1", "2"),
        partial_location_coordinates=((1, "5°N", "6°E"), (2, None, None)),
    )

    assert get_type_locality_coordinates([name], name) == ("", "")


def test_name_coordinates_take_precedence_over_partial_type_localities() -> None:
    name = _name(
        coordinates=("3°N", "4°E"),
        partial_location_coordinates=((1, "5°N", "6°E"), (2, "7°N", "8°E")),
    )

    assert get_type_locality_coordinates([name], name) == ("3", "4")


def test_mdd_coordinate_ranges_compare_semantically() -> None:
    assert _mdd_coordinates_match(
        "(10.2 to 10.2167)", "(10.2167 to 10.2)", is_latitude=False
    )
    assert _mdd_coordinates_match(
        "(99.166667 to 99.666667)", "(99.1667 to 99.6667)", is_latitude=False
    )
    assert not _mdd_coordinates_match(
        "(-12 to -10.5)", "(-10.5 to 12)", is_latitude=True
    )


def _person(
    family_name: str,
    *,
    given_names: str | None = None,
    tussenvoegsel: str | None = None,
    naming_convention: NamingConvention = NamingConvention.unspecified,
) -> Person:
    return Person.virtual(
        family_name=family_name,
        given_names=given_names,
        tussenvoegsel=tussenvoegsel,
        naming_convention=naming_convention,
        type=PersonType.checked,
        tags=(),
    )


def test_mdd_author_uses_native_order_for_vietnamese_names() -> None:
    vuong = _person(
        "Vuong",
        given_names="Tu",
        tussenvoegsel="Tan",
        naming_convention=NamingConvention.vietnamese,
    )
    hassanin = _person("Hassanin")
    article_authors = [vuong, _person("Cornette"), _person("Utge"), hassanin]
    article = Article.virtual(
        type=ArticleType.JOURNAL,
        author_tags=tuple(
            AuthorTag.Author(person=author) for author in article_authors
        ),
    )
    name = Name.virtual(
        author_tags=(AuthorTag.Author(person=vuong), AuthorTag.Author(person=hassanin)),
        original_citation=article,
    )

    assert (
        mdd_diff.get_mdd_style_authority(name, set())
        == "Vuong Tan Tu & Hassanin in Vuong Tan Tu, Cornette, Utge, & Hassanin"
    )
    assert list(mdd_diff.possible_mdd_authors(vuong)) == ["Vuong Tan Tu"]


@pytest.mark.parametrize(
    ("convention", "family_name", "given_names", "expected"),
    [
        (NamingConvention.pinyin, "Wu", "A-fang", "Wu Afang"),
        (NamingConvention.chinese, "Liu", "Ch'eng-Chao", "Liu Ch'engchao"),
        (NamingConvention.chinese, "Tan", "Heok Hui", "Tan Heok Hui"),
        (NamingConvention.chinese, "Lee", "Chien C.", "Lee Chien C."),
        (NamingConvention.pinyin, "Wu", None, "Wu"),
        (NamingConvention.chinese, "Liu", None, "Liu"),
    ],
)
def test_mdd_chinese_name_spelling(
    convention: NamingConvention,
    family_name: str,
    given_names: str | None,
    expected: str,
) -> None:
    person = _person(family_name, given_names=given_names, naming_convention=convention)

    assert (
        mdd_diff.get_mdd_style_authority_for_single_person(
            person, set(), Name.virtual()
        )
        == expected
    )
    assert list(mdd_diff.possible_mdd_authors(person)) == [expected]
