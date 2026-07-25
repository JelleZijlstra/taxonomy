from .name import ParsedLocationName, split_trailing_parenthetical


def test_parse_location_name() -> None:
    assert ParsedLocationName.parse("Foo River") == ParsedLocationName("Foo River")
    assert ParsedLocationName.parse("Foo River (California)") == ParsedLocationName(
        "Foo River", "California"
    )
    assert ParsedLocationName.parse(
        "Foo River (California): mouth"
    ) == ParsedLocationName("Foo River", "California", "mouth")
    assert ParsedLocationName.parse("Castries (Hérault): 1 km N") == ParsedLocationName(
        "Castries", "Hérault", "1 km N"
    )


def test_location_name_render_is_canonical() -> None:
    assert ParsedLocationName.parse(" Foo River (California) :  mouth ").render() == (
        "Foo River (California): mouth"
    )


def test_parse_nested_disambiguator() -> None:
    assert ParsedLocationName.parse(
        "Maastricht Formation (Limburg (Netherlands)): quarry"
    ) == ParsedLocationName("Maastricht Formation", "Limburg (Netherlands)", "quarry")


def test_catalogue_parentheses_are_not_disambiguators() -> None:
    assert ParsedLocationName.parse("HGSP 81-07(a)") == ParsedLocationName(
        "HGSP 81-07(a)"
    )
    assert split_trailing_parenthetical("HGSP 81-07(a)") is None
