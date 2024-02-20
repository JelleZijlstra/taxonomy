from .type_specimen import (
    AnySpecimen,
    SimpleSpecimen,
    SpecialSpecimen,
    Specimen,
    SpecimenRange,
    TripletSpecimen,
    parse_type_specimen,
)


def check_both_ways(text: str, expected: list[AnySpecimen]) -> None:
    assert parse_type_specimen(text) == expected
    assert ", ".join(spec.stringify() for spec in expected) == text


def _make_simple(text: str) -> Specimen:
    return Specimen(SimpleSpecimen(text))


def test_parse_type_specimen() -> None:
    check_both_ways("MVZ 42", [_make_simple("MVZ 42")])
    check_both_ways("MVZ 42, MVZ 43", [_make_simple("MVZ 42"), _make_simple("MVZ 43")])
    check_both_ways(
        "MVZ 42 through MVZ 45",
        [SpecimenRange(_make_simple("MVZ 42"), _make_simple("MVZ 45"))],
    )
    check_both_ways(
        "MVZ (unnumbered) (= AMNH 42)",
        [
            Specimen(
                SpecialSpecimen("MVZ", "unnumbered"),
                former_texts=[SimpleSpecimen("AMNH 42")],
            )
        ],
    )
    check_both_ways(
        "MVZ 123 (two specimens!) (= ZMB 42)",
        [
            Specimen(
                SimpleSpecimen("MVZ 123"),
                comment="two specimens",
                former_texts=[SimpleSpecimen("ZMB 42")],
            )
        ],
    )
    check_both_ways(
        "MVZ 123 (two specimens!) (=> PNM 123) (+ MSB 123) (= ZMB 42)",
        [
            Specimen(
                SimpleSpecimen("MVZ 123"),
                comment="two specimens",
                future_texts=[SimpleSpecimen("PNM 123")],
                extra_texts=[SimpleSpecimen("MSB 123")],
                former_texts=[SimpleSpecimen("ZMB 42")],
            )
        ],
    )
    check_both_ways("MVZ:Mamm:123", [Specimen(TripletSpecimen("MVZ", "Mamm", "123"))])
