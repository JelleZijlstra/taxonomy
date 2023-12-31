from .name_lint import (
    AnySpecimen,
    ParsedOrgan,
    SpecialSpecimen,
    Specimen,
    SpecimenRange,
    parse_date,
    parse_type_specimen,
)


def test_parse_date() -> None:
    assert parse_date("Feb 2013") == "2013-02"
    assert parse_date("1 Feb 2013") == "2013-02-01"
    assert parse_date("23 Feb 2013") == "2013-02-23"
    assert parse_date("July 2013") == "2013-07"
    assert parse_date("7 July 2013") == "2013-07-07"


def check_both_ways(text: str, expected: list[AnySpecimen]) -> None:
    assert parse_type_specimen(text) == expected
    assert ", ".join(spec.stringify() for spec in expected) == text


def test_parse_type_specimen() -> None:
    check_both_ways("MVZ 42", [Specimen("MVZ 42")])
    check_both_ways("MVZ 42, MVZ 43", [Specimen("MVZ 42"), Specimen("MVZ 43")])
    check_both_ways(
        "MVZ 42 through MVZ 45", [SpecimenRange(Specimen("MVZ 42"), Specimen("MVZ 45"))]
    )
    check_both_ways(
        "MVZ (unnumbered) (= AMNH 42)",
        [SpecialSpecimen("MVZ", "unnumbered", former_texts=["AMNH 42"])],
    )
    check_both_ways(
        "MVZ 123 (two specimens!) (= ZMB 42)",
        [Specimen("MVZ 123", comment="two specimens", former_texts=["ZMB 42"])],
    )
    check_both_ways(
        "MVZ 123 (two specimens!) (=> PNM 123) (+ MSB 123) (= ZMB 42)",
        [
            Specimen(
                "MVZ 123",
                comment="two specimens",
                future_texts=["PNM 123"],
                extra_texts=["MSB 123"],
                former_texts=["ZMB 42"],
            )
        ],
    )
