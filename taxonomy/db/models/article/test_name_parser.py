from pathlib import Path
from typing import Any
from collections.abc import Mapping

from .name_parser import NameParser


def check(
    raw_name: str,
    base_name: Mapping[str, Any] = {},
    authorship: tuple[None | str | list[str], str | None] = (None, None),
    modifier: str = "",
    extension: str = "pdf",
    expect_errors: bool = False,
) -> None:
    data_path = Path(__file__).parent / "parserdata"
    parser = NameParser(raw_name, data_path)
    assert expect_errors == bool(parser.errorDescription)
    assert parser.extension == extension
    assert parser.rawName == raw_name
    assert parser.modifier == modifier
    assert parser.authorship == authorship
    assert parser.baseName == base_name


def test_nov() -> None:
    check("Thomasomys 4nov.pdf", base_name={"nov": (4, "Thomasomys")})


def test_modifier() -> None:
    check(
        "Octodontoidea (Verzi et al. 2016).pdf",
        authorship=(["Verzi"], "2016"),
        base_name={"normal": {"names": ["Octodontoidea"]}},
    )


def test_normal() -> None:
    check(
        "Rhagomys longilingua Peru.pdf",
        base_name={
            "normal": {
                "names": ["Rhagomys longilingua"],
                "geography": [(("", "Peru"), "")],
            }
        },
    )
    check(
        "Micromammalia Slovenia Pleistocene.pdf",
        base_name={
            "normal": {
                "names": ["Micromammalia"],
                "geography": [(("", "Slovenia"), "")],
                "times": [(None, "Pleistocene")],
            }
        },
    )
    check(
        "Croatia Podumci 1 E Pleistocene.pdf",
        base_name={
            "normal": {
                "geography": [(("", "Croatia"), "Podumci 1")],
                "times": [("E", "Pleistocene")],
            }
        },
    )
    check(
        "Chiroptera, Lipotyphla, Marsupialia Austria Korneuburg Miocene.pdf",
        base_name={
            "normal": {
                "names": ["Chiroptera", "Lipotyphla", "Marsupialia"],
                "geography": [(("", "Austria"), "Korneuburg")],
                "times": [(None, "Miocene")],
            }
        },
    )
    check(
        "Gomphotheriidae Mongolia Valley of Lakes E-M Miocene.pdf",
        base_name={
            "normal": {
                "names": ["Gomphotheriidae"],
                "geography": [(("", "Mongolia"), "Valley of Lakes")],
                "times": [("E", "Miocene"), ("M", "Miocene")],
            }
        },
    )
