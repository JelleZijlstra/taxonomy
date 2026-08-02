from datetime import date

import pytest

from taxonomy.db import helpers
from taxonomy.db.constants import Group, Rank

from .helpers import (
    get_date_object,
    group_of_rank,
    make_roman_numeral,
    parse_roman_numeral,
    romanize_russian,
    trimdoi,
)


def assert_romanizes(cyrillic: str, latin: str) -> None:
    assert romanize_russian(cyrillic) == latin


def test_extract_coordinates_normalizes_typographic_symbols() -> None:
    assert helpers.extract_coordinates("at 31º28′12″ S, 64˚44’24” W") == (
        "31°28'12\"S",
        "64°44'24\"W",
    )


def test_extract_coordinate_pairs() -> None:
    assert helpers.extract_coordinate_pairs(
        "first 31°28'12\"S, 64°44'24\"W; then 31.5°S, 64.5°W"
    ) == [("31°28'12\"S", "64°44'24\"W"), ("31.5°S", "64.5°W")]


def test_extract_coordinate_pairs_accepts_colon_separator() -> None:
    assert helpers.extract_coordinate_pairs("Bukwa (01°17'04.5\"N: 34°47'07.7\"E)") == [
        ("1°17'4.5\"N", "34°47'7.7\"E")
    ]


def test_extract_coordinate_pairs_accepts_prime_as_seconds_marker() -> None:
    assert helpers.extract_coordinate_pairs("lat. 23*29'00'N, long. 68*54'45\" E") == [
        ("23°29'0\"N", "68°54'45\"E")
    ]


def test_extract_coordinate_pairs_preserves_source_order_across_formats() -> None:
    assert helpers.extract_coordinate_pairs(
        "first (−3.44785, −79.61015), then 4°S, 80°W"
    ) == [("3.44785°S", "79.61015°W"), ("4°S", "80°W")]


@pytest.mark.parametrize("text", ["Figures 4.1-4.4, 5.4", "Figs. 3.1–3.3; 5.1"])
def test_extract_coordinate_pairs_ignores_figure_number_ranges(text: str) -> None:
    assert helpers.extract_coordinate_pairs(text) == []


def test_extract_coordinate_pairs_accepts_labeled_spaced_decimals() -> None:
    assert helpers.extract_coordinate_pairs(
        "Latitude 1.2667 S., Longitude 132.2000 E."
    ) == [("1.2667°S", "132.2°E")]


def test_extract_coordinate_pairs_accepts_dotted_degrees_minutes() -> None:
    assert helpers.extract_coordinate_pairs("Medje, 2.25 N – 27.18 E") == [
        ("2°25'N", "27°18'E")
    ]


def test_extract_coordinate_pairs_respects_negative_sign_with_direction() -> None:
    assert helpers.extract_coordinate_pairs("São Bento (38.7115 N, −9.1547 E)") == [
        ("38.7115°N", "9.1547°W")
    ]


def test_extract_coordinate_pairs_accepts_two_prefixed_directions() -> None:
    assert helpers.extract_coordinate_pairs("N46°11' E 95°03'") == [
        ("46°11'N", "95°3'E")
    ]


def test_extract_coordinate_pairs_normalizes_greek_east_direction() -> None:
    assert helpers.extract_coordinate_pairs("N50°52' Ε 20°38'") == [
        ("50°52'N", "20°38'E")
    ]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("site (12.14573, -61.613847)", [("12.14573°N", "61.613847°W")]),
        ("site (–31.1588°, –65.4509°)", [("31.1588°S", "65.4509°W")]),
        ("38°39'40±02\" S, 145°40'52±03\" E", [("38°39'40\"S", "145°40'52\"E")]),
        (
            "43°27'34\" north latitude and 141°35'35\" east longitude",
            [("43°27'34\"N", "141°35'35\"E")],
        ),
        ("N48°5¢, W122°0¢", [("48°5'N", "122°0'W")]),
        ("69°24'28″[N], 145°09'50″[W]", [("69°24'28\"N", "145°9'50\"W")]),
        ("longitude 49°4'E, latitude 12°56'S", [("12°56'S", "49°4'E")]),
        ("10*16'N/61*23'W", [("10°16'N", "61°23'W")]),
        ("near Bamboo Tekri (13.373*N & 92.999*E)", [("13.373°N", "92.999°E")]),
        (
            "22.5 km S San Quintin (30°22'17\"N, −115°51'52\"W)",
            [("30°22'17\"N", "115°51'52\"W")],
        ),
    ],
)
def test_extract_coordinate_pairs_additional_provenance_formats(
    text: str, expected: list[tuple[str, str]]
) -> None:
    assert helpers.extract_coordinate_pairs(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("82°47·6'N, 42°13·3'W", ("82°47.6'N", "42°13.3'W")),
        ("latitude 82°47·6'N, longitude 42°13·3'W", ("82°47.6'N", "42°13.3'W")),
        ("24°21' S 133°43' E", ("24°21'S", "133°43'E")),
        ("3° 01' N 12° 22' E", ("3°1'N", "12°22'E")),
        ("(−3.44785*, −79.61015*)", ("3.44785°S", "79.61015°W")),
        ("MIRADOR ca. 0126S/7815W", ("1°26'S", "78°15'W")),
        ("28o 36' LN, 112o 51' LW", ("28°36'N", "112°51'W")),
    ],
)
def test_extract_coordinates_additional_source_variants(
    text: str, expected: tuple[str, str]
) -> None:
    assert helpers.extract_coordinates(text) == expected


def test_extract_coordinates_normalizes_decimal_comma_and_western_o() -> None:
    assert helpers.extract_coordinates("8° 9' 4,49\" N, 61° 46' 45,79\" O") == (
        "8°9'4.49\"N",
        "61°46'45.79\"W",
    )


def test_extract_coordinates_ignores_stray_period_before_seconds_mark() -> None:
    assert helpers.extract_coordinates("27°14'59.93\"S, 57°50'37.16.″W") == (
        "27°14'59.93\"S",
        "57°50'37.16\"W",
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("S1.70190°, E25.13970°", ("1.7019°S", "25.1397°E")),
        ("near S 1.02237*, E 24.42368*", ("1.02237°S", "24.42368°E")),
        ("S 41°57'13.5\", W 66°19'20.3\"", ("41°57'13.5\"S", "66°19'20.3\"W")),
        ("14o48'16.1\" S, 68o44'58.6\"W", ("14°48'16.1\"S", "68°44'58.6\"W")),
        ("52* 00' N – 02* 48' E", ("52°0'N", "2°48'E")),
        ("27°58' N and 89°31' E", ("27°58'N", "89°31'E")),
        ("27°14'59.93″S | 57°50'37.16.″W", ("27°14'59.93\"S", "57°50'37.16\"W")),
        ("c.1°30'N 30°30'E", ("1°30'N", "30°30'E")),
        ("about 0°35' N., 27°50' E", ("0°35'N", "27°50'E")),
        (
            "16*5'5\" östl. Länge, 48°27'13\" nördl. Breite",
            ("48°27'13\"N", "16°5'5\"E"),
        ),
    ],
)
def test_extract_coordinates_source_variants(
    text: str, expected: tuple[str, str]
) -> None:
    assert helpers.extract_coordinates(text) == expected


def test_romanize_russian() -> None:
    # These test cases taken from the examples in
    # https://en.wikipedia.org/wiki/BGN/PCGN_romanization_of_Russian
    assert_romanizes("Азов", "Azov")
    assert_romanizes("Тамбов", "Tambov")
    assert_romanizes("Барнаул", "Barnaul")
    assert_romanizes("Кубань", "Kuban'")
    assert_romanizes("Владимир", "Vladimir")
    assert_romanizes("Ульяновск", "Ul'yanovsk")
    assert_romanizes("Грозный", "Groznyy")
    assert_romanizes("Волгодонск", "Volgodonsk")
    assert_romanizes("Дзержинский", "Dzerzhinskiy")
    assert_romanizes("Нелидово", "Nelidovo")
    assert_romanizes("Елизово", "Yelizovo")
    assert_romanizes("Чапаевск", "Chapayevsk")
    assert_romanizes("Мейеровка", "Meyyerovka")
    assert_romanizes("Юрьев", "Yur'yev")
    assert_romanizes("Объезд", 'Ob"yezd')
    assert_romanizes("Белкино", "Belkino")
    assert_romanizes("Ёдва", "Yëdva")
    assert_romanizes("Змииёвка", "Zmiiyëvka")
    assert_romanizes("Айёган", "Ayyëgan")
    assert_romanizes("Воробьёво", "Vorob'yëvo")
    assert_romanizes("Кебанъёль", "Keban\"yël'")
    assert_romanizes("Озёрный", "Ozërnyy")
    assert_romanizes("Жуков", "Zhukov")
    assert_romanizes("Лужники", "Luzhniki")
    assert_romanizes("Звенигород", "Zvenigorod")
    assert_romanizes("Вязьма", "Vyaz'ma")
    assert_romanizes("Иркутск", "Irkutsk")
    assert_romanizes("Апатиты", "Apatity")
    assert_romanizes("Тыайа", "Tyaya")
    assert_romanizes("Сайылык", "Sayylyk")
    assert_romanizes("Ойусардах", "Oyusardakh")
    assert_romanizes("Йошкар-Ола", "Yoshkar-Ola")
    assert_romanizes("Бийск", "Biysk")
    assert_romanizes("Киров", "Kirov")
    assert_romanizes("Енисейск", "Yeniseysk")
    assert_romanizes("Ломоносов", "Lomonosov")
    assert_romanizes("Нелидово", "Nelidovo")
    assert_romanizes("Менделеев", "Mendeleyev")
    assert_romanizes("Каменка", "Kamenka")
    assert_romanizes("Новосибирск", "Novosibirsk")
    assert_romanizes("Кандалакша", "Kandalaksha")
    assert_romanizes("Омск", "Omsk")
    assert_romanizes("Красноярск", "Krasnoyarsk")
    assert_romanizes("Петрозаводск", "Petrozavodsk")
    assert_romanizes("Серпухов", "Serpukhov")
    assert_romanizes("Ростов", "Rostov")
    assert_romanizes("Северобайкальск", "Severobaykal'sk")
    assert_romanizes("Сковородино", "Skovorodino")
    assert_romanizes("Чайковский", "Chaykovskiy")
    assert_romanizes("Тамбов", "Tambov")
    assert_romanizes("Мытищи", "Mytishchi")
    assert_romanizes("Углич", "Uglich")
    assert_romanizes("Дудинка", "Dudinka")
    assert_romanizes("Фурманов", "Furmanov")
    assert_romanizes("Уфа", "Ufa")
    assert_romanizes("Хабаровск", "Khabarovsk")
    assert_romanizes("Прохладный", "Prokhladnyy")
    assert_romanizes("Цимлянск", "Tsimlyansk")
    assert_romanizes("Елец", "Yelets")
    assert_romanizes("Чебоксары", "Cheboksary")
    assert_romanizes("Печора", "Pechora")
    assert_romanizes("Шахтёрск", "Shakhtërsk")
    assert_romanizes("Мышкин", "Myshkin")
    assert_romanizes("Щёлково", "Shchëlkovo")
    assert_romanizes("Ртищево", "Rtishchevo")
    assert_romanizes("Куыркъявр", 'Kuyrk"yavr')
    assert_romanizes("Ыгыатта", "Ygyatta")
    assert_romanizes("Тыайа", "Tyaya")
    assert_romanizes("Тыэкан", "Tyekan")
    assert_romanizes("Суык-Су", "Suyk-Su")
    assert_romanizes("Куыркъявр", 'Kuyrk"yavr')
    assert_romanizes("Ыттык-Кюёль", "Yttyk-Kyuyël'")
    assert_romanizes("Тында", "Tynda")
    assert_romanizes("Тюмень", "Tyumen'")
    assert_romanizes("Улан-Удэ", "Ulan-Ude")
    assert_romanizes("Электрогорск", "Elektrogorsk")
    assert_romanizes("Руэм", "Ruem")
    assert_romanizes("Юбилейный", "Yubileynyy")
    assert_romanizes("Ключевская", "Klyuchevskaya")
    assert_romanizes("Якутск", "Yakutsk")
    assert_romanizes("Брянск", "Bryansk")
    assert_romanizes("Вяртсиля", "Vyartsilya")
    assert_romanizes("Ташчишма", "Tashchishma")


def test_trimdoi() -> None:
    assert trimdoi("10.1234/452. ") == "10.1234/452"
    assert trimdoi(" doi:10.1234/567 ") == "10.1234/567"


def test_get_date_object() -> None:
    assert get_date_object("1990-1991") == date(1991, 12, 31)
    assert get_date_object("1991") == date(1991, 12, 31)
    assert get_date_object("1991-02") == date(1991, 2, 28)
    assert get_date_object("1992-02") == date(1992, 2, 29)
    assert get_date_object("1991-02-11") == date(1991, 2, 11)


def test_make_roman_numeral() -> None:
    assert make_roman_numeral(1) == "I"
    assert make_roman_numeral(2) == "II"
    assert make_roman_numeral(4) == "IV"
    assert make_roman_numeral(5) == "V"


def test_parse_roman_numeral() -> None:
    assert parse_roman_numeral("IV") == 4
    assert parse_roman_numeral("III") == 3
    for i in range(1, 100):
        assert parse_roman_numeral(make_roman_numeral(i)) == i


def test_group_of_rank() -> None:
    for rank in Rank:
        if rank is Rank.synonym:
            continue
        group = group_of_rank(rank)
        assert isinstance(group, Group)
