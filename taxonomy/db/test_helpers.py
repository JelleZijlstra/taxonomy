from datetime import date

from .helpers import get_date_object, romanize_russian, trimdoi, make_roman_numeral, parse_roman_numeral


def assert_romanizes(cyrillic: str, latin: str) -> None:
    assert romanize_russian(cyrillic) == latin


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
