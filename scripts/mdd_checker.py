"""

Script to check for and fix issues in the MDD species sheet.

"""

import argparse
import csv
import datetime
import functools
import itertools
import re
import time
from collections import defaultdict
from collections.abc import Container, Generator, Iterable, Sequence
from dataclasses import dataclass, fields
from typing import Any, TypedDict, TypeVar, cast

import gspread

from taxonomy import getinput
from taxonomy.config import get_options

Syn = dict[str, str]

RANK_REGEX = r"[A-Z][a-z]+|incertae sedis|NA"
SIMPLE_LATITUDE = r"([-−]?[\d\.]+)?"
LATITUDE = rf"\({SIMPLE_LATITUDE} (to|or) {SIMPLE_LATITUDE}\)|{SIMPLE_LATITUDE}|"
PERMISSIVE_RANGES = True


RANKS = [
    "subclass",
    "infraclass",
    "magnorder",
    "superorder",
    "order",
    "suborder",
    "infraorder",
    "parvorder",
    "superfamily",
    "family",
    "subfamily",
    "tribe",
    "genus",
]

# Columns that map directly to a column in the synonyms sheet
SIMPLE_COLUMNS = [
    ("genus", "MDD_genus"),
    ("specificEpithet", "MDD_specificEpithet"),
    ("authoritySpeciesAuthor", "MDD_author"),
    ("authoritySpeciesYear", "MDD_year"),
    ("authorityParentheses", "MDD_authority_parentheses"),
    ("originalNameCombination", "MDD_original_combination"),
    ("typeVoucher", "MDD_holotype"),
    ("typeKind", "MDD_type_kind"),
    ("typeLocalityLatitude", "MDD_type_latitude"),
    ("typeLocalityLongitude", "MDD_type_longitude"),
    ("typeVoucherURIs", "MDD_type_specimen_link"),
    ("order", "MDD_order"),
    ("family", "MDD_family"),
]
# Species sheet is in ALL CAPS, syn sheet is not
UPPERCASED_COLUMNS: list[tuple[str, str]] = []


COLUMN_TO_REGEX = {
    "sciName": r"[A-Z][a-z]+_[a-z]+",
    "id": r"\d+|",
    "phylosort": r"\d+",
    "subclass": r"NA|[A-Z][a-z]+",
    "infraclass": r"NA|[A-Z][a-z]+",
    "magnorder": r"NA|[A-Z][a-z]+",
    "superorder": r"NA|[A-Z][a-z]+",
    "order": RANK_REGEX,
    "suborder": RANK_REGEX,
    "infraorder": RANK_REGEX,
    "parvorder": RANK_REGEX,
    "superfamily": RANK_REGEX,
    "family": RANK_REGEX,
    "subfamily": RANK_REGEX,
    "tribe": RANK_REGEX,
    "genus": r"[A-Z][a-z]+",
    "subgenus": r"NA|[A-Z][a-z]+|incertae sedis",
    "specificEpithet": r"[a-z]+",
    "authoritySpeciesYear": r"\d{4}",
    "authorityParentheses": r"[0-1]",
    "authoritySpeciesLink": r"https?://.*|",
    "typeKind": r"holotype|neotype|lectotype|syntypes|nonexistent|",
    "typeVoucherURIs": r"https?://.*|",
    "typeLocalityLatitude": LATITUDE,
    "typeLocalityLongitude": LATITUDE,
    # "subregionDistribution": r"",
    # "countryDistribution": r"",
    # "continentDistribution": r"",
    # "biogeographicRealm": r"",
    "iucnStatus": r"NT|LC|CR|VU|DD|NA|EN|EX|EW",
    "extinct": r"[01]",
    "domestic": r"[01]",
    "flagged": r"[01]",
    "CMW_sciName": r"[A-Z][a-z]+_[a-z]+|NA",
    "diffSinceCMW": r"[01]",
    "MSW3_sciName": r"[A-Z][a-z]+_[a-z]+|NA",
    "diffSinceMSW3": r"[01]",
}

CONTINENTS = {
    "Africa",
    "Antarctica",
    "Asia",
    "Domesticated",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "NA",
}
REALMS = {
    "Afrotropic",
    "Australasia",
    "Oceania",
    "Domesticated",
    "Indomalaya",
    "Nearctic",
    "Neotropic",
    "Palearctic",
    "Marine",
}


class CountryInfo(TypedDict):
    continents: set[str]
    realms: set[str]


COUNTRIES: dict[str, CountryInfo] = {
    # The MDD thinks Alaska is a country
    "Alaska": {"continents": {"North America"}, "realms": {"Nearctic"}},
    "Afghanistan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Albania": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Algeria": {"continents": {"Africa"}, "realms": {"Palearctic"}},
    "American Samoa": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Andaman Islands": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Angola": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Anguilla": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Antarctica": {"continents": {"Antarctica"}, "realms": set()},
    "Antigua & Barbuda": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Argentina": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Armenia": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Aruba": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Australia": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "Austria": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Azerbaijan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Azores": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Bahamas": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Bahrain": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Bangladesh": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Barbados": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Belarus": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Belgium": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Belize": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Benin": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Bermuda": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Bhutan": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Bolivia": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Bonaire": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Bosnia & Herzegovina": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Botswana": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Bouvet Island": {"continents": set(), "realms": set()},
    "Brazil": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "British Virgin Islands": {
        "continents": {"North America"},
        "realms": {"Neotropic"},
    },
    "Brunei": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Bulgaria": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Burkina Faso": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Burundi": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Cabo Verde": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Cambodia": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Cameroon": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Canada": {"continents": {"North America"}, "realms": {"Nearctic"}},
    "Canary Islands": {"continents": {"Africa"}, "realms": {"Palearctic"}},
    "Cayman Islands": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Central African Republic": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Chad": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Chile": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "China": {"continents": {"Asia"}, "realms": {"Palearctic", "Indomalaya"}},
    "Christmas Island": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Cocos Islands": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Colombia": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Comoros": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Cook Islands": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Costa Rica": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Croatia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Cuba": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Curaçao": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Cyprus": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Czech Republic": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Côte d'Ivoire": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Democratic Republic of the Congo": {
        "continents": {"Africa"},
        "realms": {"Afrotropic"},
    },
    "Denmark": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Djibouti": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Domesticated": {"continents": {"Domesticated"}, "realms": {"Domesticated"}},
    "Dominica": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Dominican Republic": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "East Timor": {"continents": {"Asia"}, "realms": {"Australasia"}},
    "Ecuador": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Egypt": {"continents": {"Africa", "Asia"}, "realms": {"Palearctic"}},
    "El Salvador": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Equatorial Guinea": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Eritrea": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Estonia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Eswatini": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Ethiopia": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Falkland Islands (Malvinas)": {
        "continents": {"South America"},
        "realms": {"Neotropic"},
    },
    "Faroe Islands": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Fiji": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Finland": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "France": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "French Guiana": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "French Polynesia": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Gabon": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Galapagos": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Gambia": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Georgia": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Germany": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Ghana": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Greece": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Greenland": {"continents": {"North America"}, "realms": {"Nearctic"}},
    "Grenada": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Guadeloupe": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Guam": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Guatemala": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Guinea": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Guinea-Bissau": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Guyana": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Haiti": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Hawai'i": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Honduras": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Hungary": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Iceland": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "India": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Indonesia": {
        "continents": {"Asia", "Oceania"},
        "realms": {"Indomalaya", "Australasia"},
    },
    "Iran": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Iraq": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Ireland": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Israel": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Italy": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Jamaica": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Japan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Jordan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Kazakhstan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Kenya": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Kerguelen Islands": {"continents": {"Antarctica"}, "realms": set()},
    "Kiribati": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Kosovo": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Kuwait": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Kyrgyzstan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Laos": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Latvia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Lebanon": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Lesotho": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Liberia": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Libya": {"continents": {"Africa"}, "realms": {"Palearctic"}},
    "Liechtenstein": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Lithuania": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Luxembourg": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Madagascar": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Madeira": {"continents": {"Africa"}, "realms": {"Palearctic"}},
    "Malawi": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Malaysia": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Maldives": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Mali": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Malta": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Marshall Islands": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Martinique": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Mauritania": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Mauritius": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Mayotte": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Mexico": {"continents": {"North America"}, "realms": {"Nearctic", "Neotropic"}},
    "Micronesia": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Moldova": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Mongolia": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Montenegro": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Montserrat": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Morocco": {"continents": {"Africa"}, "realms": {"Palearctic"}},
    "Mozambique": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Myanmar": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Namibia": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Nauru": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Nepal": {"continents": {"Asia"}, "realms": {"Indomalaya", "Palearctic"}},
    "Netherlands": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "New Caledonia": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "New Zealand": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "Nicaragua": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Nicobar Islands": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Niger": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Nigeria": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Niue": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Norfolk Island": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "North Korea": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "North Macedonia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Northern Mariana Islands": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Norway": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Oman": {"continents": {"Asia"}, "realms": {"Afrotropic"}},
    "Pakistan": {"continents": {"Asia"}, "realms": {"Indomalaya", "Palearctic"}},
    "Palau": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Palestine": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Panama": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Papua New Guinea": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "Paraguay": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Peru": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Philippines": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Pitcairn": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Poland": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Portugal": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Prince Edward Islands": {"continents": {"Antarctica"}, "realms": set()},
    "Puerto Rico": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Qatar": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Republic of the Congo": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Reunion": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Romania": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Russia": {"continents": {"Asia", "Europe"}, "realms": {"Palearctic"}},
    "Rwanda": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Saba": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Saint Barthélemy": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Saint Helena": {"continents": set(), "realms": set()},
    "Saint Kitts & Nevis": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Saint Lucia": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Saint Martin": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Saint Vincent & the Grenadines": {
        "continents": {"North America"},
        "realms": {"Neotropic"},
    },
    "Samoa": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Saudi Arabia": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Senegal": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Serbia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Seychelles": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Sierra Leone": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Singapore": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Sint Eustatius": {"continents": {"North America"}, "realms": {"Neotropic"}},
    "Slovakia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Slovenia": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Solomon Islands": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "Somalia": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "South Africa": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "South Georgia & the South Sandwich Islands": {
        "continents": {"Antarctica"},
        "realms": set(),
    },
    "South Korea": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "South Sudan": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Spain": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Sri Lanka": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Sudan": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Suriname": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Sweden": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Switzerland": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "Syria": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "São Tomé & Príncipe": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Taiwan": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Tajikistan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Tanzania": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Thailand": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Togo": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Tokelau": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Tonga": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Trinidad & Tobago": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Tunisia": {"continents": {"Africa"}, "realms": {"Palearctic"}},
    "Turkey": {"continents": {"Asia", "Europe"}, "realms": {"Palearctic"}},
    "Turkmenistan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Turks & Caicos Islands": {
        "continents": {"North America"},
        "realms": {"Neotropic"},
    },
    "Tuvalu": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Uganda": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Ukraine": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "United Arab Emirates": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "United Kingdom": {"continents": {"Europe"}, "realms": {"Palearctic"}},
    "United States": {"continents": {"North America"}, "realms": {"Nearctic"}},
    "United States Virgin Islands": {
        "continents": {"North America"},
        "realms": {"Neotropic"},
    },
    "Uruguay": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Uzbekistan": {"continents": {"Asia"}, "realms": {"Palearctic"}},
    "Vanuatu": {"continents": {"Oceania"}, "realms": {"Australasia"}},
    "Venezuela": {"continents": {"South America"}, "realms": {"Neotropic"}},
    "Vietnam": {"continents": {"Asia"}, "realms": {"Indomalaya"}},
    "Wallis & Futuna": {"continents": {"Oceania"}, "realms": {"Oceania"}},
    "Yemen": {"continents": {"Asia", "Africa"}, "realms": {"Afrotropic"}},
    "Zambia": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "Zimbabwe": {"continents": {"Africa"}, "realms": {"Afrotropic"}},
    "NA": {"continents": {"NA"}, "realms": set()},
}
if PERMISSIVE_RANGES:
    COUNTRIES["Bhutan"]["realms"].add("Palearctic")
    COUNTRIES["India"]["realms"].add("Palearctic")
    COUNTRIES["Egypt"]["realms"].add("Afrotropic")
    COUNTRIES["Taiwan"]["realms"].add("Palearctic")
    COUNTRIES["Pakistan"]["realms"].add("Palearctic")
    COUNTRIES["Nepal"]["realms"].add("Palearctic")
    COUNTRIES["Myanmar"]["realms"].add("Palearctic")
    COUNTRIES["Mauritania"]["realms"].add("Palearctic")
    COUNTRIES["Morocco"]["realms"].add("Afrotropic")
    COUNTRIES["Tunisia"]["realms"].add("Afrotropic")
    COUNTRIES["Algeria"]["realms"].add("Afrotropic")
    COUNTRIES["Oman"]["realms"].add("Palearctic")
    COUNTRIES["Yemen"]["realms"].add("Palearctic")
    COUNTRIES["Saudi Arabia"]["realms"].add("Afrotropic")
    COUNTRIES["United States"]["realms"].add("Neotropic")

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[list[T]]:
    it = iter(iterable)
    while chunk := list(itertools.islice(it, n)):
        yield chunk


class MDDSpeciesRow(TypedDict):
    sciName: str
    id: str
    phylosort: str
    mainCommonName: str
    otherCommonNames: str
    subclass: str
    infraclass: str
    magnorder: str
    superorder: str
    order: str
    suborder: str
    infraorder: str
    parvorder: str
    superfamily: str
    family: str
    subfamily: str
    tribe: str
    genus: str
    subgenus: str
    specificEpithet: str
    authoritySpeciesAuthor: str
    authoritySpeciesYear: str
    authorityParentheses: str
    originalNameCombination: str
    authoritySpeciesCitation: str
    authoritySpeciesLink: str
    typeVoucher: str
    typeKind: str
    typeVoucherURIs: str
    typeLocality: str
    typeLocalityLatitude: str
    typeLocalityLongitude: str
    nominalNames: str
    taxonomyNotes: str
    taxonomyNotesCitation: str
    distributionNotes: str
    distributionNotesCitation: str
    subregionDistribution: str
    countryDistribution: str
    continentDistribution: str
    biogeographicRealm: str
    iucnStatus: str
    extinct: str
    domestic: str
    flagged: str
    CMW_sciName: str
    diffSinceCMW: str
    MSW3_matchtype: str
    MSW3_sciName: str
    diffSinceMSW3: str


@dataclass
class Issue:
    row_idx: int
    mdd_id: str
    sci_name: str
    mdd_column: str
    mdd_value: str
    description: str
    suggested_change: str | None = None

    def describe(self) -> str:
        text = f"{self.sci_name} ({self.mdd_id or 'no id'}): {self.mdd_column}: {self.mdd_value!r}: {self.description}"
        if self.suggested_change is not None:
            text += f" (suggested fix: {self.suggested_change!r})"
        return text

    def group_description(self) -> str:
        match bool(self.mdd_value), bool(self.suggested_change):
            case True, True:
                return f"{self.mdd_column}: textual differences"
            case True, False:
                return f"{self.mdd_column}: species sheet unexpectedly has data"
            case _:
                return f"{self.mdd_column}: add data to species sheet"


@dataclass
class MDDSpecies:
    row_idx: int
    row: MDDSpeciesRow

    def make_issue(
        self, col_name: str, description: str, suggested_value: str | None = None
    ) -> Issue:
        return Issue(
            self.row_idx,
            self.row["id"],
            self.row["sciName"],
            col_name,
            str(self.row.get(col_name, "")),
            description,
            suggested_value,
        )

    def lint_standalone(self) -> Iterable[Issue]:
        for col, rgx in COLUMN_TO_REGEX.items():
            if col in self.row and not re.fullmatch(rgx, self.row[col]):  # type: ignore[literal-required]
                yield self.make_issue(col, f"does not follow expected format {rgx!r}")

        expected_sci_name = f"{self.row['genus']}_{self.row['specificEpithet']}"
        if self.row["sciName"] != expected_sci_name:
            yield self.make_issue(
                "sciName",
                "does not match name inferred from 'genus' and 'specificEpithet' columns",
                expected_sci_name,
            )
        yield from self.lint_distribution_standalone()

    def get_countries(self) -> set[str]:
        if not self.row.get("countryDistribution"):
            return set()
        else:
            return {
                c.strip()
                for c in self.row["countryDistribution"].replace("?", "").split("|")
            }

    def lint_distribution_standalone(self) -> Iterable[Issue]:
        countries = self.get_countries()
        if not countries:
            yield self.make_issue("countryDistribution", "missing country distribution")

        if not self.row.get("continentDistribution"):
            yield self.make_issue(
                "continentDistribution", "missing continent distribution"
            )
            continents = set()
        else:
            continents = {
                c.strip()
                for c in self.row["continentDistribution"].replace("?", "").split("|")
            }
            for continent in continents:
                if continent not in CONTINENTS:
                    yield self.make_issue(
                        "continentDistribution", f"unknown continent {continent!r}"
                    )

        if not self.row.get("biogeographicRealm"):
            yield self.make_issue("biogeographicRealm", "missing biogeographic realm")
            realms = set()
        else:
            realms = {
                c.strip()
                for c in self.row["biogeographicRealm"].replace("?", "").split("|")
            }
            for realm in realms:
                if realm not in REALMS:
                    yield self.make_issue(
                        "biogeographicRealm", f"unknown realm {realm!r}"
                    )

        expected_continents = set()
        expected_cont_to_countries = defaultdict(set)
        allowed_continents = set()
        expected_realms = set()
        expected_realm_to_countries = defaultdict(set)
        allowed_realms = set()

        for country in countries:
            if country not in COUNTRIES:
                yield self.make_issue(
                    "countryDistribution", f"unknown country {country!r}"
                )
                continue
            country_data = COUNTRIES[country]
            if len(country_data["continents"]) == 1:
                expected_continents.update(country_data["continents"])
                for cont in country_data["continents"]:
                    expected_cont_to_countries[cont].add(country)
            allowed_continents.update(country_data["continents"])
            if len(country_data["realms"]) == 1:
                expected_realms.update(country_data["realms"])
                for realm in country_data["realms"]:
                    expected_realm_to_countries[realm].add(country)
            allowed_realms.update(country_data["realms"])

        if continents - allowed_continents:
            yield self.make_issue(
                "continentDistribution",
                f"occurs in {sorted(continents - allowed_continents)} but not in any "
                f"countries assigned to them (countries: {sorted(countries)})",
            )
        if missing_expected_cont := (expected_continents - continents):
            text = ", ".join(
                f"{cont} (based on {sorted(expected_cont_to_countries[cont])})"
                for cont in missing_expected_cont
            )
            yield self.make_issue(
                "continentDistribution", f"missing expected continents {text}"
            )
        if realms - allowed_realms - {"Marine"}:
            yield self.make_issue(
                "biogeographicRealm",
                f"occurs in {sorted(realms - allowed_realms)} but not in any countries "
                f"assigned to them (countries: {sorted(countries)})",
            )
        if "Marine" not in realms and (
            missing_expected_realm := (expected_realms - realms)
        ):
            text = ", ".join(
                f"{realm} (based on {expected_realm_to_countries[realm]})"
                for realm in sorted(missing_expected_realm)
            )
            yield self.make_issue(
                "biogeographicRealm", f"missing expected realms {text}"
            )


@dataclass
class SpeciesWithSyns:
    species: MDDSpecies
    base_name: Syn
    syns: list[Syn]

    def get_expected_nominal_names(self) -> str:
        names = [
            syn
            for syn in self.syns
            if syn["MDD_nomenclature_status"]
            not in ("name_combination", "subsequent_usage")
        ]
        names = sorted(names, key=lambda syn: (syn["MDD_year"], syn["MDD_root_name"]))
        return "|".join(self.stringify_syn(syn) for syn in names)

    def stringify_syn(self, syn: Syn) -> str:
        parens = syn["MDD_authority_parentheses"] == "1"
        year = f", {syn['MDD_year']}" if syn["MDD_year"] else ""
        status = (
            f" [{syn['MDD_nomenclature_status'].replace('_', ' ')}]"
            if syn["MDD_nomenclature_status"] != "available"
            else ""
        )
        return f"{syn['MDD_root_name']} {'(' if parens else ''}{syn['MDD_author']}{year}{')' if parens else ''}{status}"

    def get_expected_row(self) -> dict[str, str]:
        simple = {sp_col: self.base_name[syn_col] for sp_col, syn_col in SIMPLE_COLUMNS}
        caps = {
            sp_col: self.base_name[syn_col].upper()
            for sp_col, syn_col in UPPERCASED_COLUMNS
        }
        if self.base_name["MDD_authority_page_link"]:
            link = self.base_name["MDD_authority_page_link"]
        elif self.base_name["MDD_authority_link"]:
            link = self.base_name["MDD_authority_link"]
        else:
            link = ""
        if self.base_name["MDD_authority_citation"]:
            citation = self.base_name["MDD_authority_citation"]
        elif self.base_name["MDD_unchecked_authority_citation"]:
            citation = self.base_name["MDD_unchecked_authority_citation"]
        else:
            citation = ""
        return {
            **simple,
            **caps,
            "authoritySpeciesLink": link,
            "authoritySpeciesCitation": citation,
            "nominalNames": self.get_expected_nominal_names(),
        }

    def compare_against_expected(self) -> Iterable[Issue]:
        for mdd_col, expected_val in self.get_expected_row().items():
            actual_val = self.species.row[mdd_col]  # type: ignore[literal-required]
            if expected_val != actual_val:
                description = f"Expected {expected_val!r}, found {actual_val!r}"
                if mdd_col == "authorityParentheses":
                    description += f" (original combination: {self.species.row['originalNameCombination']})"
                yield self.species.make_issue(mdd_col, description, expected_val)

        actual_countries = self.species.get_countries()
        if "Domesticated" not in actual_countries:
            for syn in self.syns:
                if (
                    syn["MDD_type_country"]
                    and syn["MDD_type_country"] not in actual_countries
                    and syn["MDD_type_country"] in COUNTRIES
                ):
                    yield self.species.make_issue(
                        "countryDistribution",
                        f"missing country {syn['MDD_type_country']} (type locality of synonym {self.stringify_syn(syn)})",
                    )


@functools.cache
def get_sheet() -> Any:
    options = get_options()
    gc = gspread.oauth()
    return gc.open(options.mdd_sheet)


def generate_match(
    species: list[MDDSpecies], syns: list[Syn]
) -> Generator[Issue, None, list[SpeciesWithSyns]]:
    sci_name_to_validity_to_sins: dict[str, dict[str, list[dict[str, str]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for syn in syns:
        sci_name_to_validity_to_sins[syn["MDD_species"].replace(" ", "_")][
            syn["MDD_validity"]
        ].append(syn)
    remaining_sci_names = {
        sci_name
        for sci_name, validity_to_sins in sci_name_to_validity_to_sins.items()
        if "species" in validity_to_sins
    }
    output: list[SpeciesWithSyns] = []
    for sp in species:
        if sp.row["sciName"] not in remaining_sci_names:
            yield sp.make_issue("sciName", "cannot find species in synonyms sheet")
            continue
        remaining_sci_names.discard(sp.row["sciName"])
        validity_to_sins = sci_name_to_validity_to_sins[sp.row["sciName"]]
        if len(validity_to_sins["species"]) != 1:
            yield sp.make_issue(
                "sciName",
                f"found {len(validity_to_sins['species'])} names marked as 'species' in synonyms sheet",
            )
            continue
        all_names = [syn for group in validity_to_sins.values() for syn in group]
        output.append(SpeciesWithSyns(sp, validity_to_sins["species"][0], all_names))
    for sci_name in remaining_sci_names:
        yield Issue(
            0,
            "",
            sci_name,
            "sciName",
            "",
            f"Name {sci_name} in synonyms sheet but not in species sheet",
        )
    return output


def check_with_syns_match(
    species: list[MDDSpecies], syns: list[Syn]
) -> Iterable[Issue]:
    spp_with_syns = yield from generate_match(species, syns)
    for sp in spp_with_syns:
        yield from sp.compare_against_expected()


def check_id_field(species: list[MDDSpecies]) -> Iterable[Issue]:
    species = sorted(species, key=lambda sp: sp.row["id"])
    for mdd_id, group_iter in itertools.groupby(species, lambda sp: sp.row["id"]):
        group = list(group_iter)
        if mdd_id != "" and len(group) != 1:
            description = f"multiple species with id {mdd_id}: {', '.join(sp.row['sciName'] for sp in group)}"
            for sp in group:
                yield sp.make_issue("id", description)
    id_less = [sp for sp in species if not sp.row["id"]]
    if id_less:
        max_id = max(
            int(sp.row["id"])
            for sp in species
            if sp.row["id"] and sp.row["id"].isnumeric()
        )
        for sp in id_less:
            max_id += 1
            yield sp.make_issue("id", "missing MDD id", str(max_id))


def check_unique_col_mapping(
    species: list[MDDSpecies],
    from_col: str,
    to_col: str,
    ignorable_values: Container[str] = frozenset(),
    fallback_columns: Sequence[str] = (),
) -> Iterable[Issue]:
    """Check that all species with the same value of from_col have the same value of to_col."""
    from_to_to_to_sp: dict[str, dict[str, list[MDDSpecies]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for sp in species:
        from_val = sp.row[from_col]  # type: ignore[literal-required]
        for fallback_col in fallback_columns:
            if from_val not in ignorable_values:
                break
            from_val = sp.row[fallback_col]  # type: ignore[literal-required]
        from_to_to_to_sp[from_val][sp.row[to_col]].append(sp)  # type: ignore[literal-required]
    for from_value, to_to_sp in from_to_to_to_sp.items():
        if len(to_to_sp) != 1:
            to_vals = ", ".join(
                f"{to_val!r} ({len(spp)})" for to_val, spp in to_to_sp.items()
            )
            description = f"species with {from_col}={from_value!r} have multiple values for {to_col}: {to_vals}"
            for spp in to_to_sp.values():
                for sp in spp:
                    yield sp.make_issue(to_col, description)


def lint_species(species: list[MDDSpecies]) -> Iterable[Issue]:
    for sp in species:
        yield from sp.lint_standalone()
    yield from check_id_field(species)

    # Should be a 1:1 mapping
    yield from check_unique_col_mapping(species, "order", "phylosort")
    yield from check_unique_col_mapping(species, "phylosort", "order")

    # All species in the same order should be in the same superorder, etc.
    for i, from_col in enumerate(RANKS):
        if i == 0:
            continue
        to_col = RANKS[i - 1]
        fallback_cols = RANKS[i + 1 :]
        yield from check_unique_col_mapping(
            species,
            from_col,
            to_col,
            ignorable_values=("NA", "incertae sedis"),
            fallback_columns=fallback_cols,
        )


def maybe_fix_issues(
    issues: list[Issue], column_to_idx: dict[str, int], dry_run: bool
) -> None:
    def _issue_sort_key(issue: Issue) -> tuple[int, bool, bool]:
        return (
            column_to_idx[issue.mdd_column],
            bool(issue.mdd_value),
            bool(issue.suggested_change),
        )

    fixable_issues = [issue for issue in issues if issue.suggested_change is not None]
    fixable_issues = sorted(fixable_issues, key=_issue_sort_key)
    sheet = get_sheet()
    worksheet = sheet.get_worksheet_by_id(get_options().mdd_species_worksheet_gid)

    for _, group_iter in itertools.groupby(fixable_issues, _issue_sort_key):
        group = list(group_iter)
        sample = group[0]
        header = f"{sample.group_description()} ({len(group)})"
        getinput.print_header(header)
        for issue in group:
            print(issue.describe())
        print(header)
        choice = getinput.choose_one_by_name(
            ["edit", "ask_individually", "skip"],
            allow_empty=False,
            history_key="overall_choice",
        )
        updates_to_make = []
        for diff in group:
            should_edit = False
            match choice:
                case "edit":
                    should_edit = True
                case "ask_individually":
                    print(issue.describe())
                    individual_choice = getinput.choose_one_by_name(
                        ["edit", "skip"],
                        allow_empty=False,
                        history_key="individual_choice",
                    )
                    match individual_choice:
                        case "edit":
                            should_edit = True
            if should_edit:
                updates_to_make.append(
                    gspread.cell.Cell(
                        row=diff.row_idx,
                        col=column_to_idx[diff.mdd_column],
                        value=process_value_for_sheets(diff.suggested_change),  # type: ignore
                    )
                )

        if dry_run:
            print("Make change:", updates_to_make)
        elif updates_to_make:
            done = 0
            print(
                f"Applying {len(updates_to_make)} changes for column {sample.mdd_column}"
            )
            for batch in batched(updates_to_make, 500):
                worksheet.update_cells(batch)
                done += len(batch)
                print(f"Done {done}/{len(updates_to_make)}")
                if len(batch) == 500:
                    time.sleep(30)


def process_value_for_sheets(value: str) -> str | int:
    if value.isdigit():
        return int(value)
    return value


def get_mdd_species(input_csv: str | None = None) -> dict[str, MDDSpecies]:
    if input_csv is None:
        sheet = get_sheet()
        options = get_options()
        worksheet = sheet.get_worksheet_by_id(options.mdd_species_worksheet_gid)
        raw_rows = worksheet.get()
    else:
        with open(input_csv) as f:
            raw_rows = list(csv.reader(f))
    headings = raw_rows[0]
    species = [
        MDDSpecies(row_idx, cast(MDDSpeciesRow, dict(zip(headings, row, strict=False))))
        for row_idx, row in enumerate(raw_rows[1:], start=2)
    ]
    return {sp.row["sciName"]: sp for sp in species}


def run(
    *,
    dry_run: bool = True,
    input_csv: str | None = None,
    syn_sheet_csv: str | None = None,
) -> None:
    options = get_options()
    backup_path = (
        options.data_path / "mdd_checker" / datetime.datetime.now().isoformat()
    )
    backup_path.mkdir(parents=True, exist_ok=True)

    print("downloading MDD names... ")
    if input_csv is None:
        sheet = get_sheet()
        worksheet = sheet.get_worksheet_by_id(options.mdd_species_worksheet_gid)
        raw_rows = worksheet.get()
    else:
        with open(input_csv) as f:
            raw_rows = list(csv.reader(f))
    headings = raw_rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    species = [
        MDDSpecies(row_idx, cast(MDDSpeciesRow, dict(zip(headings, row, strict=False))))
        for row_idx, row in enumerate(raw_rows[1:], start=2)
    ]
    print(f"done, {len(species)} found")

    print("backing up MDD names... ")
    with (backup_path / "mdd_species.csv").open("w") as file:
        writer = csv.writer(file)
        for row in raw_rows:
            writer.writerow(row)
    print(f"done, backup at {backup_path}")

    issues = list(lint_species(species))

    if syn_sheet_csv is not None:
        with open(syn_sheet_csv) as f:
            syn_sheet_rows = list(csv.reader(f))
    else:
        sheet = get_sheet()
        worksheet = sheet.get_worksheet_by_id(options.mdd_worksheet_gid)
        syn_sheet_rows = worksheet.get()
    syn_sheet_headings = syn_sheet_rows[0]
    syns = [
        dict(zip(syn_sheet_headings, row, strict=False)) for row in syn_sheet_rows[1:]
    ]
    issues += check_with_syns_match(species, syns)

    for issue in issues:
        print(issue.describe())

    with (backup_path / "differences.csv").open("w") as f:
        headings = [field.name for field in fields(Issue)]
        diff_writer = csv.DictWriter(f, headings)
        diff_writer.writeheader()
        for issue in issues:
            diff_writer.writerow(
                {heading: getattr(issue, heading) or "" for heading in headings}
            )

    maybe_fix_issues(issues, column_to_idx, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=str, default=None)
    parser.add_argument("--syn-sheet-csv", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    run(
        input_csv=args.input_csv, dry_run=args.dry_run, syn_sheet_csv=args.syn_sheet_csv
    )
