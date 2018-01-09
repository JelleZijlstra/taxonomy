"""Enums for various fields."""

import enum


class Gender(enum.IntEnum):
    masculine = 0
    feminine = 1
    neuter = 2


class Age(enum.IntEnum):
    extant = 0
    holocene = 1
    fossil = 2
    ichno = 3


class Status(enum.IntEnum):
    valid = 0
    synonym = 1
    # Treated as synonyms, but of uncertain identity. This status is being deprecated.
    dubious = 2
    # Names that have been shown to be based on unidentifiable material. This status should be used
    # if somebody who has studied the name has made the explicit assessment that it cannot be
    # identified to species (or higher) level.
    nomen_dubium = 3
    # Names that have not been explicitly shown to be invalid, but that seem to be of doubtful
    # validity based on a literature review. This status should generally be used for old taxa
    # that are not or only cursorily listed in recent literature on the group. For example,
    # _Galerix magnus_ was named by Pomel (1848) and practically never again discussed in the
    # literature until Zijlstra & Flynn (2015). It should have been tagged as "species inquirenda"
    # until the comments by Zijlstra & Flynn (2015) moved it to "nomen_dubium" status.
    # The distinction drawn here between nomen dubium and species inquirenda is to my knowledge
    # original. The motivation is that for old names like _Galerix magnus_ that have never been
    # explicitly synonymized, it is misleading to list them as valid species, but it is also not
    # justified to list them as synonyms or nomina dubia when no specialist in the group has made
    # that assessment.
    # All biodiversity estimates should omit nomina dubia and species inquirendae.
    species_inquirenda = 4
    # Never published or used in a published work. I keep these in the database only in case I was
    # wrong and they are real names.
    spurious = 5


class NomenclatureStatus(enum.IntEnum):
    available = 1
    nomen_nudum = 2  # Art. 12.1 (before 1931), 13.1 (after 1930)
    suppressed = 3  # by the Commission
    not_based_on_a_generic_name = 4  # for family-group names (cf. Art. 11.7)
    infrasubspecific = 5  # for species-group names (Art. 1.3.4; but see 45.6.4.1)
    unpublished = 6  # e.g., published in a thesis; see Art. 8
    incorrect_subsequent_spelling = 7
    unjustified_emendation = 8  # such names are available (Art. 19.1, 33)
    before_1758 = 9  # Art. 3.2: names published before 1758 are unavailable
    hypothetical_concept = 10  # Art 1.3.1
    teratological = 11  # Art 1.3.2: "teratological specimens as such"
    hybrid_as_such = 12  # Art. 1.3.3: "hybrid specimens as such" (cf. Art. 17.2)
    informal = 13  # Art. 1.3.5: "as means of temporary reference"
    work_of_extant = 14  # Art. 1.3.6: "after 1930, for the work of extant animals"; 13.6.2
    zoological_formula = 15  # Art. 1.3.7: names like Herrera's "MamXus"
    unlisted = 16  # Art. 10.7: name not in a Part of the "List of Available Names in Zoology"
    not_latin_alphabet = 17  # Art. 11.2: names must be in the Latin alphabet
    inconsistently_binominal = 18  # Art. 11.4: author must consistently use binominal nomenclature
    not_used_as_valid = 19  # Art. 11.5, 11.6
    not_used_as_genus_plural = 20  # Art. 11.7.1.2
    based_on_a_suppressed_name = 21  # Art. 11.7.1.5, 39
    not_published_with_a_generic_name = 22  # Art. 11.9.3
    multiple_words = 23  # Art. 11.9.4
    no_type_specified = 24  # Art. 13.3: genus-group name after 1930 (but not ichnotaxa); Art. 16.2 for family-group names after 1999; Art. 16.4 for species-group names after 1999
    anonymous_authorship = 25  # Art. 14: anonymously published names are unavailable after 1950
    conditional = 26  # Art. 15
    variety_or_form = 27  # Art. 15.2: after 1960, "variety" or "form" excludes the name (cf. infrasubspecific)
    not_explicitly_new = 28  # Art. 16: names published after 1999 must be explicitly new
    mandatory_change = 29  # Art. 34: rank change for family-group name and gender agreement for species-group name
    ites_name = 30  # Art. 20: names in -ites, -ytes, -ithes for fossils may not be available
    hybrid_name = 31  # names based on hybrids are available, but do not compete in priority (Art. 23.8)
    art_13_nomen_oblitum = 32  # Art. 23.12: name rejected under Art. 23b in the 1961-1973 Code
    assumed_incorrect = 33  # probably an ISS (7), but may be UE (8)
    justified_emendation = 34  # Art. 32.5: correction of incorrect original spellings
    preoccupied = 35  # junior homonym (still available)
    based_on_homonym = 39  # Art. 39: family-group names based on junior homonyms must be replaced

    def requires_type(self) -> bool:
        return self in {
            NomenclatureStatus.available,
            NomenclatureStatus.hybrid_name,
            NomenclatureStatus.art_13_nomen_oblitum,
            NomenclatureStatus.preoccupied,
        }


class Group(enum.IntEnum):
    species = 0
    genus = 1
    family = 2
    high = 3


class Rank(enum.IntEnum):
    subspecies = 0
    species = 5
    species_group = 10
    subgenus = 15
    genus = 20
    division = 22
    infratribe = 24
    subtribe = 25
    tribe = 30
    subfamily = 35
    family = 40
    superfamily = 45
    hyperfamily = 47
    parvorder = 50
    infraorder = 55
    suborder = 60
    order = 65
    superorder = 70
    subcohort = 75
    cohort = 80
    supercohort = 85
    infraclass = 90
    subclass = 95
    class_ = 100
    superclass = 105
    infraphylum = 110
    subphylum = 115
    phylum = 120
    superphylum = 125
    infrakingdom = 130
    subkingdom = 135
    kingdom = 140
    superkingdom = 145
    domain = 150
    root = 200
    unranked = 205
    informal = 210


class RegionKind(enum.IntEnum):
    continent = 0
    country = 1
    subnational = 2
    planet = 3


class PeriodSystem(enum.IntEnum):
    mn_zone = 0  # MN zones (European Neogene)
    mp_zone = 1  # MP zones (European Paleogene)
    nalma = 2  # North American Land Mammal Age (Campanian-Recent)
    salma = 3  # South American Land Mammal Age (Cenozoic)
    alma = 4  # Asian Land Mammal Age (Cenozoic)
    age = 5
    epoch = 6
    period = 7
    era = 8
    eon = 9
    local_unit = 10  # Miscellaneous local units
    bed = 20
    member = 21
    formation = 22
    group = 23
    supergroup = 24
    other_stratigraphy = 25

    def is_stratigraphy(self) -> bool:
        return self in {
            PeriodSystem.bed, PeriodSystem.member, PeriodSystem.formation, PeriodSystem.group, PeriodSystem.supergroup,
            PeriodSystem.other_stratigraphy,
        }

    def is_chronology(self) -> bool:
        return self.is_biochronology() or self.is_geochronology() or self == PeriodSystem.local_unit

    def is_biochronology(self) -> bool:
        return self in {
            PeriodSystem.mn_zone, PeriodSystem.mp_zone, PeriodSystem.nalma, PeriodSystem.salma, PeriodSystem.alma,
        }

    def is_geochronology(self) -> bool:
        return self in {
            PeriodSystem.age, PeriodSystem.epoch, PeriodSystem.period, PeriodSystem.era, PeriodSystem.eon,
        }


class OccurrenceStatus(enum.IntEnum):
    valid = 0
    rejected = 1  # formerly recorded somewhere, but occurrence now rejected
    occurrence_dubious = 2  # dubious that the species came from this locality
    introduced = 3  # introduced by humans
    extirpated = 4  # occurred during the Holocene but now extirpated
    vagrant = 5  # occasionally occurs but not a normal component of the fauna
    classification_dubious = 6  # dubious that the species is correctly classified


class SourceLanguage(enum.IntEnum):
    latin = 1
    greek = 2

    # This is for names based on indigenous languages, which are now common. Ideally the exact language
    # should be specified, but often the etymology is not specific or language names are poorly established.
    other = 3

    english = 4
    french = 5
    german = 6
    spanish = 7
    portuguese = 8
    russian = 9
    arabic = 10
    chinese = 11
    mongolian = 12


class GenderArticle(enum.IntEnum):
    art30_1_1 = 1  # Latin dictionary word
    art30_1_2 = 2  # Greek dictionary word
    art30_1_3 = 3  # Greek with Latinized ending
    art30_1_4_2 = 4  # common gender, defaulting to masculine
    art30_1_4_3 = 5  # -ops is masculine
    art30_1_4_4 = 6  # -ites, -oides, -ides, -odes, or -istes defaults to masculine
    art30_1_4_5 = 7  # Latin with adjusted ending
    art30_2_1 = 8  # gendered word from modern European language
    art30_2_2 = 9  # expressly specified
    art30_2_3 = 10  # indicated by adjectival species name
    art30_2_4 = 11  # default if unspecified and non-Western
    # Names that are incorrectly transliterated from Greek, but that don't fall under Art. 30.1.3
    # (for example, -merix instead of -meryx). The Code doesn't provide explicit guidance for such
    # names, but I interpret them as having the gender of their Greek root. The question is usually
    # academic because these names tend to be incorrect subsequent spellings.
    bad_transliteration = 12


class SpeciesNameKind(enum.IntEnum):
    adjective = 1  # Latin adjective, Art. 11.9.1.1
    noun_in_apposition = 2  # Art 11.9.1.2
    genitive = 3  # genitive Latin noun, Art 11.9.1.3
    genitive_adjective = 4  # adjective used as a noun, Art. 11.9.1.4 (probably very rare in mammals)
    non_latin = 5  # not a Latin word, treated as indeclinable (cf. Art. 31.2.3)
    ambiguous_noun = 6  # noun in apposition under Art. 31.2.2

    # Article 31.1.2 patronyms
    patronym_masculine = 7  # -i patronym
    patronym_feminine = 8 # -ae patronym
    patronym_masculine_plural = 9  # -orum patronym
    patronym_feminine_plural = 10  # -arum patronym
    patronym_latin = 11  # patronym formed from a Latin name (Art. 31.1.1)


class TypeSpeciesDesignation(enum.IntEnum):
    # in order, Art. 68.1
    original_designation = 1  # Art. 68.2. Before 1931, "gen. n., sp. n." indicates an original designation. Also for species named typus, typic-.
    monotypy = 2  # Art. 68.3
    absolute_tautonymy = 3  # Art. 68.4
    linnaean_tautonymy = 4  # Art. 68.5
    subsequent_monotypy = 5  # Art. 69.3, only for genera originally without species
    subsequent_designation = 6  # Art. 69.1
    implicit = 7  # names of nomina nova and emendations have the same type, Art. 67.8, 69.2.3
    misidentification = 8  # if the type was misidentified, you can do whatever you want (Art. 70.3)
    designated_by_the_commission = 9  # type explicitly designated by the Commission


class SpeciesGroupType(enum.IntEnum):
    holotype = 101
    lectotype = 102
    neotype = 103
    syntypes = 104
    nonexistent = 105  # no type has been designated


class SpecimenGender(enum.IntEnum):
    male = 1
    female = 2
    hermaphrodite = 3
    unknown = 4


class SpecimenAge(enum.IntEnum):
    embryo = 1
    juvenile = 2
    subadult = 3
    adult = 4


class Organ(enum.IntEnum):
    # parts of specimens that are commonly preserved
    skin = 1
    skull = 2
    postcranial_skeleton = 3
    mandible = 4
    tooth = 5
    in_alcohol = 6
    other = 7


class AltitudeUnit(enum.IntEnum):
    m = 1
    ft = 2


class CommentKind(enum.IntEnum):
    taxonomy = 1
    nomenclature = 2
    type_locality = 3
    type_specimen = 4
    availability = 5
    distribution = 6
    etymology = 7
    other = 8
