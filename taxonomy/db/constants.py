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
    # treated as synonyms but of uncertain identity
    dubious = 2
    # names treated like valid names but that do not represent definable taxa
    nomen_dubium = 3


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
