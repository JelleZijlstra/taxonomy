"""Enums for various fields."""

from __future__ import annotations

import enum
from functools import cache
from typing import Annotated


class RequirednessLevel(enum.IntEnum):
    required = 1
    optional = 2
    disallowed = 3


class GrammaticalGender(enum.IntEnum):
    masculine = 0
    feminine = 1
    neuter = 2


class AgeClass(enum.IntEnum):
    extant = 0
    holocene = 1
    fossil = 2
    ichno = 3
    removed = 4  # taxon is removed; should not have any references
    track = 5
    egg = 6
    coprolite = 7
    burrow = 8
    bite_trace = 9
    redirect = 10  # merged into another Taxon
    recently_extinct = 11

    def is_ichno(self) -> bool:
        return self in (
            AgeClass.ichno,
            AgeClass.track,
            AgeClass.egg,
            AgeClass.coprolite,
            AgeClass.burrow,
            AgeClass.bite_trace,
        )

    def get_symbol(self) -> str:
        return {
            self.extant: "",
            self.holocene: "🦴",
            self.recently_extinct: "☠",
            self.fossil: "†",
            self.ichno: "👻",
            self.removed: "!",
            self.track: "👣",
            self.egg: "🥚",
            self.coprolite: "💩",
            self.burrow: "🕳️",
            self.bite_trace: "😋",
            self.redirect: "→",
        }[self]

    def can_have_parent_of_age(self, other: AgeClass) -> bool:
        return other in _get_allowed_parents(self)


_ALLOWED_AGE_PARENTS = {
    AgeClass.recently_extinct: [AgeClass.extant],
    AgeClass.holocene: [AgeClass.recently_extinct],
    AgeClass.fossil: [AgeClass.holocene],
    AgeClass.ichno: [AgeClass.fossil],
    AgeClass.egg: [AgeClass.fossil],
    AgeClass.track: [AgeClass.ichno],
    AgeClass.coprolite: [AgeClass.ichno],
    AgeClass.burrow: [AgeClass.ichno],
    AgeClass.bite_trace: [AgeClass.ichno],
}


@cache
def _get_allowed_parents(age: AgeClass) -> set[AgeClass]:
    parents = _ALLOWED_AGE_PARENTS.get(age, [])
    allowed = {age, *parents}
    for parent in parents:
        allowed |= _get_allowed_parents(parent)
    return allowed


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
    # Name was removed; should not be used, but we keep it around to avoid breaking references.
    removed = 6
    # Merged into another name
    redirect = 7
    # Based on a composite of multiple taxa, and no lectotype.
    composite = 8
    # Based on a hybrid
    hybrid = 9
    # May represent a valid taxon, but name is not available
    unavailable = 10

    def is_base_name(self) -> bool:
        return self in (
            Status.valid,
            Status.nomen_dubium,
            Status.species_inquirenda,
            Status.spurious,
            Status.composite,
            Status.hybrid,
            Status.unavailable,
        )


class NomenclatureStatus(enum.IntEnum):
    available = 1
    nomen_nudum = 2  # Art. 12.1 (before 1931), 13.1 (after 1930)
    fully_suppressed = 3  # by the Commission
    not_based_on_a_generic_name = 4  # for family-group names (cf. Art. 11.7)
    infrasubspecific = 5  # for species-group names (Art. 1.3.4; but see 45.6.4.1)
    # e.g., published in a thesis; see Art. 8. Some more specific statuses below.
    unpublished = 6
    incorrect_subsequent_spelling = 7
    unjustified_emendation = 8  # such names are available (Art. 19.1, 33)
    before_1758 = 9  # Art. 3.2: names published before 1758 are unavailable
    hypothetical_concept = 10  # Art 1.3.1
    teratological = 11  # Art 1.3.2: "teratological specimens as such"
    hybrid_as_such = 12  # Art. 1.3.3: "hybrid specimens as such" (cf. Art. 17.2)
    informal = 13  # Art. 1.3.5: "as means of temporary reference"
    # Art. 1.3.6: "after 1930, for the work of extant animals"; 13.6.2
    work_of_extant = 14
    zoological_formula = 15  # Art. 1.3.7: names like Herrera's "MamXus"
    # Art. 10.7: name not in a Part of the "List of Available Names in Zoology"
    unlisted = 16
    not_latin_alphabet = 17  # Art. 11.2: names must be in the Latin alphabet
    # Art. 11.4: author must consistently use binominal nomenclature
    inconsistently_binominal = 18
    not_used_as_valid = 19  # Art. 11.5, 11.6
    not_used_as_genus_plural = 20  # Art. 11.7.1.2
    based_on_a_suppressed_name = 21  # Art. 11.7.1.5, 39
    not_published_with_a_generic_name = 22  # Art. 11.9.3
    multiple_words = 23  # Art. 11.9.4
    # Art. 13.3: genus-group name after 1930 (but not ichnotaxa); Art. 16.2 for family-group names after 1999; Art. 16.4 for species-group names after 1999
    no_type_specified = 24
    # Art. 14: anonymously published names are unavailable after 1950
    anonymous_authorship = 25
    conditional = 26  # Art. 15
    # Art. 15.2: after 1960, "variety" or "form" excludes the name (cf. infrasubspecific)
    variety_or_form = 27
    # Art. 16: names published after 1999 must be explicitly new
    not_explicitly_new = 28
    # Art. 34: rank change for family-group name and gender agreement for species-group name (first one moved to reranking)
    mandatory_change = 29
    # Art. 20: names in -ites, -ytes, -ithes for fossils may not be available
    ites_name = 30
    # names based on hybrids are available, but do not compete in priority (Art. 23.8)
    # Deprecated as this is redundant with setting 'status' to 'hybrid'
    hybrid_name = 31
    # Art. 23.12: name rejected under Art. 23b in the 1961-1973 Code
    art_13_nomen_oblitum = 32
    variant = 33  # probably an ISS (7), but may be UE (8)
    justified_emendation = 34  # Art. 32.5: correction of incorrect original spellings
    preoccupied = 35  # junior homonym (still available)
    # Art. 39: family-group names based on junior homonyms must be replaced
    based_on_homonym = 39
    partially_suppressed = 40  # suppressed for Priority but not Homonymy
    # Nomen novum or substitution for another name. Such names are available, but using a different status makes it easier to keep track of their types.
    nomen_novum = 41
    # if there are multiple variants in the original description
    incorrect_original_spelling = 42
    type_not_treated_as_valid = 43  # Art. 11.7.1.1: genus name must be treated as valid
    # subset of mandatory change: family-group name changed to a new rank
    reranking = 44
    # usage of a name (e.g., a misidentification) that does not create a new name
    subsequent_usage = 45
    not_intended_as_a_scientific_name = 46  # e.g., a vernacular name
    collective_group = 47
    # Art. 11.8: a genus-group name must be a nominative singular noun.
    not_nominative_singular = 48
    # Justified emendation, available with its original author and date.
    as_emended = 49
    # Regarded as unavailable by fiat
    rejected_by_fiat = 50
    unpublished_thesis = 51  # unpublished because named in an unpublished thesis
    # unpublished because named in an electronic-only work without an LSID
    unpublished_electronic = 52
    # like the above, but expected to be published in print form. We mostly treat these as available.
    unpublished_pending = 53
    # unpublished because named in electronic supplementary material only
    unpublished_supplement = 54
    name_combination = 55
    misidentification = 56
    # Art. 80.7: Names published in a work on the Official Index are treated as unavailable
    placed_on_index = 57

    def requires_type(self) -> bool:
        """Whether a name of this status should have a type designated."""
        return self in REQUIRES_TYPE

    def is_variant(self) -> bool:
        return self in {
            NomenclatureStatus.unjustified_emendation,
            NomenclatureStatus.justified_emendation,
            NomenclatureStatus.variant,
            NomenclatureStatus.incorrect_original_spelling,
            NomenclatureStatus.incorrect_subsequent_spelling,
            NomenclatureStatus.subsequent_usage,
            NomenclatureStatus.name_combination,
            NomenclatureStatus.misidentification,
            NomenclatureStatus.reranking,
        }

    def can_preoccupy(self) -> bool:
        """Whether a name of this type can preoccupy another name."""
        return self in {
            NomenclatureStatus.available,
            NomenclatureStatus.unpublished_pending,
            NomenclatureStatus.unjustified_emendation,
            NomenclatureStatus.hybrid_name,
            NomenclatureStatus.variant,
            NomenclatureStatus.justified_emendation,
            NomenclatureStatus.preoccupied,
            NomenclatureStatus.partially_suppressed,
            NomenclatureStatus.nomen_novum,
            NomenclatureStatus.as_emended,
        }

    def requires_name_complex(self) -> bool:
        return self not in {
            NomenclatureStatus.nomen_nudum,
            NomenclatureStatus.incorrect_subsequent_spelling,
            NomenclatureStatus.incorrect_original_spelling,
            NomenclatureStatus.inconsistently_binominal,
            NomenclatureStatus.placed_on_index,
            NomenclatureStatus.not_latin_alphabet,
            NomenclatureStatus.not_intended_as_a_scientific_name,
            NomenclatureStatus.zoological_formula,
            NomenclatureStatus.informal,
            NomenclatureStatus.subsequent_usage,
            NomenclatureStatus.misidentification,
            NomenclatureStatus.name_combination,
            NomenclatureStatus.not_nominative_singular,
            NomenclatureStatus.before_1758,
            NomenclatureStatus.not_used_as_valid,
            NomenclatureStatus.not_published_with_a_generic_name,
            NomenclatureStatus.unpublished,
            NomenclatureStatus.unpublished_thesis,
            NomenclatureStatus.unpublished_electronic,
            NomenclatureStatus.unpublished_supplement,
            NomenclatureStatus.unpublished_pending,
        }

    def requires_original_parent(self) -> bool:
        return self not in {
            NomenclatureStatus.not_intended_as_a_scientific_name,
            NomenclatureStatus.inconsistently_binominal,
            NomenclatureStatus.placed_on_index,
            NomenclatureStatus.not_published_with_a_generic_name,
            NomenclatureStatus.before_1758,
            NomenclatureStatus.informal,
        }

    def permissive_corrected_original_name(self) -> bool:
        """Corrected original names with one of these statuses are not strictly checked."""
        return self in {
            NomenclatureStatus.not_published_with_a_generic_name,
            NomenclatureStatus.informal,
            NomenclatureStatus.not_intended_as_a_scientific_name,
            NomenclatureStatus.not_nominative_singular,
            NomenclatureStatus.not_based_on_a_generic_name,
        }

    @classmethod
    def hierarchy(cls) -> list[list[NomenclatureStatus]]:
        """Hierarchy of the severity of various problems with a name.

        Listed from most to least severe. If multiple conditions apply to a name (e.g., it is both
        an infrasubspecific name and published in an inconsistently binominal work), the most severe
        defect should be used in the nomenclature_status field.

        """
        return [
            # The Commission's word is final.
            [cls.fully_suppressed, cls.partially_suppressed],
            # The Commission's implicit word.
            [cls.unlisted],
            # If the work is invalid, we don't need to worry about the exact status of names.
            [cls.placed_on_index, cls.before_1758, cls.inconsistently_binominal],
            [cls.subsequent_usage],
            # Clear problems with the name itself.
            [
                cls.not_based_on_a_generic_name,
                cls.hypothetical_concept,
                cls.teratological,
                cls.hybrid_as_such,
                cls.informal,
                cls.work_of_extant,
                cls.not_explicitly_new,
                cls.zoological_formula,
                cls.not_latin_alphabet,
                cls.not_used_as_genus_plural,
                cls.not_published_with_a_generic_name,
                cls.multiple_words,
                cls.no_type_specified,
                cls.anonymous_authorship,
                cls.conditional,
                cls.ites_name,
                cls.based_on_homonym,
                cls.based_on_a_suppressed_name,
                cls.type_not_treated_as_valid,
                cls.name_combination,
                cls.not_intended_as_a_scientific_name,
                cls.not_nominative_singular,
                cls.rejected_by_fiat,
            ],
            # Spelling issues that produce unavailable names.
            [cls.incorrect_subsequent_spelling, cls.incorrect_original_spelling],
            [cls.misidentification],
            [
                cls.unpublished_thesis,
                cls.unpublished_electronic,
                cls.unpublished_supplement,
                cls.unpublished,
            ],
            [cls.nomen_nudum],
            # Potentially available under some circumstances
            [cls.variety_or_form],
            [cls.infrasubspecific],
            [cls.not_used_as_valid],
            # From here on, names are available.
            [
                cls.unjustified_emendation,
                cls.justified_emendation,
                cls.mandatory_change,
                cls.art_13_nomen_oblitum,
                cls.reranking,
            ],
            # Should be replaced with ISS or UE if possible.
            [cls.variant],
            [cls.hybrid_name],
            [cls.nomen_novum],
            [cls.collective_group],
            [cls.as_emended],
            [cls.preoccupied],
            [cls.unpublished_pending],
            [cls.available],
        ]


REQUIRES_TYPE = {
    NomenclatureStatus.available,
    NomenclatureStatus.unpublished_pending,
    NomenclatureStatus.hybrid_name,
    NomenclatureStatus.art_13_nomen_oblitum,
    NomenclatureStatus.preoccupied,
    NomenclatureStatus.reranking,
    NomenclatureStatus.as_emended,
}


class EmendationJustification(enum.IntEnum):
    # Art. 32.5.1: evidence of inadvertent error in the original publication
    inadvertent_error = 1
    # Art. 32.5.2: removal of diacritics or other marks
    removal_of_mark = 2
    # Art. 32.5.3: incorrectly formed family-group name
    incorrect_family_group_name = 3
    # Art. 33.2.3.1: emendation is in prevailing usage
    prevailing_usage = 4
    # The emendation has been validated by the Commission
    conserved_by_the_commission = 5


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
    other_subgeneric = 19
    genus = 20
    division = 22
    infratribe = 24
    subtribe = 25
    tribe = 30
    infrafamily = 34
    subfamily = 35
    family = 40
    superfamily = 45
    hyperfamily = 47
    other_family = 48
    unranked_family = 49
    parvorder = 50
    infraorder = 55
    suborder = 60
    semiorder = 64
    order = 65
    superorder = 70
    magnorder = 72
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
    other = 215  # should be used only for original rank
    variety = 216
    form = 217
    infrasubspecific = 218
    synonym = (
        219  # for original rank; if the name was not treated as valid when created
    )
    aberratio = 220  # always infrasubspecific, Art. 45.6.2
    morph = 221  # always infrasubspecific, Art. 45.6.2
    natio = 222
    subvariety = 223
    other_species = 224
    informal_species = 225
    mutation = 26
    race = 27

    synonym_species = 230
    synonym_genus = 231
    synonym_family = 232
    synonym_high = 233

    @property
    def is_synonym(self) -> bool:
        return self in SYNONYM_RANKS

    @property
    def display_name(self) -> str:
        match self:
            case Rank.class_:
                return "class"
            case Rank.species_group:
                return "species group"
            case Rank.synonym_species:
                return "synonym (species group)"
            case Rank.synonym_genus:
                return "synonym (genus group)"
            case Rank.synonym_family:
                return "synonym (family group)"
            case Rank.synonym_high:
                return "synonym (higher group)"
            case Rank.other_family:
                return "other family-group rank"
            case Rank.other_subgeneric:
                return "other subgeneric rank"
            case Rank.unranked_family:
                return "family-group name without explicit rank"
            case Rank.other_species:
                return "other species-group rank"
            case Rank.informal_species:
                return "informal species-group name"
        return self.name

    @property
    def needs_textual_rank(self) -> bool:
        return self in NEED_TEXTUAL_RANK

    @property
    def comparison_value(self) -> int:
        if self.value > Rank.root:
            return -1
        if self.value in (Rank.unranked_family, Rank.other_family):
            return Rank.infratribe.value - 1
        return self.value

    @property
    def is_uncomparable(self) -> bool:
        return (
            self.is_synonym
            or self.needs_textual_rank
            or self in {Rank.informal, Rank.unranked}
        )

    @property
    def is_allowed_for_taxon(self) -> bool:
        if self is Rank.unranked:
            return True
        if self > Rank.root:
            return False
        return self not in {
            Rank.other_family,
            Rank.other_subgeneric,
            Rank.unranked_family,
            Rank.infrafamily,
            Rank.hyperfamily,
        }


SYNONYM_RANKS = {
    Rank.synonym_species,
    Rank.synonym_genus,
    Rank.synonym_family,
    Rank.synonym_high,
    Rank.synonym,
}
NEED_TEXTUAL_RANK = {
    Rank.other,
    Rank.other_family,
    Rank.other_subgeneric,
    Rank.other_species,
}


class RegionKind(enum.IntEnum):
    continent = 0
    country = 1
    # first-level subdivision without a specific kind, e.g. a Russian oblast
    subnational = 2
    planet = 3
    other = 4  # miscellaneous informal subdivisions of countries
    county = 5
    island = 6  # or island group
    state = 7
    province = 8
    department = 9
    region = 10  # things formally named "region", e.g. in Italy
    canton = 11
    prefecture = 12
    territory = 13
    supranational = 14  # Supranational regions from the UN geoscheme
    sea = 15


class PeriodSystem(enum.IntEnum):
    gts = 1  # The Geologic Time Scale
    nalma = 2  # North American land mammal age system
    elma = 3  # European land mammal age system, plus MN and MP zones
    alma = 4  # Asian land mammal age system
    salma = 5  # South American land mammal age system
    _lithostratigraphy = 6  # lithostratigraphical units, like formations (deprecated)
    aulma = 7  # Australian land mammal age system
    local_biostratigraphy = 8  # local biostratigraphic zonation
    aflma = 9  # African land mammal age system

    def is_continuous(self) -> bool:
        return self not in (PeriodSystem.salma, PeriodSystem.aulma, PeriodSystem.aflma)


class PeriodRank(enum.IntEnum):
    age = 5
    epoch = 6
    period = 7
    era = 8
    eon = 9
    _bed = 20  # deprecated
    _member = 21  # deprecated
    _formation = 22  # deprecated
    _group = 23  # deprecated
    _supergroup = 24  # deprecated
    other_chronostratigraphy = 28
    subage = 29  # e.g., the Lysitean
    biozone = 30  # e.g., Pu1
    _subgroup = 31  # deprecated
    zonation = 32  # "Period" encompassing a whole zonation system.


class StratigraphicUnitRank(enum.IntEnum):
    bed = 20
    member = 21
    formation = 22
    group = 23
    supergroup = 24
    other_lithostratigraphy = 27
    subgroup = 31


SYSTEM_TO_ALLOWED_RANKS = {
    PeriodSystem.gts: {
        PeriodRank.age,
        PeriodRank.epoch,
        PeriodRank.period,
        PeriodRank.era,
        PeriodRank.eon,
        PeriodRank.other_chronostratigraphy,
    },
    PeriodSystem.nalma: {PeriodRank.age, PeriodRank.subage, PeriodRank.biozone},
    PeriodSystem.elma: {PeriodRank.age, PeriodRank.biozone},
    PeriodSystem.alma: {PeriodRank.age, PeriodRank.subage},
    PeriodSystem.salma: {PeriodRank.age, PeriodRank.subage},
    PeriodSystem.aulma: {PeriodRank.age},
    PeriodSystem.aflma: {PeriodRank.age},
    PeriodSystem.local_biostratigraphy: {
        PeriodRank.zonation,
        PeriodRank.age,
        PeriodRank.subage,
        PeriodRank.biozone,
    },
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
    japanese = 13
    thai = 14
    korean = 15
    hungarian = 16
    vietnamese = 17
    italian = 18
    dutch = 19
    papiamento = 20
    catalan = 21
    modern_greek = 22
    hebrew = 23
    aramaic = 24
    sanskrit = 25
    hindi = 26
    old_english = 27
    bulgarian = 28
    albanian = 29
    frisian = 30
    norwegian = 31
    danish = 32
    swedish = 33
    finnish = 34


SOURCE_LANGUAGE_SYNONYMS = {
    "eng": SourceLanguage.english,
    "ger": SourceLanguage.german,
    "fre": SourceLanguage.french,
    "spa": SourceLanguage.spanish,
    "rus": SourceLanguage.russian,
    "chi": SourceLanguage.chinese,
    "ita": SourceLanguage.italian,
    "jpn": SourceLanguage.japanese,
    "dut": SourceLanguage.dutch,
    "lat": SourceLanguage.latin,
    "gre": SourceLanguage.modern_greek,
    "grc": SourceLanguage.greek,
    "pap": SourceLanguage.papiamento,
    "cat": SourceLanguage.catalan,
}


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
    # Etymology unknown, but stem is "obvious" (e.g., _Madataeus_)
    unknown_obvious_stem = 13
    # Stem explicitly set, usually in connection with an otherwise preoccupied
    # family-group name.
    stem_expressly_set = 14
    # Assumed based on form of the name; should be confirmed against the
    # original citation.
    assumed = 15


class SpeciesNameKind(enum.IntEnum):
    adjective = 1  # Latin adjective, Art. 11.9.1.1
    noun_in_apposition = 2  # Art 11.9.1.2
    genitive = 3  # genitive Latin noun, Art 11.9.1.3
    # adjective used as a noun, Art. 11.9.1.4 (probably very rare in mammals)
    genitive_adjective = 4
    non_latin = 5  # not a Latin word, treated as indeclinable (cf. Art. 31.2.3)
    ambiguous_noun = 6  # noun in apposition under Art. 31.2.2

    # Article 31.1.2 patronyms
    patronym_masculine = 7  # -i patronym
    patronym_feminine = 8  # -ae patronym
    patronym_masculine_plural = 9  # -orum patronym
    patronym_feminine_plural = 10  # -arum patronym
    patronym_latin = 11  # patronym formed from a Latin name (Art. 31.1.1)

    # no etymology given and no etymology apparent; treated as invariant by default
    unknown = 12

    def is_patronym(self) -> bool:
        return self in {
            SpeciesNameKind.patronym_masculine,
            SpeciesNameKind.patronym_masculine_plural,
            SpeciesNameKind.patronym_feminine,
            SpeciesNameKind.patronym_feminine_plural,
            SpeciesNameKind.patronym_latin,
        }

    def is_single_complex(self) -> bool:
        return self not in {SpeciesNameKind.adjective, SpeciesNameKind.ambiguous_noun}


class TypeSpeciesDesignation(enum.IntEnum):
    # in order, Art. 68.1

    # Art. 68.2. Before 1931, "gen. n., sp. n." indicates an original designation. Also for species named typus, typic-.
    original_designation = 1
    monotypy = 2  # Art. 68.3
    absolute_tautonymy = 3  # Art. 68.4
    linnaean_tautonymy = 4  # Art. 68.5
    subsequent_monotypy = 5  # Art. 69.3, only for genera originally without species
    subsequent_designation = 6  # Art. 69.1
    # names of nomina nova and emendations have the same type, Art. 67.8, 69.2.3
    implicit = 7
    # if the type was misidentified, you can do whatever you want (Art. 70.3)
    misidentification = 8
    designated_by_the_commission = 9  # type explicitly designated by the Commission
    undesignated = 10  # no type was ever designated

    def requires_tag(self) -> bool:
        return self in {
            TypeSpeciesDesignation.subsequent_designation,
            TypeSpeciesDesignation.designated_by_the_commission,
            TypeSpeciesDesignation.undesignated,
        }


class SpeciesGroupType(enum.IntEnum):
    holotype = 101
    lectotype = 102
    neotype = 103
    syntypes = 104
    # no type has been designated; unknown whether there was a holotype or syntype
    nonexistent = 105


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
    larva = 5


class SpecimenOrgan(enum.IntEnum):
    # parts of specimens that are commonly preserved
    skin = 1
    skull = 2
    postcranial_skeleton = 3
    mandible = 4
    tooth = 5
    in_alcohol = 6
    other = 7
    maxilla = 8
    antler = 9
    humerus = 10
    femur = 11
    ulna = 12
    radius = 13
    tibia = 14
    fibula = 15
    vertebra = 16
    pelvis = 17
    dentary = 18
    caudal_tube = 19
    osteoderm = 20
    coracoid = 21
    whole_animal = 22
    egg = 23
    horn_core = 24  # of bovids, and other horns of ungulates
    frontlet = 25
    petrosal = 26
    tarsometatarsus = 27
    scapula = 28
    carpal = 29
    hyoid = 30
    rib = 31
    manus = 32
    pes = 33
    astragalus = 34
    calcaneum = 35
    clavicle = 36
    navicular = 37
    sternum = 38
    baculum = 39
    tissue_sample = 40
    shell = 41  # of a turtle
    skeleton = 42  # full skeleton
    limb = 43  # fore or hind, ideally should specify bones. Includes pterosaur/bird wings and ichthyosaur fins.
    girdle = 44  # pelvic or pectoral, ideally should specify bones
    scapulocoracoid = 45
    carpometacarpal = 46
    patella = 47
    ilium = 48
    ischium = 49
    pubis = 50
    metacarpal = 51
    metatarsal = 52
    phalanx_manus = 53
    phalanx_pes = 54
    premaxilla = 55
    metapodial = 56
    tibiotarsus = 57
    furcula = 58
    phalanx = 59
    interclavicle = 61
    gastralia = 62
    prepubis = 63
    predentary = 64
    palate = 65


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
    structured_quote = 9
    type_species = 10
    homonymy = 11
    spelling = 12
    authorship = 13
    automatic_change = 14
    removal = 15
    contents = 16
    definition = 17
    removed = 18


class ArticleType(enum.IntEnum):
    ERROR = 0
    CHAPTER = 1  # published simultaneously with its enclosing work
    BOOK = 2
    THESIS = 3  # kind of degree in "series", university in "publisher"
    WEB = 5
    MISCELLANEOUS = 6
    SUPPLEMENT = 8  # ID of target in "parent", kind of supplement in "title"
    JOURNAL = 9
    REDIRECT = 10
    PART = 11  # separately published part of a larger work


class ArticleKind(enum.IntEnum):
    electronic = 1  # have an electronic copy
    physical = 2  # have a physical copy
    no_copy = 3  # don't actually have a copy
    part = 4  # the article is part of some other article
    redirect = 5  # it's a redirect
    removed = 6  # it has been removed
    reference = 7  # reference to an online resource
    alternative_version = 11  # e.g., in press version. Backed by a physical file but should not have references.

    def is_electronic(self) -> bool:
        """Is it backed by an electronic file?"""
        return self in (ArticleKind.electronic, ArticleKind.alternative_version)


class ArticleCommentKind(enum.IntEnum):
    dating = 1
    contents = 2
    authorship = 3
    location = 4  # e.g., where to find it in its parent
    other = 5


class NamingConvention(enum.IntEnum):
    unspecified = 1
    pinyin = 2
    japanese = 3
    dutch = 4  # treatment of tussenvoegsels
    burmese = 5  # no family names
    spanish = 6  # double surnames
    ancient = 7  # mononyms
    organization = 8
    hungarian = 9  # family name usually comes first
    vietnamese = 10
    german = 11  # treatment of von
    general = 12  # just first name and last name
    russian = 13  # allow Cyrillic
    turkish = 14  # for the dotted and dotless i
    chinese = 15  # Chinese-style names, but not in pinyin
    korean = 16
    mongolian = 17
    ukrainian = 18
    other = 19  # unusual language; we don't check the name for validity
    english = 20
    english_peer = 21
    french = 22
    italian = 23
    portuguese = 24


class PersonType(enum.IntEnum):
    unchecked = 1
    checked = 2
    soft_redirect = 3
    hard_redirect = 4
    deleted = 5
    alias = 6


class FillDataLevel(enum.IntEnum):
    needs_basic_data = 1  # missing data and no data from original
    missing_required_fields = 2  # missing crucial required fields
    missing_detail = 3
    incomplete_detail = 4
    incomplete_derived_tags = 5
    no_data_from_original = 6
    nothing_needed = 7

    @classmethod
    def max_level(cls) -> FillDataLevel:
        return cls.no_data_from_original


class NameDataLevel(enum.IntEnum):
    missing_crucial_fields = 1
    missing_required_fields = 2
    missing_details_tags = 3
    missing_derived_tags = 4
    nothing_needed = 5


class OriginalCitationDataLevel(enum.IntEnum):
    no_citation = 1
    no_data = 2
    some_data = 3
    all_required_data = 4


class DateSource(enum.IntEnum):
    internal = 1  # specified inside the publication itself
    external = 2  # external source discussing date
    # Map to similarly named fields in the CrossRef API
    # https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_
    doi_published_print = 3
    doi_published_online = 4
    doi_published_other = 5
    doi_published = 6
    decision = 7  # decision when there are conflicting tags


class StringKind(enum.IntEnum):
    markdown = 1  # parsed as Markdown, supports {} references
    managed = 2  # short string in a fixed format
    regex = 3  # regular expression
    url = 4  # URL


type Markdown = Annotated[str, StringKind.markdown]
type Managed = Annotated[str, StringKind.managed]
type Regex = Annotated[str, StringKind.regex]
type URL = Annotated[str, StringKind.url]
