import enum
from collections.abc import Container, Iterable
from dataclasses import dataclass
from typing import assert_never

import clirm

from taxonomy.db import models
from taxonomy.db.constants import (
    AgeClass,
    ArticleType,
    Group,
    NomenclatureStatus,
    Rank,
    SpeciesGroupType,
    Status,
    TypeSpeciesDesignation,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.collection import (
    LOST_COLLECTION,
    MULTIPLE_COLLECTION,
    UNTRACED_COLLECTION,
)

from .name import (
    CONFLICTING_TYPE_DATA_FIELDS,
    ETYMOLOGY_TAGS,
    SPECIMEN_TAGS,
    ExperimentalLintCondition,
    Name,
    TypeTag,
    TypeTagCons,
    get_tags_from_original_citation,
)

# Source details are excluded from automatic derived-data absence assertions.
DETAIL_TAGS = (TypeTag.EtymologyDetail, TypeTag.LocationDetail, TypeTag.SpecimenDetail)


class Requiredness(enum.Enum):
    # Lint error if this exists
    disallowed = enum.auto()
    # May either exist or not exist, no enforcement
    optional = enum.auto()
    # Recommended, but not enforced. Affects whether name shows up as green.
    desired = enum.auto()
    # Desired now; intended to become required in high-coverage taxa.
    pending_required_high_coverage = enum.auto()
    # Required by lint in high-coverage taxa
    required_high_coverage = enum.auto()
    # Required by lint in all taxa
    required = enum.auto()

    def is_desired(self) -> bool:
        match self:
            case (
                Requiredness.desired
                | Requiredness.pending_required_high_coverage
                | Requiredness.required_high_coverage
                | Requiredness.required
            ):
                return True
            case Requiredness.disallowed | Requiredness.optional:
                return False
            case _:
                assert_never(self)


def has_accessible_original_citation(nam: Name) -> bool:
    return (
        nam.original_citation is not None
        and not nam.original_citation.lacks_full_text()
    )


def get_requiredness(nam: Name, field: str) -> tuple[Requiredness, str | None]:
    match field:
        case "original_name":
            if has_accessible_original_citation(nam):
                return Requiredness.required, None
            else:
                return Requiredness.desired, None
        case "corrected_original_name":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            if nam.original_name is None:
                return Requiredness.disallowed, None
            else:
                return Requiredness.required, None
        case "original_rank":
            if nam.corrected_original_name is None:
                return Requiredness.disallowed, None
            elif has_accessible_original_citation(nam):
                return Requiredness.required, None
            else:
                return Requiredness.desired, None
        case "author_tags":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            return Requiredness.required_high_coverage, None
        case "year":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            return Requiredness.pending_required_high_coverage, None
        case "page_described":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            if (
                nam.original_citation is not None
                and nam.original_citation.type is ArticleType.WEB
            ):
                return Requiredness.optional, None
            elif has_accessible_original_citation(nam):
                return Requiredness.required, None
            else:
                return Requiredness.desired, None
        case "original_citation":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            return Requiredness.desired, None
        case "verbatim_citation":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            if nam.original_citation is None:
                if nam.numeric_year() > 1970:
                    return (
                        Requiredness.required,
                        "recent name must have verbatim citation",
                    )
                if nam.nomenclature_status is NomenclatureStatus.subsequent_usage:
                    return (
                        Requiredness.required,
                        "subsequent usage must have verbatim citation",
                    )
                else:
                    return Requiredness.pending_required_high_coverage, None
            else:
                return Requiredness.disallowed, None
        case "citation_group":
            if nam.original_citation is not None:
                return Requiredness.disallowed, None
            elif nam.verbatim_citation is not None:
                return Requiredness.required, None
            else:
                return Requiredness.optional, None
        case "name_complex":
            if nam.group is not Group.genus:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_name_complex():
                return Requiredness.optional, None
            elif has_accessible_original_citation(nam):
                return Requiredness.required_high_coverage, None
            else:
                return Requiredness.desired, None
        case "species_name_complex":
            if nam.group is not Group.species:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_name_complex():
                return Requiredness.optional, None
            elif (
                has_accessible_original_citation(nam)
                and nam.nomenclature_status.requires_name_complex()
            ):
                return Requiredness.required, None
            else:
                return Requiredness.desired, None
        case "type_locality":
            if nam.group is not Group.species:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_type():
                return Requiredness.optional, None
            elif (
                has_accessible_original_citation(nam)
                and nam.taxon.age is AgeClass.extant
            ):
                return (
                    Requiredness.required,
                    "missing type locality, but has original citation and is extant",
                )
            elif nam.has_type_tag(TypeTag.LocationDetail):
                return (
                    Requiredness.required,
                    "missing type locality, but has LocationDetail tag",
                )
            else:
                return Requiredness.desired, None
        case "type_specimen":
            if nam.group is not Group.species:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_type() or (
                nam.collection is not None
                # These are special Collections that indicate there is no preserved specimen.
                and nam.collection.id in {LOST_COLLECTION, UNTRACED_COLLECTION}
            ):
                return Requiredness.optional, None
            else:
                return Requiredness.desired, None
        case "collection":
            if nam.group is not Group.species:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_type():
                return Requiredness.optional, None
            else:
                return Requiredness.desired, None
        case "species_type_kind":
            if nam.group is not Group.species:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_type():
                return Requiredness.optional, None
            elif (
                nam.type_specimen is not None
                or nam.collection is not None
                or nam.has_type_tag(TypeTag.Age)
                or nam.has_type_tag(TypeTag.Gender)
            ):
                return Requiredness.required, None
            else:
                return Requiredness.desired, None
        case "type":
            match nam.group:
                case Group.family:
                    if not nam.nomenclature_status.requires_type():
                        return Requiredness.optional, None
                    else:
                        return Requiredness.required_high_coverage, None
                case Group.genus:
                    if not nam.nomenclature_status.requires_type():
                        return Requiredness.optional, None
                    elif nam.genus_type_kind is not None:
                        if nam.genus_type_kind is TypeSpeciesDesignation.undesignated:
                            return Requiredness.disallowed, None
                        else:
                            return Requiredness.required, None
                    else:
                        return Requiredness.desired, None
                case Group.high | Group.species:
                    return Requiredness.disallowed, None
                case _:
                    assert_never(nam.group)
        case "genus_type_kind":
            if nam.group is not Group.genus:
                return Requiredness.disallowed, None
            elif nam.type is None:
                if nam.genus_type_kind is TypeSpeciesDesignation.undesignated:
                    return Requiredness.required, None
                else:
                    return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_type():
                return Requiredness.optional, None
            else:
                return Requiredness.pending_required_high_coverage, None
        case "original_parent":
            if not (
                nam.group is Group.species
                or nam.original_rank in (Rank.subgenus, Rank.other_subgeneric)
            ):
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_original_parent():
                return Requiredness.optional, None
            elif (
                nam.corrected_original_name is not None
                and not nam.has_type_tag(TypeTag.NoOriginalParent)
                and models.name.lint.original_parent.should_require_subgenus_original_parent(
                    nam
                )
                and nam.original_citation is not None
            ):
                return Requiredness.required_high_coverage, None
            elif nam.has_type_tag(TypeTag.NoOriginalParent):
                return Requiredness.disallowed, None
            else:
                return Requiredness.desired, None
        case "group" | "root_name" | "status" | "nomenclature_status" | "taxon":
            return Requiredness.required, None
        case "target":
            if nam.status is Status.redirect:
                return Requiredness.required, None
            else:
                return Requiredness.disallowed, None
        case "type_tags" | "tags" | "data":
            return Requiredness.optional, None
        case _:
            raise ValueError(f"unrecognized field {field}")


@dataclass(frozen=True)
class FieldStatus:
    requiredness: Requiredness
    message: str | None
    is_nonempty: bool
    value: object


def get_status(nam: Name, field: clirm.Field) -> FieldStatus:
    """Return a FieldStatus object representing the status of the field on the Name object."""
    requiredness, message = get_requiredness(nam, field.attribute_name)
    value = getattr(nam, field.attribute_name)
    if isinstance(field, models.base.ADTField):
        # empty ADTField is represented as an empty tuple, not None
        is_nonempty = bool(value)
    else:
        is_nonempty = value is not None
    return FieldStatus(requiredness, message, is_nonempty, value)


def is_desired(nam: Name, field_name: str) -> bool:
    requiredness, _ = get_requiredness(nam, field_name)
    return requiredness.is_desired()


@dataclass(frozen=True)
class TypeTagRequiredness:
    """One obligation, satisfied by the primary tag or either alternative.

    must_be_from_original_citation applies to both the primary tag and
    alternative_with_citation. The plain alternative is source-independent.
    Without that flag, all alternatives accept any source. A source constraint
    concerns satisfying this obligation, not whether other-source tags may exist.
    """

    requiredness: Requiredness
    message: str | None = None
    must_be_from_original_citation: bool = False
    alternative_with_citation: TypeTagCons | None = None
    alternative: TypeTagCons | None = None


def get_type_tag_status(
    nam: Name,
    tag_cls: TypeTagCons,
    *,
    cfg: LintConfig = LintConfig(autofix=False, interactive=False),
) -> FieldStatus:
    """Return the tag policy and whether its obligation is satisfied.

    value is a tuple of matching tags, including any accepted alternatives.
    is_nonempty means at least one tag satisfies the source constraint, rather
    than merely that a tag of this class exists. For disallowed tags, the policy
    has no alternatives or source constraint, so this reports their presence.
    Coverage determines enforcement in the caller, not whether tags match here.
    """
    requirement = get_type_tag_requiredness(nam, tag_cls, cfg=cfg)
    sourced_tag_ids = {tag_cls._tag}
    if requirement.alternative_with_citation is not None:
        sourced_tag_ids.add(requirement.alternative_with_citation._tag)
    alternative_id = (
        requirement.alternative._tag if requirement.alternative is not None else None
    )
    conflict_field = getattr(tag_cls, "__name__", None)
    accepts_conflict = (
        conflict_field in CONFLICTING_TYPE_DATA_FIELDS
        and requirement.requiredness is not Requiredness.disallowed
    )
    matching_tags = tuple(
        tag
        for tag in nam.type_tags
        if (
            tag._tag in sourced_tag_ids
            and (
                not requirement.must_be_from_original_citation
                or (
                    nam.original_citation is not None
                    and getattr(tag, "source", None) == nam.original_citation
                )
            )
        )
        or tag._tag == alternative_id
        or (
            accepts_conflict
            and isinstance(tag, TypeTag.ConflictingTypeData)
            and tag.field == conflict_field
        )
    )
    message = requirement.message
    if not matching_tags and requirement.requiredness.is_desired():
        expected = [getattr(tag_cls, "__name__", str(tag_cls))]
        if requirement.alternative_with_citation is not None:
            expected.append(requirement.alternative_with_citation.__name__)
        description = " or ".join(expected)
        if requirement.must_be_from_original_citation:
            description += " from original_citation"
        if requirement.alternative is not None:
            description += f" or {requirement.alternative.__name__}"
        if accepts_conflict:
            description += f" or ConflictingTypeData({conflict_field!r})"
        explanation = f"expected {description}"
        message = f"{message}; {explanation}" if message else explanation
    return FieldStatus(
        requiredness=requirement.requiredness,
        message=message,
        is_nonempty=bool(matching_tags),
        value=matching_tags,
    )


def get_missing_requirements(
    nam: Name,
    *,
    levels: Container[Requiredness] | None = (
        Requiredness.pending_required_high_coverage,
    ),
    include_fields: bool = True,
    include_type_tags: bool = True,
    cfg: LintConfig = LintConfig(autofix=False, interactive=False),
) -> Iterable[tuple[str, FieldStatus]]:
    """Yield missing field/tag obligations at the selected requiredness levels.

    This reports policy, irrespective of coverage or IgnoreLint tags. Callers can
    filter the Names by coverage to measure the effect of promoting a level.
    Alternatives are counted as one obligation, not separate missing tags.
    Pass levels=None for all desired-or-stronger requirements (completeness).
    """
    if levels is None:
        levels = tuple(level for level in Requiredness if level.is_desired())
    if include_fields:
        for field_name, status in iter_field_statuses(nam):
            if status.requiredness in levels and not status.is_nonempty:
                yield f"field {field_name}", status
    if include_type_tags:
        for tag_cls, status in iter_type_tag_statuses(nam, cfg=cfg):
            if status.requiredness in levels and not status.is_nonempty:
                label = getattr(tag_cls, "__name__", str(tag_cls))
                yield f"type tag {label}", status


def describe_missing_requirements(nam: Name) -> str:
    """Describe all missing desired data, or return an empty string if complete."""
    return "; ".join(
        f"{label} ({status.message})" if status.message else label
        for label, status in get_missing_requirements(nam, levels=None)
    )


def iter_field_statuses(nam: Name) -> Iterable[tuple[str, FieldStatus]]:
    """Yield each field's identity and status, including optional/disallowed fields."""
    for field in Name.clirm_fields.values():
        yield field.attribute_name, get_status(nam, field)


def iter_type_tag_statuses(
    nam: Name,
    *,
    tag_classes: Iterable[TypeTagCons] | None = None,
    cfg: LintConfig = LintConfig(autofix=False, interactive=False),
) -> Iterable[tuple[TypeTagCons, FieldStatus]]:
    """Yield tag constructors and statuses, preserving identities and alternatives.

    Completeness callers should select all is_desired() statuses; lint callers
    additionally apply the requiredness level and coverage rules.
    """
    if tag_classes is None:
        tag_classes = TypeTag._tag_to_member.values()
    for tag_cls in tag_classes:
        yield tag_cls, get_type_tag_status(nam, tag_cls, cfg=cfg)


TYPE_TAGS_BY_GROUP: dict[type[TypeTag], tuple[Group, ...]] = {
    **dict.fromkeys(
        (
            TypeTag.Date,
            TypeTag.Gender,
            TypeTag.Age,
            TypeTag.Organ,
            TypeTag.Altitude,
            TypeTag.Coordinates,
            TypeTag.TypeLocality,
            TypeTag.StratigraphyDetail,
            TypeTag.Habitat,
            TypeTag.Host,
            TypeTag.LectotypeDesignation,
            TypeTag.NeotypeDesignation,
            TypeTag.SpecimenDetail,
            TypeTag.LocationDetail,
            TypeTag.Repository,
            TypeTag.ProbableRepository,
            TypeTag.CollectionDetail,
            TypeTag.CollectedBy,
            TypeTag.Involved,
            TypeTag.NoLocation,
            TypeTag.NoSpecimen,
            TypeTag.NoDate,
            TypeTag.NoCollector,
            TypeTag.NoOrgan,
            TypeTag.NoGender,
            TypeTag.NoAge,
            TypeTag.ImpreciseLocality,
            TypeTag.TypeSpecimenLink,
            TypeTag.FormerRepository,
            TypeTag.ExtraRepository,
            TypeTag.FutureRepository,
            TypeTag.TypeSpecimenLinkFor,
            TypeTag.GuessedRepository,
            TypeTag.InterpretedTypeLocality,
            TypeTag.InterpretedTypeSpecimen,
            TypeTag.AdditionalTypeSpecimen,
            TypeTag.OriginalTypification,
            TypeTag.PartialTypeLocality,
            TypeTag.TypeLocalityValidity,
            TypeTag.ConflictingTypeData,
        ),
        (Group.species,),
    ),
    **dict.fromkeys(
        (
            TypeTag.TypeDesignation,
            TypeTag.IncludedSpecies,
            TypeTag.GenusCoelebs,
            TypeTag.TypeSpeciesDetail,
            type(TypeTag.NoOriginalParent),
        ),
        (Group.genus,),
    ),
    TypeTag.CommissionTypeDesignation: (Group.genus, Group.family),
    TypeTag.InterpretedTypeTaxon: (Group.genus, Group.family),
}


def get_type_tag_requiredness(
    nam: Name,
    tag_cls: TypeTagCons,
    *,
    cfg: LintConfig = LintConfig(autofix=False, interactive=False),
) -> TypeTagRequiredness:
    """Return the detail and derived-tag policy used for completeness checks.

    Specialized lints may impose additional requirements. This function does not
    check whether an obligation is satisfied; alternatives are part of its result.
    """
    # NoOriginalParent is a singleton constructor, rather than a tag class.
    tag_type = tag_cls if isinstance(tag_cls, type) else type(tag_cls)
    if tag_type in TYPE_TAGS_BY_GROUP:
        allowed_groups = TYPE_TAGS_BY_GROUP[tag_type]
        if nam.group not in allowed_groups:
            return TypeTagRequiredness(
                requiredness=Requiredness.disallowed,
                message=f"{tag_type.__name__} is not allowed for group {nam.group!r}",
            )

    match tag_cls:
        case TypeTag.EtymologyDetail:
            if nam.requires_etymology():
                return TypeTagRequiredness(
                    requiredness=_detail_tag_requiredness(nam, cfg),
                    must_be_from_original_citation=True,
                    alternative_with_citation=TypeTag.NoEtymology,
                )
        case TypeTag.LocationDetail | TypeTag.SpecimenDetail:
            if nam.numeric_year() >= 1900 and nam.nomenclature_status.requires_type():
                return TypeTagRequiredness(
                    requiredness=_detail_tag_requiredness(nam, cfg),
                    must_be_from_original_citation=True,
                    alternative_with_citation=(
                        TypeTag.NoLocation
                        if tag_cls is TypeTag.LocationDetail
                        else TypeTag.NoSpecimen
                    ),
                )
        case (
            TypeTag.Date
            | TypeTag.Age
            | TypeTag.Gender
            | TypeTag.Organ
            | TypeTag.CollectedBy
        ):
            if (
                nam.type_specimen
                and nam.species_type_kind is not SpeciesGroupType.syntypes
                and (
                    not nam.is_ichno()
                    if tag_cls is TypeTag.Organ
                    else not nam.is_fossil()
                )
            ):
                alternatives = {
                    TypeTag.Date: TypeTag.NoDate,
                    TypeTag.Age: TypeTag.NoAge,
                    TypeTag.Gender: TypeTag.NoGender,
                    TypeTag.Organ: TypeTag.NoOrgan,
                    TypeTag.CollectedBy: TypeTag.NoCollector,
                }
                return TypeTagRequiredness(
                    requiredness=_derived_tag_requiredness(
                        nam,
                        cfg,
                        ExperimentalLintCondition(),
                        prerequisite_tags=SPECIMEN_TAGS,
                    ),
                    alternative_with_citation=alternatives[tag_cls],
                    alternative=(
                        TypeTag.Involved if tag_cls is TypeTag.CollectedBy else None
                    ),
                )
        case TypeTag.NamedAfter:
            if (
                nam.group is Group.species
                and nam.is_patronym()
                and not nam.nomenclature_status.is_variant()
            ):
                return TypeTagRequiredness(
                    requiredness=_derived_tag_requiredness(
                        nam,
                        cfg,
                        ExperimentalLintCondition(
                            only_with_tags_from_original=ETYMOLOGY_TAGS
                        ),
                    )
                )
        case TypeTag.Repository:
            if nam.collection is not None and nam.collection.id == MULTIPLE_COLLECTION:
                return TypeTagRequiredness(
                    requiredness=_derived_tag_requiredness(
                        nam, cfg, ExperimentalLintCondition(always=True)
                    )
                )
        case TypeTag.ProbableRepository:
            if nam.type_specimen and nam.species_type_kind and not nam.collection:
                return TypeTagRequiredness(
                    requiredness=_derived_tag_requiredness(
                        nam,
                        cfg,
                        ExperimentalLintCondition(),
                        prerequisite_tags=SPECIMEN_TAGS,
                    )
                )
        case TypeTag.IncludedSpecies:
            if nam.nomenclature_status.requires_type() and (
                nam.genus_type_kind is None or nam.genus_type_kind.requires_tag()
            ):
                return TypeTagRequiredness(
                    requiredness=_derived_tag_requiredness(
                        nam, cfg, ExperimentalLintCondition(always=True)
                    ),
                    alternative=TypeTag.GenusCoelebs,
                )
        case TypeTag.TextualOriginalRank:
            if nam.original_rank is not None and nam.original_rank.needs_textual_rank:
                return TypeTagRequiredness(
                    requiredness=_derived_tag_requiredness(
                        nam, cfg, ExperimentalLintCondition(always=True)
                    )
                )
        case (
            TypeTag._RawCollector
            | TypeTag.Altitude
            | TypeTag.Coordinates
            | TypeTag.TypeLocality
            | TypeTag.StratigraphyDetail
            | TypeTag.Habitat
            | TypeTag.Host
            | TypeTag.TypeDesignation
            | TypeTag.CommissionTypeDesignation
            | TypeTag.LectotypeDesignation
            | TypeTag.NeotypeDesignation
            | TypeTag.GenusCoelebs
            | TypeTag.TypeSpeciesDetail
            | TypeTag.CollectionDetail
            | TypeTag.CitationDetail
            | TypeTag.DefinitionDetail
            | TypeTag.DifferentAuthority
            | TypeTag.NoEtymology
            | TypeTag.NoLocation
            | TypeTag.NoSpecimen
            | TypeTag.NoDate
            | TypeTag.NoCollector
            | TypeTag.NoOrgan
            | TypeTag.NoGender
            | TypeTag.NoAge
            | TypeTag.Involved
            | TypeTag.ImpreciseLocality
            | TypeTag.NomenclatureDetail
            | TypeTag.IncorrectGrammar
            | TypeTag.LSID
            | TypeTag.TypeSpecimenLink
            | TypeTag.IgnoreLint
            | TypeTag.RejectedLSID
            | TypeTag.PartialTaxon
            | TypeTag.FormerRepository
            | TypeTag.ExtraRepository
            | TypeTag.FutureRepository
            | TypeTag.TypeSpecimenLinkFor
            | TypeTag.PhyloCodeNumber
            | TypeTag.AuthorityPageLink
            | TypeTag.GuessedRepository
            | TypeTag.NoOriginalParent
            | TypeTag.SourceDetail
            | TypeTag.VerbatimName
            | TypeTag.IgnorePotentialCitationFrom
            | TypeTag.DescriptionDetail
            | TypeTag.InterpretedTypeLocality
            | TypeTag.InterpretedTypeSpecimen
            | TypeTag.InterpretedTypeTaxon
            | TypeTag.NomenclatureComments
            | TypeTag.AdditionalTypeSpecimen
            | TypeTag.OriginalTypification
            | TypeTag.PhylogeneticDefinition
            | TypeTag.InternalSpecifier
            | TypeTag.ExternalSpecifier
            | TypeTag.TreatAsEquivalentTo
            | TypeTag.MustNotInclude
            | TypeTag.MustBePartOf
            | TypeTag.MustNotBePartOf
            | TypeTag.MustBeExtinct
            | TypeTag.StructuredVerbatimCitation
            | TypeTag.PartialTypeLocality
            | TypeTag.TypeLocalityValidity
            | TypeTag.ConflictingTypeData
        ):
            # Alternatives have no independent requirement; other tags retain
            # their specialized validation in the lint package.
            pass
        case _:
            raise ValueError(f"Unhandled type tag: {tag_cls}")
    return TypeTagRequiredness(requiredness=Requiredness.optional)


def _detail_tag_requiredness(nam: Name, cfg: LintConfig) -> Requiredness:
    if (
        nam.original_citation is None
        or not has_accessible_original_citation(nam)
        or nam.original_citation.has_tag(models.article.ArticleTag.NeedsTranslation)
    ):
        return Requiredness.desired
    if (
        # Deliberately start enforcing completeness only after some original
        # source data has been entered. Use the same evidence as the data level.
        any(get_tags_from_original_citation(nam))
        and ExperimentalLintCondition(above_article_id=69_000).should_apply(nam, cfg)
    ):
        return Requiredness.required_high_coverage
    return Requiredness.pending_required_high_coverage


def _derived_tag_requiredness(
    nam: Name,
    cfg: LintConfig,
    condition: ExperimentalLintCondition,
    prerequisite_tags: tuple[type[TypeTag], ...] | None = None,
) -> Requiredness:
    if nam.original_citation is None or not has_accessible_original_citation(nam):
        return Requiredness.desired
    if prerequisite_tags is not None:
        if not any(
            isinstance(tag, prerequisite_tags)
            and getattr(tag, "source", None) == nam.original_citation
            for tag in nam.type_tags
        ):
            return Requiredness.desired
    if condition.should_apply(nam, cfg):
        return Requiredness.required_high_coverage
    return Requiredness.pending_required_high_coverage
