import enum
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
    Status,
    TypeSpeciesDesignation,
)
from taxonomy.db.models.collection import LOST_COLLECTION, UNTRACED_COLLECTION

from .name import Name, TypeTag


class Requiredness(enum.Enum):
    # Lint error if this exists
    disallowed = enum.auto()
    # May either exist or not exist, no enforcement
    optional = enum.auto()
    # Recommended, but not enforced. Affects whether name shows up as green.
    desired = enum.auto()
    # Required by lint in high-coverage taxa
    required_high_coverage = enum.auto()
    # Required by lint in all taxa
    required = enum.auto()

    def is_desired(self) -> bool:
        match self:
            case (
                Requiredness.desired
                | Requiredness.required_high_coverage
                | Requiredness.required
            ):
                return True
            case Requiredness.disallowed | Requiredness.optional:
                return False
            case _:
                assert_never(self)


def get_requiredness(nam: Name, field: str) -> tuple[Requiredness, str | None]:
    match field:
        case "original_name":
            if (
                nam.original_citation is not None
                and not nam.original_citation.lacks_full_text()
            ):
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
            elif (
                nam.original_citation is not None
                and not nam.original_citation.lacks_full_text()
            ):
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
            # TODO: Move towards required_high_coverage
            return Requiredness.desired, None
        case "page_described":
            if nam.nomenclature_status is NomenclatureStatus.informal:
                return Requiredness.disallowed, None
            if (
                nam.original_citation is not None
                and nam.original_citation.type is ArticleType.WEB
            ):
                return Requiredness.optional, None
            elif (
                nam.original_citation is not None
                and not nam.original_citation.lacks_full_text()
            ):
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
                    # TODO: Move towards required_high_coverage
                    return Requiredness.desired, None
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
            elif (
                nam.original_citation is not None
                and not nam.original_citation.lacks_full_text()
            ):
                return Requiredness.required_high_coverage, None
            else:
                return Requiredness.desired, None
        case "species_name_complex":
            if nam.group is not Group.species:
                return Requiredness.disallowed, None
            elif not nam.nomenclature_status.requires_name_complex():
                return Requiredness.optional, None
            elif (
                nam.original_citation is not None
                and not nam.original_citation.lacks_full_text()
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
                nam.original_citation is not None
                and nam.taxon.age is AgeClass.extant
                and not nam.original_citation.lacks_full_text()
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
                # TODO: Eventually make required_high_coverage
                return Requiredness.desired, None
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
                and nam.nomenclature_status.requires_original_parent()
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
        is_nonempty = bool(
            value
        )  # empty ADTField is represented as an empty tuple, not None
    else:
        is_nonempty = value is not None
    return FieldStatus(requiredness, message, is_nonempty, value)


def is_desired(nam: Name, field_name: str) -> bool:
    requiredness, _ = get_requiredness(nam, field_name)
    return requiredness.is_desired()
