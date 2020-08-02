from typing import Any, Iterable, List, Optional

import peewee
from peewee import BooleanField, CharField, ForeignKeyField

from .. import models
from ... import events, getinput
from ..constants import GrammaticalGender, GenderArticle, Group, SourceLanguage, SpeciesNameKind

from .base import BaseModel, EnumField


class SpeciesNameComplex(BaseModel):
    """Groups of species-group names of the same derivation or nature.

    See ICZN Articles 11.9.1 and 31.

    """

    creation_event = events.Event["SpeciesNameComplex"]()
    save_event = events.Event["SpeciesNameComplex"]()
    label_field = "label"
    call_sign = "SC"

    label = CharField()
    stem = CharField()
    kind = EnumField(SpeciesNameKind)
    masculine_ending = CharField()
    feminine_ending = CharField()
    neuter_ending = CharField()
    comment = CharField()

    class Meta(object):
        db_table = "species_name_complex"

    def __repr__(self) -> str:
        if any(
            ending != ""
            for ending in (
                self.masculine_ending,
                self.feminine_ending,
                self.neuter_ending,
            )
        ):
            return f"{self.label} ({self.kind.name}, -{self.masculine_ending}, -{self.feminine_ending}, -{self.neuter_ending})"
        else:
            return f"{self.label} ({self.kind.name})"

    def self_apply(self, dry_run: bool = True) -> List["models.Name"]:
        return self.apply_to_ending(self.label, dry_run=dry_run)

    def apply_to_ending(
        self,
        ending: str,
        dry_run: bool = True,
        interactive: bool = False,
        full_name_only: bool = True,
    ) -> List["models.Name"]:
        """Adds the name complex to all names with a specific ending."""
        names = [
            name
            for name in models.Name.filter(
                models.Name.group == Group.species,
                models.Name.species_name_complex >> None,
                models.Name.root_name % f"*{ending}",
            )
            if name.root_name.endswith(ending)
        ]
        print(f"found {len(names)} names with -{ending} to apply {self}")
        for name in names:
            print(name)
            if not dry_run:
                name.species_name_complex = self
        if interactive:
            if getinput.yes_no("apply?"):
                for name in names:
                    name.species_name_complex = self
                dry_run = False
        if not dry_run:
            saved_endings = list(self.endings)
            if not any(e.ending == ending for e in saved_endings):
                print(f"saving ending {ending}")
                self.make_ending(ending, full_name_only=full_name_only)
        return names

    def get_stem_from_name(self, name: str) -> str:
        """Applies the group to a genus name to get the name's stem."""
        assert self.stem.endswith(self.masculine_ending)
        if self.masculine_ending:
            # This doesn't work as expected if len(self.masculine_ending) == 0.
            stem = self.stem[: -len(self.masculine_ending)]
        else:
            stem = self.stem
        for ending in (self.masculine_ending, self.feminine_ending, self.neuter_ending):
            if name.endswith(stem + ending):
                if ending == "":
                    return name
                else:
                    return name[: -len(ending)]
        raise ValueError(f"could not extract stem from {name} using {self}")

    def get_form(self, name: str, gender: GrammaticalGender) -> str:
        stem = self.get_stem_from_name(name)
        if gender == GrammaticalGender.masculine:
            return stem + self.masculine_ending
        elif gender == GrammaticalGender.feminine:
            return stem + self.feminine_ending
        elif gender == GrammaticalGender.neuter:
            return stem + self.neuter_ending
        else:
            raise ValueError(f"invalid gender {gender!r}")

    def get_forms(self, name: str) -> Iterable[str]:
        if self.kind == SpeciesNameKind.adjective:
            stem = self.get_stem_from_name(name)
            for ending in (
                self.masculine_ending,
                self.feminine_ending,
                self.neuter_ending,
            ):
                yield stem + ending
        else:
            yield name

    def get_names(self) -> List["models.Name"]:
        return list(self.names)

    def make_ending(
        self, ending: str, comment: Optional[str] = "", full_name_only: bool = False
    ) -> "SpeciesNameEnding":
        return SpeciesNameEnding.get_or_create(
            name_complex=self,
            ending=ending,
            comment=comment,
            full_name_only=full_name_only,
        )

    def remove(self) -> None:
        for nam in self.get_names():
            print("removing name complex from", nam)
            nam.species_name_complex = None
            nam.save()
        for ending in self.endings:
            print("removing ending", ending)
            ending.delete_instance()
        print("removing complex", self)
        self.delete_instance()

    @classmethod
    def make(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        kind: SpeciesNameKind,
        comment: Optional[str] = None,
        masculine_ending: str = "",
        feminine_ending: str = "",
        neuter_ending: str = "",
    ) -> "SpeciesNameComplex":
        return cls.create(
            label=label,
            stem=stem,
            kind=kind,
            comment=comment,
            masculine_ending=masculine_ending,
            feminine_ending=feminine_ending,
            neuter_ending=neuter_ending,
        )

    @classmethod
    def _get_or_create(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        kind: SpeciesNameKind,
        comment: Optional[str] = None,
        masculine_ending: str = "",
        feminine_ending: str = "",
        neuter_ending: str = "",
    ) -> "SpeciesNameComplex":
        try:
            return cls.get(cls.label == label, cls.stem == stem, cls.kind == kind)
        except peewee.DoesNotExist:
            print("creating new name complex with label", label)
            return cls.make(
                label=label,
                stem=stem,
                kind=kind,
                comment=comment,
                masculine_ending=masculine_ending,
                feminine_ending=feminine_ending,
                neuter_ending=neuter_ending,
            )

    @classmethod
    def by_label(cls, label: str) -> "SpeciesNameComplex":
        complexes = list(cls.filter(cls.label == label))
        if len(complexes) == 1:
            return complexes[0]
        else:
            raise ValueError(f"found {complexes} with label {label}")

    @classmethod
    def of_kind(cls, kind: SpeciesNameKind) -> "SpeciesNameComplex":
        """Indeclinable name of a particular kind."""
        return cls._get_or_create(kind.name, kind=kind)

    @classmethod
    def ambiguous(
        cls, stem: str, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        """For groups of names that are ambiguously nouns in apposition (Art. 31.2.2)."""
        return cls._get_or_create(
            stem, stem=stem, kind=SpeciesNameKind.ambiguous_noun, comment=comment
        )

    @classmethod
    def adjective(
        cls,
        stem: str,
        comment: Optional[str],
        masculine_ending: str,
        feminine_ending: str,
        neuter_ending: str,
        auto_apply: bool = False,
    ) -> "SpeciesNameComplex":
        """Name based on a Latin adjective."""
        snc = cls._get_or_create(
            stem,
            stem=stem,
            kind=SpeciesNameKind.adjective,
            comment=comment,
            masculine_ending=masculine_ending,
            feminine_ending=feminine_ending,
            neuter_ending=neuter_ending,
        )
        if auto_apply:
            snc.self_apply(dry_run=False)
        return snc

    @classmethod
    def first_declension(
        cls, stem: str, auto_apply: bool = True, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        return cls.adjective(stem, comment, "us", "a", "um", auto_apply=auto_apply)

    @classmethod
    def third_declension(
        cls, stem: str, auto_apply: bool = True, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        return cls.adjective(stem, comment, "is", "is", "e", auto_apply=auto_apply)

    @classmethod
    def invariant(
        cls, stem: str, auto_apply: bool = True, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        return cls.adjective(stem, comment, "", "", "", auto_apply=auto_apply)

    @classmethod
    def create_interactively(cls, **kwargs: Any) -> "SpeciesNameComplex":
        kind = getinput.get_with_completion(
            [
                "ambiguous",
                "adjective",
                "first_declension",
                "third_declension",
                "invariant",
            ],
            "kind> ",
        )
        assert kind is not None
        stem = getinput.get_line("stem> ")
        assert stem is not None
        comment = getinput.get_line("comment> ")
        if kind == "adjective":
            masculine = getinput.get_line("masculine_ending> ")
            feminine = getinput.get_line("feminine_ending> ")
            neuter = getinput.get_line("neuter_ending> ")
            assert masculine is not None
            assert feminine is not None
            assert neuter is not None
            return cls.adjective(stem, comment, masculine, feminine, neuter)
        else:
            return getattr(cls, kind)(stem=stem, comment=comment)


class NameComplex(BaseModel):
    """Group of genus-group names with the same derivation."""

    creation_event = events.Event["NameComplex"]()
    save_event = events.Event["NameComplex"]()
    label_field = "label"
    call_sign = "NC"

    label = CharField()
    stem = CharField()
    source_language = EnumField(SourceLanguage)
    code_article = EnumField(GenderArticle)
    gender = EnumField(GrammaticalGender)
    comment = CharField()
    stem_remove = CharField(null=False)
    stem_add = CharField(null=False)

    class Meta(object):
        db_table = "name_complex"

    def __repr__(self) -> str:
        return f'{self.label} ({self.code_article.name}, {self.gender.name}, -{self.stem_remove or ""}+{self.stem_add or ""})'

    def self_apply(self, dry_run: bool = True) -> List["models.Name"]:
        return self.apply_to_ending(self.label, dry_run=dry_run)

    def apply_to_ending(self, ending: str, dry_run: bool = True) -> List["models.Name"]:
        """Adds the name complex to all names with a specific ending."""
        names = [
            name
            for name in models.Name.filter(
                models.Name.group == Group.genus,
                models.Name.name_complex >> None,
                models.Name.root_name % f"*{ending}",
            )
            if name.root_name.endswith(ending)
        ]
        print(f"found {len(names)} names with -{ending} to apply {self}")
        output = []
        for name in names:
            if name.gender is not None and name.gender != self.gender:
                print(
                    f"ignoring {name} because its gender {name.gender} does not match"
                )
                output.append(name)
            else:
                print(name)
                if not dry_run:
                    name.name_complex = self
                    name.save()
        if not dry_run:
            saved_endings = list(self.endings)
            if not any(e.ending == ending for e in saved_endings):
                print(f"saving ending {ending}")
                self.make_ending(ending)
        return output

    def get_stem_from_name(self, name: str) -> str:
        """Applies the group to a genus name to get the name's stem."""
        if self.stem_remove:
            if not name.endswith(self.stem_remove):
                raise ValueError(f"{name} does not end with {self.stem_remove}")
            name = name[: -len(self.stem_remove)]
        return name + self.stem_add

    def make_ending(self, ending: str, comment: Optional[str] = "") -> "NameEnding":
        return NameEnding.create(name_complex=self, ending=ending, comment=comment)

    def get_names(self) -> List["models.Name"]:
        return list(self.names)

    @classmethod
    def make(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        source_language: SourceLanguage = SourceLanguage.other,
        code_article: GenderArticle,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        return cls.create(
            label=label,
            stem=stem,
            source_language=source_language,
            code_article=code_article,
            gender=gender,
            comment=comment,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def _get_or_create(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        source_language: SourceLanguage,
        code_article: GenderArticle,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        try:
            return cls.get(
                cls.label == label,
                cls.source_language == source_language,
                cls.code_article == code_article,
                cls.gender == gender,
            )
        except peewee.DoesNotExist:
            print("creating new name complex with label", label)
            return cls.make(
                label=label,
                stem=stem,
                source_language=source_language,
                code_article=code_article,
                gender=gender,
                comment=comment,
                stem_remove=stem_remove,
                stem_add=stem_add,
            )

    @classmethod
    def by_label(cls, label: str) -> "NameComplex":
        complexes = list(cls.filter(cls.label == label))
        if len(complexes) == 1:
            return complexes[0]
        else:
            raise ValueError("found {complexes} with label {label}")

    @classmethod
    def latin_stem(
        cls,
        stem: str,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word found in a Latin dictionary with a specific gender."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.art30_1_1,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def unknown_obvious_stem(
        cls,
        stem: str,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word of unknown etymology, but of obvious grammatical behavior."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.unknown_obvious_stem,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def greek_stem(
        cls,
        stem: str,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word found in a Greek dictionary with a specific gender."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.art30_1_2,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def latinized_greek(
        cls,
        stem: str,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word found in a Greek dictionary, but with a changed suffix."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.art30_1_3,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def bad_transliteration(
        cls,
        stem: str,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a Greek word, but with incorrect transliteration."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.bad_transliteration,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def common_gender(
        cls,
        stem: str,
        gender: GrammaticalGender = GrammaticalGender.masculine,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name of common gender in Latin, which defaults to masculine."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.art30_1_4_2,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def oides_name(
        cls,
        stem: str,
        gender: GrammaticalGender = GrammaticalGender.masculine,
        comment: Optional[str] = None,
    ) -> "NameComplex":
        """Names ending in -oides and a few other endings default to masculine unless the author treated it otherwise."""
        if stem not in ("ites", "oides", "ides", "odes", "istes"):
            raise ValueError("Art. 30.1.4.4 only applies to a limited set of stems")
        if gender != GrammaticalGender.masculine:
            label = f"{stem}_{gender.name}"
        else:
            label = stem
        return cls._get_or_create(
            label,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.art30_1_4_4,
            stem_remove="es",
            stem_add="",
        )

    @classmethod
    def latin_changed_ending(
        cls,
        stem: str,
        gender: GrammaticalGender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Based on a Latin word with a changed ending. Comment must specify the original word."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.art30_1_4_5,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def stem_expressly_set(
        cls, gender: GrammaticalGender, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Stem expressly set to a specific value."""
        label = cls._make_label(
            f"stem_expressly_set_{gender.name}", stem_remove, stem_add
        )
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.stem_expressly_set,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def expressly_specified(
        cls, gender: GrammaticalGender, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Gender expressly specified by the author."""
        label = cls._make_label(
            f"expressly_specified_{gender.name}", stem_remove, stem_add
        )
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.art30_2_2,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def indicated(
        cls, gender: GrammaticalGender, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Gender indicated by an adjectival species name."""
        label = cls._make_label(f"indicated_{gender.name}", stem_remove, stem_add)
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.art30_2_3,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def defaulted_masculine(
        cls, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Defaulted to masculine as a non-Western name."""
        label = cls._make_label("defaulted_masculine", stem_remove, stem_add)
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=GrammaticalGender.masculine,
            code_article=GenderArticle.art30_2_4,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def defaulted(
        cls,
        gender: GrammaticalGender,
        ending: str,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Defaulted to feminine or neuter as a non-Western name with a specific ending."""
        if gender == GrammaticalGender.masculine:
            assert False, "use defaulted_masculine instead"
        elif gender == GrammaticalGender.feminine:
            assert ending == "a", "only -a endings default to feminine"
        elif gender == GrammaticalGender.neuter:
            assert ending in (
                "um",
                "on",
                "u",
            ), "only -um, -on, and -u endings default to neuter"
        label = cls._make_label(
            f"defaulted_{gender.name}_{ending}", stem_remove, stem_add
        )
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.art30_2_4,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @staticmethod
    def _make_label(base_label: str, stem_remove: str, stem_add: str) -> str:
        if stem_remove or stem_add:
            base_label += "_stem"
        if stem_remove:
            base_label += f"_{stem_remove}"
        if stem_add:
            base_label += f"_{stem_add}"
        return base_label

    @classmethod
    def create_interactively(cls, **kwargs: Any) -> "NameComplex":
        kind = getinput.get_with_completion(
            [
                "latin_stem",
                "greek_stem",
                "latinized_greek",
                "bad_transliteration",
                "common_gender",
                "latin_changed_ending",
                "expressly_specified",
                "indicated",
                "defaulted_masculine",
                "defaulted",
                "unknown_obvious_stem",
                "stem_expressly_set",
            ],
            "kind> ",
            allow_empty=False,
        )
        assert kind is not None
        method = getattr(cls, kind)
        if kind in (
            "latin_stem",
            "greek_stem",
            "latinized_greek",
            "bad_transliteration",
            "common_gender",
            "latin_changed_ending",
            "unknown_obvious_stem",
        ):
            stem = getinput.get_line("stem> ")
            gender = getinput.get_enum_member(
                GrammaticalGender, "gender> ", allow_empty=False
            )
            comment = getinput.get_line("comment> ")
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(
                stem=stem,
                gender=gender,
                comment=comment,
                stem_remove=stem_remove,
                stem_add=stem_add,
            )
            nc.self_apply()
            if getinput.yes_no("self-apply?"):
                nc.self_apply(dry_run=False)
        elif kind in ("expressly_specified", "indicated", "stem_expressly_set"):
            gender = getinput.get_enum_member(
                GrammaticalGender, "gender> ", allow_empty=False
            )
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(gender=gender, stem_remove=stem_remove, stem_add=stem_add)
        elif kind == "defaulted_masculine":
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(stem_remove=stem_remove, stem_add=stem_add)
        elif kind == "defaulted":
            gender = getinput.get_enum_member(
                GrammaticalGender, "gender> ", allow_empty=False
            )
            ending = getinput.get_line("ending> ")
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(
                gender=gender, ending=ending, stem_remove=stem_remove, stem_add=stem_add
            )
        else:
            assert False, f"bad kind {kind}"
        return nc


class NameEnding(BaseModel):
    """Name ending that is mapped to a NameComplex."""

    label_field = "ending"
    call_sign = "NE"

    name_complex = ForeignKeyField(
        NameComplex, related_name="endings", db_column="name_complex_id"
    )
    ending = CharField()
    comment = CharField()

    class Meta(object):
        db_table = "name_ending"


class SpeciesNameEnding(BaseModel):
    """Name ending that is mapped to a SpeciesNameComplex."""

    label_field = "ending"
    call_sign = "SNE"

    name_complex = ForeignKeyField(
        SpeciesNameComplex, related_name="endings", db_column="name_complex_id"
    )
    ending = CharField()
    comment = CharField()
    full_name_only = BooleanField(default=False)

    class Meta(object):
        db_table = "species_name_ending"

    @classmethod
    def get_or_create(
        cls,
        name_complex: SpeciesNameComplex,
        ending: str,
        comment: Optional[str] = None,
        full_name_only: bool = False,
    ) -> "SpeciesNameEnding":
        try:
            return cls.get(
                cls.name_complex == name_complex,
                cls.ending == ending,
                cls.full_name_only == full_name_only,
            )
        except peewee.DoesNotExist:
            print("creating new name ending", ending, " for ", name_complex)
            return cls.create(
                name_complex=name_complex,
                ending=ending,
                comment=comment,
                full_name_only=full_name_only,
            )
