from __future__ import annotations

import operator
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Callable, Container, Iterable, Sequence
from functools import lru_cache
from typing import IO, Any, cast

import peewee
from peewee import BooleanField, CharField, ForeignKeyField, TextField
from typing_extensions import assert_never

from taxonomy.apis.cloud_search import SearchField, SearchFieldType

from ... import events, getinput
from .. import definition, helpers, models
from ..constants import (
    AgeClass,
    FillDataLevel,
    Group,
    NomenclatureStatus,
    OccurrenceStatus,
    Rank,
    Status,
)
from ..derived_data import DerivedField, SetLater
from .article import Article
from .base import ADTField, BaseModel, EnumField, LintConfig
from .fill_data import DEFAULT_LEVEL, fill_data_for_names


class _OccurrenceGetter:
    """For easily accessing occurrences of a taxon.

    This is exposed at taxon.at. You can access taxa as either taxon.at.Locality_Name or taxon.at(L.Locality_Name).

    """

    def __init__(self, instance: Any = None) -> None:
        self.instance = instance

    def __get__(self, instance: Any, instance_type: Any) -> _OccurrenceGetter:
        return self.__class__(instance)

    def __getattr__(self, loc_name: str) -> models.Occurrence:
        return self(
            models.Location.get(
                models.Location.name == loc_name.replace("_", " "),
                models.Location.deleted == False,
            )
        )

    def __call__(self, loc: models.Location) -> models.Occurrence:
        return self.instance.occurrences.filter(models.Occurrence.location == loc).get()

    def __dir__(self) -> list[str]:
        return [o.location.name.replace(" ", "_") for o in self.instance.occurrences]


def _make_parent_getter(index: int) -> Any:
    def _get_ranked_parent(taxon: Taxon) -> Taxon | None:
        return ranked_parents(taxon)[index]

    return _get_ranked_parent


class Taxon(BaseModel):
    creation_event = events.Event["Taxon"]()
    save_event = events.Event["Taxon"]()
    label_field = "valid_name"
    grouping_field = "age"
    call_sign = "T"

    rank = EnumField(Rank)
    valid_name = CharField(default="")
    age = EnumField(AgeClass)
    parent = ForeignKeyField(
        "self", related_name="children", null=True, db_column="parent_id"
    )
    data = TextField(null=True)
    is_page_root = BooleanField(default=False)
    base_name = peewee.DeferredForeignKey("Name")
    tags = ADTField(lambda: models.tags.TaxonTag)

    derived_fields = [
        DerivedField("class_", SetLater, _make_parent_getter(0)),
        DerivedField("order", SetLater, _make_parent_getter(1)),
        DerivedField("family", SetLater, _make_parent_getter(2)),
    ]
    search_fields = [
        SearchField(SearchFieldType.text, "name"),
        SearchField(SearchFieldType.literal, "age"),
        SearchField(SearchFieldType.literal, "rank"),
    ]

    class Meta:
        db_table = "taxon"

    name = property(lambda self: self.base_name)

    def get_search_dicts(self) -> list[dict[str, Any]]:
        data = {"name": self.valid_name, "age": self.age.name, "rank": self.rank.name}
        return [data]

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(
            Taxon.age != AgeClass.removed, Taxon.age != AgeClass.redirect
        )

    def edit(self) -> None:
        self.fill_field("tags")

    def get_redirect_target(self) -> Taxon | None:
        if self.age is AgeClass.redirect:
            return self.parent
        return None

    def is_invalid(self) -> bool:
        return self.age in (AgeClass.removed, AgeClass.redirect)

    def should_skip(self) -> bool:
        return self.age in (AgeClass.removed, AgeClass.redirect)

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        if self.parent is None and self.id != 1:
            yield f"{self}: missing parent"
        if self.parent is not None and not self.age.can_have_parent_of_age(
            self.parent.age
        ):
            yield (
                f"{self}: is {self.age!r}, but its parent {self.parent} is"
                f" {self.parent.age!r}"
            )
        if not self.base_name.status.is_base_name():
            yield f"{self}: base name has invalid status {self.base_name.status}"
        expected_group = helpers.group_of_rank(self.rank)
        if expected_group != self.base_name.group:
            rank = self.rank.name
            group = self.base_name.group.name
            yield f"{self}: group mismatch: rank {rank} but group {group}"
        yield from self.check_nominal_genus(cfg)
        yield from self.check_valid_name(cfg)

    def check_nominal_genus(self, cfg: LintConfig) -> Iterable[str]:
        nominal_genus_tags = list(
            self.get_tags(self.tags, models.tags.TaxonTag.NominalGenus)
        )
        if len(nominal_genus_tags) > 1:
            yield f"{self}: has multiple nominal genus tags: {nominal_genus_tags}"
        elif len(nominal_genus_tags) == 1:
            nominal_genus = nominal_genus_tags[0].genus
            if nominal_genus.group is not Group.genus:
                yield f"{self}: nominal genus {nominal_genus} is not a genus"
        if (
            self.base_name.group is Group.species
            and self.base_name.status is Status.valid
            and not nominal_genus_tags
            and not self.has_parent_of_rank(Rank.genus)
        ):
            if cfg.autofix:
                orig_nam = self.base_name.corrected_original_name
                if orig_nam:
                    orig_genus, *_ = orig_nam.split()
                    candidates = list(
                        models.Name.select_valid().filter(
                            models.Name.group == Group.genus,
                            models.Name.root_name == orig_genus,
                        )
                    )
                    if len(candidates) == 1:
                        print(f"{self}: adding NominalGenus tag: {candidates[0]}")
                        self.add_tag(
                            models.tags.TaxonTag.NominalGenus(genus=candidates[0])
                        )
                        return
            yield f"{self}: should have NominalGenus tag"

    def check_valid_name(self, cfg: LintConfig) -> Iterable[str]:
        computed = self.compute_valid_name()
        if computed is None or self.valid_name == computed:
            return
        message = (
            f"{self}: valid name mismatch: {self.valid_name} (actual) vs."
            f" {computed} (computed)"
        )
        # For species-group taxa, we always trust the computed name. Usually these
        # have been reassigned to a different genus, or changed between species and
        # subspecies, or they have become nomina dubia (in which case we use the
        # corrected original name). For family-group names we don't always trust the
        # computed name, because stems may be arbitrary.
        can_fix = cfg.autofix and (
            self.base_name.group == Group.species or self.is_nominate_subgenus()
        )
        if can_fix:
            print(message)
            self.recompute_name()
        else:
            yield message

    def group(self) -> Group:
        return helpers.group_of_rank(self.rank)

    def get_names(self) -> Iterable[models.Name]:
        return models.Name.add_validity_check(self.names)

    def sorted_names(self, exclude_valid: bool = False) -> list[models.Name]:
        names: Iterable[models.Name] = self.get_names()
        if exclude_valid:
            names = filter(lambda name: name.status != Status.valid, names)

        def sort_key(nam: models.Name) -> tuple[bool, str, str]:
            return (
                nam.status not in (Status.valid, Status.nomen_dubium),
                nam.root_name,
                (nam.year or ""),
            )

        return sorted(names, key=sort_key)

    def get_children(self) -> Iterable[Taxon]:
        return self.add_validity_check(self.children)

    def sorted_children(self) -> list[Taxon]:
        return sorted(
            self.get_children(), key=operator.attrgetter("rank", "valid_name")
        )

    def sorted_occurrences(self) -> list[models.Occurrence]:
        return sorted(self.occurrences, key=lambda o: o.location.name)

    def root_name(self) -> str:
        return self.valid_name.split(" ")[-1]

    def all_data(self) -> None:
        self.full_data()
        self.base_name.full_data()
        print(self.sorted_names())
        print(self.sorted_children())
        print(self.sorted_occurrences())

    def full_name(self) -> str:
        if self.parent is None:
            return self.valid_name
        elif self.rank == Rank.subgenus:
            if self.is_nominate_subgenus():
                return self.valid_name
            return self.parent.valid_name + " (" + self.valid_name + ")"
        if self.rank == Rank.species_group:
            return self.parent.full_name() + " (" + self.base_name.root_name + ")"
        elif self.rank == Rank.species:
            # For nomina dubia and species inquirendae, retain the name as given.
            if self.parent.rank > Rank.genus or self.base_name.status != Status.valid:
                return self.valid_name
            parent_name = self.parent.full_name()
            if self.parent.needs_is():
                parent_name += " (?)"
            return parent_name + " " + self.base_name.root_name
        elif self.rank == Rank.subspecies:
            return self.parent.full_name() + " " + self.base_name.root_name
        else:
            return self.valid_name

    def needs_is(self) -> bool:
        if not hasattr(self, "_needs_is"):
            if self.rank == Rank.subgenus:
                self._needs_is = (
                    Taxon.select()
                    .where(Taxon.parent == self, Taxon.rank == Rank.species_group)
                    .count()
                    > 0
                )
            elif self.rank == Rank.genus:
                self._needs_is = (
                    Taxon.select_valid()
                    .where(
                        Taxon.parent == self,
                        (Taxon.rank == Rank.subgenus)
                        | (Taxon.rank == Rank.species_group),
                    )
                    .count()
                    > 0
                )
            else:
                self._needs_is = False
        return self._needs_is

    def parent_of_rank(self, rank: Rank, original_taxon: Taxon | None = None) -> Taxon:
        if original_taxon is None:
            original_taxon = self
        if self.rank > rank and self.rank != Rank.unranked:
            raise ValueError(
                f"{original_taxon} (id = {original_taxon.id}) has no ancestor of rank"
                f" {rank.name}"
            )
        elif self.rank == rank:
            return self
        elif self.parent is None:
            raise ValueError(
                f"{original_taxon} (id = {original_taxon.id}) has no ancestor of rank"
                f" {rank.name}"
            )
        else:
            return self.parent.parent_of_rank(rank, original_taxon=original_taxon)

    def add_tag(self, tag: models.tags.TaxonTag) -> None:
        if self.tags:
            self.tags += (tag,)
        else:
            self.tags = (tag,)  # type: ignore

    def has_parent_of_rank(self, rank: Rank) -> bool:
        try:
            self.parent_of_rank(rank)
        except ValueError:
            return False
        else:
            return True

    def is_child_of(self, taxon: Taxon) -> bool:
        if self == taxon:
            return True
        elif self.parent is None:
            return False
        else:
            return self.parent.is_child_of(taxon)

    def children_of_rank(self, rank: Rank, age: AgeClass | None = None) -> list[Taxon]:
        if self.rank < rank:
            return []
        elif self.rank == rank:
            if age is None or self.age == age:
                return [self]
            else:
                return []
        else:
            out: list[Taxon] = []
            for child in self.get_children():
                out += child.children_of_rank(rank, age=age)
            return out

    def names_like(self, root_name: str) -> list[models.Name]:
        """Find names matching root_name within this taxon."""
        pattern = re.compile(root_name)
        nams = self.all_names()
        return [nam for nam in nams if pattern.match(nam.root_name)]

    def print_names_like(self) -> None:
        """Find names matching root_name within this taxon."""
        root_name = models.Name.getter("root_name").get_one_key("root_name> ")
        if not root_name:
            return
        for nam in self.names_like(root_name):
            nam.display(full=False)

    def find_names(
        self, root_name: str, group: Group | None = None, fuzzy: bool = True
    ) -> list[models.Name]:
        """Find instances of the given root_name within the given container taxon."""
        if fuzzy:
            query = models.Name.root_name % root_name  # LIKE
        else:
            query = models.Name.root_name == root_name
        candidates = models.Name.filter(query)

        result = []
        # maybe I could do some internal caching here but for now this is fast enough
        for candidate in candidates:
            if group is not None and candidate.group != group:
                continue
            taxon = candidate.taxon
            while taxon.parent is not None:
                if taxon.id == self.id:
                    result.append(candidate)
                    break
                taxon = taxon.parent
        return result

    def display_extant(self, max_depth: int | None = 100) -> None:
        self.display(
            exclude_fn=lambda t: t.age != AgeClass.extant
            or t.base_name.status != Status.valid,
            name_exclude_fn=lambda n: n.status == Status.synonym,
            max_depth=max_depth,
        )

    def display(
        self,
        full: bool = False,
        max_depth: int | None = 2,
        file: IO[str] = sys.stdout,
        depth: int = 0,
        exclude: Container[Taxon] = set(),
        exclude_fn: Callable[[Taxon], bool] | None = None,
        name_exclude_fn: Callable[[models.Name], bool] | None = None,
        show_occurrences: bool | None = None,
    ) -> None:
        if show_occurrences is None:
            show_occurrences = full
        if exclude_fn is not None and exclude_fn(self):
            return
        file.write(" " * (4 * depth))
        file.write(f"{self.rank.name} {self.age.get_symbol()}{self.full_name()}\n")
        if full:
            data = {"data": self.data, "is_page_root": self.is_page_root}
            for key, value in data.items():
                if value:
                    file.write(" " * ((depth + 1) * 4))
                    file.write(f"{key}: {value}\n")
        for name in self.sorted_names():
            if name_exclude_fn is None or not name_exclude_fn(name):
                file.write(name.get_description(depth=depth + 1, full=full))
        if show_occurrences:
            for occurrence in self.sorted_occurrences():
                file.write(" " * ((depth + 1) * 4))
                file.write(f"{occurrence!r}\n")
        if self in exclude:
            return
        if max_depth is None or max_depth > 0:
            new_max_depth = None if max_depth is None else max_depth - 1
            children = list(self.get_children())
            if self.base_name.status == Status.valid:
                valid_children = []
                dubious_children = []
                for child in children:
                    if child.base_name.status == Status.valid:
                        valid_children.append(child)
                    elif exclude_fn is None or not exclude_fn(child):
                        dubious_children.append(child)
                self._display_children(
                    valid_children,
                    full=full,
                    max_depth=new_max_depth,
                    file=file,
                    depth=depth + 1,
                    exclude=exclude,
                    exclude_fn=exclude_fn,
                    name_exclude_fn=name_exclude_fn,
                    show_occurrences=show_occurrences,
                )
                if dubious_children:
                    file.write(
                        " " * ((depth + 1) * 4) + f"Dubious ({self.valid_name}):\n"
                    )
                    self._display_children(
                        dubious_children,
                        full=full,
                        max_depth=new_max_depth,
                        file=file,
                        depth=depth + 2,
                        exclude=exclude,
                        exclude_fn=exclude_fn,
                        name_exclude_fn=name_exclude_fn,
                        show_occurrences=show_occurrences,
                    )
            else:
                self._display_children(
                    children,
                    full=full,
                    max_depth=new_max_depth,
                    file=file,
                    depth=depth + 1,
                    exclude=exclude,
                    exclude_fn=exclude_fn,
                    name_exclude_fn=name_exclude_fn,
                    show_occurrences=show_occurrences,
                )

    def _display_children(
        self,
        children: list[Taxon],
        full: bool,
        max_depth: int | None,
        file: IO[str],
        depth: int,
        exclude: Container[Taxon],
        exclude_fn: Callable[[Taxon], bool] | None,
        name_exclude_fn: Callable[[models.Name], bool] | None,
        show_occurrences: bool,
    ) -> None:
        for child in sorted(children, key=lambda t: (t.rank, t.valid_name)):
            child.display(
                file=file,
                depth=depth,
                max_depth=max_depth,
                full=full,
                exclude=exclude,
                exclude_fn=exclude_fn,
                name_exclude_fn=name_exclude_fn,
                show_occurrences=show_occurrences,
            )

    def display_parents(
        self, max_depth: int | None = 10, file: IO[str] = sys.stdout
    ) -> None:
        if max_depth == 0:
            return
        if max_depth is not None:
            max_depth -= 1
        if self.parent is not None:
            self.parent.display_parents(max_depth=max_depth, file=file)

        file.write(f"{self.rank.name} {self.full_name()} ({self.age.name})\n")
        file.write(self.base_name.get_description(depth=1))

    def get_citation_groups(self) -> dict[models.CitationGroup, list[models.Name]]:
        nams = self.all_names()
        by_cg: dict[models.CitationGroup, list[models.Name]] = defaultdict(list)
        for nam in nams:
            if nam.citation_group is not None:
                by_cg[nam.citation_group].append(nam)
        return by_cg

    def display_citation_groups(self) -> None:
        by_cg = self.get_citation_groups()
        items = sorted(by_cg.items(), key=lambda pair: pair[0].name)
        for cg, nams in items:
            getinput.print_header(f"{cg} ({len(nams)})")
            for nam in sorted(
                nams,
                key=lambda nam: (nam.get_date_object(), nam.numeric_page_described()),
            ):
                print(f"    {nam}")
                if nam.verbatim_citation:
                    print(f"        {helpers.clean_string(nam.verbatim_citation)}")
        getinput.flush()

    def display_type_localities(
        self,
        full: bool = False,
        geographically: bool = False,
        region: models.Region | None = None,
        exclude: Container[Taxon] = frozenset(),
        file: IO[str] = sys.stdout,
    ) -> None:
        nams = self.all_names(exclude=exclude)
        by_locality: dict[models.Location, list[models.Name]] = defaultdict(list)
        for nam in nams:
            if nam.type_locality is not None:
                by_locality[nam.type_locality].append(nam)

        def display_locs(
            by_locality: dict[models.Location, list[models.Name]], depth: int = 0
        ) -> None:
            current_periods: tuple[models.Period | None, models.Period | None] = (
                None,
                None,
            )
            for loc, nams in sorted(
                by_locality.items(),
                key=lambda pair: (
                    models.period.period_sort_key(pair[0].min_period),
                    models.period.period_sort_key(pair[0].max_period),
                    pair[0].name,
                ),
            ):
                periods = (loc.max_period, loc.min_period)
                if periods != current_periods:
                    if loc.max_period == loc.min_period:
                        period_str = str(loc.min_period)
                    else:
                        period_str = f"{loc.max_period}–{loc.min_period}"
                    file.write(f"{' ' * depth}{period_str}\n")
                    current_periods = periods
                file.write(f"{' ' * (4 + depth)}{loc}\n")
                models.name.write_names(nams, full=full, depth=depth)
                getinput.flush()

        if geographically:
            by_region: dict[models.Region, dict[models.Location, list[models.Name]]] = (
                defaultdict(dict)
            )
            for loc, loc_nams in by_locality.items():
                by_region[loc.region][loc] = loc_nams

            region_to_children: dict[models.Region | None, set[models.Region]] = (
                defaultdict(set)
            )

            def add_region(region: models.Region) -> None:
                if region in region_to_children[region.parent]:
                    return
                region_to_children[region.parent].add(region)
                if region.parent is not None:
                    add_region(region.parent)

            def display_region(region: models.Region, depth: int) -> None:
                file.write(f"{' ' * depth}{region}\n")
                display_locs(by_region[region], depth=depth + 4)
                for child in sorted(
                    region_to_children[region], key=lambda child: child.name
                ):
                    display_region(child, depth=depth + 4)

            for region_with_locs in by_region:
                add_region(region_with_locs)

            if region is not None:
                display_region(region, 0)
            else:
                display_region(next(iter(region_to_children[None])), 0)
        else:
            if region is not None:
                by_locality = {
                    loc: nams
                    for loc, nams in by_locality.items()
                    if loc.region.has_parent(region)
                }
            display_locs(by_locality)

    def add_static(
        self,
        rank: Rank,
        name: str,
        year: None | str | int = None,
        age: AgeClass | None = None,
        **kwargs: Any,
    ) -> Taxon:
        if age is None:
            age = self.age
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self)
        kwargs["group"] = helpers.group_of_rank(rank)
        kwargs["root_name"] = helpers.root_name_of_name(name, rank)
        if "status" not in kwargs:
            kwargs["status"] = Status.valid
        name_obj = models.Name.create(taxon=taxon, year=year, **kwargs)
        taxon.base_name = name_obj
        return taxon

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        return {
            **callbacks,
            "from_paper": self.from_paper,
            "add_child": self.add,
            "syn_from_paper": self.syn_from_paper,
            "add_syn": self.add_syn,
            "switch_basename": self.switch_basename,
            "synonymize": self.synonymize,
            "synonymize_all_children": self.synonymize_all_children,
            "recompute_name": self.recompute_name,
            "display_type_localities": self.display_type_localities,
            "display_citation_groups": self.display_citation_groups,
            "display_parents": self.display_parents,
            "add_comment": lambda: self.base_name.add_comment(),
            "add_occurrence": self.add_occurrence,
            "edit_occurrence": self.edit_occurrence,
            "display_occurrences": lambda: self.display(
                full=False, show_occurrences=True
            ),
            "add_type_identical": lambda: self.base_name._add_type_identical_callback(),
            "stats": self.stats,
            "fill_citation_group": self.fill_citation_group,
            "fill_data_for_names": self.fill_data_for_names,
            "fill_field_for_names": self.fill_field_for_names,
            "names_missing_field": self.print_names_missing_field,
            "add_nominate": self.add_nominate,
            "edit_all_names": self.edit_all_names,
            "edit_all_children": self.edit_all_children,
            "make_parent_of_rank": self.make_parent_of_rank,
            "names_like": self.print_names_like,
        }

    def add(self) -> Taxon | None:
        rank = getinput.get_enum_member(
            Rank,
            default=Rank.genus if self.rank > Rank.genus else Rank.species,
            allow_empty=False,
        )
        name = self.getter("valid_name").get_one_key("name> ")
        if name is None:
            return None
        default = cast(AgeClass, self.age)
        age = getinput.get_enum_member(AgeClass, default=default)
        if age is None:
            return None
        status = getinput.get_enum_member(Status, default=Status.valid)
        if status is None:
            return None
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self)
        name_obj = models.Name.create(
            taxon=taxon,
            group=helpers.group_of_rank(rank),
            root_name=helpers.root_name_of_name(name, rank),
            status=status,
            nomenclature_status=NomenclatureStatus.available,
        )
        taxon.base_name = name_obj
        name_obj.fill_required_fields()
        return taxon

    def add_syn(
        self,
        root_name: str | None = None,
        year: None | int | str = None,
        original_name: str | None = None,
        original_citation: Article | None = None,
        page_described: None | int | str = None,
        status: Status = Status.synonym,
        nomenclature_status: NomenclatureStatus = NomenclatureStatus.available,
        interactive: bool = True,
        **kwargs: Any,
    ) -> models.Name | None:
        if root_name is None:
            root_name = models.Name.getter("root_name").get_one_key("root_name> ")
        if root_name is None:
            return None
        kwargs["root_name"] = root_name
        kwargs["year"] = year
        # included in the method signature so they autocomplete in shell
        kwargs["original_name"] = original_name
        kwargs["original_citation"] = original_citation
        kwargs["page_described"] = page_described
        kwargs["status"] = status
        kwargs["taxon"] = self
        kwargs["nomenclature_status"] = nomenclature_status
        if "group" not in kwargs:
            kwargs["group"] = self.base_name.group
        name = models.Name.create(**kwargs)
        if interactive:
            name.fill_required_fields()
        return name

    def add_type_identical(
        self,
        name: str,
        page_described: None | int | str = None,
        locality: models.Location | None = None,
        **kwargs: Any,
    ) -> Taxon:
        """Convenience method to add a type species described in the same paper as the genus."""
        return self.base_name.add_type_identical(
            name, page_described=page_described, locality=locality, **kwargs
        )

    def switch_basename(self, name: models.Name | None = None) -> None:
        if name is None:
            name = models.Name.getter(None).get_one()
            if name is None:
                return
        if name.taxon != self:
            print(f"{name} is not a synonym of {self}")
            return
        old_base = self.base_name
        name.status = old_base.status
        old_base.status = Status.synonym
        self.base_name = name
        self.recompute_name()

    def add_occurrence(
        self,
        location: models.Location | None = None,
        paper: Article | None = None,
        comment: str | None = None,
        status: OccurrenceStatus = OccurrenceStatus.valid,
    ) -> models.Occurrence:
        if location is None:
            location = models.Location.getter(None).get_one("location> ")
        if paper is None:
            paper = Article.getter(None).get_one("source> ")
        try:
            return models.Occurrence.create(
                taxon=self,
                location=location,
                source=paper,
                comment=comment,
                status=status,
            )
        except peewee.IntegrityError:
            print("DUPLICATE OCCURRENCE")
            return self.at(location)  # type: ignore

    def edit_occurrence(self) -> None:
        occs = {occ.location.name: occ for occ in self.occurrences}
        occ = getinput.get_with_completion(
            occs.keys(), "location> ", disallow_other=True
        )
        if occ is None:
            return
        occs[occ].edit()

    def syn_from_paper(
        self,
        root_name: str | None = None,
        paper: Article | None = None,
        page_described: None | int | str = None,
        status: Status = Status.synonym,
        group: Group | None = None,
        age: AgeClass | None = None,
        interactive: bool = True,
        **kwargs: Any,
    ) -> models.Name | None:
        if root_name is None:
            root_name = models.Name.getter("root_name").get_one_key("root_name> ")
        if root_name is None:
            return None
        if paper is None:
            paper = self.get_value_for_foreign_class("paper", Article)
        if paper is None:
            return None

        result = self.add_syn(
            root_name=root_name,
            author_tags=paper.author_tags,
            year=paper.year,
            original_citation=paper,
            page_described=page_described,
            status=status,
            age=age,
            interactive=False,
        )
        if result is None:
            return None
        if group is not None:
            kwargs["group"] = group
        result.s(**kwargs)
        if interactive:
            if not result.fill_required_fields():
                result.edit()
        return result

    def from_paper(
        self,
        rank: Rank | None = None,
        name: str | None = None,
        paper: Article | None = None,
        page_described: None | int | str = None,
        status: Status = Status.valid,
        **override_kwargs: Any,
    ) -> Taxon | None:
        if rank is None:
            rank = getinput.get_enum_member(Rank, "rank> ")
        if rank is None:
            return None
        if name is None:
            if self.rank in (Rank.genus, Rank.species):
                default = self.valid_name
            else:
                default = ""
            name = self.getter("valid_name").get_one_key("name> ", default=default)
        if name is None:
            return None

        if paper is None:
            paper = self.get_value_for_foreign_class("paper", Article)
        if paper is None:
            return None

        default = cast(AgeClass, self.age)
        age = getinput.get_enum_member(AgeClass, default=default)
        if age is None:
            return None

        result = self.add_static(
            rank=rank,
            name=name,
            original_citation=paper,
            page_described=page_described,
            original_name=name,
            author_tags=paper.author_tags,
            year=paper.year,
            parent=self,
            status=status,
            age=age,
        )
        result.base_name.s(**override_kwargs)
        if not result.base_name.fill_required_fields():
            result.base_name.edit()
        return result

    def add_nominate(self) -> Taxon:
        if self.rank == Rank.species:
            rank = Rank.subspecies
        elif self.rank == Rank.genus:
            rank = Rank.subgenus
        elif self.rank == Rank.tribe:
            rank = Rank.subtribe
        elif self.rank == Rank.subfamily:
            rank = Rank.tribe
        elif self.rank == Rank.family:
            rank = Rank.subfamily
        elif self.rank == Rank.superfamily:
            rank = Rank.family
        else:
            assert False, "Cannot add nominate subtaxon of {} of rank {}".format(
                self, self.rank.name
            )

        base_name = self.base_name
        taxon = Taxon.make_or_revalidate(rank, base_name, self.age, self)
        base_name.taxon = taxon
        taxon.recompute_name()
        return taxon

    def edit_all_names(self) -> None:
        for nam in self.sorted_names():
            nam.display()
            nam.edit()

    def edit_all_children(self) -> None:
        for child in self.sorted_children():
            child.display()
            child.edit()

    def syn(self, name: str | None = None, **kwargs: Any) -> models.Name | None:
        """Find a synonym matching the given arguments."""
        if name is not None:
            kwargs["root_name"] = name
        for candidate in self.sorted_names():
            for key, value in kwargs.items():
                if getattr(candidate, key) != value:
                    break
            else:
                return candidate
        return None

    def open_description(self) -> bool:
        return self.base_name.open_description()

    def is_nominate_subgenus(self) -> bool:
        if self.rank is not Rank.subgenus:
            return False
        genus = self.parent_of_rank(Rank.genus)
        return genus.base_name == self.base_name

    def compute_valid_name(self) -> str:
        name = self.base_name
        if name is None:
            raise models.Name.DoesNotExist(
                "Taxon with id %d has an invalid base_name" % self.id
            )
        if self.rank == Rank.division:
            return "%s Division" % name.root_name
        elif self.is_nominate_subgenus():
            return f"{name.root_name} ({name.root_name})"
        group: Group = name.group
        # TODO there seems to be no way to combine these ifs and still make
        # both mypy and pyanalyze accept the assert_never.
        if group is Group.genus:
            return name.root_name
        elif group is Group.high:
            return name.root_name
        elif group is Group.family:
            return name.root_name + helpers.suffix_of_rank(self.rank)
        elif group is Group.species:
            if name.status is not Status.valid:
                if name.corrected_original_name is not None:
                    return name.get_default_valid_name()
                else:
                    return self.valid_name
            logical_genus = self.get_logical_genus()
            if logical_genus is not None:
                # Happy path: valid name within a genus
                return self._valid_name_of_species(logical_genus.root_name, name)
            nominal_genus = self.get_nominal_genus()
            if nominal_genus is not None:
                return self._valid_name_of_species(f'"{nominal_genus.root_name}"', name)
            if name.corrected_original_name is not None:
                return name.get_default_valid_name()
            else:
                return self.valid_name
        else:
            assert_never(group)

    def get_logical_genus(self) -> models.Name | None:
        try:
            logical_genus = self.parent_of_rank(Rank.genus)
        except ValueError:
            return None
        else:
            return logical_genus.base_name

    def get_nominal_genus(self) -> models.Name | None:
        for tag in self.get_tags(self.tags, models.tags.TaxonTag.NominalGenus):
            return tag.genus
        return None

    def _valid_name_of_species(self, genus: str, name: models.Name) -> str:
        if self.rank == Rank.species_group:
            return f"{genus} ({name.root_name})"
        elif self.rank == Rank.species:
            return f"{genus} {name.root_name}"
        else:
            assert self.rank == Rank.subspecies, f"Unexpected rank {self.rank.name}"
            species = self.parent_of_rank(Rank.species)
            return f"{genus} {species.base_name.root_name} {name.root_name}"

    def expected_base_name(self) -> models.Name | None:
        """Finds the name that is expected to be the base name for this name."""
        if self.base_name.nomenclature_status == NomenclatureStatus.informal:
            return self.base_name
        names = set(self.get_names())
        if self.base_name.taxon != self:
            names |= set(self.base_name.taxon.names)
        group = self.base_name.group
        available_names = {
            nam
            for nam in names
            if nam.nomenclature_status == NomenclatureStatus.available
            and nam.group == group
        }
        if available_names:
            names = available_names
        if not names:
            return None
        names_and_dates = sorted(
            [(nam, nam.get_date_object()) for nam in names], key=lambda pair: pair[1]
        )
        selected_pair = names_and_dates[0]
        if selected_pair[0] != self.base_name:
            possible = {
                nam for nam, date in names_and_dates if date == selected_pair[1]
            }
            if self.base_name in possible:
                # If there are multiple names from the same year, assume we got the priority right
                return self.base_name
        return selected_pair[0]

    def check_expected_base_name(self) -> bool:
        expected = self.expected_base_name()
        if expected != self.base_name:
            print(f"{self}: expected {expected} but have {self.base_name}")
            return False
        else:
            return True

    def check_base_names(self) -> Iterable[Taxon]:
        if not self.check_expected_base_name():
            yield self
        for child in self.get_children():
            yield from child.check_base_names()

    def recompute_name(self) -> None:
        new_name = self.compute_valid_name()
        if new_name != self.valid_name and new_name is not None:
            print(f"Changing valid name: {self.valid_name} -> {new_name}")
            self.valid_name = new_name

    def merge(self, into: Taxon) -> None:
        for child in self.get_children():
            child.parent = into
        for nam in self.get_names():
            if nam != self.base_name:
                nam.taxon = into

        self._merge_fields(into, exclude={"id", "base_name"})
        self.base_name.merge(into.base_name, allow_valid=True)
        self.parent = into
        self.age = AgeClass.redirect  # type: ignore

    def synonymize(self, to_taxon: Taxon | None = None) -> models.Name:
        if to_taxon is None:
            to_taxon = Taxon.getter(None).get_one()
            if to_taxon is None:
                return self.base_name
        if self.data is not None:
            print("Warning: removing data: %s" % self.data)
        if self == to_taxon:
            print(f"Cannot synonymize {self} with itself")
            return self.base_name
        original_to_status = to_taxon.base_name.status
        for child in self.get_children():
            child.parent = to_taxon
        nam = self.base_name
        if nam != to_taxon.base_name:
            nam.status = Status.synonym
        for name in self.get_names():
            name.taxon = to_taxon
        for occ in self.occurrences:
            comment = occ.comment
            try:
                occ.taxon = to_taxon
                occ.add_comment("Previously under _%s_." % self.name)
            except peewee.IntegrityError:
                print("dropping duplicate occurrence %s" % occ)
                existing = to_taxon.at(occ.location)
                additional_comment = "Also under _{}_ with source {{{}}}.".format(
                    self.name, occ.source
                )
                if comment is not None:
                    additional_comment += " " + comment
                existing.add_comment(additional_comment)
        to_taxon = to_taxon.reload()
        to_taxon.base_name.status = original_to_status
        self.age = AgeClass.redirect  # type: ignore
        self.parent = to_taxon
        return models.Name.get(models.Name.id == nam.id)

    def synonymize_all_children(self) -> None:
        self.display()
        if not getinput.yes_no("Synonymize all? "):
            return
        for taxon in self.children:
            print(taxon)
            taxon.synonymize(self)

    def make_species_group(self) -> Taxon | None:
        return self.make_parent_of_rank(Rank.species_group)

    def make_parent_of_rank(self, rank: Rank | None = None) -> Taxon | None:
        if rank is None:
            rank = getinput.get_enum_member(Rank, "rank> ")
            if rank is None:
                return None
        if self.parent is not None and self.parent.rank == rank:
            parent = self.parent.parent
        else:
            parent = self.parent
        assert parent is not None, "found no parent to attach"
        new_taxon = self.make_or_revalidate(rank, self.base_name, self.age, parent)
        new_taxon.recompute_name()
        self.parent = new_taxon
        return new_taxon

    @classmethod
    def make_or_revalidate(
        cls, rank: Rank, base_name: models.Name, age: AgeClass, parent: Taxon
    ) -> Taxon:
        try:
            existing = (
                cls.select().filter(cls.rank == rank, cls.base_name == base_name).get()
            )
        except cls.DoesNotExist:
            return cls.create(rank=rank, base_name=base_name, age=age, parent=parent)
        else:
            existing.age = age
            existing.parent = parent
            return existing

    def run_on_self_and_children(self, callback: Callable[[Taxon], object]) -> None:
        callback(self)
        for child in self.get_children():
            child.run_on_self_and_children(callback)

    def remove(self, reason: str | None = None, *, remove_names: bool = True) -> None:
        for _ in self.get_children():
            print("Cannot remove %s since it has unremoved children" % self)
            return
        print("Removing taxon %s" % self)
        if remove_names:
            for name in self.sorted_names():
                name.remove(reason=reason)
        self.age = AgeClass.removed  # type: ignore
        if reason is not None:
            self.data = reason

    def all_names(
        self,
        age: AgeClass | None = None,
        exclude: Container[Taxon] = frozenset(),
        min_year: int | None = None,
    ) -> set[models.Name]:
        if self in exclude:
            return set()
        names: set[models.Name]
        if age is not None:
            if self.age > age:
                return set()
            elif self.age == age:
                names = set(self.get_names())
            else:
                names = set()
        else:
            names = set(self.get_names())
        if min_year is not None:
            names = {nam for nam in names if nam.numeric_year() >= min_year}
        for child in self.get_children():
            names |= child.all_names(age=age, exclude=exclude, min_year=min_year)
        return names

    def all_authors(
        self,
        age: AgeClass | None = None,
        exclude: Container[Taxon] = frozenset(),
        min_year: int | None = None,
    ) -> set[models.Person]:
        nams = self.all_names(age=age, exclude=exclude, min_year=min_year)
        return {author for nam in nams for author in nam.get_authors()}

    def reassign_family_name_authors(self) -> None:
        for author in sorted(self.all_authors(), key=lambda p: p.sort_key()):
            if author.get_level() is not models.person.PersonLevel.family_name_only:
                continue
            getinput.print_header(author)
            author.reassign_names_with_verbatim(True)

    def names_missing_field(
        self,
        field: str,
        age: AgeClass | None = None,
        min_year: int | None = None,
        exclude: Container[Taxon] = frozenset(),
    ) -> set[models.Name]:
        return {
            name
            for name in self.all_names(age=age, min_year=min_year, exclude=exclude)
            if getattr(name, field) is None and field in name.get_required_fields()
        }

    def print_names_missing_field(self) -> None:
        field = getinput.get_with_completion(
            models.Name.get_field_names(),
            message="field> ",
            history_key=(type(self), "fill_field_for_names"),
            disallow_other=True,
        )
        if field is None:
            return
        nams = self.names_missing_field(field)
        for nam in sorted(nams, key=lambda nam: nam.sort_key()):
            nam.display(full=False)

    def stats(
        self,
        age: AgeClass | None = None,
        graphical: bool = False,
        focus_field: str | None = None,
        exclude: Container[Taxon] = frozenset(),
        min_year: int | None = None,
    ) -> dict[str, float]:
        names = self.all_names(age=age, min_year=min_year, exclude=exclude)
        counts: dict[str, int] = defaultdict(int)
        required_counts: dict[str, int] = defaultdict(int)
        counts_by_group: dict[Group, int] = defaultdict(int)
        for name in names:
            counts_by_group[name.group] += 1
            deprecated = set(name.get_deprecated_fields())
            required = set(name.get_required_fields())
            for field in required | deprecated:
                if field in deprecated:
                    required_counts[field] += 1
                    if getattr(name, field) is None:
                        counts[field] += 1
                else:
                    required_counts[field] += 1
                    if getattr(name, field) is not None:
                        counts[field] += 1

        total = len(names)
        output: dict[str, Any] = {"total": total}
        if focus_field is None:
            by_group = ", ".join(
                f"{v.name}: {counts_by_group[v]}" for v in reversed(Group)
            )
            print(f"Total names: {total} ({by_group})")

        def print_percentage(num: int, total: int, label: str) -> float:
            if total == 0 or num == total:
                return 100.0
            return num * 100.0 / total

        def sort_key(pair: tuple[str, int]) -> tuple[float, int]:
            attribute, total = pair
            count = counts[attribute]
            if total == 0 or count == total:
                return (100.0, total)
            else:
                percentage = count * 100.0 / total
                return (percentage, total)

        overall_count = 0
        overall_required = 0
        graphical_data = []
        for attribute, required_count in sorted(required_counts.items(), key=sort_key):
            count = counts[attribute]
            percentage = print_percentage(count, required_count, attribute)
            if focus_field is None or focus_field == attribute:
                if graphical:
                    graphical_data.append((attribute, percentage / 100))
                elif percentage < 100:
                    print(
                        f"{attribute}: {count} of {required_count} ({percentage:.2f}%)"
                    )
            output[attribute] = (percentage, count, required_count)
            overall_required += required_count
            overall_count += count
        if overall_required:
            score = overall_count / overall_required * 100
            if graphical and focus_field == "score":
                graphical_data.append(("score", score))
            output["score"] = (score, overall_required, overall_count)
        else:
            output["score"] = (0.0, overall_required, overall_count)
        if graphical:
            getinput.print_scores(graphical_data)
        if focus_field is None:
            print(f'Overall score: {output["score"][0]:.2f}')
        return output

    def edit_names_at_level(
        self,
        level: FillDataLevel = FillDataLevel.incomplete_derived_tags,
        age: AgeClass | None = None,
        reverse: bool = True,
    ) -> None:
        nams = self.all_names(age=age)
        total = len(nams)
        for i, nam in enumerate(
            sorted(nams, reverse=reverse, key=lambda nam: nam.sort_key())
        ):
            print(f"({i}/{total}) {nam}")
            name_level = nam.get_derived_field("fill_data_level")
            if name_level is level:
                name_level = nam.get_derived_field(
                    "fill_data_level", force_recompute=True
                )
            if name_level is level:
                nam.display()
                level, reason = nam.fill_data_level()
                print(f"Level: {level.name.upper()} ({reason})")
                nam.edit()

    def fill_data_for_names(
        self,
        *,
        only_with_original: bool = True,
        min_year: int | None = None,
        age: AgeClass | None = None,
        field: str | None = None,
        level: FillDataLevel = DEFAULT_LEVEL,
        ask_before_opening: bool = True,
        only_fill_cache: bool = False,
        filter_by_name_level: bool = False,
        skip_nofile: bool = True,
    ) -> None:
        """Calls fill_required_fields() for all names in this taxon."""
        all_names = self.all_names(age=age)
        fill_data_for_names(
            all_names,
            min_year=min_year,
            field=field,
            level=level,
            ask_before_opening=ask_before_opening,
            only_fill_cache=only_fill_cache,
            filter_by_name_level=filter_by_name_level,
            skip_nofile=skip_nofile,
        )

        if not only_with_original:
            for nam in self.all_names(age=age):
                nam = nam.reload()
                if nam.original_citation is None:
                    print(nam)
                    nam.fill_required_fields()

    def fill_field_for_names(
        self,
        field: str | None = None,
        exclude: Container[Taxon] = frozenset(),
        min_year: int | None = None,
    ) -> None:
        if field is None:
            field = getinput.get_with_completion(
                models.Name.get_field_names(),
                message="field> ",
                history_key=(type(self), "fill_field_for_names"),
                disallow_other=True,
            )
        if field is None:
            return

        for name in sorted(
            self.all_names(exclude=exclude, min_year=min_year),
            key=lambda nam: (nam.taxonomic_authority(), nam.year or ""),
        ):
            name = name.reload()
            name.fill_field_if_empty(field)

    def fill_citation_group(self, age: AgeClass | None = None) -> None:
        for name in sorted(
            self.all_names(age=age),
            key=lambda nam: (
                nam.taxonomic_authority(),
                nam.get_date_object(),
                nam.numeric_page_described(),
            ),
        ):
            name = name.reload()
            if name.verbatim_citation is not None and name.citation_group is None:
                name.possible_citation_groups()
                print("=== name")
                name.display()
                name.fill_field("citation_group")

    def count_attribute(
        self, field: str = "type_locality", age: AgeClass | None = None
    ) -> Counter[Any]:
        nams = self.all_names(age=age)
        return Counter(getattr(nam, field) for nam in nams)

    at = _OccurrenceGetter()

    def __str__(self) -> str:
        return self.valid_name

    def __repr__(self) -> str:
        return str(self)

    def __getattr__(self, attr: str) -> models.Name:
        """Returns a name belonging to this taxon with the given root_name or original_name."""
        if attr.startswith("_"):
            raise AttributeError(attr)
        candidates = [
            name
            for name in self.sorted_names()
            if name.root_name == attr or name.original_name == attr
        ]
        if len(candidates) == 1:
            return candidates[0]
        elif not candidates:
            raise AttributeError(attr)
        else:
            nam = getinput.choose_one(
                candidates,
                display_fn=lambda nam: f"{nam!r} (#{nam.id})",
                history_key=(self, attr),
            )
            if nam is None:
                raise AttributeError(attr)
            return nam

    def __dir__(self) -> list[str]:
        result = set(super().__dir__())
        names = self.sorted_names()
        result |= {name.original_name for name in names}
        result |= {name.root_name for name in names}
        return [name for name in result if name is not None and " " not in name]


definition.taxon_cls = Taxon


@lru_cache(maxsize=2048)
def ranked_parents(
    txn: Taxon | None,
) -> tuple[Taxon | None, Taxon | None, Taxon | None]:
    """Returns the class-level, order-level and family-level parents of the taxon.

    The family-level parent is the one parent of family rank. The order-level parent
    is of rank order if there is one, and otherwise the first unranked taxon above the
    highest-ranked family-group taxon.

    """
    if txn is None:
        return (None, None, None)
    rank = txn.rank
    if rank is Rank.class_:
        return (txn, None, None)
    if rank > Rank.class_ and rank != Rank.unranked:
        return (txn, None, None)
    parent_class, parent_order, parent_family = ranked_parents(txn.parent)
    if rank is Rank.unranked:
        if parent_family is not None:
            return (parent_class, parent_order, parent_family)
        elif parent_class is None:
            return (txn, None, None)
        elif parent_order is None:
            return (parent_class, txn, None)
        else:
            return (parent_class, parent_order, txn)
    elif rank >= Rank.order:
        return (parent_class, txn, None)
    elif rank >= Rank.family:
        return (parent_class, parent_order, txn)
    elif rank > Rank.superfamily:
        if parent_family is None and (
            parent_order is None or parent_order.rank is not Rank.order
        ):
            return (parent_class, txn, None)
        else:
            return (parent_class, parent_order, parent_family)
    else:
        return (parent_class, parent_order, parent_family)


def display_organized(
    data: Sequence[tuple[str, Taxon]], depth: int = 0, file: IO[str] = sys.stdout
) -> None:
    labeled_data = [(text, taxon, ranked_parents(taxon)) for text, taxon in data]
    labeled_data = sorted(
        labeled_data,
        key=lambda item: (
            "" if item[2][0] is None else item[2][0].valid_name,
            "" if item[2][1] is None else item[2][1].valid_name,
            "" if item[2][2] is None else item[2][2].valid_name,
            item[1].valid_name,
        ),
    )
    current_class = None
    current_order = None
    current_family = None
    for text, _, (class_, order, family) in labeled_data:
        if class_ != current_class:
            current_class = class_
            if class_ is not None:
                file.write(f"{' ' * (depth + 8)}{class_}\n")
        if order != current_order:
            current_order = order
            if order is not None:
                file.write(f"{' ' * (depth + 12)}{order}\n")
        if family != current_family:
            current_family = family
            if family is not None:
                file.write(f"{' ' * (depth + 16)}{family}\n")
        file.write(getinput.indent(text, depth + 20))
