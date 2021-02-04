from collections import defaultdict
from functools import lru_cache
import operator
import re
import sys
from typing import (
    cast,
    IO,
    Any,
    Callable,
    Container,
    Counter,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import peewee
from peewee import BooleanField, CharField, ForeignKeyField, TextField

from .. import definition, helpers, models
from ..derived_data import DerivedField, SetLater
from ... import events, getinput
from ..constants import (
    AgeClass,
    Group,
    NomenclatureStatus,
    OccurrenceStatus,
    Rank,
    Status,
    ArticleKind,
    FillDataLevel,
)

from .base import BaseModel, EnumField
from .article import Article, ArticleTag


DEFAULT_LEVEL = FillDataLevel.needs_basic_data


class _OccurrenceGetter(object):
    """For easily accessing occurrences of a taxon.

    This is exposed at taxon.at. You can access taxa as either taxon.at.Locality_Name or taxon.at(L.Locality_Name).

    """

    def __init__(self, instance: Any = None) -> None:
        self.instance = instance

    def __get__(self, instance: Any, instance_type: Any) -> "_OccurrenceGetter":
        return self.__class__(instance)

    def __getattr__(self, loc_name: str) -> "models.Occurrence":
        return self(
            models.Location.get(
                models.Location.name == loc_name.replace("_", " "),
                models.Location.deleted == False,
            )
        )

    def __call__(self, loc: "models.Location") -> "models.Occurrence":
        return self.instance.occurrences.filter(models.Occurrence.location == loc).get()

    def __dir__(self) -> List[str]:
        return [o.location.name.replace(" ", "_") for o in self.instance.occurrences]


def _make_parent_getter(index: int) -> Any:
    def _get_ranked_parent(taxon: "Taxon") -> Optional["Taxon"]:
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

    derived_fields = [
        DerivedField("class_", SetLater, _make_parent_getter(0)),
        DerivedField("order", SetLater, _make_parent_getter(1)),
        DerivedField("family", SetLater, _make_parent_getter(2)),
    ]

    class Meta(object):
        db_table = "taxon"

    name = property(lambda self: self.base_name)

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Taxon.age != AgeClass.removed)

    def should_skip(self) -> bool:
        return self.age is AgeClass.removed

    def group(self) -> Group:
        return helpers.group_of_rank(self.rank)

    def get_names(self) -> Iterable["models.Name"]:
        return self.names.filter(models.Name.status != Status.removed)

    def sorted_names(self, exclude_valid: bool = False) -> List["models.Name"]:
        names: Iterable[models.Name] = self.get_names()
        if exclude_valid:
            names = filter(lambda name: name.status != Status.valid, names)

        def sort_key(nam: "models.Name") -> Tuple[bool, str, str]:
            return (
                nam.status not in (Status.valid, Status.nomen_dubium),
                nam.root_name,
                (nam.year or ""),
            )

        return sorted(names, key=sort_key)

    def get_children(self) -> Iterable["Taxon"]:
        return self.children.filter(Taxon.age != AgeClass.removed)

    def sorted_children(self) -> List["Taxon"]:
        return sorted(
            self.get_children(), key=operator.attrgetter("rank", "valid_name")
        )

    def sorted_occurrences(self) -> List["models.Occurrence"]:
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
        if self.rank == Rank.subgenus:
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

    def parent_of_rank(
        self, rank: Rank, original_taxon: Optional["Taxon"] = None
    ) -> "Taxon":
        if original_taxon is None:
            original_taxon = self
        if self.rank > rank and self.rank != Rank.unranked:
            raise ValueError(
                "%s (id = %s) has no ancestor of rank %s"
                % (original_taxon, original_taxon.id, rank.name)
            )
        elif self.rank == rank:
            return self
        else:
            return self.parent.parent_of_rank(rank, original_taxon=original_taxon)

    def has_parent_of_rank(self, rank: Rank) -> bool:
        try:
            self.parent_of_rank(rank)
        except ValueError:
            return False
        else:
            return True

    def is_child_of(self, taxon: "Taxon") -> bool:
        if self == taxon:
            return True
        elif self.parent is None:
            return False
        else:
            return self.parent.is_child_of(taxon)

    def children_of_rank(
        self, rank: Rank, age: Optional[AgeClass] = None
    ) -> List["Taxon"]:
        if self.rank < rank:
            return []
        elif self.rank == rank:
            if age is None or self.age == age:
                return [self]
            else:
                return []
        else:
            out: List[Taxon] = []
            for child in self.get_children():
                out += child.children_of_rank(rank, age=age)
            return out

    def names_like(self, root_name: str) -> List["models.Name"]:
        """Find names matching root_name within this taxon."""
        pattern = re.compile(root_name)
        nams = self.all_names()
        return [nam for nam in nams if pattern.match(nam.root_name)]

    def find_names(
        self, root_name: str, group: Optional[Group] = None, fuzzy: bool = True
    ) -> List["models.Name"]:
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

    def display_extant(self) -> None:
        self.display(
            exclude_fn=lambda t: t.age != AgeClass.extant
            or t.base_name.status != Status.valid,
            name_exclude_fn=lambda n: n.status == Status.synonym,
        )

    def display(
        self,
        full: bool = False,
        max_depth: Optional[int] = None,
        file: IO[str] = sys.stdout,
        depth: int = 0,
        exclude: Container["Taxon"] = set(),
        exclude_fn: Optional[Callable[["Taxon"], bool]] = None,
        name_exclude_fn: Optional[Callable[["models.Name"], bool]] = None,
        show_occurrences: Optional[bool] = None,
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
                file.write("%s\n" % (occurrence if full else occurrence.location))
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
        children: List["Taxon"],
        full: bool,
        max_depth: Optional[int],
        file: IO[str],
        depth: int,
        exclude: Container["Taxon"],
        exclude_fn: Optional[Callable[["Taxon"], bool]],
        name_exclude_fn: Optional[Callable[["models.Name"], bool]],
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
        self, max_depth: Optional[int] = None, file: IO[str] = sys.stdout
    ) -> None:
        if max_depth == 0:
            return
        if max_depth is not None:
            max_depth -= 1
        if self.parent is not None:
            self.parent.display_parents(max_depth=max_depth, file=file)

        file.write(
            "{} {} ({})\n".format(self.rank.name, self.full_name(), self.age.name)
        )
        file.write(self.base_name.get_description(depth=1))

    def get_citation_groups(self) -> Dict["models.CitationGroup", List["models.Name"]]:
        nams = self.all_names()
        by_cg: Dict[models.CitationGroup, List[models.Name]] = defaultdict(list)
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
                nams, key=lambda nam: (nam.numeric_year(), nam.numeric_page_described())
            ):
                print(f"    {nam}")
                print(f"        {helpers.clean_string(nam.verbatim_citation)}")
        getinput.flush()

    def display_type_localities(
        self,
        full: bool = False,
        geographically: bool = False,
        region: Optional["models.Region"] = None,
        exclude: Container["Taxon"] = frozenset(),
        file: IO[str] = sys.stdout,
    ) -> None:
        nams = self.all_names(exclude=exclude)
        by_locality: Dict[models.Location, List[models.Name]] = defaultdict(list)
        for nam in nams:
            if nam.type_locality is not None:
                by_locality[nam.type_locality].append(nam)

        def display_locs(
            by_locality: Dict[models.Location, List[models.Name]], depth: int = 0
        ) -> None:
            current_periods: Tuple[Optional[models.Period], Optional[models.Period]] = (
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
                models.name.write_type_localities(nams, full=full, depth=depth)
                getinput.flush()

        if geographically:
            by_region: Dict[
                models.Region, Dict[models.Location, List[models.Name]]
            ] = defaultdict(dict)
            for loc, loc_nams in by_locality.items():
                by_region[loc.region][loc] = loc_nams

            region_to_children: Dict[
                Optional[models.Region], Set[models.Region]
            ] = defaultdict(set)

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
        year: Union[None, str, int] = None,
        age: Optional[AgeClass] = None,
        **kwargs: Any,
    ) -> "Taxon":
        if age is None:
            age = self.age
        return self.base_name.add_child_taxon(
            rank=rank, name=name, year=year, age=age, **kwargs
        )

    def add(self) -> "Taxon":
        rank = getinput.get_enum_member(
            Rank,
            default=Rank.genus if self.rank > Rank.genus else Rank.species,
            allow_empty=False,
        )
        name = self.getter("valid_name").get_one_key("name> ", allow_empty=False)
        assert name is not None
        default = cast(AgeClass, self.age)
        age = getinput.get_enum_member(AgeClass, default=default, allow_empty=False)
        status = getinput.get_enum_member(
            Status, default=Status.valid, allow_empty=False
        )
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self)
        name_obj = models.Name.create(
            taxon=taxon,
            group=helpers.group_of_rank(rank),
            root_name=helpers.root_name_of_name(name, rank),
            status=status,
            nomenclature_status=NomenclatureStatus.available,
        )
        taxon.base_name = name_obj
        taxon.save()
        name_obj.fill_required_fields()
        return taxon

    def add_syn(
        self,
        root_name: Optional[str] = None,
        year: Union[None, int, str] = None,
        original_name: Optional[str] = None,
        original_citation: Optional[Article] = None,
        page_described: Union[None, int, str] = None,
        status: Status = Status.synonym,
        nomenclature_status: NomenclatureStatus = NomenclatureStatus.available,
        interactive: bool = True,
        **kwargs: Any,
    ) -> Optional["models.Name"]:
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
        page_described: Union[None, int, str] = None,
        locality: Optional["models.Location"] = None,
        **kwargs: Any,
    ) -> "Taxon":
        """Convenience method to add a type species described in the same paper as the genus."""
        return self.base_name.add_type_identical(
            name, page_described=page_described, locality=locality, **kwargs
        )

    def switch_basename(self, name: "models.Name") -> None:
        assert name.taxon == self, f"{name} is not a synonym of {self}"
        old_base = self.base_name
        name.status = old_base.status
        old_base.status = Status.synonym
        self.base_name = name
        self.recompute_name()

    def add_occurrence(
        self,
        location: "models.Location",
        paper: Optional[Article] = None,
        comment: Optional[str] = None,
        status: OccurrenceStatus = OccurrenceStatus.valid,
    ) -> "models.Occurrence":
        if paper is None:
            paper = self.base_name.original_citation
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
            return self.at(location)

    def syn_from_paper(
        self,
        root_name: Optional[str] = None,
        paper: Optional[Article] = None,
        page_described: Union[None, int, str] = None,
        status: Status = Status.synonym,
        group: Optional[Group] = None,
        age: Optional[AgeClass] = None,
        interactive: bool = True,
        **kwargs: Any,
    ) -> Optional["models.Name"]:
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
            result.fill_required_fields()
        return result

    def from_paper(
        self,
        rank: Optional[Rank] = None,
        name: Optional[str] = None,
        paper: Optional[Article] = None,
        page_described: Union[None, int, str] = None,
        status: Status = Status.valid,
        age: Optional[AgeClass] = None,
        **override_kwargs: Any,
    ) -> Optional["Taxon"]:
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
        result.base_name.fill_required_fields()
        return result

    def add_nominate(self) -> "Taxon":
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

        taxon = Taxon.create(age=self.age, rank=rank, parent=self)
        taxon.base_name = self.base_name
        taxon.base_name.taxon = taxon
        taxon.recompute_name()
        return taxon

    def syn(self, name: Optional[str] = None, **kwargs: Any) -> Optional["models.Name"]:
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

    def compute_valid_name(self) -> str:
        name = self.base_name
        if name is None:
            raise models.Name.DoesNotExist(
                "Taxon with id %d has an invalid base_name" % self.id
            )
        if self.rank == Rank.division:
            return "%s Division" % name.root_name
        elif name.group in (Group.genus, Group.high):
            return name.root_name
        elif name.group == Group.family:
            return name.root_name + helpers.suffix_of_rank(self.rank)
        else:
            assert name.group == Group.species
            if name.status != Status.valid:
                return name.corrected_original_name
            try:
                genus = self.parent_of_rank(Rank.genus)
            except ValueError:
                # if there is no genus, just use the original name
                # this may be one case where we can't rely on the computed valid name
                assert self.rank in (Rank.species, Rank.subspecies), (
                    "Taxon %s should have a genus parent" % self
                )
                # default to the corrected original name
                return name.corrected_original_name
            else:
                if self.rank == Rank.species_group:
                    return f"{genus.base_name.root_name} ({name.root_name})"
                elif self.rank == Rank.species:
                    return f"{genus.base_name.root_name} {name.root_name}"
                else:
                    assert self.rank == Rank.subspecies, (
                        "Unexpected rank %s" % self.rank.name
                    )
                    species = self.parent_of_rank(Rank.species)
                    return "{} {} {}".format(
                        genus.base_name.root_name,
                        species.base_name.root_name,
                        name.root_name,
                    )

    def expected_base_name(self) -> Optional["models.Name"]:
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
        names_and_years = sorted(
            [(nam, nam.effective_year()) for nam in names], key=lambda pair: pair[1]
        )
        selected_pair = names_and_years[0]
        if selected_pair[0] != self.base_name:
            possible = {
                nam for nam, year in names_and_years if year == selected_pair[1]
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

    def check_base_names(self) -> Iterable["Taxon"]:
        if not self.check_expected_base_name():
            yield self
        for child in self.get_children():
            yield from child.check_base_names()

    def recompute_name(self) -> None:
        new_name = self.compute_valid_name()
        if new_name != self.valid_name and new_name is not None:
            print(f"Changing valid name: {self.valid_name} -> {new_name}")
            self.valid_name = new_name
            self.save()

    def merge(self, into: "Taxon") -> None:
        for child in self.get_children():
            child.parent = into
            child.save()
        for nam in self.get_names():
            if nam != self.base_name:
                nam.taxon = into
                nam.save()

        self._merge_fields(into, exclude={"id", "_base_name_id"})
        self.base_name.merge(into.base_name, allow_valid=True)
        self.remove(reason=f"Merged into {into} (T#{into.id})")

    def synonymize(self, to_taxon: "Taxon") -> "models.Name":
        if self.data is not None:
            print("Warning: removing data: %s" % self.data)
        assert self != to_taxon, "Cannot synonymize %s with itself" % self
        original_to_status = to_taxon.base_name.status
        for child in self.get_children():
            child.parent = to_taxon
            child.save()
        nam = self.base_name
        nam.status = Status.synonym
        nam.save()
        for name in self.get_names():
            name.taxon = to_taxon
            name.save()
        for occ in self.occurrences:
            occ.taxon = to_taxon
            comment = occ.comment
            try:
                occ.add_comment("Previously under _%s_." % self.name)
                occ.save()
            except peewee.IntegrityError:
                print("dropping duplicate occurrence %s" % occ)
                existing = to_taxon.at(occ.location)
                additional_comment = "Also under _{}_ with source {{{}}}.".format(
                    self.name, occ.source
                )
                if comment is not None:
                    additional_comment += " " + comment
                existing.add_comment(additional_comment)
        to_taxon.base_name.status = original_to_status
        self.remove(reason=f"Synonymized into {to_taxon} (T#{to_taxon.id})")
        return models.Name.get(models.Name.id == nam.id)

    def make_species_group(self) -> "Taxon":
        return self.make_parent_of_rank(Rank.species_group)

    def make_parent_of_rank(self, rank: Rank) -> "Taxon":
        if self.parent.rank == rank:
            parent = self.parent.parent
        else:
            parent = self.parent
        new_taxon = Taxon.create(rank=rank, age=self.age, parent=parent)
        new_taxon.base_name = self.base_name
        new_taxon.recompute_name()
        self.parent = new_taxon
        self.save()
        return new_taxon

    def run_on_self_and_children(self, callback: Callable[["Taxon"], object]) -> None:
        callback(self)
        for child in self.get_children():
            child.run_on_self_and_children(callback)

    def remove(self, reason: Optional[str] = None) -> None:
        for _ in self.get_children():
            print("Cannot remove %s since it has unremoved children" % self)
            return
        print("Removing taxon %s" % self)
        for name in self.sorted_names():
            name.remove(reason=reason)
        self.age = AgeClass.removed  # type: ignore
        if reason is not None:
            self.data = reason
        self.save()

    def all_names(
        self,
        age: Optional[AgeClass] = None,
        exclude: Container["Taxon"] = frozenset(),
        min_year: Optional[int] = None,
    ) -> Set["models.Name"]:
        if self in exclude:
            return set()
        names: Set["models.Name"]
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

    def names_missing_field(
        self,
        field: str,
        age: Optional[AgeClass] = None,
        min_year: Optional[int] = None,
        exclude: Container["Taxon"] = frozenset(),
    ) -> Set["models.Name"]:
        return {
            name
            for name in self.all_names(age=age, min_year=min_year, exclude=exclude)
            if getattr(name, field) is None and field in name.get_required_fields()
        }

    def stats(
        self,
        age: Optional[AgeClass] = None,
        graphical: bool = False,
        focus_field: Optional[str] = None,
        exclude: Container["Taxon"] = frozenset(),
        min_year: Optional[int] = None,
    ) -> Dict[str, float]:
        names = self.all_names(age=age, min_year=min_year, exclude=exclude)
        counts: Dict[str, int] = defaultdict(int)
        required_counts: Dict[str, int] = defaultdict(int)
        counts_by_group: Dict[Group, int] = defaultdict(int)
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
        output: Dict[str, Any] = {"total": total}
        if focus_field is None:
            by_group = ", ".join(
                f"{v.name}: {counts_by_group[v]}" for v in reversed(Group)
            )
            print(f"Total names: {total} ({by_group})")

        def print_percentage(num: int, total: int, label: str) -> float:
            if total == 0 or num == total:
                return 100.0
            return num * 100.0 / total

        def sort_key(pair: Tuple[str, int]) -> Tuple[float, int]:
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

    def fill_data_for_names(
        self,
        only_with_original: bool = True,
        min_year: Optional[int] = None,
        age: Optional[AgeClass] = None,
        field: Optional[str] = None,
        level: FillDataLevel = DEFAULT_LEVEL,
        ask_before_opening: bool = True,
        only_fill_cache: bool = False,
        filter_by_name_level: bool = False,
        skip_nofile: bool = True,
    ) -> None:
        """Calls fill_required_fields() for all names in this taxon."""
        all_names = self.all_names(age=age)

        def should_include(nam: models.Name) -> bool:
            if nam.original_citation is None:
                return False
            if nam.original_citation.kind is ArticleKind.no_copy:
                return False
            if filter_by_name_level and _fill_data_level_for_name(nam, level) > level:
                return False
            if field is not None and (
                getattr(nam, field) is not None
                or field not in nam.get_required_fields()
            ):
                return False
            if min_year is not None:
                try:
                    year = int(nam.year)
                except (ValueError, TypeError):
                    return True
                return min_year <= year
            else:
                return True

        citations = sorted(
            {nam.original_citation for nam in all_names if should_include(nam)},
            key=lambda art: (art.path, art.name),
        )
        fill_data_from_articles(
            citations,
            level=level,
            ask_before_opening=ask_before_opening,
            only_fill_cache=only_fill_cache,
            skip_nofile=skip_nofile,
        )
        if not only_with_original:
            for nam in self.all_names(age=age):
                nam = nam.reload()
                if not should_include(nam):
                    print(nam)
                    nam.fill_required_fields()

    def fill_field_for_names(
        self,
        field: str,
        exclude: Container["Taxon"] = frozenset(),
        min_year: Optional[int] = None,
    ) -> None:
        for name in sorted(
            self.all_names(exclude=exclude, min_year=min_year),
            key=lambda nam: (nam.taxonomic_authority(), nam.year or ""),
        ):
            name = name.reload()
            name.fill_field_if_empty(field)

    def fill_citation_group(self, age: Optional[AgeClass] = None) -> None:
        for name in sorted(
            self.all_names(age=age),
            key=lambda nam: (
                nam.taxonomic_authority(),
                nam.numeric_year(),
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
        self, field: str = "type_locality", age: Optional[AgeClass] = None
    ) -> Counter[Any]:
        nams = self.all_names(age=age)
        return Counter(getattr(nam, field) for nam in nams)

    at = _OccurrenceGetter()

    def __str__(self) -> str:
        return self.valid_name

    def __repr__(self) -> str:
        return str(self)

    def __getattr__(self, attr: str) -> "models.Name":
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
                display_fn=lambda nam: f"{nam} (#{nam.id})",
                history_key=(self, attr),
            )
            if nam is None:
                raise AttributeError(attr)
            return nam

    def __dir__(self) -> List[str]:
        result = set(super().__dir__())
        names = self.sorted_names()
        result |= {name.original_name for name in names}
        result |= {name.root_name for name in names}
        return [name for name in result if name is not None and " " not in name]


definition.taxon_cls = Taxon

_finished_papers: Set[Tuple[str, FillDataLevel]] = set()


def _name_sort_key(nam: "models.Name") -> Tuple[str, int]:
    try:
        return ("", int(nam.page_described))
    except (TypeError, ValueError):
        return (nam.page_described or "", 0)


def _get_names(paper: Article) -> List["models.Name"]:
    return sorted(
        models.Name.filter(
            models.Name.original_citation == paper, models.Name.status != Status.removed
        ),
        key=_name_sort_key,
    )


def fill_data_from_paper(
    paper: Article,
    level: FillDataLevel = DEFAULT_LEVEL,
    only_fill_cache: bool = False,
    ask_before_opening: bool = False,
    finish_what_you_start: bool = True,
) -> bool:
    if (paper.name, level) in _finished_papers:
        return True
    if paper.has_tag(ArticleTag.NeedsTranslation):
        print(f"{paper.name}: skipping because of NeedsTranslation tag")
        _finished_papers.add((paper.name, level))
        return True

    opened = False
    if finish_what_you_start:
        goal_level = max(level, FillDataLevel.needs_specimen_data)
    else:
        goal_level = level

    nams = _get_names(paper)
    nams_below_level = [
        nam for nam in nams if _fill_data_level_for_name(nam, level) <= level
    ]
    if nams_below_level:
        print(f"{paper.name}: {len(nams_below_level)} names (fill_data_from_paper)")
        if ask_before_opening and not only_fill_cache:
            edit_names_interactive(paper)
            if paper.has_tag(ArticleTag.NeedsTranslation):
                print(f"{paper.name}: skipping because of NeedsTranslation tag")
                _finished_papers.add((paper.name, level))
                return True
            nams = _get_names(paper)

        for nam in nams:
            if only_fill_cache:
                opened = True
            else:
                nam = nam.reload()
                while _fill_data_level_for_name(nam, goal_level) <= goal_level:
                    nam.display()
                    if not opened:
                        getinput.add_to_clipboard(paper.name)
                        paper.openf()
                        paper.add_to_history()
                        print(f"filling data from {paper.name}")
                        paper.specify_authors()
                    opened = True
                    current_level, reason = nam.fill_data_level()
                    print(f"Level: {current_level.name.upper()} ({reason})")
                    if list(nam.get_empty_required_fields()):
                        print(nam, "described at", nam.page_described)
                        nam.fill_required_fields()
                    else:
                        nam.fill_field("type_tags")

    if not opened:
        _finished_papers.add((paper.name, level))
        return True
    return False


def fill_data_from_articles(
    arts: Sequence[Article],
    level: FillDataLevel,
    only_fill_cache: bool,
    ask_before_opening: bool = False,
    skip_nofile: bool = True,
    specify_authors: bool = False,
) -> None:
    total = len(arts)
    if total == 0:
        print("no articles found")
        return
    done = 0
    for i, art in enumerate(arts):
        percentage = (i / total) * 100
        print(f"{percentage:.03}% ({i}/{total}) {art.path}/{art.name}")
        getinput.flush()
        if not only_fill_cache and skip_nofile and not art.isfile():
            print("skipping NOFILE article")
            continue
        if not only_fill_cache and specify_authors:
            art.specify_authors()
        if fill_data_from_paper(
            art,
            level=level,
            only_fill_cache=only_fill_cache,
            ask_before_opening=ask_before_opening,
        ):
            done += 1
        elif not only_fill_cache:
            # Redo this to make sure we finished the paper.
            fill_data_from_paper(
                art,
                level=level,
                only_fill_cache=False,
                ask_before_opening=ask_before_opening,
            )
    print(f"{done}/{total} ({(done / total) * 100:.03}%) done")


def display_names(
    art: Article, *, full: bool = False, omit_if_done: bool = False
) -> None:
    print(repr(art))
    new_names = _get_names(art)
    if new_names:
        print(f"New names ({len(new_names)}):")
        levels = []
        for nam in new_names:
            level, reason = nam.fill_data_level()
            levels.append(level)
            if omit_if_done and level is FillDataLevel.nothing_needed:
                continue
            if full:
                nam.display(full=True)
                print(f"    Level: {level.name.upper()} ({reason})")
            else:
                desc = nam.get_description(include_taxon=True, full=False).rstrip()
                print(f"{desc} ({level.name.upper()}: {reason})")
        print("Current level:", min(levels).name.upper())


def edit_names_interactive(art: Article, field: str = "corrected_original_name") -> None:
    art.openf()
    art.add_to_history()
    art.specify_authors()
    while True:
        obj = models.Name.getter(field).get_one(
            prompt=f"{field}> ",
            callbacks={
                "o": art.openf,
                "d": lambda: display_names(art),
                "f": lambda: display_names(art, full=True),
                "t": lambda: display_names(art, omit_if_done=True),
                "edit": art.edit,
            },
        )
        if obj is None:
            break
        obj.display()
        level, reason = obj.fill_data_level()
        print(f"Level: {level.name.upper()} ({reason})")
        obj.edit()


def _fill_data_level_for_name(
    nam: "models.Name", desired_level: Optional[FillDataLevel] = None
) -> FillDataLevel:
    if desired_level is None:
        return nam.get_derived_field("fill_data_level", force_recompute=True)
    level = nam.get_derived_field("fill_data_level")
    if level <= desired_level:
        level = nam.get_derived_field("fill_data_level", force_recompute=True)
    return level


@lru_cache(maxsize=2048)
def ranked_parents(
    txn: Optional[Taxon],
) -> Tuple[Optional[Taxon], Optional[Taxon], Optional[Taxon]]:
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
    data: Sequence[Tuple[str, Taxon]], depth: int = 0, file: IO[str] = sys.stdout
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
