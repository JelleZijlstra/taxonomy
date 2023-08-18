from collections import Counter
from collections.abc import Sequence

from taxonomy.db.constants import AgeClass, Group, Rank, RegionKind
from taxonomy.db.export import get_names_for_export
from taxonomy.db.models import Article, Name, Taxon, TypeTag


def has_orig_location_detail(nam: Name) -> bool:
    if nam.original_citation is None:
        return False
    for tag in nam.type_tags:
        if (
            isinstance(tag, TypeTag.LocationDetail)
            and tag.source == nam.original_citation
        ):
            return True
    return False


def kind_of_tl(nam: Name) -> str:
    if nam.type_locality is None:
        return "none"
    loc = nam.type_locality
    if loc.min_period.name != "Recent":
        return "fossil site"
    if loc.region.kind is RegionKind.country:
        return "country"
    if loc.region.kind in (
        RegionKind.planet,
        RegionKind.continent,
        RegionKind.supranational,
    ):
        return "supranational"
    return "subnational"


def compute_report(nams: Sequence[Name]) -> None:
    requires_tl = [nam for nam in nams if "type_locality" in nam.get_required_fields()]
    lack_tl = [nam for nam in requires_tl if nam.type_locality is None]
    by_age: Counter[AgeClass] = Counter()
    by_family: Counter[str] = Counter()
    for nam in lack_tl:
        by_age[nam.taxon.age] += 1
        by_family[nam.taxon.get_derived_field("family").valid_name] += 1

    has_orig_citation = [
        nam for nam in requires_tl if nam.original_citation is not None
    ]
    has_some_citation = [
        nam
        for nam in requires_tl
        if nam.original_citation is not None or nam.verbatim_citation is not None
    ]
    has_loc_detail = [nam for nam in requires_tl if has_orig_location_detail(nam)]

    most_common_sources: Counter[Article] = Counter()
    highest_count = -1
    highest_count_name: Name | None = None
    for nam in requires_tl:
        count = 0
        for tag in nam.type_tags:
            if isinstance(tag, TypeTag.LocationDetail):
                most_common_sources[tag.source] += 1
                count += 1
        if count > highest_count:
            highest_count = count
            highest_count_name = nam

    tl_kind = Counter(kind_of_tl(nam) for nam in requires_tl)
    most_common_tls = Counter(
        nam.type_locality for nam in requires_tl if nam.type_locality is not None
    )

    print(f"{len(nams)} total names")
    print(
        f"{len(nams) - len(requires_tl)} do not require a type locality (e.g.,"
        " incorrect subsequent spellings)"
    )
    print("These are excluded from the counts below.")
    print(
        f"{len(lack_tl)} ({len(lack_tl) / len(requires_tl) * 100:.2f}%) lack a type"
        " locality"
    )
    print("By age:")
    for age, count in by_age.most_common():
        print(f"- {age.name}: {count}")
    print("By family:")
    for family, count in by_family.most_common():
        print(f"- {family}: {count}")
    print(
        f"{len(has_some_citation)} ({len(has_some_citation) / len(requires_tl) * 100:.2f}%)"
        " have citation information"
    )
    print(
        f"{len(has_orig_citation)} ({len(has_orig_citation) / len(requires_tl) * 100:.2f}%)"
        " have a verified original citation"
    )
    print(
        f"{len(has_loc_detail)} ({len(has_loc_detail) / len(requires_tl) * 100:.2f}%)"
        " have location information from the original description"
    )
    print("Most common sources for type localities:")
    for source, count in most_common_sources.most_common(25):
        print(f"- {count} from {source.cite()}")
    total_count = sum(most_common_sources.values())
    print(
        f"Total number of recorded location references: {total_count} (averaging"
        f" {total_count / len(requires_tl):.2f} per name)"
    )
    print(
        f"Highest number of location references on one name: {highest_count} on"
        f" {highest_count_name}"
    )
    print("Kind of type locality:")
    for label, count in sorted(tl_kind.items()):
        print(f"- {label}: {count} ({count / len(requires_tl) * 100:.2f}%)")
    print("Most common type localities:")
    for tl, count in most_common_tls.most_common(10):
        print(f"- {tl.name}: {count}")


if __name__ == "__main__":
    txn = Taxon.select_valid().filter(Taxon.valid_name == "Mammalia").get()
    nams = get_names_for_export(
        txn,
        {AgeClass.extant, AgeClass.recently_extinct},
        Group.species,
        None,
        Rank.species,
    )
    compute_report(nams)
