from typing import Counter, Iterable, List, Optional, Sequence, Tuple

from matplotlib import pyplot as plt

from taxonomy.db.constants import AgeClass, Group
from taxonomy.db.models import Taxon


def get_years(
    taxon: Taxon, age: Optional[AgeClass] = None, group: Optional[Group] = None
) -> List[Tuple[int, int]]:
    names = taxon.all_names(age=age)
    if group is not None:
        names = {nam for nam in names if nam.group is group}
    years = [nam.numeric_year() for nam in names]
    counts = Counter(year for year in years if year != 0)
    return list(interpolate_zeroes(counts))


def interpolate_zeroes(counts: "Counter[int]") -> Iterable[Tuple[int, int]]:
    min_year = min(counts)
    max_year = max(counts)
    for year in range(min_year, max_year + 1):
        yield (year, counts[year])


def plot_years(
    taxon: Taxon,
    ages: Sequence[Optional[AgeClass]],
    title: Optional[str] = None,
    group: Optional[Group] = None,
) -> None:
    series = [get_years(taxon, age, group) for age in ages]
    fig, ax = plt.subplots()
    for age, pairs in zip(ages, series, strict=True):
        x, y = zip(*pairs, strict=True)
        if age is None:
            label = "all names"
        else:
            label = age.name
        ax.plot(x, y, label=label)

    ax.set(xlabel="year", ylabel="new specise-group names")
    ax.grid()
    ax.legend()
    if title is not None:
        plt.title(title)

    fig.savefig("test.png")
    plt.show()
