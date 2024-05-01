from __future__ import annotations

import bisect
import functools
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

from taxonomy import getinput
from taxonomy.db.constants import OccurrenceStatus, PeriodSystem, RegionKind

from .article import Article
from .location import Location
from .name import Name
from .occurrence import Occurrence
from .period import Period
from .taxon import Taxon

T = TypeVar("T")

PERIOD_BINS = [
    "Holocene",
    "Late Pleistocene",
    "Middle Pleistocene",
    "Early Pleistocene",
    "Late Pliocene",
    "Early Pliocene",
    "Late Miocene",
    "Middle Miocene",
    "Early Miocene",
    "Late Oligocene",
    "Early Oligocene",
    "Late Eocene",
    "Middle Eocene",
    "Early Eocene",
    "Late Paleocene",
    "Middle Paleocene",
    "Early Paleocene",
    "Late Cretaceous",
    "Early Cretaceous",
    "Late Jurassic",
    "Middle Jurassic",
    "Early Jurassic",
    "Late Triassic",
    "Middle Triassic",
    "Early Triassic",
]


def not_none(obj: T | None) -> T:
    if obj is None:
        raise TypeError("obj is None")
    return obj


@functools.cache
def get_bucket_maker() -> Callable[[int], int]:
    periods = [not_none(Period.getter("name")(name)) for name in PERIOD_BINS]
    min_ages: list[int] = [not_none(period.get_min_age()) for period in periods]

    def maker(age: int) -> int:
        return max(0, bisect.bisect_left(min_ages, age) - 1)

    return maker


BucketKey = tuple[str, int, int]


def get_age_bucket(
    min_period: Period | None, max_period: Period | None
) -> tuple[int, int] | str:
    if min_period is None:
        return "no min period"
    min_age = min_period.get_min_age()
    if min_age is None:
        return "no min age"
    if max_period is None:
        return "no max period"
    max_age = max_period.get_max_age()
    if max_age is None:
        return "no max age"
    bm = get_bucket_maker()
    min_period_index = bm(min_age + 1)
    max_period_index = bm(max_age)
    return min_period_index, max_period_index


def check_age_buckets(system: PeriodSystem, verbose: bool = False) -> None:
    periods = Period.select_valid().filter(Period.system == system)
    for period in periods:
        bucket = get_age_bucket(period, period)
        if isinstance(bucket, str):
            print(f"{period}: {bucket}")
            continue
        min_index, max_index = bucket
        min_period = PERIOD_BINS[min_index]
        max_period = PERIOD_BINS[max_index]
        if min_index == max_index:
            if verbose:
                print(f"{period}: {min_period}")
            continue
        print(f"{period}: {max_period}–{min_period}")


@dataclass
class DataPoint:
    location: Location
    _bucket: BucketKey | str = field(init=False, repr=False, hash=False, compare=False)

    def get_description(self) -> str:
        raise NotImplementedError

    def get_bucket(self) -> BucketKey | str:
        if not hasattr(self, "_bucket"):
            self._bucket = self._get_bucket()
        return self._bucket

    def _get_bucket(self) -> BucketKey | str:
        # TODO minimize the number of cases we return None
        region = self.location.region.parent_of_kind(RegionKind.continent)
        if region is None:
            return "no continent"
        bucket = get_age_bucket(self.location.min_period, self.location.max_period)
        if isinstance(bucket, str):
            return bucket
        return region.name, bucket[0], bucket[1]


@dataclass
class TypeLocDataPoint(DataPoint):
    name: Name

    def get_description(self) -> str:
        return (
            f"{self.location.concise_markdown_link()}, type locality of"
            f" {self.name.concise_markdown_link()}"
        )


@dataclass
class OccurrenceDataPoint(DataPoint):
    taxon: Taxon
    source: Article | None

    def get_description(self) -> str:
        return (
            f"{self.location.concise_markdown_link()}, occurrence of"
            f" {self.taxon.concise_markdown_link()} according to"
            f" {self.source.concise_markdown_link() if self.source else '(no source)'}"
        )


def make_summary_line(
    bucket: BucketKey, datapoints: Sequence[DataPoint], detail_up_to: int
) -> str:
    region, min_index, max_index = bucket
    if min_index == max_index:
        period_string = PERIOD_BINS[min_index]
    elif max_index == len(PERIOD_BINS) - 1:
        period_string = "fossil"
    else:
        period_string = f"{PERIOD_BINS[max_index]}–{PERIOD_BINS[min_index]}"
    detailed = "; ".join(dp.get_description() for dp in datapoints[:detail_up_to])
    if len(datapoints) > detail_up_to:
        extra = f"; {len(datapoints) - detail_up_to} more"
    else:
        extra = ""
    return f"- {period_string} of {region} ({detailed}{extra})\n"


_TAXON_ID_TO_DATAPOINTS: dict[int, Sequence[DataPoint]] = {}


def get_datapoints(taxon: Taxon) -> Sequence[DataPoint]:
    if taxon.id in _TAXON_ID_TO_DATAPOINTS:
        return _TAXON_ID_TO_DATAPOINTS[taxon.id]
    datapoints: list[DataPoint] = [
        TypeLocDataPoint(nam.type_locality, nam)
        for nam in taxon.get_names()
        if nam.type_locality is not None
    ]
    for occ in taxon.occurrences.filter(
        Occurrence.status.is_in((OccurrenceStatus.valid, OccurrenceStatus.extirpated))
    ):
        datapoints.append(OccurrenceDataPoint(occ.location, occ.taxon, occ.source))
    for child in taxon.get_children():
        datapoints += get_datapoints(child)
    _TAXON_ID_TO_DATAPOINTS[taxon.id] = datapoints
    return datapoints


def clear_cache() -> None:
    _TAXON_ID_TO_DATAPOINTS.clear()
    get_bucket_maker.cache_clear()


@dataclass
class RangeSummary:
    datapoints: Sequence[DataPoint]

    @classmethod
    def from_taxon(cls, taxon: Taxon) -> RangeSummary:
        return RangeSummary(get_datapoints(taxon))

    def print_summary(self) -> None:
        summary, explanation, reason_to_dp = self.summarize()
        getinput.print_header(summary)
        if reason_to_dp:
            print("## Skip reasons")
            for reason, datapoints in sorted(
                reason_to_dp.items(), key=lambda pair: -len(pair[1])
            ):
                print(reason, len(datapoints))
        print(explanation)

    def summarize(
        self, detail_up_to: int = 2
    ) -> tuple[str, str, dict[str, list[DataPoint]]]:
        bucketed: dict[BucketKey, list[DataPoint]] = {}
        reason_to_datapoints: dict[str, list[DataPoint]] = {}
        for dp in self.datapoints:
            bucket = dp.get_bucket()
            if isinstance(bucket, str):
                reason_to_datapoints.setdefault(bucket, []).append(dp)
                continue
            bucketed.setdefault(bucket, []).append(dp)
        keys = sorted(bucketed, key=lambda bucket: (bucket[0], -bucket[2], -bucket[1]))
        summary_lines = []
        current_continent: str | None = None
        last_index: int | None = None
        start_index: int | None = None
        pieces = []
        for bucket in keys:
            buckets = bucketed[bucket]
            summary_lines.append(make_summary_line(bucket, buckets, detail_up_to))
            region, min_index, max_index = bucket
            if max_index >= len(PERIOD_BINS) - 1:
                reason_to_datapoints.setdefault("too old", []).extend(buckets)
                continue
            if min_index != max_index:
                reason_to_datapoints.setdefault(
                    "covers multiple age buckets", []
                ).extend(buckets)
                # TODO
                continue
            if region != current_continent:
                if current_continent is not None:
                    assert last_index is not None
                    if last_index != start_index:
                        pieces.append("–")
                        pieces.append(PERIOD_BINS[last_index])
                    pieces.append(f", {current_continent}; ")
                current_continent = region
                last_index = None
            if last_index is None:
                pieces.append(PERIOD_BINS[min_index])
                last_index = start_index = min_index
            elif min_index == last_index - 1:
                last_index = min_index
            else:
                if last_index != start_index:
                    pieces.append("–")
                    pieces.append(PERIOD_BINS[last_index])
                pieces.append(", ")
                pieces.append(PERIOD_BINS[min_index])
                last_index = start_index = min_index
        if last_index is not None and last_index != start_index:
            pieces.append("–")
            pieces.append(PERIOD_BINS[last_index])
        if current_continent is not None:
            pieces.append(f", {current_continent}")
        return "".join(pieces), "".join(summary_lines), reason_to_datapoints
