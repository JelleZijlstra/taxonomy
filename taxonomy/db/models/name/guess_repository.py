from __future__ import annotations

import inspect
import math
import random
from collections import defaultdict, deque
from functools import cache
from typing import NamedTuple

from taxonomy import getinput
from taxonomy.db import constants, models

from .name import Name

try:
    # static analysis: ignore[import_failed]
    from typeinfer import NameData, Params, ScoreInfo, evaluate_model, get_top_choice
except ImportError:

    class NameData(NamedTuple):  # type: ignore[no-redef]
        collection: int
        tl_country: int
        year: int
        authors: list[int]
        citation_group: int
        name_id: int

    class Params(NamedTuple):  # type: ignore[no-redef]
        country_boost: float
        cg_boost: float
        author_boost: float
        year_boost: float
        year_factor: float
        score_cutoff: float
        probability_cutoff: float

    class ScoreInfo(NamedTuple):  # type: ignore[no-redef]
        score: int
        correct: int
        incorrect: int
        no_value: int

    def get_score(nam1: NameData, nam2: NameData, params: Params) -> float:
        if nam1.name_id == nam2.name_id:
            return 0
        score: float = 1
        if nam1.tl_country == nam2.tl_country:
            score *= params.country_boost
        if nam1.citation_group == nam2.citation_group:
            score *= params.cg_boost
        overlapping_authors = sum(1 for aut in nam1.authors if aut in nam2.authors)
        total_authors = len(nam1.authors) + len(nam2.authors) - overlapping_authors
        overlap_prop = overlapping_authors / total_authors
        if overlap_prop > 0:
            score *= overlap_prop * params.author_boost
        year_difference = abs(nam1.year - nam2.year)
        score *= (1 / (params.year_factor**year_difference)) * params.year_boost
        return score

    def get_probs(
        data: NameData, train_data: list[NameData], params: Params
    ) -> dict[int, float]:
        scores: dict[int, float] = defaultdict(float)
        highest_score: float = 1
        for train in train_data:
            score = get_score(train, data, params)
            if score > params.score_cutoff:
                scores[train.collection] += score
            if score > highest_score:
                highest_score = score
        scores[0] += highest_score  # inject some uncertainty
        total = sum(scores.values())
        return {c: v / total for c, v in scores.items()}

    def get_top_choice(
        nam: NameData, train_data: list[NameData], params: Params
    ) -> tuple[int, float] | None:
        probs = get_probs(nam, train_data, params)
        for c, score in probs.items():
            if score > params.probability_cutoff:
                return c, score
        return None

    FALSE_POS_COST = 10

    def evaluate_model(
        train_data: list[NameData], test_data: list[NameData], params: Params
    ) -> ScoreInfo:
        no_value = 0
        correct = 0
        incorrect = 0
        for nam in getinput.print_every_n(test_data, n=1000, label="names"):
            choice = get_top_choice(nam, train_data, params)
            if choice is None:
                no_value += 1
                continue
            coll, _ = choice
            if coll == nam.collection:
                correct += 1
            else:
                incorrect += 1
        score = correct - incorrect * FALSE_POS_COST
        return ScoreInfo(
            score=score, correct=correct, incorrect=incorrect, no_value=no_value
        )


def get_training_names() -> list[tuple[Name, bool]]:
    nams = Name.select_valid().filter(
        Name.group == constants.Group.species,
        Name.species_type_kind != constants.SpeciesGroupType.neotype,
        Name.collection != None,
    )
    mammalia = models.Taxon.getter("valid_name")("Mammalia")
    return [
        (
            nam,
            nam.taxon.age is constants.AgeClass.extant
            and nam.taxon.get_derived_field("class_") == mammalia,
        )
        for nam in nams
        if "type_specimen" in nam.get_required_fields()
    ]


def get_collection_id(nam: Name) -> int:
    if nam.collection:
        return nam.collection.id
    else:
        return 0


def make_name_data(nam: Name) -> NameData:
    if nam.type_locality:
        region = nam.type_locality.region
        country = region.parent_of_kind(constants.RegionKind.country)
        if country:
            tl_country = country.id
        else:
            continent = region.parent_of_kind(constants.RegionKind.continent)
            if continent:
                tl_country = continent.id
            else:
                tl_country = 0
    else:
        tl_country = 0
    if nam.citation_group:
        cg_id = nam.citation_group.id
    elif nam.original_citation and nam.original_citation.citation_group:
        cg_id = nam.original_citation.citation_group.id
    else:
        cg_id = 0
    authors = nam.get_authors()
    return NameData(
        collection=get_collection_id(nam),
        tl_country=tl_country,
        year=nam.numeric_year(),
        authors=sorted({aut.id for aut in authors}),
        citation_group=cg_id,
        name_id=nam.id,
    )


@cache
def get_training_data() -> list[tuple[NameData, bool]]:
    return [
        (make_name_data(nam), is_extant_mammal)
        for nam, is_extant_mammal, in get_training_names()
    ]


# score = 3464.6666666666665
DEFAULT_PARAMS = Params(
    country_boost=1.425,
    cg_boost=25.006,
    author_boost=584.512,
    year_factor=1.299,
    year_boost=59.29373821049923,
    score_cutoff=1.637,
    probability_cutoff=0.747,
)


def perturb_params(
    params: Params, rand: random.Random, history: deque[tuple[str, bool, bool]]
) -> tuple[Params, str, bool]:
    fields = list(inspect.signature(Params).parameters)
    if history and history[-1][2]:
        # If the last change was an improvement, try to change the same field
        field = history[-1][0]
        direction = history[-1][1]
    else:
        field = rand.choice(fields)
        direction = None
    current_value = getattr(params, field)
    while True:
        if field == "probability_cutoff":
            while True:
                new_value = rand.gauss(current_value, 0.1)
                if 0.5 <= new_value <= 1:
                    break
        else:
            new_value = math.exp(rand.normalvariate(math.log(current_value), 0.5))
        if direction is None or (new_value > current_value) == direction:
            break
    kwargs = {f: getattr(params, f) for f in fields}
    kwargs[field] = new_value
    return Params(**kwargs), field, new_value > current_value


def get_model_score(
    partitions: list[tuple[list[NameData], list[NameData]]], params: Params
) -> tuple[float, list[ScoreInfo]]:
    scores = [
        evaluate_model(train_data, test_data, params)
        for train_data, test_data in partitions
    ]
    return sum(s.score for s in scores) / len(partitions), scores


def tune(
    data: list[NameData],
    initial_params: Params,
    *,
    num_partitions: int,
    rand: random.Random,
    max_tries: int = 1000,
    extra_training_data: list[NameData] = [],
) -> Params:
    n = len(data)
    train_size = n // 2
    ints = range(n)
    selected_numbers = [
        set(rand.sample(ints, train_size)) for _ in range(num_partitions)
    ]
    partitions = [
        (
            [d for i, d in enumerate(data) if i in selected] + extra_training_data,
            [d for i, d in enumerate(data) if i not in selected],
        )
        for selected in selected_numbers
    ]
    params = initial_params
    current_score, score_data = get_model_score(partitions, initial_params)
    print(f"Initial score: {current_score} ({params}; {score_data})")
    history: deque[tuple[str, bool, bool]] = deque(maxlen=10)
    for _ in range(max_tries):
        new_params, changed_field, direction = perturb_params(params, rand, history)
        print(f"Trying params: {new_params}")
        model_score, score_data = get_model_score(partitions, new_params)
        print(f"Got score: {model_score} ({score_data})")
        # Even if the scores are the same, change to the new one,
        # so we get to explore more of the space
        better = model_score > current_score
        history.append((changed_field, direction, better))
        if better:
            print(f"Improve score: {current_score} -> {model_score} ({new_params})")
            params = new_params
            current_score = model_score
    return params


def run(num_partitions: int = 100, max_tries: int = 1000) -> Params:
    all_data = get_training_data()
    data = [d for d, is_extant_mammal in all_data if is_extant_mammal]
    extra_training_data = [
        d for d, is_extant_mammal in all_data if not is_extant_mammal
    ]
    print(
        f"Generated {len(data)} training names plus {len(extra_training_data)} extra training names."
    )
    rand = random.Random(234235809)
    return tune(
        data,
        DEFAULT_PARAMS,
        rand=rand,
        num_partitions=num_partitions,
        max_tries=max_tries,
        extra_training_data=extra_training_data,
    )


def get_most_likely_repository(
    nam: models.Name,
) -> tuple[models.Collection, float] | None:
    # Only trained for extant mammals
    txn = nam.taxon
    if txn.age is not constants.AgeClass.extant:
        return None
    if txn.get_derived_field("class_").valid_name != "Mammalia":
        return None
    data = get_training_data()
    name_data = make_name_data(nam)
    choice = get_top_choice(name_data, data, DEFAULT_PARAMS)
    if choice is None:
        return None
    coll_id, score = choice
    return models.Collection(coll_id), score


if __name__ == "__main__":
    print(run(1, 3))
