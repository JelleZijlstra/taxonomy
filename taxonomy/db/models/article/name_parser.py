import functools
import itertools
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any, ClassVar, TypeVar

from taxonomy.config import get_options

T = TypeVar("T", bound="NameParser")

_NAME_VALIDATOR = re.compile(
    r"""^(
    ((Cf|Aff)\.\ )?
    [A-Z][a-z?]+  # genus name
    (\ \([A-Z][a-z]+\))?  # subgenus
    ((\ (cf|aff)\.)?\ [a-z?]+(\ [a-z?]+)?)?  # optional species and subspecies name
    ( Division| group| complex)?
    |[A-Za-z\ ]+\ virus  # or a virus name ending in " virus"
    )$""",
    re.VERBOSE,
)


class NameParser:
    geographic_terms: ClassVar[set[str]]
    geographic_modifiers: ClassVar[set[str]]
    geographic_words: ClassVar[set[str]]
    period_terms: ClassVar[set[str]]
    period_modifiers: ClassVar[set[str]]
    period_words: ClassVar[set[str]]

    # Whether an error occurred, and a description.
    error_description: list[str]

    raw_name: str  # The raw file name parsed
    extension: str  # The file extension (e.g., "pdf").
    modifier: str  # Modifier (e.g., "part 2").

    # Authorship, when included in the title. This will return an array
    # of two elements. The first element will be false if only a year is given,
    # a string with the name of an author if one author is given, an array with
    # multiple elements if multiple authors are given, and an array with a
    # single element if "et al." is given. The second element is the year.
    authorship: tuple[None | str | list[str], str | None]

    # base_name is an array with elements representing parts of the title. The
    # keys may be "nov" or "normal", representing <nov-phrase> and
    # <normal-name>.
    base_name: dict[str, Any]

    def __init__(self, name: str, data_path: Path) -> None:
        self.error_description = []
        self.build_lists(data_path)
        self.raw_name = name
        self.base_name = {}
        name = self.split_extension(name)
        name = self.split_modifiers(name)
        # Now, a name may be:
        # (1) <nov-phrase>
        #      [e.g., "Agathaeromys nov" or "Dilambdogale, Qatranilestes nov"]
        # (2) <normal-name>
        #      [e.g., "Afrosoricida Egypt Eo-Oligocene.pdf"]
        # (3) <nov-phrase>, <normal-name>
        # (4) <replacement> for <preoccupied name>
        # (5) MS <genus-name> <species-name>
        # (6) <journal name> <volume>
        #
        # The preg_match below is to account for the syntax
        #      "Oryzomys palustris-Hoplopleura oryzomydis nov.pdf",
        # used with parasites.
        if name.endswith("nov") and not re.search(r"-[A-Z]", name):
            # possibility 1
            self.parse_nov_phrase(name)
        elif " nov, " in name:
            parts = re.split(r"(?<= nov), ", name, maxsplit=1)
            self.parse_nov_phrase(parts[0])
            self.parse_normal_name(parts[1])
        elif re.search(r"^[A-Z][a-z]+( [a-z]+){0,2} for [A-Za-z][a-z]+$", name):
            self.parse_for_phrase(name)
        elif name.startswith("MS "):
            self.parse_ms_phrase(name)
        elif re.search(r"^[A-Za-z\-\s]+ (\d+)(\(\d+\))?$", name):
            self.parse_full_issue(name)
        else:
            self.parse_normal_name(name)
        self.validate_names()

    def error_occurred(self) -> bool:
        return bool(self.error_description)

    def get_errors(self) -> Sequence[str]:
        return self.error_description

    def print_errors(self) -> None:
        print(
            f"{len(self.error_description)} errors while parsing name: {self.raw_name}"
        )
        for error in self.error_description:
            print(f"- {error}")

    def add_error(self, description: str) -> None:
        self.error_description.append(description.strip())

    def print_parsed(self) -> None:
        """Print as parsed."""
        print(f"Parsing name: {self.raw_name}")

        def print_if_not_empty(field: str) -> None:
            value = getattr(self, field)
            if value:
                print(f"{field.title()}: {value}")

        print_if_not_empty("extension")
        print_if_not_empty("modifier")
        if self.authorship != (None, None):
            print("Authorship:")
            if self.authorship[0] is not None:
                print(f'\tAuthors: {"; ".join(self.authorship[0])}')
            print(f"\tYear: {self.authorship[1]}")
        print_if_not_empty("base_name")
        print()

    # Parsing functions.
    def split_extension(self, name: str) -> str:
        match = re.match(r"^(.*)\.([a-z]+)$", name)
        if match:
            self.extension = match.group(2)
            return match.group(1)
        else:
            self.extension = ""
            return name

    # A name may end in "(<author-modifier>)? (<free-modifier>)?". Any other
    # pattern of parenthesized expressions is an error.
    def split_modifiers(self, name: str) -> str:
        self.modifier = ""
        self.authorship = (None, None)
        first_set = self.split_parentheses(name)
        if not isinstance(first_set, tuple):
            return name

        name = first_set[0]
        if self.is_author_modifier(first_set[1]):
            # one modifier
            self.parse_author_modifier(first_set[1])
        else:
            # last modifier is free-form
            self.modifier = first_set[1]
            second_set = self.split_parentheses(name)
            if not isinstance(second_set, tuple):
                return name

            name = second_set[0]
            # possible author modifier
            if self.is_author_modifier(second_set[1]):
                self.parse_author_modifier(second_set[1])
            else:
                self.add_error("Too many modifiers")
                return name

        # any other modifiers now would be an error, but check
        third_set = self.split_parentheses(name)
        if isinstance(third_set, tuple):
            self.add_error("Too many modifiers")
            # attempt error recovery, but if there are even more modifiers even
            # this won't work
            return third_set[0]
        else:
            return name

    def split_parentheses(self, input: str) -> Any:
        match = re.match(r"^(.*) \(([^()]+)\)$", input)
        if match:
            return (match.group(1), match.group(2))
        else:
            return input

    def is_author_modifier(self, input: str) -> bool:
        return bool(re.search(r"\d{4}$", input))

    def parse_author_modifier(self, input: str) -> None:
        # <author-modifier> is <authors>? year

        # this should always match, because we assume that input passed the
        # is_author_modifier function
        match = re.match(r"^(.*)(\d{4})$", input)
        assert match is not None
        # year
        year = match[2]
        raw_authors = match[1].strip()
        authors: None | str | list[str]
        if raw_authors:
            # <authors> may be "A", "A & B" or "A et al."
            if raw_authors.endswith(" et al."):
                authors = [raw_authors[:-7]]
            elif " & " in raw_authors:
                authors = raw_authors.split(" & ")
            else:
                authors = raw_authors
        else:
            authors = None
        self.authorship = (authors, year)

    # Nov phrases

    # Nov phrases have the forms:
    # (1) <group> <number>nov
    #      [Oryzomyini 10nov]
    # (2) <scientific name> nov
    #      [Agathaeromys nov]
    # (3) <scientific name>, <abbreviated scientific name> nov
    #      [Murina beelzebub, cinerea, walstoni nov]

    # (1) is parsed into 'nov': (10, 'Oryzomyini')
    # (2) is parsed into 'nov': ['Agathaeromys']
    # (3) becomes 'nov': ['Murina beelzebub', 'Murina cinerea', 'Murina walstoni']

    def parse_nov_phrase(self, input: str) -> None:
        match = re.match(r"^(\w+) (\d+)nov$", input)
        nov: Any
        if match:
            nov = int(match[2]), match[1]
        else:
            if not input.endswith(" nov"):
                self.add_error("Invalid nov phrase")
                return
            nov = self.parse_names(input[:-4])
        self.base_name["nov"] = nov

    # For phrases.
    # "Churcheria for Anonymus.pdf" -> 'for': ['Churcheria', 'Anonymus']
    # "Neurotrichus skoczeni for minor.pdf" ->
    #          'for': ['Neurotrichus skoczeni', 'Neurotrichus minor']
    def parse_for_phrase(self, input: str) -> None:
        match = re.match(r"(\w+)( (\w+))?( (\w+))? for (\w+)", input)
        assert match is not None, f"failed to match {input}"
        replacement = match[1]
        if match[3]:
            replacement += " " + match[3]
        if match[5]:
            replacement += " " + match[5]

        if match[5]:
            preoccupied = match[1] + " " + match[3] + " " + match[6]
        elif match[3]:
            preoccupied = match[1] + " " + match[6]
        else:
            preoccupied = match[6]

        self.base_name["for"] = [replacement, preoccupied]

    # Mammalian Species
    def parse_ms_phrase(self, input: str) -> None:
        self.base_name["mammalianspecies"] = self.parse_names(input[3:])

    # Names of type "Lemur News 12.pdf" ->
    #      'fullissue': ['Lemur News', '12']
    def parse_full_issue(self, input: str) -> None:
        match = re.search(r"^(.*) (\d+(\(\d+\))?)$", input)
        assert match is not None, f"failed to match {input}"
        self.base_name["fullissue"] = [match[1], match[2]]

    # Normal names

    # Grammar:
    #  <name-list>? <geography>? <time>? ((-<topic>)?
    # Where:
    #  <name-list> is as in a nov phrase
    #  <geography> is of the form:
    #      <geographic-modifier>? <geographic-area> <geographic-term>?
    #  <time> is of the form
    #      <period-modifier>? <period>
    #  (or a more complicated range)
    #  <topic> is an arbitrary string

    # This produces something of the form 'normal' => array(
    #      'names' => (array as for nov-phrase)
    #      'geography' => (array of arrays of two items, representing the
    #                      general and specific geography)
    #      'time' => (array of items representing either a single time unit as
    #                      a string or an array of two; time unit is
    #                      represented as array of modifier + major unit)
    #      'topic' => (array of topics)

    def parse_normal_name(self, input: str) -> None:
        out: dict[str, Any] = {}
        # first, consume names until we find a geographic or period term
        names = ""
        while True:
            if input.startswith(","):
                names += ","
                input = input[1:].strip()

            if input == "":
                # we're done
                break

            if input.startswith("-"):
                out.update(self.parse_normal_at_topic(input))
                break

            if self.check_period_words(input):
                out.update(self.parse_normal_at_time(input))
                break

            if self.find_term(input, self.geographic_words):
                out.update(self.parse_normal_at_geography(input))
                break
            name, rest = self.get_first_word(input)
            names += " " + name
            input = rest.strip()

        if names:
            out["names"] = self.parse_names(names.strip())
        self.base_name["normal"] = out

    def parse_normal_at_topic(self, input: str) -> dict[str, Any]:
        # input begins with -
        topic = input[1:].split(", ")
        return {"topic": topic}

    def parse_normal_at_time(self, input: str) -> dict[str, Any]:
        # Here we can assume that period terms are always one word
        # Except for MN7-8, that is, which we'll have to hard-code
        out: dict[str, Any] = {}
        times: list[tuple[str | None, str | None]] = []
        time: tuple[str | None, str | None] = (None, None)
        in_range = False
        while True:
            first_word, rest = self.get_first_word(input)
            if first_word in self.period_modifiers:
                # next word must be a periodTerm followed by [,-], or -
                # followed by another modifier
                if rest.startswith("-"):
                    times.append((first_word, None))
                    in_range = True
                else:
                    second_word, rest = self.get_first_word(rest.strip())

                    if second_word not in self.period_terms:
                        self.add_error("Period modifier not followed by period term")
                        break
                    time = (first_word, second_word)
            elif first_word in self.period_terms:
                time = (None, first_word)
            elif first_word == "MN7" and rest.startswith("-8"):
                time = (None, "MN7-8")
                rest = rest[2:]
            else:
                self.add_error("Invalid word in period: " + first_word)
                break
            input = rest.strip()
            if input == "" or input.startswith(", "):
                if in_range:
                    # handle "M-L Miocene" kind of stuff
                    first_time = times.pop()
                    if first_time[1] is None:
                        first_time = (first_time[0], time[1])
                    times.append(first_time)
                    in_range = False
                times.append(time)
                if input == "":
                    break
                input = input[2:]
            elif input.startswith("-"):
                input = input[1:]
                if not self.find_term(input, self.period_words):
                    out.update(self.parse_normal_at_topic("-" + input))
                    break
            else:
                self.add_error("Syntax error in period")
                break
        if times:
            out["times"] = times
        return out

    def parse_normal_at_geography(self, input: str) -> dict[str, Any]:
        # first find a major term, then minor terms
        out: dict[str, Any] = {}
        places = []
        current_major: list[str] = []
        current_minor = ""
        while True:
            if current_minor == "":
                find_modifier = self.find_term(input, self.geographic_modifiers)
                if find_modifier is None:
                    modifier = ""
                else:
                    modifier = find_modifier[1] + " "
                    input = find_modifier[0].strip()
                find_major = self.find_term(input, self.geographic_terms)
                if find_major is None:
                    if not current_major:
                        self.add_error(f"Invalid geography at {input}")
                        break
                    else:
                        # retain current_major and in
                        pass
                else:
                    current_major = [modifier, find_major[1]]
                    input = find_major[0]
            input = input.strip()
            if not input:
                places.append((tuple(current_major), current_minor))
                break
            if input.startswith(","):
                places.append((tuple(current_major), current_minor))
                current_minor = ""
                input = input[1:].strip()
            elif input.startswith("-"):
                # decide whether this starts the topic (if there's another -
                # in the text or the last word is a period, we assume it
                # doesn't)
                if (
                    "-" not in input[1:]
                    and self.get_last_word(input) not in self.period_terms
                ):
                    places.append((tuple(current_major), current_minor))
                    out.update(self.parse_normal_at_topic(input))
                    break
                else:
                    current_minor += "-"
                    minor, input = self.get_first_word(input[1:])
                    current_minor += minor
            elif self.check_period_words(input):
                places.append((tuple(current_major), current_minor))
                out.update(self.parse_normal_at_time(input))
                break
            else:
                # then it's a minor term
                if current_minor != "":
                    current_minor += " "
                minor, input = self.get_first_word(input)
                current_minor += minor
        out["geography"] = places
        return out

    # Data handling.
    did_build_lists: ClassVar[bool] = False

    @classmethod
    def build_lists(cls, data_path: Path) -> None:
        if cls.did_build_lists:
            return

        def get_data(file_name: str) -> set[str]:
            path = data_path / file_name
            with path.open() as f:
                lines = (re.sub(r"#.*$", "", line).strip() for line in f.readlines())
                return {line for line in lines if line}

        cls.geographic_terms = get_data("geography.txt")
        cls.geographic_modifiers = get_data("geography_modifiers.txt")
        cls.period_terms = get_data("periods.txt")
        cls.period_modifiers = get_data("period_modifiers.txt")

        # overall arrays that are sometimes useful
        cls.geographic_words = cls.geographic_terms | cls.geographic_modifiers
        cls.period_words = cls.period_terms | cls.period_modifiers

        cls.did_build_lists = True

    # Helper methods.
    @staticmethod
    def get_first_word(input: str) -> tuple[str, str]:
        output = re.split(r"[ \-,]", input, maxsplit=1)
        # simplify life for callers
        if len(output) == 1:
            return (output[0], "")
        return (output[0], input[len(output[0]) :])

    @staticmethod
    def get_last_word(input: str) -> str:
        output = re.split(r"[ \-,]", input)
        return output[-1].strip()

    # We need a separate function to fix the "E" issue.
    @classmethod
    def check_period_words(cls, input: str) -> bool:
        res = cls.find_term(input, cls.period_words)
        if not res:
            return False
        elif res[1] != "E":
            return True
        elif len(input) > 1 and input[1] == "-":
            # that's a range, so it's indeed time
            return True
        else:
            return cls.find_term(res[0].strip(), cls.period_terms) is not None

    # Finds whether any of the phrases in array terms occur in haystack.
    # Returns an array of the haystack without the word plus the word, or
    # None on failure to find a word.
    @classmethod
    def find_term(cls, haystack: str, terms: set[str]) -> tuple[str, str] | None:
        for term in terms:
            if haystack.startswith(term):
                new_hay = haystack[len(term) :]
                if not new_hay or new_hay[0] in (",", " ", "-"):
                    return new_hay, term
        return None

    # Parse a listing of scientific names.
    def parse_names(self, input: str) -> list[str]:
        out = []
        names = input.split(", ")
        last_name: str | None = None
        for name in names:
            if name.islower():
                if not last_name:
                    self.add_error("Invalid lowercase name")
                else:
                    name = self.get_first_word(last_name)[0] + " " + name
            out.append(name)
            last_name = name
        return out

    # After parsing is completed, check whether scientific names are valid.
    def validate_names(self) -> None:
        for key, value in self.base_name.items():
            if key == "nov":
                if isinstance(value, tuple):
                    # "Oryzomyini 10nov" type
                    self.validate_name(value[1])
                else:
                    for new in value:
                        self.validate_name(new)
            elif key in ("for", "mammalianspecies"):
                for name in value:
                    self.validate_name(name)
            elif key == "normal":
                if "names" not in value:
                    continue
                if "topic" in value:
                    topic = value["topic"]
                    topics = set(
                        itertools.chain.from_iterable(t.split() for t in topic)
                    )
                    allowed_topics = {
                        "review",
                        "types",
                        "biography",
                        "obituary",
                        "bibliography",
                        "catalog",
                        "festschrift",
                        "publication",
                        "catalogue",
                        "meeting",
                        "collection",
                    }
                    topic_is_special = bool(topics & allowed_topics)
                else:
                    topic_is_special = False
                for name in value["names"]:
                    self.validate_name(name, topic_is_special=topic_is_special)
            elif key == "fullissue":
                break
            else:
                raise RuntimeError(f"unrecognized key {key}")

    def validate_name(self, name: str, *, topic_is_special: bool = False) -> None:
        if topic_is_special:
            return
        # valid name forms
        if not _NAME_VALIDATOR.match(name):
            self.add_error("Invalid name: " + name)


@functools.lru_cache(8192)
def get_name_parser(name: str) -> NameParser:
    options = get_options()
    return NameParser(name, options.parserdata_path)
