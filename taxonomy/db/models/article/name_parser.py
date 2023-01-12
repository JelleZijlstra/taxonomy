import itertools
import re
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
)

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
    geographicTerms: ClassVar[Set[str]]
    geographicModifiers: ClassVar[Set[str]]
    geographicWords: ClassVar[Set[str]]
    periodTerms: ClassVar[Set[str]]
    periodModifiers: ClassVar[Set[str]]
    periodWords: ClassVar[Set[str]]

    # Whether an error occurred, and a description.
    errorDescription: List[str]

    rawName: str  # The raw file name parsed
    extension: str  # The file extension (e.g., "pdf").
    modifier: str  # Modifier (e.g., "part 2").

    # Authorship, when included in the title. This will return an array
    # of two elements. The first element will be false if only a year is given,
    # a string with the name of an author if one author is given, an array with
    # multiple elements if multiple authors are given, and an array with a
    # single element if "et al." is given. The second element is the year.
    authorship: Tuple[Union[None, str, List[str]], Optional[str]]

    # baseName is an array with elements representing parts of the title. The
    # keys may be "nov" or "normal", representing <nov-phrase> and
    # <normal-name>.
    baseName: Dict[str, Any]

    def __init__(self, name: str, data_path: Path) -> None:
        self.errorDescription = []
        self.buildLists(data_path)
        self.rawName = name
        self.baseName = {}
        name = self.splitExtension(name)
        name = self.splitModifiers(name)
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
            # possibility (1)
            self.parseNovPhrase(name)
        elif " nov, " in name:
            parts = re.split(r"(?<= nov), ", name, maxsplit=1)
            self.parseNovPhrase(parts[0])
            self.parseNormalName(parts[1])
        elif re.search(r"^[A-Z][a-z]+( [a-z]+){0,2} for [A-Za-z][a-z]+$", name):
            self.parseForPhrase(name)
        elif name.startswith("MS "):
            self.parseMsPhrase(name)
        elif re.search(r"^[A-Za-z\-\s]+ (\d+)(\(\d+\))?$", name):
            self.parseFullIssue(name)
        else:
            self.parseNormalName(name)
        self.validate_names()

    def errorOccurred(self) -> bool:
        return bool(self.errorDescription)

    def getErrors(self) -> Sequence[str]:
        return self.errorDescription

    def printErrors(self) -> None:
        print(f"{len(self.errorDescription)} errors while parsing name: {self.rawName}")
        for error in self.errorDescription:
            print(f"- {error}")

    def addError(self, description: str) -> None:
        self.errorDescription.append(description.strip())

    def printParsed(self) -> None:
        """Print as parsed."""
        print(f"Parsing name: {self.rawName}")

        def printIfNotEmpty(field: str) -> None:
            value = getattr(self, field)
            if value:
                print(f"{field.title()}: {value}")

        printIfNotEmpty("extension")
        printIfNotEmpty("modifier")
        if self.authorship != (None, None):
            print("Authorship:")
            if self.authorship[0] is not None:
                print(f'\tAuthors: {"; ".join(self.authorship[0])}')
            print(f"\tYear: {self.authorship[1]}")
        printIfNotEmpty("baseName")
        print()

    # Parsing functions.
    def splitExtension(self, name: str) -> str:
        match = re.match(r"^(.*)\.([a-z]+)$", name)
        if match:
            self.extension = match.group(2)
            return match.group(1)
        else:
            self.extension = ""
            return name

    # A name may end in "(<author-modifier>)? (<free-modifier>)?". Any other
    # pattern of parenthesized expressions is an error.
    def splitModifiers(self, name: str) -> str:
        self.modifier = ""
        self.authorship = (None, None)
        firstSet = self.splitParentheses(name)
        if not isinstance(firstSet, tuple):
            return name

        name = firstSet[0]
        if self.isAuthorModifier(firstSet[1]):
            # one modifier
            self.parseAuthorModifier(firstSet[1])
        else:
            # last modifier is free-form
            self.modifier = firstSet[1]
            secondSet = self.splitParentheses(name)
            if not isinstance(secondSet, tuple):
                return name

            name = secondSet[0]
            # possible author modifier
            if self.isAuthorModifier(secondSet[1]):
                self.parseAuthorModifier(secondSet[1])
            else:
                self.addError("Too many modifiers")
                return name

        # any other modifiers now would be an error, but check
        thirdSet = self.splitParentheses(name)
        if isinstance(thirdSet, tuple):
            self.addError("Too many modifiers")
            # attempt error recovery, but if there are even more modifiers even
            # this won't work
            return thirdSet[0]
        else:
            return name

    def splitParentheses(self, input: str) -> Any:
        match = re.match(r"^(.*) \(([^()]+)\)$", input)
        if match:
            return (match.group(1), match.group(2))
        else:
            return input

    def isAuthorModifier(self, input: str) -> bool:
        return bool(re.search(r"\d{4}$", input))

    def parseAuthorModifier(self, input: str) -> None:
        # <author-modifier> is <authors>? year

        # this should always match, because we assume that input passed the
        # isAuthorModifier function
        match = re.match(r"^(.*)(\d{4})$", input)
        assert match is not None
        # year
        year = match[2]
        raw_authors = match[1].strip()
        authors: Union[None, str, List[str]]
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

    def parseNovPhrase(self, input: str) -> None:
        match = re.match(r"^(\w+) (\d+)nov$", input)
        nov: Any
        if match:
            nov = int(match[2]), match[1]
        else:
            if not input.endswith(" nov"):
                self.addError("Invalid nov phrase")
                return
            nov = self.parse_names(input[:-4])
        self.baseName["nov"] = nov

    # For phrases.
    # "Churcheria for Anonymus.pdf" -> 'for': ['Churcheria', 'Anonymus']
    # "Neurotrichus skoczeni for minor.pdf" ->
    #          'for': ['Neurotrichus skoczeni', 'Neurotrichus minor']
    def parseForPhrase(self, input: str) -> None:
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

        self.baseName["for"] = [replacement, preoccupied]

    # Mammalian Species
    def parseMsPhrase(self, input: str) -> None:
        self.baseName["mammalianspecies"] = self.parse_names(input[3:])

    # Names of type "Lemur News 12.pdf" ->
    #      'fullissue': ['Lemur News', '12']
    def parseFullIssue(self, input: str) -> None:
        match = re.search(r"^(.*) (\d+(\(\d+\))?)$", input)
        assert match is not None, f"failed to match {input}"
        self.baseName["fullissue"] = [match[1], match[2]]

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

    def parseNormalName(self, input: str) -> None:
        out: Dict[str, Any] = {}
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
                out.update(self.parseNormalAtTopic(input))
                break

            if self.check_period_words(input):
                out.update(self.parseNormalAtTime(input))
                break

            if self.find_term(input, self.geographicWords):
                out.update(self.parseNormalAtGeography(input))
                break
            name, rest = self.getFirstWord(input)
            names += " " + name
            input = rest.strip()

        if names:
            out["names"] = self.parse_names(names.strip())
        self.baseName["normal"] = out

    def parseNormalAtTopic(self, input: str) -> Dict[str, Any]:
        # input begins with -
        topic = input[1:].split(", ")
        return {"topic": topic}

    def parseNormalAtTime(self, input: str) -> Dict[str, Any]:
        # Here we can assume that period terms are always one word
        # Except for MN7-8, that is, which we'll have to hard-code
        out: Dict[str, Any] = {}
        times: List[Tuple[Optional[str], Optional[str]]] = []
        time: Tuple[Optional[str], Optional[str]]
        inRange = False
        while True:
            firstWord, rest = self.getFirstWord(input)
            if firstWord in self.periodModifiers:
                # next word must be a periodTerm followed by [,-], or -
                # followed by another modifier
                if rest.startswith("-"):
                    times.append((firstWord, None))
                    inRange = True
                else:
                    secondWord, rest = self.getFirstWord(rest.strip())

                    if secondWord not in self.periodTerms:
                        self.addError("Period modifier not followed by period term")
                        break
                    time = (firstWord, secondWord)
            elif firstWord in self.periodTerms:
                time = (None, firstWord)
            elif firstWord == "MN7" and rest.startswith("-8"):
                time = (None, "MN7-8")
                rest = rest[2:]
            else:
                self.addError("Invalid word in period: " + firstWord)
                break
            input = rest.strip()
            if input == "" or input.startswith(", "):
                if inRange:
                    # handle "M-L Miocene" kind of stuff
                    firstTime = times.pop()
                    if firstTime[1] is None:
                        firstTime = (firstTime[0], time[1])
                    times.append(firstTime)
                    inRange = False
                times.append(time)
                if input == "":
                    break
                input = input[2:]
            elif input.startswith("-"):
                input = input[1:]
                if not self.find_term(input, self.periodWords):
                    out.update(self.parseNormalAtTopic("-" + input))
                    break
            else:
                self.addError("Syntax error in period")
                break
        if times:
            out["times"] = times
        return out

    def parseNormalAtGeography(self, input: str) -> Dict[str, Any]:
        # first find a major term, then minor terms
        out: Dict[str, Any] = {}
        places = []
        currentMajor: List[str] = []
        currentMinor = ""
        while True:
            if currentMinor == "":
                findModifier = self.find_term(input, self.geographicModifiers)
                if findModifier is None:
                    modifier = ""
                else:
                    modifier = findModifier[1] + " "
                    input = findModifier[0].strip()
                findMajor = self.find_term(input, self.geographicTerms)
                if findMajor is None:
                    if not currentMajor:
                        self.addError(f"Invalid geography at {input}")
                        break
                    else:
                        # retain currentMajor and in
                        pass
                else:
                    currentMajor = [modifier, findMajor[1]]
                    input = findMajor[0]
            input = input.strip()
            if not input:
                places.append((tuple(currentMajor), currentMinor))
                break
            if input.startswith(","):
                places.append((tuple(currentMajor), currentMinor))
                currentMinor = ""
                input = input[1:].strip()
            elif input.startswith("-"):
                # decide whether this starts the topic (if there's another -
                # in the text or the last word is a period, we assume it
                # doesn't)
                if (
                    "-" not in input[1:]
                    and self.getLastWord(input) not in self.periodTerms
                ):
                    places.append((tuple(currentMajor), currentMinor))
                    out.update(self.parseNormalAtTopic(input))
                    break
                else:
                    currentMinor += "-"
                    minor, input = self.getFirstWord(input[1:])
                    currentMinor += minor
            elif self.check_period_words(input):
                places.append((tuple(currentMajor), currentMinor))
                out.update(self.parseNormalAtTime(input))
                break
            else:
                # then it's a minor term
                if currentMinor != "":
                    currentMinor += " "
                minor, input = self.getFirstWord(input)
                currentMinor += minor
        out["geography"] = places
        return out

    # Data handling.
    didBuildLists: ClassVar[bool] = False

    @classmethod
    def buildLists(cls, data_path: Path) -> None:
        if cls.didBuildLists:
            return

        def getData(fileName: str) -> Set[str]:
            path = data_path / fileName
            with path.open() as f:
                lines = (re.sub(r"#.*$", "", line).strip() for line in f.readlines())
                return {line for line in lines if line}

        cls.geographicTerms = getData("geography.txt")
        cls.geographicModifiers = getData("geography_modifiers.txt")
        cls.periodTerms = getData("periods.txt")
        cls.periodModifiers = getData("period_modifiers.txt")

        # overall arrays that are sometimes useful
        cls.geographicWords = cls.geographicTerms | cls.geographicModifiers
        cls.periodWords = cls.periodTerms | cls.periodModifiers

        cls.didBuildLists = True

    # Helper methods.
    @staticmethod
    def getFirstWord(input: str) -> Tuple[str, str]:
        output = re.split(r"[ \-,]", input, maxsplit=1)
        # simplify life for callers
        if len(output) == 1:
            return (output[0], "")
        return (output[0], input[len(output[0]) :])

    @staticmethod
    def getLastWord(input: str) -> str:
        output = re.split(r"[ \-,]", input)
        return output[-1].strip()

    # We need a separate function to fix the "E" issue.
    @classmethod
    def check_period_words(cls, input: str) -> bool:
        res = cls.find_term(input, cls.periodWords)
        if not res:
            return False
        elif res[1] != "E":
            return True
        elif len(input) > 1 and input[1] == "-":
            # that's a range, so it's indeed time
            return True
        elif cls.find_term(res[0].strip(), cls.periodTerms):
            return True
        else:
            return False

    # Finds whether any of the phrases in array terms occur in haystack.
    # Returns an array of the haystack without the word plus the word, or
    # None on failure to find a word.
    @classmethod
    def find_term(cls, haystack: str, terms: Set[str]) -> Optional[Tuple[str, str]]:
        for term in terms:
            if haystack.startswith(term):
                new_hay = haystack[len(term) :]
                if not new_hay or new_hay[0] in (",", " ", "-"):
                    return new_hay, term
        return None

    # Parse a listing of scientific names.
    def parse_names(self, input: str) -> List[str]:
        out = []
        names = input.split(", ")
        lastName: Optional[str] = None
        for name in names:
            if name.islower():
                if not lastName:
                    self.addError("Invalid lowercase name")
                else:
                    name = self.getFirstWord(lastName)[0] + " " + name
            out.append(name)
            lastName = name
        return out

    # After parsing is completed, check whether scientific names are valid.
    def validate_names(self) -> None:
        for key, value in self.baseName.items():
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
                    topicIsSpecial = bool(topics & allowed_topics)
                else:
                    topicIsSpecial = False
                for name in value["names"]:
                    self.validate_name(name, topicIsSpecial)
            elif key == "fullissue":
                break
            else:
                raise RuntimeError(f"unrecognized key {key}")

    def validate_name(self, name: str, topicIsSpecial: bool = False) -> None:
        if topicIsSpecial:
            return
        # valid name forms
        if not _NAME_VALIDATOR.match(name):
            self.addError("Invalid name: " + name)
