"""

Module for fragile detection logic (e.g., stem/gender detection for genera).

Relevant ICZN provisions about gender:
- Article 31.2: defines which names are and are not declined
- Article 30: defines the gender of names. Some relevant provisions:
  - Normally based on Greek or Latin dictionary gender
  - If a name is Latinized from Greek with a different suffix, the gender appropriate to the suffix
    applies (30.1.3)
  - -cola and similar common-gender names are masculine unless explicitly treated as feminine
    (30.1.4.2)
  - -ops is always masculine (30.1.4.3)
  - names from modern European languages with gender take gender from the language (30.2.1)
  - otherwise, names take gender expressly specified (30.2.2) or implicit in species name endings (30.2.3)
  - otherwise, feminine if ending in -a, neuter with -um, -on, -u, masculine with -us (30.2.4)

See also Article 29 on stems. Notable there is that generally, whoever makes the first family-group name
gets to choose the stem.

"""

import collections
import re
from typing import Callable, List, Optional, Tuple, Union

from .constants import GrammaticalGender

detection_result = collections.namedtuple(
    "detection_result", ["stem", "gender", "confident"]
)


def _rep(pattern: str, replacement: str) -> Callable[[str], str]:
    return lambda name: re.sub(r"%s$" % pattern, replacement, name)


_endings: List[
    Tuple[str, Union[str, Callable[[str], str]], GrammaticalGender, bool]
] = [
    ("arctos", "os", GrammaticalGender.masculine, True),
    ("apis", "is", GrammaticalGender.masculine, True),
    ("apus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("alis", "is", GrammaticalGender.masculine, True),
    ("avus", "us", GrammaticalGender.masculine, True),
    ("bus", "us", GrammaticalGender.masculine, True),
    ("caremys", "s", GrammaticalGender.masculine, True),
    ("c", "", GrammaticalGender.masculine, True),  # Tenrec
    ("amys", "s", GrammaticalGender.masculine, True),
    ("anger", "", GrammaticalGender.masculine, True),  # Phalanger
    (
        "aroo",
        "oo",
        GrammaticalGender.masculine,
        True,
    ),  # Based on Nambaroo novus and some others (and Art. 30.2.4)
    ("atus", "us", GrammaticalGender.masculine, True),
    ("ates", "es", GrammaticalGender.masculine, True),
    (
        "baatar",
        "",
        GrammaticalGender.masculine,
        True,
    ),  # masculine on the evidence of _Gobibaatar parvus_ (and Art. 30.2.4)
    ("ber", _rep("er", "r"), GrammaticalGender.masculine, True),  # Coluber, Fiber
    ("bitis", "is", GrammaticalGender.masculine, True),  # Cobitis, some fish
    ("cheirus", "us", GrammaticalGender.masculine, True),
    (
        "eres",
        "es",
        GrammaticalGender.masculine,
        True,
    ),  # e.g. Loncheres, which can be both masculine and feminine but should probably be treated as masculine
    ("chirus", "us", GrammaticalGender.masculine, True),
    ("cho", "o", GrammaticalGender.masculine, True),
    ("choerus", "us", GrammaticalGender.masculine, True),
    ("chus", "us", GrammaticalGender.masculine, True),
    ("ceros", _rep("s", "t"), GrammaticalGender.masculine, True),
    ("cetus", "us", GrammaticalGender.masculine, True),
    ("citor", "", GrammaticalGender.masculine, True),
    ("cnemus", "us", GrammaticalGender.masculine, True),
    ("etes", "es", GrammaticalGender.masculine, True),
    ("cormus", "us", GrammaticalGender.masculine, True),
    ("glis", _rep("s", "r"), GrammaticalGender.masculine, True),
    ("cola", "a", GrammaticalGender.masculine, True),
    ("olus", "us", GrammaticalGender.masculine, True),
    ("lides", "es", GrammaticalGender.masculine, True),
    ("somus", "us", GrammaticalGender.masculine, True),  # Amblysomus
    ("cropus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("ico", lambda n: n + "n", GrammaticalGender.masculine, True),
    ("cus", "us", GrammaticalGender.masculine, True),
    ("dapis", "is", GrammaticalGender.masculine, True),  # Adapis
    ("eropus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("moropus", "us", GrammaticalGender.masculine, True),  # Eomoropidae
    ("eopus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("eutes", "es", GrammaticalGender.masculine, True),
    ("dypus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("cyon", "", GrammaticalGender.masculine, True),
    ("dens", _rep("s", "t"), GrammaticalGender.masculine, True),
    ("dipus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("ectes", "es", GrammaticalGender.masculine, True),
    ("sor", "", GrammaticalGender.masculine, True),
    ("eo", lambda n: n + "n", GrammaticalGender.masculine, True),
    ("eramus", _rep("s", "r"), GrammaticalGender.masculine, True),
    ("erus", "us", GrammaticalGender.masculine, True),
    ("eus", "us", GrammaticalGender.masculine, True),
    ("glis", _rep("s", "r"), GrammaticalGender.masculine, True),
    ("Glis", _rep("s", "r"), GrammaticalGender.masculine, True),
    ("gus", "us", GrammaticalGender.masculine, True),
    ("hippus", "us", GrammaticalGender.masculine, True),
    ("ichthys", "s", GrammaticalGender.masculine, True),
    ("chestes", "es", GrammaticalGender.masculine, True),
    ("edetes", "es", GrammaticalGender.masculine, True),  # Pedetes
    ("ides", "es", GrammaticalGender.masculine, True),
    ("ilus", "us", GrammaticalGender.masculine, True),
    ("imys", "s", GrammaticalGender.masculine, True),
    ("inus", "us", GrammaticalGender.masculine, True),
    ("io", lambda n: n + "n", GrammaticalGender.masculine, True),
    ("ipes", _rep("s", "d"), GrammaticalGender.masculine, True),
    ("irox", _rep("x", "g"), GrammaticalGender.masculine, True),
    ("ites", "es", GrammaticalGender.masculine, True),
    ("ius", "us", GrammaticalGender.masculine, True),
    ("lepus", _rep("us", "or"), GrammaticalGender.masculine, True),
    ("labis", _rep("s", "d"), GrammaticalGender.masculine, True),
    ("lax", _rep("x", "c"), GrammaticalGender.masculine, True),
    ("letes", "es", GrammaticalGender.masculine, True),
    ("lestes", "es", GrammaticalGender.masculine, True),  # Greek "ὁ λῃστής" robber
    ("lemur", "", GrammaticalGender.masculine, True),
    ("llodus", "us", GrammaticalGender.masculine, True),
    ("lopus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("loricus", "us", GrammaticalGender.masculine, True),
    ("mmys", "s", GrammaticalGender.masculine, True),
    ("lus", "us", GrammaticalGender.masculine, True),
    ("meryx", _rep("x", "c"), GrammaticalGender.masculine, True),
    ("mamus", "us", GrammaticalGender.masculine, True),
    ("orictes", "es", GrammaticalGender.masculine, True),
    ("ephas", _rep("s", "nt"), GrammaticalGender.masculine, True),
    ("mimus", "us", GrammaticalGender.masculine, True),
    ("myscus", "us", GrammaticalGender.masculine, True),
    ("ntes", "es", GrammaticalGender.masculine, True),
    ("nus", "us", GrammaticalGender.masculine, True),
    ("ocus", "us", GrammaticalGender.masculine, True),
    (
        "odemus",
        "us",
        GrammaticalGender.masculine,
        True,
    ),  # Apodemus is not derived from Mus
    ("adon", lambda n: n + "t", GrammaticalGender.masculine, True),
    ("edon", lambda n: n + "t", GrammaticalGender.masculine, True),
    ("idon", lambda n: n + "t", GrammaticalGender.masculine, True),
    ("odon", lambda n: n + "t", GrammaticalGender.masculine, True),
    ("odus", _rep("us", "ont"), GrammaticalGender.masculine, True),
    ("omys", "s", GrammaticalGender.masculine, True),
    ("onyx", _rep("x", "ch"), GrammaticalGender.masculine, True),
    ("opsis", "is", GrammaticalGender.masculine, True),
    ("ornis", _rep("s", "th"), GrammaticalGender.masculine, True),
    ("ous", "us", GrammaticalGender.masculine, True),
    ("khos", "os", GrammaticalGender.masculine, True),  # Pachyrukhos
    ("inos", "os", GrammaticalGender.masculine, True),
    ("ddax", _rep("x", "c"), GrammaticalGender.masculine, True),
    ("ntus", "us", GrammaticalGender.masculine, True),
    ("itus", "us", GrammaticalGender.masculine, True),
    ("ltus", "us", GrammaticalGender.masculine, True),
    ("petes", "es", GrammaticalGender.masculine, True),
    ("oryx", _rep("x", "c"), GrammaticalGender.masculine, True),
    ("phus", "us", GrammaticalGender.masculine, True),
    ("ppus", "us", GrammaticalGender.masculine, True),
    ("pithecus", "us", GrammaticalGender.masculine, True),
    ("pterus", "us", GrammaticalGender.masculine, True),
    ("ratops", "", GrammaticalGender.masculine, True),
    ("rax", _rep("x", "c"), GrammaticalGender.masculine, True),
    ("rhynchus", "us", GrammaticalGender.masculine, True),
    ("saurus", "us", GrammaticalGender.masculine, True),
    ("skos", "os", GrammaticalGender.masculine, True),
    ("rus", "us", GrammaticalGender.masculine, True),
    ("ctus", "us", GrammaticalGender.masculine, True),
    ("copus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("sciurus", "us", GrammaticalGender.masculine, True),
    ("bos", _rep("s", "v"), GrammaticalGender.masculine, True),
    ("mias", "as", GrammaticalGender.masculine, True),
    ("rsus", "us", GrammaticalGender.masculine, True),
    ("smus", "us", GrammaticalGender.masculine, True),
    ("rhysis", "is", GrammaticalGender.masculine, True),
    # http://en.wiktionary.org/wiki/%CF%83%CF%84%CE%AC%CF%87%CF%85%CF%82, but the plant genus name _Stachys_ is treated as feminine
    ("stachys", "s", GrammaticalGender.masculine, True),
    ("sorex", _rep("ex", "ic"), GrammaticalGender.masculine, True),
    ("ssus", "us", GrammaticalGender.masculine, True),
    ("stomus", "us", GrammaticalGender.masculine, True),
    ("sypus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("tabes", "es", GrammaticalGender.feminine, True),  # see Amelotabes nov.pdf
    ("tamus", "us", GrammaticalGender.masculine, True),
    ("thus", "us", GrammaticalGender.masculine, True),
    ("rtus", "us", GrammaticalGender.masculine, True),
    ("tor", "", GrammaticalGender.masculine, True),
    ("tipus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("triton", "", GrammaticalGender.masculine, True),
    ("urus", "us", GrammaticalGender.masculine, True),
    ("ophis", "s", GrammaticalGender.masculine, True),
    ("raco", lambda n: n + "n", GrammaticalGender.masculine, True),
    ("uus", "us", GrammaticalGender.masculine, True),
    ("rvus", "us", GrammaticalGender.masculine, True),
    ("umys", "s", GrammaticalGender.masculine, True),
    ("ymys", "s", GrammaticalGender.masculine, True),
    ("ykus", "us", GrammaticalGender.masculine, True),
    ("xus", "us", GrammaticalGender.masculine, True),
    ("yus", "us", GrammaticalGender.masculine, True),
    ("lerix", _rep("x", "c"), GrammaticalGender.masculine, True),
    ("eviathan", "", GrammaticalGender.masculine, True),
    ("ator", "", GrammaticalGender.masculine, True),
    ("aiman", "", GrammaticalGender.masculine, True),  # Caiman (Art. 30.2.4)
    ("rhys", "s", GrammaticalGender.masculine, True),  # Hesperhys
    ("ipus", _rep("us", "od"), GrammaticalGender.masculine, True),
    ("titan", "", GrammaticalGender.masculine, True),
    ("ops", "s", GrammaticalGender.masculine, True),  # Art. 30.1.4.3
    ("ala", "a", GrammaticalGender.feminine, True),
    ("ampsa", "a", GrammaticalGender.feminine, True),
    ("ana", "a", GrammaticalGender.feminine, True),
    ("ara", "a", GrammaticalGender.feminine, True),
    ("ca", "a", GrammaticalGender.feminine, True),
    ("ema", "a", GrammaticalGender.feminine, True),
    ("capra", "a", GrammaticalGender.feminine, True),
    ("cneme", "e", GrammaticalGender.feminine, True),
    ("coma", "a", GrammaticalGender.feminine, True),
    ("delphys", "ys", GrammaticalGender.feminine, True),
    ("da", "a", GrammaticalGender.feminine, True),
    ("dna", "a", GrammaticalGender.feminine, True),
    ("dra", "a", GrammaticalGender.feminine, True),
    ("elis", "is", GrammaticalGender.feminine, True),  # Felis
    ("ea", "a", GrammaticalGender.feminine, True),
    ("ela", "a", GrammaticalGender.feminine, True),
    ("lla", "a", GrammaticalGender.feminine, True),
    ("ena", "a", GrammaticalGender.feminine, True),
    ("era", "a", GrammaticalGender.feminine, True),
    ("erra", "a", GrammaticalGender.feminine, True),
    ("ga", "a", GrammaticalGender.feminine, True),
    (
        "genys",
        "s",
        GrammaticalGender.feminine,
        True,
    ),  # See comments under Cteniogenyidae
    ("siren", "", GrammaticalGender.feminine, True),
    ("gale", "e", GrammaticalGender.feminine, True),
    ("manis", "is", GrammaticalGender.feminine, True),
    (
        "ia",
        "a",
        GrammaticalGender.feminine,
        True,
    ),  # probably has some false positives but worth it
    ("ica", "a", GrammaticalGender.feminine, True),
    ("ictis", "is", GrammaticalGender.feminine, True),  # or sometimes -ictid
    ("ila", "a", GrammaticalGender.feminine, True),
    ("ina", "a", GrammaticalGender.feminine, True),
    ("ira", "a", GrammaticalGender.feminine, True),
    ("rna", "a", GrammaticalGender.feminine, True),
    ("nycteris", "is", GrammaticalGender.feminine, True),
    ("oa", "a", GrammaticalGender.feminine, True),
    ("otoma", "a", GrammaticalGender.feminine, True),
    ("pa", "a", GrammaticalGender.feminine, True),
    ("ona", "a", GrammaticalGender.feminine, True),
    ("ora", "a", GrammaticalGender.feminine, True),
    ("lpes", "es", GrammaticalGender.feminine, True),  # Vulpes
    ("oryctes", "es", GrammaticalGender.feminine, True),
    ("ostrix", _rep("x", "g"), GrammaticalGender.feminine, True),
    ("phitis", "is", GrammaticalGender.feminine, True),  # Mephitis
    ("physis", "is", GrammaticalGender.feminine, True),  # Coelophysis
    ("pteryx", _rep("x", "g"), GrammaticalGender.feminine, True),
    ("rta", "a", GrammaticalGender.feminine, True),
    ("semys", _rep("s", "d"), GrammaticalGender.feminine, True),  # -emys turtles
    ("sha", "a", GrammaticalGender.feminine, True),  # Manitsha
    ("ssa", "a", GrammaticalGender.feminine, True),
    ("ista", "a", GrammaticalGender.feminine, True),  # Sicista
    ("tta", "a", GrammaticalGender.feminine, True),
    (
        "theis",
        "is",
        GrammaticalGender.feminine,
        True,
    ),  # a guess, only occurs in the tunicate Nephtheis
    ("udo", _rep("o", "in"), GrammaticalGender.feminine, True),  # Testudo
    ("thrix", _rep("x", "ch"), GrammaticalGender.feminine, True),
    ("uis", "is", GrammaticalGender.feminine, True),  # Anguis only
    ("ula", "a", GrammaticalGender.feminine, True),
    ("ura", "a", GrammaticalGender.feminine, True),
    ("ufa", "a", GrammaticalGender.feminine, True),
    ("cta", "a", GrammaticalGender.feminine, True),
    ("va", "a", GrammaticalGender.feminine, True),
    ("vis", "is", GrammaticalGender.feminine, True),
    ("uris", "is", GrammaticalGender.feminine, True),  # Nanocuris
    ("zoa", "a", GrammaticalGender.feminine, True),
    ("iza", "a", GrammaticalGender.feminine, True),
    ("meles", "es", GrammaticalGender.feminine, True),
    ("ama", "a", GrammaticalGender.feminine, True),
    ("elys", "s", GrammaticalGender.feminine, True),
    ("derma", lambda n: n + "t", GrammaticalGender.neuter, True),
    ("erpeton", lambda n: n + "t", GrammaticalGender.neuter, True),
    ("ceras", _rep("s", "t"), GrammaticalGender.neuter, True),
    ("ion", "", GrammaticalGender.neuter, True),
    ("ium", "um", GrammaticalGender.neuter, True),
    ("num", "um", GrammaticalGender.neuter, True),
    ("onon", "on", GrammaticalGender.neuter, True),
    ("ron", "", GrammaticalGender.neuter, True),
    ("sma", lambda n: n + "t", GrammaticalGender.neuter, True),
    ("soma", lambda n: n + "t", GrammaticalGender.neuter, True),
    ("stoma", lambda n: n + "t", GrammaticalGender.neuter, True),
    ("therium", "um", GrammaticalGender.neuter, True),
    ("tum", "um", GrammaticalGender.neuter, True),
    ("izon", lambda n: n + "t", GrammaticalGender.neuter, True),
    ("yum", "um", GrammaticalGender.neuter, True),
    ("nion", "on", GrammaticalGender.neuter, True),
]

# To discuss:
# -eles names: Proteles, Perameles feminine, Ateles, Brachyteles, Meles masculine


def _remove_ending(name: str, end: str) -> str:
    assert name.endswith(end)
    return re.sub(r"%s$" % end, "", name)


def detect_stem_and_gender(name: str) -> Optional[detection_result]:
    for ending, to_stem, gender, confidence in _endings:
        if name.lower().endswith(ending):
            if callable(to_stem):
                stem = to_stem(name)
            else:
                stem = _remove_ending(name, to_stem)
            return detection_result(stem, gender, confidence)
    return None
