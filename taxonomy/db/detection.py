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

from . import constants

Gender = constants.Gender

detection_result = collections.namedtuple(
    "detection_result", ["stem", "gender", "confident"]
)


def _rep(pattern: str, replacement: str) -> Callable[[str], str]:
    return lambda name: re.sub(r"%s$" % pattern, replacement, name)


_endings: List[Tuple[str, Union[str, Callable[[str], str]], Gender, bool]] = [
    ("arctos", "os", Gender.masculine, True),
    ("apis", "is", Gender.masculine, True),
    ("apus", _rep("us", "od"), Gender.masculine, True),
    ("alis", "is", Gender.masculine, True),
    ("avus", "us", Gender.masculine, True),
    ("bus", "us", Gender.masculine, True),
    ("caremys", "s", Gender.masculine, True),
    ("c", "", Gender.masculine, True),  # Tenrec
    ("amys", "s", Gender.masculine, True),
    ("anger", "", Gender.masculine, True),  # Phalanger
    (
        "aroo",
        "oo",
        Gender.masculine,
        True,
    ),  # Based on Nambaroo novus and some others (and Art. 30.2.4)
    ("atus", "us", Gender.masculine, True),
    ("ates", "es", Gender.masculine, True),
    (
        "baatar",
        "",
        Gender.masculine,
        True,
    ),  # masculine on the evidence of _Gobibaatar parvus_ (and Art. 30.2.4)
    ("ber", _rep("er", "r"), Gender.masculine, True),  # Coluber, Fiber
    ("bitis", "is", Gender.masculine, True),  # Cobitis, some fish
    ("cheirus", "us", Gender.masculine, True),
    (
        "eres",
        "es",
        Gender.masculine,
        True,
    ),  # e.g. Loncheres, which can be both masculine and feminine but should probably be treated as masculine
    ("chirus", "us", Gender.masculine, True),
    ("cho", "o", Gender.masculine, True),
    ("choerus", "us", Gender.masculine, True),
    ("chus", "us", Gender.masculine, True),
    ("ceros", _rep("s", "t"), Gender.masculine, True),
    ("cetus", "us", Gender.masculine, True),
    ("citor", "", Gender.masculine, True),
    ("cnemus", "us", Gender.masculine, True),
    ("etes", "es", Gender.masculine, True),
    ("cormus", "us", Gender.masculine, True),
    ("glis", _rep("s", "r"), Gender.masculine, True),
    ("cola", "a", Gender.masculine, True),
    ("olus", "us", Gender.masculine, True),
    ("lides", "es", Gender.masculine, True),
    ("somus", "us", Gender.masculine, True),  # Amblysomus
    ("cropus", _rep("us", "od"), Gender.masculine, True),
    ("ico", lambda n: n + "n", Gender.masculine, True),
    ("cus", "us", Gender.masculine, True),
    ("dapis", "is", Gender.masculine, True),  # Adapis
    ("eropus", _rep("us", "od"), Gender.masculine, True),
    ("moropus", "us", Gender.masculine, True),  # Eomoropidae
    ("eopus", _rep("us", "od"), Gender.masculine, True),
    ("eutes", "es", Gender.masculine, True),
    ("dypus", _rep("us", "od"), Gender.masculine, True),
    ("cyon", "", Gender.masculine, True),
    ("dens", _rep("s", "t"), Gender.masculine, True),
    ("dipus", _rep("us", "od"), Gender.masculine, True),
    ("ectes", "es", Gender.masculine, True),
    ("sor", "", Gender.masculine, True),
    ("eo", lambda n: n + "n", Gender.masculine, True),
    ("eramus", _rep("s", "r"), Gender.masculine, True),
    ("erus", "us", Gender.masculine, True),
    ("eus", "us", Gender.masculine, True),
    ("glis", _rep("s", "r"), Gender.masculine, True),
    ("Glis", _rep("s", "r"), Gender.masculine, True),
    ("gus", "us", Gender.masculine, True),
    ("hippus", "us", Gender.masculine, True),
    ("ichthys", "s", Gender.masculine, True),
    ("chestes", "es", Gender.masculine, True),
    ("edetes", "es", Gender.masculine, True),  # Pedetes
    ("ides", "es", Gender.masculine, True),
    ("ilus", "us", Gender.masculine, True),
    ("imys", "s", Gender.masculine, True),
    ("inus", "us", Gender.masculine, True),
    ("io", lambda n: n + "n", Gender.masculine, True),
    ("ipes", _rep("s", "d"), Gender.masculine, True),
    ("irox", _rep("x", "g"), Gender.masculine, True),
    ("ites", "es", Gender.masculine, True),
    ("ius", "us", Gender.masculine, True),
    ("lepus", _rep("us", "or"), Gender.masculine, True),
    ("labis", _rep("s", "d"), Gender.masculine, True),
    ("lax", _rep("x", "c"), Gender.masculine, True),
    ("letes", "es", Gender.masculine, True),
    ("lestes", "es", Gender.masculine, True),  # Greek "ὁ λῃστής" robber
    ("lemur", "", Gender.masculine, True),
    ("llodus", "us", Gender.masculine, True),
    ("lopus", _rep("us", "od"), Gender.masculine, True),
    ("loricus", "us", Gender.masculine, True),
    ("mmys", "s", Gender.masculine, True),
    ("lus", "us", Gender.masculine, True),
    ("meryx", _rep("x", "c"), Gender.masculine, True),
    ("mamus", "us", Gender.masculine, True),
    ("orictes", "es", Gender.masculine, True),
    ("ephas", _rep("s", "nt"), Gender.masculine, True),
    ("mimus", "us", Gender.masculine, True),
    ("myscus", "us", Gender.masculine, True),
    ("ntes", "es", Gender.masculine, True),
    ("nus", "us", Gender.masculine, True),
    ("ocus", "us", Gender.masculine, True),
    ("odemus", "us", Gender.masculine, True),  # Apodemus is not derived from Mus
    ("adon", lambda n: n + "t", Gender.masculine, True),
    ("edon", lambda n: n + "t", Gender.masculine, True),
    ("idon", lambda n: n + "t", Gender.masculine, True),
    ("odon", lambda n: n + "t", Gender.masculine, True),
    ("odus", _rep("us", "ont"), Gender.masculine, True),
    ("omys", "s", Gender.masculine, True),
    ("onyx", _rep("x", "ch"), Gender.masculine, True),
    ("opsis", "is", Gender.masculine, True),
    ("ornis", _rep("s", "th"), Gender.masculine, True),
    ("ous", "us", Gender.masculine, True),
    ("khos", "os", Gender.masculine, True),  # Pachyrukhos
    ("inos", "os", Gender.masculine, True),
    ("ddax", _rep("x", "c"), Gender.masculine, True),
    ("ntus", "us", Gender.masculine, True),
    ("itus", "us", Gender.masculine, True),
    ("ltus", "us", Gender.masculine, True),
    ("petes", "es", Gender.masculine, True),
    ("oryx", _rep("x", "c"), Gender.masculine, True),
    ("phus", "us", Gender.masculine, True),
    ("ppus", "us", Gender.masculine, True),
    ("pithecus", "us", Gender.masculine, True),
    ("pterus", "us", Gender.masculine, True),
    ("ratops", "", Gender.masculine, True),
    ("rax", _rep("x", "c"), Gender.masculine, True),
    ("rhynchus", "us", Gender.masculine, True),
    ("saurus", "us", Gender.masculine, True),
    ("skos", "os", Gender.masculine, True),
    ("rus", "us", Gender.masculine, True),
    ("ctus", "us", Gender.masculine, True),
    ("copus", _rep("us", "od"), Gender.masculine, True),
    ("sciurus", "us", Gender.masculine, True),
    ("bos", _rep("s", "v"), Gender.masculine, True),
    ("mias", "as", Gender.masculine, True),
    ("rsus", "us", Gender.masculine, True),
    ("smus", "us", Gender.masculine, True),
    ("rhysis", "is", Gender.masculine, True),
    # http://en.wiktionary.org/wiki/%CF%83%CF%84%CE%AC%CF%87%CF%85%CF%82, but the plant genus name _Stachys_ is treated as feminine
    ("stachys", "s", Gender.masculine, True),
    ("sorex", _rep("ex", "ic"), Gender.masculine, True),
    ("ssus", "us", Gender.masculine, True),
    ("stomus", "us", Gender.masculine, True),
    ("sypus", _rep("us", "od"), Gender.masculine, True),
    ("tabes", "es", Gender.feminine, True),  # see Amelotabes nov.pdf
    ("tamus", "us", Gender.masculine, True),
    ("thus", "us", Gender.masculine, True),
    ("rtus", "us", Gender.masculine, True),
    ("tor", "", Gender.masculine, True),
    ("tipus", _rep("us", "od"), Gender.masculine, True),
    ("triton", "", Gender.masculine, True),
    ("urus", "us", Gender.masculine, True),
    ("ophis", "s", Gender.masculine, True),
    ("raco", lambda n: n + "n", Gender.masculine, True),
    ("uus", "us", Gender.masculine, True),
    ("rvus", "us", Gender.masculine, True),
    ("umys", "s", Gender.masculine, True),
    ("ymys", "s", Gender.masculine, True),
    ("ykus", "us", Gender.masculine, True),
    ("xus", "us", Gender.masculine, True),
    ("yus", "us", Gender.masculine, True),
    ("lerix", _rep("x", "c"), Gender.masculine, True),
    ("eviathan", "", Gender.masculine, True),
    ("ator", "", Gender.masculine, True),
    ("aiman", "", Gender.masculine, True),  # Caiman (Art. 30.2.4)
    ("rhys", "s", Gender.masculine, True),  # Hesperhys
    ("ipus", _rep("us", "od"), Gender.masculine, True),
    ("titan", "", Gender.masculine, True),
    ("ops", "s", Gender.masculine, True),  # Art. 30.1.4.3
    ("ala", "a", Gender.feminine, True),
    ("ampsa", "a", Gender.feminine, True),
    ("ana", "a", Gender.feminine, True),
    ("ara", "a", Gender.feminine, True),
    ("ca", "a", Gender.feminine, True),
    ("ema", "a", Gender.feminine, True),
    ("capra", "a", Gender.feminine, True),
    ("cneme", "e", Gender.feminine, True),
    ("coma", "a", Gender.feminine, True),
    ("delphys", "ys", Gender.feminine, True),
    ("da", "a", Gender.feminine, True),
    ("dna", "a", Gender.feminine, True),
    ("dra", "a", Gender.feminine, True),
    ("elis", "is", Gender.feminine, True),  # Felis
    ("ea", "a", Gender.feminine, True),
    ("ela", "a", Gender.feminine, True),
    ("lla", "a", Gender.feminine, True),
    ("ena", "a", Gender.feminine, True),
    ("era", "a", Gender.feminine, True),
    ("erra", "a", Gender.feminine, True),
    ("ga", "a", Gender.feminine, True),
    ("genys", "s", Gender.feminine, True),  # See comments under Cteniogenyidae
    ("siren", "", Gender.feminine, True),
    ("gale", "e", Gender.feminine, True),
    ("manis", "is", Gender.feminine, True),
    (
        "ia",
        "a",
        Gender.feminine,
        True,
    ),  # probably has some false positives but worth it
    ("ica", "a", Gender.feminine, True),
    ("ictis", "is", Gender.feminine, True),  # or sometimes -ictid
    ("ila", "a", Gender.feminine, True),
    ("ina", "a", Gender.feminine, True),
    ("ira", "a", Gender.feminine, True),
    ("rna", "a", Gender.feminine, True),
    ("nycteris", "is", Gender.feminine, True),
    ("oa", "a", Gender.feminine, True),
    ("otoma", "a", Gender.feminine, True),
    ("pa", "a", Gender.feminine, True),
    ("ona", "a", Gender.feminine, True),
    ("ora", "a", Gender.feminine, True),
    ("lpes", "es", Gender.feminine, True),  # Vulpes
    ("oryctes", "es", Gender.feminine, True),
    ("ostrix", _rep("x", "g"), Gender.feminine, True),
    ("phitis", "is", Gender.feminine, True),  # Mephitis
    ("physis", "is", Gender.feminine, True),  # Coelophysis
    ("pteryx", _rep("x", "g"), Gender.feminine, True),
    ("rta", "a", Gender.feminine, True),
    ("semys", _rep("s", "d"), Gender.feminine, True),  # -emys turtles
    ("sha", "a", Gender.feminine, True),  # Manitsha
    ("ssa", "a", Gender.feminine, True),
    ("ista", "a", Gender.feminine, True),  # Sicista
    ("tta", "a", Gender.feminine, True),
    (
        "theis",
        "is",
        Gender.feminine,
        True,
    ),  # a guess, only occurs in the tunicate Nephtheis
    ("udo", _rep("o", "in"), Gender.feminine, True),  # Testudo
    ("thrix", _rep("x", "ch"), Gender.feminine, True),
    ("uis", "is", Gender.feminine, True),  # Anguis only
    ("ula", "a", Gender.feminine, True),
    ("ura", "a", Gender.feminine, True),
    ("ufa", "a", Gender.feminine, True),
    ("cta", "a", Gender.feminine, True),
    ("va", "a", Gender.feminine, True),
    ("vis", "is", Gender.feminine, True),
    ("uris", "is", Gender.feminine, True),  # Nanocuris
    ("zoa", "a", Gender.feminine, True),
    ("iza", "a", Gender.feminine, True),
    ("meles", "es", Gender.feminine, True),
    ("ama", "a", Gender.feminine, True),
    ("elys", "s", Gender.feminine, True),
    ("derma", lambda n: n + "t", Gender.neuter, True),
    ("erpeton", lambda n: n + "t", Gender.neuter, True),
    ("ceras", _rep("s", "t"), Gender.neuter, True),
    ("ion", "", Gender.neuter, True),
    ("ium", "um", Gender.neuter, True),
    ("num", "um", Gender.neuter, True),
    ("onon", "on", Gender.neuter, True),
    ("ron", "", Gender.neuter, True),
    ("sma", lambda n: n + "t", Gender.neuter, True),
    ("soma", lambda n: n + "t", Gender.neuter, True),
    ("stoma", lambda n: n + "t", Gender.neuter, True),
    ("therium", "um", Gender.neuter, True),
    ("tum", "um", Gender.neuter, True),
    ("izon", lambda n: n + "t", Gender.neuter, True),
    ("yum", "um", Gender.neuter, True),
    ("nion", "on", Gender.neuter, True),
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
