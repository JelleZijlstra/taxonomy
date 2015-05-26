__doc__ = """

Module for fragile detection logic (e.g., stem/gender detection for genera).

"""

import collections
import re

from . import constants

Gender = constants.Gender

detection_result = collections.namedtuple('detection_result', ['stem', 'gender', 'confident'])

def _rep(pattern, replacement):
	return lambda name: re.sub(r'%s$' % pattern, replacement, name)

_endings = [
	('arctos', 'os', Gender.masculine, True),
	('apis', 'is', Gender.masculine, True),
	('alis', 'is', Gender.masculine, True),
	('avus', 'us', Gender.masculine, True),
	('bus', 'us', Gender.masculine, True),
	('caremys', 's', Gender.masculine, True),
	('c', '', Gender.masculine, True),  # Tenrec
	('amys', 's', Gender.masculine, True),
	('anger', '', Gender.masculine, True),  # Phalanger
	('aroo', 'oo', Gender.masculine, True),  # Based on Nambaroo novus and some others
	('atus', 'us', Gender.masculine, True),
	('ates', 'es', Gender.masculine, True),
	('baatar', '', Gender.masculine, True),  # masculine on the evidence of _Gobibaatar parvus_
	('ber', _rep('er', 'r'), Gender.masculine, True),  # Coluber, Fiber
	('bitis', 'is', Gender.masculine, True),  # Cobitis, some fish
	('cheirus', 'us', Gender.masculine, True),
	('eres', 'es', Gender.masculine, True),  # e.g. Loncheres, which can be both masculine and feminine but should probably be treated as masculine
	('chirus', 'us', Gender.masculine, True),
	('cho', 'o',  Gender.masculine, True),
	('choerus', 'us', Gender.masculine, True),
	('chus', 'us', Gender.masculine, True),
	('ceros', _rep('s', 't'), Gender.masculine, True),
	('cetus', 'us', Gender.masculine, True),
	('citor', '', Gender.masculine, True),
	('cnemus', 'us', Gender.masculine, True),
	('etes', 'es', Gender.masculine, True),
	('cormus', 'us', Gender.masculine, True),
	('glis', _rep('s', 'r'), Gender.masculine, True),
	('olus', 'us', Gender.masculine, True),
	('lides', 'es', Gender.masculine, True),
	('somus', 'us', Gender.masculine, True),  # Amblysomus
	('cropus', _rep('us', 'od'), Gender.masculine, True),
	('cus', 'us', Gender.masculine, True),
	('dapis', 'is', Gender.masculine, True),  # Adapis
	('eropus', _rep('us', 'od'), Gender.masculine, True),
	('moropus', 'us', Gender.masculine, True),  # Eomoropidae
	('eopus', _rep('us', 'od'), Gender.masculine, True),
	('eutes', 'es', Gender.masculine, True),
	('dypus', _rep('us', 'od'), Gender.masculine, True),
	('cyon', '', Gender.masculine, True),
	('dens', _rep('s', 't'), Gender.masculine, True),
	('dipus', _rep('us', 'od'), Gender.masculine, True),
	('ectes', 'es', Gender.masculine, True),
	('sor', '', Gender.masculine, True),
	('eo', lambda n: n + 'n', Gender.masculine, True),
	('eramus', _rep('s', 'r'), Gender.masculine, True),
	('erus', 'us', Gender.masculine, True),
	('eus', 'us', Gender.masculine, True),
	('glis', _rep('s', 'r'), Gender.masculine, True),
	('Glis', _rep('s', 'r'), Gender.masculine, True),
	('gus', 'us', Gender.masculine, True),
	('hippus', 'us', Gender.masculine, True),
	('ichthys', 's', Gender.masculine, True),
	('chestes', 'es', Gender.masculine, True),
	('edetes', 'es', Gender.masculine, True),  # Pedetes
	('ides', 'es', Gender.masculine, True),
	('ilus', 'us', Gender.masculine, True),
	('imys', 's', Gender.masculine, True),
	('inus', 'us', Gender.masculine, True),
	('io', lambda n: n + 'n', Gender.masculine, True),
	('ipes', _rep('s', 'd'), Gender.masculine, True),
	('irox', _rep('x', 'g'), Gender.masculine, True),
	('ites', 'es', Gender.masculine, True),
	('ius', 'us', Gender.masculine, True),
	('lepus', _rep('us', 'or'), Gender.masculine, True),
	('labis', _rep('s', 'd'), Gender.masculine, True),
	('lax', _rep('x', 'c'), Gender.masculine, True),
	('letes', 'es', Gender.masculine, True),
	('lestes', 'es', Gender.masculine, True),  # Greek "ὁ λῃστής" robber
	('lemur', '', Gender.masculine, True),
	('llodus', 'us', Gender.masculine, True),
	('lopus', _rep('us', 'od'), Gender.masculine, True),
	('loricus', 'us', Gender.masculine, True),
	('mmys', 's', Gender.masculine, True),
	('lus', 'us', Gender.masculine, True),
	('meryx', _rep('x', 'c'), Gender.masculine, True),
	('mamus', 'us', Gender.masculine, True),
	('orictes', 'es', Gender.masculine, True),
	('ephas', _rep('s', 'nt'), Gender.masculine, True),
	('mimus', 'us', Gender.masculine, True),
	('myscus', 'us', Gender.masculine, True),
	('ntes', 'es', Gender.masculine, True),
	('nus', 'us', Gender.masculine, True),
	('ocus', 'us', Gender.masculine, True),
	('odemus', 'us', Gender.masculine, True),  # Apodemus is not derived from Mus
	('adon', lambda n: n + 't', Gender.masculine, True),
	('edon', lambda n: n + 't', Gender.masculine, True),
	('idon', lambda n: n + 't', Gender.masculine, True),
	('odon', lambda n: n + 't', Gender.masculine, True),
	('odus', _rep('us', 'ont'), Gender.masculine, True),
	('omys', 's', Gender.masculine, True),
	('onyx', _rep('x', 'ch'), Gender.masculine, True),
	('opsis', 'is', Gender.masculine, True),
	('ornis', _rep('s', 'th'), Gender.masculine, True),
	('ous', 'us', Gender.masculine, True),
	('khos', 'os', Gender.masculine, True),  # Pachyrukhos
	('inos', 'os', Gender.masculine, True),
	('ddax', _rep('x', 'c'), Gender.masculine, True),
	('ntus', 'us', Gender.masculine, True),
	('itus', 'us', Gender.masculine, True),
	('ltus', 'us', Gender.masculine, True),
	('petes', 'es', Gender.masculine, True),
	('oryx', _rep('x', 'c'), Gender.masculine, True),
	('phus', 'us', Gender.masculine, True),
	('ppus', 'us', Gender.masculine, True),
	('pithecus', 'us', Gender.masculine, True),
	('pterus', 'us', Gender.masculine, True),
	('ratops', '', Gender.masculine, True),
	('rax', _rep('x', 'c'), Gender.masculine, True),
	('rhynchus', 'us', Gender.masculine, True),
	('saurus', 'us', Gender.masculine, True),
	('skos', 'os', Gender.masculine, True),
	('rus', 'us', Gender.masculine, True),
	('ctus', 'us', Gender.masculine, True),
	('copus', _rep('us', 'od'), Gender.masculine, True),
	('sciurus', 'us', Gender.masculine, True),
	('bos', _rep('s', 'v'), Gender.masculine, True),
	('mias', 'as', Gender.masculine, True),
	('rsus', 'us', Gender.masculine, True),
	('smus', 'us', Gender.masculine, True),
	('rhysis', 'is', Gender.masculine, True),
	('stachys', 's', Gender.masculine, True),  # http://en.wiktionary.org/wiki/%CF%83%CF%84%CE%AC%CF%87%CF%85%CF%82, but the plant genus name _Stachys_ is treated as feminine
	('sorex', _rep('ex', 'ic'), Gender.masculine, True),
	('ssus', 'us', Gender.masculine, True),
	('stomus', 'us', Gender.masculine, True),
	('sypus', _rep('us', 'od'), Gender.masculine, True),
	('tabes', 'es', Gender.feminine, True),  # see Amelotabes nov.pdf
	('tamus', 'us', Gender.masculine, True),
	('thus', 'us', Gender.masculine, True),
	('rtus', 'us', Gender.masculine, True),
	('tor', '', Gender.masculine, True),
	('tipus', _rep('us', 'od'), Gender.masculine, True),
	('triton', '', Gender.masculine, True),
	('urus', 'us', Gender.masculine, True),
	('uus', 'us', Gender.masculine, True),
	('rvus', 'us', Gender.masculine, True),
	('umys', 's', Gender.masculine, True),
	('ymys', 's', Gender.masculine, True),
	('ykus', 'us', Gender.masculine, True),
	('xus', 'us', Gender.masculine, True),
	('yus', 'us', Gender.masculine, True),
	('lerix', _rep('x', 'c'), Gender.masculine, True),

	('ala', 'a', Gender.feminine, True),
	('ana', 'a', Gender.feminine, True),
	('ara', 'a', Gender.feminine, True),
	('ca', 'a', Gender.feminine, True),
	('ema', 'a', Gender.feminine, True),
	('capra', 'a', Gender.feminine, True),
	('cneme', 'e', Gender.feminine, True),
	('coma', 'a', Gender.feminine, True),
	('delphys', 'ys', Gender.feminine, True),
	('da', 'a', Gender.feminine, True),
	('dna', 'a', Gender.feminine, True),
	('dra', 'a', Gender.feminine, True),
	('elis', 'is', Gender.feminine, True),  # Felis
	('ea', 'a', Gender.feminine, True),
	('ela', 'a', Gender.feminine, True),
	('lla', 'a', Gender.feminine, True),
	('ena', 'a', Gender.feminine, True),
	('era', 'a', Gender.feminine, True),
	('erra', 'a', Gender.feminine, True),
	('ga', 'a', Gender.feminine, True),
	('genys', 's', Gender.feminine, True),  # See comments under Cteniogenyidae
	('siren', '', Gender.feminine, True),
	('gale', 'e', Gender.feminine, True),
	('manis', 'is', Gender.feminine, True),
	('ia', 'a', Gender.feminine, True),  # probably has some false positives but worth it
	('ica', 'a', Gender.feminine, True),
	('ictis', 'is', Gender.feminine, True),  # or sometimes -ictid
	('ila', 'a', Gender.feminine, True),
	('ina', 'a', Gender.feminine, True),
	('ira', 'a', Gender.feminine, True),
	('rna', 'a', Gender.feminine, True),
	('nycteris', 'is', Gender.feminine, True),
	('oa', 'a', Gender.feminine, True),
	('otoma', 'a', Gender.feminine, True),
	('pa', 'a', Gender.feminine, True),
	('ona', 'a', Gender.feminine, True),
	('ops', 's', Gender.feminine, True),  # http://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.04.0057%3Aentry%3Do)%2Fy2 but frequently treated as masculine
	('ora', 'a', Gender.feminine, True),
	('lpes', 'es', Gender.feminine, True),  # Vulpes
	('oryctes', 'es', Gender.feminine, True),
	('ostrix', _rep('x', 'g'), Gender.feminine, True),
	('phitis', 'is', Gender.feminine, True),  # Mephitis
	('physis', 'is', Gender.feminine, True),  # Coelophysis
	('pteryx', _rep('x', 'g'), Gender.feminine, True),
	('rta', 'a', Gender.feminine, True),
	('semys', _rep('s', 'd'), Gender.feminine, True),  # -emys turtles
	('sha', 'a', Gender.feminine, True),  # Manitsha
	('ssa', 'a', Gender.feminine, True),
	('tta', 'a', Gender.feminine, True),
	('theis', 'is', Gender.feminine, True),  # a guess, only occurs in the tunicate Nephtheis
	('udo', _rep('o', 'in'), Gender.feminine, True),  # Testudo
	('thrix', _rep('x', 'ch'), Gender.feminine, True),
	('uis', 'is', Gender.feminine, True),  # Anguis only
	('ula', 'a', Gender.feminine, True),
	('ura', 'a', Gender.feminine, True),
	('ufa', 'a', Gender.feminine, True),
	('cta', 'a', Gender.feminine, True),
	('va', 'a', Gender.feminine, True),
	('vis', 'is', Gender.feminine, True),
	('uris', 'is', Gender.feminine, True),  # Nanocuris
	('zoa', 'a', Gender.feminine, True),
	('iza', 'a', Gender.feminine, True),
	('meles', 'es', Gender.feminine, True),

	('erpeton', lambda n: n + 't', Gender.neuter, True),
	('ceras', _rep('s', 't'), Gender.neuter, True),
	('ion', '', Gender.neuter, True),
	('ium', 'um', Gender.neuter, True),
	('num', 'um', Gender.neuter, True),
	('onon', 'on', Gender.neuter, True),
	('ron', '', Gender.neuter, True),
	('sma', lambda n: n + 't', Gender.neuter, True),
	('soma', lambda n: n + 't', Gender.neuter, True),
	('stoma', lambda n: n + 't', Gender.neuter, True),
	('therium', 'um', Gender.neuter, True),
	('tum', 'um', Gender.neuter, True),
	('izon', lambda n: n + 't', Gender.neuter, True),
	('yum', 'um', Gender.neuter, True),
]

# To discuss:
# -eles names: Proteles, Perameles feminine, Ateles, Brachyteles, Meles masculine


def _remove_ending(name, end):
	assert name.endswith(end)
	return re.sub(r'%s$' % end, '', name)


def detect_stem_and_gender(name):
	for ending, to_stem, gender, confidence in _endings:
		if name.lower().endswith(ending):
			if callable(to_stem):
				stem = to_stem(name)
			else:
				stem = _remove_ending(name, to_stem)
			return detection_result(stem, gender, confidence)
	return None
