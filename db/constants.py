'''Script to generate Python functions and constants from the constants.json file.
This performs some slightly evil manipulation of the module namespace.'''

import json
import os.path
import re
import sys

def _strip_comments(json):
	return re.sub(r'//[^\n]*', '', json)

def _my_dir():
	return os.path.dirname(__file__)

def _build():
	json_str = _strip_comments(open(_my_dir() + "/constants.json", "r").read())
	data = json.loads(json_str)
	constant_lookup = {}
	ns = sys.modules[__name__]
	for key in data:
		constant_lookup[key] = {}
		for entry in data[key]:
			setattr(ns, entry["constant"], entry["value"])
			constant_lookup[key][entry["value"]] = entry
		# Some trickery to capture the key variable
		def set_key(key):
			setattr(ns, "string_of_" + key, lambda c: constant_lookup[key][c]["name"])
		set_key(key)

	with open(_my_dir() + "/../public/js/constants.js", "w") as js_file:
		js_file.write("var constants = ")
		js_file.write(json_str)

_build()
