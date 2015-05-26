'''Script to generate a .py file from the constants.json file'''

import re


def strip_comments(json):
	return re.sub(json, r'//.*$', '')

if __name__ == '__main__':
	json = strip_comments(open("constants.json", "r").read())
