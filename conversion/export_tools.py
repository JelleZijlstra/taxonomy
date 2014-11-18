from plugins.simpleodspy.sodsspreadsheet import SodsSpreadSheet
from plugins.simpleodspy.sodsods import SodsOds

from db.constants import *

RANK_STYLES = {
	SUBSPECIES: {"font_size": "10pt", "font_style": "italic"},
	SPECIES: {"font_size": "12pt"},
	SPECIES_GROUP: {"font_size": "12pt", "font_style": "italic"},
	SUBGENUS: {"font_size": "12pt", "font_style": "italic"},
	GENUS: {"font_size": "12pt", "font_weight": "bold"},
	SUBTRIBE: {"font_size": "12pt", "font_style": "italic", "font_weight": "bold"},
	TRIBE: {"font_size": "12pt", "font_style": "italic", "font_weight": "bold"},
	SUBFAMILY: {"font_size": "14pt", "font_style": "italic"},
	FAMILY: {"font_size": "14pt"},
	SUPERFAMILY: {"font_size": "16pt"},
	PARVORDER: {"font_size": "16pt", "font_weight": "bold"},
	INFRAORDER: {"font_size": "16pt", "font_style": "italic", "font_weight": "bold"},
	SUBORDER: {"font_size": "18pt"},
	ORDER: {"font_size": "20pt"},
	SUPERORDER: {"font_size": "22pt", "font_weight": "bold"},
	SUBCOHORT: {"font_size": "24pt", "font_style": "italic"},
	COHORT: {"font_size": "24pt"},
	SUPERCOHORT: {"font_size": "24pt", "font_weight": "bold"},
	INFRACLASS: {"font_size": "26pt"},
	SUBCLASS: {"font_size": "28pt"},
	CLASS: {"font_size": "30pt"},
	ROOT: {"font_size": "30pt", "font_weight": "bold"},
	UNRANKED: {"font_size": "14pt"},
}

for rank in RANK_STYLES:
	RANK_STYLES[rank]["font_family"] = "Times New Roman"

STATUS_STYLES = {
	STATUS_SYNONYM: {"font_size": "8pt"},
	STATUS_DUBIOUS: {"font_size": "10pt"},
}

for rank in RANK_STYLES:
	RANK_STYLES[rank]["font_family"] = "Times New Roman"
for rank in STATUS_STYLES:
	STATUS_STYLES[rank]["font_family"] = "Times New Roman"

# For some reasons, the spreadsheet isn't saved properly if it isn't resized at least once.
# Therefore, make it so small initially that it will certainly be resized.
INITIAL_HEIGHT = 10
COLUMNS = 18
ALPHABET_LENGTH = 26

# Encode row,column coordinates into form like "A1", "AA1"
def _encode_column(column):
	quotient = column / ALPHABET_LENGTH
	remainder = column % ALPHABET_LENGTH
	result = ""
	if quotient > 0:
		result = _encode_column(quotient - 1)
	char = chr(remainder + ord('A'))
	return result + char

def _encode_cell(row, column):
	return _encode_column(column) + str(row + 1)

def empty_row():
	return [None] * COLUMNS

class spreadsheet(object):
	def __init__(self, name):
		self.name = name
		self.max_height = INITIAL_HEIGHT
		self.height = 0
		self.sprsh = SodsSpreadSheet(i_max=self.height, j_max=COLUMNS)

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.save()

	def add_row(self, data, status=STATUS_VALID, rank=None):
		# resize if necessary
		if self.height == self.max_height:
			self.max_height += 1000
			self.sprsh.resizeTable(i_max=self.max_height)

		if len(data) != COLUMNS:
			raise Exception("Invalid data: expected " + str(COLUMNS) + " columns")

		row = self.height
		for col, field in enumerate(data):
			if field is None:
				field = ''
			coords = _encode_cell(row, col)
			self.sprsh.setValue(coords, field)
			if status == STATUS_VALID:
				try:
					styles = RANK_STYLES[rank]
				except KeyError:
					styles = RANK_STYLES[UNRANKED]
				self.sprsh.setStyle(coords, **styles)
			else:
				self.sprsh.setStyle(coords, **(STATUS_STYLES[status]))

		self.height += 1

	def save(self):
		file_name = "export/" + self.name + ".ods"
		ods = SodsOds(self.sprsh)
		ods.save(file_name)
