#!/bin/bash
mysql5 -u taxonomy -p < $TAXONOMY_ROOT/db/database.sql

# echo "Importing Mammalia..."
# python import.py -f data/Mammalia.csv -r || exit 1

# Must be in order
FILES="root Mammalia Metatheria Eutheria Lipotyphla Chiroptera Cetartiodactyla Perissodactyla Creodonta Condylarthra Pholidotamorpha Carnivoramorpha Euarchontoglires Glires Rodentia Muridae Cricetidae Hystricomorpha Sciuromorpha Castorimorpha"

for file in $FILES; do
	echo "Importing $file..."
	python reimport.py -f $TAXONOMY_ROOT/data/$file.csv
done
