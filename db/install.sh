#!/bin/bash
mysql5 -u taxonomy -p < database.sql

# echo "Importing Mammalia..."
# python import.py -f data/Mammalia.csv -r || exit 1

# Must be in order
FILES="Excluded Mammalia Metatheria Eutheria Eulipotyphla Chiroptera Cetartiodactyla Perissodactyla Creodonta Condylarthra Pholidotamorpha Carnivoramorpha Euarchontoglires Glires Rodentia Muridae Cricetidae Hystricomorpha Sciuromorpha Castorimorpha"

for file in $FILES; do
	echo "Importing $file..."
	python import.py -f data/$file.csv || exit 1
done
