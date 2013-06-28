#!/bin/bash
mysql5 -u taxonomy -p < database.sql

python import.py -f data/Mammalia.csv -r

# Must be in order
FILES="Metatheria Eutheria Eulipotyphla Chiroptera Cetartiodactyla Perissodactyla Creodonta Condylarthra Pholidotamorpha Carnivoramorpha Euarchontoglires Glires Rodentia Muridae Cricetidae Hystricomorpha Sciuromorpha Castorimorpha"

for file in $FILES; do
	python import.py -f data/$file.csv || exit 1
done
