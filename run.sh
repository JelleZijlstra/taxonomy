#!/bin/bash
# Run all the appropriate programs for the taxonomy application
# prime sudo
sudo echo > /dev/null

echo "Starting MySQL server..."
mysql.server start

echo "Starting EHPHP server..."
cd /Users/jelle/Dropbox/git/pycatalog
.venv/bin/python -m catalog.csv_article_list --server --config-file catalog.ini &
