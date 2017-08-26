#!/bin/bash
# Run all the appropriate programs for the taxonomy application
# prime sudo
sudo echo > /dev/null

echo "Starting MySQL server..."
sudo /opt/local/lib/mysql5/bin/mysqld_safe &

echo "Starting EHPHP server..."
cd /Users/jellezijlstra/Dropbox/git/pycatalog
.venv/bin/python -m catalog.csv_article_list --server --config-file catalog.ini &
