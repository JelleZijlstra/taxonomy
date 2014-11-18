#!/bin/bash
# Run all the appropriate programs for the taxonomy application

echo "Starting MySQL server..."
sudo /opt/local/lib/mysql5/bin/mysqld_safe &

echo "Starting EHPHP server..."
php /Users/jellezijlstra/Dropbox/git/web/server.php 3001 &

