#!/bin/bash
PYTHON=$1
PORT=$2

while true; do
    TAXONOMY_CONFIG_FILE=~/taxonomy/taxonomy.ini sudo nohup /home/ec2-user/.local/bin/uv run --python=python3.12 -m hsweb  -p $PORT -b ~/hesperomys >>/home/ec2-user/hesperomys.log 2>&1
done
