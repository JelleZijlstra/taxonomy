'''Interfacing with the EHPHP server'''

import json
from typing import Any

import requests

PORT = 3001
URL = "http://localhost:" + str(PORT) + "/api"


def call_ehphp(cmd: str, args: Any) -> Any:
    if isinstance(args, list):
        args = {'files': args}
    args['includeMySQL'] = True
    params = {
        'command': cmd,
        'arguments': json.dumps(args),
        'format': 'json',
    }
    req = requests.post(URL, data=params)
    try:
        return req.json()
    except ValueError:  # invalid JSON
        raise Exception(req.text)
