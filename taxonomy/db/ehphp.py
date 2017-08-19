'''Interfacing with the EHPHP server'''

import requests
import json
from typing import Any

PORT = 3001
URL = "http://localhost:" + str(PORT) + "/"


def call_ehphp(cmd: str, args: Any) -> Any:
    if isinstance(args, list):
        args = dict(enumerate(args))
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
