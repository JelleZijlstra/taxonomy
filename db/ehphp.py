'''Interfacing with the EHPHP server'''

import requests
import json

PORT = 3001
URL = "http://localhost:" + str(PORT) + "/"

def call_ehphp(cmd, args):
	params = {
		'command': cmd,
		'arguments': json.dumps(args),
		'format': 'json',
	}
	req = requests.post(URL, data=params)
	return req.json()
