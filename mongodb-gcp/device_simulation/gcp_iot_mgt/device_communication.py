import json
import requests
import os
import ssl
import time
import datetime
import jwt
import base64


def send_messages(messages):
    #get headers (mainly for authentication)
    headers = get_auth_headers()

    for message in messages.values():

        #convert message to base64 encoded message body
        payload = json.dumps(message, sort_keys=True,
                             indent=4, separators=(',', ': '))
        encodedBytes = base64.b64encode(payload.encode("utf-8"))
        encodedStr = str(encodedBytes, "utf-8")
        data_container = {
            'binary_data': encodedStr
        }
        data = json.dumps(data_container)

        #get telemetry url for device
        telemetry_url = get_telemetry_url(get_device_id(message['station_id']))

        #post message to IoT Core
        response = requests.post(url=telemetry_url, headers=headers, data=data)
        # TODO: also post to state url


def get_telemetry_url(device_id):
    """
    This function build the url to which we can post telemetry events
    """
    return 'https://cloudiotdevice.googleapis.com/v1/projects/{0}/locations/{1}/registries/{2}/devices/{3}:publishEvent'.format(
        os.environ['IOT_PROJECT_ID'],
        os.environ['IOT_REGION'],
        os.environ['IOT_REGISTRY_ID'],
        device_id
    )

def get_device_id(id):
    #device ids must not start with a number in gcp iot core registry
    return "dev_{0}".format(id)

def get_auth_headers():
    jwtb = create_jwt(
        os.environ['IOT_PROJECT_ID'],
        "certs/rsa_private.pem", 
        "RS256")

    jwtstr = str(jwtb, "utf-8")
    headers = {
        'authorization': 'Bearer {0}'.format(jwtstr),
        'content-type': 'application/json',
        'cache-control': 'no-cache',
    }
    return headers

def create_jwt(project_id, private_key_file, algorithm):
    token = {
        # The time that the token was issued at
        'iat': datetime.datetime.utcnow(),
        # The time the token expires.
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=20),
        # The audience field should always be set to the GCP project id.
        'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()
    return jwt.encode(token, key=private_key, algorithm=algorithm)
