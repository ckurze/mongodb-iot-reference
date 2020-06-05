import json
from citibikeapi.bikeapi import get_full_station_info
from aws_iot_mgt.device_management import create_things, delete_all_things

def lambda_handler(event=None, context=None):
    stations = get_full_station_info()
    create_things(stations)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Refresh finished",
        }),
    }
