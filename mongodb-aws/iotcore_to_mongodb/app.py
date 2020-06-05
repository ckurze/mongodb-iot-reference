import json
import os

from pymongo import MongoClient
from aws_iot_mgt.device_management import get_all_things, get_status, get_station
from iot_citibike.mongodb.operations import update_station_information, update_station_status, refresh_stations, refresh_status


def lambda_handler(event, context):
    MONGO_URI = os.environ["MONGO_URI"]
    mongo_client = MongoClient(MONGO_URI)

    db = mongo_client.citibike

    station_data = event["state"]["reported"]
    station_status = get_status(station_data)
    station = get_station(station_data)
    refresh_stations(db, station)
    refresh_status(db, station_status)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }
