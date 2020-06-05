import boto3
import json




def get_all_things():
    #Return all data from all things
    thing = boto3.client('iot')
    shadow = boto3.client('iot-data')
    data = thing.list_things(
        maxResults=250
    )
    things = data["things"]
    thing_ids = []
    for devices in things:
        thing_ids.append(devices["thingName"])
    while "nextToken" in data:
        data = thing.list_things(
            nextToken = data["nextToken"],
            maxResults = 250
        )
        things = data["things"]
        for devices in things:
            thing_ids.append(devices["thingName"])

    thing_data = []
    for id in thing_ids:
        response = shadow.get_thing_shadow(thingName=id)
        streamingBody = response["payload"]
        jsonState = json.loads(streamingBody.read())
        thing_data.append(jsonState["state"]["reported"])
    return thing_data

'''
def get_all_stations(station_data):
    #return only data from Staion_Information API
    stations = []
    for station in station_data:
        station.pop("status")
        stations.append(station)
    return stations
'''

#event = {'_id': '4017', 'name': 'E 138 St & 5 Av', 'capacity': 22, 'geometry': {'type': 'Point', 'coordinates': [40.81449, -73.936153]}, 'status': {'station_id': '4017', 'last_reported': 1591154063, 'last_updated': 1591170780, 'is_renting': 0, 'num_docks_available': 12, 'num_docks_disabled': 0, 'eightd_has_available_keys': False, 'num_ebikes_available': 0, 'num_bikes_available': 0, 'num_bikes_disabled': 10, 'station_status': 'active', 'is_installed': 1, 'is_returning': 1}}}, 'metadata': {'reported': {'_id': {'timestamp': 1591170999}, 'name': {'timestamp': 1591170999}, 'capacity': {'timestamp': 1591170999}, 'geometry': {'type': {'timestamp': 1591170999}, 'coordinates': [{'timestamp': 1591170999}, {'timestamp': 1591170999}]}, 'status': {'station_id': {'timestamp': 1591170999}, 'last_reported': {'timestamp': 1591170999}, 'last_updated': {'timestamp': 1591170999}, 'is_renting': {'timestamp': 1591170999}, 'num_docks_available': {'timestamp': 1591170999}, 'num_docks_disabled': {'timestamp': 1591170999}, 'eightd_has_available_keys': {'timestamp': 1591170999}, 'num_ebikes_available': {'timestamp': 1591170999}, 'num_bikes_available': {'timestamp': 1591170999}, 'num_bikes_disabled': {'timestamp': 1591170999}, 'station_status': {'timestamp': 1591170999}, 'is_installed': {'timestamp': 1591170999}, 'is_returning': {'timestamp': 1591170999}}}}, 'version': 151, 'timestamp': 1591170999}

def get_status(station_data):
    return station_data["status"]

def get_station(station_data):
    del station_data["status"]
    return station_data

'''
def get_all_status(station_data):
    #return only data from Staion_Details API
    station_status = []
    for station in station_data:
        station_status.append(station["status"])
    return station_status
'''