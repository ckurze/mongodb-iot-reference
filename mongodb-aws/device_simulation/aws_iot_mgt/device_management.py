import boto3
import json

def create_things(stations):
    #print(stations)
    thing = boto3.client('iot')
    shadow = boto3.client('iot-data')
    thingTypeName = "BikeStations"
    # Create thing type
    try:
        thing.create_thing_type(
            thingTypeName=thingTypeName,
            thingTypeProperties={
                "thingTypeDescription": "Type for CitiBike Stations in New York City"
            }
        )
    except:
        print("Type already available!")


    for station in stations.items():
        thing.create_thing(
            thingName = station[1]["station_id"],
            thingTypeName = thingTypeName,
        )

        payload = json.dumps({"state": {"reported": {
            "_id": station[1]["station_id"],
            "name": station[1]["name"],
            "capacity": station[1]["capacity"],
            "geometry": {"type": "Point", "coordinates": [station[1]["lon"], station[1]["lat"]]},
            "status": {
                "station_id": station[1]["status"]["station_id"],
                "last_reported": station[1]["status"]["last_reported"],
                "last_updated": station[1]["status"]["last_updated"],
                "is_renting": station[1]["status"]["is_renting"],
                "num_docks_available": station[1]["status"]["num_docks_available"],
                "num_docks_disabled": station[1]["status"]["num_docks_disabled"],
                "eightd_has_available_keys": station[1]["status"]["eightd_has_available_keys"],
                "num_ebikes_available": station[1]["status"]["num_ebikes_available"],
                "num_bikes_available": station[1]["status"]["num_bikes_available"],
                "num_bikes_disabled": station[1]["status"]["num_bikes_disabled"],
                "station_status": station[1]["status"]["station_status"],
                "is_installed": station[1]["status"]["is_installed"],
                "is_returning": station[1]["status"]["is_returning"],
                }
        }}})
        shadow.update_thing_shadow(
            thingName=station[1]["station_id"],
            payload=payload
        )

def delete_all_things(station_data):
    thing = boto3.client('iot')
    for key in station_data.keys():
        thing.delete_thing(
            thingName=key
        )

