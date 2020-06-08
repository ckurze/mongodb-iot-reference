import yaml
import os
from flask import Request, Response
from citibikeapi import bikeapi as api
from gcp_iot_mgt import device_management as device_mgt
from gcp_iot_mgt import device_communication as comm

def refresh(request):
    """
    This is the entrypoint for the cloud function 'device_simulation'.
    """
    #retrieve stations from the public citibike API
    stations = api.get_full_station_info()

    #create devices in IoT Core registry
    device_mgt.create_devices(stations)
    
    #send messages to the Iot Core
    comm.send_messages(stations)
    return ""

#can be used for local development - call load_env first
def load_env(path):
    with open(path) as yaml_file:
        values = yaml.load(yaml_file, Loader=yaml.FullLoader)
        for key in values:
            os.environ[key] = str(values[key])

