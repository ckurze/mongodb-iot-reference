import sys
import os
from .citibikeapi import bikeapi as api
from .azure_iot_mgt import device_communication as comm
from .azure_iot_mgt import device_management as devicemgt
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    '''
    This is the entrypoint for the Function 'refresh' of the 
    Azure Function App 'device_simulation'.
    '''
    #retrieve stations from the public citibike API
    stations = api.get_full_station_info()

    #create devices in the Azure IoT-Hub and 
    #fill the 'hub_endpoint' attribute for each station
    devicemgt.create_devices_and_find_endpoints(stations)
    
    #send messages to the Azure IoT Hub
    comm.send_messages(stations)


