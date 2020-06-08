import io
import os
import string
import random
from google.cloud import iot_v1 as gcp_iot


def delete_all_devices():
    """
    Deletes all devices in the IoT Core registry. This function is not
    used in the Cloud Function but can be useful, if you want to reset
    the GCP environment. 
    """
    device_manager = gcp_iot.DeviceManagerClient()
    parent = device_manager.registry_path(
        os.environ['IOT_PROJECT_ID'], 
        os.environ['IOT_REGION'], 
        os.environ['IOT_REGISTRY_ID'])
    devices = list(device_manager.list_devices(parent=parent))

    for device in devices:
        dev_path = device_manager.device_path(
            os.environ['IOT_PROJECT_ID'], 
            os.environ['IOT_REGION'], 
            os.environ['IOT_REGISTRY_ID'], 
            device.id)
        try:
            device_manager.delete_device(dev_path)
        except Exception as ex:
            print("Unexpected error {0}".format(ex))


def create_devices(stations):
    """
    Creates all stations that currently do not exist in the IoT Core registry as new devices. 
    To handle authentication pre created certificates are used, which are part of this git repository. This is only for demo purpose!
    """
    try:
        #get content of pre created certificate
        with io.open('certs/rsa_public.pem') as f:
            certificate = f.read()

        #retrieve a list of already existing devices
        device_manager = gcp_iot.DeviceManagerClient()
        parent = device_manager.registry_path(
            os.environ['IOT_PROJECT_ID'], 
            os.environ['IOT_REGION'], 
            os.environ['IOT_REGISTRY_ID'])
        dev_list = list(device_manager.list_devices(parent=parent))
        devices = {dev_list[i].id: dev_list[i]
                   for i in range(0, len(dev_list))}
        del dev_list

        for station in stations.values():
            device_id = get_device_id(station.get('station_id', ''))

            if devices.get(device_id, None) == None:
                #create new devices that do not exist
                create_device(device_manager, parent, device_id, certificate)

    except Exception as ex:
        print("Unexpected error {0}".format(ex))

def get_device_id(id):
    #device ids must not start with a number in gcp iot core registry
    return "dev_{0}".format(id)

def create_device(manager, parent, device_id, certificate):
    """
    Create new device in GCP iot core registry. All 'demo' devices in this device simulation
    uses the same certificate for demo purposes. In a real iot environment this is not recommended. 
    """
    device_template = {
        'id': device_id,
        'credentials': [{
        'public_key': {
            'format': 'RSA_PEM',
            'key': certificate
        }
    }]
    }

    manager.create_device(parent, device_template)
