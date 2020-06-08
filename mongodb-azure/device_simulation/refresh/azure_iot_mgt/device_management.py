import os
import string
import random
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import ExportImportDevice, AuthenticationMechanism, SymmetricKey, QuerySpecification

__CHUNK_SIZE = 25
__DEVICE_KEY_LENGHT = 40

def delete_all_devices():
    '''
    Deletes all devices in the Azure IoT Hub. This function is not
    used in the Azure Function but can be useful, if you want to reset
    the Azure Environment. 
    '''
    connection_string = os.getenv("IOT_HUB_CONNECTION_STRING")
    iot_manager = IoTHubRegistryManager(connection_string)
    dev_list = iot_manager.get_devices()
    delete_list = []
    for d in dev_list:
        dev = ExportImportDevice(id=d.device_id)
        dev.import_mode = "delete"
        delete_list.append(dev)

    # splitting operations in chunks prevents to large messages
    # that raise exceptions
    del_chunks = chunks(delete_list, __CHUNK_SIZE)
    for chunk in del_chunks:
        iot_manager.bulk_create_or_update_devices(chunk)


def create_devices_and_find_endpoints(stations):
    '''
    Creates all stations that currently do not exist in the IoT Hub as new devices. 
    Adds 'hub_endpoint' attribute to all stations (no matter whether it already exists in the hub or not)
    which can be used to send messages to the stations
    '''
    # TODO: make create_and_get_devices indepentent of stations to allow reusing it in other iot use cases
    connection_string = os.getenv("IOT_HUB_CONNECTION_STRING")
    host = os.getenv("IOT_HUB_HOSTNAME")

    try:
        iot_manager = IoTHubRegistryManager(connection_string)
        # note that 'get_devices() only works for cnt of devices <= 1000,
        # otherwise querying keys is more complex AND time consuming
        dev_list = iot_manager.get_devices()
        # convert list to dict for easy access
        devices = {dev_list[i].device_id: dev_list[i]
                   for i in range(0, len(dev_list))}
        del dev_list

        new_devices = []

        for station in stations.values():
            device_name = station.get('station_name', station.get('station_id',''))
            device = devices.get(device_name, None)

            if device != None:
                # if device already exists, only add its endpoint to the station
                station['hub_endpoint'] = get_endpoint_uri(
                    host, device_name, device.authentication.symmetric_key.primary_key)
            else:
                # otherwise create device object and add its endpoint to station
                # the actual creation of the device in the hub happens at the end in a bulk
                new_device, new_endpoint = create_device(device_name, host)
                station['hub_endpoint'] = new_endpoint
                new_devices.append(new_device)

        # splitting operations in chunks prevents to large messages
        # that raise exceptions
        insert_chunks = chunks(new_devices, __CHUNK_SIZE)
        for chunk in insert_chunks:
            iot_manager.bulk_create_or_update_devices(chunk)
    except Exception as ex:
        print("Unexpected error {0}".format(ex))


def get_endpoint_uri(hostname, device_name, key):
    '''
    Creates the endpoint uri for Azure IoT Hub devices.
    '''
    return "HostName={0};DeviceId={1};SharedAccessKey={2}".format(hostname, device_name, key)


def create_device(device_name, host):
    '''
    creates ExportImportDevice from device_name with authentication
    and returns the device together with its endpoint uri. The device
    is not actually created in the IoT Hub. This can be achieved by
    passing the devices to the 'bulk_create_or_update_devices' function of
    a IoTHubRegistryManager instance. 
    '''

    #create authentication for device
    prim_key = gen_key(__DEVICE_KEY_LENGHT)
    sec_key = gen_key(__DEVICE_KEY_LENGHT)
    symkey = SymmetricKey(primary_key=prim_key, secondary_key=sec_key)
    authentication = AuthenticationMechanism(type="sas", symmetric_key=symkey)

    #create device with import mode = 'create'
    device = ExportImportDevice(
        id=device_name, status="enabled", authentication=authentication)
    device.import_mode = "create"
    #create endpoint uri
    endpoint = get_endpoint_uri(host, device_name, prim_key)

    return device, endpoint


def gen_key(n):
    '''
    Generates a random key, that is used to create the SymmetricKey used for
    the device authentication of our simulated devices.
    '''
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


def chunks(lst, n):
    '''Yield successive n-sized chunks from lst.'''
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
