import json
import os
import requests
from azure.iot.device import IoTHubDeviceClient, Message

def send_messages(messages):
    '''
    Sends messages to the Azure Iot Hub. The function requires
    that every message has an attribute 'hub_endpoint' which contains the
    corresponding device url of the Azure Iot Hub.
    '''
    for message in messages.values():
        send_message(message)
        
def send_message(message): 
    '''
    Sends a message to the Azure Iot Hub. The function requires
    that the message has an attribute 'hub_endpoint' which contains the
    corresponding device url of the Azure Iot Hub.
    '''
    # Definitly not the most performant way of communicating with the hub but
    # should work for device simulation and keeps the code very simple
    device_hub_url = message.get('hub_endpoint')
    msgjson = json.dumps(message)
    msg = Message(msgjson)
    client = IoTHubDeviceClient.create_from_connection_string(device_hub_url)
    client.send_message(msg)
    client.disconnect()