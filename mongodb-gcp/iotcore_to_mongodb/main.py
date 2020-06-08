import base64
import os
import yaml
import json
from collections import defaultdict
from pymongo import MongoClient
import helper
import db_operations as operations

def process(event, context):
    '''
    Entrypoint for Cloud Function 'iotcore_to_mongodb'. Read messages IoT Hub and insert them into MongoDB.
    '''
    MONGO_URI = os.environ["MONGO_URI"]
    if MONGO_URI == None:
        raise ValueError('No MongoDB Cluster provided. Will exit.')
    
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client.citibike

    #read messages from event and group them by action
    message_str = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(message_str)
    grouped_messages = defaultdict(list)

    action = message.pop('action', 'none')  
    grouped_messages[action].append(message)

    #messages with action == fullRefresh are split into status and station messages
    split_full_refresh_messages(grouped_messages)

    #do "bulk" inserts for both types of messages (we used the same code as for Azure 
    #where bulk reading messages is possible. In this implementation each message is read seperately 
    #so no actual bulk inserts should happen)
    refresh_stations(db, grouped_messages.get('refreshStation', []))
    refresh_status(db, grouped_messages.get('refreshStatus', []))
    return

def split_full_refresh_messages(grouped_messages):
    '''
    Split messages with an action 'fullRefresh' into 'refreshStatus' and 'refreshStation' messages.
    '''
    full_refresh_msgs = grouped_messages.get('fullRefresh', [])
    for msg in full_refresh_msgs:
        grouped_messages['refreshStatus'].append(msg.pop('status'))
        grouped_messages['refreshStation'].append(msg)


def refresh_stations(db, messages):
    '''
    Insert 'station_information' updates into MongoDB.
    '''
    if len(messages) == 0:
        return

    stations_collection = db.stations

    operations.ensure_indexes(
        db=db, stations_collection=stations_collection)

    #pre process station information -> convert geo information to valid geo json object
    stations = helper.preprocess_stations(messages)
    #send bulk updates to database
    operations.update_station_information(
        stations=stations, collection=stations_collection, batch_size=100)


def refresh_status(db, messages):
    '''
    Insert 'status' updates into MongoDB.
    '''
    if len(messages) == 0:
        return
        
    status_collection = db.status
    metadata_collection = db.metadata
    operations.ensure_indexes(
        db=db, status_collection=status_collection, metadata_collection=metadata_collection)
    
    #find stations that should be updated based on last update
    stations_last_updated = operations.get_station_last_updated(
        collection=metadata_collection, feed='refreshStatus')
    stations = helper.get_station_status_to_update(
        messages, stations_last_updated)
    
    #update remaining stations
    operations.update_station_status(stations=stations, collection=status_collection,
                             metadata_collection=metadata_collection, feed='STATUS_URL', batch_size=100)

def load_env(path):
    with open(path) as yaml_file:
        values = yaml.load(yaml_file, Loader=yaml.FullLoader)
        for key in values:
            os.environ[key] = str(values[key])
