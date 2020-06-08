##########################################################################################
#
# This is an example publisher to an MQTT Broker like HiveMQ.
#
# Note: It is not intended for production use since no authentication
#       and encrytpion is in place.
#
# For more information, please have a look at:
#		MongoDB Atlas: https://cloud.mongodb.com
#		MongoDB Client for python: https://api.mongodb.com/python/current/
#		HiveMQ MQTT Broker: https://www.hivemq.com/
#		Eclipse Paho MQTT Client for python: https://www.eclipse.org/paho/clients/python/ 
#											 and https://pypi.org/project/paho-mqtt/
#
###########################################################################################

import os
import json
from pymongo import MongoClient
import paho.mqtt.publish as publish

from iot_citibike.citibike.load_data import get_station_status
from iot_citibike.mongodb.operations import get_station_last_updated, set_station_last_updated

MQTT_HOST = os.environ["MQTT_HOST"] if "MQTT_HOST" in os.environ else None
if MQTT_HOST == None:
	raise ValueError('No MQTT Broker provided. Will exit.')
	exit(-1)

MONGO_URI = os.environ["MONGO_URI"] if "MONGO_URI" in os.environ else None
if MONGO_URI == None:
	raise ValueError('No MongoDB Cluster provided. Will exit.')
	exit(-1)

# Setup MongoDB connection for last import metadata
mongo_client = MongoClient(MONGO_URI)
metadata_collection = mongo_client.citibike.metadata

# The feed to get the data
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

# Get the last import timestamp
stations_last_udpated = get_station_last_updated(collection=metadata_collection, feed=STATUS_URL)

# Load current status of stations
station_status = get_station_status(url=STATUS_URL, last_updated=stations_last_udpated)

# Publish every status to HiveMQ MQTT Broker
# Prepare MQTT messages from status payload of the form 
# { 'topic':'status/<STATION_ID>', 'payload': station, 'qos':1, 'retain':True }
if station_status != None:
	messages = []
	for station in station_status['data']['stations']:
		station['last_updated'] = station_status['last_updated']
		msg = {
			'topic': 'status/' + station['station_id'],
			'payload': json.dumps(station),
			'qos': 1,
			'retain': False
		}
		messages.append(msg)

	# Publish station information to MQTT Broker
	publish.multiple(messages, hostname=MQTT_HOST, port=1883, client_id='citibike_status_publisher')

	set_station_last_updated(collection=metadata_collection, feed=STATUS_URL, last_updated=station_status['last_updated'])
