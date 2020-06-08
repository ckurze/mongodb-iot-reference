##########################################################################################
#
# This is an example subcriber to an MQTT Broker like HiveMQ.
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
import paho.mqtt.client as mqtt 
from pymongo import MongoClient

from iot_citibike.mongodb.indexes_views import prepare_mongodb
from iot_citibike.mongodb.operations import update_station_information

MONGO_URI = os.environ["MONGO_URI"] if "MONGO_URI" in os.environ else None
if MONGO_URI == None:
	raise ValueError('No MongoDB Cluster provided. Will exit.')
	exit(-1)

MQTT_HOST = os.environ["MQTT_HOST"] if "MQTT_HOST" in os.environ else None
if MQTT_HOST == None:
	raise ValueError('No MQTT Broker provided. Will exit.')
	exit(-1)

# The callback for when the client receives a CONNACK response from the MQTT server.
def on_connect(client, userdata, flags, rc):
	print('Connected to MQTT broker with result code ' + str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	client.subscribe('stations')

# The callback for when a PUBLISH message is received from the MQTT server.
# Optimization: As we receive many messages in one shot, the results should be processed in a batched manner.
def on_message(client, userdata, message):
	# print('Received message ' + str(message.payload) + ' on topic ' + message.topic + ' with QoS ' + str(message.qos))

	station = json.loads(message.payload)
	
	# Register the station in MongoDB
	update_station_information(stations=[ station ], collection=stations_collection, batch_size=1)

# Setup MQTT broker connection
client = mqtt.Client(client_id='citibike_station_subsriber')
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, 1883, 60)

# Setup MongoDB connection
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.citibike
stations_collection = db.stations

# Ensure proper status of MongoDB, i.e. indexes and views
prepare_mongodb(db=db, stations_collection=stations_collection)

# Start to listen to the HiveMQ Broker
client.loop_forever()
