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
import paho.mqtt.publish as publish

from iot_citibike.citibike.load_data import get_station_information

MQTT_HOST = os.environ["MQTT_HOST"] if "MQTT_HOST" in os.environ else None
if MQTT_HOST == None:
	raise ValueError('No MQTT Broker provided. Will exit.')
	exit(-1)

# The feed to get the data
STATION_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

# Load station data from Citibikes
stations = get_station_information(url=STATION_URL)

# Publish every station into the stations topic. 
# For the sake of simplicity, we publish all station information. 
# In a realistic environment, only new stations or changed stations would send their data.

# Prepare MQTT messages from station payload of the form 
# { 'topic':'stations', 'payload':station, 'qos':1, 'retain':True }
messages = []
for station in stations:
	msg = {
		'topic': 'stations',
		'payload': json.dumps(station),
		'qos': 1,
		'retain': True
	}
	messages.append(msg)

# Publish station information to MQTT Broker
publish.multiple(messages, hostname=MQTT_HOST, port=1883, client_id='citibike_station_publisher')
