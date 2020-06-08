import os
from pymongo import MongoClient

from iot_citibike.citibike.load_data import get_station_information
from iot_citibike.mongodb.operations import prepare_mongodb, update_station_information

### Connect to Database
MONGO_URI = os.environ["MONGO_URI"]

if MONGO_URI == None:
	raise ValueError('No MongoDB Cluster provided. Will exit.')
	exit(-1)

### Setup MongoDB connection
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.citibike
stations_collection = db.stations

### The feed to get the data
STATION_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

### Ensure proper status of MongoDB
prepare_mongodb(stations_collection=stations_collection)

### Load station data from Citibikes
stations = get_station_information(url=STATION_URL)

### Device Registration - Initial load and periodic refresh of stations:
update_station_information(stations=stations, collection=stations_collection, batch_size=100)
