import os
from pymongo import MongoClient

from iot_citibike.mongodb.indexes_views import prepare_mongodb
from iot_citibike.citibike.load_data import get_station_status
from iot_citibike.mongodb.operations import update_station_status, get_station_last_updated

### Connect to Database
MONGO_URI = os.environ["MONGO_URI"]

if MONGO_URI == None:
	raise ValueError('No MongoDB Cluster provided. Will exit.')
	exit(-1)

### Setup MongoDB connection
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.citibike
status_collection = db.status
metadata_collection =db.metadata

### The feed to get the data
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

### Ensure proper status of MongoDB
prepare_mongodb(status_collection=status_collection, metadata_collection=metadata_collection)

### Get the last import timestamp
stations_last_udpated = get_station_last_updated(collection=metadata_collection, feed=STATUS_URL)

### Load current status of stations
station_status = get_station_status(url=STATUS_URL, last_updated=stations_last_udpated)

### Write the current status to MongoDB
update_station_status(station_status=station_status, collection=status_collection, metadata_collection=metadata_collection, feed=STATUS_URL, batch_size=100)
