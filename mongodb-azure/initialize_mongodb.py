##########################################################################################
#
# This script initializes MongoDB, i.e. creates the necessary indexes and views.
# It can also go into initialization routinges of application components
# (as it is done in the other implementation examples).
#
###########################################################################################

import os
import pymongo
from iot_citibike.mongodb.indexes_views import prepare_mongodb

MONGO_URI = os.environ["MONGO_URI"] if "MONGO_URI" in os.environ else None
if MONGO_URI == None:
	raise ValueError('No MongoDB Cluster provided. Will exit.')
	exit(-1)

### Setup MongoDB connection
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client.citibike
stations_collection = db.stations
status_collection = db.status
metadata_collection = db.metadata

### Ensure proper status of MongoDB, i.e. indexes and views
prepare_mongodb(db=db, stations_collection=stations_collection, status_collection=status_collection, metadata_collection=metadata_collection)
