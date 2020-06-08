from pymongo import InsertOne, DeleteOne, ReplaceOne
import pymongo
from datetime import datetime, timedelta

def ensure_indexes(db=None, stations_collection=None, status_collection=None, metadata_collection=None):
    if status_collection != None:
        status_collection.create_index([ ('station_id', pymongo.ASCENDING), ('bucket_size', pymongo.ASCENDING) ])
        status_collection.create_index([ ('station_id', pymongo.ASCENDING), ('min_ts', pymongo.ASCENDING), ('max_ts', pymongo.ASCENDING)])
        status_collection.create_index([ ('expire_on', pymongo.ASCENDING) ], expireAfterSeconds=0 )


def get_station_last_updated(collection, feed):
    '''
    Gets the last updated timestamp and ttl for the provided feed.

    Returns: 
      - If feed found: { _id: "FEED", last_updated: SECONDS_SINCE_1970, ttl: TTL_IN_SECONDS }
      - If feed not found yet: { _id: "FEED", last_updated: 0, ttl: 0 }
      - None in case of errors.
    '''

    try:
        result = collection.find_one({'_id': feed})
        if result != None:
            return result
        else: 
            return { '_id': feed, 'last_updated': 0, 'ttl': 0 }

    except pymongo.errors.PyMongoError as e:
        print(str(datetime.today()) + ' ERROR Fetching last updated for feed ' + feed + ' failed: ' + e)
        return None

def update_station_information(stations, collection, batch_size=100):
    '''
    Bulk replace citi bike stations in MongoDB. New stations will be added automatically, changes in stations will be replaced.
    '''

    batched_operations = []
    for station in stations:
        batched_operations.append(pymongo.ReplaceOne(
            { '_id': station['_id'] },
            station,
            upsert=True))
        write_batch(batch=batched_operations, collection=collection, batch_size=batch_size, full_batch_required=True)

    # Don't forget the last batch that might not fill up the whole batch_size ;)
    write_batch(batch=batched_operations, collection=collection, batch_size=batch_size, full_batch_required=False)


def update_station_status(stations, collection, metadata_collection, feed, batch_size=100):
    '''
    Iterate over the stations and push the values into buckets.
    '''

    batched_operations = []

    for station in stations:

        # Prepare the current measurement for pushing into the bucket
        # Remove, but remember the station id, we need it for updating later on
        station_id = station.pop('station_id')

        # Add the timestamp when the data arrived, we want a date format for better readability
        station['ts'] = datetime.fromtimestamp(station['last_updated'])
        station['last_reported'] = datetime.fromtimestamp(station['last_reported'])

        # Add to the batch
        batched_operations.append(pymongo.UpdateOne(
            {
                # The station for which we add data
                'station_id': station_id,

                # We expect to execute the loader every 30 seconds, so roughly one hour will be stored in one bucket
                'bucket_size': { '$lt': 120 }
            },
            {
                # Add the new measurement to the bucket
                '$push': { 
                    'status': station
                },

                # Set the max value for the max timestamp of the document
                # Set the max value for the TTL index
                '$max': { 
                    'max_ts': station['ts'], 
                    'expire_on': station['ts'] + timedelta(days=3) 
                },

                # Set the min value for the min timestamp of the docuemnt
                '$min': { 'min_ts': station['ts'] },

                # Increase the bucket size counter by one
                '$inc': { 'bucket_size': 1 }
            },
            upsert=True))

        write_batch(batch=batched_operations, collection=collection, batch_size=batch_size, full_batch_required=True)

    # Don't forget the last batch that might not fill up the whole batch_size ;)
    write_batch(batch=batched_operations, collection=collection, batch_size=batch_size, full_batch_required=False)
    
    # Write metadata about the import
    set_station_last_updated(collection=metadata_collection, feed=feed, last_updated=stations[0]['last_updated'])


def write_batch(batch, collection, batch_size=100, full_batch_required=False):
    '''
    Writes batch of pymongo Bulk operations into the provided collection.
    Full_batch_required can be used to write smaller amounts of data, e.g. the last batch that does not fill the batch_size
    '''

    if len(batch) > 0 and ((full_batch_required and len(batch) >= batch_size) or not full_batch_required):
        try:
            result = collection.bulk_write(batch)
            print(str(datetime.today()) + ' Wrote ' + str(len(batch)) + ' to MongoDB (' + str(collection.name) + ').')
            batch.clear()
        except pymongo.errors.BulkWriteError as err:
            print(str(datetime.today()) + ' ERROR Writing to MongoDB: ' + str(err.details))


def set_station_last_updated(collection, feed, last_updated):
    '''
    Sets the last_updated value for the feed in the metadata collection

    Parameters:
      - collection: Should be the metadata collection
      - feed: Feed name to be updated
      - last_updated: Last updated value in seconds since 1970
    '''

    try:
        result = collection.update_one({'_id': feed}, { '$set': { 'last_updated': last_updated } }, upsert=True )
        print(str(datetime.today()) + ' INFO Updated last_updated for feed ' + feed + '.')

    except pymongo.errors.PyMongoError as e:
        print(str(datetime.today()) + ' ERROR Setting last updated for feed ' + feed + ' failed: ' + e)

