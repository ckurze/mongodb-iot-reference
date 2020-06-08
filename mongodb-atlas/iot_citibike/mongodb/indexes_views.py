import pymongo

def prepare_mongodb(db=None, stations_collection=None, status_collection=None, metadata_collection=None):
    ensure_indexes(db=db, stations_collection=stations_collection, status_collection=status_collection, metadata_collection=metadata_collection)
    ensure_views(db=db, stations_collection=stations_collection, status_collection=status_collection, metadata_collection=metadata_collection)

def ensure_indexes(db=None, stations_collection=None, status_collection=None, metadata_collection=None):
    if status_collection != None:
        status_collection.create_index([ ('station_id', pymongo.ASCENDING), ('bucket_size', pymongo.ASCENDING) ])
        status_collection.create_index([ ('station_id', pymongo.ASCENDING), ('min_ts', pymongo.ASCENDING), ('max_ts', pymongo.ASCENDING)])
        status_collection.create_index([ ('expire_on', pymongo.ASCENDING) ], expireAfterSeconds=0 )
    
def ensure_views(db=None, stations_collection=None, status_collection=None, metadata_collection=None):
    if db != None:
        # This is a bit harsh and should only be executed if there is a change!
        db['v_bike_availability'].drop()

        db.create_collection(
            'v_bike_availability',
            viewOn='status',
            pipeline=[ 
                # force index usage
                { '$sort': { 'station_id': 1, 'min_ts': 1, 'max_ts': 1 } }, 
                # get the latest entry per station
                { '$group': { 
                    '_id': { 'station_id': '$station_id' }, 
                    'latest_status': { '$last': '$status.num_bikes_available' }
                } }, 
                # only keep the last, i.e. most current entry of the bucket
                { '$addFields': { 'latest_status': { '$arrayElemAt': [ '$latest_status', -1 ] } } }, 
                # get the station metadata, we need capacity and geolocation
                { '$lookup': {
                    'from': 'stations', 
                    'localField': '_id.station_id', 
                    'foreignField': '_id', 
                    'as': 'station'
                } }, 
                # there is at max one station per id
                { '$unwind': '$station' }, 
                # get a beautiful output format and calculate availability ratio
                { '$project': {
                    '_id': 0, 
                    'station_id': '$_id.station_id', 
                    'station_capacity': '$station.capacity', 
                    'station_bikes_available': '$latest_status', 
                    'station_availability': {
                        '$cond': [
                            # Some stations have a capcity of 0, avoid divide by zero
                            { '$gt': [ '$station.capacity', 0 ] },
                            { '$round': [ { '$multiply': [ { '$divide': [ '$latest_status', '$station.capacity' ] }, 100 ] }, 2 ] },
                            0
                        ]
                    }, 
                    'geometry': '$station.geometry'
                } }
            ])

        db['v_avg_hourly_utilization'].drop()
        db.create_collection(
            'v_avg_hourly_utilization',
            viewOn='status',
            pipeline=[
                { '$match': {
                    '$expr': { 
                        '$and': [
                            # for index scan instead of collection scan, we add the station_id here
                            { 'gt': [ '$station_id', '0'] },
                            # we only want the last hour, i.e. 1000*60*60 = 3,600,000ms     
                            { '$lte': [ '$min_ts', '$$NOW' ] },
                            { '$gte': [ '$max_ts', { '$add': [ '$$NOW', -3600000 ] } ]}
                        ]
                    } 
                } },
                # We need all status information across the last hour, which could be across multiple buckets
                { '$unwind': { 'path': '$status' } }, 
                # Filter out those status objects that we don't need
                { '$match': {
                    '$expr': { 
                        '$and': [
                            # we only want the last hour, i.e. 1000*60*60 = 3,600,000ms     
                            { '$lte': [ '$status.ts', '$$NOW' ] },
                            { '$gte': [ '$status.ts', { '$add': [ '$$NOW', -3600000 ] } ]}
                        ]
                    } 
                } },
                { '$group': {
                    '_id': '$station_id', 
                    'status': { '$push': { 'num_bikes_available': '$status.num_bikes_available' } }
                } }, 
                { '$addFields': {
                    # Calculate the delta by abs(value - previous value)
                    'delta': {
                        '$map': {
                            'input': { '$range': [ 1, { '$size': '$status' } ] }, 
                            'as': 'i', 
                            'in': {
                                '$abs': {
                                    '$subtract': [
                                        { '$arrayElemAt': [ '$status.num_bikes_available', '$$i' ] }, 
                                        { '$arrayElemAt': [ '$status.num_bikes_available', { '$subtract': [ '$$i', 1 ] } ] }
                                    ]
                                }
                            }
                        }
                    }
                } }, 
                # The average of deltas
                { '$addFields': { 'avg_delta': { '$avg': '$delta' } } }, 
                # Get station information
                { '$lookup': {
                    'from': 'stations', 
                    'localField': '_id', 
                    'foreignField': '_id', 
                    'as': 'station'
                } }, 
                # We only have one station, so we can easily do a $unwind to the the station object
                { '$unwind': { 'path': '$station' } }, 
                # Format the output
                { '$project': {
                    '_id': 0,
                    'station_id': '$station._id',
                    'name': '$station.name',
                    'geometry': '$station.geometry',
                    'utilization': {
                        '$cond': [
                            # Some stations have a capcity of 0, avoid divide by zero
                            { '$gt': [ '$station.capacity', 0 ] }, 
                            { '$round': [ { '$multiply': [ { '$divide': [ '$avg_delta', '$station.capacity' ] }, 100 ] }, 2 ] }, 
                            0
                        ]
                    } } }
            ])