def preprocess_stations(stations):
    '''
    Transforms lon and lat attributes into a valid GeoJSON object.
    '''
    for station in stations:
        station['_id'] = station.pop('station_id')
        station['geometry'] = {'type': 'Point',
                               'coordinates': [ station.pop('lon'), station.pop('lat') ] }
    return stations

def get_station_status_to_update(stations, last_updated=None):
    try:
        station = stations[0]
        if 'last_updated' not in station:
            raise ValueError('Station Status is not provided in the correct format. "last_updated" is missing.')
    
        feed_last_updated = station['last_updated']

        if feed_last_updated > last_updated['last_updated']:
            return stations

        else:
            return None
    
    except ValueError as e:
        print(e)